"""What an instrument's identity is derived from, and what it refuses to derive it from."""

from __future__ import annotations

import datetime
from pathlib import Path

import pyarrow
import pytest

from rekep.enums import Protocol
from rekep.fix import FixCodec, FixRegistry
from rekep.market import (
    HASH,
    AssetKind,
    Currency,
    Instrument,
    InstrumentUpdate,
    Leg,
    Order,
    Side,
    TickRule,
)
from rekep.market.identity import NIL
from rekep.text import FixMsg, Message

FIX_DATA = Path(__file__).resolve().parents[3] / "data" / "fix"


def spread() -> Instrument:
    """One component whose nested values cross both conversion paths."""
    return Instrument(
        symbol="CAL-SPREAD",
        securityexchange="XCME",
        legs=[
            Leg(
                symbol="ESH7",
                side=Side.BUY,
                ratio=2,
                maturitydate=datetime.datetime(2027, 3, 19),
            )
        ],
    )


def test_scalar_component_and_update_conversion_is_bidirectional() -> None:
    component = spread()

    update = InstrumentUpdate.from_instrument(component, unix=31).identify()

    assert update.instrument is component
    assert update.xhash == component.xhash
    assert (update.creaunix, update.recunix) == (31, 31)
    assert Instrument.from_update(update) is component
    assert Instrument.from_(update) is component
    assert InstrumentUpdate.from_(component).instrument is component


def test_the_component_contains_reference_facts_and_no_event_envelope() -> None:
    assert Instrument.into_field().names == [
        "symbolticker",
        "symbol",
        "kind",
        "securityid",
        "securityidsource",
        "isincode",
        "securitytype",
        "cficode",
        "securityexchange",
        "currency",
        "contractmultiplier",
        "minpriceincrement",
        "roundlot",
        "quantitytype",
        "maturitydate",
        "strikeprice",
        "putorcall",
        "securitydesc",
        "legs",
        "tickladder",
    ]


def test_referential_key_and_tick_ladder_use_the_component_api() -> None:
    body = (
        "Referential(XLON|equity|dbi;GB00BN7SWP63_XLON_GBX|["
        "quantity-type=shares, tick-size-scale-id=PRIMARY|[[0|0.01], [100|0.05]], "
        "vendor-note=[inside|the, value]])"
    )
    message = Message.from_text(body)
    registry = FixRegistry(cache_dir=FIX_DATA)

    scalar = Instrument.from_referential_entries(message.entries, registry=registry)
    arrow = Instrument.from_referential_arrow(
        pyarrow.array(
            [[entry.into_dict() for entry in message.entries]],
            type=Message.into_field().field("entries").dtype,
        ),
        registry=registry,
    )[0].as_py()

    assert scalar == Instrument.from_dict(arrow)
    assert scalar.symbolticker == "XLON:ISINNumber:GB00BN7SWP63"
    assert scalar.kind is AssetKind.EQUITY
    assert scalar.securityid == scalar.isincode == "GB00BN7SWP63"
    assert scalar.securityidsource.name == "ISIN"
    assert scalar.securityexchange == "XLON"
    assert scalar.currency is Currency.from_str("GBX")
    assert scalar.quantitytype == 1
    assert scalar.tickladder == [
        TickRule(starttickpricerange=0, tickincrement=0.01),
        TickRule(starttickpricerange=100, tickincrement=0.05),
    ]

    parsed = FixMsg.from_message(message, registry=registry)
    (update,) = InstrumentUpdate.from_fixmsgs([parsed], registry=registry)
    assert update.instrument == scalar
    assert any(entry.key == "vendor-note" for entry in (*parsed.entries, *parsed.unmap))

    raw = next(iter(Message.into_arrow_reader([message])))
    parsed_batch = FixMsg.from_message_batch(raw, FixCodec(registry=registry))
    assert Instrument.from_dict(parsed_batch.column("instrument")[0].as_py()) == scalar
    (stored,) = FixMsg.from_arrow_reader([parsed_batch])
    (stored_update,) = InstrumentUpdate.from_fixmsgs([stored], registry=registry)
    assert stored_update.instrument == scalar


def test_oms_and_referential_share_vectorized_instrument_key_derivation() -> None:
    keys = pyarrow.array(["dbi;GB00BN7SWP63_XLON_GBX", "dbi;US0378331005_XNAS_USD"])

    found = Instrument.from_instrument_keys_arrow(
        keys,
        kind=pyarrow.array([int(AssetKind.EQUITY), int(AssetKind.EQUITY)]),
    )
    scalar = Instrument.from_instrument_key("dbi;GB00BN7SWP63_XLON_GBX", kind=AssetKind.EQUITY)

    assert Instrument.from_dict(found[0].as_py()) == scalar
    assert pyarrow.compute.struct_field(found, "symbolticker").to_pylist() == [
        "XLON:ISINNumber:GB00BN7SWP63",
        "XNAS:ISINNumber:US0378331005",
    ]
    assert pyarrow.compute.struct_field(found, "currency").to_pylist() == [
        int(Currency.from_str("GBX")),
        int(Currency.USD),
    ]


def test_tick_ladder_versions_reference_facts_without_moving_the_lifecycle() -> None:
    first = Instrument(
        symbol="AAPL",
        tickladder=[TickRule(starttickpricerange=0, tickincrement=0.01)],
    )
    revised = Instrument(
        symbol="AAPL",
        tickladder=[TickRule(starttickpricerange=0, tickincrement=0.05)],
    )
    first_update = InstrumentUpdate.from_instrument(first, unix=1).identify()
    revised_update = InstrumentUpdate.from_instrument(revised, unix=1).identify()

    assert first_update.xhash == revised_update.xhash
    assert first_update.vhash != revised_update.vhash


def test_other_protocol_cannot_publish_a_carried_instrument() -> None:
    message = FixMsg(protocol=Protocol.OTHER, instrument=Instrument(symbol="AAPL"))

    assert list(InstrumentUpdate.from_fixmsgs([message])) == []


def test_reference_updates_keep_the_plugin_that_recorded_their_source() -> None:
    event = Order(unix=3, plugin="market-reader").attach_instrument(Instrument(symbol="AAPL"))
    message = FixMsg(
        unix=4,
        plugin="fix-reader",
        protocol="FIX4.4",
        beginstring="FIX.4.4",
        instrument=Instrument(symbol="MSFT"),
    )

    (from_event,) = InstrumentUpdate.from_events([event])
    (from_message,) = InstrumentUpdate.from_fixmsgs([message])

    assert from_event.plugin == "market-reader"
    assert from_message.plugin == "fix-reader"


def test_arrow_component_and_update_conversion_matches_scalar_identity() -> None:
    components = [
        spread(),
        Instrument(symbol="AAPL", maturitydate=datetime.datetime(2027, 1, 15)),
    ]
    component_batch = Instrument.into_field().into_arrow_batch(components, owner=Instrument)

    plugins = pyarrow.array(["reference-a", "reference-b"])
    update_batch = InstrumentUpdate.from_instrument_arrow_batch(
        component_batch,
        unix=pyarrow.array([31, 32], pyarrow.int64()),
        plugin=plugins,
    )
    scalar = [
        InstrumentUpdate.from_instrument(component, unix=unix, plugin=plugin).identify()
        for component, unix, plugin in zip(components, (31, 32), plugins.to_pylist(), strict=True)
    ]

    assert update_batch.equals(InstrumentUpdate.into_arrow_batch(scalar), check_metadata=True)
    assert update_batch.column("vhash").to_pylist() == [update.vhash for update in scalar]
    assert update_batch.column("hash").to_pylist() == [
        update.into_row()["hash"] for update in scalar
    ]
    nested = update_batch.column("instrument")[0].as_py()
    assert "xhash" not in nested["legs"][0]
    assert nested["legs"][0]["maturitydate"] == datetime.datetime(2027, 3, 19)
    assert nested["legs"][0]["ratio"] == 2.0
    assert components[0].legs is not None
    assert components[0].legs[0].xhash == InstrumentUpdate.xhash_of(
        components[0].legs[0].symbolticker
    )
    assert update_batch.schema.field("xhash").type == HASH

    restored = Instrument.from_update_arrow_batch(update_batch)
    assert restored.equals(component_batch, check_metadata=True)
    assert [Instrument.from_dict(row) for row in restored.to_pylist()] == components


def test_aware_local_maturities_match_scalar_and_arrow_identity() -> None:
    east = datetime.timezone(datetime.timedelta(hours=2))
    component = Instrument(
        symbol="CAL-SPREAD",
        maturitydate=datetime.datetime(2027, 3, 19, 12, 30, tzinfo=east),
        legs=[
            Leg(
                symbol="ESH7",
                maturitydate=datetime.datetime(2027, 3, 19, 9, 15, tzinfo=east),
            )
        ],
    )
    assert component.maturitydate == datetime.datetime(2027, 3, 19, 10, 30)
    assert component.legs is not None
    assert component.legs[0].maturitydate == datetime.datetime(2027, 3, 19, 7, 15)

    component_batch = Instrument.into_field().into_arrow_batch([component], owner=Instrument)
    update_batch = InstrumentUpdate.from_instrument_arrow_batch(component_batch, unix=31)
    scalar = InstrumentUpdate.from_instrument(component, unix=31).identify()

    assert update_batch.column("vhash").to_pylist() == [scalar.vhash]
    assert update_batch.column("instrument")[0].as_py()["maturitydate"] == component.maturitydate


def test_fix_identifiers_choose_one_canonical_ticker_and_identity() -> None:
    variants = (
        Instrument(symbol="AAPL"),
        Instrument(symbol="AAPL", securityexchange="XNAS"),
        Instrument(symbol="AAPL", securityid="US0378331005", securityidsource="4"),
        Instrument(symbolticker="ignored", symbol="AAPL"),
    )
    assert [built.symbolticker for built in variants] == ["AAPL", "XNAS:AAPL", "AAPL", "AAPL"]
    assert [built.xhash for built in variants] == [
        InstrumentUpdate.xhash_of(built.symbolticker) for built in variants
    ]

    updates = [InstrumentUpdate.from_instrument(built, xhash=7, code="wrong") for built in variants]
    assert [(update.xhash, update.code) for update in updates] == [
        (InstrumentUpdate.xhash_of(built.symbolticker), built.symbolticker) for built in variants
    ]


def test_two_venues_using_the_same_symbol_are_distinct() -> None:
    """The venue, not the identifier, is what keeps them apart now."""
    one = Instrument(
        symbol="AAPL", securityexchange="XNAS", securityid="US0378331005", securityidsource="4"
    )
    other = Instrument(
        symbol="AAPL", securityexchange="XPAR", securityid="FR0000000001", securityidsource="4"
    )
    assert one.symbolticker == "XNAS:AAPL"
    assert other.symbolticker == "XPAR:AAPL"
    assert one.xhash != other.xhash


def test_two_readable_symbols_for_one_identifier_are_two_instruments() -> None:
    """The cost of leading with the symbol, stated: an ISIN no longer gathers
    the spellings a feed writes it under, so a venue that renames its symbol
    starts a second instrument. The identifier is still on the row, but it is
    not what identity is taken over."""
    one = Instrument(symbol="AAPL", securityid="US0378331005", securityidsource="4")
    other = Instrument(symbol="AAPL.OQ", securityid="US0378331005", securityidsource="4")
    assert (one.symbolticker, other.symbolticker) == ("AAPL", "AAPL.OQ")
    assert one.xhash != other.xhash


def test_a_feed_that_names_no_venue_still_gets_one_stable_identity() -> None:
    assert Instrument(symbol="BTC-USD").xhash == InstrumentUpdate.xhash_of("BTC-USD")
    assert Instrument(symbol="BTC-USD").xhash == Instrument(symbol="BTC-USD").xhash


def test_symbol_tickers_trim_whitespace_and_preserve_case() -> None:
    upper = Instrument(symbol="AAPL")
    lower = Instrument(symbol="aapl")
    spaced = Instrument(symbol=" AAPL ")
    assert upper.symbolticker == spaced.symbolticker == "AAPL"
    assert upper.xhash == spaced.xhash != lower.xhash


def test_an_instrument_with_no_key_at_all_is_visibly_unidentified() -> None:
    """A hash of emptiness would silently merge every unnamed instrument into one."""
    assert Instrument().xhash == NIL
    unidentified = InstrumentUpdate.from_instrument(
        Instrument(
            securityexchange="XCME",
            currency="USD",
            securityid="US0378331005",
        ),
        xhash=7,
        code="US0378331005",
    )
    assert (unidentified.xhash, unidentified.code) == (NIL, "")


def test_currency_input_is_normalised_to_the_persisted_int32_enum() -> None:
    assert Instrument(currency=" usd ").currency is Currency.USD


def test_reference_updates_are_not_snapshots() -> None:
    known = InstrumentUpdate.from_instrument(Instrument(symbol="AAPL"), unix=1).identify()

    assert not InstrumentUpdate.is_snapshot()
    assert known.make_snapshot(2) is None


def test_declared_reference_values_determine_the_value_hash() -> None:
    known = InstrumentUpdate.from_instrument(
        Instrument(symbol="AAPL", cficode="ESXXXX"), unix=1
    ).identify()
    revised = InstrumentUpdate.from_instrument(
        Instrument(symbol="AAPL", cficode="EXXXXX"), unix=1
    ).identify()
    observed_later = InstrumentUpdate.from_instrument(
        Instrument(symbol="AAPL", cficode="ESXXXX"), unix=1_000_001
    ).identify()
    leg = InstrumentUpdate.from_instrument(
        Instrument(symbol="SPREAD", legs=[Leg(symbol="A", ratio=1)]), unix=1
    ).identify()
    revised_leg = InstrumentUpdate.from_instrument(
        Instrument(symbol="SPREAD", legs=[Leg(symbol="A", ratio=2)]), unix=1
    ).identify()

    assert known.vhash != revised.vhash and known.hash != revised.hash
    assert leg.vhash != revised_leg.vhash and leg.hash != revised_leg.hash
    assert observed_later.vhash == known.vhash
    assert observed_later.hash != known.hash


def test_promoted_fallback_does_not_fabricate_unstated_clocks() -> None:
    message = FixMsg(unix=23, protocol="FIX4.4", instrument=Instrument(symbol="AAPL"))
    update = InstrumentUpdate.from_(message)

    assert update is not None
    assert update.unix == 23
    assert (update.creaunix, update.recunix) == (0, 0)
    assert update.instrument.symbolticker == "AAPL"
    assert Instrument.from_(message) == update.instrument


@pytest.mark.parametrize(
    ("symbol", "canonical"),
    (
        ("EUR/USD", "EUR/USD"),
        ("eur/usd", "EUR/USD"),
        ("XAU/USD", "XAU/USD"),
        ("EURUSD", "EUR/USD"),
        ("EUR.USD", "EUR/USD"),
        (" EUR/USD ", "EUR/USD"),
    ),
)
def test_an_iso_currency_pair_is_detected_at_construction(symbol: str, canonical: str) -> None:
    built = Instrument(symbol=symbol)
    assert built.symbolticker == canonical
    assert built.kind is AssetKind.CURRENCY
    assert built.currency is Currency.USD


@pytest.mark.parametrize("symbol", ("EUR-USD", "EUR/USDT", "ABC/XYZ"))
def test_non_iso_currency_spellings_are_not_autodetected(symbol: str) -> None:
    built = Instrument(symbol=symbol)
    assert built.kind is AssetKind.UNKNOWN
    assert built.currency is None


def test_explicit_pair_classification_and_currency_are_preserved() -> None:
    built = Instrument(symbol="EUR/USD", kind=AssetKind.FORWARD, currency=Currency.EUR)
    assert built.kind is AssetKind.FORWARD
    assert built.currency is Currency.EUR


def test_log_residual_tags_enrich_instruments_through_the_declared_registry() -> None:
    log = FixMsg(
        unix=1,
        protocol="FIX4.4",
        beginstring="FIX.4.4",
        msgtype="d",
        instrument=Instrument(symbol="FAKE-SYM"),
        entries=[(969, "0.01"), (561, "100"), (107, "FAKE-DESC")],
    )
    table = pyarrow.Table.from_pylist(
        [log.into_row()], schema=FixMsg.into_field().into_arrow_schema()
    )
    log = FixMsg.from_dict(table.to_pylist()[0])
    assert [(entry["tag"], entry["value"]) for entry in log.entries] == [
        (969, "0.01"),
        (561, "100"),
        (107, "FAKE-DESC"),
    ], "an Arrow round trip keeps every stored field, in wire order"
    registry = FixRegistry(cache_dir=FIX_DATA)
    (update,) = InstrumentUpdate.from_fixmsgs(
        [log],
        registry=registry,
    )
    instrument = update.instrument

    assert (instrument.minpriceincrement, instrument.roundlot, instrument.securitydesc) == (
        0.01,
        100.0,
        "FAKE-DESC",
    )
    assert update.unix == log.unix == 1


def test_repeated_tickers_merge_once_in_first_seen_order() -> None:
    first = InstrumentUpdate.from_instrument(
        Instrument(
            symbol="AAPL",
            securityid="US0378331005",
            securityidsource="4",
            securitydesc="Apple",
        ),
        unix=1,
    )
    other = InstrumentUpdate.from_instrument(Instrument(symbol="MSFT"), unix=1)
    later = InstrumentUpdate.from_instrument(
        Instrument(
            symbol="AAPL",
            securityid="US0378331005",
            securityidsource="4",
            kind=AssetKind.EQUITY,
            minpriceincrement=0.01,
            securitydesc="must not replace the first fact",
        ),
        unix=2,
    )

    found = list(InstrumentUpdate.enriched([first, other, later]))

    assert [row.instrument.symbolticker for row in found] == [
        "AAPL",
        "MSFT",
    ]
    merged = found[0]
    assert all(row.hash and row.vhash for row in found)
    assert merged.xhash == InstrumentUpdate.xhash_of(merged.instrument.symbolticker)
    assert merged.instrument.securitydesc == "Apple"
    assert merged.instrument.kind is AssetKind.EQUITY
    assert merged.instrument.minpriceincrement == 0.01


def test_repeated_reference_values_are_detected_by_vhash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Clock-only repeats have the same value hash and need no field comparison."""

    def unexpected(_self: Instrument, _other: Instrument) -> None:
        pytest.fail("equal value hashes reached field-by-field enrichment")

    monkeypatch.setattr(Instrument, "enriched_with", unexpected)

    first = InstrumentUpdate.from_instrument(Instrument(symbol="AAPL"), unix=1)
    second = InstrumentUpdate.from_instrument(Instrument(symbol="AAPL"), unix=2)
    (found,) = InstrumentUpdate.enriched([first, second])

    assert found.unix == 1
    assert found.vhash == second.identify().vhash


def test_reference_data_that_arrives_later_does_not_move_the_identity() -> None:
    """A tick or a maturity learnt afterwards is not part of the key, deliberately:
    an identity that moved when a field was enriched would break every join to it."""
    bare = Instrument(symbol="AAPL", securityexchange="XNAS")
    enriched = Instrument(
        symbol="AAPL",
        securityexchange="XNAS",
        minpriceincrement=0.01,
        roundlot=100.0,
        securitydesc="Apple Inc",
    )
    assert bare.xhash == enriched.xhash


def test_a_stored_record_only_gains_a_version_when_a_fact_is_added() -> None:
    """The versioning rule, whichever job writes the table.

    Restating a record writes nothing; adding a fact writes a new version of
    the same ticker; observing *less* than is stored writes nothing either --
    a thinner observation must not overwrite a fuller record.
    """
    stored = InstrumentUpdate.from_instrument(
        Instrument(symbol="AAPL", securityexchange="XNAS", currency="USD"), unix=10
    ).identify()
    by_ticker = {stored.instrument.symbolticker: stored}

    restated = InstrumentUpdate.from_instrument(
        Instrument(symbol="AAPL", securityexchange="XNAS", currency="USD"), unix=20
    )
    thinner = InstrumentUpdate.from_instrument(
        Instrument(symbol="AAPL", securityexchange="XNAS"), unix=20
    )
    fuller = InstrumentUpdate.from_instrument(
        Instrument(symbol="AAPL", securityexchange="XNAS", currency="USD", roundlot=100.0),
        unix=20,
    )

    assert list(InstrumentUpdate.versioned([restated.identify()], by_ticker)) == []
    assert list(InstrumentUpdate.versioned([thinner.identify()], by_ticker)) == []

    (written,) = InstrumentUpdate.versioned([fuller.identify()], by_ticker)
    assert written.instrument.symbolticker == stored.instrument.symbolticker
    assert written.xhash == stored.xhash, "a version does not move the lifecycle"
    assert written.instrument.roundlot == 100.0
    assert written.instrument.currency is Currency.USD
    assert written.vhash != stored.vhash and written.hash != stored.hash


def test_an_unknown_ticker_is_its_own_first_version() -> None:
    """An empty lookup is a table that holds nothing yet, not a refusal."""
    observed = [
        InstrumentUpdate.from_instrument(
            Instrument(symbol="AAPL", securityexchange="XNAS")
        ).identify(),
        InstrumentUpdate.from_instrument(
            Instrument(symbol="MSFT", securityexchange="XNAS")
        ).identify(),
    ]

    assert [row.instrument.symbolticker for row in InstrumentUpdate.versioned(observed, {})] == [
        "XNAS:AAPL",
        "XNAS:MSFT",
    ]
