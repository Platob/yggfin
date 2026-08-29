"""What an instrument's identity is derived from, and what it refuses to derive it from."""

from __future__ import annotations

import dataclasses
import datetime
from pathlib import Path

import pyarrow
import pytest

import rekep.market.instrument as instrument_module
from rekep.fix import FixRegistry
from rekep.market import AssetKind, Currency, Instrument, Leg, OptionKind, Side
from rekep.market.identity import NIL, hash_of
from rekep.text import FixMsg

FIX_DATA = Path(__file__).resolve().parents[3] / "data" / "fix"


def test_the_exact_symbol_forces_the_instrument_identity_and_readable_key() -> None:
    expected = hash_of("symbol", "", "AAPL")
    variants = (
        Instrument(symbol="AAPL"),
        Instrument(symbol="AAPL", securityexchange="XNAS"),
        Instrument(symbol="AAPL", securityid="US0378331005", securityidsource="4"),
        Instrument(symbol="AAPL", xhash=7, code="US0378331005"),
    )
    assert {(built.xhash, built.code) for built in variants} == {(expected, "AAPL")}


def test_two_venues_using_the_same_symbol_agree() -> None:
    one = Instrument(
        symbol="AAPL", securityexchange="XNAS", securityid="US0378331005", securityidsource="4"
    )
    other = Instrument(
        symbol="AAPL", securityexchange="XPAR", securityid="FR0000000001", securityidsource="4"
    )
    assert one.xhash == other.xhash


def test_two_symbols_for_the_same_registered_identifier_stay_distinct() -> None:
    one = Instrument(symbol="AAPL", securityid="US0378331005", securityidsource="4")
    other = Instrument(symbol="AAPL.OQ", securityid="US0378331005", securityidsource="4")
    assert one.xhash != other.xhash


def test_a_feed_that_names_no_venue_still_gets_one_stable_identity() -> None:
    assert Instrument(symbol="BTC-USD").xhash == hash_of("symbol", "", "BTC-USD")
    assert Instrument(symbol="BTC-USD").xhash == Instrument(symbol="BTC-USD").xhash


def test_symbol_identity_preserves_case_and_whitespace_exactly() -> None:
    assert (
        len(
            {
                Instrument(symbol="AAPL").xhash,
                Instrument(symbol="aapl").xhash,
                Instrument(symbol=" AAPL ").xhash,
            }
        )
        == 3
    )


def test_an_instrument_with_no_key_at_all_is_visibly_unidentified() -> None:
    """A hash of emptiness would silently merge every unnamed instrument into one."""
    assert Instrument().xhash == NIL
    unidentified = Instrument(
        xhash=7,
        code="US0378331005",
        securityexchange="XCME",
        currency="USD",
        securityid="US0378331005",
        securityidsource="4",
    )
    assert (unidentified.xhash, unidentified.code) == (NIL, "")


def test_currency_input_is_normalised_to_the_persisted_int32_enum() -> None:
    assert Instrument(currency=" usd ").currency is Currency.USD


@pytest.mark.parametrize("symbol", ("EUR/USD", "eur/usd", "XAU/USD"))
def test_a_slash_delimited_currency_pair_is_detected_at_construction(symbol: str) -> None:
    built = Instrument(symbol=symbol)
    assert built.kind is AssetKind.CURRENCY
    assert built.currency is Currency.from_str(symbol[4:])


@pytest.mark.parametrize("symbol", ("EURUSD", "EUR-USD", "EUR/USDT", " EUR/USD", "EUR/USD "))
def test_only_an_exact_three_letter_slash_pair_is_autodetected(symbol: str) -> None:
    built = Instrument(symbol=symbol)
    assert built.kind is AssetKind.UNKNOWN
    assert built.currency is None


def test_explicit_pair_classification_and_currency_are_preserved() -> None:
    built = Instrument(symbol="EUR/USD", kind=AssetKind.FORWARD, currency=Currency.EUR)
    assert built.kind is AssetKind.FORWARD
    assert built.currency is Currency.EUR


def test_log_residual_tags_enrich_instruments_through_the_declared_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log = FixMsg(
        unix=1,
        beginstring="FIX.4.4",
        msgtype="d",
        symbol="FAKE-SYM",
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
    registry = FixRegistry(cache_dir=FIX_DATA, offline=True)
    transcription = {}
    into_instruments = FixMsg.into_instruments

    def captured(self: FixMsg, **declared: object):
        transcription.update(declared)
        return into_instruments(self, **declared)

    monkeypatch.setattr(FixMsg, "into_instruments", captured)

    (instrument,) = Instrument.from_fixmsgs(
        [log],
        registry=registry,
        snapshot_every=0,
    )

    assert (instrument.minpriceincrement, instrument.roundlot, instrument.securitydesc) == (
        0.01,
        100.0,
        "FAKE-DESC",
    )
    assert transcription == {"registry": registry}


def test_instrument_log_interop_preserves_the_full_version_through_arrow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instrument = Instrument(
        unix=1_000,
        symbol="CAL-27",
        kind=AssetKind.MULTILEG,
        securityid="FR0000000001",
        securityidsource="4",
        altids={"RICCode": "CAL.N"},
        securitytype="MLEG",
        securityexchange="XPAR",
        currency=Currency.EUR,
        contractmultiplier=10.0,
        minpriceincrement=0.01,
        roundlot=1.0,
        maturitydate=datetime.date(2027, 6, 18),
        strikeprice=42.0,
        putorcall=OptionKind.CALL,
        securitydesc="Calendar spread",
        legs=[
            Leg(
                xhash=17,
                symbol="JUN-27",
                side=Side.BUY,
                ratio=1.0,
                kind=AssetKind.FUTURE,
                currency=Currency.EUR,
            ),
            Leg(symbol="SEP-27", side=Side.SELL, ratio=1.0, currency=Currency.EUR),
        ],
    ).with_previous(None)
    assert instrument is not None

    log = instrument.into_fixmsg()
    table = pyarrow.Table.from_pylist(
        [log.into_row()], schema=FixMsg.into_field().into_arrow_schema()
    )
    stored = FixMsg.from_dict(table.to_pylist()[0])

    def unexpected_fix_decode(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("normalized rows must not rebuild FIX state")

    monkeypatch.setattr(FixMsg, "into_fix_events", unexpected_fix_decode)
    restored = stored.into_instrument()

    assert restored is not None
    assert restored.into_dict() == instrument.into_dict()


def test_normalized_instrument_batches_decode_without_python_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instruments = [
        Instrument(
            unix=1_000,
            creaunix=900,
            recunix=1_100,
            parenthash=[11, 12],
            symbol="CAL-27",
            kind=AssetKind.MULTILEG,
            securityid="FR0000000001",
            securityidsource="4",
            altids={
                "orderid": "ORD-9",
                "clordid": "CL-7",
                "ISINNumber": "FR0000000001",
                "RICCode": "CAL.N",
                "Z": "vendor",
            },
            securitytype="MLEG",
            securityexchange="XPAR",
            currency=Currency.EUR,
            contractmultiplier=10.0,
            minpriceincrement=0.01,
            roundlot=1.0,
            maturitydate=datetime.date(2027, 6, 18),
            strikeprice=42.0,
            putorcall=OptionKind.CALL,
            securitydesc="Calendar spread",
            legs=[
                Leg(
                    symbol="JUN-27",
                    side=Side.BUY,
                    ratio=1.0,
                    kind=AssetKind.FUTURE,
                    currency=Currency.EUR,
                    maturitydate=datetime.date(2027, 6, 18),
                ),
                Leg(symbol="SEP-27", side=Side.SELL, ratio=2.0),
            ],
        ).identify(),
        Instrument(unix=2_000, creaunix=2_000, recunix=2_000, symbol="CASH").identify(),
    ]
    messages = [FixMsg.from_instrument(one) for one in instruments]
    source = pyarrow.Table.from_pylist(
        [one.into_row() for one in messages],
        schema=FixMsg.into_field().into_arrow_schema(),
    ).to_batches()[0]
    expected = pyarrow.Table.from_pylist(
        [one.into_row() for one in instruments],
        schema=Instrument.into_field().into_arrow_schema(),
    ).to_batches()[0]

    monkeypatch.setattr(FixMsg, "into_instrument", lambda *_args, **_kwargs: pytest.fail())
    found = FixMsg.into_instrument_arrow_batch(source)

    assert found.schema.equals(expected.schema, check_metadata=True)
    assert found.equals(expected)


def test_an_empty_normalized_instrument_batch_keeps_the_target_schema() -> None:
    schema = FixMsg.into_field().into_arrow_schema()
    empty = pyarrow.RecordBatch.from_arrays(
        [pyarrow.array([], field.type) for field in schema], schema=schema
    )
    found = FixMsg.into_instrument_arrow_batch(empty)
    assert found.num_rows == 0
    assert found.schema.equals(Instrument.into_field().into_arrow_schema(), check_metadata=True)


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


def test_a_repeated_instrument_spelling_is_hashed_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """A feed restates its instrument on every event; the spelling is the identity."""
    original = instrument_module.hash_of
    calls = 0

    def counted(*parts: object) -> int:
        nonlocal calls
        calls += 1
        return original(*parts)

    instrument_module._symbol_hash.cache_clear()
    monkeypatch.setattr(instrument_module, "hash_of", counted)
    first = Instrument(symbol="CACHE-TEST", securityexchange="XNAS")
    second = Instrument(symbol="CACHE-TEST", securityexchange="XNAS")

    assert first.xhash == second.xhash
    assert calls == 1
    instrument_module._symbol_hash.cache_clear()


def test_instrument_version_hashing_names_every_fact_and_leg_member() -> None:
    altids = Instrument.into_field().field("altids")
    assert Instrument.into_field().names.count("altids") == 1 and not altids.nullable
    declared = tuple(name for name in Instrument.__annotations__ if name != "xhash")
    assert instrument_module._INSTRUMENT_MEMBERS == (
        *declared[:5],
        "altids",
        *declared[5:],
    )
    assert instrument_module._LEG_MEMBERS == tuple(field.name for field in dataclasses.fields(Leg))


def test_instrument_version_hashing_is_stable_for_maps_dates_and_legs() -> None:
    maturity = datetime.date(2027, 6, 18)
    first = Instrument(
        symbol="CAL-27",
        kind=AssetKind.MULTILEG,
        altids={"RICCode": "CAL.N", "ISINNumber": "FR0000000001"},
        maturitydate=maturity,
        legs=[Leg(symbol="JUN-27", side=Side.BUY, ratio=1.0, maturitydate=maturity)],
    )
    reordered = dataclasses.replace(
        first,
        altids={"ISINNumber": "FR0000000001", "RICCode": "CAL.N"},
    )

    one = dataclasses.replace(first, unix=1).with_previous(None)
    two = dataclasses.replace(reordered, unix=1).with_previous(None)

    assert one is not None and two is not None
    assert one.hash == two.hash
    empty = dataclasses.replace(first, unix=1, altids={}, hash=0).identify().hash
    absent = dataclasses.replace(first, unix=1, altids=None, hash=0).identify().hash
    assert empty == absent, "the one required identifier map normalizes absence to empty"
