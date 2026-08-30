"""What an instrument's identity is derived from, and what it refuses to derive it from."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fix import FixRegistry
from rekep.market import AssetKind, Currency, Instrument, Leg, versioned
from rekep.market.identity import NIL, hash_of
from rekep.text import FixMsg

FIX_DATA = Path(__file__).resolve().parents[3] / "data" / "fix"


def test_fix_identifiers_choose_one_canonical_ticker_and_identity() -> None:
    variants = (
        Instrument(symbol="AAPL"),
        Instrument(symbol="AAPL", securityexchange="XNAS"),
        Instrument(symbol="AAPL", securityid="US0378331005", securityidsource="4"),
        Instrument(symbol="AAPL", xhash=7, code="US0378331005"),
    )
    assert [(built.symbolticker, built.code) for built in variants] == [
        ("AAPL", "AAPL"),
        ("XNAS:AAPL", "XNAS:AAPL"),
        ("AAPL", "AAPL"),
        ("AAPL", "AAPL"),
    ]
    assert [built.xhash for built in variants] == [
        hash_of(built.symbolticker) for built in variants
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
    assert Instrument(symbol="BTC-USD").xhash == hash_of("BTC-USD")
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
    unidentified = Instrument(
        xhash=7,
        code="US0378331005",
        securityexchange="XCME",
        currency="USD",
        securityid="US0378331005",
    )
    assert (unidentified.xhash, unidentified.code) == (NIL, "")


def test_currency_input_is_normalised_to_the_persisted_int32_enum() -> None:
    assert Instrument(currency=" usd ").currency is Currency.USD


def test_flat_reference_records_are_not_snapshots() -> None:
    known = Instrument(unix=1, symbol="AAPL").identify()

    assert not Instrument.is_snapshot()
    assert known.make_snapshot(2) is None


def test_declared_reference_values_determine_the_value_hash() -> None:
    known = Instrument(unix=1, symbol="AAPL", cficode="ESXXXX").identify()
    revised = Instrument(unix=1, symbol="AAPL", cficode="EXXXXX").identify()
    observed_later = Instrument(unix=1_000_001, symbol="AAPL", cficode="ESXXXX").identify()
    leg = Instrument(unix=1, symbol="SPREAD", legs=[Leg(symbol="A", ratio=1)]).identify()
    revised_leg = Instrument(unix=1, symbol="SPREAD", legs=[Leg(symbol="A", ratio=2)]).identify()

    assert known.vhash != revised.vhash and known.hash != revised.hash
    assert leg.vhash != revised_leg.vhash and leg.hash != revised_leg.hash
    assert observed_later.vhash == known.vhash
    assert observed_later.hash != known.hash


def test_promoted_fallback_preserves_its_observation_clock() -> None:
    instrument = FixMsg(unix=23, symbol="AAPL").into_instrument()

    assert instrument is not None
    assert instrument.unix == 23


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
    registry = FixRegistry(cache_dir=FIX_DATA)
    transcription = {}
    into_instruments = FixMsg.into_instruments

    def captured(self: FixMsg, **declared: object):
        transcription.update(declared)
        return into_instruments(self, **declared)

    monkeypatch.setattr(FixMsg, "into_instruments", captured)

    (instrument,) = Instrument.from_fixmsgs(
        [log],
        registry=registry,
    )

    assert (instrument.minpriceincrement, instrument.roundlot, instrument.securitydesc) == (
        0.01,
        100.0,
        "FAKE-DESC",
    )
    assert instrument.unix == log.unix == 1
    assert transcription == {"registry": registry}


def test_repeated_tickers_merge_once_in_first_seen_order() -> None:
    class Source:
        def __init__(self, *instruments: Instrument) -> None:
            self.instruments = instruments

        def into_instruments(self, **_declared: object):
            return iter(self.instruments)

    first = Instrument(
        symbol="AAPL",
        securityid="US0378331005",
        securityidsource="4",
        altids={"RICCode": "AAPL.O"},
        securitydesc="Apple",
    )
    other = Instrument(symbol="MSFT")
    later = Instrument(
        symbol="AAPL",
        securityid="US0378331005",
        securityidsource="4",
        altids={"RICCode": "other", "CUSIP": "037833100"},
        kind=AssetKind.EQUITY,
        minpriceincrement=0.01,
        securitydesc="must not replace the first fact",
    )

    found = list(Instrument.from_fixmsgs([Source(first, other), Source(later)]))

    assert [row.symbolticker for row in found] == [
        "AAPL",
        "MSFT",
    ]
    merged = found[0]
    assert all(row.hash and row.vhash for row in found)
    assert merged.xhash == hash_of(merged.symbolticker)
    assert merged.securitydesc == "Apple"
    assert merged.kind is AssetKind.EQUITY
    assert merged.minpriceincrement == 0.01
    assert merged.altids == {"RICCode": "AAPL.O", "CUSIP": "037833100"}


def test_repeated_reference_values_are_detected_by_vhash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Clock-only repeats have the same value hash and need no field comparison."""

    class Source:
        @staticmethod
        def into_instruments(**_declared: object):
            return iter((Instrument(unix=1, symbol="AAPL"), Instrument(unix=2, symbol="AAPL")))

    def unexpected(_self: Instrument, _other: Instrument) -> None:
        pytest.fail("equal value hashes reached field-by-field enrichment")

    monkeypatch.setattr(Instrument, "enriched_with", unexpected)

    (found,) = Instrument.from_fixmsgs([Source()])

    assert found.unix == 1
    assert found.vhash == Instrument(unix=2, symbol="AAPL").identify().vhash


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
    stored = Instrument(symbol="AAPL", securityexchange="XNAS", currency="USD", unix=10).identify()
    by_ticker = {stored.symbolticker: stored}

    restated = Instrument(symbol="AAPL", securityexchange="XNAS", currency="USD", unix=20)
    thinner = Instrument(symbol="AAPL", securityexchange="XNAS", unix=20)
    fuller = Instrument(
        symbol="AAPL", securityexchange="XNAS", currency="USD", roundlot=100.0, unix=20
    )

    assert list(versioned([restated.identify()], by_ticker)) == []
    assert list(versioned([thinner.identify()], by_ticker)) == []

    (written,) = versioned([fuller.identify()], by_ticker)
    assert written.symbolticker == stored.symbolticker
    assert written.xhash == stored.xhash, "a version does not move the lifecycle"
    assert written.roundlot == 100.0 and written.currency is Currency.USD
    assert written.vhash != stored.vhash and written.hash != stored.hash


def test_an_unknown_ticker_is_its_own_first_version() -> None:
    """An empty lookup is a table that holds nothing yet, not a refusal."""
    observed = [
        Instrument(symbol="AAPL", securityexchange="XNAS").identify(),
        Instrument(symbol="MSFT", securityexchange="XNAS").identify(),
    ]

    assert [row.symbolticker for row in versioned(observed, {})] == ["XNAS:AAPL", "XNAS:MSFT"]
