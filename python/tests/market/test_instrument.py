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
from rekep.text import Log

FIX_DATA = Path(__file__).resolve().parents[3] / "data" / "fix"


def test_a_registered_identifier_wins_over_the_symbol() -> None:
    """It is issued rather than chosen, so two vendors' spellings land on one identity."""
    built = Instrument(symbol="AAPL", security_id="US0378331005", security_id_source="4")
    assert built.xhash == hash_of("id", "4", "US0378331005")
    assert built.xhash != hash_of("symbol", "", "AAPL")


def test_two_vendors_spelling_the_same_registered_instrument_agree() -> None:
    """Which is the whole point of preferring the registered key."""
    one = Instrument(
        symbol="AAPL", exchange="XNAS", security_id="US0378331005", security_id_source="4"
    )
    other = Instrument(
        symbol="AAPL.OQ", exchange="XNGS", security_id="US0378331005", security_id_source="4"
    )
    assert one.xhash == other.xhash


def test_a_symbol_is_scoped_to_the_venue_that_spells_it() -> None:
    """`BTC-USD` is several different contracts, and they must not share an identity."""
    here = Instrument(symbol="BTC-USD", exchange="XCME")
    there = Instrument(symbol="BTC-USD", exchange="XCBT")
    assert here.xhash != there.xhash
    assert here.xhash == hash_of("symbol", "XCME", "BTC-USD")


def test_a_feed_that_names_no_venue_still_gets_one_stable_identity() -> None:
    assert Instrument(symbol="BTC-USD").xhash == hash_of("symbol", "", "BTC-USD")
    assert Instrument(symbol="BTC-USD").xhash == Instrument(symbol="BTC-USD").xhash


def test_a_half_registered_identifier_falls_back_rather_than_keying_on_half() -> None:
    """An id with no scheme names nothing; a scheme with no id names nothing either."""
    assert Instrument(symbol="AAPL", security_id="US0378331005").xhash == hash_of(
        "symbol", "", "AAPL"
    )
    assert Instrument(symbol="AAPL", security_id_source="4").xhash == hash_of("symbol", "", "AAPL")


def test_an_instrument_with_no_key_at_all_is_visibly_unidentified() -> None:
    """A hash of emptiness would silently merge every unnamed instrument into one."""
    assert Instrument().xhash == NIL
    assert Instrument(exchange="XCME", currency="USD").xhash == NIL, "neither is a key"


def test_currency_input_is_normalised_to_the_persisted_int32_enum() -> None:
    assert Instrument(currency=" usd ").currency is Currency.USD


def test_log_residual_tags_enrich_instruments_through_the_declared_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log = Log(
        unix=1,
        msg_type="d",
        symbol="AAPL",
        fix_tags=[(969, "0.01"), (561, "100"), (107, "Apple Inc")],
    )
    stored = log.into_dict()
    stored["fix_tags"] = [
        {"key": 969, "value": "0.01"},
        {"key": 561, "value": "100"},
        {"key": 107, "value": "Apple Inc"},
    ]
    table = pyarrow.Table.from_pylist([stored], schema=Log.into_field().into_arrow_schema())
    log = Log.from_dict(table.to_pylist()[0])
    assert log.fix_tags == [(969, "0.01"), (561, "100"), (107, "Apple Inc")]
    registry = FixRegistry(cache_dir=FIX_DATA, offline=True)
    transcription = {}
    into_instruments = Log.into_instruments

    def captured(self: Log, **declared: object):
        transcription.update(declared)
        return into_instruments(self, **declared)

    monkeypatch.setattr(Log, "into_instruments", captured)

    (instrument,) = Instrument.from_logs(
        [log],
        registry=registry,
        fix_version="4.4",
        snapshot_every=0,
    )

    assert (instrument.tick, instrument.lot, instrument.label) == (0.01, 100.0, "Apple Inc")
    assert transcription == {"registry": registry, "fix_version": "4.4"}


def test_instrument_log_interop_preserves_the_full_version_through_arrow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instrument = Instrument(
        unix=1_000,
        symbol="CAL-27",
        kind=AssetKind.MULTILEG,
        security_id="FR0000000001",
        security_id_source="4",
        alt_ids={"RIC": "CAL.N"},
        security_type="MLEG",
        exchange="XPAR",
        currency=Currency.EUR,
        multiplier=10.0,
        tick=0.01,
        lot=1.0,
        maturity=datetime.date(2027, 6, 18),
        strike=42.0,
        option_kind=OptionKind.CALL,
        label="Calendar spread",
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

    log = instrument.into_log()
    table = pyarrow.Table.from_pylist(
        [log.into_dict()], schema=Log.into_field().into_arrow_schema()
    )
    stored = Log.from_dict(table.to_pylist()[0])

    def unexpected_fix_decode(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("normalized rows must not rebuild FIX state")

    monkeypatch.setattr(Log, "into_fix_events", unexpected_fix_decode)
    restored = stored.into_instrument()

    assert restored is not None
    assert restored.into_dict() == instrument.into_dict()


def test_a_scheme_constant_keeps_a_symbol_from_colliding_with_a_registered_id() -> None:
    """Without it, a symbol that reads like an ISIN would be that ISIN."""
    symbol = Instrument(symbol="US0378331005", exchange="4")
    registered = Instrument(security_id="US0378331005", security_id_source="4")
    assert symbol.xhash != registered.xhash


def test_a_caller_that_knows_the_identity_keeps_it() -> None:
    """Instrument data owns identity; a derivation is a producer fallback."""
    assert Instrument(xhash=7, symbol="AAPL").xhash == 7


def test_reference_data_that_arrives_later_does_not_move_the_identity() -> None:
    """A tick or a maturity learnt afterwards is not part of the key, deliberately:
    an identity that moved when a field was enriched would break every join to it."""
    bare = Instrument(symbol="AAPL", exchange="XNAS")
    enriched = Instrument(symbol="AAPL", exchange="XNAS", tick=0.01, lot=100.0, label="Apple Inc")
    assert bare.xhash == enriched.xhash


def test_a_repeated_instrument_spelling_is_hashed_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """A feed restates its instrument on every event; the spelling is the identity."""
    original = instrument_module.hash_of
    calls = 0

    def counted(*parts: object) -> int:
        nonlocal calls
        calls += 1
        return original(*parts)

    instrument_module._identity_hash.cache_clear()
    monkeypatch.setattr(instrument_module, "hash_of", counted)
    first = Instrument(symbol="CACHE-TEST", exchange="XNAS")
    second = Instrument(symbol="CACHE-TEST", exchange="XNAS")

    assert first.xhash == second.xhash
    assert calls == 1
    instrument_module._identity_hash.cache_clear()


def test_instrument_identity_names_every_fact_and_leg_member() -> None:
    assert instrument_module._INSTRUMENT_MEMBERS == tuple(
        name for name in Instrument.__annotations__ if name != "xhash"
    )
    assert instrument_module._LEG_MEMBERS == tuple(field.name for field in dataclasses.fields(Leg))


def test_instrument_version_hashing_is_stable_for_maps_dates_and_legs() -> None:
    maturity = datetime.date(2027, 6, 18)
    first = Instrument(
        symbol="CAL-27",
        kind=AssetKind.MULTILEG,
        alt_ids={"RIC": "CAL.N", "ISIN": "FR0000000001"},
        maturity=maturity,
        legs=[Leg(symbol="JUN-27", side=Side.BUY, ratio=1.0, maturity=maturity)],
    )
    reordered = dataclasses.replace(
        first,
        alt_ids={"ISIN": "FR0000000001", "RIC": "CAL.N"},
    )

    one = dataclasses.replace(first, unix=1).with_previous(None)
    two = dataclasses.replace(reordered, unix=1).with_previous(None)

    assert one is not None and two is not None
    assert one.hash == two.hash
    empty = dataclasses.replace(first, unix=1, alt_ids={}, hash=0).identify().hash
    absent = dataclasses.replace(first, unix=1, alt_ids=None, hash=0).identify().hash
    assert empty != absent
