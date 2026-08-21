"""What an instrument's identity is derived from, and what it refuses to derive it from."""

from __future__ import annotations

from rekep.market import Instrument
from rekep.market.identity import NIL, hash_of


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


def test_a_scheme_constant_keeps_a_symbol_from_colliding_with_a_registered_id() -> None:
    """Without it, a symbol that reads like an ISIN would be that ISIN."""
    symbol = Instrument(symbol="US0378331005", exchange="4")
    registered = Instrument(security_id="US0378331005", security_id_source="4")
    assert symbol.xhash != registered.xhash


def test_a_caller_that_knows_the_identity_keeps_it() -> None:
    """Reference data owns identity; a derivation is what a producer falls back to."""
    assert Instrument(xhash=7, symbol="AAPL").xhash == 7


def test_reference_data_that_arrives_later_does_not_move_the_identity() -> None:
    """A tick or a maturity learnt afterwards is not part of the key, deliberately:
    an identity that moved when a field was enriched would break every join to it."""
    bare = Instrument(symbol="AAPL", exchange="XNAS")
    enriched = Instrument(symbol="AAPL", exchange="XNAS", tick=0.01, lot=100.0, label="Apple Inc")
    assert bare.xhash == enriched.xhash
