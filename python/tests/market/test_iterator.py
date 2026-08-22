"""`BookIterator`: one stream of events in, a stream of books and one of instruments out."""

from __future__ import annotations

import pytest

from rekep.market import (
    AssetKind,
    Book,
    BookIterator,
    ExecKind,
    Execution,
    Instrument,
    Order,
    Side,
    State,
)
from rekep.market.event import HOUR

#: An instant on an hour boundary, so a snapshot's `unix` is legible.
BASE = (1_787_000_000_000_000_000 // HOUR) * HOUR

BTC = Instrument(symbol="BTC-USD", exchange="XCME")
ETH = Instrument(symbol="ETH-USD", exchange="XCME")
#: The same instrument, as a later message spells it: more is known.
BTC_RICH = Instrument(
    symbol="BTC-USD", exchange="XCME", currency="USD", cfi="FFICSX", kind=AssetKind.FUTURE
)


def order(unix: int, about: Instrument, side: Side, px: float, qty: float, named: str, **given):
    declared = {
        "unix": unix,
        "symbol": about.symbol,
        "instrument": about,
        "side": side,
        "px": px,
        "qty": qty,
        "order_id": named,
        "state": State.NEW,
    }
    return Order(**{**declared, **given}).with_previous(None)


# -- one stream in, two out --------------------------------------------------


def test_the_books_and_the_instruments_are_two_views_of_one_fold() -> None:
    """Whichever is pulled drives the source; what the other would have seen is held."""
    events = [order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")]
    iterating = BookIterator(events=events)
    assert [one.unix for one in iterating.books] == [BASE]
    assert [one.symbol for one in iterating.instruments] == ["BTC-USD"]


def test_pulling_the_instruments_first_still_produces_every_book() -> None:
    events = [order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")]
    iterating = BookIterator(events=events)
    assert [one.symbol for one in iterating.instruments] == ["BTC-USD"]
    assert [one.unix for one in iterating.books] == [BASE]


def test_iterating_the_iterator_is_iterating_its_books() -> None:
    events = [order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")]
    assert [type(one) for one in BookIterator(events=events)] == [Book]


def test_the_source_is_read_once_and_not_started_over() -> None:
    """A generator source would be silently half-consumed by a second pass, and a
    list source would be folded forever."""
    events = iter([order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")])
    iterating = BookIterator(events=events)
    assert len(list(iterating.books)) == 1
    assert list(iterating.books) == [], "drained, and it says so"


# -- the state is per instrument ---------------------------------------------


def test_one_iterator_folds_every_instrument_on_its_own() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, ETH, Side.BID, 10.0, 3.0, "E1"),
        order(BASE + 20, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    iterating = BookIterator(events=events, snapshot_every=0)
    found = list(iterating.books)
    assert {one.symbol for one in found} == {"BTC-USD", "ETH-USD"}
    assert len(iterating.folding) == 2, "one mutable state per instrument, and no more"


def test_an_instrument_s_book_never_sees_another_s_liquidity() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, ETH, Side.BID, 999.0, 99.0, "E1"),
        order(BASE + 20, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    books = {one.symbol: one for one in BookIterator(events=events, snapshot_every=0)}
    assert books["BTC-USD"].bid_px == 100.0 and books["BTC-USD"].bid_qty == 5.0
    assert books["ETH-USD"].bid_px == 999.0


def test_a_stream_out_of_order_is_refused() -> None:
    """A fold asks the book to un-happen something, and there is no honest answer."""
    events = [
        order(BASE + 100, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE, ETH, Side.BID, 10.0, 3.0, "E1"),
    ]
    with pytest.raises(ValueError, match="time order"):
        list(BookIterator(events=events))


def test_the_order_is_checked_across_instruments_and_not_within_one() -> None:
    """The clock is the stream's, which is what makes an hourly boundary the same
    instant for every instrument in it."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE, ETH, Side.BID, 10.0, 3.0, "E1"),
        order(BASE + 1, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    assert len(list(BookIterator(events=events, snapshot_every=0))) == 3


# -- what the instrument stream carries --------------------------------------


def test_an_instrument_is_published_when_it_is_learnt_and_not_per_message() -> None:
    """A feed repeats the instrument on every message; a row per message would be
    the feed again rather than the reference data in it."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.BID, 99.0, 5.0, "B2"),
        order(BASE + 20, BTC, Side.BID, 98.0, 5.0, "B3"),
    ]
    assert len(list(BookIterator(events=events).instruments)) == 1


def test_a_message_that_knows_more_publishes_another_version() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC_RICH, Side.BID, 99.0, 5.0, "B2"),
    ]
    bare, rich = BookIterator(events=events).instruments
    assert bare.instrument.cfi is None and bare.instrument.kind is AssetKind.UNKNOWN
    assert rich.instrument.cfi == "FFICSX" and rich.instrument.kind is AssetKind.FUTURE
    assert rich.instrument.currency == "USD"
    assert rich.xhash == bare.xhash, "the same instrument, and an identity that did not move"
    assert rich.version == bare.version + 1 and rich.prev_hash == bare.hash
    assert rich.hash != bare.hash, "two versions of what is known, and two rows"


def test_learning_never_retracts_what_was_already_known() -> None:
    """Reference data arrives in whatever order a venue felt like sending it, and a
    later message that omits a field has not withdrawn it."""
    events = [
        order(BASE, BTC_RICH, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.BID, 99.0, 5.0, "B2"),
    ]
    (only,) = BookIterator(events=events).instruments
    assert only.instrument.cfi == "FFICSX"


# -- the hourly grid ----------------------------------------------------------


def test_a_gap_of_hours_is_filled_hour_by_hour() -> None:
    """A table whose hourly rows skip the hours nothing happened in is one you have
    to scan backwards to read, which is what hourly rows exist to avoid."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 3 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator(events=events))
    assert [(one.unix - BASE) // HOUR for one in found] == [0, 1, 2, 3, 3]
    assert [one.sunix is not None for one in found] == [False, True, True, True, False]


def test_a_snapshot_says_what_it_is_a_picture_of() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 2 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator(events=events))
    taken = [one for one in found if one.sunix is not None]
    assert [one.sunix for one in taken] == [BASE + 60, BASE + 60]
    assert [one.unix for one in taken] == [BASE + HOUR, BASE + 2 * HOUR]
    assert all(one.unix - one.sunix > 0 for one in taken), "staleness, without a join"


def test_every_instrument_gets_the_hour_and_not_only_the_one_that_traded() -> None:
    """The hour is a property of the clock: a book nothing happened to for three
    hours still stood there for three hours."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 70, ETH, Side.BID, 10.0, 3.0, "E1"),
        order(BASE + 3 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator(events=events))
    hours = {}
    for one in found:
        hours.setdefault(one.symbol, []).append((one.unix - BASE) // HOUR)
    assert hours["BTC-USD"] == [0, 1, 2, 3, 3]
    assert hours["ETH-USD"] == [0, 1, 2, 3]


def test_the_snapshots_are_versions_of_the_book_they_picture() -> None:
    """The book at 14:00 and the book at 15:00 are two rows of one book."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 2 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator(events=events))
    assert [one.version for one in found] == [0, 1, 2, 3]
    assert len({one.xhash for one in found}) == 1, "one book"
    assert len({one.hash for one in found}) == len(found), "and four versions of it"
    for before, after in zip(found, found[1:], strict=False):
        assert after.prev_hash == before.hash


def test_turning_the_grid_off_leaves_only_what_changed() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 5 * HOUR, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator(events=events, snapshot_every=0))
    assert len(found) == 2 and all(one.sunix is None for one in found)


def test_the_stream_ends_without_guessing_how_long_the_book_stood() -> None:
    """A snapshot fills the gap between two events; past the last one there is no
    gap, only a guess."""
    events = [order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1")]
    found = list(BookIterator(events=events))
    assert len(found) == 1 and found[0].sunix is None


# -- a side that did not move -------------------------------------------------


def test_a_side_that_did_not_move_keeps_the_version_it_had() -> None:
    """A `BookSide`'s hash identifies a version of that side, and a side nothing
    touched is still that version."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
        order(BASE + 20, BTC, Side.ASK, 100.4, 2.0, "A2"),
    ]
    first, second, third = BookIterator(events=events, snapshot_every=0).books
    assert second.bid_hash == first.bid_hash, "the bid did not move between them"
    assert third.bid_hash == first.bid_hash
    assert third.ask_hash != second.ask_hash, "and the ask did"


def test_a_side_that_did_not_move_carries_no_delta() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    _, second = BookIterator(events=events, snapshot_every=0).books
    assert second.bid_updates == [] and second.bid_executions == []
    assert len(second.ask_updates) == 1


def test_a_side_that_did_not_move_still_reports_its_state() -> None:
    """Carried, not dropped: a book row says what both sides are, always."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    _, second = BookIterator(events=events, snapshot_every=0).books
    assert second.bid_px == 100.0 and second.bid_qty == 5.0 and second.bid_depth == 1
    assert [one.px for one in second.bid_alive] == [100.0]
    assert second.spread == pytest.approx(0.5), "and the prices across the sides follow"


def test_a_trade_counts_as_the_side_moving() -> None:
    """It takes liquidity out, and a row that said otherwise would be wrong."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
        Execution(
            unix=BASE + 20,
            symbol="BTC-USD",
            instrument=BTC,
            side=Side.BID,
            px=100.0,
            qty=2.0,
            kind=ExecKind.TRADED,
            exec_id="EX-1",
        ).with_previous(None),
    ]
    first, _, third = BookIterator(events=events, snapshot_every=0).books
    assert third.bid_qty == 3.0 and third.bid_hash != first.bid_hash
    assert len(third.bid_executions) == 1
