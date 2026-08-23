"""`BookIterator`: one stream of events in and one book stream out."""

from __future__ import annotations

import dataclasses

import pyarrow
import pytest

import rekep.market.book as book_module
from rekep.market import (
    AssetKind,
    Book,
    BookIterator,
    Currency,
    Execution,
    Instrument,
    MarketEvent,
    MarketKind,
    Order,
    Side,
    State,
)
from rekep.market.book import _Side
from rekep.market.event import DAY, HOUR
from rekep.text import Log

#: An instant on an hour boundary, so a snapshot's `unix` is legible.
BASE = (1_787_000_000_000_000_000 // HOUR) * HOUR

BTC = Instrument(symbol="BTC-USD", exchange="XCME")
ETH = Instrument(symbol="ETH-USD", exchange="XCME")
#: The same instrument, as a later message spells it: more is known.
BTC_RICH = Instrument(
    symbol="BTC-USD", exchange="XCME", currency="USD", cfi="FFICSX", kind=AssetKind.FUTURE
)


def initial[EventT: MarketEvent](event: EventT, instrument: Instrument = BTC) -> EventT:
    """Attach transient reference data and require an initial version."""
    built = event.attach_instrument(instrument).with_previous(None)
    assert built is not None
    return built


def order(unix: int, about: Instrument, side: Side, px: float, qty: float, named: str, **given):
    declared = {
        "unix": unix,
        "code": about.symbol,
        "side": side,
        "px": px,
        "qty": qty,
        "order_id": named,
        "state": State.NEW,
    }
    return initial(Order(**{**declared, **given}), about)


def with_instruments(events, instruments):
    """One time-sorted stream, with reference versions visible at equal instants."""
    return sorted(
        [*events, *instruments],
        key=lambda event: (
            event.unix,
            0 if isinstance(event, Instrument) else 1,
            -1 if event.seq is None else event.seq,
            event.hash,
        ),
    )


# -- one stream in, one out --------------------------------------------------


def test_a_negative_snapshot_interval_is_refused_before_iteration() -> None:
    with pytest.raises(ValueError, match="snapshot_every"):
        BookIterator(snapshot_every=-1)


def test_orders_expire_after_one_unchanged_day_by_default() -> None:
    assert BookIterator().max_order_age_ns == DAY


def test_instrument_versioning_is_owned_outside_the_book_fold() -> None:
    events = [order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")]
    iterating = BookIterator.from_events(events)
    assert [one.unix for one in iterating.books] == [BASE]
    assert [one.code for one in Instrument.from_events(events)] == ["BTC-USD"]


def test_sorted_logs_feed_instruments_and_books_without_a_task_adapter() -> None:
    log = Log(
        unix=BASE,
        msg_type="D",
        symbol="BTC-USD",
        cl_ord_id="B1",
        side="1",
        ord_type="2",
        price=100.0,
        order_qty=2.0,
    )

    (instrument,) = Instrument.from_logs([log], snapshot_every=0)
    (book,) = BookIterator(logs=[instrument.into_log(), log], snapshot_every=0)

    assert instrument.symbol == book.code == "BTC-USD"
    assert book.instrument_xhash == instrument.xhash
    assert book.bid_px == 100.0 and book.bid_qty == 2.0


def test_log_symbol_uses_the_best_available_instrument_spelling() -> None:
    columns = {
        "symbol": pyarrow.array(["AAPL", None, None]),
        "security_id": pyarrow.array(["ignored", "US0378331005", None]),
        "isincode": pyarrow.array([None, "ignored", "FR0000120271"]),
    }
    assert Log.symbol_arrow(columns, 3).to_pylist() == [
        "AAPL",
        "US0378331005",
        "FR0000120271",
    ]


def test_reference_input_is_read_only_and_books_remain_the_only_output() -> None:
    events = [order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")]
    instruments = list(Instrument.from_events(events))
    iterating = BookIterator.from_events(with_instruments(events, instruments))
    assert [one.unix for one in iterating.books] == [BASE]
    assert instruments[0].version == 0, "folding did not version or replace its input"


def test_checkpoint_rows_are_globally_boundary_ordered_across_instruments() -> None:
    events = [
        order(BASE + 1, BTC, Side.BID, 100.0, 1.0, "B1"),
        order(BASE + 2, ETH, Side.BID, 10.0, 1.0, "E1"),
        order(BASE + 3 * HOUR + 1, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    streams = (
        (list(BookIterator.from_events(events)), "instrument_xhash"),
        (list(Instrument.from_events(events)), "xhash"),
    )
    for snapshots, key in streams:
        snapshots = [row for row in snapshots if row.sunix is not None]
        assert [row.unix for row in snapshots] == sorted(row.unix for row in snapshots)
        for boundary in (BASE + HOUR, BASE + 2 * HOUR, BASE + 3 * HOUR):
            assert {getattr(row, key) for row in snapshots if row.unix == boundary} == {
                BTC.xhash,
                ETH.xhash,
            }


def test_book_iteration_never_queues_a_parallel_reference_stream() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC_RICH, Side.BID, 99.0, 4.0, "B2"),
        order(BASE + 20, ETH, Side.ASK, 10.5, 3.0, "E1"),
    ]
    expected_books = [one.into_dict() for one in BookIterator.from_events(events).books]
    pulled: list[int] = []

    iterating: BookIterator

    def source():
        for event in events:
            pulled.append(event.unix)
            yield event

    iterating = BookIterator.from_events(source())
    assert [one.into_dict() for one in iterating] == expected_books
    assert pulled == [event.unix for event in events]


def test_an_out_of_order_rotated_segment_keeps_a_distinct_instrument() -> None:
    instruments = list(
        Instrument.from_observations(
            [(BASE + 10, BTC, 7), (BASE, ETH, 3)],
            snapshot_every=0,
        )
    )

    assert {known.xhash for known in instruments} == {BTC.xhash, ETH.xhash}
    assert [known.unix for known in instruments] == [BASE + 10, BASE + 10]
    assert [known.seq for known in instruments] == [7, 3]


def test_conflicting_security_ids_do_not_merge_through_a_shared_symbol() -> None:
    first = Instrument(
        symbol="ABC",
        exchange="XPAR",
        security_id="111111111",
        security_id_source="1",
    )
    second = Instrument(
        symbol="ABC",
        exchange="XPAR",
        security_id="222222222",
        security_id_source="1",
    )

    instruments = list(
        Instrument.from_observations(
            [(BASE, first), (BASE + 1, second)],
            snapshot_every=0,
        )
    )

    assert [row.security_id for row in instruments] == ["111111111", "222222222"]
    assert len({row.xhash for row in instruments}) == 2
    assert [row.version for row in instruments] == [0, 0]


def test_a_weak_symbol_can_still_be_enriched_by_its_first_security_id() -> None:
    weak = Instrument(symbol="ABC", exchange="XPAR")
    strong = Instrument(
        symbol="ABC",
        exchange="XPAR",
        security_id="111111111",
        security_id_source="1",
    )

    bare, enriched = Instrument.from_observations(
        [(BASE, weak), (BASE + 1, strong)],
        snapshot_every=0,
    )

    assert enriched.security_id == "111111111"
    assert enriched.xhash == bare.xhash
    assert enriched.version == bare.version + 1


def test_a_reference_visible_later_does_not_enrich_an_earlier_book() -> None:
    bare = Instrument(security_id="US1234567890", security_id_source="4")
    rich = Instrument(
        symbol="BTC-USD",
        security_id="US1234567890",
        security_id_source="4",
    )
    events = [
        order(BASE, bare, Side.BID, 100.0, 1.0, "B1"),
        order(BASE + 10, rich, Side.ASK, 101.0, 1.0, "A1"),
    ]
    instruments = list(
        Instrument.from_observations(
            [(BASE, bare), (BASE + 10, rich)],
            snapshot_every=0,
        )
    )

    books = list(BookIterator.from_events(with_instruments(events, instruments), snapshot_every=0))

    assert [book.code for book in books] == ["", "BTC-USD"]


def test_iterating_the_iterator_is_iterating_its_books() -> None:
    events = [order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")]
    assert [type(one) for one in BookIterator.from_events(events)] == [Book]


def test_a_book_keeps_the_source_sequence_of_the_event_it_settled() -> None:
    event = order(BASE, BTC, Side.BID, 100.0, 5.0, "B1", seq=7)

    (book,) = BookIterator.from_events([event], snapshot_every=0)

    assert book.seq == 7


def test_the_source_is_read_once_and_not_started_over() -> None:
    """A generator source would be silently half-consumed by a second pass, and a
    list source would be folded forever."""
    events = iter([order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")])
    iterating = BookIterator.from_events(events)
    assert len(list(iterating.books)) == 1
    assert list(iterating.books) == [], "drained, and it says so"


# -- the state is per instrument ---------------------------------------------


def test_one_iterator_folds_every_instrument_on_its_own() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, ETH, Side.BID, 10.0, 3.0, "E1"),
        order(BASE + 20, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    iterating = BookIterator.from_events(events, snapshot_every=0)
    found = list(iterating.books)
    assert {one.code for one in found} == {"BTC-USD", "ETH-USD"}
    assert len(iterating.folding) == 2, "one mutable state per instrument, and no more"


def test_an_instrument_s_book_never_sees_another_s_liquidity() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, ETH, Side.BID, 999.0, 99.0, "E1"),
        order(BASE + 20, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    books = {one.code: one for one in BookIterator.from_events(events, snapshot_every=0)}
    assert books["BTC-USD"].bid_px == 100.0 and books["BTC-USD"].bid_qty == 5.0
    assert books["ETH-USD"].bid_px == 999.0


def test_a_stream_out_of_order_is_refused() -> None:
    """A fold asks the book to un-happen something, and there is no honest answer."""
    events = [
        order(BASE + 100, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE, ETH, Side.BID, 10.0, 3.0, "E1"),
    ]
    with pytest.raises(ValueError, match="time order"):
        list(BookIterator.from_events(events))


def test_the_order_is_checked_across_instruments_and_not_within_one() -> None:
    """The clock is the stream's, which is what makes an hourly boundary the same
    instant for every instrument in it."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE, ETH, Side.BID, 10.0, 3.0, "E1"),
        order(BASE + 1, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    assert len(list(BookIterator.from_events(events, snapshot_every=0))) == 3


# -- reference ownership -----------------------------------------------------


def test_an_instrument_is_published_when_it_is_learnt_and_not_per_message() -> None:
    """A feed repeats the instrument on every message; a row per message would be
    the feed again rather than the reference data in it."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.BID, 99.0, 5.0, "B2"),
        order(BASE + 20, BTC, Side.BID, 98.0, 5.0, "B3"),
    ]
    assert len(list(Instrument.from_events(events))) == 1


def test_a_message_that_knows_more_publishes_another_version() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC_RICH, Side.BID, 99.0, 5.0, "B2"),
    ]
    bare, rich = Instrument.from_events(events)
    assert bare.cfi is None and bare.kind is AssetKind.UNKNOWN
    assert rich.cfi == "FFICSX" and rich.kind is AssetKind.FUTURE
    assert rich.currency is Currency.USD
    assert rich.xhash == bare.xhash, "the same instrument, and an identity that did not move"
    assert rich.version == bare.version + 1 and rich.prev_hash == bare.hash
    assert rich.hash != bare.hash, "two versions of what is known, and two rows"


def test_learning_never_retracts_what_was_already_known() -> None:
    """Instrument data arrives in whatever order a venue felt like sending it, and a
    later message that omits a field has not withdrawn it."""
    events = [
        order(BASE, BTC_RICH, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.BID, 99.0, 5.0, "B2"),
    ]
    (only,) = Instrument.from_events(events)
    assert only.cfi == "FFICSX"


def test_equal_reference_repeats_skip_enrichment_but_a_late_field_does_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The equality shortcut must still notice the last reference-data member."""
    repeated = dataclasses.replace(BTC)
    richer = dataclasses.replace(BTC, label="Bitcoin future")
    enriched_with = Instrument.enriched_with
    examined: list[Instrument] = []

    def counted(self: Instrument, other: Instrument) -> Instrument | None:
        examined.append(other)
        return enriched_with(self, other)

    monkeypatch.setattr(Instrument, "enriched_with", counted)
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, repeated, Side.BID, 99.0, 4.0, "B2"),
        order(BASE + 20, richer, Side.BID, 98.0, 3.0, "B3"),
    ]

    found = list(Instrument.from_events(events))

    assert examined == [richer], "initial and equal states need no enrichment walk"
    assert [one.label for one in found] == [None, "Bitcoin future"]


# -- the hourly grid ----------------------------------------------------------


def test_a_gap_of_hours_is_filled_hour_by_hour() -> None:
    """A table whose hourly rows skip the hours nothing happened in is one you have
    to scan backwards to read, which is what hourly rows exist to avoid."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 3 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator.from_events(events))
    assert [(one.unix - BASE) // HOUR for one in found] == [0, 1, 2, 3, 3]
    assert [one.sunix is not None for one in found] == [False, True, True, True, False]


def test_a_snapshot_says_what_it_is_a_picture_of() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 2 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator.from_events(events))
    taken = [one for one in found if one.sunix is not None]
    assert [one.sunix for one in taken] == [BASE + 60, BASE + 60]
    assert [one.unix for one in taken] == [BASE + HOUR, BASE + 2 * HOUR]
    assert all(one.unix - one.sunix > 0 for one in taken), "staleness, without a join"


def test_instrument_state_is_published_on_every_book_boundary() -> None:
    events = [
        order(BASE + 60, BTC_RICH, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 3 * HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1"),
    ]
    books = list(BookIterator.from_events(events))
    boundaries = {one.unix for one in books if one.sunix is not None}
    instruments = list(Instrument.from_events(events))

    assert [one.unix for one in instruments] == [
        BASE + 60,
        BASE + HOUR,
        BASE + 2 * HOUR,
        BASE + 3 * HOUR,
    ]
    assert {one.unix for one in instruments if one.sunix is not None} == boundaries
    assert all(one.cfi == BTC_RICH.cfi for one in instruments)


def test_expiry_delta_does_not_skip_the_instrument_boundary() -> None:
    expiring = order(
        BASE + 60,
        BTC,
        Side.BID,
        100.0,
        5.0,
        "B1",
        eunix=BASE + HOUR - 10,
    )
    clock = order(BASE + 2 * HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1")
    events = [expiring, clock]
    records = list(BookIterator.from_events(events))
    instruments = list(Instrument.from_events(events))
    assert [one.unix for one in instruments] == [BASE + 60, BASE + HOUR, BASE + 2 * HOUR]
    expired = next(one for one in records if one.unix == BASE + HOUR)
    assert expired.bid_depth == 0


def test_recovery_continues_full_instrument_snapshots() -> None:
    events = [
        order(BASE + 60, BTC_RICH, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1"),
    ]
    instruments = list(Instrument.from_events(events))
    known = next(one for one in instruments if one.unix == BASE + HOUR)
    later = [order(BASE + 3 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2")]
    resumed = Instrument.from_events(
        later,
        instruments=[Instrument.from_dict(known.into_dict())],
    )

    recovered = list(resumed)
    assert [one.unix for one in recovered] == [BASE + 2 * HOUR, BASE + 3 * HOUR]
    assert all(one.cfi == BTC_RICH.cfi for one in recovered)


def test_every_instrument_gets_the_hour_and_not_only_the_one_that_traded() -> None:
    """The hour is a property of the clock: a book nothing happened to for three
    hours still stood there for three hours."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 70, ETH, Side.BID, 10.0, 3.0, "E1"),
        order(BASE + 3 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator.from_events(events))
    hours = {}
    for one in found:
        hours.setdefault(one.code, []).append((one.unix - BASE) // HOUR)
    assert hours["BTC-USD"] == [0, 1, 2, 3, 3]
    assert hours["ETH-USD"] == [0, 1, 2, 3]


def test_an_exact_boundary_snapshots_only_the_inactive_instrument() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + HOUR, ETH, Side.BID, 10.0, 1.0, "E1"),
    ]
    records = list(BookIterator.from_events(events))
    instruments = list(Instrument.from_events(events))

    btc_books = [one for one in records if one.code == "BTC-USD"]
    btc_refs = [one for one in instruments if one.code == "BTC-USD"]
    eth_books = [one for one in records if one.code == "ETH-USD"]
    assert [one.unix for one in btc_books] == [BASE + 60, BASE + HOUR]
    assert [one.unix for one in btc_refs] == [BASE + 60, BASE + HOUR]
    assert [one.unix for one in eth_books] == [BASE + HOUR]


def test_an_active_exact_boundary_is_one_final_delta() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + HOUR, BTC, Side.ASK, 101.0, 1.0, "A1"),
        order(BASE + HOUR, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    records = list(BookIterator.from_events(events))
    instruments = list(Instrument.from_events(events))

    boundary_books = [one for one in records if one.unix == BASE + HOUR]
    boundary_refs = [one for one in instruments if one.unix == BASE + HOUR]
    assert len(boundary_books) == 1
    (delta,) = boundary_books
    assert delta.sunix is None
    assert delta.bid_depth == 2 and delta.ask_depth == 1
    assert len(boundary_refs) == 1 and boundary_refs[0].sunix == BASE + 60


def test_an_exact_boundary_keeps_one_book_identity() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100, 5.0, "B1"),
        order(BASE + HOUR, BTC, Side.ASK, 101.0, 1.0, "A1"),
        order(BASE + HOUR, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    books = list(BookIterator.from_events(events).books)

    assert len({(book.unix, book.instrument_xhash) for book in books}) == len(books)
    assert [book.hash for book in books] == [
        Book.hash_of(book.unix, book.instrument_xhash) for book in books
    ]


def test_the_snapshots_are_versions_of_the_book_they_picture() -> None:
    """The book at 14:00 and the book at 15:00 are two rows of one book."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 2 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator.from_events(events))
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
    found = list(BookIterator.from_events(events, snapshot_every=0))
    assert len(found) == 2 and all(one.sunix is None for one in found)


def test_the_stream_ends_without_guessing_how_long_the_book_stood() -> None:
    """A snapshot fills the gap between two events; past the last one there is no
    gap, only a guess."""
    events = [order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1")]
    found = list(BookIterator.from_events(events))
    assert len(found) == 1 and found[0].sunix is None


# -- a side that did not move -------------------------------------------------


def test_a_side_that_did_not_move_carries_no_levels_delta() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
        order(BASE + 20, BTC, Side.ASK, 100.4, 2.0, "A2"),
    ]
    first, second, third = BookIterator.from_events(events, snapshot_every=0).books
    assert first.bid_levels and second.bid_levels is None and third.bid_levels is None
    assert second.ask_levels and third.ask_levels, "the ask changed on both later rows"


def test_a_side_that_did_not_move_carries_no_delta() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    _, second = BookIterator.from_events(events, snapshot_every=0).books
    assert second.bid_levels is None
    assert len(second.ask_levels) == 1


def test_a_side_that_did_not_move_still_reports_its_state() -> None:
    """Carried, not dropped: a book row says what both sides are, always."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    _, second = BookIterator.from_events(events, snapshot_every=0).books
    assert second.bid_px == 100.0 and second.bid_qty == 5.0 and second.bid_depth == 1
    assert second.bid_levels is None, "an unchanged side has no levels delta"
    assert second.spread == pytest.approx(0.5), "and the prices across the sides follow"


def test_only_dirty_sides_recompute_their_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    summarise = book_module._summarise_side

    def traced(book: Book, name: str, side: _Side) -> None:
        calls.append(name)
        summarise(book, name, side)

    monkeypatch.setattr(book_module, "_summarise_side", traced)
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
        order(BASE + 20, BTC, Side.ASK, 100.4, 2.0, "A2"),
        order(BASE + 30, BTC, Side.BID, 99.0, 3.0, "B2"),
    ]

    found = list(BookIterator.from_events(events, snapshot_every=0))

    assert calls == ["bid", "ask", "ask", "ask", "bid"]
    assert [
        (
            row.bid_px,
            row.bid_qty,
            row.bid_depth,
            row.bid_total_qty,
            row.ask_px,
            row.ask_qty,
            row.ask_depth,
            row.ask_total_qty,
        )
        for row in found
    ] == [
        (100.0, 5.0, 1, 5.0, None, None, 0, 0.0),
        (100.0, 5.0, 1, 5.0, 100.5, 7.0, 1, 7.0),
        (100.0, 5.0, 1, 5.0, 100.4, 2.0, 2, 9.0),
        (100.0, 5.0, 2, 8.0, 100.4, 2.0, 2, 9.0),
    ]
    assert found[1].px == pytest.approx(100.25)
    assert found[2].micro_px == pytest.approx((100.0 * 2.0 + 100.4 * 5.0) / 7.0)


def test_a_trade_counts_as_the_side_moving() -> None:
    """It takes liquidity out, and a row that said otherwise would be wrong."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
        initial(
            Execution(
                unix=BASE + 20,
                code="BTC-USD",
                side=Side.BID,
                px=100.0,
                qty=2.0,
                state=State.FILLED,
                exec_id="EX-1",
            )
        ),
    ]
    first, _, third = BookIterator.from_events(events, snapshot_every=0).books
    assert first.bid_qty == 5.0 and third.bid_qty == 3.0
    assert len(third.bid_levels) == 1
    assert third.bid_levels[0].exec_xhash == [third.execution_events[0].xhash]


def test_a_trade_amendment_is_not_folded_as_a_fresh_fill() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        initial(
            Execution(
                unix=BASE + 10,
                code="BTC-USD",
                side=Side.BID,
                px=100.0,
                qty=2.0,
                state=State.FILLED,
                exec_id="EX-1",
            )
        ),
        initial(
            Execution(
                unix=BASE + 20,
                code="BTC-USD",
                side=Side.BID,
                px=100.0,
                qty=2.0,
                state=State.CANCELLED,
                exec_id="EX-2",
                exec_ref_id="EX-1",
            )
        ),
    ]

    found = list(BookIterator.from_events(events, snapshot_every=0))

    assert len(found) == 3 and found[-1].bid_qty == 3.0
    assert found[-1].execution_events[0].state is State.CANCELLED


# -- a picture has no delta ---------------------------------------------------


def test_a_snapshot_shows_the_book_and_not_what_changed_to_produce_it() -> None:
    """Carrying the delta forward made a consumer summing those columns count one
    level insertion once per hourly row -- four times over a three-hour quiet
    patch -- for an insertion that happened once."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 3 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator.from_events(events))
    for one in found:
        if one.sunix is None:
            continue
        assert all(not level.exec_xhash for level in one.bid_levels or ())
        assert all(not level.exec_xhash for level in one.ask_levels or ())
        assert [level.px for level in one.bid_levels] == [100.0], "the state is still there"
        assert [order.order_id for order in one.order_events] == ["B1"]
        assert one.linked_xhash == [order.xhash for order in one.order_events]
        assert one.execution_events == []


def test_forgetting_the_delta_does_not_empty_the_row_it_pictures() -> None:
    """A snapshot is a `copy.copy`, so its lists are the subject's own until
    something replaces them: clearing in place would empty the book below it."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    first, snapshot, _ = BookIterator.from_events(events).books
    assert len(first.bid_levels) == 1, "the row that was pictured still has its delta"
    assert snapshot.bid_levels is not first.bid_levels
    assert snapshot.bid_levels[0].order_xhash == first.bid_levels[0].order_xhash


# -- recovery, validation and expiry ---------------------------------------


def test_only_snapshots_carry_the_full_state_needed_to_resume() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + HOUR + 60, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    changed, snapshot, latest = BookIterator.from_events(events).books

    assert changed.bid_levels and latest.bid_levels is None
    assert [one.order_id for one in snapshot.order_events] == ["B1"]
    assert snapshot.execution_events == []


def test_a_snapshot_restores_names_levels_and_live_quantities() -> None:
    before = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + HOUR + 60, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    seed = next(one for one in BookIterator.from_events(before) if one.sunix is not None)
    after = order(BASE + HOUR + 70, BTC, Side.BID, 99.0, 3.0, "B2")

    (resumed,) = BookIterator.from_events([after], snapshots=[seed], snapshot_every=0)

    assert resumed.bid_depth == 2 and resumed.bid_total_qty == 8.0
    assert resumed.bid_px == 100.0 and resumed.ask_px is None
    assert [one.order_id for one in resumed.order_events] == ["B2"]


def test_order_lookup_falls_back_to_a_live_client_id_without_an_order_id() -> None:
    placed = initial(
        Order(
            unix=BASE,
            code=BTC.symbol,
            side=Side.BID,
            px=100.0,
            qty=5.0,
            client_order_id="client-1",
            state=State.NEW,
        ),
        BTC,
    )
    side = _Side(side=Side.BID)
    assert side.apply(placed)
    # Exercise the linear fallback, not the normal indexed lookup.
    side.named.clear()

    found = side.standing(Order(client_order_id="client-1"))

    assert found is not None and found.xhash == placed.xhash


def test_a_restored_order_continues_the_persisted_version_chain() -> None:
    placed = order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1")
    clock = order(BASE + HOUR + 60, BTC, Side.ASK, 100.5, 7.0, "A1")
    seed = next(one for one in BookIterator.from_events([placed, clock]) if one.sunix is not None)
    amended = order(BASE + HOUR + 70, BTC, Side.BID, 100.0, 4.0, "B1")

    (resumed,) = BookIterator.from_events([amended], snapshots=[seed], snapshot_every=0)

    (audited,) = resumed.order_events
    seeded = next(one for one in seed.order_events if one.order_id == "B1")
    assert audited.prev_hash == seeded.hash == placed.hash
    assert audited.version == seeded.version + 1


@pytest.mark.parametrize("anonymous", [False, True], ids=["missing-order", "anonymous-level"])
def test_recovery_refuses_a_live_level_it_cannot_reconstruct(anonymous: bool) -> None:
    placed = order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1")
    clock = order(BASE + HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1")
    seed = next(one for one in BookIterator.from_events([placed, clock]) if one.sunix is not None)
    levels = seed.bid_levels
    if anonymous:
        levels = [dataclasses.replace(level, order_xhash=[]) for level in levels]
    broken = dataclasses.replace(seed, bid_levels=levels, order_events=[])

    with pytest.raises(ValueError, match="linked (live )?Order"):
        BookIterator(snapshots=[broken])


def test_recovery_refuses_a_delta_deletion_as_a_full_level() -> None:
    placed = order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1")
    clock = order(BASE + HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1")
    seed = next(one for one in BookIterator.from_events([placed, clock]) if one.sunix is not None)
    broken = dataclasses.replace(
        seed,
        bid_levels=[dataclasses.replace(seed.bid_levels[0], qty=0.0)],
    )

    with pytest.raises(ValueError, match="must have positive qty"):
        BookIterator(snapshots=[broken])


def test_recovery_rebuilds_the_explicit_expiry_index() -> None:
    placed = order(
        BASE + 60,
        BTC,
        Side.BID,
        100.0,
        5.0,
        "B1",
        eunix=BASE + HOUR + 10,
    )
    clock = order(BASE + HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1")
    snapshot = next(
        one for one in BookIterator.from_events([placed, clock]).books if one.sunix is not None
    )
    assert snapshot.bid_levels and snapshot.order_events and placed.eunix is not None

    restored = _Side.from_snapshot(
        Side.BID,
        snapshot.bid_levels,
        snapshot.order_events,
    )

    assert restored._expiry_keys == [placed.eunix]
    (expired,) = restored.expire(placed.eunix)
    assert expired.xhash == placed.xhash and restored.orders == {}
    assert restored._expiry_keys == [] and restored._expiring == {}


def test_recovery_applies_the_side_bound_as_an_auditable_delta() -> None:
    before = [
        order(BASE + 60, BTC, Side.BID, 100.0, 1.0, "B1"),
        order(BASE + 61, BTC, Side.BID, 99.0, 1.0, "B2"),
        order(BASE + 62, BTC, Side.BID, 98.0, 1.0, "B3"),
        order(BASE + HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1"),
    ]
    seed = next(one for one in BookIterator.from_events(before) if one.sunix is not None)

    (bounded,) = BookIterator(snapshots=[seed], snapshot_every=0, max_side_alive=2).books

    expired = [one for one in bounded.order_events if one.state is State.INTERNAL_EXPIRED]
    assert [one.order_id for one in expired] == ["B3"]
    assert bounded.bid_depth == 2 and bounded.bid_total_qty == 2.0


def test_a_reference_restores_aliases_the_normalized_book_does_not_store() -> None:
    identified = Instrument(
        symbol="BTC-USD",
        exchange="XCME",
        security_id="US1234567890",
        security_id_source="4",
    )
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 70, identified, Side.BID, 99.0, 2.0, "B2"),
        order(BASE + HOUR + 60, identified, Side.ASK, 100.5, 1.0, "A1"),
    ]
    instruments = list(Instrument.from_events(events))
    folding = BookIterator.from_events(with_instruments(events, instruments))
    books = list(folding.books)
    snapshot = next(one for one in books if one.sunix is not None)
    persisted = Book.from_dict(snapshot.into_dict())
    assert persisted.into_instrument() is None

    security_id_only = Instrument(security_id="US1234567890", security_id_source="4")
    after = order(BASE + HOUR + 70, security_id_only, Side.BID, 98.0, 1.0, "B3")
    latest = max(instruments, key=lambda known: known.unix)
    resumed = BookIterator.from_events(
        with_instruments([after], [latest]),
        snapshots=[persisted],
        snapshot_every=0,
    )

    (book,) = resumed.books
    assert len(resumed.folding) == 1
    assert book.instrument_xhash == BTC.xhash


def test_a_reference_alone_canonicalizes_the_book_and_nested_order() -> None:
    canonical = Instrument(symbol="BTC-USD", exchange="XCME")
    richer = Instrument(
        symbol="BTC-USD",
        exchange="XCME",
        security_id="US1234567890",
        security_id_source="4",
    )
    known = dataclasses.replace(canonical, unix=BASE - HOUR).with_previous(None)
    assert known is not None and richer.xhash != known.xhash
    folding = BookIterator.from_events(
        with_instruments(
            [order(BASE, richer, Side.BID, 100.0, 1.0, "B1")],
            [Instrument.from_dict(known.into_dict())],
        ),
        snapshot_every=0,
    )

    (book,) = folding
    instrument = known
    (nested,) = book.order_events
    assert instrument.xhash == book.instrument_xhash == nested.instrument_xhash
    assert book.instrument_xhash == canonical.xhash
    assert nested.xhash == Order.hash_of(canonical.xhash, nested.mic, "B1", nested.side)


def test_alias_canonicalization_rewrites_execution_links_and_parent_versions() -> None:
    canonical = Instrument(symbol="BTC-USD", exchange="XCME")
    richer = Instrument(
        symbol="BTC-USD",
        exchange="XCME",
        security_id="US1234567890",
        security_id_source="4",
    )
    known = dataclasses.replace(canonical, unix=BASE - HOUR).with_previous(None)
    assert known is not None
    placed = order(BASE, richer, Side.BID, 100.0, 1.0, "B1")
    fill = (
        Execution(
            unix=BASE + 1,
            state=State.FILLED,
            side=Side.BID,
            px=100.0,
            qty=1.0,
            exec_id="X1",
            linked_xhash=[placed.xhash],
            parent_hash=[placed.hash],
        )
        .attach_instrument(richer)
        .with_previous(placed)
    )
    assert fill is not None
    books = list(
        BookIterator.from_events(
            with_instruments([placed, fill], [known]),
            snapshot_every=0,
        ).books
    )

    nested_order = books[0].order_events[0]
    nested_fill = books[-1].execution_events[0]
    assert nested_fill.primary_linked_xhash == nested_order.xhash
    assert nested_order.hash in nested_fill.parent_hash


@pytest.mark.parametrize("explicit", [False, True], ids=["max-age", "eunix"])
def test_stale_orders_expire_into_an_auditable_terminal_event(explicit: bool) -> None:
    expiring = order(
        BASE,
        BTC,
        Side.BID,
        100.0,
        5.0,
        "B1",
        eunix=BASE + 10 if explicit else None,
    )
    clock = order(BASE + 20, BTC, Side.ASK, 101.0, 1.0, "A1")
    iterator = BookIterator.from_events(
        [expiring, clock],
        snapshot_every=0,
        max_order_age_ns=None if explicit else 10,
    )

    latest = list(iterator)[-1]
    expired = [one for one in latest.order_events if one.order_id == "B1"]
    assert len(expired) == 1 and expired[0].state is State.INTERNAL_EXPIRED
    assert expired[0].eunix == BASE + 10
    assert expired[0].error and latest.bid_depth == 0


def test_an_inactive_instrument_snapshots_before_its_expiry_is_applied() -> None:
    btc = order(
        BASE + 60,
        BTC,
        Side.BID,
        100.0,
        5.0,
        "B1",
        eunix=BASE + HOUR + 10,
    )
    eth_clock = order(BASE + 2 * HOUR, ETH, Side.BID, 10.0, 1.0, "E1")

    btc_books = [one for one in BookIterator.from_events([btc, eth_clock]) if one.code == "BTC-USD"]

    snapshot = next(one for one in btc_books if one.sunix is not None)
    expired = next(
        one
        for one in btc_books
        if any(event.state is State.INTERNAL_EXPIRED for event in one.order_events)
    )
    recovered = btc_books[-1]
    assert snapshot.unix == BASE + HOUR and snapshot.bid_qty == 5.0
    assert expired.unix == eth_clock.unix and expired.bid_depth == 0
    assert expired.order_events[-1].state is State.INTERNAL_EXPIRED
    assert recovered is expired and recovered.sunix is None
    assert len({(book.unix, book.instrument_xhash) for book in btc_books}) == len(btc_books)


@pytest.mark.parametrize(
    ("explicit_offset", "max_age"),
    [
        (-10, None),
        (0, None),
        (None, HOUR - 70),
        (None, HOUR - 60),
    ],
    ids=["explicit-before", "explicit-at", "max-age-before", "max-age-at"],
)
def test_expiry_is_applied_before_a_crossed_snapshot_boundary(
    explicit_offset: int | None, max_age: int | None
) -> None:
    """A boundary never republishes interest whose known lifetime has ended."""
    if explicit_offset is not None:
        deadline = BASE + HOUR + explicit_offset
    else:
        assert max_age is not None
        deadline = BASE + 60 + max_age
    expiring = order(
        BASE + 60,
        BTC,
        Side.BID,
        100.0,
        5.0,
        "B1",
        eunix=deadline if explicit_offset is not None else None,
    )
    clock = order(BASE + 3 * HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1")

    found = list(
        BookIterator.from_events(
            [expiring, clock],
            max_order_age_ns=max_age,
        )
    )

    boundary = [one for one in found if one.unix == BASE + HOUR]
    assert boundary and all(one.bid_depth == 0 for one in boundary)
    assert len(boundary) == 1 and boundary[0].sunix is None
    expired = [
        event
        for book in found
        for event in book.order_events
        if event.order_id == "B1" and event.state is State.INTERNAL_EXPIRED
    ]
    assert len(expired) == 1
    assert expired[0].eunix == deadline
    assert [one.unix for one in found] == sorted(one.unix for one in found)
    assert not any(one.unix == BASE + 2 * HOUR for one in found), (
        "a closed book stops producing unchanged snapshots"
    )


def test_an_inactive_instrument_expires_before_its_crossed_snapshot() -> None:
    btc = order(
        BASE + 60,
        BTC,
        Side.BID,
        100.0,
        5.0,
        "B1",
        eunix=BASE + HOUR - 10,
    )
    eth_clock = order(BASE + 3 * HOUR + 60, ETH, Side.BID, 10.0, 1.0, "E1")

    btc_books = [one for one in BookIterator.from_events([btc, eth_clock]) if one.code == "BTC-USD"]

    assert [one.unix for one in btc_books] == sorted(one.unix for one in btc_books)
    boundary = next(one for one in btc_books if one.unix == BASE + HOUR)
    assert boundary.bid_depth == 0
    expired = [
        event
        for book in btc_books
        for event in book.order_events
        if event.order_id == "B1" and event.state is State.INTERNAL_EXPIRED
    ]
    assert len(expired) == 1
    assert all(one.bid_depth == 0 for one in btc_books if one.unix >= BASE + HOUR)


def test_an_incomplete_limit_order_is_rejected_but_not_lost() -> None:
    incomplete = order(
        BASE,
        BTC,
        Side.BID,
        None,
        5.0,
        "B1",
        kind=MarketKind.LIMIT_ORDER,
    )

    (book,) = BookIterator.from_events([incomplete], snapshot_every=0)

    (audited,) = book.order_events
    assert audited.state is State.INTERNAL_REJECTED and "required price" in audited.error
    assert book.bid_depth == 0 and book.bid_px is None


def test_a_new_order_without_a_side_is_rejected_instead_of_silently_ignored() -> None:
    incomplete = order(BASE, BTC, Side.UNKNOWN, 100.0, 5.0, "B1")

    (book,) = BookIterator.from_events([incomplete], snapshot_every=0)

    (audited,) = book.order_events
    assert audited.state is State.INTERNAL_REJECTED and "side is missing" in audited.error
    assert book.bid_depth == book.ask_depth == 0


def test_pending_new_is_validated_but_pending_cancel_may_omit_terms() -> None:
    incomplete = order(
        BASE,
        BTC,
        Side.BID,
        None,
        None,
        "NEW",
        kind=MarketKind.LIMIT_ORDER,
        state=State.PENDING_NEW,
    )
    standing = order(BASE + 10, BTC, Side.BID, 100.0, 5.0, "B1")
    cancel = order(
        BASE + 20,
        BTC,
        Side.BID,
        None,
        None,
        "B1",
        state=State.PENDING_CANCEL,
    )

    books = list(BookIterator.from_events([incomplete, standing, cancel], snapshot_every=0))

    assert books[0].order_events[0].state is State.INTERNAL_REJECTED
    assert books[-1].bid_qty == 5.0
    assert books[-1].order_events[0].state is State.PENDING_CANCEL
    assert books[-1].order_events[0].error is None


def test_a_rejected_replace_never_removes_the_standing_order() -> None:
    standing = order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")
    malformed = order(
        BASE + 10,
        BTC,
        Side.BID,
        float("nan"),
        5.0,
        "B1",
        kind=MarketKind.LIMIT_ORDER,
        error="upstream detail",
    )

    latest = list(BookIterator.from_events([standing, malformed], snapshot_every=0))[-1]

    assert latest.bid_px == 100.0 and latest.bid_qty == 5.0
    assert latest.order_events[0].state is State.INTERNAL_REJECTED
    assert latest.order_events[0].error == "upstream detail"


def test_negative_prices_are_valid_but_nonpositive_quantities_are_not() -> None:
    priced = order(
        BASE,
        BTC,
        Side.BID,
        -37.0,
        5.0,
        "B1",
        kind=MarketKind.LIMIT_ORDER,
    )
    bad_fill = initial(
        Execution(
            unix=BASE + 10,
            code="BTC-USD",
            side=Side.BID,
            px=-37.0,
            qty=-1.0,
            state=State.FILLED,
            exec_id="E1",
        )
    )

    latest = list(BookIterator.from_events([priced, bad_fill], snapshot_every=0))[-1]

    assert latest.bid_qty == 5.0
    assert latest.execution_events[0].state is State.INTERNAL_REJECTED
    assert "quantity" in latest.execution_events[0].error


def test_a_fill_with_authoritative_leaves_is_not_subtracted_twice() -> None:
    placed = order(BASE, BTC, Side.BID, 100.0, 1_200.0, "B1")
    remaining = order(
        BASE + 10,
        BTC,
        Side.BID,
        100.0,
        800.0,
        "B1",
        state=State.PARTIALLY_FILLED,
    )
    fill = initial(
        Execution(
            unix=BASE + 10,
            code="BTC-USD",
            side=Side.BID,
            px=100.0,
            qty=400.0,
            leaves_qty=800.0,
            filled_qty=400.0,
            state=State.FILLED,
            exec_id="E1",
            linked_xhash=[placed.xhash],
        )
    )

    latest = list(BookIterator.from_events([placed, remaining, fill], snapshot_every=0))[-1]

    assert latest.bid_qty == 800.0 and latest.bid_total_qty == 800.0
    assert latest.order_events[0].px == 100.0 and latest.order_events[0].qty == 800.0
    assert latest.order_events[0].prev_qty == 1_200.0
    assert latest.order_events[0].version == 1 and latest.order_events[0].prev_hash == placed.hash
    assert latest.execution_events[0].qty == 400.0
