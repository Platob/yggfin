"""`Book.from_events`: one instrument's stream folded into the book it describes."""

from __future__ import annotations

import pytest

import rekep.market.book as book_module
from rekep.market import (
    MIC,
    Book,
    BookIterator,
    Execution,
    Instrument,
    MarketEvent,
    Order,
    Side,
    State,
    TimeInForce,
)
from rekep.market.book import _Side

BTC = Instrument(symbol="BTC-USD", exchange="XCME", currency="USD")
ETH = Instrument(symbol="ETH-USD", exchange="XCME", currency="USD")


def initial[EventT: MarketEvent](event: EventT, instrument: Instrument = BTC) -> EventT:
    """Attach transient reference data and require an initial version."""
    built = event.attach_instrument(instrument).with_previous(None)
    assert built is not None
    return built


def order(unix: int, side: Side, px: float, qty: float, named: str, **given: object) -> Order:
    declared = {
        "unix": unix,
        "side": side,
        "px": px,
        "qty": qty,
        "order_id": named,
        "state": State.NEW,
        "px_unit": "USD",
    }
    return initial(Order(**{**declared, **given}))


def trade(unix: int, px: float, qty: float, **given: object) -> Execution:
    declared = {
        "unix": unix,
        "px": px,
        "qty": qty,
        "state": State.FILLED,
        "exec_id": f"EX-{unix}",
    }
    return initial(Execution(**{**declared, **given}))


TWO_SIDED = [
    order(10, Side.BID, 100.0, 5.0, "B1"),
    order(10, Side.BID, 99.5, 3.0, "B2"),
    order(10, Side.ASK, 100.5, 7.0, "A1"),
]


def books(events: list[object]) -> list[Book]:
    return list(Book.from_events(events))


# -- one row per instant, not one per message --------------------------------


def test_events_at_one_instant_are_one_book() -> None:
    """Three rows with the same `unix` is writing the feed, not the book."""
    (only,) = books(TWO_SIDED)
    assert only.unix == 10 and only.bid_depth == 2 and only.ask_depth == 1


def test_each_instant_that_moved_the_book_yields_one_row() -> None:
    found = books([*TWO_SIDED, order(20, Side.BID, 100.0, 9.0, "B3")])
    assert [one.unix for one in found] == [10, 20]


def test_an_instant_that_moved_nothing_still_keeps_its_audit_delta() -> None:
    """An acknowledgement changes no liquidity but remains auditable."""
    acked = initial(Execution(unix=20, code="BTC-USD", px=100.0, qty=1.0, state=State.ACCEPTED))
    first, audited = books([*TWO_SIDED, acked])
    assert [first.unix, audited.unix] == [10, 20]
    assert audited.bid_qty == first.bid_qty and audited.ask_qty == first.ask_qty
    assert audited.executions == [acked]


def test_an_empty_stream_is_an_empty_iterator() -> None:
    assert books([]) == []


def test_the_books_are_separate_rows_and_not_one_object_repeated() -> None:
    """A caller collecting them into a batch would otherwise get the last one, twice."""
    first, second = books([*TWO_SIDED, order(20, Side.BID, 100.0, 9.0, "B3")])
    assert first is not second
    assert (first.bid_depth, second.bid_depth) == (2, 2)
    assert (first.bid_qty, second.bid_qty) == (5.0, 14.0)


def test_a_book_is_versioned_like_any_other_event() -> None:
    first, second = books([*TWO_SIDED, order(20, Side.BID, 100.0, 9.0, "B3")])
    assert second.version == first.version + 1
    assert second.prev_unix == first.unix and second.xhash == first.xhash
    assert first.prev_unix is None
    assert (second.prev_bid_px, second.prev_bid_qty) == (first.bid_px, first.bid_qty)
    assert (second.prev_ask_px, second.prev_ask_qty) == (first.ask_px, first.ask_qty)


def test_a_book_hash_frames_both_ordered_live_sides_explicitly() -> None:
    (only,) = books(TWO_SIDED)
    bid, lower_bid, ask = TWO_SIDED
    expected = (
        only.unix,
        only.instrument_xhash,
        2,
        bid.hash,
        lower_bid.hash,
        1,
        ask.hash,
    )

    assert only.version_parts() == expected
    assert only.hash == Book.hash_of(*expected)


def test_book_hash_cache_follows_each_side_change() -> None:
    bid = order(10, Side.BID, 100.0, 5.0, "B1")
    ask = order(20, Side.ASK, 101.0, 7.0, "A1")
    better_bid = order(30, Side.BID, 100.5, 3.0, "B2")
    cancel = order(40, Side.BID, 100.0, 5.0, "B1", state=State.CANCELLED)

    found = list(BookIterator.from_events([bid, ask, better_bid, cancel], snapshot_every=0))
    expected_sides = [
        ([bid.hash], []),
        ([bid.hash], [ask.hash]),
        ([better_bid.hash, bid.hash], [ask.hash]),
        ([better_bid.hash], [ask.hash]),
    ]
    for book, (bids, asks) in zip(found, expected_sides, strict=True):
        expected = (
            book.unix,
            book.instrument_xhash,
            len(bids),
            *bids,
            len(asks),
            *asks,
        )
        assert book.version_parts() == expected
        assert book.hash == Book.hash_of(*expected)


# -- what it refuses ---------------------------------------------------------


def test_a_stream_carrying_two_instruments_folds_each_on_its_own() -> None:
    """One iterator over every instrument, with the state that has to be per
    instrument kept per instrument -- which is what lets a partition holding one
    and a capture holding ten thousand be the same call."""
    other = initial(
        Order(
            unix=20,
            code="ETH-USD",
            side=Side.BID,
            px=1.0,
            qty=1.0,
            state=State.NEW,
        ),
        ETH,
    )
    found = books([*TWO_SIDED, other])
    assert [one.code for one in found] == ["BTC-USD", "ETH-USD"]
    assert found[0].bid_depth == 2 and found[1].bid_depth == 1
    assert found[0].instrument_xhash != found[1].instrument_xhash


def test_one_instrument_s_book_never_sees_another_s_orders() -> None:
    """The bug the old one-instrument-per-fold guard existed to prevent."""
    other = initial(
        Order(
            unix=20,
            code="ETH-USD",
            side=Side.ASK,
            px=1.0,
            qty=99.0,
            state=State.NEW,
        ),
        ETH,
    )
    btc, eth = books([*TWO_SIDED, other])
    assert btc.ask_qty == 7.0, "not 99, which is the other instrument's"
    assert eth.bid_px is None and eth.ask_qty == 99.0


def test_a_stream_out_of_order_is_refused() -> None:
    """A fold asks the book to un-happen something, and there is no honest answer."""
    with pytest.raises(ValueError, match="time order"):
        books([*TWO_SIDED, order(5, Side.BID, 100.0, 1.0, "B9")])


def test_a_market_order_rests_nowhere_and_moves_no_book() -> None:
    """It is an execution against a side, not a level on it."""
    unpriced = Order(
        unix=20, code="BTC-USD", side=Side.BID, qty=5.0, state=State.NEW
    ).attach_instrument(BTC)
    first, audited = books([*TWO_SIDED, unpriced])
    assert audited.bid_qty == first.bid_qty and audited.ask_qty == first.ask_qty
    assert len(audited.deltas) == 1
    assert audited.deltas[0].qty == 5.0 and audited.deltas[0].px is None


# -- what it carries ---------------------------------------------------------


def test_a_book_knows_what_it_is_a_book_of() -> None:
    (only,) = books(TWO_SIDED)
    assert only.code == "BTC-USD" and only.instrument_xhash == BTC.xhash
    assert only.px_unit == "USD" and "instrument" not in Book.into_field().names


def test_the_units_come_from_the_events_folded_and_not_the_one_after_them() -> None:
    """Reading the event that *triggered* the yield gave every row the units of the
    instant after it."""
    bare = initial(Execution(unix=20, code="BTC-USD", px=100.0, qty=1.0, state=State.FILLED))
    first, _ = books([*TWO_SIDED, bare])
    assert first.px_unit == "USD"


def test_a_book_links_to_the_events_that_built_its_delta() -> None:
    (only,) = books(TWO_SIDED)
    assert only.parent_hash == [event.hash for event in only.deltas]
    assert only.linked_events == [(event.unix, event.xhash) for event in only.deltas]


# -- the levels, and the orders under them -----------------------------------


def test_orders_at_one_price_aggregate_into_one_level() -> None:
    folding = BookIterator.from_events(
        [*TWO_SIDED, order(10, Side.BID, 100.0, 9.0, "B3")], snapshot_every=0
    )
    list(folding)
    side = next(iter(folding.folding.values())).bid
    top = side.into_levels()[0]
    assert (top.px, top.qty) == (100.0, 14.0)
    assert len(side.alive[0].members) == 2


def test_the_bid_is_sorted_down_and_the_ask_up() -> None:
    events = [
        *TWO_SIDED,
        order(10, Side.BID, 101.0, 1.0, "B4"),
        order(10, Side.ASK, 100.2, 1.0, "A2"),
    ]
    folding = BookIterator.from_events(events, snapshot_every=0)
    list(folding)
    state = next(iter(folding.folding.values()))
    assert [level.px for level in state.bid.into_levels()] == [101.0, 100.0, 99.5]
    assert [level.px for level in state.ask.into_levels()] == [100.2, 100.5]


def test_orders_at_one_price_are_kept_largest_first() -> None:
    """Price is what a book is; size is the second key, and any stable one beats
    an arbitrary one."""
    side = _Side(side=Side.BID)
    for named, qty in (("small", 1.0), ("big", 9.0), ("middling", 4.0)):
        side.apply(order(10, Side.BID, 100.0, qty, named))
    assert [one.order_id for one in side.sorted_orders] == ["big", "middling", "small"]
    assert side.best.order_id == "big"


def test_a_side_with_nothing_on_it_has_no_best() -> None:
    assert _Side(side=Side.BID).best is None


def test_a_restated_order_replaces_what_it_was_resting_for() -> None:
    """A level cannot say which order contributed what, which is why the fold keeps
    the orders and not the levels."""
    first, second = books([*TWO_SIDED, order(20, Side.BID, 100.0, 2.0, "B1", state=State.OPEN)])
    assert first.bid_qty == 5.0
    assert second.bid_qty == 2.0, "replaced, not added to"


def test_a_full_duplicate_skips_copy_and_completion(monkeypatch: pytest.MonkeyPatch) -> None:
    side = _Side(side=Side.BID, max_order_age_ns=None)
    assert side.apply(order(10, Side.BID, 100.0, 5.0, "B1"))
    duplicate = order(20, Side.BID, 100.0, 5.0, "B1")

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("a complete duplicate must be skipped before copying or completion")

    monkeypatch.setattr(book_module.copy, "copy", forbidden)
    monkeypatch.setattr(Order, "completed_from", forbidden)
    monkeypatch.setattr(Order, "_completed_from_same_lifecycle", forbidden)

    assert side._applied(duplicate) == (False, None)


def test_a_partial_restatement_still_copies_and_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    side = _Side(side=Side.BID, max_order_age_ns=None)
    assert side.apply(order(10, Side.BID, 100.0, 5.0, "B1"))
    partial = Order(
        unix=20,
        code="BTC-USD",
        side=Side.BID,
        qty=4.0,
        order_id="B1",
        state=State.NEW,
    ).attach_instrument(BTC)
    copied = book_module.copy.copy
    completed = Order.completed_from
    calls: list[str] = []

    def counted_copy(value: object) -> object:
        calls.append("copy")
        return copied(value)

    def counted_completion(current: Order, previous: MarketEvent | None) -> Order:
        calls.append("complete")
        return completed(current, previous)

    monkeypatch.setattr(book_module.copy, "copy", counted_copy)
    monkeypatch.setattr(Order, "completed_from", counted_completion)

    moved, settled = side._applied(partial)

    assert moved and settled is not None
    assert (settled.px, settled.qty) == (100.0, 4.0)
    assert calls == ["copy", "complete"]


def test_a_metadata_only_same_price_update_has_no_level_delta() -> None:
    first, updated = books(
        [
            order(10, Side.BID, 100.0, 5.0, "B1"),
            order(20, Side.BID, 100.0, 5.0, "B1", state=State.ACCEPTED),
        ]
    )

    assert updated.deltas[0].state is State.ACCEPTED
    assert updated.bid_levels == []
    assert (updated.bid_px, updated.bid_qty, updated.bid_depth) == (
        first.bid_px,
        first.bid_qty,
        first.bid_depth,
    )


def test_a_same_price_quantity_update_keeps_exact_side_summaries() -> None:
    folding = BookIterator.from_events(
        [
            order(10, Side.BID, 100.0, 5.0, "B1"),
            order(10, Side.BID, 100.0, 2.0, "B2"),
            order(10, Side.BID, 99.0, 3.0, "B3"),
            order(20, Side.BID, 100.0, 4.0, "B1"),
        ],
        snapshot_every=0,
        max_order_age_ns=None,
    )

    _, updated = list(folding)
    state = next(iter(folding.folding.values()))
    assert (updated.bid_px, updated.bid_qty, updated.bid_depth) == (100.0, 6.0, 2)
    assert [(level.px, level.qty) for level in updated.bid_levels] == [(100.0, 6.0)]
    assert state.bid.levels[100.0].qty == 6.0
    assert sum(level.qty for level in state.bid.alive) == 9.0


def test_expiry_scan_starts_only_when_the_earliest_deadline_is_due(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered: list[tuple[Side, int]] = []
    expire = _Side.expire

    def counted_expire(side: _Side, unix: int) -> list[Order]:
        entered.append((side.side, unix))
        return expire(side, unix)

    monkeypatch.setattr(_Side, "expire", counted_expire)
    events = [
        order(10, Side.BID, 100.0, 5.0, "B1", eunix=100),
        order(50, Side.ASK, 101.0, 1.0, "A1"),
        order(100, Side.ASK, 102.0, 1.0, "A2"),
    ]

    found = list(
        BookIterator.from_events(
            events,
            snapshot_every=0,
            max_order_age_ns=None,
        )
    )

    assert entered == [(Side.BID, 100)]
    assert any(event.state is State.INTERNAL_EXPIRED for book in found for event in book.deltas)


def test_an_order_completed_from_the_one_it_replaces_keeps_current_quantity() -> None:
    partial = Order(
        unix=20,
        code="BTC-USD",
        side=Side.BID,
        order_id="B1",
        qty=1.0,
        state=State.PARTIALLY_FILLED,
    ).attach_instrument(BTC)
    _, second = books([*TWO_SIDED, partial])
    assert second.bid_px == 100.0, "the price it has had all along"
    assert second.bid_qty == 1.0, "and one left of the five"


def test_a_terminal_order_leaves_the_book_entirely() -> None:
    gone = Order(
        unix=20,
        code="BTC-USD",
        side=Side.BID,
        order_id="B1",
        state=State.CANCELLED,
    ).attach_instrument(BTC)
    _, second = books([*TWO_SIDED, gone])
    assert second.bid_depth == 1 and second.bid_px == 99.5


def test_a_level_of_zero_is_not_a_level() -> None:
    side = _Side(side=Side.BID)
    side.apply(order(10, Side.BID, 100.0, 5.0, "B1"))
    side.apply(order(20, Side.BID, 100.0, 5.0, "B1", state=State.FILLED))
    assert side.orders == {} and side.into_levels() == []


# -- the prices that only exist across the sides -----------------------------


def test_the_touch_and_the_spread_are_computed_across_both_sides() -> None:
    (only,) = books(TWO_SIDED)
    assert (only.bid_px, only.ask_px) == (100.0, 100.5)
    assert only.px == 100.25 and only.spread == pytest.approx(0.5)
    assert only.qty == 12.0, "the size at the touch, both sides"


def test_the_flat_pair_of_mid_and_spread_is_the_best_bid_and_offer_exactly() -> None:
    """Which is why neither is stored twice, and why there is no `crossed` flag."""
    (only,) = books(TWO_SIDED)
    assert only.px - only.spread / 2 == only.bid_px
    assert only.px + only.spread / 2 == only.ask_px


def test_the_vwap_leans_towards_the_side_with_less_size() -> None:
    (only,) = books(TWO_SIDED)
    assert only.vwap == pytest.approx((100.0 * 7.0 + 100.5 * 5.0) / 12.0)
    assert only.vwap < only.px, "more offered than bid, so the fair price is lower"


def test_the_imbalance_is_signed_towards_the_heavier_side() -> None:
    (only,) = books(TWO_SIDED)
    assert only.imbalance == pytest.approx((5.0 - 7.0) / 12.0)


def test_a_one_sided_book_has_no_prices_across_it() -> None:
    (only,) = books([order(10, Side.BID, 100.0, 5.0, "B1")])
    assert only.bid_px == 100.0
    assert only.px is None and only.spread is None and only.vwap is None


def test_an_empty_book_says_so_in_its_state() -> None:
    events = [
        order(10, Side.BID, 100.0, 5.0, "B1"),
        order(20, Side.BID, 100.0, 0.0, "B1", state=State.CANCELLED),
    ]
    _, second = books(events)
    assert second.state is State.CLOSED and second.bid_depth == 0


# -- trades ------------------------------------------------------------------


def test_a_fill_takes_liquidity_out_of_the_side_it_names() -> None:
    """An execution's `side` is the side of the order it reports, and a filled buy
    order was resting on the bid."""
    _, second = books([*TWO_SIDED, trade(20, 100.0, 2.0, side=Side.BID)])
    assert second.bid_qty == 3.0 and second.ask_qty == 7.0


def test_the_latest_filled_execution_price_carries_across_later_books() -> None:
    first, filled, carried, newer = books(
        [
            *TWO_SIDED,
            trade(20, 100.0, 2.0, side=Side.BID),
            order(30, Side.ASK, 101.0, 1.0, "A2"),
            trade(40, 100.5, 1.0, side=Side.ASK),
        ]
    )

    assert (first.exec_px, first.prev_exec_px) == (None, None)
    assert (filled.exec_px, filled.prev_exec_px) == (100.0, None)
    assert (carried.exec_px, carried.prev_exec_px) == (100.0, 100.0)
    assert (newer.exec_px, newer.prev_exec_px) == (100.5, 100.0)


def test_a_fill_that_names_an_order_takes_it_out_of_that_order_s_side() -> None:
    resting_order = TWO_SIDED[0]
    _, second = books(
        [
            *TWO_SIDED,
            trade(20, 100.0, 2.0, linked_events=[(resting_order.unix, resting_order.xhash)]),
        ]
    )
    assert second.bid_qty == 3.0


def test_a_fill_tries_linked_lifecycles_in_order() -> None:
    placed = TWO_SIDED[0]
    _, second = books(
        [
            *TWO_SIDED,
            trade(20, 100.5, 2.0, linked_events=[(0, -1), (placed.unix, placed.xhash)]),
        ]
    )
    assert second.bid_qty == 3.0 and second.ask_qty == 7.0


@pytest.mark.parametrize(
    ("name", "value"),
    (("order_id", "B1"), ("client_order_id", "C1"), ("prev_client_order_id", "C0")),
)
def test_a_fill_falls_back_to_each_source_order_identifier(name: str, value: str) -> None:
    placed = order(
        10,
        Side.BID,
        100.0,
        5.0,
        "B1",
        client_order_id="C1",
        prev_client_order_id="C0",
    )
    offered = order(10, Side.ASK, 100.5, 7.0, "A1")
    _, second = books([placed, offered, trade(20, 100.5, 2.0, **{name: value})])
    assert second.bid_qty == 3.0 and second.ask_qty == 7.0


def test_a_print_with_neither_is_read_against_the_touch() -> None:
    """The tick rule: at or below the mid it took from the bid, above it from the ask."""
    _, low = books([*TWO_SIDED, trade(20, 100.0, 2.0)])
    assert low.bid_qty == 3.0 and low.ask_qty == 7.0
    _, high = books([*TWO_SIDED, trade(20, 100.5, 2.0)])
    assert high.ask_qty == 5.0 and high.bid_qty == 5.0


def test_an_acknowledgement_takes_nothing_out() -> None:
    """Subtracting its quantity is how a book ends up empty by lunchtime."""
    acked = initial(Execution(unix=20, code="BTC-USD", px=100.0, qty=99.0, state=State.ACCEPTED))
    first, audited = books([*TWO_SIDED, acked])
    assert audited.bid_qty == first.bid_qty and audited.ask_qty == first.ask_qty
    assert audited.executions == [acked]


def test_a_trade_is_recorded_on_the_side_it_hit() -> None:
    _, second = books([*TWO_SIDED, trade(20, 100.0, 2.0)])
    (changed,) = second.bid_levels
    assert (changed.px, changed.qty) == (100.0, 3.0)
    assert [(one.px, one.qty) for one in second.executions] == [(100.0, 2.0)]
    assert second.ask_levels == []


def test_a_later_fill_does_not_mutate_the_prior_flattened_order() -> None:
    placed = order(10, Side.BID, 100.0, 5.0, "B1")
    fill = trade(20, 100.0, 2.0, linked_events=[(placed.unix, placed.xhash)])

    first, second = books([placed, fill])

    assert first.deltas[0].qty == 5.0
    assert second.bid_qty == 3.0


def test_a_trade_bigger_than_the_level_walks_the_orders_in_the_order_they_sit() -> None:
    """Which is the order a venue would have filled them in."""
    events = [*TWO_SIDED, trade(20, 100.0, 6.0)]
    _, second = books(events)
    assert second.bid_depth == 1 and second.bid_px == 99.5 and second.bid_qty == 2.0
    assert [(level.px, level.qty) for level in second.bid_levels] == [
        (100.0, 0.0),
        (99.5, 2.0),
    ]
    assert [(one.px, one.qty) for one in second.executions] == [(100.0, 6.0)]


def test_a_print_against_a_book_this_fold_never_saw_takes_nothing() -> None:
    (audited,) = books([trade(10, 100.0, 5.0)])
    assert audited.bid_depth == audited.ask_depth == 0
    assert len(audited.executions) == 1


# -- the delta is per version ------------------------------------------------


def test_the_updates_on_a_row_are_what_produced_that_row() -> None:
    first, second = books([*TWO_SIDED, order(20, Side.BID, 100.0, 9.0, "B3")])
    assert len(first.bid_levels) == 2, "two bid levels changed at the first instant"
    assert len(second.bid_levels) == 1, "and one at the second, not three"


def test_a_side_that_did_not_move_carries_no_delta_on_the_next_row() -> None:
    _, second = books([*TWO_SIDED, order(20, Side.BID, 100.0, 9.0, "B3")])
    assert second.ask_levels == [] and second.ask_depth == 1, "unchanged, and still there"


def test_a_book_whose_side_emptied_has_no_mid_rather_than_the_last_one() -> None:
    """`px` and `qty` are abstract slots that a version carries forward, which is
    right for an order's limit and wrong for a mid: inheriting it makes a one-sided
    market look two-sided for as long as it lasts."""
    gone = Order(
        unix=20,
        code="BTC-USD",
        side=Side.ASK,
        order_id="A1",
        state=State.CANCELLED,
    ).attach_instrument(BTC)
    first, second = books([*TWO_SIDED, gone])
    assert first.px == 100.25
    assert second.bid_px == 100.0 and second.ask_px is None
    assert second.px is None and second.qty is None
    assert second.spread is None and second.vwap is None and second.imbalance is None


# -- the whole way, from a venue's own lines ---------------------------------

REFRESHES = [
    "8=FIX.4.4|35=X|49=XCME|52=20260821-10:30:00.100|55=BTC-USD|207=XCME|15=USD|268=2|"
    "279=0|269=0|270=100.0|271=5|278=L1|272=20260821|273=10:30:00.010|"
    "279=0|269=1|270=100.5|271=7|278=L2|273=10:30:00.010",
    "8=FIX.4.4|35=X|49=XCME|52=20260821-10:30:00.200|55=BTC-USD|207=XCME|15=USD|268=1|"
    "279=1|269=0|270=100.0|271=9|278=L1|272=20260821|273=10:30:00.020",
    "8=FIX.4.4|35=X|49=XCME|52=20260821-10:30:00.300|55=BTC-USD|207=XCME|15=USD|268=1|"
    "279=0|269=2|270=100.5|271=3|272=20260821|273=10:30:00.030",
]


def test_a_venue_s_own_lines_fold_into_the_book_they_describe() -> None:
    """The whole path: log lines, market events, book rows."""
    from rekep.market import FixEvents
    from rekep.market.fix import unix_of

    events = [one for line in REFRESHES for one in FixEvents.from_text(line, venue="XCME")]
    found = books(events)
    assert [one.unix for one in found] == [
        unix_of("20260821-10:30:00.010"),
        unix_of("20260821-10:30:00.020"),
        unix_of("20260821-10:30:00.030"),
    ], "each entry's own time, never the message's"
    assert [(one.bid_px, one.bid_qty) for one in found] == [
        (100.0, 5.0),
        (100.0, 9.0),
        (100.0, 9.0),
    ]
    assert [one.ask_qty for one in found] == [7.0, 7.0, 4.0], "the trade took three off the ask"
    assert [(level.px, level.qty) for level in found[-1].ask_levels] == [(100.5, 4.0)]
    assert [(one.px, one.qty) for one in found[-1].executions] == [(100.5, 3.0)]


def test_the_folded_books_are_a_table() -> None:
    """A book is a row, and a run of them casts as the batch it will be written as."""
    import pyarrow

    from rekep.market import FixEvents

    events = [one for line in REFRESHES for one in FixEvents.from_text(line, venue="XCME")]
    batch = Book.into_field().cast_arrow_batch(
        pyarrow.RecordBatch.from_pylist(
            [one.into_dict() for one in books(events)], schema=Book.into_field().into_arrow_schema()
        )
    )
    assert batch.num_rows == 3
    assert batch.schema.equals(Book.into_field().into_arrow_schema())
    assert batch.column("bid_px").to_pylist() == [100.0, 100.0, 100.0]
    assert len(set(batch.column("hash").to_pylist())) == 3, "three versions, three identities"

    orders = Order.from_books_arrow_batch(batch)
    executions = Execution.from_books_arrow_batch(batch)
    assert orders.num_rows and executions.num_rows
    assert orders.schema.equals(Order.into_field().into_arrow_schema())
    assert executions.schema.equals(Execution.into_field().into_arrow_schema())
    carrying = set(batch.column("hash").to_pylist())
    assert all(
        any(parent in carrying for parent in parents)
        for parents in orders["parent_hash"].to_pylist()
    )


# -- the running totals are the walk, and that is the whole optimisation ------


def walked(side: _Side) -> list[tuple[float, float, int]]:
    """The levels, aggregated the slow way: over every live order, every time."""
    found: dict[float, list[float]] = {}
    for one in side.sorted_orders:
        px = one.px or 0.0
        totals = found.setdefault(px, [0.0, 0.0])
        totals[0] += max((one.qty or 0.0) - (one.hidden_qty or 0.0), 0.0)
        totals[1] += 1
    facing = -side.sign
    return [
        (px, totals[0], int(totals[1]))
        for px, totals in sorted(found.items(), key=lambda item: item[0] * facing)
    ]


def test_the_running_levels_are_what_walking_the_orders_would_give() -> None:
    """`_Side` keeps `levels` up to date as orders move rather than re-aggregating
    per snapshot, which is where the fold's throughput comes from -- and which is
    only sound if the two agree at every step, not just at the end."""
    import random

    generate = random.Random(11)
    for turn in range(400):
        side = _Side(side=Side.BID if turn % 2 else Side.ASK)
        for step in range(20):
            named = f"O{generate.randrange(6)}"
            side.apply(
                order(
                    10 + step,
                    side.side,
                    100.0 + generate.randrange(-3, 4),
                    float(generate.randrange(0, 8)),
                    named,
                    state=State.CANCELLED if generate.random() < 0.25 else State.NEW,
                )
            )
            if generate.random() < 0.3 and side.orders:
                side.take(trade(10 + step, 100.0, float(generate.randrange(1, 5))), 3.0)
            assert [(level.px, level.qty, len(level.members)) for level in side.alive] == walked(
                side
            ), f"turn {turn}, step {step}"


def test_the_two_parallel_lists_never_drift_apart() -> None:
    """`keys` and `alive` are the same levels in the same order, and a snapshot
    walks the second on the strength of the first. Only two places move a level;
    this is what says both of them move both lists."""
    import random

    generate = random.Random(17)
    for turn in range(200):
        side = _Side(side=Side.BID if turn % 2 else Side.ASK)
        for step in range(25):
            side.apply(
                order(
                    10 + step,
                    side.side,
                    100.0 + generate.randrange(-4, 5),
                    float(generate.randrange(0, 9)),
                    f"O{generate.randrange(7)}",
                    state=State.CANCELLED if generate.random() < 0.25 else State.NEW,
                )
            )
            if generate.random() < 0.3 and side.orders:
                side.take(trade(10 + step, 100.0, float(generate.randrange(1, 6))), 4.0)
            assert len(side.keys) == len(side.alive) == len(side.levels), f"{turn}/{step}"
            assert [one.px * side.facing for one in side.alive] == side.keys
            assert side.keys == sorted(side.keys), "and sorted, which is the whole point"
            assert [one.px for one in side.alive] == [one.px for one in side.into_levels()]


def test_a_level_that_reaches_zero_is_dropped_rather_than_kept_at_zero() -> None:
    """Leaving it would put an empty price in every `alive` list from then on."""
    side = _Side(side=Side.BID)
    side.apply(order(10, Side.BID, 100.0, 5.0, "B1"))
    side.apply(order(20, Side.BID, 100.0, 5.0, "B1", state=State.CANCELLED))
    assert side.levels == {} and side.into_levels() == []


def test_an_order_that_moved_price_leaves_the_level_it_was_on() -> None:
    side = _Side(side=Side.BID)
    side.apply(order(10, Side.BID, 100.0, 5.0, "B1"))
    side.apply(order(20, Side.BID, 99.0, 5.0, "B1", state=State.OPEN))
    assert [(level.px, level.qty) for level in side.into_levels()] == [(99.0, 5.0)]


def test_removing_an_order_forgets_the_name_that_pointed_at_it() -> None:
    """Or a later order reusing the identifier would find a version that is gone."""
    side = _Side(side=Side.BID)
    side.apply(order(10, Side.BID, 100.0, 5.0, "B1"))
    side.apply(order(20, Side.BID, 100.0, 5.0, "B1", state=State.CANCELLED))
    assert side.named == {}
    side.apply(order(30, Side.BID, 98.0, 2.0, "B1"))
    assert [(level.px, level.qty) for level in side.into_levels()] == [(98.0, 2.0)]


def test_order_xhash_precedes_conflicting_identifier_codes() -> None:
    """The lifecycle hash is the hot path and the authoritative lookup key."""

    class UnreadableCodes(dict[str, str]):
        def items(self):
            raise AssertionError("the exact xhash hit inspected codes")

    side = _Side(side=Side.BID)
    first = order(10, Side.BID, 100.0, 5.0, "A")
    second = order(11, Side.BID, 99.0, 4.0, "B")
    side.apply(first)
    side.apply(second)

    probe = Order(xhash=first.xhash, codes=UnreadableCodes(order_id="B"))

    assert side.standing(probe) is side.orders[first.xhash]


def test_an_execution_xhash_is_not_mistaken_for_its_order_lifecycle() -> None:
    side = _Side(side=Side.BID)
    first = order(10, Side.BID, 100.0, 5.0, "A")
    second = order(11, Side.BID, 99.0, 4.0, "B")
    side.apply(first)
    side.apply(second)

    probe = Execution(
        xhash=first.xhash,
        order_id="B",
        codes={"order_id": "B"},
    )

    assert side.standing(probe) is side.orders[second.xhash]


def test_a_linked_order_lifecycle_precedes_conflicting_identifier_codes() -> None:
    side = _Side(side=Side.BID)
    first = order(10, Side.BID, 100.0, 5.0, "A")
    second = order(11, Side.BID, 99.0, 4.0, "B")
    side.apply(first)
    side.apply(second)

    probe = Execution(
        linked_events=[(first.unix, first.xhash)],
        order_id="B",
        codes={"order_id": "B"},
    )

    assert side.standing(probe) is side.orders[first.xhash]


def test_a_missing_order_xhash_falls_back_to_typed_codes_without_a_scan() -> None:
    class NoScan(dict[int, Order]):
        def __iter__(self):
            raise AssertionError("identifier lookup iterated live orders")

        def values(self):
            raise AssertionError("identifier lookup scanned live orders")

    side = _Side(side=Side.BID)
    placed = order(10, Side.BID, 100.0, 5.0, "A")
    side.apply(placed)
    side.orders = NoScan(side.orders)

    found = side.standing(Order(xhash=-1, codes={"order_id": "A"}))

    assert found is not None and found.xhash == placed.xhash


@pytest.mark.parametrize(
    "name",
    (
        "order_id",
        "secondary_order_id",
        "quote_entry_id",
        "quote_id",
        "md_entry_id",
        "md_entry_ref_id",
        "orig_cl_ord_id",
        "cl_ord_id",
        "secondary_cl_ord_id",
        "quote_req_id",
    ),
)
def test_each_typed_order_code_can_anchor_a_lifecycle(name: str) -> None:
    side = _Side(side=Side.BID)
    first = initial(
        Order(
            unix=10,
            side=Side.BID,
            px=100.0,
            qty=5.0,
            codes={name: "ONLY-ID"},
            state=State.NEW,
        )
    )
    revised = initial(
        Order(
            unix=20,
            side=Side.BID,
            px=100.0,
            qty=4.0,
            codes={name: "ONLY-ID"},
            state=State.OPEN,
        )
    )
    other = initial(
        Order(
            unix=20,
            side=Side.BID,
            px=99.0,
            qty=1.0,
            codes={name: "OTHER-ID"},
            state=State.NEW,
        )
    )

    assert first.xhash and revised.xhash == first.xhash
    assert other.xhash != first.xhash
    assert first.code == revised.code == "ONLY-ID"

    side.apply(first)
    side.apply(revised)

    (standing,) = side.orders.values()
    assert standing.xhash == first.xhash and standing.version == 1


def test_order_and_client_identifier_namespaces_do_not_cross() -> None:
    side = _Side(side=Side.BID)
    client = initial(
        Order(
            unix=10,
            code="client-root",
            side=Side.BID,
            px=100.0,
            qty=5.0,
            client_order_id="42",
            codes={"cl_ord_id": "42"},
            state=State.NEW,
        )
    )
    venue = initial(
        Order(
            unix=11,
            code="venue-root",
            side=Side.BID,
            px=99.0,
            qty=4.0,
            order_id="42",
            codes={"order_id": "42"},
            state=State.NEW,
        )
    )
    side.apply(client)
    side.apply(venue)

    assert side.standing(Order(codes={"cl_ord_id": "42"})).xhash == client.xhash
    assert side.standing(Order(codes={"order_id": "42"})).xhash == venue.xhash


def test_all_venue_identifiers_precede_every_client_identifier() -> None:
    side = _Side(side=Side.BID)
    venue = order(
        10,
        Side.BID,
        100.0,
        5.0,
        "VENUE-ROOT",
        codes={"secondary_order_id": "VENUE"},
    )
    client = initial(
        Order(
            unix=11,
            side=Side.BID,
            px=99.0,
            qty=4.0,
            client_order_id="CLIENT",
            codes={"cl_ord_id": "CLIENT"},
            state=State.NEW,
        )
    )
    side.apply(venue)
    side.apply(client)

    found = side.standing(
        Order(
            prev_client_order_id="CLIENT",
            codes={"secondary_order_id": "VENUE"},
        )
    )

    assert found is not None and found.xhash == venue.xhash


def test_identifier_fallback_respects_known_mic_scope() -> None:
    side = _Side(side=Side.BID)
    xnas = order(
        10,
        Side.BID,
        100.0,
        1.0,
        "SAME",
        mic=MIC.from_str("XNAS"),
    )
    bats = order(
        11,
        Side.BID,
        99.0,
        2.0,
        "SAME",
        mic=MIC.from_str("BATS"),
    )
    side.apply(xnas)
    assert side.standing(Order(codes={"order_id": "SAME"})).xhash == xnas.xhash

    side.apply(bats)

    assert set(side.orders) == {xnas.xhash, bats.xhash}
    assert (
        side.standing(Order(mic=MIC.from_str("XNAS"), codes={"order_id": "SAME"})).xhash
        == xnas.xhash
    )
    assert (
        side.standing(Order(mic=MIC.from_str("BATS"), codes={"order_id": "SAME"})).xhash
        == bats.xhash
    )
    assert side.standing(Order(codes={"order_id": "SAME"})) is None


def test_mutating_order_codes_keep_one_lifecycle_and_a_bounded_alias_cache() -> None:
    side = _Side(side=Side.BID)
    versions = [
        initial(
            Order(
                unix=10,
                side=Side.BID,
                px=100.0,
                qty=5.0,
                client_order_id="CL-1",
                codes={"cl_ord_id": "CL-1"},
                state=State.NEW,
            )
        ),
        initial(
            Order(
                unix=20,
                side=Side.BID,
                px=100.0,
                qty=4.0,
                client_order_id="CL-2",
                prev_client_order_id="CL-1",
                codes={"orig_cl_ord_id": "CL-1", "cl_ord_id": "CL-2"},
                state=State.OPEN,
            )
        ),
        initial(
            Order(
                unix=30,
                side=Side.BID,
                px=100.0,
                qty=3.0,
                client_order_id="CL-3",
                prev_client_order_id="CL-2",
                codes={"orig_cl_ord_id": "CL-2", "cl_ord_id": "CL-3"},
                state=State.OPEN,
            )
        ),
        initial(
            Order(
                unix=40,
                side=Side.BID,
                px=100.0,
                qty=2.0,
                order_id="ORD-1",
                client_order_id="CL-3",
                prev_client_order_id="CL-2",
                codes={
                    "order_id": "ORD-1",
                    "orig_cl_ord_id": "CL-2",
                    "cl_ord_id": "CL-3",
                },
                state=State.OPEN,
            )
        ),
    ]
    lifecycle = versions[0].xhash
    for event in versions:
        side.apply(event)

    (standing,) = side.orders.values()
    assert standing.xhash == lifecycle and standing.code == "CL-1" and standing.version == 3
    for name, code in (
        ("cl_ord_id", "CL-1"),
        ("orig_cl_ord_id", "CL-2"),
        ("cl_ord_id", "CL-3"),
        ("order_id", "ORD-1"),
    ):
        assert side.standing(Order(codes={name: code})).xhash == lifecycle
    assert len(side.aliases[lifecycle]) == 4

    side.apply(
        initial(
            Order(
                unix=50,
                side=Side.BID,
                order_id="ORD-1",
                client_order_id="CL-3",
                codes={"order_id": "ORD-1", "cl_ord_id": "CL-3"},
                state=State.CANCELLED,
            )
        )
    )

    assert side.orders == {} and side.named == {} and side.aliases == {}


def test_code_only_identifier_revisions_remain_current_and_indexed() -> None:
    side = _Side(side=Side.BID)
    root = "CLIENT-ROOT"
    lifecycle = 0
    for version in range(100):
        event = initial(
            Order(
                unix=10 + version,
                side=Side.BID,
                px=100.0,
                qty=5.0,
                client_order_id=root,
                codes={"secondary_cl_ord_id": f"S{version}"},
                state=State.NEW if version == 0 else State.OPEN,
            )
        )
        lifecycle = lifecycle or event.xhash
        side.apply(event)

    (standing,) = side.orders.values()
    assert standing.xhash == lifecycle and standing.code == root
    assert standing.codes["secondary_cl_ord_id"] == "S99"
    assert side.standing(Order(codes={"secondary_cl_ord_id": "S99"})).xhash == lifecycle
    assert side.standing(Order(codes={"secondary_cl_ord_id": "S98"})).xhash == lifecycle
    assert side.standing(Order(client_order_id=root)).xhash == lifecycle
    assert len(side.aliases[lifecycle]) <= book_module._ORDER_ALIAS_LIMIT


def test_expiry_without_a_max_age_reads_only_the_explicit_index() -> None:
    class NoValues(dict[int, Order]):
        def values(self):
            raise AssertionError("expire scanned every live order")

    side = _Side(side=Side.BID)
    standing = order(10, Side.BID, 100.0, 5.0, "standing")
    expiring = order(10, Side.BID, 99.0, 2.0, "expiring", eunix=20)
    side.apply(standing)
    side.apply(expiring)
    assert set(side._deadline_tokens) == {expiring.xhash}
    side.orders = NoValues(side.orders)

    (expired,) = side.expire(20)

    assert expired.order_id == "expiring" and expired.state is State.INTERNAL_EXPIRED
    assert list(side.orders) == [standing.xhash]


def test_replacement_and_removal_keep_a_bounded_expiry_index() -> None:
    side = _Side(side=Side.BID)
    side.apply(order(10, Side.BID, 100.0, 5.0, "B1", eunix=20))
    identity = next(iter(side.orders))

    for expiry in range(21, 2_021):
        side.apply(order(expiry - 9, Side.BID, 100.0, 5.0, "B1", eunix=expiry, state=State.OPEN))
        token = side._deadline_tokens[identity]
        assert (expiry, identity, token) in side._deadlines
    assert (
        len(side._deadlines) <= len(side._deadline_tokens) * 2 + book_module._DEADLINE_STALE_BUFFER
    )

    side.remove(identity)
    assert side._deadline_tokens == side._deadline_values == {} and side._deadlines == []


@pytest.mark.parametrize(
    ("side", "prices"),
    [
        (Side.BID, [100.0, 101.0, 101.0, 101.0]),
        (Side.ASK, [100.0, 99.0, 99.0, 99.0]),
    ],
    ids=["bid", "ask"],
)
def test_a_side_bound_keeps_best_price_then_earliest_time(side: Side, prices: list[float]) -> None:
    resting = _Side(side=side)
    given = [
        order(10 + index, side, px, 1.0, f"O{index}", eunix=1_000)
        for index, px in enumerate(prices)
    ]
    for event in given:
        resting.apply(event)

    expired = resting.bound(2, 20)

    assert [one.order_id for one in resting.sorted_orders] == ["O1", "O2"]
    assert [one.order_id for one in expired] == ["O0", "O3"]
    assert all(one.state is State.INTERNAL_EXPIRED and one.version == 1 for one in expired)
    assert all(one.reason and "max_side_alive=2" in one.reason for one in expired)
    assert all(one.state is State.INTERNAL_EXPIRED for one in expired)
    assert set(resting._deadline_tokens) == set(resting.orders)
    assert all(
        (1_000, xhash, resting._deadline_tokens[xhash]) in resting._deadlines
        for xhash in resting.orders
    )


def test_a_side_bound_breaks_exact_price_time_ties_independent_of_arrival() -> None:
    given = [order(10, Side.BID, 100.0, 1.0, f"O{index}") for index in range(4)]
    expected = {one.xhash for one in sorted(given, key=lambda one: one.xhash)[:2]}
    survivors = []

    for source in (given, list(reversed(given))):
        resting = _Side(side=Side.BID)
        for event in source:
            resting.apply(event)
        expired = resting.bound(2, 20)
        assert len(expired) == 2
        survivors.append(set(resting.orders))

    assert len(expected) == 2 and survivors == [expected, expected]


def test_a_tight_large_side_bound_never_scans_every_order() -> None:
    """The level index makes one worst-price eviction independent of side width."""

    class IndexedOrders(dict[int, Order]):
        def values(self):
            raise AssertionError("bound scanned every live order")

    count = 4_096
    resting = _Side(side=Side.BID)
    for index in range(count):
        resting.apply(order(index + 1, Side.BID, count - index, 1.0, f"O{index}"))
    resting.orders = IndexedOrders(resting.orders)

    (expired,) = resting.bound(count - 1, count + 1)

    assert expired.order_id == f"O{count - 1}"
    assert len(resting.orders) == count - 1


def test_bounded_evictions_are_auditable_and_never_enter_the_book_again() -> None:
    events = [
        order(10, Side.BID, 100.0, 1.0, "B1"),
        order(11, Side.BID, 99.0, 1.0, "B2"),
        order(12, Side.BID, 101.0, 1.0, "B3"),
    ]
    iterator = BookIterator.from_events(events, snapshot_every=0, max_side_alive=2)

    found = list(iterator)
    expired = [
        event for book in found for event in book.deltas if event.state is State.INTERNAL_EXPIRED
    ]

    assert [one.order_id for one in expired] == ["B2"]
    assert found[-1].bid_depth == 2
    assert sum(level.qty for level in iterator.folding[BTC.xhash].bid.alive) == 2.0
    assert {one.order_id for one in iterator.folding[BTC.xhash].bid.orders.values()} == {
        "B1",
        "B3",
    }


def test_a_new_order_evicted_immediately_keeps_both_versions_in_order() -> None:
    events = [
        order(10, Side.BID, 100.0, 1.0, "B1"),
        order(11, Side.BID, 99.0, 1.0, "B2"),
    ]

    latest = list(BookIterator.from_events(events, snapshot_every=0, max_side_alive=1))[-1]

    placed, expired = latest.deltas
    assert placed.order_id == expired.order_id == "B2"
    assert expired.state is State.INTERNAL_EXPIRED and expired.prev_unix == placed.unix
    assert expired.version == placed.version + 1
    assert latest.bid_px == 100.0 and latest.bid_depth == 1


def test_a_zero_side_bound_keeps_only_the_audit() -> None:
    iterator = BookIterator.from_events(
        [order(10, Side.ASK, 101.0, 1.0, "A1")],
        snapshot_every=0,
        max_side_alive=0,
    )

    (book,) = iterator

    assert [one.state for one in book.deltas] == [State.NEW, State.INTERNAL_EXPIRED]
    assert book.ask_depth == 0 and iterator.folding[BTC.xhash].ask.orders == {}


@pytest.mark.parametrize("tif", [TimeInForce.IOC, TimeInForce.FOK])
def test_immediate_orders_are_audited_but_never_rest(tif: TimeInForce) -> None:
    immediate = order(10, Side.BID, 100.0, 5.0, "I1", tif=tif)

    (book,) = BookIterator.from_events([immediate], snapshot_every=0)

    assert immediate.eunix == immediate.unix
    assert book.bid_depth == 0 and book.bid_px is None and book.bid_qty is None
    assert book.deltas == [immediate]


def test_negative_side_bound_is_refused() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        BookIterator(max_side_alive=-1)


def test_a_report_that_omits_the_mic_still_finds_the_order_it_continues() -> None:
    """Source identifiers still match when a later report omits the market code."""
    side = _Side(side=Side.BID)
    side.apply(order(10, Side.BID, 100.0, 5.0, "B1", mic=MIC.from_str("XCME")))
    bare = Order(
        unix=20,
        code="BTC-USD",
        side=Side.BID,
        order_id="B1",
        qty=2.0,
        state=State.OPEN,
    ).attach_instrument(BTC)
    assert side.standing(bare) is not None
    side.apply(bare)
    assert [(level.px, level.qty) for level in side.into_levels()] == [(100.0, 2.0)]
    assert len(side.orders) == 1, "one order, replaced, and not two"
