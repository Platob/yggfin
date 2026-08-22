"""`Book.from_events`: one instrument's stream folded into the book it describes."""

from __future__ import annotations

import pytest

from rekep.market import (
    Book,
    ExecKind,
    Execution,
    Instrument,
    Order,
    Resting,
    Side,
    State,
)

BTC = Instrument(symbol="BTC-USD", exchange="XCME", currency="USD")
ETH = Instrument(symbol="ETH-USD", exchange="XCME", currency="USD")


def order(unix: int, side: Side, px: float, qty: float, named: str, **given: object) -> Order:
    declared = {
        "unix": unix,
        "symbol": "BTC-USD",
        "instrument": BTC,
        "side": side,
        "px": px,
        "qty": qty,
        "order_id": named,
        "state": State.NEW,
        "px_unit": "USD",
    }
    return Order(**{**declared, **given}).with_previous(None)


def trade(unix: int, px: float, qty: float, **given: object) -> Execution:
    declared = {
        "unix": unix,
        "symbol": "BTC-USD",
        "instrument": BTC,
        "px": px,
        "qty": qty,
        "kind": ExecKind.TRADED,
        "exec_id": f"EX-{unix}",
    }
    return Execution(**{**declared, **given}).with_previous(None)


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


def test_an_instant_that_moved_nothing_yields_nothing() -> None:
    """An acknowledgement is not a book, and neither is a fill of nothing."""
    acked = Execution(
        unix=20, instrument=BTC, symbol="BTC-USD", px=100.0, qty=1.0, kind=ExecKind.ACK
    ).with_previous(None)
    assert [one.unix for one in books([*TWO_SIDED, acked])] == [10]


def test_an_empty_stream_is_an_empty_iterator() -> None:
    assert books([]) == []


def test_the_books_are_separate_rows_and_not_one_object_repeated() -> None:
    """A caller collecting them into a batch would otherwise get the last one, twice."""
    first, second = books([*TWO_SIDED, order(20, Side.BID, 100.0, 9.0, "B3")])
    assert first is not second
    assert first.bid_total_qty == 8.0 and second.bid_total_qty == 17.0


def test_a_book_is_versioned_like_any_other_event() -> None:
    first, second = books([*TWO_SIDED, order(20, Side.BID, 100.0, 9.0, "B3")])
    assert second.version == first.version + 1
    assert second.prev_hash == first.hash and second.xhash == first.xhash
    assert first.prev_hash is None


# -- what it refuses ---------------------------------------------------------


def test_a_stream_carrying_two_instruments_folds_each_on_its_own() -> None:
    """One iterator over every instrument, with the state that has to be per
    instrument kept per instrument -- which is what lets a partition holding one
    and a capture holding ten thousand be the same call."""
    other = Order(
        unix=20,
        instrument=ETH,
        symbol="ETH-USD",
        side=Side.BID,
        px=1.0,
        qty=1.0,
        state=State.NEW,
    ).with_previous(None)
    found = books([*TWO_SIDED, other])
    assert [one.symbol for one in found] == ["BTC-USD", "ETH-USD"]
    assert found[0].bid_depth == 2 and found[1].bid_depth == 1
    assert found[0].instrument_hash != found[1].instrument_hash


def test_one_instrument_s_book_never_sees_another_s_orders() -> None:
    """The bug the old one-instrument-per-fold guard existed to prevent."""
    other = Order(
        unix=20,
        instrument=ETH,
        symbol="ETH-USD",
        side=Side.ASK,
        px=1.0,
        qty=99.0,
        state=State.NEW,
    ).with_previous(None)
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
        unix=20, instrument=BTC, symbol="BTC-USD", side=Side.BID, qty=5.0, state=State.NEW
    )
    assert [one.unix for one in books([*TWO_SIDED, unpriced])] == [10]


# -- what it carries ---------------------------------------------------------


def test_a_book_knows_what_it_is_a_book_of() -> None:
    (only,) = books(TWO_SIDED)
    assert only.symbol == "BTC-USD" and only.instrument.xhash == BTC.xhash
    assert only.instrument_hash == BTC.xhash and only.px_unit == "USD"


def test_the_units_come_from_the_events_folded_and_not_the_one_after_them() -> None:
    """Reading the event that *triggered* the yield gave every row the units of the
    instant after it."""
    bare = Execution(
        unix=20, instrument=BTC, symbol="BTC-USD", px=100.0, qty=1.0, kind=ExecKind.TRADED
    ).with_previous(None)
    first, _ = books([*TWO_SIDED, bare])
    assert first.px_unit == "USD"


def test_each_side_is_named_as_the_parent_it_came_from() -> None:
    (only,) = books(TWO_SIDED)
    assert only.parent_hash == [only.bid_hash, only.ask_hash]
    assert only.bid_hash != only.ask_hash


# -- the levels, and the orders under them -----------------------------------


def test_orders_at_one_price_aggregate_into_one_level_that_counts_them() -> None:
    """The one number an aggregated feed cannot give you."""
    (only,) = books([*TWO_SIDED, order(10, Side.BID, 100.0, 9.0, "B3")])
    top = only.bid_alive[0]
    assert (top.px, top.qty, top.orders) == (100.0, 14.0, 2)


def test_the_bid_is_sorted_down_and_the_ask_up() -> None:
    events = [
        *TWO_SIDED,
        order(10, Side.BID, 101.0, 1.0, "B4"),
        order(10, Side.ASK, 100.2, 1.0, "A2"),
    ]
    (only,) = books(events)
    assert [level.px for level in only.bid_alive] == [101.0, 100.0, 99.5]
    assert [level.px for level in only.ask_alive] == [100.2, 100.5]


def test_orders_at_one_price_are_kept_largest_first() -> None:
    """Price is what a book is; size is the second key, and any stable one beats
    an arbitrary one."""
    side = Resting(side=Side.BID)
    for named, qty in (("small", 1.0), ("big", 9.0), ("middling", 4.0)):
        side.apply(order(10, Side.BID, 100.0, qty, named))
    assert [one.order_id for one in side.sorted_orders] == ["big", "middling", "small"]
    assert side.best.order_id == "big"


def test_a_side_with_nothing_on_it_has_no_best() -> None:
    assert Resting(side=Side.BID).best is None


def test_a_restated_order_replaces_what_it_was_resting_for() -> None:
    """A level cannot say which order contributed what, which is why the fold keeps
    the orders and not the levels."""
    first, second = books([*TWO_SIDED, order(20, Side.BID, 100.0, 2.0, "B1", state=State.OPEN)])
    assert first.bid_qty == 5.0
    assert second.bid_qty == 2.0, "replaced, not added to"


def test_an_order_completed_from_the_one_it_replaces_needs_only_what_changed() -> None:
    """A report that says "partially filled, 4 done" arrives with no price."""
    partial = Order(
        unix=20,
        instrument=BTC,
        symbol="BTC-USD",
        side=Side.BID,
        order_id="B1",
        filled_qty=4.0,
        state=State.PARTIALLY_FILLED,
    )
    _, second = books([*TWO_SIDED, partial])
    assert second.bid_px == 100.0, "the price it has had all along"
    assert second.bid_qty == 1.0, "and one left of the five"


def test_a_terminal_order_leaves_the_book_entirely() -> None:
    gone = Order(
        unix=20,
        instrument=BTC,
        symbol="BTC-USD",
        side=Side.BID,
        order_id="B1",
        state=State.CANCELLED,
    )
    _, second = books([*TWO_SIDED, gone])
    assert second.bid_depth == 1 and second.bid_px == 99.5


def test_a_level_of_zero_is_not_a_level() -> None:
    side = Resting(side=Side.BID)
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


def test_the_microprice_leans_towards_the_side_with_less_size() -> None:
    (only,) = books(TWO_SIDED)
    assert only.micro_px == pytest.approx((100.0 * 7.0 + 100.5 * 5.0) / 12.0)
    assert only.micro_px < only.px, "more offered than bid, so the fair price is lower"


def test_the_imbalance_is_signed_towards_the_heavier_side() -> None:
    (only,) = books(TWO_SIDED)
    assert only.imbalance == pytest.approx((5.0 - 7.0) / 12.0)


def test_a_one_sided_book_has_no_prices_across_it() -> None:
    (only,) = books([order(10, Side.BID, 100.0, 5.0, "B1")])
    assert only.bid_px == 100.0
    assert only.px is None and only.spread is None and only.micro_px is None


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


def test_a_fill_that_names_an_order_takes_it_out_of_that_order_s_side() -> None:
    resting_order = TWO_SIDED[0]
    _, second = books([*TWO_SIDED, trade(20, 100.0, 2.0, order_xhash=resting_order.xhash)])
    assert second.bid_qty == 3.0


def test_a_print_with_neither_is_read_against_the_touch() -> None:
    """The tick rule: at or below the mid it took from the bid, above it from the ask."""
    _, low = books([*TWO_SIDED, trade(20, 100.0, 2.0)])
    assert low.bid_qty == 3.0 and low.ask_qty == 7.0
    _, high = books([*TWO_SIDED, trade(20, 100.5, 2.0)])
    assert high.ask_qty == 5.0 and high.bid_qty == 5.0


def test_an_acknowledgement_takes_nothing_out() -> None:
    """Subtracting its quantity is how a book ends up empty by lunchtime."""
    acked = Execution(
        unix=20, instrument=BTC, symbol="BTC-USD", px=100.0, qty=99.0, kind=ExecKind.ACK
    ).with_previous(None)
    assert len(books([*TWO_SIDED, acked])) == 1


def test_a_trade_is_recorded_on_the_side_it_hit() -> None:
    _, second = books([*TWO_SIDED, trade(20, 100.0, 2.0)])
    (printed,) = second.bid_executions
    assert (printed.px, printed.qty, printed.unix) == (100.0, 2.0, 20)
    assert not second.ask_executions


def test_a_trade_bigger_than_the_level_walks_the_orders_in_the_order_they_sit() -> None:
    """Which is the order a venue would have filled them in."""
    events = [*TWO_SIDED, trade(20, 100.0, 6.0)]
    _, second = books(events)
    assert second.bid_depth == 1 and second.bid_px == 99.5 and second.bid_qty == 2.0


def test_a_print_against_a_book_this_fold_never_saw_takes_nothing() -> None:
    assert books([trade(10, 100.0, 5.0)]) == []


# -- the delta is per version ------------------------------------------------


def test_the_updates_on_a_row_are_what_produced_that_row() -> None:
    first, second = books([*TWO_SIDED, order(20, Side.BID, 100.0, 9.0, "B3")])
    assert len(first.bid_updates) == 2, "two bids arrived at the first instant"
    assert len(second.bid_updates) == 1, "and one at the second, not three"


def test_a_side_that_did_not_move_carries_no_delta_on_the_next_row() -> None:
    _, second = books([*TWO_SIDED, order(20, Side.BID, 100.0, 9.0, "B3")])
    assert not second.ask_updates and second.ask_depth == 1, "unchanged, and still there"


def test_a_book_whose_side_emptied_has_no_mid_rather_than_the_last_one() -> None:
    """`px` and `qty` are abstract slots that a version carries forward, which is
    right for an order's limit and wrong for a mid: inheriting it makes a one-sided
    market look two-sided for as long as it lasts."""
    gone = Order(
        unix=20,
        instrument=BTC,
        symbol="BTC-USD",
        side=Side.ASK,
        order_id="A1",
        state=State.CANCELLED,
    )
    first, second = books([*TWO_SIDED, gone])
    assert first.px == 100.25
    assert second.bid_px == 100.0 and second.ask_px is None
    assert second.px is None and second.qty is None
    assert second.spread is None and second.micro_px is None and second.imbalance is None


# -- the whole way, from a venue's own lines ---------------------------------

REFRESHES = [
    "35=X|49=XCME|52=20260821-10:30:00.100|55=BTC-USD|207=XCME|15=USD|268=2|"
    "279=0|269=0|270=100.0|271=5|278=L1|272=20260821|273=10:30:00.010|"
    "279=0|269=1|270=100.5|271=7|278=L2|273=10:30:00.010",
    "35=X|49=XCME|52=20260821-10:30:00.200|55=BTC-USD|207=XCME|15=USD|268=1|"
    "279=1|269=0|270=100.0|271=9|278=L1|272=20260821|273=10:30:00.020",
    "35=X|49=XCME|52=20260821-10:30:00.300|55=BTC-USD|207=XCME|15=USD|268=1|"
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
    assert len(found[-1].ask_executions) == 1


def test_the_folded_books_are_a_table() -> None:
    """A book is a row, and a run of them casts as the batch it will be written as."""
    import pyarrow

    from rekep.market import FixEvents

    events = [one for line in REFRESHES for one in FixEvents.from_text(line, venue="XCME")]
    batch = Book.FIELD.cast_arrow_batch(
        pyarrow.RecordBatch.from_pylist([one.into_dict() for one in books(events)])
    )
    assert batch.num_rows == 3
    assert batch.schema.equals(Book.FIELD.into_arrow_schema())
    assert batch.column("bid_px").to_pylist() == [100.0, 100.0, 100.0]
    assert len(set(batch.column("hash").to_pylist())) == 3, "three versions, three identities"


# -- the running totals are the walk, and that is the whole optimisation ------


def walked(side: Resting) -> list[tuple[float, float, int]]:
    """The levels, aggregated the slow way: over every live order, every time."""
    found: dict[float, list[float]] = {}
    for one in side.sorted_orders:
        px = one.px or 0.0
        totals = found.setdefault(px, [0.0, 0.0])
        totals[0] += one.leaves_qty if one.leaves_qty is not None else (one.qty or 0.0)
        totals[1] += 1
    facing = -side.sign
    return [
        (px, totals[0], int(totals[1]))
        for px, totals in sorted(found.items(), key=lambda item: item[0] * facing)
    ]


def test_the_running_levels_are_what_walking_the_orders_would_give() -> None:
    """`Resting` keeps `levels` up to date as orders move rather than re-aggregating
    per snapshot, which is where the fold's throughput comes from -- and which is
    only sound if the two agree at every step, not just at the end."""
    import random

    generate = random.Random(11)
    for turn in range(400):
        side = Resting(side=Side.BID if turn % 2 else Side.ASK)
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
            assert [(level.px, level.qty, level.orders) for level in side.into_levels()] == walked(
                side
            ), f"turn {turn}, step {step}"


def test_the_two_parallel_lists_never_drift_apart() -> None:
    """`keys` and `alive` are the same levels in the same order, and a snapshot
    walks the second on the strength of the first. Only two places move a level;
    this is what says both of them move both lists."""
    import random

    generate = random.Random(17)
    for turn in range(200):
        side = Resting(side=Side.BID if turn % 2 else Side.ASK)
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
            assert [one.key for one in side.alive] == side.keys
            assert side.keys == sorted(side.keys), "and sorted, which is the whole point"
            assert [one.px for one in side.alive] == [one.px for one in side.into_levels()]


def test_a_level_that_reaches_zero_is_dropped_rather_than_kept_at_zero() -> None:
    """Leaving it would put an empty price in every `alive` list from then on."""
    side = Resting(side=Side.BID)
    side.apply(order(10, Side.BID, 100.0, 5.0, "B1"))
    side.apply(order(20, Side.BID, 100.0, 5.0, "B1", state=State.CANCELLED))
    assert side.levels == {} and side.into_levels() == []


def test_an_order_that_moved_price_leaves_the_level_it_was_on() -> None:
    side = Resting(side=Side.BID)
    side.apply(order(10, Side.BID, 100.0, 5.0, "B1"))
    side.apply(order(20, Side.BID, 99.0, 5.0, "B1", state=State.OPEN))
    assert [(level.px, level.qty) for level in side.into_levels()] == [(99.0, 5.0)]


def test_removing_an_order_forgets_the_name_that_pointed_at_it() -> None:
    """Or a later order reusing the identifier would find a version that is gone."""
    side = Resting(side=Side.BID)
    side.apply(order(10, Side.BID, 100.0, 5.0, "B1"))
    side.apply(order(20, Side.BID, 100.0, 5.0, "B1", state=State.CANCELLED))
    assert side.named == {}
    side.apply(order(30, Side.BID, 98.0, 2.0, "B1"))
    assert [(level.px, level.qty) for level in side.into_levels()] == [(98.0, 2.0)]


def test_a_report_that_omits_the_venue_still_finds_the_order_it_continues() -> None:
    """A lifecycle is hashed from the instrument, the venue and the identifier, and
    venues omit their own name because they know which one they are."""
    side = Resting(side=Side.BID)
    side.apply(order(10, Side.BID, 100.0, 5.0, "B1", venue="XCME"))
    bare = Order(
        unix=20,
        instrument=BTC,
        symbol="BTC-USD",
        side=Side.BID,
        order_id="B1",
        qty=2.0,
        state=State.OPEN,
    )
    assert side.standing(bare) is not None
    side.apply(bare)
    assert [(level.px, level.qty, level.orders) for level in side.into_levels()] == [
        (100.0, 2.0, 1)
    ], "one order, replaced, and not two"
