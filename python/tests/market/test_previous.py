"""`with_previous`: what a version carries forward, and what it works out for itself.

A venue restates only what changed, so a row is complete only once the version
before it has been read. The rules below are market rules, not conveniences,
and each test says which one it is pinning.
"""

from __future__ import annotations

import pytest

from rekep.market import (
    AssetKind,
    Book,
    BookSide,
    ExecKind,
    Execution,
    Instrument,
    Order,
    OrderKind,
    Side,
    State,
    TimeInForce,
)
from rekep.market.identity import NIL

EQUITY = Instrument(symbol="AAPL", exchange="XNAS", kind=AssetKind.EQUITY)


def resting(**given: object) -> Order:
    """A live limit order, complete, as a venue's first acknowledgement of one."""
    declared = {
        "unix": 10,
        "symbol": "AAPL",
        "instrument": EQUITY,
        "side": Side.BUY,
        "px": 100.0,
        "qty": 10.0,
        "kind": OrderKind.LIMIT_ORDER,
        "tif": TimeInForce.DAY,
        "state": State.NEW,
        "order_id": "ORD-1",
        "client_order_id": "CL-1",
        "venue": "XNAS",
    }
    return Order(**{**declared, **given}).with_previous(None)


# -- the envelope ------------------------------------------------------------


def test_a_first_version_still_gets_its_identity() -> None:
    """`with_previous(None)` is a first version, and that is the one thing left to do."""
    first = resting()
    assert first.xhash != NIL and first.hash != NIL
    assert first.version == 0 and first.prev_hash is None


def test_the_lifecycle_and_the_counter_carry_across_versions() -> None:
    first = resting()
    later = Order(unix=20, order_id="ORD-1", state=State.OPEN).with_previous(first)
    assert later.xhash == first.xhash, "the same order"
    assert later.hash != first.hash, "a different version of it"
    assert later.version == first.version + 1


def test_the_transition_is_on_the_row_rather_than_behind_a_self_join() -> None:
    first = resting()
    later = Order(unix=20, order_id="ORD-1", state=State.PARTIALLY_FILLED).with_previous(first)
    assert later.prev_hash == first.hash
    assert later.prev_state is State.NEW and later.prev_unix == first.unix
    assert later.unix - later.prev_unix == 10, "dwell time, without a join"


def test_a_previous_version_that_was_never_hashed_is_a_null_and_not_a_nil() -> None:
    unhashed = Order(unix=10, symbol="AAPL", state=State.NEW)
    assert unhashed.hash == NIL
    assert Order(unix=20).with_previous(unhashed).prev_hash is None


def test_the_creation_time_is_got_or_set_and_never_recomputed() -> None:
    """A lifecycle is created once; every later version was created then too.
    Recomputing it would make "how old is this order" mean "how long since the
    last message about it"."""
    first = resting(cunix=5)
    third = Order(unix=30, order_id="ORD-1").with_previous(
        Order(unix=20, order_id="ORD-1").with_previous(first)
    )
    assert third.cunix == 5 and third.unix == 30


def test_a_version_that_says_nothing_about_the_state_keeps_the_one_it_had() -> None:
    """Not mentioning a state is not saying it is unknown."""
    first = resting()
    silent = Order(unix=20, order_id="ORD-1").with_previous(first)
    assert silent.state is State.NEW


def test_a_message_with_no_clock_lands_where_the_version_before_it_was() -> None:
    first = resting()
    timeless = Order(order_id="ORD-1", state=State.OPEN).with_previous(first)
    assert timeless.unix == first.unix
    assert timeless.hunix == first.hunix, "and the partition follows the time"


def test_what_the_message_did_send_always_wins() -> None:
    """This completes a row; it does not correct one."""
    first = resting()
    restated = Order(unix=20, order_id="ORD-1", px=101.0, qty=3.0, state=State.OPEN)
    later = restated.with_previous(first)
    assert later.px == 101.0 and later.qty == 3.0


# -- the market slots --------------------------------------------------------


def test_the_price_and_the_instrument_are_carried_because_a_venue_stops_repeating_them() -> None:
    """A row with a null price drops out of every filter on price."""
    first = resting()
    later = Order(unix=20, order_id="ORD-1", state=State.OPEN).with_previous(first)
    assert later.px == 100.0 and later.qty == 10.0
    assert later.side is Side.BUY and later.symbol == "AAPL"
    assert later.instrument.xhash == EQUITY.xhash
    assert later.instrument_hash == first.instrument_hash, "and it stays in its partition"


def test_a_notional_is_a_product_of_three_and_absent_without_all_three() -> None:
    """A notional computed with a multiplier of "probably one" is wrong by a factor
    nobody notices until settlement."""
    assert resting().notional == 1000.0, "a cash equity really does trade one for one"
    future = Instrument(symbol="ESZ6", kind=AssetKind.FUTURE)
    assert resting(instrument=future, symbol="ESZ6").notional is None
    priced = Instrument(symbol="ESZ6", kind=AssetKind.FUTURE, multiplier=50.0)
    assert resting(instrument=priced, symbol="ESZ6").notional == 50_000.0


def test_a_notional_the_producer_computed_is_not_recomputed() -> None:
    assert resting(notional=7.0).notional == 7.0


# -- what an order works out for itself --------------------------------------


def test_a_fresh_order_rests_its_whole_quantity() -> None:
    assert resting().leaves_qty == 10.0


def test_what_is_left_is_what_was_asked_minus_what_was_done() -> None:
    """A venue that sends `CumQty <14>` and not `LeavesQty <151>` has still said
    how much is working."""
    part = Order(unix=20, order_id="ORD-1", filled_qty=4.0, state=State.PARTIALLY_FILLED)
    assert part.with_previous(resting()).leaves_qty == 6.0


def test_what_the_venue_said_is_left_is_not_second_guessed() -> None:
    part = Order(unix=20, order_id="ORD-1", filled_qty=4.0, leaves_qty=5.0)
    assert part.with_previous(resting()).leaves_qty == 5.0


@pytest.mark.parametrize(
    "state", [State.FILLED, State.CANCELLED, State.EXPIRED, State.REJECTED, State.DONE_FOR_DAY]
)
def test_a_terminal_order_rests_nothing_at_all(state: State) -> None:
    """It is done, and a book folding it has to take its liquidity out rather than
    leave it standing."""
    done = Order(unix=20, order_id="ORD-1", state=state).with_previous(resting())
    assert done.qty == 0.0 and done.leaves_qty == 0.0


def test_what_was_asked_for_is_on_the_version_before_and_not_lost() -> None:
    first = resting()
    done = Order(unix=20, order_id="ORD-1", state=State.CANCELLED).with_previous(first)
    assert done.prev_hash == first.hash and first.qty == 10.0


def test_a_fully_filled_order_filled_what_it_asked_for() -> None:
    """`FILLED` means all of it, so a report that says the state and not `CumQty`
    has still said how much was done."""
    done = Order(unix=20, order_id="ORD-1", state=State.FILLED).with_previous(resting())
    assert done.filled_qty == 10.0


def test_a_cancelled_order_does_not_claim_to_have_filled_anything() -> None:
    done = Order(unix=20, order_id="ORD-1", state=State.CANCELLED).with_previous(resting())
    assert done.filled_qty is None, "nothing said how much, and nothing may invent it"


def test_the_identifier_that_was_replaced_is_the_one_the_version_before_had() -> None:
    """FIX requires a new `ClOrdID <11>` per version and calls the old one
    `OrigClOrdID <41>`; when this version did not say, the version before is it."""
    replaced = Order(unix=20, client_order_id="CL-2", state=State.OPEN).with_previous(resting())
    assert replaced.prev_client_order_id == "CL-1"


def test_a_version_that_reuses_the_identifier_names_nothing_replaced() -> None:
    same = Order(unix=20, client_order_id="CL-1", state=State.OPEN).with_previous(resting())
    assert same.prev_client_order_id is None


def test_the_order_kind_and_time_in_force_are_carried() -> None:
    later = Order(unix=20, order_id="ORD-1", state=State.OPEN).with_previous(resting())
    assert later.kind is OrderKind.LIMIT_ORDER and later.tif is TimeInForce.DAY


# -- what an execution works out ---------------------------------------------


def fill(px: float, qty: float, unix: int, **given: object) -> Execution:
    declared = {"unix": unix, "kind": ExecKind.TRADED, "px": px, "qty": qty}
    return Execution(**{**declared, **given})


def test_a_fill_links_itself_to_the_order_it_followed() -> None:
    first = resting()
    done = fill(100.0, 4.0, 20, exec_id="EX-1").with_previous(first)
    assert done.order_xhash == first.xhash


def test_what_is_done_accumulates_across_fills() -> None:
    """A venue that sends `LastQty <32>` and not `CumQty <14>` has still said how
    much is now done."""
    one = fill(100.0, 4.0, 20, exec_id="EX-1").with_previous(resting())
    two = fill(101.0, 6.0, 30, exec_id="EX-2").with_previous(one)
    assert (one.filled_qty, two.filled_qty) == (4.0, 10.0)
    assert (one.leaves_qty, two.leaves_qty) == (6.0, 0.0)


def test_the_average_is_reweighted_and_never_copied_forward() -> None:
    """Copying it would leave every partial fill reporting the first one's price."""
    one = fill(100.0, 4.0, 20, exec_id="EX-1").with_previous(resting())
    two = fill(101.0, 6.0, 30, exec_id="EX-2").with_previous(one)
    assert one.avg_px == 100.0
    assert two.avg_px == pytest.approx((100.0 * 4 + 101.0 * 6) / 10)


def test_an_acknowledgement_changes_no_running_total() -> None:
    """Adding its quantity is how a fills table starts overcounting."""
    one = fill(100.0, 4.0, 20, exec_id="EX-1").with_previous(resting())
    acked = Execution(unix=30, kind=ExecKind.ACK, qty=99.0, exec_id="EX-3").with_previous(one)
    assert acked.filled_qty == 4.0 and acked.leaves_qty == 6.0
    assert acked.avg_px == 100.0


def test_a_correction_is_another_version_of_one_execution_not_a_second_one() -> None:
    one = fill(100.0, 4.0, 20, exec_id="EX-1").with_previous(resting())
    fixed = fill(100.5, 4.0, 25, exec_id="EX-1", kind=ExecKind.TRADE_CORRECT).with_previous(one)
    assert fixed.xhash == one.xhash and fixed.version == one.version + 1


def test_a_chain_crossing_classes_still_carries_forward() -> None:
    """One `ExecutionReport <8>` yields an order and a fill, so the version before
    either of them is regularly the other."""
    order = resting()
    done = fill(100.0, 4.0, 20, exec_id="EX-1").with_previous(order)
    after = Order(unix=25, order_id="ORD-1", state=State.PARTIALLY_FILLED).with_previous(done)
    assert after.filled_qty == 4.0 and after.leaves_qty == 6.0
    assert after.avg_px == 100.0


def test_the_abstract_slots_do_not_cross_between_shapes() -> None:
    """`px` and `qty` mean what the subclass says they mean: an order's are what it
    asked for, a fill's are what traded. Carrying one into the other made a partly
    filled order claim it had asked for exactly what had just traded."""
    done = fill(100.0, 4.0, 20, exec_id="EX-1").with_previous(resting())
    after = Order(unix=25, order_id="ORD-1", state=State.PARTIALLY_FILLED).with_previous(done)
    assert after.qty is None and after.px is None
    assert after.leaves_qty == 6.0, "but what is left means the same on both, and carries"


# -- an append that changed nothing ------------------------------------------


def test_an_append_that_moved_a_level_returns_the_new_version() -> None:
    side = BookSide(side=Side.BID, xhash=1)
    moved = side.append_order(resting())
    assert moved is side and side.version == 1 and side.total_qty == 10.0


def test_an_append_that_moved_nothing_returns_none_and_leaves_the_side_alone() -> None:
    """A caller that writes what it gets back writes one row per real change
    rather than one per message."""
    side = BookSide(side=Side.BID, xhash=1)
    side.append_order(resting())
    before = (side.version, side.hash, side.total_qty, len(side.updates or []))
    gone = Order(unix=20, side=Side.BUY, px=90.0, state=State.CANCELLED).with_previous(resting())
    assert side.append_order(gone) is None, "a level that was never there"
    assert (side.version, side.hash, side.total_qty, len(side.updates or [])) == before


def test_an_acknowledgement_does_not_version_a_side() -> None:
    side = BookSide(side=Side.BID, xhash=1)
    side.append_order(resting())
    before = side.version
    acked = Execution(unix=20, side=Side.BUY, px=100.0, qty=1.0, kind=ExecKind.ACK)
    assert side.append_execution(acked) is None
    assert side.version == before and side.total_qty == 10.0


def test_a_fill_with_no_price_moves_nothing() -> None:
    side = BookSide(side=Side.BID, xhash=1)
    side.append_order(resting())
    blind = Execution(unix=20, side=Side.BUY, qty=1.0, kind=ExecKind.TRADED)
    assert side.append_execution(blind) is None


def test_a_book_that_did_not_move_is_not_versioned_either() -> None:
    book = Book(xhash=7)
    assert book.append_order(resting()) is book and book.version == 1
    acked = Execution(unix=20, side=Side.BUY, px=100.0, qty=1.0, kind=ExecKind.ACK)
    assert book.append_execution(acked) is None
    assert book.version == 1 and book.bid_total_qty == 10.0


def test_append_event_dispatches_and_reports_the_same_way() -> None:
    side = BookSide(side=Side.BID, xhash=1)
    assert side.append_event(resting()) is side
    acked = Execution(unix=20, side=Side.BUY, px=100.0, qty=1.0, kind=ExecKind.ACK)
    assert side.append_event(acked) is None


# -- a version of it, or a different thing built from it ---------------------


def test_a_fill_is_not_a_version_of_the_order_it_happened_to() -> None:
    """A version counter counts one lifecycle, and a fill has its own."""
    order = resting()
    done = fill(100.0, 4.0, 20, exec_id="EX-1").with_previous(order)
    assert done.xhash != order.xhash
    assert done.version == 0, "version zero of its own life"
    assert done.prev_hash is None, "nothing came before it"
    assert done.parent_hash == [order.hash], "but it was built from the order"


def test_a_code_is_never_carried_from_a_shape_that_spells_it_differently() -> None:
    """`kind` is a different enum per shape -- an order's is `OrderKind`, a fill's
    is `ExecKind` -- and a version chain crosses shapes, because one
    `ExecutionReport <8>` yields both. Carried by name alone, an order following a
    fill took `ExecKind.TRADED`, which is `310`, which reads back as
    `OrderKind.STOP_ORDER`; and a fill following an order took an `OrderKind` and
    raised on the first `moves_shares`."""
    order = resting()
    done = fill(100.0, 4.0, 20, exec_id="EX-1").with_previous(order)
    after = Order(unix=25, order_id="ORD-1", state=State.PARTIALLY_FILLED).with_previous(done)
    assert after.kind is OrderKind.UNKNOWN, "and never an ExecKind wearing an int"
    crossed = Execution(unix=30, exec_id="EX-2", px=1.0, qty=1.0).with_previous(order)
    assert crossed.kind is ExecKind.UNKNOWN
    assert crossed.kind.moves_shares is False, "which is a question only an ExecKind answers"


def test_a_code_is_carried_between_versions_of_the_same_shape() -> None:
    """Which is the whole point of carrying it -- a venue stops repeating it."""
    order = resting()
    later = Order(unix=20, order_id="ORD-1", state=State.OPEN).with_previous(order)
    assert later.kind is OrderKind.LIMIT_ORDER and later.tif is TimeInForce.DAY
    one = fill(100.0, 4.0, 20, exec_id="EX-1").with_previous(order)
    two = fill(100.5, 1.0, 30, exec_id="EX-1").with_previous(one)
    assert two.kind is ExecKind.TRADED


def test_a_lifecycle_is_read_after_completing_and_not_before() -> None:
    """An order version that arrived carrying only its `OrderID <37>` does not know
    its own instrument or venue until the version before it has given them to it --
    and those are part of what its lifecycle is."""
    first = resting()
    bare = Order(unix=20, order_id="ORD-1", state=State.OPEN)
    assert bare.life_hash() != first.life_hash(), "before completing, it looks like another"
    assert bare.with_previous(first).xhash == first.xhash


def test_asking_what_a_lifecycle_is_does_not_change_it() -> None:
    """`life_hash` reads; `identify` and `with_previous` are what write."""
    bare = Order(unix=20, order_id="ORD-1")
    assert bare.life_hash() != NIL
    assert bare.xhash == NIL, "asking left it exactly as it was"


def test_an_event_with_nothing_to_identify_it_inherits_the_lifecycle() -> None:
    """The only one available to it."""
    first = resting()
    nameless = Order(unix=20, state=State.OPEN)
    assert nameless.life_hash() == NIL, "on its own it can identify nothing"
    anonymous = nameless.with_previous(first)
    assert anonymous.xhash == first.xhash and anonymous.version == 1


def test_a_parent_is_recorded_once_however_often_it_is_completed_from() -> None:
    order = resting()
    done = fill(100.0, 4.0, 20, exec_id="EX-1").with_previous(order)
    assert done.with_previous(order).parent_hash == [order.hash]
