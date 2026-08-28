"""`with_previous`: what a version carries forward, and what it works out for itself.

A venue restates only what changed, so a row is complete only once the version
before it has been read. The rules below are market rules, not conveniences,
and each test says which one it is pinning.
"""

from __future__ import annotations

import copy

import pytest

from rekep.market import (
    MIC,
    AssetKind,
    Event,
    Execution,
    Instrument,
    MarketKind,
    Order,
    Side,
    State,
    TimeInForce,
)
from rekep.market.identity import NIL, hash_bytes_of

EQUITY = Instrument(symbol="AAPL", exchange="XNAS", kind=AssetKind.EQUITY)
XNAS = MIC.from_str("XNAS")


def _order(_instrument: Instrument = EQUITY, **given: object) -> Order:
    """An order with its persisted instrument key and transient reference."""
    return Order(**{"instrumentxhash": _instrument.xhash, **given}).attach_instrument(_instrument)


def _execution(_instrument: Instrument = EQUITY, **given: object) -> Execution:
    """An execution with its persisted instrument key and transient reference."""
    values = {"instrumentxhash": _instrument.xhash, **given}
    return Execution(**values).attach_instrument(_instrument)


def changed(event: Event, previous: Event | None = None) -> Event:
    """Complete a version expected to change a stored fact."""
    completed = event.with_previous(previous)
    assert completed is not None
    return completed


def resting(**given: object) -> Order:
    """A live limit order, complete, as a venue's first acknowledgement of one."""
    instrument = given.pop("instrument", EQUITY)
    assert isinstance(instrument, Instrument)
    declared = {
        "unix": 10,
        "side": Side.BUY,
        "px": 100.0,
        "qty": 10.0,
        "kind": MarketKind.LIMIT_ORDER,
        "tif": TimeInForce.DAY,
        "state": State.NEW,
        "orderid": "ORD-1",
        "clientorderid": "CL-1",
        "mic": XNAS,
    }
    completed = changed(_order(instrument, **{**declared, **given}))
    assert isinstance(completed, Order)
    return completed


# -- the envelope ------------------------------------------------------------


def test_a_first_version_still_gets_its_identity() -> None:
    """`with_previous(None)` is a first version, and that is the one thing left to do."""
    first = resting()
    assert first.xhash != NIL and first.hash != NIL
    assert first.code == "ORD-1"
    assert first.xhash == Order.hash_of(
        hash_bytes_of(EQUITY.xhash), first.mic, first.code, first.side
    )
    assert first.version == 0 and first.prevunix is None


def test_the_lifecycle_and_the_counter_carry_across_versions() -> None:
    first = resting()
    later = changed(_order(unix=20, orderid="ORD-1", state=State.OPEN), first)
    assert later.xhash == first.xhash, "the same order"
    assert later.code == first.code == "ORD-1"
    assert later.hash != first.hash, "a different version of it"
    assert later.version == first.version + 1


def test_the_transition_time_is_on_the_row_rather_than_behind_a_self_join() -> None:
    first = resting()
    later = changed(_order(unix=20, orderid="ORD-1", state=State.PARTIALLY_FILLED), first)
    assert later.prevunix == first.unix
    assert later.unix - later.prevunix == 10, "dwell time, without a join"


def test_the_creation_time_is_got_or_set_and_never_recomputed() -> None:
    """A lifecycle is created once; every later version was created then too.
    Recomputing it would make "how old is this order" mean "how long since the
    last message about it"."""
    first = resting(cunix=5)
    second = changed(_order(unix=20, orderid="ORD-1", state=State.OPEN), first)
    third = changed(_order(unix=30, orderid="ORD-1", state=State.PARTIALLY_FILLED), second)
    assert third.cunix == 5 and third.unix == 30


def test_a_duplicate_observation_returns_none() -> None:
    """A later timestamp alone does not create another stored version."""
    first = resting()
    duplicate = _order(unix=20, orderid="ORD-1").with_previous(first)
    assert duplicate is None


def test_state_comparison_keeps_late_members_and_snapshot_time() -> None:
    """The cached projection may omit clocks only for ordinary observations."""
    first = resting()
    observed = copy.copy(first)
    observed.unix += 1
    assert observed.same_as(first), "an ordinary observation time is not stored state"

    observed.reason = "late declared field changed"
    assert not observed.same_as(first), "the projection reaches the last order member"

    snapshot = copy.copy(first)
    snapshot.sunix = first.unix
    later_snapshot = copy.copy(snapshot)
    later_snapshot.unix += 1
    assert not later_snapshot.same_as(snapshot), "snapshot time is part of its stored state"

    nonfinite = resting(px=float("nan"))
    assert not copy.copy(nonfinite).same_as(nonfinite), "NaN keeps its scalar inequality"


def test_a_message_with_no_clock_lands_where_the_version_before_it_was() -> None:
    first = resting()
    timeless = changed(_order(orderid="ORD-1", state=State.OPEN), first)
    assert timeless.unix == first.unix
    assert timeless.unixpartition == first.unixpartition, "and the partition follows the time"


def test_what_the_message_did_send_always_wins() -> None:
    """This completes a row; it does not correct one."""
    first = resting()
    restated = _order(unix=20, orderid="ORD-1", px=101.0, qty=3.0, state=State.OPEN)
    later = changed(restated, first)
    assert later.px == 101.0 and later.qty == 3.0


def test_priced_transition_values_are_kept_without_a_self_join() -> None:
    first = resting()
    later = changed(_order(unix=20, orderid="ORD-1", px=101.0, qty=8.0, state=State.OPEN), first)
    assert (later.prevpx, later.prevqty, later.prevnotional) == (
        first.px,
        first.qty,
        first.notional,
    )


def test_transition_values_do_not_turn_a_duplicate_observation_into_a_version() -> None:
    first = resting()
    assert _order(unix=20, orderid="ORD-1").with_previous(first) is None


def test_quote_sides_with_one_source_identifier_are_distinct_lifecycles() -> None:
    bid = resting(orderid="QUOTE-1", indicative=True, side=Side.BUY)
    ask = resting(orderid="QUOTE-1", indicative=True, side=Side.SELL)
    assert bid.xhash != ask.xhash


# -- the market slots --------------------------------------------------------


def test_the_price_and_the_instrument_are_carried_because_a_venue_stops_repeating_them() -> None:
    """A row with a null price drops out of every filter on price."""
    first = resting()
    later = changed(_order(unix=20, orderid="ORD-1", state=State.OPEN), first)
    assert later.px == 100.0 and later.qty == 10.0
    assert later.side is Side.BUY and later.symbol == "AAPL"
    assert later.into_instrument() is EQUITY
    assert later.instrumentxhash == first.instrumentxhash, "and it stays in its partition"


def test_the_lifecycle_code_does_not_cross_from_an_order_to_its_fill() -> None:
    first = resting()
    assert first.code == "ORD-1" and first.codes == {}
    later = changed(Order(unix=20, orderid="ORD-1", state=State.OPEN), first)
    assert later.codes == {}
    fill = changed(_execution(unix=30, execid="EX-1", state=State.FILLED, qty=4.0), first)
    assert fill.code == "EX-1", "and never `ORD-1`: an execution is not a version of its order"
    assert fill.codes == {}


def test_an_identifier_already_recorded_is_never_displaced_by_a_later_one() -> None:
    """First spelling wins, so a `codes` entry is as stable as the lifecycle."""
    order = _order(unix=10, orderid="ORD-1")
    order.name_code("symbol", "RENAMED")
    order.name_code("symbol", "SECOND")
    assert order.codes["symbol"] == "RENAMED"
    order.name_code("isin", "FAKE-ISIN-0001")
    assert order.codes == {"symbol": "RENAMED", "isin": "FAKE-ISIN-0001"}


def test_a_notional_is_a_product_of_three_and_absent_without_all_three() -> None:
    """A notional computed with a multiplier of "probably one" is wrong by a factor
    nobody notices until settlement."""
    assert resting().notional == 1000.0, "a cash equity really does trade one for one"
    future = Instrument(symbol="ESZ6", kind=AssetKind.FUTURE)
    assert resting(instrument=future).notional is None
    priced = Instrument(symbol="ESZ6", kind=AssetKind.FUTURE, multiplier=50.0)
    assert resting(instrument=priced, code="ESZ6").notional == 50_000.0


def test_a_notional_the_producer_computed_is_not_recomputed() -> None:
    assert resting(notional=7.0).notional == 7.0


# -- what an order works out for itself --------------------------------------


def test_a_fresh_order_rests_its_whole_quantity() -> None:
    assert resting().qty == 10.0


def test_current_and_previous_quantity_describe_the_order_transition() -> None:
    part = _order(unix=20, orderid="ORD-1", qty=6.0, state=State.PARTIALLY_FILLED)
    completed = changed(part, resting())
    assert completed.qty == 6.0 and completed.prevqty == 10.0


def test_displayed_quantity_is_preserved_when_remaining_quantity_shrinks() -> None:
    first = resting(hiddenqty=4.0)
    part = changed(_order(unix=20, orderid="ORD-1", qty=5.0), first)
    assert part.qty == 5.0 and part.hiddenqty == 0.0 and part.prevqty == 10.0


@pytest.mark.parametrize(
    "state", [State.FILLED, State.CANCELLED, State.EXPIRED, State.REJECTED, State.DONE_FOR_DAY]
)
def test_a_terminal_order_rests_nothing_at_all(state: State) -> None:
    """It is done, and a book folding it has to take its liquidity out rather than
    leave it standing."""
    done = changed(_order(unix=20, orderid="ORD-1", state=state), resting())
    assert done.qty == 0.0 and done.hiddenqty == 0.0 and done.prevqty == 10.0


def test_what_was_asked_for_is_on_the_version_before_and_not_lost() -> None:
    first = resting()
    done = changed(_order(unix=20, orderid="ORD-1", state=State.CANCELLED), first)
    assert done.prevqty == first.qty == 10.0


def test_a_fully_filled_order_keeps_its_prior_quantity_on_the_transition() -> None:
    done = changed(_order(unix=20, orderid="ORD-1", state=State.FILLED), resting())
    assert done.qty == 0.0 and done.prevqty == 10.0


def test_a_terminal_order_without_history_preserves_its_source_quantity() -> None:
    done = _order(unix=20, orderid="ORD-1", qty=100.0, state=State.CANCELLED)
    done.with_previous(None)
    assert done.qty == 0.0 and done.prevqty == 100.0


def test_a_cancelled_order_does_not_store_a_cumulative_fill_quantity() -> None:
    done = changed(_order(unix=20, orderid="ORD-1", state=State.CANCELLED), resting())
    assert "filledqty" not in done.into_field().names


def test_the_identifier_that_was_replaced_is_the_one_the_version_before_had() -> None:
    """FIX requires a new `ClOrdID <11>` per version and calls the old one
    `OrigClOrdID <41>`; when this version did not say, the version before is it."""
    replaced = changed(_order(unix=20, clientorderid="CL-2", state=State.OPEN), resting())
    assert replaced.prevclientorderid == "CL-1"


def test_a_version_that_reuses_the_identifier_names_nothing_replaced() -> None:
    same = changed(_order(unix=20, clientorderid="CL-1", state=State.OPEN), resting())
    assert same.prevclientorderid is None


def test_the_order_kind_and_time_in_force_are_carried() -> None:
    later = changed(_order(unix=20, orderid="ORD-1", state=State.OPEN), resting())
    assert later.kind is MarketKind.LIMIT_ORDER and later.tif is TimeInForce.DAY


# -- what an execution works out ---------------------------------------------


def fill(px: float, qty: float, unix: int, **given: object) -> Execution:
    instrument = given.pop("instrument", EQUITY)
    assert isinstance(instrument, Instrument)
    declared = {"unix": unix, "state": State.FILLED, "px": px, "qty": qty}
    return _execution(instrument, **{**declared, **given})


def test_a_fill_links_itself_to_the_order_it_followed() -> None:
    first = resting()
    done = fill(100.0, 4.0, 20, execid="EX-1").with_previous(first)
    assert done.linkedevents == [(first.unix, first.xhash)]


def test_the_matched_order_precedes_other_lifecycle_links() -> None:
    first = resting()
    done = fill(100.0, 4.0, 20, execid="EX-1", linkedevents=[(0, -1)]).with_previous(first)
    assert done is not None
    assert done.linkedevents == [(first.unix, first.xhash), (0, -1)]


def test_what_is_done_accumulates_across_fills() -> None:
    """A venue that sends `LastQty <32>` and not `CumQty <14>` has still said how
    much is now done."""
    one = fill(100.0, 4.0, 20, execid="EX-1").with_previous(resting())
    two = fill(101.0, 6.0, 30, execid="EX-2").with_previous(one)
    assert (one.filledqty, two.filledqty) == (4.0, 10.0)
    assert (one.leavesqty, two.leavesqty) == (6.0, 0.0)


def test_the_average_is_reweighted_and_never_copied_forward() -> None:
    """Copying it would leave every partial fill reporting the first one's price."""
    one = fill(100.0, 4.0, 20, execid="EX-1").with_previous(resting())
    two = fill(101.0, 6.0, 30, execid="EX-2").with_previous(one)
    assert one.vwap == 100.0
    assert two.vwap == pytest.approx((100.0 * 4 + 101.0 * 6) / 10)


def test_zero_is_a_price_when_reweighting_the_average() -> None:
    one = fill(0.0, 1.0, 20, execid="EX-1").with_previous(resting())
    two = fill(10.0, 1.0, 30, execid="EX-2").with_previous(one)
    assert two.vwap == 5.0


def test_an_unknown_prior_total_stays_unknown_when_another_fill_arrives() -> None:
    previous = _execution(
        unix=20,
        px=100.0,
        qty=1.0,
        state=State.FILLED,
        execid="EX-1",
        filledqty=None,
        leavesqty=9.0,
        vwap=100.0,
    ).with_previous(None)
    later = fill(110.0, 1.0, 30, execid="EX-2").with_previous(previous)

    assert later.filledqty is None and later.vwap is None
    assert later.leavesqty == 8.0


def test_a_fill_without_a_price_cannot_update_an_existing_average() -> None:
    previous = _execution(
        unix=20,
        qty=1.0,
        state=State.FILLED,
        execid="EX-1",
        filledqty=1.0,
        leavesqty=9.0,
        vwap=100.0,
    ).with_previous(None)
    later = _execution(unix=30, qty=1.0, state=State.FILLED, execid="EX-2").with_previous(previous)
    assert later.filledqty == 2.0 and later.vwap is None


def test_an_acknowledgement_changes_no_running_total() -> None:
    """Adding its quantity is how a fills table starts overcounting."""
    one = fill(100.0, 4.0, 20, execid="EX-1").with_previous(resting())
    acked = _execution(unix=30, state=State.NEW, qty=99.0, execid="EX-3").with_previous(one)
    assert acked.filledqty == 4.0 and acked.leavesqty == 6.0
    assert acked.vwap == 100.0


def test_a_correction_is_another_version_of_one_execution_not_a_second_one() -> None:
    one = fill(100.0, 4.0, 20, execid="EX-1").with_previous(resting())
    fixed = fill(
        100.5,
        4.0,
        25,
        execid="EX-2",
        execrefid="EX-1",
        state=State.REPLACED,
    ).with_previous(one)
    assert fixed.execid == "EX-2" and fixed.code == "EX-1"
    assert fixed.xhash == one.xhash and fixed.version == one.version + 1


def test_a_correction_replaces_the_referenced_fill_in_running_totals() -> None:
    one = fill(100.0, 4.0, 20, execid="EX-1").with_previous(resting())
    fixed = fill(
        100.5,
        4.0,
        25,
        execid="EX-2",
        execrefid="EX-1",
        state=State.REPLACED,
    ).with_previous(one)

    assert (fixed.filledqty, fixed.leavesqty) == (4.0, 6.0)
    assert fixed.vwap == 100.5


def test_a_cancel_removes_the_referenced_fill_from_running_totals() -> None:
    one = fill(100.0, 4.0, 20, execid="EX-1").with_previous(resting())
    two = fill(102.0, 2.0, 25, execid="EX-2").with_previous(one)
    cancelled = _execution(
        unix=30,
        execid="EX-3",
        execrefid="EX-2",
        state=State.CANCELLED,
    ).with_previous(two)

    assert (cancelled.filledqty, cancelled.leavesqty) == (4.0, 6.0)
    assert cancelled.vwap == 100.0


@pytest.mark.parametrize(
    ("state", "px", "qty"),
    [
        (State.CANCELLED, None, None),
        (State.REPLACED, 101.0, 3.0),
    ],
)
def test_an_unknown_referenced_price_makes_an_amended_average_unknown(
    state: State, px: float | None, qty: float | None
) -> None:
    previous = _execution(
        unix=20,
        px=None,
        qty=2.0,
        state=State.FILLED,
        execid="EX-1",
        filledqty=6.0,
        leavesqty=4.0,
        vwap=100.0,
    ).with_previous(None)
    amended = _execution(
        unix=30,
        px=px,
        qty=qty,
        state=state,
        execid="EX-2",
        execrefid="EX-1",
    ).with_previous(previous)

    assert amended.vwap is None


def test_a_preidentified_execution_is_rehashed_after_completion_supplies_scope() -> None:
    order = resting()
    done = fill(100.0, 1.0, 20, execid="EX-1").with_previous(None)
    stale = done.xhash

    done.with_previous(order)

    expected = Execution.hash_of(hash_bytes_of(order.instrumentxhash), order.mic, "EX-1", done.side)
    assert stale != expected and done.xhash == expected
    assert done.version == 0 and done.parenthash == [order.hash]


def test_a_preidentified_correction_rejoins_its_scoped_execution() -> None:
    one = fill(100.0, 1.0, 20, execid="EX-1").with_previous(resting())
    fixed = fill(
        100.5,
        1.0,
        25,
        execid="EX-2",
        execrefid="EX-1",
        state=State.REPLACED,
    )
    fixed.with_previous(None)

    fixed.with_previous(one)

    assert fixed.xhash == one.xhash and fixed.version == one.version + 1


def test_successive_corrections_keep_the_original_execution_lifecycle() -> None:
    one = fill(100.0, 1.0, 20, execid="EX-1").with_previous(resting())
    two = fill(
        100.5,
        1.0,
        25,
        execid="EX-2",
        execrefid="EX-1",
        state=State.REPLACED,
    ).with_previous(one)
    three = fill(
        101.0,
        1.0,
        30,
        execid="EX-3",
        execrefid="EX-2",
        state=State.REPLACED,
    ).with_previous(two)

    assert one.code == two.code == three.code == "EX-1"
    assert one.xhash == two.xhash == three.xhash
    assert three.version == two.version + 1


def test_a_successive_correction_can_inherit_its_omitted_kind() -> None:
    one = fill(100.0, 1.0, 20, execid="EX-1").with_previous(resting())
    two = fill(
        100.5,
        1.0,
        25,
        execid="EX-2",
        execrefid="EX-1",
        state=State.REPLACED,
    ).with_previous(one)
    three = fill(
        101.0,
        1.0,
        30,
        execid="EX-3",
        execrefid="EX-2",
        state=State.UNKNOWN,
    ).with_previous(two)

    assert three.state is State.REPLACED
    assert three.code == two.code == one.code == "EX-1"
    assert three.xhash == two.xhash and three.version == two.version + 1


def test_a_normal_trade_does_not_follow_a_stray_execrefid() -> None:
    one = fill(100.0, 1.0, 20, execid="EX-1").with_previous(resting())
    other = fill(
        101.0,
        1.0,
        25,
        execid="EX-2",
        execrefid="EX-1",
        state=State.FILLED,
    ).with_previous(one)

    assert other.code == "EX-2" and other.xhash != one.xhash
    assert other.version == 0 and other.parenthash == [one.hash]


def test_a_chain_crossing_classes_still_carries_forward() -> None:
    """One `ExecutionReport <8>` yields an order and a fill, so the version before
    either of them is regularly the other."""
    order = resting()
    done = fill(100.0, 4.0, 20, execid="EX-1").with_previous(order)
    after = _order(unix=25, orderid="ORD-1", state=State.PARTIALLY_FILLED).with_previous(done)
    assert after.qty == 6.0
    assert after.vwap == 100.0


def test_the_abstract_slots_do_not_cross_between_shapes() -> None:
    """`px` and `qty` mean what the subclass says they mean: an order's are what it
    asked for, a fill's are what traded. Carrying one into the other made a partly
    filled order claim it had asked for exactly what had just traded."""
    done = fill(100.0, 4.0, 20, execid="EX-1").with_previous(resting())
    after = _order(unix=25, orderid="ORD-1", state=State.PARTIALLY_FILLED).with_previous(done)
    assert after.qty == 6.0 and after.px is None


# -- a version of it, or a different thing built from it ---------------------


def test_a_fill_is_not_a_version_of_the_order_it_happened_to() -> None:
    """A version counter counts one lifecycle, and a fill has its own."""
    order = resting()
    done = fill(100.0, 4.0, 20, execid="EX-1").with_previous(order)
    assert done.xhash != order.xhash
    assert done.code == "EX-1" and done.code != order.code
    assert done.version == 0, "version zero of its own life"
    assert done.prevunix is None, "nothing in its own lifecycle came before it"
    assert done.parenthash == [order.hash], "but it was built from the order"


def test_specialized_order_codes_do_not_cross_shapes_but_generic_state_does() -> None:
    """Order pricing is shape-specific; lifecycle state is deliberately shared."""
    order = resting()
    done = fill(100.0, 4.0, 20, execid="EX-1").with_previous(order)
    after = _order(unix=25, orderid="ORD-1", state=State.PARTIALLY_FILLED).with_previous(done)
    assert after.kind is MarketKind.UNKNOWN
    crossed = _execution(unix=30, execid="EX-2", px=1.0, qty=1.0).with_previous(order)
    assert crossed.state is State.NEW


def test_a_code_is_carried_between_versions_of_the_same_shape() -> None:
    """Which is the whole point of carrying it -- a venue stops repeating it."""
    order = resting()
    later = _order(unix=20, orderid="ORD-1", state=State.OPEN).with_previous(order)
    assert later.kind is MarketKind.LIMIT_ORDER and later.tif is TimeInForce.DAY
    one = fill(100.0, 4.0, 20, execid="EX-1").with_previous(order)
    two = fill(100.5, 1.0, 30, execid="EX-1").with_previous(one)
    assert two.state is State.FILLED


def test_a_lifecycle_is_read_after_completing_and_not_before() -> None:
    """An order version that arrived carrying only its `OrderID <37>` does not know
    its own instrument or venue until the version before it has given them to it --
    and those are part of what its lifecycle is."""
    first = resting()
    bare = _order(unix=20, orderid="ORD-1", state=State.OPEN)
    assert bare.life_hash() != first.life_hash(), "before completing, it looks like another"
    assert bare.with_previous(first).xhash == first.xhash


def test_asking_what_a_lifecycle_is_does_not_change_it() -> None:
    """`life_hash` reads; `identify` and `with_previous` are what write."""
    bare = _order(unix=20, orderid="ORD-1")
    assert bare.life_hash() != NIL
    assert bare.xhash == NIL and bare.code == "", "asking left it exactly as it was"


def test_an_amendment_keeps_the_readable_lifecycle_code_while_its_code_moves() -> None:
    first = resting(orderid=None, clientorderid="CL-1")
    later = resting(
        unix=20,
        orderid=None,
        clientorderid="CL-2",
        prevclientorderid="CL-1",
    ).with_previous(first)
    assert later.code == first.code == "CL-1"
    assert later.xhash == first.xhash


def test_a_later_order_id_does_not_split_a_client_identified_lifecycle() -> None:
    first = resting(orderid=None, clientorderid="CL-1")
    later = _order(
        unix=20,
        orderid="ORD-1",
        clientorderid="CL-1",
        state=State.OPEN,
    ).with_previous(None)
    assert later.code == "ORD-1", "the isolated parsed row prefers the stronger identifier"
    later.with_previous(first)
    assert later.orderid == "ORD-1", "the stronger exact FIX field is retained"
    assert later.code == first.code == "CL-1", "the readable lifecycle anchor is immutable"
    assert later.xhash == first.xhash and later.version == 1


def test_a_preidentified_order_keeps_its_flat_scope_across_completion() -> None:
    first = resting(orderid="ORD-1", mic=XNAS)
    later = changed(
        _order(
            unix=20,
            orderid="ORD-1",
            state=State.OPEN,
            instrumentxhash=first.instrumentxhash,
            mic=XNAS,
        )
    )
    before = later.xhash

    completed = changed(later, first)

    assert completed.code == "ORD-1"
    assert before != first.xhash, "the isolated row did not yet know its side"
    assert first.xhash == completed.xhash
    assert completed.version == 1


def test_an_order_recovers_its_lifecycle_across_an_execution() -> None:
    first = resting(orderid=None, clientorderid="CL-1")
    done = fill(
        100.0,
        1.0,
        15,
        execid="EX-1",
        orderid="ORD-1",
        clientorderid="CL-1",
    ).with_previous(first)
    after = _order(unix=20, orderid="ORD-1", state=State.OPEN).with_previous(None)

    after.with_previous(done)

    assert done.linkedevents == [(first.unix, first.xhash)]
    assert after.code == first.code == "CL-1"
    assert after.xhash == first.xhash


def test_an_order_recovers_its_root_across_an_amendment_and_execution() -> None:
    first = resting(orderid=None, clientorderid="CL-1")
    amended = _order(
        unix=12,
        clientorderid="CL-2",
        prevclientorderid="CL-1",
        state=State.OPEN,
    ).with_previous(first)
    done = fill(
        100.0,
        1.0,
        15,
        execid="EX-1",
        orderid="ORD-1",
        clientorderid="CL-2",
    ).with_previous(amended)

    after = _order(unix=20, orderid="ORD-1", state=State.OPEN).with_previous(done)

    assert amended.code == first.code == "CL-1"
    assert done.prevclientorderid == "CL-1"
    assert done.linkedevents == [(amended.unix, first.xhash)]
    assert after.code == "CL-1" and after.xhash == first.xhash


def test_order_and_client_identifier_namespaces_never_cross_match() -> None:
    first = resting(orderid="VENUE-1", clientorderid="42")
    other = _order(
        unix=20,
        orderid="42",
        clientorderid="OTHER",
        state=State.OPEN,
    ).with_previous(first)
    assert other.code == "42"
    assert other.xhash != first.xhash and other.version == 0
    assert other.parenthash == [first.hash]


def test_conflicting_order_ids_override_a_reused_client_id() -> None:
    first = resting(orderid="ORD-A", clientorderid="CLIENT")
    other = _order(
        unix=20,
        orderid="ORD-B",
        clientorderid="CLIENT",
        state=State.OPEN,
    ).with_previous(first)

    assert other.code == "ORD-B" and other.xhash != first.xhash
    assert other.version == 0 and other.parenthash == [first.hash]


def test_equal_lifecycle_hashes_reconcile_to_the_existing_code() -> None:
    first = Event(code="A").with_previous(None)
    later = Event(code="B", xhash=first.xhash).completed_from(first)

    assert later.version == 1 and later.xhash == first.xhash
    assert later.code == first.code == "A", "a lifecycle keeps the code it was named by"
    assert Event(code="B", xhash=first.xhash).with_previous(first) is None, (
        "and so a version that differs only in what it calls itself is not one"
    )


def test_the_same_readable_lifecycle_code_is_scoped_before_it_is_hashed() -> None:
    here = _order(instrumentxhash=1, mic=XNAS, code="ORD-1").with_previous(None)
    there = _order(instrumentxhash=2, mic=XNAS, code="ORD-1").with_previous(None)
    elsewhere = _execution(instrumentxhash=1, mic=XNAS, code="ORD-1").with_previous(None)
    assert len({here.xhash, there.xhash, elsewhere.xhash}) == 3


def test_an_event_with_nothing_to_identify_it_inherits_the_lifecycle() -> None:
    """The only one available to it."""
    first = changed(Event(code="A", state=State.NEW))
    nameless = Event(unix=20, state=State.OPEN)
    assert nameless.life_hash() == NIL, "on its own it can identify nothing"
    anonymous = changed(nameless, first)
    assert anonymous.xhash == first.xhash and anonymous.version == 1


def test_a_parent_is_recorded_once_however_often_it_is_completed_from() -> None:
    order = resting()
    done = fill(100.0, 4.0, 20, execid="EX-1").with_previous(order)
    assert done.with_previous(order).parenthash == [order.hash]
