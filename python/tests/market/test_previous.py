"""`with_previous`: what a version carries forward, and what it works out for itself.

A venue restates only what changed, so a row is complete only once the version
before it has been read. The rules below are market rules, not conveniences,
and each test says which one it is pinning.
"""

from __future__ import annotations

import pytest

from rekep.market import (
    MIC,
    AssetKind,
    Book,
    Event,
    Execution,
    Instrument,
    MarketKind,
    Order,
    Side,
    State,
    TimeInForce,
)
from rekep.market.identity import NIL

EQUITY = Instrument(symbol="AAPL", securityexchange="XNAS", kind=AssetKind.EQUITY)
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
        "lastpx": 100.0,
        "lastqty": 10.0,
        "kind": MarketKind.LIMIT_ORDER,
        "timeinforce": TimeInForce.DAY,
        "state": State.NEW,
        "orderid": "ORD-1",
        "clordid": "CL-1",
        "lastmkt": XNAS,
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
    assert first.xhash == Event.xhash_of(first.code)
    assert first.version == 0 and first.prevunix is None and first.prevhash is None


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
    assert later.prevhash == first.hash
    assert later.unix - later.prevunix == 10, "dwell time, without a join"


def test_the_creation_time_is_got_or_set_and_never_recomputed() -> None:
    """A lifecycle is created once; every later version was created then too.
    Recomputing it would make "how old is this order" mean "how long since the
    last message about it"."""
    first = resting(creaunix=5)
    second = changed(_order(unix=20, orderid="ORD-1", state=State.OPEN), first)
    third = changed(_order(unix=30, orderid="ORD-1", state=State.PARTIALLY_FILLED), second)
    assert third.creaunix == 5 and third.unix == 30
    assert first.xhash == second.xhash == third.xhash


def test_a_duplicate_observation_returns_none() -> None:
    """A later timestamp alone does not create another stored version."""
    first = resting()
    duplicate = _order(unix=20, orderid="ORD-1").with_previous(first)
    assert duplicate is None


def test_vhash_drives_change_detection_while_snapshots_remain_explicit() -> None:
    first = resting()
    observed = _order(unix=first.unix + 1, orderid="ORD-1").completed_from(first)
    observed.vhash = observed.hash_of(*observed.version_parts())
    assert observed.vhash == first.vhash, "an ordinary observation time is not stored state"
    observed.reason = "late declared field changed"
    assert observed.hash_of(*observed.version_parts()) != first.vhash

    current = Book(unix=first.unix, instrumentxhash=EQUITY.xhash).identify()
    snapshot = current.make_snapshot(current.unix + 1_000)
    assert snapshot is not None
    later_snapshot = snapshot.with_previous(current)
    assert later_snapshot is not None and later_snapshot.vhash == current.vhash
    assert later_snapshot.hash != current.hash, "a snapshot remains an explicit timed row"

    nonfinite = resting(lastpx=float("nan"))
    assert _order(unix=20, orderid="ORD-1").with_previous(nonfinite) is None


def test_a_message_with_no_clock_lands_where_the_version_before_it_was() -> None:
    first = resting()
    timeless = changed(_order(orderid="ORD-1", state=State.OPEN), first)
    assert timeless.unix == first.unix
    assert timeless.unixpartition == first.unixpartition, "and the partition follows the time"


def test_what_the_message_did_send_always_wins() -> None:
    """This completes a row; it does not correct one."""
    first = resting()
    restated = _order(unix=20, orderid="ORD-1", lastpx=101.0, lastqty=3.0, state=State.OPEN)
    later = changed(restated, first)
    assert later.lastpx == 101.0 and later.lastqty == 3.0


def test_priced_transition_values_are_kept_without_a_self_join() -> None:
    first = resting()
    later = changed(
        _order(unix=20, orderid="ORD-1", lastpx=101.0, lastqty=8.0, state=State.OPEN),
        first,
    )
    assert (later.prevpx, later.prevqty, later.prevnotional) == (
        first.lastpx,
        first.lastqty,
        first.notional,
    )


def test_transition_values_do_not_turn_a_duplicate_observation_into_a_version() -> None:
    first = resting()
    assert _order(unix=20, orderid="ORD-1").with_previous(first) is None


def test_quote_sides_with_one_code_share_a_lifecycle_identity() -> None:
    bid = resting(orderid="QUOTE-1", indicative=True, side=Side.BUY)
    ask = resting(orderid="QUOTE-1", indicative=True, side=Side.SELL)
    assert bid.xhash == ask.xhash


# -- the market slots --------------------------------------------------------


def test_the_price_and_the_instrument_are_carried_because_a_venue_stops_repeating_them() -> None:
    """A row with a null price drops out of every filter on price."""
    first = resting()
    later = changed(_order(unix=20, orderid="ORD-1", state=State.OPEN), first)
    assert later.lastpx == 100.0 and later.lastqty == 10.0
    assert later.side is Side.BUY and later.symbolticker == "XNAS:AAPL"
    assert later.into_instrument() is EQUITY
    assert later.instrumentxhash == first.instrumentxhash, "and it stays in its partition"


def test_the_lifecycle_code_does_not_cross_from_an_order_to_its_fill() -> None:
    first = resting()
    assert first.code == "ORD-1"
    assert first.altids["code"] == first.altids["orderid"] == "ORD-1"
    assert first.altids["clordid"] == "CL-1"
    later = changed(Order(unix=20, orderid="ORD-1", state=State.OPEN), first)
    assert later.altids["code"] == later.altids["orderid"] == "ORD-1"
    assert later.altids["clordid"] == "CL-1"
    fill = changed(_execution(unix=30, execid="EX-1", state=State.FILLED, lastqty=4.0), first)
    assert fill.code == "EX-1", "and never `ORD-1`: an execution is not a version of its order"
    assert fill.altids["code"] == fill.altids["execid"] == "EX-1"
    assert fill.altids["orderid"] == "ORD-1"
    assert fill.linkhashes == [first.hash]
    assert fill.parenthash == [first.hash]


def test_an_identifier_already_recorded_is_never_displaced_by_a_later_one() -> None:
    """First spelling wins, so an `altids` entry is as stable as the lifecycle."""
    order = _order(unix=10, orderid="ORD-1")
    order.name_altid("symbol", "RENAMED")
    order.name_altid("symbol", "SECOND")
    assert order.altids["symbol"] == "RENAMED"
    order.name_altid("isin", "FAKE-ISIN-0001")
    assert order.altids == {
        "orderid": "ORD-1",
        "symbol": "RENAMED",
        "isin": "FAKE-ISIN-0001",
    }


def test_a_notional_is_a_product_of_three_and_absent_without_all_three() -> None:
    """A notional computed with a multiplier of "probably one" is wrong by a factor
    nobody notices until settlement."""
    assert resting().notional == 1000.0, "a cash equity really does trade one for one"
    future = Instrument(symbol="ESZ6", kind=AssetKind.FUTURE)
    assert resting(instrument=future).notional is None
    priced = Instrument(symbol="ESZ6", kind=AssetKind.FUTURE, contractmultiplier=50.0)
    assert resting(instrument=priced, code="ESZ6").notional == 50_000.0


def test_a_notional_the_producer_computed_is_not_recomputed() -> None:
    assert resting(notional=7.0).notional == 7.0


# -- what an order works out for itself --------------------------------------


def test_a_fresh_order_rests_its_whole_quantity() -> None:
    assert resting().lastqty == 10.0


def test_current_and_previous_quantity_describe_the_order_transition() -> None:
    part = _order(unix=20, orderid="ORD-1", lastqty=6.0, state=State.PARTIALLY_FILLED)
    completed = changed(part, resting())
    assert completed.lastqty == 6.0 and completed.prevqty == 10.0


def test_displayed_quantity_is_preserved_when_remaining_quantity_shrinks() -> None:
    first = resting(hiddenqty=4.0)
    part = changed(_order(unix=20, orderid="ORD-1", lastqty=5.0), first)
    assert part.lastqty == 5.0 and part.hiddenqty == 0.0 and part.prevqty == 10.0


@pytest.mark.parametrize(
    "state", [State.FILLED, State.CANCELLED, State.EXPIRED, State.REJECTED, State.DONE_FOR_DAY]
)
def test_a_terminal_order_rests_nothing_at_all(state: State) -> None:
    """It is done, and a book folding it has to take its liquidity out rather than
    leave it standing."""
    done = changed(_order(unix=20, orderid="ORD-1", state=state), resting())
    assert done.lastqty == 0.0 and done.hiddenqty == 0.0 and done.prevqty == 10.0


def test_what_was_asked_for_is_on_the_version_before_and_not_lost() -> None:
    first = resting()
    done = changed(_order(unix=20, orderid="ORD-1", state=State.CANCELLED), first)
    assert done.prevqty == first.lastqty == 10.0


def test_a_fully_filled_order_keeps_its_prior_quantity_on_the_transition() -> None:
    done = changed(_order(unix=20, orderid="ORD-1", state=State.FILLED), resting())
    assert done.lastqty == 0.0 and done.prevqty == 10.0


def test_a_terminal_order_without_history_preserves_its_source_quantity() -> None:
    done = _order(unix=20, orderid="ORD-1", lastqty=100.0, state=State.CANCELLED)
    done.with_previous(None)
    assert done.lastqty == 0.0 and done.prevqty == 100.0


def test_a_cancelled_order_does_not_store_a_cumulative_fill_quantity() -> None:
    done = changed(_order(unix=20, orderid="ORD-1", state=State.CANCELLED), resting())
    assert "cumqty" not in done.into_field().names


def test_the_identifier_that_was_replaced_is_the_one_the_version_before_had() -> None:
    """FIX requires a new `ClOrdID <11>` per version and calls the old one
    `OrigClOrdID <41>`; when this version did not say, the version before is it."""
    replaced = changed(_order(unix=20, clordid="CL-2", state=State.OPEN), resting())
    assert replaced.origclordid == "CL-1"


def test_a_version_that_reuses_the_identifier_names_nothing_replaced() -> None:
    same = changed(_order(unix=20, clordid="CL-1", state=State.OPEN), resting())
    assert same.origclordid is None


def test_the_order_kind_and_time_in_force_are_carried() -> None:
    later = changed(_order(unix=20, orderid="ORD-1", state=State.OPEN), resting())
    assert later.kind is MarketKind.LIMIT_ORDER and later.timeinforce is TimeInForce.DAY


# -- what an execution works out ---------------------------------------------


def fill(px: float, qty: float, unix: int, **given: object) -> Execution:
    instrument = given.pop("instrument", EQUITY)
    assert isinstance(instrument, Instrument)
    declared = {"unix": unix, "state": State.FILLED, "lastpx": px, "lastqty": qty}
    return _execution(instrument, **{**declared, **given})


def test_a_fill_links_itself_to_the_order_it_followed() -> None:
    first = resting()
    done = fill(100.0, 4.0, 20, execid="EX-1").with_previous(first)
    assert done.linkhashes == [first.hash]


def test_the_matched_order_precedes_other_event_links() -> None:
    first = resting()
    done = fill(100.0, 4.0, 20, execid="EX-1", linkhashes=[-1]).with_previous(first)
    assert done is not None
    assert done.linkhashes == [first.hash, -1]


def test_what_is_done_accumulates_across_fills() -> None:
    """A venue that sends `LastQty <32>` and not `CumQty <14>` has still said how
    much is now done."""
    one = fill(100.0, 4.0, 20, execid="EX-1").with_previous(resting())
    two = fill(101.0, 6.0, 30, execid="EX-2").with_previous(one)
    assert (one.cumqty, two.cumqty) == (4.0, 10.0)
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
        lastpx=100.0,
        lastqty=1.0,
        state=State.FILLED,
        execid="EX-1",
        cumqty=None,
        leavesqty=9.0,
        vwap=100.0,
    ).with_previous(None)
    later = fill(110.0, 1.0, 30, execid="EX-2").with_previous(previous)

    assert later.cumqty is None and later.vwap is None
    assert later.leavesqty == 8.0


def test_a_fill_without_a_price_cannot_update_an_existing_average() -> None:
    previous = _execution(
        unix=20,
        lastqty=1.0,
        state=State.FILLED,
        execid="EX-1",
        cumqty=1.0,
        leavesqty=9.0,
        vwap=100.0,
    ).with_previous(None)
    later = _execution(unix=30, lastqty=1.0, state=State.FILLED, execid="EX-2").with_previous(
        previous
    )
    assert later.cumqty == 2.0 and later.vwap is None


def test_an_acknowledgement_changes_no_running_total() -> None:
    """Adding its quantity is how a fills table starts overcounting."""
    one = fill(100.0, 4.0, 20, execid="EX-1").with_previous(resting())
    acked = _execution(unix=30, state=State.NEW, lastqty=99.0, execid="EX-3").with_previous(one)
    assert acked.cumqty == 4.0 and acked.leavesqty == 6.0
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

    assert (fixed.cumqty, fixed.leavesqty) == (4.0, 6.0)
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

    assert (cancelled.cumqty, cancelled.leavesqty) == (4.0, 6.0)
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
        lastpx=None,
        lastqty=2.0,
        state=State.FILLED,
        execid="EX-1",
        cumqty=6.0,
        leavesqty=4.0,
        vwap=100.0,
    ).with_previous(None)
    amended = _execution(
        unix=30,
        lastpx=px,
        lastqty=qty,
        state=state,
        execid="EX-2",
        execrefid="EX-1",
    ).with_previous(previous)

    assert amended.vwap is None


def test_a_preidentified_execution_keeps_its_lifecycle_and_records_its_order() -> None:
    order = resting()
    done = fill(100.0, 1.0, 20, execid="EX-1").with_previous(None)
    before = done.xhash

    done.with_previous(order)

    assert done.xhash == before == Event.xhash_of("EX-1")
    assert done.linkhashes == [order.hash]
    assert done.version == 0 and done.parenthash == [order.hash]


def test_a_preidentified_correction_rejoins_its_referenced_execution() -> None:
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
    assert after.lastqty == 6.0
    assert after.vwap == 100.0


def test_the_abstract_slots_do_not_cross_between_shapes() -> None:
    """`lastpx` and `lastqty` mean what the subclass says: an order's are what it
    asked for, a fill's are what traded. Carrying one into the other made a partly
    filled order claim it had asked for exactly what had just traded."""
    done = fill(100.0, 4.0, 20, execid="EX-1").with_previous(resting())
    after = _order(unix=25, orderid="ORD-1", state=State.PARTIALLY_FILLED).with_previous(done)
    assert after.lastqty == 6.0 and after.lastpx is None


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
    crossed = _execution(unix=30, execid="EX-2", lastpx=1.0, lastqty=1.0).with_previous(order)
    assert crossed.state is State.NEW


def test_a_code_is_carried_between_versions_of_the_same_shape() -> None:
    """Which is the whole point of carrying it -- a venue stops repeating it."""
    order = resting()
    later = _order(unix=20, orderid="ORD-1", state=State.OPEN).with_previous(order)
    assert later.kind is MarketKind.LIMIT_ORDER and later.timeinforce is TimeInForce.DAY
    one = fill(100.0, 4.0, 20, execid="EX-1").with_previous(order)
    two = fill(100.5, 1.0, 30, execid="EX-1").with_previous(one)
    assert two.state is State.FILLED


def test_an_amendment_keeps_the_readable_lifecycle_code_while_its_code_moves() -> None:
    first = resting(orderid=None, clordid="CL-1")
    later = resting(
        unix=20,
        orderid=None,
        clordid="CL-2",
        origclordid="CL-1",
    ).with_previous(first)
    assert later.code == first.code == "CL-1"
    assert later.altids["code"] == "CL-1"
    assert later.altids["origclordid"] == "CL-1" and later.altids["clordid"] == "CL-2"
    assert later.xhash == first.xhash


def test_a_later_order_id_does_not_split_a_client_identified_lifecycle() -> None:
    first = resting(orderid=None, clordid="CL-1")
    later = _order(
        unix=20,
        orderid="ORD-1",
        clordid="CL-1",
        state=State.OPEN,
    ).with_previous(None)
    assert later.code == "ORD-1", "the isolated parsed row prefers the stronger identifier"
    later.with_previous(first)
    assert later.orderid == "ORD-1", "the stronger exact FIX field is retained"
    assert later.code == first.code == "CL-1", "the readable lifecycle anchor is immutable"
    assert later.xhash == first.xhash and later.version == 1


def test_a_preidentified_order_keeps_its_code_identity_across_completion() -> None:
    first = resting(orderid="ORD-1", lastmkt=XNAS)
    later = changed(
        _order(
            unix=20,
            orderid="ORD-1",
            state=State.OPEN,
            instrumentxhash=first.instrumentxhash,
            lastmkt=XNAS,
        )
    )
    before = later.xhash

    completed = changed(later, first)

    assert completed.code == "ORD-1"
    assert before == first.xhash == completed.xhash
    assert completed.version == 1


def test_an_order_recovers_its_lifecycle_across_an_execution() -> None:
    first = resting(orderid=None, clordid="CL-1")
    done = fill(
        100.0,
        1.0,
        15,
        execid="EX-1",
        orderid="ORD-1",
        clordid="CL-1",
    ).with_previous(first)
    assert done is not None
    after = _order(unix=20, orderid="ORD-1", state=State.OPEN).with_previous(None)
    assert after is not None

    after.with_previous(done)

    assert done.linkhashes == [first.hash]
    assert after.code == first.code == "CL-1"
    assert after.altids["code"] == "CL-1"
    assert after.xhash == first.xhash


def test_an_execution_link_does_not_override_an_unrelated_order_code() -> None:
    first = resting(orderid=None, clordid="CL-1")
    done = fill(
        100.0,
        1.0,
        15,
        execid="EX-1",
        orderid="ORD-1",
        clordid="CL-1",
    ).with_previous(first)
    assert done is not None

    other = _order(unix=20, orderid="ORD-2", state=State.OPEN).with_previous(done)

    assert other is not None and other.code == "ORD-2"
    assert other.xhash != first.xhash


def test_an_order_recovers_its_root_across_an_amendment_and_execution() -> None:
    first = resting(orderid=None, clordid="CL-1")
    amended = _order(
        unix=12,
        clordid="CL-2",
        origclordid="CL-1",
        state=State.OPEN,
    ).with_previous(first)
    assert amended is not None
    done = fill(
        100.0,
        1.0,
        15,
        execid="EX-1",
        orderid="ORD-1",
        clordid="CL-2",
    ).with_previous(amended)
    assert done is not None

    after = _order(unix=20, orderid="ORD-1", state=State.OPEN).with_previous(done)
    assert after is not None

    assert amended.code == first.code == "CL-1"
    assert done.origclordid == "CL-1"
    assert done.linkhashes == [amended.hash]
    assert after.code == "CL-1" and after.xhash == first.xhash
    assert after.altids["code"] == "CL-1"


def test_order_and_client_identifier_namespaces_never_cross_match() -> None:
    first = resting(orderid="VENUE-1", clordid="42")
    other = _order(
        unix=20,
        orderid="42",
        clordid="OTHER",
        state=State.OPEN,
    ).with_previous(first)
    assert other.code == "42"
    assert other.xhash != first.xhash and other.version == 0
    assert other.parenthash == [first.hash]


def test_conflicting_order_ids_override_a_reused_client_id() -> None:
    first = resting(orderid="ORD-A", clordid="CLIENT")
    other = _order(
        unix=20,
        orderid="ORD-B",
        clordid="CLIENT",
        state=State.OPEN,
    ).with_previous(first)

    assert other.code == "ORD-B" and other.xhash != first.xhash
    assert other.version == 0 and other.parenthash == [first.hash]


def test_explicit_amendments_keep_one_lifecycle_while_both_ids_move() -> None:
    first = resting(orderid="ORD-A", clordid="CL-A")
    second = changed(
        _order(
            unix=20,
            orderid="ORD-B",
            clordid="CL-B",
            origclordid="CL-A",
            state=State.OPEN,
        ),
        first,
    )
    third = changed(
        _order(
            unix=30,
            orderid="ORD-C",
            clordid="CL-C",
            origclordid="CL-B",
            state=State.OPEN,
        ),
        second,
    )

    assert [event.orderid for event in (first, second, third)] == ["ORD-A", "ORD-B", "ORD-C"]
    assert [event.clordid for event in (first, second, third)] == ["CL-A", "CL-B", "CL-C"]
    assert first.code == second.code == third.code == "ORD-A"
    assert first.xhash == second.xhash == third.xhash
    assert [event.version for event in (first, second, third)] == [0, 1, 2]


def test_a_stable_root_id_reconciles_changed_venue_and_client_ids() -> None:
    first = resting(
        orderid="ORD-A",
        clordid="CL-A",
        altids={"rootorderid": "ROOT"},
    )
    moved = changed(
        _order(
            unix=20,
            orderid="ORD-B",
            clordid="CL-B",
            altids={"rootorderid": "ROOT"},
            state=State.OPEN,
        ),
        first,
    )

    assert moved.orderid == "ORD-B" and moved.clordid == "CL-B"
    assert moved.code == first.code == "ORD-A"
    assert moved.xhash == first.xhash


def test_equal_lifecycle_hashes_reconcile_to_the_existing_code() -> None:
    first = Event(code="A").with_previous(None)
    later = Event(code="B", xhash=first.xhash).completed_from(first)

    assert later.version == 1 and later.xhash == first.xhash
    assert later.code == first.code == "A", "a lifecycle keeps the code it was named by"
    assert Event(code="B", xhash=first.xhash).with_previous(first) is None, (
        "and so a version that differs only in what it calls itself is not one"
    )


def test_the_same_code_ignores_shape_instrument_and_venue() -> None:
    here = _order(instrumentxhash=1, lastmkt=XNAS, code="ORD-1").with_previous(None)
    there = _order(instrumentxhash=2, lastmkt=XNAS, code="ORD-1").with_previous(None)
    elsewhere = _execution(instrumentxhash=1, lastmkt=XNAS, code="ORD-1").with_previous(None)
    assert here.xhash == there.xhash == elsewhere.xhash


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
