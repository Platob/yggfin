"""Execution reports update normalized order state once and keep fill evidence."""

from __future__ import annotations

import pyarrow
import pytest

from rekep.market import Book, Event, Execution, FixEvents, Order, Side, State


def report(
    second: int,
    status: str | None,
    exec_type: str,
    *,
    order_qty: float | None = None,
    last_qty: float | None = None,
    cum_qty: float | None = None,
    leaves_qty: float | None = None,
    order_id: str = "ORDER-1",
    client_order_id: str = "CLIENT-1",
    prev_client_order_id: str | None = None,
    side: str | None = "1",
    price: float = 100.0,
    max_floor: float | None = None,
    cxl_qty: float | None = None,
) -> list[Order | Execution]:
    pairs: list[tuple[str, object]] = [
        ("MsgType", "8"),
        ("Symbol", "BTC-USD"),
        ("SecurityExchange", "XCME"),
        ("OrderID", order_id),
        ("ClOrdID", client_order_id),
        ("OrdType", "2"),
        ("Price", price),
        ("ExecType", exec_type),
        ("ExecID", f"EXEC-{second}"),
        ("TransactTime", f"20260821-10:00:{second:02d}"),
    ]
    if status is not None:
        pairs.append(("OrdStatus", status))
    if side is not None:
        pairs.append(("Side", side))
    if prev_client_order_id is not None:
        pairs.append(("OrigClOrdID", prev_client_order_id))
    for name, value in (
        ("OrderQty", order_qty),
        ("LastPx", 100.0 if last_qty is not None else None),
        ("LastQty", last_qty),
        ("CumQty", cum_qty),
        ("LeavesQty", leaves_qty),
        ("MaxFloor", max_floor),
        ("CxlQty", cxl_qty),
    ):
        if value is not None:
            pairs.append((name, value))
    return list(FixEvents.from_pairs(pairs, fix_version="4.4"))


def folded(*reports: list[Order | Execution]) -> list[Book]:
    events = [event for report_events in reports for event in report_events]
    return list(Book.from_events(events, snapshot_every=0))


def accepted() -> list[Order | Execution]:
    return report(0, "0", "0", order_qty=100.0, cum_qty=0.0, leaves_qty=100.0)


def test_partial_report_applies_authoritative_leaves_once() -> None:
    latest = folded(
        accepted(),
        report(1, "1", "F", order_qty=100.0, last_qty=30.0, cum_qty=30.0, leaves_qty=70.0),
    )[-1]

    (order,) = latest.deltas
    (execution,) = latest.executions
    assert (order.prev_qty, order.qty, order.state) == (100.0, 70.0, State.PARTIALLY_FILLED)
    assert execution.qty == 30.0 and latest.bid_qty == 70.0
    assert [(level.px, level.qty) for level in latest.bid_levels] == [(100.0, 70.0)]


def test_partial_report_without_leaves_uses_previous_remaining_minus_last() -> None:
    latest = folded(accepted(), report(1, "1", "F", last_qty=30.0))[-1]

    (order,) = latest.deltas
    assert (order.prev_qty, order.qty) == (100.0, 70.0)
    assert latest.bid_qty == 70.0 and latest.bid_depth == 1


def test_linked_parentless_pair_does_not_reduce_the_resulting_order_twice() -> None:
    placed = accepted()
    partial = report(1, "1", "F", last_qty=30.0, leaves_qty=70.0)
    resulting, execution = partial
    execution.parent_hash = []
    execution.linked_events = [(placed[0].unix, placed[0].xhash)]

    latest = folded(placed, [resulting, execution])[-1]

    assert latest.bid_qty == 70.0
    assert latest.deltas[0].qty == 70.0
    assert [(level.px, level.qty) for level in latest.bid_levels] == [(100.0, 70.0)]
    assert [one.xhash for one in latest.executions] == [execution.xhash]


def test_multiple_partial_reports_reduce_the_live_order_once_each() -> None:
    books = folded(
        accepted(),
        report(1, "1", "F", last_qty=30.0),
        report(2, "1", "F", last_qty=20.0),
    )

    assert [book.bid_qty for book in books] == [100.0, 70.0, 50.0]
    assert [(book.deltas[0].prev_qty, book.deltas[0].qty) for book in books[1:]] == [
        (100.0, 70.0),
        (70.0, 50.0),
    ]


def test_full_fill_is_terminal_zero_and_does_not_consume_an_unrelated_order() -> None:
    (other,) = report(
        0,
        "0",
        "0",
        order_qty=50.0,
        leaves_qty=50.0,
        order_id="ORDER-2",
        client_order_id="CLIENT-2",
    )
    filled = report(1, "2", "F", last_qty=100.0, leaves_qty=0.0)

    latest = list(Book.from_events([*accepted(), other, *filled], snapshot_every=0))[-1]

    filled_order = next(order for order in latest.deltas if order.order_id == "ORDER-1")
    assert filled_order.state is State.FILLED and filled_order.qty == 0.0
    assert latest.bid_qty == 50.0 and latest.bid_depth == 1


def test_cancel_without_a_prior_new_keeps_source_quantity_as_previous() -> None:
    (order,) = report(1, "4", "4", order_qty=100.0)
    (book,) = folded([order])

    assert (order.prev_qty, order.qty, order.state) == (100.0, 0.0, State.CANCELLED)
    assert book.deltas == [order] and book.bid_depth == 0


def test_cancel_uses_requested_minus_already_filled_as_previous_quantity() -> None:
    (order,) = report(
        1,
        "4",
        "4",
        order_qty=100.0,
        cum_qty=30.0,
        leaves_qty=0.0,
    )
    assert (order.prev_qty, order.qty, order.state) == (70.0, 0.0, State.CANCELLED)


def test_first_observed_fill_without_last_preserves_requested_quantity() -> None:
    order, execution = report(
        1,
        "2",
        "F",
        order_qty=100.0,
        cum_qty=100.0,
        leaves_qty=0.0,
    )
    assert (order.prev_qty, order.qty, order.state) == (100.0, 0.0, State.FILLED)
    assert execution.filled_qty == 100.0 and execution.leaves_qty == 0.0


def test_partial_execution_without_a_prior_new_still_yields_both_rows() -> None:
    (book,) = folded(report(1, None, "F", last_qty=30.0, cum_qty=30.0, leaves_qty=70.0))

    assert len(book.deltas) == len(book.executions) == 1
    assert (book.deltas[0].prev_qty, book.deltas[0].qty, book.bid_qty) == (100.0, 70.0, 70.0)


def test_exec_type_supplies_new_state_when_ord_status_is_absent() -> None:
    (order,) = report(1, None, "0", order_qty=100.0)
    (book,) = folded([order])

    assert (order.state, order.qty) == (State.NEW, 100.0)
    assert book.bid_qty == 100.0 and book.bid_depth == 1


@pytest.mark.parametrize(
    ("exec_type", "state"),
    [("4", State.CANCELLED), ("C", State.EXPIRED), ("8", State.REJECTED)],
)
def test_terminal_exec_type_without_ord_status_never_rests(exec_type: str, state: State) -> None:
    (order,) = report(1, None, exec_type, order_qty=100.0)
    (book,) = folded([order])

    assert (order.state, order.prev_qty, order.qty) == (state, 100.0, 0.0)
    assert book.bid_depth == 0 and book.bid_alive == []


def test_full_last_qty_without_status_or_totals_upgrades_to_filled() -> None:
    latest = folded(accepted(), report(1, None, "F", last_qty=100.0))[-1]

    assert (latest.deltas[0].state, latest.deltas[0].qty) == (State.FILLED, 0.0)
    assert latest.bid_depth == 0


def test_replacement_confirmation_updates_the_live_order() -> None:
    replacement = report(
        1,
        "5",
        "5",
        order_qty=120.0,
        leaves_qty=120.0,
        client_order_id="CLIENT-2",
        prev_client_order_id="CLIENT-1",
        price=101.0,
    )

    latest = folded(accepted(), replacement)[-1]

    assert (latest.deltas[0].state, latest.deltas[0].qty) == (State.NEW, 120.0)
    assert (latest.bid_px, latest.bid_qty, latest.bid_depth) == (101.0, 120.0, 1)


def test_pending_replacement_does_not_move_acknowledged_interest() -> None:
    pending = list(
        FixEvents.from_pairs(
            [
                ("MsgType", "G"),
                ("Symbol", "BTC-USD"),
                ("SecurityExchange", "XCME"),
                ("OrderID", "ORDER-1"),
                ("ClOrdID", "CLIENT-2"),
                ("OrigClOrdID", "CLIENT-1"),
                ("Side", "1"),
                ("OrdType", "2"),
                ("Price", 101.0),
                ("OrderQty", 120.0),
                ("TransactTime", "20260821-10:00:01"),
            ],
            fix_version="4.4",
        )
    )

    latest = folded(accepted(), pending)[-1]

    assert (latest.deltas[0].state, latest.deltas[0].px, latest.deltas[0].qty) == (
        State.PENDING_REPLACE,
        101.0,
        120.0,
    )
    assert (latest.bid_px, latest.bid_qty, latest.bid_levels) == (100.0, 100.0, [])


def test_partial_fill_preserves_displayed_floor_not_stale_hidden_quantity() -> None:
    latest = folded(
        report(0, "0", "0", order_qty=100.0, leaves_qty=100.0, max_floor=20.0),
        report(1, "1", "F", last_qty=30.0, leaves_qty=70.0),
    )[-1]

    assert (latest.deltas[0].qty, latest.deltas[0].hidden_qty) == (70.0, 50.0)
    assert latest.bid_qty == 20.0 and latest.bid_depth == 1


def test_cancellation_quantity_is_preserved_when_it_is_the_only_source() -> None:
    (order,) = report(1, "4", "4", cxl_qty=100.0)

    assert (order.prev_qty, order.qty, order.state) == (100.0, 0.0, State.CANCELLED)


def test_paired_execution_is_completed_from_the_published_order() -> None:
    raw_order, raw_execution = report(
        1,
        "1",
        "F",
        last_qty=30.0,
        leaves_qty=70.0,
        side=None,
    )
    raw_link = (raw_order.unix, raw_order.xhash)

    latest = folded(accepted(), [raw_order, raw_execution])[-1]

    execution = latest.executions[0]
    assert execution.side is Side.BID
    assert execution.primary_linked_event == (
        latest.deltas[0].unix,
        latest.deltas[0].xhash,
    )
    assert raw_link not in execution.linked_events


def test_book_collection_defaults_are_non_null_and_independent() -> None:
    first, second = Book(), Book()
    names = ("deltas", "executions", "bid_levels", "ask_levels", "bid_alive", "ask_alive")
    assert all(getattr(first, name) == [] for name in names)
    assert all(getattr(first, name) is not getattr(second, name) for name in names)
    assert first.bid_depth == first.ask_depth == 0


def test_explicit_null_book_collections_and_depths_normalize_once() -> None:
    book = Book(
        deltas=None,  # type: ignore[arg-type]
        executions=None,  # type: ignore[arg-type]
        bid_levels=None,  # type: ignore[arg-type]
        ask_levels=None,  # type: ignore[arg-type]
        bid_alive=None,  # type: ignore[arg-type]
        ask_alive=None,  # type: ignore[arg-type]
        bid_depth=None,  # type: ignore[arg-type]
        ask_depth=None,  # type: ignore[arg-type]
    )

    assert all(
        getattr(book, name) == []
        for name in ("deltas", "executions", "bid_levels", "ask_levels", "bid_alive", "ask_alive")
    )
    assert book.bid_depth == book.ask_depth == 0


def test_linked_events_preserve_order_deduplicate_and_round_trip_through_arrow() -> None:
    first = Event(unix=1, xhash=11)
    related = Event(unix=2, xhash=22)
    event = Event(unix=3, xhash=33).link_to(first, related, first)
    event.link_to(Event(unix=4, xhash=33))
    assert event.linked_events == [(1, 11), (2, 22)]

    schema = Event.into_field().into_arrow_schema()
    stored = pyarrow.Table.from_pylist([event.into_dict()], schema=schema).to_pylist()[0]
    assert stored["linked_events"] == [{"unix": 1, "xhash": 11}, {"unix": 2, "xhash": 22}]
    assert Event.from_dict(stored).linked_events == [(1, 11), (2, 22)]


def test_linked_events_normalize_null_duplicates_and_self_links() -> None:
    assert Event.from_dict({"linked_events": None}).linked_events == []
    event = Event(
        unix=3,
        xhash=33,
        linked_events=[(1, 11), (1, 11), (2, 33)],
    )
    assert event.linked_events == [(1, 11)]
