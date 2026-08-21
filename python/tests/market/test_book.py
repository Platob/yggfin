"""The derived columns, checked against arithmetic done by hand and in Python.

`summarise_arrow` is kernels; the reference it is compared against is a plain
loop over the same rows. Comparing it against itself -- re-deriving with the
same kernels and asserting they match -- would pass on every bug it has.
"""

from __future__ import annotations

import math

import pyarrow
import pytest

from rekep.market import (
    Book,
    BookSide,
    ExecKind,
    Execution,
    Order,
    OrderKind,
    Side,
    State,
    UpdateAction,
)
from rekep.market.book import Level

from .conftest import batch, identifier

#: The rows a book actually produces: two levels, none at all, an increment
#: with no snapshot, and one level. Each is a branch of the walk.
ALIVE = [
    [{"px": 10.0, "qty": 5.0, "orders": 2}, {"px": 9.5, "qty": 7.0, "orders": 1}],
    [],
    None,
    [{"px": 2.0, "qty": 3.0, "orders": None}],
]


def book(rows: int = 1, **columns: object) -> pyarrow.RecordBatch:
    """A batch of books, the two sides given as flat `bid_*` / `ask_*` columns."""
    return batch(Book, rows, **columns)


def prices(name: str, values: list[tuple[float | None, float | None]]) -> dict[str, list]:
    """One side's best price and size as the two flat columns that hold them."""
    return {f"{name}_px": [px for px, _ in values], f"{name}_qty": [qty for _, qty in values]}


def test_the_best_level_is_the_first_one_and_the_depth_is_how_many_there_are() -> None:
    out = BookSide.summarise_arrow_batch(batch(BookSide, 4, alive=ALIVE))
    assert out.column("px").to_pylist() == [10.0, None, None, 2.0]
    assert out.column("qty").to_pylist() == [5.0, None, None, 3.0]
    assert out.column("depth").to_pylist() == [2, 0, None, 1]


def test_the_totals_match_a_plain_python_sum() -> None:
    """The reference: the same numbers added up without a kernel in sight."""
    out = BookSide.summarise_arrow_batch(batch(BookSide, 4, alive=ALIVE))
    expected = [
        None if levels is None else sum(level["qty"] for level in levels) for levels in ALIVE
    ]
    assert out.column("total_qty").to_pylist() == expected


def test_an_empty_side_holds_nothing_rather_than_holding_an_unknown() -> None:
    """Zero is a quantity; null is a question. An empty book side is the first."""
    out = BookSide.summarise_arrow_batch(batch(BookSide, 4, alive=ALIVE))
    assert out.column("total_qty")[1].as_py() == 0.0
    assert out.column("depth")[1].as_py() == 0
    assert out.column("px")[1].as_py() is None, "and there is still no best price"


def test_a_row_with_no_snapshot_is_left_exactly_as_it_was_found() -> None:
    """An increment carries no levels, so deriving from them would erase the row."""
    given = batch(BookSide, 4, alive=ALIVE, px=[1.0] * 4, qty=[2.0] * 4, total_qty=[99.0] * 4)
    out = BookSide.summarise_arrow_batch(given)
    assert out.column("px")[2].as_py() == 1.0
    assert out.column("qty")[2].as_py() == 2.0
    assert out.column("total_qty")[2].as_py() == 99.0


def test_a_side_of_a_thousand_levels_still_sums_exactly() -> None:
    """A prefix-difference would lose the low bits here; a grouped sum does not."""
    levels = [{"px": 100.0 + index, "qty": 0.1, "orders": 1} for index in range(1000)]
    out = BookSide.summarise_arrow_batch(batch(BookSide, 1, alive=[levels]))
    assert out.column("total_qty")[0].as_py() == pytest.approx(100.0, abs=1e-9)
    assert out.column("depth")[0].as_py() == 1000


def test_no_rows_is_no_rows() -> None:
    assert BookSide.summarise_arrow_batch(batch(BookSide, 0)).num_rows == 0
    assert Book.summarise_arrow_batch(batch(Book, 0)).num_rows == 0


def test_the_book_prices_match_the_formulas_in_the_docstring() -> None:
    given = book(1, **prices("bid", [(10.0, 100.0)]), **prices("ask", [(10.2, 300.0)]))
    out = Book.summarise_arrow_batch(given)
    assert out.column("px")[0].as_py() == pytest.approx((10.0 + 10.2) / 2)
    assert out.column("qty")[0].as_py() == pytest.approx(400.0)
    assert out.column("spread")[0].as_py() == pytest.approx(10.2 - 10.0)
    assert out.column("micro_px")[0].as_py() == pytest.approx((10.0 * 300.0 + 10.2 * 100.0) / 400.0)
    assert out.column("imbalance")[0].as_py() == pytest.approx((100.0 - 300.0) / 400.0)


def test_the_flat_pair_reconstructs_the_best_bid_and_offer_exactly() -> None:
    """Which is why neither is duplicated as a column of its own."""
    out = Book.summarise_arrow_batch(
        book(1, **prices("bid", [(10.0, 100.0)]), **prices("ask", [(10.2, 300.0)]))
    )
    mid, spread = out.column("px")[0].as_py(), out.column("spread")[0].as_py()
    assert mid - spread / 2 == pytest.approx(10.0)
    assert mid + spread / 2 == pytest.approx(10.2)


def test_a_crossed_book_shows_as_a_negative_spread_and_a_locked_one_as_zero() -> None:
    """The range predicate that replaces two boolean flags."""
    out = Book.summarise_arrow_batch(
        book(
            2,
            **prices("bid", [(11.0, 50.0), (10.0, 50.0)]),
            **prices("ask", [(10.5, 50.0), (10.0, 50.0)]),
        )
    )
    assert out.column("spread")[0].as_py() < 0, "crossed"
    assert out.column("spread")[1].as_py() == 0.0, "locked"


def test_a_one_sided_book_has_no_mid_rather_than_half_of_one() -> None:
    out = Book.summarise_arrow_batch(
        book(1, **prices("bid", [(9.0, 10.0)]), **prices("ask", [(None, None)]))
    )
    for name in ("px", "spread", "micro_px", "imbalance"):
        assert out.column(name)[0].as_py() is None, name


def test_an_empty_book_gives_a_null_rather_than_an_infinity() -> None:
    """Dividing by a size of zero is the one place a kernel would return a number."""
    out = Book.summarise_arrow_batch(
        book(1, **prices("bid", [(5.0, 0.0)]), **prices("ask", [(5.5, 0.0)]))
    )
    micro, imbalance = out.column("micro_px")[0].as_py(), out.column("imbalance")[0].as_py()
    assert micro is None and imbalance is None
    assert not any(value is not None and math.isinf(value) for value in (micro, imbalance))


def test_the_imbalance_stays_inside_minus_one_and_one() -> None:
    rows = [
        ({"px": 1.0, "qty": 1.0}, {"px": 2.0, "qty": 1_000_000.0}),
        ({"px": 1.0, "qty": 1_000_000.0}, {"px": 2.0, "qty": 1.0}),
    ]
    out = Book.summarise_arrow_batch(
        book(
            2,
            **prices("bid", [(b["px"], b["qty"]) for b, _ in rows]),
            **prices("ask", [(a["px"], a["qty"]) for _, a in rows]),
        )
    )
    for value in out.column("imbalance").to_pylist():
        assert -1.0 <= value <= 1.0


@pytest.mark.parametrize("shape", (BookSide, Book), ids=lambda cls: cls.__name__)
def test_a_table_summarises_to_the_same_thing_as_its_batches(shape: type) -> None:
    given = batch(shape, 4, alive=ALIVE) if shape is BookSide else batch(shape, 4)
    table = pyarrow.Table.from_batches([given])
    assert shape.summarise_arrow_table(table).equals(
        pyarrow.Table.from_batches([shape.summarise_arrow_batch(given)])
    )


@pytest.mark.parametrize("shape", (BookSide, Book), ids=lambda cls: cls.__name__)
def test_the_generic_form_infers_what_it_was_handed(shape: type) -> None:
    given = batch(shape, 2)
    assert shape.summarise_arrow(given).num_rows == 2
    assert shape.summarise_arrow(pyarrow.Table.from_batches([given])).num_rows == 2


@pytest.mark.parametrize("shape", (BookSide, Book), ids=lambda cls: cls.__name__)
def test_summarising_keeps_the_declared_types_and_comments(shape: type) -> None:
    given = batch(shape, 2)
    assert shape.summarise_arrow_batch(given).schema == given.schema


def test_a_side_is_a_side_of_a_book_and_a_bid_is_a_buy() -> None:
    out = BookSide.summarise_arrow_batch(batch(BookSide, 1, alive=ALIVE[:1], side=[int(Side.BID)]))
    assert Side(out.column("side")[0].as_py()) is Side.BUY


def test_a_level_declares_the_two_fix_fields_a_book_entry_is_made_of() -> None:
    assert Level.FIELD.field("px").fix["tag"] == "270"
    assert Level.FIELD.field("qty").fix["tag"] == "271"


def test_a_book_derives_each_side_from_its_own_levels_before_pricing_across_them() -> None:
    """The half a first version left out, and the benchmark found.

    A book assembled from two feeds carries levels and nothing derived. If
    `summarise` read `bid_px` before deriving it, every price here would come
    back null -- which is what happened, and what no test at the time noticed.
    """
    bids = [{"px": 10.0, "qty": 100.0, "orders": 1}, {"px": 9.0, "qty": 20.0, "orders": 1}]
    asks = [{"px": 10.2, "qty": 300.0, "orders": 2}]
    given = book(1, bid_alive=[bids], ask_alive=[asks])
    assert given.column("bid_px")[0].as_py() is None, "nothing is derived going in"

    out = Book.summarise_arrow_batch(given)
    assert out.column("bid_px")[0].as_py() == 10.0
    assert out.column("bid_total_qty")[0].as_py() == 120.0
    assert out.column("bid_depth")[0].as_py() == 2
    assert out.column("ask_px")[0].as_py() == 10.2
    assert out.column("ask_depth")[0].as_py() == 1
    assert out.column("px")[0].as_py() == pytest.approx(10.1), "and the mid across them"
    assert out.column("spread")[0].as_py() == pytest.approx(0.2)


def test_a_book_derives_a_side_exactly_as_a_book_side_does() -> None:
    """One walk over the levels, whichever shape asked for it."""
    levels = [{"px": 10.0, "qty": 100.0, "orders": 1}, {"px": 9.5, "qty": 7.0, "orders": None}]
    apart = BookSide.summarise_arrow_batch(batch(BookSide, 1, alive=[levels]))
    together = Book.summarise_arrow_batch(book(1, bid_alive=[levels]))
    for theirs, ours in (("px", "bid_px"), ("qty", "bid_qty"), ("depth", "bid_depth")):
        assert apart.column(theirs)[0].as_py() == together.column(ours)[0].as_py(), theirs
    assert apart.column("total_qty")[0].as_py() == together.column("bid_total_qty")[0].as_py()


def test_summarising_a_book_twice_changes_nothing_the_second_time() -> None:
    """Idempotent, so a producer and a consumer running it both is not a bug."""
    given = book(
        1,
        bid_alive=[[{"px": 1.0, "qty": 2.0, "orders": 1}]],
        ask_alive=[[{"px": 1.5, "qty": 4.0, "orders": 1}]],
    )
    once = Book.summarise_arrow_batch(given)
    assert Book.summarise_arrow_batch(once).equals(once)


# -- building a book out of events ------------------------------------------


def test_an_order_adds_its_quantity_to_the_level_it_rests_at() -> None:
    side = BookSide(side=Side.BID, symbol="AAPL")
    side.append_order(Order(side=Side.BUY, px=10.0, qty=100.0, state=State.NEW))
    side.append_order(Order(side=Side.BUY, px=9.5, qty=50.0, state=State.NEW))
    side.append_order(Order(side=Side.BUY, px=10.0, qty=20.0, state=State.NEW))
    assert [(level.px, level.qty) for level in side.alive] == [(10.0, 120.0), (9.5, 50.0)]
    assert side.px == 10.0 and side.qty == 120.0
    assert side.depth == 2 and side.total_qty == 170.0
    assert len(side.updates) == 3


def test_a_bid_sorts_downwards_and_an_ask_upwards() -> None:
    """Best first on both sides, which is the only order `alive` is allowed to be in."""
    prices = [10.0, 11.0, 9.0]
    bid, ask = BookSide(side=Side.BID), BookSide(side=Side.ASK)
    for px in prices:
        bid.append_order(Order(side=Side.BUY, px=px, qty=1.0, state=State.NEW))
        ask.append_order(Order(side=Side.SELL, px=px, qty=1.0, state=State.NEW))
    assert [level.px for level in bid.alive] == [11.0, 10.0, 9.0]
    assert [level.px for level in ask.alive] == [9.0, 10.0, 11.0]
    assert bid.px == 11.0 and ask.px == 9.0


def test_a_terminal_order_rests_for_nothing() -> None:
    """A cancelled order is not liquidity, whatever quantity it still names."""
    side = BookSide(side=Side.BID)
    side.append_order(Order(side=Side.BUY, px=10.0, qty=100.0, state=State.NEW))
    side.append_order(Order(side=Side.BUY, px=10.0, qty=-100.0, state=State.NEW))
    assert side.alive == [] and side.depth == 0 and side.px is None
    assert side.updates[-1].action is UpdateAction.DELETE


def test_what_an_order_rests_for_is_what_the_venue_last_said() -> None:
    """Shown, then left, then asked for -- in that order, because that is how a book sees it."""
    side = BookSide(side=Side.BID)
    side.append_order(Order(side=Side.BUY, px=10.0, qty=100.0, display_qty=10.0, state=State.NEW))
    assert side.alive[0].qty == 10.0, "an iceberg rests for what it shows"
    side.append_order(Order(side=Side.BUY, px=9.0, qty=100.0, leaves_qty=30.0, state=State.NEW))
    assert side.alive[-1].qty == 30.0, "a partly filled order rests for what is left"


def test_a_market_order_is_refused_because_it_never_rests() -> None:
    with pytest.raises(ValueError, match="never rests"):
        BookSide(side=Side.BID).append_order(
            Order(side=Side.BUY, qty=100.0, kind=OrderKind.MARKET_ORDER, state=State.NEW)
        )


def test_a_fill_takes_quantity_out_and_leaves_its_trace() -> None:
    side = BookSide(side=Side.BID)
    side.append_order(Order(side=Side.BUY, px=10.0, qty=100.0, state=State.NEW))
    side.append_execution(Execution(side=Side.BUY, px=10.0, qty=30.0, kind=ExecKind.TRADED))
    assert side.alive[0].qty == 70.0
    assert len(side.executions) == 1
    assert side.executions[0].px == 10.0 and side.executions[0].qty == 30.0


def test_a_report_that_moved_no_shares_moves_no_liquidity() -> None:
    """Subtracting an acknowledgement's quantity is how a book empties by lunchtime."""
    side = BookSide(side=Side.BID)
    side.append_order(Order(side=Side.BUY, px=10.0, qty=100.0, state=State.NEW))
    before = [(level.px, level.qty) for level in side.alive]
    for kind in (ExecKind.ACK, ExecKind.PENDING_NEW, ExecKind.ORDER_STATUS, ExecKind.REJECTED):
        side.append_execution(Execution(side=Side.BUY, px=10.0, qty=999.0, kind=kind))
    assert [(level.px, level.qty) for level in side.alive] == before
    assert side.executions is None


def test_every_append_is_a_new_version_that_remembers_the_one_before() -> None:
    side = BookSide(side=Side.BID, xhash=identifier(7))
    first = side.hash
    side.append_order(Order(side=Side.BUY, px=10.0, qty=1.0, state=State.NEW))
    second = side.hash
    side.append_order(Order(side=Side.BUY, px=11.0, qty=1.0, state=State.NEW))
    assert side.version == 2
    assert side.prev_hash == second and second != first
    assert side.hash not in (first, second), "a new version is a new content hash"
    assert len(side.parent_hash) == 2, "and both events that caused it are on the row"
    assert side.xhash == identifier(7), "while the lifecycle is the same thing throughout"


def test_a_book_routes_each_event_to_the_side_it_belongs_on() -> None:
    built = Book(symbol="AAPL")
    built.append_event(Order(side=Side.BUY, px=10.0, qty=100.0, state=State.NEW))
    built.append_event(Order(side=Side.SELL, px=10.2, qty=300.0, state=State.NEW))
    assert built.bid_px == 10.0 and built.bid_qty == 100.0
    assert built.ask_px == 10.2 and built.ask_qty == 300.0
    assert built.px == pytest.approx(10.1) and built.spread == pytest.approx(0.2)
    assert built.micro_px == pytest.approx((10.0 * 300 + 10.2 * 100) / 400)
    assert built.imbalance == pytest.approx(-0.5)
    assert built.bid_hash is not None and built.ask_hash is not None


def test_the_flat_pair_reconstructs_the_best_bid_and_offer_when_built_too() -> None:
    """The identity holds whether the book was derived in kernels or appended to."""
    built = Book()
    built.append_event(Order(side=Side.BUY, px=10.0, qty=100.0, state=State.NEW))
    built.append_event(Order(side=Side.SELL, px=10.2, qty=300.0, state=State.NEW))
    assert built.px - built.spread / 2 == pytest.approx(built.bid_px)
    assert built.px + built.spread / 2 == pytest.approx(built.ask_px)


def test_a_fill_against_a_book_clears_the_side_it_hit_and_leaves_the_other() -> None:
    built = Book()
    built.append_event(Order(side=Side.BUY, px=10.0, qty=100.0, state=State.NEW))
    built.append_event(Order(side=Side.SELL, px=10.2, qty=300.0, state=State.NEW))
    built.append_event(Execution(side=Side.SELL, px=10.2, qty=300.0, kind=ExecKind.TRADED))
    assert built.ask_px is None and built.ask_depth == 0
    assert len(built.ask_executions) == 1
    assert built.bid_px == 10.0 and built.bid_qty == 100.0, "the other side is untouched"
    assert built.spread is None and built.micro_px is None


def test_the_generic_append_infers_what_it_was_handed() -> None:
    side = BookSide(side=Side.BID)
    side.append_event(Order(side=Side.BUY, px=10.0, qty=5.0, state=State.NEW))
    side.append_event(Execution(side=Side.BUY, px=10.0, qty=2.0, kind=ExecKind.TRADED))
    assert side.alive[0].qty == 3.0
    with pytest.raises(TypeError):
        side.append_event("not an event")


def test_a_book_lifts_a_side_out_and_puts_it_back_unchanged() -> None:
    """`into_side`/`from_side` is how routing reuses the side's own rules."""
    built = Book(symbol="AAPL")
    built.append_event(Order(side=Side.BUY, px=10.0, qty=100.0, state=State.NEW))
    side = built.into_side("bid")
    assert side.is_book_side() and side.side is Side.BID
    assert side.px == built.bid_px and side.symbol == "AAPL"
    assert side.alive == built.bid_alive
    with pytest.raises(ValueError, match="bid and an ask"):
        built.into_side("middle")
