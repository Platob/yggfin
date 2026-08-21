"""The derived columns, checked against arithmetic done by hand and in Python.

`summarise_arrow` is kernels; the reference it is compared against is a plain
loop over the same rows. Comparing it against itself -- re-deriving with the
same kernels and asserting they match -- would pass on every bug it has.
"""

from __future__ import annotations

import math

import pyarrow
import pytest

from rekep.market import Book, BookSide, Side
from rekep.market.book import Level

from .conftest import batch, value_of

#: The rows a book actually produces: two levels, none at all, an increment
#: with no snapshot, and one level. Each is a branch of the walk.
ALIVE = [
    [{"px": 10.0, "qty": 5.0, "orders": 2}, {"px": 9.5, "qty": 7.0, "orders": 1}],
    [],
    None,
    [{"px": 2.0, "qty": 3.0, "orders": None}],
]


def sides(shape, rows):
    """`bid`/`ask` struct values, filled the way the declaration requires."""
    member = shape.FIELD.field("bid")
    return [value_of(member, index) | row for index, row in enumerate(rows)]


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
    given = batch(
        Book,
        1,
        bid=sides(Book, [{"px": 10.0, "qty": 100.0}]),
        ask=sides(Book, [{"px": 10.2, "qty": 300.0}]),
    )
    out = Book.summarise_arrow_batch(given)
    assert out.column("px")[0].as_py() == pytest.approx((10.0 + 10.2) / 2)
    assert out.column("qty")[0].as_py() == pytest.approx(400.0)
    assert out.column("spread")[0].as_py() == pytest.approx(10.2 - 10.0)
    assert out.column("micro_px")[0].as_py() == pytest.approx((10.0 * 300.0 + 10.2 * 100.0) / 400.0)
    assert out.column("imbalance")[0].as_py() == pytest.approx((100.0 - 300.0) / 400.0)


def test_the_flat_pair_reconstructs_the_best_bid_and_offer_exactly() -> None:
    """Which is why neither is duplicated as a column of its own."""
    out = Book.summarise_arrow_batch(
        batch(
            Book,
            1,
            bid=sides(Book, [{"px": 10.0, "qty": 100.0}]),
            ask=sides(Book, [{"px": 10.2, "qty": 300.0}]),
        )
    )
    mid, spread = out.column("px")[0].as_py(), out.column("spread")[0].as_py()
    assert mid - spread / 2 == pytest.approx(10.0)
    assert mid + spread / 2 == pytest.approx(10.2)


def test_a_crossed_book_shows_as_a_negative_spread_and_a_locked_one_as_zero() -> None:
    """The range predicate that replaces two boolean flags."""
    out = Book.summarise_arrow_batch(
        batch(
            Book,
            2,
            bid=sides(Book, [{"px": 11.0, "qty": 50.0}, {"px": 10.0, "qty": 50.0}]),
            ask=sides(Book, [{"px": 10.5, "qty": 50.0}, {"px": 10.0, "qty": 50.0}]),
        )
    )
    assert out.column("spread")[0].as_py() < 0, "crossed"
    assert out.column("spread")[1].as_py() == 0.0, "locked"


def test_a_one_sided_book_has_no_mid_rather_than_half_of_one() -> None:
    out = Book.summarise_arrow_batch(
        batch(
            Book,
            1,
            bid=sides(Book, [{"px": 9.0, "qty": 10.0}]),
            ask=sides(Book, [{"px": None, "qty": None}]),
        )
    )
    for name in ("px", "spread", "micro_px", "imbalance"):
        assert out.column(name)[0].as_py() is None, name


def test_an_empty_book_gives_a_null_rather_than_an_infinity() -> None:
    """Dividing by a size of zero is the one place a kernel would return a number."""
    out = Book.summarise_arrow_batch(
        batch(
            Book,
            1,
            bid=sides(Book, [{"px": 5.0, "qty": 0.0}]),
            ask=sides(Book, [{"px": 5.5, "qty": 0.0}]),
        )
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
        batch(Book, 2, bid=sides(Book, [b for b, _ in rows]), ask=sides(Book, [a for _, a in rows]))
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


def test_a_book_derives_its_sides_from_their_levels_before_pricing_across_them() -> None:
    """The half the first version left out, and the benchmark found.

    A book assembled from two feeds carries levels and nothing derived. If
    `summarise` read `bid.px` before deriving it, every price here would come
    back null -- which is what happened, and what no test at the time noticed.
    """
    levels = [{"px": 10.0, "qty": 100.0, "orders": 1}, {"px": 9.0, "qty": 20.0, "orders": 1}]
    offers = [{"px": 10.2, "qty": 300.0, "orders": 2}]
    given = batch(
        Book,
        1,
        bid=sides(Book, [{"alive": levels}]),
        ask=sides(Book, [{"alive": offers}]),
    )
    assert given.column("bid")[0].as_py()["px"] is None, "nothing is derived going in"

    out = Book.summarise_arrow_batch(given)
    assert out.column("bid")[0].as_py()["px"] == 10.0, "the side's own best price"
    assert out.column("bid")[0].as_py()["total_qty"] == 120.0
    assert out.column("bid")[0].as_py()["depth"] == 2
    assert out.column("ask")[0].as_py()["px"] == 10.2
    assert out.column("px")[0].as_py() == pytest.approx(10.1), "and the mid across them"
    assert out.column("spread")[0].as_py() == pytest.approx(0.2)


def test_summarising_a_book_is_the_same_as_summarising_each_side_first() -> None:
    """The nested half must be the same code, not a second walk of the same levels."""
    levels = [{"px": 10.0, "qty": 100.0, "orders": 1}]
    given = batch(
        Book, 1, bid=sides(Book, [{"alive": levels}]), ask=sides(Book, [{"alive": levels}])
    )
    apart = BookSide.summarise_arrow_batch(
        pyarrow.RecordBatch.from_struct_array(given.column("bid"))
    ).to_struct_array()
    assert Book.summarise_arrow_batch(given).column("bid").equals(apart)


def test_summarising_a_book_twice_changes_nothing_the_second_time() -> None:
    """Idempotent, so a producer and a consumer running it both is not a bug."""
    given = batch(
        Book,
        1,
        bid=sides(Book, [{"alive": [{"px": 1.0, "qty": 2.0, "orders": 1}]}]),
        ask=sides(Book, [{"alive": [{"px": 1.5, "qty": 4.0, "orders": 1}]}]),
    )
    once = Book.summarise_arrow_batch(given)
    assert Book.summarise_arrow_batch(once).equals(once)
