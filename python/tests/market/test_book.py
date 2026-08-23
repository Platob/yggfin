"""The compact book contract and its Arrow-derived summaries."""

from __future__ import annotations

import math

import pyarrow
import pytest

from rekep.market import Book, Level

from .conftest import batch


def level(
    px: float,
    qty: float,
    order_xhash: list[int] | None = None,
    exec_xhash: list[int] | None = None,
) -> dict[str, object]:
    """One complete nested Level value for Arrow fixtures."""
    return {
        "px": px,
        "qty": qty,
        "order_xhash": order_xhash or [],
        "exec_xhash": exec_xhash or [],
    }


def book(rows: int = 1, **columns: object) -> pyarrow.RecordBatch:
    """A batch of books cast onto the declaration."""
    return batch(Book, rows, **columns)


def prices(name: str, values: list[tuple[float | None, float | None]]) -> dict[str, list]:
    """One side's best price and quantity columns."""
    return {f"{name}_px": [px for px, _ in values], f"{name}_qty": [qty for _, qty in values]}


def test_level_is_the_only_persisted_side_shape() -> None:
    assert [member.name for member in Level.into_field().fields] == [
        "px",
        "qty",
        "order_xhash",
        "exec_xhash",
    ]
    names = {member.name for member in Book.into_field().fields}
    assert {"bid_levels", "ask_levels"} <= names
    assert {"bid_alive", "ask_alive"} <= names
    assert not names & {
        "bid_updates",
        "ask_updates",
        "bid_executions",
        "ask_executions",
        "bid_orders",
        "ask_orders",
        "bid_hash",
        "ask_hash",
    }


def test_level_declares_the_fix_price_and_quantity_fields() -> None:
    assert Level.into_field().field("px").fix["tag"] == "270"
    assert Level.into_field().field("qty").fix["tag"] == "271"


def test_levels_derive_best_depth_and_total_for_each_side() -> None:
    bids = [level(10.0, 5.0, [1, 2]), level(9.5, 7.0, [3])]
    asks = [level(10.2, 3.0, [4])]
    out = Book.summarise_arrow_batch(
        book(1, sunix=[1], bid_levels=[bids], ask_levels=[asks])
    )
    assert out.column("bid_px")[0].as_py() == 10.0
    assert out.column("bid_qty")[0].as_py() == 5.0
    assert out.column("bid_depth")[0].as_py() == 2
    assert out.column("bid_total_qty")[0].as_py() == 12.0
    assert out.column("ask_px")[0].as_py() == 10.2
    assert out.column("ask_depth")[0].as_py() == 1


def test_empty_levels_mean_a_known_empty_side() -> None:
    out = Book.summarise_arrow_batch(book(1, sunix=[1], bid_levels=[[]]))
    assert out.column("bid_px")[0].as_py() is None
    assert out.column("bid_qty")[0].as_py() is None
    assert out.column("bid_depth")[0].as_py() == 0
    assert out.column("bid_total_qty")[0].as_py() == 0.0


def test_empty_delta_levels_leave_flat_summaries_unchanged() -> None:
    given = book(
        1,
        bid_levels=[[]],
        bid_px=[10.0],
        bid_qty=[2.0],
        bid_depth=[3],
        bid_total_qty=[9.0],
    )
    out = Book.summarise_arrow_batch(given)
    assert out.column("bid_px")[0].as_py() == 10.0
    assert out.column("bid_qty")[0].as_py() == 2.0
    assert out.column("bid_depth")[0].as_py() == 3
    assert out.column("bid_total_qty")[0].as_py() == 9.0


def test_a_thousand_levels_still_sum_exactly() -> None:
    levels = [level(100.0 - index, 0.1) for index in range(1000)]
    out = Book.summarise_arrow_batch(book(1, sunix=[1], bid_levels=[levels]))
    assert out.column("bid_total_qty")[0].as_py() == pytest.approx(100.0, abs=1e-9)
    assert out.column("bid_depth")[0].as_py() == 1000


def test_prices_match_the_book_formulas() -> None:
    given = book(1, **prices("bid", [(10.0, 100.0)]), **prices("ask", [(10.2, 300.0)]))
    out = Book.summarise_arrow_batch(given)
    assert out.column("px")[0].as_py() == pytest.approx(10.1)
    assert out.column("qty")[0].as_py() == 400.0
    assert out.column("spread")[0].as_py() == pytest.approx(0.2)
    assert out.column("micro_px")[0].as_py() == pytest.approx((10.0 * 300.0 + 10.2 * 100.0) / 400.0)
    assert out.column("imbalance")[0].as_py() == pytest.approx(-0.5)


def test_one_sided_and_zero_sized_books_have_no_synthetic_price() -> None:
    one_sided = Book.summarise_arrow_batch(
        book(1, **prices("bid", [(9.0, 10.0)]), **prices("ask", [(None, None)]))
    )
    assert all(
        one_sided.column(name)[0].as_py() is None
        for name in ("px", "spread", "micro_px", "imbalance")
    )
    empty = Book.summarise_arrow_batch(
        book(1, **prices("bid", [(5.0, 0.0)]), **prices("ask", [(5.5, 0.0)]))
    )
    values = [empty.column(name)[0].as_py() for name in ("micro_px", "imbalance")]
    assert values == [None, None]
    assert not any(value is not None and math.isinf(value) for value in values)


def test_summarise_is_idempotent_and_keeps_the_contract() -> None:
    given = book(
        1,
        sunix=[1],
        bid_levels=[[level(1.0, 2.0, [1])]],
        ask_levels=[[level(1.5, 4.0, [2])]],
    )
    once = Book.summarise_arrow_batch(given)
    assert Book.summarise_arrow_batch(once).equals(once)
    assert once.schema == given.schema


def test_generic_batch_and_table_forms_agree_including_zero_rows() -> None:
    empty = book(0)
    assert Book.summarise_arrow(empty).num_rows == 0
    given = book(2)
    table = pyarrow.Table.from_batches([given])
    assert Book.summarise_arrow(table).equals(
        pyarrow.Table.from_batches([Book.summarise_arrow_batch(given)])
    )
