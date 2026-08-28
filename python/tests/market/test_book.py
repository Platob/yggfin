"""The compact book contract and its Arrow-derived summaries."""

from __future__ import annotations

import math

import pyarrow
import pytest

from rekep.market import Book, Execution, Level, Order, Side, State

from .conftest import batch


def level(px: float, qty: float) -> dict[str, object]:
    """One complete nested Level value for Arrow fixtures."""
    return {"px": px, "qty": qty}


def book(rows: int = 1, **columns: object) -> pyarrow.RecordBatch:
    """A batch of books cast onto the declaration."""
    return batch(Book, rows, **columns)


def prices(name: str, values: list[tuple[float | None, float | None]]) -> dict[str, list]:
    """One side's best price and quantity columns."""
    return {f"{name}_px": [px for px, _ in values], f"{name}_qty": [qty for _, qty in values]}


def test_level_is_the_only_persisted_side_shape() -> None:
    assert [member.name for member in Level.into_field().fields] == ["px", "qty"]
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


def test_levels_derive_best_and_depth_for_each_side() -> None:
    bids = [level(10.0, 5.0), level(9.5, 7.0)]
    asks = [level(10.2, 3.0)]
    out = Book.summarise_arrow_batch(book(1, sunix=[1], bid_levels=[bids], ask_levels=[asks]))
    assert out.column("bid_px")[0].as_py() == 10.0
    assert out.column("bid_qty")[0].as_py() == 5.0
    assert out.column("bid_depth")[0].as_py() == 2
    assert out.column("ask_px")[0].as_py() == 10.2
    assert out.column("ask_qty")[0].as_py() == 3.0
    assert out.column("ask_depth")[0].as_py() == 1


def test_empty_levels_mean_a_known_empty_side() -> None:
    out = Book.summarise_arrow_batch(book(1, sunix=[1], bid_levels=[[]]))
    assert out.column("bid_px")[0].as_py() is None
    assert out.column("bid_qty")[0].as_py() is None
    assert out.column("bid_depth")[0].as_py() == 0


def test_empty_delta_levels_leave_flat_summaries_unchanged() -> None:
    given = book(
        1,
        bid_levels=[[]],
        bid_px=[10.0],
        bid_qty=[2.0],
        bid_depth=[3],
    )
    out = Book.summarise_arrow_batch(given)
    assert out.column("bid_px")[0].as_py() == 10.0
    assert out.column("bid_qty")[0].as_py() == 2.0
    assert out.column("bid_depth")[0].as_py() == 3


def test_a_thousand_levels_still_derive_the_touch_and_depth() -> None:
    levels = [level(100.0 - index, 0.1) for index in range(1000)]
    out = Book.summarise_arrow_batch(book(1, sunix=[1], bid_levels=[levels]))
    assert out.column("bid_px")[0].as_py() == 100.0
    assert out.column("bid_qty")[0].as_py() == 0.1
    assert out.column("bid_depth")[0].as_py() == 1000


def test_prices_match_the_book_formulas() -> None:
    given = book(1, **prices("bid", [(10.0, 100.0)]), **prices("ask", [(10.2, 300.0)]))
    out = Book.summarise_arrow_batch(given)
    assert out.column("px")[0].as_py() == pytest.approx(10.1)
    assert out.column("qty")[0].as_py() == 400.0
    assert out.column("spread")[0].as_py() == pytest.approx(0.2)
    assert out.column("vwap")[0].as_py() == pytest.approx((10.0 * 300.0 + 10.2 * 100.0) / 400.0)
    assert out.column("imbalance")[0].as_py() == pytest.approx(-0.5)


def test_one_sided_and_zero_sized_books_have_no_synthetic_price() -> None:
    one_sided = Book.summarise_arrow_batch(
        book(1, **prices("bid", [(9.0, 10.0)]), **prices("ask", [(None, None)]))
    )
    assert all(
        one_sided.column(name)[0].as_py() is None for name in ("px", "spread", "vwap", "imbalance")
    )
    empty = Book.summarise_arrow_batch(
        book(1, **prices("bid", [(5.0, 0.0)]), **prices("ask", [(5.5, 0.0)]))
    )
    values = [empty.column(name)[0].as_py() for name in ("vwap", "imbalance")]
    assert values == [None, None]
    assert not any(value is not None and math.isinf(value) for value in values)


def test_summarise_is_idempotent_and_keeps_the_contract() -> None:
    given = book(
        1,
        sunix=[1],
        bid_levels=[[level(1.0, 2.0)]],
        ask_levels=[[level(1.5, 4.0)]],
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


def test_book_arrow_reader_matches_nested_document_projection() -> None:
    bid = Order(
        unix=1,
        hash=11,
        xhash=12,
        linked_events=[(0, 10)],
        parent_hash=[9],
        state=State.NEW,
        code="B1",
        codes={"order": "B1"},
        metadata={"source": "bid"},
        side=Side.BID,
        px=100.0,
        qty=3.0,
        order_id="B1",
    )
    ask = Order(
        unix=1,
        hash=21,
        xhash=22,
        linked_events=[(0, 20)],
        parent_hash=[19],
        state=State.NEW,
        code="A1",
        codes={"order": "A1"},
        metadata={"source": "ask"},
        side=Side.ASK,
        px=101.0,
        qty=4.0,
        order_id="A1",
    )
    execution = Execution(
        unix=2,
        hash=31,
        xhash=32,
        linked_events=[(1, bid.xhash)],
        parent_hash=[bid.hash],
        state=State.FILLED,
        code="E1",
        codes={"execution": "E1"},
        metadata={"source": "trade"},
        side=Side.BID,
        px=100.0,
        qty=1.0,
        exec_id="E1",
    )
    rows = [
        Book(
            unix=2,
            linked_events=[(1, bid.xhash), (2, execution.xhash)],
            parent_hash=[bid.hash, execution.hash],
            codes={"symbol": "BTC-USD"},
            metadata={"kind": "delta"},
            bid_levels=[Level(px=100.0, qty=3.0)],
            ask_levels=[Level(px=101.0, qty=4.0)],
            deltas=[bid],
            executions=[execution],
        ),
        Book(
            unix=3,
            sunix=3,
            linked_events=[(1, bid.xhash), (1, ask.xhash)],
            parent_hash=[bid.hash, ask.hash],
            codes={"symbol": "BTC-USD"},
            metadata={"kind": "snapshot"},
            bid_levels=[Level(px=100.0, qty=3.0)],
            ask_levels=[Level(px=101.0, qty=4.0)],
            bid_alive=[bid],
            ask_alive=[ask],
        ),
    ]
    schema = Book.into_field().into_arrow_schema()
    expected = pyarrow.Table.from_pylist([row.into_row() for row in rows], schema=schema)
    batches = list(Book.into_arrow_reader(iter(rows), batch_row_size=1))
    actual = pyarrow.Table.from_batches(batches, schema=schema)

    assert [batch.num_rows for batch in batches] == [1, 1]
    assert actual.schema.equals(schema, check_metadata=True)
    assert actual.equals(expected)


def test_empty_book_arrow_reader_keeps_the_contract() -> None:
    reader = Book.into_arrow_reader(())
    assert reader.schema.equals(Book.into_field().into_arrow_schema(), check_metadata=True)
    assert list(reader) == []
