"""Reshaping an incoming batch onto a record's schema."""

import datetime
from typing import Annotated

import pyarrow
import pytest

from rekep import Arrow, Record, record
from rekep.records import cast_batch, cast_reader


@record
class Tick(Record):
    """One quote."""

    symbol: str
    """Instrument."""

    size: Annotated[int, Arrow(type=pyarrow.int32())]
    """Quantity, narrow on purpose."""

    day: datetime.date
    """Trading day."""

    venue: str | None = None
    """Where it traded, when known."""


def batch_of(**columns: list) -> pyarrow.RecordBatch:
    return pyarrow.RecordBatch.from_pydict(columns)


# -- casting -------------------------------------------------------------


def test_an_already_matching_batch_is_returned_untouched() -> None:
    schema = Tick.into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pylist(
        [{"symbol": "A", "size": 1, "day": datetime.date(2026, 8, 14), "venue": None}],
        schema=schema,
    )
    assert Tick.cast_arrow_batch(batch) is batch, "no copy when there is nothing to do"


def test_a_wider_column_is_narrowed_to_the_declared_type() -> None:
    """Unsafe by default: the record's declaration is the authority."""
    batch = batch_of(
        symbol=["A"],
        size=pyarrow.array([2**40], type=pyarrow.int64()),
        day=[datetime.date(2026, 8, 14)],
    )
    cast = Tick.cast_arrow_batch(batch)
    assert cast.column("size").type == pyarrow.int32()
    assert cast.column("size").to_pylist() != [2**40], "truncated, which is what unsafe means"


def test_safe_true_refuses_the_same_narrowing() -> None:
    batch = batch_of(
        symbol=["A"],
        size=pyarrow.array([2**40], type=pyarrow.int64()),
        day=[datetime.date(2026, 8, 14)],
    )
    with pytest.raises(pyarrow.ArrowInvalid):
        Tick.cast_arrow_batch(batch, safe=True)


def test_a_missing_nullable_column_is_filled_with_nulls() -> None:
    batch = batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)])
    cast = Tick.cast_arrow_batch(batch)
    assert cast.column("venue").to_pylist() == [None]


def test_a_missing_non_nullable_column_is_refused_by_name() -> None:
    batch = batch_of(symbol=["A"], size=[1])
    with pytest.raises(ValueError, match="'day' is missing and not nullable"):
        Tick.cast_arrow_batch(batch)


def test_columns_are_reordered_and_extras_dropped() -> None:
    batch = batch_of(
        venue=["X"], size=[1], noise=["ignored"], day=[datetime.date(2026, 8, 14)], symbol=["A"]
    )
    cast = Tick.cast_arrow_batch(batch)
    assert cast.schema.names == Tick.into_arrow_schema().names
    assert cast.column("venue").to_pylist() == ["X"]


def test_the_cast_batch_carries_the_records_schema_metadata() -> None:
    """Field ids matter downstream: Iceberg identifies columns by id, not name."""
    batch = batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)])
    assert Tick.cast_arrow_batch(batch).schema.equals(Tick.into_arrow_schema())


# -- streams -------------------------------------------------------------


def test_a_plain_iterator_of_batches_becomes_a_reader() -> None:
    batches = [batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)])]
    reader = Tick.cast_arrow_reader(iter(batches))
    assert isinstance(reader, pyarrow.RecordBatchReader)
    assert reader.schema.equals(Tick.into_arrow_schema())
    assert reader.read_all().num_rows == 1


def test_a_reader_is_cast_one_batch_at_a_time() -> None:
    """Laziness survives: nothing is read until the reader is."""
    read = []

    def batches():
        for value in ("A", "B"):
            read.append(value)
            yield batch_of(symbol=[value], size=[1], day=[datetime.date(2026, 8, 14)])

    reader = Tick.cast_arrow_reader(batches())
    assert read == [], "nothing consumed yet"
    first = next(reader)
    assert read == ["A"], "one batch, not both"
    assert first.column("symbol").to_pylist() == ["A"]


def test_the_module_functions_take_any_schema() -> None:
    """`cast_batch`/`cast_reader` are not record-bound: a parquet footer or
    another team's contract is a target schema just as well."""
    target = pyarrow.schema([("size", pyarrow.int16()), ("symbol", pyarrow.string())])
    batch = batch_of(symbol=["A"], size=[7], extra=[1])
    assert cast_batch(batch, target).schema.equals(target)
    assert cast_reader(iter([batch]), target).read_all().num_rows == 1
