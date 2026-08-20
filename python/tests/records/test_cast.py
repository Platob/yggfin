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


# -- merge_schema: keeping what the target does not declare ---------------


def test_merge_schema_keeps_an_unknown_column() -> None:
    batch = batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)], desk=["EQ"])
    cast = Tick.cast_arrow_reader(iter([batch]), merge_schema=True).read_all()
    assert cast.column_names == [*Tick.into_arrow_schema().names, "desk"]
    assert cast.column("desk").to_pylist() == ["EQ"]


def test_merge_schema_still_casts_the_shared_columns() -> None:
    batch = batch_of(
        symbol=["A"],
        size=pyarrow.array([1], type=pyarrow.int64()),
        day=[datetime.date(2026, 8, 14)],
        desk=["EQ"],
    )
    cast = Tick.cast_arrow_reader(iter([batch]), merge_schema=True).read_all()
    assert cast.column("size").type == pyarrow.int32(), "the record's declaration still wins"


def test_merge_schema_delivers_every_batch_exactly_once() -> None:
    """The peek pulls one batch to learn the shape; it must put it back."""
    batches = [
        batch_of(symbol=[name], size=[1], day=[datetime.date(2026, 8, 14)], desk=["EQ"])
        for name in ("A", "B", "C")
    ]
    read = Tick.cast_arrow_reader(iter(batches), merge_schema=True).read_all()
    assert read.column("symbol").to_pylist() == ["A", "B", "C"]


def test_merge_schema_on_an_empty_stream_leaves_the_schema_alone() -> None:
    reader = Tick.cast_arrow_reader(iter(()), merge_schema=True)
    assert reader.schema.equals(Tick.into_arrow_schema())
    assert reader.read_all().num_rows == 0


def test_a_readers_own_schema_is_used_without_consuming_it() -> None:
    source = pyarrow.RecordBatchReader.from_batches(
        pyarrow.schema([("symbol", pyarrow.string()), ("desk", pyarrow.string())]),
        iter(()),
    )
    assert "desk" in Tick.cast_arrow_reader(source, merge_schema=True).schema.names


def test_the_stream_shape_is_decided_once_not_per_batch() -> None:
    """A reader cannot change schema under its consumer, so later batches are
    resolved in the target's favour -- documented, not accidental."""
    first = batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)], desk=["EQ"])
    later = batch_of(symbol=["B"], size=[2], day=[datetime.date(2026, 8, 14)])
    read = Tick.cast_arrow_reader(iter([first, later]), merge_schema=True).read_all()
    assert read.column("desk").to_pylist() == ["EQ", None], "the dropped column comes back null"

    surprise = batch_of(symbol=["C"], size=[3], day=[datetime.date(2026, 8, 14)], pod=["X"])
    read = Tick.cast_arrow_reader(iter([later, surprise]), merge_schema=True).read_all()
    assert "pod" not in read.column_names, "a column only a later batch has is dropped"


# -- merging is one rule, applied at every level --------------------------


def struct_of(**members: pyarrow.DataType) -> pyarrow.DataType:
    return pyarrow.struct([(name, kind) for name, kind in members.items()])


def test_a_struct_that_grew_a_member_grows_in_the_merge() -> None:
    from rekep.records import merge_schemas

    target = pyarrow.schema([pyarrow.field("venue", struct_of(mic=pyarrow.string()))])
    source = pyarrow.schema(
        [pyarrow.field("venue", struct_of(mic=pyarrow.string(), desk=pyarrow.int64()))]
    )
    merged = merge_schemas(source, target).field("venue").type
    assert [merged.field(i).name for i in range(merged.num_fields)] == ["mic", "desk"]
    assert merged.field(1).nullable, "an addition is nullable however deep it is"


def test_a_list_of_structs_merges_its_item() -> None:
    from rekep.records import merge_schemas

    target = pyarrow.schema([pyarrow.field("legs", pyarrow.list_(struct_of(id=pyarrow.int64())))])
    source = pyarrow.schema(
        [pyarrow.field("legs", pyarrow.list_(struct_of(id=pyarrow.int64(), px=pyarrow.float64())))]
    )
    item = merge_schemas(source, target).field("legs").type.field(0).type
    assert [item.field(i).name for i in range(item.num_fields)] == ["id", "px"]


def test_a_map_merges_its_values_but_never_its_keys() -> None:
    """A key is what identifies an entry; changing its shape changes which
    entries exist, which is not a merge."""
    from rekep.records import merge_schemas

    target = pyarrow.schema(
        [pyarrow.field("m", pyarrow.map_(pyarrow.string(), struct_of(a=pyarrow.int64())))]
    )
    source = pyarrow.schema(
        [
            pyarrow.field(
                "m",
                pyarrow.map_(
                    pyarrow.large_string(), struct_of(a=pyarrow.int64(), b=pyarrow.string())
                ),
            )
        ]
    )
    merged = merge_schemas(source, target).field("m").type
    assert merged.key_type == pyarrow.string(), "the target's key survives"
    assert merged.item_type.num_fields == 2


def test_a_container_facing_a_scalar_leaves_the_target_alone() -> None:
    from rekep.records import merge_schemas

    target = pyarrow.schema([pyarrow.field("x", pyarrow.int64())])
    source = pyarrow.schema([pyarrow.field("x", struct_of(a=pyarrow.int64()))])
    assert merge_schemas(source, target).field("x").type == pyarrow.int64()


def test_merge_fields_is_the_same_rule_on_one_field() -> None:
    from rekep.records import merge_fields

    target = pyarrow.field("venue", struct_of(mic=pyarrow.string()))
    source = pyarrow.field("venue", struct_of(mic=pyarrow.string(), desk=pyarrow.int64()))
    merged = merge_fields(source, target)
    assert merged.name == "venue"
    assert merged.type.num_fields == 2


def test_nested_additions_get_ids_after_everything_already_numbered() -> None:
    from rekep.records.arrow import FIELD_ID_KEY, merge_schemas

    target = Tick.into_arrow_schema()
    highest = max(int((f.metadata or {})[FIELD_ID_KEY]) for f in target)
    source = pyarrow.schema([pyarrow.field("book", struct_of(bid=pyarrow.float64()))])
    merged = merge_schemas(source, target)
    added = merged.field("book")
    assert int((added.metadata or {})[FIELD_ID_KEY]) == highest + 1
    child = added.type.field(0)
    assert int((child.metadata or {})[FIELD_ID_KEY]) == highest + 2, "children numbered too"
