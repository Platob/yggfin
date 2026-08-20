"""Reshaping an incoming batch onto a field's schema, and widening that schema."""

import datetime
from typing import Annotated

import pyarrow
import pytest

from rekep import Convertible, Field, field
from rekep.fields import cast_batch, cast_reader, cast_table, merge_fields, merge_schemas


@field
class Tick(Convertible):
    """One quote."""

    symbol: str
    """Instrument."""

    size: Annotated[int, Field(arrow_type=pyarrow.int32())]
    """Quantity, narrow on purpose."""

    day: datetime.date
    """Trading day."""

    venue: str | None = None
    """Where it traded, when known."""


def batch_of(**columns: list) -> pyarrow.RecordBatch:
    return pyarrow.RecordBatch.from_pydict(columns)


def struct_of(**members: pyarrow.DataType) -> pyarrow.DataType:
    return pyarrow.struct([(name, kind) for name, kind in members.items()])


# -- casting ----------------------------------------------------------------


def test_an_already_matching_batch_is_returned_untouched() -> None:
    batch = pyarrow.RecordBatch.from_pylist(
        [{"symbol": "A", "size": 1, "day": datetime.date(2026, 8, 14), "venue": None}],
        schema=Tick.FIELD.into_arrow_schema(),
    )
    assert Tick.FIELD.cast_arrow_batch(batch) is batch, "no copy when there is nothing to do"


def test_a_wider_column_is_narrowed_to_the_declared_type() -> None:
    """Unsafe by default: the declaration is the authority."""
    batch = batch_of(
        symbol=["A"],
        size=pyarrow.array([2**40], type=pyarrow.int64()),
        day=[datetime.date(2026, 8, 14)],
    )
    cast = Tick.FIELD.cast_arrow_batch(batch)
    assert cast.column("size").type == pyarrow.int32()
    assert cast.column("size").to_pylist() != [2**40], "truncated, which is what unsafe means"


def test_safe_true_refuses_the_same_narrowing() -> None:
    batch = batch_of(
        symbol=["A"],
        size=pyarrow.array([2**40], type=pyarrow.int64()),
        day=[datetime.date(2026, 8, 14)],
    )
    with pytest.raises(pyarrow.ArrowInvalid):
        Tick.FIELD.cast_arrow_batch(batch, safe=True)


def test_a_missing_nullable_column_is_filled_with_nulls() -> None:
    batch = batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)])
    assert Tick.FIELD.cast_arrow_batch(batch).column("venue").to_pylist() == [None]


def test_a_missing_non_nullable_column_is_refused_by_name() -> None:
    batch = batch_of(symbol=["A"], size=[1])
    with pytest.raises(ValueError, match=r"'Tick\.day' is missing and not nullable"):
        Tick.FIELD.cast_arrow_batch(batch)


def test_columns_are_reordered_and_extras_dropped() -> None:
    batch = batch_of(
        venue=["X"], size=[1], noise=["ignored"], day=[datetime.date(2026, 8, 14)], symbol=["A"]
    )
    cast = Tick.FIELD.cast_arrow_batch(batch)
    assert cast.schema.names == Tick.FIELD.into_arrow_schema().names
    assert cast.column("venue").to_pylist() == ["X"]


def test_the_cast_batch_carries_the_declared_schema_metadata() -> None:
    batch = batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)])
    cast = Tick.FIELD.cast_arrow_batch(batch)
    assert cast.schema.equals(Tick.FIELD.into_arrow_schema())


# -- streams ----------------------------------------------------------------


def test_a_plain_iterator_of_batches_becomes_a_reader() -> None:
    batches = [batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)])]
    reader = Tick.FIELD.cast_arrow_reader(iter(batches))
    assert isinstance(reader, pyarrow.RecordBatchReader)
    assert reader.schema.equals(Tick.FIELD.into_arrow_schema())
    assert reader.read_all().num_rows == 1


def test_a_reader_is_cast_one_batch_at_a_time() -> None:
    """Laziness survives: nothing is read until the reader is."""
    read = []

    def batches():
        for value in ("A", "B"):
            read.append(value)
            yield batch_of(symbol=[value], size=[1], day=[datetime.date(2026, 8, 14)])

    reader = Tick.FIELD.cast_arrow_reader(batches())
    assert read == [], "nothing consumed yet"
    first = next(reader)
    assert read == ["A"], "one batch, not both"
    assert first.column("symbol").to_pylist() == ["A"]


def test_the_module_functions_take_any_schema() -> None:
    """`cast_batch`/`cast_reader` are not field-bound: a parquet footer or
    another team's contract is a target schema just as well."""
    target = pyarrow.schema([("size", pyarrow.int16()), ("symbol", pyarrow.string())])
    batch = batch_of(symbol=["A"], size=[7], extra=[1])
    assert cast_batch(batch, target).schema.equals(target)
    assert cast_reader(iter([batch]), target).read_all().num_rows == 1


# -- merge_schema: keeping what the target does not declare -----------------


def test_merge_schema_keeps_an_unknown_column() -> None:
    batch = batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)], desk=["EQ"])
    cast = Tick.FIELD.cast_arrow_reader(iter([batch]), merge_schema=True).read_all()
    assert cast.column_names == [*Tick.FIELD.into_arrow_schema().names, "desk"]
    assert cast.column("desk").to_pylist() == ["EQ"]


def test_merge_schema_still_casts_the_shared_columns() -> None:
    batch = batch_of(
        symbol=["A"],
        size=pyarrow.array([1], type=pyarrow.int64()),
        day=[datetime.date(2026, 8, 14)],
        desk=["EQ"],
    )
    cast = Tick.FIELD.cast_arrow_reader(iter([batch]), merge_schema=True).read_all()
    assert cast.column("size").type == pyarrow.int32(), "the declaration still wins"


def test_merge_schema_delivers_every_batch_exactly_once() -> None:
    """The peek pulls one batch to learn the shape; it must put it back."""
    batches = [
        batch_of(symbol=[name], size=[1], day=[datetime.date(2026, 8, 14)], desk=["EQ"])
        for name in ("A", "B", "C")
    ]
    read = Tick.FIELD.cast_arrow_reader(iter(batches), merge_schema=True).read_all()
    assert read.column("symbol").to_pylist() == ["A", "B", "C"]


def test_merge_schema_on_an_empty_stream_leaves_the_schema_alone() -> None:
    reader = Tick.FIELD.cast_arrow_reader(iter(()), merge_schema=True)
    assert reader.schema.equals(Tick.FIELD.into_arrow_schema())
    assert reader.read_all().num_rows == 0


def test_a_readers_own_schema_is_used_without_consuming_it() -> None:
    source = pyarrow.RecordBatchReader.from_batches(
        pyarrow.schema([("symbol", pyarrow.string()), ("desk", pyarrow.string())]),
        iter(()),
    )
    assert "desk" in Tick.FIELD.cast_arrow_reader(source, merge_schema=True).schema.names


def test_the_stream_shape_is_decided_once_not_per_batch() -> None:
    """A reader cannot change schema under its consumer, so later batches are
    resolved in the target's favour -- documented, not accidental."""
    first = batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)], desk=["EQ"])
    later = batch_of(symbol=["B"], size=[2], day=[datetime.date(2026, 8, 14)])
    read = Tick.FIELD.cast_arrow_reader(iter([first, later]), merge_schema=True).read_all()
    assert read.column("desk").to_pylist() == ["EQ", None], "the dropped column comes back null"

    surprise = batch_of(symbol=["C"], size=[3], day=[datetime.date(2026, 8, 14)], pod=["X"])
    read = Tick.FIELD.cast_arrow_reader(iter([later, surprise]), merge_schema=True).read_all()
    assert "pod" not in read.column_names, "a column only a later batch has is dropped"


def test_merge_arrow_schema_adds_nullable_columns() -> None:
    incoming = pyarrow.schema([("symbol", pyarrow.string()), ("desk", pyarrow.string())])
    merged = Tick.FIELD.merge_arrow_schema(incoming)
    assert merged.field("desk").nullable, "rows already stored have nothing to put in it"
    assert merged.field("size").type == pyarrow.int32(), "shared columns stay the target's"


# -- merging is one rule, applied at every level ----------------------------


def test_a_struct_that_grew_a_member_grows_in_the_merge() -> None:
    target = pyarrow.schema([pyarrow.field("venue", struct_of(mic=pyarrow.string()))])
    source = pyarrow.schema(
        [pyarrow.field("venue", struct_of(mic=pyarrow.string(), desk=pyarrow.int64()))]
    )
    merged = merge_schemas(source, target).field("venue").type
    assert [merged.field(i).name for i in range(merged.num_fields)] == ["mic", "desk"]
    assert merged.field(1).nullable, "an addition is nullable however deep it is"


def test_a_list_of_structs_merges_its_item() -> None:
    target = pyarrow.schema([pyarrow.field("legs", pyarrow.list_(struct_of(id=pyarrow.int64())))])
    source = pyarrow.schema(
        [pyarrow.field("legs", pyarrow.list_(struct_of(id=pyarrow.int64(), px=pyarrow.float64())))]
    )
    item = merge_schemas(source, target).field("legs").type.field(0).type
    assert [item.field(i).name for i in range(item.num_fields)] == ["id", "px"]


def test_a_map_merges_its_values_but_never_its_keys() -> None:
    """A key is what identifies an entry; changing its shape changes which
    entries exist, which is not a merge."""
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
    target = pyarrow.schema([pyarrow.field("x", pyarrow.int64())])
    source = pyarrow.schema([pyarrow.field("x", struct_of(a=pyarrow.int64()))])
    assert merge_schemas(source, target).field("x").type == pyarrow.int64()


def test_a_schema_with_nothing_to_add_is_returned_as_it_was() -> None:
    target = Tick.FIELD.into_arrow_schema()
    assert merge_schemas(pyarrow.schema([("symbol", pyarrow.string())]), target) is target


def test_merge_fields_is_the_same_rule_on_one_field() -> None:
    target = pyarrow.field("venue", struct_of(mic=pyarrow.string()))
    source = pyarrow.field("venue", struct_of(mic=pyarrow.string(), desk=pyarrow.int64()))
    merged = merge_fields(source, target)
    assert merged.name == "venue"
    assert merged.type.num_fields == 2


# -- arrays, recursively ----------------------------------------------------


def venue(**members: pyarrow.DataType) -> pyarrow.DataType:
    return pyarrow.struct([(name, kind) for name, kind in members.items()])


def test_an_array_that_already_matches_is_handed_back() -> None:
    array = pyarrow.array([1, 2], type=pyarrow.int32())
    assert Field(name="size", arrow_type=pyarrow.int32()).cast_arrow_array(array) is array


def test_a_leaf_array_is_cast() -> None:
    array = pyarrow.array([2**40], type=pyarrow.int64())
    cast = Field(name="size", arrow_type=pyarrow.int32()).cast_arrow_array(array)
    assert cast.type == pyarrow.int32(), "unsafe by default, like every other cast here"


def test_a_chunked_array_is_cast_chunk_by_chunk() -> None:
    chunked = pyarrow.chunked_array([[1, 2], [3]], type=pyarrow.int64())
    cast = Field(name="size", arrow_type=pyarrow.int32()).cast_arrow_array(chunked)
    assert isinstance(cast, pyarrow.ChunkedArray)
    assert cast.num_chunks == 2, "the chunking survives; a column is never materialised whole"
    assert cast.type == pyarrow.int32()


def test_a_struct_array_is_cast_member_by_member() -> None:
    """Members in another order, one narrowed, one the source never had."""
    array = pyarrow.array(
        [{"desk": "EQ", "mic": "XPAR", "extra": 1}, None],
        type=venue(desk=pyarrow.string(), mic=pyarrow.string(), extra=pyarrow.int64()),
    )
    target = Field(
        name="venue",
        arrow_type=pyarrow.struct(
            [
                pyarrow.field("mic", pyarrow.string(), nullable=False),
                pyarrow.field("size", pyarrow.int32()),
            ]
        ),
        nullable=True,
    )
    cast = target.cast_arrow_array(array)
    assert cast.type == target.arrow_type
    assert cast.to_pylist() == [{"mic": "XPAR", "size": None}, None], "the null row stays null"


def test_a_missing_non_nullable_member_names_its_path() -> None:
    array = pyarrow.array([{"mic": "XPAR"}], type=venue(mic=pyarrow.string()))
    target = Field(
        name="venue",
        arrow_type=pyarrow.struct(
            [("mic", pyarrow.string()), pyarrow.field("desk", pyarrow.string(), nullable=False)]
        ),
    )
    with pytest.raises(ValueError, match=r"'venue\.desk' is missing and not nullable"):
        target.cast_arrow_array(array)


def test_a_list_of_structs_casts_its_item() -> None:
    array = pyarrow.array(
        [[{"mic": "XPAR"}], None, []], type=pyarrow.list_(venue(mic=pyarrow.string()))
    )
    item = pyarrow.struct([("mic", pyarrow.large_string()), ("desk", pyarrow.string())])
    target = Field(
        name="legs", arrow_type=pyarrow.list_(pyarrow.field("item", item)), nullable=True
    )
    cast = target.cast_arrow_array(array)
    assert cast.type == target.arrow_type
    assert cast.to_pylist() == [[{"mic": "XPAR", "desk": None}], None, []]


def test_a_map_casts_both_halves() -> None:
    array = pyarrow.array(
        [[("a", {"n": 1})], None],
        type=pyarrow.map_(pyarrow.string(), venue(n=pyarrow.int64())),
    )
    target = Field(
        name="tags",
        arrow_type=pyarrow.map_(pyarrow.large_string(), venue(n=pyarrow.int32())),
        nullable=True,
    )
    cast = target.cast_arrow_array(array)
    assert cast.type == target.arrow_type
    assert cast.to_pylist() == [[("a", {"n": 1})], None]


def test_the_recursion_agrees_with_arrows_own_cast() -> None:
    """Where Arrow can do the same cast, the answer must be the same one."""
    array = pyarrow.array(
        [[{"mic": "XPAR"}], None], type=pyarrow.list_(venue(mic=pyarrow.string()))
    )
    target = Field(
        name="legs",
        arrow_type=pyarrow.list_(pyarrow.field("item", venue(mic=pyarrow.large_string()))),
        nullable=True,
    )
    assert target.cast_arrow_array(array).equals(array.cast(target.arrow_type))


# -- tables -----------------------------------------------------------------


def test_a_table_is_cast_batch_by_batch() -> None:
    table = pyarrow.Table.from_batches(
        [
            batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)]),
            batch_of(symbol=["B"], size=[2], day=[datetime.date(2026, 8, 15)]),
        ]
    )
    cast = Tick.FIELD.cast_arrow_table(table)
    assert cast.schema.equals(Tick.FIELD.into_arrow_schema())
    assert cast.num_rows == 2
    assert cast.column("size").type == pyarrow.int32()
    assert cast.column("venue").to_pylist() == [None, None], "the missing nullable column is filled"


def test_a_table_that_already_matches_is_handed_back() -> None:
    table = Tick.FIELD.cast_arrow_table(
        pyarrow.Table.from_batches(
            [batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)])]
        )
    )
    assert Tick.FIELD.cast_arrow_table(table) is table


def test_a_table_can_merge_the_columns_it_has() -> None:
    table = pyarrow.Table.from_batches(
        [batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)], desk=["EQ"])]
    )
    cast = Tick.FIELD.cast_arrow_table(table, merge_schema=True)
    assert cast.column_names == [*Tick.FIELD.names, "desk"]


def test_a_batch_can_merge_the_columns_it_has() -> None:
    batch = batch_of(symbol=["A"], size=[1], day=[datetime.date(2026, 8, 14)], desk=["EQ"])
    cast = Tick.FIELD.cast_arrow_batch(batch, merge_schema=True)
    assert cast.schema.names == [*Tick.FIELD.names, "desk"]
    assert cast.column("size").type == pyarrow.int32(), "shared columns stay the target's"


def test_the_module_functions_cover_batch_table_and_stream() -> None:
    target = pyarrow.schema([("size", pyarrow.int16()), ("symbol", pyarrow.string())])
    batch = batch_of(symbol=["A"], size=[7], extra=[1])
    table = pyarrow.Table.from_batches([batch])
    assert cast_batch(batch, target).schema.equals(target)
    assert cast_table(table, target).schema.equals(target)
    assert cast_reader(iter([batch]), target).read_all().num_rows == 1
