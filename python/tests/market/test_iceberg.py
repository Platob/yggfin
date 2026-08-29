"""The market shapes through Iceberg: the projection, and a real round trip.

The projection is checked, and then a table is actually written and read back
-- because a type that projects is not a type that stores. `fixed[16]` was
chosen over Iceberg's own `uuid` on the strength of that round trip, so the
round trip is the test.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fields import StructField
from rekep.market import HASH, Book, Execution, Instrument, MarketEvent, Order
from rekep.market.event import SECOND

from ..conftest import catalog_properties
from .conftest import UNIX, batch

pytest.importorskip("pyiceberg")

SHAPES = (MarketEvent, Order, Execution, Book, Instrument)


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_every_shape_projects_onto_iceberg(shape: type) -> None:
    schema = shape.into_field().into_iceberg_schema()
    assert [member.name for member in schema.fields] == shape.into_field().names


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_the_primary_key_becomes_the_identifier_fields(shape: type) -> None:
    schema = shape.into_field().into_iceberg_schema()
    assert schema.identifier_field_ids == [
        schema.find_field(name).field_id for name in shape.into_field().primary_keys()
    ]


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_every_column_comment_travels(shape: type) -> None:
    schema = shape.into_field().into_iceberg_schema()
    for member in shape.into_field().fields:
        assert schema.find_field(member.name).doc == member.description, member.name


def test_hash_widths_are_preserved_in_every_engine() -> None:
    schema = MarketEvent.into_field().into_iceberg_schema()
    assert str(schema.find_field("hash").field_type) == "fixed[16]"
    assert str(schema.find_field("vhash").field_type) == "long"
    assert str(schema.find_field("xhash").field_type) == "long"
    assert str(schema.find_field("instrumentxhash").field_type) == "long"
    back = StructField.from_iceberg_schema(schema)
    assert back.field("hash").dtype == HASH
    assert back.field("vhash").dtype == pyarrow.int64()


def test_a_stable_code_is_a_plain_iceberg_integer() -> None:
    """A code is an integer to Iceberg, as wide as its own declaration packs."""
    schema = MarketEvent.into_field().into_iceberg_schema()
    assert str(schema.find_field("state").field_type) == "long", "State packs eight bytes"
    assert str(schema.find_field("side").field_type) == "int", "Side packs four"


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_a_nested_key_is_not_an_identifier_field(shape: type) -> None:
    """Iceberg only takes top-level identifier fields, so nothing must offer it one."""
    schema = shape.into_field().into_iceberg_schema()
    for field_id in schema.identifier_field_ids:
        assert "." not in (schema.find_column_name(field_id) or "")


def test_the_partition_is_the_hour_and_only_the_hour() -> None:
    """An identity every engine reads alike. The instrument is not one: bucketing a
    64-bit hash multiplies the files in each hour without pruning anything more."""
    schema = Order.into_field().into_iceberg_schema()
    spec = Order.into_field().into_iceberg_partition_spec(schema)
    assert [partition.name for partition in spec.fields] == ["unixpartition"]
    assert str(spec.fields[0].transform) == "identity"
    assert schema.find_column_name(spec.fields[0].source_id) == "unixpartition"
    assert str(schema.find_field("unixpartition").field_type) == "int"
    for partition in spec.fields:
        assert "[" not in partition.name, "a partition name becomes a directory name"


def test_the_table_is_laid_out_in_time_inside_the_partition() -> None:
    """The partition scope precedes the event time used for range pruning."""
    schema = Order.into_field().into_iceberg_schema()
    order = Order.into_field().into_iceberg_sort_order(schema)
    assert [schema.find_column_name(field.source_id) for field in order.fields] == [
        "unixpartition",
        "unix",
    ]
    assert all(str(field.transform) == "identity" for field in order.fields)
    assert all(field.direction.name.lower() == "asc" for field in order.fields)


@pytest.mark.parametrize("shape", (Order, Book), ids=lambda cls: cls.__name__)
def test_a_batch_written_to_a_table_comes_back_as_it_went_in(shape: type, tmp_path: Path) -> None:
    """The round trip the type choice was made on, through a real catalog."""
    from rekep.iceberg import IcebergDataset

    dataset = IcebergDataset(
        field=shape.into_field(f"market.{shape.__name__.lower()}"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    unixpartition = UNIX // SECOND
    given = batch(shape, 3, unixpartition=[unixpartition] * 3)
    # `append_`, not `overwrite_`: the fixture rows share a key, and what this
    # pins is the Arrow types surviving a round trip, not how rows are matched.
    dataset.append_arrow_table(pyarrow.Table.from_batches([given]))
    read = dataset.read_arrow_table()

    assert read.num_rows == 3
    assert read.column("hash").to_pylist() == given.column("hash").to_pylist()
    assert read.column("state").type == pyarrow.int64(), "the code kept its own width"
    assert read.column("side").type == pyarrow.int32(), "and a four-byte code stayed narrow"
    assert read.column("unixpartition").type == pyarrow.int32()
    assert read.column("hash").type == HASH, "and the identifier kept its stored width"
    written = [task.file for task in dataset.iceberg_table.scan().plan_files()]
    assert {one.partition[0] for one in written} == {unixpartition}
    assert all(f"unixpartition={unixpartition}" in one.file_path for one in written), (
        "the second-scale partition value keeps paths compact"
    )


def test_a_book_keeps_its_levels_and_its_flat_sides_through_a_write(tmp_path: Path) -> None:
    """A list of structs is storable; the sides beside it stay flat and prunable."""
    from rekep.iceberg import IcebergDataset

    dataset = IcebergDataset(
        field=Book.into_field("market.books"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    levels = [
        [{"px": 10.0, "qty": 5.0}],
        [{"px": 11.0, "qty": 6.0}],
    ]
    given = Book.summarise_arrow_batch(
        batch(Book, 2, snapunix=[1, 2], bidlevels=levels, asklevels=levels)
    )
    # `append_`, not `overwrite_`: the fixture rows share a key, and what this
    # pins is the Arrow types surviving a round trip, not how rows are matched.
    dataset.append_arrow_table(pyarrow.Table.from_batches([given]))
    read = dataset.read_arrow_table()

    assert read.num_rows == 2
    assert read.column("bidpx").to_pylist() == [10.0, 11.0], "derived, written and read back"
    assert read.column("biddepth").type == pyarrow.int32()
    assert pyarrow.types.is_list(
        read.schema.field("bidlevels").type
    ) or pyarrow.types.is_large_list(read.schema.field("bidlevels").type)
    assert read.column("bidlevels")[0][0]["qty"].as_py() == 5.0


def test_the_metrics_budget_covers_every_flat_column_of_a_book() -> None:
    """The reason the sides were unnested, on the *Iceberg* projection rather
    than the Arrow one: the derived prices have to arrive as top-level scalars,
    because Iceberg writes no bounds at all under a list or a map -- a nested
    one prunes nothing however much budget is left.

    The budget itself is counted in **leaves**, in pre-order, and where each
    filtered column falls in that walk is what `test_shapes.py` pins. What a
    projection's own width says is the half that is necessary here: one wider
    than the 100 leaves the budget stops at could not have kept the last of
    these columns inside it.
    """
    schema = Book.into_field().into_iceberg_schema()
    flat = [
        member.name
        for member in schema.fields
        if not str(member.field_type).startswith(("list", "map", "struct"))
    ]
    assert {"spread", "vwap", "execpx", "imbalance", "bidpx", "askpx"} <= set(flat)
    assert len(schema.fields) < 100, "no more top-level fields than the budget counts leaves"
