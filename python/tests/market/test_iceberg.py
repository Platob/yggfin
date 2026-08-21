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
from rekep.market import Book, BookSide, Execution, Instrument, MarketEvent, Order
from rekep.market.identity import uuids_of

from .conftest import batch

pytest.importorskip("pyiceberg")

SHAPES = (MarketEvent, Order, Execution, BookSide, Book, Instrument)


def catalog_properties(tmp_path: Path) -> dict[str, str]:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(exist_ok=True)
    return {
        "type": "sql",
        "uri": f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
        "warehouse": warehouse.as_uri(),
    }


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_every_shape_projects_onto_iceberg(shape: type) -> None:
    schema = shape.FIELD.into_iceberg_schema()
    assert [member.name for member in schema.fields] == shape.FIELD.names


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_the_primary_key_becomes_the_identifier_fields(shape: type) -> None:
    schema = shape.FIELD.into_iceberg_schema()
    assert schema.identifier_field_ids == [
        schema.find_field(name).field_id for name in shape.FIELD.primary_keys()
    ]


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_every_column_comment_travels(shape: type) -> None:
    schema = shape.FIELD.into_iceberg_schema()
    for member in shape.FIELD.fields:
        assert schema.find_field(member.name).doc == member.description, member.name


def test_the_sixteen_bytes_stay_sixteen_bytes() -> None:
    """`fixed[16]`, not `uuid`: the reason the Python type is a UUID and the column is not."""
    schema = MarketEvent.FIELD.into_iceberg_schema()
    assert str(schema.find_field("hash").field_type) == "fixed[16]"
    assert str(schema.find_field("xhash").field_type) == "fixed[16]"
    back = StructField.from_iceberg_schema(schema)
    assert back.field("hash").arrow_type == pyarrow.binary(16)


def test_a_ranged_code_is_an_iceberg_int() -> None:
    schema = MarketEvent.FIELD.into_iceberg_schema()
    assert str(schema.find_field("state").field_type) == "int"
    assert str(schema.find_field("side").field_type) == "int"


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_a_nested_key_is_not_an_identifier_field(shape: type) -> None:
    """Iceberg only takes top-level identifier fields, so nothing must offer it one."""
    schema = shape.FIELD.into_iceberg_schema()
    for field_id in schema.identifier_field_ids:
        assert "." not in (schema.find_column_name(field_id) or "")


def test_the_partition_is_the_denormalised_day_with_an_identity_transform() -> None:
    """An identity partition on a real date column is what every engine reads alike."""
    schema = Order.FIELD.into_iceberg_schema()
    spec = Order.FIELD.into_iceberg_partition_spec(schema)
    assert [partition.name for partition in spec.fields] == ["date"]
    assert str(spec.fields[0].transform) == "identity"
    assert schema.find_column_name(spec.fields[0].source_id) == "date"


@pytest.mark.parametrize("shape", (Order, Book), ids=lambda cls: cls.__name__)
def test_a_batch_written_to_a_table_comes_back_as_it_went_in(shape: type, tmp_path: Path) -> None:
    """The round trip the type choice was made on, through a real catalog."""
    from rekep.iceberg import IcebergDataset

    dataset = IcebergDataset(
        name=f"market.{shape.__name__.lower()}",
        catalog="test",
        properties=catalog_properties(tmp_path),
        struct=shape.FIELD,
    )
    given = batch(shape, 3)
    dataset.write_arrow_table(pyarrow.Table.from_batches([given]))
    read = dataset.read_arrow_table()

    assert read.num_rows == 3
    assert uuids_of(read.column("hash").combine_chunks()) == uuids_of(given.column("hash"))
    assert read.column("state").type == pyarrow.int32(), "the code stayed narrow"
    assert read.column("hash").type == pyarrow.binary(16), "and the identifier stayed fixed"


def test_a_book_keeps_its_levels_and_its_flat_sides_through_a_write(tmp_path: Path) -> None:
    """A list of structs is storable; the sides beside it stay flat and prunable."""
    from rekep.iceberg import IcebergDataset

    dataset = IcebergDataset(
        name="market.books",
        catalog="test",
        properties=catalog_properties(tmp_path),
        struct=Book.FIELD,
    )
    levels = [[{"px": 10.0, "qty": 5.0, "orders": 1}], [{"px": 11.0, "qty": 6.0, "orders": None}]]
    given = Book.summarise_arrow_batch(batch(Book, 2, bid_alive=levels, ask_alive=levels))
    dataset.write_arrow_table(pyarrow.Table.from_batches([given]))
    read = dataset.read_arrow_table()

    assert read.num_rows == 2
    assert read.column("bid_px").to_pylist() == [10.0, 11.0], "derived, written and read back"
    assert read.column("bid_depth").type == pyarrow.int32()
    assert pyarrow.types.is_list(
        read.schema.field("bid_alive").type
    ) or pyarrow.types.is_large_list(read.schema.field("bid_alive").type)
    assert read.column("bid_alive")[0][0]["qty"].as_py() == 5.0


def test_the_metrics_budget_covers_every_flat_column_of_a_book(tmp_path: Path) -> None:
    """The reason the sides were unnested, checked against Iceberg's own schema."""
    schema = Book.FIELD.into_iceberg_schema()
    flat = [
        member.name
        for member in schema.fields
        if not str(member.field_type).startswith(("list", "map", "struct"))
    ]
    assert {"spread", "micro_px", "imbalance", "bid_px", "ask_px"} <= set(flat)
    assert len(schema.fields) < 100, "every top-level field is inside the default budget"
