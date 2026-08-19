import datetime
import pathlib
from typing import Annotated

import pyarrow
import pytest
from pyiceberg import types as iceberg

from rekep import Arrow, Record, record
from rekep.models import Log


@record
class Venue(Record):
    """A trading venue."""

    mic: str
    """ISO 10383 market identifier."""

    timeout: float | None = None
    """Seconds before giving up on a quote."""


@record
class Book(Record):
    """A book of orders."""

    name: str
    """Human name of the book."""

    opened: datetime.date
    size: Annotated[int, Arrow(type=pyarrow.int32())]
    venues: list[Venue] = ()
    root: pathlib.Path | None = None


@pytest.fixture(scope="module")
def schema() -> object:
    return Book.into_iceberg_schema()


# -- structure --------------------------------------------------------------


def test_schema_carries_every_field_in_order(schema) -> None:
    assert [f.name for f in schema.fields] == ["name", "opened", "size", "venues", "root"]


def test_field_ids_are_unique_and_positive(schema) -> None:
    ids = list(schema.field_ids)
    assert len(ids) == len(set(ids))
    assert all(field_id > 0 for field_id in ids), "-1 placeholders must not survive"


def test_types_map_through_arrow(schema) -> None:
    by_name = {f.name: f for f in schema.fields}
    assert isinstance(by_name["name"].field_type, iceberg.StringType)
    assert isinstance(by_name["opened"].field_type, iceberg.DateType)
    assert isinstance(by_name["size"].field_type, iceberg.IntegerType), "int32 override survives"
    assert isinstance(by_name["venues"].field_type, iceberg.ListType)
    assert isinstance(by_name["root"].field_type, iceberg.StringType)


def test_nullability_becomes_required(schema) -> None:
    by_name = {f.name: f for f in schema.fields}
    assert by_name["name"].required
    assert not by_name["root"].required


def test_nested_record_becomes_a_documented_struct(schema) -> None:
    venue = {f.name: f for f in schema.fields}["venues"].field_type.element_type
    assert isinstance(venue, iceberg.StructType)
    inner = {f.name: f for f in venue.fields}
    assert inner["mic"].doc == "ISO 10383 market identifier."
    assert inner["mic"].required
    assert not inner["timeout"].required


# -- documentation ----------------------------------------------------------


def test_descriptions_become_docs() -> None:
    schema = Log.into_iceberg_schema()
    for field in schema.fields:
        assert field.doc, f"{field.name} lost its description"


def test_docs_match_the_arrow_descriptions() -> None:
    arrow, ice = Log.into_arrow_schema(), Log.into_iceberg_schema()
    for field in ice.fields:
        assert field.doc == arrow.field(field.name).metadata[b"description"].decode()


# -- the field entry point --------------------------------------------------


def test_into_iceberg_field(schema) -> None:
    field = Book.into_iceberg_field()
    assert field.name == "Book"
    assert field.required
    assert field.doc == "A book of orders."
    assert isinstance(field.field_type, iceberg.StructType)

    named = Book.into_iceberg_field("book", field_id=7, required=False)
    assert (named.name, named.field_id, named.required) == ("book", 7, False)


def test_into_iceberg_type_is_the_schema_struct(schema) -> None:
    assert Book.into_iceberg_type() == schema.as_struct()


# -- caching ----------------------------------------------------------------


def test_iceberg_projections_are_cached() -> None:
    assert Book.into_iceberg_schema() is Book.into_iceberg_schema()
    assert Book.into_iceberg_field("a") is Book.into_iceberg_field("a")
    assert Book.into_iceberg_field("a") is not Book.into_iceberg_field("b")


# -- round trip through pyiceberg's own converter ---------------------------


def test_schema_survives_pyiceberg_arrow_round_trip() -> None:
    """pyiceberg must accept our schema as one of its own."""
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    back = schema_to_pyarrow(Log.into_iceberg_schema(), include_field_ids=False)
    ours = Log.into_arrow_schema()
    assert back.names == ours.names
    assert [f.nullable for f in back] == [f.nullable for f in ours]
