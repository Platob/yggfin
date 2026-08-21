"""`Log`'s own contract; the parser that fills it is tested beside it."""

import datetime

import pyarrow

from rekep import Field, Log

EXPECTED_COLUMNS = [
    "url",
    "recorded_at_unix",
    "recorded_at_date",
    "recorded_at_time",
    "thread_name",
    "driver_name",
    "category_id",
    "category_name",
    "message",
    "hash64",
]


def test_columns_in_declaration_order() -> None:
    assert Log.FIELD.into_arrow_schema().names == EXPECTED_COLUMNS


def test_every_column_is_required() -> None:
    assert all(not member.nullable for member in Log.FIELD.fields)


def test_every_column_is_documented() -> None:
    for member in Log.FIELD.fields:
        assert member.description, f"{member.name} has no description"
        assert "\n" not in member.description, f"{member.name} description is not one line"


def test_the_key_is_the_moment_and_the_line() -> None:
    """Two columns: a hash identifies the line, the time is what an engine prunes on."""
    assert Log.FIELD.primary_keys() == ["recorded_at_unix", "hash64"]
    assert Log.FIELD.partition_keys() == {"recorded_at_date": "identity"}


def test_recorded_at_unix_declares_its_unit() -> None:
    metadata = Log.FIELD.field("recorded_at_unix").metadata
    assert metadata["unit"] == "nanosecond"
    assert metadata["epoch"] == "1970-01-01"


def test_wide_columns_are_int64_not_smaller() -> None:
    for name in ("recorded_at_unix", "hash64"):
        assert Log.FIELD.field(name).arrow_type == pyarrow.int64()


def test_the_schema_says_which_class_it_came_from() -> None:
    schema = Log.FIELD.into_arrow_schema()
    assert schema.metadata[b"name"] == b"Log"
    assert Field.from_arrow_schema(schema) == Log.FIELD


def test_a_row_round_trips_as_a_document() -> None:
    row = Log(
        url="a.txt",
        recorded_at_unix=2,
        recorded_at_date=datetime.date(2026, 8, 14),
        recorded_at_time=datetime.time(0, 5, 1, 147_250),
        thread_name="t",
        driver_name="d",
        category_id=0,
        category_name="",
        message="m",
        hash64=3,
    )
    assert Log.from_json(row.into_json()) == row
