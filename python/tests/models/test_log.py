"""Log's own contract; the parser that fills it is tested in logs/."""

import datetime

import pyarrow

from rekep.models import Log

EXPECTED_COLUMNS = ["url", "unix", "date", "time", "thread_name", "driver", "message", "hash64"]


def test_columns_in_declaration_order() -> None:
    assert Log.into_arrow_schema().names == EXPECTED_COLUMNS


def test_every_column_is_required() -> None:
    assert all(not field.nullable for field in Log.into_arrow_schema())


def test_every_column_is_documented() -> None:
    schema = Log.into_arrow_schema()
    for field in schema:
        description = (field.metadata or {}).get(b"description", b"").decode()
        assert description, f"{field.name} has no description"
        assert "\n" not in description, f"{field.name} description is not one line"


def test_unix_declares_its_unit() -> None:
    metadata = Log.into_arrow_schema().field("unix").metadata
    assert metadata[b"unit"] == b"nanosecond"
    assert metadata[b"epoch"] == b"1970-01-01"


def test_wide_columns_are_int64_not_smaller() -> None:
    schema = Log.into_arrow_schema()
    for name in ("unix", "hash64"):
        assert schema.field(name).type == pyarrow.int64()


def test_a_row_round_trips_as_a_record() -> None:
    row = Log(
        url="a.txt",
        unix=2,
        date=datetime.date(2026, 8, 14),
        time=datetime.time(0, 5, 1, 147_250),
        thread_name="t",
        driver="d",
        message="m",
        hash64=3,
    )
    assert Log.from_json(row.into_json()) == row
