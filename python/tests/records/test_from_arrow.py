"""Building record classes back out of Arrow, and dumping declarations."""

import datetime
import json
import tomllib

import pyarrow
import pytest
import yaml

from rekep import Record
from rekep.models import Log

SCHEMA = pyarrow.schema(
    [
        pyarrow.field("symbol", pyarrow.string(), nullable=False),
        pyarrow.field(
            "qty",
            pyarrow.int32(),
            nullable=False,
            metadata={"description": "Signed quantity.", "unit": "lots"},
        ),
        pyarrow.field("note", pyarrow.string()),
        pyarrow.field(
            "venue",
            pyarrow.struct(
                [
                    pyarrow.field("mic", pyarrow.string(), nullable=False),
                    pyarrow.field("timeout", pyarrow.float64()),
                ]
            ),
            nullable=False,
        ),
        pyarrow.field(
            "tags",
            pyarrow.list_(pyarrow.field("item", pyarrow.string(), nullable=False)),
            nullable=False,
        ),
    ],
    metadata={"description": "One order."},
)


@pytest.fixture(scope="module")
def order() -> type[Record]:
    return Record.from_arrow_schema(SCHEMA, name="Order")


# -- from_arrow_schema ------------------------------------------------------


def test_projection_round_trips_losslessly(order: type[Record]) -> None:
    """Types, nullability and authored metadata survive; field ids are added.

    The source schema carries no ids, the projection always stamps them -- so
    the comparison is everything except the stamp.
    """
    projected = order.into_arrow_schema()
    assert projected.equals(SCHEMA, check_metadata=False)
    qty = projected.field("qty")
    assert qty.metadata[b"unit"] == b"lots"
    assert qty.metadata[b"description"] == b"Signed quantity."
    assert projected.metadata[b"description"] == b"One order."


def test_log_record_schema_round_trips_with_identity() -> None:
    """The clone takes name and namespace back from the schema metadata."""
    clone = Record.from_arrow_schema(Log.into_arrow_schema())
    assert clone.__name__ == "Log"
    assert clone.__module__ == "rekep.models.log"
    assert clone.into_arrow_schema().equals(Log.into_arrow_schema(), check_metadata=True)


def test_an_explicit_name_renames_the_clone() -> None:
    clone = Record.from_arrow_schema(Log.into_arrow_schema(), name="Copy")
    assert clone.__name__ == "Copy"
    schema = clone.into_arrow_schema()
    assert schema.metadata[b"name"] == b"Copy", "the projection follows the new identity"


def test_generated_class_is_a_usable_record(order: type[Record]) -> None:
    row = order.from_dict({"symbol": "AAA", "qty": 5, "venue": {"mic": "XPAR"}, "tags": ["a"]})
    assert row.symbol == "AAA"
    assert row.note is None, "nullable fields default to None"
    assert type(row.venue).__name__ == "Venue", "nested structs become nested records"
    assert order.from_json(row.into_json()) == row


def test_generated_fields_are_keyword_only(order: type[Record]) -> None:
    with pytest.raises(TypeError):
        order("AAA")


def test_exact_types_survive_via_overrides(order: type[Record]) -> None:
    assert order.into_arrow_schema().field("qty").type == pyarrow.int32()


def test_from_arrow_field_with_a_struct() -> None:
    generated = Record.from_arrow_field(SCHEMA.field("venue"))
    assert generated.__name__ == "Venue"
    assert generated.into_arrow_schema().names == ["mic", "timeout"]


def test_from_arrow_field_with_a_leaf() -> None:
    generated = Record.from_arrow_field(SCHEMA.field("qty"))
    assert generated.into_arrow_schema().field("qty").type == pyarrow.int32()


def test_from_dispatch_builds_from_a_schema() -> None:
    assert Record.from_(SCHEMA).into_arrow_schema().equals(SCHEMA)


# -- class-level dumps ------------------------------------------------------


def test_class_dump_is_the_declaration() -> None:
    described = Log.into_dict()
    assert described["name"] == "Log"
    assert "namespace" not in described, "where the class lives is Python's business"
    assert described["description"] == "One parsed line of a trading log."
    assert [f["name"] for f in described["fields"]] == list(Log.into_arrow_schema().names)


def test_class_dump_carries_type_nullability_and_metadata(order: type[Record]) -> None:
    fields = {f["name"]: f for f in order.into_dict()["fields"]}
    assert fields["qty"]["type"] == "int32"
    assert fields["qty"]["metadata"] == {"unit": "lots"}
    assert fields["qty"]["description"] == "Signed quantity."
    assert fields["note"]["nullable"] is True
    assert "nullable" not in fields["symbol"], "required is the default, not restated"


def test_class_dumps_in_all_three_formats() -> None:
    as_yaml = yaml.safe_load(Log.into_yaml())
    as_json = json.loads(Log.into_json())
    as_toml = tomllib.loads(Log.into_toml().decode())
    assert as_yaml == as_json == as_toml


def test_class_dump_writes_to_a_file(tmp_path) -> None:
    path = tmp_path / "log_record.schema.yaml"
    assert Log.into_yaml(path) is None
    assert yaml.safe_load(path.read_bytes())["name"] == "Log"


def test_instance_dump_is_still_values() -> None:
    row = Log(
        url="u",
        unix=1,
        date=datetime.date(2026, 8, 14),
        time=datetime.time(0, 5, 1),
        thread_name="t",
        driver="d",
        message="m",
        hash64=2,
    )
    assert row.into_dict() == {
        "url": "u",
        "unix": 1,
        "date": "2026-08-14",
        "time": "00:05:01",
        "thread_name": "t",
        "driver": "d",
        "message": "m",
        "hash64": 2,
    }
