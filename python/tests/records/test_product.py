"""Whole-definition dumps: the class-level serialisers, nesting included."""

import datetime
import json
import tomllib
from dataclasses import field
from typing import Annotated

import pytest
import yaml

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

    day: Annotated[datetime.date, Arrow(partition=True)]
    """Trading day."""

    venues: list[Venue] = field(default_factory=list)
    """Where the book trades."""

    limits: dict[str, int] = field(default_factory=dict)
    """Named exposure limits."""

    parent: Venue | None = None
    """Primary venue, when there is one."""


@pytest.fixture(scope="module")
def dumped() -> dict:
    return yaml.safe_load(Book.into_yaml())


def by_name(fields: list[dict]) -> dict[str, dict]:
    return {entry["name"]: entry for entry in fields}


# -- the envelope -----------------------------------------------------------


def test_envelope(dumped: dict) -> None:
    assert dumped["name"] == "Book"
    assert "namespace" not in dumped
    assert dumped["description"] == "A book of orders."
    assert [entry["name"] for entry in dumped["fields"]] == [
        "name",
        "day",
        "venues",
        "limits",
        "parent",
    ]


def test_yaml_is_the_default_and_all_formats_agree() -> None:
    as_yaml = yaml.safe_load(Book.into_yaml())
    assert json.loads(Book.into_json()) == as_yaml
    assert tomllib.loads(Book.into_toml().decode()) == as_yaml


# -- nesting ----------------------------------------------------------------


def test_a_nested_record_is_a_fields_block_not_a_type_string(dumped: dict) -> None:
    parent = by_name(dumped["fields"])["parent"]
    assert parent["type"] == "struct"
    assert parent["nullable"] is True
    assert "struct<" not in str(parent), "no flat struct<...> strings"
    inner = by_name(parent["fields"])
    assert inner["mic"]["description"] == "ISO 10383 market identifier."
    assert inner["timeout"]["nullable"] is True


def test_a_list_of_records_nests_through_item(dumped: dict) -> None:
    venues = by_name(dumped["fields"])["venues"]
    assert venues["type"] == "list"
    assert venues["item"]["type"] == "struct"
    inner = by_name(venues["item"]["fields"])
    assert inner["timeout"]["description"] == "Seconds before giving up on a quote."


def test_a_map_nests_through_key_and_value(dumped: dict) -> None:
    limits = by_name(dumped["fields"])["limits"]
    assert limits["type"] == "map"
    assert limits["key"]["type"] == "string"
    assert limits["value"]["type"] == "int64"


def test_field_ids_are_dumped_at_every_level(dumped: dict) -> None:
    fields = by_name(dumped["fields"])
    assert [entry["iceberg"]["field_id"] for entry in dumped["fields"]] == [1, 2, 3, 4, 5]
    item = fields["venues"]["item"]
    assert item["iceberg"]["field_id"] == 6
    assert [f["iceberg"]["field_id"] for f in item["fields"]] == [7, 8]
    assert fields["limits"]["key"]["iceberg"]["field_id"] == 9
    assert fields["limits"]["value"]["iceberg"]["field_id"] == 10


def test_partition_shorthand_groups_under_iceberg(dumped: dict) -> None:
    day = by_name(dumped["fields"])["day"]
    assert day["iceberg"]["partition_key"] is True
    assert "iceberg:partition_key" not in day.get("metadata", {})


def test_descriptions_reach_every_level(dumped: dict) -> None:
    fields = by_name(dumped["fields"])
    assert fields["name"]["description"] == "Human name of the book."
    assert fields["venues"]["description"] == "Where the book trades."


# -- the shipped product files ----------------------------------------------


def test_the_shipped_product_file_matches_the_class() -> None:
    """stacks/product must be regenerated when the model changes."""
    import pathlib

    shipped = pathlib.Path(__file__).parents[3] / "stacks" / "product" / "log.yaml"
    assert yaml.safe_load(shipped.read_bytes()) == yaml.safe_load(Log.into_yaml())
