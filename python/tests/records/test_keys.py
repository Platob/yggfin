"""Partition keys and primary keys, across every projection."""

import datetime
from typing import Annotated

import pytest
import yaml

from rekep import Arrow, Record, record


@record
class Fill(Record):
    """One fill."""

    day: Annotated[datetime.date, Arrow(partition="day", key=True)]
    """Trading day."""

    order_id: Annotated[str, Arrow(key=True)]
    """Exchange order id."""

    account: Annotated[str, Arrow(partition="bucket[16]")]
    """Account code."""

    venue: Annotated[str, Arrow(partition=True)]
    """Where it traded."""

    qty: int
    """Signed quantity."""


# -- declaration ------------------------------------------------------------


def test_flags_land_as_protocol_metadata() -> None:
    schema = Fill.into_arrow_schema()
    assert schema.field("day").metadata[b"iceberg:partition_key"] == b"day"
    assert schema.field("day").metadata[b"iceberg:primary_key"] == b"true"
    assert schema.field("venue").metadata[b"iceberg:partition_key"] == b"identity"
    assert b"iceberg:partition_key" not in (schema.field("qty").metadata or {})


def test_the_iceberg_mapping_shorthand_still_works() -> None:
    @record
    class Short(Record):
        day: Annotated[str, Arrow(iceberg={"partition_key": "true"})]

    assert Short.into_arrow_schema().field("day").metadata[b"iceberg:partition_key"] == b"true"


def test_a_nullable_primary_key_is_refused() -> None:
    @record
    class Bad(Record):
        maybe: Annotated[str | None, Arrow(key=True)]

    with pytest.raises(TypeError, match="primary key"):
        Bad.into_arrow_schema()


# -- iceberg ----------------------------------------------------------------


def test_keys_become_identifier_fields() -> None:
    assert Fill.into_iceberg_schema().identifier_field_names() == {"day", "order_id"}


def test_partition_spec_carries_transforms_and_spec_ids() -> None:
    spec = Fill.into_iceberg_partition_spec()
    by_name = {field.name: field for field in spec.fields}
    assert set(by_name) == {"day", "account", "venue"}
    assert str(by_name["day"].transform) == "day"
    assert str(by_name["account"].transform) == "bucket[16]"
    assert str(by_name["venue"].transform) == "identity"
    assert [field.field_id for field in spec.fields] == [1000, 1001, 1002]


def test_partition_sources_are_schema_field_ids() -> None:
    schema = Fill.into_iceberg_schema()
    ids = {field.name: field.field_id for field in schema.fields}
    spec = Fill.into_iceberg_partition_spec()
    assert {field.name: field.source_id for field in spec.fields} == {
        "day": ids["day"],
        "account": ids["account"],
        "venue": ids["venue"],
    }


def test_a_record_without_declarations_has_empty_spec_and_keys() -> None:
    @record
    class Plain(Record):
        value: int

    assert not Plain.into_iceberg_partition_spec().fields
    assert Plain.into_iceberg_schema().identifier_field_names() == set()


# -- ddl --------------------------------------------------------------------


def test_ddl_partitions_spell_transforms() -> None:
    ddl = Fill.into_iceberg_ddl()
    assert "PARTITIONED BY (days(day), bucket(16, account), venue)" in ddl


def test_ddl_sets_identifier_fields() -> None:
    ddl = Fill.into_iceberg_ddl("fills")
    assert ddl.rstrip().endswith("ALTER TABLE fills SET IDENTIFIER FIELDS day, order_id;")


def test_explicit_partition_by_still_overrides() -> None:
    ddl = Fill.into_iceberg_ddl(partitioned_by=["venue"])
    assert "PARTITIONED BY (venue)" in ddl
    assert "days(day)" not in ddl


# -- product dump -----------------------------------------------------------


def test_dump_groups_protocol_keys_under_iceberg() -> None:
    fields = {entry["name"]: entry for entry in yaml.safe_load(Fill.into_yaml())["fields"]}
    assert fields["day"]["iceberg"]["partition_key"] == "day"
    assert fields["day"]["iceberg"]["primary_key"] is True
    assert fields["venue"]["iceberg"]["partition_key"] is True
    assert "partition" not in fields["qty"].get("iceberg", {})
    assert "iceberg:partition_key" not in fields["day"].get("metadata", {}), (
        "grouped, not flattened"
    )


# -- round trip -------------------------------------------------------------


def test_flags_survive_the_reverse_projection() -> None:
    clone = Record.from_arrow_schema(Fill.into_arrow_schema(), name="Clone")
    schema = clone.into_arrow_schema()
    assert schema.field("day").metadata[b"iceberg:partition_key"] == b"day"
    assert schema.field("day").metadata[b"iceberg:primary_key"] == b"true"
    assert clone.into_iceberg_schema().identifier_field_names() == {"day", "order_id"}
