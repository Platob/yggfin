"""Rekep's generic metadata adapters over Yggdryl's native field."""

from collections.abc import Callable
from typing import Annotated

import pyarrow
import pytest
import yggdryl

from rekep import Field
from rekep.fields import PARTITION_KEY, derived_from, partition_key, sort_key
from rekep.iceberg import partition_keys


def make_field() -> Field:
    return Field("value", pyarrow.string(), True, {"description": "Stored value."})


def test_rekep_field_is_yggdryl_field() -> None:
    assert Field is yggdryl.Field
    assert type(make_field()) is yggdryl.Field


def test_native_metadata_is_live_and_survives_arrow_projection() -> None:
    built = make_field()
    built.metadata["source"] = "capture"
    assert built.into_arrow().metadata == {
        b"description": b"Stored value.",
        b"source": b"capture",
    }


def test_false_partition_and_sort_annotations_are_unset() -> None:
    built = Field.from_pyhint(
        "value",
        Annotated[str, partition_key(False), sort_key(False)],
    )
    assert not built.is_partition
    assert not built.iceberg.get("partition_key")
    assert not built.iceberg.get("sort_key")


def test_identity_partitions_use_the_native_field_projection() -> None:
    member = Field.from_pyhint("value", Annotated[str, partition_key()])
    root = Field.from_arrow_schema(pyarrow.schema([member.into_arrow()]), "Row")

    assert root.partition_field_names == ["value"]
    assert partition_keys(root) == {"value": "identity"}


@pytest.mark.parametrize(
    "declaration",
    [
        lambda: partition_key(metadata={PARTITION_KEY: "day"}),
        lambda: partition_key("day", metadata={"field:partition": "true"}),
    ],
)
def test_partition_annotation_rejects_the_opposite_physical_marker(
    declaration: Callable[[], object],
) -> None:
    with pytest.raises(ValueError, match=r"field:partition.*iceberg:partition_key"):
        declaration()


def test_conflicting_physical_partition_markers_are_rejected() -> None:
    member = pyarrow.field(
        "value",
        pyarrow.string(),
        metadata={"field:partition": "true", PARTITION_KEY: "day"},
    )
    root = Field.from_arrow_schema(pyarrow.schema([member]), "Row")

    with pytest.raises(ValueError, match="both identity and 'day'"):
        partition_keys(root)


def test_derived_annotation_uses_the_native_partition_protocol() -> None:
    built = Field.from_pyhint(
        "year",
        Annotated[int, derived_from("event", "dayofmonth")],
    )

    assert built.partition.sources == ["event"]
    assert built.partition.transform == "day"
    assert built.metadata["partition:sources"] == '["event"]'
    assert built.metadata["partition:transform"] == "day"
