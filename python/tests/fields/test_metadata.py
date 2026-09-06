"""Rekep's generic metadata adapters over Yggdryl's native field."""

from typing import Annotated

import pyarrow
import yggdryl

from rekep import Field
from rekep.fields import partition_key, sort_key


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
        Annotated[str, partition_key(False, derived_from="raw"), sort_key(False)],
    )
    assert not built.iceberg.get("partition_key")
    assert not built.iceberg.get("derived_from")
    assert not built.iceberg.get("sort_key")
