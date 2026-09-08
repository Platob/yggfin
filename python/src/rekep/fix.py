"""The bundled FIX dictionary and native FIX public surface."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pyarrow
from yggdryl import IOBase
from yggdryl.fix import (
    STANDARD_BRANCH,
    ULBRIDGE_BRANCH,
    USER_TAG_MAX,
    USER_TAG_MIN,
    FixBranch,
    FixCodec,
    FixMsg,
    FixRegistry,
    UlPlugin,
    classify_arrow_array,
    fix_cfb_fields,
    fix_crate_fields,
    fix_schema,
    fix_schema_carrying,
    fix_schema_tags,
    fix_ulbridge_fields,
    global_registry,
    install_global_registry,
    parse_arrow_reader,
)

from rekep.fields import Field

_REGISTRY_PATH = Path(__file__).with_name("_data") / "fix"


def registry_path() -> Path:
    """The specification dictionary shipped inside this installation."""
    if not _REGISTRY_PATH.is_dir():
        raise FileNotFoundError(f"rekep FIX registry is absent: {_REGISTRY_PATH}")
    return _REGISTRY_PATH


def _load_registry(location: Any) -> FixRegistry:
    """Load one registry location and add the capture vocabulary."""
    owned = None
    handle = location
    if not isinstance(location, IOBase):
        owned = IOBase.from_uri(os.fspath(location))
        handle = owned
    try:
        dictionary = FixRegistry.from_handle(handle)
    finally:
        if owned is not None:
            owned.close()
    if len(dictionary) == len(fix_crate_fields()):
        raise ValueError(f"FIX registry contains no specification fields: {location}")
    dictionary.with_ulbridge_fields()
    return dictionary


_DEFAULT_REGISTRY = _load_registry(registry_path())
try:
    install_global_registry(_DEFAULT_REGISTRY)
except ValueError:
    # A host may deliberately install its process registry before importing
    # rekep. Keep that explicit choice; rekep's own default remains available
    # through ``fix_registry()`` and is passed by every bundled task.
    pass


def fix_registry(location: str | os.PathLike[str] | None = None) -> FixRegistry:
    """The bundled registry, or one explicit dictionary with capture fields."""
    return _DEFAULT_REGISTRY if location is None else _load_registry(location)


def iceberg_fix_field(schema: pyarrow.Schema, name: str = "FixMessage") -> Field:
    """A parser schema narrowed to the timestamp precision Iceberg v2 stores."""
    members = [member.with_type(_microseconds(member.type)) for member in schema]
    return Field.from_arrow_schema(pyarrow.schema(members), name=name)


def fix_message_field(
    registry: FixRegistry | None = None,
    carrier: Field | None = None,
    *,
    name: str = "FixMessage",
) -> Field:
    """The complete fixed table field without consuming an input row."""
    if carrier is None:
        from rekep.text import Message

        carrier = Message.field()
    source = pyarrow.RecordBatchReader.from_batches(carrier.into_arrow_schema(), [])
    parsed = parse_arrow_reader(source, registry or fix_registry(), "body", branch=ULBRIDGE_BRANCH)
    try:
        return iceberg_fix_field(parsed.schema, name)
    finally:
        parsed.close()
        source.close()


def _microseconds(dtype: pyarrow.DataType) -> pyarrow.DataType:
    """Recursively narrow nanosecond timestamps for Iceberg v2."""
    if pyarrow.types.is_timestamp(dtype) and dtype.unit == "ns":
        return pyarrow.timestamp("us", tz=dtype.tz)
    if pyarrow.types.is_list(dtype):
        item = dtype.field(0)
        return pyarrow.list_(item.with_type(_microseconds(item.type)))
    if pyarrow.types.is_large_list(dtype):
        item = dtype.field(0)
        return pyarrow.large_list(item.with_type(_microseconds(item.type)))
    if pyarrow.types.is_struct(dtype):
        return pyarrow.struct([member.with_type(_microseconds(member.type)) for member in dtype])
    return dtype


__all__ = [
    "STANDARD_BRANCH",
    "ULBRIDGE_BRANCH",
    "USER_TAG_MAX",
    "USER_TAG_MIN",
    "FixBranch",
    "FixCodec",
    "FixMsg",
    "FixRegistry",
    "UlPlugin",
    "classify_arrow_array",
    "fix_cfb_fields",
    "fix_crate_fields",
    "fix_message_field",
    "fix_registry",
    "fix_schema",
    "fix_schema_carrying",
    "fix_schema_tags",
    "fix_ulbridge_fields",
    "global_registry",
    "iceberg_fix_field",
    "parse_arrow_reader",
    "registry_path",
]
