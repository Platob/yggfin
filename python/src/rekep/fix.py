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
    FixMessages,
    FixMsg,
    FixRegistry,
    MsgType,
    UlPlugin,
    fix_cfb_fields,
    fix_crate_fields,
    fix_schema,
    fix_schema_carrying,
    fix_schema_tags,
    fix_ulbridge_fields,
    global_registry,
    install_global_registry,
)

from rekep.fields import PRIMARY_KEY, Field

_REGISTRY_PATH = Path(__file__).with_name("_data") / "fix"

#: The payload column every bundled codec reads a line's bytes from. It is
#: `Message.body`, and naming it here keeps the codec and the raw contract
#: from drifting apart.
PAYLOAD_COLUMN = "body"

#: The parsed-message digest, which completes the fixed table's identity. One
#: captured line is one message everywhere except a bridge configuration
#: document, which states several -- so `(url, rownum)` alone stops being a key
#: exactly there, and a merge on it would keep one of them and drop the rest.
MESSAGE_DIGEST_COLUMN = "msghash"


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


def fix_codec(
    registry: FixRegistry | None = None,
    *,
    branch: str | None = ULBRIDGE_BRANCH,
    version: str | None = None,
) -> FixCodec:
    """One codec over the bundled registry, pinned to the capture dialect."""
    return FixCodec(
        registry or fix_registry(),
        branch=branch,
        version=version,
        payload_column=PAYLOAD_COLUMN,
    )


def iceberg_fix_field(schema: pyarrow.Schema, name: str = "FixMessage") -> Field:
    """A parser schema narrowed to the timestamp precision Iceberg v2 stores."""
    members = [_identified(member.with_type(_microseconds(member.type))) for member in schema]
    return Field.from_arrow_schema(pyarrow.schema(members), name=name)


def _identified(member: pyarrow.Field) -> pyarrow.Field:
    """Add the parsed-message digest to the carrier's declared identity."""
    if member.name != MESSAGE_DIGEST_COLUMN or member.nullable:
        return member
    metadata = {
        (key.decode() if isinstance(key, bytes) else key): (
            value.decode() if isinstance(value, bytes) else value
        )
        for key, value in (member.metadata or {}).items()
    }
    metadata[PRIMARY_KEY] = "true"
    return member.with_metadata(metadata)


def fix_message_field(
    registry: FixRegistry | None = None,
    carrier: Field | None = None,
    *,
    name: str = "FixMessage",
) -> Field:
    """The complete fixed table field without consuming an input row."""
    if carrier is None:
        from rekep.text import Message

        carrier = Message.into_field()
    source = pyarrow.RecordBatchReader.from_batches(carrier.into_arrow_schema(), [])
    parsed = fix_codec(registry).parse_text_arrow_reader(source)
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
    "PAYLOAD_COLUMN",
    "STANDARD_BRANCH",
    "ULBRIDGE_BRANCH",
    "USER_TAG_MAX",
    "USER_TAG_MIN",
    "FixBranch",
    "FixCodec",
    "FixMessages",
    "FixMsg",
    "FixRegistry",
    "MsgType",
    "UlPlugin",
    "fix_cfb_fields",
    "fix_codec",
    "fix_crate_fields",
    "fix_message_field",
    "fix_registry",
    "fix_schema",
    "fix_schema_carrying",
    "fix_schema_tags",
    "fix_ulbridge_fields",
    "global_registry",
    "iceberg_fix_field",
    "registry_path",
]
