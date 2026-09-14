"""The bundled FIX dictionary and native FIX public surface."""

from __future__ import annotations

import datetime
import os
from pathlib import Path
from typing import Any

import pyarrow
from yggdryl import IOBase
from yggdryl.fix import (
    PLUGIN_DIALECT,
    PLUGINCONFIG_CODE_NAME,
    FixCodec,
    FixLifecycle,
    FixMessages,
    FixMsg,
    FixRegistry,
    MsgType,
    Plugin,
    Plugins,
    fix_cfb_fields,
    fix_crate_fields,
    fix_plugin_fields,
    fix_plugin_message,
    fix_schema,
    fix_schema_carrying,
    fix_schema_tags,
    global_registry,
    install_global_registry,
)

from rekep.fields import PRIMARY_KEY, Field

_REGISTRY_PATH = Path(__file__).with_name("_data") / "fix"

#: The column a message's own identity is published as, and the third member
#: of the fixed table's key: a capture line answers one row per message, so
#: its URL and row number alone no longer name one.
MESSAGE_KEY = "uuid"

#: The column the codec reads a stated `SendingTime(52)` from, and the
#: precision it reads it at.
SENDING_TIME = "sendingtime"
SENDING_TIME_TYPE = pyarrow.timestamp("ns", tz="UTC")

#: What dates a message no clock reached at all: neither its own `SendingTime`
#: nor a capture clock, because the line carried no header the reader matched.
#: An instant is what the field holds, so it holds the one that means none --
#: reproducibly, which is the whole point of dating it here.
UNDATED = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)


def registry_path() -> Path:
    """The specification dictionary shipped inside this installation."""
    if not _REGISTRY_PATH.is_dir():
        raise FileNotFoundError(f"rekep FIX registry is absent: {_REGISTRY_PATH}")
    return _REGISTRY_PATH


def _load_registry(location: Any) -> FixRegistry:
    """Load one registry location and add the bridge vocabulary."""
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
    # A store that defined nothing loads as a bare registry: the crate's own
    # scalars and the two standard clocks it seeds beside them.
    if len(dictionary) == len(FixRegistry()):
        raise ValueError(f"FIX registry contains no specification fields: {location}")
    dictionary.with_plugin_fields()
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
    """The bundled registry, or one explicit dictionary with bridge fields."""
    return _DEFAULT_REGISTRY if location is None else _load_registry(location)


def fix_codec(registry: FixRegistry | None = None, **pinned: Any) -> FixCodec:
    """One codec over a dictionary, pinned for the whole run it reads.

    The codec is the whole parse surface, so every reader a task composes is
    built here rather than configured per call. A dialect is not among the
    pins: the registry is one namespace.
    """
    return FixCodec(registry or fix_registry(), **pinned)


def dated_arrow_reader(
    source: pyarrow.RecordBatchReader,
    clock: str = "timestamp",
) -> pyarrow.RecordBatchReader:
    """`source` with its capture clock offered as the message's `SendingTime`.

    A message carrying no `SendingTime(52)` is otherwise dated by the instant
    the parse ran, and its computed identity is then a different one on every
    read. A column named after a field outranks that default, so the instant
    the capture recorded dates the message it carried and one line answers the
    same identities however often it is replayed. The column clashes with the
    FIX column of that name, so it lands there rather than beside it.

    The column is filled, never merely cast: a line whose header the reader did
    not match carries no clock either, and a null would hand that message back
    to the same unrepeatable default. `UNDATED` is what those rows state.
    """
    if clock not in source.schema.names or SENDING_TIME in source.schema.names:
        return source
    stamped = pyarrow.field(SENDING_TIME, SENDING_TIME_TYPE, nullable=False)
    schema = source.schema.append(stamped)
    undated = pyarrow.scalar(UNDATED, SENDING_TIME_TYPE)

    def _dated():
        for batch in source:
            yield batch.append_column(
                stamped,
                pyarrow.compute.fill_null(batch.column(clock).cast(SENDING_TIME_TYPE), undated),
            )

    return pyarrow.RecordBatchReader.from_batches(schema, _dated())


def iceberg_fix_field(schema: pyarrow.Schema, name: str = "FixMessage") -> Field:
    """A parser schema narrowed to what Iceberg v2 stores: precision, and identity.

    The message's own `uuid` joins the carrier's key here: the codec answers
    one row per message rather than one per line, so a line holding two frames
    would otherwise publish two rows under one identity.

    That key is stored as the sixteen bytes it is. A UUID reaches Arrow as the
    canonical `arrow.uuid` extension, which carries no equality, ordering or
    grouping kernel, and a row filter is lowered to exactly those kernels -- so
    a column of that type can appear in no predicate, including the one a merge
    deletes by. Stored as its own storage type it names itself in every one.
    """
    members = [_keyed(member.with_type(_stored(member.type))) for member in schema]
    return Field.from_arrow_schema(pyarrow.schema(members), name=name)


def _keyed(member: pyarrow.Field) -> pyarrow.Field:
    """`member` marked as part of the table identity where it names one."""
    if member.name != MESSAGE_KEY:
        return member
    held = {key.decode(): value.decode() for key, value in (member.metadata or {}).items()}
    return member.with_metadata({**held, PRIMARY_KEY: "true"})


def fix_message_field(
    codec: FixCodec | None = None,
    carrier: Field | None = None,
    *,
    name: str = "FixMessage",
) -> Field:
    """The complete fixed table field without consuming an input row.

    The codec answers its schema from the carrier and the dictionary alone, so
    an empty reader of the carrier's schema is all it takes to publish one.
    """
    if carrier is None:
        from rekep.text import Message

        carrier = Message.into_field()
    source = pyarrow.RecordBatchReader.from_batches(carrier.into_arrow_schema(), [])
    dated = dated_arrow_reader(source)
    parsed = (codec or fix_codec()).parse_text_arrow_reader(dated)
    try:
        return iceberg_fix_field(parsed.schema, name)
    finally:
        parsed.close()
        dated.close()
        source.close()


def _stored(dtype: pyarrow.DataType) -> pyarrow.DataType:
    """Recursively narrow a parsed type to the one Iceberg v2 stores."""
    if isinstance(dtype, pyarrow.BaseExtensionType):
        return _stored(dtype.storage_type)
    if pyarrow.types.is_timestamp(dtype) and dtype.unit == "ns":
        return pyarrow.timestamp("us", tz=dtype.tz)
    if pyarrow.types.is_list(dtype):
        item = dtype.field(0)
        return pyarrow.list_(item.with_type(_stored(item.type)))
    if pyarrow.types.is_large_list(dtype):
        item = dtype.field(0)
        return pyarrow.large_list(item.with_type(_stored(item.type)))
    if pyarrow.types.is_map(dtype):
        return pyarrow.map_(_stored(dtype.key_type), _stored(dtype.item_type))
    if pyarrow.types.is_struct(dtype):
        return pyarrow.struct([member.with_type(_stored(member.type)) for member in dtype])
    return dtype


__all__ = [
    "MESSAGE_KEY",
    "PLUGINCONFIG_CODE_NAME",
    "PLUGIN_DIALECT",
    "SENDING_TIME",
    "SENDING_TIME_TYPE",
    "UNDATED",
    "FixCodec",
    "FixLifecycle",
    "FixMessages",
    "FixMsg",
    "FixRegistry",
    "MsgType",
    "Plugin",
    "Plugins",
    "dated_arrow_reader",
    "fix_cfb_fields",
    "fix_codec",
    "fix_crate_fields",
    "fix_message_field",
    "fix_plugin_fields",
    "fix_plugin_message",
    "fix_registry",
    "fix_schema",
    "fix_schema_carrying",
    "fix_schema_tags",
    "global_registry",
    "iceberg_fix_field",
    "install_global_registry",
    "registry_path",
]
