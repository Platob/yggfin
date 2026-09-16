"""The bundled FIX dictionary and the native FIX pipeline surface.

The pipeline a bridge capture goes through is three native stages over one
codec, in this order and no other:

```text
parse -> enrich -> lifecycle
```

`parse` reads every frame a line carried; `enrich` fills what a message
implied but did not carry, and remembers the bridge configurations it passed
so a later message naming one takes its session pair; `lifecycle` names the
chains -- the `code` a message belongs to, its `updatedat` on the snapshot
grid, the `createdat` its chain opened at, and the `prevmsghash` linking it to
the message before it.

Each stage is a call rather than a pin, and each has two doors that the native
core requires to agree row for row: `fix_line_messages` reads lines one at a
time and `fix_arrow_messages` reads a whole stored capture in batches. A task
holding a table takes the batch door; a reader holding lines takes the line
door. Neither reimplements the other.
"""

from __future__ import annotations

import datetime
import os
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import pyarrow
from yggdryl import IOBase, TextLine, TextOptions
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
    fix_generic_message,
    fix_plugin_fields,
    fix_plugin_message,
    fix_schema,
    fix_schema_carrying,
    fix_schema_tags,
    global_registry,
    install_global_registry,
)

from rekep.fields import PRIMARY_KEY, Field
from rekep.times import ULBRIDGE_ROWHEADER, UTC

_REGISTRY_PATH = Path(__file__).with_name("_data") / "fix"

#: The column a message's own identity is published as, and the third member
#: of the fixed table's key: a capture line answers one row per message, so
#: its source URL and row number alone no longer name one. Sixteen ordered
#: bytes over the message's settled instant and its named content.
MESSAGE_KEY = "msghash"

#: The column the codec reads a stated `SendingTime(52)` from, and the
#: precision it reads it at.
SENDING_TIME = "sendingtime"
SENDING_TIME_TYPE = pyarrow.timestamp("ns", tz="UTC")

#: What dates a message no clock reached at all: neither its own `SendingTime`
#: nor a capture clock, because the line carried no header the reader matched.
#: An instant is what the field holds, so it holds the one that means none --
#: reproducibly, which is the whole point of dating it here.
UNDATED = datetime.datetime(1970, 1, 1, tzinfo=UTC)

#: What the codec accepts as a pin. Stated here because a pin that is not one
#: used to be forwarded silently: `version` was a legal pin until a version
#: became what a row states rather than what a caller chose, and the parse
#: went on answering rows under a dictionary nobody asked for.
CODEC_PINS = frozenset(
    {
        "default_sending_time",
        "separator",
        "payload_column",
        "capture_names",
        "null_values",
        "direction",
        "batch_byte_size",
    }
)

#: The Arrow schema metadata a semantic native datatype crosses on. Iceberg
#: stores the storage type, so a column carrying one is narrowed here rather
#: than refused at the table boundary where the reason is no longer legible.
_EXTENSION_KEYS = (b"ARROW:extension:name", b"ARROW:extension:metadata")


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


def fix_text_options(
    field: Field | None = None,
    rowheader: str | None = None,
) -> TextOptions:
    """The bridge text read every stage of this pipeline is pinned against.

    Every capture a row header declares is named for the field it fills, so
    `capture_names` alone is what tells the codec which bracket part is which
    and nothing maps a spelling onto a tag. `rowheader` reads a bridge writing
    those same facts in a layout of its own; the default is the one this
    package ships. A reader that also stores its rows takes the header through
    `Message.text_options`, which additionally checks the names against the
    columns that hold them.
    """
    options = TextOptions()
    options.start_rownum = 1
    options.parse_mtime = False
    options.rowheader = ULBRIDGE_ROWHEADER if rowheader is None else rowheader
    options.timezone = "UTC"
    options.safe = False
    if field is not None:
        options.field = field
    return options


def fix_codec(
    registry: FixRegistry | None = None,
    *,
    options: TextOptions | None = None,
    **pinned: Any,
) -> FixCodec:
    """One codec over a dictionary, pinned for the whole run it reads.

    The codec is the whole parse surface, so every reader a task composes is
    built here rather than configured per call. A dialect is not among the
    pins and neither is a version: the registry is one namespace, and what a
    message was read at is what its own `beginstring` said.

    `options` hands over the compiled capture order, so the line door reads
    every bracket part by position without one name lookup per line. An
    undated message takes `UNDATED` rather than the instant the parse ran, so
    a replay of the same bytes answers the same identity.
    """
    unknown = sorted(set(pinned) - CODEC_PINS)
    if unknown:
        raise TypeError(
            f"{', '.join(unknown)} is no codec pin; the pins are {', '.join(sorted(CODEC_PINS))}"
        )
    if options is not None:
        pinned.setdefault("capture_names", list(options.capture_names))
    pinned.setdefault("default_sending_time", UNDATED)
    return FixCodec(registry or fix_registry(), **pinned)


def dated_arrow_reader(
    source: pyarrow.RecordBatchReader,
    clock: str = "timestamp",
) -> pyarrow.RecordBatchReader:
    """`source` with its capture clock offered as the message's `SendingTime`.

    A message carrying no `SendingTime(52)` of its own is dated by its
    carrier, and a stored capture's carrier is a column. The codec's
    `default_sending_time` is the floor under both, but it is one instant for
    the whole run, so the per-row clock is what a line actually captured and
    this is where it is offered. The column clashes with the FIX column of
    that name, so it lands there rather than beside it.

    The column is filled, never merely cast: a line whose header the reader
    did not match carries no clock either, and a null would hand that message
    to the run-wide floor rather than to anything the capture recorded.
    `UNDATED` is what those rows state, which is the same instant the floor
    would have given them.
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


def fix_messages(
    codec: FixCodec,
    messages: Iterable[FixMsg],
    *,
    lifecycle: bool = True,
) -> Iterator[FixMsg]:
    """The two stages after any parse: enrich, then name the chains.

    Stated once so both doors settle a message the same way, and lazily, so a
    capture of any size reads in constant memory.

    `enrich_messages` and not `enrich_message`: only the stream form carries
    the memory that fills a later message's session pair from a bridge
    configuration an earlier line announced.
    """
    filled: FixMessages = codec.enrich_messages(messages)
    return codec.lifecycle(filled) if lifecycle else filled


def fix_line_messages(
    codec: FixCodec,
    lines: Iterable[TextLine],
    *,
    lifecycle: bool = True,
) -> Iterator[FixMsg]:
    """The whole pipeline over lines: parse, enrich, then name the chains.

    The line door, for a capture read straight through without a table in
    between.

    One thing this door does not do, and the batch door does: a line's own
    capture clock is context and dates nothing, because the bridge spells it
    the way a log spells a clock and not the way `SendingTime` is spelled. A
    message that stated none of its own therefore takes the codec's `UNDATED`
    floor here, while `fix_arrow_reader` offers it the typed capture column.
    Both are replayable -- neither reads the instant the parse ran -- and the
    two doors answer the same messages carrying the same arrival record from
    the same bytes.
    """
    return fix_messages(codec, codec.parse_text_lines(lines), lifecycle=lifecycle)


def fix_arrow_messages(
    codec: FixCodec,
    source: pyarrow.RecordBatchReader,
    *,
    lifecycle: bool = True,
) -> Iterator[FixMsg]:
    """The same pipeline over a stored capture's batches.

    The batch door. `parse_text_arrow_reader` is the twin of
    `parse_text_lines` -- the capture's own columns lead each row, a column
    named after a field fills it, and one source row answers a row per message
    it carried -- and `messages` crosses back to the shape the remaining two
    stages read. `dated_arrow_reader` is what this door has and the line door
    has not: the typed capture clock, offered to a message that stated none.
    """
    parsed = codec.parse_text_arrow_reader(dated_arrow_reader(source))
    return fix_messages(codec, codec.messages(parsed), lifecycle=lifecycle)


def fix_arrow_reader(
    codec: FixCodec,
    source: pyarrow.RecordBatchReader,
    *,
    lifecycle: bool = True,
    name: str = "fix",
) -> pyarrow.RecordBatchReader:
    """The batch door end to end: a stored capture in, settled rows out.

    The shape the rows land in is the parse's own -- the carrier's columns
    first, the dictionary's after -- read off the parsed reader rather than
    rebuilt, so the stages cannot disagree about a column. Narrowing it to
    what a table stores is the storage boundary's job, not this one's.
    """
    parsed = codec.parse_text_arrow_reader(dated_arrow_reader(source))
    field = Field.from_arrow_schema(parsed.schema, name=name)
    settled = fix_messages(codec, codec.messages(parsed), lifecycle=lifecycle)
    return codec.arrow_reader(field, settled)


def fix_generic_reader(
    codec: FixCodec,
    source: pyarrow.RecordBatchReader,
    field: Field | None = None,
) -> pyarrow.RecordBatchReader:
    """Settled rows lifted into the message a consumer reads them as."""
    return codec.format_arrow_reader(source, field or fix_generic_message(codec.registry, "fix"))


def iceberg_fix_field(
    schema: pyarrow.Schema,
    name: str = "FixMessage",
    carrier: Field | None = None,
) -> Field:
    """A parser schema narrowed to what Iceberg v2 stores: precision, and identity.

    The message's own `msghash` joins the carrier's key here: the codec
    answers one row per message rather than one per line, so a line holding
    two frames would otherwise publish two rows under one identity.

    The carrier's own key members join it too, and have to be restated
    because the parse folds a capture column onto the FIX column of the same
    name -- which is what naming a capture after the field it fills is for,
    and which loses the carrier's marking on the way. A member the carrier
    required stays required, so a row reaching the table without one is
    refused where it is read rather than stored under a hole in its key.

    Two narrowings, and both are about what a row filter can be lowered to. A
    semantic datatype -- a URL, an ISIN, a MIC, a currency -- crosses Arrow as
    its storage type under an extension name in the column's metadata, and a
    table that stored the storage type reads back a column that no longer
    merges with the declaration; the name is dropped here so the two agree.
    A nanosecond instant is stored at microsecond precision, which is the
    finest an Iceberg v2 timestamp holds.
    """
    keys = {MESSAGE_KEY} | _carried_keys(carrier)
    members = [_keyed(_plain(member.with_type(_stored(member.type))), keys) for member in schema]
    return Field.from_arrow_schema(pyarrow.schema(members), name=name)


def _carried_keys(carrier: Field | None) -> set[str]:
    """Which of the carrier's own columns name a row of the carrier's table."""
    if carrier is None:
        return set()
    return {
        member.name
        for member in carrier.into_arrow_schema()
        if (member.metadata or {}).get(PRIMARY_KEY.encode()) == b"true"
    }


def _keyed(member: pyarrow.Field, keys: set[str]) -> pyarrow.Field:
    """`member` marked as part of the table identity where it names one."""
    if member.name not in keys:
        return member
    held = {key.decode(): value.decode() for key, value in (member.metadata or {}).items()}
    return member.with_metadata({**held, PRIMARY_KEY: "true"}).with_nullable(False)


def _plain(member: pyarrow.Field) -> pyarrow.Field:
    """`member` without the extension name its semantic datatype crossed on."""
    held = member.metadata or {}
    if not any(key in held for key in _EXTENSION_KEYS):
        return member
    return member.with_metadata(
        {key: value for key, value in held.items() if key not in _EXTENSION_KEYS}
    )


def fix_message_field(
    codec: FixCodec | None = None,
    carrier: Field | None = None,
    *,
    name: str = "FixMessage",
) -> Field:
    """The complete fixed table field without consuming an input row.

    The codec answers its schema from the carrier and the dictionary alone, so
    an empty reader of the carrier's schema is all it takes to publish one --
    and the whole pipeline runs over it, because the published contract has to
    be the field the task actually writes.
    """
    if carrier is None:
        from rekep.text import Message

        carrier = Message.into_field()
    source = pyarrow.RecordBatchReader.from_batches(carrier.into_arrow_schema(), [])
    parsed = (codec or fix_codec()).parse_text_arrow_reader(dated_arrow_reader(source))
    try:
        return iceberg_fix_field(parsed.schema, name, carrier)
    finally:
        parsed.close()
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
    "CODEC_PINS",
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
    "fix_arrow_messages",
    "fix_arrow_reader",
    "fix_cfb_fields",
    "fix_codec",
    "fix_crate_fields",
    "fix_generic_message",
    "fix_generic_reader",
    "fix_line_messages",
    "fix_message_field",
    "fix_messages",
    "fix_plugin_fields",
    "fix_plugin_message",
    "fix_registry",
    "fix_schema",
    "fix_schema_carrying",
    "fix_schema_tags",
    "fix_text_options",
    "global_registry",
    "iceberg_fix_field",
    "install_global_registry",
    "registry_path",
]
