"""The bundled FIX dictionary and the native FIX pipeline surface.

The pipeline a bridge capture goes through is two native stages over one
codec, in this order and no other:

```text
parse -> lifecycle
```

`parse` reads every frame a line carried and settles it where it is read: a
parsed message already carries what it implied -- its typed facts lifted, its
deprecated fields restated, the dictionary's derivations run, its identifiers
and its side's lane filled, its instant and its identity settled -- so there
is no enriching stage between the two. `lifecycle` reads those messages as the
chains they belong to, filling the `prevuuid` a message follows, the `seqnum`
it stands at, the `prevpx` and `prevqty` the step before it settled on, and
the `creatunix` its chain opened at.

Each stage is a call rather than a pin, and each has two doors the native core
requires to agree row for row: `fix_line_messages` reads lines one at a time
and `fix_arrow_messages` reads a whole stored capture in batches. A task
holding a table takes the batch door; a reader holding lines takes the line
door. Neither reimplements the other, and neither dates a message from
anything the other cannot see -- a capture's own clock is context and stamps
nothing, so the same bytes answer the same event through either.
"""

from __future__ import annotations

import datetime
import json
import os
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import pyarrow
from yggdryl import IOBase, TextLine, TextOptions
from yggdryl.fix import (
    FixCodec,
    FixMessages,
    FixMsg,
    FixRegistry,
    MsgType,
    fix_cfb_fields,
    fix_crate_fields,
    fix_schema,
    fix_schema_carrying,
    fix_schema_tags,
    global_registry,
    install_global_registry,
)

from rekep.fields import (
    HOUR,
    PARTITION_KEY,
    PRIMARY_KEY,
    SORT_KEY,
    SORT_ORDER,
    Field,
)
from rekep.times import ULBRIDGE_ROWHEADER, UTC

_REGISTRY_PATH = Path(__file__).with_name("_data") / "fix"

#: The registry document the fixed row is published as, and the one name
#: `fix_schema` is asked for. Yggdryl owns the row; this is where yggfin says
#: which of its shapes the table is.
FIXMSG = "fixmsg"

#: The column a message's own identity is published as, and the table's whole
#: key. A UUIDv7 over the instant the event settled on and the code of its
#: content, so one message logged at three hops settles on one of them: an
#: arrival under an identity already held is a restatement, not a second row.
MESSAGE_KEY = "curruuid"

#: The instant the table is laid out by: the one the event happened at, which
#: is what the message stated and never what a line was printed at.
EVENT_CLOCK = "unix"

#: The column the codec reads each message out of. The default pin; a codec
#: naming another payload column answers that one instead.
PAYLOAD = "body"

#: The digest of those bytes, as the raw text row names it.
TEXT_DIGEST = "bodyhash"

#: The two capture columns `fix.messages` does not store: the bytes and the
#: digest of them.
#:
#: Both are facts about one *line*, and a row here is an *event*. One message
#: logged at four hops is four lines -- four different bodies, four different
#: digests -- and one row, so either column would be one arrival's answer
#: standing in for the event's. `logs.messages` holds every one of them; a row
#: here names the line it was read from with `sourceurl` and `rownum`, and what
#: it is re-emitted from is `fixentries`, the arrival record, which rebuilds
#: the message and never the line that carried it.
UNSTORED = (PAYLOAD, TEXT_DIGEST)

#: What the lifecycle walk filled for a message's place in its chain. A chain
#: read in `unix, seqnum` order is the order the venue described.
CHAIN_STEP = "seqnum"

#: Where a row sits inside its partition: the instant, then the step its chain
#: put it at, then its own identity, so two events of one instant still read
#: in one order and a chain never interleaves with itself.
SORT_COLUMNS = (EVENT_CLOCK, CHAIN_STEP, MESSAGE_KEY)

#: What dates a message no clock reached at all: neither its own `SendingTime`
#: nor a `TransactTime`, because the frame carried neither. An instant is what
#: the field holds, so it holds the one that means none -- reproducibly, which
#: is the whole point of dating it here: unpinned, each undated message reads
#: UTC now, and a replay of the same bytes answers a different identity.
UNDATED = datetime.datetime(1970, 1, 1, tzinfo=UTC)

#: What the codec accepts as a pin. Stated here because a pin that is not one
#: used to be forwarded silently: `version` was a legal pin until a version
#: became what a row states rather than what a caller chose, and the parse
#: went on answering rows under a dictionary nobody asked for.
#:
#: `include_msgtypes` and `exclude_msgtypes` are the message types a parse
#: keeps and refuses, read before a frame is built. Leaving `exclude_msgtypes`
#: unstated is not the same as stating `[]`: unstated is the core's own
#: refusal of `Heartbeat`, `TestRequest` and the untyped line, and `[]` keeps
#: every type. It is not defaulted here, because a default spelled twice is a
#: default that drifts from the one the core states.
CODEC_PINS = frozenset(
    {
        "default_sending_time",
        "separator",
        "payload_column",
        "capture_names",
        "null_values",
        "direction",
        "batch_byte_size",
        "batch_row_size",
        "include_msgtypes",
        "exclude_msgtypes",
    }
)

#: The Arrow field metadata a semantic native datatype crosses on. Iceberg
#: stores the storage type, so a column carrying one is narrowed here rather
#: than refused at the table boundary where the reason is no longer legible.
_EXTENSION_KEYS = (b"ARROW:extension:name", b"ARROW:extension:metadata")

#: What the carrier states about its own table and not about this one. A
#: capture column carried in front of the fixed row keeps what it *is* and
#: loses where `logs.messages` put it: this table is keyed and laid out by
#: the event, not by the line the event was read from.
_CARRIED_LAYOUT = (PRIMARY_KEY.encode(), PARTITION_KEY.encode())


def registry_path() -> Path:
    """The specification dictionary shipped inside this installation."""
    if not _REGISTRY_PATH.is_dir():
        raise FileNotFoundError(f"rekep FIX registry is absent: {_REGISTRY_PATH}")
    return _REGISTRY_PATH


def _load_registry(location: Any) -> FixRegistry:
    """Load one registry location, refusing a store that defined nothing."""
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
    # columns and the two standard clocks it seeds beside them, which every
    # registry holds from construction.
    if len(dictionary) == len(FixRegistry()):
        raise ValueError(f"FIX registry contains no specification fields: {location}")
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
    """The bundled registry, or one explicit dictionary."""
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

    The message-type filter is a pin and not a default stated here: unpinned,
    `exclude_msgtypes` is the core's own refusal of `Heartbeat`, `TestRequest`
    and the untyped line, which is not what `exclude_msgtypes=[]` means.
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


def fix_messages(
    codec: FixCodec,
    messages: Iterable[FixMsg],
    *,
    lifecycle: bool = True,
) -> Iterator[FixMsg]:
    """The stage after any parse: read the messages as the chains they are in.

    Stated once so both doors settle a message the same way, and lazily, so a
    capture of any size reads in constant memory. A parse fills what a message
    implied about itself; only this fills what it implied about the message
    before it.
    """
    return codec.lifecycle(messages) if lifecycle else iter(messages)


def fix_line_messages(
    codec: FixCodec,
    lines: Iterable[TextLine],
    *,
    lifecycle: bool = True,
) -> Iterator[FixMsg]:
    """The whole pipeline over lines: parse, then name the chains.

    The line door, for a capture read straight through without a table in
    between. It answers the same messages as `fix_arrow_messages` over the
    same bytes: a line's own capture clock is context in both, so a message
    stating no clock takes the codec's `UNDATED` floor whichever door read it.
    """
    parsed: FixMessages = codec.parse_text_lines(lines)
    return fix_messages(codec, parsed, lifecycle=lifecycle)


def fix_arrow_messages(
    codec: FixCodec,
    source: pyarrow.RecordBatchReader,
    *,
    lifecycle: bool = True,
) -> Iterator[FixMsg]:
    """The same pipeline over a stored capture's batches, as messages.

    The batch door's message shape. `parse_text_arrow_reader` is the twin of
    `parse_text_lines` -- the capture's own columns lead each row, a column
    named after a field fills it, and one source row answers a row per message
    it carried -- and `messages` crosses back from the row whole: a column no
    tag and no counter names is the capture's own, so the message keeps it and
    states it as no content of its own.
    """
    parsed = codec.parse_text_arrow_reader(source)
    return fix_messages(codec, codec.messages(parsed), lifecycle=lifecycle)


def fix_arrow_reader(
    codec: FixCodec,
    source: pyarrow.RecordBatchReader,
    *,
    lifecycle: bool = True,
) -> pyarrow.RecordBatchReader:
    """The batch door end to end: a stored capture in, settled rows out.

    The shape is the parse's own -- the capture's columns in front, the
    dictionary's after, which is what `fix_parse_field` states -- so nothing
    crosses back through the message shape to be written again. That shape
    still carries the capture's own text columns; dropping them, and narrowing
    the rest to what a table stores, is the storage boundary's job and
    `fix_stored_reader` is where that happens, not here.
    """
    parsed = codec.parse_text_arrow_reader(source)
    return codec.lifecycle_arrow_reader(parsed) if lifecycle else parsed


def fix_stored_reader(
    source: pyarrow.RecordBatchReader,
    field: Field,
) -> pyarrow.RecordBatchReader:
    """The parse's rows as the table stores them, ready for the write.

    The data half of `iceberg_fix_field`. Projecting onto `field` is most of
    it, and the field apply does that for free: the text columns the parse
    carried are not declared, so they are dropped here rather than written, and
    `logs.messages` stays the one place the bytes and their digest live.

    The rest exists for one column type.
    A content code is an unsigned sixty-four-bit integer and Iceberg's only
    sixty-four-bit integer is signed, so a code above 2**63 has no `int64` to
    be cast to -- the native cast refuses it rather than wrapping, and
    PyIceberg's own metrics refuse it a second time when it packs the column's
    bounds. The same eight bytes read as signed are the code, so the column is
    *viewed* rather than converted: one zero-copy reinterpretation per batch,
    no row pass, and a filter on either side names the same rows.

    The field apply runs after it, in its native order, so the cast, the
    derived partitions and the digests still happen where they always did.
    """
    stored = field.into_arrow_schema()
    unsigned = [
        member.name
        for member in source.schema
        if pyarrow.types.is_unsigned_integer(member.type)
        and member.name in stored.names
        and pyarrow.types.is_signed_integer(stored.field(member.name).type)
    ]
    if not unsigned:
        return field.apply_arrow_reader(source, safe=False, nullability="strict")
    viewed = pyarrow.schema(
        [
            member.with_type(stored.field(member.name).type) if member.name in unsigned else member
            for member in source.schema
        ],
        metadata=source.schema.metadata,
    )

    def _viewed() -> Iterator[pyarrow.RecordBatch]:
        for batch in source:
            columns = [
                batch.column(index).view(viewed.field(index).type)
                if viewed.field(index).name in unsigned
                else batch.column(index)
                for index in range(batch.num_columns)
            ]
            yield pyarrow.RecordBatch.from_arrays(columns, schema=viewed)

    return field.apply_arrow_reader(
        pyarrow.RecordBatchReader.from_batches(viewed, _viewed()),
        safe=False,
        nullability="strict",
    )


def fix_carrier(carrier: Field | None = None) -> Field:
    """A capture's own columns as the parse carries them in front of the row.

    The raw `Message` contract with its own table's layout taken off: a
    carried column states what the capture saw, and this table is keyed and
    partitioned by the event instead. `logs.messages` is keyed on the line --
    `(sourceurl, rownum)` -- and laid out by the hour the line was printed in;
    `fix.messages` is keyed on `curruuid` and laid out by the hour the event
    happened in, because one message logged at three hops is three lines and
    one row. Leaving either marking on would publish a second key and a second
    partition spec that nothing here means.

    A carried column whose folded name the fixed row already takes is dropped
    by `fix_schema_carrying` rather than duplicated, which is what naming a
    capture after the field it fills is for: `sourceurl`, `msgsessionid`,
    `msgctxid`, `msgseqnum` and `pluginid` are the message's own columns and
    only `rownum`, `timestamp`, `timepartition`, `threadId`, `level`,
    `bodyhash` and `body` ride in front.

    Seven here and five in the table: `UNSTORED` -- the payload the parse reads
    every message out of, and the digest of it -- rides through the parse and
    is dropped at the storage boundary, because both are a line's and a stored
    row is an event's.
    """
    if carrier is None:
        from rekep.text import Message

        carrier = Message.into_field()
    members = [_carried(member) for member in carrier.into_arrow_schema()]
    return Field.from_arrow_schema(pyarrow.schema(members), name=carrier.name)


def _carried(member: pyarrow.Field) -> pyarrow.Field:
    """`member` without what it says about the table it came from."""
    held = member.metadata or {}
    if not any(key in held for key in _CARRIED_LAYOUT):
        return member
    return member.with_metadata(
        {key: value for key, value in held.items() if key not in _CARRIED_LAYOUT}
    )


def fix_parse_field(
    codec: FixCodec | None = None,
    carrier: Field | None = None,
    *,
    name: str = "FixMessage",
) -> Field:
    """The row the codec writes: the fixed row behind the capture's own columns.

    `fix_schema(registry, "fixmsg")` is the row yggdryl publishes and
    `fix_schema_carrying` is the supported way to put a capture's own columns
    in front of it, so no column here is spelled twice and none is yggfin's to
    define. It is answered from the dictionary alone, without consuming an
    input row, so an empty capture creates the same table a full one does.
    """
    registry = codec.registry if codec is not None else fix_registry()
    return fix_schema_carrying(fix_carrier(carrier), fix_schema(registry, FIXMSG))


def fix_message_field(
    codec: FixCodec | None = None,
    carrier: Field | None = None,
    *,
    name: str = "FixMessage",
) -> Field:
    """The complete `fix.messages` field: that row, as a table stores it.

    What yggfin adds to the row is the three things a table is -- which column
    names one, which lays it out, and where a row sits inside a partition --
    the narrowing a stored column needs, and the two carried columns a stored
    row does not hold. Nothing else: a column yggdryl named is not renamed,
    retyped or duplicated here, and the two that are dropped are the capture's
    own text rather than any of the dictionary's.
    """
    return iceberg_fix_field(
        fix_parse_field(codec, carrier, name=name).into_arrow_schema(),
        name,
        UNSTORED if codec is None else (codec.payload_column, TEXT_DIGEST),
    )


def iceberg_fix_field(
    schema: pyarrow.Schema,
    name: str = "FixMessage",
    unstored: Iterable[str] = UNSTORED,
) -> Field:
    """A parsed schema narrowed to what Iceberg v2 stores, and declared as a table.

    `unstored` names the capture's own text columns, and they are dropped
    rather than narrowed: the bytes the parse read each message out of and the
    digest of them are the line's, and a row here is the event's. A row names
    the line it was read from with `sourceurl` and `rownum`; `logs.messages`
    holds the bytes, the digest and every other arrival of the same event.

    Three narrowings after that, and each is about what a row filter can be
    lowered to.
    A semantic datatype -- a URL, an ISIN, a MIC, a currency, a side -- crosses
    Arrow as its storage type under an extension name in the column's
    metadata, and a table that stored the storage type reads back a column
    that no longer merges with the declaration; the name is dropped here so
    the two agree. A nanosecond instant is stored at the microsecond an
    Iceberg v2 timestamp holds. A `uint64` content code is stored as the only
    sixty-four-bit integer Iceberg has, which is signed: the eight bytes are
    the same eight bytes and half of them read back negative, so the cast is
    stated here rather than met as a metrics overflow at the first commit.

    An identity is sixteen ordered bytes here and not the `uuid` Iceberg would
    otherwise store, for the same reason: Arrow sorts, compares and hashes
    `fixed_size_binary[16]` and refuses the extension over it, so a table
    keyed, ordered and merged on an identity needs the bytes it can lower a
    predicate to. The value is the same value either way.
    """
    dropped = set(unstored)
    members = [
        _declared(_plain(member.with_type(_stored(member.type))))
        for member in schema
        if member.name not in dropped
    ]
    sorted_by = json.dumps([[column, "asc"] for column in SORT_COLUMNS], separators=(",", ":"))
    return Field.from_arrow_schema(
        pyarrow.schema(members, metadata={SORT_ORDER: sorted_by}),
        name=name,
    )


def _declared(member: pyarrow.Field) -> pyarrow.Field:
    """`member` carrying what this table -- and not the row -- says of it."""
    marked: dict[str, str] = {}
    if member.name == MESSAGE_KEY:
        marked[PRIMARY_KEY] = "true"
    if member.name == EVENT_CLOCK:
        marked[PARTITION_KEY] = HOUR
    if member.name in SORT_COLUMNS:
        marked[SORT_KEY] = "asc"
    if not marked:
        return member
    held = {key.decode(): value.decode() for key, value in (member.metadata or {}).items()}
    return member.with_metadata({**held, **marked})


def _plain(member: pyarrow.Field) -> pyarrow.Field:
    """`member` without the extension name its semantic datatype crossed on."""
    held = member.metadata or {}
    if not any(key in held for key in _EXTENSION_KEYS):
        return member
    return member.with_metadata(
        {key: value for key, value in held.items() if key not in _EXTENSION_KEYS}
    )


def _stored(dtype: pyarrow.DataType) -> pyarrow.DataType:
    """Recursively narrow a parsed type to the one Iceberg v2 stores."""
    if isinstance(dtype, pyarrow.BaseExtensionType):
        return _stored(dtype.storage_type)
    if pyarrow.types.is_timestamp(dtype) and dtype.unit == "ns":
        return pyarrow.timestamp("us", tz=dtype.tz)
    if pyarrow.types.is_unsigned_integer(dtype):
        return pyarrow.int64()
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
    "CHAIN_STEP",
    "CODEC_PINS",
    "EVENT_CLOCK",
    "FIXMSG",
    "MESSAGE_KEY",
    "PAYLOAD",
    "TEXT_DIGEST",
    "UNSTORED",
    "SORT_COLUMNS",
    "UNDATED",
    "FixCodec",
    "FixMessages",
    "FixMsg",
    "FixRegistry",
    "MsgType",
    "fix_arrow_messages",
    "fix_arrow_reader",
    "fix_carrier",
    "fix_cfb_fields",
    "fix_codec",
    "fix_crate_fields",
    "fix_line_messages",
    "fix_message_field",
    "fix_messages",
    "fix_parse_field",
    "fix_registry",
    "fix_stored_reader",
    "fix_schema",
    "fix_schema_carrying",
    "fix_schema_tags",
    "fix_text_options",
    "global_registry",
    "iceberg_fix_field",
    "install_global_registry",
    "registry_path",
]
