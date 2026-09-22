"""The native FIX registry and the bronze/silver storage boundary.

Bronze parses frames independently, lifting typed facts and retaining only
unprojected content in `fixentries`. The native codec can parse batches in
parallel while preserving source order. It owns null filtering, identifiers,
code validation and the row's schema.

Silver uses the native finite lifecycle: date by transaction time where
needed, stably order events, suppress repeated deliveries, learn validated
instrument associations, link predecessors, and emit expirations. It folds
`creaunix`, `exprtime` and `state`; Python projects the native message row and
narrows the resulting Arrow batches for Iceberg. The task supplies
only the preceding hour plus its job window and publishes its window's rows.
"""

from __future__ import annotations

import datetime
import functools
import json
import os
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import pyarrow
import pyarrow.compute
from yggdryl import IOBase, TextLine, TextOptions
from yggdryl.fix import (
    FixCodec,
    FixMessages,
    FixMsg,
    FixRegistry,
    MsgType,
    fix_crate_fields,
    fix_schema,
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
#: key. A UUIDv7 over the millisecond the event settled on, its place in its
#: chain and the cross-seeded code of its content, so one message logged at
#: three hops settles on one of them: an arrival under an identity already
#: held is a restatement, not a second row.
MESSAGE_KEY = "curruuid"

#: The instant every table here is laid out by, and the core's own name for
#: it: the first of the nineteen columns every event's schema opens with. On a
#: FIX row it is what the message stated and never what a line was printed
#: at; on a raw one it is what the line was printed at, because there a line
#: is the event. Each read settles its own, and the hour of it is the layout.
EVENT_CLOCK = "currunix"

#: The clock the walk dates a message by where the parse could not: the
#: `TransactTime(60)` it states. A parse pins an undated message at `UNDATED`,
#: so a bronze row at the pin whose transaction time falls in a window is that
#: window's, and `fix_window_filter` reads it there.
TRANSACTION_CLOCK = "transacttime"

#: The identities a message was read from: provenance, never lineage. A
#: bronze row names the one line it was parsed from; a silver row names every
#: line its event was logged on, because the walk merges the observations of
#: one event. Source location and capture context remain on `logs.messages`,
#: reached through these identities; no walk reads them as lineage.
SOURCES = "srcuuids"

#: The capture column a parse reads the line's own identity off, and puts in
#: `srcuuids`. `logs.messages` stores it as the sixteen ordered bytes Arrow
#: can key, sort and merge on, so the parse is handed it back at the type the
#: read states before it is asked to read it.
CAPTURE_KEY = "curruuid"

#: What the lifecycle walk filled for a message's place in its chain. A chain
#: read in `currunix, seqnum` order is the order the venue described, and the
#: column is empty on every bronze row.
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

#: The Arrow field metadata a semantic native datatype crosses on. Iceberg
#: stores the storage type, so a column carrying one is narrowed here rather
#: than refused at the table boundary where the reason is no longer legible.
_EXTENSION_KEYS = (b"ARROW:extension:name", b"ARROW:extension:metadata")


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

    Every capture a row header declares is named for what the native read
    fills from it -- a field for `msgpluginid`, `msgsessionid`, `msgctxid`
    and `msgseqnum`, the settled `currunix` for `mtime` -- so `capture_names`
    alone is what tells the codec which bracket part is which and nothing
    maps a spelling onto a tag. `rowheader` reads a bridge writing those same
    facts in a layout of its own; the default is the one this package ships.
    A reader that also stores its rows takes the header through
    `Message.text_options`, which additionally checks those names against the
    ones its contract is filled from.
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
    if options is not None:
        pinned.setdefault("capture_names", list(options.capture_names))
    pinned.setdefault("default_sending_time", UNDATED)
    return FixCodec(fix_registry() if registry is None else registry, **pinned)


def fix_parse_lines(codec: FixCodec, lines: Iterable[TextLine]) -> Iterator[FixMsg]:
    """The parse over lines: every frame a line carried, as messages.

    The line door of the first stage, for a capture read straight through
    without a table in between. It answers the same messages as
    `fix_parse_arrow_reader` over the same bytes: a line's own capture clock
    is context in both, so a message stating no clock takes the codec's
    `UNDATED` floor whichever door read it.
    """
    parsed: FixMessages = codec.parse_text_lines(lines)
    return parsed


def fix_parse_arrow_reader(
    codec: FixCodec,
    source: pyarrow.RecordBatchReader,
) -> pyarrow.RecordBatchReader:
    """Parse stored capture batches into native FIX rows.

    The native parser consumes capture columns to resolve each message and
    puts the raw line's `curruuid` in `srcuuids`. Project its result to the
    dictionary's fixed row: where the line was read from, what it was printed
    at and the line itself remain on `logs.messages`, reachable through that
    source identity. Projection shares the parsed column buffers and works
    for empty readers too.

    Nothing has walked: `seqnum`, `prevuuid`, `prevunix` and `parentuuids`
    are empty. One source row answers one row per frame it contains.
    """
    return _dictionary_rows(codec, codec.parse_text_arrow_reader(_carried_rows(source)))


def fix_lifecycle_messages(codec: FixCodec, messages: Iterable[FixMsg]) -> Iterator[FixMsg]:
    """The walk over messages: each read as the chain it belongs to.

    The line door of the second stage, stated once so both doors settle a
    message the same way. A parse fills what a message implied about itself;
    only this fills what it implied about the message before it -- and what
    it implied about its own instant, where the parse could not date it: the
    walk holds every message until it has read the last one, dates the
    undated by their transaction time, and answers them in that order.
    """
    return codec.lifecycle(messages)


def fix_row_messages(
    codec: FixCodec,
    source: pyarrow.RecordBatchReader,
) -> Iterator[FixMsg]:
    """A FIX table's rows read back as the messages that wrote them.

    The message shape of either table, bronze or silver. `messages` crosses
    back from a *fixed row*, so the projection is there and not a convenience:
    a message is read back out of the columns the dictionary defines, and a
    capture's own column beside them would be read as content the message
    never carried. A stored row is widened first, because the walk and the
    message read the row as the dictionary types it and a table holds the
    narrowing `stored_arrow_reader` gave it.
    """
    return codec.messages(_dictionary_rows(codec, source))


def fix_lifecycle_arrow_reader(
    codec: FixCodec,
    source: pyarrow.RecordBatchReader,
) -> pyarrow.RecordBatchReader:
    """Walk native FIX rows, merging every observation's `srcuuids` as capture provenance.

    Stored rows are widened to the dictionary's types before the native
    lifecycle dates, stably sorts, deduplicates and folds them. Its finite
    input is the preceding hour plus the job window, bounded by the caller.
    Only native message columns are passed into and returned from the walk.
    """
    return codec.lifecycle_arrow_reader(_dictionary_rows(codec, source))


def _dictionary_rows(
    codec: FixCodec,
    source: pyarrow.RecordBatchReader,
) -> pyarrow.RecordBatchReader:
    """`source` narrowed to the fixed row's own columns, typed as it types them.

    The reverse of `stored_arrow_reader`, for a row read back off a table: the
    content codes stored as the signed integers Iceberg has are viewed as the
    unsigned ones they are -- the same eight bytes, no row pass -- and the
    dictionary's field then widens the rest in its native order, an identity
    from the sixteen bytes a table keeps to the UUID the row states and an
    instant from the microsecond stored to the nanosecond read. A row that
    already carries the dictionary's types passes through untouched.
    """
    kept = _row_columns(codec, source.schema)
    row = fix_schema(codec.registry, FIXMSG)
    native = row.into_arrow_schema()
    narrowed = pyarrow.schema([source.schema.field(name) for name in kept])
    if narrowed.equals(native, check_metadata=False):
        if len(kept) == len(source.schema.names):
            return source
        return pyarrow.RecordBatchReader.from_batches(
            native, (batch.select(kept) for batch in source)
        )
    unsigned = [
        name
        for name in kept
        if pyarrow.types.is_unsigned_integer(native.field(name).type)
        and pyarrow.types.is_signed_integer(narrowed.field(name).type)
    ]
    viewed = pyarrow.schema(
        [
            member.with_type(native.field(member.name).type) if member.name in unsigned else member
            for member in narrowed
        ]
    )

    def _viewed() -> Iterator[pyarrow.RecordBatch]:
        for batch in source:
            yield pyarrow.RecordBatch.from_arrays(
                [
                    batch.column(name).view(viewed.field(name).type)
                    if name in unsigned
                    else batch.column(name)
                    for name in kept
                ],
                schema=viewed,
            )

    return row.apply_arrow_reader(
        pyarrow.RecordBatchReader.from_batches(viewed, _viewed()),
        safe=False,
        nullability="strict",
    )


def _carried_rows(source: pyarrow.RecordBatchReader) -> pyarrow.RecordBatchReader:
    """`source` with the line's identity viewed back to the type the read states.

    The reverse of the raw table's own narrowing, and the reason that
    narrowing is safe: a stored `curruuid` is the sixteen ordered bytes
    Iceberg keys and sorts on, and a parse handed those bytes reads no
    identity at all -- it recomputes one from the line's content, at the
    undated pin, which is the line's own only by accident. Viewed back, the
    parse names the line that landed, so `srcuuids` is a join and not a
    coincidence. The bytes are the same bytes; nothing is copied or cast.
    """
    held = source.schema
    if CAPTURE_KEY not in held.names:
        return source
    stated = _capture_key_type()
    carried = held.field(CAPTURE_KEY)
    # The view is the stored narrowing read back and nothing else: a column
    # the read already states, or one holding anything but those sixteen
    # bytes, is the parse's to read as it finds it.
    if carried.type != stated.storage_type:
        return source
    place = held.get_field_index(CAPTURE_KEY)
    viewed = held.set(place, carried.with_type(stated))

    def _viewed() -> Iterator[pyarrow.RecordBatch]:
        for batch in source:
            columns = list(batch.columns)
            columns[place] = pyarrow.ExtensionArray.from_storage(stated, columns[place])
            yield pyarrow.RecordBatch.from_arrays(columns, schema=viewed)

    return pyarrow.RecordBatchReader.from_batches(viewed, _viewed())


@functools.cache
def _capture_key_type() -> pyarrow.DataType:
    """The type the native text read states a line's own identity at."""
    return TextOptions().source_field().into_arrow_schema().field(CAPTURE_KEY).type


def _row_columns(codec: FixCodec, schema: pyarrow.Schema) -> list[str]:
    """Which of `schema`'s columns the fixed row itself defines.

    Read off the codec's own dictionary, because that is the one that parsed
    the rows. Capture-only columns are excluded at the parse boundary.
    """
    row = {member.name for member in fix_schema(codec.registry, FIXMSG)}
    return [name for name in schema.names if name in row]


def fix_window_filter(window: tuple[datetime.datetime, datetime.datetime]) -> Any:
    """The bronze rows whose event a window covers, as the predicate a scan prunes by.

    Read off the event clock, because a bronze row is already an event: the
    rows whose `currunix` falls in `[start, end)`. And the rows the parse
    could not date -- those at the `UNDATED` pin, which the walk will date by
    the `TransactTime` they state -- where that transaction time falls in the
    window, or where they state none and so belong to every window until a
    walk can place them. The pin is one hour of one partition, so the second
    reading opens that partition alone and prunes its files by the
    transaction clock they hold.
    """
    from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, IsNull, LessThan, Or

    lower, upper = window
    dated = And(GreaterThanOrEqual(EVENT_CLOCK, lower), LessThan(EVENT_CLOCK, upper))
    transacted = And(
        GreaterThanOrEqual(TRANSACTION_CLOCK, lower), LessThan(TRANSACTION_CLOCK, upper)
    )
    pinned = And(EqualTo(EVENT_CLOCK, UNDATED), Or(transacted, IsNull(TRANSACTION_CLOCK)))
    return Or(dated, pinned)


def fix_parse_field(
    codec: FixCodec | None = None,
    *,
    name: str = "FixMsg",
) -> Field:
    """The native message row, without columns belonging to a captured line.

    The registry owns every column. The field is resolved without reading
    input, so empty and populated streams publish the same contract.
    """
    registry = codec.registry if codec is not None else fix_registry()
    field = fix_schema(registry, FIXMSG)
    field.set_name(name)
    return field


def fix_message_field(
    codec: FixCodec | None = None,
    *,
    name: str = "FixMsg",
) -> Field:
    """The native row narrowed for Iceberg, with one event key and layout.

    Both FIX tables declare the same columns. The lifecycle fills lineage
    and folded state without adding capture columns. `srcuuids` links back
    to the raw lines in `logs.messages`.
    """
    return iceberg_fix_field(fix_parse_field(codec, name=name).into_arrow_schema(), name)


def iceberg_fix_field(
    schema: pyarrow.Schema,
    name: str = "FixMsg",
) -> Field:
    """A parsed schema narrowed to what Iceberg v2 stores, and declared as a table.

    The layout, declared once here and carried on the field alone. The key is
    `curruuid` and nothing beside it: one message logged at every hop it
    passed is one event, so an arrival under an identity already held
    replaces it rather than landing beside it. The partition is the hour of
    `currunix` and nothing beside it: a key is scoped to its partition, so the
    partition has to be the event's own instant for two arrivals of one event
    to meet -- and the row already carries that instant, so a materialized
    copy of it would be a second owner of one fact. The sort order is
    `currunix, seqnum, curruuid` within a partition, so two events of one
    instant still read in one order and a chain never interleaves with
    itself.

    The schema contains only native message columns. The source identities
    in `srcuuids` link to `logs.messages`, which holds the capture facts.

    Three narrowings make row filters representable in storage.
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
    members = [_declared(_plain(member.with_type(_stored(member.type)))) for member in schema]
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
    "CAPTURE_KEY",
    "CHAIN_STEP",
    "EVENT_CLOCK",
    "FIXMSG",
    "MESSAGE_KEY",
    "SORT_COLUMNS",
    "SOURCES",
    "TRANSACTION_CLOCK",
    "UNDATED",
    "FixCodec",
    "FixMessages",
    "FixMsg",
    "FixRegistry",
    "MsgType",
    "fix_codec",
    "fix_crate_fields",
    "fix_lifecycle_arrow_reader",
    "fix_lifecycle_messages",
    "fix_message_field",
    "fix_parse_arrow_reader",
    "fix_parse_field",
    "fix_parse_lines",
    "fix_registry",
    "fix_row_messages",
    "fix_schema",
    "fix_schema_tags",
    "fix_text_options",
    "fix_window_filter",
    "global_registry",
    "iceberg_fix_field",
    "install_global_registry",
    "registry_path",
]
