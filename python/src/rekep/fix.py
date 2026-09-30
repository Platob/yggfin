"""The native FIX registry and the raw/refined storage boundary.

Raw parses each frame on its own, lifting typed facts and retaining only
unprojected content in `fixentries`, and carries one piece of stream state:
the place it gives a message among the messages of its instant, in the order
the stream hands them over, which reaches the message's identity. The native
codec can parse batches in parallel -- threads and batch sizes change no
answer -- while the input's order does, so the raw read is ordered by
`SORT_COLUMNS`. It owns null filtering, identifiers, code validation and the
row's schema.

Refined uses the native finite lifecycle: date by transaction time where
needed, stably order events, suppress repeated deliveries, learn validated
instrument associations, link predecessors, and emit expirations. It folds
`creaunix`, `exprunix` and `state`; Python projects the native message row and
narrows the resulting Arrow batches for Iceberg.
`rekep.pipeline.parse_fix_messages_refined` supplies the preceding hour plus its window
and publishes its window's rows.
"""

from __future__ import annotations

import datetime
import functools
import json
import os
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

import pyarrow
import pyarrow.compute
from yggdryl import TextLine, TextOptions
from yggdryl.fix import (
    FixCodec,
    FixMessages,
    FixMsg,
    FixRegistry,
    MsgType,
    fix_crate_fields,
    fix_schema,
    fix_schema_tags,
)

from rekep.fields import (
    HOUR,
    PARTITION_KEY,
    PRIMARY_KEY,
    SORT_KEY,
    SORT_ORDER,
    Field,
)
from rekep.times import UTC

#: The FIX dictionary this package ships, installed as the process default.
_REGISTRY = Path(__file__).with_name("_data") / "fix"

#: The environment variable naming a dictionary folder the process reads in
#: place of the one this package ships. The native registry reads it; this
#: package only leaves its own dictionary uninstalled where it is set.
REGISTRY_VARIABLE = "YGGDRYL_FIX_REGISTRY"

#: The registry document the fixed row is published as, and the one name
#: `fix_schema` is asked for. Yggdryl owns the row; this is where yggfin says
#: which of its shapes the table is.
FIXMSG = "fixmsg"

#: The column a message's own identity is published as, and the table's whole
#: key. The native UUIDv7 orders the event's millisecond and its place among
#: the events of that instant, with a deterministic content payload seeded by
#: its cross hash. Yggdryl owns its derivation. The place reaches it, so the
#: copies of a message the hops logged at one instant are bronze rows of
#: their own, and the walk folds the repeated deliveries into one event. The
#: instant reaches it at the millisecond alone: copies of one content dated
#: apart within one millisecond, each at place zero of its own instant, are
#: one identity, and the key keeps the first.
MESSAGE_KEY = "curruuid"

#: The instant every table here is laid out by, and the core's own name for
#: it: the first of the fifteen event columns every row here states. On a
#: FIX row it is what the message stated and never what a line was printed
#: at; on a text row it is what the line was printed at, because there a line
#: is the event. Each read settles its own, and the hour of it is the layout.
EVENT_CLOCK = "currunix"

#: The clock the walk dates a message by where the parse could not: the
#: `TransactTime(60)` it states. A parse pins an undated message at `UNDATED`,
#: so a bronze `fix_messages` row at the pin whose transaction time falls in a window is that
#: window's, and `fix_window_filter` reads it there.
TRANSACTION_CLOCK = "transacttime"

#: The identities a message was read from: provenance, never lineage. A
#: bronze `fix_messages` row names the one line it was parsed from; a silver one
#: names the lines of every observation of its session event, which the walk
#: merges, and not the line of a copy another session event delivered, which
#: the walk folds into it and only that copy's bronze row names. Source
#: location and capture context remain on `log_messages`, reached through
#: these identities; no walk reads them as lineage.
SOURCES = "srcuuids"

#: The stored line's column a parse reads its identity off, and puts in
#: `srcuuids`. `log_messages` stores it as the sixteen ordered bytes Arrow
#: can key, sort and merge on, so the parse is handed it back at the type the
#: read states before it is asked to read it.
CAPTURE_KEY = "curruuid"

#: An event's place among the events of its instant: null at place zero, the
#: first, and one more for each next. The parse places a message in its run,
#: the messages its stream handed over at that instant one after another, and
#: an instant the stream comes back to starts a run of its own; the walk
#: places each event again once it has sorted and folded them. It is no step
#: along a chain -- `prevuuid` links those.
EVENT_PLACE = "seqnum"

#: The columns of a stored line the parse reads, and the projection the
#: raw stage pushes into its scan: the line's own clock, which is context to
#: the messages it states; its identity, which each of them names as its
#: source; the body the frames are read out of; and the four captures that
#: fill a field by their name. The line's content code, cross code and row
#: number are the line's facts and say nothing about a message, and
#: `msgthreadid` and `loglevel` name no field, so none of them is read.
PARSE_COLUMNS = (
    EVENT_CLOCK,
    CAPTURE_KEY,
    "body",
    "msgsessionid",
    "msgctxid",
    "msgseqnum",
    "msgpluginid",
)

#: Where a row sits inside its partition: the instant, then its place there --
#: a null, place zero, first -- then its own identity, so the events of one
#: instant read in the order they were placed in.
SORT_COLUMNS = (EVENT_CLOCK, EVENT_PLACE, MESSAGE_KEY)

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


def _refuse_bare(registry: FixRegistry, location: Any) -> None:
    """Refuse a registry that defines nothing, which would type a narrow table.

    A store that defined nothing loads as a bare registry: the crate's own
    columns and the two standard clocks it seeds beside them, which every
    registry holds from construction.
    """
    if len(registry) == len(FixRegistry()):
        raise ValueError(f"FIX registry contains no specification fields: {location}")


def _install_shipped() -> None:
    """Make the dictionary this package ships the one `FixRegistry.from_env()` answers.

    Unless the process chose one already: a registry a host installed before
    importing this package, or a folder the environment names, is kept, so
    `FixRegistry.from_env()` and `FixCodec.from_env()` answer the caller's
    choice first and this package's dictionary otherwise.
    """
    if os.environ.get(REGISTRY_VARIABLE):
        return
    registry = FixRegistry.from_handle(_REGISTRY)
    _refuse_bare(registry, _REGISTRY)
    try:
        FixRegistry.install_env(registry)
    except ValueError:
        pass


_install_shipped()


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
    puts the stored line's `curruuid` in `srcuuids`. Project its result to the
    dictionary's fixed row: where the line was read from, what it was printed
    at and the line itself remain on `log_messages`, reachable through that
    source identity. Projection shares the parsed column buffers and works
    for empty readers too.

    Nothing has walked: `prevuuid` and `prevunix` are empty, and `seqnum` is
    the message's place among the messages the stream handed over at its
    instant, which reaches its identity. One source row answers one row per
    frame it contains, and the copies of a message logged at several hops are
    one row each.

    The parse answers the capture's own columns ahead of the row -- the
    `body` it read the frames out of, and whatever else the source carried
    that no field claims -- because a message carries the line it was read
    off. A table holds the row, so those lead columns are left here, and
    that is the whole of what this door narrows: a source read projected to
    what the parse consumes (`PARSE_COLUMNS`) carries nothing else.
    """
    parsed = codec.parse_text_arrow_reader(_carried_rows(source))
    row = fix_schema(codec.registry, FIXMSG).into_arrow_schema()
    if parsed.schema.names == row.names:
        return parsed
    return pyarrow.RecordBatchReader.from_batches(
        row, (batch.select(row.names) for batch in parsed)
    )


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

    The message shape of either table, raw or refined. `messages` crosses
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
    """Walk native FIX rows into events, each naming the lines it was logged on.

    Stored rows are widened to the dictionary's types, and the groups a table
    read emptied made absent again, before the native lifecycle dates, sorts,
    deduplicates and folds them: the whole input at once, or, where
    `codec.sorted_lifecycle` states the rows arrive in instant order, one
    epoch hour at a time as they come. An event's `srcuuids` merges those of
    the observations of its session event, `msgsesseventid`; a copy another
    session event delivered at its instant with its content is folded into it
    without naming its line. Only native message columns are passed into and
    returned from the walk.
    """
    return codec.lifecycle_arrow_reader(_dictionary_rows(codec, source))


def _dictionary_rows(
    codec: FixCodec,
    source: pyarrow.RecordBatchReader,
) -> pyarrow.RecordBatchReader:
    """`source`, a table's rows, typed as the fixed row types them.

    The reverse of `stored_arrow_reader`, and the one thing a table read
    cannot express: the content codes stored as the signed integers Iceberg
    has are viewed as the unsigned ones they are -- the same eight bytes, no
    row pass, where a cast would refuse half of them -- and the dictionary's
    field then widens the rest in its native order, an identity from the
    sixteen bytes a table keeps to the UUID the row states and an instant
    from the microsecond stored to the nanosecond read. The read itself is
    handed the table's field, so what arrives here is the row's own columns
    and nothing to narrow; a source already at the dictionary's types passes
    through untouched.

    It also gives back what a table read loses: PyIceberg reads a null list
    of structs back as an empty one, so every group a message never stated
    arrives as `[]`, and `_stated_groups` makes it absent again -- so the
    walk and the fold read a stored message as the content the parse
    recorded, and fold its repeat deliveries as they fold the parse's own.
    """
    native = fix_schema(codec.registry, FIXMSG).into_arrow_schema()
    if source.schema.equals(native, check_metadata=False):
        return source
    held = source.schema
    declared = [
        native.field(member.name) if member.name in native.names else None for member in held
    ]
    unsigned = [
        member.name
        for member in held
        if member.name in native.names
        and pyarrow.types.is_unsigned_integer(native.field(member.name).type)
        and pyarrow.types.is_signed_integer(member.type)
    ]
    viewed = pyarrow.schema(
        [
            member.with_type(native.field(member.name).type) if member.name in unsigned else member
            for member in held
        ],
        metadata=held.metadata,
    )

    def _viewed() -> Iterator[pyarrow.RecordBatch]:
        for batch in source:
            yield pyarrow.RecordBatch.from_arrays(
                [
                    column.view(viewed.field(index).type)
                    if viewed.field(index).name in unsigned
                    else column
                    for index, column in enumerate(_stated_groups(batch.columns, declared))
                ],
                schema=viewed,
            )

    return fix_schema(codec.registry, FIXMSG).apply_arrow_reader(
        pyarrow.RecordBatchReader.from_batches(viewed, _viewed()), safe=False
    )


#: The metadata a group names the tag of its counter under, and the one a
#: counter states its own tag under.
_GROUP_COUNTER = b"FIX:counter"
_FIELD_TAG = b"FIX:tag"


def _stated_groups(
    columns: Sequence[pyarrow.Array], declared: Sequence[pyarrow.Field | None]
) -> list[pyarrow.Array]:
    """One level of a stored row -- the row, or a struct in it -- with every
    group a table read emptied absent again.

    `declared` is the dictionary's field of each column, None for one it does
    not define. A group read back empty beside a counter stating nothing is
    null again, because a counter is an integer and a table keeps its null;
    one beside a stated zero stays the empty group the message stated. Only a
    column holding a group is rebuilt -- a party's sub-ids inside its party
    included -- and its buffers are shared.
    """
    counters = {
        member.metadata[_FIELD_TAG]: place
        for place, member in enumerate(declared)
        if member is not None and member.metadata and _FIELD_TAG in member.metadata
    }
    stated = []
    for column, member in zip(columns, declared, strict=True):
        if member is None or not _holds_group(member.type):
            stated.append(column)
            continue
        absent = column.is_null()
        counter = counters.get((member.metadata or {}).get(_GROUP_COUNTER))
        if counter is not None:
            unstated = pyarrow.compute.and_(
                columns[counter].is_null(),
                pyarrow.compute.equal(pyarrow.compute.list_value_length(column), 0),
            )
            absent = pyarrow.compute.or_(absent, pyarrow.compute.fill_null(unstated, False))
        stated.append(_absent_where(column, member.type, absent))
    return stated


def _absent_where(
    column: pyarrow.Array, declared: pyarrow.DataType, absent: pyarrow.Array
) -> pyarrow.Array:
    """`column` -- a list or a struct -- null where `absent` holds, and the
    groups inside it made absent by `_stated_groups`."""
    if pyarrow.types.is_struct(column.type):
        names = [member.name for member in column.type]
        children = _stated_groups(
            [column.field(index) for index in range(len(names))],
            [
                declared.field(name) if declared.get_field_index(name) >= 0 else None
                for name in names
            ],
        )
        return pyarrow.StructArray.from_arrays(children, fields=list(column.type), mask=absent)
    # Arrow rebuilds a list under a mask only from offsets starting at zero,
    # so a sliced column's offsets are rebased onto the values they span --
    # and an empty span onto none at no offset, the only empty slice the
    # native read takes.
    offsets = column.offsets
    first = offsets[0]
    span = offsets[-1].as_py() - first.as_py()
    values = column.values.slice(first.as_py() if span else 0, span)
    if _holds_group(declared.value_type):
        values = _absent_where(values, declared.value_type, values.is_null())
    return type(column).from_arrays(
        pyarrow.compute.subtract(offsets, first), values, type=column.type, mask=absent
    )


def _holds_group(dtype: pyarrow.DataType) -> bool:
    """Whether a column of `dtype` holds a group -- a list of structs -- at any depth."""
    if pyarrow.types.is_list(dtype) or pyarrow.types.is_large_list(dtype):
        return pyarrow.types.is_struct(dtype.value_type) or _holds_group(dtype.value_type)
    if pyarrow.types.is_struct(dtype):
        return any(_holds_group(member.type) for member in dtype)
    return False


def _carried_rows(source: pyarrow.RecordBatchReader) -> pyarrow.RecordBatchReader:
    """`source` with the line's identity viewed back to the type the read states.

    The reverse of the narrowing `log_messages` holds an identity at, and
    the reason that narrowing is safe: a stored `curruuid` is the sixteen
    ordered bytes Iceberg keys and sorts on, and a parse handed those bytes
    reads no identity off them -- it derives one of its own from the line's
    clock and content, which names no line that landed. Viewed back, the
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


def fix_window_filter(window: tuple[datetime.datetime, datetime.datetime]) -> Any:
    """The bronze `fix_messages` rows whose event a window covers, as a scan predicate.

    Read off the event clock, because a parsed FIX row is already an event: the
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
    input, so empty and populated streams publish the same contract. A codec
    over a registry that defines nothing is refused here, where every FIX
    table's shape is built, before any table is.
    """
    registry = codec.registry if codec is not None else FixRegistry.from_env()
    _refuse_bare(registry, "the codec's registry")
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
    to the lines in `log_messages`.
    """
    return iceberg_event_field(fix_parse_field(codec, name=name).into_arrow_schema(), name)


def iceberg_event_field(
    schema: pyarrow.Schema,
    name: str = "FixMsg",
) -> Field:
    """A parsed schema narrowed to what Iceberg v2 stores, and declared as a table.

    The layout, declared once here and carried on the field alone. The key is
    `curruuid` and nothing beside it: a row landed again under an identity
    already held replaces it rather than landing beside it, so a replay lands
    nothing twice. The partition is the hour of `currunix` and nothing beside
    it: a key is scoped to its partition, so the partition has to be the
    event's own instant for two arrivals of one row to meet -- and the row
    already carries that instant, so a materialized copy of it would be a
    second owner of one fact. The sort order is `currunix, seqnum, curruuid`
    within a partition, so the events of one instant read in the order of
    their places, place zero's null first.

    The schema contains only native message columns. The source identities
    in `srcuuids` link to `log_messages`, which holds the capture facts.

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
    members = [_declared(_plain(member)) for member in schema]
    sorted_by = json.dumps([[column, "asc"] for column in SORT_COLUMNS], separators=(",", ":"))
    return Field.from_arrow_schema(
        pyarrow.schema(members, metadata={SORT_ORDER: sorted_by}),
        name=name,
    )


def _declared(member: pyarrow.Field) -> pyarrow.Field:
    """`member` carrying what this table -- and not the row -- says of it."""
    marked: dict[str, str] = {}
    if member.name == MESSAGE_KEY:
        # A key identifies every row, so it is required even where the
        # native row -- the book's, a union over every market leaf -- states
        # it nullable; the write then refuses a row that carries none.
        member = member.with_nullable(False)
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
    """A storage field without semantic extension metadata, including children."""
    member = member.with_type(_stored(member.type))
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
        return pyarrow.list_(_plain(dtype.value_field))
    if pyarrow.types.is_large_list(dtype):
        return pyarrow.large_list(_plain(dtype.value_field))
    if pyarrow.types.is_map(dtype):
        return pyarrow.map_(
            _plain(dtype.key_field), _plain(dtype.item_field), keys_sorted=dtype.keys_sorted
        )
    if pyarrow.types.is_struct(dtype):
        return pyarrow.struct([_plain(member) for member in dtype])
    return dtype


__all__ = [
    "CAPTURE_KEY",
    "EVENT_CLOCK",
    "EVENT_PLACE",
    "FIXMSG",
    "MESSAGE_KEY",
    "PARSE_COLUMNS",
    "REGISTRY_VARIABLE",
    "SORT_COLUMNS",
    "SOURCES",
    "TRANSACTION_CLOCK",
    "UNDATED",
    "FixCodec",
    "FixMessages",
    "FixMsg",
    "FixRegistry",
    "MsgType",
    "fix_crate_fields",
    "fix_lifecycle_arrow_reader",
    "fix_lifecycle_messages",
    "fix_message_field",
    "fix_parse_arrow_reader",
    "fix_parse_field",
    "fix_parse_lines",
    "fix_row_messages",
    "fix_schema",
    "fix_schema_tags",
    "fix_window_filter",
    "iceberg_event_field",
]
