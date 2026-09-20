"""The bundled FIX dictionary and the two FIX stages over two tables.

A bridge capture goes through two native stages over one codec, each landing
in a table of its own, in this order and no other:

```text
logs.messages -> parse     -> fix.bronze
fix.bronze    -> lifecycle -> fix.silver
```

`parse` reads every frame a line carried and settles it where it is read: a
parsed message already carries what it implied -- its typed facts lifted, its
deprecated fields restated, the dictionary's derivations run, its identifiers
filled, its instant and its identity settled -- so there is no enriching stage
between the two. `fix.bronze` is that and nothing more: `seqnum` and `prevuuid`
are empty on every row, because nothing has walked yet. `lifecycle` reads the
stored rows back as the messages that wrote them and as the chains they belong
to, filling the `prevuuid` a message follows, the `seqnum` it stands at, the
`parentuuids` before it, the `creaunix`, `expirunix` and `state` its chain
folded forward -- and dating a message the parse could not date by the
`TransactTime` it states, which re-settles its identity. `fix.silver` is the
same row under the same key and layout, restated.

Each stage is a call rather than a pin, and each has two doors the native core
requires to agree row for row: `fix_parse_lines` and `fix_lifecycle_messages`
read messages one at a time, `fix_parse_arrow_reader` and
`fix_lifecycle_arrow_reader` read a whole table in batches. A task holding a
table takes the batch door; a reader holding lines takes the line door.
Neither dates a message from anything the other cannot see -- a capture's own
clock is context and stamps nothing -- so the same bytes answer the same event
through either.
"""

from __future__ import annotations

import datetime
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
    IDENTITY_PARTITION,
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

#: The instant both tables are laid out by: the one the event happened at,
#: which is what the message stated and never what a line was printed at. The
#: core's own name for it, and the first of the sixteen columns every event's
#: schema opens with.
EVENT_CLOCK = "currunix"

#: The clock the walk dates a message by where the parse could not: the
#: `TransactTime(60)` it states. A parse pins an undated message at `UNDATED`,
#: so a bronze row at the pin whose transaction time falls in a window is that
#: window's, and `fix_window_filter` reads it there.
TRANSACTION_CLOCK = "transacttime"

#: The identities a message was read from: provenance, never lineage. A row
#: parsed from a stored line names that line's `curruuid` here and nothing
#: else, and no walk moves it -- which is what puts a capture's own columns
#: back beside a walked row.
SOURCES = "srcuuids"

#: The column the codec reads each message out of. The default pin; a codec
#: naming another payload column answers that one instead.
PAYLOAD = "body"


#: The capture column neither FIX table stores: the bytes themselves.
#:
#: They are a fact about one *line*, and a row here is an *event*. One message
#: logged at four hops is four lines -- four different bodies -- and one row,
#: so the column would be one arrival's answer standing in for the event's.
#: `logs.messages` holds every one of them; a row here names the line it was
#: read from with `sourceurl` and `rownum`, and what it is re-emitted from is
#: `fixentries`, the arrival record, which rebuilds the message and never the
#: line that carried it. The line's own `currhashcode` needs no dropping: the
#: fixed row takes that name for the event's code, so `fix_schema_carrying`
#: drops the carried one rather than duplicating it.
UNSTORED = (PAYLOAD,)

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

#: The Arrow field metadata a semantic native datatype crosses on. Iceberg
#: stores the storage type, so a column carrying one is narrowed here rather
#: than refused at the table boundary where the reason is no longer legible.
_EXTENSION_KEYS = (b"ARROW:extension:name", b"ARROW:extension:metadata")

#: What the carrier states about its own table and not about this one: its
#: key, its partition -- a transform or the identity one -- and its sort. A
#: capture column carried in front of the fixed row keeps what it *is* and
#: loses where `logs.messages` put it: both FIX tables are keyed, laid out
#: and sorted by the event, not by the line the event was read from.
_CARRIED_LAYOUT = (
    PRIMARY_KEY.encode(),
    PARTITION_KEY.encode(),
    IDENTITY_PARTITION.encode(),
    SORT_KEY.encode(),
)


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
    """The parse over a stored capture's batches, as rows: `fix.bronze`'s door.

    `parse_text_arrow_reader` is the twin of `parse_text_lines`: the capture's
    own columns lead each row, a column named after a field fills that field,
    and one source row answers a row per message it carried. The shape is
    what `fix_parse_field` states, so nothing crosses back through the message
    shape to be written again. That shape still carries the capture's own text
    columns; dropping them, and narrowing the rest to what a table stores, is
    the storage boundary's job and `stored_arrow_reader` is where that happens.

    Nothing here has walked: `seqnum`, `prevuuid`, `prevunix` and
    `parentuuids` are empty on every row this answers, and a message read back
    off one stands at step zero of a chain no walk has named yet. A carrier's
    `curruuid` is each message's one source, `srcuuids`; a carrier stating
    none leaves the parse to recompute it from the line's bytes and instant,
    which is equal only while those are.
    """
    return codec.parse_text_arrow_reader(source)


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


def fix_arrival_reader(source: pyarrow.RecordBatchReader) -> pyarrow.RecordBatchReader:
    """A window's rows in the order the capture logged them: `sourceurl`, then `rownum`.

    A chain is read off a stream, and a stream has an order: the walk states
    each message as the one after the live message it follows, so the order it
    is handed decides which message that is. The parse hands it the lines in
    the order they were logged; a table hands back whatever its layout is --
    one partition after another, the pinned hour first -- which is no order a
    venue described. So a window read off a table is put back in the order its
    lines were read in before the walk sees it: the object each row names and
    the line's place in it, which is the order the text reader traversed them
    in. The whole window is held for the sort, as the walk holds it anyway.
    """
    held = source.read_all()
    ordered = held.sort_by([("sourceurl", "ascending"), ("rownum", "ascending")])
    return pyarrow.RecordBatchReader.from_batches(
        source.schema, ordered.to_batches() if ordered.num_rows else []
    )


def fix_lifecycle_arrow_reader(
    codec: FixCodec,
    source: pyarrow.RecordBatchReader,
) -> pyarrow.RecordBatchReader:
    """The walk over stored rows, as rows: `fix.silver`'s door.

    The walk reads each row back as the message that wrote it, so what it is
    given has to be a message and nothing else: a capture's own column carried
    beside the row would be read as an arrived pair, and a pair that differs
    between two logs of one message -- a line number, a line clock -- is
    enough to make one event look like several. The capture's columns are held
    back here and put in front again afterwards. The rows are walked in the
    order given, which is the stream's order the chains are read off: the
    parse's own for its output, and `fix_arrival_reader`'s for a window read
    back off a table.

    Put back by name and not by position: the walk dates a message the parse
    could not, orders what it answers by that instant, and settles the
    identity again, so its rows do not come out in the order they went in.
    What it never moves is `srcuuids`, the line each row was read from, and a
    capture's columns are that line's facts -- so each walked row takes them
    from the line it names. A row read off a table arrives narrowed and is
    widened to the dictionary's own types first, which is why this door reads
    `fix.bronze` and `fix_parse_arrow_reader`'s output alike.
    """
    kept = _row_columns(codec, source.schema)
    carried = [name for name in source.schema.names if name not in set(kept)]
    if not carried:
        return codec.lifecycle_arrow_reader(_dictionary_rows(codec, source))
    held: list[pyarrow.RecordBatch] = []
    keys: list[pyarrow.Array] = []
    beside = pyarrow.schema([source.schema.field(name) for name in carried])
    narrowed = pyarrow.schema([source.schema.field(name) for name in kept])

    def _split() -> Iterator[pyarrow.RecordBatch]:
        # The walk holds every row until it has read the last one, so the
        # columns held back for it are whole before the first one is asked.
        for batch in source:
            held.append(batch.select(carried))
            row = batch.select(kept)
            keys.append(_source_keys(row.column(SOURCES)))
            yield row

    rows = _dictionary_rows(codec, pyarrow.RecordBatchReader.from_batches(narrowed, _split()))
    walk = codec.lifecycle_arrow_reader(rows)
    answered = pyarrow.schema(
        [
            source.schema.field(name) if name in carried else walk.schema.field(name)
            for name in source.schema.names
        ],
        metadata=source.schema.metadata,
    )

    def _joined() -> Iterator[pyarrow.RecordBatch]:
        lines = pyarrow.Table.from_batches(held, beside) if held else beside.empty_table()
        named = pyarrow.concat_arrays(keys) if keys else pyarrow.array([], pyarrow.binary(16))
        for batch in walk:
            at = pyarrow.compute.index_in(_source_keys(batch.column(SOURCES)), value_set=named)
            if at.null_count:
                raise ValueError("the lifecycle answered a row from a line it was not given")
            taken = lines.take(at)
            yield pyarrow.RecordBatch.from_arrays(
                [
                    taken.column(name).combine_chunks() if name in carried else batch.column(name)
                    for name in source.schema.names
                ],
                schema=answered,
            )

    return pyarrow.RecordBatchReader.from_batches(answered, _joined())


def _source_keys(sources: pyarrow.Array) -> pyarrow.Array:
    """The one line each row was read from, as the sixteen bytes it is.

    A row the batch door answered names exactly one source: the line it was
    parsed out of. A row naming none was made from raw bytes and a row naming
    two was not written by this pipeline, and neither can take a capture's
    columns back, so both are refused rather than matched to nothing.
    """
    if not len(sources):
        return pyarrow.array([], pyarrow.binary(16))
    counted = pyarrow.compute.fill_null(pyarrow.compute.list_value_length(sources), 0)
    if not pyarrow.compute.all(pyarrow.compute.equal(counted, 1)).as_py():
        raise ValueError("a row read off a capture names one source line")
    flat = pyarrow.compute.list_flatten(sources)
    return flat.storage if isinstance(flat, pyarrow.ExtensionArray) else flat


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


def _row_columns(codec: FixCodec, schema: pyarrow.Schema) -> list[str]:
    """Which of `schema`'s columns the fixed row itself defines.

    Read off the codec's own dictionary, because that is the one that parsed
    the rows. A capture column named after a field is folded onto that field
    by the parse, so it is one of these; the rest are the capture's own and
    ride in front of the row.
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


def fix_carrier(carrier: Field | None = None) -> Field:
    """A capture's own columns as the parse carries them in front of the row.

    The raw `Message` contract with its own table's layout taken off: a
    carried column states what the capture saw, and both FIX tables are keyed
    and partitioned by the event instead. `logs.messages` is keyed on the
    line's own content code -- its `currhashcode` -- and laid out by the hour
    the line was printed in; `fix.bronze` and `fix.silver` are keyed on `curruuid` and laid
    out by the hour the event happened in, because one message logged at
    three hops is three lines and one row. Leaving either marking on would
    publish a second key and a second partition spec that nothing here means.

    A carried column whose folded name the fixed row already takes is dropped
    by `fix_schema_carrying` rather than duplicated, which is what naming a
    capture after the field it fills is for: `sourceurl`, `msgsessionid`,
    `msgctxid` and `msgseqnum` are the message's own columns, the line's
    `curruuid` is dropped as the event's own identity takes its name -- it
    reaches the row as the message's one source, `srcuuids`, and nowhere else
    -- and the line's `currhashcode` is dropped the same way, because the
    fixed row takes that name for the code of the event. `rownum`,
    `timestamp`, `timepartition`, `threadId`, `pluginid`, `level` and `body`
    ride in front: `pluginid` among them, because the row's own column for the
    plugin is `msgpluginid` and the raw contract spells its capture as the
    bridge does.

    Seven here and six in the table: `UNSTORED` -- the payload the parse reads
    every message out of -- rides through the parse and is dropped at the
    storage boundary, because the bytes are a line's and a stored row is an
    event's.
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
    """The row the parse writes: the fixed row behind the capture's own columns.

    `fix_schema(registry, "fixmsg")` is the row yggdryl publishes and
    `fix_schema_carrying` is the supported way to put a capture's own columns
    in front of it, so no column here is spelled twice and none is yggfin's to
    define. It is answered from the dictionary alone, without consuming an
    input row, so an empty window creates the same table a full one does. The
    walk answers this same shape back, restated.
    """
    registry = codec.registry if codec is not None else fix_registry()
    return fix_schema_carrying(fix_carrier(carrier), fix_schema(registry, FIXMSG))


def fix_message_field(
    codec: FixCodec | None = None,
    carrier: Field | None = None,
    *,
    name: str = "FixMessage",
) -> Field:
    """The one field both FIX tables are declared with: that row, as a table stores it.

    `fix.bronze` and `fix.silver` are one shape -- what the parse answered and
    what the walk restated are the same row, and only what the walk filled
    tells a silver row from its bronze twin -- so there is one field, one key,
    one partition and one sort order, declared here once. What yggfin adds to
    the row is the three things a table is -- which column names one, which
    lays it out, and where a row sits inside a partition -- the narrowing a
    stored column needs, and the two carried columns a stored row does not
    hold. Nothing else: a column yggdryl named is not renamed, retyped or
    duplicated here, and the two that are dropped are the capture's own text
    rather than any of the dictionary's.
    """
    return iceberg_fix_field(
        fix_parse_field(codec, carrier, name=name).into_arrow_schema(),
        name,
        UNSTORED if codec is None else (codec.payload_column,),
    )


def iceberg_fix_field(
    schema: pyarrow.Schema,
    name: str = "FixMessage",
    unstored: Iterable[str] = UNSTORED,
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
    "SORT_COLUMNS",
    "SOURCES",
    "TRANSACTION_CLOCK",
    "UNDATED",
    "UNSTORED",
    "FixCodec",
    "FixMessages",
    "FixMsg",
    "FixRegistry",
    "MsgType",
    "fix_arrival_reader",
    "fix_carrier",
    "fix_cfb_fields",
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
    "fix_schema_carrying",
    "fix_schema_tags",
    "fix_text_options",
    "fix_window_filter",
    "global_registry",
    "iceberg_fix_field",
    "install_global_registry",
    "registry_path",
]
