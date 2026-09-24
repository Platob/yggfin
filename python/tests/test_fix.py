"""The FIX seam: the two stages, the row they answer, and what storage narrows."""

from __future__ import annotations

import datetime
from collections import defaultdict
from pathlib import Path

import pyarrow
import pytest
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, IsNull, LessThan, Or
from yggdryl import IOBase
from yggdryl.fix import fix_schema

from rekep.fields import stored_arrow_reader
from rekep.fix import (
    CHAIN_STEP,
    EVENT_CLOCK,
    FIXMSG,
    MESSAGE_KEY,
    SORT_COLUMNS,
    SOURCES,
    TRANSACTION_CLOCK,
    UNDATED,
    fix_codec,
    fix_crate_fields,
    fix_lifecycle_arrow_reader,
    fix_lifecycle_messages,
    fix_message_field,
    fix_parse_arrow_reader,
    fix_parse_field,
    fix_parse_lines,
    fix_registry,
    fix_row_messages,
    fix_text_options,
    fix_window_filter,
    iceberg_fix_field,
)
from rekep.iceberg import partition_keys, primary_keys, sort_keys
from rekep.text import Message

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "data" / "capture" / "ulbridge.log"

#: What the bundled capture states, read through the pins this package sets.
#: The same numbers `cargo run --example fix_capture` prints in a core
#: checkout: 144 physical lines answer 79 messages, because a line can carry
#: two frames and a line carrying none answers nothing.
LINES = 144
MESSAGES = 79

#: The lifecycle's business chains as `rows, identities, last seqnum`. A
#: crosscode is the first stated business identifier, never the bridge header
#: capture; lifecycle removes repeated deliveries and may emit an expiry. The
#: shipped header matches every one of the 144 lines, so every observation
#: carries the session, context and sequence the fold merges on.
CHAINS = {
    "": (1, 1, 0),
    "00026877709XOEA0": (1, 1, 0),
    "00026877711XOEA0": (4, 4, 2),
    "00026877712XOEA0": (2, 1, 0),
    "00026877713XOEA0": (3, 3, 2),
    "175631111-2274-42616_225": (4, 3, 1),
    "20260814_TP1_CLIENT_1013": (1, 1, 0),
    "923465840": (1, 1, 0),
    "OD9EOEDJ401": (1, 1, 0),
    "OD9EOEDJ402": (1, 1, 0),
}

#: How many rows a table keyed on `curruuid` holds after the whole capture,
#: in either stage: the walk restates events and adds none.
EVENTS = sum(events for _, events, _ in CHAINS.values())

#: The native row is the only FIX row shape at every stage.
ROW = 128
CRATE = 32
PARSED_EVENTS = 49

#: Two lines the bridge header matches, one under a point and one under a
#: comma, and two messages: the second states no `SendingTime` of its own, so
#: it settles at the codec's pin whatever its line was stamped at -- a line's
#: clock is context to a message and never its event.
BRIEF = (
    b"2026-08-14 00:05:01.147 [250-e7256476:9effef3e6a:72504] [ULBridge] (INFO) "
    b"Sending : 8=FIX.4.4|35=D|11=A1|55=AAPL|10=0|\n"
    b"2026-08-14 00:05:01,148 [77] [FixSession_XPAR] (INFO) "
    b"sending >> 8=FIX.4.2|35=D|11=A2|55=TTF|10=0|\n"
)

#: The zone every instant here is spelled in.
UTC = datetime.timezone.utc


def _capture(tmp_path: Path) -> IOBase:
    source = tmp_path / "bridge.log"
    source.write_bytes(BRIEF)
    return IOBase.from_uri(source.as_uri())


def _codec():
    """The codec the two FIX stages pin: the dictionary and the undated floor, and
    no capture order, because the batch door fills a field from the column
    named after it."""
    return fix_codec(fix_registry())


def _line_codec():
    """The codec a reader holding lines pins: the same, plus the header's
    capture order, because the line door resolves a bracket part by position."""
    return fix_codec(fix_registry(), options=fix_text_options())


def _reader(table: pyarrow.Table) -> pyarrow.RecordBatchReader:
    return pyarrow.RecordBatchReader.from_batches(table.schema, table.to_batches())


def _msgtype(message) -> str | None:
    """The wire code a message answers, however it spells having none."""
    held = message.get_by_name("msgtype")
    return None if held is None else held.as_py()


def _parsed(handle: IOBase) -> pyarrow.Table:
    """One capture through the parse, in the parse's own shape."""
    return fix_parse_arrow_reader(
        _codec(), handle.read_arrow_reader(options=Message.text_options())
    ).read_all()


def _parsed_fixture() -> pyarrow.Table:
    """The fixture through the parse's native, pre-storage row shape."""
    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        return _parsed(handle)
    finally:
        handle.close()


def _raw(handle: IOBase) -> pyarrow.Table:
    """One capture through the first stage, as `fix.raw` stores it."""
    codec = _codec()
    reader = handle.read_arrow_reader(options=Message.text_options())
    return stored_arrow_reader(
        fix_parse_arrow_reader(codec, reader), fix_message_field(codec)
    ).read_all()


def _refined(raw: pyarrow.Table) -> pyarrow.Table:
    """The second stage over stored `fix.raw` rows, as `fix.refined` stores it."""
    codec = _codec()
    return stored_arrow_reader(
        fix_lifecycle_arrow_reader(codec, _reader(raw)), fix_message_field(codec)
    ).read_all()


def _chains(rows: pyarrow.Table) -> dict[str, tuple[int, int, int]]:
    """Each chain as `messages, events, last seqnum`, off the stored columns."""
    held: dict[str, list] = defaultdict(lambda: [0, set(), 0])
    for code, identity, step in zip(
        rows.column("crosscode").to_pylist(),
        rows.column(MESSAGE_KEY).to_pylist(),
        rows.column(CHAIN_STEP).to_pylist(),
        strict=True,
    ):
        seen = held[code or ""]
        seen[0] += 1
        seen[1].add(identity)
        seen[2] = max(seen[2], step or 0)
    return {code: (count, len(seen), step) for code, (count, seen, step) in held.items()}


def _sources(rows: pyarrow.Table) -> list[bytes]:
    """The one line each row names, as the bytes `logs.messages` keys it by."""
    return [held[0] for held in rows.column(SOURCES).to_pylist()]


@pytest.fixture(scope="module")
def lines() -> pyarrow.Table:
    """The whole bridge corpus as `logs.messages` holds it: the `Message`
    contract past the storage boundary, the line's own identity on every row
    as the sixteen bytes a table keys on."""
    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        return stored_arrow_reader(
            handle.read_arrow_reader(options=Message.text_options()), Message.into_field()
        ).read_all()
    finally:
        handle.close()


@pytest.fixture(scope="module")
def raw() -> pyarrow.Table:
    """The whole bridge corpus, through `parse_fix_raw`'s parse."""
    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        return _raw(handle)
    finally:
        handle.close()


@pytest.fixture(scope="module")
def refined(raw: pyarrow.Table) -> pyarrow.Table:
    """The same corpus through the second stage, over the first one's rows."""
    return _refined(raw)


# -- the row -----------------------------------------------------------------


def test_the_published_row_is_the_dictionarys_native_shape() -> None:
    """Field for field: names, types, nullability and order, all yggdryl's."""
    registry = fix_registry()
    declared = fix_schema(registry, FIXMSG)

    assert (
        fix_parse_field()
        .into_arrow_schema()
        .equals(declared.into_arrow_schema(), check_metadata=True)
    )
    assert len(fix_schema(registry, FIXMSG)) == ROW
    assert len(fix_crate_fields()) == CRATE
    assert {member.name for member in fix_crate_fields()} >= {
        "currunix",
        "creaunix",
        "exprtime",
        "prevunix",
        "snapunix",
        "curruuid",
        "crossuuid",
        "crosscode",
        "currhashcode",
        "crosshashcode",
        "prevuuid",
        "seqnum",
        "parentuuids",
        "srcuuids",
        "identifiers",
        "state",
        "msgcat",
        "isincode",
        "cusipcode",
        "sedolcode",
        "bloombergcode",
        "miccode",
        "figicode",
    }
    names = [member.name for member in declared]
    for capture in ("msgthreadid", "loglevel", "body"):
        assert capture not in names, capture
    assert {"msgsessionid", "msgctxid", "msgseqnum", "msgpluginid"} <= set(names)
    assert len(declared) == ROW


def test_one_field_declares_both_tables_keyed_partitioned_and_sorted_by_the_event() -> None:
    """The key, the partition and the sort order, declared once on the field
    both tables are created with, and nothing else carrying either mark."""
    field = fix_message_field()

    assert primary_keys(field) == [MESSAGE_KEY] == ["curruuid"]
    assert partition_keys(field) == {EVENT_CLOCK: "hour"} == {"currunix": "hour"}
    assert list(sort_keys(field)) == list(SORT_COLUMNS) == ["currunix", "seqnum", "curruuid"]
    assert field[MESSAGE_KEY].nullable is False
    assert field[EVENT_CLOCK].nullable is False


def test_the_retired_columns_are_gone_rather_than_kept_beside_the_new_ones() -> None:
    """A fact answered twice is a fact two readers disagree about."""
    names = set(fix_message_field().into_arrow_schema().names)

    # Renamed by yggdryl, under the same tag: the old spelling is not kept.
    for retired, holds in (
        ("unix", "currunix"),
        ("hashcode", "currhashcode"),
        ("updatedat", "currunix"),
        ("prevupdatedat", "prevunix"),
        ("createdat", "creaunix"),
        ("snapshotat", "snapunix"),
        ("expiredat", "exprtime"),
        ("msghash", "currhashcode"),
        ("msgphash", "crosshashcode"),
        ("prevmsghash", "prevuuid"),
        ("altids", "identifiers"),
        ("bridgesessionid", "msgsessionid"),
    ):
        assert retired not in names, retired
        assert holds in names, holds
    # Retired outright: the event answers the fact under its own vocabulary,
    # and a market fact is FIX's own field rather than a trait's column.
    for retired in (
        "instuuid",
        "code",
        "version",
        "parentclordid",
        "parentorderid",
        "sendersessionid",
        "targetsessionid",
        "prevpluginid",
        "sessionmsgid",
        "instids",
        "px",
        "qty",
        "prevpx",
        "prevqty",
        "unit",
        "symbolticker",
        "tradable",
        "recordedat",
    ):
        assert retired not in names, retired
    # Tags 44, 38 and 53 are their own columns again; what a message is about
    # is the trait `FixMsg.price` answers off them, never a second column.
    assert {"price", "orderqty", "quantity", "lastpx", "avgpx", "lastqty"} <= names


def test_the_storage_boundary_narrows_what_a_row_filter_cannot_be_lowered_to() -> None:
    """Sixteen ordered bytes, microsecond instants, signed codes, no name."""
    schema = fix_message_field().into_arrow_schema()

    for name in ("curruuid", "crossuuid", "prevuuid"):
        assert schema.field(name).type == pyarrow.binary(16), name
    for name in ("parentuuids", SOURCES):
        assert schema.field(name).type.field(0).type == pyarrow.binary(16), name
    assert not [
        member.name for member in schema if isinstance(member.type, pyarrow.BaseExtensionType)
    ]
    # A semantic datatype crosses on the column's own metadata rather than as
    # an Arrow extension type, and a table stores neither.
    assert not [
        member.name for member in schema if b"ARROW:extension:name" in (member.metadata or {})
    ]
    assert not [
        member.name
        for member in schema
        if pyarrow.types.is_timestamp(member.type) and member.type.unit == "ns"
    ]
    # Iceberg's only sixty-four-bit integer is signed, and a content code
    # fills all of it, so the column says so rather than overflowing a commit.
    for code in ("currhashcode", "crosshashcode", "seqnum"):
        assert schema.field(code).type == pyarrow.int64(), code
    assert len(schema) == ROW
    assert schema.field(MESSAGE_KEY).metadata[b"FIX:tag"] == b"65039"
    assert schema.field(MESSAGE_KEY).metadata[b"ICEBERG:primary_key"] == b"true"


def test_the_stored_row_holds_none_of_the_text_it_was_read_from(raw) -> None:
    """The bytes are a line's, the digest of them is a line's, and this row is
    an event's.

    A message logged at four hops is four lines -- four different bodies, four
    different digests -- and one row, so either column would be one arrival's
    answer standing in for the event's. `logs.messages` holds all four; the row
    names the line it was read from and is re-emitted from its own arrival
    record.
    """
    stored = fix_message_field().into_arrow_schema()
    parsed = fix_parse_field().into_arrow_schema()

    for column in ("msgthreadid", "loglevel", "body"):
        assert column not in raw.column_names, column
        assert column not in stored.names, column
        assert column not in parsed.names, column
    assert raw.column(SOURCES).null_count == 0
    # What the wire is rebuilt from is on the row. A fully projected message
    # has no residual entries; one with an unprojected pair retains it.
    assert raw.column("nofixentries").null_count == 0
    counts = raw.column("nofixentries").to_pylist()
    assert 0 in counts and any(count > 0 for count in counts)
    assert counts == [len(entries) for entries in raw.column("fixentries").to_pylist()]


def test_the_narrowing_walks_into_a_nested_type() -> None:
    """An identity inside a group is narrowed where it sits."""
    nested = pyarrow.schema(
        [
            pyarrow.field(
                "parties",
                pyarrow.list_(
                    pyarrow.field(
                        "party",
                        pyarrow.struct([pyarrow.field("partyuuid", pyarrow.uuid())]),
                        nullable=False,
                    )
                ),
            )
        ]
    )

    narrowed = iceberg_fix_field(nested, "Nested").into_arrow_schema()

    member = narrowed.field("parties").type.field(0).type.field(0)
    assert member.type == pyarrow.binary(16)


def test_a_content_code_above_the_signed_range_is_read_and_not_refused() -> None:
    """The eight bytes are the code; which half of the range they land in is
    the storage type's business and never a lost row."""
    field = fix_message_field()
    parsed = fix_parse_field().into_arrow_schema()
    code = 2**63 + 5
    columns = []
    for member in parsed:
        if member.name in ("currhashcode", "crosshashcode"):
            columns.append(pyarrow.array([code], member.type))
        elif member.name in ("curruuid", "crossuuid"):
            columns.append(
                pyarrow.ExtensionArray.from_storage(
                    member.type, pyarrow.array([b"\x01" * 16], pyarrow.binary(16))
                )
            )
        elif member.name in ("currunix", "creaunix"):
            columns.append(pyarrow.array([UNDATED], member.type))
        elif member.name == "beginstring":
            columns.append(pyarrow.array(["FIX.4.4"], member.type))
        else:
            columns.append(pyarrow.nulls(1, member.type))
    source = pyarrow.RecordBatchReader.from_batches(
        parsed, [pyarrow.RecordBatch.from_arrays(columns, schema=parsed)]
    )

    held = stored_arrow_reader(source, field).read_all()

    assert held.schema.field("currhashcode").type == pyarrow.int64()
    assert held.column("currhashcode").to_pylist() == [code - 2**64]
    assert field.into_arrow_schema().field("currhashcode").type == pyarrow.int64()


def test_a_pin_the_codec_does_not_take_is_refused_by_name() -> None:
    """A version is what a row states, so pinning one is an error and not a
    silently forwarded keyword that parses every line under the wrong rule."""
    with pytest.raises(TypeError, match="unexpected keyword argument 'version'"):
        fix_codec(fix_registry(), version="4.4")


# -- the first stage: the parse ----------------------------------------------


def test_a_message_stating_no_clock_answers_the_same_identity_on_every_read(tmp_path) -> None:
    """What makes a replay idempotent: nothing in the identity is read from now."""
    handle = _capture(tmp_path)
    try:
        first, second = _raw(handle), _raw(handle)
    finally:
        handle.close()

    assert first.num_rows == second.num_rows == 2
    assert first.column(MESSAGE_KEY).to_pylist() == second.column(MESSAGE_KEY).to_pylist()
    assert len(set(first.column(MESSAGE_KEY).to_pylist())) == 2
    # The message inside the second line states no clock, so it takes the
    # codec's floor rather than the instant the parse ran.
    assert first.column(EVENT_CLOCK).to_pylist()[1] == UNDATED


def test_the_capture_answers_a_message_per_frame_and_not_a_row_per_line(raw) -> None:
    """144 lines, 79 messages, and 49 parsed event identities."""
    assert raw.num_rows == MESSAGES
    assert len(set(raw.column(MESSAGE_KEY).to_pylist())) == PARSED_EVENTS


def test_the_parse_places_no_message_in_a_chain(raw) -> None:
    """`fix.raw` has no chain, and says so.

    The parse fills what a message implied about itself and nothing about the
    message before it: `seqnum`, `prevuuid` and `prevunix` are empty on every
    row and `parentuuids` names nobody. A message read back off the row stands
    at step zero, which is what an empty place reads as.
    """
    assert raw.column(CHAIN_STEP).null_count == raw.num_rows
    assert raw.column("prevuuid").null_count == raw.num_rows
    assert raw.column("prevunix").null_count == raw.num_rows
    assert all(not parents for parents in raw.column("parentuuids").to_pylist())
    # What the parse did settle is on every row: the state the message states
    # and the instant its chain opened at, before any fold.
    assert raw.column("state").null_count == 0
    assert raw.column("creaunix").null_count == 0
    messages = list(fix_row_messages(_codec(), _reader(raw)))
    assert len(messages) == MESSAGES
    assert {message.seqnum for message in messages} == {0}
    assert all(message.prevuuid is None for message in messages)


def test_a_message_names_the_stored_line_it_was_parsed_out_of(lines, raw) -> None:
    """Provenance, never lineage.

    `logs.messages` states the line's own `curruuid` on every row, and the
    batch door reads it back -- viewed from the sixteen bytes a table keys on
    to the identity the read states -- as each message's one source. That
    view is what makes the join exact: a carrier stating no identity leaves
    the parse to recompute one from the line's clock and body alone, which is
    never the one the read derived over the line's place and the object it
    was read from.
    """
    assert lines.num_rows == LINES
    assert lines.column("curruuid").null_count == 0
    named = set(lines.column("curruuid").to_pylist())
    sources = raw.column(SOURCES).to_pylist()

    assert all(len(held) == 1 for held in sources), "one line per message"
    assert set(_sources(raw)) <= named
    # The identity the line landed under, not one the body alone implies: a
    # carrier that states none answers a recomputed identity instead -- at
    # the line's own instant, over a code that digests neither its row number
    # nor its object -- which is the whole reason the column is stated on
    # every row the read produces.
    unstated = stored_arrow_reader(
        fix_parse_arrow_reader(_codec(), _reader(lines.drop_columns(["curruuid"]))),
        fix_message_field(),
    ).read_all()
    recomputed = [
        (held, source)
        for held, source in zip(_sources(unstated), _sources(raw), strict=True)
        if held != source
    ]
    assert len(recomputed) == len(sources), "no recomputed identity is the stored one"
    assert all(held[:6] == source[:6] for held, source in recomputed), "the line's millisecond"
    assert set(held for held, _ in recomputed).isdisjoint(named)
    # And a table half of whose rows predate the column: each message names
    # the identity its own line stated, and a recomputed one where it stated
    # none.
    halved = lines.set_column(
        lines.column_names.index("curruuid"),
        "curruuid",
        pyarrow.array(
            [
                held if index % 2 else None
                for index, held in enumerate(lines.column("curruuid").to_pylist())
            ],
            pyarrow.binary(16),
        ),
    )
    partly = stored_arrow_reader(
        fix_parse_arrow_reader(_codec(), _reader(halved)), fix_message_field()
    ).read_all()
    stated = [held in named for held in _sources(partly)]
    assert any(stated) and not all(stated), "each message reads what its own row stated"
    assert set(_sources(partly)) <= named | set(_sources(unstated))


def test_every_restatement_of_an_event_settles_on_one_identity(raw) -> None:
    """The gap between a chain's messages and its events, row by row: the same
    message logged at a second hop answers the identity the first one did, and
    the rows it was read from are different lines."""
    held: dict[bytes, set[bytes]] = defaultdict(set)
    for identity, source in zip(raw.column(MESSAGE_KEY).to_pylist(), _sources(raw), strict=True):
        held[identity].add(source)
    restated = {identity: lines for identity, lines in held.items() if len(lines) > 1}

    assert restated, "the capture logs messages at several hops"
    assert len(held) == PARSED_EVENTS
    # Each restatement is its own line, so the arrivals of one event span
    # several rows of `logs.messages` -- which is why the row cannot carry one
    # line's bytes or one line's digest as if they were the event's.
    assert sum(len(lines) for lines in restated.values()) > len(restated)


def test_a_market_fact_is_fixs_own_field_and_the_trait_answers_off_it(raw) -> None:
    """`Price(44)`, `OrderQty(38)` and `LastPx(31)` are columns of the row and
        the ladder is not: what a message is about is `FixMsg.price`, read off those
    columns by a reader holding the message, never a second column stored
    beside them."""
    rows = raw.select(("price", "lastpx", "avgpx", "orderqty", "lastqty")).to_pylist()
    messages = list(fix_row_messages(_codec(), _reader(raw)))

    assert sum(1 for row in rows if row["lastpx"]) == 57
    assert sum(1 for row in rows if row["price"] is not None) == 63
    for row, message in zip(rows, messages, strict=True):
        stated = next(
            (row[name] for name in ("price", "lastpx", "avgpx") if row[name] is not None), None
        )
        answered = message.price.as_py() if message.price is not None else None
        if stated is not None:
            assert answered is not None and float(answered) == float(stated)


def test_the_instrument_columns_say_what_the_capture_says(raw) -> None:
    """`symbol` is what `Symbol(55)` stated, and a market that said nothing
    about trading is not a market that said closed."""
    symbols = set(raw.column("symbol").to_pylist())
    assert "ABBN.S" in symbols
    assert None in symbols, "an instrument the capture named only by code"
    # No message in this capture states SecurityTradingStatus(326),
    # TradSesStatus(340) or SecurityStatus(965).
    for stated in ("securitytradingstatus", "tradsesstatus", "securitystatus"):
        assert raw.column(stated).null_count == raw.num_rows, stated


def test_the_bridge_bracket_fills_the_columns_it_names(raw) -> None:
    """A capture is named for the field it fills, and the bracket's first part
    is the session instance -- never what the message says about itself."""
    assert raw.column("msgsessionid").null_count < raw.num_rows
    assert raw.column("msgctxid").null_count < raw.num_rows
    assert raw.column("msgseqnum").null_count < raw.num_rows
    # The plugin the bridge logged the line under, which the header captures
    # as the field it fills: a capture named anything else leaves this column
    # empty on every row, which is what the name is for.
    assert raw.column("msgpluginid").null_count < raw.num_rows


def test_the_two_doors_read_the_same_capture_as_the_same_messages() -> None:
    """A door is a way in, not a second reading: the line door and the batch
    door answer the same messages, in the same order, at the same instants,
    under the same content codes, each naming the same line it was read from.

    The line door pins the header's capture order, because it resolves a
    bracket part by position; the batch door pins none, because a column
    named after a field fills it. What the two still disagree on at the
    pinned core revision is one column the code does not cover:
    `msgdirection`, which the batch door reads as sent off the prose of 56
    bridge rows and the line door leaves unstated.
    """
    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        by_line = [
            (_msgtype(message), message.currunix, message.srcuuids, message.currhashcode)
            for message in fix_parse_lines(
                _line_codec(), handle.read_text_lines(options=fix_text_options())
            )
        ]
        rows = fix_parse_arrow_reader(
            _codec(), handle.read_arrow_reader(options=Message.text_options())
        ).read_all()
    finally:
        handle.close()

    assert rows.num_rows == len(by_line) == MESSAGES
    assert [msgtype for msgtype, _, _, _ in by_line] == rows.column("msgtype").to_pylist()
    assert [code for _, _, _, code in by_line] == rows.column("currhashcode").to_pylist()
    assert [instant for _, instant, _, _ in by_line] == [
        (held - UNDATED) // datetime.timedelta(microseconds=1) * 1_000
        for held in rows.column(EVENT_CLOCK).to_pylist()
    ]
    assert [[str(source.as_py()) for source in sources] for _, _, sources, _ in by_line] == [
        [str(source) for source in sources] for sources in rows.column(SOURCES).to_pylist()
    ]


def test_the_batch_door_also_answers_messages(raw) -> None:
    """The message shape of a stored table, for a reader that wants events
    rather than rows: the same messages the table holds, in the same order,
    read back out of the columns the dictionary defines."""
    held = list(fix_row_messages(_codec(), _reader(raw)))

    assert len(held) == MESSAGES
    assert [_msgtype(message) for message in held] == raw.column("msgtype").to_pylist()
    assert [message.crosscode for message in held] == [
        code or "" for code in raw.column("crosscode").to_pylist()
    ]


# -- the second stage: the walk ----------------------------------------------


def test_the_walk_reads_the_chains_the_capture_describes(refined) -> None:
    """The counts `cargo run --example fix_capture` prints, column for column."""
    assert _chains(refined) == CHAINS
    assert refined.num_rows == sum(rows for rows, _, _ in CHAINS.values())
    assert len(set(refined.column(MESSAGE_KEY).to_pylist())) == EVENTS


def test_the_walk_restates_the_events_and_adds_none(raw, refined) -> None:
    """A `fix.refined` row differs from its `fix.raw` twin in what the walk filled --
    its place, its lineage, the folded state and clocks -- and in the identity
    those re-settle to. The parse could date only the messages that stated a
    sending clock; the walk dates the rest by their transaction time, so those
    leave the pin's hour for the hour they happened in and settle a new
    identity there. What the walk never does is add an event."""
    assert len(set(refined.column(MESSAGE_KEY).to_pylist())) == EVENTS
    pinned = sum(1 for instant in raw.column(EVENT_CLOCK).to_pylist() if instant == UNDATED)
    assert pinned == 63, "the parse dates only what stated SendingTime"
    assert raw.column("sendingtime").null_count == pinned
    assert not any(instant == UNDATED for instant in refined.column(EVENT_CLOCK).to_pylist())
    assert refined.column("prevuuid").null_count < refined.num_rows
    assert refined.column(CHAIN_STEP).null_count < refined.num_rows
    # Provenance is never moved by a walk: each row still names its line.
    assert set(_sources(refined)) <= set(_sources(raw))


def test_a_refined_message_follows_the_messages_before_it(refined) -> None:
    """Lineage: `prevuuid` and every one of `parentuuids` are the `curruuid`
    of messages this table holds, and a step never precedes what it follows."""
    identities = set(refined.column(MESSAGE_KEY).to_pylist())
    instants = dict(
        zip(
            refined.column(MESSAGE_KEY).to_pylist(),
            refined.column(EVENT_CLOCK).to_pylist(),
            strict=True,
        )
    )
    followed = [
        (identity, previous, parents, instant)
        for identity, previous, parents, instant in zip(
            refined.column(MESSAGE_KEY).to_pylist(),
            refined.column("prevuuid").to_pylist(),
            refined.column("parentuuids").to_pylist(),
            refined.column(EVENT_CLOCK).to_pylist(),
            strict=True,
        )
        if previous is not None
    ]

    assert followed, "a walked chain has steps that follow one another"
    for _identity, previous, parents, instant in followed:
        assert previous in identities
        assert parents and all(parent in identities for parent in parents)
        assert previous in parents, "the predecessor is among the parents"
        assert instants[previous] <= instant


def test_a_duplicate_is_not_a_successor(refined) -> None:
    """One message logged at several hops answers one identity, and the walk
    gives every copy the same place, the same lineage and the same state: the
    chain grows by nothing. Dropping a repeat from a stream is `FixDedup`'s
    job, not the walk's, so every copy is still a row here."""
    held: dict[bytes, set[tuple]] = defaultdict(set)
    for row in refined.select(
        (MESSAGE_KEY, "prevuuid", CHAIN_STEP, "state", "parentuuids", EVENT_CLOCK)
    ).to_pylist():
        held[row[MESSAGE_KEY]].add(
            (
                row["prevuuid"],
                row[CHAIN_STEP],
                row["state"],
                tuple(row["parentuuids"] or ()),
                row[EVENT_CLOCK],
            )
        )
    copies = {identity: places for identity, places in held.items() if len(places) > 0}

    assert len(copies) == EVENTS
    assert all(len(places) == 1 for places in held.values())
    assert sum(1 for identity in refined.column(MESSAGE_KEY).to_pylist()) > len(held)


def test_the_walk_reads_the_row_and_never_the_capture_beside_it(raw, refined) -> None:
    """The one thing the batch door must not do.

    A line's text is not content here. The walk reads only the native row,
    whose `srcuuids` name the stored lines it joins back to.
    """
    assert _chains(refined)["00026877711XOEA0"] == CHAINS["00026877711XOEA0"] == (4, 4, 2)
    assert refined.column_names == raw.column_names
    assert set(_sources(refined)) <= set(_sources(raw))
    assert refined.column(EVENT_CLOCK).to_pylist() == sorted(
        refined.column(EVENT_CLOCK).to_pylist()
    )


def test_the_walk_over_stored_rows_rebuilds_native_row_types(raw) -> None:
    """A row read off a table arrives narrowed -- identities as bytes, instants
    at the microsecond, codes signed -- and in the table's own order, one
    partition after another. Widening restores its native fixed-row types
    before lifecycle reads it."""
    codec = _codec()
    field = fix_message_field(codec)
    stored = raw
    native = stored_arrow_reader(
        fix_lifecycle_arrow_reader(codec, _reader(_parsed_fixture())), field
    )
    restored = stored_arrow_reader(fix_lifecycle_arrow_reader(codec, _reader(stored)), field)

    assert restored.read_all().equals(native.read_all())


def test_the_walk_sorts_distinct_effective_instants_and_keeps_equal_ties(raw) -> None:
    """Lifecycle orders different event times while equal times retain input
    order, so a stored scan supplies the deterministic tie order."""
    selected: list[int] = []
    seen = set()
    for index, row in enumerate(raw.select((EVENT_CLOCK, TRANSACTION_CLOCK)).to_pylist()):
        instant = row[EVENT_CLOCK]
        if instant == UNDATED and row[TRANSACTION_CLOCK] is not None:
            instant = row[TRANSACTION_CLOCK]
        if instant not in seen:
            seen.add(instant)
            selected.append(index)
    chronological = raw.take(pyarrow.array(selected))
    reversed_times = chronological.take(pyarrow.array(list(reversed(range(len(selected))))))
    assert _refined(reversed_times).equals(_refined(chronological))

    # Two observations of one event at one instant are one event, so the tie
    # is no longer two rows to order but one row's provenance: the walk keeps
    # both lines under one identity in the core's canonical identity order.
    tied = raw.take(pyarrow.array([0, 1]))
    reversed_tied = tied.take(pyarrow.array([1, 0]))
    held, reversed_held = _refined(tied), _refined(reversed_tied)
    lines = lambda rows: rows.column(SOURCES).to_pylist()  # noqa: E731

    assert held.num_rows == reversed_held.num_rows == 1
    assert set(lines(held)[0]) == set(lines(reversed_held)[0]) == set(_sources(tied))
    assert lines(held)[0] == lines(reversed_held)[0] == sorted(lines(held)[0])
    assert _refined(raw.slice(0, 0)).num_rows == 0


def test_the_walk_settles_the_same_identities_however_often_it_runs(raw) -> None:
    """A replay is a second walk over the same rows, and the table is keyed
    on what it answers, so the answer has to be the same one."""
    first, second = _refined(raw), _refined(raw)

    assert first.column(MESSAGE_KEY).to_pylist() == second.column(MESSAGE_KEY).to_pylist()
    assert first.column("currhashcode").to_pylist() == second.column("currhashcode").to_pylist()
    assert first.column(EVENT_CLOCK).to_pylist() == second.column(EVENT_CLOCK).to_pylist()


def test_the_line_door_walks_the_same_way() -> None:
    """The second stage over messages rather than rows: as many events, and the
    same chain steps, off the same capture."""
    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        walked = list(
            fix_lifecycle_messages(
                _line_codec(),
                fix_parse_lines(_line_codec(), handle.read_text_lines(options=fix_text_options())),
            )
        )
    finally:
        handle.close()

    assert len(walked) == sum(rows for rows, _, _ in CHAINS.values())
    assert len({str(message.curruuid) for message in walked}) == EVENTS
    assert max(message.seqnum for message in walked) == max(step for _, _, step in CHAINS.values())


def test_a_walk_needs_no_capture_sidecar(raw) -> None:
    """Lifecycle consumes native rows; provenance is optional row metadata."""
    unnamed = raw.set_column(
        raw.column_names.index(SOURCES),
        SOURCES,
        pyarrow.nulls(raw.num_rows, raw.schema.field(SOURCES).type),
    )

    walked = fix_lifecycle_arrow_reader(_codec(), _reader(unnamed)).read_all()
    assert walked.num_rows == _refined(raw).num_rows
    assert walked.column(SOURCES).null_count == walked.num_rows


def test_the_refined_window_reads_the_event_clock_and_the_pin() -> None:
    """The `fix.raw` rows a window covers: dated in it, or waiting at the pin
    with a transaction time in it -- or none at all, which is every window."""
    lower = datetime.datetime(2026, 8, 14, tzinfo=UTC)
    upper = datetime.datetime(2026, 8, 15, tzinfo=UTC)

    assert fix_window_filter((lower, upper)) == Or(
        And(GreaterThanOrEqual(EVENT_CLOCK, lower), LessThan(EVENT_CLOCK, upper)),
        And(
            EqualTo(EVENT_CLOCK, UNDATED),
            Or(
                And(
                    GreaterThanOrEqual(TRANSACTION_CLOCK, lower),
                    LessThan(TRANSACTION_CLOCK, upper),
                ),
                IsNull(TRANSACTION_CLOCK),
            ),
        ),
    )
    assert TRANSACTION_CLOCK == "transacttime"
