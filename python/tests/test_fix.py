"""The FIX seam: the two stages, the row they answer, and what storage narrows."""

from __future__ import annotations

import datetime
from collections import defaultdict
from pathlib import Path

import pyarrow
import pytest
from yggdryl import IOBase
from yggdryl.fix import fix_schema, fix_schema_carrying

from rekep.fix import (
    CHAIN_STEP,
    EVENT_CLOCK,
    FIXMSG,
    MESSAGE_KEY,
    PAYLOAD,
    SORT_COLUMNS,
    TEXT_DIGEST,
    UNDATED,
    UNSTORED,
    fix_arrow_messages,
    fix_arrow_reader,
    fix_carrier,
    fix_codec,
    fix_line_messages,
    fix_message_field,
    fix_parse_field,
    fix_registry,
    fix_stored_reader,
    fix_text_options,
    iceberg_fix_field,
)
from rekep.iceberg import partition_keys, primary_keys, sort_keys
from rekep.text import Message

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "python" / "tests" / "data" / "ulbridge.log"

#: What the bundled capture states, read through the pins this package sets.
#: The same numbers `cargo run --example fix_capture` prints in a yggdryl
#: checkout: 144 physical lines answer 79 messages, because a line can carry
#: two frames and a line carrying none answers nothing.
LINES = 144
MESSAGES = 79

#: The chains that walk read, as `messages, events, last seqnum`. Fewer events
#: than messages is the bridge logging one message at every hop it passed: each
#: arrival restates the event rather than opening a second one, so they share
#: one `curruuid` and the primary key folds them.
CHAINS = {
    "": (1, 1, 0),
    "00026877709XOEA0": (1, 1, 0),
    "00026877711XOEA0": (49, 31, 7),
    "00026877712XOEA0": (3, 2, 0),
    "00026877713XOEA0": (8, 4, 3),
    "175631111-2274-42616_225": (10, 7, 1),
    "20260814_TP1_CLIENT_1013": (3, 3, 2),
    "923465840": (1, 1, 0),
    "OD9EOEDJ401": (2, 2, 1),
    "OD9EOEDJ402": (1, 1, 0),
}

#: How many rows a table keyed on `curruuid` holds after the whole capture.
EVENTS = sum(events for _, events, _ in CHAINS.values())

#: One line the bridge header matches, and one it does not: the second spells
#: its fraction `,148`, which the row header does not read, so the row carries
#: no clock at all and the message inside it states none either.
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
    return fix_codec(fix_registry(), options=fix_text_options())


def _msgtype(message) -> str | None:
    """The wire code a message answers, however it spells having none."""
    held = message.get_by_name("msgtype")
    return None if held is None else held.as_py()


def _stored(handle: IOBase) -> pyarrow.Table:
    """One capture through the whole pipeline, as the table stores it."""
    codec = _codec()
    reader = handle.read_arrow_reader(options=Message.text_options())
    return fix_stored_reader(fix_arrow_reader(codec, reader), fix_message_field(codec)).read_all()


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


@pytest.fixture(scope="module")
def tracked() -> pyarrow.Table:
    """The whole bridge corpus, through the pipeline the task runs."""
    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        return _stored(handle)
    finally:
        handle.close()


def test_the_published_row_is_the_dictionarys_with_the_capture_in_front() -> None:
    """Field for field: names, types, nullability and order, all yggdryl's."""
    registry = fix_registry()
    declared = fix_schema_carrying(fix_carrier(), fix_schema(registry, FIXMSG))

    assert (
        fix_parse_field()
        .into_arrow_schema()
        .equals(declared.into_arrow_schema(), check_metadata=True)
    )
    # The capture's own columns lead, the dictionary's follow, and a capture
    # column the row already spells is folded onto it rather than repeated.
    carried = [member.name for member in declared][: len(fix_carrier()) - 5]
    assert carried == [
        "rownum",
        "timestamp",
        "timepartition",
        "threadId",
        "level",
        "bodyhash",
        "body",
    ]
    for folded in ("sourceurl", "msgsessionid", "msgctxid", "msgseqnum", "pluginid"):
        assert [member.name for member in declared].count(folded) == 1, folded
    assert len(declared) == len(fix_schema(registry, FIXMSG)) + len(carried)


def test_the_capture_has_no_layout_of_its_own_in_this_table() -> None:
    """`logs.messages` is keyed on the line and laid out by the line's hour;
    carrying its columns must not carry either statement in with them."""
    carried = fix_carrier().into_arrow_schema()

    assert primary_keys(Message.into_field()) == ["bodyhash"]
    assert partition_keys(Message.into_field()) == {"timepartition": "hour"}
    assert not [
        member.name
        for member in carried
        if b"iceberg:primary_key" in (member.metadata or {})
        or b"iceberg:partition_key" in (member.metadata or {})
    ]
    # What a column *is* survives: the digest still names the bytes it reads.
    assert fix_carrier()["bodyhash"].digest.sources == ["body"]


def test_the_table_is_keyed_partitioned_and_sorted_by_the_event() -> None:
    field = fix_message_field()

    assert primary_keys(field) == [MESSAGE_KEY] == ["curruuid"]
    assert partition_keys(field) == {EVENT_CLOCK: "hour"} == {"unix": "hour"}
    assert list(sort_keys(field)) == list(SORT_COLUMNS) == ["unix", "seqnum", "curruuid"]
    assert field[MESSAGE_KEY].nullable is False


def test_the_retired_columns_are_gone_rather_than_kept_beside_the_new_ones() -> None:
    """A fact answered twice is a fact two readers disagree about."""
    names = set(fix_message_field().into_arrow_schema().names)

    # Renamed by yggdryl, under the same tag: the old spelling is not kept.
    for retired, holds in (
        ("updatedat", "unix"),
        ("prevupdatedat", "prevunix"),
        ("createdat", "creatunix"),
        ("snapshotat", "snapunix"),
        ("expiredat", "expirunix"),
        ("msghash", "hashcode"),
        ("msgphash", "crosshashcode"),
        ("prevmsghash", "prevuuid"),
        ("altids", "identifiers"),
        ("offercurrency", "askcurrency"),
        ("bridgesessionid", "msgsessionid"),
    ):
        assert retired not in names, retired
        assert holds in names, holds
    # Retired outright: the event answers the fact under its own vocabulary.
    for retired in (
        "instuuid",
        "code",
        "version",
        "parentclordid",
        "parentorderid",
        "sendersessionid",
        "targetsessionid",
        "sendersessionname",
        "targetsessionname",
        "prevpluginid",
        "sessionmsgid",
        "sessionmsgseqid",
        "instids",
    ):
        assert retired not in names, retired
    # One fact, one column: tags 44, 38 and 53 are read and written through
    # the crate's own `px` and `qty`.
    assert not {"price", "orderqty", "quantity"} & names
    assert {"px", "qty", "prevpx", "prevqty"} <= names


def test_the_storage_boundary_narrows_what_a_row_filter_cannot_be_lowered_to() -> None:
    """Sixteen ordered bytes, microsecond instants, signed codes, no name."""
    schema = fix_message_field().into_arrow_schema()

    for name in ("curruuid", "crossuuid", "prevuuid"):
        assert schema.field(name).type == pyarrow.binary(16), name
    assert schema.field("parentuuids").type.field(0).type == pyarrow.binary(16)
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
    for code in ("hashcode", "crosshashcode", "seqnum"):
        assert schema.field(code).type == pyarrow.int64(), code
    assert len(schema) == 128
    assert schema.field(MESSAGE_KEY).metadata[b"fix:tag"] == b"65039"
    assert schema.field(MESSAGE_KEY).metadata[b"iceberg:primary_key"] == b"true"


def test_the_stored_row_holds_none_of_the_text_it_was_read_from(tracked) -> None:
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

    assert UNSTORED == (PAYLOAD, TEXT_DIGEST) == ("body", "bodyhash")
    for column in UNSTORED:
        assert column not in tracked.column_names, column
        assert column not in stored.names, column
        # The parse still reads its payload and still carries the digest -- it
        # is the stored row that keeps neither.
        assert column in parsed.names, column
    # What names the line instead, filled on every row a capture read answers.
    assert tracked.column("sourceurl").null_count == 0
    assert tracked.column("rownum").null_count == 0
    # And what the wire is rebuilt from is on the row.
    assert tracked.column("nofixentries").null_count == 0
    assert all(count > 0 for count in tracked.column("nofixentries").to_pylist())


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
        if member.name == "hashcode" or member.name == "crosshashcode":
            columns.append(pyarrow.array([code], member.type))
        elif member.name in ("curruuid", "crossuuid"):
            columns.append(
                pyarrow.ExtensionArray.from_storage(
                    member.type, pyarrow.array([b"\x01" * 16], pyarrow.binary(16))
                )
            )
        elif member.name in ("unix", "creatunix"):
            columns.append(pyarrow.array([UNDATED], member.type))
        elif member.name == "beginstring":
            columns.append(pyarrow.array(["FIX.4.4"], member.type))
        elif member.name == "body":
            columns.append(pyarrow.array([b"8=FIX.4.4|35=D|10=0|"], member.type))
        elif member.name == "rownum":
            columns.append(pyarrow.array([1], member.type))
        elif member.name == "bodyhash":
            columns.append(pyarrow.array([b"\x00" * 16], member.type))
        else:
            columns.append(pyarrow.nulls(1, member.type))
    source = pyarrow.RecordBatchReader.from_batches(
        parsed, [pyarrow.RecordBatch.from_arrays(columns, schema=parsed)]
    )

    held = fix_stored_reader(source, field).read_all()

    assert held.schema.field("hashcode").type == pyarrow.int64()
    assert held.column("hashcode").to_pylist() == [code - 2**64]
    assert field.into_arrow_schema().field("hashcode").type == pyarrow.int64()


def test_a_pin_the_codec_does_not_take_is_refused_by_name() -> None:
    """A version is what a row states, so pinning one is an error and not a
    silently forwarded keyword that parses every line under the wrong rule."""
    with pytest.raises(TypeError, match="version is no codec pin"):
        fix_codec(fix_registry(), version="4.4")


def test_a_message_stating_no_clock_answers_the_same_identity_on_every_read(tmp_path) -> None:
    """What makes a replay idempotent: nothing in the identity is read from now."""
    handle = _capture(tmp_path)
    try:
        first, second = _stored(handle), _stored(handle)
    finally:
        handle.close()

    assert first.num_rows == second.num_rows == 2
    assert first.column(MESSAGE_KEY).to_pylist() == second.column(MESSAGE_KEY).to_pylist()
    assert len(set(first.column(MESSAGE_KEY).to_pylist())) == 2
    # The line the header did not match carries no clock, and the message
    # inside it states none either, so it takes the floor the codec is pinned
    # with rather than the instant the parse ran.
    assert first.column("timestamp").to_pylist()[1] is None
    assert first.column(EVENT_CLOCK).to_pylist()[1] == UNDATED


def test_the_capture_answers_a_message_per_frame_and_not_a_row_per_line(tracked) -> None:
    """144 lines, 79 messages, 53 events."""
    assert tracked.num_rows == MESSAGES
    assert len(set(tracked.column(MESSAGE_KEY).to_pylist())) == EVENTS == 53


def test_the_walk_reads_the_chains_the_capture_describes(tracked) -> None:
    """The counts `cargo run --example fix_capture` prints, column for column."""
    assert _chains(tracked) == CHAINS


def test_every_restatement_of_an_event_settles_on_one_identity(tracked) -> None:
    """The gap between a chain's messages and its events, row by row: the same
    message logged at a second hop answers the identity the first one did, and
    the rows it was read from are different lines."""
    held: dict[bytes, set[int]] = defaultdict(set)
    for identity, rownum in zip(
        tracked.column(MESSAGE_KEY).to_pylist(),
        tracked.column("rownum").to_pylist(),
        strict=True,
    ):
        held[identity].add(rownum)
    restated = {identity: lines for identity, lines in held.items() if len(lines) > 1}

    assert restated, "the capture logs messages at several hops"
    assert len(held) == EVENTS
    # Each restatement is its own line, so the arrivals of one event span
    # several rows of `logs.messages` -- which is why the row cannot carry one
    # line's bytes or one line's digest as if they were the event's.
    assert sum(len(lines) for lines in restated.values()) > len(restated)


def test_px_and_qty_are_filled_from_what_the_message_did_state(tmp_path) -> None:
    """A report that states only what last traded still answers what it is
    about: the price is what it last traded and the quantity what it last
    filled, and the side's own lane fills from both."""
    source = tmp_path / "fills.log"
    source.write_bytes(
        b"2026-08-14 00:05:01.147 [250-e7256476:9effef3e6a:72504] [ULBridge] (INFO) "
        b"8=FIX.4.4|35=8|37=O-9|17=E-1|39=2|150=F|55=AAPL|54=1|31=10.5|32=60|10=0|\n"
    )
    handle = IOBase.from_uri(source.as_uri())
    try:
        held = (
            _stored(handle)
            .select(("px", "qty", "lastpx", "lastqty", "bidpx", "bidsize", "offerpx"))
            .to_pylist()
        )
    finally:
        handle.close()

    assert len(held) == 1
    (row,) = held
    assert float(row["px"]) == row["lastpx"] == 10.5
    assert float(row["qty"]) == row["lastqty"] == 60.0
    # A buy quotes its own side, and the lane takes the whole of it.
    assert row["bidpx"] == 10.5
    assert row["bidsize"] == 60.0
    assert row["offerpx"] is None


def test_the_capture_prices_every_message_that_stated_a_price(tracked) -> None:
    """The ladder, over real bridge traffic: the price is what the message is
    about, else what it last traded; the quantity is what it orders, else what
    it last traded. A message that stated none of them answers null rather
    than a zero that would read as a price."""
    rows = tracked.select(
        ("px", "qty", "lastpx", "lastqty", "avgpx", "bidpx", "offerpx", "side")
    ).to_pylist()

    traded = [row for row in rows if row["lastpx"]]
    assert len(traded) == 57
    assert all(float(row["px"]) == row["lastpx"] for row in traded)
    # What is done and what is left are not on the ladder: together they are
    # the quantity ordered, so `qty` takes `OrderQty` before `LastQty`.
    assert any(float(row["qty"]) != row["lastqty"] for row in rows if row["lastqty"])
    # Null means the message stated no price at all -- not zero, and not a
    # lane it never quoted.
    for row in rows:
        if row["px"] is None:
            assert not row["lastpx"] and not row["avgpx"]
            assert not row["bidpx"] and not row["offerpx"]
    # The side's lane fills from all of it.
    for row in rows:
        if row["side"] == "BUY" and row["px"] is not None:
            assert row["bidpx"] == float(row["px"])


def test_the_instrument_columns_say_what_the_capture_says(tracked) -> None:
    """`symbolticker` is what `Symbol(55)` settled on minus FIX's non-answer,
    and `tradable` is null where no message stated a trading status."""
    tickers = set(tracked.column("symbolticker").to_pylist())
    assert "[N/A]" not in tickers
    assert None in tickers, "an instrument the capture named only by code"
    assert {held for held in tickers if held} == set(
        tracked.filter(pyarrow.compute.is_valid(tracked.column("symbol")))
        .column("symbolticker")
        .to_pylist()
    ) - {None}
    # No message in this capture states SecurityTradingStatus(326),
    # TradSesStatus(340) or SecurityStatus(965), and a market that said
    # nothing is not a market that said closed.
    assert tracked.column("tradable").null_count == tracked.num_rows
    for stated in ("securitytradingstatus", "tradsesstatus", "securitystatus"):
        assert tracked.column(stated).null_count == tracked.num_rows, stated


def test_the_bridge_bracket_fills_the_columns_it_names(tracked) -> None:
    """A capture is named for the field it fills, and the bracket's first part
    is the session instance -- never what the message says about itself."""
    assert tracked.column("msgsessionid").null_count < tracked.num_rows
    assert tracked.column("msgctxid").null_count < tracked.num_rows
    assert tracked.column("msgseqnum").null_count < tracked.num_rows
    # A line the header did not match captures nothing, so its message names
    # no plugin -- which is a stated absence and not a dropped column.
    assert 0 < tracked.column("pluginid").null_count < tracked.num_rows
    assert "ULBridge" in set(tracked.column("pluginid").to_pylist())


def test_a_row_names_the_object_its_line_was_read_from(tracked) -> None:
    """The key member that used to be empty on every row: the reader names its
    source `sourceurl`, and a contract spelling it anything else stores ''."""
    held = set(tracked.column("sourceurl").to_pylist())

    assert held == {str(IOBase.from_uri(FIXTURE.as_uri()).url)}
    assert "" not in held


def test_the_pipeline_without_the_walk_names_no_chain_step(tmp_path) -> None:
    """The stage is a call, so a run that does not make it says so in the row."""
    handle = _capture(tmp_path)
    try:
        codec = _codec()
        reader = handle.read_arrow_reader(options=Message.text_options())
        held = fix_stored_reader(
            fix_arrow_reader(codec, reader, lifecycle=False), fix_message_field(codec)
        ).read_all()
    finally:
        handle.close()

    assert held.column("prevuuid").null_count == held.num_rows
    assert held.column("prevpx").null_count == held.num_rows
    assert all(step in (None, 0) for step in held.column(CHAIN_STEP).to_pylist())


def test_the_two_doors_read_the_same_capture_as_the_same_messages() -> None:
    """A door is a way in, not a second reading: the line door and the batch
    door answer the same messages, in the same order, carrying the same
    settled content -- which is what `hashcode` is a code of."""
    codec = _codec()

    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        options = fix_text_options()
        by_line = [
            (_msgtype(message), message.hashcode)
            for message in fix_line_messages(
                codec, handle.read_text_lines(options=options), lifecycle=False
            )
        ]
    finally:
        handle.close()

    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        reader = handle.read_arrow_reader(options=Message.text_options())
        rows = fix_arrow_reader(codec, reader, lifecycle=False).read_all()
    finally:
        handle.close()

    assert rows.num_rows == len(by_line) == MESSAGES
    assert (
        list(
            zip(
                rows.column("msgtype").to_pylist(), rows.column("hashcode").to_pylist(), strict=True
            )
        )
        == by_line
    )


def test_the_batch_door_also_answers_messages(tracked) -> None:
    """The message shape of the batch door, for a reader that wants events
    rather than rows: the same messages the table holds, in the same order,
    read back out of the columns the dictionary defines."""
    codec = _codec()
    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        held = list(
            fix_arrow_messages(codec, handle.read_arrow_reader(options=Message.text_options()))
        )
    finally:
        handle.close()

    assert len(held) == MESSAGES
    assert [_msgtype(message) for message in held] == tracked.column("msgtype").to_pylist()
    assert [message.crosscode for message in held] == [
        code or "" for code in tracked.column("crosscode").to_pylist()
    ]


def test_the_walk_reads_the_row_and_never_the_capture_beside_it() -> None:
    """The one thing the batch door must not do.

    A capture's own column is not content: a line number and a line clock
    differ between two logs of one message, so a walk that read them would
    give each arrival its own identity and the table would hold every hop
    rather than every event. Read the row alone and the capture's 49-message
    chain folds to the 31 events yggdryl's own walk reads; read the columns
    beside it and it folds to none of them.
    """
    codec = _codec()
    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        walked = fix_arrow_reader(
            codec, handle.read_arrow_reader(options=Message.text_options())
        ).read_all()
    finally:
        handle.close()

    assert _chains(walked)["00026877711XOEA0"] == CHAINS["00026877711XOEA0"] == (49, 31, 7)
    # And the capture's own columns are still there, beside the row.
    assert walked.column("rownum").null_count == 0
    assert walked.column("body").null_count == 0
    assert walked.column("timestamp").null_count < walked.num_rows


def test_the_walk_settles_the_same_identities_however_often_it_runs() -> None:
    """A replay is a second walk over the same bytes, and the table is keyed
    on what it answers, so the answer has to be the same one."""
    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        first, second = _stored(handle), _stored(handle)
    finally:
        handle.close()

    assert first.column(MESSAGE_KEY).to_pylist() == second.column(MESSAGE_KEY).to_pylist()
    assert first.column("hashcode").to_pylist() == second.column("hashcode").to_pylist()
    assert first.column(EVENT_CLOCK).to_pylist() == second.column(EVENT_CLOCK).to_pylist()
