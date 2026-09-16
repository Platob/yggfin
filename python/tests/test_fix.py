"""The FIX seam: the three stages, what dates a message, and what storage narrows."""

from __future__ import annotations

import datetime
from pathlib import Path

import pyarrow
import pytest
from yggdryl import IOBase

from rekep.fix import (
    MESSAGE_KEY,
    SENDING_TIME,
    UNDATED,
    dated_arrow_reader,
    fix_arrow_messages,
    fix_arrow_reader,
    fix_codec,
    fix_line_messages,
    fix_message_field,
    fix_registry,
    fix_text_options,
    iceberg_fix_field,
)
from rekep.text import Message

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "python" / "tests" / "data" / "ulbridge.log"

#: One line the bridge header matches, and one it does not: the second spells
#: its fraction `,148`, which the row header does not read, so the row carries
#: no clock at all and the message inside it states none either.
LINES = (
    b"2026-08-14 00:05:01.147 [250-e7256476:9effef3e6a:72504] [ULBridge] (INFO) "
    b"Sending : 8=FIX.4.4|35=D|11=A1|55=AAPL|10=0|\n"
    b"2026-08-14 00:05:01,148 [77] [FixSession_XPAR] (INFO) "
    b"sending >> 8=FIX.4.2|35=D|11=A2|55=TTF|10=0|\n"
)


def _capture(tmp_path: Path) -> IOBase:
    source = tmp_path / "bridge.log"
    source.write_bytes(LINES)
    return IOBase.from_uri(source.as_uri())


def _codec():
    return fix_codec(fix_registry(), options=fix_text_options())


def _msgtype(message) -> str | None:
    """The wire code a message answers, however it spells having none."""
    held = message.get_by_name("msgtype")
    return None if held is None else held.as_py()


def _parsed(handle: IOBase) -> pyarrow.Table:
    reader = handle.read_arrow_reader(options=Message.text_options())
    return fix_arrow_reader(_codec(), reader).read_all()


@pytest.fixture(scope="module")
def tracked() -> pyarrow.Table:
    """The whole bridge corpus, through the pipeline the task runs."""
    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        return _parsed(handle)
    finally:
        handle.close()


def test_every_row_carries_a_sending_time_even_where_no_clock_was_read(tmp_path) -> None:
    """A capture clock that is null still has to date its message."""
    handle = _capture(tmp_path)
    try:
        reader = handle.read_arrow_reader(options=Message.text_options())
        dated = dated_arrow_reader(reader)
        held = dated.read_all()
    finally:
        handle.close()

    assert SENDING_TIME in held.schema.names
    assert held.schema.field(SENDING_TIME).nullable is False
    assert held.column(SENDING_TIME).null_count == 0
    # The matched line keeps its own capture instant; the unmatched one states
    # the instant that means none was read.
    clocks = held.column(SENDING_TIME).to_pylist()
    assert clocks[0] == datetime.datetime(2026, 8, 14, 0, 5, 1, 147000, tzinfo=datetime.UTC)
    assert clocks[1] == UNDATED


def test_a_message_stating_no_clock_answers_the_same_identity_on_every_read(tmp_path) -> None:
    """What makes a replay idempotent: nothing in the identity is read from now."""
    handle = _capture(tmp_path)
    try:
        first, second = _parsed(handle), _parsed(handle)
    finally:
        handle.close()

    assert first.num_rows == second.num_rows == 2
    assert first.column(MESSAGE_KEY).to_pylist() == second.column(MESSAGE_KEY).to_pylist()
    assert len(set(first.column(MESSAGE_KEY).to_pylist())) == 2


def test_a_reader_already_carrying_a_sending_time_is_left_alone(tmp_path) -> None:
    """The column is the caller's statement, and this one does not overwrite it."""
    schema = pyarrow.schema(
        [
            pyarrow.field("timestamp", pyarrow.timestamp("us", tz="UTC")),
            pyarrow.field(SENDING_TIME, pyarrow.timestamp("ns", tz="UTC")),
            pyarrow.field("body", pyarrow.string(), nullable=False),
        ]
    )
    source = pyarrow.RecordBatchReader.from_batches(schema, [])

    assert dated_arrow_reader(source) is source


def test_a_pin_the_codec_does_not_take_is_refused_by_name() -> None:
    """A version is what a row states, so pinning one is an error and not a
    silently forwarded keyword that parses every line under the wrong rule."""
    with pytest.raises(TypeError, match="version is no codec pin"):
        fix_codec(fix_registry(), version="4.4")


def test_the_storage_boundary_narrows_what_a_row_filter_cannot_be_lowered_to() -> None:
    """Sixteen ordered bytes, microsecond instants, and no semantic name."""
    field = fix_message_field()
    schema = field.into_arrow_schema()

    for name in ("msghash", "msgphash", "instuuid", "prevmsghash"):
        assert schema.field(name).type == pyarrow.binary(16), name
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
    # Narrowing keeps everything the contract is read from.
    assert len(schema) == 128
    assert schema.field(MESSAGE_KEY).metadata[b"fix:tag"] == b"65017"
    assert schema.field(MESSAGE_KEY).metadata[b"iceberg:primary_key"] == b"true"
    assert schema.field(MESSAGE_KEY).nullable is False


def test_the_carrier_keeps_its_key_where_a_fix_column_absorbed_it() -> None:
    """`sourceurl` is the carrier's own key member and the crate's own column
    at once; the fold onto one spelling must not drop the marking."""
    schema = fix_message_field().into_arrow_schema()

    keys = [
        member.name
        for member in schema
        if (member.metadata or {}).get(b"iceberg:primary_key") == b"true"
    ]
    assert keys == ["rownum", MESSAGE_KEY, "sourceurl"]
    assert schema.field("sourceurl").nullable is False


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


def test_the_tracked_capture_answers_one_identity_per_message(tracked) -> None:
    """The whole bridge corpus, where one line answers two configurations."""
    keys = list(
        zip(
            tracked.column("sourceurl").to_pylist(),
            tracked.column("rownum").to_pylist(),
            tracked.column(MESSAGE_KEY).to_pylist(),
            strict=True,
        )
    )
    assert tracked.num_rows == 71
    assert len(set(keys)) == 71, "every message names itself"
    assert len({(url, rownum) for url, rownum, _ in keys}) == 70, "one line answered two"


def test_a_row_names_the_object_its_line_was_read_from(tracked) -> None:
    """The key member that used to be empty on every row: the reader names its
    source `sourceurl`, and a contract spelling it anything else stores ''."""
    held = set(tracked.column("sourceurl").to_pylist())

    assert held == {str(IOBase.from_uri(FIXTURE.as_uri()).url)}
    assert "" not in held


def test_the_bridge_bracket_fills_the_columns_it_names(tracked) -> None:
    """A capture is named for the field it fills, and the bracket's first part
    is the session instance -- never what the message says about itself."""
    assert tracked.column("bridgesessionid").null_count < tracked.num_rows
    assert tracked.column("msgctxid").null_count < tracked.num_rows
    assert tracked.column("pluginid").null_count == 0
    # The header carries the sequence number for a line whose frame did not,
    # so only a line whose bracket held no message context is left without
    # one -- against a spelling that reached no column at all before.
    assert tracked.column("msgseqnum").null_count == 3
    # 65007 is what a bridge row spells for the counterparty session, so the
    # bracket must never have landed there.
    assert (
        tracked.column("sendersessionid").null_count > tracked.column("bridgesessionid").null_count
    )


def test_the_lifecycle_names_a_chain_and_links_it(tracked) -> None:
    """The third stage: every message belongs to a chain, its instant sits on
    the chain's grid, and a message after the first names the one before it."""
    codes = [held for held in tracked.column("code").to_pylist() if held]
    # A message naming an identifier joins a chain; one naming none is still
    # a row, and states the empty unknown name rather than a made-up chain.
    assert 1 < len(set(codes)) < tracked.num_rows
    assert len(codes) > tracked.num_rows // 2

    # The grid is one second, so a settled instant carries no sub-second part
    # while the real event instant stays on `createdat`.
    assert all(held.microsecond == 0 for held in tracked.column("updatedat").to_pylist())
    assert any(held.microsecond for held in tracked.column("createdat").to_pylist())
    assert tracked.column("prevmsghash").null_count < tracked.num_rows


def test_the_pipeline_without_the_lifecycle_settles_nothing(tmp_path) -> None:
    """The stage is a call, so a run that does not make it says so in the row."""
    handle = _capture(tmp_path)
    try:
        reader = handle.read_arrow_reader(options=Message.text_options())
        held = fix_arrow_reader(_codec(), reader, lifecycle=False).read_all()
    finally:
        handle.close()

    assert held.column("code").to_pylist() == ["", ""]
    assert held.column("prevmsghash").null_count == held.num_rows


def test_the_two_doors_read_the_same_capture_as_the_same_messages() -> None:
    """A door is a way in, not a second reading: the line door and the batch
    door answer the same messages carrying the same arrival record."""
    options = fix_text_options()

    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        by_line = [
            (_msgtype(message), len(message.entries()))
            for message in fix_line_messages(_codec(), handle.read_text_lines(options=options))
        ]
    finally:
        handle.close()

    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        reader = handle.read_arrow_reader(options=Message.text_options())
        by_batch = [
            (_msgtype(message), len(message.entries()))
            for message in fix_arrow_messages(_codec(), reader)
        ]
    finally:
        handle.close()

    assert len(by_line) == len(by_batch) == 71
    assert by_line == by_batch
