"""The FIX seam: what dates a message, and what the storage boundary narrows."""

from __future__ import annotations

import datetime
from pathlib import Path

import pyarrow
from yggdryl import IOBase

from rekep.fix import (
    SENDING_TIME,
    UNDATED,
    dated_arrow_reader,
    fix_codec,
    fix_message_field,
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


def _parsed(handle: IOBase) -> pyarrow.Table:
    reader = handle.read_arrow_reader(options=Message.text_options())
    return fix_codec().parse_text_arrow_reader(dated_arrow_reader(reader)).read_all()


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
    assert [str(held) for held in first.column("uuid").to_pylist()] == [
        str(held) for held in second.column("uuid").to_pylist()
    ]
    assert len({str(held) for held in first.column("uuid").to_pylist()}) == 2


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


def test_the_storage_boundary_narrows_an_identity_arrow_cannot_compare() -> None:
    """A UUID reaches Arrow as an extension type with no kernels; storage has them."""
    field = fix_message_field()
    schema = field.into_arrow_schema()

    for name in ("uuid", "puuid", "instuuid", "prevuuid"):
        assert schema.field(name).type == pyarrow.binary(16), name
    assert not [
        member.name for member in schema if isinstance(member.type, pyarrow.BaseExtensionType)
    ]
    # Narrowing keeps everything the contract is read from.
    assert len(schema) == 118
    assert schema.field("uuid").metadata[b"fix:tag"] == b"65017"
    assert schema.field("uuid").metadata[b"iceberg:primary_key"] == b"true"
    assert schema.field("uuid").nullable is False


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


def test_the_tracked_capture_answers_one_identity_per_message() -> None:
    """The whole bridge corpus, where one line answers two configurations."""
    handle = IOBase.from_uri(FIXTURE.as_uri())
    try:
        held = _parsed(handle)
    finally:
        handle.close()

    keys = list(
        zip(
            held.column("url").to_pylist(),
            held.column("rownum").to_pylist(),
            [str(value) for value in held.column("uuid").to_pylist()],
            strict=True,
        )
    )
    assert held.num_rows == 71
    assert len(set(keys)) == 71, "every message names itself"
    assert len({(url, rownum) for url, rownum, _ in keys}) == 70, "one line answered two"
