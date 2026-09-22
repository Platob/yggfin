"""The `Message` contract is the native ULBridge text read."""

import datetime
import os
import re
from pathlib import Path

import pyarrow
import pytest
from yggdryl import IOBase

from rekep import Message
from rekep.fix import fix_text_options
from rekep.iceberg import primary_keys
from rekep.text.message import decoded
from rekep.times import EPOCH, ULBRIDGE_ROWHEADER

#: The zone every instant here is spelled in.
UTC = datetime.timezone.utc


def test_message_declares_the_text_row_and_its_storage_columns() -> None:
    field = Message.into_field()

    # The native text read's own layout, in its own order: the event it
    # settles over the line -- and the object it was read from and its row
    # number are two of the event's own columns, not two beside it -- the
    # line past its header, and the bridge's captures under the names a
    # parse fills from.
    assert [member.name for member in field] == [
        "currunix",
        "curruuid",
        "currhashcode",
        "crosscode",
        "seqnum",
        "body",
        "msgthreadid",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "msgpluginid",
        "loglevel",
    ]
    assert field["currunix"].into_arrow().type.unit == "us"
    assert field["curruuid"].into_arrow().type == pyarrow.binary(16)
    # The read settles all three on every row, so all three are stated.
    assert [field[held].nullable for held in ("currunix", "curruuid", "currhashcode")] == [
        False,
        False,
        False,
    ]
    # The table is laid out by the hour of the event itself, exactly as both
    # FIX tables are: the row carries the instant, so nothing materializes a
    # second copy of it to partition by.
    assert field["currunix"].iceberg["partition_key"] == "hour"
    assert not [member.name for member in field if member.partition.sources]
    assert primary_keys(field) == ["curruuid"]
    # The content code is not a second identity. This contract holds the type
    # a table stores; `read_field` answers the read's own.
    assert not [member.name for member in field if member.digest.is_holder()]
    assert field["currhashcode"].into_arrow().type == pyarrow.int64()
    # Where a line was read from is nullable -- a buffer nothing addressed
    # names no object -- and its row number is null where zero; the table
    # holds both as the signed types Iceberg has.
    assert field["crosscode"].nullable and field["crosscode"].into_arrow().type == pyarrow.string()
    assert field["seqnum"].nullable and field["seqnum"].into_arrow().type == pyarrow.int64()


def test_the_read_field_is_the_native_row_projected_onto_the_contract() -> None:
    """`read_field` is a projection of `TextOptions.source_field()` -- the row
    the read states before a byte is read -- onto the contract's columns, at
    the read's own types. Nothing is retyped here: the storage boundary owns
    the narrowing, in one place, for every stage."""
    stated = Message.text_options().source_field().into_arrow_schema()
    read = Message.read_field().into_arrow_schema()

    assert read.names == [member.name for member in Message.into_field()]
    for member in read:
        assert member.equals(stated.field(member.name)), member.name
    assert read.field("currunix").type == pyarrow.timestamp("ns", tz="UTC")
    assert read.field("curruuid").type == pyarrow.uuid()
    assert read.field("currhashcode").type == pyarrow.uint64()
    assert read.field("seqnum").type == pyarrow.uint64()
    assert read.field("crosscode").type == pyarrow.string()
    # The read states the nineteen event columns and the line; the contract
    # keeps the five of them a text row is read by, and every capture.
    assert set(stated.names) - set(read.names) == Message.READ_COLUMNS - set(read.names)
    assert len(stated) == 20 + len(Message.captures()) - 1, "the record clock has no column"


def test_message_text_options_own_the_complete_native_read() -> None:
    options = Message.text_options()

    assert options.start_rownum == 1
    # The native default, and the whole reason the clock is captured as
    # `mtime`: on, the capture dates the line into `currunix` and lands no
    # column beside it. Off, every line would take the handle's own
    # modification time -- one instant for a day of lines -- and the capture
    # would land as a column of its own, with no error anywhere.
    assert options.parse_mtime is True
    assert "mtime" not in options.source_field().into_arrow_schema().names
    assert options.rowheader == ULBRIDGE_ROWHEADER
    assert str(options.timezone) == "UTC"
    assert options.safe is False
    assert options.field == Message.read_field()
    assert options.capture_names == (
        "mtime",
        "msgthreadid",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "msgpluginid",
        "loglevel",
    )


def test_the_text_reader_produces_messages_without_a_python_row_pass(tmp_path) -> None:
    source = tmp_path / "bridge.log"
    source.write_bytes(
        b"2026-08-14 00:05:01.147 [250-e7256476:9effef3e6a:72504] "
        b"[ULBridge] (INFO) Sending : 8=FIX.4.4|35=D|10=0|\n"
        b"2026-08-14 00:05:01.148 [653] [Spot_FX_TradeCapture] (WARN) prose\n"
    )

    reader = IOBase.from_uri(source.as_uri()).read_arrow_reader(options=Message.text_options())
    try:
        table = reader.read_all()
    finally:
        reader.close()

    assert table.schema.equals(Message.read_field().into_arrow_schema(), check_metadata=True)
    settled = ("seqnum", "currunix", "msgthreadid")
    bracket = ("msgsessionid", "msgctxid", "msgseqnum", "msgpluginid")
    assert table.select(settled + bracket).to_pylist() == [
        {
            "seqnum": 1,
            "currunix": datetime.datetime(2026, 8, 14, 0, 5, 1, 147000, tzinfo=UTC),
            "msgthreadid": 250,
            "msgsessionid": "e7256476",
            "msgctxid": "9effef3e6a",
            "msgseqnum": 72504,
            "msgpluginid": "ULBridge",
        },
        {
            "seqnum": 2,
            "currunix": datetime.datetime(2026, 8, 14, 0, 5, 1, 148000, tzinfo=UTC),
            "msgthreadid": 653,
            "msgsessionid": None,
            "msgctxid": None,
            "msgseqnum": None,
            "msgpluginid": "Spot_FX_TradeCapture",
        },
    ]
    # The body is the line past its row header: the captures are what the
    # header stated, and the body is what the bridge printed after it.
    assert table.column("body").to_pylist() == [
        "Sending : 8=FIX.4.4|35=D|10=0|",
        "prose",
    ]
    # The object each line was read from, as the identifier the read was
    # addressed under: one file, so one value on both rows.
    assert len(set(table.column("crosscode").to_pylist())) == 1
    assert table.column("crosscode").to_pylist()[0].endswith("bridge.log")
    codes = table.column("currhashcode")
    assert codes.type == pyarrow.uint64() and codes.null_count == 0
    assert len(set(codes.to_pylist())) == 2
    # And the line's own identity, stated by the read as the `uuid` it is --
    # the table keeps its sixteen bytes -- one per line, which is what a
    # message parsed out of the line names as its source.
    identities = table.column("curruuid").to_pylist()
    assert table.schema.field("curruuid").type == pyarrow.uuid()
    assert all(len(value.bytes) == 16 for value in identities)
    assert len(set(identities)) == 2


#: An instant a capture object may have been written at, as `os.utime` takes
#: it: the modification time a line the header did not match is dated by.
WRITTEN_AT = datetime.datetime(2026, 8, 14, 18, 0, tzinfo=UTC)


def test_a_line_without_the_bridge_header_is_dated_by_the_object_it_was_read_from(
    tmp_path,
) -> None:
    source = tmp_path / "unframed.log"
    source.write_bytes(b"one physical line\n")
    os.utime(source, (WRITTEN_AT.timestamp(), WRITTEN_AT.timestamp()))

    reader = IOBase.from_uri(source.as_uri()).read_arrow_reader(options=Message.text_options())
    try:
        row = reader.read_all().to_pylist()[0]
    finally:
        reader.close()

    assert row["body"] == "one physical line"
    assert row["seqnum"] == 1
    assert all(row[capture] is None for capture in Message.captures() - {Message.RECORD_CLOCK})
    # The line is still an event, dated by the one clock the read has for a
    # line whose header stated none: the object's own modification time --
    # the same instant on every re-read of the same object, and the epoch
    # pin only where a handle has none. That instant is what its identity
    # is derived from, so a copy of the capture written at another time
    # states another identity for every line the header did not match.
    assert row["currunix"] == WRITTEN_AT
    identity = row["curruuid"]
    assert identity.bytes != bytes(16)
    assert identity.version == 7
    assert all(
        row[name] is None
        for name in (
            "msgthreadid",
            "msgsessionid",
            "msgctxid",
            "msgseqnum",
            "msgpluginid",
            "loglevel",
        )
    )


def test_two_lines_of_one_text_are_two_rows_of_one_table(tmp_path) -> None:
    """`logs.messages` is keyed on `curruuid` alone, so a file that prints the
    same bytes twice has to answer two identities or one of the two lines is
    gone. The code is the line's and not its bytes': it digests the row
    number and the object beside the body, so the two differ, and so do the
    identities derived from them."""
    source = tmp_path / "twice.log"
    source.write_bytes(b"one physical line\none physical line\n")
    os.utime(source, (WRITTEN_AT.timestamp(), WRITTEN_AT.timestamp()))

    def read() -> list[dict]:
        reader = IOBase.from_uri(source.as_uri()).read_arrow_reader(options=Message.text_options())
        try:
            return reader.read_all().to_pylist()
        finally:
            reader.close()

    first, second = read()

    assert first["body"] == second["body"]
    assert first["crosscode"] == second["crosscode"]
    assert (first["seqnum"], second["seqnum"]) == (1, 2)
    assert first["currhashcode"] != second["currhashcode"]
    assert first["curruuid"] != second["curruuid"]
    # A replay of the same bytes answers the same two identities, because the
    # read dates each line by its object's modification time and its place,
    # never by a clock of the run.
    assert [row["curruuid"] for row in read()] == [first["curruuid"], second["curruuid"]]


def test_message_instance_normalizes_scalar_inputs() -> None:
    message = Message.from_text(
        b"body",
        currunix="2026-08-14 02:05:01.147250+02:00",
        msgthreadid="250",
        msgseqnum="72504",
    )

    assert message.currunix == datetime.datetime(
        2026,
        8,
        14,
        0,
        5,
        1,
        147250,
        tzinfo=UTC,
    )
    assert (message.msgthreadid, message.msgseqnum) == (250, 72504)
    assert message.body == "body"


#: What a bridge writes that is not UTF-8, and what the read makes of it: a
#: record in one encoding, a record in the other, the bytes windows-1252
#: fills the C1 range with, and the five it leaves undefined.
ENCODED = (
    "caf\u00e9 \u20ac".encode(),
    "caf\u00e9".encode("latin-1"),
    b"\x80\x93\x92",
    b"\x81\x8d\x8f\x90\x9d",
    b"\xff\xfe",
    # One record in both encodings at once, which is what a relayed line is:
    # the fallback is per run of bytes and not per record.
    b"caf\xc3\xa9 caf\xe9",
)


@pytest.mark.parametrize("payload", ENCODED, ids=[repr(held) for held in ENCODED])
def test_a_body_built_by_hand_is_the_text_the_read_would_have_answered(payload, tmp_path) -> None:
    """A capture is not written in one encoding, and the read decodes each
    record in the one it is in rather than refusing it or filling it with
    replacement characters. A row built out of the same bytes says the same."""
    line = b"2026-08-14 00:05:01.147 [77-e7256476:9effef3e6a:72503] [P] (INFO) " + payload
    source = tmp_path / "encoded.log"
    source.write_bytes(line + b"\n")

    reader = IOBase.from_uri(source.as_uri()).read_arrow_reader(options=Message.text_options())
    try:
        stored = reader.read_all().column("body")[0].as_py()
    finally:
        reader.close()

    # A row built by hand states the body the read would have answered: the
    # text past the header, so the payload is handed in alone.
    assert Message.from_text(payload).body == stored
    assert decoded(payload) == stored
    # Never the replacement character: the bytes are read, not papered over.
    assert "\ufffd" not in stored


def test_a_message_made_by_hand_states_the_event_that_names_no_line() -> None:
    """Only the native read states an instant, a code and an identity: a row
    built anywhere else says so, at the pin, the zero code and the sixteen
    zero bytes -- rather than inventing one of the three."""
    message = Message.from_text("body")

    assert (message.currunix, message.currhashcode, message.curruuid) == (EPOCH, 0, bytes(16))


def test_the_text_read_is_the_bridge_read_the_codec_is_pinned_against() -> None:
    """Two spellings of one read, because a text row is readable without the
    dictionary behind it -- so the day they disagree is a column the codec
    stops filling, and this is what makes that day a failing test."""
    text = Message.text_options()
    bridge = fix_text_options()

    assert text.rowheader == bridge.rowheader
    assert text.capture_names == bridge.capture_names
    assert (text.start_rownum, text.parse_mtime, text.safe) == (
        bridge.start_rownum,
        bridge.parse_mtime,
        bridge.safe,
    )
    assert text.parse_mtime is True, "both date a line from its own header"
    assert str(text.timezone) == str(bridge.timezone)
    # The one difference, and the reason there are two: the field.
    assert text.field == Message.read_field()
    assert bridge.field != text.field


#: The same header with its fraction widened past what the shipped one reads:
#: six digits straight on, which this bridge never writes and which the
#: shipped header therefore leaves to a header of its own. The names it
#: captures are unchanged, which is the whole rule.
WIDENED = ULBRIDGE_ROWHEADER.replace(
    r"(?:[.,]\d{3}(?:_\d{3})?)?",
    r"(?:[.,]\d{3}(?:_?\d{3})?)?",
)

#: Every line of the shipped sample: fourteen lines, ten of them under the
#: bridge's bracket, spelling their clock `.147`, `,148` and `.147_250` in one
#: file -- every one of which the shipped header reads.
SAMPLE = Path(__file__).resolve().parents[2] / "data" / "capture"


def test_the_row_header_defaults_to_the_bridge_s_own() -> None:
    """Naming none is naming the one this package ships."""
    assert Message.text_options().rowheader == ULBRIDGE_ROWHEADER
    assert Message.text_options(None).rowheader == ULBRIDGE_ROWHEADER
    assert Message.text_options(ULBRIDGE_ROWHEADER).rowheader == ULBRIDGE_ROWHEADER


def test_the_shipped_header_dates_every_fraction_this_bridge_writes() -> None:
    """One capture, several loggers, three spellings of one clock -- and one
    header that reads them all, because a line the header misses is a line
    the walk cannot fold, not merely a line without a clock."""
    handle = IOBase.from_uri(SAMPLE.as_uri())
    try:
        sample = handle.read_arrow_reader(options=Message.text_options()).read_all()
    finally:
        handle.close()

    assert sample.num_rows == 14
    dated = sample.filter(pyarrow.compute.is_valid(sample.column("msgpluginid")))
    assert dated.num_rows == 10, "four lines are under no bracket at all"
    spelled = {instant.microsecond for instant in dated.column("currunix").to_pylist()}
    assert {147_000, 148_000, 147_250} <= spelled, "millis, a comma, and grouped micros"
    # The four lines the bracket does not frame take the sample file's own
    # modification time -- one instant, at whatever precision the filesystem
    # keeps -- which is not one of the clocks the bridge wrote.
    unframed = sample.filter(pyarrow.compute.is_null(sample.column("msgpluginid")))
    assert unframed.num_rows == 4
    written = set(unframed.column("currunix").cast(pyarrow.int64()).to_pylist())
    assert len(written) == 1
    assert written.isdisjoint(dated.column("currunix").cast(pyarrow.int64()).to_pylist())


def test_a_header_of_its_own_reads_a_bridge_that_writes_the_clock_differently(tmp_path) -> None:
    """What the parameter is for: a bridge writing a fraction this one never
    does -- six digits straight on -- is read by naming its own header, and
    the columns are the same columns either way. The width types nothing: the
    `mtime` capture is consumed into `currunix` at the read's own precision,
    whatever the expression admits."""
    source = tmp_path / "micros.log"
    source.write_bytes(
        b"2026-08-14 00:05:01.147250 [250-e7256476:9effef3e6a:72504] [ULBridge] (INFO) one\n"
        b"2026-08-14 00:05:01.147_250 [77] [ULBridge] (INFO) two\n"
        b"2026-08-14 00:05:01.147 [77] [ULBridge] (INFO) three\n"
    )
    handle = IOBase.from_uri(source.as_uri())
    try:
        plain = handle.read_arrow_reader(options=Message.text_options()).read_all()
        widened = handle.read_arrow_reader(options=Message.text_options(WIDENED)).read_all()
    finally:
        handle.close()

    # A header frames every physical line either way -- what changes is how
    # many of them it could date -- and answers the same schema either way.
    assert plain.num_rows == widened.num_rows == 3
    assert plain.schema.equals(widened.schema, check_metadata=True)
    assert plain.column("msgpluginid").null_count == 1, "the first line is under no bracket"
    assert widened.column("msgpluginid").null_count == 0
    assert widened.column("currunix").to_pylist()[0] == datetime.datetime(
        2026, 8, 14, 0, 5, 1, 147250, tzinfo=UTC
    )
    # The two agree on every line both could date, the clock read to the
    # microsecond the bridge wrote.
    assert plain.slice(1).equals(widened.slice(1))


@pytest.mark.parametrize(
    ("spelled", "refused"),
    [
        (
            ULBRIDGE_ROWHEADER.replace(r"(?P<loglevel>[A-Z]+)", r"(?P<severity>[A-Z]+)"),
            "captures nothing for loglevel and captures severity, "
            "which this read fills nothing from",
        ),
        (
            ULBRIDGE_ROWHEADER.replace(r" \((?P<loglevel>[A-Z]+)\) ", r" \([A-Z]+\) "),
            "captures nothing for loglevel",
        ),
    ],
    ids=["renamed", "dropped"],
)
def test_a_header_that_renames_a_column_is_refused_rather_than_stored_as_nulls(
    spelled: str, refused: str
) -> None:
    """The failure this check exists for: the read drops a capture no column
    holds without a word, so the table lands complete, keyed, and empty down
    one column. The names have to be checked where the mismatch is legible."""
    with pytest.raises(ValueError, match=re.escape(refused)):
        Message.text_options(spelled)


def test_the_columns_a_header_is_expected_to_fill_are_the_contract_s_own() -> None:
    """Stated by the contract rather than beside it, so a column added to one
    is a column the other expects."""
    assert Message.captures() == frozenset(Message.text_options().capture_names)
    # Spelled out once here, so a column that stops being captured is a
    # failing test rather than a silently empty one.
    assert Message.captures() == {
        "mtime",
        "msgthreadid",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "msgpluginid",
        "loglevel",
    }
    # A member the read settles or a field apply derives is not a capture,
    # and says so itself rather than being remembered in a list: the columns
    # a bare read states with no header at all are read off the native
    # options.
    assert (
        Message.READ_COLUMNS
        == {member.name for member in Message.text_options().source_field()} - Message.captures()
    )
    assert {"crosscode", "seqnum", "body", "currunix", "curruuid", "currhashcode"} <= (
        Message.READ_COLUMNS
    )
    field = Message.into_field()
    # The record clock is the one capture no column holds: what it fills is
    # the settled `currunix`, and a second copy of the reading is not kept.
    assert Message.RECORD_CLOCK == "mtime"
    assert {member.name for member in field} - Message.captures() <= Message.READ_COLUMNS
    assert Message.captures() - {member.name for member in field} == {Message.RECORD_CLOCK}
