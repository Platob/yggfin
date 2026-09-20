"""The raw Message contract is the native ULBridge text read."""

import datetime
import re
from pathlib import Path

import pyarrow
import pytest
from yggdryl import IOBase

from rekep import Message
from rekep.fix import fix_text_options
from rekep.times import ULBRIDGE_ROWHEADER

#: The zone every instant here is spelled in.
UTC = datetime.timezone.utc


def test_message_declares_the_text_row_and_its_storage_columns() -> None:
    field = Message.into_field()

    assert [member.name for member in field] == [
        "sourceurl",
        "rownum",
        "timestamp",
        "timepartition",
        "threadId",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "pluginid",
        "level",
        "currhashcode",
        "body",
        "curruuid",
    ]
    assert field["timestamp"].into_arrow().type.unit == "us"
    assert field["curruuid"].into_arrow().type == pyarrow.binary(16)
    # Nullable, because a table that already exists takes the column only as
    # an optional one; the read fills it on every row regardless.
    assert field["curruuid"].nullable is True
    assert field["timepartition"].partition.sources == ["timestamp"]
    assert field["timepartition"].iceberg["partition_key"] == "hour"
    # The key is the code the read states, not a digest computed beside it:
    # this contract holds the type a table stores and `read_field` widens the
    # one column the read answers unsigned.
    assert not [member.name for member in field if member.digest.is_holder()]
    assert field["currhashcode"].into_arrow().type == pyarrow.int64()
    assert Message.read_field()["currhashcode"].into_arrow().type == pyarrow.uint64()
    assert [member.name for member in Message.read_field()] == [member.name for member in field]


def test_message_text_options_own_the_complete_native_read() -> None:
    options = Message.text_options()

    assert options.start_rownum == 1
    assert options.parse_mtime is False
    assert options.rowheader == ULBRIDGE_ROWHEADER
    assert str(options.timezone) == "UTC"
    assert options.safe is False
    assert options.field == Message.read_field()
    assert options.capture_names == (
        "timestamp",
        "threadId",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "pluginid",
        "level",
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
    assert table.select(
        ("rownum", "timestamp", "threadId", "msgsessionid", "msgctxid", "msgseqnum", "pluginid")
    ).to_pylist() == [
        {
            "rownum": 1,
            "timestamp": datetime.datetime(2026, 8, 14, 0, 5, 1, 147000, tzinfo=UTC),
            "threadId": 250,
            "msgsessionid": "e7256476",
            "msgctxid": "9effef3e6a",
            "msgseqnum": 72504,
            "pluginid": "ULBridge",
        },
        {
            "rownum": 2,
            "timestamp": datetime.datetime(2026, 8, 14, 0, 5, 1, 148000, tzinfo=UTC),
            "threadId": 653,
            "msgsessionid": None,
            "msgctxid": None,
            "msgseqnum": None,
            "pluginid": "Spot_FX_TradeCapture",
        },
    ]
    assert table.column("timepartition").equals(table.column("timestamp"))
    # The body is the whole line as the native read retains it, the row
    # header included: the header's captures are read off it, not cut out of
    # it, and the code the read states is over these same bytes.
    assert table.column("body").to_pylist() == [
        b"2026-08-14 00:05:01.147 [250-e7256476:9effef3e6a:72504] "
        b"[ULBridge] (INFO) Sending : 8=FIX.4.4|35=D|10=0|",
        b"2026-08-14 00:05:01.148 [653] [Spot_FX_TradeCapture] (WARN) prose",
    ]
    codes = table.column("currhashcode")
    assert codes.type == pyarrow.uint64() and codes.null_count == 0
    assert len(set(codes.to_pylist())) == 2
    # And the line's own identity, stated by the read: sixteen bytes, one per
    # line, which is what a message parsed out of the line names as its source.
    identities = table.column("curruuid").to_pylist()
    assert all(len(value) == 16 for value in identities)
    assert len(set(identities)) == 2


def test_a_line_without_the_bridge_header_is_kept_as_an_unstamped_message(tmp_path) -> None:
    source = tmp_path / "unframed.log"
    source.write_bytes(b"one physical line\n")

    reader = IOBase.from_uri(source.as_uri()).read_arrow_reader(options=Message.text_options())
    try:
        row = reader.read_all().to_pylist()[0]
    finally:
        reader.close()

    assert row["body"] == b"one physical line"
    assert row["rownum"] == 1
    assert all(
        row[name] is None
        for name in (
            "timestamp",
            "timepartition",
            "threadId",
            "msgsessionid",
            "msgctxid",
            "msgseqnum",
        )
    )


def test_message_instance_normalizes_scalar_inputs() -> None:
    message = Message.from_text(
        b"body",
        timestamp="2026-08-14 02:05:01.147250+02:00",
        threadId="250",
        msgseqnum="72504",
    )

    assert message.timestamp == datetime.datetime(
        2026,
        8,
        14,
        0,
        5,
        1,
        147250,
        tzinfo=UTC,
    )
    assert (message.threadId, message.msgseqnum) == (250, 72504)


def test_the_raw_read_is_the_bridge_read_the_codec_is_pinned_against() -> None:
    """Two spellings of one read, because a raw row is readable without the
    dictionary behind it -- so the day they disagree is a column the codec
    stops filling, and this is what makes that day a failing test."""
    raw = Message.text_options()
    bridge = fix_text_options()

    assert raw.rowheader == bridge.rowheader
    assert raw.capture_names == bridge.capture_names
    assert (raw.start_rownum, raw.parse_mtime, raw.safe) == (
        bridge.start_rownum,
        bridge.parse_mtime,
        bridge.safe,
    )
    assert str(raw.timezone) == str(bridge.timezone)
    # The one difference, and the reason there are two: the field.
    assert raw.field == Message.read_field()
    assert bridge.field != raw.field


#: The same header with its fraction widened to what this capture's several
#: loggers actually write: a comma decimal sign, and a micro suffix after the
#: millis. The names it captures are unchanged, which is the whole rule.
WIDENED = ULBRIDGE_ROWHEADER.replace(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})",
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[.,]\d{3}(?:_\d{3})?)",
)

#: Every line of the shipped sample, and the two spellings of its clock: the
#: default header reads the plain millis, and a widened one reads the rest.
SAMPLE = Path(__file__).resolve().parents[2] / "data" / "capture"


def test_the_row_header_defaults_to_the_bridge_s_own() -> None:
    """Naming none is naming the one this package ships."""
    assert Message.text_options().rowheader == ULBRIDGE_ROWHEADER
    assert Message.text_options(None).rowheader == ULBRIDGE_ROWHEADER
    assert Message.text_options(ULBRIDGE_ROWHEADER).rowheader == ULBRIDGE_ROWHEADER


def test_a_header_of_its_own_reads_a_bridge_that_writes_the_clock_differently() -> None:
    """What the parameter is for: one capture, several loggers, and a fraction
    they do not agree on. The columns are the same columns either way."""
    handle = IOBase.from_uri(SAMPLE.as_uri())
    try:
        plain = handle.read_arrow_reader(options=Message.text_options()).read_all()
        widened = handle.read_arrow_reader(options=Message.text_options(WIDENED)).read_all()
    finally:
        handle.close()

    # A header frames every physical line either way -- what changes is how
    # many of them it could date.
    assert plain.num_rows == widened.num_rows
    assert plain.schema.equals(widened.schema, check_metadata=True)
    assert plain.num_rows - plain.column("timestamp").null_count == 2
    assert widened.num_rows - widened.column("timestamp").null_count == 10


@pytest.mark.parametrize(
    ("spelled", "refused"),
    [
        (
            ULBRIDGE_ROWHEADER.replace(r"(?P<level>[A-Z]+)", r"(?P<severity>[A-Z]+)"),
            "captures nothing for level and captures severity, which no column holds",
        ),
        (
            ULBRIDGE_ROWHEADER.replace(r" \((?P<level>[A-Z]+)\) ", r" \([A-Z]+\) "),
            "captures nothing for level",
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
        "timestamp",
        "threadId",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "pluginid",
        "level",
    }
    # A member the read fills or a field apply derives is not a capture, and
    # says so itself rather than being remembered in a list.
    assert Message.READ_COLUMNS == {
        "sourceurl",
        "rownum",
        "body",
        "curruuid",
        "currhashcode",
    }
    field = Message.into_field()
    assert {member.name for member in field} - Message.captures() == {
        *Message.READ_COLUMNS,
        "timepartition",
    }
    assert field["timepartition"].partition.sources == ["timestamp"]
