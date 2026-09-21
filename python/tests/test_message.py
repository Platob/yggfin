"""The raw Message contract is the native ULBridge text read."""

import datetime
import re
from pathlib import Path

import pyarrow
import pytest
from yggdryl import IOBase

from rekep import Message
from rekep.fix import fix_text_options
from rekep.text.message import decoded
from rekep.times import EPOCH, ULBRIDGE_ROWHEADER

#: The zone every instant here is spelled in.
UTC = datetime.timezone.utc

#: What a UUIDv7 carries below its instant and its variant bits: the sixty-two
#: a random one fills, and the ones the read fills with the content code.
RANDOM = (1 << 62) - 1


def test_message_declares_the_text_row_and_its_storage_columns() -> None:
    field = Message.into_field()

    # The native text read's own layout, in its own order: the event it
    # settles over the line, the two columns its traversal names, the line,
    # and the bridge's captures under the names a parse fills from.
    assert [member.name for member in field] == [
        "currunix",
        "curruuid",
        "currhashcode",
        "sourceurl",
        "rownum",
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
    settled = ("rownum", "currunix", "msgthreadid")
    bracket = ("msgsessionid", "msgctxid", "msgseqnum", "msgpluginid")
    assert table.select(settled + bracket).to_pylist() == [
        {
            "rownum": 1,
            "currunix": datetime.datetime(2026, 8, 14, 0, 5, 1, 147000, tzinfo=UTC),
            "msgthreadid": 250,
            "msgsessionid": "e7256476",
            "msgctxid": "9effef3e6a",
            "msgseqnum": 72504,
            "msgpluginid": "ULBridge",
        },
        {
            "rownum": 2,
            "currunix": datetime.datetime(2026, 8, 14, 0, 5, 1, 148000, tzinfo=UTC),
            "msgthreadid": 653,
            "msgsessionid": None,
            "msgctxid": None,
            "msgseqnum": None,
            "msgpluginid": "Spot_FX_TradeCapture",
        },
    ]
    # The body is the whole line as the native read retains it, the row
    # header included: the header's captures are read off it, not cut out of
    # it, and the code the read states is over these same bytes.
    assert table.column("body").to_pylist() == [
        "2026-08-14 00:05:01.147 [250-e7256476:9effef3e6a:72504] "
        "[ULBridge] (INFO) Sending : 8=FIX.4.4|35=D|10=0|",
        "2026-08-14 00:05:01.148 [653] [Spot_FX_TradeCapture] (WARN) prose",
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

    assert row["body"] == "one physical line"
    assert row["rownum"] == 1
    # The line is still an event: the read settles it at the epoch pin, which
    # is the same instant on every re-read of these bytes and the same one an
    # undated message takes in `fix.bronze`, and states an identity over it.
    assert row["currunix"] == EPOCH
    # The pin's own identity, stated over the line: a UUIDv7 whose instant is
    # the pin and whose rest is that line's own code -- so an undated line is
    # still told from every other one, and not the zero identity a row built
    # anywhere but the read would carry.
    identity = row["curruuid"]
    assert identity != bytes(16) and identity.startswith(bytes(6))
    assert identity[6] >> 4 == 7, "a UUIDv7, at the pin"
    assert int.from_bytes(identity[8:], "big") & RANDOM == row["currhashcode"] & RANDOM
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

    assert Message.from_text(line).body == stored
    assert decoded(payload) == stored[-len(decoded(payload)) :]
    # Never the replacement character: the bytes are read, not papered over.
    assert "\ufffd" not in stored


def test_a_message_made_by_hand_states_the_event_that_names_no_line() -> None:
    """Only the native read states an instant, a code and an identity: a row
    built anywhere else says so, at the pin, the zero code and the sixteen
    zero bytes -- rather than inventing one of the three."""
    message = Message.from_text("body")

    assert (message.currunix, message.currhashcode, message.curruuid) == (EPOCH, 0, bytes(16))


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
    r"(?P<mtime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})",
    r"(?P<mtime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[.,]\d{3}(?:_\d{3})?)",
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
    # What the widened header could date, the plain one settled at the pin.
    undated = [held.column("currunix").to_pylist().count(EPOCH) for held in (plain, widened)]
    assert [plain.num_rows - held for held in undated] == [2, 10]


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
    # and says so itself rather than being remembered in a list.
    assert Message.READ_COLUMNS == {
        "sourceurl",
        "rownum",
        "body",
        "currunix",
        "curruuid",
        "currhashcode",
    }
    field = Message.into_field()
    # The record clock is the one capture no column holds: what it fills is
    # the settled `currunix`, and a second copy of the reading is not kept.
    assert Message.RECORD_CLOCK == "mtime"
    assert {member.name for member in field} - Message.captures() == Message.READ_COLUMNS
    assert Message.captures() - {member.name for member in field} == {Message.RECORD_CLOCK}
