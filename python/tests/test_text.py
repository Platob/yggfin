"""`rekep.text`: the bridge read and the `log_messages` row it lands."""

import datetime
import os
import re
from pathlib import Path

import pyarrow
import pytest
from yggdryl import IOBase

from rekep import State
from rekep.fields import stored_arrow_reader
from rekep.iceberg import partition_keys, primary_keys, sort_keys
from rekep.text import (
    CAPTURES,
    LOG_MESSAGES_NAME,
    RECORD_CLOCK,
    log_message_field,
    text_options,
)
from rekep.times import ULBRIDGE_ROWHEADER

#: The zone every instant here is spelled in.
UTC = datetime.timezone.utc

#: The sixteen columns the native read states over every line, in its order:
#: the line as the event it is, `currunix` first and `state` last.
EVENT_COLUMNS = (
    "currunix",
    "creaunix",
    "execunix",
    "recdunix",
    "exprunix",
    "prevunix",
    "snapunix",
    "curruuid",
    "crossuuid",
    "crosscode",
    "currhashcode",
    "crosshashcode",
    "prevuuid",
    "seqnum",
    "srcuuids",
    "state",
)

#: The header's captures in the order it declares them, the record clock
#: first: the one capture no column holds.
CAPTURE_ORDER = (
    "mtime",
    "msgthreadid",
    "msgsessionid",
    "msgctxid",
    "msgseqnum",
    "msgpluginid",
    "loglevel",
)

#: The shipped capture: 144 lines under the bridge's bracket, fifteen of them
#: spelling the grouped micros after their millis, as `.524_315`.
SAMPLE = Path(__file__).resolve().parents[2] / "data" / "capture" / "ulbridge.log"

#: An instant a capture object may have been written at, as `os.utime` takes
#: it: the modification time a line the header did not match is dated by.
WRITTEN_AT = datetime.datetime(2026, 8, 14, 18, 0, tzinfo=UTC)

#: The same header with its fraction widened past what the shipped one reads:
#: six digits straight on, which this bridge never writes. The names it
#: captures are unchanged, which is the whole rule.
WIDENED = ULBRIDGE_ROWHEADER.replace(
    r"(?:[.,]\d{3}(?:_\d{3})?)?",
    r"(?:[.,]\d{3}(?:_?\d{3})?)?",
)


def _read(source: Path, options=None) -> pyarrow.Table:
    """Every line of `source` as the native read answers it."""
    handle = IOBase.from_uri(source.as_uri())
    try:
        return handle.read_arrow_reader(options=options or text_options()).read_all()
    finally:
        handle.close()


# -- the captures ---------------------------------------------------------------


def test_the_captures_are_the_shipped_headers_own() -> None:
    """Spelled out once here, so a capture that stops being declared is a
    failing test rather than a silently empty column."""
    assert RECORD_CLOCK == "mtime"
    assert CAPTURES == frozenset(CAPTURE_ORDER)
    assert text_options().capture_names == CAPTURE_ORDER


# -- the read -------------------------------------------------------------------


def test_the_options_own_the_complete_native_read() -> None:
    options = text_options()

    assert options.start_rownum == 1
    # The native default, and the whole reason the clock is captured as
    # `mtime`: on, the capture dates the line into `currunix` and lands no
    # column beside it. Off, every line would take the handle's own
    # modification time -- one instant for a day of lines -- and the capture
    # would land as a column of its own, with no error anywhere.
    assert options.parse_mtime is True
    assert RECORD_CLOCK not in options.source_field().into_arrow_schema().names
    assert options.rowheader == ULBRIDGE_ROWHEADER
    assert str(options.timezone) == "UTC"
    assert options.safe is False
    # No field is declared: the read states its own, which is the table's.
    assert options.field is None


def test_the_record_clock_is_read_in_the_zone_the_bridge_prints() -> None:
    """A bridge printing local time is read in its zone; UTC otherwise."""
    assert str(text_options().timezone) == "UTC"
    assert str(text_options(timezone="Europe/Zurich").timezone) == "Europe/Zurich"
    assert text_options(WIDENED, "Europe/Zurich").rowheader == WIDENED


def test_the_row_header_defaults_to_the_bridge_s_own() -> None:
    """Naming none is naming the one this package ships."""
    assert text_options().rowheader == ULBRIDGE_ROWHEADER
    assert text_options(None).rowheader == ULBRIDGE_ROWHEADER
    assert text_options(ULBRIDGE_ROWHEADER).rowheader == ULBRIDGE_ROWHEADER
    assert text_options(WIDENED).rowheader == WIDENED


@pytest.mark.parametrize(
    ("spelled", "refused"),
    [
        (
            ULBRIDGE_ROWHEADER.replace(r"(?P<loglevel>[A-Z]+)", r"(?P<severity>[A-Z]+)"),
            "row header captures nothing for loglevel and captures severity, "
            "which the read fills nothing from",
        ),
        (
            ULBRIDGE_ROWHEADER.replace(r" \((?P<loglevel>[A-Z]+)\) ", r" \([A-Z]+\) "),
            "row header captures nothing for loglevel",
        ),
        (
            ULBRIDGE_ROWHEADER + r"(?P<venue>[A-Z]{4})?",
            "row header captures venue, which the read fills nothing from",
        ),
        (
            ULBRIDGE_ROWHEADER.replace("(?P<mtime>", "(?:"),
            "row header captures nothing for mtime",
        ),
    ],
    ids=["renamed", "dropped", "added", "undated"],
)
def test_a_header_that_renames_a_column_is_refused_rather_than_stored_as_nulls(
    spelled: str, refused: str
) -> None:
    """The failure this check exists for: the read drops a capture no column
    holds without a word, so the table lands complete, keyed, and empty down
    one column. The names are checked where the mismatch is still legible,
    and the field is refused by the same rule, since it is the read's own."""
    with pytest.raises(ValueError, match=f"^{re.escape(refused)}$"):
        text_options(spelled)
    with pytest.raises(ValueError, match=f"^{re.escape(refused)}$"):
        log_message_field(spelled)


def test_the_text_reader_produces_rows_without_a_python_row_pass(tmp_path) -> None:
    source = tmp_path / "bridge.log"
    source.write_bytes(
        b"2026-08-14 00:05:01.147 [250-e7256476:9effef3e6a:72504] "
        b"[ULBridge] (INFO) Sending : 8=FIX.4.4|35=D|10=0|\n"
        b"2026-08-14 00:05:01.148 [653] [Spot_FX_TradeCapture] (WARN) prose\n"
    )

    table = _read(source)

    assert table.schema.equals(
        text_options().source_field().into_arrow_schema(), check_metadata=True
    )
    settled = ("seqnum", "currunix", "msgthreadid", "state")
    bracket = ("msgsessionid", "msgctxid", "msgseqnum", "msgpluginid", "loglevel")
    assert table.select(settled + bracket).to_pylist() == [
        {
            "seqnum": 1,
            "currunix": datetime.datetime(2026, 8, 14, 0, 5, 1, 147000, tzinfo=UTC),
            "msgthreadid": 250,
            "state": State.UNKNOWN.value,
            "msgsessionid": "e7256476",
            "msgctxid": "9effef3e6a",
            "msgseqnum": 72504,
            "msgpluginid": "ULBridge",
            "loglevel": "INFO",
        },
        {
            "seqnum": 2,
            "currunix": datetime.datetime(2026, 8, 14, 0, 5, 1, 148000, tzinfo=UTC),
            "msgthreadid": 653,
            "state": State.UNKNOWN.value,
            "msgsessionid": None,
            "msgctxid": None,
            "msgseqnum": None,
            "msgpluginid": "Spot_FX_TradeCapture",
            "loglevel": "WARN",
        },
    ]
    # The body is the line past its row header: the captures are what the
    # header stated, and the body is what the bridge printed after it.
    assert table.column("body").to_pylist() == ["Sending : 8=FIX.4.4|35=D|10=0|", "prose"]
    # The object each line was read from, as the identifier the read was
    # addressed under: one file, so one value and one cross identity.
    assert len(set(table.column("crosscode").to_pylist())) == 1
    assert table.column("crosscode")[0].as_py().endswith("bridge.log")
    assert len(set(table.column("crossuuid").to_pylist())) == 1
    codes = table.column("currhashcode")
    assert codes.type == pyarrow.uint64() and codes.null_count == 0
    assert len(set(codes.to_pylist())) == 2
    # A line is an event nothing has walked: no chain, no source, no clock
    # beside its own.
    for empty in ("creaunix", "execunix", "recdunix", "exprunix", "prevunix", "snapunix"):
        assert table.column(empty).null_count == 2, empty
    assert table.column("prevuuid").null_count == table.column("srcuuids").null_count == 2
    # And the line's own identity, stated as the `uuid` it is -- the table
    # keeps its sixteen bytes -- one per line, which is what a message
    # parsed out of the line names as its source.
    identities = table.column("curruuid").to_pylist()
    assert table.schema.field("curruuid").type == pyarrow.uuid()
    assert all(identity.version == 7 for identity in identities)
    assert len(set(identities)) == 2


def test_a_line_without_the_bridge_header_is_dated_by_the_object_it_was_read_from(
    tmp_path,
) -> None:
    source = tmp_path / "unframed.log"
    source.write_bytes(b"one physical line\n")
    os.utime(source, (WRITTEN_AT.timestamp(), WRITTEN_AT.timestamp()))

    row = _read(source).to_pylist()[0]

    assert row["body"] == "one physical line"
    assert row["seqnum"] == 1
    assert all(row[capture] is None for capture in CAPTURES - {RECORD_CLOCK})
    # The line is still an event, dated by the one clock the read has for a
    # line whose header stated none: the object's own modification time --
    # the same instant on every re-read of the same object. That instant is
    # what its identity is derived from, so a copy of the capture written at
    # another time states another identity for every line the header did not
    # match.
    assert row["currunix"] == WRITTEN_AT
    assert row["curruuid"].bytes != bytes(16)
    assert row["curruuid"].version == 7


def test_two_lines_of_one_text_are_two_rows_of_one_table(tmp_path) -> None:
    """`log_messages` is keyed on `curruuid` alone, so a file that prints the
    same bytes twice has to answer two identities or one of the two lines is
    gone. The code is the line's and not its bytes': it digests the row
    number and the object beside the body, so the two differ, and so do the
    identities derived from them."""
    source = tmp_path / "twice.log"
    source.write_bytes(b"one physical line\none physical line\n")
    os.utime(source, (WRITTEN_AT.timestamp(), WRITTEN_AT.timestamp()))

    first, second = _read(source).to_pylist()

    assert first["body"] == second["body"]
    assert first["crosscode"] == second["crosscode"]
    assert (first["seqnum"], second["seqnum"]) == (1, 2)
    assert first["currhashcode"] != second["currhashcode"]
    assert first["curruuid"] != second["curruuid"]
    # A replay of the same bytes answers the same two identities, because the
    # read dates each line by its object's modification time and its place,
    # never by a clock of the run.
    assert [row["curruuid"] for row in _read(source).to_pylist()] == [
        first["curruuid"],
        second["curruuid"],
    ]


#: What a bridge writes that is not UTF-8, and what the read makes of it: a
#: record in one encoding, a record in the other, the bytes windows-1252
#: fills the C1 range with, the five it leaves undefined, and one record in
#: both encodings at once, which is what a relayed line is.
ENCODED = {
    "café €".encode(): "café €",
    "café".encode("latin-1"): "café",
    b"\x80\x93\x92": "€“’",
    b"\x81\x8d\x8f\x90\x9d": "\x81\x8d\x8f\x90\x9d",
    b"\xff\xfe": "ÿþ",
    b"caf\xc3\xa9 caf\xe9": "café café",
}


@pytest.mark.parametrize(("payload", "text"), ENCODED.items(), ids=[repr(key) for key in ENCODED])
def test_a_body_is_decoded_per_run_and_never_papered_over(payload, text, tmp_path) -> None:
    """A capture is not written in one encoding, and the read decodes each
    run of bytes in the one it is in -- UTF-8, else windows-1252 with its five
    holes read as the C1 controls they stand for -- rather than refusing the
    record or filling it with replacement characters."""
    source = tmp_path / "encoded.log"
    source.write_bytes(
        b"2026-08-14 00:05:01.147 [77-e7256476:9effef3e6a:72503] [P] (INFO) " + payload + b"\n"
    )

    assert _read(source).column("body")[0].as_py() == text


def test_the_shipped_header_dates_every_fraction_this_bridge_writes() -> None:
    """One capture, several loggers, two spellings of one clock -- and one
    header that reads them both, because a line the header misses is a line
    the walk cannot fold, not merely a line without a clock."""
    sample = _read(SAMPLE)

    assert sample.num_rows == 144
    assert sample.column("msgpluginid").null_count == 0, "the header matched every line"
    micros = [instant.microsecond % 1000 for instant in sample.column("currunix").to_pylist()]
    assert sum(1 for micro in micros if micro) == 15, "the grouped micros, read to the micro"
    hours = sorted(instant.hour for instant in sample.column("currunix").to_pylist())
    assert {hour: hours.count(hour) for hour in set(hours)} == {3: 16, 14: 112, 16: 1, 23: 15}


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

    plain = _read(source)
    widened = _read(source, text_options(WIDENED))

    # A header frames every physical line either way -- what changes is how
    # many of them it could date -- and answers the same schema either way.
    assert plain.num_rows == widened.num_rows == 3
    assert plain.schema.equals(widened.schema, check_metadata=True)
    assert log_message_field(WIDENED) == log_message_field()
    assert plain.column("msgpluginid").null_count == 1, "the first line is under no bracket"
    assert widened.column("msgpluginid").null_count == 0
    assert widened.column("currunix")[0].as_py() == datetime.datetime(
        2026, 8, 14, 0, 5, 1, 147250, tzinfo=UTC
    )
    # The two agree on every line both could date, the clock read to the
    # microsecond the bridge wrote.
    assert plain.slice(1).equals(widened.slice(1))


# -- the row --------------------------------------------------------------------


def test_the_row_is_the_event_then_the_body_then_the_captures() -> None:
    """Nothing is declared here: the table's columns are the read's own, in
    its own order -- the sixteen event columns it settles over every line,
    the line past its header, and one column per capture but the clock."""
    field = log_message_field()

    assert field.name == LOG_MESSAGES_NAME == "log_messages"
    assert [member.name for member in field] == [
        *EVENT_COLUMNS,
        "body",
        *(capture for capture in CAPTURE_ORDER if capture != RECORD_CLOCK),
    ]
    assert [member.name for member in field] == [
        member.name for member in text_options().source_field()
    ]


def test_the_row_is_keyed_partitioned_and_sorted_by_the_line_s_event() -> None:
    field = log_message_field()

    assert primary_keys(field) == ["curruuid"]
    assert partition_keys(field) == {"currunix": "hour"}
    assert list(sort_keys(field)) == ["currunix", "seqnum", "curruuid"]
    # The read settles an instant, an identity and a code on every line, so
    # all three are stated.
    for settled in ("currunix", "curruuid", "currhashcode", "body"):
        assert field[settled].nullable is False, settled
    assert field["crosscode"].nullable and field["seqnum"].nullable


def test_the_row_narrows_the_read_to_what_iceberg_stores() -> None:
    """Microsecond instants, sixteen ordered bytes, signed codes, and no
    extension name: the same columns the read states, at the types a table
    holds."""
    read = text_options().source_field().into_arrow_schema()
    stored = log_message_field().into_arrow_schema()

    assert read.field("currunix").type == pyarrow.timestamp("ns", tz="UTC")
    assert stored.field("currunix").type == pyarrow.timestamp("us", tz="UTC")
    for identity in ("curruuid", "crossuuid", "prevuuid"):
        assert read.field(identity).type == pyarrow.uuid(), identity
        assert stored.field(identity).type == pyarrow.binary(16), identity
    assert stored.field("srcuuids").type.value_type == pyarrow.binary(16)
    for code in ("currhashcode", "crosshashcode", "seqnum"):
        assert read.field(code).type == pyarrow.uint64(), code
        assert stored.field(code).type == pyarrow.int64(), code
    assert not [
        member.name for member in stored if b"ARROW:extension:name" in (member.metadata or {})
    ]


def test_the_state_is_an_int32_lifecycle_code_under_its_extension() -> None:
    """A line states `UNKNOWN`: nothing it holds says how far any lifecycle
    reached. The read states the column under `yggdryl.state`, whose scalar is
    the enum member; the table stores the `int32` code alone."""
    read = text_options().source_field()
    stated = read.into_arrow_schema().field("state")

    assert stated.type == pyarrow.int32()
    assert stated.metadata[b"ARROW:extension:name"] == b"yggdryl.state"
    assert read["state"].scalar(0).as_py() is State.UNKNOWN
    assert read["state"].scalar(State.FILLED.value).as_py() is State.FILLED
    stored = log_message_field().into_arrow_schema().field("state")
    assert stored.type == pyarrow.int32() and stored.nullable
    assert b"ARROW:extension:name" not in stored.metadata


def test_a_stored_line_is_the_read_past_the_storage_boundary(tmp_path) -> None:
    """What `parse_log_messages` hands the table: the read's rows at the
    field's types, the unsigned codes viewed as the signed bits they are."""
    source = tmp_path / "bridge.log"
    source.write_bytes(b"2026-08-14 00:05:01.147 [250] [ULBridge] (INFO) body\n")
    handle = IOBase.from_uri(source.as_uri())
    try:
        read = handle.read_arrow_reader(options=text_options()).read_all()
        stored = stored_arrow_reader(
            handle.read_arrow_reader(options=text_options()), log_message_field()
        ).read_all()
    finally:
        handle.close()

    assert stored.schema.equals(log_message_field().into_arrow_schema(), check_metadata=True)
    assert stored.column("curruuid")[0].as_py() == read.column("curruuid")[0].as_py().bytes
    assert stored.column("currhashcode").to_pylist() == [
        code - 2**64 if code >= 2**63 else code for code in read.column("currhashcode").to_pylist()
    ]
    assert stored.column("state").to_pylist() == [State.UNKNOWN.value]
