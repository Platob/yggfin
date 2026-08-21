import datetime
import gzip
import re
from pathlib import Path

import pyarrow
import pyarrow.fs
import pytest

from rekep import Dataset, Field, Log
from rekep.logs import HEADER_PATTERN, TextFile

SAMPLE = Path(__file__).parent.parent / "data" / "app_sample.txt"
SAMPLE_BYTES = SAMPLE.read_bytes()

#: Expectations are derived from the sample, then pinned, so that a regression
#: in HEADER_PATTERN cannot quietly move both sides of an assertion together.
RECORDS = [line for line in SAMPLE_BYTES.split(b"\n") if HEADER_PATTERN.match(line)]
CONTINUATIONS = [
    line for line in SAMPLE_BYTES.split(b"\n") if line and not HEADER_PATTERN.match(line)
]
EXPECTED_RECORDS = 24
EXPECTED_CONTINUATIONS = 4
FIRST_UNIX = 1_786_665_901_147_250_000  # 2026-08-14 00:05:01.147_250 as ns


def test_sample_shape_is_what_the_tests_assume() -> None:
    assert len(RECORDS) == EXPECTED_RECORDS
    assert len(CONTINUATIONS) == EXPECTED_CONTINUATIONS


@pytest.fixture
def plain(tmp_path: Path) -> Path:
    path = tmp_path / "app.txt"
    path.write_bytes(SAMPLE_BYTES)
    return path


@pytest.fixture
def gzipped(tmp_path: Path) -> Path:
    """Written by stdlib gzip, so this proves interop rather than a round-trip."""
    path = tmp_path / "app.txt.gz"
    path.write_bytes(gzip.compress(SAMPLE_BYTES))
    return path


@pytest.fixture
def zstandard(tmp_path: Path) -> Path:
    path = tmp_path / "app.txt.zst"
    with pyarrow.CompressedOutputStream(str(path), "zstd") as out:
        out.write(SAMPLE_BYTES)
    return path


# -- header pattern ---------------------------------------------------------


def test_header_pattern_splits_a_row() -> None:
    match = HEADER_PATTERN.match(RECORDS[0])
    assert match is not None
    assert match["timestamp"] == b"2026-08-14 00:05:01.147_250"
    assert match["thread_name"] == b"250-e7256476:9effef3e6a:72505"
    assert match["driver_name"] == b"OMSSales_Enrichment"
    assert match["level"] == b"DEBUG"
    assert match["message"].startswith(b"-> [5] {trade")


def test_header_pattern_tolerates_a_missing_level() -> None:
    (row,) = [r for r in RECORDS if HEADER_PATTERN.match(r)["level"] is None]
    assert HEADER_PATTERN.match(row)["message"] == b"no level printed by this driver"


@pytest.mark.parametrize(
    "line",
    [
        b"java.lang.IllegalStateException: no binding for token 'venue.mic'",
        b"\tat com.example.objkey.TagWrapper.evaluate(TagWrapper.java:214)",
        b"",
        b"not a log line at all",
    ],
)
def test_header_pattern_rejects_continuations(line: bytes) -> None:
    assert HEADER_PATTERN.match(line) is None


# -- construction -----------------------------------------------------------


def test_post_init_builds_the_filesystem_and_rewrites_url(plain: Path) -> None:
    log = TextFile(url=plain.as_uri())
    assert isinstance(log.filesystem, pyarrow.fs.LocalFileSystem)
    assert log.url != plain.as_uri(), "url should be rewritten as a filesystem path"
    assert log.url.endswith("app.txt")
    assert "://" not in log.url
    log.close()


def test_supplied_filesystem_leaves_url_alone(plain: Path) -> None:
    filesystem = pyarrow.fs.LocalFileSystem()
    log = TextFile(url=str(plain), filesystem=filesystem)
    assert log.filesystem is filesystem
    assert log.url == str(plain)
    with log:
        assert log.read() == SAMPLE_BYTES


def test_from_url(plain: Path) -> None:
    with TextFile.from_url(plain.as_uri()) as log:
        assert log.read() == SAMPLE_BYTES


def test_from_path_accepts_a_relative_path(plain: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(plain.parent)
    with TextFile.from_path("app.txt") as log:
        assert log.read() == SAMPLE_BYTES


def test_construction_is_only_ever_a_classmethod() -> None:
    """There is no module-level factory to drift out of step with the class."""
    import rekep.logs.text_file as module

    assert not hasattr(module, "text_file")
    assert {"from_", "from_url", "from_path"} <= set(dir(TextFile))


# -- generic dispatch -------------------------------------------------------


def test_from_redirects_on_the_source_type(plain: Path) -> None:
    assert TextFile.redirect_of(plain) == "path"
    assert TextFile.redirect_of(plain.as_uri()) == "url"
    with TextFile.from_(plain) as from_path, TextFile.from_(plain.as_uri()) as from_url:
        assert from_path.url == from_url.url


@pytest.mark.parametrize(
    ("requested", "stem"),
    [
        (pyarrow.Table, "arrow_table"),
        (pyarrow.RecordBatchReader, "arrow_reader"),
        (pyarrow.RecordBatch, "arrow_batches"),
    ],
)
def test_into_redirects_on_the_requested_type(plain: Path, requested: type, stem: str) -> None:
    assert TextFile.redirect_of(requested) == stem
    with TextFile.from_(plain) as log:
        assert log.into_(requested) is not None


def test_into_table_via_dispatch_matches_the_named_method(plain: Path) -> None:
    with TextFile.from_(plain) as dispatched, TextFile.from_(plain) as named:
        assert dispatched.into_(pyarrow.Table).equals(named.into_arrow_table())


def test_dispatch_refuses_what_it_cannot_infer(plain: Path) -> None:
    with TextFile.from_(plain) as log, pytest.raises(TypeError, match="cannot infer"):
        log.into_(object())


# -- codec detection --------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("app.txt", None),
        ("app.log", None),
        ("app", None),
        ("app.txt.gz", "gzip"),
        ("app.txt.zst", "zstd"),
        ("app.txt.bz2", "bz2"),
        ("app.txt.lz4", "lz4"),
    ],
)
def test_codec_detected_from_extension(tmp_path: Path, name: str, expected: str | None) -> None:
    assert TextFile(url=tmp_path.joinpath(name).as_uri())._codec == expected


@pytest.mark.parametrize("fixture", ["plain", "gzipped", "zstandard"])
def test_reads_whole_log_whatever_the_encoding(
    request: pytest.FixtureRequest, fixture: str
) -> None:
    path: Path = request.getfixturevalue(fixture)
    with TextFile(url=path.as_uri()) as log:
        assert log.read() == SAMPLE_BYTES


def test_compressed_log_is_decoded_not_raw(gzipped: Path) -> None:
    assert gzipped.stat().st_size < len(SAMPLE_BYTES)
    with TextFile(url=gzipped.as_uri()) as log:
        assert log.read().startswith(b"2026-")


# -- schema -----------------------------------------------------------------


def test_schema(plain: Path) -> None:
    schema = TextFile(url=plain.as_uri()).schema
    assert schema.names == [
        "url",
        "recorded_at_unix",
        "recorded_at_date",
        "recorded_at_time",
        "thread_name",
        "driver_name",
        "category_id",
        "category_name",
        "message",
        "h64",
    ]
    assert schema.field("recorded_at_unix").type == pyarrow.int64()
    assert schema.field("h64").type == pyarrow.int64()
    assert schema.field("message").type == pyarrow.string()


# -- arrow reader -----------------------------------------------------------


@pytest.mark.parametrize("fixture", ["plain", "gzipped", "zstandard"])
def test_reader_parses_every_record(request: pytest.FixtureRequest, fixture: str) -> None:
    path: Path = request.getfixturevalue(fixture)
    with TextFile(url=path.as_uri()) as log:
        table = log.into_arrow_reader().read_all()
    assert table.num_rows == EXPECTED_RECORDS
    assert table.schema == log.schema


def test_reader_returns_a_record_batch_reader(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        reader = log.into_arrow_reader()
        assert isinstance(reader, pyarrow.RecordBatchReader)
        assert reader.schema == log.schema
        reader.read_all()


def test_first_row(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
        url = log.url

    first = table.slice(0, 1).to_pylist()[0]
    assert first["url"] == url
    assert first["recorded_at_unix"] == FIRST_UNIX
    assert first["thread_name"] == "250-e7256476:9effef3e6a:72505"
    assert first["driver_name"] == "OMSSales_Enrichment"
    assert first["message"].startswith("-> [5] {trade")


def test_date_and_time_are_derived_from_the_timestamp(plain: Path) -> None:
    """The denormalised columns must agree with the nanosecond column."""
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
    for row in table.to_pylist():
        moment = datetime.datetime.fromtimestamp(row["recorded_at_unix"] / 1e9, tz=datetime.UTC)
        assert row["recorded_at_date"] == moment.date()
        assert row["recorded_at_time"].replace(microsecond=0) == moment.time().replace(
            microsecond=0
        )
        micros = (row["recorded_at_unix"] // 1000) % 1_000_000
        assert row["recorded_at_time"].microsecond == micros


def test_unix_is_total_nanos_since_epoch(plain: Path) -> None:
    moment = datetime.datetime(2026, 8, 14, 0, 5, 1, 147_250, tzinfo=datetime.UTC)
    expected = int(moment.timestamp()) * 1_000_000_000 + 147_250 * 1_000
    assert expected == FIRST_UNIX

    with TextFile(url=plain.as_uri()) as log:
        unix = log.into_arrow_table().column("recorded_at_unix").to_pylist()

    assert unix[0] == expected
    assert unix == sorted(unix), "the sample is in chronological order"


def test_url_column_identifies_the_source(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
        assert set(table.column("url").to_pylist()) == {log.url}


def test_hash64_is_per_line_and_fits_int64(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        hashes = log.into_arrow_table().column("h64").to_pylist()
    assert len(set(hashes)) == EXPECTED_RECORDS, "distinct lines hash distinctly"
    assert all(-(2**63) <= h < 2**63 for h in hashes)


def test_hash64_is_stable_across_reads(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as first, TextFile(url=plain.as_uri()) as second:
        assert first.into_arrow_table().column("h64").to_pylist() == (
            second.into_arrow_table().column("h64").to_pylist()
        )


def test_rows_stay_in_file_order(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        unix = log.into_arrow_table().column("recorded_at_unix").to_pylist()
    assert unix[0] == FIRST_UNIX
    assert unix == sorted(unix), "the sample is chronological, so parsing must keep it so"


def test_level_is_stripped_from_the_message(plain: Path) -> None:
    """`level` is parsed by the regex but not a column: it must not leak."""
    with TextFile(url=plain.as_uri()) as log:
        messages = log.into_arrow_table().column("message").to_pylist()
    assert not any(message.startswith(("(DEBUG)", "(INFO)", "(WARNING)")) for message in messages)


def test_continuations_fold_into_the_previous_message(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        messages = log.into_arrow_table().column("message").to_pylist()

    (folded,) = [m for m in messages if "java.lang.IllegalStateException" in m]
    assert folded.startswith("Expression from CODE-0000058 raised while evaluating")
    assert folded.count("\n") == EXPECTED_CONTINUATIONS


def test_continuations_are_dropped_when_folding_is_off(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_reader(fold_continuations=False).read_all()
    assert table.num_rows == EXPECTED_RECORDS
    assert all("\n" not in message for message in table.column("message").to_pylist())


@pytest.mark.parametrize("batch_row_size", [1, 5, 23, EXPECTED_RECORDS, 10_000])
def test_batching_does_not_change_the_result(plain: Path, batch_row_size: int) -> None:
    """The rows *and their messages*: a folded continuation is a message, not a row.

    Counting alone passes over a continuation dropped at a batch boundary,
    because dropping one never changes how many rows there are.
    """
    with TextFile(url=plain.as_uri()) as log:
        batches = list(log.into_arrow_reader(batch_row_size=batch_row_size))
    with TextFile(url=plain.as_uri()) as log:
        whole = log.into_arrow_table(batch_row_size=EXPECTED_RECORDS * 2)
    assert sum(batch.num_rows for batch in batches) == EXPECTED_RECORDS
    assert max(batch.num_rows for batch in batches) <= batch_row_size
    messages = [message for batch in batches for message in batch.column("message").to_pylist()]
    assert messages == whole.column("message").to_pylist()


def test_a_continuation_on_the_batch_boundary_is_still_folded(tmp_path: Path) -> None:
    """The row a continuation belongs to must still be reachable when it arrives."""
    path = tmp_path / "boundary.txt"
    with path.open("wb") as out:
        for index in range(6):
            out.write(b"2026-08-14 00:05:%02d.000_000 [t] [M] (INFO) r%d\n" % (index, index))
            if index == 3:  # the last row of a four-row batch
                out.write(b"\tat com.example.A.b(A.java:1)\n")
    with TextFile(url=path.as_uri()) as log:
        table = log.into_arrow_table(batch_row_size=4)
    assert table.num_rows == 6
    assert table.column("message")[3].as_py() == "r3\n\tat com.example.A.b(A.java:1)"


@pytest.mark.parametrize("read_byte_size", [1, 7, 64, 1 << 20])
def test_read_byte_size_does_not_change_the_result(plain: Path, read_byte_size: int) -> None:
    """A record split across two reads must still be parsed once, whole."""
    with TextFile(url=plain.as_uri()) as log:
        table = log.into_arrow_reader(read_byte_size=read_byte_size).read_all()
    assert table.num_rows == EXPECTED_RECORDS
    assert table.column("recorded_at_unix").to_pylist()[-1] == max(
        table.column("recorded_at_unix").to_pylist()
    )


def test_reader_is_lazy_until_pulled(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        reader = log.into_arrow_reader(batch_row_size=1)
        assert log.tell() == 0  # nothing scanned yet
        reader.read_next_batch()
        assert log.tell() > 0
        reader.close()


def test_custom_header_pattern(tmp_path: Path) -> None:
    """A caller's pattern must supply the same groups the schema is built from.

    A *different* pattern, over a differently shaped line -- the timestamp
    written the way `datetime.isoformat()` writes it, which is one character
    shorter than the bundled shape and therefore cannot be sliced at the
    bundled offsets.
    """
    pattern = re.compile(
        rb"^(?P<timestamp>\S+)\|(?P<thread_name>[^|]*)\|(?P<driver_name>[^|]*)\|(?P<message>.*)$",
        re.DOTALL,
    )
    path = tmp_path / "custom.txt"
    path.write_bytes(
        b"2026-08-14T00:05:01.167520|t1|Mod|first\n2026-08-14T00:05:02.000001|t2|Mod|second\n"
    )
    with TextFile(url=path.as_uri(), header_pattern=pattern) as log:
        table = log.into_arrow_table()
    assert table.column("message").to_pylist() == ["first", "second"]
    assert [time.microsecond for time in table.column("recorded_at_time").to_pylist()] == [
        167520,
        1,
    ]


@pytest.mark.parametrize(
    ("stamp", "microsecond"),
    [
        (b"2026-08-14 00:05:01.167_520", 167520),  # the bundled shape
        (b"2026-08-14 00:05:01,167,520", 167520),  # which the bundled regex also admits
        (b"2026-08-14T00:05:01.167520", 167520),  # what `datetime.isoformat()` writes
        (b"2026-08-14 00:05:01.167520123", 167520),  # nanoseconds, truncated
        (b"2026-08-14 00:05:01", 0),  # no fraction at all
    ],
)
def test_a_timestamp_is_read_not_sliced_at_another_width(stamp: bytes, microsecond: int) -> None:
    """Slicing at fixed offsets is only sound at the width the pattern pins.

    One character short, the same slices land on other digits and cast
    happily: `.167520` came back as `.167200`, silently.
    """
    from rekep.logs.text_file import _local_micros

    assert _local_micros([stamp])[0].as_py().microsecond == microsecond


def test_a_store_that_cannot_append_says_so(tmp_path: Path) -> None:
    """S3 and GCS have no append, and a log is written by appending.

    The refusal comes from `pyarrow.fs` itself, so no network is involved --
    what is tested is that it arrives naming the two things that do work
    instead of as an `ArrowNotImplementedError` from three frames down.
    """
    import pyarrow.fs

    store = pyarrow.fs.S3FileSystem(endpoint_override="http://127.0.0.1:1", region="us-east-1")
    log = TextFile(url="bucket/app.txt", filesystem=store)
    with pytest.raises(NotImplementedError, match="cannot append"):
        log._append(b"a line\n")


def test_from_path_takes_the_zone_too(plain: Path) -> None:
    """The documented example: a local log is the one most likely to be local time."""
    naive = TextFile.from_path(plain).read_arrow_table()
    zoned = TextFile.from_path(plain, timezone="Europe/Paris").read_arrow_table()
    assert (
        zoned.column("recorded_at_unix").to_pylist() != naive.column("recorded_at_unix").to_pylist()
    )
    assert (
        zoned.column("recorded_at_time").to_pylist() == naive.column("recorded_at_time").to_pylist()
    ), "same wall clock"


def test_reading_the_same_log_twice_reads_it_twice(plain: Path) -> None:
    """A reader takes the stream, so the next one has to be given a new one."""
    log = TextFile(url=plain.as_uri())
    assert [log.read_arrow_table().num_rows for _ in range(3)] == [EXPECTED_RECORDS] * 3


def test_a_write_is_visible_to_the_next_read_of_the_same_object(tmp_path: Path) -> None:
    """Reopening, not seeking: a seek rewinds the decoded position, not the file."""
    source = tmp_path / "in.txt"
    source.write_bytes(b"2026-08-14 00:05:01.167_520 [t] [M] (INFO) one\n")
    grown = tmp_path / "out.txt"
    rows = TextFile.from_path(source).read_arrow_table()
    writer = TextFile.from_path(grown)
    writer.write_arrow(rows)
    assert writer.read_arrow_table().num_rows == 1
    writer.write_arrow(rows)
    assert writer.read_arrow_table().num_rows == 2


@pytest.mark.parametrize("zone", [None, "Europe/Paris", "America/New_York", "Asia/Tokyo"])
def test_a_write_renders_the_zone_it_read(tmp_path: Path, plain: Path, zone: str | None) -> None:
    """`unix` is an instant and a line is a wall clock: rendering as UTC shifts it.

    And shifts it again on the next round trip, since reading adds the offset
    back -- so this compares the columns, not just the row count.
    """
    rows = TextFile.from_url(plain.as_uri(), timezone=zone).read_arrow_table()
    written = tmp_path / "written.txt"
    TextFile.from_url(written.as_uri(), timezone=zone).write_arrow(rows)
    back = TextFile.from_url(written.as_uri(), timezone=zone).read_arrow_table()
    for column in ("recorded_at_unix", "recorded_at_date", "recorded_at_time", "message"):
        assert back.column(column).to_pylist() == rows.column(column).to_pylist(), column


def test_reader_on_a_closed_log_raises(plain: Path) -> None:
    log = TextFile(url=plain.as_uri())
    log.close()
    with pytest.raises(ValueError, match="closed file"):
        log.into_arrow_reader()


# -- stream surface ---------------------------------------------------------


def test_plain_log_is_seekable(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        assert log.seekable()
        assert log.read(5) == b"2026-"
        assert log.tell() == 5
        log.seek(0)
        assert log.read(5) == b"2026-"


def test_compressed_log_is_not_seekable(gzipped: Path) -> None:
    with TextFile(url=gzipped.as_uri()) as log:
        assert not log.seekable()


def test_readinto(plain: Path) -> None:
    buffer = bytearray(5)
    with TextFile(url=plain.as_uri()) as log:
        assert log.readinto(buffer) == 5
    assert bytes(buffer) == b"2026-"


def test_is_read_only(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        assert log.readable()
        assert not log.writable()
        with pytest.raises(OSError):
            log.write(b"nope")


# -- laziness and lifecycle -------------------------------------------------


def test_nothing_is_opened_until_first_read(plain: Path) -> None:
    log = TextFile(url=plain.as_uri())
    assert "_stream" not in log.__dict__
    log.read(1)
    assert "_stream" in log.__dict__
    log.close()


def test_stream_is_cached(plain: Path) -> None:
    with TextFile(url=plain.as_uri()) as log:
        assert log._stream is log._stream


def test_missing_file_only_fails_on_read(tmp_path: Path) -> None:
    log = TextFile(url=tmp_path.joinpath("absent.txt").as_uri())
    with pytest.raises(FileNotFoundError):
        log.read()


def test_close_does_not_open(tmp_path: Path) -> None:
    """Closing an unread log must not open anything -- __del__ takes this path."""
    log = TextFile(url=tmp_path.joinpath("absent.txt").as_uri())
    log.close()
    assert log.closed
    assert "_stream" not in log.__dict__


def test_close_is_idempotent(plain: Path) -> None:
    log = TextFile(url=plain.as_uri())
    log.read(1)
    log.close()
    log.close()
    assert log.closed


@pytest.mark.parametrize("operation", [lambda f: f.read(), lambda f: f.tell(), lambda f: f.seek(0)])
def test_use_after_close_raises(plain: Path, operation) -> None:
    log = TextFile(url=plain.as_uri())
    log.read(1)
    log.close()
    with pytest.raises(ValueError, match="closed file"):
        operation(log)


def test_repr_shows_url(plain: Path) -> None:
    log = TextFile(url=plain.as_uri())
    assert "app.txt" in repr(log)
    log.close()


def test_a_crlf_log_parses_identically(plain: Path, tmp_path: Path) -> None:
    """Windows-written captures must not leak carriage returns into messages."""
    crlf = tmp_path / "crlf.txt"
    crlf.write_bytes(SAMPLE_BYTES.replace(b"\n", b"\r\n"))
    with TextFile(url=plain.as_uri()) as a, TextFile(url=crlf.as_uri()) as b:
        left, right = a.into_arrow_table(), b.into_arrow_table()
    assert left.drop_columns("url").equals(right.drop_columns("url"))


# -- timezone: the wall clock is local, the instant is not ----------------


#: The first record line's wall clock, read out of the fixture rather than
#: assumed -- then pinned, so a regex regression cannot move both sides.
FIRST_CLOCK = datetime.datetime.fromisoformat(
    RECORDS[0][:26].decode().replace("_", "").replace(",", ".")
)


def test_the_fixture_starts_where_the_assertions_below_say() -> None:
    assert FIRST_CLOCK == datetime.datetime(2026, 8, 14, 0, 5, 1, 147250)  # noqa: DTZ001


def test_without_a_timezone_the_clock_is_read_as_utc() -> None:
    with TextFile.from_url(SAMPLE.resolve().as_uri()) as log:
        batch = next(iter(log.into_arrow_batches()))
    instant = FIRST_CLOCK.replace(tzinfo=datetime.UTC)
    assert (
        batch.column("recorded_at_unix")[0].as_py() == int(instant.timestamp() * 1_000_000) * 1_000
    )


def test_a_timezone_shifts_the_instant_by_its_offset() -> None:
    """Same characters in the file, different moment in time."""
    naive, paris, york = (
        next(iter(TextFile.from_url(SAMPLE.resolve().as_uri(), timezone=zone).into_arrow_batches()))
        .column("recorded_at_unix")[0]
        .as_py()
        for zone in (None, "Europe/Paris", "America/New_York")
    )
    assert paris == naive - 2 * 3_600 * 1_000_000_000, "CEST is UTC+2 in August"
    assert york == naive + 4 * 3_600 * 1_000_000_000, "EDT is UTC-4 in August"


def test_the_date_and_time_columns_stay_on_the_local_clock() -> None:
    """They are what the line said; `unix` is the column that answers when."""
    columns = {}
    for zone in (None, "Europe/Paris", "Pacific/Auckland"):
        with TextFile.from_url(SAMPLE.resolve().as_uri(), timezone=zone) as log:
            batch = next(iter(log.into_arrow_batches()))
        columns[zone] = (
            batch.column("recorded_at_date")[0].as_py(),
            batch.column("recorded_at_time")[0].as_py(),
        )
    assert len(set(columns.values())) == 1, "the wall clock does not move"
    assert columns[None] == (FIRST_CLOCK.date(), FIRST_CLOCK.time())


def test_a_repeated_hour_resolves_rather_than_raising() -> None:
    """A DST fall-back hour is the calendar's doing, not a broken log --
    pyarrow would raise by default, which would kill the parse once a year."""
    import pyarrow

    from rekep.logs.text_file import _unix_nanos

    ambiguous = pyarrow.array(
        [datetime.datetime(2026, 10, 25, 2, 30)], type=pyarrow.timestamp("us")
    )
    assert _unix_nanos(ambiguous, "Europe/Paris")[0].as_py() is not None


def test_a_pre_epoch_timestamp_lands_on_the_right_day() -> None:
    import pyarrow

    from rekep.logs.text_file import _date_and_time

    before = pyarrow.array(
        [datetime.datetime(1969, 12, 31, 23, 59, 59)], type=pyarrow.timestamp("us")
    )
    date, time = _date_and_time(before)
    assert date[0].as_py() == datetime.date(1969, 12, 31)
    assert time[0].as_py() == datetime.time(23, 59, 59)


# -- static values ----------------------------------------------------------


def test_static_values_land_at_the_end_in_insertion_order(plain: Path) -> None:
    """After the data columns, so adding one moves nothing a reader selects."""
    log = TextFile.from_path(plain, static_values={"bridge": "bridge-1", "shard": 7})
    table = log.read_arrow_table()
    assert table.schema.names[-2:] == ["bridge", "shard"]
    assert table.schema.names[:-2] == Log.FIELD.into_arrow_schema().names
    assert table.column("bridge").to_pylist() == ["bridge-1"] * table.num_rows
    assert table.column("shard").to_pylist() == [7] * table.num_rows


def test_nothing_names_the_source_but_the_caller(plain: Path) -> None:
    """No column is hardcoded: a capture says what it is, or says nothing."""
    assert TextFile.from_path(plain).read_arrow_table().schema.names == (
        Log.FIELD.into_arrow_schema().names
    )


def test_a_static_value_infers_its_arrow_type(plain: Path) -> None:
    log = TextFile.from_path(
        plain, static_values={"text": "a", "count": 2, "ratio": 0.5, "flag": True}
    )
    schema = log.schema
    assert schema.field("text").type == pyarrow.string()
    assert schema.field("count").type == pyarrow.int64()
    assert schema.field("ratio").type == pyarrow.float64()
    assert schema.field("flag").type == pyarrow.bool_()


def test_a_static_value_can_state_its_type(plain: Path) -> None:
    """A scalar is the explicit form -- and the only way to say a typed null."""
    log = TextFile.from_path(
        plain,
        static_values={
            "desk": pyarrow.scalar("EU", pyarrow.large_string()),
            "region": pyarrow.scalar(None, pyarrow.string()),
        },
    )
    assert log.schema.field("desk").type == pyarrow.large_string()
    assert log.schema.field("region").type == pyarrow.string()
    assert log.schema.field("region").nullable is True
    assert log.schema.field("desk").nullable is False
    table = log.read_arrow_table()
    assert table.column("region").to_pylist() == [None] * table.num_rows


def test_a_static_value_of_none_is_refused(plain: Path) -> None:
    """Arrow's `null` type is a column no store can widen later."""
    log = TextFile.from_path(plain, static_values={"region": None})
    with pytest.raises(ValueError, match="has no Arrow type"):
        log.read_arrow_table()


def test_a_static_column_is_part_of_the_declared_shape(plain: Path) -> None:
    log = TextFile.from_path(plain, static_values={"bridge": "bridge-1"})
    assert log.into_struct_field().names[-1] == "bridge"
    assert log.into_struct_field().field("bridge").arrow_type == pyarrow.string()


def test_static_columns_are_not_written_back_into_a_line(plain: Path, tmp_path: Path) -> None:
    """A line is what the header says; a constant column is not in it."""
    rows = TextFile.from_path(plain, static_values={"bridge": "bridge-1"}).read_arrow_table()
    out = TextFile.from_path(tmp_path / "copy.txt")
    out.write_arrow(rows)
    assert out.read_arrow_table().num_rows == rows.num_rows


# -- the dataset ------------------------------------------------------------


def test_a_text_file_is_a_dataset(plain: Path) -> None:
    log = TextFile.from_path(plain)
    assert isinstance(log, Dataset)
    assert log.exists
    assert log.into_struct_field() is Log.FIELD
    assert log.read_arrow_table().num_rows == EXPECTED_RECORDS


def test_a_missing_file_does_not_exist_yet(tmp_path: Path) -> None:
    assert not TextFile.from_path(tmp_path / "absent.txt").exists


def test_reading_casts_only_when_asked(plain: Path) -> None:
    log = TextFile.from_path(plain)
    assert log.read_arrow_reader().schema.equals(Log.FIELD.into_arrow_schema())
    narrow = pyarrow.schema([("message", pyarrow.large_string())])
    assert log.read_arrow_reader(narrow).schema.field("message").type == pyarrow.large_string()


def test_a_write_renders_lines_that_parse_back(plain: Path, tmp_path: Path) -> None:
    """The renderer is the header regex read backwards; the proof is a round trip."""
    source = TextFile.from_path(plain).read_arrow_table()
    written = TextFile.from_path(tmp_path / "written.txt")
    written.write_arrow(source)

    again = TextFile.from_path(tmp_path / "written.txt").read_arrow_table()
    assert again.num_rows == source.num_rows
    for column in (
        "recorded_at_unix",
        "recorded_at_date",
        "recorded_at_time",
        "thread_name",
        "driver_name",
        "message",
    ):
        assert again.column(column).to_pylist() == source.column(column).to_pylist(), column


def test_a_write_creates_the_file(tmp_path: Path) -> None:
    target = tmp_path / "fresh.txt"
    log = TextFile.from_path(target)
    assert not log.exists
    log.write_arrow(TextFile.from_path(SAMPLE).read_arrow_table())
    assert log.exists and target.stat().st_size > 0


def test_writes_append_rather_than_replace(plain: Path, tmp_path: Path) -> None:
    rows = TextFile.from_path(plain).read_arrow_table()
    target = TextFile.from_path(tmp_path / "appended.txt")
    target.write_arrow(rows)
    target.write_arrow(rows)
    assert target.read_arrow_table().num_rows == 2 * rows.num_rows


def test_commit_row_size_writes_in_chunks(plain: Path, tmp_path: Path) -> None:
    rows = TextFile.from_path(plain).read_arrow_table()
    target = TextFile.from_path(tmp_path / "chunked.txt")
    target.write_arrow_reader(rows.to_reader(max_chunksize=5), commit_row_size=5)
    assert target.read_arrow_table().num_rows == rows.num_rows


def test_a_write_casts_a_nearly_right_batch(tmp_path: Path) -> None:
    batch = pyarrow.RecordBatch.from_pydict(
        {
            "recorded_at_unix": pyarrow.array([1_786_665_901_147_250_000], pyarrow.int64()),
            "message": ["hello"],
            "thread_name": ["t"],
            "driver_name": ["d"],
            "noise": ["dropped"],
        }
    )
    target = TextFile.from_path(tmp_path / "cast.txt")
    target.write_arrow(batch)
    parsed = target.read_arrow_table()
    assert parsed.column("message").to_pylist() == ["hello"]
    assert parsed.column("driver_name").to_pylist() == ["d"]


def test_a_text_file_cannot_merge(tmp_path: Path) -> None:
    log = TextFile.from_path(tmp_path / "merge.txt")
    with pytest.raises(ValueError, match="cannot merge"):
        log.write_arrow(TextFile.from_path(SAMPLE).read_arrow_table(), merge_by=True)


def test_an_empty_write_leaves_an_empty_file(tmp_path: Path) -> None:
    log = TextFile.from_path(tmp_path / "empty.txt")
    log.write_arrow_reader(iter(()))
    assert log.exists
    assert log.read_arrow_table().num_rows == 0


def test_create_with_adopts_a_shape(tmp_path: Path) -> None:
    log = TextFile.from_path(tmp_path / "shaped.txt")
    narrow = Field.from_arrow_schema(pyarrow.schema([("message", pyarrow.string())]))
    log.create_with(narrow)
    assert log.exists
    assert log.into_struct_field() is narrow
    assert log.read_arrow_table().column_names == ["message"]
