import datetime
import gzip
import re
from pathlib import Path

import pyarrow
import pyarrow.fs
import pytest

from rekep.logs import HEADER_PATTERN, LogFile

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
    assert match["driver"] == b"OMSSales_Enrichment"
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
    log = LogFile(url=plain.as_uri())
    assert isinstance(log.filesystem, pyarrow.fs.LocalFileSystem)
    assert log.url != plain.as_uri(), "url should be rewritten as a filesystem path"
    assert log.url.endswith("app.txt")
    assert "://" not in log.url
    log.close()


def test_supplied_filesystem_leaves_url_alone(plain: Path) -> None:
    filesystem = pyarrow.fs.LocalFileSystem()
    log = LogFile(url=str(plain), filesystem=filesystem)
    assert log.filesystem is filesystem
    assert log.url == str(plain)
    with log:
        assert log.read() == SAMPLE_BYTES


def test_from_url(plain: Path) -> None:
    with LogFile.from_url(plain.as_uri()) as log:
        assert log.read() == SAMPLE_BYTES


def test_from_path_accepts_a_relative_path(plain: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(plain.parent)
    with LogFile.from_path("app.txt") as log:
        assert log.read() == SAMPLE_BYTES


def test_construction_is_only_ever_a_classmethod() -> None:
    """There is no module-level factory to drift out of step with the class."""
    import rekep.logs.log_file as module

    assert not hasattr(module, "log_file")
    assert {"from_", "from_url", "from_path"} <= set(dir(LogFile))


# -- generic dispatch -------------------------------------------------------


def test_from_redirects_on_the_source_type(plain: Path) -> None:
    assert LogFile.redirect_of(plain) == "path"
    assert LogFile.redirect_of(plain.as_uri()) == "url"
    with LogFile.from_(plain) as from_path, LogFile.from_(plain.as_uri()) as from_url:
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
    assert LogFile.redirect_of(requested) == stem
    with LogFile.from_(plain) as log:
        assert log.into_(requested) is not None


def test_into_table_via_dispatch_matches_the_named_method(plain: Path) -> None:
    with LogFile.from_(plain) as dispatched, LogFile.from_(plain) as named:
        assert dispatched.into_(pyarrow.Table).equals(named.into_arrow_table())


def test_dispatch_refuses_what_it_cannot_infer(plain: Path) -> None:
    with LogFile.from_(plain) as log, pytest.raises(TypeError, match="cannot infer"):
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
    assert LogFile(url=tmp_path.joinpath(name).as_uri())._codec == expected


@pytest.mark.parametrize("fixture", ["plain", "gzipped", "zstandard"])
def test_reads_whole_log_whatever_the_encoding(
    request: pytest.FixtureRequest, fixture: str
) -> None:
    path: Path = request.getfixturevalue(fixture)
    with LogFile(url=path.as_uri()) as log:
        assert log.read() == SAMPLE_BYTES


def test_compressed_log_is_decoded_not_raw(gzipped: Path) -> None:
    assert gzipped.stat().st_size < len(SAMPLE_BYTES)
    with LogFile(url=gzipped.as_uri()) as log:
        assert log.read().startswith(b"2026-")


# -- schema -----------------------------------------------------------------


def test_schema(plain: Path) -> None:
    schema = LogFile(url=plain.as_uri()).schema
    assert schema.names == [
        "url",
        "unix",
        "date",
        "time",
        "thread_name",
        "driver",
        "message",
        "hash64",
    ]
    assert schema.field("unix").type == pyarrow.int64()
    assert schema.field("hash64").type == pyarrow.int64()
    assert schema.field("message").type == pyarrow.string()


# -- arrow reader -----------------------------------------------------------


@pytest.mark.parametrize("fixture", ["plain", "gzipped", "zstandard"])
def test_reader_parses_every_record(request: pytest.FixtureRequest, fixture: str) -> None:
    path: Path = request.getfixturevalue(fixture)
    with LogFile(url=path.as_uri()) as log:
        table = log.into_arrow_reader().read_all()
    assert table.num_rows == EXPECTED_RECORDS
    assert table.schema == log.schema


def test_reader_returns_a_record_batch_reader(plain: Path) -> None:
    with LogFile(url=plain.as_uri()) as log:
        reader = log.into_arrow_reader()
        assert isinstance(reader, pyarrow.RecordBatchReader)
        assert reader.schema == log.schema
        reader.read_all()


def test_first_row(plain: Path) -> None:
    with LogFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
        url = log.url

    first = table.slice(0, 1).to_pylist()[0]
    assert first["url"] == url
    assert first["unix"] == FIRST_UNIX
    assert first["thread_name"] == "250-e7256476:9effef3e6a:72505"
    assert first["driver"] == "OMSSales_Enrichment"
    assert first["message"].startswith("-> [5] {trade")


def test_date_and_time_are_derived_from_the_timestamp(plain: Path) -> None:
    """The denormalised columns must agree with the nanosecond column."""
    with LogFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
    for row in table.to_pylist():
        moment = datetime.datetime.fromtimestamp(row["unix"] / 1e9, tz=datetime.UTC)
        assert row["date"] == moment.date()
        assert row["time"].replace(microsecond=0) == moment.time().replace(microsecond=0)
        micros = (row["unix"] // 1000) % 1_000_000
        assert row["time"].microsecond == micros


def test_unix_is_total_nanos_since_epoch(plain: Path) -> None:
    moment = datetime.datetime(2026, 8, 14, 0, 5, 1, 147_250, tzinfo=datetime.UTC)
    expected = int(moment.timestamp()) * 1_000_000_000 + 147_250 * 1_000
    assert expected == FIRST_UNIX

    with LogFile(url=plain.as_uri()) as log:
        unix = log.into_arrow_table().column("unix").to_pylist()

    assert unix[0] == expected
    assert unix == sorted(unix), "the sample is in chronological order"


def test_url_column_identifies_the_source(plain: Path) -> None:
    with LogFile(url=plain.as_uri()) as log:
        table = log.into_arrow_table()
        assert set(table.column("url").to_pylist()) == {log.url}


def test_hash64_is_per_line_and_fits_int64(plain: Path) -> None:
    with LogFile(url=plain.as_uri()) as log:
        hashes = log.into_arrow_table().column("hash64").to_pylist()
    assert len(set(hashes)) == EXPECTED_RECORDS, "distinct lines hash distinctly"
    assert all(-(2**63) <= h < 2**63 for h in hashes)


def test_hash64_is_stable_across_reads(plain: Path) -> None:
    with LogFile(url=plain.as_uri()) as first, LogFile(url=plain.as_uri()) as second:
        assert first.into_arrow_table().column("hash64").to_pylist() == (
            second.into_arrow_table().column("hash64").to_pylist()
        )


def test_rows_stay_in_file_order(plain: Path) -> None:
    with LogFile(url=plain.as_uri()) as log:
        unix = log.into_arrow_table().column("unix").to_pylist()
    assert unix[0] == FIRST_UNIX
    assert unix == sorted(unix), "the sample is chronological, so parsing must keep it so"


def test_level_is_stripped_from_the_message(plain: Path) -> None:
    """`level` is parsed by the regex but not a column: it must not leak."""
    with LogFile(url=plain.as_uri()) as log:
        messages = log.into_arrow_table().column("message").to_pylist()
    assert not any(message.startswith(("(DEBUG)", "(INFO)", "(WARNING)")) for message in messages)


def test_continuations_fold_into_the_previous_message(plain: Path) -> None:
    with LogFile(url=plain.as_uri()) as log:
        messages = log.into_arrow_table().column("message").to_pylist()

    (folded,) = [m for m in messages if "java.lang.IllegalStateException" in m]
    assert folded.startswith("Expression from CODE-0000058 raised while evaluating")
    assert folded.count("\n") == EXPECTED_CONTINUATIONS


def test_continuations_are_dropped_when_folding_is_off(plain: Path) -> None:
    with LogFile(url=plain.as_uri()) as log:
        table = log.into_arrow_reader(fold_continuations=False).read_all()
    assert table.num_rows == EXPECTED_RECORDS
    assert all("\n" not in message for message in table.column("message").to_pylist())


@pytest.mark.parametrize("batch_row_size", [1, 5, 23, EXPECTED_RECORDS, 10_000])
def test_batching_does_not_change_the_result(plain: Path, batch_row_size: int) -> None:
    with LogFile(url=plain.as_uri()) as log:
        batches = list(log.into_arrow_reader(batch_row_size=batch_row_size))
    assert sum(batch.num_rows for batch in batches) == EXPECTED_RECORDS
    assert max(batch.num_rows for batch in batches) <= batch_row_size


@pytest.mark.parametrize("read_byte_size", [1, 7, 64, 1 << 20])
def test_read_byte_size_does_not_change_the_result(plain: Path, read_byte_size: int) -> None:
    """A record split across two reads must still be parsed once, whole."""
    with LogFile(url=plain.as_uri()) as log:
        table = log.into_arrow_reader(read_byte_size=read_byte_size).read_all()
    assert table.num_rows == EXPECTED_RECORDS
    assert table.column("unix").to_pylist()[-1] == max(table.column("unix").to_pylist())


def test_reader_is_lazy_until_pulled(plain: Path) -> None:
    with LogFile(url=plain.as_uri()) as log:
        reader = log.into_arrow_reader(batch_row_size=1)
        assert log.tell() == 0  # nothing scanned yet
        reader.read_next_batch()
        assert log.tell() > 0
        reader.close()


def test_custom_header_pattern(plain: Path) -> None:
    """A caller's pattern must supply the same groups the schema is built from."""
    pattern = re.compile(HEADER_PATTERN.pattern.replace(rb"[ \t]*(?P<seqnum>", rb"\s*(?P<seqnum>"))
    with LogFile(url=plain.as_uri(), header_pattern=pattern) as log:
        assert log.into_arrow_table().num_rows == EXPECTED_RECORDS


def test_reader_on_a_closed_log_raises(plain: Path) -> None:
    log = LogFile(url=plain.as_uri())
    log.close()
    with pytest.raises(ValueError, match="closed file"):
        log.into_arrow_reader()


# -- stream surface ---------------------------------------------------------


def test_plain_log_is_seekable(plain: Path) -> None:
    with LogFile(url=plain.as_uri()) as log:
        assert log.seekable()
        assert log.read(5) == b"2026-"
        assert log.tell() == 5
        log.seek(0)
        assert log.read(5) == b"2026-"


def test_compressed_log_is_not_seekable(gzipped: Path) -> None:
    with LogFile(url=gzipped.as_uri()) as log:
        assert not log.seekable()


def test_readinto(plain: Path) -> None:
    buffer = bytearray(5)
    with LogFile(url=plain.as_uri()) as log:
        assert log.readinto(buffer) == 5
    assert bytes(buffer) == b"2026-"


def test_is_read_only(plain: Path) -> None:
    with LogFile(url=plain.as_uri()) as log:
        assert log.readable()
        assert not log.writable()
        with pytest.raises(OSError):
            log.write(b"nope")


# -- laziness and lifecycle -------------------------------------------------


def test_nothing_is_opened_until_first_read(plain: Path) -> None:
    log = LogFile(url=plain.as_uri())
    assert "_stream" not in log.__dict__
    log.read(1)
    assert "_stream" in log.__dict__
    log.close()


def test_stream_is_cached(plain: Path) -> None:
    with LogFile(url=plain.as_uri()) as log:
        assert log._stream is log._stream


def test_missing_file_only_fails_on_read(tmp_path: Path) -> None:
    log = LogFile(url=tmp_path.joinpath("absent.txt").as_uri())
    with pytest.raises(FileNotFoundError):
        log.read()


def test_close_does_not_open(tmp_path: Path) -> None:
    """Closing an unread log must not open anything -- __del__ takes this path."""
    log = LogFile(url=tmp_path.joinpath("absent.txt").as_uri())
    log.close()
    assert log.closed
    assert "_stream" not in log.__dict__


def test_close_is_idempotent(plain: Path) -> None:
    log = LogFile(url=plain.as_uri())
    log.read(1)
    log.close()
    log.close()
    assert log.closed


@pytest.mark.parametrize("operation", [lambda f: f.read(), lambda f: f.tell(), lambda f: f.seek(0)])
def test_use_after_close_raises(plain: Path, operation) -> None:
    log = LogFile(url=plain.as_uri())
    log.read(1)
    log.close()
    with pytest.raises(ValueError, match="closed file"):
        operation(log)


def test_repr_shows_url(plain: Path) -> None:
    log = LogFile(url=plain.as_uri())
    assert "app.txt" in repr(log)
    log.close()


def test_a_crlf_log_parses_identically(plain: Path, tmp_path: Path) -> None:
    """Windows-written captures must not leak carriage returns into messages."""
    crlf = tmp_path / "crlf.txt"
    crlf.write_bytes(SAMPLE_BYTES.replace(b"\n", b"\r\n"))
    with LogFile(url=plain.as_uri()) as a, LogFile(url=crlf.as_uri()) as b:
        left, right = a.into_arrow_table(), b.into_arrow_table()
    assert left.drop_columns("url").equals(right.drop_columns("url"))
