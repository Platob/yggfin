import gzip
from pathlib import Path

import pyarrow
import pyarrow.fs
import pytest

from rekep import Dataset, Field, Log, TextFile, TextFiles
from rekep.text import HEADER_PATTERN
from rekep.text.text_files import _natural

SAMPLE = Path(__file__).parent.parent / "data" / "app_sample.txt"
SAMPLE_BYTES = SAMPLE.read_bytes()

#: Derived from the sample, then pinned, so a regression in HEADER_PATTERN
#: cannot move both sides of an assertion together.
RECORDS = [line for line in SAMPLE_BYTES.split(b"\n") if HEADER_PATTERN.match(line)]
EXPECTED_RECORDS = 24

#: The files the `capture` fixture holds, in the order a walk must produce
#: them: digit runs compare as numbers, so `2` comes before `10`, and the
#: subdirectory sorts under its own name like any other entry.
CAPTURE_ORDER = (
    "app.1.txt.gz",
    "app.2.txt.gz",
    "app.10.txt.gz",
    "app.txt",
    "archive/old.txt",
)


def test_the_sample_is_what_the_tests_assume() -> None:
    assert len(RECORDS) == EXPECTED_RECORDS


@pytest.fixture
def capture(tmp_path: Path) -> Path:
    """A folder shaped like a real capture: a live log, rotations, an archive.

    The files are written in an order that is neither sorted nor reversed, so
    a walk that handed the store's own listing over would not match
    `CAPTURE_ORDER` by luck.
    """
    (tmp_path / "archive").mkdir()
    (tmp_path / "app.2.txt.gz").write_bytes(gzip.compress(SAMPLE_BYTES))
    (tmp_path / "app.txt").write_bytes(SAMPLE_BYTES)
    (tmp_path / "app.10.txt.gz").write_bytes(gzip.compress(SAMPLE_BYTES))
    (tmp_path / "archive" / "old.txt").write_bytes(SAMPLE_BYTES)
    (tmp_path / "app.1.txt.gz").write_bytes(gzip.compress(SAMPLE_BYTES))
    (tmp_path / "notes.json").write_text("{}")
    return tmp_path


def relative(files: TextFiles, root: Path) -> list[str]:
    """The set's paths, relative to the folder, so assertions read as names.

    `as_posix()`, not `str()`: `Url` spells every local path with forward
    slashes on either host, so a Windows `str(root)` is a prefix of none of
    them. Reducing against it silently handed back whole paths -- which
    compare against nothing, and turned one spelling difference into fifteen
    failures on the windows-latest leg. The reduction is asserted here for
    that reason: a helper that can quietly not reduce cannot be trusted by the
    assertions built on it.
    """
    base = root.as_posix()
    paths = []
    for url in files.into_urls():
        assert url.startswith(base), f"{url!r} is not under {base!r}, so nothing reduces it"
        paths.append(url[len(base) :].lstrip("/"))
    return paths


# -- ordering ---------------------------------------------------------------


def test_paths_come_out_in_natural_path_order(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    assert tuple(relative(files, capture)) == CAPTURE_ORDER


def test_a_digit_run_sorts_as_a_number_not_as_text() -> None:
    """The comparator itself, on a listing no filesystem is asked for.

    Sorting `app.10` before `app.9` is what a plain string sort does, so the
    fixture above cannot tell the two apart on its own if a store happens to
    answer in order.
    """
    scrambled = [
        pyarrow.fs.FileInfo(name) for name in ("a/app.10.txt", "a/app.2.txt", "a/app.9.txt")
    ]
    assert [info.path for info in sorted(scrambled, key=_natural)] == [
        "a/app.2.txt",
        "a/app.9.txt",
        "a/app.10.txt",
    ]
    assert sorted(info.path for info in scrambled) == ["a/app.10.txt", "a/app.2.txt", "a/app.9.txt"]


def test_zero_padded_siblings_have_an_order_of_their_own(tmp_path: Path) -> None:
    """`app01` and `app1` are one number: without a tiebreak the store decides."""
    for name in ("app1.txt", "app01.txt", "app001.txt"):
        (tmp_path / name).write_bytes(SAMPLE_BYTES)
    files = TextFiles.from_folder(tmp_path, pattern="*.txt")
    assert relative(files, tmp_path) == ["app001.txt", "app01.txt", "app1.txt"]
    keys = [_natural(pyarrow.fs.FileInfo(name)) for name in ("app1.txt", "app01.txt")]
    assert keys[0] != keys[1]


def test_reverse_reads_the_same_order_backwards(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*", reverse=True)
    assert tuple(relative(files, capture)) == tuple(reversed(CAPTURE_ORDER))


def test_reverse_turns_each_root_and_not_their_order(capture: Path) -> None:
    """The roots are the caller's statement about time; the flag is about the store."""
    roots = [capture / "archive", capture]
    forward = relative(TextFiles.from_folders(roots, pattern="*.txt*"), capture)
    backward = relative(TextFiles.from_folders(roots, pattern="*.txt*", reverse=True), capture)
    assert forward[0] == "archive/old.txt"
    assert backward[0] == "archive/old.txt", "the archive is still read first"
    assert backward[1:] == list(reversed(forward[1:]))


def test_roots_are_read_in_the_order_given(capture: Path) -> None:
    """A stated order is a statement about time, so it is never re-sorted."""
    files = TextFiles.from_folders([capture / "archive", capture], pattern="*.txt*")
    assert relative(files, capture)[0] == "archive/old.txt"


def test_recursive_false_stays_in_the_folder(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*", recursive=False)
    assert "archive/old.txt" not in relative(files, capture)
    assert len(relative(files, capture)) == 4


def test_a_name_with_an_odd_digit_still_sorts(tmp_path: Path) -> None:
    """`"²".isdigit()` is True and `\\d` does not match it -- one file took the walk down."""
    (tmp_path / "app.1\u00b22.txt").write_bytes(SAMPLE_BYTES)
    (tmp_path / "app.2.txt").write_bytes(SAMPLE_BYTES)
    files = TextFiles.from_folder(tmp_path, pattern="*.txt")
    assert len(list(files.into_urls())) == 2
    assert files.exists is True


def test_a_directory_is_walked_once(tmp_path: Path) -> None:
    """A symlink back up the tree costs the whole capture twice, or forever."""
    (tmp_path / "app.txt").write_bytes(SAMPLE_BYTES)
    (tmp_path / "loop").symlink_to(tmp_path, target_is_directory=True)
    files = TextFiles.from_folder(tmp_path, pattern="*.txt")
    assert len(list(files.into_urls())) == 1


# -- what is in the set -----------------------------------------------------


def test_the_pattern_filters_on_the_base_name(capture: Path) -> None:
    assert relative(TextFiles.from_folder(capture, pattern="*.gz"), capture) == [
        "app.1.txt.gz",
        "app.2.txt.gz",
        "app.10.txt.gz",
    ]
    assert "notes.json" in relative(TextFiles.from_folder(capture), capture)


def test_the_pattern_is_case_sensitive_on_every_platform(capture: Path) -> None:
    """`fnmatch` folds case on Windows only, so the set would differ by host."""
    assert relative(TextFiles.from_folder(capture, pattern="*.TXT"), capture) == []


def test_a_file_root_is_taken_as_it_is(capture: Path) -> None:
    """Naming a file *is* the selection, so the pattern does not second-guess it.

    And it is the same builder: a second one taking files rather than folders
    would be one name for the behaviour this already has.
    """
    files = TextFiles.from_folders([capture / "notes.json"], pattern="*.txt")
    assert relative(files, capture) == ["notes.json"]


def test_a_missing_root_is_refused(capture: Path) -> None:
    files = TextFiles.from_folders([capture, capture / "nowhere"])
    with pytest.raises(FileNotFoundError, match="nowhere"):
        list(files.into_urls())


def test_the_walk_is_lazy(capture: Path) -> None:
    """The first path arrives before a later root has even been looked at."""
    files = TextFiles.from_folders([capture, capture / "nowhere"], pattern="*.txt*")
    assert next(files.into_urls()).endswith("app.1.txt.gz")


def test_an_empty_set_reads_as_no_rows() -> None:
    files = TextFiles()
    assert files.exists is False
    table = files.into_arrow_table()
    assert table.num_rows == 0
    assert table.schema.equals(Log.FIELD.into_arrow_schema())


def test_a_folder_with_no_logs_does_not_exist_yet(tmp_path: Path) -> None:
    assert TextFiles.from_folder(tmp_path).exists is False
    (tmp_path / "app.txt").write_bytes(SAMPLE_BYTES)
    assert TextFiles.from_folder(tmp_path).exists is True


def test_a_missing_folder_does_not_exist_rather_than_raising(tmp_path: Path) -> None:
    """Asking whether it is there is the one question a missing root answers."""
    files = TextFiles.from_folder(tmp_path / "nowhere")
    assert files.exists is False
    with pytest.raises(FileNotFoundError):
        files.read_arrow_table()


# -- construction -----------------------------------------------------------


def test_from_folder_resolves_a_local_path_through_its_filesystem(capture: Path) -> None:
    """A root is rewritten as the path its filesystem understands, as a url is."""
    files = TextFiles.from_folder(capture)
    assert files.roots == (capture.as_posix(),)
    assert isinstance(files.filesystem, pyarrow.fs.LocalFileSystem)


def test_a_supplied_filesystem_resolves_nothing_but_still_spells_a_local_root(
    capture: Path,
) -> None:
    """The path stays the caller's; only its separators are this package's.

    `pyarrow.fs` answers a local listing with forward slashes on either host,
    so a root spelled `C:\\logs` is a prefix of none of the paths the set goes
    on to hold -- and everything that reduces one against the other silently
    stops reducing.
    """
    files = TextFiles.from_folder(str(capture), pyarrow.fs.LocalFileSystem())
    assert files.roots == (capture.as_posix(),)
    assert all(url.startswith(files.roots[0]) for url in files.into_urls())


def test_the_declaration_reaches_every_file(capture: Path) -> None:
    files = TextFiles.from_folder(
        capture, pattern="*.txt", timezone="Europe/Paris", static_values={"bridge": "bridge-1"}
    )
    for log in files.into_files():
        assert isinstance(log, TextFile)
        assert log.timezone == "Europe/Paris"
        assert log.static_values == {"bridge": "bridge-1"}
        assert log.filesystem is files.filesystem


def test_a_set_is_a_dataset(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    assert isinstance(files, Dataset)
    assert files.into_struct_field() is Log.FIELD
    assert files.read_arrow_table().num_rows == len(CAPTURE_ORDER) * EXPECTED_RECORDS


def test_from_redirects_on_the_source(capture: Path) -> None:
    assert TextFiles.from_(str(capture)).roots == (capture.as_posix(),)


@pytest.mark.parametrize(
    ("requested", "stem"),
    [
        (pyarrow.Table, "arrow_table"),
        (pyarrow.RecordBatchReader, "arrow_reader"),
        (pyarrow.RecordBatch, "arrow_batches"),
    ],
)
def test_into_redirects_on_the_requested_type(capture: Path, requested: type, stem: str) -> None:
    assert TextFiles.redirect_of(requested) == stem
    assert TextFiles.from_folder(capture, pattern="*.txt*").into_(requested) is not None


def test_read_arrow_redirects_on_the_requested_type(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    assert files.read_arrow(pyarrow.Table).num_rows == len(CAPTURE_ORDER) * EXPECTED_RECORDS
    assert files.read_arrow(pyarrow.RecordBatchReader).read_all().num_rows == (
        len(CAPTURE_ORDER) * EXPECTED_RECORDS
    )


def test_dispatch_refuses_what_it_cannot_infer(capture: Path) -> None:
    with pytest.raises(TypeError, match="cannot infer"):
        TextFiles.from_folder(capture).into_(object())


def test_static_values_are_the_sets_and_reach_every_file(capture: Path) -> None:
    """One capture is one shape: the columns are declared once, not per file."""
    files = TextFiles.from_folder(
        capture, pattern="*.txt*", static_values={"bridge": "bridge-1", "shard": 2}
    )
    assert files.into_struct_field().names[-2:] == ["bridge", "shard"]
    table = files.read_arrow_table()
    assert table.schema.names[-2:] == ["bridge", "shard"]
    assert set(table.column("bridge").to_pylist()) == {"bridge-1"}
    assert table.num_rows == len(CAPTURE_ORDER) * EXPECTED_RECORDS


# -- parsing ----------------------------------------------------------------


def test_every_record_of_every_file_is_parsed(capture: Path) -> None:
    table = TextFiles.from_folder(capture, pattern="*.txt*").into_arrow_table()
    assert table.num_rows == len(CAPTURE_ORDER) * EXPECTED_RECORDS
    assert table.schema.equals(Log.FIELD.into_arrow_schema())


def test_rows_stay_in_the_order_the_files_are_read(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    walked = list(TextFiles.from_folder(capture, pattern="*.txt*").into_urls())
    read = files.into_arrow_table().column("url").to_pylist()
    assert read[::EXPECTED_RECORDS] == walked

    # Every file's rows are contiguous: a set never interleaves two logs. Cut
    # the column into runs of equal url and compare those, rather than
    # sampling it every 24 rows -- which is true of any list of any order.
    runs = [
        read[start : start + EXPECTED_RECORDS] for start in range(0, len(read), EXPECTED_RECORDS)
    ]
    assert [set(run) for run in runs] == [{url} for url in walked]


def test_a_compressed_file_and_a_plain_one_read_the_same(capture: Path) -> None:
    plain = TextFiles.from_folders([capture / "app.txt"]).into_arrow_table()
    zipped = TextFiles.from_folders([capture / "app.1.txt.gz"]).into_arrow_table()
    assert plain.column("message").to_pylist() == zipped.column("message").to_pylist()


@pytest.mark.parametrize("batch_row_size", [1, 7, EXPECTED_RECORDS, 10_000])
def test_batching_does_not_change_the_result(capture: Path, batch_row_size: int) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    reader = files.into_arrow_reader(batch_row_size=batch_row_size)
    table = reader.read_all()
    assert table.num_rows == len(CAPTURE_ORDER) * EXPECTED_RECORDS
    assert (
        table.column("hash").to_pylist()
        == (
            TextFiles.from_folder(capture, pattern="*.txt*").into_arrow_table().column("hash")
        ).to_pylist()
    )


def test_short_files_are_combined_into_batches_of_the_size_asked_for(capture: Path) -> None:
    """Five files of 24 rows are not five batches: they are one of 120.

    Without the combining, every rotated log costs a batch of its own, and a
    folder of them is mostly rotated logs.
    """
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    sizes = [batch.num_rows for batch in files.into_arrow_reader(batch_row_size=100)]
    assert sizes == [120]
    assert [batch.num_rows for batch in files.into_arrow_reader(batch_row_size=40)] == [48, 48, 24]


def test_a_full_batch_is_handed_over_untouched(capture: Path) -> None:
    """A big log pays no copy: what `TextFile` produced is what comes out.

    Asserted on the buffer address, because the row counts are the same
    whether the batch was passed through or copied through `combine_chunks`.
    """
    files = TextFiles.from_folders([capture / "app.txt"])
    batches = list(files.into_arrow_batches(batch_row_size=10))
    assert [batch.num_rows for batch in batches] == [10, 10, 4]

    with TextFile.from_path(capture / "app.txt") as log:
        alone = list(log.into_arrow_batches(10))
    addresses = [batch.column("hash").buffers()[1].address for batch in batches]
    assert len(set(addresses)) == len(addresses), "no batch shares another's buffer"
    assert [batch.num_rows for batch in alone] == [10, 10, 4]


def test_continuations_do_not_fold_across_two_files(capture: Path) -> None:
    """The next file was written before or after this one, never inside it."""
    table = TextFiles.from_folder(capture, pattern="*.txt*").into_arrow_table()
    messages = table.column("message").to_pylist()
    assert len(messages) == len(CAPTURE_ORDER) * EXPECTED_RECORDS
    first_of_each = messages[::EXPECTED_RECORDS]
    assert all(message == first_of_each[0] for message in first_of_each)


def test_reading_casts_only_when_asked(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt")
    assert files.read_arrow_reader().schema.equals(Log.FIELD.into_arrow_schema())
    narrow = Field.from_arrow_schema(
        pyarrow.schema([("message", pyarrow.large_string())]), "Narrow"
    )
    table = files.read_arrow_table(narrow)
    assert table.schema.names == ["message"]
    assert table.schema.field("message").type == pyarrow.large_string()


def test_one_file_is_open_at_a_time(capture: Path) -> None:
    """A batch out of the first log must not have opened the other four."""

    class Counting(pyarrow.fs.LocalFileSystem):
        opened: list[str] = []
        listed: list[str] = []

        def open_input_file(self, path):  # noqa: ANN001, ANN202 - pyarrow's own signature
            Counting.opened.append(path)
            return super().open_input_file(path)

        def open_input_stream(self, path, compression="detect"):  # noqa: ANN001, ANN202
            Counting.opened.append(path)
            return super().open_input_stream(path, compression=compression)

        def get_file_info(self, source):  # noqa: ANN001, ANN202
            if isinstance(source, pyarrow.fs.FileSelector):
                Counting.listed.append(source.base_dir)
            return super().get_file_info(source)

    files = TextFiles.from_folder(str(capture), Counting(), pattern="*.txt*")
    reader = files.into_arrow_reader(batch_row_size=10)
    next(reader)
    assert Counting.opened == [(capture / "app.1.txt.gz").as_posix()]
    # And one listing, not a recursive walk of the tree, to get there.
    assert Counting.listed == [capture.as_posix()]
    reader.read_all()
    assert len(Counting.opened) == len(CAPTURE_ORDER)
    assert Counting.listed == [capture.as_posix(), (capture / "archive").as_posix()]


# -- the byte stream --------------------------------------------------------


def test_byte_chunks_are_every_file_in_order(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    assert files.into_bytes() == SAMPLE_BYTES * len(CAPTURE_ORDER)


def test_the_last_file_is_not_given_a_newline_it_did_not_have(tmp_path: Path) -> None:
    """A separator goes *between* two files; there is nothing after the last."""
    (tmp_path / "a.txt").write_bytes(RECORDS[0])
    files = TextFiles.from_folder(tmp_path)
    assert files.into_bytes() == RECORDS[0]


def test_a_file_without_a_trailing_newline_is_separated_from_the_next(tmp_path: Path) -> None:
    """Otherwise the last line of one log and the first of the next are one row."""
    (tmp_path / "a.txt").write_bytes(RECORDS[0])
    (tmp_path / "b.txt").write_bytes(RECORDS[1] + b"\n")
    files = TextFiles.from_folder(tmp_path)
    assert files.into_bytes() == RECORDS[0] + b"\n" + RECORDS[1] + b"\n"
    assert files.into_arrow_table().num_rows == 2


@pytest.mark.parametrize("read_byte_size", [1, 13, 1 << 20])
def test_the_read_size_does_not_change_the_bytes(capture: Path, read_byte_size: int) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    assert files.into_bytes(read_byte_size=read_byte_size) == SAMPLE_BYTES * len(CAPTURE_ORDER)


@pytest.mark.parametrize("compression", ["gzip", "zstd"])
def test_a_compressed_flow_decodes_back_to_the_raw_one(capture: Path, compression: str) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    blob = files.into_bytes(compression=compression)
    raw = files.into_bytes()
    assert len(blob) < len(raw)
    with pyarrow.CompressedInputStream(pyarrow.BufferReader(blob), compression) as stream:
        assert stream.read() == raw


def test_a_gzip_flow_is_one_member_the_stdlib_reads(capture: Path) -> None:
    """A `.gz` written here has to be a `.gz` everywhere, not an Arrow detail."""
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    assert gzip.decompress(files.into_bytes(compression="gzip")) == files.into_bytes()


def test_a_compressed_flow_arrives_in_pieces(tmp_path: Path) -> None:
    """It is a stream: the codec's output leaves while the input is still coming.

    The lines have to vary, and there have to be enough of them: the codec
    hands over a block at a time, so a capture whose *compressed* form fits in
    one block comes out in one piece however it was fed in -- which is exactly
    the case that cannot tell a streaming encoder from `Codec.compress`.
    """
    payload = b"".join(
        b"2026-08-14 00:00:%02d.%03d_%03d [t-%d] [ModuleFoo] (DEBUG) order %d filled at %d\n"
        % (row % 60, row % 1000, row % 997, row % 31, row, row * 7919)
        for row in range(60_000)
    )
    (tmp_path / "big.txt").write_bytes(payload)
    files = TextFiles.from_folder(tmp_path)
    chunks = list(files.into_byte_chunks(read_byte_size=1 << 16, compression="gzip"))
    assert len(chunks) > 1
    assert gzip.decompress(b"".join(chunks)) == payload


def test_read_serves_the_same_bytes_as_the_chunks(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    pieces = []
    while piece := files.read(7):
        pieces.append(piece)
    assert b"".join(pieces) == SAMPLE_BYTES * len(CAPTURE_ORDER)
    assert all(len(piece) == 7 for piece in pieces[:-1])


def test_read_everything(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    assert files.read() == SAMPLE_BYTES * len(CAPTURE_ORDER)
    assert files.read() == b""


def test_readinto(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    buffer = bytearray(27)
    assert files.readinto(buffer) == 27
    assert bytes(buffer) == SAMPLE_BYTES[:27]


def test_a_set_is_read_only(capture: Path) -> None:
    files = TextFiles.from_folder(capture)
    assert files.readable() is True
    assert files.writable() is False
    assert files.seekable() is False


# -- lifecycle --------------------------------------------------------------


def test_nothing_is_opened_until_the_first_read(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    assert "_byte_chunks" not in files.__dict__
    files.read(1)
    assert "_byte_chunks" in files.__dict__


def test_close_does_not_open(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    files.close()
    assert "_byte_chunks" not in files.__dict__
    assert files.closed is True


def test_close_is_idempotent(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    files.read(1)
    files.close()
    files.close()
    assert files.closed is True


def test_use_after_close_raises(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    files.close()
    with pytest.raises(ValueError, match="closed file"):
        files.read(1)


@pytest.mark.parametrize(
    "operation",
    [
        lambda files: files.read(1),
        lambda files: files.into_bytes(),
        lambda files: files.into_arrow_table(),
        lambda files: list(files.into_arrow_batches()),
        lambda files: list(files.into_byte_chunks()),
    ],
)
def test_both_faces_of_a_closed_set_refuse(capture: Path, operation) -> None:
    """Rows and bytes are two faces of one stream, and it is closed."""
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    files.close()
    with pytest.raises(ValueError, match="closed file"):
        operation(files)


def test_a_set_reads_twice(capture: Path) -> None:
    """Each read walks the store again, so a folder that grew is read again."""
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    first = files.into_arrow_table().num_rows
    (capture / "app.11.txt").write_bytes(SAMPLE_BYTES)
    assert files.into_arrow_table().num_rows == first + EXPECTED_RECORDS


# -- writing ----------------------------------------------------------------


def test_appending_to_a_set_is_refused_too(capture: Path) -> None:
    """And before the capture is parsed to build a key column nobody can use."""
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    batch = files.into_arrow_table().to_batches()[0]
    with pytest.raises(NotImplementedError, match="TextFile"):
        files.append_arrow(batch, merge_by=True)


def test_writing_a_set_is_refused(capture: Path) -> None:
    files = TextFiles.from_folder(capture, pattern="*.txt*")
    batch = files.into_arrow_table().to_batches()[0]
    with pytest.raises(NotImplementedError, match="TextFile"):
        files.write_arrow(batch)


def test_creating_a_set_adopts_the_shape_and_touches_nothing(tmp_path: Path) -> None:
    files = TextFiles.from_folder(tmp_path / "nothing-here")
    narrow = Field.from_arrow_schema(pyarrow.schema([("message", pyarrow.string())]), "Narrow")
    assert files.create_with(narrow) is files
    assert files.into_struct_field() == narrow
    assert not (tmp_path / "nothing-here").exists()
