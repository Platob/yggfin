"""Text log files, read and written as Arrow."""

from __future__ import annotations

import datetime
import io
import os
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from functools import cached_property
from typing import Any, ClassVar

import pyarrow
import pyarrow.compute
import pyarrow.fs
import xxhash

from rekep.dataset import Dataset, arrow_chunks
from rekep.fields import Field, StructField
from rekep.filesystems import resolve
from rekep.logs.log import Log
from rekep.urls import Url

#: Matches the fixed header every log row opens with, leaving the free-form
#: payload to `message`::
#:
#:     2026-08-14 00:05:01.167_520 [77-e72:9ef:72503] [ModuleFoo] (DEBUG) Found code
#:     ^timestamp                  ^thread_name       ^driver_name ^level ^message
#:
#: `level` is optional -- some drivers print none -- and the fractional second
#: carries millis and micros separated by an underscore. Matching is done on
#: bytes so lines never have to be decoded just to be classified; a line that
#: does not match is a wrapped continuation of the row above it.
HEADER_PATTERN = re.compile(
    rb"^[ \t]*"
    rb"(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[.,]\d{3}[._,]\d{3})[ \t]+"
    rb"\[(?P<thread_name>[^\]]*)\][ \t]+"
    rb"\[(?P<driver_name>[^\]]*)\][ \t]*"
    rb"(?:\((?P<level>[A-Za-z]{1,12})\)[ \t]*)?"
    rb"(?P<message>.*)$",
    re.DOTALL,
)

#: Characters `HEADER_PATTERN` pins a timestamp to. The slicing path is sound
#: only at this width, so anything else is read rather than sliced.
STAMP_WIDTH = 27

#: A timestamp split into "up to the seconds" and "everything after the first
#: separator", so a fraction written `167_520` or `167,520` can be put back
#: together as digits instead of as more separators.
_FRACTION = re.compile(r"^([^.,]*)([.,])?(.*)$")

#: Seed every line hash is taken under. Fixed and module-level: `h64` is
#: half of the primary key, so a seed that moved between runs -- Python's own
#: salted `hash()`, a per-process random -- would store the same line twice.
HASH_SEED = 0x9E3779B185EBCA87

#: Rows per record batch: memory is bounded by it, per-batch Arrow overhead is
#: amortised over it.
DEFAULT_BATCH_ROW_SIZE = 65_536

#: Bytes pulled from the stream per read. On an object store every read is one
#: ranged HTTP request, so this is also the request granularity: too small
#: floods the store with GETs, too large holds memory. 4 MiB suits both
#: S3-like and local reads.
DEFAULT_READ_BYTE_SIZE = 1 << 22


@dataclass(eq=False)
class TextFile(Dataset, io.BufferedIOBase):
    """A text log addressed by URI: a dataset, and a readable binary stream.

        Reading parses the lines into Arrow batches; writing renders batches back
        into lines, in Arrow string kernels rather than a loop, so a log is a
        dataset like any other -- `read_arrow_table()`, `write_arrow(batches)` --
        while staying a plain file underneath.

        Reading works wherever `pyarrow.fs` reaches, object stores included.
    **Writing needs a filesystem that can append**, which S3 and GCS cannot:
    they have no append, only a whole-object put, and reading a log back to
    rewrite it is what a log is least able to afford. Write to a local path
    and upload, or write to a dataset that owns its own files.

    `filesystem` is optional: when omitted it is resolved from `url` at
        construction -- cached, so an object store's credential chain is not
        re-walked per file -- while a caller holding a configured filesystem
        passes it in and has `url` treated as a path on it. Either way `url` is
        rewritten in `__post_init__` to the path its filesystem understands, so
        the two fields always agree and `url` can be used directly as the source
        column.

        Opening stays lazy -- construction resolves the filesystem but touches no
        data; the first read opens the stream. Arrow infers compression from the
        path extension, so `app.txt.gz` and `app.txt.zst` decode transparently.
        Uncompressed logs are opened for random access and are seekable; compressed
        ones can only be read forward.
    """

    REDIRECTS: ClassVar[dict[object, str]] = {
        pyarrow.RecordBatchReader: "arrow_reader",
        pyarrow.Table: "arrow_table",
        pyarrow.RecordBatch: "arrow_batches",
        str: "url",
        os.PathLike: "path",
    }

    #: Class whose fields define the parsed columns. Override it, and the
    #: schema, the descriptions and the column order all follow.
    ROW: ClassVar[type[Log]] = Log

    url: str
    filesystem: pyarrow.fs.FileSystem | None = None
    header_pattern: re.Pattern[bytes] = HEADER_PATTERN

    #: Columns one written line is made of. The rest of `ROW` is derived when
    #: the line is read back -- the day and the hash are functions of the line,
    #: the url is the file, the categories are placeholders -- so a write must
    #: not demand them.
    RENDERED: ClassVar[tuple[str, ...]] = (
        "recorded_at_unix",
        "thread_name",
        "driver_name",
        "message",
    )

    #: Shape reads and writes land on. None is `ROW`'s own -- what the parser
    #: fills -- and anything else is cast onto on the way out and in.
    row: StructField | None = None

    #: IANA zone the wall clock in the header belongs to (`Europe/Paris`).
    #: None keeps the historical reading: the clock *is* UTC. Naming the real
    #: zone is what makes `recorded_at_unix` a true instant -- see
    #: `_unix_nanos`.
    timezone: str | None = None

    #: Constant columns every parsed row carries, appended **after** the data
    #: columns in the order they are given here -- the bridge that wrote the
    #: capture, the desk, the environment, whatever the file itself never says.
    #: Nothing here is hardcoded: a source names its own columns.
    #:
    #: A plain Python value has its Arrow type inferred; a `pyarrow.Scalar`
    #: states it (`pyarrow.scalar("bridge-1", pyarrow.large_string())`), which
    #: is also the only way to say "null, of this type".
    static_values: Mapping[str, Any] = dataclass_field(default_factory=dict)

    def __post_init__(self) -> None:
        """Resolve the filesystem, and rewrite `url` as a path on it."""
        if self.filesystem is None:
            self.filesystem, self.url = resolve(self.url)

    # -- building -----------------------------------------------------------

    @classmethod
    def from_url(
        cls,
        url: str,
        filesystem: pyarrow.fs.FileSystem | None = None,
        *,
        timezone: str | None = None,
        **declared: Any,
    ) -> TextFile:
        """Build from a URI, or from a path when `filesystem` is given.

        Anything else the file declares -- `static_values`, `row`,
        `header_pattern`, `id_epoch_ms` -- is a keyword here, so a call reads
        as one shape.
        """
        return cls(url=url, filesystem=filesystem, timezone=timezone, **declared)

    @classmethod
    def from_path(
        cls,
        path: str | os.PathLike[str],
        filesystem: pyarrow.fs.FileSystem | None = None,
        *,
        timezone: str | None = None,
        **declared: Any,
    ) -> TextFile:
        """Build from a local path, absolute or relative.

        Takes `timezone` like `from_url` does: a local log is the one most
        likely to be in local time, and the alternative was a documented
        example that raised `TypeError`.
        """
        if filesystem is not None:
            return cls(url=os.fspath(path), filesystem=filesystem, timezone=timezone, **declared)
        return cls(url=Url.from_path(path).into_string(), timezone=timezone, **declared)

    # -- the dataset ---------------------------------------------------------

    @cached_property
    def static_columns(self) -> tuple[tuple[str, pyarrow.Scalar], ...]:
        """Each static value as an Arrow scalar, in the order it was declared."""
        return static_columns_of(self.static_values)

    @cached_property
    def parsed_field(self) -> StructField:
        """What the parser produces: the row shape, then the constant columns."""
        return parsed_field_of(self.ROW.FIELD, self.static_columns)

    def into_struct_field(self) -> StructField:
        """The shape this file holds: the declared one, or what the parser fills."""
        return self.row if self.row is not None else self.parsed_field

    @cached_property
    def rendered_field(self) -> StructField:
        """What a write has to carry: the header's own columns and the message."""
        parsed = self.parsed_field
        return Field.from_arrow_schema(
            pyarrow.schema([parsed.field(name).into_arrow_field() for name in self.RENDERED]),
            parsed.name,
        )

    @cached_property
    def schema(self) -> pyarrow.Schema:
        """Arrow schema of the parsed rows, projected from `ROW`.

        Cached because `_batch` reads it once per batch: building a schema is
        cheap, but not free, and it cannot change while the file is open.
        """
        return self.parsed_field.into_arrow_schema()

    @property
    def exists(self) -> bool:
        """Whether the file is there yet."""
        return self.filesystem.get_file_info(self.url).type != pyarrow.fs.FileType.NotFound

    def create_with_field(self, field: StructField, **kwargs: Any) -> TextFile:
        """Adopt `field` as this file's shape and make sure the file is there.

        Creating an empty log is writing nothing to it, so this only has to
        touch the file -- and remember the shape, which is what later reads
        are cast onto.
        """
        self.row = field
        if not self.exists:
            with self.filesystem.open_output_stream(self.url) as stream:
                stream.write(b"")
        return self

    def read_arrow_reader(self, schema: Any = None, **kwargs: Any) -> pyarrow.RecordBatchReader:
        """Parse the file, cast onto `schema` when one is asked for.

        With none, the reader is the parser's own -- see `into_arrow_reader`
        for the parsing options, which are passed straight through.
        """
        reader = self.into_arrow_reader(**kwargs)
        target = self.target_field(schema)
        if target.arrow_schema.equals(reader.schema):
            return reader
        return target.cast_arrow_reader(reader)

    def write_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
    ) -> None:
        """Append a stream to the file, one write per chunk, as text.

        The rows are cast onto the columns a line is made of -- the timestamp,
        the two bracketed fields and the message -- and rendered back into the
        header layout, so a file written here parses back into the same rows.
        Everything else `ROW` declares is derived on the way in, so a write is
        never asked for a day, a hash or a url it would only recompute.

        `merge_by` has no meaning for a text file: there is nothing to match a
        row against, so asking for one is refused rather than quietly appending.
        """
        if merge_by:
            raise ValueError(
                f"{type(self).__name__} appends lines and cannot merge on {merge_by!r}; "
                "write to a dataset that can, or drop merge_by"
            )
        self.get_or_create()
        # With no schema named, the rendered columns are the only shape a write
        # has to satisfy: casting onto the whole row first would demand the
        # very columns reading derives.
        stream = source if schema is None else self.target_field(schema).cast_arrow_reader(source)
        reader = self.rendered_field.cast_arrow_reader(stream)
        for chunk in arrow_chunks(reader, commit_row_size):
            self._append(_rendered(chunk, self.timezone))

    def _append(self, payload: bytes) -> None:
        """Add already-rendered bytes to the end of the file.

        Appending is the whole of writing here -- a log is a file you add lines
        to -- and an object store cannot do it: S3 and GCS have no append, only
        a whole-object put. Reading one back to rewrite it is what a log is
        least able to afford, so the refusal is passed on with the two things
        that do work in its place.
        """
        if not payload:
            return
        try:
            stream = self.filesystem.open_append_stream(self.url)
        except pyarrow.ArrowNotImplementedError as error:
            raise NotImplementedError(
                f"{self.filesystem.type_name} cannot append, and a log is written by appending; "
                "write to a local path and upload the file, or write to a dataset that owns its "
                "own files (IcebergDataset)"
            ) from error
        with stream:
            stream.write(payload)

    # -- converting ---------------------------------------------------------

    def into_arrow_reader(
        self,
        *,
        batch_row_size: int = DEFAULT_BATCH_ROW_SIZE,
        read_byte_size: int = DEFAULT_READ_BYTE_SIZE,
        fold_continuations: bool = True,
    ) -> pyarrow.RecordBatchReader:
        """Stream the log as Arrow record batches.

        Nothing is materialised whole. Decompression happens in Arrow's C++
        layer, lines are cut out of `read_byte_size` reads, and each field is
        accumulated as raw bytes and handed to Arrow once per batch --
        timestamps included, which are converted to nanoseconds by Arrow
        compute rather than parsed row by row in Python.

        `fold_continuations` appends lines that do not match `header_pattern`
        -- wrapped payloads, stack traces -- to the preceding row's message,
        which keeps a multi-line exception one record instead of many dropped
        ones.

        The reader takes over this log's stream: consume the reader, not `self`
        -- and only one reader at a time, because they share that stream.

        Each call reopens it, so reading the same log twice reads the log
        twice. Reopening rather than seeking, because a seek to zero on a
        compressed stream rewinds the *decoded* position and a file that has
        grown since -- which a write through this same object does -- would
        still be read from its old end.
        """
        self._check_open()
        self.__dict__.pop("_stream", None)
        return pyarrow.RecordBatchReader.from_batches(
            self.schema,
            self.into_arrow_batches(batch_row_size, read_byte_size, fold_continuations),
        )

    def into_arrow_table(self, **kwargs: object) -> pyarrow.Table:
        """Read the whole log into one Arrow table. Needs it to fit in memory."""
        return self.into_arrow_reader(**kwargs).read_all()  # type: ignore[arg-type]

    def into_arrow_batches(
        self,
        batch_row_size: int = DEFAULT_BATCH_ROW_SIZE,
        read_byte_size: int = DEFAULT_READ_BYTE_SIZE,
        fold_continuations: bool = True,
    ) -> Iterator[pyarrow.RecordBatch]:
        """Yield one record batch per `batch_row_size` parsed lines.

        The row loop is deliberately spartan -- profiling puts it, not Arrow,
        on the critical path. Groups come out in one `group(...)` call against
        indices resolved once, and land as one tuple append; everything
        columnar happens once per batch in `_batch`.
        """
        groups = self.header_pattern.groupindex
        indices = tuple(
            groups[name] for name in ("timestamp", "thread_name", "driver_name", "message")
        )
        rows: list[tuple[bytes, bytes | None, bytes | None, bytes | None]] = []
        hashes: list[int] = []
        match_header = self.header_pattern.match

        for line in self._iter_lines(read_byte_size):
            match = match_header(line)
            if match is None:
                if fold_continuations and rows:
                    timestamp, thread, driver, message = rows[-1]
                    rows[-1] = (timestamp, thread, driver, (message or b"") + b"\n" + line)
                continue
            rows.append(match.group(*indices))
            hashes.append(_hash64(line))
            # One row past the size, not at it: a continuation belongs to the
            # row above it, and cutting the batch the moment that row is
            # complete puts it out of reach of the next line. A stack trace
            # that happens to land on the boundary would be dropped, silently,
            # at any batch size -- including the default one.
            if len(rows) > batch_row_size:
                yield self._batch(rows[:batch_row_size], hashes[:batch_row_size])
                del rows[:batch_row_size], hashes[:batch_row_size]
        if rows:
            yield self._batch(rows, hashes)

    def _batch(self, rows: list[tuple], hashes: list[int]) -> pyarrow.RecordBatch:
        timestamps, threads, drivers, messages = zip(*rows, strict=True)
        local = _local_micros(timestamps)
        unix = _unix_nanos(local, self.timezone)
        date, time = _date_and_time(local)
        return pyarrow.RecordBatch.from_arrays(
            [
                pyarrow.repeat(self.url, len(rows)),
                unix,
                date,
                time,
                _utf8(threads),
                _utf8(drivers),
                # The category placeholders: zero and empty, never null, so a
                # store keeps the columns NOT NULL for the categoriser to fill.
                pyarrow.repeat(pyarrow.scalar(0, pyarrow.int32()), len(rows)),
                pyarrow.repeat("", len(rows)),
                _utf8(messages),
                pyarrow.array(hashes, type=pyarrow.int64()),
                *(pyarrow.repeat(scalar, len(rows)) for _, scalar in self.static_columns),
            ],
            schema=self.schema,
        )

    def _iter_lines(self, read_byte_size: int) -> Iterator[bytes]:
        """Cut newline-delimited lines out of fixed-size reads.

        One trailing carriage return is dropped per line, so a CRLF log parses
        identically to an LF one; a carriage return anywhere else is payload.
        """
        tail = b""
        while chunk := self.read(read_byte_size):
            lines = (tail + chunk).split(b"\n")
            tail = lines.pop()
            for line in lines:
                yield line.removesuffix(b"\r")
        if tail:
            yield tail.removesuffix(b"\r")

    # -- opening ------------------------------------------------------------

    def _open(self) -> pyarrow.NativeFile:
        """Open a new Arrow stream over `url`.

        Plain logs go through `open_input_file` because it is the only opener
        that yields a seekable handle; compressed ones must go through
        `open_input_stream`, which is the only one that decodes.
        """
        filesystem = self.filesystem
        if filesystem is None:  # pragma: no cover - established in __post_init__
            raise RuntimeError("filesystem was not resolved")
        if self._codec is None:
            return filesystem.open_input_file(self.url)
        return filesystem.open_input_stream(self.url, compression=self._codec)

    @cached_property
    def _codec(self) -> str | None:
        """Compression implied by the path extension, or None when plain.

        Arrow owns the extension-to-codec mapping; it signals "no codec for
        this suffix" by raising rather than by returning None.
        """
        try:
            return pyarrow.Codec.detect(self.url).name
        except (TypeError, ValueError, pyarrow.ArrowException):
            return None

    @cached_property
    def _stream(self) -> pyarrow.NativeFile:
        """This log's stream, opened on first use."""
        return self._open()

    # -- io.BufferedIOBase --------------------------------------------------

    def readable(self) -> bool:
        return True

    def read(self, size: int | None = -1) -> bytes:
        self._check_open()
        return self._stream.read(_nbytes(size))

    def read1(self, size: int = -1) -> bytes:
        return self.read(size)

    def readinto(self, buffer: bytearray | memoryview) -> int:
        self._check_open()
        return self._stream.readinto(buffer)

    def readinto1(self, buffer: bytearray | memoryview) -> int:
        return self.readinto(buffer)

    def seekable(self) -> bool:
        self._check_open()
        return self._stream.seekable()

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        self._check_open()
        return self._stream.seek(offset, whence)

    def tell(self) -> int:
        self._check_open()
        return self._stream.tell()

    def close(self) -> None:
        """Close the stream if one was ever opened, without opening one."""
        stream = self.__dict__.pop("_stream", None)
        if stream is not None:
            stream.close()
        super().close()

    def _check_open(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file.")


def _rendered(rows: pyarrow.Table, timezone: str | None = None) -> bytes:
    """One chunk of parsed rows back as log lines, in string kernels only.

    The inverse of the header regex, and deliberately built the same way: the
    timestamp is formatted by Arrow (`strftime` prints microseconds for a
    `us` column), the underscore between millis and micros is one slice
    insertion, the parts are joined column-wise, and the whole chunk is joined
    into a single blob by wrapping it in a one-row list. Nothing here runs per
    row in Python -- which is what makes writing a log as cheap as reading it.

    `timezone` is the inverse of the one reading assumed. `recorded_at_unix` is an
    instant, and a log line is a wall clock, so rendering the instant as UTC
    would move every stamp by the offset -- and by twice it on the next round
    trip, since reading would then add the offset back. Naming the zone here
    is what makes "a file written here parses back into the same rows" true
    of a zoned log and not only of a naive one. Both casts are metadata: an
    Arrow timestamp is the same integer in any zone.
    """
    if rows.num_rows == 0:
        return b""
    compute = pyarrow.compute
    stamps = compute.divide(rows.column("recorded_at_unix"), 1000).cast(pyarrow.timestamp("us"))
    if timezone:
        stamps = stamps.cast(pyarrow.timestamp("us", "UTC")).cast(pyarrow.timestamp("us", timezone))
    stamps = compute.strftime(stamps, format="%Y-%m-%d %H:%M:%S")
    stamps = compute.utf8_replace_slice(stamps, start=23, stop=23, replacement="_")
    lines = compute.binary_join_element_wise(
        stamps.cast(pyarrow.string()),
        " [",
        rows.column("thread_name").cast(pyarrow.string()),
        "] [",
        rows.column("driver_name").cast(pyarrow.string()),
        "] ",
        rows.column("message").cast(pyarrow.string()),
        "",
    )
    flat = lines.combine_chunks() if isinstance(lines, pyarrow.ChunkedArray) else lines
    whole = pyarrow.ListArray.from_arrays(
        pyarrow.array([0, len(flat)], pyarrow.int32()),
        flat.combine_chunks() if isinstance(flat, pyarrow.ChunkedArray) else flat,
    )
    return compute.binary_join(whole, "\n")[0].as_py().encode() + b"\n"


def parsed_field_of(
    row: StructField, static_columns: Sequence[tuple[str, pyarrow.Scalar]]
) -> StructField:
    """`row`, with each static column appended after the data columns.

    At the **end**, in insertion order, so adding one never moves a column a
    reader already selects by position and the data columns keep the order the
    declaration gives them. Shared with `TextFiles`, whose shape is the shape
    of the files it opens.
    """
    if not static_columns:
        return row
    schema = row.into_arrow_schema()
    for name, scalar in static_columns:
        schema = schema.append(pyarrow.field(name, scalar.type, nullable=not scalar.is_valid))
    return Field.from_arrow_schema(schema, row.name)


def static_columns_of(static_values: Mapping[str, Any]) -> tuple[tuple[str, pyarrow.Scalar], ...]:
    """Each static value as an Arrow scalar, in the order it was declared."""
    return tuple((name, _scalar(name, value)) for name, value in static_values.items())


def _scalar(name: str, value: Any) -> pyarrow.Scalar:
    """A static value as an Arrow scalar, its type inferred unless it says one.

    A bare `None` is refused: it would infer Arrow's `null` type, which no
    store can widen later, and the caller who meant "unknown, but a string"
    has a way to say it -- `pyarrow.scalar(None, pyarrow.string())`.
    """
    if isinstance(value, pyarrow.Scalar):
        return value
    if value is None:
        raise ValueError(
            f"static value {name!r} is None, which has no Arrow type; say which one it is with "
            "pyarrow.scalar(None, pyarrow.string())"
        )
    try:
        return pyarrow.scalar(value)
    except (pyarrow.ArrowInvalid, pyarrow.ArrowTypeError, TypeError) as error:
        raise TypeError(
            f"static value {name!r} ({type(value).__name__}) has no Arrow type; pass a "
            "pyarrow.scalar(...) that states one"
        ) from error


def _nbytes(size: int | None) -> int | None:
    """Translate the io convention for "read everything" to Arrow's."""
    return None if size is None or size < 0 else size


def _utf8(values: Sequence[bytes | None]) -> pyarrow.Array:
    """Cast raw bytes to a string array, falling back when the payload is dirty."""
    array = pyarrow.array(values, type=pyarrow.binary())
    try:
        return array.cast(pyarrow.string())
    except pyarrow.ArrowInvalid:
        return pyarrow.array(
            [None if v is None else v.decode("utf-8", "replace") for v in values],
            type=pyarrow.string(),
        )


def _local_micros(timestamps: Sequence[bytes]) -> pyarrow.Array:
    """One batch of raw header timestamps to a naive `timestamp("us")` column.

    The wall clock exactly as the line wrote it, no zone applied yet -- which
    is the only honest intermediate, since the line itself does not say what
    zone it is in.

    The bundled header regex pins every component to a fixed offset, so the
    separators -- `T` or space, `.` or `,`, and the `_` between millis and
    micros -- are never even read: the components are sliced out and joined
    into canonical ISO form, and one cast parses the whole column.

    That is only sound for the width the bundled pattern pins,
    `STAMP_WIDTH` characters. A custom `header_pattern` matching anything else
    is not caught by the cast -- `2026-08-14 00:05:01.167520`, one character
    shorter, slices into valid ISO holding *other digits* and casts happily to
    `.167200` -- so the width is checked first and anything else goes row by
    row. Measured on the bundled shape the check is one `utf8_length` pass per
    batch, under a percent.
    """
    compute = pyarrow.compute
    raw = pyarrow.array(timestamps, type=pyarrow.binary()).cast(pyarrow.string())
    fixed = compute.all(compute.equal(compute.utf8_length(raw), STAMP_WIDTH), min_count=0).as_py()
    if fixed:
        joined = compute.binary_join_element_wise(
            compute.utf8_slice_codeunits(raw, 0, 10),
            " ",
            compute.utf8_slice_codeunits(raw, 11, 19),
            ".",
            compute.utf8_slice_codeunits(raw, 20, 23),
            compute.utf8_slice_codeunits(raw, 24, 27),
            "",
        )
        try:
            return joined.cast(pyarrow.timestamp("us"))
        except pyarrow.ArrowInvalid:
            pass
    micros = [_epoch_nanos(stamp) // 1000 for stamp in timestamps]
    return pyarrow.array(micros, type=pyarrow.int64()).cast(pyarrow.timestamp("us"))


def _unix_nanos(local: pyarrow.Array, timezone: str | None) -> pyarrow.Array:
    """The wall clock as an instant: int64 nanoseconds since the epoch.

    `timezone` is the whole point. A log writes local time and says nothing
    about which local, so reading it as UTC is a guess that is wrong by the
    offset -- an hour or nine, silently, and differently either side of a DST
    change. Naming the zone turns the same characters into a real instant,
    for one `assume_timezone` kernel per batch (about 1% end to end); leaving
    it None keeps the older reading rather than inventing a zone.

    `ambiguous`/`nonexistent` default to `earliest`/`latest` rather than
    pyarrow's `raise`: a DST transition is a property of the calendar, not a
    defect in the log, and a parser that dies once a year on an hour that
    repeats is worse than one that picks the first of the two. The cost is
    that `recorded_at_unix` is not monotonic across a fall-back hour, true of
    the underlying reality too.

    The `int64` cast after it is a reinterpret, not a conversion: an Arrow
    timestamp is already microseconds since the epoch in its storage.
    """
    if timezone:
        local = pyarrow.compute.assume_timezone(
            local, timezone, ambiguous="earliest", nonexistent="latest"
        )
    return pyarrow.compute.multiply(local.cast(pyarrow.int64()), 1000)


def _date_and_time(local: pyarrow.Array) -> tuple[pyarrow.Array, pyarrow.Array]:
    """Split the wall clock into a date32 day and a time64 time of day.

    Denormalised at parse time -- two casts per batch in Arrow -- so the
    partition column exists in the data instead of every reader re-deriving
    it.

    Taken from the **local** clock, not from `recorded_at_unix`: these columns are
    what the line says, and a line stamped `2026-08-14 00:05` belongs to the
    14th for whoever wrote it, whatever instant that was in UTC. `recorded_at_unix` is
    the column that answers "when", `date` and `time` answer "what did the
    log say" -- and a partition on the local day is the one an operator can
    reason about.
    """
    return local.cast(pyarrow.date32()), local.cast(pyarrow.time64("us"))


_EPOCH = datetime.date(1970, 1, 1)
_EPOCH_DATETIME = datetime.datetime(1970, 1, 1)  # noqa: DTZ001 - log timestamps are naive UTC
_DAY_SECONDS: dict[bytes, int] = {}


def _epoch_nanos(timestamp: bytes) -> int:
    """`2026-08-14 00:05:01.167_520` -> nanoseconds since the epoch, naive UTC.

    Per-row fallback for batches the Arrow path will not take. Sliced rather
    than parsed: the bundled header regex has already pinned every field to a
    fixed offset. The date half is cached because a log covers few distinct
    days but many rows. Any other width goes to `_epoch_nanos_slow`, which
    reads the string rather than assuming where its parts are.
    """
    if len(timestamp) != STAMP_WIDTH:
        return _epoch_nanos_slow(timestamp)
    try:
        day = timestamp[:10]
        seconds = _DAY_SECONDS.get(day)
        if seconds is None:
            date = datetime.date(int(day[0:4]), int(day[5:7]), int(day[8:10]))
            seconds = (date - _EPOCH).days * 86_400
            _DAY_SECONDS[day] = seconds
        hours, minutes = int(timestamp[11:13]), int(timestamp[14:16])
        seconds += hours * 3_600 + minutes * 60 + int(timestamp[17:19])
        millis, micros = int(timestamp[20:23]), int(timestamp[24:27])
        return seconds * 1_000_000_000 + millis * 1_000_000 + micros * 1_000
    except (ValueError, IndexError):
        return _epoch_nanos_slow(timestamp)


def _epoch_nanos_slow(timestamp: bytes) -> int:
    """Fallback for timestamps a custom `header_pattern` shapes differently.

    The fraction's separators are *removed*, not replaced: the bundled pattern
    itself admits `01,167,520`, and turning each comma into a dot builds
    `01.167.520`, which no ISO parser takes. One dot goes back in front of
    whatever digits followed the seconds.
    """
    text = timestamp.decode("utf-8", "replace")
    head, separator, fraction = _FRACTION.match(text).groups()
    if separator:
        text = f"{head}.{re.sub(r'[._,]', '', fraction)}"
    delta = datetime.datetime.fromisoformat(text) - _EPOCH_DATETIME
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _hash64(raw: bytes) -> int:
    """The line's hash, signed so an Arrow `int64` column holds it.

    xxh3 under a fixed seed, and only that. It is half of the primary key, so
    it is data rather than an implementation detail: a fallback hash -- which
    is what this used to have -- made `h64` stable within an environment and
    not across two that disagreed about whether `xxhash` was installed, and a
    row keyed on it could be stored twice. `xxhash` is a dependency of this
    package for that reason, imported at module top with no guard.
    """
    value = xxhash.xxh3_64_intdigest(raw, seed=HASH_SEED)
    return value - (1 << 64) if value >= (1 << 63) else value
