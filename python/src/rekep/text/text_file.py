"""Text log files, read and written as Arrow."""

from __future__ import annotations

import datetime
import io
import os
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from functools import cache, cached_property
from types import MappingProxyType
from typing import Any

import pyarrow
import pyarrow.compute
import pyarrow.fs

from rekep.dataset import Dataset, arrow_chunks
from rekep.enums import MIC
from rekep.fields import Field, StructField
from rekep.fields.arrays import groups_of, scattered
from rekep.filesystems import resolve
from rekep.fix.fields import cast_arrow_fix
from rekep.fix.transcribe import FixCodec
from rekep.market.event import CODES_TYPE, HOUR
from rekep.market.identity import HASH, hash_bytes
from rekep.text.log import Log, LogRules, MessageCodec
from rekep.urls import Url

#: Matches the fixed header every log row opens with, leaving the free-form
#: payload to `message`::
#:
#:     2026-08-14 00:05:01.167_520 [77-e72:9ef:72503] [ModuleFoo] (DEBUG) Found code
#:     ^timestamp                  ^thread_name       ^driver_name ^level ^message
#:
#: `level` is optional -- some drivers print none -- and the fractional second
#: is **millis, and micros after them when the driver prints any**: the same
#: capture writes `01.147`, `01,147`, `01.147250` and `01.147_250`, because one
#: capture is written by several loggers and they do not agree. Matching is
#: done on bytes so lines never have to be decoded just to be classified; a
#: line that does not match is a wrapped continuation of the row above it.
HEADER_PATTERN = re.compile(
    rb"^[ \t]*"
    rb"(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[.,]\d{3}(?:[._,]?\d{3})?)[ \t]+"
    rb"\[(?P<thread_name>[^\]]*)\][ \t]+"
    rb"\[(?P<driver_name>[^\]]*)\][ \t]*"
    rb"(?:\((?P<level>[A-Za-z]{1,12})\)[ \t]*)?"
    rb"(?P<message>.*)$",
    re.DOTALL,
)

#: Every width `HEADER_PATTERN` pins a timestamp to: seconds and millis (23),
#: and those plus micros with or without a separator between them (26 or 27).
#:
#: The slicing path reads every component from a fixed offset, so it is sound
#: at these widths and at no other: a stamp one character shorter slices into
#: valid ISO holding *other digits* and casts happily to the wrong instant.
#: Anything else is read rather than sliced.
STAMP_WIDTHS = (23, 26, 27)

#: A timestamp split into "up to the seconds" and "everything after the first
#: separator", so a fraction written `167_520` or `167,520` can be put back
#: together as digits instead of as more separators.
_FRACTION = re.compile(r"^([^.,]*)([.,])?(.*)$")

#: The Arrow type a line's digest is, and the list of them a lineage would be.
#: Named here so the parser builds the empty ones without re-deriving the type.
PARENTS = pyarrow.list_(pyarrow.field("item", HASH, nullable=False))

#: Rows per record batch: memory is bounded by it, per-batch Arrow overhead is
#: amortised over it.
DEFAULT_BATCH_ROW_SIZE = 65_536

#: Bytes pulled from the stream per read. On an object store every read is one
#: ranged HTTP request, so this is also the request granularity: too small
#: floods the store with GETs, too large holds memory. 4 MiB suits both
#: S3-like and local reads.
DEFAULT_READ_BYTE_SIZE = 1 << 22

# Columns a line physically carries; the rest of the parsed row is derived.
_RENDERED = ("unix", "thread_name", "driver_name", "message")


@dataclass(eq=False)
class TextFile(Dataset, io.BufferedIOBase):
    """A text log addressed by URI: a dataset, and a readable binary stream."""

    @classmethod
    @cache
    def into_redirects(cls) -> Mapping[object, str]:
        """Conversions this file infers from its source or target."""
        return MappingProxyType(
            {
                pyarrow.RecordBatchReader: "arrow_reader",
                pyarrow.Table: "arrow_table",
                pyarrow.RecordBatch: "arrow_batches",
                str: "url",
                os.PathLike: "path",
            }
        )

    @classmethod
    @cache
    def into_row(cls) -> type[Log]:
        """Class whose declaration defines parsed rows."""
        return Log

    @classmethod
    @cache
    def into_kind(cls) -> str:
        """Document kind registered with `Dataset`."""
        return "text_file"

    url: str
    filesystem: pyarrow.fs.FileSystem | None = None
    header_pattern: re.Pattern[bytes] = HEADER_PATTERN

    #: Shape reads and writes land on. None is `into_row()`'s own -- what the parser
    #: fills -- and anything else is cast onto on the way out and in.
    row: StructField | None = None

    #: What decides each line's `etype`, tried in order, `UNKNOWN` when nothing
    #: matches. The default reads a FIX trading log; an empty `LogRules(rules=[])`
    #: skips the matching entirely and leaves every line `UNKNOWN`.
    rules: LogRules = dataclass_field(default_factory=LogRules)

    #: What turns a message into the columns a row carries: which category it
    #: is, its pairs, and the tags behind them. `FixCodec` reads a FIX-carrying
    #: trading log; another `MessageCodec` can plug in without changing the
    #: file pipeline.
    #:
    #: A codec whose rule set is empty categorises every line OTHER, which
    #: parses nothing -- so a file that declares no rules reads exactly as it
    #: did before any of this existed.
    codec: MessageCodec = dataclass_field(default_factory=FixCodec)

    #: IANA zone the wall clock in the header belongs to (`Europe/Paris`).
    #: None keeps the historical reading: the clock *is* UTC. Naming the real
    #: zone is what makes `unix` a true instant -- see
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
        `header_pattern`, `rules` -- is a keyword here, so a call reads as one
        shape.
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
        return parsed_field_of(self.into_row().into_field(), self.static_columns)

    def into_struct_field(self) -> StructField:
        """The shape this file holds: the declared one, or what the parser fills."""
        return self.row if self.row is not None else self.parsed_field

    @cached_property
    def rendered_field(self) -> StructField:
        """What a write has to carry: the header's own columns and the message."""
        parsed = self.parsed_field
        return Field.from_arrow_schema(
            pyarrow.schema([parsed.field(name).into_arrow_field() for name in _RENDERED]),
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
        """Append a stream to the file, one write per chunk, as text."""
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
        """Stream the log as Arrow record batches."""
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
            # A fallback digest here differed between environments, so one row
            # could be stored twice under two keys. `xxhash` is a hard dependency.
            hashes.append(hash_bytes(line))
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
        """One batch of parsed lines, as the `Event` columns a `Log` is.

        Assembled **by name** and then ordered by the schema, rather than as a
        positional list: a column added to `Log` then fails here by its own
        name instead of silently shifting every column after it into the wrong
        one. The dict costs twenty-odd entries per batch against sixty-five
        thousand rows.
        """
        timestamps, threads, drivers, messages = zip(*rows, strict=True)
        count = len(rows)
        local = _local_micros(timestamps)
        unix = _unix_nanos(local, self.timezone)
        message = _utf8(messages)
        # `hash` identifies the raw line. `xhash` starts there too, then moves
        # to the parsed lifecycle when the message supplies a readable key.
        digest = pyarrow.array(hashes, type=HASH)
        columns: dict[str, Any] = {
            "unix": unix,
            "unix_hour": _hour_nanos(unix),
            "etype": self.rules.etype_arrow(message),
            # A line is created when it is stamped. `runix` is when somebody
            # wrote it down *here*, which the parser does not know and must not
            # invent: a clock read at parse time would make the same file parse
            # into different rows every run.
            "cunix": unix,
            "runix": _zeros(count, pyarrow.int64()),
            "eunix": pyarrow.nulls(count, pyarrow.int64()),
            "sunix": pyarrow.nulls(count, pyarrow.int64()),
            "hash": digest,
            "xhash": digest,
            "version": _zeros(count, pyarrow.int64()),
            "state": _zeros(count, pyarrow.int32()),
            "code": pyarrow.repeat("", count),
            "codes": pyarrow.repeat(pyarrow.scalar({}, CODES_TYPE), count),
            "prev_unix": pyarrow.nulls(count, pyarrow.int64()),
            "parent_hash": pyarrow.nulls(count, PARENTS),
            "mic": pyarrow.nulls(count, pyarrow.int32()),
            "reason": pyarrow.nulls(count, pyarrow.string()),
            "url": pyarrow.repeat(self.url, count),
            "thread_name": _utf8(threads),
            "driver_name": _utf8(drivers),
            "message": message,
        }
        for name, column in self._message_columns(message, columns["driver_name"], count).items():
            if name in columns:
                column = pyarrow.compute.coalesce(
                    cast_arrow_fix(column, columns[name].type), columns[name]
                )
            columns[name] = column
        columns["mic"] = _mic_arrow(columns, message, count)
        columns["reason"] = columns.get("text", columns["reason"])
        columns.update(
            (name, pyarrow.repeat(scalar, count)) for name, scalar in self.static_columns
        )
        schema = self.schema
        linked_events = schema.field("linked_events")
        columns.setdefault(
            "linked_events", pyarrow.repeat(pyarrow.scalar([], type=linked_events.type), count)
        )
        missing_required = [
            field.name for field in schema if field.name not in columns and not field.nullable
        ]
        if missing_required:
            raise ValueError(f"parser did not produce required columns {missing_required}")
        for field in schema:
            if field.name not in columns:
                columns[field.name] = pyarrow.nulls(count, field.type)
        row = self.into_row()
        columns["symbol"] = row.symbol_arrow(columns, count)
        columns["code"] = row.code_arrow(columns, count)
        linked = pyarrow.compute.not_equal(columns["code"], "")
        linked_count = int(pyarrow.compute.sum(linked).as_py() or 0)
        if linked_count:
            selected = (
                columns["code"]
                if linked_count == count
                else pyarrow.compute.filter(columns["code"], linked)
            )
            hashes = row.hash_arrow(selected)
            columns["xhash"] = (
                hashes
                if linked_count == count
                else pyarrow.compute.replace_with_mask(digest, linked, hashes)
            )
        # `cast_arrow_fix` and not a plain cast, because the session columns
        # arrive as the text the wire carried: `20260814-09:30:00.123` is an
        # instant and `Y` is a boolean, and Arrow's own cast raises on both.
        return pyarrow.RecordBatch.from_arrays(
            [cast_arrow_fix(columns[name], schema.field(name).type) for name in schema.names],
            schema=schema,
        )

    def _message_columns(self, messages: Any, drivers: Any, count: int) -> dict[str, Any]:
        """What a message fills: which protocol it is, its fields, its columns.

        A batch mixes protocols and dictionary versions, and both are read per
        row; the codec is handed one homogeneous slice at a time and the slices
        are scattered back into the batch's own order.
        """
        del count
        compute = pyarrow.compute
        protocols = self.codec.categorise(messages, drivers)
        parts = []
        for name, slice_, where in _grouped(protocols, messages):
            pairs = self.codec.into_pairs(slice_, name.as_py())
            versions = compute.fill_null(self.codec.versions_of_pairs(pairs), "")
            for version, read, inner in _grouped(versions, pairs):
                rows = where if len(inner) == len(where) else compute.take(where, inner)
                parts.append((self.codec.into_log_columns(read, version.as_py() or None), rows))
        return {"protocol": protocols, **_scattered_columns(parts)}

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
    """One chunk of parsed rows back as log lines, in string kernels only."""
    if rows.num_rows == 0:
        return b""
    compute = pyarrow.compute
    micros = compute.divide(rows.column("unix"), 1000).cast(pyarrow.int64())
    if timezone and os.name == "nt":
        stamps = _windows_local_micros(micros, timezone).cast(pyarrow.timestamp("us"))
    else:
        stamps = micros.cast(pyarrow.timestamp("us"))
    if timezone and os.name != "nt":
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
    """`row`, with each static column appended after the data columns."""
    if not static_columns:
        return row
    schema = row.into_arrow_schema()
    for name, scalar in static_columns:
        if name in schema.names:
            raise ValueError(
                f"static value {name!r} is already a column of {row.name}; name it something else"
            )
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
    """One batch of raw header timestamps to a naive `timestamp("us")` column."""
    compute = pyarrow.compute
    raw = pyarrow.array(timestamps, type=pyarrow.binary()).cast(pyarrow.string())
    lengths = compute.utf8_length(raw)
    for width in STAMP_WIDTHS:
        if not compute.all(compute.equal(lengths, width), min_count=0).as_py():
            continue
        try:
            return _sliced_micros(raw, width).cast(pyarrow.timestamp("us"))
        except pyarrow.ArrowInvalid:
            break
    micros = [_epoch_nanos(stamp) // 1000 for stamp in timestamps]
    return pyarrow.array(micros, type=pyarrow.int64()).cast(pyarrow.timestamp("us"))


def _sliced_micros(raw: pyarrow.Array, width: int) -> pyarrow.Array:
    """A column of fixed-width stamps as canonical ISO microseconds.

    One canonical spelling out of all of them, so the cast that follows has
    one shape to parse: `YYYY-MM-DD HH:MM:SS.ffffff`. Where the stamp carries
    no micros the field is filled with the literal zeros that make millis
    micros; where it carries them with a separator the separator is at 23 and
    the digits after it, and where it carries them without one they are at 23
    already.
    """
    compute = pyarrow.compute
    fraction = (
        pyarrow.scalar("000")
        if width == 23
        else compute.utf8_slice_codeunits(raw, width - 3, width)
    )
    return compute.binary_join_element_wise(
        compute.utf8_slice_codeunits(raw, 0, 10),
        " ",
        compute.utf8_slice_codeunits(raw, 11, 19),
        ".",
        compute.utf8_slice_codeunits(raw, 20, 23),
        fraction,
        "",
    )


def _unix_nanos(local: pyarrow.Array, timezone: str | None) -> pyarrow.Array:
    """The wall clock as an instant: int64 nanoseconds since the epoch."""
    if timezone and os.name == "nt":
        return pyarrow.compute.multiply(_windows_utc_micros(local, timezone), 1000)
    if timezone:
        local = pyarrow.compute.assume_timezone(
            local, timezone, ambiguous="earliest", nonexistent="latest"
        )
    return pyarrow.compute.multiply(local.cast(pyarrow.int64()), 1000)


@cache
def _timezone_transitions(
    timezone: str, first_year: int, last_year: int
) -> tuple[int, tuple[tuple[int, int, int], ...]]:
    """Offsets and local transition instants from Python's bundled Windows tzdata."""
    from zoneinfo import ZoneInfo

    zone = ZoneInfo(timezone)
    start = datetime.datetime(first_year, 1, 1)
    end = (
        datetime.datetime(last_year + 1, 1, 1)
        if last_year < datetime.MAXYEAR
        else datetime.datetime.max
    )

    def offset(value: datetime.datetime) -> int:
        found = value.replace(tzinfo=zone, fold=0).utcoffset()
        if found is None:  # pragma: no cover - ZoneInfo always supplies one
            raise ValueError(f"{timezone!r} has no UTC offset at {value}")
        return (found.days * 86_400 + found.seconds) * 1_000_000 + found.microseconds

    current = offset(start)
    initial = current
    transitions: list[tuple[int, int, int]] = []
    left = start
    step = datetime.timedelta(hours=1)
    while left < end:
        right = min(left + step, end)
        changed = offset(right)
        if changed != current:
            low, high = left, right
            while high - low > datetime.timedelta(microseconds=1):
                middle = low + (high - low) // 2
                if offset(middle) == current:
                    low = middle
                else:
                    high = middle
            boundary = _datetime_micros(high)
            transitions.append((boundary, current, changed))
            current = changed
        left = right
    return initial, tuple(transitions)


def _windows_utc_micros(local: pyarrow.Array, timezone: str) -> pyarrow.Array:
    """Vectorized local-to-UTC conversion where Arrow's Windows IANA lookup can abort."""
    compute = pyarrow.compute
    if not len(local) or local.null_count == len(local):
        return local.cast(pyarrow.int64())
    bounds = compute.min_max(local).as_py()
    first, last = bounds["min"], bounds["max"]
    initial, transitions = _timezone_transitions(timezone, first.year, last.year)
    micros = local.cast(pyarrow.int64())
    offsets = pyarrow.repeat(pyarrow.scalar(initial, pyarrow.int64()), len(local))
    for boundary, _, changed in transitions:
        offsets = compute.if_else(
            compute.greater_equal(micros, boundary),
            pyarrow.scalar(changed, pyarrow.int64()),
            offsets,
        )
    utc = compute.subtract(micros, offsets)
    # Arrow's `nonexistent="latest"` clamps every wall time in a forward gap
    # to its first valid instant. A simple offset subtraction would shift it.
    for boundary, previous, changed in transitions:
        if changed <= previous:
            continue
        gap_start = boundary - (changed - previous)
        missing = compute.and_(
            compute.greater_equal(micros, gap_start), compute.less(micros, boundary)
        )
        utc = compute.if_else(missing, pyarrow.scalar(boundary - changed), utc)
    return utc


def _windows_local_micros(utc: Any, timezone: str) -> pyarrow.Array:
    """Vectorized UTC-to-local conversion without Arrow's incomplete Windows mapping."""
    compute = pyarrow.compute
    if isinstance(utc, pyarrow.ChunkedArray):
        utc = utc.combine_chunks()
    if not len(utc) or utc.null_count == len(utc):
        return utc.cast(pyarrow.int64())
    bounds = compute.min_max(utc.cast(pyarrow.timestamp("us"))).as_py()
    first = max(1, bounds["min"].year - 1)
    last = min(datetime.MAXYEAR, bounds["max"].year + 1)
    initial, transitions = _timezone_transitions(timezone, first, last)
    offsets = pyarrow.repeat(pyarrow.scalar(initial, pyarrow.int64()), len(utc))
    for boundary, _, changed in transitions:
        # `boundary` is the first local wall time with the new offset; this is
        # the UTC instant at which that offset starts.
        offsets = compute.if_else(
            compute.greater_equal(utc, boundary - changed),
            pyarrow.scalar(changed, pyarrow.int64()),
            offsets,
        )
    return compute.add(utc, offsets)


def _datetime_micros(value: datetime.datetime) -> int:
    """A naive datetime as exact microseconds since the Unix epoch."""
    delta = value - datetime.datetime(1970, 1, 1)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _hour_nanos(unix: pyarrow.Array) -> pyarrow.Array:
    """`unix` truncated down to the hour it falls in -- the partition column."""
    compute = pyarrow.compute
    hour = pyarrow.scalar(HOUR, pyarrow.int64())
    remainder = compute.subtract(unix, compute.multiply(compute.divide(unix, hour), hour))
    return compute.subtract(
        unix,
        compute.if_else(compute.less(remainder, 0), compute.add(remainder, hour), remainder),
    )


def _row_indices(count: int) -> pyarrow.Array:
    """`0..count-1`, built in kernels -- where a scatter puts each row back."""
    ones = pyarrow.repeat(pyarrow.scalar(1, pyarrow.int32()), count)
    return pyarrow.compute.subtract(
        pyarrow.compute.cumulative_sum(ones), pyarrow.scalar(1, pyarrow.int32())
    )


def _mic_arrow(columns: Mapping[str, Any], messages: Any, rows: int) -> Any:
    """ISO exchange fields, then direction-aware FIX session endpoints."""
    compute = pyarrow.compute
    missing = pyarrow.nulls(rows, pyarrow.string())
    stored = columns.get("kwargs")
    tags = _stored_tag_arrows(stored, (30, 100, 275, 1301), rows) if stored is not None else {}
    explicit = [
        tags.get(30, missing),
        columns.get("security_exchange", missing),
        tags.get(100, missing),
        tags.get(275, missing),
        tags.get(1301, missing),
    ]
    explicit = [value for value in explicit if value.null_count < rows]
    venue = MIC.arrow_from_strings(*explicit) if explicit else pyarrow.nulls(rows, pyarrow.int32())
    sender_source = columns.get("sender_comp_id", missing)
    target_source = columns.get("target_comp_id", missing)
    sender = (
        MIC.arrow_from_strings(sender_source)
        if sender_source.null_count < rows
        else pyarrow.nulls(rows, pyarrow.int32())
    )
    target = (
        MIC.arrow_from_strings(target_source)
        if target_source.null_count < rows
        else pyarrow.nulls(rows, pyarrow.int32())
    )
    if sender.null_count == rows:
        return compute.coalesce(venue, target)
    if target.null_count == rows:
        return compute.coalesce(venue, sender)
    text = messages.cast(pyarrow.string(), safe=False)
    outbound = compute.fill_null(
        compute.match_substring_regex(text, r"(?i)\b(?:send|sending|sent|outbound)\b"), False
    )
    inbound = compute.fill_null(
        compute.match_substring_regex(text, r"(?i)\b(?:recv|receive|received|receiving|inbound)\b"),
        False,
    )
    directed = compute.if_else(
        outbound,
        target,
        compute.if_else(inbound, sender, pyarrow.nulls(rows, pyarrow.int32())),
    )
    return compute.coalesce(venue, directed, target, sender)


def _stored_tag_arrows(stored: Any, wanted: Sequence[int], rows: int) -> dict[int, Any]:
    """First value of each wanted residual FIX tag, found in one list scan."""
    if isinstance(stored, pyarrow.ChunkedArray):
        stored = stored.combine_chunks()
    if not rows or stored.null_count == rows:
        return {}
    compute = pyarrow.compute
    parents = compute.list_parent_indices(stored).cast(pyarrow.int32())
    entries = compute.list_flatten(stored)
    keys = compute.struct_field(entries, "tag")
    values = compute.struct_field(entries, "value")
    matches = compute.fill_null(
        compute.is_in(keys, value_set=pyarrow.array(wanted, keys.type)), False
    )
    if not compute.any(matches, min_count=0).as_py():
        return {}
    matched_keys = compute.filter(keys, matches)
    matched_parents = compute.filter(parents, matches)
    matched_values = compute.filter(values, matches)
    row_ids = _row_indices(rows)
    found = {}
    for tag in compute.unique(matched_keys).to_pylist():
        at = compute.equal(matched_keys, tag)
        where = compute.filter(matched_parents, at)
        values = compute.filter(matched_values, at)
        found[tag] = compute.take(values, compute.index_in(row_ids, value_set=where))
    return found


def _grouped(keys: pyarrow.Array, values: Any) -> Iterator[tuple[Any, Any, pyarrow.Array]]:
    """`(key, the rows carrying it, where in the column they were)`.

    One group takes nothing: its positions are the identity permutation, and a
    batch of one protocol at one dictionary version -- which is nearly every
    batch of a real capture -- then pays no `take` at all.
    """
    for key, where in groups_of(keys):
        yield (
            key,
            (values if len(where) == len(keys) else pyarrow.compute.take(values, where)),
            where,
        )


def _scattered_columns(
    parts: Sequence[tuple[tuple[Any, Mapping[str, Any]], Any]],
) -> dict[str, Any]:
    """Every slice's columns back in the batch's own row order.

    Every slice answers with the same columns -- a projection fills the ones
    its protocol and version had nothing for with nulls of the right type --
    so the parts concatenate and one sort puts every row back where it was. A
    column no slice produced at all is `_batch`'s to fill, like any other the
    schema declares and a line does not carry.
    """
    positions = [where for _, where in parts]
    lifted = [columns for (_, columns), _ in parts]
    return {
        "kwargs": scattered([kwargs for (kwargs, _), _ in parts], positions),
        **{
            name: scattered([part[name] for part in lifted], positions)
            for name in (lifted[0] if lifted else ())
        },
    }


def _zeros(count: int, arrow_type: pyarrow.DataType) -> pyarrow.Array:
    """A column of `count` zeros -- the envelope members a parsed line leaves unset.

    Zero and not null, because they are NOT NULL columns: what a log line does
    not have is stated, so a store never has to widen a column for it later,
    and a value repeated down a whole file encodes away to nothing on disk.
    """
    return pyarrow.repeat(pyarrow.scalar(0, arrow_type), count)


_EPOCH_DATETIME = datetime.datetime(1970, 1, 1)  # noqa: DTZ001 - log timestamps are naive UTC


def _epoch_nanos(timestamp: bytes) -> int:
    """`2026-08-14 00:05:01.167_520` -> nanoseconds since the epoch, naive UTC."""
    text = timestamp.decode("utf-8", "replace")
    head, separator, fraction = _FRACTION.match(text).groups()
    if separator:
        text = f"{head}.{re.sub(r'[._,]', '', fraction)}"
    delta = datetime.datetime.fromisoformat(text) - _EPOCH_DATETIME
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000
