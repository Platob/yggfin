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
from rekep.enums import EventType
from rekep.fields import Field, StructField
from rekep.fields.arrays import groups_of, scattered
from rekep.filesystems import resolve
from rekep.market.event import CODES_TYPE, unix_partition_arrow
from rekep.market.identity import HASH
from rekep.text.message import Message
from rekep.times import COMPACT, SHAPES, Stamp
from rekep.urls import Url

#: Every spelling of an instant a header may open with, as one alternation.
#: Derived from `rekep.times.SHAPES` rather than restated: the set of accepted
#: spellings is one behavior, and a shape this reader admitted and `times` did
#: not would be a stamp a window could not name.
_TIMESTAMP = "|".join(f"(?:{stamp.pattern})" for stamp in SHAPES)

#: Matches the fixed header every log row opens with, leaving the free-form
#: payload to `message`::
#:
#:     2026-08-14 00:05:01.167_520 [77-e72:9ef:72503] [ModuleFoo] (DEBUG) Found code
#:     ^timestamp                  ^thread_name       ^plugin_code ^level ^message
#:
#: `level` is optional -- some plugins print none -- and the fraction is one
#: to nine digits or absent: the same capture writes `01.147`, `01,147`,
#: `01.147250` and `01.147_250`, because one capture is written by several
#: loggers and they do not agree. Beside that ISO spelling a header may open
#: with FIX's own `20260824-10:00:01.123` or with a compact
#: `20260824100001123`. Matching is done on bytes so lines never have to be
#: decoded just to be classified; a line that does not match is a wrapped
#: continuation of the row above it.
HEADER_PATTERN = re.compile(
    rb"^[ \t]*"
    rb"(?P<timestamp>" + _TIMESTAMP.encode() + rb")[ \t]+"
    rb"\[(?P<thread_name>[^\]]*)\][ \t]+"
    rb"\[(?P<plugin_code>[^\]]*)\][ \t]*"
    rb"(?:\((?P<level>[A-Za-z]{1,12})\)[ \t]*)?"
    rb"(?P<message>.*)$",
    re.DOTALL,
)

#: Which shape a column of stamps is, by the characters only that shape writes
#: at those offsets: ISO writes a `-` at 4 where the other two write a digit,
#: and FIX writes one at 8 where compact writes a digit. Compact is told by
#: both absences rather than by nothing at all -- a column holding a FIX stamp
#: and a compact one of the same width matches neither shape whole, and has to
#: be grouped rather than sliced as whichever was tried last.
#:
#: A width alone never decides, because three of them are shared: 17 is a FIX
#: stamp and a compact one with millis, 23 an ISO stamp with millis and a
#: compact one with nanos, 27 an ISO stamp with a split fraction and a FIX one
#: with nanos. The slicing path reads every component from a fixed offset, so
#: a stamp read as the wrong shape slices into valid ISO holding *other
#: digits* and casts happily to the wrong instant.
_SHAPE_MARKS: Mapping[str, tuple[tuple[int, str, bool], ...]] = MappingProxyType(
    {
        "iso": ((4, "-", True),),
        "fix": ((4, "-", False), (8, "-", True)),
        "compact": ((4, "-", False), (8, "-", False)),
    }
)

#: One integer per shape, so a `(shape, width)` pair packs into one key a
#: single grouping pass can take.
_SHAPE_CODES: Mapping[str, int] = MappingProxyType(
    {stamp.name: code for code, stamp in enumerate(SHAPES)}
)
_SHAPES_BY_CODE: Mapping[int, Stamp] = MappingProxyType(dict(enumerate(SHAPES)))

#: Every width the declared shapes can be sliced at -- which a stamp of a width
#: not here still reads correctly through, because a shape's offsets hold
#: whatever its fraction is.
STAMP_WIDTHS: tuple[int, ...] = tuple(sorted({width for stamp in SHAPES for width in stamp.widths}))

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
_RENDERED = ("unix", "thread_name", "plugin_code", "message")


def compiled_header(source: re.Pattern[bytes] | str | bytes) -> re.Pattern[bytes]:
    """A header pattern however a job spelled it: compiled, or already compiled.

    Bytes, because a line is classified before it is decoded -- a `str` source
    is a document's spelling of the same pattern and is encoded here.
    """
    if isinstance(source, re.Pattern):
        return source
    return re.compile(source.encode() if isinstance(source, str) else source, re.DOTALL)


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
    def into_row(cls) -> type[Message]:
        """Class whose declaration defines parsed rows."""
        return Message

    @classmethod
    @cache
    def into_kind(cls) -> str:
        """Document kind registered with `Dataset`."""
        return "text_file"

    url: str
    filesystem: pyarrow.fs.FileSystem | None = None

    #: What a line's fixed header is, as `HEADER_PATTERN` documents one. A job
    #: configures its own by handing over the pattern source: a `str` or
    #: `bytes` is compiled here, so a log whose header this package has never
    #: seen is a document change and not a code change. It must name the same
    #: groups -- `timestamp`, `thread_name`, `plugin_code`, `message`.
    header_pattern: re.Pattern[bytes] | str | bytes = HEADER_PATTERN

    #: Shape reads and writes land on. None is `into_row()`'s own -- what the parser
    #: fills -- and anything else is cast onto on the way out and in.
    row: StructField | None = None

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
        """Compile a configured header, resolve the filesystem, rewrite `url` on it."""
        self.header_pattern = compiled_header(self.header_pattern)
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
        `header_pattern` -- is a keyword here, so a call reads as one shape.
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

    def read_arrow_reader(
        self,
        schema: Any = None,
        *,
        exclude_plugins: Sequence[str] = (),
        **kwargs: Any,
    ) -> pyarrow.RecordBatchReader:
        """Parse the file, cast onto `schema` when one is asked for.

        With none, the reader is the parser's own -- see `into_arrow_reader`
        for the parsing options, which are passed straight through.
        """
        reader = self.into_arrow_reader(exclude_plugins=exclude_plugins, **kwargs)
        target = self.target_field(schema)
        if target.arrow_schema.equals(reader.schema):
            return reader
        return target.cast_arrow_reader(reader)

    def overwrite_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] = True,
        commit_row_size: int | None = None,
    ) -> None:
        """Refused: a log is appended to, and has no key to replace a line by.

        An overwrite replaces the rows whose keys match, which needs stored
        rows addressable by key -- a text file is a sequence of lines. Use
        `append_arrow_*`, or a store that owns its own files
        (`IcebergDataset`).
        """
        raise NotImplementedError(
            f"{type(self).__name__} cannot overwrite: a log is a sequence of lines with no "
            "key to replace one by; use append_arrow_* to add lines"
        )

    def append_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """Add lines to the file, and refuse to skip the ones it already holds.

        The generic insert-only append reads the stored keys to anti-join
        against them, which here means parsing the whole capture on every
        write -- and a log has no key by which a line is the same line.
        """
        if merge_by:
            raise ValueError(
                f"{type(self).__name__} appends lines and cannot merge on {merge_by!r}; "
                "append to a dataset that can, or drop merge_by"
            )
        return super().append_arrow_reader(source, schema, merge_by, commit_row_size, **kwargs)

    def _append_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        commit_row_size: int | None = None,
    ) -> None:
        """Add every row to the file, one write per chunk, as text."""
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
        exclude_plugins: Sequence[str] = (),
    ) -> pyarrow.RecordBatchReader:
        """Stream the log as Arrow record batches, omitting exact plugin codes."""
        self._check_open()
        self.__dict__.pop("_stream", None)
        return pyarrow.RecordBatchReader.from_batches(
            self.schema,
            self.into_arrow_batches(
                batch_row_size, read_byte_size, fold_continuations, exclude_plugins
            ),
        )

    def into_arrow_table(self, **kwargs: object) -> pyarrow.Table:
        """Read the whole log into one Arrow table. Needs it to fit in memory."""
        return self.into_arrow_reader(**kwargs).read_all()  # type: ignore[arg-type]

    def into_arrow_batches(
        self,
        batch_row_size: int = DEFAULT_BATCH_ROW_SIZE,
        read_byte_size: int = DEFAULT_READ_BYTE_SIZE,
        fold_continuations: bool = True,
        exclude_plugins: Sequence[str] = (),
    ) -> Iterator[pyarrow.RecordBatch]:
        """Yield one record batch per `batch_row_size` parsed lines.

        The row loop is deliberately spartan -- profiling puts it, not Arrow,
        on the critical path. Groups come out in one `group(...)` call against
        indices resolved once, and land as one tuple append; everything
        columnar happens once per batch in `_batch`. Plugin exclusions are
        exact and case-sensitive.
        """
        excluded = _excluded_plugins(exclude_plugins)
        groups = self.header_pattern.groupindex
        indices = tuple(
            groups[name] for name in ("timestamp", "thread_name", "plugin_code", "message")
        )
        rows: list[tuple[bytes, bytes | None, bytes | None, bytes | None]] = []
        # Physical lines, not parsed rows: a folded continuation must not shift
        # the number every row after it reports.
        rownums: list[int] = []
        rownum = 0
        match_header = self.header_pattern.match

        for line in self._iter_lines(read_byte_size):
            rownum += 1
            match = match_header(line)
            if match is None:
                if fold_continuations and rows:
                    timestamp, thread, plugin, message = rows[-1]
                    rows[-1] = (timestamp, thread, plugin, (message or b"") + b"\n" + line)
                continue
            rows.append(match.group(*indices))
            rownums.append(rownum)
            # One row past the size, not at it: a continuation belongs to the
            # row above it, and cutting the batch the moment that row is
            # complete puts it out of reach of the next line. A stack trace
            # that happens to land on the boundary would be dropped, silently,
            # at any batch size -- including the default one.
            if len(rows) > batch_row_size:
                batch = self._batch(rows[:batch_row_size], rownums[:batch_row_size], excluded)
                if batch.num_rows:
                    yield batch
                del rows[:batch_row_size], rownums[:batch_row_size]
        if rows:
            batch = self._batch(rows, rownums, excluded)
            if batch.num_rows:
                yield batch

    def _batch(
        self,
        rows: list[tuple],
        rownums: list[int],
        excluded_plugins: pyarrow.Array | None = None,
    ) -> pyarrow.RecordBatch:
        """One batch of parsed headers and uninterpreted payloads.

        Assembled **by name** and then ordered by the schema, rather than as a
        positional list: a column added to `Message` then fails here by its own
        name instead of silently shifting every column after it into the wrong
        one.
        """
        timestamps, threads, plugins, messages = zip(*rows, strict=True)
        count = len(rows)
        local = _local_micros(timestamps)
        unix = _unix_nanos(local, self.timezone)
        message = pyarrow.compute.fill_null(_utf8(messages), "")
        columns: dict[str, Any] = {
            "unix": unix,
            "unix_partition": unix_partition_arrow(unix),
            "etype": pyarrow.repeat(pyarrow.scalar(int(EventType.UNKNOWN), pyarrow.int32()), count),
            "cunix": unix,
            "runix": unix,
            "eunix": pyarrow.nulls(count, pyarrow.int64()),
            "sunix": pyarrow.nulls(count, pyarrow.int64()),
            "version": _zeros(count, pyarrow.int64()),
            "state": _zeros(count, pyarrow.int32()),
            "code": pyarrow.repeat("", count),
            "codes": pyarrow.repeat(pyarrow.scalar({}, CODES_TYPE), count),
            "prev_unix": pyarrow.nulls(count, pyarrow.int64()),
            "parent_hash": pyarrow.nulls(count, PARENTS),
            "mic": pyarrow.nulls(count, pyarrow.int32()),
            "reason": pyarrow.nulls(count, pyarrow.string()),
            "source_url": pyarrow.repeat(self.url, count),
            "source_rownum": pyarrow.array(rownums, type=pyarrow.int64()),
            "thread_name": pyarrow.compute.fill_null(_utf8(threads), ""),
            "plugin_code": pyarrow.compute.fill_null(_utf8(plugins), ""),
            "message": message,
        }
        columns.update(
            (name, pyarrow.repeat(scalar, count)) for name, scalar in self.static_columns
        )
        if excluded_plugins is not None:
            keep = pyarrow.compute.invert(
                pyarrow.compute.is_in(columns["plugin_code"], value_set=excluded_plugins)
            )
            columns = {
                name: pyarrow.compute.filter(column, keep) for name, column in columns.items()
            }
            count = len(columns["plugin_code"])
        schema = self.schema
        if not count:
            return pyarrow.RecordBatch.from_arrays(
                [pyarrow.nulls(0, field.type) for field in schema], schema=schema
            )
        # `Message.identified` fills these once every raw column is here.
        for name in ("hash", "xhash"):
            columns.setdefault(name, pyarrow.nulls(count, schema.field(name).type))
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
        return self.into_row().identified(columns, schema, count)

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
        rows.column("plugin_code").cast(pyarrow.string()),
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


def _excluded_plugins(values: Sequence[str]) -> pyarrow.Array | None:
    """Exact plugin codes a reader omits, as one Arrow lookup set."""
    values = _exclude_plugin_codes(values)
    return pyarrow.array(values, type=pyarrow.string()) if values else None


def _exclude_plugin_codes(values: Sequence[str]) -> tuple[str, ...]:
    """One unambiguous sequence of exact plugin codes."""
    if isinstance(values, str):
        raise TypeError("exclude_plugins must be a sequence of plugin codes, not a string")
    return tuple(values)


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

    Sliced, never read a row at a time. A batch of one shape at one width --
    which is nearly every batch, because a file is written by one logger --
    is decided by two character comparisons and sliced whole; only a batch
    that actually mixes shapes or widths pays to be grouped.
    """
    compute = pyarrow.compute
    raw = pyarrow.array(timestamps, type=pyarrow.binary()).cast(pyarrow.string())
    if not len(raw):
        return raw.cast(pyarrow.timestamp("us"), safe=False)
    lengths = compute.utf8_length(raw)
    bounds = compute.min_max(lengths).as_py()
    if bounds["min"] == bounds["max"]:
        width = int(bounds["min"])
        for stamp in SHAPES:
            if _is_shape(raw, stamp):
                return _sliced_micros(raw, stamp, width).cast(pyarrow.timestamp("us"), safe=False)
    parts, positions = [], []
    for key, where in groups_of(_stamp_keys(raw, lengths)):
        stamp, width = _stamp_shape(key.as_py())
        sliced = _sliced_micros(compute.take(raw, where), stamp, width)
        parts.append(sliced.cast(pyarrow.timestamp("us"), safe=False))
        positions.append(where)
    return scattered(parts, positions)


def _is_shape(raw: pyarrow.Array, stamp: Stamp) -> bool:
    """Whether *every* row of a column is this shape rather than another of its width.

    A mark the shape writes has to hold on every row, and one it never writes
    on none of them -- `all` for the first and `any` for the second, because
    "not every row has a dash here" is not "no row does". A column holding a
    FIX stamp and a compact one, which share three widths, satisfies neither
    shape whole and is grouped instead.
    """
    compute = pyarrow.compute
    for at, character, wanted in _SHAPE_MARKS[stamp.name]:
        found = compute.fill_null(
            compute.equal(compute.utf8_slice_codeunits(raw, at, at + 1), character), False
        )
        settled = compute.all if wanted else compute.any
        if bool(settled(found, min_count=0).as_py()) is not wanted:
            return False
    return True


def _stamp_keys(raw: pyarrow.Array, lengths: pyarrow.Array) -> pyarrow.Array:
    """One `(shape, width)` key per row, in kernels.

    The same two comparisons `_is_shape` makes over a whole column, made per
    row instead, and packed with the width into one integer -- which is what a
    single grouping pass takes.
    """
    compute = pyarrow.compute
    found = pyarrow.repeat(pyarrow.scalar(_SHAPE_CODES[COMPACT.name], pyarrow.int32()), len(raw))
    for name in ("fix", "iso"):
        at, character, _ = _SHAPE_MARKS[name][-1]
        marked = compute.equal(compute.utf8_slice_codeunits(raw, at, at + 1), character)
        found = compute.if_else(
            compute.fill_null(marked, False),
            pyarrow.scalar(_SHAPE_CODES[name], pyarrow.int32()),
            found,
        )
    return compute.add(
        compute.multiply(found, pyarrow.scalar(1 << 8, pyarrow.int32())),
        lengths.cast(pyarrow.int32()),
    )


def _stamp_shape(key: int) -> tuple[Stamp, int]:
    """The shape and width one packed key names."""
    return _SHAPES_BY_CODE[key >> 8], key & 0xFF


def _sliced_micros(raw: pyarrow.Array, stamp: Stamp, width: int) -> pyarrow.Array:
    """A column of one shape at one width as canonical ISO microseconds.

    One canonical spelling out of all of them, so the cast that follows has
    one shape to parse: `YYYY-MM-DD HH:MM:SS.ffffff`. Assembled in a single
    join, with the literal separators passed as elements of it -- a shape that
    already writes the date or the clock that way hands over the run whole,
    and only one that spells it differently pays to have it taken apart.
    """
    compute = pyarrow.compute
    parts: list[Any] = [_run(raw, stamp.date_at, stamp.offsets[:3], "-"), " "]
    parts += [_run(raw, stamp.clock_at, stamp.offsets[3:], ":"), "."]
    slices, pad = stamp.micro_slices(width)
    parts += [compute.utf8_slice_codeunits(raw, start, stop) for start, stop in slices]
    if pad:
        parts.append("0" * pad)
    # The last argument is the separator, and it is empty because the
    # separators are already in `parts`: one kernel for the whole stamp.
    return compute.binary_join_element_wise(*parts, "")


def _run(
    raw: pyarrow.Array,
    whole: tuple[int, int] | None,
    offsets: Sequence[tuple[int, int]],
    separator: str,
) -> Any:
    """One canonical run: copied where the shape writes it, rebuilt where not."""
    compute = pyarrow.compute
    if whole is not None:
        return compute.utf8_slice_codeunits(raw, *whole)
    return compute.binary_join_element_wise(
        *(compute.utf8_slice_codeunits(raw, start, stop) for start, stop in offsets),
        separator,
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


def _zeros(count: int, arrow_type: pyarrow.DataType) -> pyarrow.Array:
    """A column of `count` zeros -- the envelope members a parsed line leaves unset.

    Zero and not null, because they are NOT NULL columns: what a log line does
    not have is stated, so a store never has to widen a column for it later,
    and a value repeated down a whole file encodes away to nothing on disk.
    """
    return pyarrow.repeat(pyarrow.scalar(0, arrow_type), count)
