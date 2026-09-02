"""Text log files, read and written as Arrow."""

from __future__ import annotations

import datetime
import io
import os
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import InitVar, dataclass
from dataclasses import field as dataclass_field
from functools import cache, cached_property
from types import MappingProxyType
from typing import Any

import pyarrow
import pyarrow.compute
import pyarrow.fs

from rekep.arrow_path import ArrowPath
from rekep.arrow_reader import OwnedRecordBatchReader
from rekep.dataset import Dataset, arrow_chunks
from rekep.fields import Field, StructField, TimestampField
from rekep.fields.arrays import groups_of, scattered
from rekep.filesystems import ArrowFile
from rekep.market.event import ALTIDS_TYPE, unix_partition_arrow
from rekep.market.identity import HASH
from rekep.text.message import Message, repaired_text_arrow
from rekep.times import COMPACT, SHAPES, Stamp
from rekep.urls import Url

#: Every spelling of an instant a header may open with, as one alternation.
#: Derived from `rekep.times.SHAPES` rather than restated: the set of accepted
#: spellings is one behavior, and a shape this reader admitted and `times` did
#: not would be a stamp a window could not name.
_TIMESTAMP = "|".join(f"(?:{stamp.pattern})" for stamp in SHAPES)


#: Matches the fixed header every log row opens with, leaving the free-form
#: payload to `body`::
#:
#:     2026-08-14 00:05:01.167_520 [77-e72:9ef:72503] [ModuleFoo] (DEBUG) Found code
#:     ^timestamp                  ^threadname       ^plugin     ^level ^body
#:
#: `level` is optional -- some plugins print none -- and the fraction is one
#: to nine digits or absent: the same capture writes `01.147`, `01,147`,
#: `01.147250` and `01.147_250`, because one capture is written by several
#: loggers and they do not agree. Beside that ISO spelling a header may open
#: with FIX's own `20260824-10:00:01.123` or with a compact
#: `20260824100001123`. Matching is done on bytes so lines never have to be
#: decoded just to be classified; a line that does not match is a wrapped
#: continuation of the row above it.
#:
#: The optional byte order mark is on the *first* line of a capture a .NET or
#: Java writer produced, and it is a byte the encoding declares rather than one
#: the record carries. Skipped like indentation, because a first record read as
#: a continuation of nothing is a first record dropped without a row to say so;
#: a job supplying its own `header_pattern` inherits that trap and should skip
#: it the same way.
HEADER_PATTERN = re.compile(
    rb"^(?:\xef\xbb\xbf)?[ \t]*"
    rb"(?P<timestamp>" + _TIMESTAMP.encode() + rb")[ \t]+"
    rb"\[(?P<threadname>[^\]]*)\][ \t]+"
    rb"\[(?P<plugin>[^\]]*)\][ \t]*"
    rb"(?:\((?P<level>[A-Za-z]{1,12})\)[ \t]*)?"
    rb"(?P<body>.*)$",
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

#: The Arrow type of parent event hashes a line may carry.
#: Named here so the parser builds empty lists without re-deriving the type.
PARENTS = pyarrow.list_(pyarrow.field("item", HASH, nullable=False))

#: Rows per record batch: memory is bounded by it, per-batch Arrow overhead is
#: amortised over it.
DEFAULT_BATCH_ROW_SIZE = 65_536

# Raw header rows held before Arrow owns them. Row counts alone do not bound a
# capture with multi-megabyte diagnostics, so batches also stop at 64 MiB.
DEFAULT_BATCH_BYTE_SIZE = 1 << 26

#: Bytes pulled from the stream per read. On an object store every read is one
#: ranged HTTP request, so this is also the request granularity: too small
#: floods the store with GETs, too large holds memory. 4 MiB suits both
#: S3-like and local reads.
DEFAULT_READ_BYTE_SIZE = 1 << 22

#: Retained bytes per parsed row -- one physical line plus everything folded
#: into it. A newline is a writer's promise, and a capture truncated mid-write,
#: a binary blob logged by accident or a runaway diagnostic breaks it: without
#: this bound `readline` holds the whole run, and past 2 GiB Arrow's 32-bit
#: binary offsets cannot hold the row at all. Bytes past it are dropped and
#: counted, never read into memory, and the row says so in `reason`.
DEFAULT_MAX_ROW_BYTE_SIZE = 1 << 26

# Columns a line physically carries; the rest of the parsed row is derived.
_RENDERED = ("unix", "threadname", "plugin", "level", "body")


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
    #: groups -- `timestamp`, `threadname`, `plugin`, `body`; an optional
    #: `level` group persists the logger severity when the header carries one.
    header_pattern: re.Pattern[bytes] | str | bytes = HEADER_PATTERN

    #: Shape reads and writes land on. None is `into_row()`'s own -- what the parser
    #: fills -- and anything else is cast onto on the way out and in.
    row: StructField | None = None

    #: IANA zone the wall clock in the header belongs to (`Europe/Paris`).
    #: None keeps the historical reading: the clock *is* UTC. Naming the real
    #: zone is what makes `unix` a true instant -- see
    #: `_unix_nanos`.
    timezone: str | None = None

    #: Registry-owned MsgType values to their stored event kinds. A payload
    #: with no discriminator is MISC; a discriminator absent from this map is
    #: UNKNOWN, so registry coverage remains observable.
    msg_type_event_types: Mapping[str, int | str] = dataclass_field(default_factory=dict)

    #: Protocol classifier and registry-version authority shared with FIX transcription.
    protocol_codec: Any | None = None

    #: Raw key replacements selected by the plugin named in the log header.
    plugin_keys: Mapping[str, Mapping[str, str]] = dataclass_field(default_factory=dict)

    #: Case-insensitive payload values omitted before fields are promoted.
    null_values: Sequence[str] = ()

    #: Materialize remote compressed bytes locally before decoding. False
    #: leaves Arrow reading and decoding the object-store stream directly.
    spill: bool = False

    #: Constant columns every parsed row carries, appended **after** the data
    #: columns in the order they are given here -- the bridge that wrote the
    #: capture, the desk, the environment, whatever the file itself never says.
    #: Nothing here is hardcoded: a source names its own columns.
    #:
    #: A plain Python value has its Arrow type inferred; a `pyarrow.Scalar`
    #: states it (`pyarrow.scalar("bridge-1", pyarrow.large_string())`), which
    #: is also the only way to say "null, of this type".
    static_values: Mapping[str, Any] = dataclass_field(default_factory=dict)

    #: Runtime owner for the input handle. An InitVar keeps native handles and
    #: temporary paths out of Dataset serialization; __post_init__ publishes
    #: the normalized owner as `self.fileio`.
    fileio: InitVar[ArrowFile | None] = None

    def __post_init__(self, fileio: ArrowFile | None) -> None:
        """Normalize the source URI and bind one lazy Arrow input owner."""
        self.header_pattern = compiled_header(self.header_pattern)
        self.plugin_keys = {
            str(plugin): dict(replacements) for plugin, replacements in self.plugin_keys.items()
        }
        self.null_values = tuple(str(value) for value in self.null_values)
        location = self.url
        if fileio is None or fileio.opened is None:
            path = ArrowPath.from_url(location, self.filesystem)
            self.url = path.uri
            self.filesystem = path.filesystem
            if fileio is None:
                fileio = ArrowFile.from_location(path.path, path.filesystem)
            else:
                fileio = fileio.at(path.path, path.filesystem)
        else:
            self.url = Url.from_string(location).into_string()
        if self.filesystem is None:
            self.filesystem = fileio.filesystem
        self.fileio = fileio

    @cached_property
    def arrow_path(self) -> ArrowPath:
        """The normalized source URI paired with its backend-relative path."""
        filesystem = self.filesystem
        path = self.fileio.path
        if filesystem is None or path is None:
            raise NotImplementedError("an injected input stream has no filesystem path")
        return ArrowPath(self.url, filesystem, filesystem_path=path)

    # -- building -----------------------------------------------------------

    @classmethod
    def from_url(
        cls,
        url: str,
        filesystem: pyarrow.fs.FileSystem | None = None,
        *,
        timezone: str | None = None,
        fileio: ArrowFile | None = None,
        **declared: Any,
    ) -> TextFile:
        """Build from a URI, or from a path when `filesystem` is given.

        Anything else the file declares -- `static_values`, `row`,
        `header_pattern` -- is a keyword here, so a call reads as one shape.
        """
        return cls(url=url, filesystem=filesystem, timezone=timezone, fileio=fileio, **declared)

    @classmethod
    def from_path(
        cls,
        path: str | os.PathLike[str],
        filesystem: pyarrow.fs.FileSystem | None = None,
        *,
        timezone: str | None = None,
        fileio: ArrowFile | None = None,
        **declared: Any,
    ) -> TextFile:
        """Build from a local path, absolute or relative.

        Takes `timezone` like `from_url` does: a local log is the one most
        likely to be in local time, and the alternative was a documented
        example that raised `TypeError`.
        """
        if filesystem is not None:
            return cls(
                url=os.fspath(path),
                filesystem=filesystem,
                timezone=timezone,
                fileio=fileio,
                **declared,
            )
        return cls(
            url=Url.from_path(path).into_string(),
            timezone=timezone,
            fileio=fileio,
            **declared,
        )

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
        """What a write has to carry: the header columns and exact body."""
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
        if self.filesystem is not None:
            return self.arrow_path.exists()
        opened = self.fileio.opened
        return bool(opened is not None and getattr(opened, "exists", lambda: True)())

    def create_with_field(self, field: StructField, **kwargs: Any) -> TextFile:
        """Adopt `field` as this file's shape and make sure the file is there.

        Creating an empty log is writing nothing to it, so this only has to
        touch the file -- and remember the shape, which is what later reads
        are cast onto.
        """
        self.row = field
        filesystem = self.filesystem
        if filesystem is None:
            raise NotImplementedError("an injected input stream cannot create a text file")
        try:
            self.arrow_path.write_bytes(b"", overwrite=False)
        except FileExistsError:
            pass
        return self

    def read_arrow_reader(
        self,
        schema: Any = None,
        *,
        include_regexes: Sequence[str] = (),
        exclude_regexes: Sequence[str] = (),
        include_msgtypes: Sequence[str] = (),
        exclude_msgtypes: Sequence[str] = (),
        technical_plugins: Sequence[str] = (),
        start_unix: int | None = None,
        end_unix: int | None = None,
        duration_ns: int | None = None,
        **kwargs: Any,
    ) -> pyarrow.RecordBatchReader:
        """Parse the file, cast onto `schema` when one is asked for.

        With none, the reader is the parser's own -- see `into_arrow_reader`
        for the parsing options, which are passed straight through.
        """
        reader = self.into_arrow_reader(
            include_regexes=include_regexes,
            exclude_regexes=exclude_regexes,
            include_msgtypes=include_msgtypes,
            exclude_msgtypes=exclude_msgtypes,
            technical_plugins=technical_plugins,
            start_unix=start_unix,
            end_unix=end_unix,
            duration_ns=duration_ns,
            **kwargs,
        )
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
        """Add every line, or refuse a key lookup a text sequence cannot answer."""
        if merge_by:
            raise ValueError(
                f"{type(self).__name__} appends lines and cannot merge on {merge_by!r}; "
                "append to a dataset that can, or drop merge_by"
            )
        self.get_or_create()
        # With no schema named, the rendered columns are the only shape a write
        # has to satisfy: casting onto the whole row first would demand the
        # very columns reading derives.
        stream = source if schema is None else self.target_field(schema).cast_arrow_reader(source)
        reader = self.rendered_field.cast_arrow_reader(stream)
        inserted = 0
        for chunk in arrow_chunks(reader, commit_row_size):
            self._append(_rendered(chunk, self.timezone))
            inserted += chunk.num_rows
            # `arrow_chunks` accumulates the next chunk while this name still
            # holds the last one.
            del chunk
        return inserted

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
            filesystem = self.filesystem
            if filesystem is None:
                raise pyarrow.ArrowNotImplementedError("an injected input stream cannot append")
            stream = self.arrow_path.open_append()
        except pyarrow.ArrowNotImplementedError as error:
            raise NotImplementedError(
                f"{getattr(self.filesystem, 'type_name', 'input stream')} cannot append, and a "
                "log is written by appending; "
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
        batch_byte_size: int = DEFAULT_BATCH_BYTE_SIZE,
        max_row_byte_size: int = DEFAULT_MAX_ROW_BYTE_SIZE,
        fold_continuations: bool = True,
        include_regexes: Sequence[str] = (),
        exclude_regexes: Sequence[str] = (),
        include_msgtypes: Sequence[str] = (),
        exclude_msgtypes: Sequence[str] = (),
        technical_plugins: Sequence[str] = (),
        start_unix: int | None = None,
        end_unix: int | None = None,
        duration_ns: int | None = None,
    ) -> pyarrow.RecordBatchReader:
        """Stream filtered messages in row- or duration-bounded Arrow batches."""
        self._check_open()
        self._check_unread()
        # Nothing is parsing, so this only drops a handle the `read` API left
        # part way through the file.
        self._close_stream()
        batches = self.into_arrow_batches(
            batch_row_size,
            read_byte_size,
            fold_continuations,
            batch_byte_size=batch_byte_size,
            max_row_byte_size=max_row_byte_size,
            include_regexes=include_regexes,
            exclude_regexes=exclude_regexes,
            include_msgtypes=include_msgtypes,
            exclude_msgtypes=exclude_msgtypes,
            technical_plugins=technical_plugins,
            start_unix=start_unix,
            end_unix=end_unix,
            duration_ns=duration_ns,
        )
        return OwnedRecordBatchReader(self.schema, batches, self._close_stream)

    def into_arrow_table(self, **kwargs: object) -> pyarrow.Table:
        """Read the whole log into one Arrow table. Needs it to fit in memory."""
        return self.into_arrow_reader(**kwargs).read_all()  # type: ignore[arg-type]

    def into_arrow_batches(
        self,
        batch_row_size: int = DEFAULT_BATCH_ROW_SIZE,
        read_byte_size: int = DEFAULT_READ_BYTE_SIZE,
        fold_continuations: bool = True,
        *,
        batch_byte_size: int = DEFAULT_BATCH_BYTE_SIZE,
        max_row_byte_size: int = DEFAULT_MAX_ROW_BYTE_SIZE,
        include_regexes: Sequence[str] = (),
        exclude_regexes: Sequence[str] = (),
        include_msgtypes: Sequence[str] = (),
        exclude_msgtypes: Sequence[str] = (),
        technical_plugins: Sequence[str] = (),
        start_unix: int | None = None,
        end_unix: int | None = None,
        duration_ns: int | None = None,
    ) -> Iterator[pyarrow.RecordBatch]:
        """Yield retained messages whenever the row or duration bound ends.

        The row loop is deliberately spartan -- profiling puts it, not Arrow,
        on the critical path. Groups come out in one `group(...)` call against
        indices resolved once, and land as one tuple append; everything
        columnar happens once per batch in `_batch`.
        """
        self._check_open()
        self._check_unread()
        includes = _regexes("include_regexes", include_regexes)
        excludes = _regexes("exclude_regexes", exclude_regexes)
        included_msgtypes = _msgtypes("include_msgtypes", include_msgtypes)
        excluded_msgtypes = _msgtypes("exclude_msgtypes", exclude_msgtypes)
        technical_plugin_codes = _plugins(technical_plugins)
        _validate_read_sizes(batch_row_size, read_byte_size, batch_byte_size, max_row_byte_size)
        _validate_window(start_unix, end_unix, duration_ns)
        batches = self._filtered_batches(
            batch_row_size,
            read_byte_size,
            batch_byte_size,
            max_row_byte_size,
            fold_continuations,
            includes,
            excludes,
            included_msgtypes,
            excluded_msgtypes,
            technical_plugin_codes,
            start_unix,
            end_unix,
        )
        if duration_ns is None:
            return batches
        return _windowed_batches(
            batches,
            batch_row_size,
            batch_byte_size=batch_byte_size,
            duration_ns=duration_ns,
            start_unix=start_unix,
        )

    def _filtered_batches(
        self,
        batch_row_size: int,
        read_byte_size: int,
        batch_byte_size: int,
        max_row_byte_size: int,
        fold_continuations: bool,
        include_regexes: Sequence[str],
        exclude_regexes: Sequence[str],
        include_msgtypes: Sequence[str],
        exclude_msgtypes: Sequence[str],
        technical_plugins: Sequence[str],
        start_unix: int | None,
        end_unix: int | None,
    ) -> Iterator[pyarrow.RecordBatch]:
        """Parse bounded raw rows only after their payload and time survive."""
        groups = self.header_pattern.groupindex
        indices = tuple(groups[name] for name in ("timestamp", "threadname", "plugin", "body"))
        level_index = groups.get("level")
        rows: list[
            tuple[bytes, bytes | None, bytes | None, bytes | None, bytes | bytearray | None]
        ] = []
        row_byte_sizes: list[int] = []
        # Bytes of each row the reader saw and did not keep, so a truncated row
        # says how much of itself is missing instead of looking whole.
        dropped_byte_sizes: list[int] = []
        held_bytes = 0
        # Physical lines, not parsed rows: a folded continuation must not shift
        # the number every row after it reports.
        rownums: list[int] = []
        rownum = 0
        match_header = self.header_pattern.match

        for line, dropped in self._iter_lines(read_byte_size, max_row_byte_size):
            rownum += 1
            match = match_header(line)
            if match is None:
                if dropped and not rows:
                    # Every dropped byte is on some row's `reason` or it is
                    # refused: there is no row here to carry this one, and a
                    # bound this far below a header reads a whole log as no
                    # rows at all.
                    raise ValueError(
                        f"max_row_byte_size of {max_row_byte_size} cut line {rownum} of "
                        f"{self.url} before the header pattern could match it, and no row can "
                        "carry what it dropped; raise the bound"
                    )
                if fold_continuations and rows:
                    # The newline the fold puts back counts against the bound
                    # with the line it separates, so a row can never exceed it.
                    room = max_row_byte_size - row_byte_sizes[-1]
                    added = len(line) + 1
                    kept = added if added <= room else max(room, 0)
                    if kept:
                        timestamp, thread, plugin, level, body = rows[-1]
                        if not isinstance(body, bytearray):
                            body = bytearray(body or b"")
                        body.extend(b"\n")
                        body.extend(line if kept == added else line[: kept - 1])
                        rows[-1] = (timestamp, thread, plugin, level, body)
                        row_byte_sizes[-1] += kept
                        held_bytes += kept
                    dropped_byte_sizes[-1] += added - kept + dropped
                continue
            timestamp, thread, plugin, body = match.group(*indices)
            level = match.group(level_index) if level_index is not None else None
            found = (timestamp, thread, plugin, level, body)
            rows.append(found)
            rownums.append(rownum)
            row_byte_sizes.append(len(line))
            dropped_byte_sizes.append(dropped)
            held_bytes += len(line)
            # One row past the size, not at it: a continuation belongs to the
            # row above it, and cutting the batch the moment that row is
            # complete puts it out of reach of the next line. A stack trace
            # that happens to land on the boundary would be dropped, silently,
            # at any batch size -- including the default one.
            cut = 0
            if len(rows) > batch_row_size:
                cut = batch_row_size
            elif held_bytes > batch_byte_size and len(rows) > 1:
                cut = len(rows) - 1
            if cut:
                batch = self._batch(
                    rows[:cut],
                    rownums[:cut],
                    dropped_byte_sizes[:cut],
                    include_regexes,
                    exclude_regexes,
                    include_msgtypes,
                    exclude_msgtypes,
                    technical_plugins,
                    start_unix,
                    end_unix,
                )
                if batch.num_rows:
                    yield batch
                held_bytes -= sum(row_byte_sizes[:cut])
                del rows[:cut], rownums[:cut], row_byte_sizes[:cut], dropped_byte_sizes[:cut]
        if rows:
            batch = self._batch(
                rows,
                rownums,
                dropped_byte_sizes,
                include_regexes,
                exclude_regexes,
                include_msgtypes,
                exclude_msgtypes,
                technical_plugins,
                start_unix,
                end_unix,
            )
            if batch.num_rows:
                yield batch

    def _batch(
        self,
        rows: list[tuple],
        rownums: list[int],
        dropped_byte_sizes: Sequence[int] = (),
        include_regexes: Sequence[str] = (),
        exclude_regexes: Sequence[str] = (),
        include_msgtypes: Sequence[str] = (),
        exclude_msgtypes: Sequence[str] = (),
        technical_plugins: Sequence[str] = (),
        start_unix: int | None = None,
        end_unix: int | None = None,
    ) -> pyarrow.RecordBatch:
        """One batch of parsed headers and protocol-neutral payloads.

        Assembled **by name** and then ordered by the schema, rather than as a
        positional list: a column added to `Message` then fails here by its own
        name instead of silently shifting every column after it into the wrong
        one.
        """
        schema = self.schema
        timestamps, threads, plugins, levels, bodies = (
            pyarrow.array(values, type=pyarrow.binary()) for values in zip(*rows, strict=True)
        )
        rownums_array = pyarrow.array(rownums, type=pyarrow.int64())
        reasons = _truncated_reasons(dropped_byte_sizes, len(rows))

        selected = _plugin_mask(plugins, technical_plugins)
        if selected is not None:
            timestamps, threads, plugins, levels, bodies, rownums_array, reasons = (
                pyarrow.compute.filter(values, selected)
                for values in (
                    timestamps,
                    threads,
                    plugins,
                    levels,
                    bodies,
                    rownums_array,
                    reasons,
                )
            )
        if not len(timestamps):
            return _empty_batch(schema)

        local = _local_micros(timestamps)
        unix = _unix_nanos(local, self.timezone)
        selected = _unix_mask(unix, start_unix, end_unix)
        if selected is not None:
            unix, threads, plugins, levels, bodies, rownums_array, reasons = (
                pyarrow.compute.filter(values, selected)
                for values in (unix, threads, plugins, levels, bodies, rownums_array, reasons)
            )
        if not len(unix):
            return _empty_batch(schema)

        selected = _message_mask(bodies, include_regexes, exclude_regexes)
        if selected is not None:
            unix, threads, plugins, levels, bodies, rownums_array, reasons = (
                pyarrow.compute.filter(values, selected)
                for values in (unix, threads, plugins, levels, bodies, rownums_array, reasons)
            )
        if not len(unix):
            return _empty_batch(schema)

        selected = _msgtype_mask(bodies, include_msgtypes, exclude_msgtypes)
        if selected is not None:
            unix, threads, plugins, levels, bodies, rownums_array, reasons = (
                pyarrow.compute.filter(values, selected)
                for values in (unix, threads, plugins, levels, bodies, rownums_array, reasons)
            )
        count = len(unix)
        if not count:
            return _empty_batch(schema)

        columns: dict[str, Any] = {
            "unix": unix,
            "unixpartition": unix_partition_arrow(unix),
            "creaunix": unix,
            "recunix": unix,
            "expunix": pyarrow.nulls(count, pyarrow.int64()),
            "snapunix": pyarrow.nulls(count, pyarrow.int64()),
            "version": _zeros(count, pyarrow.int64()),
            "state": _zeros(count, pyarrow.int64()),
            "code": _constant_column(count, ""),
            "altids": _constant_column(count, pyarrow.scalar({}, ALTIDS_TYPE)),
            "prevunix": pyarrow.nulls(count, pyarrow.int64()),
            "parenthash": pyarrow.nulls(count, PARENTS),
            "lastmkt": pyarrow.nulls(count, pyarrow.int32()),
            "reason": reasons,
            "sourceurl": _constant_column(count, self.url),
            "sourcerownum": rownums_array,
            "threadname": pyarrow.compute.fill_null(_utf8(threads), ""),
            "level": _utf8(levels),
            "plugin": pyarrow.compute.fill_null(_utf8(plugins), ""),
            "body": pyarrow.compute.fill_null(bodies, b""),
        }
        parsed = self.into_row().parse_arrow(
            bodies,
            self.msg_type_event_types,
            columns["plugin"],
            self.protocol_codec,
            self.plugin_keys,
            self.null_values,
        )
        parse_errors = parsed.pop("parseerror")
        columns["reason"] = _merge_reasons(columns["reason"], parse_errors)
        columns.update(parsed)
        columns.update(
            (name, _constant_column(count, scalar)) for name, scalar in self.static_columns
        )
        # `Message.identified` fills these once every raw column is here.
        for name in ("hash", "vhash", "xhash"):
            columns.setdefault(name, pyarrow.nulls(count, schema.field(name).type))
        linkhashes = schema.field("linkhashes")
        columns.setdefault(
            "linkhashes", _constant_column(count, pyarrow.scalar([], type=linkhashes.type))
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

    def _iter_lines(
        self, read_byte_size: int, max_row_byte_size: int = DEFAULT_MAX_ROW_BYTE_SIZE
    ) -> Iterator[tuple[bytes, int]]:
        """`(line, bytes past the bound)` for every newline-delimited line.

        One trailing carriage return is dropped per line, so a CRLF log parses
        identically to an LF one; a carriage return anywhere else is payload.

        `readline` with no bound holds a whole line, and a line is only as long
        as the writer's next newline -- which a truncated capture, a logged
        binary blob or a runaway diagnostic never writes. The bound is passed
        to `readline` rather than checked after it, so the bytes past it are
        read in `read_byte_size` pieces and dropped instead of held.
        """
        self._check_unread()
        self.__dict__["_reading"] = True
        buffered = io.BufferedReader(self, buffer_size=read_byte_size)
        try:
            while line := buffered.readline(max_row_byte_size):
                dropped = 0
                # A line that fills the bound may or may not be the whole line;
                # what says which is the newline, and only EOF ends one without.
                # The terminator is not content, so draining exactly it -- a
                # line the bound fits precisely -- drops nothing. Only the
                # chunk that ends on the newline carries one: another ended
                # because `read_byte_size` ran out, and its last byte is
                # payload however much it looks like half a terminator.
                while not line.endswith(b"\n") and (rest := buffered.readline(read_byte_size)):
                    if not rest.endswith(b"\n"):
                        dropped += len(rest)
                        continue
                    dropped += len(rest.removesuffix(b"\n").removesuffix(b"\r"))
                    break
                line = line.removesuffix(b"\n")
                # A `\r` is the other half of a terminator only where one is;
                # on a line the bound cut, it is the payload's own byte.
                yield (line if dropped else line.removesuffix(b"\r")), dropped
        finally:
            # Detaching keeps the reusable TextFile open while discarding the
            # line buffer; `_close_stream` owns the Arrow decoder underneath.
            try:
                buffered.detach()
            except ValueError:
                # A reader closed by its caller has already closed this
                # wrapper and the underlying TextFile together.
                pass
            # The decoder closes before the owning temporary ArrowFile, so
            # Windows can remove the raw compressed spill immediately.
            self._close_stream()

    # -- opening ------------------------------------------------------------

    def _open(self) -> pyarrow.NativeFile:
        """Open a new Arrow stream over `url`.

        Plain logs go through `open_input_file` because it is the only opener
        that yields a seekable handle. Compressed logs stream their decoding;
        an opted-in spill first copies the raw compressed bytes to local disk.
        """
        active = self.fileio
        if self.spill and self._codec is not None:
            active = active.spill(temporary=True)
        if active is None:
            raise FileNotFoundError(f"compressed source does not exist: {self.url}")
        self.__dict__["_active_fileio"] = active
        return active.open(seekable=self._codec is None, compression=self._codec)

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
        self._close_stream()
        super().close()

    def _close_stream(self) -> None:
        """Close and forget the lazily opened stream without opening it."""
        self.__dict__.pop("_reading", None)
        stream = self.__dict__.pop("_stream", None)
        if stream is not None:
            stream.close()
        active = self.__dict__.pop("_active_fileio", None)
        if active is not None:
            active.close()

    def _check_open(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file.")

    def _check_unread(self) -> None:
        """Refuse a second parse of a file that has one stream to parse it with.

        Both parses read through the one lazily opened handle, so the second
        one starting rewinds it under the first: the rows already read come
        back again, and the two readers then split every buffer between them --
        which lands mid-line and hands a spliced record over as data. A log is
        read once at a time; a second reading of it is a second `TextFile`.
        """
        if self.__dict__.get("_reading"):
            raise ValueError(
                f"{type(self).__name__} is already being read; finish or close that reader "
                f"before parsing {self.url} again, or open a second {type(self).__name__}"
            )


def _rendered(rows: pyarrow.Table, timezone: str | None = None) -> bytes:
    """One chunk of parsed rows back as log lines, in Arrow kernels only."""
    if rows.num_rows == 0:
        return b""
    compute = pyarrow.compute
    micros = compute.divide(rows.column("unix"), TimestampField.factor_of("us")).cast(
        pyarrow.int64()
    )
    if timezone and os.name == "nt":
        stamps = _windows_local_micros(micros, timezone).cast(pyarrow.timestamp("us"))
    else:
        stamps = TimestampField.of("us").from_unix_arrow(micros, unit="us")
    if timezone and os.name != "nt":
        stamps = stamps.cast(pyarrow.timestamp("us", "UTC")).cast(pyarrow.timestamp("us", timezone))
    stamps = compute.strftime(stamps, format="%Y-%m-%d %H:%M:%S")
    stamps = compute.utf8_replace_slice(stamps, start=23, stop=23, replacement="_")
    levels = rows.column("level").cast(pyarrow.binary())
    rendered_levels = compute.if_else(
        compute.is_valid(levels),
        compute.binary_join_element_wise(b"(", compute.fill_null(levels, b""), b") ", b""),
        pyarrow.scalar(b"", pyarrow.binary()),
    )
    lines = compute.binary_join_element_wise(
        stamps.cast(pyarrow.binary()),
        b" [",
        rows.column("threadname").cast(pyarrow.binary()),
        b"] [",
        rows.column("plugin").cast(pyarrow.binary()),
        b"] ",
        rendered_levels,
        rows.column("body").cast(pyarrow.binary()),
        b"",
    )
    flat = lines.combine_chunks() if isinstance(lines, pyarrow.ChunkedArray) else lines
    whole = pyarrow.ListArray.from_arrays(
        pyarrow.array([0, len(flat)], pyarrow.int32()),
        flat.combine_chunks() if isinstance(flat, pyarrow.ChunkedArray) else flat,
    )
    return compute.binary_join(whole, b"\n")[0].as_py() + b"\n"


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


def _regexes(name: str, values: Sequence[str]) -> tuple[str, ...]:
    """A regex list rather than one string accidentally treated as characters."""
    if isinstance(values, str):
        raise TypeError(f"{name} must be a sequence of regex strings, not one string")
    patterns = tuple(values)
    invalid = [type(pattern).__name__ for pattern in patterns if not isinstance(pattern, str)]
    if invalid:
        raise TypeError(f"{name} must contain only regex strings, got {invalid[0]}")
    probe = pyarrow.array([""], type=pyarrow.string())
    for pattern in patterns:
        pyarrow.compute.match_substring_regex(probe, pattern)
    return patterns


def _msgtypes(name: str, values: Sequence[str]) -> tuple[str, ...]:
    """An exact MsgType list, never a string split into characters."""
    if isinstance(values, str):
        raise TypeError(f"{name} must be a sequence of MsgType strings, not one string")
    found = tuple(values)
    invalid = next((type(value).__name__ for value in found if not isinstance(value, str)), None)
    if invalid is not None:
        raise TypeError(f"{name} must contain only MsgType strings, got {invalid}")
    return tuple(dict.fromkeys(found))


def _plugins(values: Sequence[str]) -> tuple[str, ...]:
    """Case-folded technical plugin codes, never one code split into characters."""
    if isinstance(values, str):
        raise TypeError("technical_plugins must be a sequence of plugin strings, not one string")
    found = tuple(values)
    invalid = next((type(value).__name__ for value in found if not isinstance(value, str)), None)
    if invalid is not None:
        raise TypeError(f"technical_plugins must contain only plugin strings, got {invalid}")
    return tuple(dict.fromkeys(value.lower() for value in found))


def _plugin_mask(plugins: pyarrow.Array, technical_plugins: Sequence[str]) -> pyarrow.Array | None:
    """Rows whose recorder plugin is not declared technical."""
    if not technical_plugins:
        return None
    codes = pyarrow.compute.utf8_lower(_utf8(plugins))
    technical = pyarrow.compute.fill_null(
        pyarrow.compute.is_in(codes, value_set=pyarrow.array(technical_plugins)), False
    )
    return pyarrow.compute.invert(technical)


def _msgtype_mask(
    bodies: pyarrow.Array,
    include_msgtypes: Sequence[str],
    exclude_msgtypes: Sequence[str],
) -> pyarrow.Array | None:
    """Rows admitted by an exact include and no exact exclude.

    The discriminator is probed here rather than by the caller because it is
    read for this bound and nothing else: the probe is five RE2 passes over
    every payload -- 42 to 160 ms of a 65,536-row batch, by how much of it is
    a message -- and a read that declares no msgtype would pay them for a mask
    it then discards.
    """
    if not include_msgtypes and not exclude_msgtypes:
        return None
    compute = pyarrow.compute
    msgtypes = Message.msg_types_arrow(bodies)
    included = (
        None
        if not include_msgtypes
        else compute.fill_null(
            compute.is_in(msgtypes, value_set=pyarrow.array(include_msgtypes)), False
        )
    )
    excluded = (
        None
        if not exclude_msgtypes
        else compute.fill_null(
            compute.is_in(msgtypes, value_set=pyarrow.array(exclude_msgtypes)), False
        )
    )
    if excluded is None:
        return included
    allowed = compute.invert(excluded)
    return allowed if included is None else compute.and_(included, allowed)


def _message_mask(
    bodies: pyarrow.Array,
    include_regexes: Sequence[str],
    exclude_regexes: Sequence[str],
) -> pyarrow.Array | None:
    """Rows admitted by any include and no exclude, matched by Arrow RE2.

    Decoding the payloads belongs to this bound too: the text it matches over
    is not a column any row carries, so a read that declares no regex would
    decode a batch for nothing.
    """
    if not include_regexes and not exclude_regexes:
        return None
    messages = pyarrow.compute.fill_null(_utf8(bodies), "")

    def matches(patterns: Sequence[str]) -> pyarrow.Array | None:
        selected = None
        for pattern in patterns:
            found = pyarrow.compute.fill_null(
                pyarrow.compute.match_substring_regex(messages, pattern), False
            )
            selected = found if selected is None else pyarrow.compute.or_(selected, found)
        return selected

    included = matches(include_regexes)
    excluded = matches(exclude_regexes)
    if excluded is None:
        return included
    allowed = pyarrow.compute.invert(excluded)
    return allowed if included is None else pyarrow.compute.and_(included, allowed)


def _unix_mask(
    unix: pyarrow.Array, start_unix: int | None, end_unix: int | None
) -> pyarrow.Array | None:
    """The inclusive start and exclusive end of a recording-time interval."""
    selected = None
    if start_unix is not None:
        selected = pyarrow.compute.greater_equal(unix, start_unix)
    if end_unix is not None:
        before = pyarrow.compute.less(unix, end_unix)
        selected = before if selected is None else pyarrow.compute.and_(selected, before)
    return selected


_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


def _validate_window(start_unix: int | None, end_unix: int | None, duration_ns: int | None) -> None:
    """Refuse ambiguous time bounds before the source is consumed."""
    for name, value in (("start_unix", start_unix), ("end_unix", end_unix)):
        if value is not None and (not isinstance(value, int) or isinstance(value, bool)):
            raise TypeError(f"{name} must be an integer number of nanoseconds or None")
        if value is not None and not _INT64_MIN <= value <= _INT64_MAX:
            raise ValueError(f"{name} must fit in a signed 64-bit integer")
    if start_unix is not None and end_unix is not None and start_unix > end_unix:
        raise ValueError("start_unix must be less than or equal to end_unix")
    if duration_ns is not None and (
        not isinstance(duration_ns, int)
        or isinstance(duration_ns, bool)
        or not 0 < duration_ns <= _INT64_MAX
    ):
        raise ValueError(
            "duration_ns must be a positive integer number of nanoseconds that fits in int64"
        )


def _validate_read_sizes(
    batch_row_size: int,
    read_byte_size: int,
    batch_byte_size: int = DEFAULT_BATCH_BYTE_SIZE,
    max_row_byte_size: int = DEFAULT_MAX_ROW_BYTE_SIZE,
) -> None:
    """Keep every parser buffer bounded by positive explicit units."""
    for name, value, unit in (
        ("batch_row_size", batch_row_size, "rows"),
        ("read_byte_size", read_byte_size, "bytes"),
        ("batch_byte_size", batch_byte_size, "bytes"),
        ("max_row_byte_size", max_row_byte_size, "bytes"),
    ):
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"{name} must be a positive integer number of {unit}")
    # An int32 offset addresses the bytes of a whole binary array, so it bounds
    # the batch **and** the one row inside it. A bound above it is a bound
    # Arrow cannot build, and it fails while concatenating rather than at the
    # row that overflowed it.
    for name, value in (
        ("batch_byte_size", batch_byte_size),
        ("max_row_byte_size", max_row_byte_size),
    ):
        if value > _BINARY_OFFSET_MAX:
            raise ValueError(
                f"{name} must be at most {_BINARY_OFFSET_MAX} bytes, which is what an Arrow "
                "binary offset addresses"
            )


#: What one 32-bit binary offset reaches, and so the most bytes a batch of them
#: can hold -- one row included, since a row is one value in that array.
_BINARY_OFFSET_MAX = (1 << 31) - 1


def _truncated_reasons(dropped_byte_sizes: Sequence[int], rows: int) -> pyarrow.Array:
    """`reason` for each row: what it lost to `max_row_byte_size`, or null."""
    if not any(dropped_byte_sizes):
        return pyarrow.nulls(rows, pyarrow.string())
    dropped = pyarrow.array(dropped_byte_sizes, type=pyarrow.int64())
    compute = pyarrow.compute
    return compute.if_else(
        compute.greater(dropped, 0),
        compute.binary_join_element_wise(
            "row truncated at max_row_byte_size; dropped bytes: ",
            dropped.cast(pyarrow.string()),
            "",
        ),
        pyarrow.scalar(None, pyarrow.string()),
    )


def _merge_reasons(current: Any, added: Any) -> pyarrow.Array:
    """Append nullable parser diagnostics without replacing truncation facts."""
    compute = pyarrow.compute
    left = compute.fill_null(current.cast(pyarrow.string(), safe=False), "")
    right = compute.fill_null(added.cast(pyarrow.string(), safe=False), "")
    both = compute.and_(compute.not_equal(left, ""), compute.not_equal(right, ""))
    joined = compute.binary_join_element_wise(
        left,
        compute.if_else(both, "; ", ""),
        right,
        "",
    )
    return compute.if_else(
        compute.equal(joined, ""), pyarrow.nulls(len(joined), pyarrow.string()), joined
    )


def _windowed_batches(
    batches: Iterator[pyarrow.RecordBatch],
    batch_row_size: int,
    *,
    batch_byte_size: int = DEFAULT_BATCH_BYTE_SIZE,
    duration_ns: int | None,
    start_unix: int | None,
) -> Iterator[pyarrow.RecordBatch]:
    """Coalesce short batches without crossing an event-time window."""
    pending: list[pyarrow.RecordBatch] = []
    rows = 0
    held_bytes = 0
    origin = start_unix
    current_window: int | None = None
    for batch in batches:
        if not batch.num_rows:
            continue
        if duration_ns is None:
            runs = ((None, batch),)
        else:
            if origin is None:
                first = batch.column("unix")[0].as_py()
                origin = first - first % duration_ns
            runs = _window_runs(batch, duration_ns, origin)
        for window, run in runs:
            if duration_ns is not None:
                if current_window is not None and window < current_window:
                    raise ValueError(
                        "a duration window recurs after a later window; order the source by unix"
                    )
                if current_window is not None and window != current_window and pending:
                    yield _one(pending)
                    pending, rows, held_bytes = [], 0, 0
                current_window = window
            if duration_ns is None:
                if pending and held_bytes + run.nbytes > batch_byte_size:
                    yield _one(pending)
                    pending, rows, held_bytes = [], 0, 0
                pending.append(run)
                rows += run.num_rows
                held_bytes += run.nbytes
                if rows >= batch_row_size or held_bytes >= batch_byte_size:
                    yield _one(pending)
                    pending, rows, held_bytes = [], 0, 0
                continue
            while run.num_rows:
                take = min(batch_row_size - rows, run.num_rows)
                part = run.slice(0, take)
                if pending and held_bytes + part.nbytes > batch_byte_size:
                    yield _one(pending)
                    pending, rows, held_bytes = [], 0, 0
                    continue
                pending.append(part)
                rows += take
                held_bytes += part.nbytes
                run = run.slice(take)
                if rows == batch_row_size or held_bytes >= batch_byte_size:
                    yield _one(pending)
                    pending, rows, held_bytes = [], 0, 0
    if pending:
        yield _one(pending)


def _window_runs(
    batch: pyarrow.RecordBatch, duration_ns: int, origin: int
) -> Iterator[tuple[int, pyarrow.RecordBatch]]:
    """Contiguous duration-window runs, located with Arrow kernels."""
    unix = batch.column("unix")
    bounds = pyarrow.compute.min_max(unix).as_py()
    if bounds["min"] < origin:
        raise ValueError("a unix value precedes the duration origin; order the source by unix")
    if bounds["max"] - origin <= _INT64_MAX:
        delta = pyarrow.compute.subtract(unix, pyarrow.scalar(origin, unix.type))
        windows = pyarrow.compute.divide(delta, pyarrow.scalar(duration_ns, delta.type))
    else:
        # Two valid int64 instants can be farther apart than int64 can hold.
        # Decimal256 keeps this rare path columnar without narrowing the delta.
        wide_type = pyarrow.decimal256(38, 0)
        delta = pyarrow.compute.subtract(unix.cast(wide_type), pyarrow.scalar(origin, wide_type))
        quotient = pyarrow.compute.divide(delta, pyarrow.scalar(duration_ns, pyarrow.int64()))
        windows = pyarrow.compute.round(quotient, ndigits=0, round_mode="down")
    encoded = pyarrow.compute.run_end_encode(windows)
    start = 0
    for end, window in zip(encoded.run_ends, encoded.values, strict=True):
        stop = end.as_py()
        yield int(window.as_py()), batch.slice(start, stop - start)
        start = stop


def _one(batches: list[pyarrow.RecordBatch]) -> pyarrow.RecordBatch:
    """The batches as one, handing a single batch over without a copy."""
    if len(batches) == 1:
        return batches[0]
    first = batches[0]
    return pyarrow.RecordBatch.from_arrays(
        [
            pyarrow.concat_arrays([batch.column(index) for batch in batches])
            for index in range(first.num_columns)
        ],
        schema=first.schema,
    )


def _empty_batch(schema: pyarrow.Schema) -> pyarrow.RecordBatch:
    """A schema-carrying batch with no rows."""
    return pyarrow.RecordBatch.from_arrays(
        [pyarrow.nulls(0, field.type) for field in schema], schema=schema
    )


def _utf8(values: Sequence[bytes | None] | pyarrow.Array) -> pyarrow.Array:
    """Cast raw bytes to a string array, repairing only the rows that are dirty."""
    array = values if isinstance(values, pyarrow.Array) else pyarrow.array(values)
    return repaired_text_arrow(array.cast(pyarrow.binary()))


def _local_micros(timestamps: Sequence[bytes] | pyarrow.Array) -> pyarrow.Array:
    """One batch of raw header timestamps to a naive `timestamp("us")` column.

    Sliced, never read a row at a time. A batch of one shape at one width --
    which is nearly every batch, because a file is written by one logger --
    is decided by two character comparisons and sliced whole; only a batch
    that actually mixes shapes or widths pays to be grouped.
    """
    compute = pyarrow.compute
    raw = timestamps if isinstance(timestamps, pyarrow.Array) else pyarrow.array(timestamps)
    raw = raw.cast(pyarrow.string())
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
        micros = _windows_utc_micros(local, timezone)
        return pyarrow.compute.multiply(micros, TimestampField.factor_of("us"))
    if timezone:
        local = pyarrow.compute.assume_timezone(
            local, timezone, ambiguous="earliest", nonexistent="latest"
        )
    return TimestampField.into_unix_arrow(local)


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


def _constant_column(count: int, value: Any) -> pyarrow.Array:
    """A column of `count` copies of one value.

    Taken from a one-row array rather than repeated, because `pyarrow.repeat`
    builds a *nested* value once per row: over a 65,536-row batch an empty
    `altids` map costs 34 ms repeated against 0.4 ms taken, and an empty
    `linkhashes` list 8.3 ms against 0.34 ms. A flat value goes the other way,
    0.10 ms to 0.32 ms, so the seven flat columns here pay about 2 ms for the
    42 ms the two nested ones save -- and for one implementation of a constant.

    A null has no copies to take: `take` leaves a nested child array one row
    long where `repeat` leaves it `count`, which is the same column and not
    the same bytes, and these bytes are written to a store.
    """
    if isinstance(value, pyarrow.Scalar) and not value.is_valid:
        return pyarrow.nulls(count, value.type)
    return pyarrow.compute.take(
        pyarrow.repeat(value, 1), pyarrow.repeat(pyarrow.scalar(0, pyarrow.int32()), count)
    )


def _zeros(count: int, dtype: pyarrow.DataType) -> pyarrow.Array:
    """A column of `count` zeros -- the envelope members a parsed line leaves unset.

    Zero and not null, because they are NOT NULL columns: what a log line does
    not have is stated, so a store never has to widen a column for it later,
    and a value repeated down a whole file encodes away to nothing on disk.
    """
    return _constant_column(count, pyarrow.scalar(0, dtype))
