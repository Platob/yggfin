"""Trading log file access."""

from __future__ import annotations

import datetime
import hashlib
import io
import os
import pathlib
import re
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import ClassVar

import pyarrow
import pyarrow.compute
import pyarrow.fs

from rekep.convert import Convertible
from rekep.filesystems import resolve
from rekep.logs.log import Log

#: Matches the fixed header every log row opens with, leaving the free-form
#: payload to `message`::
#:
#:     2026-08-14 00:05:01.167_520 [77-e72:9ef:72503] [ModuleFoo] (DEBUG) Found code
#:     ^timestamp                  ^thread_name       ^driver     ^level  ^message
#:
#: `level` is optional -- some drivers print none -- and the fractional second
#: carries millis and micros separated by an underscore. Matching is done on
#: bytes so lines never have to be decoded just to be classified; a line that
#: does not match is a wrapped continuation of the row above it.
HEADER_PATTERN = re.compile(
    rb"^[ \t]*"
    rb"(?P<timestamp>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[.,]\d{3}[._,]\d{3})[ \t]+"
    rb"\[(?P<thread_name>[^\]]*)\][ \t]+"
    rb"\[(?P<driver>[^\]]*)\][ \t]*"
    rb"(?:\((?P<level>[A-Za-z]{1,12})\)[ \t]*)?"
    rb"(?P<message>.*)$",
    re.DOTALL,
)

#: Rows per record batch: memory is bounded by it, per-batch Arrow overhead is
#: amortised over it.
DEFAULT_BATCH_ROW_SIZE = 65_536

#: Bytes pulled from the stream per read. On an object store every read is one
#: ranged HTTP request, so this is also the request granularity: too small
#: floods the store with GETs, too large holds memory. 4 MiB suits both
#: S3-like and local reads.
DEFAULT_READ_BYTE_SIZE = 1 << 22


@dataclass(eq=False)
class LogFile(Convertible, io.BufferedIOBase):
    """A trading log addressed by URI, exposed as a readable binary stream.

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

    #: IANA zone the wall clock in the header belongs to (`Europe/Paris`).
    #: None keeps the historical reading: the clock *is* UTC. Naming the real
    #: zone is what makes `unix` a true instant -- see `_unix_nanos`.
    timezone: str | None = None

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
    ) -> LogFile:
        """Build from a URI, or from a path when `filesystem` is given."""
        return cls(url=url, filesystem=filesystem, timezone=timezone)

    @classmethod
    def from_path(
        cls, path: str | os.PathLike[str], filesystem: pyarrow.fs.FileSystem | None = None
    ) -> LogFile:
        """Build from a local path, absolute or relative."""
        if filesystem is not None:
            return cls(url=os.fspath(path), filesystem=filesystem)
        return cls(url=pathlib.Path(path).resolve().as_uri())

    # -- converting ---------------------------------------------------------

    @cached_property
    def schema(self) -> pyarrow.Schema:
        """Arrow schema of the parsed rows, projected from `ROW`.

        Cached because `_batch` reads it once per batch: building a schema is
        cheap, but not free, and it cannot change while the file is open.
        """
        return self.ROW.FIELD.into_arrow_schema()

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

        The reader takes over this log's stream: consume the reader, not `self`.
        """
        self._check_open()
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
        indices = tuple(groups[name] for name in ("timestamp", "thread_name", "driver", "message"))
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
            if len(rows) >= batch_row_size:
                yield self._batch(rows, hashes)
        if rows:
            yield self._batch(rows, hashes)

    def _batch(self, rows: list[tuple], hashes: list[int]) -> pyarrow.RecordBatch:
        timestamps, threads, drivers, messages = zip(*rows, strict=True)
        local = _local_micros(timestamps)
        unix = _unix_nanos(local, self.timezone)
        date, time = _date_and_time(local)
        batch = pyarrow.RecordBatch.from_arrays(
            [
                pyarrow.repeat(self.url, len(rows)),
                unix,
                date,
                time,
                _utf8(threads),
                _utf8(drivers),
                _utf8(messages),
                pyarrow.array(hashes, type=pyarrow.int64()),
            ],
            schema=self.schema,
        )
        rows.clear()
        hashes.clear()
        return batch

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

    The header regex has pinned every component to a fixed offset, so the
    separators -- `T` or space, `.` or `,`, and the `_` between millis and
    micros -- are never even read: the components are sliced out and joined
    into canonical ISO form, and one cast parses the whole column. A batch a
    custom `header_pattern` shapes differently fails that cast and drops to
    the per-row Python fallback.
    """
    compute = pyarrow.compute
    raw = pyarrow.array(timestamps, type=pyarrow.binary()).cast(pyarrow.string())
    try:
        joined = compute.binary_join_element_wise(
            compute.utf8_slice_codeunits(raw, 0, 10),
            " ",
            compute.utf8_slice_codeunits(raw, 11, 19),
            ".",
            compute.utf8_slice_codeunits(raw, 20, 23),
            compute.utf8_slice_codeunits(raw, 24, 27),
            "",
        )
        return joined.cast(pyarrow.timestamp("us"))
    except pyarrow.ArrowInvalid:
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
    that `unix` is not monotonic across a fall-back hour, which is true of
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

    Taken from the **local** clock, not from `unix`: these two columns are
    what the line says, and a line stamped `2026-08-14 00:05` belongs to the
    14th for whoever wrote it, whatever instant that was in UTC. `unix` is
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

    Per-row fallback for batches the Arrow path cannot cast. Sliced rather
    than parsed: the header regex has already pinned every field to a fixed
    offset. The date half is cached because a log covers few distinct days but
    many rows.
    """
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
    """Fallback for timestamps a custom `header_pattern` shapes differently."""
    text = timestamp.decode("utf-8", "replace").replace("_", "").replace(",", ".")
    delta = datetime.datetime.fromisoformat(text) - _EPOCH_DATETIME
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _select_hash64() -> Callable[[bytes], int]:
    """Pick a line hash, preferring xxhash when it is installed.

    Note the two disagree: a `hash64` is stable across runs on one machine, not
    across environments that differ in whether xxhash is present.
    """
    try:
        from xxhash import xxh64_intdigest
    except ImportError:

        def hash64(raw: bytes) -> int:
            digest = hashlib.blake2b(raw, digest_size=8).digest()
            return int.from_bytes(digest, "little", signed=True)

    else:

        def hash64(raw: bytes) -> int:
            value = xxh64_intdigest(raw)
            return value - (1 << 64) if value >= (1 << 63) else value

    return hash64


_hash64 = _select_hash64()
