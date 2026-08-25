"""Many log files, read in path order as one stream."""

from __future__ import annotations

import fnmatch
import io
import os
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from functools import cache, cached_property
from types import MappingProxyType
from typing import Any

import pyarrow
import pyarrow.fs

from rekep.dataset import Dataset
from rekep.fields import StructField
from rekep.filesystems import resolve
from rekep.text.text_file import (
    DEFAULT_BATCH_BYTE_SIZE,
    DEFAULT_BATCH_ROW_SIZE,
    DEFAULT_READ_BYTE_SIZE,
    HEADER_PATTERN,
    TextFile,
    _regexes,
    _validate_read_sizes,
    _validate_window,
    _windowed_batches,
    compiled_header,
    parsed_field_of,
    static_columns_of,
)
from rekep.urls import Url

#: Cuts a path into its digit runs and everything between them, so ordering
#: can compare a run as a number: `app.9.txt` before `app.10.txt`, which a
#: plain string sort puts the other way round.
_DIGITS = re.compile(r"(\d+)")


@dataclass(eq=False)
class TextFiles(Dataset, io.BufferedIOBase):
    """Every log under a set of roots, read in path order as one Arrow stream."""

    @classmethod
    @cache
    def into_redirects(cls) -> Mapping[object, str]:
        """Conversions this file set infers from its source or target."""
        return MappingProxyType(
            {
                pyarrow.RecordBatchReader: "arrow_reader",
                pyarrow.Table: "arrow_table",
                pyarrow.RecordBatch: "arrow_batches",
                str: "folder",
                os.PathLike: "folder",
            }
        )

    @classmethod
    @cache
    def into_file(cls) -> type[TextFile]:
        """Class used to read each file in the set."""
        return TextFile

    @classmethod
    @cache
    def into_kind(cls) -> str:
        """Document kind registered with `Dataset`."""
        return "text_files"

    #: Roots to read, in the order given: folders to walk, or files to take.
    #: The order a caller states is never reshuffled -- yesterday's archive
    #: directory before today's live one is a statement about time.
    roots: tuple[str, ...] = ()

    filesystem: pyarrow.fs.FileSystem | None = None

    #: Basename glob a file has to match to be part of the set (`*.txt*`).
    #: Matched case-sensitively on every platform (`fnmatchcase`): the default
    #: `fnmatch` folds case on Windows only, which would make a set's contents
    #: depend on where the job ran.
    pattern: str = "*"

    #: Whether a directory root is walked to the bottom or only one level deep.
    recursive: bool = True

    #: Read each root's contents backwards -- the tail of a capture first, or a
    #: rotation whose numbering runs the other way. Which direction is
    #: chronological is the writer's convention, so the class states neither.
    #:
    #: The **roots** keep the order they were given, reversed or not: that
    #: order is the caller's statement about time, and this flag is about what
    #: the store decides.
    reverse: bool = False

    #: What a line's fixed header is; a job may hand over the source of one.
    header_pattern: re.Pattern[bytes] | str | bytes = HEADER_PATTERN

    #: Shape reads land on. None is what the parser fills.
    row: StructField | None = None

    #: IANA zone the wall clock in the headers belongs to, passed to every file.
    timezone: str | None = None

    #: Registry-owned MsgType values shared by every file in the stream.
    msg_type_event_types: Mapping[str, int | str] = dataclass_field(default_factory=dict)

    #: Registry-owned operational MsgTypes excluded from market parsing.
    technical_msg_types: frozenset[str] = frozenset()

    #: Registry-owned plugin codes excluded from market parsing.
    technical_plugin_codes: frozenset[str] = frozenset()

    #: Syntax-only protocol classifier shared by every file in the stream.
    protocol_rules: Any | None = None

    #: Constant columns every row of the capture carries, appended after the
    #: data columns in insertion order -- the same declaration `TextFile` takes,
    #: passed to every file the walk opens, so a set is one shape and not one
    #: per file.
    static_values: Mapping[str, Any] = dataclass_field(default_factory=dict)

    def __post_init__(self) -> None:
        """Resolve one filesystem for every root, and rewrite the roots as paths on it."""
        self.header_pattern = compiled_header(self.header_pattern)
        self.roots = tuple(self.roots)
        if self.filesystem is not None or not self.roots:
            return
        resolved: list[str] = []
        for root in self.roots:
            filesystem, path = resolve(root)
            if self.filesystem is None:
                self.filesystem = filesystem
            elif not self.filesystem.equals(filesystem):
                raise ValueError(
                    f"{root!r} is on {filesystem.type_name}, and this set is already reading "
                    f"{self.filesystem.type_name}; one set is one stream off one store, so read "
                    "each store with its own TextFiles"
                )
            resolved.append(path)
        self.roots = tuple(resolved)

    # -- building -----------------------------------------------------------

    @classmethod
    def from_folder(
        cls,
        source: str | os.PathLike[str],
        filesystem: pyarrow.fs.FileSystem | None = None,
        **declared: Any,
    ) -> TextFiles:
        """Build from one folder, named by URI or by local path.

        The whole of the common case: `TextFiles.from_folder("/var/log/app")`,
        `TextFiles.from_folder("s3://bucket/logs/2026-08-14")`. Anything else
        the set declares -- `pattern`, `recursive`, `reverse`, `timezone`,
        `msg_type_event_types`, `static_values` -- is a keyword here, so a call reads
        as one shape.
        """
        return cls.from_folders([source], filesystem, **declared)

    @classmethod
    def from_folders(
        cls,
        sources: Iterable[str | os.PathLike[str]],
        filesystem: pyarrow.fs.FileSystem | None = None,
        **declared: Any,
    ) -> TextFiles:
        """Build from several roots, read in the order given.

        The order is the caller's and is kept: a set that reads an archive
        directory before a live one is saying which rows come first, and
        sorting the roots would throw that away.

        A root that is a **file** is taken as it is, so a caller holding the
        paths of the logs it wants hands them over here rather than to a second
        builder that would only do the same thing under another name.
        """
        return cls(
            roots=tuple(_root(source, filesystem) for source in sources),
            filesystem=filesystem,
            **declared,
        )

    # -- the dataset ---------------------------------------------------------

    @cached_property
    def static_columns(self) -> tuple[tuple[str, pyarrow.Scalar], ...]:
        """Each static value as an Arrow scalar, in the order it was declared."""
        return static_columns_of(self.static_values)

    @cached_property
    def parsed_field(self) -> StructField:
        """What the parser produces: the row shape, then the constant columns.

        Built the same way a file builds its own, because the set's shape *is*
        the files' -- including the static columns, which the set declares and
        hands to every file it opens.
        """
        return parsed_field_of(self.into_file().into_row().into_field(), self.static_columns)

    def into_struct_field(self) -> StructField:
        """The shape this set holds: the declared one, or what the parser fills."""
        return self.row if self.row is not None else self.parsed_field

    @cached_property
    def schema(self) -> pyarrow.Schema:
        """Arrow schema of the parsed rows -- one schema for every file in the set."""
        return self.parsed_field.into_arrow_schema()

    @property
    def exists(self) -> bool:
        """Whether the set holds a log yet.

        The walk stops at the first one, so this costs one directory listing
        rather than a listing of the tree. A root that is not there answers
        False rather than raising: the refusal belongs to a *read*, which would
        otherwise hand back no rows and call that success, while asking whether
        something exists is exactly the question a missing folder answers.
        """
        try:
            return next(self.into_file_infos(), None) is not None
        except (FileNotFoundError, NotADirectoryError):
            return False

    def create_with_field(self, field: StructField, **kwargs: Any) -> TextFiles:
        """Adopt `field` as this set's shape. Nothing is created on the store.

        A set of logs is discovered, not deployed: the files are written by
        whatever produces them, and creating an empty folder would only make
        `exists` lie about what can be read. Kept idempotent by doing nothing,
        which is what the `Dataset` contract asks of it.
        """
        self.row = field
        return self

    def read_arrow_reader(
        self,
        schema: Any = None,
        *,
        include_regexes: Sequence[str] = (),
        exclude_regexes: Sequence[str] = (),
        start_unix: int | None = None,
        end_unix: int | None = None,
        duration_ns: int | None = None,
        **kwargs: Any,
    ) -> pyarrow.RecordBatchReader:
        """Parse every log in order, cast onto `schema` when one is asked for.

        With none, the reader is the parser's own -- see `into_arrow_reader`
        for the parsing options, which are passed straight through.
        """
        reader = self.into_arrow_reader(
            include_regexes=include_regexes,
            exclude_regexes=exclude_regexes,
            start_unix=start_unix,
            end_unix=end_unix,
            duration_ns=duration_ns,
            **kwargs,
        )
        target = self.target_field(schema)
        if target.arrow_schema.equals(reader.schema):
            return reader
        return target.cast_arrow_reader(reader)

    def append_arrow_reader(self, source: Any, *args: Any, **kwargs: Any) -> int:
        """Refused, for the reason a write is -- and before reading anything.

        The generic append reads the stored key columns first so it can
        anti-join against them, which here means parsing the whole capture
        before failing on the write it cannot do.
        """
        self._refuse()

    def overwrite_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] = True,
        commit_row_size: int | None = None,
    ) -> None:
        """Refused: a set of files has no one file to write into.

        Reading many logs as one stream is well defined; writing back into
        them is not -- the rows would have to be cut across files by a rule
        nobody stated, and the file a line belongs in is a property of when it
        was captured. Write one file with `TextFile`, or a store that owns its
        own files (`IcebergDataset`).
        """
        self._refuse()

    def _refuse(self) -> None:
        """The one refusal every write here raises."""
        raise NotImplementedError(
            f"{type(self).__name__} reads a set of logs and cannot write one: nothing here says "
            "which file a row belongs in; write a file with TextFile, or a dataset that owns its "
            "own files (IcebergDataset)"
        )

    # -- the files ----------------------------------------------------------

    def into_file_infos(self) -> Iterator[pyarrow.fs.FileInfo]:
        """Every log this set holds, in path order, listed a directory at a time.

        The generator is the point: a root is listed only when the walk
        reaches it, so the first rows of a folder of a thousand logs come back
        after one listing rather than after all of them, and a store is never
        asked to materialise every key under a prefix.
        """
        filesystem = self.filesystem
        if filesystem is None:
            return
        for root in self.roots:
            info = filesystem.get_file_info(root)
            if info.type == pyarrow.fs.FileType.Directory:
                yield from self._walk(root)
            elif info.type == pyarrow.fs.FileType.File:
                yield info
            else:
                raise FileNotFoundError(
                    f"no log source at {root!r}: a root that is not there yields no rows, and a "
                    "set that skipped it would read a capture short and report success"
                )

    def into_urls(self) -> Iterator[str]:
        """The path of every log in the set, in the order it is read."""
        return (info.path for info in self.into_file_infos())

    def into_files(self) -> Iterator[TextFile]:
        """Every log as a `TextFile`, carrying this set's declaration.

        Each one is built closed and unopened -- construction resolves nothing
        but the filesystem, which is this set's own -- so walking the set does
        not open a file per name.
        """
        for info in self.into_file_infos():
            yield self.into_file()(
                url=info.path,
                filesystem=self.filesystem,
                header_pattern=self.header_pattern,
                row=self.row,
                timezone=self.timezone,
                msg_type_event_types=self.msg_type_event_types,
                technical_msg_types=self.technical_msg_types,
                technical_plugin_codes=self.technical_plugin_codes,
                protocol_rules=self.protocol_rules,
                static_values=self.static_values,
            )

    def _walk(self, directory: str, seen: set[str] | None = None) -> Iterator[pyarrow.fs.FileInfo]:
        """One directory, in path order, descending as the names come up."""
        seen = set() if seen is None else seen
        identity = self._identity(directory)
        if identity in seen:
            return
        seen.add(identity)
        selector = pyarrow.fs.FileSelector(directory, recursive=False, allow_not_found=True)
        listing = self.filesystem.get_file_info(selector)
        for info in sorted(listing, key=_natural, reverse=self.reverse):
            if info.type == pyarrow.fs.FileType.Directory:
                if self.recursive:
                    yield from self._walk(info.path, seen)
            elif info.type == pyarrow.fs.FileType.File and fnmatch.fnmatchcase(
                info.base_name, self.pattern
            ):
                yield info

    def _identity(self, directory: str) -> str:
        """What makes two listings the same directory rather than two of them.

        Only a local filesystem has symlinks to resolve; an object store has
        prefixes, where the path already *is* the identity. Asking `os.path`
        about a store path would be wrong and slow, so it is asked only about
        the filesystem that has the question.
        """
        if isinstance(self.filesystem, pyarrow.fs.LocalFileSystem):
            return os.path.realpath(directory)
        return directory

    # -- converting ---------------------------------------------------------

    def into_arrow_reader(
        self,
        *,
        include_regexes: Sequence[str] = (),
        exclude_regexes: Sequence[str] = (),
        start_unix: int | None = None,
        end_unix: int | None = None,
        duration_ns: int | None = None,
        **kwargs: Any,
    ) -> pyarrow.RecordBatchReader:
        """Stream the whole set as Arrow record batches, one file open at a time."""
        batches = self.into_arrow_batches(
            include_regexes=include_regexes,
            exclude_regexes=exclude_regexes,
            start_unix=start_unix,
            end_unix=end_unix,
            duration_ns=duration_ns,
            **kwargs,
        )
        return pyarrow.RecordBatchReader.from_batches(self.schema, batches)

    def into_arrow_table(self, **kwargs: Any) -> pyarrow.Table:
        """Read the whole set into one table. Needs all of it to fit in memory."""
        return self.into_arrow_reader(**kwargs).read_all()

    def into_arrow_batches(
        self,
        batch_row_size: int = DEFAULT_BATCH_ROW_SIZE,
        read_byte_size: int = DEFAULT_READ_BYTE_SIZE,
        fold_continuations: bool = True,
        *,
        batch_byte_size: int = DEFAULT_BATCH_BYTE_SIZE,
        include_regexes: Sequence[str] = (),
        exclude_regexes: Sequence[str] = (),
        start_unix: int | None = None,
        end_unix: int | None = None,
        duration_ns: int | None = None,
    ) -> Iterator[pyarrow.RecordBatch]:
        """Parse every log in order without splitting a shared duration window."""
        self._check_open()
        includes = _regexes("include_regexes", include_regexes)
        excludes = _regexes("exclude_regexes", exclude_regexes)
        _validate_read_sizes(batch_row_size, read_byte_size, batch_byte_size)
        _validate_window(start_unix, end_unix, duration_ns)
        batches = self._filtered_batches(
            batch_row_size,
            read_byte_size,
            batch_byte_size,
            fold_continuations,
            includes,
            excludes,
            start_unix,
            end_unix,
        )
        batches = _windowed_batches(
            batches,
            batch_row_size,
            batch_byte_size=batch_byte_size,
            duration_ns=duration_ns,
            start_unix=start_unix,
        )
        return self._release_arrow_batches(batches)

    def _release_arrow_batches(
        self, batches: Iterator[pyarrow.RecordBatch]
    ) -> Iterator[pyarrow.RecordBatch]:
        """Forget a completed stream and close it when its consumer stops."""
        self._check_open()
        self.__dict__.setdefault("_arrow_batches", set()).add(batches)
        try:
            yield from batches
        finally:
            close = getattr(batches, "close", None)
            if close is not None:
                close()
            active = self.__dict__.get("_arrow_batches")
            if active is not None:
                active.discard(batches)

    def _filtered_batches(
        self,
        batch_row_size: int,
        read_byte_size: int,
        batch_byte_size: int,
        fold_continuations: bool,
        include_regexes: Sequence[str],
        exclude_regexes: Sequence[str],
        start_unix: int | None,
        end_unix: int | None,
    ) -> Iterator[pyarrow.RecordBatch]:
        """Filtered per-file batches, leaving set-wide windowing to the caller."""
        for log in self.into_files():
            with log:
                batches = log._filtered_batches(
                    batch_row_size,
                    read_byte_size,
                    batch_byte_size,
                    fold_continuations,
                    include_regexes,
                    exclude_regexes,
                    start_unix,
                    end_unix,
                )
                yield from batches

    def into_byte_chunks(
        self,
        *,
        read_byte_size: int = DEFAULT_READ_BYTE_SIZE,
        compression: str | None = None,
    ) -> Iterator[bytes]:
        """The bytes of every log in order, decoded -- or re-encoded by a codec."""
        self._check_open()
        raw = self._raw_chunks(read_byte_size)
        if compression is None:
            yield from raw
            return
        sink = _Sink()
        with pyarrow.CompressedOutputStream(pyarrow.PythonFile(sink, mode="w"), compression) as out:
            for chunk in raw:
                out.write(chunk)
                yield from sink.drain()
        # The codec writes its trailer on close, which the `with` has just
        # done: draining once more is what makes the stream a whole member.
        yield from sink.drain()

    def into_bytes(self, **kwargs: Any) -> bytes:
        """The whole set as one blob. Needs it to fit in memory."""
        return b"".join(self.into_byte_chunks(**kwargs))

    def _raw_chunks(self, read_byte_size: int) -> Iterator[bytes]:
        """Decoded bytes of every log in order, one open file at a time.

        A separator is held rather than emitted: it belongs *between* two
        files, so a capture whose last log has no trailing newline comes out as
        those bytes and not as those bytes plus one.
        """
        pending = b""
        for log in self.into_files():
            trailing = b""
            with log:
                while chunk := log.read(read_byte_size):
                    if pending:
                        yield pending
                        pending = b""
                    trailing = chunk[-1:]
                    yield chunk
            if trailing and trailing != b"\n":
                pending = b"\n"

    # -- io.BufferedIOBase --------------------------------------------------

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        """False: the set is a stream. Seeking would mean reopening files behind it."""
        return False

    def read(self, size: int | None = -1) -> bytes:
        """Up to `size` bytes of the concatenated, decoded logs."""
        self._check_open()
        want = None if size is None or size < 0 else size
        buffer = self._buffer
        while want is None or len(buffer) < want:
            chunk = next(self._byte_chunks, b"")
            if not chunk:
                break
            buffer += chunk
        if want is None or want >= len(buffer):
            payload = bytes(buffer)
            buffer.clear()
            return payload
        payload = bytes(buffer[:want])
        del buffer[:want]
        return payload

    def read1(self, size: int = -1) -> bytes:
        return self.read(size)

    def readinto(self, buffer: bytearray | memoryview) -> int:
        payload = self.read(len(buffer))
        buffer[: len(payload)] = payload
        return len(payload)

    def readinto1(self, buffer: bytearray | memoryview) -> int:
        return self.readinto(buffer)

    @cached_property
    def _byte_chunks(self) -> Iterator[bytes]:
        """The stream `read` is served from, started on first use."""
        return self.into_byte_chunks()

    @cached_property
    def _buffer(self) -> bytearray:
        """What one read took and the next has not returned yet."""
        return bytearray()

    def close(self) -> None:
        """Close the stream if one was ever started, without starting one.

        Closing either generator raises `GeneratorExit` inside it, which leaves
        the `with` around the file it was reading -- so the open log is closed
        by the same statement that opened it.
        """
        for batches in self.__dict__.pop("_arrow_batches", ()):
            close = getattr(batches, "close", None)
            if close is not None:
                close()
        chunks = self.__dict__.pop("_byte_chunks", None)
        if chunks is not None:
            chunks.close()
        self.__dict__.pop("_buffer", None)
        super().close()

    def _check_open(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file.")


class _Sink(io.RawIOBase):
    """Where a codec writes: the chunks it produced, taken away as they appear.

    `pyarrow.PythonFile` wants a real file object, and a `BufferOutputStream`
    would hold the whole encoded capture -- which is the accumulation
    `into_byte_chunks` exists to avoid. This is the smallest sink that lets
    the bytes leave as they are encoded.
    """

    def __init__(self) -> None:
        self.chunks: list[bytes] = []

    def writable(self) -> bool:
        return True

    def write(self, payload: Any) -> int:
        self.chunks.append(bytes(payload))
        return len(payload)

    def drain(self) -> list[bytes]:
        """Everything written since the last drain, and nothing held after it."""
        chunks, self.chunks = self.chunks, []
        return chunks


def _natural(info: pyarrow.fs.FileInfo) -> tuple[tuple[int, int | str, str], ...]:
    """Sort key for one directory entry, with digit runs compared as numbers."""
    return tuple(
        # `isdecimal`, not `isdigit`: the two disagree on characters like "²",
        # which `\d` does not match but `isdigit` accepts -- and one such name
        # anywhere under a root took the whole walk down with a ValueError.
        (1, int(part), part) if part.isdecimal() else (0, part, "")
        # The base name, not the path: sorting happens inside one directory, so
        # every entry shares the prefix and the order is the same either way --
        # measured, keying the whole path is twice the work and holds the
        # difference in memory as well.
        for part in _DIGITS.split(info.base_name)
        if part != ""
    )


def _root(source: str | os.PathLike[str], filesystem: pyarrow.fs.FileSystem | None) -> str:
    """A source as this set addresses it: a URI, whatever it arrived as."""
    text = os.fspath(source)
    if filesystem is None:
        return Url.from_string(text).into_string()
    if isinstance(filesystem, pyarrow.fs.LocalFileSystem):
        return Url.from_string(text).path
    return text
