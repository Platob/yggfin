"""One URL and the Arrow filesystem that serves its path."""

from __future__ import annotations

import datetime
import errno
import fnmatch
import os
import re
import urllib.parse
from collections.abc import Iterator
from typing import Any, Self

import pyarrow
import pyarrow.fs

from rekep.urls import LOCAL, S3, SCHEME, Url

_TIME_PATTERN = re.compile(r"(?:\{(?:year|month|day)\}|%7[bB](?:year|month|day)%7[dD])")


class ArrowPath(os.PathLike[str]):
    """An immutable path paired with the Arrow filesystem that addresses it.

    An injected filesystem reads `location` as its own path. Without one, the
    URL resolves the filesystem and its filesystem-relative path together.
    """

    __slots__ = ("_filesystem", "_path", "_uri", "_url")

    def __init__(
        self,
        location: str | os.PathLike[str],
        filesystem: pyarrow.fs.FileSystem | None = None,
        *,
        filesystem_path: str | None = None,
    ) -> None:
        if isinstance(location, ArrowPath):
            self._url = location._url.copy()
            self._filesystem = location.filesystem if filesystem is None else filesystem
            path = location.path if filesystem_path is None else filesystem_path
            self._path = str(path).replace("\\", "/")
            self._uri = location.uri
            return
        uri = os.fspath(location)
        parsed = _url(uri)
        if filesystem is None:
            filesystem, resolved = parsed.into_filesystem()
        else:
            resolved = uri if filesystem_path is None else filesystem_path
        self._url = parsed
        self._filesystem = filesystem
        self._path = str(resolved).replace("\\", "/")
        self._uri = uri

    @classmethod
    def from_url(
        cls,
        location: str | os.PathLike[str],
        filesystem: pyarrow.fs.FileSystem | None = None,
    ) -> Self:
        """Build from a normalized URI while retaining its filesystem path.

        A bare name supplied with a filesystem is already a path on that
        filesystem. A URI is resolved through its parsed store path instead.
        """
        spelled = os.fspath(location)
        url = Url.from_string(spelled)
        if filesystem is None:
            filesystem, path = url.into_filesystem()
        elif SCHEME.match(spelled):
            path = url.store_path
        elif isinstance(filesystem, pyarrow.fs.LocalFileSystem):
            path = url.path
        else:
            path = spelled
        return cls.from_parts(url, filesystem, path, uri=url.into_string())

    @classmethod
    def from_parts(
        cls,
        url: Url,
        filesystem: pyarrow.fs.FileSystem,
        filesystem_path: str,
        *,
        uri: str | None = None,
    ) -> Self:
        """Build from already resolved parts without parsing or resolving again."""
        built = cls.__new__(cls)
        built._url = url.copy()
        built._filesystem = filesystem
        built._path = str(filesystem_path).replace("\\", "/")
        built._uri = _uri_of(url) if uri is None else str(uri)
        return built

    @property
    def filesystem(self) -> pyarrow.fs.FileSystem:
        """The one Arrow filesystem serving this path."""
        return self._filesystem

    @property
    def url(self) -> Url:
        """The parsed URL as a detached value."""
        return self._url.copy()

    @property
    def path(self) -> str:
        """The path as `filesystem` addresses it."""
        return self._path

    @property
    def uri(self) -> str:
        """The location spelling supplied by the caller."""
        return self._uri

    @property
    def location(self) -> str:
        """PyIceberg's name for `uri`."""
        return self._uri

    @property
    def name(self) -> str:
        """The final path segment."""
        return self._path.rstrip("/").rsplit("/", 1)[-1]

    @property
    def suffix(self) -> str:
        """The final extension, lowercased."""
        _, dot, suffix = self.name.rpartition(".")
        return f".{suffix.lower()}" if dot else ""

    @property
    def stem(self) -> str:
        """The final segment without its last extension."""
        suffix = self.suffix
        return self.name[: -len(suffix)] if suffix else self.name

    @property
    def parent(self) -> Self:
        """The path one segment above this one; a root is its own parent."""
        url = self._url.copy().parent()
        if url.path == self._url.path:
            return self
        return type(self).from_parts(
            url,
            self._filesystem,
            _parent_path(self._path),
            uri=_parent_uri(self._uri),
        )

    def joinpath(self, *segments: str | os.PathLike[str]) -> Self:
        """Return a descendant while keeping the same filesystem and URL authority."""
        parts = tuple(os.fspath(segment).replace("\\", "/") for segment in segments)
        url = self._url.copy().join(*parts)
        return type(self).from_parts(
            url,
            self._filesystem,
            _joined_path(self._path, *parts),
            uri=_joined_uri(self._uri, *parts),
        )

    def with_name(self, name: str) -> Self:
        """Return a sibling carrying `name`."""
        if not name or "/" in name or "\\" in name:
            raise ValueError(f"a path name is one non-empty segment, got {name!r}")
        return self.parent.joinpath(name)

    def with_suffix(self, suffix: str) -> Self:
        """Return this path with its final extension replaced."""
        if suffix and (not suffix.startswith(".") or "/" in suffix or "\\" in suffix):
            raise ValueError(f"suffix must be empty or start with '.', got {suffix!r}")
        return self.with_name(f"{self.stem}{suffix}")

    def __truediv__(self, segment: str | os.PathLike[str]) -> Self:
        return self.joinpath(segment)

    def __fspath__(self) -> str:
        return self._path

    def __str__(self) -> str:
        return self._uri

    def __repr__(self) -> str:
        return f"ArrowPath({self._url.masked!r}, filesystem={self._filesystem.type_name!r})"

    def same_path(self, other: ArrowPath) -> bool:
        """Whether both paths address the same name on equal filesystems."""
        return self._path == other._path and (
            self._filesystem is other._filesystem or self._filesystem.equals(other._filesystem)
        )

    def resolve(self, root: str | os.PathLike[str] = ".") -> Self:
        """Resolve a relative local path while leaving every remote URI untouched."""
        if self._url.scheme not in LOCAL or not isinstance(
            self._filesystem, pyarrow.fs.LocalFileSystem
        ):
            return self
        resolved = self._url.resolve(root).replace("\\", "/")
        if resolved == self._path:
            return self
        return type(self).from_parts(
            Url.from_string(resolved),
            self._filesystem,
            resolved,
            uri=resolved,
        )

    def info(self) -> pyarrow.fs.FileInfo:
        """Arrow's current facts about the path, including `NotFound`."""
        try:
            return self._filesystem.get_file_info(self._path)
        except OSError as error:
            translated = _path_error(error, "get file info", self._uri)
            if translated is not error:
                raise translated from error
            raise

    def exists(self) -> bool:
        """Whether any file or directory exists at this path."""
        try:
            return self.info().type != pyarrow.fs.FileType.NotFound
        except FileNotFoundError:
            return False

    def is_file(self) -> bool:
        """Whether this path is a file."""
        try:
            return self.info().type == pyarrow.fs.FileType.File
        except FileNotFoundError:
            return False

    def is_dir(self) -> bool:
        """Whether this path is a directory."""
        try:
            return self.info().type == pyarrow.fs.FileType.Directory
        except FileNotFoundError:
            return False

    def ls(self, recursive: bool = False) -> Iterator[Self]:
        """Yield existing descendants in path order; a missing path is empty."""
        for path, _ in self.ls_with_info(recursive=recursive):
            yield path

    def ls_with_info(
        self,
        recursive: bool = False,
        *,
        strict: bool = False,
    ) -> Iterator[tuple[Self, pyarrow.fs.FileInfo]]:
        """Yield paths with the listing facts; absence is empty unless strict."""
        selector = pyarrow.fs.FileSelector(
            self._path,
            recursive=recursive,
            allow_not_found=not strict,
        )
        try:
            listing = self._filesystem.get_file_info(selector)
        except OSError as error:
            translated = _path_error(error, "list directory", self._uri)
            if strict or not _missing_error(error):
                if translated is not error:
                    raise translated from error
                raise
            # Some object-store adapters ignore `allow_not_found`; absence is
            # still an empty listing rather than a failed discovery pass.
            return
        for info in sorted(listing, key=lambda item: item.path):
            yield self._from_filesystem_path(info.path), info

    def iterdir(self, *, recursive: bool = False) -> Iterator[Self]:
        """Yield children in path order, optionally below every descendant."""
        yield from self.ls(recursive=recursive)

    def glob(self, pattern: str) -> Iterator[Self]:
        """Yield paths matching one POSIX-style relative pattern."""
        normalized = str(pattern).replace("\\", "/").lstrip("/")
        if not normalized:
            return
        recursive = "/" in normalized or "**" in normalized.split("/")
        for candidate in self.ls(recursive=recursive):
            relative = _relative_path(candidate.path, self._path)
            if _glob_match(relative, normalized):
                yield candidate

    def rglob(self, pattern: str) -> Iterator[Self]:
        """Yield recursive matches below this path."""
        yield from self.glob(f"**/{str(pattern).lstrip('/')}")

    def _from_filesystem_path(self, filesystem_path: str) -> Self:
        relative = _relative_path(filesystem_path, self._path)
        return self.joinpath(relative)

    def mkdir(self, *, parents: bool = True) -> Self:
        """Create this directory and return the same path."""
        try:
            self._filesystem.create_dir(self._path, recursive=parents)
        except OSError as error:
            translated = _path_error(error, "create directory", self._uri)
            if translated is not error:
                raise translated from error
            raise
        return self

    def replace(self, target: Self) -> Self:
        """Move this path over `target` on the same filesystem."""
        if not (
            self._filesystem is target.filesystem or self._filesystem.equals(target.filesystem)
        ):
            raise ValueError("a replacement must stay on the same filesystem")
        try:
            self._filesystem.move(self._path, target.path)
        except OSError as error:
            translated = _path_error(error, "replace file", self._uri)
            if translated is not error:
                raise translated from error
            raise
        return target

    def delete(self, *, strict: bool = False) -> bool:
        """Delete this file; report acceptance or backend-reported absence.

        `strict=True` preserves a missing-path error at boundaries where an
        absent source is data loss rather than an already-complete cleanup.
        Object stores may accept deletion without revealing prior existence.
        """
        try:
            self._filesystem.delete_file(self._path)
        except OSError as error:
            translated = _path_error(error, "delete file", self._uri)
            if not _missing_error(error):
                if translated is not error:
                    raise translated from error
                raise
            if strict:
                if translated is not error:
                    raise translated from error
                raise
            return False
        return True

    def open_input_file(self) -> pyarrow.NativeFile:
        """Open a seekable input file."""
        try:
            return self._filesystem.open_input_file(self._path)
        except OSError as error:
            translated = _path_error(error, "open file", self._uri)
            if translated is not error:
                raise translated from error
            raise

    def open_input_stream(
        self,
        compression: str | None = "detect",
        *,
        buffer_size: int | None = None,
    ) -> pyarrow.NativeFile:
        """Open a sequential input stream, optionally decoding it."""
        options: dict[str, Any] = {"compression": compression}
        if buffer_size is not None:
            options["buffer_size"] = buffer_size
        try:
            return self._filesystem.open_input_stream(self._path, **options)
        except OSError as error:
            translated = _path_error(error, "open file", self._uri)
            if translated is not error:
                raise translated from error
            raise

    def open_input(
        self,
        *,
        seekable: bool = True,
        compression: str | None = None,
        buffer_size: int | None = None,
    ) -> pyarrow.NativeFile:
        """Open input with the seekability and codec the caller needs."""
        if compression is None and seekable:
            return self.open_input_file()
        return self.open_input_stream(compression=compression, buffer_size=buffer_size)

    def open_output_stream(
        self,
        compression: str | None = "detect",
        *,
        buffer_size: int | None = None,
    ) -> pyarrow.NativeFile:
        """Open a truncating output stream, optionally encoding it."""
        options: dict[str, Any] = {"compression": compression}
        if buffer_size is not None:
            options["buffer_size"] = buffer_size
        try:
            return self._filesystem.open_output_stream(self._path, **options)
        except OSError as error:
            translated = _path_error(error, "create file", self._uri)
            if translated is not error:
                raise translated from error
            raise

    def open_append(
        self,
        compression: str | None = "detect",
        *,
        buffer_size: int | None = None,
    ) -> pyarrow.NativeFile:
        """Open a backend append stream without emulating unsupported append."""
        options: dict[str, Any] = {"compression": compression}
        if buffer_size is not None:
            options["buffer_size"] = buffer_size
        try:
            return self._filesystem.open_append_stream(self._path, **options)
        except OSError as error:
            translated = _path_error(error, "append file", self._uri)
            if translated is not error:
                raise translated from error
            raise

    def open_output(
        self,
        *,
        overwrite: bool = False,
        compression: str | None = "detect",
        buffer_size: int | None = None,
    ) -> pyarrow.NativeFile:
        """Create output, refusing an existing path unless allowed."""
        if not overwrite and self.exists():
            raise FileExistsError(f"Cannot create file, already exists: {self._uri}")
        try:
            return self.open_output_stream(compression, buffer_size=buffer_size)
        except OSError as error:
            # `open_output_stream` has already normalized backend-specific
            # missing and permission spellings.
            if not isinstance(error, FileNotFoundError):
                raise
            parent = self.parent
            if parent.same_path(self):
                raise
            # Object stores need no directory object, while local and mock
            # filesystems do. Try the write first so the common store path
            # pays no metadata request and creates no synthetic prefix.
            parent.mkdir()
            return self.open_output_stream(compression, buffer_size=buffer_size)

    def open(
        self,
        mode: str = "rb",
        *,
        seekable: bool = True,
        compression: str | None = None,
        buffer_size: int | None = None,
    ) -> pyarrow.NativeFile:
        """Open a binary reader, truncating writer, or exclusive writer."""
        if mode == "rb":
            return self.open_input(
                seekable=seekable,
                compression=compression,
                buffer_size=buffer_size,
            )
        if mode in {"wb", "xb"}:
            return self.open_output(
                overwrite=mode == "wb",
                compression=compression,
                buffer_size=buffer_size,
            )
        raise ValueError(f"ArrowPath supports only 'rb', 'wb', and 'xb', got {mode!r}")

    def read_bytes(
        self,
        *,
        strict: bool = False,
        compression: str | None = None,
    ) -> bytes | None:
        """Read the complete file, or None when it is absent.

        `strict=True` preserves a missing-path error at required-input
        boundaries. The read itself decides; no preceding metadata request can
        race with the open.
        """
        try:
            with self.open_input(
                seekable=compression is None,
                compression=compression,
            ) as stream:
                return stream.read()
        except OSError as error:
            translated = _path_error(error, "open file", self._uri)
            if not _missing_error(error):
                if translated is not error:
                    raise translated from error
                raise
            if strict:
                if translated is not error:
                    raise translated from error
                raise
            return None

    def write_bytes(self, payload: bytes, *, overwrite: bool = True) -> Self:
        """Write complete bytes and return this path."""
        with self.open_output(overwrite=overwrite, compression=None) as stream:
            stream.write(payload)
        return self

    @property
    def has_time_pattern(self) -> bool:
        """Whether the URL path carries an exact year, month, or day token."""
        return _TIME_PATTERN.search(self._url.path) is not None

    def at_time(self, value: datetime.date | datetime.datetime) -> Self:
        """Render URL-path time tokens with zero-padded calendar values."""
        if not self.has_time_pattern:
            return self
        day = value.date() if isinstance(value, datetime.datetime) else value
        replacements = {
            "year": f"{day.year:04d}",
            "month": f"{day.month:02d}",
            "day": f"{day.day:02d}",
        }
        url = self._url.copy()
        url.path = _render_time(url.path, replacements)
        path = _render_time(self._path, replacements)
        prefix, uri_path, suffix = _split_uri_path(self._uri)
        uri = f"{prefix}{_render_time(uri_path, replacements)}{suffix}"
        return type(self).from_parts(url, self._filesystem, path, uri=uri)

    def iter_times(
        self,
        start: datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime,
    ) -> Iterator[Self]:
        """Expand an inclusive chronological calendar window once per path."""
        lower = start.date() if isinstance(start, datetime.datetime) else start
        upper = end.date() if isinstance(end, datetime.datetime) else end
        if upper < lower:
            raise ValueError("end must not precede start")
        if not self.has_time_pattern:
            yield self
            return
        seen: set[str] = set()
        current = lower
        while current <= upper:
            rendered = self.at_time(current)
            if rendered.uri not in seen:
                seen.add(rendered.uri)
                yield rendered
            current += datetime.timedelta(days=1)


def _url(uri: str) -> Url:
    """Parse once, retaining S3 object-key escapes as literal key bytes."""
    parsed = Url.from_string(uri, decode=False)
    return parsed if parsed.scheme in S3 else Url.from_string(uri)


def _uri_of(url: Url) -> str:
    """Render a URL without consuming path escapes or time-template braces."""
    safe = "/:%{}"
    if url.scheme in LOCAL:
        path = urllib.parse.quote(url.store_path, safe=safe)
        return f"file:///{path.lstrip('/')}"
    credentials = ""
    if url.user is not None:
        credentials = urllib.parse.quote(url.user, safe="")
        if url.password is not None:
            credentials += ":" + urllib.parse.quote(url.password, safe="")
        credentials += "@"
    path = urllib.parse.quote(url.path, safe=safe)
    uri = f"{url.scheme}://{credentials}{url.netloc}/{path.lstrip('/')}"
    if url.query:
        uri += "?" + urllib.parse.urlencode(url.query)
    return uri


def _joined_path(path: str, *segments: str) -> str:
    absolute = path.startswith("/")
    parts = [path.strip("/"), *(segment.strip("/") for segment in segments)]
    joined = "/".join(part for part in parts if part)
    return f"/{joined}" if absolute else joined


def _parent_path(path: str) -> str:
    stripped = path.rstrip("/")
    if not stripped or stripped == "/":
        return path or ""
    head, separator, _ = stripped.rpartition("/")
    if not separator:
        return ""
    if not head and path.startswith("/"):
        return "/"
    if re.fullmatch(r"[A-Za-z]:", head):
        return f"{head}/"
    return head


def _relative_path(path: str, base: str) -> str:
    prefix = base.rstrip("/")
    if not prefix:
        return path.lstrip("/")
    expected = f"{prefix}/"
    if path == prefix:
        return ""
    if not path.startswith(expected):
        raise ValueError(f"{path!r} is not below {base!r}")
    return path[len(expected) :]


def _glob_match(path: str, pattern: str) -> bool:
    """Match path segments, reserving `**` for zero or more whole segments."""
    parts = path.split("/") if path else []
    positions = {0}
    for token in pattern.split("/"):
        if token == "**":
            first = min(positions, default=len(parts) + 1)
            positions = set(range(first, len(parts) + 1))
        else:
            positions = {
                position + 1
                for position in positions
                if position < len(parts) and fnmatch.fnmatchcase(parts[position], token)
            }
        if not positions:
            return False
    return len(parts) in positions


def _missing_error(error: OSError) -> bool:
    """Whether one filesystem error says the addressed path is absent."""
    if isinstance(error, (FileNotFoundError, NotADirectoryError)) or error.errno in {
        errno.ENOENT,
        errno.ENOTDIR,
    }:
        return True
    message = str(error).lower()
    return "path does not exist" in message or "no such file or directory" in message


def _path_error(error: OSError, action: str, location: str) -> OSError:
    """Normalize backend-specific missing and permission spellings."""
    if isinstance(error, (FileNotFoundError, NotADirectoryError, PermissionError)):
        return error
    if _missing_error(error):
        return FileNotFoundError(f"Cannot {action}, does not exist: {location}")
    if error.errno == errno.EACCES or "aws error [code 15]" in str(error).lower():
        return PermissionError(f"Cannot {action}, access denied: {location}")
    return error


def _render_time(text: str, replacements: dict[str, str]) -> str:
    """Render raw or percent-encoded time tokens in one path string."""
    for name, rendered in replacements.items():
        text = re.sub(rf"(?:\{{{name}\}}|%7[bB]{name}%7[dD])", rendered, text)
    return text


def _joined_uri(uri: str, *segments: str) -> str:
    prefix, path, suffix = _split_uri_path(uri)
    joined = _joined_path(path, *segments)
    if "://" in prefix and joined and not joined.startswith("/"):
        joined = f"/{joined}"
    return f"{prefix}{joined}{suffix}"


def _parent_uri(uri: str) -> str:
    prefix, path, suffix = _split_uri_path(uri)
    return f"{prefix}{_parent_path(path)}{suffix}"


def _split_uri_path(uri: str) -> tuple[str, str, str]:
    head, query_mark, query = uri.partition("?")
    scheme = re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:(?://[^/]*)?", head)
    start = 0 if scheme is None else scheme.end()
    suffix = f"{query_mark}{query}" if query_mark else ""
    return head[:start], head[start:], suffix
