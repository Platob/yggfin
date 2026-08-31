"""Filesystem and resource resolution shared by every project subsystem."""

from __future__ import annotations

import functools
import hashlib
import os
import pathlib
import tempfile
import urllib.request
from collections.abc import Mapping
from typing import Any, Self

import pyarrow.fs

from rekep.arrow_path import ArrowPath
from rekep.urls import HTTP, Url

# Raw bytes moved from an object store are copied in the same request-sized
# chunks the text reader uses. The source may be compressed; this layer must
# never decode it before the local Arrow stream opens the codec.
SPILL_COPY_BYTE_SIZE = 1 << 22


class _ArrowFile:
    """One path on an Arrow filesystem, with the small input-file contract we use."""

    def __init__(self, location: str, path: str, filesystem: pyarrow.fs.FileSystem) -> None:
        self.arrow_path = ArrowPath(location, filesystem, filesystem_path=path)

    @property
    def location(self) -> str:
        return self.arrow_path.location

    @property
    def _path(self) -> str:
        return self.arrow_path.path

    @property
    def _filesystem(self) -> pyarrow.fs.FileSystem:
        return self.arrow_path.filesystem

    def open(self, seekable: bool = True) -> pyarrow.NativeFile:
        return self.arrow_path.open_input(seekable=seekable)

    def exists(self) -> bool:
        return self.arrow_path.is_file()

    def __len__(self) -> int:
        info = self.arrow_path.info()
        if info.type == pyarrow.fs.FileType.NotFound:
            raise FileNotFoundError(self.location)
        return info.size


class ArrowFile:
    """A lazily opened Arrow file that owns any temporary local materialization.

    This PyArrow-only owner keeps text parsing independent of the optional
    Iceberg dependency. `ArrowFileIO` -- the one Iceberg FileIO -- subclasses
    it, so both paths share one open, spill, and close lifecycle.
    """

    opened: Any | None
    temporary: bool

    def __init__(
        self,
        location: str | os.PathLike[str] | None = None,
        filesystem: pyarrow.fs.FileSystem | None = None,
        *,
        opened: Any | None = None,
        temporary: bool = False,
    ) -> None:
        if opened is not None and location is not None:
            raise ValueError("pass either location or opened, not both")
        self.opened = opened
        self.temporary = temporary
        self._temporary_deleted = False
        if location is not None:
            self.opened = self._new_openable(os.fspath(location), filesystem)

    @classmethod
    def from_location(
        cls,
        location: str | os.PathLike[str],
        filesystem: pyarrow.fs.FileSystem | None = None,
    ) -> Self:
        """Bind a new owner to `location` without opening its stream."""
        return cls(location=location, filesystem=filesystem)

    @property
    def location(self) -> str | None:
        """The bound location, or None for an unbound FileIO factory."""
        if self.opened is None:
            return None
        location = getattr(self.opened, "location", None)
        return None if location is None else str(location)

    @property
    def filesystem(self) -> pyarrow.fs.FileSystem | None:
        """The bound Arrow filesystem, without resolving or opening anything."""
        parts = openable_parts(self.opened)
        return None if parts is None else parts[0]

    @property
    def path(self) -> str | None:
        """The bound path as its Arrow filesystem addresses it."""
        parts = openable_parts(self.opened)
        return None if parts is None else parts[1]

    def at(
        self,
        location: str | os.PathLike[str],
        filesystem: pyarrow.fs.FileSystem | None = None,
    ) -> Self:
        """Return this owner when already bound there, otherwise a bound peer."""
        spelling = os.fspath(location)
        if self.opened is not None:
            parts = openable_parts(self.opened)
            if (
                parts is not None
                and parts[1] == spelling
                and (filesystem is None or parts[0] is filesystem or parts[0].equals(filesystem))
            ):
                return self
        return self._spawn(self._new_openable(spelling, filesystem), temporary=False)

    def _new_openable(self, location: str, filesystem: pyarrow.fs.FileSystem | None = None) -> Any:
        if filesystem is None:
            filesystem, path = resolve(location)
        else:
            path = location
        return _ArrowFile(location=location, path=path, filesystem=filesystem)

    def _spawn(self, opened: Any, *, temporary: bool) -> Self:
        return type(self)(opened=opened, temporary=temporary)

    def open(self, *, seekable: bool = True, compression: str | None = None) -> pyarrow.NativeFile:
        """Open the bound file once, optionally decoding it as a stream."""
        if self.opened is None:
            raise ValueError("ArrowFile is not bound to a file")
        self._close_stream()
        parts = openable_parts(self.opened)
        owned = getattr(self.opened, "arrow_path", None)
        if parts is None and not hasattr(self.opened, "open"):
            raw = self.opened
            stream = raw if compression is None else pyarrow.CompressedInputStream(raw, compression)
        elif compression is None:
            stream = self.opened.open(seekable=seekable)
        elif parts is None:
            raw = self.opened.open(seekable=False)
            stream = pyarrow.CompressedInputStream(raw, compression)
        else:
            filesystem, path = parts
            resource = owned if isinstance(owned, ArrowPath) else ArrowPath(path, filesystem)
            stream = resource.open_input_stream(compression=compression)
        self.__dict__["_stream"] = stream
        return stream

    def spill(
        self,
        local: str | os.PathLike[str] | None = None,
        *,
        temporary: bool = False,
    ) -> Self | None:
        """Return a local owner for raw bytes, or self when already local.

        Persistent spills use one deterministic size-validated cache path.
        Temporary spills use a uniquely owned path so closing one reader cannot
        delete a file another reader is still decoding.
        """
        parts = openable_parts(self.opened)
        if parts is None:
            raise ValueError("ArrowFile is not bound to an Arrow-backed file")
        filesystem, path = parts
        if is_local_filesystem(filesystem):
            return self
        target = spill_path(
            path,
            filesystem,
            local,
            identity=self._spill_identity(path, filesystem),
            temporary=temporary,
        )
        if target is None:
            return None
        opened = self._local_openable(target)
        return self._spawn(opened, temporary=temporary)

    def _local_openable(self, target: str) -> Any:
        return _ArrowFile(target, target, pyarrow.fs.LocalFileSystem())

    def _spill_identity(self, path: str, filesystem: pyarrow.fs.FileSystem) -> str | None:
        return None

    def _close_stream(self) -> None:
        stream = self.__dict__.pop("_stream", None)
        if stream is not None:
            stream.close()

    def close(self) -> None:
        """Close the stream, then remove a temporary spill exactly once."""
        self._close_stream()
        if not self.temporary or self._temporary_deleted:
            return
        parts = openable_parts(self.opened)
        if parts is not None:
            filesystem, path = parts
            ArrowPath(path, filesystem).delete()
        self._temporary_deleted = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


@functools.lru_cache(maxsize=256)
def resolve(url: str) -> tuple[pyarrow.fs.FileSystem, str]:
    """The filesystem a location lives on, and the path on it -- cached."""
    return Url.from_string(url).into_filesystem()


def read_bytes(
    source: str | os.PathLike[str] | urllib.request.Request,
    filesystem: pyarrow.fs.FileSystem | None = None,
    *,
    timeout: float | None = None,
    headers: Mapping[str, str] | None = None,
) -> bytes:
    """Read one resource through HTTP or the Arrow filesystem it names."""
    request = source if isinstance(source, urllib.request.Request) else None
    location = request.full_url if request is not None else os.fspath(source)
    if filesystem is None:
        parsed = Url.from_string(location)
        if parsed.scheme in HTTP:
            request = request or urllib.request.Request(location, headers=dict(headers or {}))
            with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
                return response.read()
        filesystem, path = resolve(location)
    else:
        path = location
    resource = ArrowPath(
        location,
        filesystem,
        filesystem_path=path,
    )
    # This helper predates missing-safe reads: its source stays required, and
    # it keeps Arrow's suffix codec detection.
    payload = resource.read_bytes(strict=True, compression="detect")
    if payload is None:  # pragma: no cover - `strict=True` raises instead
        raise FileNotFoundError(location)
    return payload


def write_bytes(
    payload: bytes,
    target: str | os.PathLike[str],
    filesystem: pyarrow.fs.FileSystem | None = None,
) -> str:
    """Write one resource through Arrow and return its path on that filesystem."""
    location = os.fspath(target)
    if filesystem is None:
        parsed = Url.from_string(location)
        if parsed.scheme in HTTP:
            raise OSError(f"HTTP resource {parsed.masked!r} is read-only")
        filesystem, path = resolve(location)
    else:
        path = location
    ArrowPath(location, filesystem, filesystem_path=path).write_bytes(payload)
    return path


def is_local_filesystem(filesystem: pyarrow.fs.FileSystem) -> bool:
    """Whether `filesystem`, including a subtree wrapper, stays on this host."""
    return _filesystem_local_path(filesystem, "") is not None


def spill_path(
    source: str | os.PathLike[str],
    filesystem: pyarrow.fs.FileSystem | None = None,
    local: str | os.PathLike[str] | None = None,
    *,
    identity: str | None = None,
    temporary: bool = False,
) -> str | None:
    """Materialize a remote file under a deterministic local cache path.

    `local` names a cache directory. With none, the process-local resource
    directory is used. Persistent targets are reused by identity and size;
    temporary targets are unique so one owner can safely delete its copy. A
    local source comes back unchanged, and a missing remote never serves an
    older cached copy.
    """
    location = os.fspath(source)
    injected = filesystem is not None
    path = location
    if filesystem is None:
        filesystem, path = resolve(location)
    local_path = _filesystem_local_path(filesystem, path)
    if local_path is not None:
        return local_path

    source_path = ArrowPath(location, filesystem, filesystem_path=path)
    info = source_path.info()
    if info.type == pyarrow.fs.FileType.NotFound:
        return None
    if info.type != pyarrow.fs.FileType.File:
        raise IsADirectoryError(f"spill source is not a file: {location}")

    identity = identity or _spill_identity(location, path, filesystem, injected)
    suffix = Url.from_string(location).suffix or Url.from_string(path).suffix
    directory = pathlib.Path(local or _materialization_directory().name)
    digest = hashlib.sha256(identity.encode()).hexdigest()
    target = directory / f"{digest}{suffix}"
    local_filesystem = pyarrow.fs.LocalFileSystem()
    if not temporary:
        cached = ArrowPath(target, local_filesystem).info()
        if info.size >= 0 and cached.type == pyarrow.fs.FileType.File and cached.size == info.size:
            return os.fspath(target)

    directory.mkdir(parents=True, exist_ok=True)
    descriptor, scratch_name = tempfile.mkstemp(
        prefix=f".{digest}.", suffix=f"{suffix}.tmp", dir=directory
    )
    os.close(descriptor)
    scratch = pathlib.Path(scratch_name)
    try:
        pyarrow.fs.copy_files(
            path,
            os.fspath(scratch),
            source_filesystem=filesystem,
            destination_filesystem=local_filesystem,
            chunk_size=SPILL_COPY_BYTE_SIZE,
            use_threads=False,
        )
        copied = ArrowPath(scratch, local_filesystem).info()
        if copied.type != pyarrow.fs.FileType.File or (info.size >= 0 and copied.size != info.size):
            raise OSError(f"spill of {location!r} copied {copied.size} bytes; expected {info.size}")
        if temporary:
            target = scratch.with_suffix("")
        os.replace(scratch, target)
    finally:
        scratch.unlink(missing_ok=True)
    return os.fspath(target)


def openable_parts(opened: Any | None) -> tuple[pyarrow.fs.FileSystem, str] | None:
    """The Arrow filesystem and path behind a core or PyIceberg input file.

    How a caller holding a FileIO's own input file reaches the store *that*
    FileIO is configured for, rather than resolving the location again.
    """
    while opened is not None and hasattr(opened, "_inner"):
        opened = opened._inner
    owned = getattr(opened, "arrow_path", None)
    if isinstance(owned, ArrowPath):
        return owned.filesystem, owned.path
    filesystem = getattr(opened, "_filesystem", None)
    path = getattr(opened, "_path", None)
    if isinstance(filesystem, pyarrow.fs.FileSystem) and isinstance(path, str):
        return filesystem, path
    return None


def _filesystem_local_path(filesystem: pyarrow.fs.FileSystem, path: str) -> str | None:
    """Resolve a path through local subtree mounts, or say it is remote."""
    while isinstance(filesystem, pyarrow.fs.SubTreeFileSystem):
        path = "/".join(
            part for part in (filesystem.base_path.rstrip("/"), path.lstrip("/")) if part
        )
        filesystem = filesystem.base_fs
    return path if isinstance(filesystem, pyarrow.fs.LocalFileSystem) else None


def _spill_identity(
    location: str,
    path: str,
    filesystem: pyarrow.fs.FileSystem,
    injected: bool,
) -> str:
    """A cache identity that does not alias two stores carrying one path."""
    if not injected:
        parsed = Url.from_string(location).copy()
        parsed.scheme = parsed.transport
        parsed.user = None
        parsed.password = None
        return parsed.into_string()
    try:
        # Native filesystems make their connection settings pickle arguments.
        # They can include credentials, so the value is only ever hashed.
        settings = repr(filesystem.__reduce__()[1:])
    except (AttributeError, TypeError):
        # An injected mock or custom filesystem may have no stable serialized
        # identity. Its object identity still prevents two live stores from
        # sharing a cache entry inside this process.
        settings = str(id(filesystem))
    return f"{filesystem.type_name}\0{settings}\0{path}"


def local_path(
    source: str | os.PathLike[str],
    filesystem: pyarrow.fs.FileSystem | None = None,
    *,
    missing_ok: bool = False,
) -> str:
    """An OS-local path for a resource, copied once only when it is remote."""
    location = os.fspath(source)
    parsed = Url.from_string(location)
    injected = filesystem is not None
    path = location
    if not injected and parsed.scheme not in HTTP:
        filesystem, path = resolve(location)
    if filesystem is not None:
        materialized = _filesystem_local_path(filesystem, path)
        if materialized is not None:
            return materialized
    identity = f"{id(filesystem)}:{path}" if injected else parsed.into_string()
    suffix = parsed.suffix
    target = pathlib.Path(_materialization_directory().name) / (
        hashlib.sha256(identity.encode()).hexdigest() + suffix
    )
    if not target.exists():
        try:
            payload = (
                read_bytes(path, filesystem) if filesystem is not None else read_bytes(location)
            )
        except FileNotFoundError:
            if missing_ok:
                return os.fspath(target)
            raise
        scratch = target.with_suffix(target.suffix + ".tmp")
        scratch.write_bytes(payload)
        scratch.replace(target)
    return os.fspath(target)


@functools.cache
def _materialization_directory() -> tempfile.TemporaryDirectory[str]:
    """Process-local resource cache, removed automatically at interpreter exit."""
    return tempfile.TemporaryDirectory(prefix="rekep-resources-")
