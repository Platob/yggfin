"""Filesystem and resource resolution shared by every project subsystem."""

from __future__ import annotations

import functools
import hashlib
import os
import pathlib
import tempfile
import urllib.request
from collections.abc import Mapping

import pyarrow.fs

from rekep.urls import HTTP, Url


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
    with filesystem.open_input_stream(path) as stream:
        return stream.read()


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
    parent = path.rpartition("/")[0]
    if parent:
        filesystem.create_dir(parent, recursive=True)
    with filesystem.open_output_stream(path) as stream:
        stream.write(payload)
    return path


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
        if isinstance(filesystem, pyarrow.fs.LocalFileSystem):
            return path
    elif injected:
        if isinstance(filesystem, pyarrow.fs.LocalFileSystem):
            return path
    identity = f"{id(filesystem)}:{path}" if injected else parsed.into_string()
    suffix = pathlib.PurePosixPath(parsed.path or location).suffix
    target = pathlib.Path(_materialization_directory().name) / (
        hashlib.sha256(identity.encode()).hexdigest() + suffix
    )
    if not target.exists():
        try:
            payload = (
                read_bytes(path, filesystem)
                if filesystem is not None
                else read_bytes(location)
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
