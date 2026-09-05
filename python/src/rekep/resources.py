"""Bind resource locations to yggdryl and read required byte sources."""

from __future__ import annotations

import os
import pathlib
import re
import urllib.parse
import urllib.request
from collections.abc import Mapping

import pyarrow.fs
from yggdryl import IOBase, Url

HTTP = frozenset({"http", "https"})
WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")

Location = IOBase | str | os.PathLike[str]


def resource(
    location: Location,
    filesystem: pyarrow.fs.FileSystem | None = None,
    *,
    root: Location | None = None,
) -> IOBase:
    """One location bound to its yggdryl resource.

    An injected filesystem owns its path spelling. Plain local paths become
    absolute; a relative location under ``root`` derives from the bound root
    so URI and injected-filesystem identity are retained.
    """
    if isinstance(location, IOBase):
        if filesystem is not None or root is not None:
            raise ValueError("an IOBase is already bound to its filesystem and root")
        return location

    spelled = os.fspath(location)
    if root is not None and _is_relative(spelled):
        base = root if isinstance(root, IOBase) else resource(root, filesystem)
        return base.joinpath(spelled)
    if filesystem is not None:
        return IOBase.from_fs(filesystem, spelled)

    scheme = _scheme(spelled)
    if scheme in HTTP:
        raise ValueError(f"{scheme} resources are byte streams; read them with read_bytes")
    if scheme == "file":
        return IOBase(Url(spelled).into_path())
    if scheme:
        owner, path = pyarrow.fs.FileSystem.from_uri(spelled)
        return IOBase.from_fs(owner, path)

    path = pathlib.Path(spelled).expanduser().resolve()
    return IOBase(path)


def read_bytes(
    source: str | os.PathLike[str] | urllib.request.Request,
    filesystem: pyarrow.fs.FileSystem | None = None,
    *,
    root: Location | None = None,
    timeout: float | None = None,
    headers: Mapping[str, str] | None = None,
) -> bytes:
    """Read one required resource, decoding its yggdryl content codec."""
    request = source if isinstance(source, urllib.request.Request) else None
    location = request.full_url if request is not None else os.fspath(source)
    if filesystem is None and _scheme(location) in HTTP:
        request = request or urllib.request.Request(location, headers=dict(headers or {}))
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            return response.read()

    opened = resource(location, filesystem, root=root)
    try:
        if not opened.is_file():
            raise FileNotFoundError(location)
        if opened.codec is None:
            return opened.read_bytes()
        decoded = IOBase.from_bytes()
        try:
            opened.decompress_into(decoded)
            return decoded.read_bytes()
        finally:
            decoded.close()
    finally:
        opened.close()


def _scheme(location: str) -> str:
    """A URI scheme, excluding a Windows drive letter."""
    if WINDOWS_DRIVE.match(location):
        return ""
    return urllib.parse.urlsplit(location).scheme.lower()


def _is_relative(location: str) -> bool:
    """Whether ``location`` can be derived from another bound resource."""
    if _scheme(location) or pathlib.PureWindowsPath(location).drive:
        return False
    return not pathlib.PurePosixPath(location).is_absolute()
