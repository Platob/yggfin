"""Filesystem resolution, cached so object stores are not rebuilt per file."""

from __future__ import annotations

import functools

import pyarrow.fs

from rekep.urls import Url


@functools.lru_cache(maxsize=256)
def resolve(url: str) -> tuple[pyarrow.fs.FileSystem, str]:
    """The filesystem a location lives on, and the path on it -- cached."""
    return Url.from_string(url).into_filesystem()
