"""Filesystem resolution, cached so object stores are not rebuilt per file."""

from __future__ import annotations

import functools

import pyarrow.fs


@functools.lru_cache(maxsize=256)
def resolve(url: str) -> tuple[pyarrow.fs.FileSystem, str]:
    """`FileSystem.from_uri`, cached on the URL.

    Building an S3 or GCS filesystem walks a credential chain that can itself
    issue HTTP requests (environment, config files, instance metadata), so
    reopening a file must not repeat it. The cache is keyed on the whole URL --
    correct by construction, since `from_uri` is a pure function of it. A
    caller opening *many* files on one bucket should still build the
    filesystem once and pass it explicitly; that skips this lookup entirely.
    """
    return pyarrow.fs.FileSystem.from_uri(url)
