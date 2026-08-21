"""Filesystem resolution, cached so object stores are not rebuilt per file."""

from __future__ import annotations

import functools

import pyarrow.fs

from rekep.urls import Url


@functools.lru_cache(maxsize=256)
def resolve(url: str) -> tuple[pyarrow.fs.FileSystem, str]:
    """The filesystem a location lives on, and the path on it -- cached.

    One parser reads every location here (`rekep.urls.Url`), so a Windows
    drive letter survives, an S3 endpoint is not mistaken for a bucket, and a
    secret with a colon in it arrives intact. Arrow still owns every scheme it
    reads correctly; this is the two it does not.

    Building an S3 or GCS filesystem walks a credential chain that can itself
    issue HTTP requests (environment, config files, instance metadata), so
    reopening a file must not repeat it. The cache is keyed on the whole URL --
    correct by construction, since the resolution is a pure function of it. A
    caller opening *many* files on one bucket should still build the
    filesystem once and pass it explicitly; that skips this lookup entirely.
    """
    return Url.from_string(url).into_filesystem()
