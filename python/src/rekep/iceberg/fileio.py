"""Arrow's FileIO, taught three things: Windows drive letters, S3 endpoints, and not to fetch the
same immutable file twice."""

from __future__ import annotations

import os
import re
import threading
from collections import OrderedDict
from typing import Any

import pyarrow
from pyiceberg.io import InputFile, InputStream, OutputFile, OutputStream
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.typedef import EMPTY_DICT, Properties

from rekep.urls import S3, Url, properties_of, s3_environment

#: The `metadata.json` names Iceberg mints per attempt: a version number, a
#: UUID and the suffix. A name *without* the UUID -- `v3.metadata.json`, which
#: a Hadoop-style catalog writes -- is one two racing writers can both produce
#: with different bytes, so caching it is caching a guess about which won.
_VERSIONED = re.compile(
    r"^\d+-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\.metadata\.json$"
)

#: Properties naming the locations a catalog was configured with, most
#: specific first -- the first one that says anything is the one read, since
#: a warehouse URL describes the store a `uri` may only point a metastore at.
#: An endpoint and credentials in one of these is how a MinIO catalog is
#: usually spelled.
LOCATION_PROPERTIES = ("warehouse", "location", "uri")

#: Decided per host, held as data so a test can exercise the other answer.
_WINDOWS = os.name == "nt"

#: Catalog property naming the cache budget in bytes. `0` opts a catalog out;
#: any other value resizes the shared cache, since the files are shared too.
CACHE_BYTES_PROPERTY = "rekep.io.cache-bytes"

#: Enough for thousands of manifests -- they run single-digit KBs each on the
#: tables this package writes -- and small next to one Arrow table of data.
DEFAULT_CACHE_BYTES = 64 * 1024 * 1024


def _immutable(location: str) -> bool:
    """Whether Iceberg promises never to rewrite the file at `location`."""
    name = location.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    return name.endswith(".avro") or bool(_VERSIONED.match(name))


class ContentCache:
    """Bytes of immutable files, bounded and shared across the process."""

    def __init__(self, limit: int = DEFAULT_CACHE_BYTES) -> None:
        self.limit = limit
        self._lock = threading.Lock()
        self._entries: OrderedDict[str, bytes] = OrderedDict()
        self._bytes = 0
        self._hits = 0
        self._misses = 0
        self._stores = 0

    def get(self, location: str) -> bytes | None:
        with self._lock:
            data = self._entries.get(location)
            if data is None:
                self._misses += 1
                return None
            self._entries.move_to_end(location)
            self._hits += 1
            return data

    def peek(self, location: str) -> bytes | None:
        """`get` without touching the counters or the recency order."""
        with self._lock:
            return self._entries.get(location)

    def put(self, location: str, data: bytes) -> None:
        if len(data) > self.limit // 8:
            return
        with self._lock:
            self._bytes -= len(self._entries.pop(location, b""))
            self._entries[location] = data
            self._bytes += len(data)
            self._stores += 1
            while self._bytes > self.limit and self._entries:
                _, evicted = self._entries.popitem(last=False)
                self._bytes -= len(evicted)

    def evict(self, location: str) -> None:
        with self._lock:
            self._bytes -= len(self._entries.pop(location, b""))

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._bytes = 0

    def resize(self, limit: int) -> None:
        with self._lock:
            self.limit = limit
            while self._bytes > self.limit and self._entries:
                _, evicted = self._entries.popitem(last=False)
                self._bytes -= len(evicted)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "hits": self._hits,
                "misses": self._misses,
                "stores": self._stores,
                "entries": len(self._entries),
                "bytes": self._bytes,
                "limit": self.limit,
            }


#: The process-wide cache every `ArrowFileIO` shares unless its catalog says
#: `rekep.io.cache-bytes: "0"`.
CONTENT_CACHE = ContentCache()


class CachedInputFile(InputFile):
    """An input file served from the cache, read through it on a miss.

    `open` hands back a `pyarrow.BufferReader` over the cached bytes -- a real
    seekable `NativeFile`, so avro and JSON readers cannot tell the store from
    the memory. A hit also answers `exists` and `__len__` without the HEAD
    they would otherwise cost.
    """

    def __init__(self, inner: InputFile, cache: ContentCache) -> None:
        super().__init__(location=inner.location)
        self._inner = inner
        self._cache = cache

    def __len__(self) -> int:
        data = self._cache.peek(self.location)
        return len(data) if data is not None else len(self._inner)

    def exists(self) -> bool:
        if self._cache.peek(self.location) is not None:
            return True
        return self._inner.exists()

    def open(self, seekable: bool = True) -> InputStream:
        data = self._cache.get(self.location)
        if data is None:
            with self._inner.open() as stream:
                data = stream.read()
            self._cache.put(self.location, data)
        return pyarrow.BufferReader(data)


class CachedOutputFile(OutputFile):
    """An output file whose bytes land in the cache as they land in the store."""

    def __init__(self, inner: OutputFile, cache: ContentCache) -> None:
        super().__init__(location=inner.location)
        self._inner = inner
        self._cache = cache

    def __len__(self) -> int:
        data = self._cache.peek(self.location)
        return len(data) if data is not None else len(self._inner)

    def exists(self) -> bool:
        if self._cache.peek(self.location) is not None:
            return True
        return self._inner.exists()

    def to_input_file(self) -> CachedInputFile:
        return CachedInputFile(self._inner.to_input_file(), self._cache)

    def create(self, overwrite: bool = False) -> OutputStream:
        return _TeeStream(self._inner.create(overwrite), self.location, self._cache)


class _TeeStream:
    """The store's own output stream, with a copy kept for the cache.

    Everything is delegated, so whatever the writer calls -- `tell`, `flush`,
    a property -- behaves exactly as it would on the bare stream; only `write`
    is watched and only `close` decides. An exception seen by `__exit__`, or
    one raised by the store's own `close`, discards the copy: a file that was
    not written whole must not be readable as if it had been.
    """

    def __init__(self, inner: Any, location: str, cache: ContentCache) -> None:
        self._inner = inner
        self._location = location
        self._cache = cache
        self._cap = cache.limit // 8
        self._buffer: bytearray | None = bytearray()
        self._failed = False

    def write(self, data: Any) -> int:
        written = self._inner.write(data)
        if self._buffer is not None:
            self._buffer += data
            if len(self._buffer) > self._cap:
                # `put` refuses anything this big at the door, so going on
                # copying is paying the whole cost of the copy -- and holding a
                # second copy of the file -- for something that is dropped on
                # arrival. Which one is being written decides: the manifest
                # this exists for is single-digit KBs.
                self._buffer = None
        return written

    def close(self) -> None:
        try:
            self._inner.close()
        except Exception:
            self._failed = True
            raise
        finally:
            if not self._failed and self._buffer is not None:
                self._cache.put(self._location, bytes(self._buffer))
            self._buffer = None

    def __enter__(self) -> _TeeStream:
        return self

    def __exit__(self, exc_type: Any, _exc_value: Any, _traceback: Any) -> None:
        if exc_type is not None:
            self._failed = True
        self.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def inferred_properties(properties: Properties) -> Properties:
    """`properties`, plus S3 process and location defaults."""
    environment = s3_environment()
    inferred: dict[str, str] = {}
    normalized = dict(properties)
    endpoint_decided = False
    for name in LOCATION_PROPERTIES:
        location = properties.get(name)
        if not location:
            continue
        url = Url.from_string(str(location))
        if url.scheme not in S3:
            continue
        location_defaults = dict(properties_of(url))
        if endpoint_decided:
            location_defaults.pop("s3.endpoint", None)
        for key, value in location_defaults.items():
            inferred.setdefault(key, value)
        if url.endpoint is not None:
            endpoint_decided = True
        if url.user is not None:
            # A session token belongs to its access-key pair; never combine a
            # portable token with credentials explicitly carried by the URL.
            environment.pop("s3.session-token", None)
        if url.query:
            clean = url.copy()
            clean.query.clear()
            normalized[name] = clean.into_string()
    if endpoint_decided and "s3.endpoint" not in inferred:
        environment.pop("s3.endpoint", None)
    if not environment and not inferred and normalized == properties:
        return properties
    return {**environment, **inferred, **normalized}


class ArrowFileIO(PyArrowFileIO):
    """`PyArrowFileIO` whose locations are read the way this package reads every location, and
    whose immutable metadata is fetched once."""

    def __init__(self, properties: Properties = EMPTY_DICT) -> None:
        super().__init__(properties=inferred_properties(properties))
        budget = properties.get(CACHE_BYTES_PROPERTY)
        self._content_cache: ContentCache | None = CONTENT_CACHE
        if budget is not None:
            if int(budget) <= 0:
                self._content_cache = None
            else:
                CONTENT_CACHE.resize(int(budget))

    @staticmethod
    def parse_location(location: str, properties: Properties = EMPTY_DICT) -> tuple[str, str, str]:
        """Where a location is: its scheme, its netloc, and the path on it."""
        url = Url.from_string(location)
        if _WINDOWS and url.scheme == "file":
            return "file", "", url.path
        if url.scheme in S3:
            return url.scheme, url.bucket, url.store_path
        return PyArrowFileIO.parse_location(location, properties)

    def new_input(self, location: str) -> InputFile:
        inner = super().new_input(location)
        if self._content_cache is None or not _immutable(location):
            return inner
        return CachedInputFile(inner, self._content_cache)

    def new_output(self, location: str) -> OutputFile:
        inner = super().new_output(location)
        if self._content_cache is None or not _immutable(location):
            return inner
        return CachedOutputFile(inner, self._content_cache)

    def delete(self, location: str | InputFile | OutputFile) -> None:
        # Evicted first, whether or not the store's delete then fails: a
        # cached copy of a file the caller wants gone is the copy that lies.
        name = location.location if isinstance(location, (InputFile, OutputFile)) else location
        CONTENT_CACHE.evict(name)
        super().delete(location)
