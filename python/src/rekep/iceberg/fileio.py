"""Arrow's FileIO, taught two things: Windows drive letters, and not to fetch
the same immutable file twice.

**The parse fix.** `PyArrowFileIO.parse_location` splits a URI and glues netloc
and path back together, so `file:///C:/warehouse` comes out as `/C:/warehouse`
-- a spelling `pyarrow`'s local filesystem refuses on Windows with
`WinError 123`. The same split hands a bare `C:/warehouse` to `urlparse`, which
reads the drive as a one-letter URI scheme and refuses it as a filesystem. Both
are the standard spellings a Windows path arrives in, so the default FileIO
cannot write a local warehouse there at all.

**The cache.** Every file Iceberg writes below the catalog pointer is immutable
and lives at a name no other write will ever reuse -- manifests, manifest
lists and `metadata.json` versions all carry a UUID minted per attempt. Yet
pyiceberg re-reads them constantly: the manifest list on *every* scan plan (its
own comment calls the re-read intentional), and every manifest on every
`fetch_manifest_entry`. A streaming merge plans a scan per chunk, so on an
object store the same few-KB files are fetched hundreds of times -- measured on
8 merge commits, 104 of 136 GETs were manifests and manifest lists the process
had already read. Immutability makes the fix safe: entries can go cold, never
stale. So this FileIO keeps a bounded, process-wide cache of those files'
bytes, filled on first read *and on write* -- the manifest list a commit just
wrote is the one the next chunk's scan plans from, so write-through means the
steady state fetches nothing at all.

Data files are never cached: they are the bytes worth streaming, and one of
them would evict the whole point.
"""

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

#: The `metadata.json` names Iceberg mints per attempt: a version number, a
#: UUID and the suffix. A name *without* the UUID -- `v3.metadata.json`, which
#: a Hadoop-style catalog writes -- is one two racing writers can both produce
#: with different bytes, so caching it is caching a guess about which won.
_VERSIONED = re.compile(
    r"^\d+-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\.metadata\.json$"
)

#: A path whose first segment is a drive letter -- `C:/x` or `C:\x`.
_DRIVE = re.compile(r"^[A-Za-z]:[/\\]")

#: The slash a URI split leaves in front of one -- `/C:/x`.
_ROOTED_DRIVE = re.compile(r"^/+(?=[A-Za-z]:[/\\])")

#: Decided per host, held as data so a test can exercise the other answer.
_WINDOWS = os.name == "nt"

#: Catalog property naming the cache budget in bytes. `0` opts a catalog out;
#: any other value resizes the shared cache, since the files are shared too.
CACHE_BYTES_PROPERTY = "rekep.io.cache-bytes"

#: Enough for thousands of manifests -- they run single-digit KBs each on the
#: tables this package writes -- and small next to one Arrow table of data.
DEFAULT_CACHE_BYTES = 64 * 1024 * 1024


def _immutable(location: str) -> bool:
    """Whether Iceberg promises never to rewrite the file at `location`.

    Manifests and manifest lists (`.avro`) and metadata versions are written
    once at a UUID-bearing name and referenced forever after; the one mutable
    file near them, a Hadoop catalog's `version-hint.text`, matches neither.
    Data files are immutable too, but caching is for what is fetched
    *repeatedly*, and a data file is read once per scan that wants its rows.

    The UUID is what the promise rests on, so a metadata version has to carry
    one: pyiceberg mints `00007-<uuid>.metadata.json` per attempt, but the
    `v7.metadata.json` a Hadoop-style catalog writes is a name two racing
    writers can both produce, with different bytes. That one is read from the
    store every time, which is the only honest answer about a file whose name
    does not say which write it came from.
    """
    name = location.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    return name.endswith(".avro") or bool(_VERSIONED.match(name))


class ContentCache:
    """Bytes of immutable files, bounded and shared across the process.

    One cache for everything, the way pyiceberg shares its own manifest-file
    cache: the entries are keyed by full location, and a location is unique
    across catalogs, warehouses and threads. Eviction is LRU by total bytes;
    a file bigger than an eighth of the budget is never stored, so one bloated
    manifest cannot evict everything else.

    `stats()` is how a benchmark -- or an operator wondering what their object
    store is being asked -- sees it working: hits, misses, stores, and the
    bytes currently held.
    """

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
    """An output file whose bytes land in the cache as they land in the store.

    Write-through is what makes a streaming write quiet: the manifest list a
    commit writes is exactly the file the next chunk's scan plans from, so
    caching it *now* means that plan never asks the store. The bytes are only
    stored when the stream closes cleanly -- a write abandoned mid-file must
    not leave its half in the cache -- and a failed *commit* is harmless
    either way, because its files sit at names nothing will ever reference.
    """

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

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        if exc_type is not None:
            self._failed = True
        self.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class ArrowFileIO(PyArrowFileIO):
    """`PyArrowFileIO` whose locations survive Windows and whose immutable
    metadata is fetched once.

    Two changes, one per failure mode. Parsing: on Windows, `/C:/x` sheds the
    slash the URI split left in front of the drive, and a bare `C:/x` is a
    local path rather than a URI with scheme `c`; everywhere else the parent's
    answer stands, so a POSIX directory literally named `C:` keeps meaning what
    it says. Fetching: manifests, manifest lists and `metadata.json` versions
    are served from `CONTENT_CACHE` and kept there as they are written, which
    is what keeps a scan-per-chunk write flow from asking an object store for
    the same bytes on every chunk.
    """

    def __init__(self, properties: Properties = EMPTY_DICT) -> None:
        super().__init__(properties=properties)
        budget = properties.get(CACHE_BYTES_PROPERTY)
        self._content_cache: ContentCache | None = CONTENT_CACHE
        if budget is not None:
            if int(budget) <= 0:
                self._content_cache = None
            else:
                CONTENT_CACHE.resize(int(budget))

    @staticmethod
    def parse_location(location: str, properties: Properties = EMPTY_DICT) -> tuple[str, str, str]:
        if _WINDOWS and _DRIVE.match(location):
            return "file", "", location
        scheme, netloc, path = PyArrowFileIO.parse_location(location, properties)
        if _WINDOWS and scheme == "file":
            path = _ROOTED_DRIVE.sub("", path)
        return scheme, netloc, path

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
