"""Arrow FileIO with portable locations, remote spills, and immutable-content caching."""

from __future__ import annotations

import importlib
import itertools
import os
import re
import threading
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Set
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from typing import Any

import pyarrow
from pyiceberg.io import FileIO, InputFile, InputStream, OutputFile, OutputStream
from pyiceberg.io.pyarrow import PyArrowFile, PyArrowFileIO
from pyiceberg.typedef import EMPTY_DICT, Properties

from rekep.filesystems import ArrowFile
from rekep.urls import S3, Url, s3_environment

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

#: Table properties naming where a table's own bytes are written, when it does
#: not write them under its location. Canonicalised with the location and read
#: with it, because all three name objects on the same store.
STORAGE_PROPERTIES = ("write.data.path", "write.metadata.path")

#: Decided per host, held as data so a test can exercise the other answer.
_WINDOWS = os.name == "nt"

#: Catalog property naming the cache budget in bytes. `0` opts a catalog out;
#: any other value resizes the shared cache, since the files are shared too.
CACHE_BYTES_PROPERTY = "rekep.io.cache-bytes"

#: Enough for thousands of manifests -- they run single-digit KBs each on the
#: tables this package writes -- and small next to one Arrow table of data.
DEFAULT_CACHE_BYTES = 64 * 1024 * 1024

DELEGATE_FILE_IO = "rekep.io.delegate-file-io"
TRACKED_FILE_IO = "rekep.arrow_file_io.TrackedFileIO"

_EXECUTOR_LOCK = threading.Lock()


class _OutputTracker(Set[str]):
    """Paths and worker futures owned by one transaction context."""

    def __init__(self) -> None:
        self._paths: set[str] = set()
        self._futures: set[Future[Any]] = set()
        self._lock = threading.Lock()

    def add(self, path: str) -> None:
        with self._lock:
            self._paths.add(path)

    def watch(self, future: Future[Any]) -> None:
        with self._lock:
            self._futures.add(future)

    def settle(self) -> None:
        """Wait until this context has no submitted worker still writing."""
        while True:
            with self._lock:
                futures = tuple(self._futures)
            if not futures:
                return
            for future in futures:
                try:
                    future.result()
                except BaseException:
                    pass
            with self._lock:
                self._futures.difference_update(future for future in futures if future.done())

    def snapshot(self) -> set[str]:
        with self._lock:
            return set(self._paths)

    def __contains__(self, path: object) -> bool:
        with self._lock:
            return path in self._paths

    def __iter__(self) -> Iterator[str]:
        return iter(self.snapshot())

    def __len__(self) -> int:
        with self._lock:
            return len(self._paths)


# Catalog commits load a fresh FileIO for the next metadata JSON. A context
# tracker, rather than instance state, observes that writer without mixing
# concurrent commits in other threads or tasks.
_OUTPUT_TRACKERS: ContextVar[tuple[_OutputTracker, ...]] = ContextVar(
    "rekep_arrow_file_io_output_trackers", default=()
)


class _ContextExecutor(Executor):
    """Keep the submitting context around PyIceberg's worker functions."""

    def __init__(self, executor: ThreadPoolExecutor) -> None:
        self.executor = executor
        self._max_workers = executor._max_workers  # noqa: SLF001

    def submit(
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future[Any]:
        context = copy_context()
        trackers = _OUTPUT_TRACKERS.get()
        future = self.executor.submit(context.run, fn, *args, **kwargs)
        for tracker in trackers:
            tracker.watch(future)
        return future

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        self.executor.shutdown(wait=wait, cancel_futures=cancel_futures)


def _propagate_worker_context() -> None:
    """Install one context-preserving wrapper around PyIceberg's shared pool."""
    from pyiceberg.utils.concurrent import ExecutorFactory

    with _EXECUTOR_LOCK:
        executor = ExecutorFactory.get_or_create()
        if isinstance(executor, ThreadPoolExecutor):
            ExecutorFactory._instance = _ContextExecutor(executor)  # noqa: SLF001


@contextmanager
def track_outputs() -> Iterator[_OutputTracker]:
    """Collect every output location opened in this commit context."""
    _propagate_worker_context()
    outputs = _OutputTracker()
    token = _OUTPUT_TRACKERS.set((*_OUTPUT_TRACKERS.get(), outputs))
    try:
        yield outputs
    finally:
        _OUTPUT_TRACKERS.reset(token)
        outputs.settle()


def _record_output(location: str) -> None:
    """Attach one output to every transaction context inherited by this call."""
    for outputs in _OUTPUT_TRACKERS.get():
        outputs.add(location)


class TrackedFileIO(FileIO):
    """A custom PyIceberg FileIO with transaction output ownership added."""

    def __init__(self, properties: Properties = EMPTY_DICT) -> None:
        super().__init__(properties)
        implementation = properties.get(DELEGATE_FILE_IO)
        if not implementation or implementation == TRACKED_FILE_IO:
            raise ValueError(f"{DELEGATE_FILE_IO!r} must name the wrapped FileIO")
        module_name, separator, class_name = implementation.rpartition(".")
        if not separator:
            raise ValueError(f"py-io-impl must be a full class path, got {implementation!r}")
        delegate_properties = {**properties, "py-io-impl": implementation}
        file_io = getattr(importlib.import_module(module_name), class_name)
        self.delegate: FileIO = file_io(delegate_properties)

    def new_input(self, location: str) -> InputFile:
        return self.delegate.new_input(location)

    def new_output(self, location: str) -> OutputFile:
        _record_output(location)
        return self.delegate.new_output(location)

    def delete(self, location: str | InputFile | OutputFile) -> None:
        self.delegate.delete(location)

    def copy_from_local(self, source: str | os.PathLike[str], target: str) -> str:
        """Use a delegate's native copy, or a bounded stream when it has none."""
        _record_output(target)
        copier = getattr(self.delegate, "copy_from_local", None)
        if callable(copier):
            copier(source, target)
            return target
        with (
            open(source, "rb") as incoming,
            self.delegate.new_output(target).create(overwrite=True) as output,
        ):
            while payload := incoming.read(1 << 22):
                output.write(payload)
        return target

    def __getattr__(self, name: str) -> Any:
        return getattr(self.delegate, name)


def _immutable(location: str) -> bool:
    """Whether Iceberg promises never to rewrite the file at `location`."""
    name = Url.from_string(location).name
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
        with self._lock:
            if len(data) > self.limit // 8:
                return
            self._bytes -= len(self._entries.pop(location, b""))
            self._entries[location] = data
            self._bytes += len(data)
            self._stores += 1
            while self._bytes > self.limit and self._entries:
                _, evicted = self._entries.popitem(last=False)
                self._bytes -= len(evicted)

    def accepts(self, size: int) -> bool:
        """Whether one file fits the current per-entry budget."""
        with self._lock:
            return size <= self.limit // 8

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

    `open` hands cacheable misses back as a `pyarrow.BufferReader` over cached
    bytes -- a real seekable `NativeFile`, so avro and JSON readers cannot tell
    the store from memory. Oversized misses keep the underlying stream. A hit
    also answers `exists` and `__len__` without the HEAD they would otherwise
    cost.
    """

    def __init__(self, inner: InputFile, cache: ContentCache, key: str | None = None) -> None:
        super().__init__(location=inner.location)
        self._inner = inner
        self._cache = cache
        self._key = key or inner.location

    def __len__(self) -> int:
        data = self._cache.peek(self._key)
        return len(data) if data is not None else len(self._inner)

    def exists(self) -> bool:
        if self._cache.peek(self._key) is not None:
            return True
        return self._inner.exists()

    def open(self, seekable: bool = True) -> InputStream:
        data = self._cache.get(self._key)
        if data is None:
            if not self._cache.accepts(len(self._inner)):
                return self._inner.open(seekable)
            with self._inner.open() as stream:
                data = stream.read()
            self._cache.put(self._key, data)
        return pyarrow.BufferReader(data)


class CachedOutputFile(OutputFile):
    """An output file whose bytes land in the cache as they land in the store."""

    def __init__(self, inner: OutputFile, cache: ContentCache, key: str | None = None) -> None:
        super().__init__(location=inner.location)
        self._inner = inner
        self._cache = cache
        self._key = key or inner.location

    def __len__(self) -> int:
        data = self._cache.peek(self._key)
        return len(data) if data is not None else len(self._inner)

    def exists(self) -> bool:
        if self._cache.peek(self._key) is not None:
            return True
        return self._inner.exists()

    def to_input_file(self) -> CachedInputFile:
        return CachedInputFile(self._inner.to_input_file(), self._cache, self._key)

    def create(self, overwrite: bool = False) -> OutputStream:
        return _TeeStream(self._inner.create(overwrite), self._key, self._cache)


class _TeeStream:
    """The store's own output stream, with a copy kept for the cache.

    Everything is delegated, so whatever the writer calls -- `tell`, `flush`,
    a property -- behaves exactly as it would on the bare stream; only `write`
    is watched and only `close` decides. An exception seen by `__exit__`, or
    one raised by the store's own `close`, discards the copy: a file that was
    not written whole must not be readable as if it had been.
    """

    def __init__(self, inner: Any, key: str, cache: ContentCache) -> None:
        self._inner = inner
        self._key = key
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
                self._cache.put(self._key, bytes(self._buffer))
            self._buffer = None

    def __enter__(self) -> _TeeStream:
        return self

    def __exit__(self, exc_type: Any, _exc_value: Any, _traceback: Any) -> None:
        if exc_type is not None:
            self._failed = True
        self.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def canonical_location(location: str) -> str:
    """An S3 location containing only its scheme, bucket, and object key."""
    url = spelled(location)
    return url.canonical if url.scheme in S3 else location


def spelled(location: str) -> Url:
    """A location whose object key is the text that named it, escapes kept.

    Iceberg escapes a partition value into the path -- `v=a%2Fb` -- and every
    location this FileIO is handed names an object it wrote, so the escape *is*
    the key rather than a spelling of `a/b`: decoded it would name an object
    carrying a directory level no manifest ever recorded, which a read misses
    and the orphan sweep deletes as the live file it could not match.
    """
    return Url.from_string(location, decode=False)


def _location_properties(locations: Iterable[str]) -> tuple[dict[str, str], bool, bool]:
    """Shared settings named by explicit S3 locations."""
    inferred: dict[str, str] = {}
    endpoint_decided = False
    credentials_decided = False
    for location in locations:
        url = Url.from_string(location)
        if url.scheme not in S3:
            continue
        for key, value in url.into_properties().items():
            if key in inferred and inferred[key] != value:
                raise ValueError(f"conflicting {key!r} across explicit S3 locations")
            inferred[key] = value
        endpoint_decided = endpoint_decided or url.endpoint is not None
        credentials_decided = credentials_decided or url.user is not None
    return inferred, endpoint_decided, credentials_decided


def location_properties(properties: Properties, *, locations: Iterable[str] = ()) -> Properties:
    """`properties` with explicit S3 locations made portable."""
    normalized = dict(properties)
    declared: list[str] = []
    for name in LOCATION_PROPERTIES:
        location = properties.get(name)
        if not location:
            continue
        declared.append(str(location))
        normalized[name] = canonical_location(str(location))
    inferred, _, _ = _location_properties(itertools.chain(declared, locations))
    return {**inferred, **normalized}


#: Iceberg's own server-side-encryption settings, which nothing under this can
#: send. `pyarrow.fs.S3FileSystem` has no parameter for one and drops an
#: `x-amz-server-side-encryption` handed to `default_metadata`; PyIceberg reads
#: none of these names in either of its FileIOs. A store encrypts what this
#: package writes through its *bucket's* default encryption instead, which also
#: decrypts on read -- see `docs/storage/iceberg.md`.
SSE_PROPERTIES = ("s3.sse.type", "s3.sse.key", "s3.sse.md5")

#: The one value of them this can honour, because it asks for nothing.
SSE_NONE = {"s3.sse.type": "none"}


def _check_encryption(properties: Properties) -> None:
    """Refuse an encryption this cannot request, rather than writing plaintext.

    A catalog carrying `s3.sse.type` is saying its objects must be encrypted,
    and a layer that ignores it writes them in the clear and reports success --
    which is the one failure a reader of the table can never see.
    """
    requested = {name: properties[name] for name in SSE_PROPERTIES if name in properties}
    if not requested or requested == SSE_NONE:
        return
    raise ValueError(
        f"{sorted(requested)} asks for server-side encryption that neither pyarrow's S3 "
        "filesystem nor pyiceberg can send, so setting it would write plaintext and report "
        "success; turn on the bucket's default encryption, which encrypts every object this "
        "writes and decrypts every one it reads, or name a FileIO that can send it with "
        f"{DELEGATE_FILE_IO!r}"
    )


def inferred_properties(properties: Properties, *, locations: Iterable[str] = ()) -> Properties:
    """`properties`, plus S3 process and explicit-location defaults.

    **The one precedence rule, stated here and nowhere else.** Lowest to
    highest: the process environment (`s3_environment`), then what an explicit
    location says (`Url.into_properties` over every `LOCATION_PROPERTIES` value
    and every location handed in), then the properties a caller wrote down. A
    location that names its own store or its own credentials also suppresses
    the portable default for that one setting, because half a store's
    configuration from one place and half from another reaches neither.
    """
    _check_encryption(properties)
    locations = tuple(locations)
    declared = [str(location) for name in LOCATION_PROPERTIES if (location := properties.get(name))]
    _, endpoint_decided, credentials_decided = _location_properties(
        itertools.chain(declared, locations)
    )
    normalized = location_properties(properties, locations=locations)
    environment = s3_environment()
    if credentials_decided:
        # A session token belongs to its access-key pair; never combine a
        # portable token with credentials explicitly carried by the URL.
        environment.pop("s3.session-token", None)
    if endpoint_decided and "s3.endpoint" not in normalized:
        environment.pop("s3.endpoint", None)
    if not environment and normalized == properties:
        return properties
    return {**environment, **normalized}


class ArrowFileIO(ArrowFile, PyArrowFileIO):
    """`PyArrowFileIO` whose locations are read the way this package reads every location, and
    whose immutable metadata is fetched once."""

    def __init__(
        self,
        properties: Properties = EMPTY_DICT,
        *,
        location: str | os.PathLike[str] | None = None,
        filesystem: pyarrow.fs.FileSystem | None = None,
        opened: InputFile | None = None,
        temporary: bool = False,
    ) -> None:
        PyArrowFileIO.__init__(self, properties=inferred_properties(properties))
        budget = properties.get(CACHE_BYTES_PROPERTY)
        self._content_cache: ContentCache | None = CONTENT_CACHE
        if budget is not None:
            if int(budget) <= 0:
                self._content_cache = None
            else:
                CONTENT_CACHE.resize(int(budget))
        #: One filesystem per store a location described, so a sweep over a
        #: table's files does not rebuild one per file.
        self._described: dict[str, pyarrow.fs.FileSystem] = {}
        ArrowFile.__init__(
            self,
            location=location,
            filesystem=filesystem,
            opened=opened,
            temporary=temporary,
        )

    @staticmethod
    def parse_location(location: str, properties: Properties = EMPTY_DICT) -> tuple[str, str, str]:
        """Where a location is: its scheme, its netloc, and the path on it."""
        url = spelled(location)
        if _WINDOWS and url.scheme == "file":
            return "file", "", url.path
        if url.scheme in S3:
            return url.scheme, url.bucket, url.store_path
        return PyArrowFileIO.parse_location(location, properties)

    def _new_openable(
        self, location: str, filesystem: pyarrow.fs.FileSystem | None = None
    ) -> InputFile:
        if filesystem is not None:
            return PyArrowFile(location=location, path=location, fs=filesystem)
        return self.new_input(location)

    def _spawn(self, opened: InputFile, *, temporary: bool) -> ArrowFileIO:
        return type(self)(self.properties, opened=opened, temporary=temporary)

    def _local_openable(self, target: str) -> PyArrowFile:
        return PyArrowFile(location=target, path=target, fs=pyarrow.fs.LocalFileSystem())

    def _spill_identity(self, path: str, filesystem: pyarrow.fs.FileSystem) -> str | None:
        # The store and the path, and no credentials: two keys reading one
        # object read the same bytes, so a spill of them is one local file.
        opened = self.opened
        if opened is None:
            return None
        url = spelled(str(opened.location))
        return "\0".join((*self._store_of(url), path)) if url.scheme else None

    def content_identity(self, location: str) -> str:
        """A cache key scoped to the S3-compatible store serving a location.

        The access key is in it where the spill identity leaves it out: this
        caches what a *reader* is allowed to see, and two keys on one store may
        be shown different objects under one name.
        """
        url = spelled(location)
        if url.scheme not in S3:
            return location
        access_key = str(url.user or self.properties.get("s3.access-key-id", ""))
        region = str(url.region or self.properties.get("s3.region", ""))
        return "\0".join((*self._store_of(url), access_key, region, url.key))

    def _store_of(self, url: Url) -> tuple[str, str, str]:
        """`(transport, endpoint, bucket)` -- which store a location is on.

        The transport and not the caller's spelling, so `s3a://` and `s3://`
        naming one object share one entry; and the location fills what it says
        over what the catalog configured, the same precedence
        `_described_filesystem` reads a file with.
        """
        endpoint = "" if url.scheme not in S3 else str(url.endpoint or self.endpoint or "")
        return url.transport, endpoint, url.bucket

    @property
    def endpoint(self) -> str:
        """The store this FileIO was configured for, or nothing."""
        return str(self.properties.get("s3.endpoint", ""))

    def _described_filesystem(self, location: str) -> pyarrow.fs.FileSystem | None:
        """The store a location names itself, when it names one.

        `parse_location` hands PyIceberg the *bucket* as the netloc, so an
        endpoint, a port or credentials written into the location would be
        discarded and the file opened against a default AWS filesystem. A table
        this package wrote carries a canonical location and never comes here;
        one written by another tool, or before those settings moved onto the
        catalog, can -- and reading it has to reach the store it names.

        The catalog fills what the location leaves unsaid, and the location
        wins where they disagree.
        """
        url = Url.from_string(location)
        if url.scheme not in S3 or (url.endpoint is None and url.user is None):
            return None
        # Everything but the key, so one filesystem serves every file on a
        # store rather than one being built per file.
        key = self.content_identity(location).rsplit("\0", 1)[0]
        filesystem = self._described.get(key)
        if filesystem is None:
            described = PyArrowFileIO({**self.properties, **url.into_properties()})
            filesystem = described.fs_by_scheme(url.scheme, url.bucket)
            self._described[key] = filesystem
        return filesystem

    def _described_file(self, location: str) -> PyArrowFile | None:
        """That store's own handle on the location, or None where it has none."""
        filesystem = self._described_filesystem(location)
        if filesystem is None:
            return None
        return PyArrowFile(fs=filesystem, location=location, path=spelled(location).store_path)

    def new_input(self, location: str) -> InputFile:
        # `is None`, not `or`: an input file's truthiness is its length, and
        # asking for that is a HEAD request against the store.
        described = self._described_file(location)
        inner = super().new_input(location) if described is None else described
        if self._content_cache is None or not _immutable(location):
            return inner
        return CachedInputFile(inner, self._content_cache, self.content_identity(location))

    def new_output(self, location: str) -> OutputFile:
        _record_output(location)
        described = self._described_file(location)
        inner = super().new_output(location) if described is None else described
        if self._content_cache is None or not _immutable(location):
            return inner
        return CachedOutputFile(inner, self._content_cache, self.content_identity(location))

    def copy_from_local(self, source: str | os.PathLike[str], target: str) -> str:
        """Copy one local file to a location through this configured Arrow filesystem."""
        source_path = os.path.abspath(os.fspath(source))
        source_filesystem = pyarrow.fs.LocalFileSystem()
        source_info = source_filesystem.get_file_info(source_path)
        if source_info.type != pyarrow.fs.FileType.File:
            raise FileNotFoundError(source_path)

        output = self.new_output(target)
        filesystem = getattr(output, "_filesystem", None)
        path = getattr(output, "_path", None)
        if not isinstance(filesystem, pyarrow.fs.FileSystem) or not isinstance(path, str):
            raise TypeError(f"{type(output).__name__} does not expose an Arrow filesystem")
        parent = path.rpartition("/")[0]
        if parent and filesystem.type_name == "local":
            filesystem.create_dir(parent, recursive=True)
        try:
            pyarrow.fs.copy_files(
                source_path,
                path,
                source_filesystem=source_filesystem,
                destination_filesystem=filesystem,
                chunk_size=1 << 22,
                use_threads=False,
            )
            copied = filesystem.get_file_info(path)
            if copied.type != pyarrow.fs.FileType.File or copied.size != source_info.size:
                raise OSError(
                    f"copy to {target!r} wrote {copied.size} bytes; expected {source_info.size}"
                )
        except Exception:
            try:
                filesystem.delete_file(path)
            except FileNotFoundError:
                pass
            raise
        return target

    def delete(self, location: str | InputFile | OutputFile) -> None:
        # Evicted first, whether or not the store's delete then fails: a
        # cached copy of a file the caller wants gone is the copy that lies.
        name = location.location if isinstance(location, (InputFile, OutputFile)) else location
        CONTENT_CACHE.evict(self.content_identity(name))
        filesystem = self._described_filesystem(name)
        if filesystem is None:
            super().delete(location)
            return
        filesystem.delete_file(self.parse_location(name)[2])
