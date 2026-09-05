"""PyIceberg file ownership over its native PyArrow streams."""

from __future__ import annotations

import importlib
import os
import re
import threading
from collections.abc import Callable, Iterator, Set
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from typing import Any

import pyarrow.fs
from pyiceberg.io import FileIO, InputFile, OutputFile
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.typedef import EMPTY_DICT, Properties
from yggdryl import Url

DELEGATE_FILE_IO = "rekep.io.delegate-file-io"
TRACKED_FILE_IO = "rekep.iceberg.file_io.TrackedFileIO"

_EXECUTOR_LOCK = threading.Lock()
_WINDOWS = os.name == "nt"
_WINDOWS_LOCAL = re.compile(r"^(?:file:|[A-Za-z]:[\\/]|\\\\)", re.IGNORECASE)


class _OutputTracker(Set[str]):
    """Output locations and workers owned by one transaction."""

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
        """Wait until every inherited PyIceberg worker has finished."""
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


# Catalog commits create fresh FileIO instances, and data writers use
# PyIceberg's shared pool. The context is the one ownership boundary both see.
_OUTPUT_TRACKERS: ContextVar[tuple[_OutputTracker, ...]] = ContextVar(
    "rekep_iceberg_output_trackers", default=()
)


class _ContextExecutor(Executor):
    """Carry the submitting transaction into PyIceberg worker calls."""

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
    """Install one context-preserving wrapper around PyIceberg's pool."""
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
    for outputs in _OUTPUT_TRACKERS.get():
        outputs.add(location)


class IcebergFileIO(PyArrowFileIO):
    """PyIceberg's native streaming FileIO with transaction ownership."""

    @staticmethod
    def parse_location(location: str, properties: Properties = EMPTY_DICT) -> tuple[str, str, str]:
        """Resolve Windows file locations without treating the drive as a scheme."""
        if _WINDOWS and _WINDOWS_LOCAL.match(location):
            return "file", "", os.fspath(Url(location).into_path())
        return PyArrowFileIO.parse_location(location, properties)

    def new_output(self, location: str) -> OutputFile:
        _record_output(location)
        return super().new_output(location)

    def copy_from_local(self, source: str | os.PathLike[str], target: str) -> str:
        """Stream one local stage into this FileIO's configured store."""
        return _stream_local(self, source, target)


class TrackedFileIO(FileIO):
    """A configured PyIceberg FileIO with transaction ownership added."""

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
        """Use the delegate's copy, or stream through its output in bounded chunks."""
        _record_output(target)
        copier = getattr(self.delegate, "copy_from_local", None)
        if callable(copier):
            copier(source, target)
            return target
        return _stream_local(self.delegate, source, target)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.delegate, name)


def _stream_local(file_io: FileIO, source: str | os.PathLike[str], target: str) -> str:
    """Copy a local file through one bounded native output stream."""
    _record_output(target)
    try:
        with (
            open(source, "rb") as incoming,
            file_io.new_output(target).create(overwrite=True) as output,
        ):
            while payload := incoming.read(1 << 22):
                output.write(payload)
    except Exception:
        try:
            file_io.delete(target)
        except FileNotFoundError:
            pass
        raise
    return target


def configured_store(file_io: FileIO, location: str) -> tuple[pyarrow.fs.FileSystem, str]:
    """Return the exact Arrow filesystem and path configured for one location."""
    if isinstance(file_io, TrackedFileIO):
        return configured_store(file_io.delegate, location)
    if isinstance(file_io, PyArrowFileIO):
        scheme, netloc, path = file_io.parse_location(location, file_io.properties)
        return file_io.fs_by_scheme(scheme, netloc), path
    resolver = getattr(file_io, "configured_store", None)
    if callable(resolver):
        filesystem, path = resolver(location)
        if isinstance(filesystem, pyarrow.fs.FileSystem) and isinstance(path, str):
            return filesystem, path
    raise TypeError(
        f"{type(file_io).__name__} cannot expose its configured Arrow store for maintenance"
    )
