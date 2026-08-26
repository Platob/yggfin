"""An Arrow reader that owns the stream producing its batches."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, Self

import pyarrow


class _OwnedIterator:
    """An iterator whose final reference releases its source and owner."""

    def __init__(
        self,
        batches: Iterator[pyarrow.RecordBatch],
        release: Callable[[], None],
    ) -> None:
        self._batches: Iterator[pyarrow.RecordBatch] | None = batches
        self._release: Callable[[], None] | None = release

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> pyarrow.RecordBatch:
        batches = self._batches
        if batches is None:
            raise StopIteration
        try:
            return next(batches)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        batches, release = self._batches, self._release
        self._batches = self._release = None
        close = getattr(batches, "close", None)
        try:
            if close is not None:
                close()
        finally:
            if release is not None:
                release()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class OwnedRecordBatchReader(pyarrow.RecordBatchReader):
    """A record-batch reader that closes its Python source and owner."""

    def __init__(
        self,
        schema: pyarrow.Schema,
        batches: Iterator[pyarrow.RecordBatch],
        release: Callable[[], None],
    ) -> None:
        owned = _OwnedIterator(batches, release)
        self._schema = schema
        self._batches: _OwnedIterator | None = owned
        self._reader: pyarrow.RecordBatchReader | None = pyarrow.RecordBatchReader.from_batches(
            schema, owned
        ).cast(schema)
        self._closed = False

    @property
    def schema(self) -> pyarrow.Schema:
        return self._schema

    def _native(self) -> pyarrow.RecordBatchReader:
        if self._reader is None:
            raise ValueError("reader is closed or its Arrow C stream was transferred")
        return self._reader

    def read_next_batch(self) -> pyarrow.RecordBatch:
        try:
            return self._native().read_next_batch()
        except BaseException:
            self.close()
            raise

    def read_next_batch_with_custom_metadata(self) -> Any:
        try:
            return self._native().read_next_batch_with_custom_metadata()
        except BaseException:
            self.close()
            raise

    def read_all(self) -> pyarrow.Table:
        try:
            return self._native().read_all()
        finally:
            self.close()

    def read_pandas(self, **options: Any) -> Any:
        try:
            return self._native().read_pandas(**options)
        finally:
            self.close()

    def iter_batches_with_custom_metadata(self) -> Iterator[Any]:
        try:
            yield from self._native().iter_batches_with_custom_metadata()
        finally:
            self.close()

    def cast(self, target_schema: pyarrow.Schema) -> pyarrow.RecordBatchReader:
        return type(self).from_reader(self._native().cast(target_schema), self.close)

    @classmethod
    def from_reader(cls, reader: pyarrow.RecordBatchReader, release: Callable[[], None]) -> Self:
        """Own an existing reader and cascade its release."""
        return cls(reader.schema, reader, release)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        reader, batches = self._reader, self._batches
        self._reader = None
        self._batches = None
        try:
            if reader is not None:
                reader.close()
        finally:
            if batches is not None:
                batches.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> pyarrow.RecordBatch:
        return self.read_next_batch()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        reader = self._native()
        capsule = reader.__arrow_c_stream__(requested_schema)
        # The capsule now owns the native reader. Drop our references without
        # closing them; its final C-stream release destroys `_OwnedIterator`,
        # whose finalizer closes the Python source and owner even on early stop.
        self._reader = None
        self._batches = None
        self._closed = True
        return capsule

    def _export_to_c(self, out_ptr: int) -> None:
        self._native()._export_to_c(out_ptr)  # noqa: SLF001
        self._reader = None
        self._batches = None
        self._closed = True
