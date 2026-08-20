"""What a dataset is: something a stream of Arrow batches comes out of and goes into."""

from __future__ import annotations

import abc
from collections.abc import Iterator, Sequence
from typing import Any

import pyarrow

from rekep.convert import Convertible
from rekep.fields import Field, StructField


class Dataset(Convertible, abc.ABC):
    """A stored data product, read and written as Arrow, whatever stores it.

    Three things make one, and everything else here is built from them:

    - `into_struct_field()` -- the shape it holds, as a `StructField`. What the
      data *is*: names, types, nullability, keys, partitioning.
    - `read_arrow_reader(schema)` -- a stream out of it, cast onto `schema`
      when one is given and handed over as the store produced it when not.
    - `write_arrow_reader(source, ...)` -- a stream into it, cast onto this
      dataset's shape first, appended or merged, one commit per chunk.

    Readers and writers are streams on purpose: a dataset is the one thing here
    that is bigger than memory, so nothing about this interface may require the
    whole of it at once. `read_arrow_table` and `write_arrow_table` are there
    for when it does fit and the caller says so.

    A dataset is a `Convertible` dataclass, so an implementation's
    configuration is also a document: `IcebergDataset.from_yaml("logs.yaml")`.
    """

    # -- what it holds ------------------------------------------------------

    @abc.abstractmethod
    def into_struct_field(self) -> StructField:
        """The shape this dataset holds."""

    def into_arrow_schema(self) -> pyarrow.Schema:
        """That shape as an Arrow schema."""
        return self.into_struct_field().into_arrow_schema()

    def target_field(self, schema: pyarrow.Schema | StructField | None = None) -> StructField:
        """The shape a cast should land on: `schema` if given, else ours.

        Every read and write takes an optional schema, and they all mean the
        same three things by it -- a field, an Arrow schema, or nothing at all
        -- so none of them decides that for itself.
        """
        if schema is None:
            return self.into_struct_field()
        if isinstance(schema, StructField):
            return schema
        return Field.from_arrow_schema(schema)

    def merge_columns(self, merge_by: bool | Sequence[str] | None) -> list[str]:
        """Columns a write merges on: the primary key for True, else what is named.

        `False`/`None`/`[]` all mean "append", which is what makes `merge_by` a
        single argument rather than a flag and a list.
        """
        if not merge_by:
            return []
        if merge_by is True:
            keys = self.into_struct_field().primary_keys()
            if not keys:
                raise ValueError(
                    f"{type(self).__name__} cannot merge on its primary key: no member declares "
                    "one; mark it with Field.primary_key() or name the columns to merge on"
                )
            return keys
        return list(merge_by)

    # -- reading ------------------------------------------------------------

    @abc.abstractmethod
    def read_arrow_reader(
        self, schema: pyarrow.Schema | StructField | None = None, **kwargs: Any
    ) -> pyarrow.RecordBatchReader:
        """Stream this dataset, cast onto `schema` when one is asked for."""

    def read_arrow_table(
        self, schema: pyarrow.Schema | StructField | None = None, **kwargs: Any
    ) -> pyarrow.Table:
        """Read the whole dataset into one table. Needs it to fit in memory."""
        return self.read_arrow_reader(schema, **kwargs).read_all()

    # -- writing ------------------------------------------------------------

    @abc.abstractmethod
    def write_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: pyarrow.Schema | StructField | None = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
    ) -> None:
        """Write a stream into this dataset.

        `schema` is the shape to cast onto on the way in, defaulting to this
        dataset's own. `merge_by` is True to merge on the primary key, a list
        of column names to merge on those, and falsy to append. `commit_row_size`
        bounds how many rows one commit carries -- None writes the whole stream
        as one.
        """

    def write_arrow_table(
        self,
        table: pyarrow.Table,
        schema: pyarrow.Schema | StructField | None = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
    ) -> None:
        """`write_arrow_reader` for a table already in memory."""
        self.write_arrow_reader(table.to_reader(), schema, merge_by, commit_row_size)


def arrow_chunks(
    source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch], row_size: int | None
) -> Iterator[pyarrow.Table]:
    """Group a stream into tables of at least `row_size` rows.

    **A batch is not a unit of work downstream.** A store that commits per call
    -- Iceberg lands a file and a snapshot each time -- would turn a stream of
    64k-row batches into thousands of tiny files, so the writer accumulates
    first and commits once per chunk. `None` means the whole stream is one
    chunk, which is the atomic write and the one that costs the most memory.
    """
    batches: list[pyarrow.RecordBatch] = []
    rows = 0
    schema = source.schema if isinstance(source, pyarrow.RecordBatchReader) else None
    for batch in source:
        schema = schema or batch.schema
        batches.append(batch)
        rows += batch.num_rows
        if row_size and rows >= row_size:
            yield pyarrow.Table.from_batches(batches, schema)
            batches, rows = [], 0
    if batches:
        yield pyarrow.Table.from_batches(batches, schema)
