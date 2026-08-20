"""What a dataset is: something a stream of Arrow batches comes out of and goes into."""

from __future__ import annotations

import abc
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Self

import pyarrow

from rekep.convert import Convertible
from rekep.fields import StructField, field_of


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

    Writes **append by default, and create what is not there yet**: a first
    write to an empty catalog or a missing file builds it from the declared
    shape, so a pipeline does not need a separate "deploy" step. `create_with`
    is there for when the shape has to exist before anything is written.

    A dataset is a `Convertible` dataclass, so an implementation's
    configuration is also a document: `IcebergDataset.from_yaml("logs.yaml")`.
    """

    #: What `read_arrow` redirects to, keyed by the type asked for.
    READS: ClassVar[dict[Any, str]] = {
        pyarrow.Table: "arrow_table",
        pyarrow.RecordBatchReader: "arrow_reader",
    }

    #: What `write_arrow` redirects to, keyed by what is handed over. A batch
    #: has its own method because wrapping it in a stream is this class's job,
    #: not the caller's.
    WRITES: ClassVar[dict[Any, str]] = {
        pyarrow.RecordBatch: "arrow_batch",
        pyarrow.Table: "arrow_table",
        pyarrow.RecordBatchReader: "arrow_reader",
        Iterator: "arrow_reader",
        list: "arrow_reader",
        tuple: "arrow_reader",
    }

    # -- what it holds ------------------------------------------------------

    @abc.abstractmethod
    def into_struct_field(self) -> StructField:
        """The shape this dataset holds."""

    def into_arrow_schema(self) -> pyarrow.Schema:
        """That shape as an Arrow schema."""
        return self.into_struct_field().into_arrow_schema()

    def target_field(self, schema: Any = None) -> StructField:
        """The shape a cast should land on: `schema` if given, else ours.

        Every read and write takes an optional schema, and they all mean the
        same by it -- a field, an Arrow schema, field or type, a `@field`
        class, or nothing at all -- so none of them decides that for itself.
        """
        return self.into_struct_field() if schema is None else field_of(schema)

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

    # -- creating -----------------------------------------------------------

    @property
    @abc.abstractmethod
    def exists(self) -> bool:
        """Whether this dataset is there yet."""

    @abc.abstractmethod
    def create_with_field(self, field: StructField, **kwargs: Any) -> Self:
        """Make this dataset exist, shaped by `field`, and hand it back.

        Idempotent by contract: creating one that is already there is not an
        error, which is what lets a write create as it goes.
        """

    def create_with(self, source: Any = None, **kwargs: Any) -> Self:
        """`create_with_field`, from whatever names a shape.

        A field, an Arrow schema, field or type, or a `@field` class -- and
        nothing at all means this dataset's own declared shape.
        """
        return self.create_with_field(self.target_field(source), **kwargs)

    def create_with_arrow_schema(self, schema: pyarrow.Schema, **kwargs: Any) -> Self:
        """`create_with_field`, from an Arrow schema."""
        return self.create_with_field(field_of(schema), **kwargs)

    def create_with_arrow_field(self, field: pyarrow.Field, **kwargs: Any) -> Self:
        """`create_with_field`, from an Arrow field."""
        return self.create_with_field(field_of(field), **kwargs)

    def get_or_create(self, source: Any = None, **kwargs: Any) -> Self:
        """This dataset, created with that shape when it is not there yet.

        What every write calls first: appending to something that does not
        exist yet is a create, not a failure.
        """
        return self if self.exists else self.create_with(source, **kwargs)

    # -- reading ------------------------------------------------------------

    def read_arrow(self, target: Any = pyarrow.Table, **kwargs: Any) -> Any:
        """Read, picking the method by the type asked for.

        `read_arrow(pyarrow.Table)` materialises, `read_arrow(RecordBatchReader)`
        streams; the keywords go through to whichever it is.
        """
        return getattr(self, f"read_{self.redirect_of(target, self.READS)}")(**kwargs)

    @abc.abstractmethod
    def read_arrow_reader(self, schema: Any = None, **kwargs: Any) -> pyarrow.RecordBatchReader:
        """Stream this dataset, cast onto `schema` when one is asked for."""

    def read_arrow_table(self, schema: Any = None, **kwargs: Any) -> pyarrow.Table:
        """Read the whole dataset into one table. Needs it to fit in memory."""
        return self.read_arrow_reader(schema, **kwargs).read_all()

    # -- writing ------------------------------------------------------------

    @abc.abstractmethod
    def write_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
    ) -> None:
        """Write a stream into this dataset, creating it if it is not there.

        `schema` is the shape to cast onto on the way in, defaulting to this
        dataset's own. `merge_by` is True to merge on the primary key, a list
        of column names to merge on those, and falsy to append. `commit_row_size`
        bounds how many rows one commit carries -- None writes the whole stream
        as one.
        """

    def write_arrow(self, source: Any, *args: Any, **kwargs: Any) -> None:
        """Write, picking the method by what is handed over.

        A batch, a table, a reader or a plain iterator of batches each have
        their own `write_arrow_*`; this redirects to the one that fits rather
        than making every call site branch.
        """
        return getattr(self, f"write_{self.redirect_of(source, self.WRITES)}")(
            source, *args, **kwargs
        )

    def write_arrow_batch(
        self,
        batch: pyarrow.RecordBatch,
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        """`write_arrow_reader` for one batch."""
        self.write_arrow_reader(iter([batch]), schema, merge_by, commit_row_size, **kwargs)

    def write_arrow_table(
        self,
        table: pyarrow.Table,
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        """`write_arrow_reader` for a table already in memory.

        Whatever else an implementation takes -- a branch, snapshot properties
        -- goes straight through, so the generic `write_arrow` can hand any
        shape to any dataset without knowing what it supports.
        """
        self.write_arrow_reader(table.to_reader(), schema, merge_by, commit_row_size, **kwargs)


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
