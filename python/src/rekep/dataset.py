"""What a dataset is: something a stream of Arrow batches comes out of and goes into."""

from __future__ import annotations

import abc
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Self

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field, StructField, field_of

#: Marker columns the key joins below carry, named like pyiceberg's reserved
#: pair so a merge key of either name is refused with the library's own
#: message before a join fails on the duplicate column instead.
SOURCE_INDEX = "__source_index"
TARGET_INDEX = "__target_index"


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

    # -- appending -----------------------------------------------------------

    def append_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Append a stream, skipping the rows a stored row already matches.

        Same arguments as `write_arrow_reader`, and the same falsy-appends
        rule -- but `merge_by` means something cheaper here. A *write* with
        `merge_by` upserts: it finds the stored row a key matches and rewrites
        it. An *append* never touches what is stored: a row whose key is
        already there is dropped, the rest are inserted. That is the half of
        an upsert a stream of immutable rows needs -- replaying it inserts
        nothing, rewrites nothing, and costs no delete files.

        Duplicate keys *inside* the stream collapse to their first row for the
        same reason: by the time the second arrives, the dataset already has
        that key. A null merge key is refused -- no join can match it, so a
        replay would insert it again every time.

        This generic form reads the stored key columns once and anti-joins
        each chunk against them in Arrow, so it fits any store that can read;
        a store that can plan better overrides it (`IcebergDataset` prunes
        the stored side to each chunk's key ranges).
        """
        join = self.merge_columns(merge_by)
        if not join:
            self.write_arrow_reader(source, schema, None, commit_row_size, **kwargs)
            return
        target = self.target_field(schema)
        key_field = _key_field(target, join)
        reader = target.cast_arrow_reader(source)
        seen = (
            self.read_arrow_table(key_field)
            if self.exists
            else key_field.arrow_schema.empty_table()
        )
        seen = normalised_keys(seen, join)
        for chunk in arrow_chunks(reader, commit_row_size):
            _refuse_null_keys(chunk, join)
            fresh = first_rows(normalised_keys(chunk, join), join)
            if seen.num_rows:
                fresh = anti_join(fresh, seen, join)
            if fresh.num_rows == 0:
                continue
            self.write_arrow_table(fresh, target, None, None, **kwargs)
            seen = pyarrow.concat_tables([seen, fresh.select(list(join))])

    def append_arrow(self, source: Any, *args: Any, **kwargs: Any) -> None:
        """Append, picking the method by what is handed over, like `write_arrow`."""
        return getattr(self, f"append_{self.redirect_of(source, self.WRITES)}")(
            source, *args, **kwargs
        )

    def append_arrow_batch(
        self,
        batch: pyarrow.RecordBatch,
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        """`append_arrow_reader` for one batch."""
        self.append_arrow_reader(iter([batch]), schema, merge_by, commit_row_size, **kwargs)

    def append_arrow_table(
        self,
        table: pyarrow.Table,
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        """`append_arrow_reader` for a table already in memory."""
        self.append_arrow_reader(table.to_reader(), schema, merge_by, commit_row_size, **kwargs)


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


# -- key joins ---------------------------------------------------------------
#
# The vocabulary every merge-shaped write is made of, shared here so a store
# never grows a second copy: which stored rows a chunk references, which of a
# chunk's rows are new, and one row per key. All of it is Arrow joins over the
# key columns and an index -- never the whole row, because Acero refuses
# nested columns as join payload, and never a Python row loop.


def keys_of(table: pyarrow.Table, join: Sequence[str], marker: str) -> pyarrow.Table:
    """Just the key columns, numbered, and normalised for Arrow's equality.

    Arrow's equality is not every store's on one point: `-0.0` and `0.0` are
    the same number to IEEE 754, but they hash apart in a join, which would
    let one key match nothing and be inserted twice. Normalising the sign of
    zero on float key columns keeps a stored `-0.0` -- written before this
    package normalised keys, or by another engine -- joinable to the `0.0` a
    chunk carries.
    """
    columns = []
    for name in join:
        column = table.column(name).combine_chunks()
        if pyarrow.types.is_floating(column.type):
            zero = pyarrow.scalar(0.0, column.type)
            column = pyarrow.compute.if_else(pyarrow.compute.equal(column, zero), zero, column)
        columns.append(column)
    keys = pyarrow.Table.from_arrays(columns, names=list(join))
    from rekep.fields import arrays

    return keys.append_column(marker, arrays.sequence(table.num_rows))


def normalised_keys(table: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """`table` with `-0.0` in a float merge key replaced by the `0.0` it equals.

    On the values that are *written*, not only inside the joins, because three
    things have to agree about a key and only two of them can be talked round:
    a store's comparison says they are the same number, an Arrow join hashes
    them apart, and so does `pc.is_in` -- what a predicate filter becomes once
    it reaches Arrow. Storing one spelling is the only way a later merge or
    append finds the row again.

    Nothing else is touched: not a float that is not a key, not a key that is
    not a float, and not the sign of anything that is not zero.
    """
    columns = list(table.columns)
    changed = False
    for name in join:
        index = table.schema.get_field_index(name)
        if index < 0:
            continue
        column = table.column(index)
        if not pyarrow.types.is_floating(column.type):
            continue
        # Applied whether or not a negative zero is in there: telling requires
        # a pass of its own, and the kernel is the same pass either way.
        zero = pyarrow.scalar(0.0, column.type)
        columns[index] = pyarrow.compute.if_else(pyarrow.compute.equal(column, zero), zero, column)
        changed = True
    return pyarrow.Table.from_arrays(columns, schema=table.schema) if changed else table


def semi_join(matched: pyarrow.Table, chunk: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """The rows of `matched` whose key the chunk references."""
    if matched.num_rows == 0:
        return matched
    if chunk.num_rows > matched.num_rows:
        kept = normalised_keys(chunk, join).select(list(join)).join(
            keys_of(matched, join, TARGET_INDEX), keys=list(join), join_type="right semi"
        )
    else:
        kept = keys_of(matched, join, TARGET_INDEX).join(
            normalised_keys(chunk, join).select(list(join)),
            keys=list(join),
            join_type="left semi",
        )
    index = kept.column(TARGET_INDEX).combine_chunks()
    return matched.take(index.take(pyarrow.compute.sort_indices(index)))


def anti_join(chunk: pyarrow.Table, matched: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """The rows of `chunk` no row of `matched` shares a key with.

    One Arrow anti-join over the keys alone, rather than binding a per-row
    equality expression and filtering with it once per batch, which is what
    makes the insert half of a merge linear instead of quadratic.
    """
    if matched.num_rows == 0:
        return chunk
    if matched.num_rows > chunk.num_rows:
        fresh = normalised_keys(matched, join).select(list(join)).join(
            keys_of(chunk, join, SOURCE_INDEX), keys=list(join), join_type="right anti"
        )
    else:
        fresh = keys_of(chunk, join, SOURCE_INDEX).join(
            normalised_keys(matched, join).select(list(join)),
            keys=list(join),
            join_type="left anti",
        )
    index = fresh.column(SOURCE_INDEX).combine_chunks()
    return chunk.take(index.take(pyarrow.compute.sort_indices(index)))


def first_rows(table: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """One row per distinct key -- the first -- in the table's own order.

    What makes an insert-only append idempotent *within* a stream: by the
    time a duplicate key arrives, the dataset already holds that key, so
    keeping the first row is the same answer the replay would produce. One
    `group_by` and one `take`; a table with no duplicate keys comes back
    untouched, which is the common case and costs the group alone.
    """
    keys = keys_of(table, join, SOURCE_INDEX)
    firsts = keys.group_by(list(join)).aggregate([(SOURCE_INDEX, "min")])
    if firsts.num_rows == table.num_rows:
        return table
    indices = firsts.column(f"{SOURCE_INDEX}_min").combine_chunks()
    return table.take(indices.take(pyarrow.compute.sort_indices(indices)))


def _key_field(target: StructField, join: Sequence[str]) -> StructField:
    """The key columns of `target` as a shape of their own, to read and cast onto."""
    return Field.from_arrow_schema(
        pyarrow.schema([target.field(name).into_arrow_field() for name in join]),
        target.name,
    )


def _refuse_null_keys(chunk: pyarrow.Table, join: Sequence[str]) -> None:
    """A null merge key matches nothing, so appending on it would duplicate rows."""
    for name in join:
        if chunk.column(name).null_count:
            raise ValueError(
                f"column {name!r} is a merge key and cannot be null; "
                "a null key matches nothing, so appending on it would duplicate rows"
            )
