"""What a dataset is: something a stream of Arrow batches comes out of and goes into."""

from __future__ import annotations

import abc
import functools
import importlib
from collections.abc import Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Any, Self

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field, StructField

#: Marker columns the key joins below carry, named like pyiceberg's reserved
#: pair so a merge key of either name is refused with the library's own
#: message before a join fails on the duplicate column instead.
SOURCE_INDEX = "__source_index"
TARGET_INDEX = "__target_index"

#: Default bounded Polars-to-Arrow handoff; commit size remains independent.
POLARS_BATCH_ROW_SIZE = 65_536

# Implementations register here from `__init_subclass__`; lazy modules make
# the shipped kinds available without importing optional dependencies eagerly.
_KINDS: dict[str, type[Dataset]] = {}
_MODULES = MappingProxyType(
    {
        "iceberg": "rekep.iceberg.dataset",
        "text_file": "rekep.text.text_file",
        "text_files": "rekep.text.text_files",
    }
)
_READS = MappingProxyType(
    {
        pyarrow.Table: "arrow_table",
        pyarrow.RecordBatchReader: "arrow_reader",
    }
)
_OVERWRITES = MappingProxyType(
    {
        pyarrow.RecordBatch: "arrow_batch",
        pyarrow.Table: "arrow_table",
        pyarrow.RecordBatchReader: "arrow_reader",
        Iterator: "arrow_reader",
        list: "arrow_reader",
        tuple: "arrow_reader",
    }
)


class Dataset(Convertible, abc.ABC):
    """A stored data product, read and written as Arrow, whatever stores it."""

    @classmethod
    @functools.cache
    def into_kind(cls) -> str:
        """Document kind this implementation claims; empty on the base."""
        return ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        kind = cls.into_kind()
        if kind:
            _KINDS[kind] = cls

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Self:
        """Build the dataset a document declares, dispatching on its `kind`.

        Called on `Dataset` it picks the implementation; called on one of them
        it builds that one, and refuses a document naming a different kind
        rather than quietly building the wrong store from the right fields.
        A document with no `kind` read through a concrete class is just that
        class, which is what keeps `IcebergDataset.from_yaml(...)` working
        unchanged.
        """
        kind = str(mapping.get("kind", "") or "")
        if cls is Dataset:
            if not kind:
                raise ValueError(
                    "a dataset document says which store it is: add a `kind`, one of "
                    f"{sorted(set(_KINDS) | set(_MODULES))}"
                )
            built = _KINDS.get(kind) or Dataset._imported(kind)
            if built is None:
                known = sorted(set(_KINDS) | set(_MODULES))
                raise ValueError(f"no dataset of kind {kind!r}; there is {known}")
            return built.from_dict(mapping)  # type: ignore[return-value]
        claimed = cls.into_kind()
        if kind and kind != claimed:
            raise ValueError(f"{cls.__name__} is {claimed!r}, and the document says {kind!r}")
        return super().from_dict({key: value for key, value in mapping.items() if key != "kind"})

    @staticmethod
    def _imported(kind: str) -> type[Dataset] | None:
        """The implementation for `kind`, imported if this package ships one."""
        module = _MODULES.get(kind)
        if module is None:
            return None
        importlib.import_module(module)
        return _KINDS.get(kind)

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
        same by it -- a field, an Arrow schema, field or type, a `@scalar`
        class, or nothing at all -- so none of them decides that for itself.
        """
        return self.into_struct_field() if schema is None else Field.from_(schema)

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

    @property
    def records(self) -> int | None:
        """How many rows this holds, when the store can say without reading them.

        None by default, which means "this store cannot say cheaply" and is a
        different answer from zero. A caller that needs the number counts it;
        one that only wants to report what it landed says so, or says it does
        not know -- rather than paying for a scan to decorate a log line.
        """
        return None

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

        A field, an Arrow schema, field or type, or a `@scalar` class -- and
        nothing at all means this dataset's own declared shape.
        """
        return self.create_with_field(self.target_field(source), **kwargs)

    def create_with_arrow_schema(self, schema: pyarrow.Schema, **kwargs: Any) -> Self:
        """`create_with_field`, from an Arrow schema."""
        return self.create_with_field(Field.from_(schema), **kwargs)

    def create_with_arrow_field(self, field: pyarrow.Field, **kwargs: Any) -> Self:
        """`create_with_field`, from an Arrow field."""
        return self.create_with_field(Field.from_(field), **kwargs)

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
        return getattr(self, f"read_{self.redirect_of(target, _READS)}")(**kwargs)

    @abc.abstractmethod
    def read_arrow_reader(self, schema: Any = None, **kwargs: Any) -> pyarrow.RecordBatchReader:
        """Stream this dataset, cast onto `schema` when one is asked for."""

    def read_arrow_table(self, schema: Any = None, **kwargs: Any) -> pyarrow.Table:
        """Read the whole dataset into one table. Needs it to fit in memory."""
        return self.read_arrow_reader(schema, **kwargs).read_all()

    def read_polars_batches(self, schema: Any = None, **kwargs: Any) -> Iterator[Any]:
        """Yield one Polars frame per Arrow batch without materialising the dataset."""
        from rekep.require import require

        polars = require("polars", "polars")
        for batch in self.read_arrow_reader(schema, **kwargs):
            yield polars.from_arrow(batch, rechunk=False)

    def read_polars(self, schema: Any = None, **kwargs: Any) -> Any:
        """Read the whole dataset into one Polars frame. Needs it to fit in memory."""
        from rekep.require import require

        polars = require("polars", "polars")
        return polars.from_arrow(self.read_arrow_table(schema, **kwargs), rechunk=False)

    # -- writing ------------------------------------------------------------

    @abc.abstractmethod
    def overwrite_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] = True,
        commit_row_size: int | None = None,
    ) -> None:
        """Replaces the rows whose keys match and inserts the rest.

        Creates the dataset if it is not there. `schema` is the shape to cast
        onto on the way in, defaulting to this dataset's own. `merge_by` is
        True to match on the primary key or a list of column names to match on
        those. A store may give false `merge_by` a bounded replacement meaning
        -- Iceberg replaces the touched identity partitions -- otherwise an
        overwrite needs keys; `append_arrow_*` is the blind insert.
        `commit_row_size` bounds how many rows one commit carries. None uses
        the store's default; a store with no configured default writes the
        whole stream as one.
        """

    def overwrite_arrow(self, source: Any, *args: Any, **kwargs: Any) -> None:
        """Replaces the rows whose keys match and inserts the rest, whatever the shape.

        A batch, a table, a reader or a plain iterator of batches each have
        their own `overwrite_arrow_*`; this redirects to the one that fits
        rather than making every call site branch.
        """
        return getattr(self, f"overwrite_{self.redirect_of(source, _OVERWRITES)}")(
            source, *args, **kwargs
        )

    def overwrite_arrow_batch(
        self,
        batch: pyarrow.RecordBatch,
        schema: Any = None,
        merge_by: bool | Sequence[str] = True,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Replaces the rows whose keys match and inserts the rest, for one batch."""
        self.overwrite_arrow_reader(iter([batch]), schema, merge_by, commit_row_size, **kwargs)

    def overwrite_arrow_table(
        self,
        table: pyarrow.Table,
        schema: Any = None,
        merge_by: bool | Sequence[str] = True,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Replaces the rows whose keys match and inserts the rest, from memory.

        Whatever else an implementation takes -- a branch, snapshot properties
        -- goes straight through, so the generic `overwrite_arrow` can hand any
        shape to any dataset without knowing what it supports.
        """
        self.overwrite_arrow_reader(table.to_reader(), schema, merge_by, commit_row_size, **kwargs)

    def overwrite_polars(
        self,
        source: Any,
        schema: Any = None,
        merge_by: bool | Sequence[str] = True,
        commit_row_size: int | None = None,
        *,
        batch_row_size: int = POLARS_BATCH_ROW_SIZE,
        **kwargs: Any,
    ) -> None:
        """Replaces the rows whose keys match and inserts the rest, from a Polars frame.

        Streamed through bounded, schema-checked Arrow batches.
        """
        target = self.target_field(schema)
        reader = _polars_reader(source, target, batch_row_size)
        self.overwrite_arrow_reader(reader, target, merge_by, commit_row_size, **kwargs)

    # -- appending -----------------------------------------------------------

    @abc.abstractmethod
    def append_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """Append a stream and return how many rows were inserted.

        A false `merge_by` blindly appends every row. True inserts only rows
        whose declared primary key is absent; a sequence names alternate key
        columns. Implementations keep lookup beside their storage engine so it
        can be pushed down instead of collecting stored keys here.
        """

    def append_arrow(self, source: Any, *args: Any, **kwargs: Any) -> int:
        """Append the inferred Arrow shape and return rows inserted."""
        return getattr(self, f"append_{self.redirect_of(source, _OVERWRITES)}")(
            source, *args, **kwargs
        )

    def append_arrow_batch(
        self,
        batch: pyarrow.RecordBatch,
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """`append_arrow_reader` for one batch."""
        return self.append_arrow_reader(iter([batch]), schema, merge_by, commit_row_size, **kwargs)

    def append_arrow_table(
        self,
        table: pyarrow.Table,
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """`append_arrow_reader` for a table already in memory."""
        return self.append_arrow_reader(
            table.to_reader(), schema, merge_by, commit_row_size, **kwargs
        )

    def append_polars(
        self,
        source: Any,
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
        *,
        batch_row_size: int = POLARS_BATCH_ROW_SIZE,
        **kwargs: Any,
    ) -> int:
        """Append a Polars frame through bounded, schema-checked Arrow batches."""
        target = self.target_field(schema)
        reader = _polars_reader(source, target, batch_row_size)
        return self.append_arrow_reader(reader, target, merge_by, commit_row_size, **kwargs)


def _polars_reader(
    source: Any, target: StructField, batch_row_size: int
) -> pyarrow.RecordBatchReader:
    """A DataFrame or streaming LazyFrame cast onto `target` batch by batch."""
    if batch_row_size <= 0:
        raise ValueError("batch_row_size must be positive")
    from rekep.require import require

    polars = require("polars", "polars")
    if isinstance(source, polars.DataFrame):
        frames = iter((source,))
    elif isinstance(source, polars.LazyFrame):
        collect_batches = getattr(source, "collect_batches", None)
        if collect_batches is None:
            raise ImportError("streaming a LazyFrame requires Polars with collect_batches support")
        frames = collect_batches(
            chunk_size=batch_row_size,
            maintain_order=True,
            engine="streaming",
        )
    else:
        raise TypeError(f"Polars input needs a DataFrame or LazyFrame, got {type(source).__name__}")

    def batches() -> Iterator[pyarrow.RecordBatch]:
        for frame in frames:
            table = _polars_table(frame, target, polars)
            yield from table.to_batches(max_chunksize=batch_row_size)

    return pyarrow.RecordBatchReader.from_batches(target.into_arrow_schema(), batches())


def _polars_table(frame: Any, target: StructField, polars: Any) -> pyarrow.Table:
    """Export at the newest compatible level, then enforce the Arrow contract."""
    compatibility = getattr(polars, "CompatLevel", None)
    options = {}
    if compatibility is not None and not _needs_compatible_polars_arrow(target.dtype):
        options["compat_level"] = compatibility.newest()
    return target.cast_arrow_table(frame.to_arrow(**options))


def _needs_compatible_polars_arrow(dtype: pyarrow.DataType) -> bool:
    """Whether newest Polars export would replace a declared text buffer with a view."""
    types = pyarrow.types
    if (
        types.is_string(dtype)
        or types.is_large_string(dtype)
        or types.is_binary(dtype)
        or types.is_large_binary(dtype)
    ):
        return True
    if types.is_dictionary(dtype):
        return _needs_compatible_polars_arrow(dtype.value_type)
    storage = getattr(dtype, "storage_type", None)
    if storage is not None:
        return _needs_compatible_polars_arrow(storage)
    return any(
        _needs_compatible_polars_arrow(dtype.field(index).type) for index in range(dtype.num_fields)
    )


def arrow_chunks(
    source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch], row_size: int | None
) -> Iterator[pyarrow.Table]:
    """Group a stream into tables of at most `row_size` rows.

    **A batch is not a unit of work downstream.** A store that commits per call
    -- Iceberg lands a file and a snapshot each time -- would turn a stream of
    64k-row batches into thousands of tiny files, so the writer accumulates
    first and commits once per chunk. `None` means the whole stream is one
    chunk, which is the atomic write and the one that costs the most memory.
    """
    if row_size is not None and row_size <= 0:
        raise ValueError("row_size must be positive")
    batches: list[pyarrow.RecordBatch] = []
    rows = 0
    schema = source.schema if isinstance(source, pyarrow.RecordBatchReader) else None
    for batch in source:
        if not batch.num_rows:
            continue
        schema = schema or batch.schema
        offset = 0
        while offset < batch.num_rows:
            available = batch.num_rows - offset
            take = available if row_size is None else min(available, row_size - rows)
            batches.append(batch.slice(offset, take))
            rows += take
            offset += take
            if row_size is not None and rows == row_size:
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
    """`table` with `-0.0` in a float merge key replaced by the `0.0` it equals."""
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
    """The rows of `matched` whose key the chunk references, in its own order."""
    if matched.num_rows == 0:
        return matched
    kept = keys_of(matched, join, TARGET_INDEX).join(
        keys_of(chunk, join, SOURCE_INDEX).select(list(join)),
        keys=list(join),
        join_type="left semi",
    )
    return matched.take(_in_order(kept.column(TARGET_INDEX)))


def anti_join(chunk: pyarrow.Table, matched: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """The rows of `chunk` no row of `matched` shares a key with, in its own order.

    One Arrow anti-join over the keys alone, rather than binding a per-row
    equality expression and filtering with it once per batch, which is what
    makes the insert half of a merge linear instead of quadratic.
    """
    if matched.num_rows == 0:
        return chunk
    fresh = keys_of(chunk, join, SOURCE_INDEX).join(
        keys_of(matched, join, TARGET_INDEX).select(list(join)),
        keys=list(join),
        join_type="left anti",
    )
    return chunk.take(_in_order(fresh.column(SOURCE_INDEX)))


def _in_order(taken: Any) -> Any:
    """Row positions a join handed back, put back into the table's own order."""
    positions = taken.combine_chunks()
    return positions.take(pyarrow.compute.sort_indices(positions))


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
