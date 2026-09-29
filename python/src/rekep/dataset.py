"""What a dataset is: something a stream of Arrow batches comes out of and goes into."""

from __future__ import annotations

import abc
from collections.abc import Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.annotations import Self
from rekep.fields import Field, field_of

#: Marker columns the key joins below carry, named like pyiceberg's reserved
#: pair so a merge key of either name is refused with the library's own
#: message before a join fails on the duplicate column instead.
SOURCE_INDEX = "__source_index"
TARGET_INDEX = "__target_index"

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
    }
)


def _stem_of(value: Any, stems: Mapping[type, str]) -> str:
    """The method stem for `value`, a requested Arrow type or an Arrow value."""
    for kind, stem in stems.items():
        if issubclass(value, kind) if isinstance(value, type) else isinstance(value, kind):
            return stem
    raise TypeError(f"no Arrow method for {value!r}")


class Dataset(abc.ABC):
    """A stored data product, read and written as Arrow, whatever stores it."""

    # -- what it holds ------------------------------------------------------

    @abc.abstractmethod
    def into_struct_field(self) -> Field:
        """The shape this dataset holds."""

    def into_arrow_schema(self) -> pyarrow.Schema:
        """That shape as an Arrow schema."""
        return self.into_struct_field().into_arrow_schema()

    def target_field(self, schema: Any = None) -> Field:
        """The shape a cast should land on: `schema` if given, else ours.

        Every read and write takes an optional schema, and they all mean the
        same by it -- a field, an Arrow schema, field or type, a `@scalar`
        class, or nothing at all -- so none of them decides that for itself.
        """
        return self.into_struct_field() if schema is None else field_of(schema)

    def merge_columns(self, merge_by: bool | Sequence[str] | None) -> list[str]:
        """Columns a keyed write matches on: the primary key, else what is named.

        None is the primary key when the shape declares one and nothing when
        it does not; True requires one. `False` and `[]` name nothing, which
        is what makes `merge_by` a single argument rather than a flag and a
        list; what a write that names nothing does is the verb's to say.
        """
        from rekep.iceberg.fields import primary_keys

        if merge_by is None:
            return list(primary_keys(self.into_struct_field()))
        if not merge_by:
            return []
        if merge_by is True:
            keys = primary_keys(self.into_struct_field())
            if not keys:
                raise ValueError(
                    f"{type(self).__name__} cannot merge on its primary key: no member declares "
                    "one; mark it with primary_key() or name the columns to merge on"
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
    def create_with_field(self, field: Field, **kwargs: Any) -> Self:
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
        return getattr(self, f"read_{_stem_of(target, _READS)}")(**kwargs)

    @abc.abstractmethod
    def read_arrow_reader(self, schema: Any = None, **kwargs: Any) -> pyarrow.RecordBatchReader:
        """Stream this dataset, cast onto `schema` when one is asked for."""

    def read_arrow_table(self, schema: Any = None, **kwargs: Any) -> pyarrow.Table:
        """Read the whole dataset into one table. Needs it to fit in memory."""
        return self.read_arrow_reader(schema, **kwargs).read_all()

    # -- writing ------------------------------------------------------------

    @abc.abstractmethod
    def overwrite_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader,
        schema: Any = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """Replace what the stream covers, and return the rows it wrote.

        What it covers is the store's to say -- Iceberg replaces the rows a
        `row_filter` selects, or the partitions the stream touches -- and
        every row of the stream lands. Creates the dataset if it is not there.
        `schema` is the shape to cast onto on the way in, defaulting to this
        dataset's own. `commit_row_size` bounds how many rows one commit
        carries; None uses the store's default. A write matched row by row on
        a key is `merge_arrow_reader`.
        """

    def overwrite_arrow(self, source: Any, *args: Any, **kwargs: Any) -> int:
        """`overwrite_arrow_reader`, whatever the shape.

        A batch, table, or schema-bearing reader each has its own
        `overwrite_arrow_*`; this redirects to the one that fits rather than
        making every call site branch.
        """
        return getattr(self, f"overwrite_{_stem_of(source, _OVERWRITES)}")(source, *args, **kwargs)

    def overwrite_arrow_batch(
        self,
        batch: pyarrow.RecordBatch,
        schema: Any = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """`overwrite_arrow_reader` for one batch."""
        reader = pyarrow.RecordBatchReader.from_batches(batch.schema, [batch])
        return self.overwrite_arrow_reader(reader, schema, commit_row_size, **kwargs)

    def overwrite_arrow_table(
        self,
        table: pyarrow.Table,
        schema: Any = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """`overwrite_arrow_reader` for a table already in memory.

        Whatever else an implementation takes -- a branch, snapshot properties
        -- goes straight through, so the generic `overwrite_arrow` can hand any
        shape to any dataset without knowing what it supports.
        """
        return self.overwrite_arrow_reader(table.to_reader(), schema, commit_row_size, **kwargs)

    # -- merging -------------------------------------------------------------

    @abc.abstractmethod
    def merge_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader,
        schema: Any = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """Upsert a stream by key, and return the rows it inserted or replaced.

        `merge_by` -- the primary key unless it names other columns -- matches
        each row to the stored row of its key: a row no stored row matches is
        inserted, one whose stored row holds other values replaces it, and one
        whose stored row holds the same values is left as it is, so a replay
        writes nothing.
        """

    def merge_arrow(self, source: Any, *args: Any, **kwargs: Any) -> int:
        """Merge the inferred Arrow shape and return the rows it wrote."""
        return getattr(self, f"merge_{_stem_of(source, _OVERWRITES)}")(source, *args, **kwargs)

    def merge_arrow_batch(
        self,
        batch: pyarrow.RecordBatch,
        schema: Any = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """`merge_arrow_reader` for one batch."""
        reader = pyarrow.RecordBatchReader.from_batches(batch.schema, [batch])
        return self.merge_arrow_reader(reader, schema, commit_row_size, **kwargs)

    def merge_arrow_table(
        self,
        table: pyarrow.Table,
        schema: Any = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """`merge_arrow_reader` for a table already in memory."""
        return self.merge_arrow_reader(table.to_reader(), schema, commit_row_size, **kwargs)

    # -- appending -----------------------------------------------------------

    @abc.abstractmethod
    def append_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader,
        schema: Any = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """Append a stream, and return how many rows it added.

        A store may take a `merge_by` naming a key -- the primary key when it
        names nothing and the shape declares one -- and then append only the
        rows whose key it does not hold, so a replay adds nothing; one that
        names nothing, or a store that takes none, appends every row. A write
        that also replaces the stored rows that differ is `merge_arrow_reader`.
        """

    def append_arrow(self, source: Any, *args: Any, **kwargs: Any) -> int:
        """Append the inferred Arrow shape and return rows added."""
        return getattr(self, f"append_{_stem_of(source, _OVERWRITES)}")(source, *args, **kwargs)

    def append_arrow_batch(
        self,
        batch: pyarrow.RecordBatch,
        schema: Any = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """`append_arrow_reader` for one batch."""
        reader = pyarrow.RecordBatchReader.from_batches(batch.schema, [batch])
        return self.append_arrow_reader(reader, schema, commit_row_size, **kwargs)

    def append_arrow_table(
        self,
        table: pyarrow.Table,
        schema: Any = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        """`append_arrow_reader` for a table already in memory."""
        return self.append_arrow_reader(table.to_reader(), schema, commit_row_size, **kwargs)


def arrow_chunks(
    source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
    row_size: int | None,
    batch_num: int | None = None,
) -> Iterator[pyarrow.Table]:
    """Group a stream into tables bounded by rows, batches, or both.

    **A batch is not a unit of work downstream.** A store that commits per call
    -- Iceberg lands a file and a snapshot each time -- would turn a stream of
    64k-row batches into thousands of tiny files, so the writer accumulates
    first and commits once per chunk. With both bounds absent the whole stream
    is one chunk, which is the atomic write and the one that costs the most
    memory.

    **A consumer drops its chunk before asking for the next one.** The next
    chunk is accumulated while the caller's loop target still names the last,
    so a `for` body that ends holding it pays for two: 2.10x the chunk against
    1.09x with a `del` on the way out, measured over eight 10 MiB batches.
    """
    if row_size is not None:
        row_size = _positive_int(row_size, "row_size")
    if batch_num is not None:
        batch_num = _positive_int(batch_num, "batch_num")
    batches: list[pyarrow.RecordBatch] = []
    rows = 0
    batch_count = 0
    schema = source.schema if isinstance(source, pyarrow.RecordBatchReader) else None
    for batch in source:
        if not batch.num_rows:
            continue
        schema = schema or batch.schema
        offset = 0
        while offset < batch.num_rows:
            if batch_num is not None and batch_count == batch_num:
                yield pyarrow.Table.from_batches(batches, schema)
                batches, rows, batch_count = [], 0, 0
            available = batch.num_rows - offset
            take = available if row_size is None else min(available, row_size - rows)
            batches.append(batch.slice(offset, take))
            rows += take
            batch_count += 1
            offset += take
            if (row_size is not None and rows == row_size) or (
                batch_num is not None and batch_count == batch_num
            ):
                yield pyarrow.Table.from_batches(batches, schema)
                batches, rows, batch_count = [], 0, 0
    if batches:
        yield pyarrow.Table.from_batches(batches, schema)


def _positive_int(value: Any, name: str) -> int:
    """A positive integer boundary, excluding bool's integer subclass."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


# -- ordering ----------------------------------------------------------------
#
# What "in order" means, once, for everything that asks: a writer deciding
# whether a chunk needs sorting, a reader checking what a file recorded, and
# the key joins below deciding whether equal keys are already neighbours.

SORT_DIRECTIONS = MappingProxyType(
    {
        "asc": "ascending",
        "ascending": "ascending",
        "desc": "descending",
        "descending": "descending",
    }
)


def sort_direction(direction: Any) -> str:
    """One Arrow direction spelling from a declaration or Iceberg value."""
    value = str(direction).lower()
    try:
        return SORT_DIRECTIONS[value]
    except KeyError as error:
        raise ValueError(f"unknown sort direction {direction!r}") from error


def sort_order_fields(
    columns: Sequence[str] | Sequence[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    """Normalize name-only ascending keys and explicit direction pairs."""
    return tuple(
        (column, "ascending")
        if isinstance(column, str)
        else (str(column[0]), sort_direction(column[1]))
        for column in columns
    )


def sorted_rows(
    rows: pyarrow.Table, columns: Sequence[str] | Sequence[tuple[str, str]]
) -> pyarrow.Table:
    """`rows` in the directional, null-last order `columns` name, ties in their own order.

    Ordered as `in_sort_order` reads them, on `comparable` values, because a
    column of an extension type -- a UUID key -- has no sort kernel of its own.
    """
    fields = sort_order_fields(columns)
    keys = pyarrow.table(
        {f"key_{index}": comparable(rows.column(name)) for index, (name, _) in enumerate(fields)}
    )
    order = [(f"key_{index}", direction) for index, (_, direction) in enumerate(fields)]
    return rows.take(pyarrow.compute.sort_indices(keys, sort_keys=order))


def one_array(column: Any) -> Any:
    """A column as one Arrow array, without copying one that already is one.

    `combine_chunks` copies whatever it is given, a column of one chunk
    included, and these comparisons read a column rather than keep it.
    """
    if not isinstance(column, pyarrow.ChunkedArray):
        return column
    return column.chunk(0) if column.num_chunks == 1 else column.combine_chunks()


def storage_of(column: Any) -> Any:
    """An extension column as its storage, in the shape it arrived in.

    An extension type carries no Arrow kernels of its own, and every reading
    of a key here is an equality, an order, a grouping or a literal a store
    is handed: storage holds the same values in the same order, so a UUID key
    joins, sorts and groups as the bytes it is -- and a column read back
    without its extension compares against a freshly parsed one.
    """
    if not isinstance(column.type, pyarrow.BaseExtensionType):
        return column
    if isinstance(column, pyarrow.ChunkedArray):
        return pyarrow.chunked_array(
            [chunk.storage for chunk in column.chunks], type=column.type.storage_type
        )
    return column.storage


def comparable(column: Any) -> Any:
    """One Arrow array the equality, ordering and grouping kernels accept."""
    return storage_of(one_array(column))


def in_sort_order(
    rows: pyarrow.RecordBatch | pyarrow.Table,
    columns: Sequence[str] | Sequence[tuple[str, str]],
) -> bool:
    """Whether a batch or table follows its directional, null-last ordering."""
    compute = pyarrow.compute
    ordered = None
    for name, direction in reversed(sort_order_fields(columns)):
        column = comparable(rows.column(name))
        before, after = column[:-1], column[1:]
        before_null, after_null = compute.is_null(before), compute.is_null(after)
        if pyarrow.types.is_floating(column.type):
            before_nan = compute.fill_null(compute.is_nan(before), False)
            after_nan = compute.fill_null(compute.is_nan(after), False)
        else:
            before_nan = compute.and_(before_null, compute.invert(before_null))
            after_nan = compute.and_(after_null, compute.invert(after_null))
        before_regular = compute.invert(compute.or_(before_null, before_nan))
        after_regular = compute.invert(compute.or_(after_null, after_nan))
        regular_precedes = compute.and_(
            compute.and_(before_regular, after_regular),
            compute.fill_null(
                (compute.greater if direction == "descending" else compute.less)(before, after),
                False,
            ),
        )
        precedes = compute.or_(
            regular_precedes,
            compute.or_(
                compute.and_(before_regular, compute.or_(after_nan, after_null)),
                compute.and_(before_nan, after_null),
            ),
        )
        equal = compute.or_(
            compute.or_(
                compute.and_(before_null, after_null),
                compute.and_(before_nan, after_nan),
            ),
            compute.and_(
                compute.and_(before_regular, after_regular),
                compute.fill_null(compute.equal(before, after), False),
            ),
        )
        ordered = (
            compute.or_(precedes, equal)
            if ordered is None
            else compute.or_(precedes, compute.and_(equal, ordered))
        )
    if ordered is None:
        return True
    return bool(compute.all(ordered, min_count=0).as_py())


# -- key joins ---------------------------------------------------------------
#
# The vocabulary a keyed write is made of, shared here so a store never grows
# a second copy: which of a stored file's rows a chunk replaces, and one row
# per key. All of it is Arrow joins over the key columns and an index -- never
# the whole row, because Acero refuses nested columns as join payload, and
# never a Python row loop.


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
        column = comparable(table.column(name))
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


def anti_join(rows: pyarrow.Table, matched: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """The rows of `rows` no row of `matched` shares a key with, in their own order.

    One Arrow anti-join over the keys alone, rather than a per-row equality
    expression evaluated once per batch, which is what keeps taking a chunk's
    keys out of a stored file linear rather than quadratic. `matched`'s keys
    are brought onto `rows`' types first: a scan hands a stored `string` back
    as `large_string`, and Acero refuses to join the two.
    """
    if matched.num_rows == 0 or rows.num_rows == 0:
        return rows
    stored = keys_of(rows, join, SOURCE_INDEX)
    wanted = keys_of(matched, join, TARGET_INDEX).select(list(join))
    for index, name in enumerate(join):
        kind = stored.schema.field(name).type
        if wanted.schema.field(name).type != kind:
            wanted = wanted.set_column(
                index, wanted.schema.field(index).with_type(kind), wanted.column(name).cast(kind)
            )
    fresh = stored.join(wanted, keys=list(join), join_type="left anti")
    if fresh.num_rows == rows.num_rows:
        return rows
    return rows.take(_in_order(fresh.column(SOURCE_INDEX)))


def semi_join(rows: pyarrow.Table, matched: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """The rows of `rows` some row of `matched` shares a key with, in their own order.

    `anti_join`'s complement, typed the same way: `matched`'s keys are brought
    onto `rows`' types first.
    """
    if matched.num_rows == 0 or rows.num_rows == 0:
        return rows.slice(0, 0)
    stored = keys_of(rows, join, SOURCE_INDEX)
    found = stored.join(_keys_onto(stored, matched, join), keys=list(join), join_type="left semi")
    if found.num_rows == rows.num_rows:
        return rows
    return rows.take(_in_order(found.column(SOURCE_INDEX)))


def key_pairs(rows: pyarrow.Table, matched: pyarrow.Table, join: Sequence[str]) -> tuple[Any, Any]:
    """`(rows index, matched index)` of every pair of rows sharing a key.

    Typed as `anti_join` is: `matched`'s keys are brought onto `rows`' types.
    """
    left = keys_of(rows, join, SOURCE_INDEX)
    right = keys_of(matched, join, TARGET_INDEX)
    right = _keys_onto(left, right, join).append_column(TARGET_INDEX, right.column(TARGET_INDEX))
    pairs = left.join(right, keys=list(join), join_type="inner")
    return one_array(pairs.column(SOURCE_INDEX)), one_array(pairs.column(TARGET_INDEX))


def _keys_onto(stored: pyarrow.Table, matched: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """`matched`'s key columns alone, cast onto the types `stored` holds them in."""
    wanted = keys_of(matched, join, TARGET_INDEX).select(list(join))
    for index, name in enumerate(join):
        kind = stored.schema.field(name).type
        if wanted.schema.field(name).type != kind:
            wanted = wanted.set_column(
                index, wanted.schema.field(index).with_type(kind), wanted.column(name).cast(kind)
            )
    return wanted


def equal_rows(left: pyarrow.Table, right: pyarrow.Table) -> Any:
    """Which rows of two aligned tables hold the same values, column by column.

    Columns are `left`'s, matched by name: one `right` lacks is equal only
    where `left` is null, which is what writing `right` over `left` leaves.
    Two nulls are equal and so are two NaNs; a nested value is equal when
    every value inside it is. A pair no kernel can compare is unequal, the
    direction that rewrites a row rather than keeps a stale one.
    """
    compute = pyarrow.compute
    same = _constant(left.num_rows, True)
    for name in left.column_names:
        column = one_array(left.column(name))
        if name in right.column_names:
            equal = _equal_values(column, one_array(right.column(name)))
        else:
            equal = compute.is_null(column)
        same = compute.and_(same, equal)
    return same


def _row_numbers(rows: int) -> Any:
    from rekep.fields import arrays

    return arrays.sequence(rows)


def _constant(rows: int, value: bool) -> Any:
    """`rows` booleans, every one `value`."""
    return pyarrow.compute.fill_null(pyarrow.nulls(rows, pyarrow.bool_()), value)


#: What refusing a comparison looks like in Arrow's kernels.
_INCOMPARABLE = (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError, pyarrow.ArrowTypeError)


def _equal_values(left: Any, right: Any) -> Any:
    """Which values of two aligned arrays are the same, nulls and NaNs included."""
    compute = pyarrow.compute
    try:
        left, right = _comparable_pair(left, right)
        kind = left.type
        both_null = compute.and_(compute.is_null(left), compute.is_null(right))
        if pyarrow.types.is_null(kind):
            return compute.is_null(left)
        valid = compute.and_(compute.is_valid(left), compute.is_valid(right))
        if pyarrow.types.is_struct(kind):
            same = valid
            for index in range(kind.num_fields):
                same = compute.and_(same, _equal_values(left.field(index), right.field(index)))
            return compute.or_(both_null, same)
        if _is_listlike(kind):
            return compute.or_(both_null, _equal_lists(left, right, valid))
        equal = compute.fill_null(compute.equal(left, right), False)
        if pyarrow.types.is_floating(kind):
            equal = compute.or_(
                equal,
                compute.and_(
                    compute.fill_null(compute.is_nan(left), False),
                    compute.fill_null(compute.is_nan(right), False),
                ),
            )
        return compute.or_(both_null, equal)
    except _INCOMPARABLE:
        return _constant(len(left), False)


def _equal_lists(left: Any, right: Any, valid: Any) -> Any:
    """Which of two aligned list arrays' valid values hold equal elements in order."""
    compute = pyarrow.compute
    lengths = compute.fill_null(
        compute.equal(compute.list_value_length(left), compute.list_value_length(right)), False
    )
    candidates = compute.and_(valid, lengths)
    if not compute.any(candidates, min_count=0).as_py():
        return candidates
    shortlist, versus = left.filter(candidates), right.filter(candidates)
    elements = _equal_values(compute.list_flatten(shortlist), compute.list_flatten(versus))
    parents = compute.list_parent_indices(shortlist)
    unequal = compute.unique(parents.filter(compute.invert(elements)))
    kept = compute.invert(compute.is_in(_row_numbers(len(shortlist)), value_set=unequal))
    return compute.replace_with_mask(candidates, candidates, kept)


def _is_listlike(kind: Any) -> bool:
    """Whether `kind` holds a sequence of values per row."""
    kinds = pyarrow.types
    return kinds.is_list(kind) or kinds.is_large_list(kind) or kinds.is_fixed_size_list(kind)


def _comparable_pair(left: Any, right: Any) -> tuple[Any, Any]:
    """Two arrays on one type Arrow's kernels compare: storage, decoded, `right` cast to `left`."""
    left, right = storage_of(left), storage_of(right)
    if pyarrow.types.is_dictionary(left.type):
        left = left.dictionary_decode()
    if pyarrow.types.is_dictionary(right.type):
        right = right.dictionary_decode()
    if right.type != left.type:
        right = right.cast(left.type, safe=False)
    if pyarrow.types.is_map(left.type):
        # A map is a list of its entries, and no list kernel takes one as a
        # map: one of them aborts the interpreter rather than raising.
        entry = pyarrow.struct([left.type.key_field, left.type.item_field])
        entries = pyarrow.list_(pyarrow.field("entries", entry, nullable=False))
        left, right = left.cast(entries), right.cast(entries)
    return left, right


def _in_order(taken: Any) -> Any:
    """Row positions, put back into the table's own order.

    The positions themselves are sorted, never the ranks `sort_indices` answers:
    taking a table by ranks reads whichever rows sit at those ranks instead of
    the ones that were found.
    """
    positions = one_array(taken)
    return positions.take(pyarrow.compute.sort_indices(positions))


def first_rows(table: pyarrow.Table, join: Sequence[str]) -> pyarrow.Table:
    """One row per distinct key -- the first -- in the table's own order.

    What makes a keyed write idempotent *within* a chunk: a stream that
    carries a line twice means the line once, and the first row is the one
    an earlier write of the same stream would have landed.

    A table already ordered on `join` is answered by comparing neighbours,
    because a sort is what puts equal keys next to each other: the first of
    each run *is* the first of its key. That is the shape a stream arrives
    in -- rows sorted by the key they are keyed on -- and it costs a pass
    rather than a hash table over every key in the chunk: measured on 524,288
    unique keys, 1.8 ms and 0.1 MiB against 153 ms and 45.8 MiB. Anything
    else is grouped, and a table with no duplicate keys still comes back
    untouched.
    """
    if table.num_rows < 2 or not join:
        return table
    if in_sort_order(table, join):
        repeats = _repeats_its_neighbour(table, join)
        if not pyarrow.compute.any(repeats, min_count=0).as_py():
            return table
        return table.filter(pyarrow.compute.invert(repeats))
    keys = keys_of(table, join, SOURCE_INDEX)
    firsts = keys.group_by(list(join)).aggregate([(SOURCE_INDEX, "min")])
    if firsts.num_rows == table.num_rows:
        return table
    indices = firsts.column(f"{SOURCE_INDEX}_min").combine_chunks()
    return table.take(indices.take(pyarrow.compute.sort_indices(indices)))


def _repeats_its_neighbour(table: pyarrow.Table, join: Sequence[str]) -> Any:
    """Which rows carry the key of the row before them, grouping's way.

    Two nulls are one key and two NaNs are one key, because that is what a
    `group_by` on those columns answers. A float key of either sign of zero
    is one key too, and by both routes: Arrow equality says so here, and
    `keys_of` normalises it for the group below.
    """
    compute = pyarrow.compute
    same = None
    for name in join:
        column = comparable(table.column(name))
        before, after = column[:-1], column[1:]
        equal = compute.fill_null(compute.equal(before, after), False)
        equal = compute.or_(
            equal,
            compute.and_(compute.is_null(before), compute.is_null(after)),
        )
        if pyarrow.types.is_floating(column.type):
            equal = compute.or_(
                equal,
                compute.and_(
                    compute.fill_null(compute.is_nan(before), False),
                    compute.fill_null(compute.is_nan(after), False),
                ),
            )
        same = equal if same is None else compute.and_(same, equal)
    # The first row has no neighbour before it, so it is never a repeat.
    return pyarrow.concat_arrays([pyarrow.array([False]), same])
