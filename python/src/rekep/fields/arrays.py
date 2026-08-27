"""Building one Arrow array out of another, in kernels and builders only."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any

import pyarrow
import pyarrow.compute


def sequence(length: int) -> pyarrow.Array:
    """`0 .. length - 1` as int64, built by summing a constant array.

    The one primitive the interleaving below is made of: Arrow has no "arange"
    kernel, and a Python range would be exactly the per-row work this module
    exists to avoid.
    """
    ones = pyarrow.repeat(pyarrow.scalar(1, pyarrow.int64()), length)
    return pyarrow.compute.subtract(pyarrow.compute.cumulative_sum(ones), 1)


def dense_counts(ids: pyarrow.Array, size: int) -> pyarrow.Array:
    """Counts for dense ids `0..size - 1`, including ids with no value."""
    compute = pyarrow.compute
    if not len(ids):
        return pyarrow.repeat(pyarrow.scalar(0, pyarrow.int64()), size)
    counted = compute.value_counts(ids)
    where = compute.index_in(sequence(size), value_set=counted.field("values"))
    return compute.fill_null(compute.take(counted.field("counts"), where), 0).cast(pyarrow.int64())


def null_mask(array: pyarrow.Array) -> pyarrow.Array | None:
    """Which rows are null, or None when none are -- a builder wants no mask then."""
    return array.is_null() if array.null_count else None


def groups_of(keys: pyarrow.Array) -> Iterator[tuple[Any, pyarrow.Array]]:
    """Each distinct value of `keys`, with where in the column its rows are.

    What a **one-style-per-call** transform needs from a column that mixes
    styles: the rows it can do in one pass, and the positions to put them back
    at. One `equal` and one `filter` per distinct value, so the cost counts
    styles and not rows -- and a column of one style yields one group, which is
    the case worth not paying for.
    """
    positions = sequence(len(keys))
    for key in pyarrow.compute.unique(keys).sort():
        yield key, pyarrow.compute.filter(positions, pyarrow.compute.equal(keys, key))


def scattered(parts: Sequence[pyarrow.Array], positions: Sequence[pyarrow.Array]) -> pyarrow.Array:
    """`parts` back in the row order `positions` says each of them came from.

    The inverse of a split, in two kernels and no Python: the positions of
    every part concatenated are a **permutation** of the whole column, and
    sorting a permutation is the same thing as inverting it -- so one `take`
    with the sorted indices puts every row back where it was.
    """
    if len(parts) == 1:
        return parts[0]
    order = pyarrow.concat_arrays([one.cast(pyarrow.int64()) for one in positions])
    return pyarrow.concat_arrays(parts).take(pyarrow.compute.array_sort_indices(order))


# -- list-likes -------------------------------------------------------------


def list_parts(array: pyarrow.Array) -> tuple[pyarrow.Array, pyarrow.Array]:
    """`(sizes, values)` of any list-like: list, large, view, fixed size, map.

    `list_flatten` drops the values of null and sliced-away rows, and
    `list_value_length` counts what it kept, so the two line up: offsets
    rebuilt from the sizes address exactly the values that came back.
    """
    if pyarrow.types.is_map(array.type):
        array = as_entry_list(array)
    sizes = pyarrow.compute.fill_null(pyarrow.compute.list_value_length(array), 0)
    return sizes.cast(pyarrow.int64()), pyarrow.compute.list_flatten(array)


def list_offsets(sizes: pyarrow.Array, width: pyarrow.DataType) -> pyarrow.Array:
    """Offsets for `sizes`: a leading zero and the running total, in `width`."""
    running = pyarrow.compute.cumulative_sum(sizes)
    return without_validity(
        pyarrow.concat_arrays([pyarrow.array([0], type=width), running.cast(width)])
    )


def without_validity(array: pyarrow.Array) -> pyarrow.Array:
    """The same values, carrying no validity bitmap.

    Arithmetic kernels and `concat_arrays` leave an all-valid bitmap behind,
    and a list builder reads *any* bitmap on its offsets or sizes as "these
    rows are null" -- which then conflicts with the mask it was also given
    ("Ambiguous to specify both validity map and offsets with nulls"). Dropping
    the buffer is free: no values move.
    """
    if array.buffers()[0] is None:
        return array
    return pyarrow.Array.from_buffers(
        array.type, len(array), [None, array.buffers()[1]], null_count=0, offset=array.offset
    )


def as_entry_list(array: pyarrow.Array) -> pyarrow.Array:
    """A map as the `list<struct<key, value>>` it physically already is.

    Arrow casts a map to that list itself, which keeps slicing and null rows
    right without this module touching a buffer.
    """
    data_type = array.type
    entries = pyarrow.struct([data_type.key_field, data_type.item_field])
    return array.cast(pyarrow.list_(pyarrow.field("entries", entries, nullable=False)))


def build_list(
    data_type: pyarrow.DataType,
    sizes: pyarrow.Array,
    values: pyarrow.Array,
    mask: pyarrow.Array | None = None,
) -> pyarrow.Array:
    """`values` cut into rows of `sizes`, in whichever list flavour is asked for.

    A fixed-size list is built as a plain list and cast, because that is the
    one flavour whose builder cannot be told which rows are null: Arrow's own
    cast lines the slots up.
    """
    kinds = pyarrow.types
    if kinds.is_large_list(data_type):
        offsets = list_offsets(sizes, pyarrow.int64())
        return pyarrow.LargeListArray.from_arrays(offsets, values, type=data_type, mask=mask)
    if kinds.is_list_view(data_type) or kinds.is_large_list_view(data_type):
        large = kinds.is_large_list_view(data_type)
        width = pyarrow.int64() if large else pyarrow.int32()
        build = pyarrow.LargeListViewArray if large else pyarrow.ListViewArray
        offsets = list_offsets(sizes, width)[:-1]
        return build.from_arrays(
            offsets, without_validity(sizes.cast(width)), values, type=data_type, mask=mask
        )
    if kinds.is_fixed_size_list(data_type):
        plain = pyarrow.list_(data_type.field(0))
        return build_list(plain, sizes, values, mask).cast(data_type)
    offsets = list_offsets(sizes, pyarrow.int32())
    return pyarrow.ListArray.from_arrays(offsets, values, type=data_type, mask=mask)


def list_type_like(source: pyarrow.DataType, item: pyarrow.Field) -> pyarrow.DataType:
    """The same flavour of list as `source`, around `item`."""
    kinds = pyarrow.types
    if kinds.is_large_list(source):
        return pyarrow.large_list(item)
    if kinds.is_large_list_view(source):
        return pyarrow.large_list_view(item)
    if kinds.is_list_view(source):
        return pyarrow.list_view(item)
    if kinds.is_fixed_size_list(source):
        return pyarrow.list_(item, source.list_size)
    return pyarrow.list_(item)


def rewrap_list(
    data_type: pyarrow.DataType,
    array: pyarrow.Array,
    values: pyarrow.Array,
    mask: pyarrow.Array | None = None,
) -> pyarrow.Array:
    """The same rows around new `values`, reusing the array's own row layout.

    When the source and the target are the same flavour of list, nothing about
    *where the rows are* changes -- only what is inside them. Handing the
    array's own offsets (and a view's sizes) straight to the builder makes that
    a re-wrap: no offsets are recomputed and no values are copied, which is
    several times cheaper than flattening and cutting the rows again.
    """
    kinds = pyarrow.types
    offsets = without_validity(array.offsets)
    if kinds.is_large_list(data_type):
        return pyarrow.LargeListArray.from_arrays(offsets, values, type=data_type, mask=mask)
    if kinds.is_list_view(data_type) or kinds.is_large_list_view(data_type):
        build = (
            pyarrow.LargeListViewArray
            if kinds.is_large_list_view(data_type)
            else pyarrow.ListViewArray
        )
        return build.from_arrays(
            offsets, without_validity(array.sizes), values, type=data_type, mask=mask
        )
    return pyarrow.ListArray.from_arrays(offsets, values, type=data_type, mask=mask)


def rewrap_map(
    data_type: pyarrow.DataType,
    array: pyarrow.Array,
    keys: pyarrow.Array,
    items: pyarrow.Array,
    mask: pyarrow.Array | None = None,
) -> pyarrow.Array:
    """`rewrap_list` for a map: the same entries, around cast halves."""
    return pyarrow.MapArray.from_arrays(
        without_validity(array.offsets), keys, items, type=data_type, mask=mask
    )


def build_map(
    data_type: pyarrow.DataType,
    sizes: pyarrow.Array,
    keys: pyarrow.Array,
    items: pyarrow.Array,
    mask: pyarrow.Array | None = None,
) -> pyarrow.Array:
    """Entries cut into rows of `sizes` as a map."""
    offsets = list_offsets(sizes, pyarrow.int32())
    return pyarrow.MapArray.from_arrays(offsets, keys, items, type=data_type, mask=mask)


# -- struct-likes -----------------------------------------------------------


def struct_columns(array: pyarrow.Array) -> dict[str, pyarrow.Array]:
    """A struct array's children, by name."""
    data_type = array.type
    return {
        data_type.field(index).name: array.field(index) for index in range(data_type.num_fields)
    }


def map_column(array: pyarrow.Array, key: str) -> pyarrow.Array:
    """The value each row maps `key` to, null where the row has no such entry.

    `map_lookup` is Arrow's own kernel for exactly this, so a map becoming a
    struct is one pass per member rather than a search per row.
    """
    query = pyarrow.scalar(key, type=array.type.key_type)
    return pyarrow.compute.map_lookup(array, query_key=query, occurrence="first")


def list_column(array: pyarrow.Array, index: int) -> pyarrow.Array:
    """Element `index` of every row of a list-like."""
    return pyarrow.compute.list_element(array, index)


def interleave(
    columns: list[pyarrow.Array | pyarrow.ChunkedArray], length: int
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """`(entry values, member index)` for `length` rows of `len(columns)` members."""
    columns = [
        column.combine_chunks() if isinstance(column, pyarrow.ChunkedArray) else column
        for column in columns
    ]
    members = len(columns)
    positions = sequence(length * members)
    rows = pyarrow.compute.divide(positions, members)
    member = pyarrow.compute.subtract(positions, pyarrow.compute.multiply(rows, members))
    indices = pyarrow.compute.add(pyarrow.compute.multiply(member, length), rows)
    values = pyarrow.compute.take(pyarrow.concat_arrays(columns), indices)
    return values, member


def repeat_sizes(width: int, length: int) -> pyarrow.Array:
    """`width` entries per row, for `length` rows."""
    return pyarrow.repeat(pyarrow.scalar(width, pyarrow.int64()), length)


def names_array(names: list[str], member: pyarrow.Array, key_type: pyarrow.DataType) -> Any:
    """The member names, one per entry, in the order `interleave` produced."""
    return pyarrow.compute.take(pyarrow.array(names, type=key_type), member)
