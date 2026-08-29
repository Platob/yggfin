"""A class's instances as one Arrow column: rows in, columns out.

`classes.py` builds a class out of a field; this builds an *array* out of that
class's instances. The declaration already says every member's Arrow type, so
nothing is sniffed per row and no row is ever held as a dictionary: each member
is read straight off the objects into a column, and a nested class or a
repeating group recurses the same way.

The one thing a class may still have to say is how it *spells* a member its
column stores differently from the attribute -- a time-anchored market hash is
an integer in hand and sixteen bytes in a column. That is one call per member,
keyed by the member's name, which is why `market.identity.stored_member` is
keyed by name too.
"""

from __future__ import annotations

import dataclasses
import functools
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pyarrow

from rekep.fields import arrays
from rekep.fields.field import Field, StructField

#: How a class spells one member for its column: `(name, value) -> stored`.
#: A class with nothing to say passes nothing and pays no call.
Spelling = Callable[[str, Any], Any]


def struct_array(
    declared: StructField,
    rows: Sequence[Any],
    spell: Spelling | None = None,
    owner: type | None = None,
    *,
    required: bool = False,
) -> pyarrow.StructArray:
    """`rows` as one struct column of exactly the type `declared` says.

    Member by member: one attribute read per row per member, one Arrow call per
    member, and a nested class or repeating group recursing into the same walk.

    `required` is the declaration's own `NOT NULL`, and it decides what a row
    of `None` means: a nullable struct masks the row, a required one has no
    null to record and writes each member's zero instead -- which is what Arrow
    itself writes there, and what parquet will accept.
    """
    members = declared.fields
    owner = owner if owner is not None else _owner_of(rows)
    attributes = _attributes(owner)
    columns = [
        _column(
            member,
            _values(rows, attributes.get(member.name, member.name), member.name, spell),
            spell,
        )
        for member in members
    ]
    if not columns:
        return pyarrow.array([None if row is None else {} for row in rows], type=declared.dtype)
    fields = [member.into_arrow_field() for member in members]
    mask = None if required else _mask(rows)
    return pyarrow.StructArray.from_arrays(columns, fields=fields, mask=mask)


def _values(rows: Sequence[Any], attribute: str, name: str, spell: Spelling | None) -> list[Any]:
    """One member off every row, spelled the way its column holds it."""
    if spell is None:
        return [None if row is None else getattr(row, attribute, None) for row in rows]
    return [None if row is None else spell(name, getattr(row, attribute, None)) for row in rows]


def _column(member: Field, values: list[Any], spell: Spelling | None) -> pyarrow.Array:
    """One member's values as its column, recursing while they are objects."""
    dtype = member.dtype
    required = member.nullable is False
    if pyarrow.types.is_struct(dtype) and _is_row(_first(values)):
        return struct_array(
            Field.from_arrow_type(dtype, member.name), values, spell, required=required
        )
    if _is_list(dtype):
        return _list_column(member, values, spell, required=required)
    # Everything else Arrow builds itself from the values as they are: a leaf,
    # a mapping, and any shape a spelling already reduced to one.
    return (
        _dense(pyarrow.array(values, type=dtype)) if required else pyarrow.array(values, type=dtype)
    )


def _list_column(
    member: Field, values: list[Any], spell: Spelling | None, *, required: bool
) -> pyarrow.Array:
    """A repeating group: its entries flattened, built once, and cut back."""
    entry = member.item
    flattened: list[Any] = []
    sizes: list[int] = []
    for one in values:
        if one is None:
            sizes.append(0)
            continue
        flattened.extend(one)
        sizes.append(len(one))
    built = _column(entry, flattened, spell)
    return arrays.build_list(
        member.dtype,
        pyarrow.array(sizes, type=pyarrow.int64()),
        built,
        None if required else _mask(values),
    )


def _dense(array: pyarrow.Array) -> pyarrow.Array:
    """The same values carrying no validity bitmap, so a required column has none.

    A member a row must carry has no null to record; where the row itself was
    missing, its members hold their type's zero -- an empty string, a zero
    integer -- which is exactly what Arrow's own builder writes there and the
    only shape parquet will accept for a `NOT NULL` column.
    """
    if array.null_count == 0:
        return array
    if array.type.num_fields:
        # A nested required member of a row that is null: no buffer of this
        # array holds its values, so the first row that has one stands in.
        valid = array.is_valid().to_pylist()
        keep = next((index for index, one in enumerate(valid) if one), None)
        if keep is None:
            return array
        positions = [index if one else keep for index, one in enumerate(valid)]
        return array.take(pyarrow.array(positions, type=pyarrow.int32()))
    own = array.type.num_buffers
    return pyarrow.Array.from_buffers(
        array.type,
        len(array),
        [None, *array.buffers()[1:own]],
        null_count=0,
        offset=array.offset,
    )


def _mask(rows: Sequence[Any]) -> pyarrow.Array | None:
    """Which rows are null, or None when none are -- a builder wants no mask then."""
    missing = [row is None for row in rows]
    return pyarrow.array(missing, type=pyarrow.bool_()) if any(missing) else None


def _first(values: Sequence[Any]) -> Any:
    """The first value that is one, so a column's shape is asked once."""
    return next((one for one in values if one is not None), None)


def _is_row(value: Any) -> bool:
    """Whether a value is an object to walk rather than a shape Arrow reads."""
    return dataclasses.is_dataclass(value) and not isinstance(value, type)


def _is_list(dtype: pyarrow.DataType) -> bool:
    """Every list flavour `arrays.build_list` can cut."""
    kinds = pyarrow.types
    return (
        kinds.is_list(dtype)
        or kinds.is_large_list(dtype)
        or kinds.is_list_view(dtype)
        or kinds.is_large_list_view(dtype)
        or kinds.is_fixed_size_list(dtype)
    )


def _owner_of(rows: Sequence[Any]) -> type | None:
    """The class a column of rows is of, or None when there is nothing to ask."""
    first = _first(rows)
    return type(first) if first is not None else None


@functools.cache
def _attributes(owner: type | None) -> Mapping[str, str]:
    """`{column name: attribute}`, which differ only where Python made them.

    Read once per class: a column called `yield` is an attribute called
    `yield_`, and every other column is its own attribute.
    """
    if owner is None or not dataclasses.is_dataclass(owner):
        return {}
    columns_of = getattr(owner, "into_field_columns", None)
    columns = dict(columns_of()) if callable(columns_of) else {}
    return {
        columns.get(member.name, member.name): member.name for member in dataclasses.fields(owner)
    }
