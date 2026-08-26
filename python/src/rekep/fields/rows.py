"""Declared dataclass rows into one Arrow batch, built by columns.

The plan for a class -- one attrgetter over its members, and which members
hold dataclasses that need their own child builds -- is read from the type
hints once and cached, so a batch pays only the extraction and the typed
Arrow builders.
"""

from __future__ import annotations

import dataclasses
import functools
import operator
from collections.abc import Callable, Sequence
from typing import Any, get_origin, get_type_hints

import pyarrow

from rekep.annotations import (
    SEQUENCE_ORIGINS,
    SET_ORIGINS,
    item_annotation,
    unwrap_annotated,
    unwrap_optional,
)
from rekep.fields.arrays import build_list


def dataclass_arrow_batch(rows: Sequence[Any], schema: pyarrow.Schema) -> pyarrow.RecordBatch:
    """`rows` of one declared shape as a batch, one typed builder per column.

    Members are read directly rather than through a dict per row, so a value
    `into_dict` has to spell for a text document -- an enum, a date, a tuple
    link -- reaches Arrow natively; `bench_market` measures the difference.
    `schema` is the class's own projection, and rows are normalized instances
    of one class, which is what a `__post_init__` here guarantees.
    """
    members = list(schema)
    if not rows:
        arrays = [pyarrow.array((), type=member.type) for member in members]
    else:
        arrays = _columns(type(rows[0]), rows, members)
    return pyarrow.RecordBatch.from_arrays(arrays, schema=schema)


def _columns(cls: type, rows: Sequence[Any], members: list[pyarrow.Field]) -> list[pyarrow.Array]:
    """One array per member of `cls`, driven by its prebuilt plan."""
    picked, nested = _plan(cls)
    columns = zip(*map(picked, rows), strict=True) if len(nested) > 1 else [list(map(picked, rows))]
    return [
        _column(list(column), member.type, item)
        for column, member, item in zip(columns, members, nested, strict=True)
    ]


@functools.cache
def _plan(cls: type) -> tuple[Callable[[Any], Any], tuple[type | None, ...]]:
    """`cls`'s serialisation plan: the member reader, and who needs a child build."""
    hints = get_type_hints(cls, include_extras=True)
    names = tuple(member.name for member in dataclasses.fields(cls))
    return operator.attrgetter(*names), tuple(_dataclass_of(hints[name]) for name in names)


def _dataclass_of(annotation: Any) -> type | None:
    """The dataclass a member holds -- itself or its items -- or None."""
    _, annotation = unwrap_annotated(annotation)
    _, annotation = unwrap_optional(annotation)
    if get_origin(annotation) in SEQUENCE_ORIGINS or get_origin(annotation) in SET_ORIGINS:
        _, annotation = unwrap_annotated(item_annotation(annotation))
        _, annotation = unwrap_optional(annotation)
    return annotation if dataclasses.is_dataclass(annotation) else None


def _column(values: Sequence[Any], declared: pyarrow.DataType, item: type | None) -> pyarrow.Array:
    """One column, child-built where the plan says the values are dataclasses."""
    if item is None:
        return pyarrow.array(values, type=declared)
    if pyarrow.types.is_struct(declared):
        return _struct(values, declared, item)
    return _struct_list(values, declared, item)


def _struct(values: Sequence[Any], declared: pyarrow.DataType, item: type) -> pyarrow.StructArray:
    """Dataclass values as one struct column, each member its own child build."""
    members = [declared.field(index) for index in range(declared.num_fields)]
    nulls = [one is None for one in values]
    if any(nulls):
        mask = pyarrow.array(nulls, pyarrow.bool_())
        _, nested = _plan(item)
        columns: Sequence[Sequence[Any]] = [
            [None if one is None else getattr(one, member.name) for one in values]
            for member in members
        ]
        children = [
            _column(list(column), member.type, inner)
            for column, member, inner in zip(columns, members, nested, strict=True)
        ]
        return pyarrow.StructArray.from_arrays(children, fields=members, mask=mask)
    if not values:
        children = [pyarrow.array((), type=member.type) for member in members]
        return pyarrow.StructArray.from_arrays(children, fields=members)
    return pyarrow.StructArray.from_arrays(_columns(item, values, members), fields=members)


def _struct_list(values: Sequence[Any], declared: pyarrow.DataType, item: type) -> pyarrow.Array:
    """Lists of dataclasses as one column: flatten once, build children flat."""
    nulls = [row is None for row in values]
    sizes = pyarrow.array([0 if row is None else len(row) for row in values], pyarrow.int32())
    flat = [one for row in values if row for one in row]
    mask = pyarrow.array(nulls, pyarrow.bool_()) if any(nulls) else None
    return build_list(declared, sizes, _struct(flat, declared.value_type, item), mask)
