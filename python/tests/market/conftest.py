"""Batches of market events, built from the declaration rather than beside it.

A shape here has two dozen columns and a test cares about three of them, so the
builder fills what a shape *requires* and the test states what it is about.
The filling goes through `cast_arrow_batch`, which is the package's own way of
making a nearly-right batch fit -- so a column added to a shape is filled here
without this file changing, and a NOT NULL one that nothing supplies is refused
by its path instead of arriving as a silent zero.
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any

import pyarrow

from rekep.fields import Field, StructField

#: A day and an instant every builder starts from, so two batches built in one
#: test are comparable and nothing depends on the clock.
DAY = datetime.date(2024, 3, 14)
UNIX = 1710374400_000000000


def identifier(seed: int) -> uuid.UUID:
    """A stable sixteen-byte value, so a fixture is the same on every run."""
    return uuid.UUID(int=seed + 1)


def value_of(member: Field, row: int) -> Any:
    """Something of `member`'s type, for a column nothing in the test is about.

    Deliberately dull: a test asserts on what it passed in, and nothing should
    read meaning into what the scaffolding put in the rest.
    """
    kinds = pyarrow.types
    arrow_type = member.arrow_type
    if kinds.is_struct(arrow_type):
        return {inner.name: value_of(inner, row) for inner in member.fields if not inner.nullable}
    if kinds.is_list(arrow_type) or kinds.is_large_list(arrow_type):
        return []
    if kinds.is_map(arrow_type):
        return {}
    if arrow_type == pyarrow.binary(16):
        return identifier(row).bytes
    if kinds.is_date(arrow_type):
        return DAY
    if kinds.is_integer(arrow_type):
        return UNIX if member.name.endswith("unix") else 0
    if kinds.is_floating(arrow_type):
        return 0.0
    if kinds.is_boolean(arrow_type):
        return False
    return ""


def required(shape: StructField, rows: int) -> dict[str, Any]:
    """Every NOT NULL column of `shape`, filled one row at a time."""
    return {
        member.name: [value_of(member, row) for row in range(rows)]
        for member in shape.fields
        if not member.nullable
    }


def batch(shape: Any, rows: int = 1, **columns: Any) -> pyarrow.RecordBatch:
    """`rows` rows of `shape`, with `columns` overriding what they name.

    Cast onto the declaration rather than assembled to match it, so the fixture
    exercises the same path a producer takes and a column this file does not
    know about is filled as nullable rather than missing.
    """
    declared = shape if isinstance(shape, Field) else shape.into_field()
    given = required(declared, rows) | columns
    return declared.cast_arrow_batch(pyarrow.RecordBatch.from_pydict(given))
