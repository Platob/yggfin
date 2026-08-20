"""Fields: a dataclass is its own Arrow schema."""

from rekep.fields.arrow import merge_fields, merge_schemas
from rekep.fields.builder import FieldBuilder
from rekep.fields.classes import ClassBuilder
from rekep.fields.field import (
    DESCRIPTION,
    NAME,
    NAMESPACE,
    PARTITION_KEY,
    PRIMARY_KEY,
    Field,
    ListField,
    MapField,
    StructField,
    cast_batch,
    cast_reader,
    cast_table,
    field,
)

__all__ = [
    "DESCRIPTION",
    "NAME",
    "NAMESPACE",
    "PARTITION_KEY",
    "PRIMARY_KEY",
    "ClassBuilder",
    "Field",
    "FieldBuilder",
    "ListField",
    "MapField",
    "StructField",
    "cast_batch",
    "cast_reader",
    "cast_table",
    "field",
    "merge_fields",
    "merge_schemas",
]
