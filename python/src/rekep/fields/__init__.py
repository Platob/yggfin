"""Fields: a dataclass is its own Arrow schema."""

from rekep.fields.arrow import cast_batch, cast_reader, merge_fields, merge_schemas
from rekep.fields.classes import ClassBuilder
from rekep.fields.field import DESCRIPTION, NAME, NAMESPACE, Field, FieldBuilder, field

__all__ = [
    "DESCRIPTION",
    "NAME",
    "NAMESPACE",
    "ClassBuilder",
    "Field",
    "FieldBuilder",
    "cast_batch",
    "cast_reader",
    "field",
    "merge_fields",
    "merge_schemas",
]
