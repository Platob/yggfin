"""Projecting a class's type hints onto fields."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import pathlib
import types
import uuid
from typing import Any, ClassVar, Union, get_args, get_origin, get_type_hints

import pyarrow

from rekep.annotations import (
    MAPPING_ORIGINS,
    SEQUENCE_ORIGINS,
    SET_ORIGINS,
    docstring_attributes,
    docstring_summary,
    item_annotation,
    unwrap_optional,
)
from rekep.fields.field import DESCRIPTION, NAMESPACE, Field, StructField


class FieldBuilder:
    """Projects Python type hints onto fields, one case at a time.

    The cases are: `Annotated` unwraps to what it declares, `X | None` becomes a
    nullable field and **anything else becomes a non-nullable one**, a dataclass
    becomes a struct, a sequence becomes a list of a field (so item nullability
    survives), a mapping becomes a map, an enum becomes its value type, and a
    leaf is looked up in `SCALARS`.

    Subclass and extend `SCALARS`, or override `scalar`, to teach it a type it
    does not know, and point a class at it with `FIELD_BUILDER`.
    """

    #: Leaf Python type -> Arrow type. Matched exactly first, then by subclass.
    SCALARS: ClassVar[dict[type, pyarrow.DataType]] = {
        bool: pyarrow.bool_(),
        int: pyarrow.int64(),
        float: pyarrow.float64(),
        str: pyarrow.string(),
        bytes: pyarrow.binary(),
        datetime.datetime: pyarrow.timestamp("us"),
        datetime.date: pyarrow.date32(),
        datetime.time: pyarrow.time64("us"),
        datetime.timedelta: pyarrow.duration("us"),
        decimal.Decimal: pyarrow.decimal128(38, 9),
        uuid.UUID: pyarrow.string(),
        pathlib.PurePath: pyarrow.string(),
    }

    def __init__(self) -> None:
        #: Classes currently being built, so a cycle is reported not chased.
        self._building: list[type] = []

    # -- entry points -------------------------------------------------------

    def dataclass_field(self, cls: type, name: str | None = None) -> StructField:
        """`cls` as one field: a struct of its members, documented by its docstring.

        The class name is the field name and the module is metadata, so the
        projection stays self-describing wherever it travels -- a schema written
        from it can name the class it came from, and `into_dataclass` can
        rebuild that class in its own module.
        """
        metadata = {NAMESPACE: cls.__module__}
        summary = docstring_summary(cls)
        if summary:
            metadata[DESCRIPTION] = summary
        return Field(
            name=name or cls.__qualname__,
            arrow_type=self.struct(cls),
            nullable=False,
            metadata=metadata,
        )

    def fields(self, cls: type) -> list[Field]:
        """One field per dataclass member, in declaration order."""
        if not dataclasses.is_dataclass(cls):
            raise TypeError(f"{cls.__name__} must be a dataclass to be projected onto Arrow")
        hints = get_type_hints(cls, include_extras=True)
        described = docstring_attributes(cls)
        return [
            self.field(member.name, hints[member.name], description=described.get(member.name))
            for member in dataclasses.fields(cls)
        ]

    def struct(self, cls: type) -> pyarrow.DataType:
        """Struct type for `cls`."""
        if cls in self._building:
            cycle = " -> ".join(c.__name__ for c in (*self._building, cls))
            raise TypeError(f"Arrow has no recursive types, but the fields cycle: {cycle}")
        self._building.append(cls)
        try:
            return pyarrow.struct([member.into_arrow_field() for member in self.fields(cls)])
        finally:
            self._building.pop()

    # -- cases --------------------------------------------------------------

    def field(self, name: str, annotation: Any, *, description: str | None = None) -> Field:
        """One field, what the annotation declares winning over what was inferred."""
        declared, annotation = Field.unwrap(annotation)
        optional, annotation = unwrap_optional(annotation)
        if description is not None:
            declared = Field(metadata={DESCRIPTION: description}).merge(declared)
        built = Field(
            name=name,
            arrow_type=(
                declared.arrow_type
                if declared.arrow_type is not None
                else self.data_type(annotation)
            ),
            nullable=optional if declared.nullable is None else declared.nullable,
            metadata=declared.metadata,
        )
        if built.is_primary_key and built.nullable:
            raise TypeError(
                f"field {name!r} is a primary key and cannot be nullable; "
                "drop the `| None` or the key"
            )
        return built

    def data_type(self, annotation: Any) -> pyarrow.DataType:
        """Arrow type for `annotation`, recursing through containers."""
        declared, annotation = Field.unwrap(annotation)
        if declared.arrow_type is not None:
            return declared.arrow_type

        origin = get_origin(annotation)
        if origin in SEQUENCE_ORIGINS or origin in SET_ORIGINS:
            return pyarrow.list_(self.field("item", item_annotation(annotation)).into_arrow_field())
        if origin is tuple:
            return self._tuple(get_args(annotation))
        if origin in MAPPING_ORIGINS:
            key, value = (get_args(annotation) or (str, Any))[:2]
            return pyarrow.map_(self.data_type(key), self.field("value", value).into_arrow_field())
        if origin in (Union, types.UnionType):
            named = ", ".join(getattr(a, "__name__", str(a)) for a in get_args(annotation))
            raise TypeError(f"Arrow cannot infer a type for the union of {named}")

        if dataclasses.is_dataclass(annotation):
            return self.struct(annotation)

        inferred = self.scalar(annotation)
        if inferred is None:
            raise TypeError(
                f"no Arrow type for {annotation!r}; declare it with Field(arrow_type=...)"
            )
        return inferred

    def scalar(self, annotation: Any) -> pyarrow.DataType | None:
        """Arrow type for a leaf, or None when this builder does not know it."""
        if not isinstance(annotation, type):
            return None
        if annotation in self.SCALARS:
            return self.SCALARS[annotation]
        if issubclass(annotation, enum.Enum):
            return self._enum(annotation)
        for python_type, arrow_type in self.SCALARS.items():
            if issubclass(annotation, python_type):
                return arrow_type
        return None

    def _tuple(self, args: tuple[Any, ...]) -> pyarrow.DataType:
        """`tuple[X, ...]` is a list; a fixed tuple is a positional struct."""
        if not args:
            return pyarrow.list_(pyarrow.string())
        if len(args) == 2 and args[1] is Ellipsis:
            return pyarrow.list_(self.field("item", args[0]).into_arrow_field())
        return pyarrow.struct(
            [self.field(f"f{index}", arg).into_arrow_field() for index, arg in enumerate(args)]
        )

    def _enum(self, annotation: type[enum.Enum]) -> pyarrow.DataType:
        """An enum is stored as its values, which is what `into_dict` writes."""
        values = {type(member.value) for member in annotation}
        if len(values) == 1:
            inferred = self.scalar(values.pop())
            if inferred is not None:
                return inferred
        return pyarrow.string()
