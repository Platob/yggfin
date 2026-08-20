"""`Field`: one named Arrow type with metadata, and the decorator that makes a class one."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import functools
import pathlib
import re
import types
import uuid
from collections.abc import Mapping
from typing import Any, ClassVar, Self, Union, get_args, get_origin, get_type_hints

import pyarrow

from rekep.annotations import (
    MAPPING_ORIGINS,
    SEQUENCE_ORIGINS,
    SET_ORIGINS,
    docstring_attributes,
    docstring_summary,
    hide_private,
    item_annotation,
    unwrap_annotated,
    unwrap_optional,
)
from rekep.convert import Convertible
from rekep.fields.arrow import cast_batch, cast_reader, merge_schemas

#: Metadata key a documentation line lands under -- the one Arrow, parquet and
#: every viewer downstream read as the column comment.
DESCRIPTION = "description"

#: Metadata key carrying the module a class-shaped field was declared in, so
#: `into_dataclass` can give the rebuilt class its identity back.
NAMESPACE = "namespace"

#: Metadata key carrying the name of a struct field flattened into a schema.
NAME = "name"


@dataclasses.dataclass(frozen=True)
class Field(Convertible):
    """One field: a name, an Arrow type, and metadata.

    The same three things Arrow itself holds, kept as ours so a field can be
    *declared* before it is resolved. That is the one class doing both jobs:

    - As a declaration it rides in `Annotated`, saying only what inference
      cannot know -- an exact width, a unit, a comment::

          size: Annotated[int, Field(arrow_type=pyarrow.int32(), metadata={"unit": "lots"})]

      A bare `pyarrow.DataType`, `Mapping` or `str` in `Annotated` is read as
      the type, the metadata or the description, so the short forms work too.
    - As a resolved field it is what a `@field` class projects to -- name,
      struct type, metadata -- and converts from and to Arrow in both
      directions: `into_arrow_field`, `into_arrow_schema`, `from_arrow_schema`.

    Nullability is declared, never guessed: `str` is NOT NULL, `str | None` is
    nullable, and item nullability survives (`list[str | None]`). A `None`
    `nullable` means "unstated" while merging declarations, and reads as NOT
    NULL once the field is resolved.

    Being a `Convertible` dataclass, a field serialises itself -- `into_json`,
    `into_yaml`, `into_toml` dump the declaration, nested fields and all, and
    `from_dict` reads one back.
    """

    REDIRECTS: ClassVar[dict[Any, str]] = {
        **Convertible.REDIRECTS,
        pyarrow.Schema: "arrow_schema",
        pyarrow.Field: "arrow_field",
        pyarrow.DataType: "arrow_type",
    }

    name: str = ""
    arrow_type: pyarrow.DataType | None = None
    nullable: bool | None = None
    metadata: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        """Normalise metadata to a plain `str -> str` dict, never None.

        Downstream code reads `field.metadata[...]` without a guard, and Arrow
        would coerce the values on the way out anyway; doing it once here keeps
        two fields that differ only in how metadata was spelled equal.
        """
        metadata = {str(k): str(v) for k, v in (self.metadata or {}).items()}
        object.__setattr__(self, "metadata", metadata)

    # -- reading ------------------------------------------------------------

    @property
    def description(self) -> str:
        """The documentation line this field carries, or an empty string."""
        return (self.metadata or {}).get(DESCRIPTION, "")

    @functools.cached_property
    def fields(self) -> tuple[Field, ...]:
        """Members of this field, when it is a struct; empty otherwise.

        Cached: the field is frozen and an Arrow type is immutable, so the walk
        cannot come out differently the second time.
        """
        if self.arrow_type is None or not pyarrow.types.is_struct(self.arrow_type):
            return ()
        data_type = self.arrow_type
        return tuple(
            Field.from_arrow_field(data_type.field(index)) for index in range(data_type.num_fields)
        )

    def field(self, name: str) -> Field:
        """One member by name."""
        for member in self.fields:
            if member.name == name:
                return member
        raise KeyError(f"{self.name or 'field'} has no member {name!r}")

    def merge(self, other: Field) -> Field:
        """Combine two declarations, letting `other` win where it says anything."""
        return Field(
            name=other.name or self.name,
            arrow_type=other.arrow_type if other.arrow_type is not None else self.arrow_type,
            nullable=other.nullable if other.nullable is not None else self.nullable,
            metadata={**(self.metadata or {}), **(other.metadata or {})},
        )

    # -- building -----------------------------------------------------------

    @classmethod
    def of(cls, extra: Any) -> Field:
        """Read one `Annotated` argument as a declaration."""
        if isinstance(extra, Field):
            return extra
        if isinstance(extra, pyarrow.DataType):
            return cls(arrow_type=extra)
        if isinstance(extra, Mapping):
            return cls(metadata=extra)
        if isinstance(extra, str):
            return cls(metadata={DESCRIPTION: extra})
        return cls()

    @classmethod
    def unwrap(cls, annotation: Any) -> tuple[Field, Any]:
        """Split `Annotated[X, ...]` into the declaration it carries and X."""
        extras, inner = unwrap_annotated(annotation)
        declared = cls()
        for extra in extras:
            declared = declared.merge(cls.of(extra))
        return declared, inner

    @classmethod
    def from_annotation(cls, name: str, annotation: Any, *, description: str | None = None) -> Self:
        """Resolve one type hint into a field, applying what it declares."""
        return FieldBuilder().field(name, annotation, description=description)

    @classmethod
    def from_dataclass(cls, target: type, name: str | None = None) -> Self:
        """A whole class as one field: its members are the struct's members."""
        builder: type[FieldBuilder] = getattr(target, "FIELD_BUILDER", FieldBuilder)
        return builder().dataclass_field(target, name)

    @classmethod
    def from_arrow_field(cls, source: pyarrow.Field) -> Self:
        """Take an Arrow field as it stands, metadata decoded."""
        return cls(
            name=source.name,
            arrow_type=source.type,
            nullable=source.nullable,
            metadata=_decoded(source.metadata),
        )

    @classmethod
    def from_arrow_type(cls, source: pyarrow.DataType, name: str = "") -> Self:
        """An Arrow type as a field, non-nullable and undocumented."""
        return cls(name=name, arrow_type=source, nullable=False)

    @classmethod
    def from_arrow_schema(cls, source: pyarrow.Schema, name: str | None = None) -> Self:
        """A whole schema as one struct field, its identity taken back.

        The inverse of `into_arrow_schema`: a schema this package wrote carries
        the field's name and metadata, so the round trip returns the same field
        rather than an anonymous struct.
        """
        metadata = _decoded(source.metadata)
        return cls(
            name=name or metadata.pop(NAME, ""),
            arrow_type=pyarrow.struct(list(source)),
            nullable=False,
            metadata=metadata,
        )

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Self:
        """Read a field back from the plain containers `into_dict` writes."""
        metadata = dict(mapping.get("metadata") or {})
        described = mapping.get(DESCRIPTION)
        if described:
            metadata[DESCRIPTION] = described
        return cls(
            name=mapping.get(NAME, ""),
            arrow_type=cls._type_of(mapping),
            nullable=bool(mapping.get("nullable", False)),
            metadata=metadata,
        )

    @classmethod
    def _type_of(cls, mapping: Mapping[str, Any]) -> pyarrow.DataType:
        """The Arrow type one dumped field describes, nesting recursively."""
        kind = mapping.get("type")
        if kind == "struct":
            return pyarrow.struct(
                [cls.from_dict(member).into_arrow_field() for member in mapping.get("fields", [])]
            )
        if kind == "list":
            return pyarrow.list_(cls._member(mapping, "item").into_arrow_field())
        if kind == "map":
            key = cls._member(mapping, "key")
            return pyarrow.map_(
                key.into_arrow_field(), cls._member(mapping, "value").into_arrow_field()
            )
        return _arrow_type_for(str(kind))

    @classmethod
    def _member(cls, mapping: Mapping[str, Any], key: str) -> Self:
        """A nested `item`/`key`/`value` block, whose name Arrow owns."""
        return cls.from_dict({NAME: key, **(mapping.get(key) or {})})

    # -- converting ---------------------------------------------------------

    def into_arrow_field(self) -> pyarrow.Field:
        """This field as Arrow's own, unstated nullability reading NOT NULL."""
        if self.arrow_type is None:
            raise TypeError(f"field {self.name!r} has no Arrow type to convert")
        return pyarrow.field(
            self.name,
            self.arrow_type,
            nullable=bool(self.nullable),
            metadata=dict(self.metadata or {}) or None,
        )

    def into_arrow_type(self) -> pyarrow.DataType:
        """This field's Arrow type."""
        if self.arrow_type is None:
            raise TypeError(f"field {self.name!r} has no Arrow type to convert")
        return self.arrow_type

    def into_arrow_schema(self) -> pyarrow.Schema:
        """This field as a schema: its members flat, or itself as one column.

        A struct's members *are* a schema, which is what a class-shaped field
        means by "its columns". The name and metadata go to schema metadata, so
        the schema still says which field it came from wherever it travels --
        that is what `from_arrow_schema` reads back.
        """
        members = self.fields
        if not members:
            return pyarrow.schema([self.into_arrow_field()])
        return pyarrow.schema(
            [member.into_arrow_field() for member in members],
            metadata={NAME: self.name, **(self.metadata or {})},
        )

    def into_dataclass(self, name: str | None = None) -> type:
        """Rebuild a `@field` class whose projection is exactly this field.

        Imported at the point of use: the class builder decorates what it
        builds with `field`, which lives here.
        """
        from rekep.fields.classes import ClassBuilder

        return ClassBuilder().dataclass(self, name)

    def into_dict(self) -> dict[str, Any]:
        """This field as plain containers, nesting rather than flattening.

        A struct is a `fields:` list, a list an `item:`, a map a `key:`/`value:`
        pair -- never a flat `struct<...>` string, which would bury the nested
        descriptions the dump exists to show. Scalars stay one line.
        """
        nested = _describe_type(self.arrow_type)
        described: dict[str, Any] = {NAME: self.name, "type": nested.pop("type")}
        if self.nullable:
            described["nullable"] = True
        metadata = dict(self.metadata or {})
        described_as = metadata.pop(DESCRIPTION, None)
        if described_as:
            described[DESCRIPTION] = described_as
        if metadata:
            described["metadata"] = metadata
        described.update(nested)  # fields/item/key/value blocks read best last
        return described

    # -- reshaping data onto this field -------------------------------------

    def cast_arrow_batch(self, batch: pyarrow.RecordBatch, *, safe: bool = False) -> Any:
        """`batch` reshaped onto this field's schema: cast, filled, reordered.

        The declaration is the authority on what the data *is*, so a batch that
        is only nearly the right shape -- a wider integer, a column in another
        order, one this source never produced -- is adapted to it rather than
        rejected. Unsafe by default: see `fields.arrow.cast_batch`.
        """
        return cast_batch(batch, self.into_arrow_schema(), safe=safe)

    def cast_arrow_reader(
        self, reader: Any, *, safe: bool = False, merge_schema: bool = False
    ) -> pyarrow.RecordBatchReader:
        """`cast_arrow_batch` over a whole stream, still one batch at a time.

        Takes a plain iterator of batches too, so a transform's output becomes a
        reader of this field's shape in one step. `merge_schema=True` keeps the
        columns the stream has and this field does not, appended after the
        declared ones rather than dropped.
        """
        return cast_reader(reader, self.into_arrow_schema(), safe=safe, merge_schema=merge_schema)

    def merge_arrow_schema(self, incoming: pyarrow.Schema) -> pyarrow.Schema:
        """This field's schema, extended with whatever `incoming` has and it does not.

        Shared columns stay this field's (so data is cast onto them), new ones
        are added nullable -- at every level, so a struct column that grew a
        member grows here too (`fields.arrow.merge_fields`).
        """
        return merge_schemas(incoming, self.into_arrow_schema())


# -- the decorator ----------------------------------------------------------


def field(cls: type | None = None, /, **kwargs: Any) -> Any:
    """Turn a class into a field: a dataclass whose members are its Arrow struct.

    Wraps `dataclasses.dataclass`, so every keyword it takes is accepted here,
    and the declaration becomes the schema: the class projects to one `Field`,
    reachable as `FIELD`, whose `arrow_type` is a struct of its members and
    whose metadata carries the class docstring::

        @field
        class Venue(Convertible):
            mic: str
            '''ISO 10383 market identifier.'''

        Venue.FIELD.name                    # 'Venue'
        Venue.FIELD.into_arrow_schema()     # mic: string not null
        Venue.FIELD.field("mic").description

    Annotations whose name starts with `__` are dropped before the dataclass
    machinery sees them, which is how a class carries private working state --
    caches, handles, memoised views -- without it becoming a member, an
    `__init__` argument, or a column. Python mangles those names inside a class
    body, so both the written and the mangled spelling are excluded.

    Inherit `Convertible` alongside, as above, for the instance to serialise
    itself; the projection here needs no base class of its own.
    """

    def wrap(target: type) -> type:
        hide_private(target)
        built = dataclasses.dataclass(**kwargs)(target)
        built.FIELD = _ClassField()
        return built

    return wrap if cls is None else wrap(cls)


class _ClassField:
    """Descriptor building, once and on first use, the `Field` a class is.

    Lazy rather than built at decoration time: a class body may name a type
    declared further down its module, and `get_type_hints` cannot resolve a
    forward reference until the module finishes loading. The built field
    replaces the descriptor on the class that asked for it, so the walk over
    hints, docstrings and nested classes happens once per class -- and a
    subclass gets its own, not its base's.
    """

    def __get__(self, instance: Any, owner: type) -> Field:
        built = Field.from_dataclass(owner)
        owner.FIELD = built
        return built


# -- projecting python onto arrow -------------------------------------------


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

    def dataclass_field(self, cls: type, name: str | None = None) -> Field:
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
        return Field(
            name=name,
            arrow_type=(
                declared.arrow_type
                if declared.arrow_type is not None
                else self.data_type(annotation)
            ),
            nullable=optional if declared.nullable is None else declared.nullable,
            metadata=declared.metadata,
        )

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


# -- describing a type ------------------------------------------------------

#: `decimal128(38, 9)`, which has no Arrow alias and has to be rebuilt by hand.
_DECIMAL = re.compile(r"^decimal(?P<bits>128|256)\((?P<precision>\d+),\s*(?P<scale>-?\d+)\)$")

#: `timestamp[us, tz=Europe/Paris]`, likewise.
_TIMESTAMP = re.compile(r"^timestamp\[(?P<unit>\w+),\s*tz=(?P<timezone>.+)\]$")


def _arrow_type_for(text: str) -> pyarrow.DataType:
    """The Arrow type `text` names, as `str(type)` wrote it.

    Arrow parses most of its own spellings; the two it has no alias for are
    rebuilt here rather than dumped in a shape it could not read back.
    """
    decimal_type = _DECIMAL.match(text)
    if decimal_type:
        build = pyarrow.decimal128 if decimal_type["bits"] == "128" else pyarrow.decimal256
        return build(int(decimal_type["precision"]), int(decimal_type["scale"]))
    timestamp = _TIMESTAMP.match(text)
    if timestamp:
        return pyarrow.timestamp(timestamp["unit"], tz=timestamp["timezone"])
    return pyarrow.type_for_alias(text)


def _describe_type(data_type: pyarrow.DataType | None) -> dict[str, Any]:
    """One Arrow type as plain containers: a kind, and whatever nests inside it."""
    if data_type is None:
        return {"type": None}
    kinds = pyarrow.types
    if kinds.is_struct(data_type):
        return {
            "type": "struct",
            "fields": [
                Field.from_arrow_field(data_type.field(index)).into_dict()
                for index in range(data_type.num_fields)
            ],
        }
    if kinds.is_list(data_type) or kinds.is_large_list(data_type):
        return {"type": "list", "item": _describe_member(data_type.field(0))}
    if kinds.is_map(data_type):
        return {
            "type": "map",
            "key": _describe_member(data_type.key_field),
            "value": _describe_member(data_type.item_field),
        }
    return {"type": str(data_type)}


def _describe_member(member: pyarrow.Field) -> dict[str, Any]:
    """A list item or map half: a field whose name Arrow owns, not the author."""
    described = Field.from_arrow_field(member).into_dict()
    described.pop(NAME, None)
    return described


def _decoded(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    """Arrow metadata as text, which is how a `Field` holds it."""
    return {key.decode(): value.decode() for key, value in (metadata or {}).items()}
