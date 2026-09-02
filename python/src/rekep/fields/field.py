"""`Field`: one named Arrow type with metadata, and the decorator that makes a class one."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import functools
import json
import logging
import re
import struct
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Any, Self

import pyarrow

from rekep.annotations import hide_private, restore_private_slots, unwrap_annotated
from rekep.arrow_reader import OwnedRecordBatchReader
from rekep.convert import Convertible
from rekep.fields import arrays
from rekep.fields.arrow import merge_fields, promoted_type
from rekep.fields.metadata import (
    ASCENDING as ASCENDING,
)
from rekep.fields.metadata import (
    IDENTITY as IDENTITY,
)
from rekep.fields.metadata import (
    EnumMetadata,
    FixMetadata,
    IcebergMetadata,
    ProtocolMetadata,
)
from rekep.fields.names import column_name

LOGGER = logging.getLogger(__name__)

#: Metadata key a documentation line lands under -- the one Arrow, parquet and
#: every viewer downstream read as the column comment.
DESCRIPTION = "description"

#: Metadata key carrying the module a class-shaped field was declared in, so
#: `into_dataclass` can give the rebuilt class its identity back.
NAMESPACE = "namespace"

#: Metadata key carrying the name of a struct field flattened into a schema.
NAME = "name"

#: What Arrow calls a list's element when nobody named it, so a document
#: leaves the name out; it is written back only when the author chose one.
ITEM = "item"

#: Keys a downstream protocol owns are prefixed with its name, so one
#: namespace's keys can never collide with another's. `Field.protocol` is the
#: one reader and writer of a prefix; these two spell out the keys the Iceberg
#: protocol already claims.
ICEBERG = "iceberg"
FIX = "fix"
ENUM = "enum"
PRIMARY_KEY = "iceberg:primary_key"
PARTITION_KEY = "iceberg:partition_key"
#: Iceberg identifies a column by id and never by name, so an id is part of
#: what a schema *is* once a table exists. It rides under the protocol's own
#: prefix like every other Iceberg key -- the ecosystem's `PARQUET:field_id`
#: is what parquet files carry, and the two are translated at the Iceberg
#: boundary rather than mixed here.
FIELD_ID = "iceberg:field_id"

#: Which columns a table is kept sorted by, and which way.
SORT_KEY = "iceberg:sort_key"

#: Exact ordered sort fields read from an Iceberg table. A root declaration is
#: needed because an external table's priority need not follow schema order.
SORT_ORDER = "iceberg:sort_order"

#: Keys owned by the field document rather than by a protocol map.
_DOCUMENT_KEYS = frozenset(
    {
        NAME,
        "type",
        "nullable",
        DESCRIPTION,
        "metadata",
        "fields",
        "keys_sorted",
        "list_size",
    }
)

#: The declaration; everything else a field holds is derived from these.
DECLARED = ("name", "dtype", "nullable", "metadata")

_DERIVED = (
    "fields",
    "_by_name",
    "item",
    "key",
    "value",
    "arrow_field",
    "arrow_fields",
    "arrow_schema",
)
#: The typed view each known protocol answers with; anything else is generic.
_PROTOCOLS: Mapping[str, type[ProtocolMetadata]] = MappingProxyType(
    {ICEBERG: IcebergMetadata, FIX: FixMetadata, ENUM: EnumMetadata}
)

_FIELD_CASTS = MappingProxyType(
    {
        pyarrow.Array: "arrow_array",
        pyarrow.ChunkedArray: "arrow_array",
    }
)


def _protocol_keyed(metadata: Mapping[str, Any]) -> bool:
    """Whether a declaration says anything under the FIX protocol prefix."""
    prefix = f"{FIX}:"
    return any(str(key).startswith(prefix) for key in metadata)


@dataclasses.dataclass(eq=True)
class Field(Convertible):
    """One field: a name, an Arrow type, and metadata."""

    @classmethod
    @functools.cache
    def into_redirects(cls) -> Mapping[Any, str]:
        """Conversions inferred for fields and their serialized forms."""
        return MappingProxyType(
            {
                **super().into_redirects(),
                Field: "field",
                pyarrow.Schema: "arrow_schema",
                pyarrow.Field: "arrow_field",
                pyarrow.DataType: "arrow_type",
                # Last, so every narrower key wins: a class declares its own
                # shape, and anything else lands in `from_class`'s refusal.
                object: "class",
            }
        )

    @classmethod
    @functools.cache
    def into_casts(cls) -> Mapping[Any, str]:
        """Arrow values this field can cast, keyed by source type."""
        return _FIELD_CASTS

    #: Container this field is a member of, when it was reached through one.
    #: Written without an annotation on purpose: it is a link between fields,
    #: not part of the declaration, so the dataclass must not see it.
    _parent = None

    name: str = ""
    dtype: pyarrow.DataType | None = None
    nullable: bool | None = None
    metadata: Mapping[str, str] | None = None

    def __new__(
        cls,
        name: str = "",
        dtype: pyarrow.DataType | None = None,
        nullable: bool | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> Field:
        """Redirect to the subclass `dtype` calls for.

        Declared here rather than in a factory so that every path that builds
        a field -- `Field(...)`, `from_arrow_field`, `from_dict`, a builder,
        `dataclasses.replace` -- lands on the right class without any of them
        having to know the rule. The package's own kind classes follow the
        type: replacing a timestamp field's dtype with a date re-dispatches,
        so equality with a fresh declaration holds. A subclass declared
        outside the dispatch table is honoured as written.
        """
        if cls is Field:
            return object.__new__(_class_for(dtype))
        if cls in _kind_classes():
            wanted = _class_for(dtype)
            if wanted is not cls:
                # A sideways re-dispatch builds the field whole: Python only
                # runs `__init__` when `__new__` answers an instance of `cls`,
                # and this answer deliberately is not one.
                return wanted(name, dtype, nullable, metadata)
        return object.__new__(cls)

    def __post_init__(self) -> None:
        """Normalise metadata to a plain `str -> str` dict, never None.

        Downstream code reads `field.metadata[...]` without a guard, and Arrow
        would coerce the values on the way out anyway; doing it once here keeps
        two fields that differ only in how metadata was spelled equal.
        """
        self.metadata = {str(k): str(v) for k, v in (self.metadata or {}).items()}

    def __setattr__(self, name: str, value: Any) -> None:
        """Assign, then drop what was derived and tell the container.

        A field is a view of the container it came from, so a change has to
        reach the struct, list or map holding it -- otherwise setting a key on
        a member would be silently lost. Rebuilding the parent sets *its* type,
        which recurses, so a change at any depth reaches the root.
        """
        super().__setattr__(name, value)
        if name in DECLARED:
            for derived in _DERIVED:
                self.__dict__.pop(derived, None)
            if self._parent is not None:
                self._parent._member_changed(self)

    def _member_changed(self, member: Field) -> None:
        """A member of this container changed; rebuild around it."""

    def _member_of(self, member: pyarrow.Field) -> Field:
        """One Arrow field as a live member of this container."""
        built = Field.from_arrow_field(member)
        built._parent = self
        return built

    @property
    def fields(self) -> tuple[Field, ...]:
        """Fields inside this one; a leaf has none."""
        return ()

    # -- reading ------------------------------------------------------------

    @property
    def description(self) -> str:
        """The documentation line this field carries, or an empty string."""
        return self.metadata.get(DESCRIPTION, "")

    @description.setter
    def description(self, value: str) -> None:
        self.metadata = {**self.metadata, DESCRIPTION: value}

    def protocol(self, prefix: str) -> ProtocolMetadata:
        """This field's metadata under one protocol's prefix, live.

        The one reader and writer of `prefix:key` keys: a protocol never
        spells its prefix at a call site, so two spellings of one key cannot
        drift apart. The proxy is a view -- `field.protocol("iceberg")["x"]`
        reads the metadata in place, and a write through it mutates the
        original mapping and rebuilds the containers above exactly as
        assigning `metadata` would. A protocol this package knows answers
        with its typed view.
        """
        return _PROTOCOLS.get(prefix, ProtocolMetadata)(self, prefix)

    def _metadata_changed(self) -> None:
        """Metadata mutated under this field in place: drop what was derived
        from it and tell the container, without copying the mapping."""
        for derived in _DERIVED:
            self.__dict__.pop(derived, None)
        if self._parent is not None:
            self._parent._member_changed(self)

    @property
    def iceberg(self) -> IcebergMetadata:
        """The keys the Iceberg protocol owns, typed: `iceberg:primary_key`, ..."""
        return IcebergMetadata(self, ICEBERG)

    @property
    def fix(self) -> FixMetadata:
        """The keys the FIX protocol owns, typed: `fix:tag`, `fix:type`, ..."""
        return FixMetadata(self, FIX)

    @property
    def enum(self) -> EnumMetadata:
        """The keys the enum protocol owns, typed: `enum:name`, `enum:values`, ..."""
        return EnumMetadata(self, ENUM)

    @property
    def is_primary_key(self) -> bool:
        """Whether this field is part of the primary key.

        The one list Iceberg calls identifier fields and an upsert joins on --
        declared once, read from metadata like every other protocol property.
        """
        return self.iceberg.primary_key

    @is_primary_key.setter
    def is_primary_key(self, value: bool) -> None:
        self.iceberg.primary_key = value

    @property
    def is_partition_key(self) -> bool:
        """Whether the data is partitioned on this field."""
        return bool(self.iceberg.partition_key)

    @is_partition_key.setter
    def is_partition_key(self, value: bool | str) -> None:
        """Set the partition transform: True is `identity`, a string is itself."""
        self.iceberg.partition_key = value

    @property
    def field_id(self) -> int | None:
        """The Iceberg column id this field carries, or None when it has none."""
        return self.iceberg.field_id

    @field_id.setter
    def field_id(self, value: int | None) -> None:
        self.iceberg.field_id = value

    @property
    def partition_transform(self) -> str:
        """How the data is partitioned on this field, or an empty string."""
        return self.iceberg.partition_key

    @property
    def derived_from(self) -> tuple[str, ...]:
        """Columns this field is a function of, or nothing when it stands alone."""
        return self.iceberg.derived_from

    @derived_from.setter
    def derived_from(self, value: str | Sequence[str] | None) -> None:
        self.iceberg.derived_from = value

    @property
    def is_sort_key(self) -> bool:
        """Whether the data is kept sorted on this field.

        A *sort order*, not a partition: it does not decide which file a row
        lands in, it decides where in the file it lands. That is what makes a
        range filter on it read a few row groups instead of all of them, and
        what makes the column's own min/max in a manifest narrow instead of
        spanning everything the file holds.
        """
        return bool(self.iceberg.sort_key)

    @is_sort_key.setter
    def is_sort_key(self, value: bool | str) -> None:
        """Set the direction: True is ascending, a string is itself."""
        self.iceberg.sort_key = value

    @property
    def sort_direction(self) -> str:
        """Which way the data is sorted on this field, or an empty string."""
        return self.iceberg.sort_key

    def merge(self, other: Field) -> Field:
        """Combine two declarations, letting `other` win where it says anything.

        Winning is per reading, not per key: a later declaration overrides the
        description it restates, and *adds* to the spellings, tags, versions
        and values the earlier one gathered. Overwriting those would make an
        overlay that names one alias drop every other alias the registry holds
        for the identity.

        The type is the exception: it is the one reading a declaration can
        *lose* rather than restate, so two readings widen into the type that
        holds both, at every depth.
        """
        built = Field(
            name=other.name or self.name,
            dtype=promoted_type(self.dtype, other.dtype),
            nullable=other.nullable if other.nullable is not None else self.nullable,
            metadata={**self.metadata, **other.metadata},
        )
        if _protocol_keyed(self.metadata) and _protocol_keyed(other.metadata):
            built.fix.accumulate(self.fix)
        return built

    def with_name(self, name: str) -> Self:
        """A copy carrying `name`, without changing this declaration."""
        return dataclasses.replace(self, name=name)

    def merge_with(self, other: Any) -> Field:
        """This field widened with whatever `other` has and it does not."""
        return self.merge_with_arrow_field(Field.from_(other).into_arrow_field())

    def merge_with_arrow_field(self, other: pyarrow.Field) -> Field:
        """`merge_with`, for an Arrow field already in hand."""
        return Field.from_arrow_field(merge_fields(other, self.into_arrow_field()))

    # -- building -----------------------------------------------------------

    @classmethod
    def of(cls, extra: Any) -> Field:
        """Read one `Annotated` argument as a declaration."""
        if isinstance(extra, Field):
            return extra
        if isinstance(extra, pyarrow.DataType):
            return cls(dtype=extra)
        if isinstance(extra, Mapping):
            return cls(metadata=extra)
        if isinstance(extra, str):
            return cls(metadata={DESCRIPTION: extra})
        return Field()

    @classmethod
    def column(cls, name: str = "", **declared: Any) -> Field:
        """A declaration for a column the FIX dictionary does not name.

        A column's name is folded -- lowercase letters and digits, nothing
        else -- so `sourceurl` cannot spell the words it is made of. This is
        where it states the readable protocol name.
        """
        built = cls(**declared)
        if name:
            built.fix.name = name
        return built

    @classmethod
    def primary_key(cls, **declared: Any) -> Field:
        """A declaration marking its member part of the primary key."""
        built = Field(**declared)
        built.is_primary_key = True
        return built

    @classmethod
    def partition_key(
        cls,
        transform: bool | str = True,
        derived_from: str | Sequence[str] = (),
        **declared: Any,
    ) -> Field:
        """A declaration partitioning the data on its member.

        `derived_from` names the columns this one is a function of, for a
        partition column that is a denormalisation of something already stored.
        """
        built = Field(**declared)
        built.is_partition_key = transform
        if derived_from:
            built.derived_from = derived_from
        return built

    @classmethod
    def sort_key(cls, direction: bool | str = True, **declared: Any) -> Field:
        """A declaration keeping the data sorted on its member.

        `True` is ascending; `"desc"` is descending. Several members may
        declare one, and **the declaration order is the sort order** -- there
        is no position to keep consistent, because the struct already has one.
        """
        built = Field(**declared)
        built.is_sort_key = direction
        return built

    @classmethod
    def unwrap(cls, annotation: Any) -> tuple[Field, Any]:
        """Split `Annotated[X, ...]` into the declaration it carries and X."""
        extras, inner = unwrap_annotated(annotation)
        declared = Field()
        for extra in extras:
            declared = declared.merge(cls.of(extra))
        return declared, inner

    @classmethod
    def from_annotation(
        cls, name: str, annotation: Any, *, description: str | None = None
    ) -> Field:
        """Resolve one type hint into a field, applying what it declares."""
        from rekep.fields.builder import FieldBuilder

        return FieldBuilder().field(name, annotation, description=description)

    @classmethod
    def from_dataclass(cls, target: type, name: str | None = None) -> StructField:
        """A whole class as one field: its members are the struct's members."""
        from rekep.fields.builder import FieldBuilder

        builder_of = getattr(target, "into_field_builder", None)
        builder: type[FieldBuilder] = builder_of() if callable(builder_of) else FieldBuilder
        return builder().dataclass_field(target, name)

    @classmethod
    def from_field(cls, source: Field, name: str = "") -> Field:
        """A field is already one; `name` renames a copy of it."""
        return source.with_name(name) if name else source

    @classmethod
    def from_class(cls, source: Any, name: str = "") -> Field:
        """Whatever declares a shape by identity: a field, or a class.

        The terminal redirect, so it is also where anything that names no shape
        at all is refused.
        """
        if isinstance(source, Field):
            return cls.from_field(source, name)
        declared = getattr(source, "into_field", None)
        built = declared() if callable(declared) else None
        if isinstance(built, Field):
            return built.with_name(name) if name else built
        if isinstance(source, type) and dataclasses.is_dataclass(source):
            return cls.from_dataclass(source, name or None)
        raise TypeError(
            f"{source!r} does not name a shape: pass a Field, an Arrow schema or a class"
        )

    @classmethod
    def from_arrow_field(cls, source: pyarrow.Field) -> Field:
        """Take an Arrow field as it stands, metadata decoded."""
        return Field(
            name=source.name,
            dtype=source.type,
            nullable=source.nullable,
            metadata=decoded(source.metadata),
        )

    @classmethod
    def from_arrow_type(cls, source: pyarrow.DataType, name: str = "") -> Field:
        """An Arrow type as a field, non-nullable and undocumented."""
        return Field(name=name, dtype=source, nullable=False)

    @classmethod
    def from_arrow_schema(cls, source: pyarrow.Schema, name: str | None = None) -> StructField:
        """A whole schema as one struct field, its identity taken back.

        The inverse of `into_arrow_schema`: a schema this package wrote carries
        the field's name and metadata, so the round trip returns the same field
        rather than an anonymous struct.
        """
        metadata = decoded(source.metadata)
        return Field(
            name=name or metadata.pop(NAME, ""),
            dtype=pyarrow.struct(list(source)),
            nullable=False,
            metadata=metadata,
        )

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Field:
        """Read a field back from the plain containers `into_dict` writes."""
        metadata = _document_metadata(mapping)
        described = mapping.get(DESCRIPTION)
        if described:
            metadata[DESCRIPTION] = described
        return Field(
            name=mapping.get(NAME, ""),
            dtype=cls._type_of(mapping),
            nullable=_flag(mapping, "nullable"),
            metadata=metadata,
        )

    @classmethod
    def _type_of(cls, mapping: Mapping[str, Any]) -> pyarrow.DataType:
        """The Arrow type one dumped field describes, nesting recursively.

        Every container names its own flavour, so a contract file read back is
        the type that was dumped and not a resemblance of it: `large_list`
        stays 64-bit, a view stays a view, and a `fixed_size_list` keeps the
        width that is part of its type.
        """
        kind = mapping.get("type")
        if kind is None:
            raise ValueError(
                f"field {mapping.get(NAME, '')!r} has no type: a contract says what the data is, "
                'so every field names an Arrow type ("int64", "struct", "list", "map", ...)'
            )
        kind = str(kind)
        if kind == "struct":
            return pyarrow.struct(cls._fields_of(mapping.get("fields") or ()))
        if kind == "map":
            key, value = cls._map_halves(mapping)
            return pyarrow.map_(key, value, keys_sorted=_flag(mapping, "keys_sorted"))
        if kind == "fixed_size_list":
            return pyarrow.list_(cls._item(mapping), _list_size(mapping))
        build = _LIST_KINDS.get(kind)
        if build is not None:
            return build(cls._item(mapping))
        return arrow_type_for(kind)

    @classmethod
    def _fields_of(cls, blocks: Sequence[Mapping[str, Any]], *named: str) -> list[pyarrow.Field]:
        """`blocks` as Arrow fields, under the names `_anonymous` drops.

        A struct's members name themselves, so `named` is empty for one; a
        name the document does write always wins.
        """
        return [
            cls.from_dict(
                {NAME: named[index] if index < len(named) else "", **block}
            ).into_arrow_field()
            for index, block in enumerate(blocks)
        ]

    @classmethod
    def _item(cls, mapping: Mapping[str, Any]) -> pyarrow.Field:
        """The one field a list repeats."""
        blocks = mapping.get("fields") or ()
        if len(blocks) != 1:
            raise ValueError(
                f"list {mapping.get(NAME, '')!r} holds {len(blocks)} fields, and a list repeats "
                "exactly one; dump it with into_dict() rather than writing it by hand"
            )
        return cls._fields_of(blocks, ITEM)[0]

    @classmethod
    def _map_halves(cls, mapping: Mapping[str, Any]) -> tuple[pyarrow.Field, pyarrow.Field]:
        """A map entry's two halves, read from the document rather than rebuilt.

        The entry is *checked* to be a struct, never constructed as one: a
        struct read supplies no positional names, so both halves would come
        back named `""`, `_anonymous` would keep that spelling on the next dump
        -- it only drops the name Arrow owns -- and a store would look mutated
        every time it was rewritten.
        """
        name = mapping.get(NAME, "")
        blocks = mapping.get("fields") or ()
        if len(blocks) != 1:
            raise ValueError(
                f"map {name!r} holds {len(blocks)} fields, and a map holds one entry; "
                "dump it with into_dict() rather than writing it by hand"
            )
        entry = blocks[0]
        if str(entry.get("type")) != "struct":
            raise ValueError(
                f"map {name!r} holds a {entry.get('type')!r} entry, and a map's entry is a struct "
                "of a key and a value; dump it with into_dict() rather than writing it by hand"
            )
        halves = entry.get("fields") or ()
        if len(halves) != 2:
            raise ValueError(
                f"map {name!r} holds an entry of {len(halves)} fields, and one entry is a key "
                "and a value; dump it with into_dict() rather than writing it by hand"
            )
        key, value = cls._fields_of(halves, "key", "value")
        # Arrow forces a map key NOT NULL, so a document saying otherwise would
        # read back as a type the cast that follows refuses.
        return key.with_nullable(False), value

    # -- converting ---------------------------------------------------------

    @functools.cached_property
    def arrow_field(self) -> pyarrow.Field:
        """This field as Arrow's own, built once per declaration."""
        if self.dtype is None:
            raise TypeError(f"field {self.name!r} has no Arrow type to convert")
        return pyarrow.field(
            self.name,
            self.dtype,
            nullable=bool(self.nullable),
            metadata=dict(self.metadata) or None,
        )

    def into_arrow_field(self) -> pyarrow.Field:
        """This field as Arrow's own, unstated nullability reading NOT NULL."""
        return self.arrow_field

    def into_arrow_type(self) -> pyarrow.DataType:
        """This field's Arrow type."""
        if self.dtype is None:
            raise TypeError(f"field {self.name!r} has no Arrow type to convert")
        return self.dtype

    def into_arrow_schema(self) -> pyarrow.Schema:
        """This field as a schema of one column."""
        return pyarrow.schema([self.into_arrow_field()])

    def into_iceberg_field(self, field_id: int = 1) -> Any:
        """This field as a `pyiceberg` NestedField.

        Imported at the point of use: the Iceberg projection lives with the
        rest of the Iceberg code, and pyiceberg is an extra.
        """
        from rekep.iceberg.fields import iceberg_field

        return iceberg_field(self, field_id)

    def into_dict(self) -> dict[str, Any]:
        """This field as plain containers, nesting rather than flattening.

        Every container writes one `fields:` block, so a walker that knows
        `fields` has walked the whole tree and a member reads back the same
        whether it was written as a struct's or as the thing a list repeats:
        the `PartyID` that `NoPartyIDs <453>` repeats is the one field its
        list holds. A map writes one entry, which is a struct of its key and
        its value. Scalars stay one line, and a flat `struct<...>` string
        would bury the nested descriptions the dump exists to show.
        """
        described: dict[str, Any] = {NAME: self.name, "type": self.kind()}
        if self.nullable:
            described["nullable"] = True
        metadata = dict(self.metadata)
        described_as = metadata.pop(DESCRIPTION, None)
        if described_as:
            described[DESCRIPTION] = described_as
        plain, protocols = _protocol_maps(metadata)
        if plain:
            described["metadata"] = plain
        described.update(protocols)
        described.update(self.nested())  # the members read best last
        return described

    def _dump_yaml(self, yaml: Any) -> str:
        """Keep metadata and protocol maps in compact YAML flow form."""

        class Dumper(yaml.SafeDumper):
            pass

        Dumper.add_representer(_FlowMap, _represent_flow_map)
        return yaml.dump(
            _yaml_document(self.into_dict()),
            Dumper=Dumper,
            sort_keys=False,
            allow_unicode=True,
            width=1_000_000,
        )

    def kind(self) -> str:
        """How `into_dict` names this field's type."""
        return str(self.dtype)

    def nested(self) -> dict[str, Any]:
        """What `into_dict` says about what is inside this field; nothing here."""
        return {}

    def leaf_names(self) -> list[str]:
        """Every leaf reachable from this field, as a dotted path.

        A scalar is its own leaf, so this is `[""]` -- the paths are relative
        to the field, and the field itself is the whole of it. `StructField`
        and the containers extend them by what they hold, which is what lets a
        column added *inside* a struct be compared like a top-level one.
        """
        return [""]

    def _extend(self, inside: dict[str, Field]) -> list[str]:
        """`leaf_names` for a container: each member's paths under its own name."""
        return [
            f"{name}.{leaf}" if leaf else name
            for name, member in inside.items()
            for leaf in member.leaf_names()
        ]

    # -- casting ------------------------------------------------------------

    def cast_arrow(self, source: Any, **kwargs: Any) -> Any:
        """Cast whatever is handed over, picking the method by what it is.

        An array, a chunked array, a record batch, a table, a reader or a plain
        iterator of batches each have their own `cast_arrow_*`; this redirects
        to the one that fits rather than making every call site branch.
        """
        return getattr(self, f"cast_{self.redirect_of(source, self.into_casts())}")(
            source, **kwargs
        )

    def cast_arrow_array(self, array: Any, *, safe: bool = False) -> Any:
        """`array` cast to this field's type, or handed back when it already is.

        Unsafe by default, deliberately: a cast to a *declared* type is a
        statement that the declaration is the authority, so a narrowing is the
        intent rather than an accident. Pass `safe=True` for Arrow's checking.

        A `ChunkedArray` is cast chunk by chunk, so a column of a table costs
        no more memory than a column of a batch.
        """
        if isinstance(array, pyarrow.ChunkedArray):
            return pyarrow.chunked_array(
                [self.cast_arrow_array(chunk, safe=safe) for chunk in array.chunks],
                type=self.dtype,
            )
        if array.type == self.dtype:
            return array
        return array.cast(self.dtype, safe=safe)

    def cast_arrow_scalar(self, value: Any, *, safe: bool = False) -> pyarrow.Scalar:
        """One value as a `pyarrow.Scalar` of this field's type."""
        if isinstance(value, pyarrow.Scalar):
            return value if value.type == self.dtype else value.cast(self.dtype)
        return pyarrow.scalar(self.cast_py(value), type=self.dtype, from_pandas=not safe)

    def cast_py(self, value: Any) -> Any:
        """One value as the Python type this field's Arrow type stands for.

        Integers are `int`, floats `float`, instants `datetime`, lists
        `list`, structs the dataclass the declaration spells. `None` stays
        `None`, and a value the type cannot hold raises rather than being
        silently rounded into it.
        """
        if value is None:
            return None
        if isinstance(value, pyarrow.Scalar):
            value = value.as_py()
            if value is None:
                return None
        return _py_of(self.dtype, value)

    def into_bytes(self, value: Any) -> bytes:
        """One value as the bytes this field stores it in.

        Fixed-width numbers are exactly their width, big-endian; an instant
        is its epoch integer in the declared unit, UTC; text is UTF-8. A
        nested value is its members' bytes concatenated, so one field's
        rendering is one blob whatever its shape.
        """
        return BinaryField.encode(self, value)


# -- clocks -----------------------------------------------------------------


class TimestampField(Field):
    """A timestamp column, and the clock castings every site shares.

    The pipeline's clocks are epoch integers (`unix`, nanoseconds) as often
    as Arrow timestamps, so the conversions between the two spellings live
    here, parametrized by unit -- one factor table, one widening rule --
    instead of a divisor literal per call site.
    """

    #: Nanoseconds in one tick of each Arrow timestamp unit.
    FACTORS: Mapping[str, int] = MappingProxyType(
        {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}
    )

    @property
    def unit(self) -> str:
        """The Arrow unit this field stores: `s`, `ms`, `us` or `ns`."""
        return self.dtype.unit

    @property
    def timezone(self) -> str | None:
        """The zone the stored instants are read in, or None when naive."""
        return self.dtype.tz

    @classmethod
    def of(
        cls,
        unit: str = "us",
        timezone: str | None = None,
        *,
        name: str = "",
        nullable: bool | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> TimestampField:
        """One parametrized declaration: `TimestampField.of("us", "UTC")`."""
        return cls(
            name=name,
            dtype=pyarrow.timestamp(unit, tz=timezone),
            nullable=nullable,
            metadata=metadata,
        )

    @classmethod
    def factor_of(cls, unit: Any) -> int:
        """Nanoseconds in one tick of `unit` -- the one table every cast shares."""
        try:
            return cls.FACTORS[str(unit)]
        except KeyError:
            raise ValueError(f"unknown timestamp unit {unit!r}") from None

    @classmethod
    def into_unix_arrow(cls, column: Any, unit: str = "ns") -> Any:
        """A timestamp column as epoch integers of `unit`, `int64`.

        The stored ticks are reinterpreted, then rescaled by the two units'
        factors; a zoned column's ticks are already epoch-anchored, so the
        zone drops without a shift.
        """
        compute = pyarrow.compute
        source = cls.factor_of(column.type.unit)
        target = cls.factor_of(unit)
        ticks = column.cast(pyarrow.int64(), safe=False)
        if source == target:
            return ticks
        if source > target:
            return compute.multiply(ticks, pyarrow.scalar(source // target, pyarrow.int64()))
        return compute.divide(ticks, pyarrow.scalar(target // source, pyarrow.int64()))

    def from_unix_arrow(self, column: Any, unit: str = "ns") -> Any:
        """Epoch integers of `unit` as this field's own timestamp type.

        A zoned declaration reads them as UTC epoch ticks, which is what an
        epoch integer is; rendering in another zone is a plain cast after.
        """
        compute = pyarrow.compute
        source = self.factor_of(unit)
        target = self.factor_of(self.unit)
        ticks = column.cast(pyarrow.int64(), safe=False)
        if source > target:
            ticks = compute.multiply(ticks, pyarrow.scalar(source // target, pyarrow.int64()))
        elif target > source:
            ticks = compute.divide(ticks, pyarrow.scalar(target // source, pyarrow.int64()))
        return ticks.cast(self.dtype, safe=False)


# -- bytes and dictionaries -------------------------------------------------


class BinaryField(Field):
    """A binary column, and the value-to-bytes rendering every field shares.

    One rule per Arrow type, so a value has one blob wherever it is rendered:
    a fixed-width number is exactly its width big-endian, an instant is its
    epoch integer in the declared unit, text is UTF-8, and a nested value is
    its members' bytes concatenated.
    """

    #: How many bytes each fixed-width type occupies, and how it is read.
    WIDTHS: Mapping[str, int] = MappingProxyType(
        {"int8": 1, "int16": 2, "int32": 4, "int64": 8, "float": 4, "double": 8}
    )

    @classmethod
    def encode(cls, field: Field, value: Any) -> bytes:
        """`value` as the bytes `field` stores it in."""
        if value is None:
            return b""
        return _bytes_of(field, value)

    def cast_py(self, value: Any) -> Any:
        """Bytes, as the column holds them."""
        if value is None:
            return None
        if isinstance(value, pyarrow.Scalar):
            value = value.as_py()
        return value if isinstance(value, bytes) else _bytes_of(self, value)

    def cast_arrow_array(self, array: Any, *, safe: bool = False) -> Any:
        """A column of anything as a column of the bytes this field stores.

        Arrow has no cast from a number to binary -- a width is a decision,
        not a conversion -- so where it refuses, the rendering `cast_py`
        already states is applied value by value. A column Arrow *can* cast
        (text, other binary) still goes through the kernel.
        """
        if isinstance(array, pyarrow.ChunkedArray) or array.type == self.dtype:
            return super().cast_arrow_array(array, safe=safe)
        if not _needs_rendering(array.type, self.dtype):
            return super().cast_arrow_array(array, safe=safe)
        return pyarrow.array([self.cast_py(one) for one in array.to_pylist()], type=self.dtype)


class DictionaryField(Field):
    """A dictionary column: positions into a values array, and the values.

    The casts a dictionary needs sit here rather than being branched for at
    every call site -- what the indices are, what the values are, and how a
    plain column of values becomes one.
    """

    @property
    def index_type(self) -> pyarrow.DataType:
        """The Arrow type the positions are stored in."""
        return self.dtype.index_type

    @property
    def value_type(self) -> pyarrow.DataType:
        """The Arrow type the dictionary's values are."""
        return self.dtype.value_type

    @property
    def values(self) -> Field:
        """The values as a field of their own, for casting through."""
        return Field(name=self.name, dtype=self.value_type, nullable=self.nullable)

    def cast_py(self, value: Any) -> Any:
        """One value as the Python type the *values* stand for."""
        return self.values.cast_py(value)

    def into_bytes(self, value: Any) -> bytes:
        """A dictionary value is its value's bytes: the position is storage."""
        return self.values.into_bytes(value)

    def cast_arrow_array(self, array: Any, *, safe: bool = False) -> Any:
        """`array` as this dictionary, encoding a plain column of values."""
        if isinstance(array, pyarrow.ChunkedArray):
            return pyarrow.chunked_array(
                [self.cast_arrow_array(chunk, safe=safe) for chunk in array.chunks],
                type=self.dtype,
            )
        if array.type == self.dtype:
            return array
        if pyarrow.types.is_dictionary(array.type):
            return array.cast(self.dtype, safe=safe)
        return self.values.cast_arrow_array(array, safe=safe).dictionary_encode().cast(self.dtype)


# -- containers -------------------------------------------------------------


class ListField(Field):
    """A field whose values are lists, with one `item` field inside it.

    The base of every list flavour Arrow has -- `large_list`, the two list
    views and `fixed_size_list` are subclasses that differ only in how they are
    built, so a cast into any of them is the same walk.
    """

    @functools.cached_property
    def item(self) -> Field:
        """What one element of the list is, as a field of its own."""
        return self._member_of(self.dtype.field(0))

    @property
    def fields(self) -> tuple[Field, ...]:
        return (self.item,)

    def with_item(self, item: pyarrow.Field) -> pyarrow.DataType:
        """This flavour of list, around another item."""
        return pyarrow.list_(item)

    def _member_changed(self, member: Field) -> None:
        self.dtype = self.with_item(member.into_arrow_field())

    def kind(self) -> str:
        return "list"

    def nested(self) -> dict[str, Any]:
        return {"fields": [_anonymous(self.item, ITEM)]}

    def leaf_names(self) -> list[str]:
        return self._extend({ITEM: self.item})

    def cast_arrow_array(self, array: Any, *, safe: bool = False) -> Any:
        """Cast the values, then cut them back into rows of this flavour."""
        if isinstance(array, pyarrow.ChunkedArray) or array.type == self.dtype:
            return super().cast_arrow_array(array, safe=safe)
        if pyarrow.types.is_struct(array.type):
            columns = [
                self.item.cast_arrow_array(column, safe=safe)
                for column in arrays.struct_columns(array).values()
            ]
            values, _ = arrays.interleave(columns, len(array))
            sizes = arrays.repeat_sizes(len(columns), len(array))
            return arrays.build_list(self.dtype, sizes, values, arrays.null_mask(array))
        source = array.type
        if not _is_list_like(source):
            return super().cast_arrow_array(array, safe=safe)
        if self._reusable(array):
            return self._rewrapped(array, safe=safe)
        return self._rebuilt(array, safe=safe)

    def _reusable(self, array: Any) -> bool:
        """Whether the source's own row layout can be handed to a builder as is."""
        kinds = pyarrow.types
        if kinds.is_map(array.type) or kinds.is_fixed_size_list(array.type):
            return False
        if array.offset:
            return False
        views = kinds.is_list_view(array.type) or kinds.is_large_list_view(array.type)
        return not views or arrays.list_type_like(array.type, self.item.into_arrow_field()) == (
            self.dtype
        )

    def _rebuilt(self, array: Any, *, safe: bool) -> Any:
        """Cut the rows again, from the sizes: right for any layout at all."""
        sizes, values = arrays.list_parts(array)
        return arrays.build_list(
            self.dtype,
            sizes,
            self.item.cast_arrow_array(values, safe=safe),
            arrays.null_mask(array),
        )

    def _rewrapped(self, array: Any, *, safe: bool) -> Any:
        """Cast the item in the source's own layout, then change the flavour.

        The two halves of a list cast are independent: what is inside a row is
        this field's business, and where the rows are is Arrow's. Casting the
        item and re-wrapping keeps the offsets untouched, and the flavour
        change that may follow is one Arrow call over the layout alone. Only
        the list views, which Arrow does not cast to or from, fall back to
        cutting the rows again.
        """
        item = self.item.into_arrow_field()
        middle = arrays.list_type_like(array.type, item)
        wrapped = arrays.rewrap_list(
            middle,
            array,
            self.item.cast_arrow_array(array.values, safe=safe),
            arrays.null_mask(array),
        )
        if middle == self.dtype:
            return wrapped
        try:
            return wrapped.cast(self.dtype, safe)
        except pyarrow.ArrowNotImplementedError:
            # A flavour Arrow will not cast between at all: cut the rows again.
            return type(self)(name=self.name, dtype=self.dtype)._rebuilt(wrapped, safe=True)


class LargeListField(ListField):
    """A list whose offsets are 64 bit."""

    def with_item(self, item: pyarrow.Field) -> pyarrow.DataType:
        return pyarrow.large_list(item)

    def kind(self) -> str:
        return "large_list"


class ListViewField(ListField):
    """A list whose rows carry an offset and a size, so they may be out of order."""

    def with_item(self, item: pyarrow.Field) -> pyarrow.DataType:
        return pyarrow.list_view(item)

    def kind(self) -> str:
        return "list_view"


class LargeListViewField(ListViewField):
    """A list view whose offsets and sizes are 64 bit."""

    def with_item(self, item: pyarrow.Field) -> pyarrow.DataType:
        return pyarrow.large_list_view(item)

    def kind(self) -> str:
        return "large_list_view"


class FixedSizeListField(ListField):
    """A list with the same number of elements in every row."""

    @property
    def list_size(self) -> int:
        """How many elements every row holds."""
        return self.dtype.list_size

    def with_item(self, item: pyarrow.Field) -> pyarrow.DataType:
        return pyarrow.list_(item, self.list_size)

    def kind(self) -> str:
        return "fixed_size_list"

    def nested(self) -> dict[str, Any]:
        """The width first, then the item: it is part of the type, not of it."""
        return {"list_size": self.list_size, **super().nested()}


class MapField(Field):
    """A field whose values are maps, with a `key` and a `value` field."""

    @functools.cached_property
    def key(self) -> Field:
        """The key half of one entry."""
        return self._member_of(self.dtype.key_field)

    @functools.cached_property
    def value(self) -> Field:
        """The value half of one entry."""
        return self._member_of(self.dtype.item_field)

    @property
    def fields(self) -> tuple[Field, ...]:
        return (self.key, self.value)

    def _member_changed(self, member: Field) -> None:
        halves = {"key": self.key, "value": self.value}
        halves[member.name if member.name in halves else "value"] = member
        self.dtype = pyarrow.map_(
            halves["key"].into_arrow_field(), halves["value"].into_arrow_field()
        )

    def kind(self) -> str:
        return "map"

    def nested(self) -> dict[str, Any]:
        """One entry -- and whether the keys are sorted, which is in the type.

        A map is a list of entries and an entry is a struct of a key and a
        value, so it writes the one `fields` block every container writes.
        Arrow compares two maps that disagree on `keys_sorted` as different
        types, so a dump that left it out read back as a map a cast would
        refuse.
        """
        described: dict[str, Any] = {}
        if self.dtype.keys_sorted:
            described["keys_sorted"] = True
        described["fields"] = [
            {
                "type": "struct",
                "fields": [_anonymous(self.key, "key"), _anonymous(self.value, "value")],
            }
        ]
        return described

    def leaf_names(self) -> list[str]:
        return self._extend({"key": self.key, "value": self.value})

    def cast_arrow_array(self, array: Any, *, safe: bool = False) -> Any:
        """Cast both halves, then cut the entries back into rows.

        A **struct** becomes a map of its members: the names are the keys, the
        values are the members, and the transpose that interleaves them is a
        `take` with computed indices (`fields.arrays.interleave`). A **list of
        two-member structs** is already a map physically, so its halves are
        cast and rebuilt.
        """
        if isinstance(array, pyarrow.ChunkedArray) or array.type == self.dtype:
            return super().cast_arrow_array(array, safe=safe)
        if pyarrow.types.is_struct(array.type):
            return self._from_struct(array, safe=safe)
        if not _is_list_like(array.type):
            return super().cast_arrow_array(array, safe=safe)
        if pyarrow.types.is_map(array.type) and not array.offset:
            # Already entries in rows: cast the halves and re-wrap them. Only
            # when the array owns its offsets, though -- a *sliced* map hands
            # the builder a slice of the offsets buffer, which Arrow refuses
            # beside a validity mask. Cutting the entries again works for both.
            return arrays.rewrap_map(
                self.dtype,
                array,
                self.key.cast_arrow_array(array.keys, safe=safe),
                self.value.cast_arrow_array(array.items, safe=safe),
                arrays.null_mask(array),
            )
        sizes, entries = arrays.list_parts(array)
        if not pyarrow.types.is_struct(entries.type) or entries.type.num_fields != 2:
            raise TypeError(
                f"field {self.name!r} is a map, so a list becomes one only when its item is a "
                f"key/value struct; this one is {entries.type}"
            )
        halves = list(arrays.struct_columns(entries).values())
        return arrays.build_map(
            self.dtype,
            sizes,
            self.key.cast_arrow_array(halves[0], safe=safe),
            self.value.cast_arrow_array(halves[1], safe=safe),
            arrays.null_mask(array),
        )

    def _from_struct(self, array: Any, *, safe: bool) -> Any:
        """A struct as a map: its member names are the keys."""
        columns = arrays.struct_columns(array)
        if not columns:
            # A struct with no members is a map with no entries -- which is
            # what Arrow infers from a column of empty dictionaries, and what
            # the general path below cannot build, having nothing to lay out.
            return arrays.build_map(
                self.dtype,
                arrays.repeat_sizes(0, len(array)),
                pyarrow.array([], self.dtype.key_type),
                pyarrow.array([], self.dtype.item_type),
                arrays.null_mask(array),
            )
        values, member = arrays.interleave(
            [self.value.cast_arrow_array(column, safe=safe) for column in columns.values()],
            len(array),
        )
        keys = arrays.names_array(list(columns), member, self.key.dtype)
        sizes = arrays.repeat_sizes(len(columns), len(array))
        return arrays.build_map(self.dtype, sizes, keys, values, arrays.null_mask(array))


class StructField(Field):
    """A field whose members are fields: what a `@scalar` class projects to.

    A struct is also a *schema*: `into_arrow_schema` lays its members out flat
    with the field's own name and metadata as schema metadata, and the casts
    take a batch, a table or a whole stream onto that shape.
    """

    @classmethod
    @functools.cache
    def into_casts(cls) -> Mapping[Any, str]:
        """Array and schema-shaped Arrow values this struct can cast."""
        return MappingProxyType(
            {
                **super().into_casts(),
                pyarrow.RecordBatch: "arrow_batch",
                pyarrow.Table: "arrow_table",
                pyarrow.RecordBatchReader: "arrow_reader",
                Iterator: "arrow_reader",
                list: "arrow_reader",
                tuple: "arrow_reader",
            }
        )

    # -- members ------------------------------------------------------------

    @functools.cached_property
    def fields(self) -> tuple[Field, ...]:
        """Members of this struct, in declaration order.

        Cached: the walk cannot come out differently until the declaration
        changes, and each member is a live view of this struct, so setting
        something on one rebuilds this field around it.
        """
        dtype = self.dtype
        return tuple(self._member_of(dtype.field(index)) for index in range(dtype.num_fields))

    @functools.cached_property
    def _by_name(self) -> dict[str, Field]:
        """Members by their own name, and by the fold every name matches on.

        A column's name is already folded, so the two agree for anything this
        package declares; the second key is what lets a caller ask for the
        spelling it has -- `SecurityID` from the dictionary, `security_id`
        from a bridge -- and reach the one column that is.
        """
        found = {member.name: member for member in self.fields}
        for member in self.fields:
            found.setdefault(column_name(member.name), member)
        return found

    def field(self, name: str) -> Field:
        """One member, by its name or by any spelling that folds onto it."""
        found = self._by_name.get(name)
        if found is None:
            found = self._by_name.get(column_name(name))
        if found is None:
            raise KeyError(f"{self.name or 'struct'} has no member {name!r}")
        return found

    @property
    def names(self) -> list[str]:
        """Member names, in declaration order."""
        return [member.name for member in self.fields]

    def primary_keys(self) -> list[str]:
        """Members declared part of the primary key, in declaration order."""
        return [member.name for member in self.fields if member.is_primary_key]

    def partition_keys(self) -> dict[str, str]:
        """Members the data is partitioned on, mapped to their transform."""
        return {
            member.name: member.partition_transform
            for member in self.fields
            if member.is_partition_key
        }

    def sort_keys(self) -> dict[str, str]:
        """Members the data is sorted on, mapped to their direction, in order."""
        encoded = self.metadata.get(SORT_ORDER)
        if encoded:
            try:
                declared = json.loads(encoded)
                ordered = [(str(name), str(direction)) for name, direction in declared]
            except (TypeError, ValueError):
                raise ValueError(f"field {self.name!r} has an invalid {SORT_ORDER!r}") from None
            if len({name for name, _ in ordered}) != len(ordered):
                raise ValueError(f"field {self.name!r} repeats a column in {SORT_ORDER!r}")
            for name, direction in ordered:
                member = self.field(name)
                if not member.is_sort_key or member.sort_direction != direction:
                    raise ValueError(
                        f"field {self.name!r} has inconsistent {SORT_ORDER!r} metadata"
                    )
            flagged = {member.name for member in self.fields if member.is_sort_key}
            if flagged != {name for name, _ in ordered}:
                raise ValueError(f"field {self.name!r} has incomplete {SORT_ORDER!r} metadata")
            return dict(ordered)
        return {member.name: member.sort_direction for member in self.fields if member.is_sort_key}

    def derived_keys(self) -> dict[str, tuple[str, ...]]:
        """Members that are a function of other members, mapped to those."""
        return {member.name: member.derived_from for member in self.fields if member.derived_from}

    def _member_changed(self, member: Field) -> None:
        encoded = self.metadata.get(SORT_ORDER)
        if encoded:
            try:
                ordered = [(str(name), str(direction)) for name, direction in json.loads(encoded)]
            except (TypeError, ValueError):
                ordered = []
            members = {
                other.name: member if other.name == member.name else other for other in self.fields
            }
            current = {
                name: candidate.sort_direction
                for name, candidate in members.items()
                if candidate.is_sort_key
            }
            if current != dict(ordered):
                self.metadata = _without(self.metadata, SORT_ORDER)
        self.dtype = pyarrow.struct(
            [
                (member if other.name == member.name else other).into_arrow_field()
                for other in self.fields
            ]
        )

    # -- converting ---------------------------------------------------------

    @functools.cached_property
    def arrow_fields(self) -> list[pyarrow.Field]:
        """Members as Arrow fields, built once: every cast reads them."""
        return [member.into_arrow_field() for member in self.fields]

    @functools.cached_property
    def arrow_schema(self) -> pyarrow.Schema:
        return pyarrow.schema(self.arrow_fields, metadata={NAME: self.name, **self.metadata})

    def into_arrow_schema(self) -> pyarrow.Schema:
        """This struct's members, flat, with its identity as schema metadata.

        The name and metadata travel with the schema, so wherever it lands it
        still says which field it came from -- that is what `from_arrow_schema`
        reads back.
        """
        return self.arrow_schema

    def into_arrow_array(
        self,
        rows: Iterable[Any],
        spell: Any = None,
        owner: type | None = None,
    ) -> pyarrow.StructArray:
        """Instances of the class this struct declares, as one struct column.

        Member by member off the objects, never through a dictionary: the
        declaration already says every member's Arrow type, so nothing is
        inferred per row. `spell` is how a class writes a member its column
        holds differently from the attribute.
        """
        from rekep.fields.rows import struct_array

        return struct_array(self, list(rows), spell, owner)

    def into_arrow_batch(
        self,
        rows: Iterable[Any],
        spell: Any = None,
        owner: type | None = None,
    ) -> pyarrow.RecordBatch:
        """The same instances as a batch of this struct's own schema.

        A struct array and a record batch are the same buffers with a
        different name on them, so Arrow's own conversion does it and the
        schema this field declares is put back on the result -- a batch that
        lost its metadata would no longer say which class it came from.
        """
        built = pyarrow.RecordBatch.from_struct_array(self.into_arrow_array(rows, spell, owner))
        return pyarrow.RecordBatch.from_arrays(built.columns, schema=self.into_arrow_schema())

    def into_iceberg_schema(self) -> Any:
        """This struct as a `pyiceberg.schema.Schema`, ids numbered from one."""
        from rekep.iceberg.fields import iceberg_schema

        return iceberg_schema(self)

    def into_iceberg_partition_spec(self, schema: Any = None) -> Any:
        """The `pyiceberg` partition spec this struct's members declare."""
        from rekep.iceberg.fields import iceberg_partition_spec

        return iceberg_partition_spec(self, schema)

    def into_iceberg_sort_order(
        self, schema: Any = None, sort_by: Sequence[str] | None = None
    ) -> Any:
        """The declared sort order, or an explicit ascending column order."""
        from rekep.iceberg.fields import iceberg_sort_order

        return iceberg_sort_order(self, schema, sort_by)

    @classmethod
    def from_iceberg_schema(
        cls,
        source: Any,
        name: str = "",
        spec: Any = None,
        sort_order: Any = None,
    ) -> StructField:
        """A `pyiceberg` schema as a struct field, including its table layout."""
        from rekep.iceberg.fields import iceberg_struct_field

        return iceberg_struct_field(source, name, spec, sort_order)

    def into_dataclass(self, name: str | None = None) -> type:
        """Rebuild a `@scalar` class whose projection is exactly this field.

        Imported at the point of use: the class builder decorates what it
        builds with `scalar`, which lives here.
        """
        from rekep.fields.classes import ClassBuilder

        return ClassBuilder().dataclass(self, name)

    def kind(self) -> str:
        return "struct"

    def nested(self) -> dict[str, Any]:
        return {"fields": [member.into_dict() for member in self.fields]}

    def leaf_names(self) -> list[str]:
        """Every leaf below this struct, dotted: `["symbol", "venue.mic", ...]`.

        What schema evolution has to compare. Top-level names alone miss a
        member added *inside* a struct, a list or a map -- and `union_by_name`
        adds those perfectly well, so missing them meant the next write dropped
        the value with nothing raised.
        """
        return self._extend({member.name: member for member in self.fields})

    # -- casting ------------------------------------------------------------

    def cast_arrow_array(self, array: Any, *, safe: bool = False) -> Any:
        """Cast every member, then rebuild the struct around them.

        Member by member rather than in one Arrow call, because only this
        field knows what a member the data does not have means: null when it
        is nullable, an error naming the path when it is not.

        A **map** becomes a struct by looking each member up as a key
        (Arrow's `map_lookup`, one pass per member), and a **list** by
        position, so `list[a, b]` fills the first two members.
        """
        if isinstance(array, pyarrow.ChunkedArray) or array.type == self.dtype:
            return super().cast_arrow_array(array, safe=safe)
        column_of = self._column_of(array)
        if column_of is None:
            return super().cast_arrow_array(array, safe=safe)
        return pyarrow.StructArray.from_arrays(
            self.cast_arrow_columns(column_of, len(array), safe=safe),
            fields=self.arrow_fields,
            mask=arrays.null_mask(array),
        )

    def _column_of(self, array: Any) -> Callable[[str], Any] | None:
        """How to find one member in `array`, or None when it is not a container."""
        kinds = pyarrow.types
        if kinds.is_struct(array.type):
            return arrays.struct_columns(array).get
        if kinds.is_map(array.type):

            def from_map(name: str) -> Any:
                column = arrays.map_column(array, name)
                # No entry anywhere is the same as no column at all: filled when
                # the member may be null, refused by name when it may not. A
                # map with no rows at all says nothing either way, and reading
                # it as "no column" would make an empty batch -- routine in any
                # stream -- refuse a member every other batch produces.
                if len(column) and column.null_count == len(column):
                    return None
                return column

            return from_map
        if _is_list_like(array.type):
            shortest = pyarrow.compute.min(pyarrow.compute.list_value_length(array)).as_py()
            if shortest is not None and shortest < len(self.fields):
                raise ValueError(
                    f"{self.name or 'struct'} takes {len(self.fields)} members from a list, but "
                    f"one row has only {shortest}; pad the rows or declare fewer members"
                )
            positions = {member.name: index for index, member in enumerate(self.fields)}
            return lambda name: pyarrow.compute.list_element(array, positions[name])
        return None

    def cast_arrow_columns(
        self, column_of: Callable[[str], Any], length: int, *, safe: bool = False
    ) -> list[Any]:
        """One array per member: cast what `column_of` finds, null when it may be."""
        columns = []
        for member in self.fields:
            column = column_of(member.name)
            if column is not None:
                cast = member.cast_arrow_array(column, safe=safe)
                if not member.nullable and cast.null_count:
                    # The same reason the missing case is refused, one step
                    # later: a schema that says NOT NULL over a column holding
                    # nulls is a lie every other reader has to discover for
                    # itself, and the write that fails on it is far from here.
                    # `pyarrow.Table.cast` refuses this too.
                    raise ValueError(
                        f"column {self._path(member.name)!r} is not nullable and "
                        f"{cast.null_count} of {len(cast)} values are null; fill them upstream "
                        "or make the field optional"
                    )
                columns.append(cast)
            elif member.nullable:
                columns.append(pyarrow.nulls(length, member.dtype))
            else:
                raise ValueError(
                    f"column {self._path(member.name)!r} is missing and not nullable, so it "
                    "cannot be filled with nulls; produce it upstream or make the field optional"
                )
        return columns

    def cast_arrow_batch(
        self, batch: pyarrow.RecordBatch, *, safe: bool = False, merge_schema: bool = False
    ) -> pyarrow.RecordBatch:
        """`batch` reshaped onto this field: cast, filled, reordered."""
        target = self.merged(batch.schema) if merge_schema else self
        schema = target.arrow_schema
        if batch.schema.equals(schema, check_metadata=True):
            return batch
        if batch.schema.equals(schema):
            # The types already line up and only the metadata does not: the
            # column comments and the identity this field declares are part of
            # the shape (AGENTS.md, "Arrow is the hub"), so a cast has to
            # attach them -- but no value moves, only the schema is swapped.
            return pyarrow.RecordBatch.from_arrays(batch.columns, schema=schema)
        columns = target.cast_arrow_columns(_column_of(batch), batch.num_rows, safe=safe)
        return pyarrow.RecordBatch.from_arrays(columns, schema=schema)

    def cast_arrow_table(
        self, table: pyarrow.Table, *, safe: bool = False, merge_schema: bool = False
    ) -> pyarrow.Table:
        """`cast_arrow_batch` over a whole table, one batch at a time.

        Batch by batch rather than column by column: a table's columns are
        chunked, and casting a chunk at a time is what keeps the peak at one
        batch instead of a second copy of the whole column.
        """
        target = self.merged(table.schema) if merge_schema else self
        if table.schema.equals(target.arrow_schema, check_metadata=True):
            return table
        batches = (target.cast_arrow_batch(batch, safe=safe) for batch in table.to_batches())
        return pyarrow.Table.from_batches(batches, target.arrow_schema)

    def cast_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        *,
        safe: bool = False,
        merge_schema: bool = False,
    ) -> pyarrow.RecordBatchReader:
        """`cast_arrow_batch` over a whole stream, still one batch at a time."""
        target = self
        if merge_schema:
            source, incoming = _peek_schema(source)
            if incoming is not None:
                target = self.merged(incoming)
        # Per stream, not per batch: `cast_arrow_batch` runs once per
        # `batch_row_size` rows, which is thousands of records over one file.
        LOGGER.debug(
            "casting a stream onto %s: %d columns%s",
            target.name or "an unnamed shape",
            len(target.names),
            " (widened by merge_schema)" if merge_schema and target is not self else "",
        )

        def generate() -> Iterator[pyarrow.RecordBatch]:
            for batch in source:
                yield target.cast_arrow_batch(batch, safe=safe)

        batches = generate()
        close = getattr(source, "close", None)
        return OwnedRecordBatchReader(
            target.arrow_schema,
            batches,
            close if close is not None else lambda: None,
        )

    # -- merging ------------------------------------------------------------

    def merged(self, incoming: Any) -> StructField:
        """This field widened with whatever `incoming` has and it does not.

        Shared members stay this field's (so data is cast onto them), new ones
        are added nullable -- at every level, so a struct column that grew a
        member grows here too (`fields.arrow.merge_fields`).
        """
        return self.merge_with(incoming)

    def merge_arrow_schema(self, incoming: pyarrow.Schema) -> pyarrow.Schema:
        """`merged`, as the Arrow schema it produces."""
        return self.merged(incoming).arrow_schema

    def narrowed(self, incoming: Any) -> StructField:
        """This field's reading of the columns `incoming` actually has.

        `merged` widens, which is what a *write* wants: every member this
        field declares survives, and one the batch is missing is filled with
        nulls. A projected read is the other direction -- a column absent
        from the batch is one the reader chose not to select, and filling it
        invents data -- so this keeps the incoming columns in their own
        order and gives each the type, the nullability and the comment this
        field declares for it. Anything this field does not declare stays as
        it arrived.

        Together with `cast_arrow_batch` it is how a batch read back from
        storage is brought onto the declaration without inventing a column:
        a `large_string` a scan hands back becomes the `string` the schema
        says, and a projection stays a projection.
        """
        declared = self._by_name
        members = [declared.get(member.name, member) for member in Field.from_(incoming).fields]
        return Field(
            name=self.name,
            dtype=pyarrow.struct([member.into_arrow_field() for member in members]),
            nullable=self.nullable,
            metadata=dict(self.metadata),
        )

    def _path(self, name: str) -> str:
        """A member's name, prefixed by this field's when it has one."""
        return f"{self.name}.{name}" if self.name else name


# -- the decorator ----------------------------------------------------------


def scalar(cls: type | None = None, /, **kwargs: Any) -> Any:
    """Turn a class into a field: a dataclass whose members are its Arrow struct."""

    def wrap(target: type) -> type:
        private = hide_private(target)
        if kwargs.get("slots"):
            restore_private_slots(target, private)
        built = dataclasses.dataclass(**kwargs)(target)
        if private:
            hide_private(built)
            for name in private:
                built.__dataclass_fields__.pop(name, None)
        built.into_field = classmethod(_into_class_field)
        built.into_arrow_array = classmethod(_into_arrow_array)
        built.into_arrow_batch = classmethod(_into_arrow_batch)
        if not callable(getattr(built, "into_field_builder", None)):
            built.into_field_builder = classmethod(_into_field_builder)
        if not callable(getattr(built, "into_field_metadata", None)):
            built.into_field_metadata = classmethod(_into_field_metadata)
        return built

    return wrap if cls is None else wrap(cls)


@functools.cache
def _into_class_field(owner: type, name: str | None = None) -> StructField:
    """Build a class declaration once, optionally with another outer name."""
    if name is not None:
        return _into_class_field(owner).with_name(name)
    return Field.from_dataclass(owner)


def _into_arrow_array(owner: type, rows: Iterable[Any]) -> pyarrow.StructArray:
    """Instances of this class as one struct column of its own type."""
    return owner.into_field().into_arrow_array(rows, _spelling_of(owner), owner)


def _into_arrow_batch(owner: type, rows: Iterable[Any]) -> pyarrow.RecordBatch:
    """The same instances as a batch of this class's own schema."""
    return owner.into_field().into_arrow_batch(rows, _spelling_of(owner), owner)


@functools.cache
def _spelling_of(owner: type) -> Any:
    """How this class spells a member its column holds differently, or nothing.

    Read once per class: the check is a `getattr`, and paying it per member
    per row is the whole cost this builder exists to avoid.
    """
    spell = getattr(owner, "into_column_value", None)
    return spell if callable(spell) else None


@functools.cache
def _into_field_builder(_owner: type) -> type:
    """Default projection builder for a `@scalar` class."""
    from rekep.fields.builder import FieldBuilder

    return FieldBuilder


@functools.cache
def _into_field_metadata(_owner: type) -> Mapping[str, str]:
    """Default contract metadata for a `@scalar` class."""
    return MappingProxyType({})


# -- casting onto a plain schema --------------------------------------------


def cast_batch(
    batch: pyarrow.RecordBatch, schema: pyarrow.Schema, *, safe: bool = False
) -> pyarrow.RecordBatch:
    """`batch` reshaped onto `schema`, for a target nobody declared as a class.

    A parquet footer or another team's contract is a target shape just as well
    as a `@scalar` class, so the schema gets the same machinery.
    """
    return Field.from_arrow_schema(schema).cast_arrow_batch(batch, safe=safe)


def cast_table(
    table: pyarrow.Table, schema: pyarrow.Schema, *, safe: bool = False
) -> pyarrow.Table:
    """`cast_batch` over a whole table."""
    return Field.from_arrow_schema(schema).cast_arrow_table(table, safe=safe)


def cast_reader(
    source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
    schema: pyarrow.Schema,
    *,
    safe: bool = False,
    merge_schema: bool = False,
) -> pyarrow.RecordBatchReader:
    """`cast_batch` over a whole stream."""
    return Field.from_arrow_schema(schema).cast_arrow_reader(
        source, safe=safe, merge_schema=merge_schema
    )


# -- helpers ----------------------------------------------------------------


class _FlowMap(dict):
    """A mapping rendered between braces in a Field YAML document."""


def _yaml_document(value: Any, key: str | None = None) -> Any:
    """Mark only metadata and protocol maps for compact YAML rendering."""
    if isinstance(value, list):
        return [_yaml_document(item) for item in value]
    if not isinstance(value, Mapping):
        return value
    rendered = {name: _yaml_document(item, str(name)) for name, item in value.items()}
    if key == "metadata" or (key is not None and key not in _DOCUMENT_KEYS):
        return _FlowMap(rendered)
    return rendered


def _represent_flow_map(dumper: Any, value: _FlowMap) -> Any:
    """Represent one marked map without an indented YAML block."""
    return dumper.represent_mapping("tag:yaml.org,2002:map", value, flow_style=True)


def decoded(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    """Arrow metadata as text, which is how a `Field` holds it."""
    return {key.decode(): value.decode() for key, value in (metadata or {}).items()}


def _protocol_maps(
    metadata: Mapping[str, str],
) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    """Split Arrow's `protocol:key` metadata into readable document maps."""
    plain: dict[str, str] = {}
    protocols: dict[str, dict[str, str]] = {}
    for full, value in metadata.items():
        prefix, marker, key = full.partition(":")
        if marker and prefix not in _DOCUMENT_KEYS and key:
            protocols.setdefault(prefix, {})[key] = value
        else:
            plain[full] = value
    return plain, protocols


def _document_metadata(mapping: Mapping[str, Any]) -> dict[str, Any]:
    """Restore top-level protocol maps to Arrow's prefixed metadata keys."""
    metadata = dict(mapping.get("metadata") or {})
    prefixed = sorted(key for key in metadata if ":" in str(key))
    if prefixed:
        raise ValueError(
            f"field {mapping.get(NAME, '')!r} puts protocol metadata in `metadata`: "
            f"{prefixed}; declare each protocol as its own top-level map"
        )
    for prefix, values in mapping.items():
        if prefix in _DOCUMENT_KEYS or not isinstance(values, Mapping):
            continue
        for key, value in values.items():
            full = f"{prefix}:{key}"
            metadata[full] = value
    return metadata


def arrow_type_for(text: str) -> pyarrow.DataType:
    """The Arrow type `text` names, as `str(type)` wrote it.

    Arrow parses most of its own spellings; the two it has no alias for are
    rebuilt here rather than dumped in a shape it could not read back.
    """
    decimal_type = re.fullmatch(r"decimal(128|256)\((\d+),\s*(-?\d+)\)", text)
    if decimal_type:
        build = pyarrow.decimal128 if decimal_type[1] == "128" else pyarrow.decimal256
        return build(int(decimal_type[2]), int(decimal_type[3]))
    timestamp = re.fullmatch(r"timestamp\[(\w+),\s*tz=(.+)\]", text)
    if timestamp:
        return pyarrow.timestamp(timestamp[1], tz=timestamp[2])
    fixed_binary = re.fullmatch(r"fixed_size_binary(?:\[(\d+)\]|\((\d+)\))", text)
    if fixed_binary:
        return pyarrow.binary(int(fixed_binary[1] or fixed_binary[2]))
    return pyarrow.type_for_alias(text)


#: How each dumped list flavour keeps its offset width and view semantics.
#: `fixed_size_list` is not here: its width is a second argument, so it is
#: rebuilt where that argument is read.
_LIST_KINDS: dict[str, Callable[[pyarrow.Field], pyarrow.DataType]] = {
    "list": pyarrow.list_,
    "large_list": pyarrow.large_list,
    "list_view": pyarrow.list_view,
    "large_list_view": pyarrow.large_list_view,
}


#: Which subclass speaks for which Arrow kind. Every `is_*` here is a type-id
#: equality, so the seven are disjoint and the order is for reading only --
#: measured, not assumed: no kind satisfies another's test. A new kind is one
#: more row.
_KINDS: tuple[tuple[Callable[[pyarrow.DataType], bool], str], ...] = (
    (pyarrow.types.is_struct, "StructField"),
    (pyarrow.types.is_map, "MapField"),
    (pyarrow.types.is_large_list, "LargeListField"),
    (pyarrow.types.is_large_list_view, "LargeListViewField"),
    (pyarrow.types.is_list_view, "ListViewField"),
    (pyarrow.types.is_fixed_size_list, "FixedSizeListField"),
    (pyarrow.types.is_list, "ListField"),
    (pyarrow.types.is_timestamp, "TimestampField"),
    (pyarrow.types.is_dictionary, "DictionaryField"),
    (pyarrow.types.is_binary, "BinaryField"),
    (pyarrow.types.is_large_binary, "BinaryField"),
    (pyarrow.types.is_fixed_size_binary, "BinaryField"),
)


@functools.cache
def _kind_classes() -> frozenset[type]:
    """The classes the dispatch table owns -- the ones that follow the type."""
    return frozenset(globals()[name] for _, name in _KINDS)


def _class_for(dtype: pyarrow.DataType | None) -> type[Field]:
    """The `Field` subclass that speaks for `dtype`."""
    if dtype is None:
        return Field
    for matches, name in _KINDS:
        if matches(dtype):
            return globals()[name]
    return Field


def _is_list_like(dtype: pyarrow.DataType) -> bool:
    """Whether rows of `dtype` are runs of values: any list flavour, or a map."""
    kinds = pyarrow.types
    return bool(
        kinds.is_list(dtype)
        or kinds.is_large_list(dtype)
        or kinds.is_list_view(dtype)
        or kinds.is_large_list_view(dtype)
        or kinds.is_fixed_size_list(dtype)
        or kinds.is_map(dtype)
    )


def _column_of(batch: pyarrow.RecordBatch) -> Callable[[str], Any]:
    """Look a column up by name, without materialising the ones nobody asked for."""

    def column_of(name: str) -> Any:
        index = batch.schema.get_field_index(name)
        return batch.column(index) if index >= 0 else None

    return column_of


def _list_size(mapping: Mapping[str, Any]) -> int:
    """The width a `fixed_size_list` document states, checked rather than coerced.

    `int(size)` took a float, a bool and a string, and a negative width built a
    plain `list` -- a contract that says one thing and loads as another, which
    is the one failure a contract cannot have.
    """
    name = mapping.get(NAME, "")
    size = mapping.get("list_size")
    if size is None:
        raise ValueError(
            f"fixed_size_list {name!r} has no list_size, and the width is part of the type; "
            "dump it with into_dict() rather than writing it by hand"
        )
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise ValueError(
            f"fixed_size_list {name!r} has list_size {size!r}: it is the number of elements in "
            "every row, so it has to be a whole number and not negative"
        )
    return size


def _flag(mapping: Mapping[str, Any], key: str) -> bool:
    """A boolean a document states, read strictly.

    Python truthiness would read the text `"false"` as true. These flags are
    part of the Arrow type and therefore accept only boolean spellings.
    """
    value = mapping.get(key, False)
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    raise ValueError(
        f"field {mapping.get(NAME, '')!r} has {key}={value!r}: write true or false, since it is "
        "part of the type and not a value to be coerced"
    )


def _py_of(dtype: pyarrow.DataType, value: Any) -> Any:
    """One value as the Python type `dtype` stands for."""
    kinds = pyarrow.types
    if kinds.is_dictionary(dtype):
        return _py_of(dtype.value_type, value)
    if kinds.is_boolean(dtype):
        return bool(value)
    if kinds.is_integer(dtype):
        return int(value)
    if kinds.is_floating(dtype):
        return float(value)
    if kinds.is_decimal(dtype):
        return value if isinstance(value, decimal.Decimal) else decimal.Decimal(str(value))
    if kinds.is_string(dtype) or kinds.is_large_string(dtype):
        return value if isinstance(value, str) else str(value)
    if kinds.is_binary(dtype) or kinds.is_large_binary(dtype) or kinds.is_fixed_size_binary(dtype):
        return _binary_bytes(dtype, value)
    if kinds.is_timestamp(dtype):
        return _instant_of(dtype, value)
    if kinds.is_date(dtype):
        return value if isinstance(value, datetime.date) else _instant_of(dtype, value).date()
    if kinds.is_time(dtype):
        if isinstance(value, datetime.time):
            return value
        return datetime.time.fromisoformat(str(value))
    if kinds.is_duration(dtype):
        if isinstance(value, datetime.timedelta):
            return value
        return datetime.timedelta(**{_DURATIONS[dtype.unit]: int(value)})
    if kinds.is_struct(dtype):
        return _dataclass_of(dtype, value)
    if kinds.is_map(dtype):
        items = value.items() if isinstance(value, Mapping) else value
        return {_py_of(dtype.key_type, key): _py_of(dtype.item_type, item) for key, item in items}
    if _is_list_like(dtype):
        item = dtype.value_type
        return [_py_of(item, one) for one in value]
    return value


#: What a duration's unit is called where `timedelta` takes it.
_DURATIONS: Mapping[str, str] = MappingProxyType(
    {"s": "seconds", "ms": "milliseconds", "us": "microseconds", "ns": "microseconds"}
)


def _instant_of(dtype: pyarrow.DataType, value: Any) -> datetime.datetime:
    """One value as an aware `datetime`, read on the type's own clock."""
    if isinstance(value, datetime.datetime):
        found = value
    elif isinstance(value, datetime.date):
        found = datetime.datetime(value.year, value.month, value.day)
    elif isinstance(value, str):
        found = datetime.datetime.fromisoformat(value)
    else:
        unit = getattr(dtype, "unit", "us")
        seconds = int(value) / (1_000_000_000 / TimestampField.FACTORS[unit])
        found = datetime.datetime.fromtimestamp(seconds, datetime.UTC)
    zone = getattr(dtype, "tz", None)
    if zone is None:
        return found.replace(tzinfo=None) if found.tzinfo is not None else found
    return found.replace(tzinfo=datetime.UTC) if found.tzinfo is None else found


@functools.cache
def _declared_dataclass(dtype: pyarrow.DataType) -> type:
    """The one dataclass a struct type spells -- built once, so a value cast
    twice is the same class both times."""
    return Field(name="", dtype=dtype).into_dataclass()


def _dataclass_of(dtype: pyarrow.DataType, value: Any) -> Any:
    """One struct value as the dataclass its declaration spells."""
    declared = _declared_dataclass(dtype)
    if isinstance(value, declared):
        return value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        value = dataclasses.asdict(value)
    members = {member.name: member for member in Field(name="", dtype=dtype).fields}
    read = value if isinstance(value, Mapping) else dict(zip(members, value, strict=False))
    return declared(**{name: member.cast_py(read.get(name)) for name, member in members.items()})


def _bytes_of(field: Field, value: Any) -> bytes:
    """One value as the bytes its field stores it in."""
    dtype, kinds = field.dtype, pyarrow.types
    if kinds.is_dictionary(dtype):
        return field.values.into_bytes(value)
    if kinds.is_boolean(dtype):
        return b"\x01" if value else b"\x00"
    if kinds.is_string(dtype) or kinds.is_large_string(dtype):
        return str(value).encode("utf-8")
    if kinds.is_binary(dtype) or kinds.is_large_binary(dtype) or kinds.is_fixed_size_binary(dtype):
        return _binary_bytes(dtype, value)
    if kinds.is_timestamp(dtype) or kinds.is_date(dtype) or kinds.is_time(dtype):
        return _stamp_bytes(field, value)
    if kinds.is_duration(dtype):
        found = field.cast_py(value)
        scale = TimestampField.FACTORS["s"] / TimestampField.FACTORS[dtype.unit]
        ticks = int(found.total_seconds() * scale)
        return ticks.to_bytes(8, "big", signed=True)
    if kinds.is_decimal(dtype):
        found = field.cast_py(value)
        if dtype.scale:
            return str(found).encode("ascii")
        width = 16 if pyarrow.types.is_decimal128(dtype) else 32
        return int(found).to_bytes(width, "big", signed=True)
    if kinds.is_integer(dtype) or kinds.is_floating(dtype):
        return _number_bytes(dtype, field.cast_py(value))
    if kinds.is_struct(dtype):
        read = _member_values(field, value)
        return b"".join(member.into_bytes(read.get(member.name)) for member in field.fields)
    if kinds.is_map(dtype):
        items = value.items() if isinstance(value, Mapping) else (value or ())
        return b"".join(
            field.key.into_bytes(key) + field.value.into_bytes(item) for key, item in items
        )
    if _is_list_like(dtype):
        return b"".join(field.item.into_bytes(one) for one in (value or ()))
    raise TypeError(f"no byte rendering for {dtype}")


def _member_values(field: Field, value: Any) -> Mapping[str, Any]:
    """A struct value as `{member name: value}`, whatever shape it arrived in."""
    if isinstance(value, Mapping):
        return value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {member.name: getattr(value, member.name, None) for member in field.fields}
    return dict(zip((member.name for member in field.fields), value or (), strict=False))


def _binary_bytes(dtype: pyarrow.DataType, value: Any) -> bytes:
    """A binary column's own value, with a number written to its width.

    `bytes(7)` is seven zero bytes in Python and not the number seven, so an
    integer is written big-endian two's complement at the width the column
    declares -- the same bytes a wide identifier is already stored as, and the
    only rendering that makes the stored column sort as the values do.
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, int) and not isinstance(value, bool):
        width = dtype.byte_width if pyarrow.types.is_fixed_size_binary(dtype) else 8
        return (int(value) & ((1 << (width * 8)) - 1)).to_bytes(width, "big")
    if isinstance(value, str):
        return value.encode("utf-8")
    return bytes(value)


def _needs_rendering(source: pyarrow.DataType, target: pyarrow.DataType) -> bool:
    """Whether a column has to be rendered rather than cast into `target`.

    A number or an instant has no Arrow cast to binary: how wide it is and
    which end the bytes start at are the declaration's decisions, which is
    exactly what `BinaryField` states.
    """
    kinds = pyarrow.types
    if not (kinds.is_fixed_size_binary(target) or kinds.is_binary(target)):
        return False
    return (
        kinds.is_integer(source)
        or kinds.is_floating(source)
        or kinds.is_decimal(source)
        or kinds.is_temporal(source)
        or kinds.is_boolean(source)
    )


def _number_bytes(dtype: pyarrow.DataType, value: Any) -> bytes:
    """A fixed-width number as exactly its width, big-endian."""
    named = str(dtype)
    width = BinaryField.WIDTHS.get(named, 8)
    if pyarrow.types.is_floating(dtype):
        return struct.pack(">f" if width == 4 else ">d", float(value))
    return int(value).to_bytes(width, "big", signed=not named.startswith("u"))


def _stamp_bytes(field: Field, value: Any) -> bytes:
    """An instant as its epoch integer in the declared unit, UTC."""
    dtype = field.dtype
    found = field.cast_py(value)
    if isinstance(found, datetime.time):
        micros = (found.hour * 3600 + found.minute * 60 + found.second) * 1_000_000
        ticks = (micros + found.microsecond) * 1_000 // TimestampField.FACTORS[dtype.unit]
    elif isinstance(found, datetime.datetime):
        aware = found if found.tzinfo is not None else found.replace(tzinfo=datetime.UTC)
        nanos = int(aware.timestamp() * 1_000_000) * 1_000
        ticks = nanos // TimestampField.FACTORS[getattr(dtype, "unit", "us")]
    else:
        ticks = found.toordinal() - datetime.date(1970, 1, 1).toordinal()
        if str(dtype) == "date64[ms]":
            ticks *= 86_400_000
    return int(ticks).to_bytes(8, "big", signed=True)


def _anonymous(member: Field, owned: str) -> dict[str, Any]:
    """A list item or map half, keeping a name only when the author chose it.

    Arrow names a list's element `item` and a map's halves `key` and `value`;
    those are the container's spelling and the document says them as the block
    it writes. Anything else was named on purpose -- a FIX group repeats a
    `PartyID`, not an `item` -- and a document that dropped it would read back
    as a different type.
    """
    described = member.into_dict()
    if described.get(NAME) == owned:
        described.pop(NAME, None)
    return described


def _without(metadata: Mapping[str, str], key: str) -> dict[str, str]:
    return {name: value for name, value in metadata.items() if name != key}


def _peek_schema(
    source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
) -> tuple[Any, pyarrow.Schema | None]:
    """`(source, its schema)`, reading one batch only when it has to.

    A `RecordBatchReader` states its schema up front, so it comes back
    untouched and still fully lazy. A plain iterator only reveals its shape by
    producing a batch, so one is pulled and then chained back on the front --
    the caller still sees every batch, in order, exactly once.
    """
    if isinstance(source, pyarrow.RecordBatchReader):
        return source, source.schema
    iterator = iter(source)
    first = next(iterator, None)
    if first is None:
        _close_iterators(iterator, source)
        return iter(()), None

    def restored() -> Iterator[pyarrow.RecordBatch]:
        try:
            yield first
            yield from iterator
        finally:
            _close_iterators(iterator, source)

    return restored(), first.schema


def _close_iterators(iterator: Any, source: Any) -> None:
    """Close an iterator and its distinct iterable owner once each."""
    close = getattr(iterator, "close", None)
    if close is not None:
        close()
    if source is not iterator and (close := getattr(source, "close", None)) is not None:
        close()
