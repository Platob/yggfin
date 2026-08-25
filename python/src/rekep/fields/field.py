"""`Field`: one named Arrow type with metadata, and the decorator that makes a class one."""

from __future__ import annotations

import dataclasses
import functools
import itertools
import re
from collections.abc import Callable, Iterator, Mapping, MutableMapping, Sequence
from types import MappingProxyType
from typing import Any, Self

import pyarrow

from rekep.annotations import hide_private, restore_private_slots, unwrap_annotated
from rekep.convert import Convertible
from rekep.fields import arrays
from rekep.fields.arrow import merge_fields

#: Metadata key a documentation line lands under -- the one Arrow, parquet and
#: every viewer downstream read as the column comment.
DESCRIPTION = "description"

#: Metadata key carrying the module a class-shaped field was declared in, so
#: `into_dataclass` can give the rebuilt class its identity back.
NAMESPACE = "namespace"

#: Metadata key carrying the name of a struct field flattened into a schema.
NAME = "name"

#: Keys a downstream protocol owns are prefixed with its name, so one
#: namespace's keys can never collide with another's. `Field.protocol` is the
#: one reader and writer of a prefix; these two spell out the keys the Iceberg
#: protocol already claims.
ICEBERG = "iceberg"
FIX = "fix"
PRIMARY_KEY = "iceberg:primary_key"
PARTITION_KEY = "iceberg:partition_key"

#: Iceberg identifies a column by id and never by name, so an id is part of
#: what a schema *is* once a table exists. It rides under the protocol's own
#: prefix like every other Iceberg key -- the ecosystem's `PARQUET:field_id`
#: is what parquet files carry, and the two are translated at the Iceberg
#: boundary rather than mixed here.
FIELD_ID = "iceberg:field_id"

#: The partition transform that means "the value itself".
IDENTITY = "identity"

#: Which columns a table is kept sorted by, and which way. Declaration order is
#: the sort order -- a second key would have to be a number every declaration
#: then has to keep consistent, and the members are already in an order.
SORT_KEY = "iceberg:sort_key"

#: What a sort key means when a declaration only says there is one.
ASCENDING = "asc"

#: The declaration; everything else a field holds is derived from these.
DECLARED = ("name", "arrow_type", "nullable", "metadata")

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
_FIELD_CASTS = MappingProxyType(
    {
        pyarrow.Array: "arrow_array",
        pyarrow.ChunkedArray: "arrow_array",
    }
)


class ProtocolMetadata(MutableMapping):
    """One protocol's keys in a field's metadata: `prefix:key = value`."""

    __slots__ = ("field", "prefix")

    def __init__(self, field: Field, prefix: str) -> None:
        self.field = field
        self.prefix = prefix

    def key_of(self, key: str) -> str:
        """The metadata key one of this protocol's keys lands under."""
        return f"{self.prefix}:{key}"

    def __getitem__(self, key: str) -> str:
        try:
            return self.field.metadata[self.key_of(key)]
        except KeyError:
            raise KeyError(f"{self.field.name or 'field'} has no {self.key_of(key)!r}") from None

    def __setitem__(self, key: str, value: Any) -> None:
        self.field.metadata = {**self.field.metadata, self.key_of(key): str(value)}

    def __delitem__(self, key: str) -> None:
        full = self.key_of(key)
        if full not in self.field.metadata:
            raise KeyError(f"{self.field.name or 'field'} has no {full!r}")
        self.field.metadata = _without(self.field.metadata, full)

    def __iter__(self) -> Iterator[str]:
        marker = f"{self.prefix}:"
        return (key[len(marker) :] for key in self.field.metadata if key.startswith(marker))

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.prefix!r}, {dict(self)!r})"


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
    arrow_type: pyarrow.DataType | None = None
    nullable: bool | None = None
    metadata: Mapping[str, str] | None = None

    def __new__(
        cls,
        name: str = "",
        arrow_type: pyarrow.DataType | None = None,
        nullable: bool | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> Field:
        """Redirect to the subclass `arrow_type` calls for.

        Declared here rather than in a factory so that every path that builds a
        field -- `Field(...)`, `from_arrow_field`, `from_dict`, a builder --
        lands on the right class without any of them having to know the rule.
        Asking for a subclass explicitly is honoured as written.
        """
        return object.__new__(_class_for(arrow_type) if cls is Field else cls)

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
        reads the metadata in place, and setting through it rebuilds the
        containers above exactly as assigning `metadata` would.
        """
        return ProtocolMetadata(self, prefix)

    @property
    def iceberg(self) -> ProtocolMetadata:
        """The keys the Iceberg protocol owns: `iceberg:primary_key`, ..."""
        return self.protocol(ICEBERG)

    @property
    def fix(self) -> ProtocolMetadata:
        """The keys the FIX protocol owns: `fix:tag`, `fix:type`, ..."""
        return self.protocol(FIX)

    @property
    def is_primary_key(self) -> bool:
        """Whether this field is part of the primary key.

        The one list Iceberg calls identifier fields and an upsert joins on --
        declared once, read from metadata like every other protocol property.
        """
        return bool(self.iceberg.get("primary_key"))

    @is_primary_key.setter
    def is_primary_key(self, value: bool) -> None:
        if value and self.nullable:
            raise TypeError(
                f"field {self.name!r} is a primary key and cannot be nullable; "
                "drop the `| None` or the key"
            )
        if not value:
            self.iceberg.pop("primary_key", None)
        else:
            self.iceberg["primary_key"] = "true"

    @property
    def is_partition_key(self) -> bool:
        """Whether the data is partitioned on this field."""
        return bool(self.iceberg.get("partition_key"))

    @is_partition_key.setter
    def is_partition_key(self, value: bool | str) -> None:
        """Set the partition transform: True is `identity`, a string is itself.

        The transform is spelled as it was declared -- `identity`, `day`,
        `bucket[16]` -- and stays a string here: what it means is the reading
        protocol's business.
        """
        if not value:
            self.iceberg.pop("partition_key", None)
            return
        self.iceberg["partition_key"] = IDENTITY if value is True else str(value)

    @property
    def field_id(self) -> int | None:
        """The Iceberg column id this field carries, or None when it has none."""
        declared = self.iceberg.get("field_id")
        return int(declared) if declared else None

    @field_id.setter
    def field_id(self, value: int | None) -> None:
        if value is None:
            self.iceberg.pop("field_id", None)
            return
        if int(value) < 1:
            raise ValueError(
                f"{self.name!r} cannot have field_id {value}: Iceberg numbers columns from 1"
            )
        self.iceberg["field_id"] = int(value)

    @property
    def partition_transform(self) -> str:
        """How the data is partitioned on this field, or an empty string."""
        return self.iceberg.get("partition_key", "")

    @property
    def derived_from(self) -> tuple[str, ...]:
        """Columns this field is a function of, or nothing when it stands alone."""
        declared = self.iceberg.get("derived_from", "")
        return tuple(name for name in declared.split(",") if name)

    @derived_from.setter
    def derived_from(self, value: str | Sequence[str] | None) -> None:
        names = [value] if isinstance(value, str) else list(value or ())
        if not names:
            self.iceberg.pop("derived_from", None)
            return
        if self.name and self.name in names:
            raise ValueError(f"field {self.name!r} cannot be derived from itself")
        self.iceberg["derived_from"] = ",".join(names)

    @property
    def is_sort_key(self) -> bool:
        """Whether the data is kept sorted on this field.

        A *sort order*, not a partition: it does not decide which file a row
        lands in, it decides where in the file it lands. That is what makes a
        range filter on it read a few row groups instead of all of them, and
        what makes the column's own min/max in a manifest narrow instead of
        spanning everything the file holds.
        """
        return bool(self.iceberg.get("sort_key"))

    @is_sort_key.setter
    def is_sort_key(self, value: bool | str) -> None:
        """Set the direction: True is ascending, a string is itself."""
        if not value:
            self.iceberg.pop("sort_key", None)
            return
        self.iceberg["sort_key"] = ASCENDING if value is True else str(value)

    @property
    def sort_direction(self) -> str:
        """Which way the data is sorted on this field, or an empty string."""
        return self.iceberg.get("sort_key", "")

    def merge(self, other: Field) -> Field:
        """Combine two declarations, letting `other` win where it says anything."""
        return Field(
            name=other.name or self.name,
            arrow_type=other.arrow_type if other.arrow_type is not None else self.arrow_type,
            nullable=other.nullable if other.nullable is not None else self.nullable,
            metadata={**self.metadata, **other.metadata},
        )

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
            return cls(arrow_type=extra)
        if isinstance(extra, Mapping):
            return cls(metadata=extra)
        if isinstance(extra, str):
            return cls(metadata={DESCRIPTION: extra})
        return Field()

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
            arrow_type=source.type,
            nullable=source.nullable,
            metadata=decoded(source.metadata),
        )

    @classmethod
    def from_arrow_type(cls, source: pyarrow.DataType, name: str = "") -> Field:
        """An Arrow type as a field, non-nullable and undocumented."""
        return Field(name=name, arrow_type=source, nullable=False)

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
            arrow_type=pyarrow.struct(list(source)),
            nullable=False,
            metadata=metadata,
        )

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Field:
        """Read a field back from the plain containers `into_dict` writes."""
        metadata = dict(mapping.get("metadata") or {})
        described = mapping.get(DESCRIPTION)
        if described:
            metadata[DESCRIPTION] = described
        return Field(
            name=mapping.get(NAME, ""),
            arrow_type=cls._type_of(mapping),
            nullable=bool(mapping.get("nullable", False)),
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
            return pyarrow.struct(
                [cls.from_dict(member).into_arrow_field() for member in mapping.get("fields", [])]
            )
        if kind == "map":
            key = cls._member(mapping, "key").into_arrow_field()
            value = cls._member(mapping, "value").into_arrow_field()
            return pyarrow.map_(key, value, keys_sorted=_flag(mapping, "keys_sorted"))
        if kind == "fixed_size_list":
            return pyarrow.list_(
                cls._member(mapping, "item").into_arrow_field(),
                _list_size(mapping),
            )
        build = _LIST_KINDS.get(kind)
        if build is not None:
            return build(cls._member(mapping, "item").into_arrow_field())
        return arrow_type_for(kind)

    @classmethod
    def _member(cls, mapping: Mapping[str, Any], key: str) -> Field:
        """A nested `item`/`key`/`value` block, whose name Arrow owns."""
        return cls.from_dict({NAME: key, **(mapping.get(key) or {})})

    # -- converting ---------------------------------------------------------

    @functools.cached_property
    def arrow_field(self) -> pyarrow.Field:
        """This field as Arrow's own, built once per declaration."""
        if self.arrow_type is None:
            raise TypeError(f"field {self.name!r} has no Arrow type to convert")
        return pyarrow.field(
            self.name,
            self.arrow_type,
            nullable=bool(self.nullable),
            metadata=dict(self.metadata) or None,
        )

    def into_arrow_field(self) -> pyarrow.Field:
        """This field as Arrow's own, unstated nullability reading NOT NULL."""
        return self.arrow_field

    def into_arrow_type(self) -> pyarrow.DataType:
        """This field's Arrow type."""
        if self.arrow_type is None:
            raise TypeError(f"field {self.name!r} has no Arrow type to convert")
        return self.arrow_type

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

        A struct is a `fields:` list, a list an `item:`, a map a `key:`/`value:`
        pair -- never a flat `struct<...>` string, which would bury the nested
        descriptions the dump exists to show. Scalars stay one line.
        """
        described: dict[str, Any] = {NAME: self.name, "type": self.kind()}
        if self.nullable:
            described["nullable"] = True
        metadata = dict(self.metadata)
        described_as = metadata.pop(DESCRIPTION, None)
        if described_as:
            described[DESCRIPTION] = described_as
        if metadata:
            described["metadata"] = metadata
        described.update(self.nested())  # fields/item/key/value blocks read best last
        return described

    def kind(self) -> str:
        """How `into_dict` names this field's type."""
        return str(self.arrow_type)

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
                type=self.arrow_type,
            )
        if array.type == self.arrow_type:
            return array
        return array.cast(self.arrow_type, safe=safe)


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
        return self._member_of(self.arrow_type.field(0))

    @property
    def fields(self) -> tuple[Field, ...]:
        return (self.item,)

    def with_item(self, item: pyarrow.Field) -> pyarrow.DataType:
        """This flavour of list, around another item."""
        return pyarrow.list_(item)

    def _member_changed(self, member: Field) -> None:
        self.arrow_type = self.with_item(member.into_arrow_field())

    def kind(self) -> str:
        return "list"

    def nested(self) -> dict[str, Any]:
        return {"item": _anonymous(self.item)}

    def leaf_names(self) -> list[str]:
        return self._extend({"item": self.item})

    def cast_arrow_array(self, array: Any, *, safe: bool = False) -> Any:
        """Cast the values, then cut them back into rows of this flavour."""
        if isinstance(array, pyarrow.ChunkedArray) or array.type == self.arrow_type:
            return super().cast_arrow_array(array, safe=safe)
        if pyarrow.types.is_struct(array.type):
            columns = [
                self.item.cast_arrow_array(column, safe=safe)
                for column in arrays.struct_columns(array).values()
            ]
            values, _ = arrays.interleave(columns, len(array))
            sizes = arrays.repeat_sizes(len(columns), len(array))
            return arrays.build_list(self.arrow_type, sizes, values, arrays.null_mask(array))
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
            self.arrow_type
        )

    def _rebuilt(self, array: Any, *, safe: bool) -> Any:
        """Cut the rows again, from the sizes: right for any layout at all."""
        sizes, values = arrays.list_parts(array)
        return arrays.build_list(
            self.arrow_type,
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
        if middle == self.arrow_type:
            return wrapped
        try:
            return wrapped.cast(self.arrow_type, safe)
        except pyarrow.ArrowNotImplementedError:
            # A flavour Arrow will not cast between at all: cut the rows again.
            return type(self)(name=self.name, arrow_type=self.arrow_type)._rebuilt(
                wrapped, safe=True
            )


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
        return self.arrow_type.list_size

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
        return self._member_of(self.arrow_type.key_field)

    @functools.cached_property
    def value(self) -> Field:
        """The value half of one entry."""
        return self._member_of(self.arrow_type.item_field)

    @property
    def fields(self) -> tuple[Field, ...]:
        return (self.key, self.value)

    def _member_changed(self, member: Field) -> None:
        halves = {"key": self.key, "value": self.value}
        halves[member.name if member.name in halves else "value"] = member
        self.arrow_type = pyarrow.map_(
            halves["key"].into_arrow_field(), halves["value"].into_arrow_field()
        )

    def kind(self) -> str:
        return "map"

    def nested(self) -> dict[str, Any]:
        """Both halves -- and whether the keys are sorted, which is in the type.

        Arrow compares two maps that disagree on it as different types, so a
        dump that left it out read back as a map a cast would refuse.
        """
        described: dict[str, Any] = {}
        if self.arrow_type.keys_sorted:
            described["keys_sorted"] = True
        described["key"] = _anonymous(self.key)
        described["value"] = _anonymous(self.value)
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
        if isinstance(array, pyarrow.ChunkedArray) or array.type == self.arrow_type:
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
                self.arrow_type,
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
            self.arrow_type,
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
                self.arrow_type,
                arrays.repeat_sizes(0, len(array)),
                pyarrow.array([], self.arrow_type.key_type),
                pyarrow.array([], self.arrow_type.item_type),
                arrays.null_mask(array),
            )
        values, member = arrays.interleave(
            [self.value.cast_arrow_array(column, safe=safe) for column in columns.values()],
            len(array),
        )
        keys = arrays.names_array(list(columns), member, self.key.arrow_type)
        sizes = arrays.repeat_sizes(len(columns), len(array))
        return arrays.build_map(self.arrow_type, sizes, keys, values, arrays.null_mask(array))


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
        data_type = self.arrow_type
        return tuple(
            self._member_of(data_type.field(index)) for index in range(data_type.num_fields)
        )

    @functools.cached_property
    def _by_name(self) -> dict[str, Field]:
        return {member.name: member for member in self.fields}

    def field(self, name: str) -> Field:
        """One member by name."""
        try:
            return self._by_name[name]
        except KeyError:
            raise KeyError(f"{self.name or 'struct'} has no member {name!r}") from None

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
        return {member.name: member.sort_direction for member in self.fields if member.is_sort_key}

    def derived_keys(self) -> dict[str, tuple[str, ...]]:
        """Members that are a function of other members, mapped to those."""
        return {member.name: member.derived_from for member in self.fields if member.derived_from}

    def _member_changed(self, member: Field) -> None:
        self.arrow_type = pyarrow.struct(
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
    def from_iceberg_schema(cls, source: Any, name: str = "", spec: Any = None) -> StructField:
        """A `pyiceberg` schema as a struct field: docs, keys and partitions."""
        from rekep.iceberg.fields import iceberg_struct_field

        return iceberg_struct_field(source, name, spec)

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
        if isinstance(array, pyarrow.ChunkedArray) or array.type == self.arrow_type:
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
            return lambda name: arrays.list_column(array, positions[name])
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
                columns.append(pyarrow.nulls(length, member.arrow_type))
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

        def generate() -> Iterator[pyarrow.RecordBatch]:
            for batch in source:
                yield target.cast_arrow_batch(batch, safe=safe)

        return pyarrow.RecordBatchReader.from_batches(target.arrow_schema, generate())

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


def decoded(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    """Arrow metadata as text, which is how a `Field` holds it."""
    return {key.decode(): value.decode() for key, value in (metadata or {}).items()}


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


#: How a dumped list flavour is built back. Every flavour used to dump itself
#: as `list`, so a contract file read back narrowed a `large_list` to 32-bit
#: offsets and turned a view into a list -- silently, since both cast.
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
)


def _class_for(arrow_type: pyarrow.DataType | None) -> type[Field]:
    """The `Field` subclass that speaks for `arrow_type`."""
    if arrow_type is None:
        return Field
    for matches, name in _KINDS:
        if matches(arrow_type):
            return globals()[name]
    return Field


def _is_list_like(arrow_type: pyarrow.DataType) -> bool:
    """Whether rows of `arrow_type` are runs of values: any list flavour, or a map."""
    kinds = pyarrow.types
    return bool(
        kinds.is_list(arrow_type)
        or kinds.is_large_list(arrow_type)
        or kinds.is_list_view(arrow_type)
        or kinds.is_large_list_view(arrow_type)
        or kinds.is_fixed_size_list(arrow_type)
        or kinds.is_map(arrow_type)
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

    `bool("false")` is True, so a hand-written `keys_sorted: 'false'` used to
    turn the flag *on* -- and `keys_sorted` is part of a map's Arrow type, so
    that is a different type read back from the same file.
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


def _anonymous(member: Field) -> dict[str, Any]:
    """A list item or map half: a field whose name Arrow owns, not the author."""
    described = member.into_dict()
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
        return iter(()), None
    return itertools.chain([first], iterator), first.schema
