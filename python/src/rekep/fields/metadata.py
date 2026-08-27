"""Typed views over a field's metadata, one class per protocol namespace.

`ProtocolMetadata` is the mechanism: a live `MutableMapping` over the
`prefix:key` slice of one field's metadata, mutating the original mapping in
place -- no copy of the dict per write -- and telling the field so the
containers above rebuild exactly as assigning `metadata` would.

The subclasses are the vocabularies. `FixMetadata`, `IcebergMetadata` and
`EnumMetadata` spell each protocol's keys as typed properties -- `fix.tag`
is an `int`, `iceberg.primary_key` a `bool`, `enum.members` the decoded
`values` map -- so a reader never re-derives a key's encoding at a call
site, and a writer cannot spell it two ways. A decoded map whose stored key
would shadow a mapping method (`values`) answers under its own name, so the
view stays the `MutableMapping` it advertises.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, MutableMapping
from typing import TYPE_CHECKING, Any

from rekep.enums import EventType, State

if TYPE_CHECKING:
    from rekep.fields.field import Field

#: The partition transform that means "the value itself".
IDENTITY = "identity"

#: What a sort key means when a declaration only says there is one.
ASCENDING = "asc"


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
        stored = self.field.metadata
        if isinstance(stored, dict):
            stored[self.key_of(key)] = str(value)
            self.field._metadata_changed()
        else:
            self.field.metadata = {**(stored or {}), self.key_of(key): str(value)}

    def __delitem__(self, key: str) -> None:
        full = self.key_of(key)
        stored = self.field.metadata
        if not stored or full not in stored:
            raise KeyError(f"{self.field.name or 'field'} has no {full!r}")
        if isinstance(stored, dict):
            del stored[full]
            self.field._metadata_changed()
        else:
            self.field.metadata = {name: value for name, value in stored.items() if name != full}

    def __iter__(self) -> Iterator[str]:
        marker = f"{self.prefix}:"
        return (key[len(marker) :] for key in self.field.metadata or {} if key.startswith(marker))

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.prefix!r}, {dict(self)!r})"


class _Text:
    """One plain-text key as an attribute: `''` when absent, dropped when empty."""

    __slots__ = ("key",)

    def __init__(self, key: str = "") -> None:
        self.key = key

    def __set_name__(self, owner: type, name: str) -> None:
        self.key = self.key or name

    def __get__(self, view: ProtocolMetadata | None, owner: type | None = None) -> Any:
        if view is None:
            return self
        return view.get(self.key, "")

    def __set__(self, view: ProtocolMetadata, value: Any) -> None:
        if value:
            view[self.key] = str(value)
        else:
            view.pop(self.key, None)


class _Number:
    """One integer key as an attribute: `None` when absent, dropped on `None`."""

    __slots__ = ("key",)

    def __init__(self, key: str = "") -> None:
        self.key = key

    def __set_name__(self, owner: type, name: str) -> None:
        self.key = self.key or name

    def __get__(self, view: ProtocolMetadata | None, owner: type | None = None) -> Any:
        if view is None:
            return self
        declared = view.get(self.key)
        return int(declared) if declared else None

    def __set__(self, view: ProtocolMetadata, value: Any) -> None:
        if value is None:
            view.pop(self.key, None)
        else:
            view[self.key] = int(value)


class _Document:
    """One JSON-encoded key as an attribute, decoded on read.

    `shape` renders the decoded value -- `dict` for an object, `tuple` for
    an array -- and an empty value drops the key, so absent and empty stay
    one fact. Writing encodes compactly, exactly as the registry stores it.
    """

    __slots__ = ("key", "shape")

    def __init__(self, key: str = "", shape: type = dict) -> None:
        self.key = key
        self.shape = shape

    def __set_name__(self, owner: type, name: str) -> None:
        self.key = self.key or name

    def __get__(self, view: ProtocolMetadata | None, owner: type | None = None) -> Any:
        if view is None:
            return self
        declared = view.get(self.key)
        return self.shape() if not declared else self.shape(json.loads(declared))

    def __set__(self, view: ProtocolMetadata, value: Any) -> None:
        if not value:
            view.pop(self.key, None)
            return
        rendered = dict(value) if self.shape is dict else list(value)
        view[self.key] = json.dumps(rendered, separators=(",", ":"))


class FixMetadata(ProtocolMetadata):
    """The FIX protocol's keys, typed: `field.fix.tag`, `field.fix.meanings`.

    What a registry record states about a field, read off the field itself
    -- the step that lets a generic `Field` carry a record whole, without a
    `FieldEntry` beside it.
    """

    __slots__ = ()

    tag = _Number()
    type = _Text()
    name = _Text()
    version = _Text()
    kind = _Text()
    column = _Text()
    note = _Text()
    component = _Text()
    meanings = _Document("values")
    value_names = _Document()
    versions = _Document(shape=tuple)
    msgtypes = _Document(shape=tuple)
    components = _Document(shape=tuple)
    aliases = _Document(shape=tuple)

    @property
    def event_types(self) -> dict[str, EventType]:
        """The `{MsgType: EventType}` map a MsgType record carries."""
        declared = self.get("event_types")
        if not declared:
            return {}
        return {key: EventType.from_int(value) for key, value in json.loads(declared).items()}

    @event_types.setter
    def event_types(self, value: Mapping[str, Any] | None) -> None:
        if not value:
            self.pop("event_types", None)
            return
        rendered = {str(key): int(kind) for key, kind in dict(value).items()}
        self["event_types"] = json.dumps(rendered, separators=(",", ":"))

    @property
    def states(self) -> dict[str, State]:
        """The `{wire value: State}` map a lifecycle field carries."""
        declared = self.get("states")
        if not declared:
            return {}
        return {key: State.from_int(value) for key, value in json.loads(declared).items()}

    @states.setter
    def states(self, value: Mapping[str, Any] | None) -> None:
        if not value:
            self.pop("states", None)
            return
        rendered = {str(key): int(state) for key, state in dict(value).items()}
        self["states"] = json.dumps(rendered, separators=(",", ":"))


class IcebergMetadata(ProtocolMetadata):
    """The Iceberg protocol's keys, typed: what a table reads off a schema."""

    __slots__ = ()

    @property
    def primary_key(self) -> bool:
        """Whether this field is part of the primary key."""
        return bool(self.get("primary_key"))

    @primary_key.setter
    def primary_key(self, value: bool) -> None:
        if value and self.field.nullable:
            raise TypeError(
                f"field {self.field.name!r} is a primary key and cannot be nullable; "
                "drop the `| None` or the key"
            )
        if not value:
            self.pop("primary_key", None)
        else:
            self["primary_key"] = "true"

    @property
    def partition_key(self) -> str:
        """The partition transform, or an empty string when not partitioned."""
        return self.get("partition_key", "")

    @partition_key.setter
    def partition_key(self, value: bool | str) -> None:
        """Set the partition transform: True is `identity`, a string is itself.

        The transform is spelled as it was declared -- `identity`, `day`,
        `bucket[16]` -- and stays a string here: what it means is the reading
        protocol's business.
        """
        if not value:
            self.pop("partition_key", None)
            return
        self["partition_key"] = IDENTITY if value is True else str(value)

    @property
    def field_id(self) -> int | None:
        """The Iceberg column id this field carries, or None when it has none."""
        declared = self.get("field_id")
        return int(declared) if declared else None

    @field_id.setter
    def field_id(self, value: int | None) -> None:
        if value is None:
            self.pop("field_id", None)
            return
        if int(value) < 1:
            raise ValueError(
                f"{self.field.name!r} cannot have field_id {value}: Iceberg numbers columns from 1"
            )
        self["field_id"] = int(value)

    @property
    def sort_key(self) -> str:
        """The sort direction, or an empty string when not a sort key."""
        return self.get("sort_key", "")

    @sort_key.setter
    def sort_key(self, value: bool | str) -> None:
        if not value:
            self.pop("sort_key", None)
            return
        self["sort_key"] = ASCENDING if value is True else str(value)

    @property
    def derived_from(self) -> tuple[str, ...]:
        """Columns this field is a function of, or nothing when it stands alone."""
        declared = self.get("derived_from", "")
        return tuple(name for name in declared.split(",") if name)

    @derived_from.setter
    def derived_from(self, value: Any) -> None:
        names = [value] if isinstance(value, str) else list(value or ())
        if not names:
            self.pop("derived_from", None)
            return
        if self.field.name and self.field.name in names:
            raise ValueError(f"field {self.field.name!r} cannot be derived from itself")
        self["derived_from"] = ",".join(names)

    sort_order = _Document(shape=tuple)


class EnumMetadata(ProtocolMetadata):
    """The enum protocol's keys, typed: what a stored code column means."""

    __slots__ = ()

    name = _Text()
    key_type = _Text()
    value_type = _Text()
    encoding = _Text()
    padding = _Text()
    pattern = _Text()
    byte_width = _Number()
    members = _Document("values")
    aliases = _Document()
    fix_values = _Document()
    fix_aliases = _Document()
