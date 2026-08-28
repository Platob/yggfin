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

A FIX field's enumerated values are `FixFieldValue` records rather than
several parallel maps: one value knows what it means and every spelling
that names it, and the one lookup a parse needs -- a written spelling to
the FIX value it names -- is derived from that list and cached, never
stored beside it. There is no lookup the other way: the wire value is the
fact, and the meaning is what it means.
"""

from __future__ import annotations

import dataclasses
import functools
import json
import re
from collections.abc import Iterable, Iterator, Mapping, MutableMapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from rekep.convert import Convertible
from rekep.enums import EventType, State

if TYPE_CHECKING:
    from rekep.fields.field import Field

#: The partition transform that means "the value itself".
IDENTITY = "identity"

#: What a sort key means when a declaration only says there is one.
ASCENDING = "asc"


#: A version list that holds for every version, which is what a field outside
#: the standard has.
ANY_VERSION = "*"

#: What an encoded key keeps: nothing that is not a letter or a digit, so
#: `ORDER_SUBMISSION_TIME`, `Order Submission Time` and `ordersubmissiontime`
#: are one key where plain lowercasing leaves three.
_ENCODED_DROP = re.compile(r"[^a-z0-9]+", re.ASCII)


def fold(name: Any) -> str:
    """One spelling as it is matched: case-folded, separators kept."""
    return str(name).strip().casefold()


@functools.lru_cache(maxsize=4096)
def encoded_key(text: Any) -> str:
    """A value or its name as `encoded` keys it: casefolded letters and digits.

    Memoized because a feed asks the same few hundred spellings on every
    message.
    """
    return _ENCODED_DROP.sub("", str(text).casefold())


def version_rank(version: str) -> tuple[int, ...]:
    """A sortable reading of `4.0`, `5.0.SP2`, `FIXT1.1`, newest last.

    The transport (`FIXT1.1`) ranks *above* every application version here, so
    that sorting a record's versions gives `versions.json`'s declared order.
    It is deliberately the opposite of what `newest_of` picks: the session
    layer redefines a handful of application fields, and letting it own their
    reading would give a session-layer meaning to fields it merely carries.
    """
    transport = 1 if version.upper().startswith("FIXT") else 0
    return (transport, *(int(part) for part in re.findall(r"\d+", version)))


def newest_rank(version: str) -> tuple[int, ...]:
    """`version_rank` with the transport ranked below every application version."""
    transport, *numbers = version_rank(version)
    return (1 - transport, *numbers)


def newest_of(versions: Iterable[str]) -> str:
    """Which version owns a record's reading: the newest *application* one.

    `FIXT1.1` only wins where nothing else declares the field, which is what
    keeps a session-layer reading off the application fields it merely carries.
    """
    found = tuple(versions)
    if not found:
        raise ValueError("a FIX registry record is declared for no version")
    return max(found, key=newest_rank)


def canonical_versions(versions: Iterable[str]) -> tuple[str, ...]:
    """A record's version list in canonical order: oldest first, transport last."""
    return tuple(sorted(dict.fromkeys(versions), key=version_rank))


@dataclasses.dataclass(frozen=True)
class Alias(Convertible):
    """Another name one identity has been seen under, and where that was seen.

    Provenance rather than a bare string, because an alias earned from a real
    capture and one typed in by hand are not the same evidence -- and a near
    miss counted forty times in one bridge is a different proposition from one
    counted once. A FIX version is a source too: a spelling only 4.2 used is
    recorded here, because the record itself keeps one name.
    """

    name: str
    source: str = ""
    occurrences: int = 0

    def __post_init__(self) -> None:
        """Refuse an unnamed alias, which would match the empty key."""
        if not str(self.name).strip():
            raise ValueError("a FIX registry alias has no name")

    @property
    def folded(self) -> str:
        """How this alias is matched."""
        return fold(self.name)

    def into_dict(self) -> dict[str, Any]:
        """The alias as it is stored, carrying provenance only when it has any."""
        if not self.source and not self.occurrences:
            return {"name": self.name}
        return {"name": self.name, "source": self.source, "occurrences": self.occurrences}

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any] | str) -> Alias:
        """Read either spelling: a plain name, or a name with its provenance."""
        if isinstance(mapping, str):
            return cls(name=mapping)
        return cls(
            name=str(mapping.get("name") or ""),
            source=str(mapping.get("source") or ""),
            occurrences=int(mapping.get("occurrences") or 0),
        )


@dataclasses.dataclass(frozen=True)
class FixFieldValue(Convertible):
    """One enumerated value of one FIX field, and everything that names it.

    The wire value, the prose a person reads, and every other spelling the
    dictionary or a feed writes for it. One record instead of the parallel
    maps this replaces: the value's meaning, its spec symbol and the
    spelling-to-value lookup were the same fact stored three times, and only
    the first two are facts at all -- `encodings_of` derives the lookup.

    A value is what the wire carries and what it officially means. There is
    no reverse: a reader that has the value already has the fact, and a name
    derived back out of it was a second vocabulary nobody declared.
    """

    value: str = ""
    """What the wire carries: `Side <54>` value `1`."""

    meaning: str = ""
    """What it means, as the dictionary's prose spells it: `Buy`."""

    aliases: tuple[str, ...] = ()
    """Every other spelling naming this value, the spec symbol (`BUY`) first."""

    def __post_init__(self) -> None:
        """Refuse an unnamed value and settle the spellings' shape."""
        value = str(self.value).strip()
        if not value:
            raise ValueError("a FIX field value carries no value")
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "meaning", str(self.meaning or ""))
        object.__setattr__(self, "aliases", _aliases(self.aliases))

    def spellings(self) -> tuple[str, ...]:
        """Every spelling that names this value, the raw value included."""
        return tuple(one for one in (self.meaning, *self.aliases, self.value) if one)

    def into_dict(self) -> dict[str, Any]:
        """The value as it is stored, carrying aliases only when it has any."""
        found: dict[str, Any] = {"value": self.value, "meaning": self.meaning}
        if self.aliases:
            found["aliases"] = list(self.aliases)
        return found

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> FixFieldValue:
        """One value from its stored document."""
        return cls(
            value=str(mapping.get("value") or ""),
            meaning=str(mapping.get("meaning") or ""),
            aliases=tuple(str(one) for one in mapping.get("aliases") or ()),
        )


def _aliases(declared: Any) -> tuple[str, ...]:
    """Declared alternative spellings, stripped, non-empty and deduplicated."""
    found: dict[str, str] = {}
    for one in declared or ():
        spelled = str(one).strip()
        if spelled:
            found.setdefault(spelled, spelled)
    return tuple(found)


def values_of(declared: Any) -> tuple[FixFieldValue, ...]:
    """One field's enumerated values from whatever spelling declared them.

    A list of records is the stored spelling and a `{value: meaning}` mapping
    is the one a declaration writes by hand; both land here, so no caller has
    to build the records to state four values.
    """
    if not declared:
        return ()
    if isinstance(declared, Mapping):
        return tuple(
            FixFieldValue(value=str(value), meaning=str(meaning))
            for value, meaning in declared.items()
        )
    return tuple(
        one if isinstance(one, FixFieldValue) else FixFieldValue.from_dict(one) for one in declared
    )


@functools.lru_cache(maxsize=8192)
def encodings_of(values: tuple[FixFieldValue, ...]) -> Any:
    """`({normalized spelling: value}, {dropped spelling: the values claiming it})`.

    Built from the prose, the symbols and the raw values themselves, so a
    caller has one lookup path rather than several. A spelling two values
    both normalize to is emitted for neither: an ambiguous translation that
    silently picks one is worse than none, and the lookup falls through to
    the raw value.

    Cached wide enough to hold the whole published dictionary, because a
    transcription walks every field of a version and asks each for this.
    """
    claimed: dict[str, list[str]] = {}
    for one in values:
        for spelled in one.spellings():
            owners = claimed.setdefault(encoded_key(spelled), [])
            if one.value not in owners:
                owners.append(one.value)
    found = {key: owners[0] for key, owners in claimed.items() if key and len(owners) == 1}
    dropped = {key: tuple(owners) for key, owners in claimed.items() if key and len(owners) > 1}
    return MappingProxyType(found), MappingProxyType(dropped)


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


@functools.lru_cache(maxsize=8192)
def _enumerated(declared: str) -> tuple[FixFieldValue, ...]:
    """One record's stored values, decoded once for that exact spelling."""
    return values_of(json.loads(declared))


class FixMetadata(ProtocolMetadata):
    """The FIX protocol's keys, typed: `field.fix.tag`, `field.fix.enumerated`.

    What a registry record states about a field, read off the field itself.
    A record *is* a field here -- there is no second object beside it -- so
    this is the whole of what the dictionary knows about one identity: what it
    is called, what it is, which versions declare it, what its values mean, and
    the names it has been seen under.
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
    #: The message type a declaration defines, where it defines one -- `"D"`,
    #: `"8"`. Empty for a reusable component, which is what most blocks are.
    msgtype = _Text()
    versions = _Document(shape=tuple)
    msgtypes = _Document(shape=tuple)
    components = _Document(shape=tuple)
    aliases = _Document(shape=tuple)

    @property
    def enumerated(self) -> tuple[FixFieldValue, ...]:
        """Every value this field enumerates, in the order the record lists them.

        Stored under `fix:values` and read under its own name, because a
        `values` attribute would shadow the mapping's own `values()` -- the
        same reason `meanings` and `EnumMetadata.members` are spelled as they
        are.

        Decoded once per stored spelling rather than once per read: a parse
        asks the same few hundred records their values on every message, and
        the string in the metadata is exactly the cache key for what it says.
        """
        declared = self.get("values")
        return _enumerated(declared) if declared else ()

    @enumerated.setter
    def enumerated(self, declared: Any) -> None:
        found = values_of(declared)
        if not found:
            self.pop("values", None)
            return
        self["values"] = json.dumps([one.into_dict() for one in found], separators=(",", ":"))

    @property
    def meanings(self) -> dict[str, str]:
        """`{wire value: prose}` for the values that define one."""
        return {one.value: one.meaning for one in self.enumerated if one.meaning}

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

    # -- what the record is -------------------------------------------------

    @property
    def canonical(self) -> str:
        """The FIX name this field carries, or the field's own where they agree.

        `fix:name` is written only where the two can differ -- a lifted column
        called `sending_time` carrying `SendingTime` -- so a record that is
        simply itself says its name once.
        """
        return self.name or self.field.name

    @property
    def key(self) -> int | str:
        """What this identity is stored under: its tag, or its folded name."""
        tag = self.tag
        return tag if tag is not None else self.folded

    @property
    def folded(self) -> str:
        """The canonical name as it is matched."""
        return fold(self.canonical)

    @property
    def newest(self) -> str:
        """The version this record's reading was taken from."""
        return newest_of(self.versions)

    @property
    def named_aliases(self) -> tuple[Alias, ...]:
        """Every alias as the record it is, rather than as the document it stores.

        The stored form is a list of mappings, because that is what travels in
        one metadata string; a caller matching spellings wants the provenance
        and the fold, which is what `Alias` is.
        """
        return tuple(Alias.from_dict(one) for one in self.aliases)

    @named_aliases.setter
    def named_aliases(self, value: Any) -> None:
        self.aliases = [
            (one if isinstance(one, Alias) else Alias.from_dict(one)).into_dict()
            for one in (value or ())
        ]

    # -- what the record answers --------------------------------------------

    def declares(self, version: str) -> bool:
        """Whether this field holds for `version`, wildcard included."""
        declared = self.versions
        return version in declared or ANY_VERSION in declared

    def spellings(self) -> tuple[str, ...]:
        """Every name this field answers to: canonical first, then its aliases,
        deduplicated by fold and in the order a lookup applies them."""
        found: dict[str, str] = {}
        for name in (self.canonical, *(str(alias.get("name", "")) for alias in self.aliases)):
            if name.strip():
                found.setdefault(fold(name), name)
        return tuple(found.values())

    def encode(self, value: Any) -> str:
        """The FIX value a spelling names, or the spelling itself when none does."""
        return self.encoded.get(encoded_key(value), str(value))

    def value_of(self, value: Any) -> FixFieldValue | None:
        """The record for one wire value, or None where no version defines it."""
        spelled = str(value)
        for one in self.enumerated:
            if one.value == spelled:
                return one
        return None

    def meaning(self, value: Any) -> str | None:
        """What one value means where this field enumerates its values.

        The prose before the symbol -- `Side <54>` value `1` is "Buy" for a
        person -- and None where nothing defines it.
        """
        found = self.value_of(value)
        if found is None:
            return None
        return found.meaning or (found.aliases[0] if found.aliases else None)

    def event_type(self, value: Any) -> EventType:
        """The configured kind of one MsgType, MISC when known, else UNKNOWN."""
        spelled = str(value) if value is not None else ""
        configured = self.event_types.get(spelled)
        if configured is not None:
            return configured
        return EventType.MISC if self.value_of(spelled) else EventType.UNKNOWN

    @property
    def encoded(self) -> Mapping[str, str]:
        """`{normalized spelling: wire value}`, derived from what is stored."""
        return encodings_of(self.enumerated)[0]


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
