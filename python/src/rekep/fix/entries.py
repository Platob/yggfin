"""One record per field or component *identity*, cross-version by nature.

A field's reading is not a property of a FIX version: one tag means one thing,
and a set of versions declare it. So a record holds that one reading -- the
name, the datatype, the prose, the enumerated values -- beside `versions`, the
list of versions that declare it. Where two versions disagree the newest one
wins and the collapse is reported, which is the only judgement the shape asks
for.

The same shape holds fields FIX never numbered -- a bridge's rendered
`AMON.ISINCODE`, a vendor's `TECH.CLIENTID` -- with no tag and `ANY_VERSION`
for their version list. They are aliased and looked up exactly like numbered
tags, rather than living in a second incompatible mapping.
"""

from __future__ import annotations

import dataclasses
import json
import re
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any, Self

import pyarrow

from rekep.convert import Convertible
from rekep.entries import fold
from rekep.enums import EventType, State
from rekep.fields import Field
from rekep.fields.metadata import (
    ANY_VERSION,
    Alias,
    FixFieldValue,
    canonical_versions,
    newest_of,
    values_of,
)
from rekep.fix import quickfix
from rekep.fix.fields import fix_field

#: What a field record is: a numbered FIX tag, or a name a renderer prints with
#: no tag behind it. Stored so a reader never has to infer it from a null tag,
#: and so a projection or a report can select one kind without guessing.
STANDARD = "standard"
NAMESPACE = "namespace"
KINDS: frozenset[str] = frozenset({STANDARD, NAMESPACE})

#: What `NAMESPACE` used to be called, still read out of a store somebody's
#: cache already holds. Written back under the current name, so a store
#: converts itself the first time anything rewrites it.
_RENAMED_KINDS: Mapping[str, str] = MappingProxyType({"vendor": NAMESPACE})

#: Every key a stored field record may carry. One reading, and the versions
#: that declare it -- there is no per-version object to differ from.
RECORD_KEYS: tuple[str, ...] = (
    "name",
    "tag",
    "kind",
    "column",
    "note",
    "type",
    "description",
    "versions",
    "values",
    "event_types",
    "states",
    "used_in",
    "components",
    "aliases",
)

_SLUG_SPLIT = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", re.ASCII)
_SLUG_DROP = re.compile(r"[^a-z0-9]+", re.ASCII)

# Registry identifiers come from prose pages as often as from XML. A suffix is
# annotation, never part of the FIX name that a renderer writes.
_NAME_DETAIL = re.compile(r"[<(].*$")
_NAME_VERSION = re.compile(r"\s+prior\s+to\s+FIX\b.*$", re.IGNORECASE)
_NAME_DROP = re.compile(r"[^A-Za-z0-9]+", re.ASCII)


def name_of(text: str) -> str:
    """A prose label as one FIX identifier."""
    named = _NAME_DETAIL.sub("", str(text).strip())
    named = _NAME_VERSION.sub("", named)
    return _NAME_DROP.sub("", named)


def slug_of(name: str) -> str:
    """The file name one component is stored under: `Parties` -> `parties`.

    Dots and case become underscores, so `AMON.ISINCODE` is `amon_isincode`
    and `NoPartyIDs` is `no_party_ids`. Two identities that slug alike are a
    collision the store refuses rather than one silently overwriting the other.
    """
    text = str(name).strip()
    if not text:
        raise ValueError("a FIX registry entry has no name")
    slug = _SLUG_DROP.sub("_", _SLUG_SPLIT.sub("_", text).lower()).strip("_")
    if not slug:
        raise ValueError(f"{name!r} does not spell a FIX registry entry name")
    return slug


#: Where a FIX name splits into words: an acronym before a capitalised word,
#: and a lowercase or digit before a capital.
_SNAKE_SPLIT = re.compile(r"(?<=[A-Z])(?=[A-Z][a-z])|(?<=[a-z0-9])(?=[A-Z])", re.ASCII)


def snake_of(name: str) -> str:
    """FIX's canonical name as a public Arrow name: `NoPartyIDs` -> `no_party_ids`.

    One rule for a lifted column and for a component member, so the flat column
    and the nested one of the same field are spelled alike.
    """
    return _SNAKE_SPLIT.sub("_", re.sub(r"IDs$", "Ids", name)).lower()


def _kind_of(stored: Any) -> str:
    """One stored `kind`, under whatever name the store spelled it."""
    kind = str(stored or STANDARD)
    return _RENAMED_KINDS.get(kind, kind)


# -- a field record, which is a field ---------------------------------------
#
# A record used to be a dataclass beside the `Field` it projected into. It is
# the field now: everything a shard stores about an identity has a `fix:` key,
# so the record and the declaration are one object and nothing is written
# twice. What is left here is the two directions the store needs -- the
# document it holds on disk, and the version a caller asked about.


def record_of(mapping: Mapping[str, Any]) -> Field:
    """One stored field document as the record it is, refusing what cannot resolve.

    The document's keys are unprefixed because that is what a shard has always
    held; they land under `fix:` where the rest of the package reads them.
    """
    unknown = sorted(set(mapping) - set(RECORD_KEYS))
    if unknown:
        raise ValueError(f"a FIX field record declares unknown {unknown}")
    stored = mapping.get("tag")
    tag = int(stored) if stored is not None else None
    name = str(mapping.get("name") or "")
    kind = _kind_of(mapping.get("kind"))
    versions = canonical_versions(str(version) for version in mapping.get("versions") or ())
    _refuse_record(name, tag, kind, versions, mapping.get("event_types"))
    built = fix_field(
        name,
        tag or 0,
        str(mapping.get("type") or "") or None,
        description=str(mapping.get("description") or "") or None,
        values=values_of(mapping.get("values")),
    )
    fix = built.fix
    if tag is None:
        # A namespaced field has no tag, and a `0` where one goes would
        # collide with every other one of them in a tag index.
        fix.tag = None
        fix.kind = NAMESPACE
    fix.versions = versions
    fix.column = str(mapping.get("column") or "")
    fix.note = str(mapping.get("note") or "")
    fix.event_types = _event_types(mapping.get("event_types"))
    fix.states = _states(mapping.get("states"))
    fix.msgtypes = [str(one) for one in mapping.get("used_in") or ()]
    fix.components = [str(one) for one in mapping.get("components") or ()]
    fix.named_aliases = _aliases_of(mapping.get("aliases"))
    return built


def _refuse_record(
    name: str, tag: int | None, kind: str, versions: Sequence[str], event_types: Any
) -> None:
    """Every reason a field record could answer no lookup, said once."""
    if not name.strip():
        raise ValueError("a FIX field record has no name")
    if kind not in KINDS:
        raise ValueError(f"unknown FIX field kind {kind!r}; one of {sorted(KINDS)}")
    if kind == STANDARD and not tag:
        raise ValueError(f"standard FIX field {name!r} has no tag")
    if kind == NAMESPACE and tag:
        raise ValueError(f"namespaced FIX field {name!r} must not claim tag {tag}")
    if not versions:
        raise ValueError(f"FIX field {name!r} is declared for no version")
    if event_types and tag != 35:
        raise ValueError("FIX event types belong to MsgType <35>")


def record_document(record: Field) -> dict[str, Any]:
    """One record as its shard holds it, under the unprefixed keys it has always used."""
    fix = record.fix
    return _document(
        {
            "name": fix.canonical,
            "tag": fix.tag,
            "kind": "" if record_kind(record) == STANDARD else record_kind(record),
            "column": fix.column,
            "note": fix.note,
            "type": fix.type,
            "description": record.description,
            "versions": list(fix.versions),
            "values": [one.into_dict() for one in fix.enumerated],
            "event_types": _enum_document(fix.event_types),
            "states": _enum_document(fix.states),
            "used_in": list(fix.msgtypes),
            "components": list(fix.components),
            "aliases": [alias.into_dict() for alias in fix.named_aliases],
        }
    )


def record_kind(record: Field) -> str:
    """Whether a record is a numbered FIX field or a name outside the standard."""
    return NAMESPACE if record.fix.tag is None else STANDARD


def record_copy(record: Field) -> Field:
    """A record nothing else holds, so a caller mutating it corrupts no cache."""
    return Field(
        name=record.name,
        dtype=record.dtype,
        nullable=record.nullable,
        metadata=dict(record.metadata),
    )


def record_for(record: Field, version: str) -> Field | None:
    """This record as `version` declares it, or None when that version has none.

    The reading is the same for every version that declares it -- that is what
    one record per identity means -- and only `fix:version` differs, because a
    caller still has to know which version it asked about.
    """
    if not record.fix.declares(version):
        return None
    built = record_copy(record)
    built.fix.version = version
    built.fix.pop("versions", None)
    built.fix.pop("aliases", None)
    return built


def records_for(record: Field, order: Sequence[str]) -> list[Field]:
    """This record as every version in `order` declares it, in that order."""
    fix = record.fix
    found = [record_for(record, version) for version in order if fix.declares(version)]
    if not found and ANY_VERSION in fix.versions:
        found = [record_for(record, ANY_VERSION)]
    return [member for member in found if member is not None]


def merged_record(record: Field, order: Sequence[str] = ()) -> Field:
    """The declaration `scalar()` hands out: one identity, every version of it.

    `order` names the versions newest first, which is the order `fix:versions`
    carries them in; the record's own canonical order is used when nothing is
    named.
    """
    fix = record.fix
    listed = [version for version in order if fix.declares(version)] or list(fix.versions)
    if not listed:
        raise KeyError(f"FIX field {fix.name!r} declares none of {list(order)}")
    built = record_copy(record)
    built.fix.name = fix.canonical
    built.fix.version = listed[0]
    built.fix.versions = listed
    return built


def collapsed_record(members: Sequence[Field], versions: Sequence[str]) -> Field:
    """One record out of the same field read from several versions.

    `members` and `versions` run **oldest first** together, so a newer reading
    simply overwrites what an older one said -- which is the whole collapse
    rule, and the reason a value only 4.2 ever had survives it.
    """
    if not members:
        raise ValueError("a FIX field record needs at least one declaration")
    latest = members[-1]
    values: dict[str, FixFieldValue] = {}
    event_types: dict[str, EventType] = {}
    states: dict[str, State] = {}
    for member in members:
        for one in member.fix.enumerated:
            values[one.value] = merged_value(values.get(one.value), one)
        event_types.update(_event_types(_json_any(member.fix.get("event_types"))))
        states.update(_states(_json_any(member.fix.get("states"))))
    # Newest first, unlike the values: where a field is used is a list and not
    # a mapping, so the newest version's reading leads it rather than
    # correcting it key by key.
    used_in: list[str] = []
    components: list[str] = []
    for member in reversed(members):
        for name in _json_sequence(member.fix.get("msgtypes")):
            if name not in used_in:
                used_in.append(name)
        for name in _json_sequence(member.fix.get("components")):
            if name not in components:
                components.append(name)
    built = record_copy(latest)
    fix = built.fix
    fix.name = latest.name
    fix.versions = canonical_versions(versions)
    fix.pop("version", None)
    # The same refusals a stored document meets: a collapse is a write, and a
    # record no lookup could answer for must not reach a shard from either side.
    _refuse_record(fix.canonical, fix.tag, record_kind(built), fix.versions, event_types)
    fix.enumerated = tuple(values.values())
    fix.event_types = event_types
    fix.states = states
    fix.msgtypes = used_in
    fix.components = components
    return built


@dataclasses.dataclass(frozen=True)
class ComponentRecord(Convertible):
    """One component identity: one declaration, and the versions declaring it.

    The declaration is a `Field`, which is what a component *is*: a struct of
    its members, a list where one of them repeats, and an empty struct where
    it defers to a block declared elsewhere. One shape for a field, a group
    and a message, so nothing here has a second tree to keep in step.
    """

    name: str
    versions: tuple[str, ...] = ()
    #: This component as one Field, in wire order, references unexpanded.
    declaration: Field = dataclasses.field(default_factory=lambda: quickfix.block("", ()))
    aliases: tuple[Alias, ...] = ()

    def __post_init__(self) -> None:
        """Refuse a record no lookup could answer for."""
        if not str(self.name).strip():
            raise ValueError("a FIX component record has no name")
        if not self.versions:
            raise ValueError(f"FIX component {self.name!r} is declared for no version")
        object.__setattr__(self, "versions", canonical_versions(self.versions))
        if self.declaration.name != self.name:
            object.__setattr__(
                self, "declaration", dataclasses.replace(self.declaration, name=self.name)
            )

    @property
    def members(self) -> tuple[Field, ...]:
        """The declaration's members, in wire order."""
        return quickfix.members_of(self.declaration)

    @property
    def msg_type(self) -> str:
        """The message type this declaration defines, where it defines one."""
        return self.declaration.fix.msgtype

    @property
    def slug(self) -> str:
        """The file name this record is stored under."""
        return slug_of(self.name)

    @property
    def folded(self) -> str:
        """The canonical name as it is matched."""
        return fold(self.name)

    @property
    def newest(self) -> str:
        """The version this record's member tree was taken from."""
        return newest_of(self.versions)

    def declares(self, version: str) -> bool:
        """Whether this component holds for `version`."""
        return version in self.versions

    def spellings(self) -> tuple[str, ...]:
        """Every name this record answers to, in resolution order."""
        found: dict[str, str] = {self.folded: self.name}
        for alias in self.aliases:
            found.setdefault(alias.folded, alias.name)
        return tuple(found.values())

    def into_component(self, version: str = "") -> Field | None:
        """This component's declaration, or None for a version it has none for."""
        if version and not self.declares(version):
            return None
        return self.declaration

    def into_field(
        self,
        version: str,
        types: Mapping[str, Any] | None = None,
        components: Mapping[str, ComponentRecord] | None = None,
    ) -> Field | None:
        """This component's declaration as one Arrow field, or None for a version
        it has none for.

        The spec's own `required` decides nullability, which is the whole point:
        a member a message *must* carry is a column a reader must not have to
        null-check, and one it may omit is one they must. A repeating group is
        a list of its members, its entries never null; a referenced component
        is inlined where it sits, because that is where its fields arrive on
        the wire.
        """
        if not self.declares(version):
            return None
        members = _component_fields(self.declaration, types or {}, components or {}, frozenset())
        return Field(
            name=snake_of(self.name),
            dtype=pyarrow.struct([member.into_arrow_field() for member in members]),
            nullable=True,
            metadata={"fix:component": self.name, "fix:version": version},
        )

    def paths(self) -> dict[str, tuple[str, ...]]:
        """`{member name: the groups it sits under}`.

        The derived half of a component: the tree says it, but a consumer
        splitting a message wants it flat, and deriving it in two places is how
        two readers of one declaration come to disagree.
        """
        found: dict[str, tuple[str, ...]] = {}
        for member, path in quickfix.walk(self.declaration):
            found.setdefault(member.name, path)
        return found

    def delimiters(self) -> dict[tuple[str, ...], str]:
        """`{group path: the member that opens one entry}`.

        A repeating group's first member is its delimiter -- the standard says
        so, and it is what tells one entry from the next.
        """
        found: dict[tuple[str, ...], str] = {}
        for member, path in quickfix.walk(self.declaration):
            if quickfix.is_group(member):
                entry = quickfix.members_of(quickfix.entry_of(member))
                if entry:
                    found[(*path, member.name)] = entry[0].name
        return found

    def into_dict(self) -> dict[str, Any]:
        """The record as its file holds it.

        The declaration is a `Field`, so its document is the one every other
        declaration in this package writes -- a struct with its members, a
        list with its item -- and a component file reads like a contract file
        because it is one.
        """
        return _document(
            {
                "name": self.name,
                "versions": list(self.versions),
                "aliases": [alias.into_dict() for alias in self.aliases],
                "declaration": self.declaration.into_dict(),
            }
        )

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Self:
        """Build one record from its stored document."""
        declared = mapping.get("declaration")
        if not isinstance(declared, Mapping):
            raise TypeError("a FIX component record's declaration must be a document")
        return cls(
            name=str(mapping.get("name") or ""),
            versions=tuple(str(version) for version in mapping.get("versions") or ()),
            declaration=Field.from_dict(declared),
            aliases=_aliases_of(mapping.get("aliases")),
        )

    @classmethod
    def from_components(cls, declared: Sequence[Field], versions: Sequence[str]) -> Self:
        """One record out of the same component read from several versions.

        `declared` and `versions` run **oldest first** together, so the newest
        member tree is the one kept; members only an older version had are
        dropped, and the collapse says which.
        """
        if not declared:
            raise ValueError("a FIX component record needs at least one declaration")
        latest = declared[-1]
        return cls(
            name=latest.name,
            versions=tuple(versions),
            declaration=latest,
        )


def _component_fields(
    declared: Field,
    types: Mapping[str, Any],
    components: Mapping[str, ComponentRecord],
    seen: frozenset[str],
) -> list[Field]:
    """One level of a declaration as Arrow fields, `required` and all.

    The Arrow projection is where a reference *is* expanded: its fields
    arrive inline on the wire, so that is where they belong in a column. The
    stored declaration keeps the reference, because expanding it there turns
    three thousand members into a hundred and twenty thousand.
    """
    built: list[Field] = []
    for member in quickfix.members_of(declared):
        if quickfix.is_group(member):
            entry = quickfix.entry_of(member)
            item = _component_fields(entry, types, components, seen)
            built.append(
                Field(
                    name=snake_of(member.name),
                    dtype=pyarrow.list_(
                        Field(
                            name=snake_of(entry.name),
                            dtype=pyarrow.struct([one.into_arrow_field() for one in item]),
                            nullable=False,
                            metadata={"fix:name": entry.name},
                        ).into_arrow_field()
                    ),
                    nullable=member.nullable is not False,
                    metadata={"fix:name": member.name},
                )
            )
        elif quickfix.is_reference(member):
            key = fold(member.name)
            nested = components.get(key)
            if nested is None or key in seen:
                continue
            built.extend(_component_fields(nested.declaration, types, components, seen | {key}))
        else:
            built.append(
                Field(
                    name=snake_of(member.name),
                    dtype=types.get(member.name) or pyarrow.string(),
                    nullable=member.nullable is not False,
                    metadata={"fix:name": member.name},
                )
            )
    return built


# -- reading and writing the stored parts -------------------------------------


def _document(payload: Mapping[str, Any]) -> dict[str, Any]:
    """A stored document with its empty parts dropped, for a small clean diff."""
    return {key: value for key, value in payload.items() if value not in (None, "", [], {})}


def _aliases_of(declared: Any) -> tuple[Alias, ...]:
    """Stored aliases, deduplicated by what they fold to, in declared order."""
    found: dict[str, Alias] = {}
    for entry in declared or ():
        alias = Alias.from_dict(entry)
        found.setdefault(alias.folded, alias)
    return tuple(found.values())


def merged_value(held: FixFieldValue | None, fresh: FixFieldValue) -> FixFieldValue:
    """One value read twice: each half taken from the newer reading that has it.

    The prose and the spellings collapse independently, because a version
    that lists a value without writing it up still names it -- so a reading
    that says nothing about one half does not erase the other's.
    """
    if held is None:
        return fresh
    return dataclasses.replace(
        fresh,
        meaning=fresh.meaning or held.meaning,
        aliases=fresh.aliases or held.aliases,
    )


def folded_values(
    held: Sequence[FixFieldValue],
    fresh: Sequence[FixFieldValue],
    *,
    newest: Sequence[FixFieldValue],
) -> tuple[FixFieldValue, ...]:
    """Two readings of one field's values, with `newest` winning each half."""
    older = fresh if newest is held else held
    found = {one.value: one for one in older}
    for one in newest:
        found[one.value] = merged_value(found.get(one.value), one)
    return tuple(found.values())


def _event_types(mapping: Any) -> dict[str, EventType]:
    """One stored `{MsgType: EventType}` map, accepting enum names or codes."""
    if not isinstance(mapping, Mapping):
        return {}
    found: dict[str, EventType] = {}
    for key, value in mapping.items():
        try:
            found[str(key)] = _enum_value(EventType, value)
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"unknown EventType {value!r} for MsgType {key!r}") from error
    return found


def _states(mapping: Any) -> dict[str, State]:
    """One stored `{wire value: State}` map, accepting enum names or codes."""
    if not isinstance(mapping, Mapping):
        return {}
    found: dict[str, State] = {}
    for key, value in mapping.items():
        try:
            found[str(key)] = _enum_value(State, value)
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"unknown State {value!r} for value {key!r}") from error
    return found


def _enum_value(enum_type: Any, value: Any) -> Any:
    """Read an enum name, id, or the explicit pair stored in registry JSON.

    The name and the id must agree; an id no member stores is refused rather
    than read as a degraded member.
    """
    if isinstance(value, Mapping):
        if set(value) != {"name", "id"}:
            raise ValueError("an enum object needs name and id")
        name = value["name"]
        identifier = value["id"]
        if type(name) is not str or not name.strip():
            raise ValueError("an enum object needs a nonempty string name")
        if type(identifier) is not int:
            raise ValueError("an enum object needs an integer id")
        named = enum_type[name.upper()]
        if identifier != int(named):
            raise ValueError("an enum name and id disagree")
        return named
    parsed: Any = int(value) if isinstance(value, str) and value.isdigit() else value
    if isinstance(parsed, str):
        return enum_type[parsed.upper()]
    member = enum_type(parsed)
    if int(member) != int(parsed):
        raise ValueError("no member stores this id")
    return member


def _enum_document(mapping: Mapping[str, Any]) -> dict[str, dict[str, str | int]]:
    """Enum mappings with a readable name and stable integer id."""
    return {str(key): {"name": value.name, "id": int(value)} for key, value in mapping.items()}


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def _json_any(value: str | None) -> Any:
    try:
        return json.loads(value or "null")
    except (TypeError, ValueError):
        return None


def _json_mapping(value: str | None) -> dict[str, str]:
    decoded = _json_any(value)
    return (
        {str(key): str(item) for key, item in decoded.items()} if isinstance(decoded, dict) else {}
    )


def _json_sequence(value: str | None) -> list[str]:
    decoded = _json_any(value)
    return [str(item) for item in decoded] if isinstance(decoded, list) else []
