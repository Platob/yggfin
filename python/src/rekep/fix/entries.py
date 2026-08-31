"""One record per field or component *identity*, cross-version by nature.

A field's reading is not a property of a FIX version: one tag means one thing,
and a set of versions declare it. So a record holds that one reading -- the
name, the datatype, the prose, the enumerated values -- beside `versions`, the
list of versions that declare it. Where two versions disagree the newest one
wins and the collapse is reported, which is the only judgement the shape asks
for.

The same shape holds fields FIX never numbered -- a bridge's rendered
`AMON.isincode`, a vendor's `TECH.CLIENTID` -- with no tag and `ANY_VERSION`
for their version list. They are aliased and looked up exactly like numbered
tags, rather than living in a second incompatible mapping.
"""

from __future__ import annotations

import dataclasses
import json
import re
from collections.abc import Mapping, Sequence
from typing import Any, Self

import pyarrow

from rekep.convert import Convertible
from rekep.entries import fold
from rekep.enums import EventType, State
from rekep.fields import Field, column_name
from rekep.fields.metadata import (
    ANY_VERSION,
    Alias,
    FixFieldValue,
    canonical_versions,
    newest_of,
)
from rekep.fix import quickfix

#: What a field record is: a numbered FIX tag, or a name a renderer prints with
#: no tag behind it. Derived from the tag by `record_kind` rather than stored,
#: because a record carrying both could contradict itself; these two names are
#: what a projection or a report selects on.
STANDARD = "standard"
NAMESPACE = "namespace"

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

    Dots and case become underscores, so `AMON.isincode` is `amon_isincode`
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


# -- a field record, which is a field ---------------------------------------
#
# A record used to be a dataclass beside the `Field` it projected into. It is
# the field now: everything a shard stores about an identity has a `fix:` key,
# so the record and the declaration are one object and nothing is written
# twice. What is left here is the two directions the store needs -- the
# document it holds on disk, and the version a caller asked about.


def refuse_record(record: Field) -> Field:
    """Refuse a field record no lookup could answer for; return it otherwise.

    Whether a record is standard or namespaced is the presence of its tag,
    so the two used to be checked against each other and now cannot disagree.
    What is left is what a document can still get wrong.
    """
    fix = record.fix
    name = fix.canonical
    if not name.strip():
        raise ValueError("a FIX field record has no name")
    if not fix.versions:
        raise ValueError(f"FIX field {name!r} is declared for no version")
    _refuse_source_metadata(fix, name)
    # The one tag written down rather than asked for. This runs while the
    # store is being read, and asking the registry which tag `MsgType` is
    # would re-enter the read that is calling it.
    if fix.event_types and fix.tag != 35:
        raise ValueError("FIX event types belong to MsgType <35>")
    return record


def _refuse_source_metadata(fix: Any, name: str) -> None:
    """Refuse source provenance that cannot attribute the field parts it names."""
    try:
        sources = json.loads(fix.get("sources") or "[]")
        origins = json.loads(fix.get("origins") or "{}")
    except (TypeError, ValueError) as error:
        raise ValueError(f"FIX field {name!r} has invalid source metadata") from error
    if (
        not isinstance(sources, list)
        or any(type(source) is not str or not source.strip() for source in sources)
        or len(set(sources)) != len(sources)
    ):
        raise ValueError(f"FIX field {name!r} needs distinct source names")
    primary = fix.source
    if bool(primary) != bool(sources) or (sources and sources[0] != primary):
        raise ValueError(f"FIX field {name!r} needs its primary source first")
    if not isinstance(origins, Mapping):
        raise ValueError(f"FIX field {name!r} has invalid source origins")
    for part, origin in origins.items():
        if type(part) is not str or not part:
            raise ValueError(f"FIX field {name!r} has invalid source origins")
        if isinstance(origin, Mapping):
            if any(type(key) is not str for key in origin):
                raise ValueError(f"FIX field {name!r} has invalid source origins")
            stated = origin.values()
        else:
            stated = (origin,)
        if any(type(source) is not str or source not in sources for source in stated):
            raise ValueError(f"FIX field {name!r} has an unknown source origin")


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
    added = next((member for member in reversed(members) if member.fix.added), None)
    event_types: dict[str, EventType] = {}
    states: dict[str, State] = {}
    for member in members:
        event_types.update(member.fix.event_types)
        states.update(member.fix.states)
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
    if added is not None:
        fix.added = added.fix.added
    # The same refusals a stored document meets: a collapse is a write, and a
    # record no lookup could answer for must not reach a shard from either side.
    fix.event_types = event_types
    refuse_record(built)
    values, origins = folded_field_values(members)
    fix.enumerated = values
    scalar_origins = {
        part: source
        for part, source in latest.fix.origins.items()
        if part not in ("values", "aliases", "added")
    }
    if added is not None and (source := added.fix.source_of("added")):
        scalar_origins["added"] = source
    fix.origins = {**scalar_origins, **origins}
    fix.sources = tuple(
        dict.fromkeys(source for member in reversed(members) for source in member.fix.sources)
    )
    fix.source = fix.sources[0] if fix.sources else latest.fix.source
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
        """The message type this declaration defines, where it defines one.

        A message carries the code it arrives under; a reusable block leaves
        it empty, which is the one difference between the two declarations.
        """
        return self.declaration.fix.msgtype

    @property
    def msgtypes(self) -> tuple[str, ...]:
        """The messages that carry this block, as a field's `used_in` reads.

        Derived on the collapse rather than scraped, and empty for a message:
        a message is what carries a block, not something a block is carried by.
        """
        return self.declaration.fix.msgtypes

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
        """Whether this component holds for `version`.

        `ANY_VERSION` answers for every version, exactly as a field record's
        does: a block declared outside the standard's versioning is declared
        for whatever a caller asks about.
        """
        return version in self.versions or ANY_VERSION in self.versions

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
        fields: Mapping[str, Field] | None = None,
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
        members = _component_fields(self.declaration, fields or {}, components or {}, frozenset())
        return Field(
            name=column_name(self.name),
            dtype=pyarrow.struct([member.into_arrow_field() for member in members]),
            nullable=True,
            metadata={
                "fix:component": self.name,
                "fix:version": version,
            },
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
    fields: Mapping[str, Field],
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
        field = fields.get(column_name(member.name))
        name = column_name(member.name)
        if field is not None and field.fix.column:
            name = field.fix.column
        if quickfix.is_group(member):
            entry = quickfix.entry_of(member)
            item = _component_fields(entry, fields, components, seen)
            built.append(
                Field(
                    name=name,
                    dtype=pyarrow.list_(
                        Field(
                            name=column_name(entry.name),
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
            built.extend(_component_fields(nested.declaration, fields, components, seen | {key}))
        else:
            built.append(
                Field(
                    name=name,
                    dtype=field.dtype if field is not None else pyarrow.string(),
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


def folded_field_values(
    members: Sequence[Field],
) -> tuple[tuple[FixFieldValue, ...], dict[str, dict[str, str]]]:
    """Per-version values and their sources, with the newest stated half winning."""
    values: dict[str, FixFieldValue] = {}
    origins: dict[str, dict[str, str]] = {"values": {}, "aliases": {}}
    for member in members:
        for fresh in member.fix.enumerated:
            values[fresh.value] = merged_value(values.get(fresh.value), fresh)
            for part, stated in (("values", fresh.meaning), ("aliases", fresh.aliases)):
                if not stated:
                    continue
                source = member.fix.source_of(part, fresh.value)
                if source:
                    origins[part][fresh.value] = source
                else:
                    origins[part].pop(fresh.value, None)
    return tuple(values.values()), {part: found for part, found in origins.items() if found}


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
