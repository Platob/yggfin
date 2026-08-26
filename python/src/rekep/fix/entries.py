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
import functools
import json
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Any, Self

import pyarrow

from rekep.convert import Convertible
from rekep.enums import EventType, State
from rekep.fields import Field
from rekep.fix.fields import fix_field
from rekep.fix.quickfix import SpecComponent, SpecComponentRef, SpecGroup, SpecMember

#: The version list of a record that holds for every version, which is what a
#: field outside the standard has: a bridge renders `TECH.CLIENTID` the same
#: way whichever FIX version the session negotiated.
ANY_VERSION = "*"

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
    "value_names",
    "event_types",
    "states",
    "encoded",
    "decoded",
    "used_in",
    "components",
    "aliases",
)

_SLUG_SPLIT = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", re.ASCII)
_SLUG_DROP = re.compile(r"[^a-z0-9]+", re.ASCII)

#: What an encoded key keeps: nothing that is not a letter or a digit. This
#: is what makes `ORDER_SUBMISSION_TIME` and `Order Submission Time` one key --
#: lowercasing alone does not, because of the underscores and the spaces.
_ENCODED_DROP = re.compile(r"[^a-z0-9]+", re.ASCII)

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


def fold(name: str) -> str:
    """A name as it is matched: case, and nothing else.

    Separators are part of a name here. Dropping them made `PartyID` and
    `Part_yid` one key and, worse, silently merged two identities a store
    holds apart -- a match a registry cannot then tell from a real collision.
    A spelling that differs by more than case is an alias, which is a thing
    the store records.
    """
    return str(name).strip().lower()


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


def canonical_versions(versions: Iterable[str]) -> tuple[str, ...]:
    """A record's version list in canonical order: oldest first, transport last."""
    return tuple(sorted(dict.fromkeys(versions), key=version_rank))


def newest_of(versions: Iterable[str]) -> str:
    """Which version owns a record's reading: the newest *application* one.

    `FIXT1.1` only wins where nothing else declares the field, which is what
    keeps a session-layer reading off the application fields it merely carries.
    """
    found = tuple(versions)
    if not found:
        raise ValueError("a FIX registry record is declared for no version")
    return max(found, key=newest_rank)


def newest_rank(version: str) -> tuple[int, ...]:
    """`version_rank` with the transport ranked below every application version."""
    transport, *numbers = version_rank(version)
    return (1 - transport, *numbers)


@functools.lru_cache(maxsize=4096)
def encoded_key(text: str) -> str:
    """A value or its name as `encoded` keys it: casefolded letters and digits.

    `ORDER_SUBMISSION_TIME`, `Order Submission Time` and `ordersubmissiontime`
    are one key; plain lowercasing leaves three.

    Memoized because a feed asks the same few hundred spellings of it on every
    single message -- a hundred thousand calls over two thousand lines, of
    which two hundred were distinct (benchmarks/bench_market.py).
    """
    return _ENCODED_DROP.sub("", str(text).casefold())


def encodings_of(
    values: Mapping[str, str], value_names: Mapping[str, str]
) -> tuple[dict[str, str], dict[str, list[str]]]:
    """`({normalized spelling: value}, {dropped spelling: the values claiming it})`.

    Built from the prose, the symbols and the raw values themselves, so a
    caller has one lookup path rather than two. A spelling two values both
    normalize to is emitted for neither: an ambiguous translation that silently
    picks one is worse than none, and the lookup falls through to the raw value.
    """
    claimed: dict[str, list[str]] = {}
    for source in (values, value_names):
        for value, spelled in source.items():
            _claim(claimed, encoded_key(spelled), str(value))
    for value in (*values, *value_names):
        _claim(claimed, encoded_key(value), str(value))
    found = {key: owners[0] for key, owners in claimed.items() if key and len(owners) == 1}
    collisions = {key: owners for key, owners in claimed.items() if key and len(owners) > 1}
    return found, collisions


def decodings_of(values: Mapping[str, str], value_names: Mapping[str, str]) -> dict[str, str]:
    """Wire values to one deterministic normalized string each."""
    decoded: dict[str, str] = {}
    for value in dict.fromkeys((*values, *value_names)):
        source = value_names.get(value) or values.get(value) or value
        decoded[str(value)] = encoded_key(source) or str(value)
    return decoded


def _claim(claimed: dict[str, list[str]], key: str, value: str) -> None:
    owners = claimed.setdefault(key, [])
    if value not in owners:
        owners.append(value)


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


def _kind_of(stored: Any) -> str:
    """One stored `kind`, under whatever name the store spelled it."""
    kind = str(stored or STANDARD)
    return _RENAMED_KINDS.get(kind, kind)


@dataclasses.dataclass(frozen=True)
class FieldEntry(Convertible):
    """One field identity: one tag, one reading, and the versions declaring it."""

    name: str
    tag: int | None = None
    kind: str = STANDARD
    #: Every version that declares this field, in canonical order.
    versions: tuple[str, ...] = ()
    type: str = ""
    description: str = ""
    #: `{value: prose}` from the dictionary and `{value: SYMBOL}` from the
    #: spec, unioned across versions so a value only 4.2 ever had still parses.
    values: Mapping[str, str] = dataclasses.field(default_factory=dict)
    value_names: Mapping[str, str] = dataclasses.field(default_factory=dict)
    #: `{MsgType: EventType}` for classifying a message before transcription.
    event_types: Mapping[str, EventType] = dataclasses.field(default_factory=dict)
    #: `{wire value: State}` for this field's market lifecycle meaning.
    states: Mapping[str, State] = dataclasses.field(default_factory=dict)
    #: `{normalized spelling: value}`, so `Side=Buy` and `Side=BUY` both reach
    #: `1`. Generated from `values` and `value_names`; a hand-written entry
    #: here survives a rebuild, because the generated map is the default and
    #: not the whole map. Read through `encode`, off the record -- a
    #: projected `Field` carries the values it was built from, not this.
    encoded: Mapping[str, str] = dataclasses.field(default_factory=dict)
    #: `{value: normalized spelling}` for simple string decoding.
    decoded: Mapping[str, str] = dataclasses.field(default_factory=dict)
    used_in: tuple[str, ...] = ()
    components: tuple[str, ...] = ()
    note: str = ""
    aliases: tuple[Alias, ...] = ()
    #: The parsed-log column this field is lifted into. Only a field the log
    #: declares a column for carries one; everything else stays in the pairs.
    column: str = ""

    def __post_init__(self) -> None:
        """Refuse a record no lookup could answer for, and fill its codecs."""
        if not str(self.name).strip():
            raise ValueError("a FIX field record has no name")
        if self.kind not in KINDS:
            raise ValueError(f"unknown FIX field kind {self.kind!r}; one of {sorted(KINDS)}")
        if self.kind == STANDARD and not self.tag:
            raise ValueError(f"standard FIX field {self.name!r} has no tag")
        if self.kind == NAMESPACE and self.tag:
            raise ValueError(f"namespaced FIX field {self.name!r} must not claim tag {self.tag}")
        if not self.versions:
            raise ValueError(f"FIX field {self.name!r} is declared for no version")
        if self.event_types and self.tag != 35:
            raise ValueError("FIX event types belong to MsgType <35>")
        object.__setattr__(self, "versions", canonical_versions(self.versions))
        object.__setattr__(self, "event_types", _event_types(self.event_types))
        object.__setattr__(self, "states", _states(self.states))
        generated, _ = encodings_of(self.values, self.value_names)
        object.__setattr__(self, "encoded", {**generated, **dict(self.encoded)})
        object.__setattr__(
            self, "decoded", {**decodings_of(self.values, self.value_names), **dict(self.decoded)}
        )

    @property
    def key(self) -> int | str:
        """What this identity is stored under: its tag, or its folded name."""
        return int(self.tag) if self.tag is not None else self.folded

    @property
    def folded(self) -> str:
        """The canonical name as it is matched."""
        return fold(self.name)

    @property
    def newest(self) -> str:
        """The version this record's reading was taken from."""
        return newest_of(self.versions)

    def declares(self, version: str) -> bool:
        """Whether this field holds for `version`, wildcard included."""
        return version in self.versions or ANY_VERSION in self.versions

    def spellings(self) -> tuple[str, ...]:
        """Every name this record answers to: canonical first, then its aliases.

        In resolution order and deduplicated by fold, so a caller walking it is
        walking the precedence `FixRegistry` applies.
        """
        found: dict[str, str] = {self.folded: self.name}
        for alias in self.aliases:
            found.setdefault(alias.folded, alias.name)
        return tuple(found.values())

    def encode(self, value: str) -> str:
        """The FIX value a spelling names, or the spelling itself when none does."""
        return self.encoded.get(encoded_key(value), str(value))

    def decode(self, value: str) -> str:
        """The normalized name of a FIX value, or the value when none is known."""
        return self.decoded.get(str(value), str(value))

    def meaning(self, value: str) -> str | None:
        """What one value means, where this field enumerates its values.

        The other direction from `translate`, and the prose before the symbol:
        `Side <54>` value `1` is "Buy" for a person and `BUY` for a program,
        and this is read by people. None where the field enumerates nothing or
        no version of it defines the value -- which is honest, and better than
        echoing back a code the dictionary does not know.

        Read off the record and never stored beside the value: it is one
        string per enumerated field per row, derivable from the dictionary
        the row is read under.
        """
        spelled = str(value)
        return self.values.get(spelled) or self.value_names.get(spelled)

    def event_type(self, value: Any) -> EventType:
        """The configured kind of one MsgType, MISC when known, else UNKNOWN."""
        spelled = str(value) if value is not None else ""
        configured = self.event_types.get(spelled)
        if configured is not None:
            return configured
        if spelled in self.values or spelled in self.value_names:
            return EventType.MISC
        return EventType.UNKNOWN

    def into_field(self, version: str) -> Field | None:
        """This field as `version` declares it, or None when that version has none.

        The reading is the same for every version that declares it -- that is
        what one record per identity means -- and only `fix:version` differs,
        because a caller still has to know which version it asked about.
        """
        if not self.declares(version):
            return None
        built = fix_field(
            self.name,
            int(self.tag or 0),
            self.type or None,
            description=self.description or None,
            version=version,
            values=self.values,
        )
        if self.tag is None:
            # A namespaced field has no tag, and a `0` where one goes would
            # collide with every other one of them in a tag index.
            del built.fix["tag"]
            built.fix["kind"] = NAMESPACE
        if self.column:
            built.fix["column"] = self.column
        if self.note:
            built.fix["note"] = self.note
        for key, value in (
            ("value_names", dict(self.value_names)),
            ("event_types", {key: int(value) for key, value in self.event_types.items()}),
            ("states", {key: int(value) for key, value in self.states.items()}),
            ("msgtypes", list(self.used_in)),
            ("components", list(self.components)),
        ):
            if value:
                built.fix[key] = _json(value)
        return built

    def into_fields(self, order: Sequence[str]) -> list[Field]:
        """This field as every version in `order` declares it, in that order."""
        found = [self.into_field(version) for version in order if self.declares(version)]
        if not found and ANY_VERSION in self.versions:
            found = [self.into_field(ANY_VERSION)]
        return [member for member in found if member is not None]

    def into_merged(self, order: Sequence[str] = ()) -> Field:
        """The declaration `scalar()` hands out: one identity, every version of it.

        `order` names the versions newest first, which is the order
        `fix:versions` carries them in; the record's own canonical order is
        used when nothing is named.
        """
        listed = [version for version in order if self.declares(version)] or list(self.versions)
        built = self.into_field(listed[0])
        if built is None:  # pragma: no cover - `declares` selected these versions
            raise KeyError(f"FIX field {self.name!r} declares none of {list(order)}")
        built.fix["name"] = self.name
        built.fix["versions"] = _json(listed)
        if self.aliases:
            built.fix["aliases"] = _json([alias.into_dict() for alias in self.aliases])
        return built

    def into_dict(self) -> dict[str, Any]:
        """The record as its shard holds it."""
        return _document(
            {
                "name": self.name,
                "tag": self.tag,
                "kind": "" if self.kind == STANDARD else self.kind,
                "column": self.column,
                "note": self.note,
                "type": self.type,
                "description": self.description,
                "versions": list(self.versions),
                "values": dict(self.values),
                "value_names": dict(self.value_names),
                "event_types": _enum_document(self.event_types),
                "states": _enum_document(self.states),
                "encoded": dict(self.encoded),
                "decoded": dict(self.decoded),
                "used_in": list(self.used_in),
                "components": list(self.components),
                "aliases": [alias.into_dict() for alias in self.aliases],
            }
        )

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Self:
        """Build one record from its stored document."""
        unknown = sorted(set(mapping) - set(RECORD_KEYS))
        if unknown:
            raise ValueError(f"a FIX field record declares unknown {unknown}")
        tag = mapping.get("tag")
        return cls(
            name=str(mapping.get("name") or ""),
            tag=int(tag) if tag is not None else None,
            kind=_kind_of(mapping.get("kind")),
            versions=tuple(str(version) for version in mapping.get("versions") or ()),
            type=str(mapping.get("type") or ""),
            description=str(mapping.get("description") or ""),
            values=_strings(mapping.get("values")),
            value_names=_strings(mapping.get("value_names")),
            event_types=_event_types(mapping.get("event_types")),
            states=_states(mapping.get("states")),
            encoded=_strings(mapping.get("encoded")),
            decoded=_strings(mapping.get("decoded")),
            used_in=tuple(str(name) for name in mapping.get("used_in") or ()),
            components=tuple(str(name) for name in mapping.get("components") or ()),
            note=str(mapping.get("note") or ""),
            aliases=_aliases_of(mapping.get("aliases")),
            column=str(mapping.get("column") or ""),
        )

    @classmethod
    def from_fields(cls, members: Sequence[Field], versions: Sequence[str]) -> Self:
        """One record out of the same field read from several versions.

        `members` and `versions` run **oldest first** together, so a newer
        reading simply overwrites what an older one said -- which is the whole
        collapse rule, and the reason a value only 4.2 ever had survives it.
        """
        if not members:
            raise ValueError("a FIX field record needs at least one declaration")
        latest = members[-1]
        tag = latest.fix.get("tag")
        values: dict[str, str] = {}
        value_names: dict[str, str] = {}
        event_types: dict[str, EventType] = {}
        states: dict[str, State] = {}
        for member in members:
            values.update(_json_mapping(member.fix.get("values")))
            value_names.update(_json_mapping(member.fix.get("value_names")))
            event_types.update(_event_types(_json_any(member.fix.get("event_types"))))
            states.update(_states(_json_any(member.fix.get("states"))))
        # Newest first, unlike the values: where a field is used is a list and
        # not a mapping, so the newest version's reading leads it rather than
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
        return cls(
            name=latest.name,
            tag=int(tag) if tag else None,
            kind=STANDARD if tag else NAMESPACE,
            versions=tuple(versions),
            type=str(latest.fix.get("type") or ""),
            description=latest.description,
            values=values,
            value_names=value_names,
            event_types=event_types,
            states=states,
            used_in=tuple(used_in),
            components=tuple(components),
            note=str(latest.fix.get("note") or ""),
            column=str(latest.fix.get("column") or ""),
        )


@dataclasses.dataclass(frozen=True)
class ComponentEntry(Convertible):
    """One component identity: one member tree, and the versions declaring it."""

    name: str
    versions: tuple[str, ...] = ()
    members: tuple[SpecMember, ...] = ()
    #: The message type this component defines, where it defines one -- `"D"`,
    #: `"8"`. Empty, and absent from the document, for a reusable component.
    msg_type: str = ""
    aliases: tuple[Alias, ...] = ()

    def __post_init__(self) -> None:
        """Refuse a record no lookup could answer for."""
        if not str(self.name).strip():
            raise ValueError("a FIX component record has no name")
        if not self.versions:
            raise ValueError(f"FIX component {self.name!r} is declared for no version")
        object.__setattr__(self, "versions", canonical_versions(self.versions))
        object.__setattr__(self, "members", tuple(self.members))

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

    def into_component(self, version: str = "") -> SpecComponent | None:
        """This component's declaration, or None for a version it has none for."""
        if version and not self.declares(version):
            return None
        return SpecComponent(name=self.name, members=self.members, msg_type=self.msg_type)

    def into_field(
        self,
        version: str,
        types: Mapping[str, Any] | None = None,
        components: Mapping[str, ComponentEntry] | None = None,
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
        members = _component_fields(self.members, types or {}, components or {}, frozenset())
        return Field(
            name=snake_of(self.name),
            arrow_type=pyarrow.struct([member.into_arrow_field() for member in members]),
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
        for member, path in _walk(self.members, ()):
            found.setdefault(member.name, path)
        return found

    def delimiters(self) -> dict[tuple[str, ...], str]:
        """`{group path: the member that opens one entry}`.

        A repeating group's first member is its delimiter -- the standard says
        so, and it is what tells one entry from the next.
        """
        found: dict[tuple[str, ...], str] = {}
        for member, path in _walk(self.members, ()):
            if isinstance(member, SpecGroup) and member.members:
                found[(*path, member.name)] = member.members[0].name
        return found

    def into_dict(self) -> dict[str, Any]:
        """The record as its file holds it."""
        return _document(
            {
                "name": self.name,
                "msg_type": self.msg_type,
                "versions": list(self.versions),
                "aliases": [alias.into_dict() for alias in self.aliases],
                "members": [member.into_dict() for member in self.members],
            }
        )

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Self:
        """Build one record from its stored document."""
        members = mapping.get("members", ())
        if not isinstance(members, list | tuple):
            raise TypeError("a FIX component record's members must be a sequence")
        return cls(
            name=str(mapping.get("name") or ""),
            versions=tuple(str(version) for version in mapping.get("versions") or ()),
            members=tuple(SpecMember.from_dict(member) for member in members),
            msg_type=str(mapping.get("msg_type") or ""),
            aliases=_aliases_of(mapping.get("aliases")),
        )

    @classmethod
    def from_components(cls, declared: Sequence[SpecComponent], versions: Sequence[str]) -> Self:
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
            members=latest.members,
            msg_type=latest.msg_type,
        )


def _component_fields(
    members: Sequence[SpecMember],
    types: Mapping[str, Any],
    components: Mapping[str, ComponentEntry],
    seen: frozenset[str],
) -> list[Field]:
    """One level of a component tree as Arrow fields, `required` and all."""
    built: list[Field] = []
    for member in members:
        if isinstance(member, SpecGroup):
            item = _component_fields(member.members, types, components, seen)
            built.append(
                Field(
                    name=snake_of(member.name),
                    arrow_type=pyarrow.list_(
                        pyarrow.field(
                            "item",
                            pyarrow.struct([one.into_arrow_field() for one in item]),
                            nullable=False,
                        )
                    ),
                    nullable=not member.required,
                    metadata={"fix:name": member.name},
                )
            )
        elif isinstance(member, SpecComponentRef):
            key = fold(member.name)
            nested = components.get(key)
            if nested is None or key in seen:
                continue
            built.extend(_component_fields(nested.members, types, components, seen | {key}))
        else:
            arrow_type = types.get(member.name) or pyarrow.string()
            built.append(
                Field(
                    name=snake_of(member.name),
                    arrow_type=arrow_type,
                    nullable=not member.required,
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


def _strings(mapping: Any) -> dict[str, str]:
    """One stored `{value: text}` map, with both halves read as text."""
    if not isinstance(mapping, Mapping):
        return {}
    return {str(key): str(value) for key, value in mapping.items()}


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
    """Read an enum name, id, or the explicit pair stored in registry JSON."""
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
        identified = enum_type(identifier)
        if named is not identified:
            raise ValueError("an enum name and id disagree")
        return named
    parsed: Any = int(value) if isinstance(value, str) and value.isdigit() else value
    return enum_type[parsed.upper()] if isinstance(parsed, str) else enum_type(parsed)


def _enum_document(mapping: Mapping[str, Any]) -> dict[str, dict[str, str | int]]:
    """Enum mappings with a readable name and stable integer id."""
    return {str(key): {"name": value.name, "id": int(value)} for key, value in mapping.items()}


def _walk(
    members: Iterable[SpecMember], path: tuple[str, ...]
) -> Iterator[tuple[SpecMember, tuple[str, ...]]]:
    """Every member under `members`, with the groups it sits inside."""
    for member in members:
        yield member, path
        nested = getattr(member, "members", ())
        if nested:
            yield from _walk(nested, (*path, member.name))


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
