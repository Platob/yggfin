"""One entry per field or component *identity*, holding every version of it.

The dictionary used to be stored one file per FIX version, each listing every
field that version declares. Asking "does PartyRole exist, and how does it
differ across versions" then meant reading nine files and diffing them by hand,
and adding one field meant touching every version file that mentions it.

An entry inverts that: one file per identity, holding the canonical name, the
tag, the names it is also known by, and a map of version to the parts that
differ. "How does this field change across versions" is a single file, and
adding a field or a newly observed alias is one small reviewable edit.

The same shape holds fields FIX never numbered -- a bridge's rendered
`AMON.ISINCODE`, a vendor's `TECH.CLIENTID` -- with no tag and a variant under
`ANY_VERSION`. They are versioned, merged, aliased and looked up exactly like
numbered tags, rather than living in a second incompatible mapping.
"""

from __future__ import annotations

import dataclasses
import json
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Any, Self

from rekep.convert import Convertible
from rekep.fields import Field
from rekep.fix.fields import fix_field
from rekep.fix.quickfix import SpecComponent, SpecGroup, SpecMember

#: The version key of a variant that holds for every version, which is what a
#: field outside the standard has: a bridge renders `TECH.CLIENTID` the same
#: way whichever FIX version the session negotiated.
ANY_VERSION = "*"

#: What a field entry is: a numbered FIX tag, or a name a renderer prints with
#: no tag behind it. Stored so a reader never has to infer it from a null tag,
#: and so a projection or a report can select one kind without guessing.
STANDARD = "standard"
NAMESPACE = "namespace"
KINDS: frozenset[str] = frozenset({STANDARD, NAMESPACE})

#: What `NAMESPACE` used to be called, still read out of a store somebody's
#: cache already holds. Written back under the current name, so a store
#: converts itself the first time anything rewrites it.
_RENAMED_KINDS: Mapping[str, str] = MappingProxyType({"vendor": NAMESPACE})

#: Per-version parts of a field a variant may carry. Everything else about a
#: field belongs to its identity and cannot differ between versions.
VARIANT_KEYS: tuple[str, ...] = (
    "name",
    "tag",
    "type",
    "description",
    "values",
    "value_names",
    "used_in",
    "note",
)

#: `fix:` keys a stored variant holds as a document and a `Field` holds as
#: JSON text, because that is how `fix_field` writes them.
_JSON_KEYS: tuple[str, ...] = ("values", "value_names", "used_in")

_SLUG_SPLIT = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", re.ASCII)
_SLUG_DROP = re.compile(r"[^a-z0-9]+", re.ASCII)

#: A name is matched folded: case, and the separators a renderer's convention
#: adds (`party_role`, `PARTY-ROLE`, `Party Role` are one name). The same fold
#: `rekep.fix.message` applies to a rendered key, so a name resolves here
#: exactly as it resolves there.
_FOLD_DROP = re.compile(r"[ _\-]+", re.ASCII)


def slug_of(name: str) -> str:
    """The file name one identity is stored under: `PartyRole` -> `party_role`.

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


def fold(name: str) -> str:
    """A name as it is matched: lowercased, with renderer separators dropped."""
    return _FOLD_DROP.sub("", str(name).strip().lower())


@dataclasses.dataclass(frozen=True)
class Alias(Convertible):
    """Another name one identity has been seen under, and where that was seen.

    Provenance rather than a bare string, because an alias earned from a real
    capture and one typed in by hand are not the same evidence -- and a near
    miss counted forty times in one bridge is a different proposition from one
    counted once.
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
    """One field identity, and every version's reading of it."""

    name: str
    tag: int | None = None
    kind: str = STANDARD
    aliases: tuple[Alias, ...] = ()
    #: `{version: {variant key: value}}`, newest version first once stored.
    variants: Mapping[str, Mapping[str, Any]] = dataclasses.field(default_factory=dict)
    #: The parsed-log column this field is lifted into. Only a field the log
    #: declares a column for carries one; everything else stays in the pairs.
    column: str = ""

    def __post_init__(self) -> None:
        """Refuse an entry no lookup could ever answer for."""
        if not str(self.name).strip():
            raise ValueError("a FIX field entry has no name")
        if self.kind not in KINDS:
            raise ValueError(f"unknown FIX field kind {self.kind!r}; one of {sorted(KINDS)}")
        if self.kind == STANDARD and not self.tag:
            raise ValueError(f"standard FIX field {self.name!r} has no tag")
        if self.kind == NAMESPACE and self.tag:
            raise ValueError(f"namespaced FIX field {self.name!r} must not claim tag {self.tag}")
        if not self.variants:
            raise ValueError(f"FIX field {self.name!r} is declared for no version")
        for version, variant in self.variants.items():
            unknown = sorted(set(variant) - set(VARIANT_KEYS))
            if unknown:
                raise ValueError(f"FIX field {self.name!r} {version} declares unknown {unknown}")

    @property
    def slug(self) -> str:
        """The file name this entry is stored under."""
        return slug_of(self.name)

    @property
    def folded(self) -> str:
        """The canonical name as it is matched."""
        return fold(self.name)

    @property
    def versions(self) -> tuple[str, ...]:
        """Every version this field is declared for, in stored order."""
        return tuple(self.variants)

    def names(self) -> dict[str, str]:
        """`{version: the name that version spells}`, canonical where they agree."""
        return {
            version: str(variant.get("name") or self.name)
            for version, variant in self.variants.items()
        }

    def tags(self) -> dict[str, int]:
        """`{version: tag}` for a numbered field; empty for a namespaced one."""
        if self.tag is None:
            return {}
        return {
            version: int(variant.get("tag") or self.tag)
            for version, variant in self.variants.items()
        }

    def spellings(self) -> tuple[str, ...]:
        """Every name this entry answers to: canonical, per-version, then aliases.

        In resolution order and deduplicated by fold, so a caller walking it is
        walking the precedence `FixRegistry` applies.
        """
        found: dict[str, str] = {self.folded: self.name}
        for spelled in self.names().values():
            found.setdefault(fold(spelled), spelled)
        for alias in self.aliases:
            found.setdefault(alias.folded, alias.name)
        return tuple(found.values())

    def variant(self, version: str) -> Mapping[str, Any] | None:
        """One version's reading, or the wildcard a namespaced field carries."""
        found = self.variants.get(version)
        return self.variants.get(ANY_VERSION) if found is None else found

    def into_field(self, version: str) -> Field | None:
        """One version's declaration as the `Field` the registry hands out.

        None when this field says nothing about that version, which is not the
        same as having no type there: a caller must be able to tell a field 4.2
        never had from one it had and nobody wrote up.
        """
        found = self.variant(version)
        if found is None:
            return None
        built = fix_field(
            str(found.get("name") or self.name),
            int(found.get("tag") or self.tag or 0),
            found.get("type"),
            description=found.get("description"),
            version=version,
            values=found.get("values"),
        )
        if self.tag is None:
            # A namespaced field has no tag, and a `0` where one goes would
            # collide with every other one of them in a tag index.
            del built.fix["tag"]
            built.fix["kind"] = NAMESPACE
        if self.column:
            built.fix["column"] = self.column
        for key in ("value_names", "used_in"):
            value = found.get(key)
            if value:
                built.fix[key] = json.dumps(value, separators=(",", ":"))
        note = found.get("note")
        if note:
            built.fix["note"] = str(note)
        return built

    def into_fields(self, order: Sequence[str]) -> list[Field]:
        """This field as every version in `order` declares it, in that order."""
        found = [self.into_field(version) for version in order if version in self.variants]
        if not found and ANY_VERSION in self.variants:
            found = [self.into_field(ANY_VERSION)]
        return [member for member in found if member is not None]

    def into_dict(self) -> dict[str, Any]:
        """The entry as its file holds it."""
        return _document(
            {
                "name": self.name,
                "tag": self.tag,
                "kind": "" if self.kind == STANDARD else self.kind,
                "column": self.column,
                "aliases": [alias.into_dict() for alias in self.aliases],
                "versions": {
                    version: _document(variant) for version, variant in self.variants.items()
                },
            }
        )

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Self:
        """Build one entry from its stored document."""
        versions = mapping.get("versions") or {}
        if not isinstance(versions, Mapping):
            raise TypeError("a FIX field entry's versions must be a mapping")
        tag = mapping.get("tag")
        return cls(
            name=str(mapping.get("name") or ""),
            tag=int(tag) if tag is not None else None,
            kind=_kind_of(mapping.get("kind")),
            aliases=_aliases_of(mapping.get("aliases")),
            variants={str(version): dict(variant) for version, variant in versions.items()},
            column=str(mapping.get("column") or ""),
        )

    @classmethod
    def from_fields(cls, members: Sequence[Field], versions: Sequence[str]) -> Self:
        """One entry out of the same field read from several versions.

        `members` and `versions` run newest first together, which is the order
        every merged reading here walks and so the order the variants keep.
        """
        if not members:
            raise ValueError("a FIX field entry needs at least one declaration")
        latest = members[0]
        tag = latest.fix.get("tag")
        return cls(
            name=latest.name,
            tag=int(tag) if tag else None,
            kind=STANDARD if tag else NAMESPACE,
            column=latest.fix.get("column", ""),
            variants={
                version: variant_of(member, latest.name, int(tag) if tag else None)
                for member, version in zip(members, versions, strict=True)
            },
        )


@dataclasses.dataclass(frozen=True)
class ComponentEntry(Convertible):
    """One component identity, and every version's member tree for it."""

    name: str
    aliases: tuple[Alias, ...] = ()
    #: `{version: {"order": n, "members": [...]}}`, newest version first once
    #: stored. `order` is where the version's spec declares this component,
    #: which is the order `components()` hands a version's declarations back
    #: in -- a fact about the version that survives being stored per identity.
    variants: Mapping[str, Mapping[str, Any]] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        """Refuse an entry no lookup could answer for."""
        if not str(self.name).strip():
            raise ValueError("a FIX component entry has no name")
        if not self.variants:
            raise ValueError(f"FIX component {self.name!r} is declared for no version")

    @property
    def slug(self) -> str:
        """The file name this entry is stored under."""
        return slug_of(self.name)

    @property
    def folded(self) -> str:
        """The canonical name as it is matched."""
        return fold(self.name)

    @property
    def versions(self) -> tuple[str, ...]:
        """Every version this component is declared for, in stored order."""
        return tuple(self.variants)

    def spellings(self) -> tuple[str, ...]:
        """Every name this entry answers to, in resolution order."""
        found: dict[str, str] = {self.folded: self.name}
        for alias in self.aliases:
            found.setdefault(alias.folded, alias.name)
        return tuple(found.values())

    def order(self, version: str) -> int:
        """Where this version's spec declares the component, among its others."""
        return int(self.variants.get(version, {}).get("order", 0))

    def into_component(self, version: str) -> SpecComponent | None:
        """One version's declaration, or None when it has none."""
        found = self.variants.get(version)
        if found is None:
            return None
        return SpecComponent.from_dict({"name": self.name, "members": found.get("members", ())})

    def paths(self, version: str) -> dict[str, tuple[str, ...]]:
        """`{member name: the groups it sits under}` for one version.

        The derived half of a component: the tree says it, but a consumer
        splitting a message wants it flat, and deriving it in two places is how
        two readers of one declaration come to disagree.
        """
        found: dict[str, tuple[str, ...]] = {}
        for member, path in self._walk(version):
            found.setdefault(member.name, path)
        return found

    def delimiters(self, version: str) -> dict[tuple[str, ...], str]:
        """`{group path: the member that opens one entry}` for one version.

        A repeating group's first member is its delimiter -- the standard says
        so, and it is what tells one entry from the next.
        """
        found: dict[tuple[str, ...], str] = {}
        for member, path in self._walk(version):
            if isinstance(member, SpecGroup) and member.members:
                found[(*path, member.name)] = member.members[0].name
        return found

    def diff(self) -> dict[str, tuple[str, ...]]:
        """`{version: the members it declares that the newest does not}`.

        What one file makes answerable that nine did not: where a component
        changed, and in which direction, without diffing nine documents.
        """
        versions = self.versions
        if not versions:
            return {}
        newest = {member.name for member, _ in self._walk(versions[0])}
        return {
            version: tuple(sorted({member.name for member, _ in self._walk(version)} - newest))
            for version in versions[1:]
        }

    def _walk(self, version: str) -> Iterator[tuple[SpecMember, tuple[str, ...]]]:
        declared = self.into_component(version)
        return _walk(declared.members, ()) if declared is not None else iter(())

    def into_dict(self) -> dict[str, Any]:
        """The entry as its file holds it."""
        return _document(
            {
                "name": self.name,
                "aliases": [alias.into_dict() for alias in self.aliases],
                "versions": {
                    version: _document(variant) for version, variant in self.variants.items()
                },
            }
        )

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Self:
        """Build one entry from its stored document."""
        versions = mapping.get("versions") or {}
        if not isinstance(versions, Mapping):
            raise TypeError("a FIX component entry's versions must be a mapping")
        return cls(
            name=str(mapping.get("name") or ""),
            aliases=_aliases_of(mapping.get("aliases")),
            variants={str(version): dict(declared) for version, declared in versions.items()},
        )

    @classmethod
    def from_components(
        cls,
        declared: Sequence[SpecComponent],
        versions: Sequence[str],
        orders: Sequence[int] = (),
    ) -> Self:
        """One entry out of the same component read from several versions."""
        if not declared:
            raise ValueError("a FIX component entry needs at least one declaration")
        ranks = orders or range(len(declared))
        return cls(
            name=declared[0].name,
            variants={
                version: component_variant(found, rank)
                for found, version, rank in zip(declared, versions, ranks, strict=True)
            },
        )


def component_variant(declared: SpecComponent, order: int) -> dict[str, Any]:
    """One version's component declaration as the document a variant holds."""
    return {"order": order, "members": [member.into_dict() for member in declared.members]}


def merged_field(members: Sequence[Field]) -> Field:
    """Newest field identity with every version's non-conflicting knowledge.

    `members` runs newest version first. What `FixRegistry.scalar` hands out
    for a key with no version, and -- through `merged_fields` -- what a whole
    unified table is made of.
    """
    latest = members[0]
    typed = next((member for member in members if member.fix.get("type")), latest)
    metadata = dict(latest.metadata)
    metadata["fix:versions"] = _json([member.fix["version"] for member in members])
    metadata["fix:types"] = _json(
        {member.fix["version"]: member.fix["type"] for member in members if member.fix.get("type")}
    )

    names = {member.fix["version"]: member.name for member in members}
    if len(set(names.values())) > 1:
        metadata["fix:names"] = _json(names)
    tags = {member.fix["version"]: member.fix["tag"] for member in members if member.fix.get("tag")}
    if len(set(tags.values())) > 1:
        metadata["fix:tags"] = _json(tags)

    for key in ("values", "value_names"):
        combined: dict[str, str] = {}
        # Oldest first, so a newer correction of one code wins without losing a
        # value that disappeared from the newest prose/spec page.
        for member in reversed(members):
            combined.update(_json_mapping(member.fix.get(key)))
        if combined:
            metadata[f"fix:{key}"] = _json(combined)

    used: list[str] = []
    for member in members:
        for message in _json_sequence(member.fix.get("used_in")):
            if message not in used:
                used.append(message)
    if used:
        metadata["fix:used_in"] = _json(used)

    description = next((member.description for member in members if member.description), "")
    if description:
        metadata["description"] = description
    if typed.fix.get("type"):
        metadata["fix:type"] = typed.fix["type"]
    return Field(
        name=latest.name,
        arrow_type=typed.arrow_type,
        nullable=True,
        metadata=metadata,
    )


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


def variant_of(member: Field, name: str, tag: int | None) -> dict[str, Any]:
    """One version's differences from an identity, and nothing it agrees on.

    A variant states only what this version does not share with the identity,
    so `PartyRole` in nine versions is nine small documents rather than nine
    copies of the same name and tag.
    """
    variant: dict[str, Any] = {}
    if member.name != name:
        variant["name"] = member.name
    own = member.fix.get("tag")
    if own and int(own) != (tag or 0):
        variant["tag"] = int(own)
    for key in ("type", "note"):
        value = member.fix.get(key)
        if value:
            variant[key] = value
    if member.description:
        variant["description"] = member.description
    for key in _JSON_KEYS:
        decoded = _json_any(member.fix.get(key))
        if decoded:
            variant[key] = decoded
    return variant


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
