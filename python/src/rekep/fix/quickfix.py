"""The QuickFIX spec as a second source: what the standard says, machine-readable."""

from __future__ import annotations

import dataclasses
import functools
import re
from collections.abc import Mapping, Sequence
from typing import Any
from xml.etree import ElementTree

from rekep.convert import Convertible

#: Where the spec files live. Override to point at a fork or a mirror.
QUICKFIX_URL = "https://raw.githubusercontent.com/quickfix/quickfix/master/spec"

#: Versions the repository publishes, in the spelling this package uses. Pinned
#: rather than discovered: the directory is not listable over raw HTTP, and a
#: version nobody publishes is a 404 either way.
SPEC_VERSIONS: tuple[str, ...] = (
    "4.0",
    "4.1",
    "4.2",
    "4.3",
    "4.4",
    "5.0",
    "5.0.SP1",
    "5.0.SP2",
    "FIXT1.1",
)


@dataclasses.dataclass(frozen=True)
class SpecField:
    """One field as the spec declares it: its tag, its name, its datatype.

    `values` is `{enum: SYMBOL}` -- the machine-readable name of each
    enumerated value, which is the half the scraped dictionary does not have.
    """

    tag: int
    name: str
    datatype: str
    values: dict[str, str] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class SpecMember(Convertible):
    """One ordered member of a QuickFIX component declaration."""

    @classmethod
    @functools.cache
    def into_kind(cls) -> str:
        """Stored member kind; empty on the base."""
        return ""

    name: str
    required: bool = False

    def into_dict(self) -> dict[str, Any]:
        """The member as a declaration that names its concrete kind."""
        return {"kind": type(self).into_kind(), "name": self.name, "required": self.required}

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> SpecMember:
        """Build the concrete member named by a stored declaration."""
        kind = str(mapping.get("kind") or "")
        member = _MEMBER_KINDS.get(kind)
        if member is None:
            raise ValueError(f"unknown FIX component member kind {kind!r}")
        if cls is not SpecMember and member is not cls:
            raise ValueError(f"{cls.__name__} cannot read a {kind!r} member")
        return member._from_dict(mapping)

    @classmethod
    def _from_dict(cls, mapping: Mapping[str, Any]) -> SpecMember:
        return cls(name=_stored_name(mapping), required=bool(mapping.get("required", False)))


@dataclasses.dataclass(frozen=True)
class SpecFieldRef(SpecMember):
    """A field used by a component, resolved to its FIX tag."""

    @classmethod
    @functools.cache
    def into_kind(cls) -> str:
        """Stored member kind."""
        return "field"

    tag: int = 0

    def into_dict(self) -> dict[str, Any]:
        return {**super().into_dict(), "tag": self.tag}

    @classmethod
    def _from_dict(cls, mapping: Mapping[str, Any]) -> SpecFieldRef:
        return cls(
            name=_stored_name(mapping),
            required=bool(mapping.get("required", False)),
            tag=_stored_tag(mapping),
        )


@dataclasses.dataclass(frozen=True)
class SpecComponentRef(SpecMember):
    """A reference to another component, expanded only by its consumer."""

    @classmethod
    @functools.cache
    def into_kind(cls) -> str:
        """Stored member kind."""
        return "component"


@dataclasses.dataclass(frozen=True)
class SpecGroup(SpecMember):
    """A repeating group: its count field and ordered entry declaration."""

    @classmethod
    @functools.cache
    def into_kind(cls) -> str:
        """Stored member kind."""
        return "group"

    tag: int = 0
    members: tuple[SpecMember, ...] = ()

    def into_dict(self) -> dict[str, Any]:
        return {
            **super().into_dict(),
            "tag": self.tag,
            "members": [member.into_dict() for member in self.members],
        }

    @classmethod
    def _from_dict(cls, mapping: Mapping[str, Any]) -> SpecGroup:
        members = mapping.get("members", ())
        if not isinstance(members, list | tuple):
            raise TypeError("a FIX group declaration's members must be a sequence")
        return cls(
            name=_stored_name(mapping),
            required=bool(mapping.get("required", False)),
            tag=_stored_tag(mapping),
            members=tuple(SpecMember.from_dict(member) for member in members),
        )


_MEMBER_KINDS: dict[str, type[SpecMember]] = {
    SpecFieldRef.into_kind(): SpecFieldRef,
    SpecComponentRef.into_kind(): SpecComponentRef,
    SpecGroup.into_kind(): SpecGroup,
}


@dataclasses.dataclass(frozen=True)
class SpecComponent(Convertible):
    """One reusable FIX component, with its members in wire order.

    `msg_type` is the message type a declaration defines where it defines one
    -- `"D"`, `"8"` -- and empty for a reusable block, which is what every
    `<components>` entry is.
    """

    name: str
    members: tuple[SpecMember, ...] = ()
    msg_type: str = ""

    def into_dict(self) -> dict[str, Any]:
        declared: dict[str, Any] = {"name": self.name}
        if self.msg_type:
            declared["msg_type"] = self.msg_type
        declared["members"] = [member.into_dict() for member in self.members]
        return declared

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> SpecComponent:
        members = mapping.get("members", ())
        if not isinstance(members, list | tuple):
            raise TypeError("a FIX component declaration's members must be a sequence")
        return cls(
            name=_stored_name(mapping),
            members=tuple(SpecMember.from_dict(member) for member in members),
            msg_type=str(mapping.get("msg_type") or ""),
        )


def declared_group(
    members: Sequence[SpecMember],
    wanted: str,
    components: Mapping[str, SpecComponent],
    seen: frozenset[str] = frozenset(),
) -> SpecGroup | None:
    """Find a nested group through component references without cycles."""
    for member in members:
        if isinstance(member, SpecGroup):
            if member.name.lower() == wanted.lower():
                return member
            if found := declared_group(member.members, wanted, components, seen):
                return found
        elif isinstance(member, SpecComponentRef):
            key = member.name.lower()
            component = components.get(key)
            if component is not None and key not in seen:
                if found := declared_group(component.members, wanted, components, seen | {key}):
                    return found
    return None


def first_declared_name(
    members: Sequence[SpecMember],
    components: Mapping[str, SpecComponent],
    seen: frozenset[str] = frozenset(),
) -> str | None:
    """The first physical field after recursive component expansion."""
    for member in members:
        if isinstance(member, SpecFieldRef | SpecGroup):
            return member.name
        if isinstance(member, SpecComponentRef):
            key = member.name.lower()
            component = components.get(key)
            if component is not None and key not in seen:
                if found := first_declared_name(component.members, components, seen | {key}):
                    return found
    return None


def spec_name(version: str) -> str:
    """`4.4` -> `FIX44.xml`, `5.0.SP2` -> `FIX50SP2.xml`, `FIXT1.1` -> `FIXT11.xml`.

    The punctuation is decoration on both sides of the mapping, so dropping it
    is the whole rule -- except `FIXT`, which is a different protocol rather
    than a spelling of `FIX` and keeps its own prefix.
    """
    text = re.sub(r"[^A-Za-z0-9]", "", str(version).strip().upper())
    if text.startswith("FIXT"):
        return f"{text}.xml"
    return f"FIX{text[3:] if text.startswith('FIX') else text}.xml"


def parse_spec(document: str) -> dict[int, SpecField]:
    """`{tag: SpecField}` out of one spec file, or empty when it says nothing.

    Only `<fields>` is read. The message and component blocks describe *where*
    a field may appear, which is a different question from what a field is --
    and the one this package answers through the dictionary's own `used_in`.
    """
    root = _root(document)
    if root is None:
        return {}
    found: dict[int, SpecField] = {}
    for element in root.findall("./fields/field"):
        number = element.get("number")
        name = element.get("name")
        if not number or not name or not number.isdigit():
            continue
        values = {
            value.get("enum", ""): value.get("description", "")
            for value in element.findall("./value")
            if value.get("enum") and value.get("description")
        }
        found[int(number)] = SpecField(int(number), name, element.get("type") or "", values)
    return found


def parse_components(document: str) -> dict[str, SpecComponent]:
    """Reusable components in one QuickFIX document, preserving their tree.

    A declaration that names a message type carries it; `<components>` entries
    never do, so the published dictionary's components all leave it empty.
    """
    root = _root(document)
    if root is None:
        return {}
    tags = {field.name: field.tag for field in parse_spec(document).values()}
    found: dict[str, SpecComponent] = {}
    for element in root.findall("./components/component"):
        name = _element_name(element, "component")
        if name in found:
            raise ValueError(f"FIX component {name!r} is declared twice")
        found[name] = SpecComponent(
            name,
            _component_members(element, tags, (name,)),
            str(element.get("msgtype") or ""),
        )
    _check_component_refs(found)
    return found


def parse_session(document: str) -> tuple[tuple[str, bool], ...]:
    """`((name, required), ...)` for the standard header, then the trailer.

    The spec's own answer to which fields every message carries and which of
    them it must -- the two facts `rekep.fix.columns` declares by hand, so a
    test can hold the declaration to them.

    A `<component>` inside the header is a repeating group (`NoHops`), and is
    skipped: one row of a group is not one value, which is the same reason it
    is not a column.
    """
    root = _root(document)
    if root is None:
        return ()
    session: list[tuple[str, bool]] = []
    for part in ("header", "trailer"):
        for element in root.findall(f"./{part}/field"):
            name = element.get("name")
            if name:
                session.append((name, element.get("required") == "Y"))
    return tuple(session)


def _root(document: str) -> Any:
    """The parsed document, or None for anything that is not one."""
    try:
        return ElementTree.fromstring(document)  # noqa: S314
    except ElementTree.ParseError:
        return None


def _component_members(
    element: Any, tags: Mapping[str, int], path: tuple[str, ...]
) -> tuple[SpecMember, ...]:
    """The ordered declaration directly inside one component or group."""
    members: list[SpecMember] = []
    for child in element:
        kind = str(child.tag)
        name = _element_name(child, ".".join(path))
        required = child.get("required") == "Y"
        if kind == SpecFieldRef.into_kind():
            members.append(SpecFieldRef(name, required, _field_tag(name, tags, path)))
        elif kind == SpecComponentRef.into_kind():
            members.append(SpecComponentRef(name, required))
        elif kind == SpecGroup.into_kind():
            members.append(
                SpecGroup(
                    name,
                    required,
                    _field_tag(name, tags, path),
                    _component_members(child, tags, (*path, name)),
                )
            )
        else:
            raise ValueError(
                f"FIX component {'.'.join(path)!r} contains unknown member kind {kind!r}"
            )
    return tuple(members)


def _element_name(element: Any, owner: str) -> str:
    """A declaration's required name, refusing an anonymous member."""
    name = str(element.get("name") or "").strip()
    if not name:
        raise ValueError(f"FIX {owner} contains a member with no name")
    return name


def _field_tag(name: str, tags: Mapping[str, int], path: tuple[str, ...]) -> int:
    """The tag of a referenced field, which every usable declaration needs."""
    tag = tags.get(name)
    if tag is None:
        raise ValueError(f"FIX component {'.'.join(path)!r} references unknown field {name!r}")
    return tag


def _check_component_refs(components: Mapping[str, SpecComponent]) -> None:
    """Refuse missing and recursive component references by their full path."""
    done: set[str] = set()

    def visit(name: str, path: tuple[str, ...]) -> None:
        if name in path:
            chain = " -> ".join((*path, name))
            raise ValueError(f"recursive FIX component reference: {chain}")
        if name in done:
            return
        component = components.get(name)
        if component is None:
            owner = " -> ".join(path) or "component declaration"
            raise ValueError(f"FIX component {owner} references unknown component {name!r}")
        for reference in _component_refs(component.members):
            visit(reference, (*path, name))
        done.add(name)

    for name in components:
        visit(name, ())


def _component_refs(members: tuple[SpecMember, ...]) -> tuple[str, ...]:
    """Component names referenced anywhere under `members`."""
    found: list[str] = []
    for member in members:
        if isinstance(member, SpecComponentRef):
            found.append(member.name)
        elif isinstance(member, SpecGroup):
            found.extend(_component_refs(member.members))
    return tuple(found)


def _stored_name(mapping: Mapping[str, Any]) -> str:
    """A stored declaration's non-empty name."""
    name = str(mapping.get("name") or "").strip()
    if not name:
        raise ValueError("a stored FIX component declaration has no name")
    return name


def _stored_tag(mapping: Mapping[str, Any]) -> int:
    """A stored field reference's positive FIX tag."""
    tag = int(mapping.get("tag") or 0)
    if tag <= 0:
        raise ValueError(f"a stored FIX component member has invalid tag {tag!r}")
    return tag
