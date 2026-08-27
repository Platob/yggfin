"""The QuickFIX spec as a second source: what the standard says, machine-readable.

A component, a message and a repeating group are all one shape here: a
`Field`. A block is a struct, a repeating group is a list of one, and a
reference to another block is a struct with no members yet and the block's
name in `fix:component` -- expanded by whoever reads it, because expanding
the published dictionary in place turns three thousand members into a
hundred and twenty thousand.

Names are FIX's own throughout. The Arrow projection snakes them when it
builds columns; the declaration says what the standard says.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Iterator, Mapping, Sequence
from typing import Any
from xml.etree import ElementTree

import pyarrow

from rekep.fields import Field

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


#: What an unexpanded reference is: a block whose members live elsewhere.
#: A concrete dtype is required, and an empty struct is the honest one --
#: this says nothing about the members, which is exactly the state it is in.
REFERENCE: pyarrow.DataType = pyarrow.struct([])


def field_member(name: str, tag: int, *, required: bool = False) -> Field:
    """One plain field of a declaration, at the tag the spec gives it."""
    return Field(
        name=name, dtype=pyarrow.string(), nullable=not required, metadata={"fix:tag": str(tag)}
    )


def group_member(name: str, tag: int, members: Sequence[Field], *, required: bool = False) -> Field:
    """One repeating group: a list of the entry its members describe."""
    return Field(
        name=name,
        dtype=pyarrow.list_(pyarrow.field("item", _struct(members), nullable=False)),
        nullable=not required,
        metadata={"fix:tag": str(tag)},
    )


def reference_member(name: str, *, required: bool = False) -> Field:
    """A reference to another block, left for its consumer to expand."""
    return Field(
        name=name, dtype=REFERENCE, nullable=not required, metadata={"fix:component": name}
    )


def block(name: str, members: Sequence[Field], msg_type: str = "") -> Field:
    """One component or message declaration: its members, in wire order."""
    declared = Field(
        name=name, dtype=_struct(members), nullable=True, metadata={"fix:component": name}
    )
    if msg_type:
        declared.fix.msgtype = msg_type
    return declared


def _struct(members: Sequence[Field]) -> pyarrow.DataType:
    return pyarrow.struct([member.into_arrow_field() for member in members])


def is_group(member: Field) -> bool:
    """Whether one member is a repeating group rather than a value."""
    return pyarrow.types.is_list(member.dtype)


def is_reference(member: Field) -> bool:
    """Whether one member defers to a block declared elsewhere."""
    return member.dtype == REFERENCE


def entry_of(member: Field) -> Field:
    """The entry a repeating group repeats, as a block of its own."""
    return Field(name=member.name, dtype=member.dtype.value_type, nullable=False)


def members_of(declared: Field) -> tuple[Field, ...]:
    """One block's members in wire order, or nothing for a leaf."""
    return tuple(declared.fields) if pyarrow.types.is_struct(declared.dtype) else ()


def walk(declared: Field, path: tuple[str, ...] = ()) -> Iterator[tuple[Field, tuple[str, ...]]]:
    """Every member under one block, with the groups it sits under."""
    for member in members_of(declared):
        yield member, path
        if is_group(member):
            yield from walk(entry_of(member), (*path, member.name))


def component_refs(declared: Field) -> tuple[str, ...]:
    """Every block this one defers to, however deeply a group nests it."""
    return tuple(member.name for member, _ in walk(declared) if is_reference(member))


def declared_group(
    declared: Field,
    wanted: str,
    components: Mapping[str, Field],
    seen: frozenset[str] = frozenset(),
) -> Field | None:
    """Find a nested repeating group through references without cycles."""
    for member in members_of(declared):
        if is_group(member):
            if member.name.lower() == wanted.lower():
                return member
            if found := declared_group(entry_of(member), wanted, components, seen):
                return found
        elif is_reference(member):
            key = member.name.lower()
            block = components.get(key)
            if block is not None and key not in seen:
                if found := declared_group(block, wanted, components, seen | {key}):
                    return found
    return None


def first_declared_name(
    declared: Field,
    components: Mapping[str, Field],
    seen: frozenset[str] = frozenset(),
) -> str | None:
    """The first physical field after recursive reference expansion."""
    for member in members_of(declared):
        if not is_reference(member):
            return member.name
        key = member.name.lower()
        block = components.get(key)
        if block is not None and key not in seen:
            if found := first_declared_name(block, components, seen | {key}):
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


def parse_components(document: str) -> dict[str, Field]:
    """Reusable components in one QuickFIX document, preserving their tree.

    A declaration that names a message type carries it; `<components>` entries
    never do, so the published dictionary's components all leave it empty.
    """
    root = _root(document)
    if root is None:
        return {}
    tags = {field.name: field.tag for field in parse_spec(document).values()}
    found: dict[str, Field] = {}
    for element in root.findall("./components/component"):
        name = _element_name(element, "component")
        if name in found:
            raise ValueError(f"FIX component {name!r} is declared twice")
        found[name] = block(
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
) -> tuple[Field, ...]:
    """The ordered declaration directly inside one component or group."""
    members: list[Field] = []
    for child in element:
        kind = str(child.tag)
        name = _element_name(child, ".".join(path))
        required = child.get("required") == "Y"
        if kind == "field":
            members.append(field_member(name, _field_tag(name, tags, path), required=required))
        elif kind == "component":
            members.append(reference_member(name, required=required))
        elif kind == "group":
            members.append(
                group_member(
                    name,
                    _field_tag(name, tags, path),
                    _component_members(child, tags, (*path, name)),
                    required=required,
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


def _check_component_refs(components: Mapping[str, Field]) -> None:
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
        for reference in component_refs(component):
            visit(reference, (*path, name))
        done.add(name)

    for name in components:
        visit(name, ())
