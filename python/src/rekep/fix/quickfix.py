"""The QuickFIX spec as a second source: what the standard says, machine-readable.

The dictionary this package scrapes is prose written for people -- `Side <54>`
value `1` is "Buy". QuickFIX publishes the same standard as XML written for
programs, where that value is `BUY`: a symbol, stable across versions, and the
name any other FIX tool will have used. Neither is the other's replacement, so
both are kept -- `fix["values"]` stays the description and `fix["value_names"]`
carries the symbol.

    <value enum='1' description='MATCH' />

`description` there is the symbol, not a description, which is exactly why
merging it *into* the descriptions would replace prose with shouting.

One file per version rather than one page per field, so enriching a whole
version is a single request against the seven hundred a scrape costs.
"""

from __future__ import annotations

import dataclasses
import re
from typing import Any
from xml.etree import ElementTree

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
