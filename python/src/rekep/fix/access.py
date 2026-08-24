"""One way to read a FIX field, however a caller names it.

A caller holds a field one of four ways -- a numeric tag (``770``), a canonical
name (``TrdRegTimestampType``), a component path
(``NoTrdRegTimestamps[0].TrdRegTimestamp``) or a namespace-qualified key
(``TECH.CLIENTID``) -- and every one of them resolves here, against one rule
table, to the same reading. The scalar execution is this module; the columnar
one is `TagIndex` (fix/transcribe.py), which reads the same rules in Arrow
kernels, and `TagIndex.resolve_key` is the shared seam the two meet at.
"""

from __future__ import annotations

import dataclasses
import functools
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from functools import cached_property
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.fix.entries import FieldEntry, fold, translation_key
from rekep.fix.fields import cast_arrow_fix
from rekep.fix.registry import FixRegistry
from rekep.fix.transcribe import TagIndex

#: A rendered key cut into whatever stood in front, the name, and its entry
#: index: `NoPartyIDs[0].PartyID` is lead `NoPartyIDs[0]` name `PartyID`;
#: `TECH.CLIENTID` is lead `TECH` name `CLIENTID`; `Side[0]` is name `Side`
#: index `0`. Greedy lead, so the *last* dot is the cut -- the same rule the
#: parser and the transcription apply.
_KEY = re.compile(
    r"(?s)^(?:(?P<lead>.*)\.)?(?P<name>[^.\[\]]*)(?:\[(?P<index>[0-9]+)\])?$",
    re.ASCII,
)

#: A lead that names a repeating-group entry rather than a namespace: it ends
#: with an index. The one dotted lead a bare name still answers through --
#: `get("PartyID")` finds `NoPartyIDs[0].PartyID`, because the group is where
#: the field sits and not what it is, while `TECH.CLIENTID` stays out of reach
#: of `get("CLIENTID")` because a vendor namespace is part of the name.
_ENTRY_LEAD = re.compile(r"\[[0-9]+\]$", re.ASCII)

_MISSING = object()


@dataclasses.dataclass(frozen=True)
class Entry:
    """One field as a row carries it, whichever shape the row stored it in."""

    tag: int = 0
    name: str = ""
    index: int | None = None
    lead: str | None = None
    #: Whether a bare-name ask may reach through `lead`: true for a group
    #: entry (`NoPartyIDs[0]`) and for a stored `comp`, false for a namespace.
    entry_lead: bool = False
    value: Any = None

    @classmethod
    def from_pair(cls, key: Any, value: Any) -> Entry:
        """A wire `(key, value)` pair, split under the parser's own key rule."""
        text = str(key)
        if text.isascii() and text.isdigit() and len(text) <= 9:
            return cls(tag=int(text), name=text, value=value)
        match = _KEY.match(text)
        if match is None:
            return cls(name=text, value=value)
        lead, name, index = match.group("lead", "name", "index")
        return cls(
            name=name or text,
            index=None if index is None else int(index),
            lead=lead,
            entry_lead=bool(lead) and _ENTRY_LEAD.search(lead) is not None,
            value=value,
        )

    @classmethod
    def from_stored(cls, stored: Mapping[str, Any]) -> Entry:
        """One stored `kwargs` struct entry, `comp`/`namespace` already split."""
        comp = stored.get("comp")
        lead = comp if comp else stored.get("namespace")
        key = str(stored.get("key") or "")
        match = _KEY.match(key)
        name, index = key, None
        if match is not None and match.group("index") is not None:
            name, index = match.group("name"), int(match.group("index"))
        return cls(
            tag=int(stored.get("tag") or 0),
            name=name,
            index=index,
            lead=lead,
            entry_lead=bool(comp),
            value=stored.get("value"),
        )


def entries_of(fields: Iterable[Any]) -> Iterator[Entry]:
    """`Entry` views over pairs, stored `kwargs` structs, or ready entries."""
    for field in fields or ():
        if isinstance(field, Entry):
            yield field
        elif isinstance(field, Mapping):
            yield Entry.from_stored(field)
        else:
            key, value = field
            yield Entry.from_pair(key, value)


@dataclasses.dataclass(frozen=True)
class Resolved:
    """What one asked-for field is, whichever of the four ways it was named."""

    spelling: str
    tag: int | None = None
    #: Every folded name that answers for the field: the ask's own tail, and
    #: -- with a dictionary -- the canonical name and its recorded aliases.
    names: frozenset[str] = frozenset()
    #: The separator-blind reading of the tail, `translation_key`'s rule: the
    #: last tier, so `msg_type` still reaches `MsgType` without an alias.
    norm: str = ""
    lead: str | None = None
    index: int | None = None

    def matches(self, entry: Entry) -> bool:
        """Whether `entry` answers for this field -- the one matching rule."""
        if self.lead is not None:
            if fold(entry.lead or "") != fold(self.lead):
                return False
            return self._named(entry) or (self.tag is not None and entry.tag == self.tag)
        if self.tag is not None and entry.tag and entry.tag == self.tag:
            return True
        if entry.lead is not None and not entry.entry_lead:
            return False
        if self.index is not None and entry.index != self.index:
            return False
        return self._named(entry)

    def _named(self, entry: Entry) -> bool:
        folded = fold(entry.name)
        if folded in self.names:
            return True
        return bool(self.norm) and translation_key(entry.name) == self.norm


class Reading:
    """One field read out of one row: the wire text, and the typed reading.

    Both from one call, so no call site chooses an accessor by which half it
    wants: `raw` is the value exactly as the row stored it, and `value` is
    what the dictionary makes of it -- the spelling translated where the field
    enumerates its values, then cast to the field's own type. Falsy when the
    row does not carry the field at all.
    """

    __slots__ = ("found", "raw", "tag", "key", "_access", "_value")

    def __init__(
        self,
        found: bool,
        raw: Any = None,
        tag: int = 0,
        key: str = "",
        access: FieldAccess | None = None,
    ) -> None:
        self.found = found
        self.raw = raw
        self.tag = tag
        self.key = key
        self._access = access
        self._value = _MISSING

    def __bool__(self) -> bool:
        return self.found

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Reading(found={self.found}, key={self.key!r}, raw={self.raw!r})"

    @property
    def value(self) -> Any:
        """The typed reading, computed once on first use."""
        if self._value is _MISSING:
            if self._access is None or self.raw is None:
                self._value = self.raw
            else:
                self._value = self._access.typed(self.tag or self.key, self.raw)
        return self._value


_ABSENT = Reading(found=False)


@dataclasses.dataclass(eq=False)
class FieldAccess:
    """The one field accessor: a dictionary's resolution rules, held ready.

    `registry=None` resolves by spelling alone, which is the reading a bare
    wire model gets; a dictionary adds canonical names, aliases, enumerated
    values and types. `version=None` reads the cross-version dictionary.
    """

    registry: FixRegistry | None = None
    version: str | None = None

    @classmethod
    @functools.lru_cache(maxsize=64)
    def of(cls, registry: FixRegistry | None = None, version: str | None = None) -> FieldAccess:
        """One shared accessor per `(registry, version)`, memos and all."""
        return cls(registry=registry, version=version)

    @classmethod
    def spelling_only(cls) -> FieldAccess:
        """The dictionary-less accessor a bare wire model reads through."""
        return cls.of(None, None)

    @cached_property
    def index(self) -> TagIndex:
        """The name index the columnar path uses, shared so the rules are one."""
        if self.registry is None:
            return TagIndex.from_tags({})
        try:
            tags = self.registry.tags(self.version)
        except (KeyError, OSError, ValueError):
            tags = {}
        return TagIndex.from_tags(tags, self._containers())

    def _containers(self) -> tuple[str, ...]:
        """Every container a dotted key may name, across the versions read."""
        if self.registry is None:
            return ()
        try:
            if self.version is not None:
                return tuple(component.name for component in self.registry.components(self.version))
            return tuple(self.registry.component_entries())
        except (KeyError, OSError, ValueError):
            return ()

    @cached_property
    def _resolved(self) -> dict[Any, Resolved]:
        return {}

    def resolve(self, field: int | str) -> Resolved:
        """The one resolution of one asked-for field, memoized per spelling."""
        found = self._resolved.get(field)
        if found is None:
            found = self._resolved[field] = self._resolve(field)
        return found

    def _resolve(self, field: int | str) -> Resolved:
        spelling = field if type(field) is str else str(field)
        if spelling.isascii() and spelling.isdigit() and len(spelling) <= 9:
            tag = int(spelling)
            names = frozenset({spelling}) | self._spellings_of(tag)
            return Resolved(spelling=spelling, tag=tag, names=names)
        match = _KEY.match(spelling)
        lead = name = None
        index = None
        if match is not None:
            lead, name, spelled_index = match.group("lead", "name", "index")
            index = None if spelled_index is None else int(spelled_index)
        name = name or spelling
        tag, hit, _, _ = self.index.resolve_key(spelling)
        record = self._record(name) if self.registry is not None else None
        if not hit and record is not None and record.tag is not None:
            tag = int(record.tag)
        names = {fold(name)}
        if record is not None:
            names.update(fold(one) for one in record.spellings())
        if tag is not None:
            names.update(self._spellings_of(tag))
        return Resolved(
            spelling=spelling,
            tag=tag,
            names=frozenset(names),
            norm=translation_key(name),
            lead=lead,
            index=index,
        )

    def _spellings_of(self, tag: int) -> frozenset[str]:
        """Every folded name a tag answers to, empty without a dictionary."""
        record = self._record(tag)
        if record is None:
            return frozenset()
        return frozenset(fold(one) for one in record.spellings())

    def _record(self, key: int | str) -> FieldEntry | None:
        """The dictionary's record for one tag or name, or None."""
        if self.registry is None:
            return None
        try:
            return self.registry.entry(key)
        except (OSError, ValueError):
            return None

    # -- reading rows ---------------------------------------------------------

    def reading(self, fields: Iterable[Any], field: int | str) -> Reading:
        """The first value of `field` in wire order, with its typed reading."""
        resolved = self.resolve(field)
        for entry in entries_of(fields):
            if resolved.matches(entry):
                return self._reading(entry)
        return _ABSENT

    def readings(self, fields: Iterable[Any], field: int | str) -> list[Reading]:
        """Every value of `field`, in wire order -- what a repeating tag is."""
        resolved = self.resolve(field)
        return [self._reading(entry) for entry in entries_of(fields) if resolved.matches(entry)]

    def _reading(self, entry: Entry) -> Reading:
        key = entry.name if entry.lead is None else f"{entry.lead}.{entry.name}"
        return Reading(found=True, raw=entry.value, tag=entry.tag, key=key, access=self)

    def tag_text(self, field: int | str) -> str:
        """A field as the wire tag it resolves to, or the spelling itself."""
        resolved = self.resolve(field)
        if resolved.tag is not None:
            return str(resolved.tag)
        return resolved.spelling

    def tagged_pairs(self, pairs: Iterable[tuple[Any, Any]]) -> list[tuple[str, Any]]:
        """`pairs` with each resolvable name replaced by its tag, order kept.

        What the market reader indexes a message by: a wire message is already
        all tags and passes through untouched; a rendered one resolves each
        distinct spelling once through the shared rule table.
        """
        built: list[tuple[str, Any]] = []
        for key, value in pairs:
            text = key if type(key) is str else str(key)
            if text.isascii() and text.isdigit():
                built.append((text, value))
                continue
            tag, hit, _, _ = self.index.resolve_key(text)
            if not hit and self.registry is not None:
                record = self._record(_KEY_TAIL(text))
                if record is not None and record.tag is not None:
                    tag, hit = int(record.tag), True
            built.append((str(tag) if hit and tag is not None else text, value))
        return built

    # -- the typed half -------------------------------------------------------

    def typed(self, field: int | str, raw: Any) -> Any:
        """`raw` as the dictionary reads it: translated, then cast to type.

        The translation is `FieldEntry.translate` -- the dictionary's own
        resolver -- and the cast is `cast_arrow_fix`, the same reading the
        columnar path applies, over one value.
        """
        if raw is None or not isinstance(raw, str):
            return raw
        record = self._record(field if type(field) is int else _KEY_TAIL(str(field)))
        if record is None:
            return raw
        text = record.translate(raw)
        arrow_type = self._arrow_type(record)
        if arrow_type is None or pyarrow.types.is_string(arrow_type):
            return text
        try:
            return cast_arrow_fix(pyarrow.array([text], pyarrow.string()), arrow_type)[0].as_py()
        except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError, ValueError):
            return text

    @cached_property
    def _arrow_types(self) -> dict[int | str, pyarrow.DataType | None]:
        return {}

    def _arrow_type(self, record: FieldEntry) -> pyarrow.DataType | None:
        if self.registry is None:
            return None
        if record.key not in self._arrow_types:
            try:
                found = self.registry.field(record.key, self.version).arrow_type
            except (KeyError, OSError, ValueError):
                found = None
            self._arrow_types[record.key] = found
        return self._arrow_types[record.key]


def _KEY_TAIL(spelling: str) -> str | int:
    """The name segment a dictionary record is asked for, index stripped."""
    match = _KEY.match(spelling)
    if match is None:
        return spelling
    return match.group("name") or spelling


# -- whole columns ------------------------------------------------------------


def first_arrow_tags(stored: Any, wanted: Sequence[int], rows: int) -> dict[int, Any]:
    """First value of each wanted tag out of a stored `kwargs` column.

    The columnar execution of `reading` for the tag-numbered case, in one
    list scan -- what a per-row loop over `reading` would answer, batched.
    """
    from rekep.fields.arrays import sequence

    if isinstance(stored, pyarrow.ChunkedArray):
        stored = stored.combine_chunks()
    if not rows or stored.null_count == rows:
        return {}
    compute = pyarrow.compute
    parents = compute.list_parent_indices(stored).cast(pyarrow.int32())
    entries = compute.list_flatten(stored)
    keys = compute.struct_field(entries, "tag")
    values = compute.struct_field(entries, "value")
    matches = compute.fill_null(
        compute.is_in(keys, value_set=pyarrow.array(wanted, keys.type)), False
    )
    if not compute.any(matches, min_count=0).as_py():
        return {}
    matched_keys = compute.filter(keys, matches)
    matched_parents = compute.filter(parents, matches)
    matched_values = compute.filter(values, matches)
    row_ids = sequence(rows)
    found = {}
    for tag in compute.unique(matched_keys).to_pylist():
        at = compute.equal(matched_keys, tag)
        where = compute.filter(matched_parents, at)
        chosen = compute.filter(matched_values, at)
        found[tag] = compute.take(chosen, compute.index_in(row_ids, value_set=where))
    return found
