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

from rekep.fields.arrays import sequence
from rekep.fix.entries import FieldEntry, fold, translation_key
from rekep.fix.fields import cast_arrow_fix, scalar_fix_temporal
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

    @cached_property
    def folded(self) -> str:
        """`name` as `Resolved.matches` compares it: folded once per entry.

        Once and not once per compare, because reading several dozen fields
        off one row compares every entry against every ask.
        """
        return fold(self.name)

    @cached_property
    def folded_lead(self) -> str:
        """`lead` folded, empty where the entry carries none."""
        return fold(self.lead or "")

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
        numeric = bool(name) and name.isascii() and name.isdigit() and len(name) <= 9
        return cls(
            tag=int(name) if numeric else 0,
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


@dataclasses.dataclass(frozen=True)
class Resolved:
    """What one asked-for field is, whichever of the four ways it was named."""

    spelling: str
    tag: int | None = None
    #: Every folded name that answers for the field: the ask's own tail, and
    #: -- with a dictionary -- the canonical name and its recorded aliases.
    names: frozenset[str] = frozenset()
    #: The separator-blind reading of the tail, `translation_key`'s rule: the
    #: last tier, so registry-exact `MsgType` remains directly addressable.
    norm: str = ""
    lead: str | None = None
    index: int | None = None

    @cached_property
    def folded_lead(self) -> str:
        """The asked-for `lead`, folded once rather than once per entry."""
        return fold(self.lead or "")

    def matches(self, entry: Entry) -> bool:
        """Whether `entry` answers for this field -- the one matching rule."""
        if self.lead is not None:
            if entry.folded_lead != self.folded_lead:
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
        if entry.folded in self.names:
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

    __slots__ = ("found", "raw", "tag", "key", "_access", "_value", "_meaning")

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
        self._meaning = _MISSING

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

    @property
    def meaning(self) -> str | None:
        """What the value means, where its field enumerates its values.

        Derived here rather than stored beside every field: it is a fact about
        the dictionary and the value, not about the row, so a row read under a
        newer dictionary says what that dictionary says.
        """
        if self._meaning is _MISSING:
            if self._access is None or self.raw is None:
                self._meaning = None
            else:
                self._meaning = self._access.meaning(self.tag or self.key, self.raw)
        return self._meaning


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
        for entry in self.entries_of(fields):
            if resolved.matches(entry):
                return self._reading(entry)
        return _ABSENT

    def readings(self, fields: Iterable[Any], field: int | str) -> list[Reading]:
        """Every value of `field`, in wire order -- what a repeating tag is."""
        resolved = self.resolve(field)
        return [
            self._reading(entry) for entry in self.entries_of(fields) if resolved.matches(entry)
        ]

    def _reading(self, entry: Entry) -> Reading:
        key = entry.name if entry.lead is None else f"{entry.lead}.{entry.name}"
        return Reading(found=True, raw=entry.value, tag=entry.tag, key=key, access=self)

    @cached_property
    def _tag_texts(self) -> dict[Any, str]:
        return {}

    def tag_text(self, field: int | str) -> str:
        """A field as the wire tag it resolves to, or the spelling itself.

        Memoized per asked-for spelling, and not only per resolution: a
        translation asks for the same few hundred names on every message, and
        the digit test, the `Resolved` probe and the `str()` of its tag were
        together a third of a conversion (benchmarks/bench_market.py).

        A key already spelled in digits *is* the tag, and keeps its own
        spelling: `007` names tag 7 and a wire message keys it `007`.
        """
        found = self._tag_texts.get(field)
        if found is None:
            found = self._tag_texts[field] = self._tag_text(field)
        return found

    def _tag_text(self, field: int | str) -> str:
        text = field if type(field) is str else str(field)
        if text.isascii() and text.isdigit():
            return text
        resolved = self.resolve(field)
        return str(resolved.tag) if resolved.tag is not None else resolved.spelling

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

    @cached_property
    def _temporal_fields(self) -> dict[int | str, pyarrow.DataType | None]:
        return {}

    def canonical_value(self, field: int | str, raw: Any) -> Any:
        """Normalize temporal text whose original precision storage cannot retain."""
        if not isinstance(raw, str):
            return raw
        temporal = self._temporal_fields.get(field, _MISSING)
        if temporal is _MISSING:
            record = self._record(field if type(field) is int else _KEY_TAIL(str(field)))
            arrow_type = None if record is None else self._arrow_type(record)
            temporal = (
                arrow_type
                if arrow_type is not None and pyarrow.types.is_temporal(arrow_type)
                else None
            )
            self._temporal_fields[field] = temporal
        if temporal is None:
            return raw
        typed = scalar_fix_temporal(raw, temporal)
        return typed if typed is not None else raw

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

    def meaning(self, field: int | str, raw: Any) -> str | None:
        """What one value means, through the record's own `meaning`.

        Translated first, so a value spelled by its meaning still finds it:
        `Side=Buy` and `Side=1` both mean "Buy".
        """
        if raw is None or not isinstance(raw, str):
            return None
        record = self._record(field if type(field) is int else _KEY_TAIL(str(field)))
        if record is None:
            return None
        return record.meaning(record.translate(raw))

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

    # -- whole columns --------------------------------------------------------
    #
    # The columnar execution of `reading`: one scan of a stored column, where a
    # per-row loop over `reading` would answer the same thing a row at a time.
    # On the class the scalar reading lives on, because they are one rule.

    @staticmethod
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

    @classmethod
    def first_named(cls, stored: Any, tag: int, name: str, rows: int) -> Any:
        """First value of one field per row, by its tag *or* by its name.

        What the message stage reads a field with, before anything has resolved
        a name: a wire message spells the key `35` and a rendered one spells it
        `MsgType`, and both are the same field. Two comparisons over the child
        array, and no dictionary -- which is the point, since this runs before
        one is consulted.
        """
        flattened = cls._flattened(stored, rows)
        if flattened is None:
            return pyarrow.nulls(rows, pyarrow.string())
        parents, entries, values = flattened
        compute = pyarrow.compute
        numbered = compute.fill_null(
            compute.equal(compute.struct_field(entries, "tag"), tag), False
        )
        named = compute.fill_null(
            compute.equal(compute.utf8_lower(compute.struct_field(entries, "key")), name.lower()),
            False,
        )
        matches = compute.or_(numbered, named)
        if not compute.any(matches, min_count=0).as_py():
            return pyarrow.nulls(rows, pyarrow.string())
        return cls._first_per_row(
            compute.filter(values, matches), compute.filter(parents, matches), sequence(rows)
        )

    @classmethod
    def first_arrow_tags(cls, stored: Any, wanted: Sequence[int], rows: int) -> dict[int, Any]:
        """First value of each wanted tag out of a stored `kwargs` column."""
        flattened = cls._flattened(stored, rows)
        if flattened is None:
            return {}
        parents, entries, values = flattened
        compute = pyarrow.compute
        keys = compute.struct_field(entries, "tag")
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
            found[tag] = cls._first_per_row(
                compute.filter(matched_values, at), compute.filter(matched_parents, at), row_ids
            )
        return found

    @classmethod
    def first_arrow_fields(
        cls, stored: Any, wanted: Sequence[tuple[int, str]], rows: int
    ) -> dict[str, Any]:
        """First value per row for each field, matching numeric or rendered keys."""
        flattened = cls._flattened(stored, rows)
        if flattened is None or not wanted:
            return {}
        parents, entries, values = flattened
        compute = pyarrow.compute
        tags = compute.struct_field(entries, "tag")
        keys = compute.utf8_lower(compute.struct_field(entries, "key"))
        numbered = [(index, tag) for index, (tag, _) in enumerate(wanted) if tag]
        if numbered:
            positions, tag_values = zip(*numbered, strict=True)
            by_tag = compute.take(
                pyarrow.array(positions, pyarrow.int32()),
                compute.index_in(tags, value_set=pyarrow.array(tag_values, tags.type)),
            )
        else:
            by_tag = pyarrow.nulls(len(tags), pyarrow.int32())
        by_name = compute.index_in(
            keys,
            value_set=pyarrow.array([name.casefold() for _, name in wanted], pyarrow.string()),
        )
        matched_positions = compute.coalesce(by_tag, by_name)
        matches = compute.is_valid(matched_positions)
        if not compute.any(matches, min_count=0).as_py():
            return {}
        matched_positions = compute.filter(matched_positions, matches)
        matched_parents = compute.filter(parents, matches)
        matched_values = compute.filter(values, matches)
        row_ids = sequence(rows)
        found = {}
        for position in compute.unique(matched_positions).to_pylist():
            at = compute.equal(matched_positions, position)
            found[wanted[position][1]] = cls._first_per_row(
                compute.filter(matched_values, at), compute.filter(matched_parents, at), row_ids
            )
        return found

    @staticmethod
    def _flattened(stored: Any, rows: int) -> tuple[Any, Any, Any] | None:
        """`(parents, entries, values)` of a stored column; None where it holds none."""
        if isinstance(stored, pyarrow.ChunkedArray):
            stored = stored.combine_chunks()
        if not rows or stored is None or stored.null_count == rows:
            return None
        compute = pyarrow.compute
        entries = compute.list_flatten(stored)
        return (
            compute.list_parent_indices(stored).cast(pyarrow.int32()),
            entries,
            compute.struct_field(entries, "value"),
        )

    @staticmethod
    def _first_per_row(values: Any, parents: Any, row_ids: pyarrow.Array) -> Any:
        """One value per row out of entries already cut down to the matches."""
        return pyarrow.compute.take(values, pyarrow.compute.index_in(row_ids, value_set=parents))


def _KEY_TAIL(spelling: str) -> str | int:
    """The name segment a dictionary record is asked for, index stripped."""
    match = _KEY.match(spelling)
    if match is None:
        return spelling
    return match.group("name") or spelling
