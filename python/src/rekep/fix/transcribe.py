"""A log line's pairs as the tags FIX gave them -- as far as the dictionary goes."""

from __future__ import annotations

import dataclasses
import functools
import re
from collections.abc import Iterable, Mapping, Sequence
from functools import cached_property
from types import MappingProxyType
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.entries import ENTRIES, ENTRY_PARTS, TAG, Entry
from rekep.entries import IS_TAG as _IS_TAG
from rekep.enums import Protocol
from rekep.fields import Field, column_name, column_names, encoded_key
from rekep.fields.arrays import build_list, dense_counts, groups_of, scattered, sequence
from rekep.fix.columns import COLUMNS as FLAT_COLUMNS
from rekep.fix.columns import DECLARATIONS as FLAT_DEFAULTS
from rekep.fix.columns import (
    NAMESPACE_COLUMNS,
    QUOTE_GROUP_COUNTS,
    QUOTE_GROUP_STRUCTURE,
    named_columns,
)
from rekep.fix.columns import TYPES as FLAT_TYPES
from rekep.fix.components import (
    ComponentGroup,
    Legs,
    Parties,
    SecurityAltIDs,
    SideTrdRegTimestamps,
    TrdRegTimestamps,
    indexed_component_paths,
)
from rekep.fix.fields import FieldRule, FieldRules, cast_arrow_field, cast_arrow_fix
from rekep.fix.message import (
    _MEMBER_NAME_VECTOR,
    MARKED_SEPARATOR_VECTOR,
    MARKER,
    NAMED_SEPARATOR_VECTOR,
    RENDERED_SEPARATOR_VECTOR,
    SEPARATOR_VECTOR,
    SEPARATORS,
    SOH,
    XML_DATA_TAG,
    _glued_group_boundaries,
    carries_message,
    parse_arrow_array,
    parse_entries_array,
    stored_entry_separators,
)
from rekep.fix.quickfix import entry_of, members_of
from rekep.fix.registry import FixRegistry
from rekep.fix.rekep import REKEP_TAGS
from rekep.fix.rules import Rules

#: `XmlData <213>` as a rendered key and as a wire tag, which are the two ways
#: a line writes the field whose payload is another message.
_XML_DATA_KEY = FLAT_DEFAULTS[XML_DATA_TAG].fix.canonical
_XML_DATA_NAME = pyarrow.scalar(column_name(_XML_DATA_KEY))
_XML_DATA_TAG = pyarrow.scalar(str(XML_DATA_TAG))

# Package tags apply to every protocol version, including an unversioned UL
# row. Their frozen identities say that directly now their names are ordinary.
_REKEP_TAG_VALUES = frozenset(REKEP_TAGS.values())

#: What a payload writes between its fields. Neither of the two things
#: `separators_of` reads -- a BeginString or a `#` -- is inside one, so this
#: reads the character between the first `name=value` and the next `name=`,
#: which is the same rule `MARKED_SEPARATOR_VECTOR` applies to a marked line.
#: `\^A` before `.`, or a caret-A payload reads its separator as `^` and every
#: key after the first comes back with an `A` glued to the front.
_PAYLOAD_SEPARATOR = r"(?s)[A-Za-z0-9_.\-]+[ \t]*=.*?(?P<sep>\^A|.)[A-Za-z0-9_.\-]+[ \t]*="

# The parser keeps its efficient map-shaped intermediate private; a stored
# field is a list entry, because repeated keys are data and not a broken map.
_RAW_PAIRS: pyarrow.DataType = pyarrow.map_(
    pyarrow.string(), pyarrow.field("value", pyarrow.string(), nullable=False)
)

#: The container a rendered key sits directly inside, with its entry index
#: dropped: `Instrument.NoLegs[0].LegSymbol` sits in `NoLegs`, `TECH.CLIENTID`
#: in `TECH`, and `Side` in nothing at all.
_CONTAINED_KEY = r"(?s)^(?:.*\.)?(?P<inner>[^.]*?)(?:\[[0-9]+\])?\.[^.]*$"

#: The scalar compilations of the vectorized key rules, from the same source
#: strings, so `TagIndex.resolve_key` and `TagIndex.resolve_with_match` cannot
#: disagree on what a key means. `re.ASCII` because RE2's classes are.
_IS_TAG_SCALAR = re.compile(_IS_TAG, re.ASCII)
_CONTAINED_KEY_SCALAR = re.compile(_CONTAINED_KEY, re.ASCII)
_MEMBER_NAME_SCALAR = re.compile(_MEMBER_NAME_VECTOR, re.ASCII)

#: Value spellings that mean **there is no value**. A bridge that has nothing
#: to say for a field says it in whichever of these its renderer prefers, and
#: they are not values: `ACCOUNT=<null>` is an absent account, and storing the
#: literal text makes every consumer downstream re-implement this same check --
#: differently, and one of them wrong.
#:
#: Matched case-blind and after trimming, because the spelling drifts with the
#: renderer and the padding with the log format. Configuration, not a rule: a
#: feed whose `n/a` really is a value passes its own set, and an empty set
#: keeps every pair.
NULL_VALUES: frozenset[str] = frozenset({"", "null", "<null>", "n/a", "none"})

#: Where an inferred version came from. Unknown evidence stays distinct from
#: either transport or application evidence.
BEGIN_STRING_SOURCE = "begin_string"
APPLICATION_VERSION_SOURCE = "application_version"
PROTOCOL_SOURCE = "protocol"
REGISTRY_LATEST_SOURCE = "registry_latest"
NO_SOURCE = "none"

_APPL_VERSIONS = {
    "2": "4.0",
    "3": "4.1",
    "4": "4.2",
    "5": "4.3",
    "6": "4.4",
    "7": "5.0",
    "8": "5.0.SP1",
    "9": "5.0.SP2",
}
_APPL_VERSION_KEYS = pyarrow.array(list(_APPL_VERSIONS), pyarrow.string())
_APPL_VERSION_VALUES = pyarrow.array(list(_APPL_VERSIONS.values()), pyarrow.string())
_BEGIN_KEYS = pyarrow.array(["8", "beginstring"], pyarrow.string())
_APPLICATION_KEYS = pyarrow.array(["1128", "applverid"], pyarrow.string())
_DEFAULT_APPLICATION_KEYS = pyarrow.array(["1137", "defaultapplverid"], pyarrow.string())

_GLUED_MARKER = "\x1eREKEP_GROUP\x1f"
_GLUED_MEMBER = r"(?s)^(?P<key>[^=]+)=(?P<value>.*)$"


@dataclasses.dataclass(frozen=True)
class TagIndex:
    """One FIX version's names as an Arrow value set, and the tags behind it."""

    #: Every name the version knows, folded. Folded *here* so the probe is one
    #: kernel and never a scan; `pairs` keeps the log's own spelling.
    names: pyarrow.Array

    #: The tag behind each name, in the same order.
    tags: pyarrow.Array

    #: Every name a dotted key may sit *inside*: a component, a group, a field.
    #: What tells `NoPartyIDs[0].PartyID` -- `PartyID` in a group this version
    #: declares -- from `TECH.CLIENTID`, which is a vendor's own field and not
    #: `ClientID <109>` wearing a prefix.
    containers: pyarrow.Array = dataclasses.field(
        default_factory=lambda: pyarrow.array([], pyarrow.string())
    )

    @classmethod
    def from_tags(cls, tags: Mapping[str, int], containers: Iterable[str] = ()) -> TagIndex:
        """An index out of `FixRegistry.tags()`; an empty one resolves nothing."""
        names = {column_name(name): tag for name, tag in tags.items()}
        inside = dict.fromkeys(column_name(name) for name in (*tags, *containers))
        return cls(
            names=pyarrow.array(list(names), pyarrow.string()),
            tags=pyarrow.array(list(names.values()), TAG),
            containers=pyarrow.array(list(inside), pyarrow.string()),
        )

    # -- the same rules, one key at a time ------------------------------------
    #
    # `resolve_with_match` is the rule table; these read it scalar-wise off the
    # same data and the same pattern sources, so the two executions cannot
    # drift. `FieldAccess` (fix/access.py) is the caller.

    @functools.cached_property
    def _tags_by_name(self) -> Mapping[str, int]:
        """The vectorized value sets as one scalar lookup, built once."""
        return MappingProxyType(
            {
                name: tag
                for name, tag in zip(self.names.to_pylist(), self.tags.to_pylist(), strict=True)
            }
        )

    @functools.cached_property
    def _tag_set(self) -> frozenset[int]:
        return frozenset(self.tags.to_pylist())

    @functools.cached_property
    def _container_set(self) -> frozenset[str]:
        return frozenset(self.containers.to_pylist())

    def resolve_key(self, key: str) -> tuple[int | None, bool, str, bool]:
        """One key under `resolve_with_match`'s rules: (tag, hit, name, contained)."""
        if _IS_TAG_SCALAR.match(key) is not None:
            tag = int(key)
            return tag, tag in self._tag_set, key, True
        member = _MEMBER_NAME_SCALAR.search(key)
        reduced = member.group("name") if member is not None else ""
        contained = self._contained_key(key)
        if not contained:
            reduced = key
        if _IS_TAG_SCALAR.match(reduced) is not None:
            tag = int(reduced)
            return tag, tag in self._tag_set, reduced, contained
        tag = self._tags_by_name.get(column_name(reduced))
        return tag, tag is not None, reduced, contained

    def _contained_key(self, key: str) -> bool:
        """`_contained`, one key at a time: the immediate container decides."""
        found = _CONTAINED_KEY_SCALAR.match(key)
        inner = found.group("inner") if found is not None else None
        if not inner:
            return True
        return column_name(inner) in self._container_set

    def resolve(self, keys: Any) -> pyarrow.Array:
        """A key column as tag numbers, null where no reading finds one."""
        return self.resolve_with_match(keys)[0]

    def resolve_with_match(self, keys: Any) -> tuple[Any, Any, Any, Any]:
        """Resolved tags, registry hits, terminal names, and containment.

        Four answers because they are one scan: whether a key sits inside a
        container this version declares decides whether its tail may be
        resolved, and computing that twice would read the same column through
        the same regex twice.

        Scanned once per **distinct** spelling and taken back across the
        entries: a message keys its fields out of a bounded vocabulary, so a
        batch of a hundred thousand entries carries a few dozen spellings. 25x
        on a captured bridge batch, where every key is a name and none of the
        fast paths below fire (benchmarks/bench_text_file.py).
        """
        compute = pyarrow.compute
        if isinstance(keys, pyarrow.ChunkedArray):
            keys = keys.combine_chunks()
        encoded = keys.dictionary_encode()
        indices = encoded.indices
        return tuple(
            compute.take(one, indices) for one in self._resolve_distinct(encoded.dictionary)
        )

    def _resolve_distinct(self, keys: Any) -> tuple[Any, Any, Any, Any]:
        """`resolve_with_match` over one column of distinct spellings."""
        compute = pyarrow.compute
        plain = compute.fill_null(compute.match_substring_regex(keys, _IS_TAG), False)
        if compute.all(plain, min_count=0).as_py():
            # Every key is already a tag, which is what a wire message is made
            # of and so what most of a capture's pairs are. One cast, and none
            # of the name machinery below is touched -- measured at roughly a
            # third of what the general path costs on the same column.
            resolved = keys.cast(TAG)
            return (
                resolved,
                compute.fill_null(compute.is_in(resolved, value_set=self.tags), False),
                keys,
                # A bare tag stands inside nothing, which is the same answer
                # `_contained` gives a key with no dot in it.
                pyarrow.repeat(True, len(keys)),
            )
        reduced = compute.fill_null(
            compute.struct_field(compute.extract_regex(keys, _MEMBER_NAME_VECTOR), "name"), ""
        )
        # A dotted key gives up its tail only when the container it names is
        # one this version has. `TECH.CLIENTID` is a vendor's own field, not
        # `ClientID <109>` wearing a prefix, and reading it as one files an
        # enrichment value under a standard tag it has nothing to do with.
        contained = self._contained(keys)
        reduced = compute.if_else(contained, reduced, keys)
        numeric = compute.fill_null(compute.match_substring_regex(reduced, _IS_TAG), False)
        # Cast the whole column rather than a filtered subset: a non-numeric
        # key is replaced by a digit that casts, and the `if_else` after throws
        # it away. Filter-and-scatter costs two more kernels than the waste.
        as_tag = compute.if_else(numeric, reduced, pyarrow.scalar("0")).cast(TAG)
        name_index = compute.index_in(column_names(reduced), value_set=self.names)
        by_name = compute.take(self.tags, name_index)
        resolved = compute.if_else(numeric, as_tag, by_name)
        matched = compute.if_else(
            numeric,
            compute.fill_null(compute.is_in(as_tag, value_set=self.tags), False),
            compute.is_valid(name_index),
        )
        return resolved, matched, reduced, contained

    def _contained(self, keys: Any) -> pyarrow.Array:
        """Whether each key sits inside something this version declares.

        True for a key with nothing in front of it, which is most of them.
        Otherwise the *immediate* container decides -- the segment nearest the
        field -- because that is the one that says what the field is a member
        of; anything further out only says where that container came from.
        """
        compute = pyarrow.compute
        inner = compute.fill_null(
            compute.struct_field(compute.extract_regex(keys, _CONTAINED_KEY), "inner"), ""
        )
        plain = compute.equal(inner, "")
        if not len(self.containers) or compute.all(plain, min_count=0).as_py():
            return plain
        known = compute.fill_null(
            compute.is_in(column_names(inner), value_set=self.containers), False
        )
        return compute.or_(plain, known)


#: What `field_rules` answers for a codec no job declared anything for.
_NO_RULES: Mapping[int, FieldRule] = MappingProxyType({})


def _as_array(column: Any, rows: int) -> Any:
    """One column as a plain Array, whatever the batch handed over."""
    if column is None:
        return pyarrow.nulls(rows, pyarrow.string())
    if isinstance(column, pyarrow.ChunkedArray):
        return column.combine_chunks()
    return column


@dataclasses.dataclass(eq=False)
class FixCodec(Convertible):
    """A log line read as FIX: which protocol it is, its pairs, and its tags."""

    #: Which category each line is. `Rules.into_default()` unless a
    #: document says otherwise.
    rules: Rules = dataclasses.field(default_factory=Rules)

    #: The dictionary names resolve through: the packaged registry unless
    #: `FixRegistry.set_builtin` installed another. A registry serving a store
    #: never scrapes, because a parse that met its first bridge line and
    #: answered it by fetching fourteen thousand pages mid-batch would be a
    #: worse surprise than an unresolved name. Pass a complete store only
    #: when a deployment intentionally replaces the packaged registry.
    registry: FixRegistry = dataclasses.field(default_factory=FixRegistry.from_builtin)

    #: Values that mean the field is absent, dropped from the pairs before
    #: anything else looks at them. Empty keeps every pair.
    null_values: frozenset[str] = NULL_VALUES

    #: How named fields read, where a job says something the dictionary does
    #: not: a vendor tag that carries an instant, a date a feed writes as text,
    #: a value spelling only this estate uses. One declaration reaches every
    #: reading of that field, because every one of them resolves through
    #: `tag_field` and casts through `cast_arrow_fix`.
    fields: FieldRules = dataclasses.field(default_factory=FieldRules)

    # -- the seam -----------------------------------------------------------

    def into_pairs(self, messages: Any, protocol: Protocol | str | int = Protocol.OTHER) -> Any:
        """One `map<string, string>` per row: the message as the line spells it."""
        return self.complete_pairs(self.into_raw_pairs(messages, protocol), protocol)

    def into_pairs_from_entries(
        self, entries: Any, protocol: Protocol | str | int = Protocol.OTHER
    ) -> Any:
        """Apply one protocol rule to generic arguments without reading text again."""
        rows = len(entries)
        rule = self.rules.rule(protocol)
        if rule.named is None:
            return pyarrow.nulls(rows, _RAW_PAIRS)
        if rule.entry_separator is not None:
            return parse_entries_array(
                entries,
                named=rule.named,
                entry_separator=rule.entry_separator,
            ).cast(_RAW_PAIRS)

        groups = (
            list(groups_of(stored_entry_separators(entries, rule.extra_entry_separators)))
            if rule.named
            else []
        )
        if len(groups) <= 1:
            entry_separator = groups[0][0].as_py() or None if groups else None
            return parse_entries_array(
                entries,
                named=rule.named,
                entry_separator=entry_separator,
            ).cast(_RAW_PAIRS)
        parts, positions = [], []
        for entry_separator, where in groups:
            parts.append(
                parse_entries_array(
                    pyarrow.compute.take(entries, where),
                    named=rule.named,
                    entry_separator=entry_separator.as_py() or None,
                ).cast(_RAW_PAIRS)
            )
            positions.append(where)
        return scattered(parts, positions)

    def complete_pairs(
        self,
        pairs: Any,
        protocol: Protocol | str | int = Protocol.OTHER,
    ) -> Any:
        """Apply the shared payload, null and protocol-replacement boundary."""
        completed = self.drop_null_values(self.into_payload_pairs(pairs))
        return _popped_pairs(completed, self.rules.rule(protocol).pop)

    def into_payload_pairs(self, pairs: Any) -> Any:
        """`XmlData <213>` read as the message it carries, where it carries one.

        The standard calls tag 213 an XML data stream; real FIXML traffic puts
        a `key=value` message in it. A payload that reads as pairs becomes
        pairs under `XmlData.<key>`, in the place the tag sat, and resolves
        like any other nested key; one that reads as XML, or as nothing, stays
        exactly as it was.
        """
        if isinstance(pairs, pyarrow.ChunkedArray):
            parts = [self.into_payload_pairs(chunk) for chunk in pairs.chunks]
            return pyarrow.chunked_array(parts, type=_RAW_PAIRS)
        compute = pyarrow.compute
        if not len(pairs) or pairs.null_count == len(pairs):
            return pairs
        lengths, keys, items = _entries_of(pairs)
        if not len(keys):
            return pairs
        carried = compute.or_(
            compute.equal(keys, _XML_DATA_TAG),
            compute.equal(column_names(compute.utf8_trim_whitespace(keys)), _XML_DATA_NAME),
        )
        if not compute.any(carried, min_count=0).as_py():
            # Nearly every batch. Two string compares over the keys settle it,
            # and the regexes below -- which read *values*, the expensive half
            # of any pass here -- never run.
            return pairs
        payloads = compute.filter(items, carried)
        reads = carries_message(payloads)
        readable = _scattered_mask(carried, reads)
        if not compute.any(readable, min_count=0).as_py():
            return pairs
        parsed = _payload_pairs(compute.filter(payloads, reads))
        counts, inner = _payload_counts(readable, parsed)
        if inner is None:
            return pairs
        taken, rank = _repeated(counts)
        expanded = compute.greater(compute.take(counts, taken), 1)
        starts = compute.take(_payload_starts(readable, parsed), taken)
        where = compute.add(starts, rank)
        return _mapped(
            pairs,
            _row_totals(pairs, lengths, counts),
            pyarrow.repeat(True, len(taken)),
            compute.if_else(expanded, compute.take(inner[0], where), compute.take(keys, taken)),
            compute.if_else(expanded, compute.take(inner[1], where), compute.take(items, taken)),
            _RAW_PAIRS,
        )

    def into_raw_pairs(self, messages: Any, protocol: Protocol | str | int = Protocol.OTHER) -> Any:
        """Parsed pairs before configured null spellings are removed."""
        compute = pyarrow.compute
        if not len(messages):
            return pyarrow.array([], type=_RAW_PAIRS)
        if isinstance(messages, pyarrow.ChunkedArray):
            messages = messages.combine_chunks()
        rule = self.rules.rule(protocol)
        if rule.named is None:
            return pyarrow.nulls(len(messages), _RAW_PAIRS)
        if rule.separator is not None:
            return parse_arrow_array(
                messages,
                rule.separator,
                named=rule.named,
                entry_separator=rule.entry_separator,
                extra_entry_separators=rule.extra_entry_separators,
            )
        # `parse_arrow_array` samples a column **once** and reads every row of
        # it that way -- which is right, and which is why a category is not a
        # fine enough slice on its own: one FIX session writes `|` and the next
        # writes SOH, and a capture holds both, so a single sample would read
        # one of them as a message of one field. The rows that share a
        # separator are parsed together and put back where they were.
        groups = list(groups_of(self.separators_of(messages, rule.named)))
        if len(groups) == 1:
            # One separator down the whole slice, which is every capture that
            # holds one session. Handed over as it stands rather than through a
            # `take` of every row, because that copy is the whole column.
            return parse_arrow_array(
                messages,
                named=rule.named,
                entry_separator=rule.entry_separator,
                extra_entry_separators=rule.extra_entry_separators,
            )
        parts, positions = [], []
        for _, where in groups:
            parts.append(
                parse_arrow_array(
                    compute.take(messages, where),
                    named=rule.named,
                    entry_separator=rule.entry_separator,
                    extra_entry_separators=rule.extra_entry_separators,
                )
            )
            positions.append(where)
        return scattered(parts, positions)

    def separators_of(self, messages: Any, named: bool | None = None) -> pyarrow.Array:
        """What each line writes between its fields, read off the line itself."""
        compute = pyarrow.compute
        text = messages.cast(pyarrow.string(), safe=False)
        found = None
        if named is not True:
            found = compute.struct_field(compute.extract_regex(text, SEPARATOR_VECTOR), "sep")
        if named is not False:
            named_begin = compute.struct_field(
                compute.extract_regex(text, NAMED_SEPARATOR_VECTOR), "sep"
            )
            found = named_begin if found is None else compute.coalesce(found, named_begin)
            bridge = compute.struct_field(
                compute.extract_regex(text, MARKED_SEPARATOR_VECTOR), "sep"
            )
            bridge = compute.if_else(
                compute.is_in(bridge, value_set=pyarrow.array(SEPARATORS, pyarrow.string())),
                bridge,
                pyarrow.scalar(MARKER),
            )
            found = bridge if found is None else compute.coalesce(found, bridge)
            rendered = compute.struct_field(
                compute.extract_regex(text, RENDERED_SEPARATOR_VECTOR), "sep"
            )
            found = rendered if found is None else compute.coalesce(found, rendered)
        return compute.fill_null(found, "")

    def drop_null_values(self, pairs: Any) -> Any:
        """`pairs` without the fields that have no value."""
        if isinstance(pairs, pyarrow.ChunkedArray):
            return pyarrow.chunked_array(
                [self.drop_null_values(chunk) for chunk in pairs.chunks], type=_RAW_PAIRS
            )
        compute = pyarrow.compute
        lengths, keys, items = _entries_of(pairs)
        keep = compute.is_valid(items)
        if self.null_values:
            absent = compute.is_in(
                compute.utf8_lower(compute.utf8_trim_whitespace(items)),
                value_set=pyarrow.array(sorted(self.null_values), pyarrow.string()),
            )
            keep = compute.and_(keep, compute.invert(compute.fill_null(absent, False)))
        if compute.all(keep, min_count=0).as_py() and pyarrow.types.is_map(pairs.type):
            return pairs.cast(_RAW_PAIRS)
        return _mapped(
            pairs,
            lengths,
            keep,
            compute.filter(keys, keep),
            compute.filter(items, keep),
            _RAW_PAIRS,
        )

    def into_entries(self, pairs: Any, version: str | None = None) -> Any:
        """Every field a message carried, as the one entry a parsed log stores.

        One column whatever the dictionary made of the field, so a consumer
        that wants "the fields of this line" reads it in wire order and one
        that wants only the resolved ones filters on `tag` -- `0` where
        nothing answered for the name.
        """
        rows = len(pairs)
        if isinstance(pairs, pyarrow.ChunkedArray):
            parts = [self.into_entries(chunk, version) for chunk in pairs.chunks]
            return pyarrow.chunked_array(parts, type=ENTRIES)
        if not rows or pairs.null_count == rows:
            # Every row of this slice is "not a message", which is most of a
            # capture, and the kernels below would run over an empty child
            # array to establish it.
            return pyarrow.nulls(rows, ENTRIES)
        lengths, keys, values = _entries_of(pairs)
        return _entries_column(
            pairs,
            lengths,
            pyarrow.repeat(True, len(keys)),
            *self.transcribe(keys, values, version),
        )

    def transcribe(self, keys: Any, values: Any, version: str | None = None) -> tuple[Any, ...]:
        """Resolved stored members for a run of parsed fields.

        The whole of what the dictionary adds to a field, in kernels and in one
        place, so nothing downstream resolves a name or reads an enumeration a
        second way. A group member keeps its indexed path in `comp`; every
        other spelling stays whole in `key`.
        """
        compute = pyarrow.compute
        tags, _, _, _ = self.index_of(version).resolve_with_match(keys)
        # The split is `structure`'s, not a second rule: where a field stood is
        # a fact about the spelling, so the message stage settles it and the
        # dictionary never revises it. What the dictionary adds is the tag.
        _, key, value, comp = self.structure(keys, values)
        return (
            compute.fill_null(tags, pyarrow.scalar(0, TAG)),
            key,
            value,
            comp,
        )

    # -- the message stage ----------------------------------------------------
    #
    # Structuration without the dictionary: what a line spells, cut into the
    # same struct the resolved rows use. `parse_fix_*` completes the same column
    # in place rather than converting a shape.

    def into_message_entries(self, pairs: Any) -> Any:
        """Every field a message carried, structured but not resolved.

        The same `ENTRIES` struct at its unresolved fill level: `key`, `value`
        and `comp` are what the line spells, and `tag` is the number only where
        the line spelled one -- `0` otherwise. No name is looked up and no
        value is translated, so this needs no dictionary.
        """
        rows = len(pairs)
        if isinstance(pairs, pyarrow.ChunkedArray):
            parts = [self.into_message_entries(chunk) for chunk in pairs.chunks]
            return pyarrow.chunked_array(parts, type=ENTRIES)
        if not rows or pairs.null_count == rows:
            return pyarrow.nulls(rows, ENTRIES)
        lengths, keys, values = _entries_of(pairs)
        return _entries_column(
            pairs,
            lengths,
            pyarrow.repeat(True, len(keys)),
            *self.structure(keys, values),
        )

    def structure(self, keys: Any, values: Any) -> tuple[Any, ...]:
        """Stored argument members from the spelling alone.

        The dictionary-free half of `transcribe`, and the same split: an
        indexed group path goes to `comp`; every other spelling stays whole in
        `key`.
        """
        return Entry.structure_arrow(keys, values)

    def versions_of_entries(
        self,
        entries: Any,
        begin_strings: Any,
        application_versions: Any = None,
        protocols: Any = None,
    ) -> tuple[Any, Any]:
        """`(version, where it came from)` per row, off the structured fields.

        Off `entries` rather than off the message, because by this point the
        message has been split once and splitting it again is the work this
        stage exists to stop paying twice. Reads only `registry.versions` --
        the version list -- and no field, component or enumerated value.

        `begin_strings` and `application_versions` are the columns the raw
        stage lifted `BeginString <8>` and `ApplVerID <1128>` into. Each leads
        and `entries` fills it, which is the one rule every lifted column is
        read under: a null column and a column a projection dropped are the
        same absence, and the tag is still in the list either way. Both are
        stated rather than defaulted, so a caller says which it is handing
        over -- and `ApplVerID` has to be one of them, because under FIXT it
        is the version, and a transport row whose only evidence was lifted
        out of `entries` would resolve to nothing.
        """
        from rekep.fix.access import FieldAccess

        rows = len(entries)
        if not rows:
            empty = pyarrow.array([], pyarrow.string())
            return empty, empty
        lifted = FieldAccess.first_named(entries, 8, "BeginString", rows)
        begins = (
            lifted
            if begin_strings is None
            else pyarrow.compute.coalesce(
                _as_array(begin_strings, rows).cast(pyarrow.string(), safe=False), lifted
            )
        )
        held = FieldAccess.first_named(entries, 1128, "ApplVerID", rows)
        application = (
            held
            if application_versions is None
            else pyarrow.compute.coalesce(
                _as_array(application_versions, rows).cast(pyarrow.string(), safe=False), held
            )
        )
        default = FieldAccess.first_named(entries, 1137, "DefaultApplVerID", rows)
        compute = pyarrow.compute
        version_keys, version_values = self._version_lookup

        def registered(evidence: Any) -> Any:
            return compute.take(
                version_values,
                compute.index_in(_version_keys_arrow(evidence), value_set=version_keys),
            )

        def applied(evidence: Any) -> Any:
            direct = compute.take(
                _APPL_VERSION_VALUES,
                compute.index_in(
                    compute.utf8_trim_whitespace(evidence), value_set=_APPL_VERSION_KEYS
                ),
            )
            return compute.coalesce(direct, registered(evidence))

        begin_keys = _version_keys_arrow(begins)
        fixt = compute.fill_null(compute.match_substring_regex(begin_keys, r"^FIXT"), False)
        application_version = compute.if_else(
            compute.is_valid(application), applied(application), applied(default)
        )
        versions = compute.if_else(fixt, application_version, registered(begins))
        sources = compute.if_else(
            compute.is_valid(versions),
            compute.if_else(fixt, APPLICATION_VERSION_SOURCE, BEGIN_STRING_SOURCE),
            NO_SOURCE,
        )
        if protocols is not None:
            embedded = self._protocol_versions(protocols, rows)
            sources = compute.if_else(compute.is_valid(embedded), PROTOCOL_SOURCE, sources)
            versions = compute.coalesce(embedded, versions)
        default_version = self._latest_version
        if protocols is not None and default_version is not None:
            families = Protocol.into_family_arrow(_as_array(protocols, rows))
            unstated = _all_absent(begins, application, default)
            selected = compute.and_(
                compute.equal(families, Protocol.UL.into_stored()),
                compute.and_(compute.is_null(versions), unstated),
            )
            versions = compute.if_else(selected, default_version, versions)
            sources = compute.if_else(selected, REGISTRY_LATEST_SOURCE, sources)
        return versions, sources

    def into_versioned_protocols(
        self,
        entries: Any,
        begin_strings: Any,
        application_versions: Any,
        protocols: Any,
    ) -> pyarrow.Array:
        """Protocol codes carrying their authoritative registry version."""
        versions, _ = self.versions_of_entries(
            entries,
            begin_strings,
            application_versions,
            protocols,
        )
        resolved = Protocol.with_versions_arrow(protocols, versions)
        if begin_strings is None:
            return resolved
        # A valid wire version remains source data before this registry can
        # type its fields. Resolved and embedded application versions lead;
        # BeginString only fills rows for which neither supplied an answer.
        public_versions = pyarrow.compute.coalesce(
            Protocol.into_versions_arrow(resolved),
            _as_array(begin_strings, len(resolved)).cast(pyarrow.string(), safe=False),
        )
        return Protocol.with_versions_arrow(resolved, public_versions)

    def complete_entries(self, entries: Any, version: str | None = None) -> Any:
        """A message-stage `entries` column, resolved the rest of the way.

        A fill and not a shape conversion. Three members are filled -- `tag`,
        `key` canonicalized to the registry's spelling, `value` translated
        where its field enumerates its values -- while `comp` comes through
        unchanged.
        """
        rows = len(entries)
        if isinstance(entries, pyarrow.ChunkedArray):
            parts = [self.complete_entries(chunk, version) for chunk in entries.chunks]
            return pyarrow.chunked_array(parts, type=ENTRIES)
        if not rows or entries.null_count == rows or version is None:
            return entries
        compute = pyarrow.compute
        lengths, _, items = _flattened(entries)
        stored = compute.struct_field(items, "tag")
        keys = compute.struct_field(items, "key")
        values = compute.struct_field(items, "value")
        comp = compute.struct_field(items, "comp")
        # A stored key is the field's own name with its container beside it, so
        # the container goes back in front before it is resolved: that is the
        # spelling `resolve_with_match` reads, and `TECH.CLIENTID` must not
        # resolve as `CLIENTID`.
        whole = compute.if_else(
            compute.is_valid(comp),
            compute.binary_join_element_wise(compute.fill_null(comp, ""), keys, "."),
            keys,
        )
        tags, matched, _, _ = self.index_of(version).resolve_with_match(whole)
        # Only an unresolved entry is filled: one the message stage already
        # numbered was numbered off the wire, and the wire is the authority.
        fill = compute.and_(compute.equal(stored, 0), compute.fill_null(matched, False))
        tags = compute.if_else(fill, compute.fill_null(tags, pyarrow.scalar(0, TAG)), stored)
        return _entries_column(
            entries,
            lengths,
            pyarrow.repeat(True, len(keys)),
            tags,
            self._canonical(keys, tags, version),
            self._encoded(tags, values, version),
            comp,
        )

    def _canonical(self, keys: Any, tags: Any, version: str | None) -> Any:
        """Each key as the registry spells it, where the registry answers for it."""
        compute = pyarrow.compute
        spelled, named = self._canonical_names(version)
        if not len(spelled):
            return keys
        found = compute.take(named, compute.index_in(tags, value_set=spelled))
        return compute.if_else(compute.is_valid(found), found, keys)

    def _encoded(self, tags: Any, values: Any, version: str | None) -> Any:
        """Each value as the dictionary reads its spelling, where it enumerates any."""
        compute = pyarrow.compute
        spelled, resolved = self._encodings(version)
        if not len(spelled):
            return values
        composite = compute.binary_join_element_wise(
            compute.fill_null(tags, 0).cast(pyarrow.string()),
            column_names(compute.fill_null(values, "")),
            "\x00",
        )
        found = compute.take(resolved, compute.index_in(composite, value_set=spelled))
        return compute.if_else(compute.is_valid(found), found, values)

    def into_wire_values(self, field: int | str, values: Any, version: str | None) -> Any:
        """One promoted FIX field's values in their canonical wire spelling.

        Promoted session fields no longer sit in `entries` when completion
        translates its values, so they pass through the same tag-scoped
        registry lookup explicitly here.
        """
        try:
            declared = self.registry.field(field, version)
        except (KeyError, OSError, ValueError):
            declared = None
        tag = None if declared is None else declared.fix.tag
        if tag is None or not declared.fix.encoded:
            return values
        source = values.combine_chunks() if isinstance(values, pyarrow.ChunkedArray) else values
        if not len(source) or source.null_count == len(source):
            return values
        if not (pyarrow.types.is_string(source.type) or pyarrow.types.is_large_string(source.type)):
            return values
        return self._encoded(
            pyarrow.repeat(pyarrow.scalar(int(tag), TAG), len(source)),
            source,
            version,
        )

    def _canonical_names(self, version: str | None) -> tuple[Any, Any]:
        """`(tag, the registry's spelling of it)` for one version, built once."""
        if version not in self._canonicals:
            self._canonicals[version] = _canonical_names(
                self.registry,
                version,
                self._supplemental_fields(version),
            )
        return self._canonicals[version]

    def _encodings(self, version: str | None) -> tuple[Any, Any]:
        """`(tag and folded spelling, the value it names)` for one reading.

        The job's own declared spellings lead the dictionary's, so a rule wins
        a collision: `index_in` takes the first occurrence of a value. A
        versionless promoted field uses the merged registry record; its tag
        already fixes which enumeration owns the value.
        """
        if version not in self._encoded_values:
            spelled, resolved = _encodings(
                self.registry,
                version,
                self._supplemental_fields(version),
            )
            declared = self._declared_encodings(version)
            if declared:
                spelled = pyarrow.concat_arrays(
                    [pyarrow.array([one for one, _ in declared], pyarrow.string()), spelled]
                )
                resolved = pyarrow.concat_arrays(
                    [pyarrow.array([one for _, one in declared], pyarrow.string()), resolved]
                )
            self._encoded_values[version] = (spelled, resolved)
        return self._encoded_values[version]

    def _declared_encodings(self, version: str | None) -> list[tuple[str, str]]:
        """`(tag and folded spelling, value)` for every declared encoding."""
        return [
            (f"{tag}\x00{encoded_key(spelling)}", str(value))
            for tag, rule in self.field_rules(version).items()
            for spelling, value in rule.values.items()
        ]

    def into_fixmsg_columns(
        self, pairs: Any, version: str | None = None
    ) -> tuple[Any, dict[str, Any]]:
        """One parsed pair column as the fields a log keeps and the columns it lifts."""
        entries = self.into_entries(pairs, version)
        components, entries = self.into_component_columns(entries, version)
        lifted, entries = self.into_lifted_columns(entries, version)
        return entries, {**components, **lifted}

    def into_lifted_columns(
        self, entries: Any, version: str | None = None
    ) -> tuple[dict[str, Any], Any]:
        """The fields worth a column of their own, and what is left of `entries`.

        Both kinds in one pass, because they are one question asked of one
        column: a numbered tag the log declares a column for, and a rendered
        name it does. A raw value stays beside its typed column only when that
        column cannot reproduce its exact spelling.
        """
        rows = len(entries)
        declared = self.named_fields()
        liftable = (
            declared
            if version is not None
            else {
                spelling: field
                for spelling, field in declared.items()
                if field.fix.tag in _REKEP_TAG_VALUES
            }
        )
        declared_by_tag = {
            int(field.fix.tag): field for field in liftable.values() if field.fix.tag is not None
        }
        columns: dict[str, Any] = {
            name: pyarrow.nulls(rows, FLAT_TYPES[tag]) for tag, name in FLAT_COLUMNS.items()
        }
        columns.update(
            (field.name, pyarrow.nulls(rows, field.dtype)) for field in declared.values()
        )
        if isinstance(entries, pyarrow.ChunkedArray):
            entries = entries.combine_chunks()
        if not rows or entries.null_count == rows:
            return columns, entries
        compute = pyarrow.compute
        # Package-owned columns belong to every protocol version. That is why
        # a bare UL document can lift `Unix`; unnumbered vendor fields still
        # need the version whose dictionary makes their identity clear.
        fields = {**self.flat_fields(version), **declared_by_tag}
        lengths, parents, items = _flattened(entries)
        tags = compute.struct_field(items, "tag")
        keys = compute.struct_field(items, "key")
        values = compute.struct_field(items, "value")

        # One integer per liftable field: its tag, or a negative code for a
        # rendered name. Distinct codes are all `_liftable` needs, and one
        # integer key spares the composite string a mixed column would take.
        numbered = compute.fill_null(compute.is_in(tags, value_set=_tags_of(fields)), False)
        named = pyarrow.array(list(liftable), pyarrow.string())
        matched = _declared_index(keys, _lead_of(items), named)
        named_outputs: list[Field] = []
        output_codes: dict[str, int] = {}
        output_ranks: dict[str, int] = {}
        named_codes: list[int] = []
        named_priorities: list[int] = []
        for field in liftable.values():
            output = field.name
            if output not in output_codes:
                output_codes[output] = -(len(named_outputs) + 1)
                named_outputs.append(field)
            named_codes.append(output_codes[output])
            named_priorities.append(output_ranks.get(output, 0))
            output_ranks[output] = output_ranks.get(output, 0) + 1
        wanted = numbered
        code = tags
        priority = pyarrow.repeat(pyarrow.scalar(0, pyarrow.int16()), len(tags))
        if len(named):
            rendered = compute.and_(compute.equal(tags, 0), compute.is_valid(matched))
            wanted = compute.or_(numbered, rendered)
            code = compute.if_else(
                rendered,
                compute.take(pyarrow.array(named_codes, TAG), matched),
                tags,
            )
            priority = compute.if_else(
                rendered,
                compute.take(pyarrow.array(named_priorities, pyarrow.int16()), matched),
                priority,
            )
        if not compute.any(wanted, min_count=0).as_py():
            return columns, entries
        agreed, chosen = _liftable(parents, code, values, priority)
        lift = compute.and_(wanted, agreed)
        taken = compute.and_(wanted, chosen)
        # Sorted once by which column an entry belongs to, so each column is a
        # *slice* of the run rather than two filters over the whole of it: a
        # filter per column walks every lifted entry once per column, which
        # is sixty passes over the batch where this is one sort and sixty
        # zero-copy slices -- 2.7x on a captured batch
        # (benchmarks/bench_text_file.py). The sort is stable, so parents stay
        # row ordered inside a column, which is what the shortcut below reads.
        order = compute.array_sort_indices(compute.filter(code, taken))
        codes = compute.take(compute.filter(code, taken), order)
        where = compute.take(compute.filter(parents, taken), order)
        selected_values = compute.take(compute.filter(values, taken), order)
        selected_identities = compute.add(
            compute.multiply(where, pyarrow.scalar(1 << 32, pyarrow.int64())),
            codes.cast(pyarrow.int64()),
        )
        retained_identities: list[pyarrow.Array] = []
        row_ids = sequence(rows)
        # `value_counts` answers in first-appearance order, and a sorted
        # column's first appearances are its groups, in order.
        at = 0
        for counted in compute.value_counts(codes).to_pylist():
            one, run = counted["values"], counted["counts"]
            raw = selected_values.slice(at, run)
            column_rows = where.slice(at, run)
            identities = selected_identities.slice(at, run)
            at += run
            if one >= 0:
                field = fields[one]
                column = cast_arrow_field(raw, field, FLAT_TYPES.get(one, field.dtype))
            else:
                field = named_outputs[-1 - one]
                column = cast_arrow_fix(raw, field.dtype)
            changed = _raw_spelling_changed(raw, column)
            if compute.any(changed, min_count=0).as_py():
                retained_identities.append(compute.filter(identities, changed))
            # Parent indices are row ordered and `_liftable` chose at most one
            # value per row, so covering every row already is the column.
            if run != rows:
                column = compute.take(column, compute.index_in(row_ids, value_set=column_rows))
            if one >= 0:
                columns[FLAT_COLUMNS.get(one, field.name)] = column
            else:
                columns[field.name] = column
        keep = compute.or_(compute.invert(lift), _quote_group_structure(parents, tags))
        if retained_identities:
            identities = compute.add(
                compute.multiply(parents, pyarrow.scalar(1 << 32, pyarrow.int64())),
                code.cast(pyarrow.int64()),
            )
            keep = compute.or_(
                keep,
                compute.is_in(
                    identities,
                    value_set=pyarrow.concat_arrays(retained_identities),
                ),
            )
        return columns, _entries_column(entries, lengths, keep, *_columns_of(items, keep))

    @classmethod
    @functools.cache
    def into_components(cls) -> Mapping[str, type[ComponentGroup]]:
        """`{column: the extractor that fills it}`, in the order they are applied.

        In order and against what the last one left: a member lifted into one
        component's entries cannot also be lifted into another's.
        """
        return MappingProxyType(
            {
                "parties": Parties,
                "trdregtimestamps": TrdRegTimestamps,
                "sidetrdregts": SideTrdRegTimestamps,
                "securityaltid": SecurityAltIDs,
                "legs": Legs,
            }
        )

    def into_component_columns(
        self, entries: Any, version: str | None = None
    ) -> tuple[dict[str, Any], Any]:
        """Structured FIX components and what is left of `entries`."""
        columns, rest, _ = self.into_component_columns_with_errors(entries, version)
        return columns, rest

    def into_component_columns_with_errors(
        self, entries: Any, version: str | None = None
    ) -> tuple[dict[str, Any], Any, Any]:
        """Structured components, residual entries and non-fatal group diagnostics."""
        columns: dict[str, Any] = {}
        rest = entries
        errors = pyarrow.nulls(len(entries), pyarrow.string())
        for column in self.into_components():
            columns[column], rest, found = self.component_of(
                column, version
            ).into_arrow_arrays_with_errors(rest)
            errors = _merge_group_errors(errors, found)
        return columns, rest, errors

    def split_group_entries(self, entries: Any, version: str | None) -> tuple[Any, Any]:
        """Expand separator-free indexed group values under one registry version."""
        if isinstance(entries, pyarrow.ChunkedArray):
            parts = [self.split_group_entries(chunk, version) for chunk in entries.chunks]
            return (
                pyarrow.chunked_array([found for found, _ in parts], type=ENTRIES),
                pyarrow.chunked_array([error for _, error in parts], type=pyarrow.string()),
            )
        rows = len(entries)
        declared = self._group_member_names(version)
        if not rows or entries.null_count == rows or not declared:
            return entries, pyarrow.nulls(rows, pyarrow.string())

        compute = pyarrow.compute
        lengths, parents, items = _flattened(entries)
        if not len(items):
            return entries, pyarrow.nulls(rows, pyarrow.string())
        tags = compute.struct_field(items, "tag")
        keys = compute.struct_field(items, "key")
        values = compute.struct_field(items, "value")
        components = compute.struct_field(items, "comp")
        if components.null_count == len(components):
            return entries, pyarrow.nulls(rows, pyarrow.string())
        # Read on the distinct paths and kept there: the loop below asks one
        # question per declared group, and asking it of a handful of spellings
        # is what makes the groups a batch does *not* carry cost nothing.
        view, at = indexed_component_paths(compute.fill_null(components, ""))
        groups = column_names(compute.struct_field(view, "group"))
        counts = pyarrow.repeat(pyarrow.scalar(1, pyarrow.int32()), len(items))
        expanded: list[tuple[Any, Any, Any, Any]] = []
        entry_errors = pyarrow.nulls(len(items), pyarrow.string())

        for folded, (display, members) in declared.items():
            named = compute.fill_null(compute.equal(groups, folded), False)
            if not compute.any(named, min_count=0).as_py():
                continue
            selected = compute.take(named, at)
            raw = compute.filter(values, selected)
            marked = compute.replace_substring_regex(
                raw,
                pattern=_glued_member_pattern(members),
                replacement=f"{_GLUED_MARKER}\\1=",
            )
            selected_parts = compute.split_pattern(marked, pattern=_GLUED_MARKER)
            selected_counts = compute.list_value_length(selected_parts).cast(pyarrow.int32())
            part_values = compute.list_flatten(selected_parts)
            _, part_rank = _repeated(selected_counts)
            readings = compute.extract_regex(part_values, _GLUED_MEMBER)
            part_keys = compute.if_else(
                compute.equal(part_rank, 0),
                pyarrow.scalar(None, pyarrow.string()),
                compute.struct_field(readings, "key"),
            )
            part_values = compute.if_else(
                compute.equal(part_rank, 0),
                part_values,
                compute.struct_field(readings, "value"),
            )
            counts = compute.if_else(
                selected,
                _scattered_int(selected, selected_counts),
                counts,
            )
            found_errors = _glued_ambiguity_errors(display, raw, members)
            entry_errors = compute.coalesce(
                entry_errors,
                _scattered_values(selected, found_errors),
            )
            expanded.append((selected, part_keys, part_values, selected_counts))

        if not expanded:
            return entries, pyarrow.nulls(rows, pyarrow.string())

        taken, rank = _repeated(counts)
        built_tags = compute.take(tags, taken)
        built_keys = compute.take(keys, taken)
        built_values = compute.take(values, taken)
        built_components = compute.take(components, taken)
        for selected, part_keys, part_values, _ in expanded:
            slots = compute.take(selected, taken)
            original_keys = compute.filter(built_keys, slots)
            replacement_keys = compute.coalesce(part_keys, original_keys)
            replacement_tags = compute.if_else(
                compute.equal(compute.filter(rank, slots), 0),
                compute.filter(built_tags, slots),
                pyarrow.scalar(0, TAG),
            )
            built_tags = compute.replace_with_mask(built_tags, slots, replacement_tags)
            built_keys = compute.replace_with_mask(built_keys, slots, replacement_keys)
            built_values = compute.replace_with_mask(built_values, slots, part_values)

        output = pyarrow.StructArray.from_arrays(
            [built_tags, built_keys, built_values, built_components],
            fields=list(ENTRIES.value_type),
        )
        expanded_entries = build_list(
            ENTRIES,
            _row_totals(entries, lengths, counts),
            output,
            mask=compute.is_null(entries) if entries.null_count else None,
        )
        return expanded_entries, _entry_errors(entry_errors, parents, rows)

    def _group_member_names(self, version: str | None) -> Mapping[str, tuple[str, tuple[str, ...]]]:
        """Indexed group names and their direct registry-declared members."""
        if version not in self._group_members:
            found: dict[str, tuple[str, tuple[str, ...]]] = {}
            if version is not None:
                for extractor in self.into_components().values():
                    try:
                        group = self.registry.component_group_field(
                            extractor.component, extractor.group, version
                        )
                    except (KeyError, OSError, ValueError):
                        group = None
                    if group is None:
                        continue
                    names = tuple(
                        str(member.fix.get("name") or member.name)
                        for member in members_of(entry_of(group))
                    )
                    if names:
                        found[column_name(extractor.group)] = (extractor.group, names)
            self._group_members[version] = MappingProxyType(found)
        return self._group_members[version]

    # -- versions -----------------------------------------------------------

    def version_of(
        self, message: str | None, protocol: Protocol | str | int = Protocol.OTHER
    ) -> tuple[str | None, str]:
        """Which FIX version a message is read under, and where that came from."""
        parse_protocol = Protocol.from_str(protocol)
        if (
            parse_protocol.version is not None
            and not parse_protocol.version.startswith("FIXT")
            and (embedded := self.version_named(parse_protocol.version)) is not None
        ):
            return embedded, PROTOCOL_SOURCE
        if message:
            if parse_protocol is Protocol.OTHER:
                parse_protocol = Protocol.from_str(
                    self.rules.into_arrow_protocol_array(
                        pyarrow.array([message], pyarrow.string())
                    )[0].as_py()
                )
            pairs = self.into_pairs(pyarrow.array([message]), parse_protocol)
            begin_column, application_column, default_column = _version_columns(pairs)
            begin = begin_column[0].as_py()
            version, source = _version_from_evidence(
                begin,
                application_column[0].as_py(),
                default_column[0].as_py(),
                self._spellings,
            )
            if (
                version is None
                and parse_protocol.family is Protocol.UL
                and self._latest_version is not None
            ):
                evidence = (
                    begin,
                    application_column[0].as_py(),
                    default_column[0].as_py(),
                )
                if all(not str(value or "").strip() for value in evidence):
                    return self._latest_version, REGISTRY_LATEST_SOURCE
            return version, source
        return None, NO_SOURCE

    def versions_of(
        self, messages: Any, protocol: Protocol | str | int = Protocol.OTHER
    ) -> pyarrow.Array:
        """Resolved version per row, after parsing each row's actual separator."""
        if isinstance(messages, pyarrow.ChunkedArray):
            messages = messages.combine_chunks()
        declared = Protocol.from_str(protocol)
        if declared is not Protocol.OTHER:
            return self.versions_of_pairs(self.into_pairs(messages, declared), declared)
        if not len(messages):
            return pyarrow.array([], pyarrow.string())
        groups = list(groups_of(self.rules.into_arrow_protocol_array(messages)))
        parts, positions = [], []
        for code, where in groups:
            pairs = self.into_pairs(pyarrow.compute.take(messages, where), code.as_py())
            parts.append(self.versions_of_pairs(pairs, code.as_py()))
            positions.append(where)
        return scattered(parts, positions)

    def versions_of_pairs(
        self, pairs: Any, protocol: Protocol | str | int = Protocol.OTHER
    ) -> pyarrow.Array:
        """Resolved application version per parsed row."""
        declared = Protocol.from_str(protocol)
        named = self.rules.rule(declared).named if declared is not Protocol.OTHER else None
        begins, application, default_application = _version_columns(pairs, named)
        compute = pyarrow.compute
        is_fixt = compute.fill_null(
            compute.match_substring_regex(compute.utf8_upper(begins), r"^FIXT"), False
        )
        versions = pyarrow.nulls(len(pairs), pyarrow.string())
        valid = compute.filter(begins, compute.is_valid(begins))
        for begin in compute.unique(valid).to_pylist():
            named = self.version_named(begin)
            if named is not None and not named.startswith("FIXT"):
                versions = compute.if_else(
                    compute.fill_null(compute.equal(begins, begin), False),
                    pyarrow.scalar(named),
                    versions,
                )
        valid_application = compute.filter(application, compute.is_valid(application))
        for spelling in compute.unique(valid_application).to_pylist():
            named = _APPL_VERSIONS.get(spelling) or self.version_named(spelling)
            if named is not None:
                versions = compute.if_else(
                    compute.and_(
                        is_fixt,
                        compute.fill_null(compute.equal(application, spelling), False),
                    ),
                    pyarrow.scalar(named),
                    versions,
                )
        valid_default = compute.filter(default_application, compute.is_valid(default_application))
        for spelling in compute.unique(valid_default).to_pylist():
            named = _APPL_VERSIONS.get(spelling) or self.version_named(spelling)
            if named is not None:
                versions = compute.if_else(
                    compute.and_(
                        compute.and_(is_fixt, compute.is_null(application)),
                        compute.fill_null(compute.equal(default_application, spelling), False),
                    ),
                    pyarrow.scalar(named),
                    versions,
                )
        if (
            declared.version is not None
            and not declared.version.startswith("FIXT")
            and (embedded := self.version_named(declared.version)) is not None
        ):
            versions = pyarrow.repeat(pyarrow.scalar(embedded, pyarrow.string()), len(pairs))
        if declared.family is Protocol.UL and self._latest_version is not None:
            versions = compute.if_else(
                compute.and_(
                    compute.is_null(versions),
                    _all_absent(begins, application, default_application),
                ),
                self._latest_version,
                versions,
            )
        return versions

    def version_named(self, begin_string: str) -> str | None:
        """`8=FIX.4.2` as the version the dictionary spells `4.2`; None if unknown.

        Matched on the digits and letters alone, because the two spellings
        agree on nothing else: `FIX.4.2` is `4.2`, `FIXT.1.1` is `FIXT1.1` and
        `FIX.5.0SP2` is `5.0.SP2`.
        """
        return self._spellings.get(_version_key(begin_string))

    def index_of(self, version: str | None = None) -> TagIndex:
        """The name index for one version, built once and held."""
        if version not in self._indexes:
            self._indexes[version] = TagIndex.from_tags(
                self._tags(version), self._containers(version)
            )
        return self._indexes[version]

    @cached_property
    def known_tags(self) -> pyarrow.Array:
        """Numeric identities present anywhere in the stored registry."""
        try:
            tags = self.registry.tag_numbers()
        except (OSError, ValueError):
            tags = frozenset()
        return pyarrow.array(sorted(tags), TAG)

    def tag_field(self, tag: int, version: str | None = None) -> Field | None:
        """One tag's declaration: the job's own where it has one, else the dictionary's."""
        declared = None
        if version is not None:
            try:
                declared = self.registry.field(tag, version)
            except (OSError, ValueError):
                declared = None
        rule = self.field_rules(version).get(tag)
        if rule is None:
            return declared
        return rule.applied(declared, declared.name if declared else str(tag))

    def flat_fields(self, version: str | None = None) -> dict[int, Field]:
        """Promoted registry fields, with contract fallbacks only for a cold registry."""
        if version not in self._flat_fields:
            if version is None:
                self._flat_fields[version] = {}
            elif not self.registry.fields_available(version):
                rules = self.field_rules(version)
                self._flat_fields[version] = {
                    tag: rule.applied(FLAT_DEFAULTS[tag], FLAT_COLUMNS[tag])
                    if (rule := rules.get(tag)) is not None
                    else FLAT_DEFAULTS[tag]
                    for tag in FLAT_COLUMNS
                }
            else:
                self._flat_fields[version] = {
                    tag: field
                    for tag in FLAT_COLUMNS
                    if (field := self.tag_field(tag, version)) is not None
                }
        return self._flat_fields[version]

    def named_fields(self) -> Mapping[str, Field]:
        """`{rendered spelling: column}` for the fields FIX never numbered.

        Read from *this codec's* registry rather than from the packaged one, so
        declaring a namespaced field is a change to a dictionary and never a change
        here. The parsed log's own contract still decides which of these
        columns it keeps; a codec that lifts one the log does not declare hands
        it to a caller that can, and drops it otherwise.
        """
        if self._named is None:
            try:
                built = named_columns(self.registry)
            except (OSError, ValueError):
                built = NAMESPACE_COLUMNS
            named = self._named_rules
            self._named = (
                built
                if not named
                else {
                    spelling: rule.applied(field, field.name)
                    if (rule := named.get(column_name(spelling))) is not None
                    else field
                    for spelling, field in built.items()
                }
            )
        return self._named

    def component_of(self, column: str, version: str | None = None) -> ComponentGroup:
        """Version-aware extractor for one structured component, cached per version."""
        built = self.into_components()[column]
        key = (column, version)
        if key not in self._components:
            self._components[key] = built(
                components=self._component_declarations(version),
                names=self._tags(version),
            )
        return self._components[key]

    def _component_declarations(self, version: str | None) -> list[Field]:
        """One version's trees plus safe typed-member tag aliases."""
        if version is None:
            return []
        try:
            declared = list(self.registry.components(version))
        except (KeyError, OSError, ValueError):
            return []
        for column, fields in self._component_supplements_for(version).items():
            if not fields:
                continue
            extractor = self.into_components()[column]
            declared.append(
                Field(
                    name=extractor.component,
                    dtype=pyarrow.struct([field.into_arrow_field() for field in fields]),
                    nullable=True,
                    metadata={"fix:component": extractor.component},
                )
            )
        return declared

    def _supplemental_fields(self, version: str | None) -> tuple[Field, ...]:
        """Typed component fields safely backported into one registry version."""
        if version is None:
            return ()
        return tuple(
            field
            for fields in self._component_supplements_for(version).values()
            for field in fields
        )

    def _component_supplements_for(self, version: str) -> Mapping[str, tuple[Field, ...]]:
        """Missing typed members whose group and physical type this version already knows.

        A bridge can backport an Extension Pack member without changing its
        BeginString. The selected version must already declare the containing
        group, and an occupied tag or a different physical type refuses the
        supplement, so an older definition always remains authoritative.
        """
        if version not in self._component_supplements:
            try:
                exact = self.registry.fields(version)
            except (KeyError, OSError, ValueError):
                exact = []
            occupied: dict[int, Field] = {}
            for field in exact:
                for tag in field.fix.tag_priority:
                    occupied.setdefault(tag, field)

            found: dict[str, tuple[Field, ...]] = {}
            for column, extractor in self.into_components().items():
                try:
                    group = self.registry.component_group_field(
                        extractor.component, extractor.group, version
                    )
                except (KeyError, OSError, ValueError):
                    group = None
                if group is None:
                    continue
                declared = {
                    column_name(member.fix.canonical) for member in members_of(entry_of(group))
                }
                projected = {
                    member.name: member for member in extractor.into_row().into_field().fields
                }
                added: list[Field] = []
                for name, fix_name in extractor.into_projection():
                    if column_name(fix_name) not in declared:
                        continue
                    try:
                        selected = self.registry.field(fix_name, version)
                        candidate = self.registry.field(fix_name)
                    except (KeyError, OSError, ValueError):
                        continue
                    if selected is not None or candidate is None:
                        continue
                    supplement = _typed_component_supplement(
                        candidate,
                        projected[name],
                        occupied,
                    )
                    if supplement is not None:
                        added.append(supplement)
                if added:
                    found[column] = tuple(added)
            self._component_supplements[version] = MappingProxyType(found)
        return self._component_supplements[version]

    # -- held state ---------------------------------------------------------

    @cached_property
    def _indexes(self) -> dict[str | None, TagIndex]:
        return {}

    @cached_property
    def _components(self) -> dict[tuple[str, str | None], ComponentGroup]:
        return {}

    @cached_property
    def _component_supplements(self) -> dict[str, Mapping[str, tuple[Field, ...]]]:
        return {}

    _named: Mapping[str, Field] | None = None

    @cached_property
    def _named_names(self) -> pyarrow.Array:
        """Folded rendered fields the registry declares without numeric tags."""
        return pyarrow.array(
            sorted({column_name(spelling) for spelling in self.named_fields()}),
            pyarrow.string(),
        )

    @cached_property
    def _flat_fields(self) -> dict[str | None, dict[int, Field]]:
        return {}

    # -- what a job declares ---------------------------------------------------

    def field_rules(self, version: str | None = None) -> Mapping[int, FieldRule]:
        """The declared readings this version numbers, by tag.

        Resolved through the same `TagIndex` a key resolves through, so a rule
        may name its field however the log does -- `60`, `TransactTime`, or a
        rendered key -- and mean the same field either way. Built once per
        version: a rule set is a handful of entries and a batch is thousands.
        """
        if not self.fields:
            return _NO_RULES
        if version not in self._field_rules:
            self._field_rules[version] = self._resolved_rules(version)
        return self._field_rules[version]

    def _resolved_rules(self, version: str | None) -> dict[int, FieldRule]:
        index = self.index_of(version)
        found: dict[int, FieldRule] = {}
        for rule in self.fields:
            tag = rule.tag
            if tag is None:
                tag, hit, _, _ = index.resolve_key(rule.field)
                if not hit or tag is None:
                    continue
            found.setdefault(int(tag), rule)
        return found

    @cached_property
    def _named_rules(self) -> Mapping[str, FieldRule]:
        """Declared readings by folded spelling, for the fields FIX never numbered."""
        return {rule.folded: rule for rule in self.fields if rule.tag is None}

    @cached_property
    def _field_rules(self) -> dict[str | None, dict[int, FieldRule]]:
        return {}

    @cached_property
    def _canonicals(self) -> dict[str | None, tuple[Any, Any]]:
        return {}

    @cached_property
    def _encoded_values(self) -> dict[str | None, tuple[Any, Any]]:
        return {}

    @cached_property
    def _group_members(self) -> dict[str | None, Mapping[str, tuple[str, tuple[str, ...]]]]:
        return {}

    @cached_property
    def _spellings(self) -> dict[str, str]:
        """`{version key: canonical spelling}` for every version the store holds."""
        return dict(version_spellings(self.registry))

    @cached_property
    def _version_lookup(self) -> tuple[pyarrow.Array, pyarrow.Array]:
        """Registry version keys and canonical spellings as one Arrow lookup."""
        return (
            pyarrow.array(list(self._spellings), pyarrow.string()),
            pyarrow.array(list(self._spellings.values()), pyarrow.string()),
        )

    @cached_property
    def _latest_version(self) -> str | None:
        """Newest application version this codec's registry can resolve."""
        return self.registry.latest_application_version

    def _protocol_versions(self, protocols: Any, rows: int) -> pyarrow.Array:
        """Registered application versions already embedded in protocol codes."""
        embedded = Protocol.into_versions_arrow(_as_array(protocols, rows))
        version_keys, version_values = self._version_lookup
        registered = pyarrow.compute.take(
            version_values,
            pyarrow.compute.index_in(_version_keys_arrow(embedded), value_set=version_keys),
        )
        transport = pyarrow.compute.fill_null(pyarrow.compute.starts_with(embedded, "FIXT"), False)
        return pyarrow.compute.if_else(
            transport, pyarrow.scalar(None, pyarrow.string()), registered
        )

    def default_version(self, protocol: Protocol | str | int) -> str | None:
        """Latest registry version for unversioned UL syntax."""
        parsed = Protocol.from_str(protocol)
        embedded = parsed.version
        if embedded is not None and not embedded.startswith("FIXT"):
            return self.version_named(embedded)
        return self._latest_version if parsed.family is Protocol.UL else None

    def _tags(self, version: str | None) -> dict[str, int]:
        """`{name: tag}` for one version plus safe typed component supplements."""
        if version is None:
            return {}
        try:
            found = dict(self.registry.tags(version))
        except (KeyError, OSError, ValueError):
            return {}
        for field in self._supplemental_fields(version):
            canonical = field.fix.tag
            if canonical is None:
                continue
            for spelling in field.fix.spellings():
                found.setdefault(column_name(spelling), canonical)
            for tag in field.fix.tag_priority:
                found.setdefault(str(tag), tag)
        return found

    def _containers(self, version: str | None) -> tuple[str, ...]:
        """Every component this version declares, which a dotted key may name."""
        if version is None:
            return ()
        try:
            return tuple(component.name for component in self.registry.components(version))
        except (KeyError, OSError, ValueError):
            return ()


@functools.cache
def version_spellings(registry: FixRegistry) -> Mapping[str, str]:
    """Canonical registry spellings indexed once by wire-version spelling."""
    try:
        return {_version_key(version): version for version in registry.versions}
    except (OSError, ValueError):
        return {}


#: The three fields that say which version a message speaks, under every
#: spelling one of them arrives as. A constant because it was a dict literal
#: rebuilt once per key of every message.
_VERSION_EVIDENCE: Mapping[str, str] = MappingProxyType(
    {
        "8": "begin",
        "beginstring": "begin",
        "1128": "application",
        "applverid": "application",
        "1137": "default",
        "defaultapplverid": "default",
    }
)

#: Where the header stops: CheckSum <10> ends the message, so nothing after it
#: is evidence of anything.
_CHECKSUM_KEYS = frozenset({"10", "checksum"})


def infer_version_from_pairs(
    pairs: Iterable[tuple[Any, Any]], registry: FixRegistry | None = None
) -> tuple[str | None, str]:
    """Infer one FIX application version from tags 8, 1128 and 1137."""
    evidence: dict[str, str] = {}
    for key, value in pairs:
        text = key if type(key) is str else str(key)
        outer = text if text.isdigit() else column_name(text)
        if outer in _CHECKSUM_KEYS:
            break
        # A key already spelled in digits *is* its own tail, which is every
        # key of a wire message: the tail pattern only earns its call on a
        # rendered, dotted or indexed one.
        if text.isdigit():
            name = text
        else:
            member = _MEMBER_NAME_SCALAR.search(text)
            name = column_name(member["name"] if member is not None else text)
        selected = _VERSION_EVIDENCE.get(name)
        if selected is None or value is None:
            continue
        rendered = str(value).strip()
        if rendered:
            evidence.setdefault(selected, rendered)
    return _version_from_evidence(
        evidence.get("begin"),
        evidence.get("application"),
        evidence.get("default"),
        version_spellings(registry or FixRegistry.from_builtin()),
    )


def _version_from_evidence(
    begin: str | None,
    application: str | None,
    default_application: str | None,
    spellings: Mapping[str, str],
) -> tuple[str | None, str]:
    """Resolve parsed transport/application evidence without choosing a default."""
    if begin is None:
        return None, NO_SOURCE
    if not _version_key(begin).startswith("FIXT"):
        resolved = spellings.get(_version_key(begin))
        return (resolved, BEGIN_STRING_SOURCE) if resolved is not None else (None, NO_SOURCE)
    if application is not None:
        resolved = _APPL_VERSIONS.get(str(application).strip()) or spellings.get(
            _version_key(application)
        )
        return (resolved, APPLICATION_VERSION_SOURCE) if resolved is not None else (None, NO_SOURCE)
    if default_application is not None:
        resolved = _APPL_VERSIONS.get(str(default_application).strip()) or spellings.get(
            _version_key(default_application)
        )
        return (resolved, APPLICATION_VERSION_SOURCE) if resolved is not None else (None, NO_SOURCE)
    return None, NO_SOURCE


def _version_columns(
    pairs: Any, named: bool | None = None
) -> tuple[pyarrow.Array, pyarrow.Array, pyarrow.Array]:
    """BeginString and application-version values from parsed pairs."""
    if isinstance(pairs, pyarrow.ChunkedArray):
        pairs = pairs.combine_chunks()
    compute = pyarrow.compute
    listed = _listed(pairs)
    parents = compute.list_parent_indices(listed).cast(pyarrow.int64())
    _, keys, values = _entries_of(listed)
    reduced = keys
    if named is not False:
        reduced = compute.fill_null(
            compute.struct_field(compute.extract_regex(keys, _MEMBER_NAME_VECTOR), "name"), keys
        )
        reduced = column_names(reduced)
    rows = sequence(len(pairs))

    def first(wanted: pyarrow.Array) -> pyarrow.Array:
        matches = compute.is_in(reduced, value_set=wanted)
        selected_parents = compute.filter(parents, matches)
        selected_values = compute.filter(values, matches)
        return compute.take(
            selected_values,
            compute.index_in(rows, value_set=selected_parents),
        )

    return first(_BEGIN_KEYS), first(_APPLICATION_KEYS), first(_DEFAULT_APPLICATION_KEYS)


def _lead_of(entries: Any) -> Any:
    """The indexed container stored beside each key."""
    return pyarrow.compute.struct_field(entries, "comp")


def _declared_index(keys: Any, lead: Any, declared: Any) -> Any:
    """Where each field sits in `declared`, or null; the whole name first.

    Whole first because a vendor prefix is part of a name -- `TECH.CLIENTID`
    is not `CLIENTID` -- and the tail second because a dictionary that declares
    only one of the two spellings still answers for the other.
    """
    compute = pyarrow.compute
    if not len(declared):
        return pyarrow.nulls(len(keys), pyarrow.int32())
    tail = column_names(keys)
    whole = compute.if_else(
        compute.is_valid(lead),
        compute.binary_join_element_wise(column_names(lead), tail, ""),
        tail,
    )
    found = compute.index_in(whole, value_set=declared)
    return compute.if_else(
        compute.is_valid(found), found, compute.index_in(tail, value_set=declared)
    )


def _liftable(
    parents: Any,
    keys: Any,
    values: Any,
    priorities: Any | None = None,
) -> tuple[Any, Any]:
    """`(every entry of a liftable key, the one that becomes the column)`.

    A key repeated in one row still lifts where its entries **agree**: a
    bridge writes the same fact twice on purpose -- `#Side` as it arrived and
    `Side` after enrichment -- on a third to a half of a real capture's lines.
    Repeats that disagree lift neither: that is a group, or an enrichment that
    rewrote something, and picking between them would be a guess.

    Namespaced aliases are the exception: canonical name, then aliases in
    registry order form an explicit priority list. Only the first spelling
    present in a row participates; repeats of that spelling must still agree.
    """
    compute = pyarrow.compute
    if pyarrow.types.is_integer(keys.type):
        composite = compute.add(
            compute.multiply(parents, pyarrow.scalar(2**32, pyarrow.int64())),
            keys.cast(pyarrow.int64()),
        )
    else:
        composite = compute.binary_join_element_wise(
            parents.cast(pyarrow.string()), keys.cast(pyarrow.string()), "\x00"
        )
    if priorities is not None and len(composite):
        grouped = (
            pyarrow.table({"identity": composite, "priority": priorities})
            .group_by("identity", use_threads=False)
            .aggregate([("priority", "min")])
        )
        minimum = compute.take(
            grouped["priority_min"],
            compute.index_in(composite, value_set=grouped["identity"]),
        )
        preferred = compute.equal(priorities, minimum)
        if not compute.all(preferred, min_count=0).as_py():
            positions = sequence(len(composite))
            selected = compute.filter(positions, preferred)
            agreed, chosen = _liftable(
                compute.filter(parents, preferred),
                compute.filter(keys, preferred),
                compute.filter(values, preferred),
            )
            return (
                compute.is_in(positions, value_set=compute.filter(selected, agreed)),
                compute.is_in(positions, value_set=compute.filter(selected, chosen)),
            )
    distinct = compute.unique(composite)
    if len(distinct) == len(composite):
        whole = pyarrow.repeat(True, len(composite))
        return whole, whole
    reading = compute.binary_join_element_wise(
        composite.cast(pyarrow.string()),
        compute.fill_null(values.cast(pyarrow.string(), safe=False), ""),
        "\x00",
    )
    # `index_in` against the column itself gives each entry the position of the
    # first entry reading the same way, so an entry is a repeat exactly when
    # that position is not its own. One hash table, no grouping.
    first = compute.equal(compute.index_in(reading, value_set=reading), sequence(len(reading)))
    counted = compute.value_counts(compute.filter(composite, first))
    seen = compute.take(
        counted.field("counts"), compute.index_in(composite, value_set=counted.field("values"))
    )
    agreed = compute.equal(seen, 1)
    return agreed, compute.and_(agreed, first)


def _quote_group_structure(parents: Any, keys: Any) -> Any:
    """Structural quote tags retained on rows that declare a quote group."""
    compute = pyarrow.compute
    counts = compute.fill_null(compute.is_in(keys, value_set=QUOTE_GROUP_COUNTS), False)
    if not compute.any(counts, min_count=0).as_py():
        return pyarrow.repeat(False, len(keys))
    grouped = compute.unique(compute.filter(parents, counts))
    return compute.and_(
        compute.is_in(parents, value_set=grouped),
        compute.fill_null(compute.is_in(keys, value_set=QUOTE_GROUP_STRUCTURE), False),
    )


def _listed(pairs: Any) -> Any:
    """A pair column as a list-of-struct view.

    Stored pairs already have this shape. Parsed maps cast without moving data,
    letting the list kernels honour sliced offsets and validity.
    """
    if pyarrow.types.is_list(pairs.type):
        return pairs
    return pairs.cast(
        pyarrow.list_(
            pyarrow.field(
                "item",
                pyarrow.struct(
                    [
                        pyarrow.field("key", pairs.type.key_type, nullable=False),
                        pairs.type.item_field,
                    ]
                ),
                nullable=False,
            )
        )
    )


def _payload_pairs(payloads: Any) -> Any:
    """Each payload read as the message it is, under its own separator.

    Its own, because a payload sits *inside* a token of the message around it
    and so cannot be written with that message's separator. One parse per
    distinct separator in the batch, which is how every other reading here
    handles a column that mixes them.
    """
    compute = pyarrow.compute
    if not len(payloads):
        return parse_arrow_array(payloads, SOH, named=True)
    separators = compute.fill_null(
        compute.struct_field(compute.extract_regex(payloads, _PAYLOAD_SEPARATOR), "sep"), ""
    )
    parts, positions = [], []
    for separator, where in groups_of(separators):
        spelled = separator.as_py() or SOH
        parts.append(parse_arrow_array(compute.take(payloads, where), spelled, named=True))
        positions.append(where)
    return scattered(parts, positions)


def _payload_counts(readable: Any, parsed: Any) -> tuple[Any, tuple[Any, Any] | None]:
    """How many pairs each entry becomes, and the payload pairs behind them.

    One for an entry that is not a payload or whose payload read as nothing --
    it stays itself -- and the payload's own pair count otherwise.
    """
    compute = pyarrow.compute
    found = compute.fill_null(compute.list_value_length(_listed(parsed)), 0).cast(pyarrow.int32())
    counts = compute.if_else(
        readable,
        compute.if_else(
            compute.greater(_scattered_int(readable, found), 1),
            _scattered_int(readable, found),
            pyarrow.scalar(1, pyarrow.int32()),
        ),
        pyarrow.scalar(1, pyarrow.int32()),
    )
    if not compute.any(compute.greater(counts, 1), min_count=0).as_py():
        return counts, None
    entries = compute.list_flatten(_listed(parsed))
    prefix = pyarrow.scalar(f"{_XML_DATA_KEY}.")
    return counts, (
        compute.binary_join_element_wise(prefix, compute.struct_field(entries, 0), ""),
        compute.struct_field(entries, 1),
    )


def _payload_starts(readable: Any, parsed: Any) -> Any:
    """Where each entry's payload pairs begin in the flattened payload child."""
    compute = pyarrow.compute
    found = compute.fill_null(compute.list_value_length(_listed(parsed)), 0).cast(pyarrow.int32())
    running = compute.subtract(compute.cumulative_sum(found), found)
    return compute.fill_null(_scattered_int(readable, running), 0)


def _scattered_mask(mask: Any, found: Any) -> Any:
    """A truth per masked entry, put back where its entry was; false elsewhere."""
    compute = pyarrow.compute
    return compute.and_(mask, compute.equal(_scattered_int(mask, found.cast(pyarrow.int32())), 1))


def _scattered_int(mask: Any, values: Any) -> Any:
    """A value per masked entry, put back where its entry was; zero elsewhere."""
    compute = pyarrow.compute
    slots = compute.if_else(
        mask,
        compute.subtract(
            compute.cumulative_sum(mask.cast(pyarrow.int32())),
            pyarrow.scalar(1, pyarrow.int32()),
        ),
        pyarrow.scalar(None, pyarrow.int32()),
    )
    return compute.fill_null(compute.take(values, slots), 0).cast(pyarrow.int32())


def _scattered_values(mask: Any, values: Any) -> Any:
    """Masked values put back at their source entries, null elsewhere."""
    compute = pyarrow.compute
    slots = compute.if_else(
        mask,
        compute.subtract(
            compute.cumulative_sum(mask.cast(pyarrow.int32())),
            pyarrow.scalar(1, pyarrow.int32()),
        ),
        pyarrow.scalar(None, pyarrow.int32()),
    )
    return compute.take(values, slots)


def _entry_errors(errors: Any, parents: Any, rows: int) -> Any:
    """Flattened entry diagnostics joined once per source row."""
    compute = pyarrow.compute
    valid = compute.is_valid(errors)
    if not compute.any(valid, min_count=0).as_py():
        return pyarrow.nulls(rows, pyarrow.string())
    found = compute.filter(errors, valid)
    sizes = dense_counts(compute.filter(parents, valid), rows)
    listed = build_list(
        pyarrow.list_(pyarrow.string()),
        sizes,
        found,
    )
    joined = compute.binary_join(listed, "; ")
    return compute.if_else(compute.equal(joined, ""), pyarrow.scalar(None), joined)


def _merge_group_errors(left: Any, right: Any) -> Any:
    """Append nullable group diagnostics without text on clean rows."""
    compute = pyarrow.compute
    left = compute.fill_null(left.cast(pyarrow.string(), safe=False), "")
    right = compute.fill_null(right.cast(pyarrow.string(), safe=False), "")
    separator = compute.if_else(
        compute.and_(compute.not_equal(left, ""), compute.not_equal(right, "")),
        "; ",
        "",
    )
    joined = compute.binary_join_element_wise(left, separator, right, "")
    return compute.if_else(compute.equal(joined, ""), pyarrow.scalar(None), joined)


def _glued_group_error(group: str, ambiguities: Sequence[Sequence[str]]) -> str | None:
    """A deterministic note for every longest-match boundary choice."""
    if not ambiguities:
        return None
    choices = [f"{names[0]} over {', '.join(names[1:])}" for names in ambiguities]
    return f"{group} glued member boundary was ambiguous; chose " + "; ".join(choices)


@functools.cache
def _glued_member_pattern(members: tuple[str, ...]) -> str:
    """Declared group-member boundaries, longest alternative first."""
    ordered = tuple(member for member, _ in _glued_group_boundaries(members))
    return "(?i)(" + "|".join(re.escape(member) for member in ordered) + ")="


def _glued_ambiguity_errors(group: str, values: Any, members: tuple[str, ...]) -> Any:
    """Longest-match diagnostics over a group-value column."""
    compute = pyarrow.compute
    errors = pyarrow.nulls(len(values), pyarrow.string())
    for selected, matches in _glued_group_boundaries(members):
        if len(matches) < 2:
            continue
        carries = compute.fill_null(
            compute.match_substring_regex(values, f"(?i){re.escape(selected)}="),
            False,
        )
        detail = _glued_group_error(group, (matches,))
        found = compute.if_else(
            carries,
            pyarrow.scalar(detail, pyarrow.string()),
            pyarrow.scalar(None, pyarrow.string()),
        )
        errors = _merge_group_errors(errors, found)
    return errors


def _all_absent(*columns: Any) -> Any:
    """Whether every evidence column is null or empty after trimming."""
    compute = pyarrow.compute
    empty = None
    for column in columns:
        text = column.cast(pyarrow.string(), safe=False)
        absent = compute.fill_null(compute.equal(compute.utf8_trim_whitespace(text), ""), True)
        empty = absent if empty is None else compute.and_(empty, absent)
    return empty


def _repeated(counts: Any) -> tuple[Any, Any]:
    """`(which entry each slot came from, its rank within that entry)`.

    `repeat` in kernels: a list array whose offsets are the running counts has
    exactly one slot per output pair, so `list_parent_indices` *is* the repeat.
    """
    compute = pyarrow.compute
    bounds = pyarrow.concat_arrays(
        [pyarrow.array([0], pyarrow.int32()), compute.cumulative_sum(counts)]
    )
    total = bounds[len(bounds) - 1].as_py()
    holder = pyarrow.ListArray.from_arrays(bounds, pyarrow.nulls(total, pyarrow.int8()))
    taken = compute.list_parent_indices(holder)
    running = compute.subtract(
        compute.cumulative_sum(pyarrow.repeat(pyarrow.scalar(1, pyarrow.int32()), total)),
        pyarrow.scalar(1, pyarrow.int32()),
    )
    return taken, compute.subtract(running, compute.take(bounds.slice(0, len(bounds) - 1), taken))


def _row_totals(pairs: Any, lengths: Any, counts: Any) -> Any:
    """Each row's length after an entry became several, in the same order."""
    compute = pyarrow.compute
    running = pyarrow.concat_arrays(
        [pyarrow.array([0], pyarrow.int32()), compute.cumulative_sum(counts)]
    )
    bounds = pyarrow.concat_arrays(
        [pyarrow.array([0], pyarrow.int32()), compute.cumulative_sum(lengths)]
    )
    ends = running.take(bounds)
    del pairs
    return compute.subtract(ends.slice(1), ends.slice(0, len(ends) - 1)).cast(pyarrow.int32())


def _entries_column(source: Any, lengths: Any, keep: Any, *parts: Any) -> pyarrow.Array:
    """A `ENTRIES` column out of the entries `keep` admits, in the source's rows.

    `keep` is over the source's own flattened entries and `parts` are already
    filtered by it: the offsets come from the mask and the children from the
    filter, which is how every other split here rebuilds a list column.
    """
    entries = pyarrow.StructArray.from_arrays(
        [
            part.cast(ENTRIES.value_type.field(name).type, safe=False)
            for name, part in zip(ENTRY_PARTS, parts, strict=True)
        ],
        fields=[ENTRIES.value_type.field(name) for name in ENTRY_PARTS],
    )
    return pyarrow.ListArray.from_arrays(
        _selected_offsets(source, lengths, keep), entries, type=ENTRIES
    )


def _flattened(entries: Any) -> tuple[Any, Any, Any]:
    """`(row lengths, parent row per entry, the entries)` for a `ENTRIES` column."""
    compute = pyarrow.compute
    listed = _listed(entries)
    return (
        compute.fill_null(compute.list_value_length(listed), 0).cast(pyarrow.int32()),
        compute.list_parent_indices(listed).cast(pyarrow.int64()),
        compute.list_flatten(listed),
    )


def _columns_of(entries: Any, keep: Any) -> tuple[Any, ...]:
    """The children of the entries a mask keeps, in declaration order."""
    compute = pyarrow.compute
    return tuple(compute.filter(compute.struct_field(entries, name), keep) for name in ENTRY_PARTS)


def _raw_spelling_changed(raw: Any, typed: Any) -> Any:
    """Whether a typed Arrow value cannot reproduce its source FIX text."""
    compute = pyarrow.compute
    kinds = pyarrow.types
    if kinds.is_boolean(typed.type):
        rendered = compute.if_else(typed, pyarrow.scalar("Y"), pyarrow.scalar("N"))
    elif (
        kinds.is_string(typed.type)
        or kinds.is_large_string(typed.type)
        or kinds.is_integer(typed.type)
        or kinds.is_floating(typed.type)
        or kinds.is_decimal(typed.type)
    ):
        rendered = typed.cast(pyarrow.string(), safe=False)
    else:
        # Temporal and opaque values have protocol-specific text spellings.
        # Retaining their source is cheaper and safer than guessing a renderer.
        return compute.is_valid(raw)
    return compute.and_(
        compute.is_valid(raw),
        compute.fill_null(compute.not_equal(raw.cast(pyarrow.string()), rendered), True),
    )


def _tags_of(fields: Mapping[int, Field]) -> pyarrow.Array:
    """The tags a version can lift, as the value set a probe takes."""
    return pyarrow.array(sorted(fields), TAG)


def _typed_component_supplement(
    candidate: Field,
    projected: Field,
    occupied: Mapping[int, Field],
) -> Field | None:
    """A merged member safe to read under an older typed component contract."""
    canonical = candidate.fix.tag
    if canonical is None or candidate.dtype != projected.dtype:
        return None
    identity = column_name(candidate.fix.canonical)
    alternates: list[int] = []
    for tag in candidate.fix.tag_priority:
        exact = occupied.get(tag)
        conflict = exact is not None and (
            column_name(exact.fix.canonical) != identity or exact.dtype != candidate.dtype
        )
        if conflict:
            if tag == canonical:
                return None
            continue
        if tag != canonical:
            alternates.append(tag)
    built = Field(
        name=candidate.name,
        dtype=candidate.dtype,
        nullable=candidate.nullable,
        metadata=dict(candidate.metadata),
    )
    built.fix.tags = alternates
    return built


def _canonical_names(
    registry: FixRegistry,
    version: str | None,
    supplements: Sequence[Field] = (),
) -> tuple[Any, Any]:
    """`(tag, canonical spelling)` for a version and scoped typed supplements.

    What `parse_fix_*` canonicalizes a key to: a bridge writes `PARTYID` and the
    registry spells it `PartyID`, and a stored column read by a person should
    say what the standard says. A typed component can admit a later member
    only after its containing group and physical type have been checked.
    """
    tags: list[int] = []
    names: list[str] = []
    seen: set[int] = set()
    if version is not None:
        try:
            members = (*registry.fields(version), *supplements)
        except (KeyError, OSError, ValueError):
            members = []
        for member in members:
            tag = member.fix.get("tag")
            if tag and int(tag) not in seen:
                numeric = int(tag)
                seen.add(numeric)
                tags.append(numeric)
                names.append(member.name)
    return pyarrow.array(tags, TAG), pyarrow.array(names, pyarrow.string())


def _encodings(
    registry: FixRegistry,
    version: str | None,
    supplements: Sequence[Field] = (),
) -> tuple[Any, Any]:
    """`(tag and folded spelling, value)` for a version and typed supplements.

    The dictionary's own `encoded`, as the value set one kernel probes:
    `side=Buy` and `side=BUY` both reach `1`, and a spelling two values share
    reaches neither. Scoped component supplements follow, while a collision
    retains the meaning declared by BeginString.
    """
    spelled: list[str] = []
    resolved: list[str] = []
    seen: set[str] = set()
    try:
        entries = (
            tuple(registry.field_records().values())
            if version is None
            else (*registry.fields(version), *supplements)
        )
    except (KeyError, OSError, ValueError):
        entries = ()
    for entry in entries:
        fix = entry.fix
        tag, encoded = fix.tag, fix.encoded
        if tag is None or not encoded:
            continue
        for spelling, value in encoded.items():
            composite = f"{tag}\x00{spelling}"
            if composite in seen:
                continue
            seen.add(composite)
            spelled.append(composite)
            resolved.append(value)
    return pyarrow.array(spelled, pyarrow.string()), pyarrow.array(resolved, pyarrow.string())


def _popped_pairs(pairs: Any, pop: Mapping[str, str]) -> Any:
    """Apply ordered source-to-target replacements without changing other pair order."""
    if not pop:
        return pairs
    if isinstance(pairs, pyarrow.ChunkedArray):
        return pyarrow.chunked_array(
            [_popped_pairs(chunk, pop) for chunk in pairs.chunks],
            type=_RAW_PAIRS,
        )
    replaced = pairs
    compute = pyarrow.compute
    for source, target in pop.items():
        lengths, keys, values = _entries_of(replaced)
        if not len(keys):
            continue
        listed = _listed(replaced)
        parents = compute.list_parent_indices(listed).cast(pyarrow.int64())
        folded = column_names(keys)
        source_mask = compute.fill_null(
            compute.equal(folded, column_name(source)),
            False,
        )
        if not compute.any(source_mask, min_count=0).as_py():
            continue

        positions = sequence(len(keys))
        source_parents = compute.filter(parents, source_mask)
        source_positions = compute.filter(positions, source_mask)
        previous = pyarrow.concat_arrays(
            [
                pyarrow.array([-1], pyarrow.int64()),
                source_parents.slice(0, len(source_parents) - 1),
            ]
        )
        first_positions = compute.filter(
            source_positions,
            compute.not_equal(source_parents, previous),
        )
        first_source = compute.is_in(positions, value_set=first_positions)
        rows_with_source = compute.unique(source_parents)
        target_mask = compute.fill_null(
            compute.equal(folded, column_name(target)),
            False,
        )
        superseded = compute.and_(
            target_mask,
            compute.is_in(parents, value_set=rows_with_source),
        )
        keep = compute.or_(
            first_source,
            compute.invert(compute.or_(source_mask, superseded)),
        )
        renamed = compute.if_else(first_source, pyarrow.scalar(target), keys)
        replaced = _mapped(
            replaced,
            lengths,
            keep,
            compute.filter(renamed, keep),
            compute.filter(values, keep),
            _RAW_PAIRS,
        )
    return replaced


def _entries_of(pairs: Any) -> tuple[Any, Any, Any]:
    """One pair column as `(row lengths, keys, values)`."""
    compute = pyarrow.compute
    listed = _listed(pairs)
    lengths = compute.fill_null(compute.list_value_length(listed), 0).cast(pyarrow.int32())
    entries = compute.list_flatten(listed)
    keys = compute.struct_field(entries, "key")
    if entries.type.get_field_index("comp") >= 0:
        lead = compute.struct_field(entries, "comp")
        keys = compute.if_else(
            compute.is_valid(lead),
            compute.binary_join_element_wise(compute.fill_null(lead, ""), keys, "."),
            keys,
        )
    return lengths, keys, compute.struct_field(entries, "value")


def _mapped(
    source: Any,
    lengths: Any,
    mask: Any,
    keys: Any,
    items: Any,
    dtype: pyarrow.DataType,
) -> pyarrow.Array:
    """One half of a split, with the source's own rows and nulls.

    The offsets are rebuilt from a cumulative sum of the mask -- the same
    construction `parse_arrow_array` builds its rows from -- so an entry that
    went to the other half costs nothing here and the ones that stayed keep
    their order. A null row takes to null, which is Arrow's own `from_arrays`
    convention and the one way to keep "not a message" apart from "a message
    with nothing in it".
    """
    offsets = _selected_offsets(source, lengths, mask)
    if pyarrow.types.is_map(dtype):
        return pyarrow.MapArray.from_arrays(offsets, keys, items, type=dtype)
    entries = pyarrow.StructArray.from_arrays(
        [keys, items], fields=[dtype.value_type.field(0), dtype.value_type.field(1)]
    )
    return pyarrow.ListArray.from_arrays(offsets, entries, type=dtype)


def _selected_offsets(source: Any, lengths: Any, mask: Any) -> pyarrow.Array:
    """List offsets after a flattened child mask, preserving null rows."""
    compute = pyarrow.compute
    counted = compute.cumulative_sum(mask.cast(pyarrow.int32()))
    bounds = pyarrow.concat_arrays([pyarrow.array([0], pyarrow.int32()), counted])
    rows = pyarrow.concat_arrays(
        [pyarrow.array([0], pyarrow.int32()), compute.cumulative_sum(lengths)]
    )
    offsets = bounds.take(rows)
    if not source.null_count:
        return offsets
    head = compute.if_else(
        compute.is_null(source),
        pyarrow.scalar(None, pyarrow.int32()),
        offsets.slice(0, len(source)),
    )
    return pyarrow.concat_arrays([head, offsets.slice(len(source))])


def _version_key(spelling: str) -> str:
    """A FIX version in the one spelling both sides of the lookup agree on.

    Uppercased with the punctuation dropped, and the `FIX` prefix with it --
    unless it is `FIXT`, which is a different protocol and not decoration. So
    `8=FIX.4.2`, `FIX.4.2` and `4.2` all key on `42`, and `FIXT.1.1` keys on
    `FIXT11` rather than colliding with `1.1`.
    """
    text = re.sub(r"[^A-Za-z0-9]", "", spelling.strip().upper())
    if text.startswith("8FIX"):
        text = text[1:]
    if text.startswith("FIXT"):
        return text
    return text[3:] if text.startswith("FIX") else text


def _version_keys_arrow(spellings: Any) -> Any:
    """`_version_key` over a string column in Arrow kernels."""
    compute = pyarrow.compute
    keys = compute.replace_substring_regex(
        compute.utf8_upper(compute.utf8_trim_whitespace(spellings)),
        r"[^A-Za-z0-9]",
        "",
    )
    prefixed = compute.fill_null(compute.match_substring_regex(keys, r"^8FIX"), False)
    keys = compute.if_else(prefixed, compute.utf8_slice_codeunits(keys, 1), keys)
    transport = compute.fill_null(compute.match_substring_regex(keys, r"^FIXT"), False)
    fix = compute.fill_null(compute.match_substring_regex(keys, r"^FIX"), False)
    return compute.if_else(
        transport,
        keys,
        compute.if_else(fix, compute.utf8_slice_codeunits(keys, 3), keys),
    )
