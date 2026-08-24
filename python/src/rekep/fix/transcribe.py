"""A log line's pairs as the tags FIX gave them -- as far as the dictionary goes."""

from __future__ import annotations

import dataclasses
import functools
import json
import re
import warnings
from collections.abc import Iterable, Mapping
from functools import cached_property
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field
from rekep.fields.arrays import groups_of, scattered, sequence
from rekep.fix.columns import COLUMNS as FLAT_COLUMNS
from rekep.fix.columns import DECLARATIONS as FLAT_DEFAULTS
from rekep.fix.columns import KWARGS, QUOTE_GROUP_COUNTS, QUOTE_GROUP_STRUCTURE, TAG, named_columns
from rekep.fix.columns import NAMED as NAMED_COLUMNS
from rekep.fix.columns import TYPES as FLAT_TYPES
from rekep.fix.components import Parties
from rekep.fix.fields import cast_arrow_fix
from rekep.fix.message import (
    _MEMBER_NAME_VECTOR,
    BRIDGE_SEPARATOR_VECTOR,
    MARKER,
    NAMED_SEPARATOR_VECTOR,
    SEPARATOR_VECTOR,
    SEPARATORS,
    SOH,
    parse_arrow_array,
)
from rekep.fix.quickfix import SpecComponent
from rekep.fix.registry import FixRegistry
from rekep.fix.rules import NO_PROTOCOL, Rules

#: `XmlData <213>` as a rendered key and as a wire tag, which are the two ways
#: a line writes the field whose payload is another message.
_XML_DATA_KEY = "XmlData"
_XML_DATA_NAME = pyarrow.scalar("xmldata")
_XML_DATA_TAG = pyarrow.scalar("213")

#: What makes a payload a message rather than a document: two `name=` tokens.
#: The same "two and not one" `BRIDGE` uses, and for the same reason -- one
#: `a=b` inside prose is a sentence.
_PAYLOAD_PAIRS = r"[A-Za-z0-9_.\-]+[ \t]*=.*[^A-Za-z0-9_.\-][A-Za-z0-9_.\-]+[ \t]*="

#: And what makes it a document: the standard says XML, and a payload that
#: opens a tag is taken at its word however rare it turns out to be.
_LOOKS_XML = r"^[ \t\r\n]*<"

#: What a payload writes between its fields. Neither of the two things
#: `separators_of` reads -- a BeginString or a `#` -- is inside one, so this
#: reads the character between the first `name=value` and the next `name=`,
#: which is the same rule `BRIDGE_SEPARATOR_VECTOR` applies to a marked line.
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

#: A rendered key cut into the name it ends with and whatever stood in front:
#: `NoPartyIDs[0].PartyID` is `PartyID` under `NoPartyIDs[0]`, `TECH.CLIENTID`
#: is `CLIENTID` under `TECH`, and `Side` is `Side` under nothing. Greedy, so
#: the *last* dot is the cut and a deeper path keeps its depth.
_NAMESPACED_KEY = r"(?s)^(?:(?P<namespace>.*)\.)?(?P<key>[^.]*)$"

#: A key that is already a tag: digits, and few enough of them to be one.
#: Ten digits can overflow an `int32` and no FIX tag has ten, so the width is
#: the guard -- an epoch-millis key is not a tag, and letting it through would
#: turn a resolution into an Arrow overflow long after the decision was made.
_IS_TAG = r"^[0-9]{1,9}$"

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
NULL_VALUES: frozenset[str] = frozenset({"", "null", "<null>", "n/a"})

#: Where an inferred version came from. Unknown evidence stays distinct from
#: either transport or application evidence.
BEGIN_STRING_SOURCE = "begin_string"
APPLICATION_VERSION_SOURCE = "application_version"
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
_BEGIN_KEYS = pyarrow.array(["8", "beginstring"], pyarrow.string())
_APPLICATION_KEYS = pyarrow.array(["1128", "applverid"], pyarrow.string())
_DEFAULT_APPLICATION_KEYS = pyarrow.array(["1137", "defaultapplverid"], pyarrow.string())


@dataclasses.dataclass(frozen=True)
class TagIndex:
    """One FIX version's names as an Arrow value set, and the tags behind it."""

    #: Every name the version knows, lowercased. Lowercased *here* so the probe
    #: is one kernel and never a scan: case-insensitivity is the dictionary's
    #: business, not the parser's, and `pairs` keeps the log's own spelling.
    names: pyarrow.Array

    #: The tag behind each name, in the same order.
    tags: pyarrow.Array

    #: Every name a dotted key may sit *inside*: a component, a group, a field.
    #: What tells `NoPartyIDs[0].PartyID` -- `PartyID` in a group this version
    #: declares -- from `TECH.CLIENTID`, which is a vendor's own namespace and
    #: not `ClientID <109>` wearing a prefix.
    containers: pyarrow.Array = dataclasses.field(
        default_factory=lambda: pyarrow.array([], pyarrow.string())
    )

    @classmethod
    def from_tags(cls, tags: Mapping[str, int], containers: Iterable[str] = ()) -> TagIndex:
        """An index out of `FixRegistry.tags()`; an empty one resolves nothing."""
        inside = dict.fromkeys(name.lower() for name in (*tags, *containers))
        return cls(
            names=pyarrow.array(list(tags), pyarrow.string()),
            tags=pyarrow.array(list(tags.values()), TAG),
            containers=pyarrow.array(list(inside), pyarrow.string()),
        )

    def resolve(self, keys: Any) -> pyarrow.Array:
        """A key column as tag numbers, null where no reading finds one."""
        return self.resolve_with_match(keys)[0]

    def resolve_with_match(self, keys: Any) -> tuple[Any, Any, Any, Any]:
        """Resolved tags, registry hits, terminal names, and containment.

        Four answers because they are one scan: whether a key sits inside a
        container this version declares decides both whether its tail may be
        resolved and whether what stands in front of it is a component or a
        namespace, and computing it twice would read the same column
        through the same regex twice.
        """
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
        name_index = compute.index_in(compute.utf8_lower(reduced), value_set=self.names)
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
            compute.is_in(compute.utf8_lower(inner), value_set=self.containers), False
        )
        return compute.or_(plain, known)


@dataclasses.dataclass(eq=False)
class FixCodec(Convertible):
    """A log line read as FIX: which protocol it is, its pairs, and its tags."""

    #: Which category each line is. `Rules.into_default()` unless a
    #: document says otherwise.
    rules: Rules = dataclasses.field(default_factory=Rules)

    #: The dictionary names resolve through, **offline**: the default is the
    #: user's own cache (`~/.config/fix`), read and never scraped, because a
    #: parse that met its first bridge line and answered it by fetching seven
    #: thousand pages mid-batch would be a worse surprise than an unresolved
    #: name. Point it at `data/fix/` or `data/fix.zip` for the dictionary this repository
    #: publishes, or hand over `FixRegistry()` to let it scrape.
    registry: FixRegistry = dataclasses.field(default_factory=lambda: FixRegistry(offline=True))

    #: Values that mean the field is absent, dropped from the pairs before
    #: anything else looks at them. Empty keeps every pair.
    null_values: frozenset[str] = NULL_VALUES

    # -- the seam -----------------------------------------------------------

    def categorise(self, messages: Any, plugins: Any = None) -> Any:
        """One `protocol` name per row, in kernels."""
        return self.rules.into_arrow_protocol_array(messages, plugins)

    def into_pairs(self, messages: Any, protocol: str = NO_PROTOCOL) -> Any:
        """One `map<string, string>` per row: the message as the line spells it."""
        return self.into_payload_pairs(
            self.drop_null_values(self.into_raw_pairs(messages, protocol))
        )

    def into_payload_pairs(self, pairs: Any) -> Any:
        """`XmlData <213>` read as the message it carries, where it carries one.

        The standard calls tag 213 an XML data stream. Real bridge traffic puts
        a `key=value` message in it instead -- in every sampled line of a
        capture, with no counterexample -- and read as one opaque blob its
        fields are neither resolvable nor queryable. So a payload that reads as
        pairs becomes pairs, under `XmlData.<key>`, in the place the tag sat;
        one that reads as XML, or as nothing, stays exactly as it was.

        `XmlData.ClOrdID` then resolves like any other nested key -- a rendered
        `NoPartyIDs.PartyID` already does -- and a payload contradicting the
        message around it lifts neither, which is what `_liftable` is for.
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
            compute.equal(compute.utf8_lower(compute.utf8_trim_whitespace(keys)), _XML_DATA_NAME),
        )
        if not compute.any(carried, min_count=0).as_py():
            # Nearly every batch. Two string compares over the keys settle it,
            # and the regexes below -- which read *values*, the expensive half
            # of any pass here -- never run.
            return pairs
        payloads = compute.filter(items, carried)
        reads = compute.and_(
            compute.fill_null(compute.match_substring_regex(payloads, _PAYLOAD_PAIRS), False),
            compute.invert(
                compute.fill_null(compute.match_substring_regex(payloads, _LOOKS_XML), False)
            ),
        )
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

    def into_raw_pairs(self, messages: Any, protocol: str = NO_PROTOCOL) -> Any:
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
                messages, named=rule.named, entry_separator=rule.entry_separator
            )
        parts, positions = [], []
        for _, where in groups:
            parts.append(
                parse_arrow_array(
                    compute.take(messages, where),
                    named=rule.named,
                    entry_separator=rule.entry_separator,
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
                compute.extract_regex(text, BRIDGE_SEPARATOR_VECTOR), "sep"
            )
            bridge = compute.if_else(
                compute.is_in(bridge, value_set=pyarrow.array(SEPARATORS, pyarrow.string())),
                bridge,
                pyarrow.scalar(MARKER),
            )
            found = bridge if found is None else compute.coalesce(found, bridge)
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
        if compute.all(keep, min_count=0).as_py():
            return pairs.cast(_RAW_PAIRS)
        return _mapped(
            pairs,
            lengths,
            keep,
            compute.filter(keys, keep),
            compute.filter(items, keep),
            _RAW_PAIRS,
        )

    def into_kwargs(self, pairs: Any, version: str | None = None) -> Any:
        """Every field a message carried, as the one entry a parsed log stores.

        The transcription, in one place. A field arrives as the log spelled it
        and leaves carrying what the dictionary makes of it: the tag behind its
        name (`0` when nothing answers for it), the name itself, the namespace
        or component path in front of it, and -- where the field enumerates its
        values -- what the value means.

        One column, whatever the dictionary made of the field: a consumer that
        wants "the fields of this line" reads one column in wire order, and one
        that wants only the resolved ones filters on `tag`.
        """
        rows = len(pairs)
        if isinstance(pairs, pyarrow.ChunkedArray):
            parts = [self.into_kwargs(chunk, version) for chunk in pairs.chunks]
            return pyarrow.chunked_array(parts, type=KWARGS)
        if not rows or pairs.null_count == rows:
            # Every row of this slice is "not a message", which is most of a
            # capture, and the kernels below would run over an empty child
            # array to establish it.
            return pyarrow.nulls(rows, KWARGS)
        lengths, keys, values = _entries_of(pairs)
        return _kwargs(
            pairs,
            lengths,
            pyarrow.repeat(True, len(keys)),
            *self.transcribe(keys, values, version),
        )

    def transcribe(
        self, keys: Any, values: Any, version: str | None = None
    ) -> tuple[Any, Any, Any, Any, Any, Any]:
        """`(tag, key, value, trans, namespace, comp)` for a run of parsed fields.

        The whole of what the dictionary adds to a field, in kernels and in one
        place, so nothing downstream resolves a name or reads an enumeration a
        second way. `key` is the field's own name; whatever the line wrote in
        front of it goes to `comp` where this version declares that container
        and to `namespace` where it does not.
        """
        compute = pyarrow.compute
        tags, _, reduced, contained = self.index_of(version).resolve_with_match(keys)
        parts = compute.extract_regex(keys, _NAMESPACED_KEY)
        # Kept whole, entry index and all, so `lead.key` is the rendered key
        # back again and `NoPartyIDs[0]` stays a different place from `[1]`.
        lead = compute.struct_field(parts, "namespace")
        led = compute.fill_null(compute.greater(compute.binary_length(lead), 0), False)
        nothing = pyarrow.scalar(None, pyarrow.string())
        return (
            compute.fill_null(tags, pyarrow.scalar(0, TAG)),
            compute.fill_null(compute.struct_field(parts, "key"), keys),
            values,
            self.transcribed(tags, reduced, values, version),
            compute.if_else(compute.and_(led, compute.invert(contained)), lead, nothing),
            compute.if_else(compute.and_(led, contained), lead, nothing),
        )

    def transcribed(self, tags: Any, names: Any, values: Any, version: str | None = None) -> Any:
        """What each value means, where the field that carries it enumerates them.

        One composite lookup rather than one pass per enumerated tag: a version
        has hundreds of them, and a batch would otherwise pay a kernel for
        every one it does not carry.
        """
        compute = pyarrow.compute
        spelled, meanings = self.meanings(version)
        if not len(spelled):
            return pyarrow.nulls(len(values), pyarrow.string())
        # Keyed on the tag where there is one and on the name where there is
        # not, so a field FIX never numbered enumerates like any other.
        numbered = compute.fill_null(compute.greater(tags, 0), False)
        owner = compute.if_else(
            numbered,
            compute.fill_null(tags, 0).cast(pyarrow.string()),
            compute.utf8_lower(compute.fill_null(names, "")),
        )
        composite = compute.binary_join_element_wise(owner, values, "\x00")
        return compute.take(meanings, compute.index_in(composite, value_set=spelled))

    def meanings(self, version: str | None = None) -> tuple[Any, Any]:
        """`(tag-or-name and value, what it means)` for one version, built once."""
        if version not in self._meanings:
            self._meanings[version] = _meanings(self.registry, version)
        return self._meanings[version]

    def into_log_columns(
        self, pairs: Any, version: str | None = None
    ) -> tuple[Any, dict[str, Any]]:
        """One parsed pair column as the fields a log keeps and the columns it lifts."""
        kwargs = self.into_kwargs(pairs, version)
        components, kwargs = self.into_component_columns(kwargs, version)
        lifted, kwargs = self.into_lifted_columns(kwargs, version)
        return kwargs, {**components, **lifted}

    def into_lifted_columns(
        self, kwargs: Any, version: str | None = None
    ) -> tuple[dict[str, Any], Any]:
        """The fields worth a column of their own, and what is left of `kwargs`.

        Both kinds in one pass, because they are one question asked of one
        column: a numbered tag the log declares a column for, and a rendered
        name it does. They were two passes over two columns only because the
        two kinds were stored apart.
        """
        rows = len(kwargs)
        declared = self.named_fields()
        columns: dict[str, Any] = {
            name: pyarrow.nulls(rows, FLAT_TYPES[tag]) for tag, name in FLAT_COLUMNS.items()
        }
        columns.update(
            (field.name, pyarrow.nulls(rows, field.arrow_type)) for field in declared.values()
        )
        if isinstance(kwargs, pyarrow.ChunkedArray):
            kwargs = kwargs.combine_chunks()
        if not rows or kwargs.null_count == rows or version is None:
            return columns, kwargs
        compute = pyarrow.compute
        fields = self.flat_fields(version)
        lengths, parents, entries = _flattened(kwargs)
        tags = compute.struct_field(entries, "tag")
        keys = compute.struct_field(entries, "key")
        values = compute.struct_field(entries, "value")

        # One integer per liftable field: its tag, or a negative code for a
        # rendered name. Distinct codes are all `_liftable` needs, and one
        # integer key spares the composite string a mixed column would take.
        numbered = compute.fill_null(compute.is_in(tags, value_set=_tags_of(fields)), False)
        named = pyarrow.array(list(declared), pyarrow.string())
        matched = _declared_index(keys, _lead_of(entries), named)
        wanted = numbered
        code = tags
        if len(named):
            rendered = compute.and_(compute.equal(tags, 0), compute.is_valid(matched))
            wanted = compute.or_(numbered, rendered)
            code = compute.if_else(
                rendered,
                compute.subtract(pyarrow.scalar(-1, TAG), matched.cast(TAG)),
                tags,
            )
        if not compute.any(wanted, min_count=0).as_py():
            return columns, kwargs
        agreed, chosen = _liftable(parents, code, values)
        lift = compute.and_(wanted, agreed)
        taken = compute.and_(wanted, chosen)
        found = compute.filter(code, taken)
        where = compute.filter(parents, taken)
        selected_values = compute.filter(values, taken)
        row_ids = sequence(rows)
        for one in compute.unique(found).to_pylist():
            at = compute.equal(found, one)
            column = compute.filter(selected_values, at)
            column_rows = compute.filter(where, at)
            # Parent indices are row ordered and `_liftable` chose at most one
            # value per row, so covering every row already is the column.
            if len(column_rows) != rows:
                column = compute.take(column, compute.index_in(row_ids, value_set=column_rows))
            if one >= 0:
                columns[FLAT_COLUMNS[one]] = _cast(column, fields[one], FLAT_TYPES[one])
            else:
                field = declared[named[-1 - one].as_py()]
                columns[field.name] = cast_arrow_fix(column, field.arrow_type)
        keep = compute.or_(compute.invert(lift), _quote_group_structure(parents, tags))
        return columns, _kwargs(kwargs, lengths, keep, *_columns_of(entries, keep))

    def into_component_columns(
        self, kwargs: Any, version: str | None = None
    ) -> tuple[dict[str, Any], Any]:
        """Structured FIX components and what is left of `kwargs`."""
        parties, rest = self.parties_of(version).into_arrow_arrays(kwargs)
        return {"parties": parties}, rest

    # -- versions -----------------------------------------------------------

    def version_of(
        self, message: str | None, protocol: str = NO_PROTOCOL
    ) -> tuple[str | None, str]:
        """Which FIX version a message is read under, and where that came from."""
        if message:
            parse_protocol = protocol
            if protocol == NO_PROTOCOL:
                parse_protocol = self.rules.categorise(message).protocol
            pairs = self.into_pairs(pyarrow.array([message]), parse_protocol)
            begin_column, application_column, default_column = _version_columns(pairs)
            begin = begin_column[0].as_py()
            return _version_from_evidence(
                begin,
                application_column[0].as_py(),
                default_column[0].as_py(),
                self._spellings,
            )
        return None, NO_SOURCE

    def versions_of(self, messages: Any, protocol: str = NO_PROTOCOL) -> pyarrow.Array:
        """Resolved version per row, after parsing each row's actual separator."""
        if isinstance(messages, pyarrow.ChunkedArray):
            messages = messages.combine_chunks()
        if protocol != NO_PROTOCOL:
            return self.versions_of_pairs(self.into_pairs(messages, protocol), protocol)
        if not len(messages):
            return pyarrow.array([], pyarrow.string())
        groups = list(groups_of(self.categorise(messages)))
        parts, positions = [], []
        for category, where in groups:
            pairs = self.into_pairs(pyarrow.compute.take(messages, where), category.as_py())
            parts.append(self.versions_of_pairs(pairs, NO_PROTOCOL))
            positions.append(where)
        return scattered(parts, positions)

    def versions_of_pairs(self, pairs: Any, protocol: str = NO_PROTOCOL) -> pyarrow.Array:
        """Resolved application version per parsed row."""
        named = self.rules.rule(protocol).named if protocol != NO_PROTOCOL else None
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

    def tag_field(self, tag: int, version: str | None = None) -> Field | None:
        """The dictionary's own declaration of one tag, or None when it has none."""
        if version is None:
            return None
        try:
            return self.registry.field(tag, version)
        except (KeyError, OSError, ValueError):
            return None

    def flat_fields(self, version: str | None = None) -> dict[int, Field]:
        """Promoted registry fields, with contract fallbacks only for a cold registry."""
        if version not in self._flat_fields:
            if version is None:
                self._flat_fields[version] = {}
            elif not self.registry.fields_available(version):
                self._flat_fields[version] = {tag: FLAT_DEFAULTS[tag] for tag in FLAT_COLUMNS}
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
                self._named = named_columns(self.registry)
            except (OSError, ValueError):
                self._named = NAMED_COLUMNS
        return self._named

    def parties_of(self, version: str | None = None) -> Parties:
        """Version-aware Parties extractor, cached with the tag index."""
        if version not in self._parties:
            components, fallback = self._party_declaration(version)
            self._parties[version] = Parties(
                components=components,
                names=self._tags(version),
                fallback=fallback,
            )
        return self._parties[version]

    def _party_declaration(self, version: str | None) -> tuple[list[SpecComponent], bool]:
        """One version's Parties declaration, and whether to keep the legacy tags.

        Three answers, and the middle one is why this is not two lines. A
        registry that *declares* the component answers it. A registry that
        declares components and has no Parties among them answers nothing, and
        is right to: 4.0 through 4.2 have no such component. A registry that
        declares no components at all for a version it otherwise knows is
        neither -- it is a store written before component declarations were
        kept, or a projection that dropped them -- and answering nothing there
        extracted no party from any message at all, silently, for every version
        the wire named. That one says so and keeps the legacy tags.
        """
        if version is None:
            return [], False
        try:
            declared = self.registry.components(version)
        except (KeyError, OSError, ValueError):
            declared = []
        if any(component.name.lower() == "parties" for component in declared):
            return list(declared), False
        try:
            available = self.registry.components_available(version)
        except (KeyError, OSError, ValueError):
            available = False
        if available:
            return [], False
        warnings.warn(
            f"the FIX registry holds no component declarations for {version!r}: "
            "party extraction falls back to the legacy tags. Rebuild the registry "
            "so its components travel with its fields.",
            RuntimeWarning,
            stacklevel=3,
        )
        return [], True

    # -- held state ---------------------------------------------------------

    @cached_property
    def _indexes(self) -> dict[str | None, TagIndex]:
        return {}

    @cached_property
    def _parties(self) -> dict[str | None, Parties]:
        return {}

    @cached_property
    def _meanings(self) -> dict[str | None, tuple[Any, Any]]:
        return {}

    _named: Mapping[str, Field] | None = None

    @cached_property
    def _flat_fields(self) -> dict[str | None, dict[int, Field]]:
        return {}

    @cached_property
    def _spellings(self) -> dict[str, str]:
        """`{version key: canonical spelling}` for every version the store holds."""
        return dict(version_spellings(self.registry))

    def _tags(self, version: str | None) -> dict[str, int]:
        """`{name: tag}` for one explicit version; empty when unknown."""
        if version is None:
            return {}
        try:
            return self.registry.tags(version)
        except (KeyError, OSError, ValueError):
            return {}

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


def infer_version_from_pairs(
    pairs: Iterable[tuple[Any, Any]], registry: FixRegistry | None = None
) -> tuple[str | None, str]:
    """Infer one FIX application version from tags 8, 1128 and 1137."""
    evidence: dict[str, str] = {}
    for key, value in pairs:
        text = str(key)
        member = re.search(_MEMBER_NAME_VECTOR, text, re.ASCII)
        name = member.group("name").lower() if member is not None else text.lower()
        if name in {"10", "checksum"}:
            break
        selected = {
            "8": "begin",
            "beginstring": "begin",
            "1128": "application",
            "applverid": "application",
            "1137": "default",
            "defaultapplverid": "default",
        }.get(name)
        rendered = str(value).strip() if value is not None else ""
        if selected is not None and rendered:
            evidence.setdefault(selected, rendered)
    selected_registry = registry or FixRegistry.from_builtin()
    return _version_from_evidence(
        evidence.get("begin"),
        evidence.get("application"),
        evidence.get("default"),
        version_spellings(selected_registry),
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
        reduced = compute.utf8_lower(reduced)
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
    """Whatever a stored field wrote in front of its name, either place it went."""
    compute = pyarrow.compute
    return compute.coalesce(
        compute.struct_field(entries, "namespace"), compute.struct_field(entries, "comp")
    )


def _declared_index(keys: Any, lead: Any, declared: Any) -> Any:
    """Where each field sits in `declared`, or null; the whole name first.

    Whole first because a namespace is part of a name -- `TECH.CLIENTID`
    is not `CLIENTID` -- and the tail second because a dictionary that declares
    only one of the two spellings still answers for the other.
    """
    compute = pyarrow.compute
    if not len(declared):
        return pyarrow.nulls(len(keys), pyarrow.int32())
    tail = compute.utf8_lower(keys)
    whole = compute.if_else(
        compute.is_valid(lead),
        compute.binary_join_element_wise(compute.utf8_lower(lead), tail, "."),
        tail,
    )
    found = compute.index_in(whole, value_set=declared)
    return compute.if_else(
        compute.is_valid(found), found, compute.index_in(tail, value_set=declared)
    )


def _liftable(parents: Any, keys: Any, values: Any) -> tuple[Any, Any]:
    """`(every entry of a liftable key, the one that becomes the column)`.

    Both, because a key is lifted or it is not, and every entry of a lifted key
    leaves the residual pairs with it -- otherwise a line that wrote the same
    fact twice keeps a copy of what was already promoted.

    Which entries are the only *reading* of their key in their row.

    One composite key per entry -- the row shifted above the tag, so no pair of
    them can collide -- counted in a single `value_counts`. Per entry and not
    per tag, because a batch carries thirty-odd liftable tags and one pass over
    the child array answers for all of them at once.

    The reading and not the entry, because a bridge writes the same fact twice
    on purpose. A rendered line carries two namespaces -- `#Side` as it arrived
    and `Side` after enrichment -- and on a third to a half of the lines in a
    real capture some fields appear in both. Refusing to lift a key that
    repeats dropped every one of those out of its typed column and into the
    residual pairs, silently, on lines that agreed with themselves.

    Repeats that *disagree* are still refused: two different values under one
    key is a group, or an enrichment that rewrote something, and picking
    between them would be a guess.
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


#: The parts of one stored field, in the order `KWARGS` declares them.
_PARTS: tuple[str, ...] = ("tag", "key", "value", "trans", "namespace", "comp")


def _kwargs(source: Any, lengths: Any, keep: Any, *parts: Any) -> pyarrow.Array:
    """A `KWARGS` column out of the entries `keep` admits, in the source's rows.

    `keep` is over the source's own flattened entries and `parts` are already
    filtered by it: the offsets come from the mask and the children from the
    filter, which is how every other split here rebuilds a list column.
    """
    entries = pyarrow.StructArray.from_arrays(
        [
            part.cast(KWARGS.value_type.field(name).type, safe=False)
            for name, part in zip(_PARTS, parts, strict=True)
        ],
        fields=[KWARGS.value_type.field(name) for name in _PARTS],
    )
    return pyarrow.ListArray.from_arrays(
        _selected_offsets(source, lengths, keep), entries, type=KWARGS
    )


def _flattened(kwargs: Any) -> tuple[Any, Any, Any]:
    """`(row lengths, parent row per entry, the entries)` for a `KWARGS` column."""
    compute = pyarrow.compute
    listed = _listed(kwargs)
    return (
        compute.fill_null(compute.list_value_length(listed), 0).cast(pyarrow.int32()),
        compute.list_parent_indices(listed).cast(pyarrow.int64()),
        compute.list_flatten(listed),
    )


def _columns_of(entries: Any, keep: Any) -> tuple[Any, ...]:
    """The children of the entries a mask keeps, in declaration order."""
    compute = pyarrow.compute
    return tuple(compute.filter(compute.struct_field(entries, name), keep) for name in _PARTS)


def _cast(column: Any, field: Field, arrow_type: pyarrow.DataType) -> Any:
    """One lifted column at the width its log column stores."""
    read = cast_arrow_fix(column, field.arrow_type)
    return read if read.type.equals(arrow_type) else read.cast(arrow_type, safe=False)


def _tags_of(fields: Mapping[int, Field]) -> pyarrow.Array:
    """The tags a version can lift, as the value set a probe takes."""
    return pyarrow.array(sorted(fields), TAG)


def _meanings(registry: FixRegistry, version: str | None) -> tuple[Any, Any]:
    """`(owner and value, what it means)` for every enumeration one version has.

    The owner is the tag where a field has one and its folded name where it
    does not, so a field FIX never numbered enumerates like any other. Prose
    over symbol: `Side <54>` value `1` is "Buy" for a person and `BUY` for a
    program, and a log column is read by people.
    """
    spelled: list[str] = []
    meanings: list[str] = []
    if version is None:
        return pyarrow.array(spelled, pyarrow.string()), pyarrow.array(meanings, pyarrow.string())
    try:
        members = registry.fields(version)
    except (KeyError, OSError, ValueError):
        members = []
    for member in members:
        tag = member.fix.get("tag")
        owner = tag if tag else member.name.lower()
        found = _json_mapping(member.fix.get("values")) or _json_mapping(
            member.fix.get("value_names")
        )
        for value, meaning in found.items():
            spelled.append(f"{owner}\x00{value}")
            meanings.append(meaning)
    return pyarrow.array(spelled, pyarrow.string()), pyarrow.array(meanings, pyarrow.string())


def _json_mapping(value: str | None) -> dict[str, str]:
    """One `fix:` metadata mapping, or nothing for anything that is not one."""
    try:
        decoded = json.loads(value or "{}")
    except (TypeError, ValueError):  # pragma: no cover - the registry writes these
        return {}
    return (
        {str(key): str(item) for key, item in decoded.items()} if isinstance(decoded, dict) else {}
    )


def _entries_of(pairs: Any) -> tuple[Any, Any, Any]:
    """One pair column as `(row lengths, keys, values)`."""
    compute = pyarrow.compute
    listed = _listed(pairs)
    lengths = compute.fill_null(compute.list_value_length(listed), 0).cast(pyarrow.int32())
    entries = compute.list_flatten(listed)
    return lengths, compute.struct_field(entries, 0), compute.struct_field(entries, 1)


def _mapped(
    source: Any,
    lengths: Any,
    mask: Any,
    keys: Any,
    items: Any,
    arrow_type: pyarrow.DataType,
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
    if pyarrow.types.is_map(arrow_type):
        return pyarrow.MapArray.from_arrays(offsets, keys, items, type=arrow_type)
    entries = pyarrow.StructArray.from_arrays(
        [keys, items], fields=[arrow_type.value_type.field(0), arrow_type.value_type.field(1)]
    )
    return pyarrow.ListArray.from_arrays(offsets, entries, type=arrow_type)


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
