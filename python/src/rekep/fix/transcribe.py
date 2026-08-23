"""A log line's pairs as the tags FIX gave them -- as far as the dictionary goes."""

from __future__ import annotations

import dataclasses
import re
from functools import cached_property
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field
from rekep.fields.arrays import groups_of, scattered, sequence
from rekep.fix.columns import COLUMNS as FLAT_COLUMNS
from rekep.fix.columns import DECLARATIONS as FLAT_DEFAULTS
from rekep.fix.columns import NAMED as NAMED_COLUMNS
from rekep.fix.columns import QUOTE_GROUP_COUNTS, QUOTE_GROUP_STRUCTURE
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
    parse_arrow_array,
)
from rekep.fix.registry import FixRegistry
from rekep.fix.rules import NO_PROTOCOL, Rules

#: What a resolved key is: the tag number, as the `int32` every other code
#: column here is.
TAG: pyarrow.DataType = pyarrow.int32()

# Stored pairs are lists because repeated keys are data, not a malformed map.
# The parser keeps its efficient map-shaped intermediate private.
_VALUE = pyarrow.field("value", pyarrow.string(), nullable=False)
_RAW_PAIRS: pyarrow.DataType = pyarrow.map_(pyarrow.string(), _VALUE)


def _pair_list(key_type: pyarrow.DataType) -> pyarrow.DataType:
    entry = pyarrow.struct(
        [
            pyarrow.field("key", key_type, nullable=False),
            _VALUE,
        ]
    )
    return pyarrow.list_(pyarrow.field("item", entry, nullable=False))


FIX_TAGS: pyarrow.DataType = _pair_list(TAG)
KEYVAL: pyarrow.DataType = _pair_list(pyarrow.string())
FIX_MISS_TAGS: pyarrow.DataType = pyarrow.list_(
    pyarrow.field("item", pyarrow.string(), nullable=False)
)

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

#: Where each of the three answers to "which version" came from. Recorded
#: rather than inferred later: `4.4` resolved off a BeginString and `4.4`
#: because nobody said otherwise are the same string and not the same fact.
BEGIN_STRING_SOURCE = "begin_string"
APPLICATION_VERSION_SOURCE = "application_version"
RULE_SOURCE = "rule"
DEFAULT_SOURCE = "default"
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

    @classmethod
    def from_tags(cls, tags: dict[str, int]) -> TagIndex:
        """An index out of `FixRegistry.tags()`; an empty one resolves nothing."""
        return cls(
            names=pyarrow.array(list(tags), pyarrow.string()),
            tags=pyarrow.array(list(tags.values()), TAG),
        )

    def resolve(self, keys: Any) -> pyarrow.Array:
        """A key column as tag numbers, null where no reading finds one."""
        return self.resolve_with_match(keys)[0]

    def resolve_with_match(self, keys: Any) -> tuple[pyarrow.Array, pyarrow.Array, pyarrow.Array]:
        """Resolved tags, registry hits, and terminal key names in one scan."""
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
            )
        reduced = compute.fill_null(
            compute.struct_field(compute.extract_regex(keys, _MEMBER_NAME_VECTOR), "name"), ""
        )
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
        return resolved, matched, reduced

    def matched(self, keys: Any, resolved: Any | None = None) -> pyarrow.Array:
        """Whether each key has a field in this registry version."""
        compute = pyarrow.compute
        if resolved is None:
            return self.resolve_with_match(keys)[1]
        reduced = compute.fill_null(
            compute.struct_field(compute.extract_regex(keys, _MEMBER_NAME_VECTOR), "name"), ""
        )
        numeric = compute.fill_null(compute.match_substring_regex(reduced, _IS_TAG), False)
        numeric_known = compute.fill_null(compute.is_in(resolved, value_set=self.tags), False)
        return compute.if_else(numeric, numeric_known, compute.is_valid(resolved))


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

    #: Which FIX version to resolve names against when neither the message nor
    #: the rule says. None means every version the dictionary holds, newest
    #: winning -- which is what a name means when nobody said which version.
    fix_version: str | None = None

    #: Values that mean the field is absent, dropped from the pairs before
    #: anything else looks at them. Empty keeps every pair.
    null_values: frozenset[str] = NULL_VALUES

    # -- the seam -----------------------------------------------------------

    def categorise(self, messages: Any, drivers: Any = None) -> Any:
        """One `protocol` name per row, in kernels."""
        return self.rules.into_arrow_protocol_array(messages, drivers)

    def into_pairs(self, messages: Any, protocol: str = NO_PROTOCOL) -> Any:
        """One `map<string, string>` per row: the message as the line spells it."""
        return self.drop_null_values(self.into_raw_pairs(messages, protocol))

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

    def into_fix_pairs(self, pairs: Any, version: str | None = None) -> tuple[Any, Any, Any]:
        """Split pairs into FIX tags, residual pairs, and registry misses."""
        if len(pairs) and pairs.null_count == len(pairs):
            # Every row of this slice is "not a message", which is most of a
            # capture. Both halves are null, and the kernels below would run
            # over an empty child array to establish it.
            return (
                pyarrow.nulls(len(pairs), FIX_TAGS),
                pyarrow.nulls(len(pairs), KEYVAL),
                pyarrow.nulls(len(pairs), FIX_MISS_TAGS),
            )
        index = self.index_of(version)
        if isinstance(pairs, pyarrow.ChunkedArray):
            parts = [self.into_fix_pairs(chunk, version) for chunk in pairs.chunks]
            return (
                pyarrow.chunked_array([tags for tags, _, _ in parts], type=FIX_TAGS),
                pyarrow.chunked_array([rest for _, rest, _ in parts], type=KEYVAL),
                pyarrow.chunked_array([misses for _, _, misses in parts], type=FIX_MISS_TAGS),
            )
        compute = pyarrow.compute
        lengths, keys, items = _entries_of(pairs)
        tags, matched, _ = index.resolve_with_match(keys)
        resolved_key = compute.is_valid(tags)
        unknown = compute.invert(resolved_key)
        resolved = _mapped(
            pairs,
            lengths,
            resolved_key,
            compute.filter(tags, resolved_key),
            compute.filter(items, resolved_key),
            FIX_TAGS,
        )
        rest = _mapped(
            pairs,
            lengths,
            unknown,
            compute.filter(keys, unknown),
            compute.filter(items, unknown),
            KEYVAL,
        )
        missed = compute.invert(matched)
        misses = _selected_list(
            pairs,
            lengths,
            missed,
            compute.filter(keys, missed),
            FIX_MISS_TAGS,
        )
        return resolved, rest, misses

    def into_log_columns(
        self, pairs: Any, version: str | None = None
    ) -> tuple[Any, Any, Any, dict[str, Any]]:
        """Project one parsed pair column without rebuilding its children between views."""
        # A subclass that customises a projection stage owns that contract;
        # use the public stages so the fused implementation never bypasses it.
        concrete = type(self)
        if (
            concrete.into_flat_columns is not FixCodec.into_flat_columns
            or concrete.into_component_columns is not FixCodec.into_component_columns
            or concrete.into_named_columns is not FixCodec.into_named_columns
        ):
            tags, keyval, misses = self.into_fix_pairs(pairs, version)
            components, tags = self.into_component_columns(tags, version)
            flat, tags = self.into_flat_columns(tags, version)
            named, keyval = self.into_named_columns(keyval)
            return tags, keyval, misses, {**components, **flat, **named}
        if isinstance(pairs, pyarrow.ChunkedArray):
            pairs = pairs.combine_chunks()
        rows = len(pairs)
        if not rows or pairs.null_count == rows:
            tags, keyval, misses = self.into_fix_pairs(pairs, version)
            components, tags = self.into_component_columns(tags, version)
            flat, tags = self.into_flat_columns(tags, version)
            named, keyval = self.into_named_columns(keyval)
            return tags, keyval, misses, {**components, **flat, **named}

        compute = pyarrow.compute
        listed = _listed(pairs)
        lengths = compute.fill_null(compute.list_value_length(listed), 0).cast(pyarrow.int32())
        parents = compute.list_parent_indices(listed).cast(pyarrow.int64())
        entries = compute.list_flatten(listed)
        keys, items = compute.struct_field(entries, 0), compute.struct_field(entries, 1)
        tags, matched, reduced = self.index_of(version).resolve_with_match(keys)
        resolved = compute.is_valid(tags)
        unknown = compute.invert(resolved)

        fields = self.flat_fields(version)
        available = pyarrow.array(sorted(fields), TAG)
        flat_candidate = compute.and_(
            resolved,
            compute.fill_null(compute.is_in(tags, value_set=available), False),
        )
        flat_lift = (
            compute.and_(flat_candidate, _once(parents, tags))
            if compute.any(flat_candidate, min_count=0).as_py()
            else pyarrow.repeat(False, len(tags))
        )
        flat_found = compute.filter(tags, flat_lift)
        flat_where = compute.filter(parents, flat_lift)
        flat_values = compute.filter(items, flat_lift)
        flat = {name: pyarrow.nulls(rows, FLAT_TYPES[tag]) for tag, name in FLAT_COLUMNS.items()}
        row_ids = sequence(rows)
        for tag in compute.unique(flat_found).to_pylist():
            at = compute.equal(flat_found, tag)
            selected = compute.filter(flat_values, at)
            selected_rows = compute.filter(flat_where, at)
            column = (
                selected
                if len(selected_rows) == rows
                else compute.take(selected, compute.index_in(row_ids, value_set=selected_rows))
            )
            column = cast_arrow_fix(column, fields[tag].arrow_type)
            if not column.type.equals(FLAT_TYPES[tag]):
                column = column.cast(FLAT_TYPES[tag], safe=False)
            flat[FLAT_COLUMNS[tag]] = column

        named_names = compute.utf8_lower(reduced)
        named_keys = pyarrow.array(list(NAMED_COLUMNS), pyarrow.string())
        named_index = compute.index_in(named_names, value_set=named_keys)
        # Unknown keys share an irrelevant sentinel. Known named fields retain
        # distinct integer codes, avoiding composite string construction.
        uniqueness_key = compute.fill_null(named_index, pyarrow.scalar(-1, pyarrow.int32()))
        named_candidate = compute.and_(unknown, compute.is_valid(named_index))
        named_lift = (
            compute.and_(named_candidate, _once(parents, uniqueness_key))
            if compute.any(named_candidate, min_count=0).as_py()
            else pyarrow.repeat(False, len(keys))
        )
        named_found = compute.filter(named_names, named_lift)
        named_where = compute.filter(parents, named_lift)
        named_values = compute.filter(items, named_lift)
        named = {
            field.name: pyarrow.nulls(rows, field.arrow_type) for field in NAMED_COLUMNS.values()
        }
        for name in compute.unique(named_found).to_pylist():
            at = compute.equal(named_found, name)
            selected = compute.filter(named_values, at)
            selected_rows = compute.filter(named_where, at)
            column = (
                selected
                if len(selected_rows) == rows
                else compute.take(selected, compute.index_in(row_ids, value_set=selected_rows))
            )
            named[name] = cast_arrow_fix(column, NAMED_COLUMNS[name].arrow_type)

        quote_structure = _quote_group_structure(parents, tags)
        fix_keep = compute.and_(
            resolved,
            compute.or_(compute.invert(flat_lift), quote_structure),
        )
        fix_tags = _mapped(
            pairs,
            lengths,
            fix_keep,
            compute.filter(tags, fix_keep),
            compute.filter(items, fix_keep),
            FIX_TAGS,
        )
        components, fix_tags = self.into_component_columns(fix_tags, version)
        keyval_keep = compute.and_(unknown, compute.invert(named_lift))
        keyval = _mapped(
            pairs,
            lengths,
            keyval_keep,
            compute.filter(keys, keyval_keep),
            compute.filter(items, keyval_keep),
            KEYVAL,
        )
        missed = compute.invert(matched)
        misses = _selected_list(
            pairs,
            lengths,
            missed,
            compute.filter(keys, missed),
            FIX_MISS_TAGS,
        )
        return fix_tags, keyval, misses, {**components, **flat, **named}

    def into_flat_columns(
        self, tags: Any, version: str | None = None
    ) -> tuple[dict[str, Any], Any]:
        """Lift fields using the selected registry, or contract types if it is unavailable."""
        rows = len(tags)
        columns: dict[str, Any] = {
            name: pyarrow.nulls(rows, FLAT_TYPES[tag]) for tag, name in FLAT_COLUMNS.items()
        }
        if isinstance(tags, pyarrow.ChunkedArray):
            tags = tags.combine_chunks()
        if not rows or tags.null_count == rows:
            return columns, tags
        compute = pyarrow.compute
        fields = self.flat_fields(version)
        available = pyarrow.array(sorted(fields), TAG)
        if not len(available):
            return columns, tags
        listed = _listed(tags)
        lengths = compute.fill_null(compute.list_value_length(listed), 0).cast(pyarrow.int32())
        parents = compute.list_parent_indices(listed).cast(pyarrow.int64())
        entries = compute.list_flatten(listed)
        keys, items = compute.struct_field(entries, 0), compute.struct_field(entries, 1)
        lift = compute.and_(
            compute.fill_null(compute.is_in(keys, value_set=available), False),
            _once(parents, keys),
        )
        if not compute.any(lift, min_count=0).as_py():
            return columns, tags
        found = compute.filter(keys, lift)
        where = compute.filter(parents, lift)
        values = compute.filter(items, lift)
        row_ids = sequence(rows)
        for tag in compute.unique(found).to_pylist():
            at = compute.equal(found, tag)
            selected = compute.filter(values, at)
            selected_rows = compute.filter(where, at)
            # Parent indices are row ordered and `_once` admitted at most one
            # value per row. Covering every row therefore already is the
            # target column; no hash index or take is needed.
            column = (
                selected
                if len(selected_rows) == rows
                else compute.take(selected, compute.index_in(row_ids, value_set=selected_rows))
            )
            field = fields[tag]
            column = cast_arrow_fix(column, field.arrow_type)
            if not column.type.equals(FLAT_TYPES[tag]):
                column = column.cast(FLAT_TYPES[tag], safe=False)
            columns[FLAT_COLUMNS[tag]] = column
        carried = compute.or_(compute.invert(lift), _quote_group_structure(parents, keys))
        rest = _mapped(
            tags,
            lengths,
            carried,
            compute.filter(keys, carried),
            compute.filter(items, carried),
            FIX_TAGS,
        )
        return columns, rest

    def into_named_columns(self, pairs: Any) -> tuple[dict[str, Any], Any]:
        """Configured rendered names lifted from the residual ordered pairs."""
        rows = len(pairs)
        columns = {
            field.name: pyarrow.nulls(rows, field.arrow_type) for field in NAMED_COLUMNS.values()
        }
        if isinstance(pairs, pyarrow.ChunkedArray):
            pairs = pairs.combine_chunks()
        if not rows or pairs.null_count == rows:
            return columns, pairs
        compute = pyarrow.compute
        listed = _listed(pairs)
        lengths = compute.fill_null(compute.list_value_length(listed), 0).cast(pyarrow.int32())
        parents = compute.list_parent_indices(listed).cast(pyarrow.int64())
        entries = compute.list_flatten(listed)
        keys, items = compute.struct_field(entries, 0), compute.struct_field(entries, 1)
        names = compute.utf8_lower(
            compute.fill_null(
                compute.struct_field(compute.extract_regex(keys, _MEMBER_NAME_VECTOR), "name"),
                keys,
            )
        )
        lift = compute.and_(
            compute.fill_null(
                compute.is_in(names, value_set=pyarrow.array(list(NAMED_COLUMNS))), False
            ),
            _once(parents, names),
        )
        if not compute.any(lift, min_count=0).as_py():
            return columns, pairs
        found = compute.filter(names, lift)
        where = compute.filter(parents, lift)
        values = compute.filter(items, lift)
        row_ids = sequence(rows)
        for name in compute.unique(found).to_pylist():
            at = compute.equal(found, name)
            selected = compute.filter(values, at)
            selected_rows = compute.filter(where, at)
            column = (
                selected
                if len(selected_rows) == rows
                else compute.take(selected, compute.index_in(row_ids, value_set=selected_rows))
            )
            field = NAMED_COLUMNS[name]
            columns[field.name] = cast_arrow_fix(column, field.arrow_type)
        carried = compute.invert(lift)
        rest = _mapped(
            pairs,
            lengths,
            carried,
            compute.filter(keys, carried),
            compute.filter(items, carried),
            KEYVAL,
        )
        return columns, rest

    def into_component_columns(
        self, tags: Any, version: str | None = None
    ) -> tuple[dict[str, Any], Any]:
        """Structured FIX components and the residual ordered tags."""
        parties, rest = self.parties_of(version).into_arrow_arrays(tags)
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
            if begin is not None:
                named = self.version_named(begin)
                if _version_key(begin).startswith("FIXT"):
                    application = application_column[0].as_py()
                    if application is not None:
                        resolved = _APPL_VERSIONS.get(application) or self.version_named(
                            application
                        )
                        if resolved is not None:
                            return resolved, APPLICATION_VERSION_SOURCE
                        return self._fallback_version(protocol)
                    default = default_column[0].as_py()
                    if default is not None:
                        resolved = _APPL_VERSIONS.get(default) or self.version_named(default)
                        if resolved is not None:
                            return resolved, APPLICATION_VERSION_SOURCE
                    return self._fallback_version(protocol)
                if named is not None:
                    return named, BEGIN_STRING_SOURCE
        return self._fallback_version(protocol)

    def _fallback_version(self, protocol: str) -> tuple[str | None, str]:
        """Rule, configured default, or no application version."""
        rule = self.rules.rule(protocol)
        if rule.fix_version:
            return rule.fix_version, RULE_SOURCE
        if self.fix_version:
            return self.fix_version, DEFAULT_SOURCE
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
            # Categorisation selects only the grammar. An omitted public
            # protocol still uses the configured default, as `version_of` does.
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
        fallback, _ = self._fallback_version(protocol)
        if fallback is None:
            versions = pyarrow.nulls(len(pairs), pyarrow.string())
        else:
            versions = pyarrow.repeat(pyarrow.scalar(fallback), len(pairs))
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
        wanted = version if version is not None else self.fix_version
        if wanted not in self._indexes:
            self._indexes[wanted] = TagIndex.from_tags(self._tags(wanted))
        return self._indexes[wanted]

    def tag_field(self, tag: int, version: str | None = None) -> Field | None:
        """The dictionary's own declaration of one tag, or None when it has none."""
        try:
            return self.registry.field(tag, version if version is not None else self.fix_version)
        except (KeyError, OSError, ValueError):
            return None

    def flat_fields(self, version: str | None = None) -> dict[int, Field]:
        """Promoted registry fields, with contract fallbacks only for a cold registry."""
        wanted = version if version is not None else self.fix_version
        if wanted not in self._flat_fields:
            if not self.registry.fields_available(wanted):
                self._flat_fields[wanted] = {tag: FLAT_DEFAULTS[tag] for tag in FLAT_COLUMNS}
            else:
                self._flat_fields[wanted] = {
                    tag: field
                    for tag in FLAT_COLUMNS
                    if (field := self.tag_field(tag, wanted)) is not None
                }
        return self._flat_fields[wanted]

    def parties_of(self, version: str | None = None) -> Parties:
        """Version-aware Parties extractor, cached with the tag index."""
        wanted = version if version is not None else self.fix_version
        if wanted not in self._parties:
            components = []
            try:
                candidates = (wanted,) if wanted is not None else self.registry.versions
                for candidate in candidates:
                    declared = self.registry.components(candidate)
                    if any(component.name.lower() == "parties" for component in declared):
                        components.extend(declared)
                        break
            except (KeyError, OSError, ValueError):
                components = []
            self._parties[wanted] = Parties(
                components=components,
                names=self._tags(wanted),
                fallback=False,
            )
        return self._parties[wanted]

    # -- held state ---------------------------------------------------------

    @cached_property
    def _indexes(self) -> dict[str | None, TagIndex]:
        return {}

    @cached_property
    def _parties(self) -> dict[str | None, Parties]:
        return {}

    @cached_property
    def _flat_fields(self) -> dict[str | None, dict[int, Field]]:
        return {}

    @cached_property
    def _spellings(self) -> dict[str, str]:
        """`{version key: canonical spelling}` for every version the store holds."""
        try:
            versions = self.registry.versions
        except (OSError, ValueError):
            return {}
        return {_version_key(version): version for version in versions}

    def _tags(self, version: str | None) -> dict[str, int]:
        """`{name: tag}` for one version, or for all of them; empty when unknown."""
        try:
            return self.registry.tags(version)
        except (KeyError, OSError, ValueError):
            return {}


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


def _once(parents: Any, keys: Any) -> Any:
    """Which entries are the only one of their key in their row.

    One composite key per entry -- the row shifted above the tag, so no pair of
    them can collide -- counted in a single `value_counts`. Per entry and not
    per tag, because a batch carries thirty-odd liftable tags and one pass over
    the child array answers for all of them at once.
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
        return pyarrow.repeat(True, len(composite))
    counted = compute.value_counts(composite)
    seen = compute.take(
        counted.field("counts"), compute.index_in(composite, value_set=counted.field("values"))
    )
    return compute.equal(seen, 1)


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


def _selected_list(
    source: Any,
    lengths: Any,
    mask: Any,
    values: Any,
    arrow_type: pyarrow.DataType,
) -> pyarrow.Array:
    """Selected child values rebuilt under their source rows and nulls."""
    return pyarrow.ListArray.from_arrays(
        _selected_offsets(source, lengths, mask), values, type=arrow_type
    )


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
