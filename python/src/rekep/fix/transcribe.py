"""A log line's pairs as the tags FIX gave them -- as far as the dictionary goes.

The registry side of the message layer. What the parser produces is
`(key, value)` text, because that is what the line says; what a reader wants is
`(tag, value)`, because that is what every other FIX consumer speaks. This is
the step between, and its one rule is that **it never guesses**: a name the
dictionary answers for becomes its tag, and a name it does not stays exactly as
the log spelled it. No fuzzy match, no `search()` fallback, nothing dropped.
"""

from __future__ import annotations

import dataclasses
import re
from functools import cached_property
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field
from rekep.fix.message import _BEGIN, _MEMBER_NAME_VECTOR, parse_arrow_array
from rekep.fix.registry import FixRegistry
from rekep.fix.rules import Rule, Rules

#: What a resolved key is: the tag number, as the `int32` every other code
#: column here is.
TAG: pyarrow.DataType = pyarrow.int32()

#: The two map shapes a parsed line lands in. A **map** and not a
#: `list<struct>` because tags repeat -- a repeating group *is* tags repeating
#: -- and an Arrow map is the one nested type that keeps duplicate keys in
#: order, which is exactly why `parse_arrow_array` already returns one.
FIX_TAGS: pyarrow.DataType = pyarrow.map_(TAG, pyarrow.string())
KEYVAL: pyarrow.DataType = pyarrow.map_(pyarrow.string(), pyarrow.string())

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
RULE_SOURCE = "rule"
DEFAULT_SOURCE = "default"
NO_SOURCE = "none"


@dataclasses.dataclass(frozen=True)
class TagIndex:
    """One FIX version's names as an Arrow value set, and the tags behind it.

    Built once per version and probed with one kernel, which is the whole
    point: raced against a Python dict over `to_pylist()` and against a polars
    join on 720,896 keys and 1,566 names, `index_in` ran at **74.8M-78.7M
    keys/s** -- 24x the dictionary-rebuilt-per-call path this replaced and 8x
    the dict (`benchmarks/bench_text_file.py --only messages`). The rebuild is
    what cost the 24x, so the index is a value it is worth holding.
    """

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
        """A key column as tag numbers, null where no reading finds one.

        Three readings, in one pass each. The **decoration comes off first**:
        `NoPartyIDs[0].PartyID`, `PartyID[1]` and `Instrument.Symbol` all say
        *where* a field sits and the trailing segment says which field it is.
        What is left is either digits -- already a tag -- or a name, probed
        against the value set.

        Null, never a guess, for anything else. A key that resolves to nothing
        is not an error here: it is a key that belongs in `keyval`.
        """
        compute = pyarrow.compute
        reduced = compute.fill_null(
            compute.struct_field(compute.extract_regex(keys, _MEMBER_NAME_VECTOR), "name"), ""
        )
        numeric = compute.fill_null(compute.match_substring_regex(reduced, _IS_TAG), False)
        # Cast the whole column rather than a filtered subset: a non-numeric
        # key is replaced by a digit that casts, and the `if_else` after throws
        # it away. Filter-and-scatter costs two more kernels than the waste.
        as_tag = compute.if_else(numeric, reduced, pyarrow.scalar("0")).cast(TAG)
        by_name = compute.take(
            self.tags, compute.index_in(compute.utf8_lower(reduced), value_set=self.names)
        )
        return compute.if_else(numeric, as_tag, by_name)


@dataclasses.dataclass(eq=False)
class FixCodec(Convertible):
    """A log line read as FIX: which category it is, its pairs, and its tags.

    The three verbs a source needs to turn a message column into the columns a
    row carries -- `categorise`, `into_pairs`, `into_fix_pairs` -- and nothing
    else. That is deliberate and it is the seam: a second codec over another
    protocol implements the same three and `TextFile` never learns which one it
    is holding (`docs/logs.md`).

    Everything here is per **batch**: one mask per rule over the whole message
    column, one parse per category slice, one name index per FIX version. There
    is no per-row work in any of it, which is what a capture of millions of
    lines needs.
    """

    #: Which category each line is. `Rules.DEFAULT`'s three built-ins unless a
    #: document says otherwise.
    rules: Rules = dataclasses.field(default_factory=Rules)

    #: The dictionary names resolve through, **offline**: the default is the
    #: user's own cache (`~/.config/fix`), read and never scraped, because a
    #: parse that met its first bridge line and answered it by fetching seven
    #: thousand pages mid-batch would be a worse surprise than an unresolved
    #: name. Point it at `data/fix.zip` for the dictionary this repository
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

    def categorise(self, messages: Any, drivers: Any = None) -> tuple[Any, Any]:
        """One `(category_id, category_name)` pair per row, in kernels."""
        return self.rules.into_arrow_category_arrays(messages, drivers)

    def into_pairs(self, messages: Any, rule: Rule) -> Any:
        """One `map<string, string>` per row: the message as the line spells it.

        A rule whose codec is `none` parses nothing and says so with a column
        of nulls -- **not** empty maps. "Parsed, and there was nothing in it"
        and "this line is not a message" are different facts, and a store that
        spelled them the same way could not tell a bridge that sent an empty
        payload from a stack trace.
        """
        if rule.named is None:
            return pyarrow.nulls(len(messages), KEYVAL)
        return self.drop_null_values(
            parse_arrow_array(
                messages,
                rule.separator,
                named=rule.named,
                entry_separator=rule.entry_separator,
            )
        )

    def drop_null_values(self, pairs: Any) -> Any:
        """`pairs` without the fields whose value is one of `null_values`.

        Dropped rather than kept as text and dropped rather than kept as an
        empty string, because those are two different lies: an absent field is
        absent, and `58=` on the wire is a malformed message rather than an
        empty Text -- the same reading `FixMessage.from_pairs` makes of a
        `None` value.

        Done here, above the parser and below the split, so a spelling that
        means "nothing" never reaches `fix_tags` *or* `keyval`. A row whose
        every field was absent comes back as an empty map, not a null one: the
        line was a message, and it said nothing.

        One `is_in` over the whole flattened value child and one `all` to find
        out whether anything has to move, so a capture that carries no absent
        field pays two kernels a batch and rebuilds nothing.
        """
        if not self.null_values:
            return pairs
        if isinstance(pairs, pyarrow.ChunkedArray):
            return pyarrow.chunked_array(
                [self.drop_null_values(chunk) for chunk in pairs.chunks], type=pairs.type
            )
        compute = pyarrow.compute
        lengths, keys, items = _entries_of(pairs)
        absent = compute.is_in(
            compute.utf8_lower(compute.utf8_trim_whitespace(items)),
            value_set=pyarrow.array(sorted(self.null_values), pyarrow.string()),
        )
        keep = compute.invert(compute.fill_null(absent, False))
        if compute.all(keep, min_count=0).as_py():
            return pairs
        return _mapped(
            pairs, lengths, keep, compute.filter(keys, keep), compute.filter(items, keep)
        )

    def into_fix_pairs(self, pairs: Any, version: str | None = None) -> tuple[Any, Any]:
        """`pairs` split into the keys FIX names and the keys it does not.

        Two maps out of one, **in wire order**, which is the whole of how a
        repeating group survives the transcription. `NoPartyIDs=2` then its
        entries flattened -- `453, 448, 447, 452, 448, 447, 452` -- is exactly
        what the same message looks like on the wire, and it is exactly what a
        reader that knows the group can walk. So the `[i]` index is **dropped**
        once the order carries it: the index was a rendering convenience, the
        order is the standard.

        A key the dictionary answers for becomes `(tag, value)`; every other
        key stays `(key, value)`, spelled as the log spelled it. Nothing is
        dropped and nothing is guessed -- no fuzzy match, no `search()`
        fallback in the hot path -- because a venue's own field is data and a
        near-miss is a wrong answer that looks like a right one.

        A null row stays null in both halves. A row whose keys all resolve
        comes back with an *empty* map on the `keyval` side, and vice versa:
        the row parsed, and one half of it was empty.
        """
        index = self.index_of(version)
        if isinstance(pairs, pyarrow.ChunkedArray):
            halves = [self.into_fix_pairs(chunk, version) for chunk in pairs.chunks]
            return (
                pyarrow.chunked_array([one for one, _ in halves], type=FIX_TAGS),
                pyarrow.chunked_array([other for _, other in halves], type=KEYVAL),
            )
        compute = pyarrow.compute
        lengths, keys, items = _entries_of(pairs)
        tags = index.resolve(keys)
        known = compute.is_valid(tags)
        unknown = compute.invert(known)
        resolved = _mapped(
            pairs, lengths, known, compute.filter(tags, known), compute.filter(items, known)
        )
        rest = _mapped(
            pairs, lengths, unknown, compute.filter(keys, unknown), compute.filter(items, unknown)
        )
        return resolved, rest

    # -- versions -----------------------------------------------------------

    def version_of(self, message: str | None, rule: Rule | None = None) -> tuple[str | None, str]:
        """Which FIX version a message is read under, and where that came from.

        Three answers in order of authority: **tag 8**, which is the message
        saying so itself; the **rule**, which is the desk saying what its
        bridge speaks; and the **configured default**, which is this codec
        saying what to assume. The source comes back with the version because
        `4.4` read off a BeginString and `4.4` because nobody said otherwise
        are the same string and not the same fact -- one is evidence and the
        other is a setting.

        A BeginString the dictionary has no version for -- a truncated `FIX4`,
        a vendor's own spelling -- falls through rather than being coerced into
        the nearest one.
        """
        if message:
            begin = _BEGIN.search(message)
            if begin is not None:
                named = self.version_named(begin.group(0))
                if named is not None:
                    return named, BEGIN_STRING_SOURCE
        if rule is not None and rule.fix_version:
            return rule.fix_version, RULE_SOURCE
        if self.fix_version:
            return self.fix_version, DEFAULT_SOURCE
        return None, NO_SOURCE

    def version_named(self, begin_string: str) -> str | None:
        """`8=FIX.4.2` as the version the dictionary spells `4.2`; None if unknown.

        Matched on the digits and letters alone, because the two spellings
        agree on nothing else: `FIX.4.2` is `4.2`, `FIXT.1.1` is `FIXT1.1` and
        `FIX.5.0SP2` is `5.0.SP2`.
        """
        return self._spellings.get(_version_key(begin_string))

    def index_of(self, version: str | None = None) -> TagIndex:
        """The name index for one version, built once and held.

        Built from `FixRegistry.tags`, which walks whole versions -- so it is
        built per *batch* at the most, never per row, and in practice once per
        version for the life of the codec.

        A version the registry cannot answer for -- offline with a cold cache,
        a spelling it has never seen -- is an **empty index**, not an
        exception. A parsed line still has to land: its keys go to `keyval`
        and the capture is stored. A pipeline that died on a cold cache would
        lose the log rather than the tags.
        """
        wanted = version if version is not None else self.fix_version
        if wanted not in self._indexes:
            self._indexes[wanted] = TagIndex.from_tags(self._tags(wanted))
        return self._indexes[wanted]

    def tag_field(self, tag: int, version: str | None = None) -> Field | None:
        """The dictionary's own declaration of one tag, or None when it has none.

        The seam for **typed** values, and the reason values stay text in
        `fix_tags`: the Arrow type on this field is `rekep.fix.fields`'
        `FIX_SCALARS` projection of the FIX datatype, and its `fix["values"]`
        carries the enumeration. So decoding a column of one tag is a cast
        against the field that knows what the tag is -- which is where a cast
        belongs -- rather than a second type table here that would have to be
        kept in step with it.
        """
        try:
            return self.registry.field(tag, version if version is not None else self.fix_version)
        except (KeyError, OSError, ValueError):
            return None

    # -- held state ---------------------------------------------------------

    @cached_property
    def _indexes(self) -> dict[str | None, TagIndex]:
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


def _entries_of(pairs: Any) -> tuple[Any, Any, Any]:
    """One map column as `(row lengths, keys, values)`.

    Through a list-of-struct **view** of the same buffers, for the reason
    `tag_arrow_array` takes the same route: the list kernels honour a sliced
    array's own offset and validity where the raw `.keys`/`.offsets` accessors
    do not, and neither `list_flatten` nor `list_value_length` has a map
    kernel. The cast moves no data.
    """
    compute = pyarrow.compute
    listed = pairs.cast(
        pyarrow.list_(
            pyarrow.struct([("key", pairs.type.key_type), ("value", pairs.type.item_type)])
        )
    )
    lengths = compute.fill_null(compute.list_value_length(listed), 0).cast(pyarrow.int32())
    entries = compute.list_flatten(listed)
    return lengths, compute.struct_field(entries, 0), compute.struct_field(entries, 1)


def _mapped(source: Any, lengths: Any, mask: Any, keys: Any, items: Any) -> pyarrow.MapArray:
    """One half of a split, as a map with the source's own rows and nulls.

    The offsets are rebuilt from a cumulative sum of the mask -- the same
    construction `parse_arrow_array` builds its rows from -- so an entry that
    went to the other half costs nothing here and the ones that stayed keep
    their order. A null row takes to null, which is Arrow's own `from_arrays`
    convention and the one way to keep "not a message" apart from "a message
    with nothing in it".
    """
    compute = pyarrow.compute
    counted = compute.cumulative_sum(mask.cast(pyarrow.int32()))
    bounds = pyarrow.concat_arrays([pyarrow.array([0], pyarrow.int32()), counted])
    rows = pyarrow.concat_arrays(
        [pyarrow.array([0], pyarrow.int32()), compute.cumulative_sum(lengths)]
    )
    offsets = bounds.take(rows)
    if source.null_count:
        head = compute.if_else(
            compute.is_null(source),
            pyarrow.scalar(None, pyarrow.int32()),
            offsets.slice(0, len(source)),
        )
        offsets = pyarrow.concat_arrays([head, offsets.slice(len(source))])
    return pyarrow.MapArray.from_arrays(offsets, keys, items)


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
