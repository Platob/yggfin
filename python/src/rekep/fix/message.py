"""One FIX message out of a log line, and whole columns of them at once."""

from __future__ import annotations

import dataclasses
import functools
import re
from collections.abc import Collection, Iterator, Mapping
from typing import Any, ClassVar

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible

#: The delimiter the standard writes between fields: ASCII 0x01, Start of
#: Heading. Unprintable, which is why logs substitute something visible.
SOH = "\x01"

#: Stand-ins seen in real logs, in the order they are tried when a message
#: does not say which it uses: the standard SOH first, then the printable
#: substitutions tools make -- a pipe, the caret spelling of Ctrl-A, a bare
#: caret, a semicolon. Order matters: `^A` must be tried before `^`, or a
#: caret-A log reads every tag with an `A` glued to the front.
SEPARATORS = (SOH, "|", "^A", "^", ";")

#: Where a message starts inside a log line: `8=FIX...` at the start or after
#: anything that is not a digit, so the `8=` of tag 18 or 58 never matches.
#: What follows the BeginString value is, by construction, the separator.
#:
#: Every scalar pattern here is compiled `re.ASCII`, because the vectorised
#: twins run under RE2, whose `\d`/`\s`/`\w` are ASCII-only -- and the two
#: parsers are contracted to agree. Without the flag a tag written in
#: Arabic-Indic digits was a pair to the scalar parser and noise to the
#: vectorised one; ASCII is also what the FIX standard means by a digit.
_BEGIN = re.compile(r"(?:^|(?<=[^\d]))8=FIXT?[^\x01|;^\s]*", re.ASCII)

#: A rendered field or group name, as the tools around a bridge print one --
#: letters first, then the word characters, dots (a component path like
#: `Instrument.Symbol`) and dashes real feeds put in names. Deliberately not
#: containing `[`, which is what lets one greedy regex split `NoPartyIDs[0]`
#: into the name and the entry index.
_NAME = r"[A-Za-z][A-Za-z0-9_.\-]*"

#: Whitespace, spelled out. Python's ASCII `\s` holds `\x0b` and RE2's does
#: not, so a `\s` in a pattern that exists in both engines is a divergence
#: waiting for a vertical tab; one explicit class reads the same everywhere.
_WS = r"[ \t\r\n\f\x0b]"

#: One token of a message, in every spelling the logs use. Four shapes come
#: out of the same regex::
#:
#:     54=1                       tag = value
#:     Side=1                     name = value
#:     NoPartyIDs[0]=PartyID=x    group[entry] = member = value
#:     NoPartyIDs[0].PartyID=x    the canonical spelling this parser writes
#:
#: `key` is greedy, so a dotted *name* is eaten whole and `member` engages in
#: two places only: after an `[index]`, and after a bare digit key (`54.5=x`,
#: whose dot `\d+` cannot eat -- `_parse_token` puts it back on the key).
#: Which is also what keeps `58=a=b` reading as tag 58 with value `a=b`: the
#: inner `member=` of the third shape is cut out of `rest` by `_MEMBER` only
#: when an index said the token is a group entry; without one, an `=` in the
#: rest is part of the value.
_TOKEN = re.compile(
    rf"^{_WS}*(?P<key>\d+|{_NAME})"
    rf"(?:\[(?P<index>\d+)\])?"
    rf"(?:\.(?P<member>[A-Za-z0-9_.\-]+))?"
    rf"{_WS}*=(?P<rest>.*)$",
    re.DOTALL | re.ASCII,
)

#: The inner `member=value` of a group token (`NoPartyIDs[0]=PartyID=x`).
_MEMBER = re.compile(rf"^{_WS}*(?P<member>\d+|{_NAME}){_WS}*=(?P<value>.*)$", re.DOTALL | re.ASCII)

#: The vectorised token classifiers: what counts as `key=value` at all. Tag
#: mode is digits only -- the standard's own rule (Vol 1: "a tag number is a
#: positive integer"), and what lets the parser shrug off the log's own
#: `key=value` noise around a wire message. Named mode admits the rendered
#: spellings above, because there the line *is* the pairs.
_PAIR_TOKEN = rf"^{_WS}*\d+{_WS}*="
_PAIR_TOKEN_NAMED = rf"^{_WS}*(?:\d+|{_NAME})(?:\[\d+\])?(?:\.[A-Za-z0-9_.\-]+)?{_WS}*="

#: `_TOKEN` and `_MEMBER` for RE2, which has no DOTALL flag argument.
_TOKEN_VECTOR = (
    rf"(?s)^{_WS}*(?P<key>\d+|{_NAME})"
    rf"(?:\[(?P<index>\d+)\])?"
    rf"(?:\.(?P<member>[A-Za-z0-9_.\-]+))?"
    rf"{_WS}*=(?P<rest>.*)$"
)
_MEMBER_VECTOR = rf"(?s)^{_WS}*(?P<member>\d+|{_NAME}){_WS}*=(?P<value>.*)$"

#: The member half of a stored key, for resolving it back to a tag number:
#: the trailing name segment of `PartyID[1]`, `NoPartyIDs[0].PartyID` or a
#: dotted component path, with the index stripped.
_MEMBER_NAME = re.compile(r"([A-Za-z0-9_\-]+)(?:\[\d+\])?\s*$")

#: BodyLength and CheckSum, the two fields whose *position* the standard
#: fixes: 8, 9 lead and 10 ends the message.
CHECKSUM = "10"


def detect_separator(text: str) -> str:
    """The character standing in for SOH in `text`.

    The one honest place to read it is right after the BeginString value:
    whatever follows `8=FIX.4.2` *is* the separator, whether or not it is on
    the candidate list. Without a BeginString -- a fragment, a heartbeat cut
    from its header -- the first candidate present in the text wins, and a
    text with none reads as SOH-separated, which parses it as one field.
    """
    match = _BEGIN.search(text)
    if match is not None and match.end() < len(text):
        following = text[match.end()]
        if following == "^" and text[match.end() + 1 : match.end() + 2] == "A":
            return "^A"
        if not following.isspace():
            return following
    for candidate in SEPARATORS:
        if candidate in text:
            return candidate
    return SOH


@dataclasses.dataclass
class FixMessage(Convertible):
    """One FIX message: its fields in wire order, tags and values as text.

    Order and repetition are the message -- a repeating group *is* tags
    repeating -- so the fields are a sequence of pairs, never a mapping, and
    `get`/`values` read over it. Values stay text: what a value *is* depends
    on a dictionary (`FixRegistry`) and on the message, and decoding is a
    cast against the field that knows (`rekep.fix.fields`).
    """

    REDIRECTS: ClassVar[dict[Any, str]] = {**Convertible.REDIRECTS, str: "text"}

    pairs: list[tuple[str, str]] = dataclasses.field(default_factory=list)

    # -- building -----------------------------------------------------------

    @classmethod
    def from_text(
        cls, text: str | bytes, separator: str | None = None, *, named: bool | None = None
    ) -> FixMessage:
        """Parse one log line, however it spells its separator and its keys.

        Robust the way a log demands: the message may sit inside a line with
        its own prefix and suffix, so parsing starts at `8=FIX` when one is
        there, every token that is not `key=value` is skipped rather than
        fatal, whitespace around tokens (a ` | `-joined log) is trimmed, and
        the CheckSum <10> ends the message so trailing log noise cannot glue
        itself onto the last value.

        `named` decides what a *key* may be. False is the wire's rule -- a
        numeric tag, everything else is log noise. True admits the rendered
        spellings too: `Side=1`, and a repeating group printed entry by entry
        as `NoPartyIDs[0]=PartyID=x` or `PartyID[1]=y`, which land under the
        canonical keys `NoPartyIDs[0].PartyID` and `PartyID[1]` so the index
        survives into the pairs. None reads the line itself: a BeginString
        means a wire message buried in noise (tags only), no BeginString
        means the line *is* the rendered pairs (names admitted).
        """
        if isinstance(text, bytes):
            text = text.decode("utf-8", "replace")
        begin = _BEGIN.search(text)
        if begin is not None:
            text = text[begin.start() :]
        if named is None:
            named = begin is None
        separator = separator or detect_separator(text)
        pairs: list[tuple[str, str]] = []
        for token in text.split(separator):
            parsed = _parse_token(token, named)
            if parsed is None:
                continue
            pairs.append(parsed)
            if parsed[0] == CHECKSUM:
                break
        return cls(pairs=pairs)

    # -- reading ------------------------------------------------------------

    def get(self, tag: int | str, default: str | None = None) -> str | None:
        """The first value of `tag`, or `default`.

        Exact key first -- the fast path every tag lookup takes -- and then
        the rendered spellings of the same field: `Side` also answers for
        `side`, `Side[0]` and `NoPartyIDs[0].Side`, because the index and the
        group are *where* the field sits, not what it is.
        """
        wanted = str(tag)
        for name, value in self.pairs:
            if name == wanted:
                return value
        rendered = _member_pattern(wanted)
        for name, value in self.pairs:
            if rendered.match(name):
                return value
        return default

    def values(self, tag: int | str) -> list[str]:
        """Every value of `tag`, in wire order -- what a repeating tag is.

        Falls back to the rendered spellings the way `get` does, so
        `values("PartyID")` collects one value per printed group entry.
        """
        wanted = str(tag)
        found = [value for name, value in self.pairs if name == wanted]
        if found:
            return found
        rendered = _member_pattern(wanted)
        return [value for name, value in self.pairs if rendered.match(name)]

    @property
    def begin_string(self) -> str | None:
        """The protocol the message claims: BeginString <8>."""
        return self.get(8)

    @property
    def msg_type(self) -> str | None:
        """MsgType <35>, when the message carries one."""
        return self.get(35)

    def __len__(self) -> int:
        return len(self.pairs)

    def __iter__(self) -> Iterator[tuple[str, str]]:
        return iter(self.pairs)

    # -- repeating groups ---------------------------------------------------

    def group(
        self, count_tag: int | str, members: Collection[int | str] | None = None
    ) -> list[list[tuple[str, str]]]:
        """The entries of the repeating group `count_tag` counts.

        The standard's own rules (FIX Vol 1, repeating groups): the NumInGroup
        field precedes its entries; every entry starts with the same
        *delimiter* tag -- the first tag after the count, by definition; tags
        never repeat within one entry; and the count says how many entries
        there are, so a later reappearance of the delimiter elsewhere in the
        message is not one more.

        Where the *last* entry ends is the one thing the wire does not say
        without a dictionary. `members` -- the group's tags, from `FixRegistry`
        or a spec -- makes the boundary exact; without it, an entry ends where
        a tag repeats inside it, which the no-repetition rule guarantees is
        past the end, and anything after the last entry that never repeated
        stays in it. Compliant senders order group fields consistently, which
        is what makes the fallback honest in practice.
        """
        wanted = str(count_tag)
        allowed = {str(member) for member in members} if members is not None else None
        pairs = self.pairs
        start = next((i for i, (tag, _) in enumerate(pairs) if tag == wanted), None)
        if start is None:
            return []
        try:
            count = int(pairs[start][1])
        except ValueError:
            count = 0
        after = pairs[start + 1 :]
        if count <= 0 or not after:
            return []
        delimiter = after[0][0]
        entries: list[list[tuple[str, str]]] = []
        seen: set[str] = set()
        for tag, value in after:
            if tag == delimiter:
                if len(entries) == count:
                    break
                entries.append([])
                seen = set()
            elif not entries or tag in seen or (allowed is not None and tag not in allowed):
                break
            seen.add(tag)
            entries[-1].append((tag, value))
        return entries

    def indexed_group(self, name: int | str) -> list[list[tuple[str, str]]]:
        """The entries of a group a log rendered with indexes, in index order.

        The other spelling of a repeating group: not a count tag followed by
        wire-order entries, but each field labelled with the entry it belongs
        to -- `NoPartyIDs[0]=PartyID=x`, `NoPartyIDs[1]=PartyID=y` -- which
        `from_text` stores under `NoPartyIDs[0].PartyID`. Here those keys are
        folded back into entries, ordered by index however the log
        interleaved them, sparse indexes tolerated. A bare `Name[i]=value`
        token is one `(Name, value)` pair of entry `i`. Case-insensitive,
        like the rest of the rendered-name handling.
        """
        wanted = str(name)
        pattern = re.compile(rf"^{re.escape(wanted)}\[(\d+)\](?:\.(.+))?$", re.IGNORECASE)
        entries: dict[int, list[tuple[str, str]]] = {}
        for key, value in self.pairs:
            match = pattern.match(key)
            if match is not None:
                entries.setdefault(int(match[1]), []).append((match[2] or wanted, value))
        return [entries[index] for index in sorted(entries)]

    # -- converting ---------------------------------------------------------

    def into_text(self, separator: str = SOH) -> str:
        """The message back as one line, `tag=value` joined by `separator`.

        Indexed keys render in their canonical spelling
        (`NoPartyIDs[0].PartyID=x`), which `from_text` reads back to the same
        pairs -- the round trip is exact even where the source spelled the
        entry `NoPartyIDs[0]=PartyID=x`.
        """
        return separator.join(f"{tag}={value}" for tag, value in self.pairs)


# -- whole columns -----------------------------------------------------------


def parse_arrow_array(
    column: Any, separator: str | None = None, *, named: bool | None = None
) -> Any:
    """A column of FIX log lines as one `map<string, string>` per row.

    The vectorised `FixMessage.from_text`: one `split_pattern` cuts every
    line into tokens, one regex match classifies every token as `key=value`
    or noise, one more pass cuts key from value, and the map offsets are
    rebuilt from a cumulative sum of the matches -- kernels throughout, no
    Python per row, which is what a column of millions of lines needs.
    (The tag/value cut is a `split_pattern` and two `list_element` calls on
    purpose: raced against one `extract_regex` in `benchmarks/bench_fix.py`,
    the split ties the regex that skips trimming -- which is only equal on
    unpadded tokens -- and beats the one that trims by ~3x, measured twice.)

    A **map**, not a struct, because tags repeat -- a repeating group is tags
    repeating, and an Arrow map is the one nested type that keeps duplicate
    keys in order. Values stay text for the same reason they do on
    `FixMessage`. A null line stays null; a line with no `key=value` in it
    becomes an empty map.

    `named` is `from_text`'s: False takes numeric tags only, True admits
    rendered names and indexed group entries (`NoPartyIDs[0]=PartyID=x`
    lands under `NoPartyIDs[0].PartyID`, exactly as the scalar parser stores
    it). None reads the *column*: the first non-empty line with a
    BeginString means wire messages (tags only), none means rendered pairs.
    `separator` is sampled the same way when not given -- one style per
    call: a column mixing styles should be split first, or parsed row by row
    with `FixMessage.from_text`.
    """
    if separator is None or named is None:
        # Sampled from the column as handed over -- once, even for a chunked
        # column, so where a chunk boundary falls can never change what a row
        # parses to. Skipped entirely when the caller said both.
        sampled_separator, sampled_named = _column_style(column)
        separator = sampled_separator if separator is None else separator
        named = sampled_named if named is None else named
    if isinstance(column, pyarrow.ChunkedArray):
        parsed = [parse_arrow_array(chunk, separator, named=named) for chunk in column.chunks]
        # The explicit type is for the zero-chunk column, which is legal and
        # has nothing to infer from.
        return pyarrow.chunked_array(parsed, type=pyarrow.map_(pyarrow.string(), pyarrow.string()))
    compute = pyarrow.compute
    values = column.cast(pyarrow.string(), safe=False)
    # The scalar rule, in one kernel: a line with a message inside it starts
    # at its `8=FIX`, so the log's own prefix never glues onto the first tag.
    # RE2 has no lookbehind, so the non-digit guard rides outside the capture
    # -- and `(?s)`, or a message holding a newline would end at it here
    # where the scalar slice keeps it.
    begun = compute.struct_field(
        compute.extract_regex(values, r"(?s)(?:^|[^0-9])(?P<msg>8=FIXT?.*)"), "msg"
    )
    values = compute.if_else(compute.is_null(begun), values, begun)
    tokens = compute.split_pattern(values, separator)
    # `.values`, not `.flatten()`: the boundaries below index into the child
    # array as the offsets wrote it, and `flatten` re-slices around null rows.
    # A kernel output owns its buffers from zero, so the two only agree here.
    flat = tokens.values
    matched = compute.match_substring_regex(flat, _PAIR_TOKEN_NAMED if named else _PAIR_TOKEN)
    matched = compute.fill_null(matched, False)
    kept = compute.filter(flat, matched)
    if named:
        tags, entries = _named_pairs(kept)
    else:
        halves = compute.split_pattern(kept, "=", max_splits=1)
        tags = compute.utf8_trim_whitespace(compute.list_element(halves, 0))
        entries = compute.utf8_trim_whitespace(compute.list_element(halves, 1))
    counted = compute.cumulative_sum(matched.cast(pyarrow.int32()))
    bounds = pyarrow.concat_arrays([pyarrow.array([0], pyarrow.int32()), counted])
    offsets = bounds.take(_boundaries(tokens))
    parents = compute.filter(compute.list_parent_indices(tokens), matched)
    tags, entries, offsets = _until_checksum(tags, entries, offsets, parents)
    if column.null_count:
        # A null in the offsets marks its row null, which is Arrow's own
        # `from_arrays` convention -- the one way to keep "no line" distinct
        # from "a line with nothing in it" without a second pass.
        head = compute.if_else(
            compute.is_null(values),
            pyarrow.scalar(None, pyarrow.int32()),
            offsets.slice(0, len(values)),
        )
        offsets = pyarrow.concat_arrays([head, offsets.slice(len(values))])
    return pyarrow.MapArray.from_arrays(offsets, tags, entries)


def _named_pairs(kept: Any) -> tuple[Any, Any]:
    """Canonical `(keys, values)` for named-mode tokens, in kernels.

    The vectorised `_parse_token`: one `extract_regex` reads key, index,
    member and rest out of every token at once; a second one cuts the inner
    `member=` out of `rest` -- applied through masks, only where an index
    said the token is a group entry and no canonical `.member` already named
    it. The canonical key is then one element-wise join, so
    `NoPartyIDs[0]=PartyID=x` and `NoPartyIDs[0].PartyID=x` come out
    identical, exactly as the scalar parser stores them.
    """
    compute = pyarrow.compute
    empty = pyarrow.scalar("")
    token = compute.extract_regex(kept, _TOKEN_VECTOR)
    # An optional group that did not take part comes back as the *empty
    # string*, not null -- RE2's convention through `extract_regex` -- and no
    # real index or member can be empty, so emptiness is the test throughout.
    key = compute.struct_field(token, "key")
    index = compute.fill_null(compute.struct_field(token, "index"), "")
    member = compute.fill_null(compute.struct_field(token, "member"), "")
    value = compute.fill_null(compute.struct_field(token, "rest"), "")
    indexed = compute.not_equal(index, empty)
    # Only an indexed token with no canonical `.member` can hide an inner
    # `member=`, so the second regex runs over that subset alone and its
    # results are scattered back by a take -- a null slot takes to null,
    # which fills to "not grouped". What the subset buys is the *skip*: a
    # rendered column with no group entries never pays for the pass at all.
    # The two rendered cases in `benchmarks/bench_fix.py` bracket it, ~260k
    # rows/s without group entries against ~140k with, and the token regex
    # above -- not this pass -- is where the grouped column's time goes.
    needs_inner = compute.and_(indexed, compute.equal(member, empty))
    if compute.any(needs_inner, min_count=0).as_py():
        inner = compute.extract_regex(compute.filter(value, needs_inner), _MEMBER_VECTOR)
        slots = compute.if_else(
            needs_inner,
            compute.subtract(
                compute.cumulative_sum(needs_inner.cast(pyarrow.int32())),
                pyarrow.scalar(1, pyarrow.int32()),
            ),
            pyarrow.scalar(None, pyarrow.int32()),
        )
        inner_member = compute.fill_null(
            compute.take(compute.struct_field(inner, "member"), slots), ""
        )
        inner_value = compute.take(compute.struct_field(inner, "value"), slots)
        grouped = compute.not_equal(inner_member, empty)
        member = compute.if_else(grouped, inner_member, member)
        value = compute.if_else(grouped, compute.fill_null(inner_value, ""), value)
    bracket = compute.if_else(indexed, compute.binary_join_element_wise("[", index, "]", ""), empty)
    dotted = compute.if_else(
        compute.not_equal(member, empty),
        compute.binary_join_element_wise(".", member, ""),
        empty,
    )
    tags = compute.binary_join_element_wise(key, bracket, dotted, "")
    return tags, compute.utf8_trim_whitespace(value)


def _until_checksum(tags: Any, entries: Any, offsets: Any, parents: Any) -> tuple[Any, Any, Any]:
    """Each row cut after its first CheckSum <10>, the way the scalar parser cuts.

    The scalar loop `break`s once it stores tag 10, so anything pair-shaped
    after the checksum -- a log suffix that happens to spell `key=value` --
    never lands. The vectorised cut is three integer kernels: a running count
    of checksums over the kept tokens, that count at each token's own row
    start (`parents` says which row a token is in), and a filter keeping the
    tokens whose row has no checksum before them -- the checksum itself
    included, everything after it not. The row offsets are then renumbered by
    the same cumulative-sum trick the noise filter uses. A column with no
    checksum at all -- every rendered column -- pays one `equal` and one
    `any`.
    """
    compute = pyarrow.compute
    checks = compute.equal(tags, CHECKSUM)
    if not compute.any(checks, min_count=0).as_py():
        return tags, entries, offsets
    counted = compute.cumulative_sum(checks.cast(pyarrow.int32()))
    prefix = pyarrow.concat_arrays([pyarrow.array([0], pyarrow.int32()), counted])
    before_token = prefix.slice(0, len(tags))
    before_row = prefix.take(offsets.slice(0, len(offsets) - 1)).take(parents)
    keep = compute.equal(compute.subtract(before_token, before_row), 0)
    renumbered = pyarrow.concat_arrays(
        [
            pyarrow.array([0], pyarrow.int32()),
            compute.cumulative_sum(keep.cast(pyarrow.int32())),
        ]
    )
    return (
        compute.filter(tags, keep),
        compute.filter(entries, keep),
        renumbered.take(offsets),
    )


def _column_style(column: Any) -> tuple[str, bool]:
    """`(separator, named)` off the first non-empty line; `(SOH, False)` blind.

    One sample decides for the column -- the same reading `from_text` makes
    per line: a BeginString means wire tags buried in log noise, none means
    the line is rendered `name=value` pairs. Sampled from the column *before*
    any cast, and decoded the way `from_text` decodes, so a binary column
    holding a byte no UTF-8 admits is sampled rather than crashed on.
    """
    for value in column:
        if not value.is_valid:
            continue
        try:
            text = value.as_py()
        except UnicodeDecodeError:
            text = bytes(value.as_buffer())
        if isinstance(text, bytes):
            text = text.decode("utf-8", "replace")
        if text:
            return detect_separator(text), _BEGIN.search(text) is None
    return SOH, False


def _boundaries(tokens: Any) -> Any:
    """Each row's start and end in the flattened token stream: the offsets.

    `ListArray.offsets` already is that -- `len(rows) + 1` positions into the
    values -- except a null row's offsets may be garbage, so they are healed
    to the previous boundary first.
    """
    offsets = tokens.offsets
    if tokens.null_count:
        offsets = pyarrow.compute.fill_null_backward(offsets)
        offsets = pyarrow.compute.fill_null(offsets, len(tokens.values))
    return offsets


# -- tag numbers -------------------------------------------------------------


def tag_arrow_array(
    maps: Any,
    key_type: pyarrow.DataType | None = None,
    names: Mapping[str, int | str] | None = None,
    *,
    drop_unknown: bool = False,
) -> Any:
    """The same maps with integer tags for keys: `map<int32, string>`.

    What a join, a filter or a dictionary lookup wants after
    `parse_arrow_array`: keys as the numbers FIX defines instead of the text
    the log printed. The entry layout is reused as it stands -- the values
    move by reference and only the key child is rebuilt -- so the common case
    (every key already a tag number, which is what tag-mode parsing
    produces) is a single cast kernel: ~140M keys/s, measured twice
    (`benchmarks/bench_fix.py`).

    Rendered keys resolve through `names` -- `{name: tag}`, case-insensitive,
    which is exactly what `FixRegistry.tags()` builds -- after the member is
    cut out of the indexed spellings by regex: `NoPartyIDs[0].PartyID`,
    `PartyID[1]` and `Instrument.Symbol` all resolve by their trailing name.
    The keys are dictionary-encoded first, so each distinct spelling is
    resolved once however many million entries carry it.

    A key that resolves nowhere is refused **by name**: a map key cannot be
    null, and dropping data silently is worse than stopping. Pass
    `drop_unknown=True` to drop those entries instead -- the layout is then
    rebuilt around them, and a rendered log's `took=5ms` noise falls out.
    """
    key_type = pyarrow.int32() if key_type is None else key_type
    if isinstance(maps, pyarrow.ChunkedArray):
        tagged = [
            tag_arrow_array(chunk, key_type, names, drop_unknown=drop_unknown)
            for chunk in maps.chunks
        ]
        # The explicit type is for the zero-chunk column, which is legal and
        # has nothing to infer from.
        return pyarrow.chunked_array(tagged, type=pyarrow.map_(key_type, maps.type.item_type))
    compute = pyarrow.compute
    # Through a list-of-struct *view* of the same buffers (the map->list cast
    # moves no data), because the list kernels honour a sliced array's own
    # offset and validity where the raw `.keys`/`.offsets` accessors do not
    # -- and neither `list_flatten` nor `list_value_length` has a map kernel.
    listed = maps.cast(
        pyarrow.list_(pyarrow.struct([("key", maps.type.key_type), ("value", maps.type.item_type)]))
    )
    entries = compute.list_flatten(listed)
    keys = compute.struct_field(entries, 0)
    items = compute.struct_field(entries, 1)
    resolved = _tag_numbers(keys, names, key_type)
    lengths = compute.fill_null(compute.list_value_length(listed), 0).cast(pyarrow.int32())
    offsets = pyarrow.concat_arrays(
        [pyarrow.array([0], pyarrow.int32()), compute.cumulative_sum(lengths)]
    )
    if maps.null_count:
        head = compute.if_else(
            compute.is_null(maps),
            pyarrow.scalar(None, pyarrow.int32()),
            offsets.slice(0, len(maps)),
        )
        offsets = pyarrow.concat_arrays([head, offsets.slice(len(maps))])
    if not resolved.null_count:
        return pyarrow.MapArray.from_arrays(offsets, resolved, items)
    if not drop_unknown:
        unknown = compute.unique(compute.filter(keys, compute.is_null(resolved)))
        shown = ", ".join(repr(key.as_py()) for key in unknown.slice(0, 5))
        raise KeyError(
            f"{len(unknown)} map keys name no FIX tag ({shown}); resolve them with "
            "names=FixRegistry().tags(), or pass drop_unknown=True to drop those entries"
        )
    known = compute.is_valid(resolved)
    counted = compute.cumulative_sum(known.cast(pyarrow.int32()))
    bounds = pyarrow.concat_arrays([pyarrow.array([0], pyarrow.int32()), counted])
    # `take` with a null index yields null, so a null row stays a null row.
    offsets = bounds.take(offsets)
    return pyarrow.MapArray.from_arrays(
        offsets, compute.filter(resolved, known), compute.filter(items, known)
    )


def _tag_numbers(keys: Any, names: Mapping[str, int | str] | None, key_type: Any) -> Any:
    """A key column as tag numbers, null where no reading finds one.

    The cast is *attempted* first because it is the whole cost of the common
    case; only a column that actually carries rendered names pays for the
    dictionary encoding, and then only once per distinct spelling.
    """
    try:
        return keys.cast(key_type)
    except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError):
        pass
    lookup = {str(name).strip().lower(): int(tag) for name, tag in (names or {}).items()}
    encoded = keys.dictionary_encode()
    resolved = pyarrow.array(
        [_tag_number(spelled, lookup, key_type) for spelled in encoded.dictionary.to_pylist()],
        key_type,
    )
    return pyarrow.compute.take(resolved, encoded.indices)


def _tag_number(spelled: str | None, lookup: Mapping[str, int], key_type: Any) -> int | None:
    """One stored key as a tag number: itself, its name, or its member's name.

    None -- "no tag", which the caller refuses or drops -- for anything else,
    *including* a number `key_type` cannot hold: an epoch-millis key is not a
    FIX tag, and letting it through would turn the whole transform into an
    Arrow overflow error after the decision to drop was already made.
    """
    if spelled is None:
        return None
    text = spelled.strip()
    found = None
    if text.isascii() and text.isdigit():
        found = int(text)
    else:
        found = lookup.get(text.lower())
        if found is None:
            member = _MEMBER_NAME.search(text)
            if member is not None:
                name = member[1]
                found = int(name) if name.isdigit() else lookup.get(name.lower())
    if found is None:
        return None
    try:
        pyarrow.scalar(found, key_type)
    except (pyarrow.ArrowInvalid, OverflowError):
        return None
    return found


# -- one token ---------------------------------------------------------------


def _parse_token(token: str, named: bool) -> tuple[str, str] | None:
    """One separator-delimited token as a `(key, value)` pair, or noise.

    The whole grammar is `_TOKEN`; what this adds is the two readings a
    regex alone cannot make. In tag mode anything that is not a bare
    `digits=value` is noise -- that is what sheds `latency=5ms` around a wire
    message. In named mode an indexed token is a group entry, so the inner
    `member=` is cut out of the rest (`_MEMBER`) and the key is rebuilt in
    its canonical spelling, index kept: `NoPartyIDs[0].PartyID`.
    """
    match = _TOKEN.match(token)
    if match is None:
        return None
    key, index, member, rest = match.group("key", "index", "member", "rest")
    if not named and (index is not None or member is not None or not key.isdigit()):
        return None
    if index is None:
        # Only a digit key can capture a member without an index (`54.5=x`;
        # a name eats its dots greedily): the dot was part of the key, so it
        # goes back on -- which is also what the vectorised join produces.
        plain = f"{key}.{member}" if member else key
        return plain, rest.strip()
    if member is None:
        inner = _MEMBER.match(rest)
        if inner is not None:
            member, rest = inner.group("member", "value")
    canonical = f"{key}[{index}].{member}" if member else f"{key}[{index}]"
    return canonical, rest.strip()


@functools.lru_cache(maxsize=1024)
def _member_pattern(wanted: str) -> re.Pattern[str]:
    """`wanted` as any rendered spelling of one field, case-insensitive.

    The fallback `get`/`values` read with: the plain name, the name with an
    entry index (`Side[0]`), or the member of any indexed group
    (`NoPartyIDs[0].Side`). Cached because a caller polling one field over a
    stream of messages would otherwise compile the same pattern per message.
    """
    name = re.escape(wanted)
    return re.compile(
        rf"^(?:{name}|{name}\[\d+\]|{_NAME}\[\d+\]\.{name})$",
        re.IGNORECASE,
    )
