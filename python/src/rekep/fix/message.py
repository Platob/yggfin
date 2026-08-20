"""One FIX message out of a log line, and whole columns of them at once."""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Collection, Iterator
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
_BEGIN = re.compile(r"(?:^|(?<=[^\d]))8=FIXT?[^\x01|;^\s]*")

#: One field: a numeric tag, `=`, a value. The tag is digits by the standard
#: (Vol 1: "a tag number is a positive integer"), which is also what lets the
#: parser shrug off the log's own `key=value` noise around a message.
_PAIR = re.compile(r"^\s*(\d+)\s*=(.*?)\s*$", re.DOTALL)

#: The vectorised spelling of `_PAIR`, over a whole column of tokens.
_PAIR_TOKEN = r"^\s*\d+\s*="

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
    def from_text(cls, text: str | bytes, separator: str | None = None) -> FixMessage:
        """Parse one log line, however it spells its separator.

        Robust the way a log demands: the message may sit inside a line with
        its own prefix and suffix, so parsing starts at `8=FIX` when one is
        there, every token that is not `tag=value` with a numeric tag is
        skipped rather than fatal, whitespace around tokens (a ` | `-joined
        log) is trimmed, and the CheckSum <10> ends the message so trailing
        log noise cannot glue itself onto the last value.
        """
        if isinstance(text, bytes):
            text = text.decode("utf-8", "replace")
        begin = _BEGIN.search(text)
        if begin is not None:
            text = text[begin.start() :]
        separator = separator or detect_separator(text)
        pairs: list[tuple[str, str]] = []
        for token in text.split(separator):
            match = _PAIR.match(token)
            if match is None:
                continue
            tag, value = match.groups()
            pairs.append((tag, value))
            if tag == CHECKSUM:
                break
        return cls(pairs=pairs)

    # -- reading ------------------------------------------------------------

    def get(self, tag: int | str, default: str | None = None) -> str | None:
        """The first value of `tag`, or `default`."""
        wanted = str(tag)
        for name, value in self.pairs:
            if name == wanted:
                return value
        return default

    def values(self, tag: int | str) -> list[str]:
        """Every value of `tag`, in wire order -- what a repeating tag is."""
        wanted = str(tag)
        return [value for name, value in self.pairs if name == wanted]

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

    # -- converting ---------------------------------------------------------

    def into_text(self, separator: str = SOH) -> str:
        """The message back as one line, `tag=value` joined by `separator`."""
        return separator.join(f"{tag}={value}" for tag, value in self.pairs)


# -- whole columns -----------------------------------------------------------


def parse_arrow_array(column: Any, separator: str | None = None) -> Any:
    """A column of FIX log lines as one `map<string, string>` per row.

    The vectorised `FixMessage.from_text`: one `split_pattern` cuts every
    line into tokens, one regex match classifies every token as `tag=value`
    or noise, one more split cuts tag from value, and the map offsets are
    rebuilt from a cumulative sum of the matches -- kernels throughout, no
    Python per row, which is what a column of millions of lines needs.

    A **map**, not a struct, because tags repeat -- a repeating group is tags
    repeating, and an Arrow map is the one nested type that keeps duplicate
    keys in order. Values stay text for the same reason they do on
    `FixMessage`. A null line stays null; a line with no `tag=value` in it
    becomes an empty map.

    `separator` is read from the first line that has one when not given --
    one message per call rule: a column mixing separators should be split by
    separator first, or parsed row by row with `FixMessage.from_text`.
    """
    if isinstance(column, pyarrow.ChunkedArray):
        return pyarrow.chunked_array(
            [parse_arrow_array(chunk, separator) for chunk in column.chunks]
        )
    compute = pyarrow.compute
    values = column.cast(pyarrow.string(), safe=False)
    if separator is None:
        separator = _column_separator(values)
    # The scalar rule, in one kernel: a line with a message inside it starts
    # at its `8=FIX`, so the log's own prefix never glues onto the first tag.
    # RE2 has no lookbehind; the non-digit guard rides outside the capture.
    begun = compute.struct_field(
        compute.extract_regex(values, r"(?:^|[^0-9])(?P<msg>8=FIXT?.*)"), "msg"
    )
    values = compute.if_else(compute.is_null(begun), values, begun)
    tokens = compute.split_pattern(values, separator)
    flat = tokens.flatten()
    matched = compute.match_substring_regex(flat, _PAIR_TOKEN)
    matched = compute.fill_null(matched, False)
    kept = compute.filter(flat, matched)
    halves = compute.split_pattern(kept, "=", max_splits=1)
    tags = compute.utf8_trim_whitespace(compute.list_element(halves, 0))
    entries = compute.utf8_trim_whitespace(compute.list_element(halves, 1))
    counted = compute.cumulative_sum(matched.cast(pyarrow.int32()))
    bounds = pyarrow.concat_arrays([pyarrow.array([0], pyarrow.int32()), counted])
    offsets = bounds.take(_boundaries(tokens))
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


def _column_separator(values: Any) -> str:
    """The separator of the first line that reveals one; SOH when none does."""
    for value in values:
        if value.is_valid:
            text = value.as_py()
            if text:
                return detect_separator(text)
    return SOH


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
