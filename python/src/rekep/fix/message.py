"""One FIX message out of a log line, and whole columns of them at once."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import functools
import re
from collections.abc import Collection, Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields.arrays import sequence

#: The delimiter the standard writes between fields: ASCII 0x01, Start of
#: Heading. Unprintable, which is why logs substitute something visible.
SOH = "\x01"

#: Stand-ins seen in real logs, in the order they are tried when a message
#: does not say which it uses: the standard SOH first, then the printable
#: substitutions tools make -- a pipe, the caret spelling of Ctrl-A, a bare
#: caret, a semicolon. Order matters: `^A` must be tried before `^`, or a
#: caret-A log reads every tag with an `A` glued to the front.
SEPARATORS = (SOH, "|", "^A", "^", ";")

#: What a bridge writes in front of every key, and -- where it writes nothing
#: else -- what separates them: `#A=1#B=2` has no delimiter between its tokens
#: at all, so the marker of the next key is the end of the value before it.
#:
#: Not a member of `SEPARATORS`, deliberately. That list is the blind scan's
#: candidates and the entry separator's, and a `#` anywhere in a line nobody
#: has established is a bridge message is a hash, not a delimiter.
MARKER = "#"

#: What a BeginString *value* may hold: anything that is not a separator
#: candidate and not whitespace -- so the value stops exactly where the
#: separator starts. Spelled out for `_WS`'s reason, and shared, because the
#: scalar reading of a separator and the vectorised one are one rule.
_NOT_SEPARATOR = r"[^\x01|;^ \t\r\n\f\x0b]"

#: The guard that keeps the `8=` inside tag 18 or 58 from reading as a
#: BeginString: the start of the text, or a character that is not a digit.
#: RE2 has no lookbehind, so wherever this rule is run vectorised the guard
#: rides *outside* the capture rather than behind it.
_NOT_A_TAG = r"(?:^|[^0-9])"

#: Where a message starts inside a log line: `8=FIX...` at the start or after
#: anything that is not a digit. Public because **a classification rule is
#: data** -- `Rules.into_default()`'s FIX rule is this string, and a rule set loaded
#: from a document has to be able to spell it -- and because what follows the
#: BeginString value is, by construction, the separator.
BEGIN_STRING = rf"{_NOT_A_TAG}8=FIXT?"

#: `BEGIN_STRING` with the message after it captured: how a vectorised parse
#: cuts the log's own prefix off a line. `(?s)`, or a message holding a
#: newline would end at it here where the scalar slice keeps it.
_BEGIN_VECTOR = rf"(?s){_NOT_A_TAG}(?P<msg>8=FIXT?.*)"

#: The scalar reading of the same rule, and the one spelling of it that uses a
#: lookbehind: it has to report where the `8` is and not where the guard is,
#: and Python has lookbehind where RE2 does not.
#:
#: Every scalar pattern here is compiled `re.ASCII`, because the vectorised
#: twins run under RE2, whose `\d`/`\s`/`\w` are ASCII-only -- and the two
#: parsers are contracted to agree. Without the flag a tag written in
#: Arabic-Indic digits was a pair to the scalar parser and noise to the
#: vectorised one; ASCII is also what the FIX standard means by a digit.
_BEGIN = re.compile(rf"(?:^|(?<=[^\d]))8=FIXT?{_NOT_SEPARATOR}*", re.ASCII)

#: A rendered field or group name, as the tools around a bridge print one --
#: letters first, then the word characters, dots (a component path like
#: `Instrument.Symbol`) and dashes real feeds put in names. Deliberately not
#: containing `[`, which is what lets one greedy regex split `NoPartyIDs[0]`
#: into the name and the entry index.
_NAME = r"[A-Za-z][A-Za-z0-9_.\-]*"

#: What a bridge writes between brackets, beside a group's entry index: the
#: name of a member of the struct in front of it. `INSTRUMENT[EXCHANGE]=XPAR`
#: and `INSTRUMENT.EXCHANGE=XPAR` are the same field written two ways, and a
#: parser that read only the digits saw the second and not the first -- which
#: cost the whole *line*, because a line whose keys do not tokenise is not a
#: bridge message and every other field on it went with them.
_SELECTOR = r"[A-Za-z][A-Za-z0-9_.\-]*"

#: One bracketed part of a rendered key: an entry index, or a member name.
_BRACKET = rf"\[(?:\d+|{_SELECTOR})\]"

#: Whitespace, spelled out. Python's ASCII `\s` holds `\x0b` and RE2's does
#: not, so a `\s` in a pattern that exists in both engines is a divergence
#: waiting for a vertical tab; one explicit class reads the same everywhere.
_WS = r"[ \t\r\n\f\x0b]"

#: The same characters as a set `str.strip` takes, for the paths that do not
#: run the pattern. `str.strip()` with no argument strips *Unicode* whitespace,
#: which is wider than what any of these regexes call whitespace.
_STRIPPED = " \t\r\n\f\x0b"

#: One `#NAME=` token, which is how a UL bridge marks a field: the `#` says
#: "a key starts here", which is the only thing in a rendered line that does.
#:
#: The bracket and the dotted member are both admitted, because both are how a
#: real line spells a group member -- `#NoPartyIDs[0].PartyID=` is what this
#: parser's own canonical key looks like, and a rule that would not recognise
#: it called such a line no message at all and lost every field on it.
_BRIDGE_TOKEN = rf"#{_NAME}(?:{_BRACKET})?(?:\.[A-Za-z0-9_.\-]+)?{_WS}*="

#: What makes a line a **bridge message**: two or more of those tokens.
#: Public for the same reason `BEGIN_STRING` is -- it is the UL classification
#: rule, and a rule is data. Two and not one, because a lone `#FOO=bar` in
#: prose is a sentence, and a rule that called it a message would parse every
#: log line that mentions a hashtag.
BRIDGE = rf"(?s){_BRIDGE_TOKEN}.*{_BRIDGE_TOKEN}"

#: `BRIDGE` with the message after it captured: where a bridge message starts
#: inside a log line, exactly as `_BEGIN_VECTOR` says where a wire message
#: does. `toBridge #ISINCODE=x|#SIDE=1` carries the plugin's own prefix, and
#: without a start marker the first key would be `toBridge #ISINCODE`.
_BRIDGE_VECTOR = rf"(?s)(?P<msg>{_BRIDGE_TOKEN}.*{_BRIDGE_TOKEN}.*)"

#: A wire message whose **body** is a bridge one: a BeginString, and MsgType
#: `UL` somewhere after it. Some venues wrap the bridge's own `#NAME=` payload
#: in a FIX envelope, and such a line answers to both tells -- so it needs a
#: rule of its own, or the FIX one claims it first and every named field in it
#: is read as noise.
#:
#: The MsgType is the discriminator and not the `#` tokens, because that is
#: what the *sender* said the message is: a wire message with a hash in a Text
#: field is not a bridge message, and one that says `35=UL` is one however few
#: fields it happens to carry.
BRIDGE_WIRE = rf"(?s){BEGIN_STRING}.*[^0-9]35=UL(?:[^A-Za-z0-9]|$)"

#: The scalar reading of the same rule: the first `#NAME=` that has another
#: after it. A lookahead rather than a capture, because the scalar path wants
#: the *position* and RE2 -- which has neither -- reads it off the capture.
_BRIDGE = re.compile(rf"{_BRIDGE_TOKEN}(?=.*{_BRIDGE_TOKEN})", re.DOTALL | re.ASCII)

#: One `#NAME=` on its own, for finding the *second* one -- whatever sits in
#: front of which is the separator (`detect_separator`).
_BRIDGE_NEXT = re.compile(_BRIDGE_TOKEN, re.ASCII)

#: `detect_separator`, vectorised, in its two halves: whatever follows the
#: BeginString value, and -- for a line that has none -- whatever sits in front
#: of the second `#NAME=`. `\^A` is offered before `.` in both, or a caret-A
#: log reads its separator as `^` and every tag comes back with an `A` glued to
#: the front.
#:
#: One column of these is what lets a batch that mixes sessions be parsed at
#: all: `parse_arrow_array` samples a column once by contract, so the rows that
#: share a separator have to be handed to it together, and this is how a caller
#: finds out which those are without reading a row in Python.
SEPARATOR_VECTOR = rf"(?s){_NOT_A_TAG}8=FIXT?{_NOT_SEPARATOR}*(?P<sep>\^A|.)"
NAMED_SEPARATOR_VECTOR = (
    rf"(?s)(?:^|[^A-Za-z0-9])#?"
    rf"(?:8|[Bb][Ee][Gg][Ii][Nn][Ss][Tt][Rr][Ii][Nn][Gg]){_WS}*="
    rf"[Ff][Ii][Xx][Tt]?{_NOT_SEPARATOR}*(?P<sep>\^A|.)"
)
_BRIDGE_PAIR_TOKEN = rf"#(?:\d+|{_NAME})(?:{_BRACKET})?(?:\.[A-Za-z0-9_.\-]+)?{_WS}*="
BRIDGE_SEPARATOR_VECTOR = rf"(?s){_BRIDGE_PAIR_TOKEN}.*?(?P<sep>\^A|.){_BRIDGE_PAIR_TOKEN}"

#: One token of a message, in every spelling the logs use. Five shapes come
#: out of the same regex::
#:
#:     54=1                       tag = value
#:     Side=1                     name = value
#:     #SIDE=1                    the same, as a bridge marks a key
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
#:
#: The `#` is **captured and not merely allowed**, because only named mode may
#: shed it: a `#54=1` in a wire message is not tag 54, it is a rendered key
#: that happens to be spelled with digits, and tag mode has to be able to tell.
_TOKEN = re.compile(
    rf"^{_WS}*(?P<marker>#)?(?P<key>\d+|{_NAME})"
    rf"(?:\[(?:(?P<index>\d+)|(?P<select>{_SELECTOR}))\])?"
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
_PAIR_TOKEN_NAMED = rf"^{_WS}*#?(?:\d+|{_NAME})(?:{_BRACKET})?(?:\.[A-Za-z0-9_.\-]+)?{_WS}*="

#: `_TOKEN` and `_MEMBER` for RE2, which has no DOTALL flag argument.
_TOKEN_VECTOR = (
    rf"(?s)^{_WS}*#?(?P<key>\d+|{_NAME})"
    rf"(?:\[(?:(?P<index>\d+)|(?P<select>{_SELECTOR}))\])?"
    rf"(?:\.(?P<member>[A-Za-z0-9_.\-]+))?"
    rf"{_WS}*=(?P<rest>.*)$"
)
_MEMBER_VECTOR = rf"(?s)^{_WS}*(?P<member>\d+|{_NAME}){_WS}*=(?P<value>.*)$"

#: The member half of a stored key, for resolving it back to a tag number:
#: the trailing name segment of `PartyID[1]`, `NoPartyIDs[0].PartyID` or a
#: dotted component path, with the index stripped. One spelling, compiled here
#: and handed to RE2 there, because the scalar reading of a key and the
#: vectorised one are contracted to agree like everything else in this module.
_MEMBER_NAME_VECTOR = rf"(?P<name>[A-Za-z0-9_\-]+)(?:\[\d+\])?{_WS}*$"
_MEMBER_NAME = re.compile(_MEMBER_NAME_VECTOR, re.ASCII)

#: A **rendered key** as tooling spells one, split into the part that says
#: *which field* and the part that only says *where it sits*::
#:
#:     Side                    lead ''            name 'Side'     index ''
#:     side                    lead ''            name 'side'     index ''
#:     msg_type                lead ''            name 'msg_type' index ''
#:     Msg Type                lead ''            name 'Msg Type' index ''
#:     Instrument.Symbol       lead 'Instrument.' name 'Symbol'   index ''
#:     NoPartyIDs[0].PartyID   lead 'NoPartyIDs[0].' name 'PartyID' index ''
#:     PartyID[1]              lead ''            name 'PartyID'  index '[1]'
#:
#: `lead` is greedy, so the *last* segment is the field's own name and every
#: component or group in front of it is decoration that `from_pairs` keeps.
#: The classes cover both cases already, so the case-insensitivity is in the
#: fold below rather than in a flag: `IGNORECASE` on an ASCII class buys
#: nothing and costs a pass.
_RENDERED_KEY = re.compile(
    rf"^{_WS}*(?P<lead>(?:[A-Za-z0-9_\- ]+(?:{_BRACKET})?\.)*)"
    rf"(?P<name>[A-Za-z0-9_\- ]+)(?P<index>\[\d+\])?{_WS}*$",
    re.ASCII,
)

#: BodyLength and CheckSum, the two fields whose *position* the standard
#: fixes: 8, 9 lead and 10 ends the message.
CHECKSUM = "10"
_CHECKSUM_NAME = "checksum"


#: An indexed token's head -- `#NoPartyIDs[0]=` -- which is the only place a
#: *second* separator can be, and so the only place `detect_entry_separator`
#: looks for one.
_INDEXED_HEAD = re.compile(rf"^{_WS}*#?{_NAME}\[\d+\]{_WS}*=", re.ASCII)


def detect_separator(text: str) -> str:
    """The character standing in for SOH in `text`."""
    match = _BEGIN.search(text)
    if match is not None:
        following = _separator_at(text, match.end())
        if following is not None:
            return following
    bridge = _BRIDGE.search(text)
    if bridge is not None:
        second = _BRIDGE_NEXT.search(text, bridge.end())
        if second is not None:
            return _separator_before(text, second.start())
    for candidate in SEPARATORS:
        if candidate in text:
            return candidate
    return SOH


def _separator_at(text: str, index: int) -> str | None:
    """The separator starting at `index`, or None where nothing readable is."""
    if index >= len(text):
        return None
    following = text[index]
    if following == "^" and text[index + 1 : index + 2] == "A":
        return "^A"
    return None if following.isspace() else following


def _separator_before(text: str, index: int) -> str:
    """The separator ending just before `index`; `MARKER` where the tokens abut."""
    if index < 1:
        return MARKER
    if text[index - 2 : index] == "^A":
        return "^A"
    preceding = text[index - 1]
    return preceding if preceding in SEPARATORS else MARKER


def detect_entry_separator(text: str, separator: str) -> str | None:
    """The character a group *entry* is written with inside one token, if any."""
    for token in text.split(separator):
        head = _INDEXED_HEAD.match(token)
        if head is None:
            continue
        rest = token[head.end() :]
        for candidate in SEPARATORS:
            if candidate in rest:
                return candidate
    return None


@dataclasses.dataclass
class FixPairs(Convertible):
    """One FIX message: its fields in wire order, tags and values as text.

    Order and repetition are the message -- a repeating group *is* tags
    repeating -- so the fields are a sequence of pairs, never a mapping, and
    `get`/`values` read over it. Values stay text: what a value *is* depends
    on a dictionary (`FixRegistry`) and on the message, and decoding is a
    cast against the field that knows (`rekep.fix.fields`).
    """

    @classmethod
    @functools.cache
    def into_redirects(cls) -> Mapping[Any, str]:
        """Generic conversions plus direct text parsing."""
        return MappingProxyType({**super().into_redirects(), str: "text"})

    pairs: list[tuple[str, str]] = dataclasses.field(default_factory=list)

    # -- building -----------------------------------------------------------

    @classmethod
    def from_text(
        cls,
        text: str | bytes,
        separator: str | None = None,
        *,
        named: bool | None = None,
        entry_separator: str | None = None,
    ) -> FixPairs:
        """Parse one log line, however it spells its separators and its keys."""
        if isinstance(text, bytes):
            text = text.decode("utf-8", "replace")
        begin = _BEGIN.search(text)
        if begin is not None:
            text = text[begin.start() :]
        if named is None:
            named = begin is None
        if named and begin is None:
            bridge = _BRIDGE.search(text)
            if bridge is not None:
                text = text[bridge.start() :]
        separator = separator or detect_separator(text)
        if named and entry_separator is None:
            entry_separator = detect_entry_separator(text, separator)
        pairs: list[tuple[str, str]] = []
        for token in text.split(separator):
            parsed = _parse_token(token, named)
            if parsed is None:
                continue
            key, value = parsed
            if entry_separator and "[" in key and entry_separator in value:
                pairs.extend(_entry_members(key, value, entry_separator))
            else:
                pairs.append(parsed)
            if _is_checksum(key):
                break
        return cls(pairs=pairs)

    @classmethod
    def from_pairs(
        cls,
        pairs: Iterable[tuple[Any, Any]],
        names: Mapping[str, int | str] | None = None,
    ) -> FixPairs:
        """A message out of `(key, value)` pairs, where a key is a tag *or* a name."""
        folded = _Names.of(names).keys
        built: list[tuple[str, str]] = []
        for key, value in pairs:
            if value is None:
                continue
            resolved = _resolved_key(key, folded)
            if resolved is not None:
                built.append((resolved, _rendered(value)))
        return cls(pairs=built)

    # -- reading ------------------------------------------------------------

    def get(self, tag: int | str, default: str | None = None) -> str | None:
        """The first value of `tag`, or `default`.

        One rule set, `FieldAccess` (fix/access.py): the exact key answers
        first, and then the rendered spellings of the same field -- `Side`
        also answers for `side`, `Side[0]` and `NoPartyIDs[0].Side`, because
        the index and the group are *where* the field sits, not what it is.
        """
        found = self._access().reading(self.pairs, tag)
        return found.raw if found else default

    def values(self, tag: int | str) -> list[str]:
        """Every value of `tag`, in wire order -- what a repeating tag is.

        The same rules as `get`, so `values("PartyID")` collects one value per
        printed group entry.
        """
        return [found.raw for found in self._access().readings(self.pairs, tag)]

    @staticmethod
    def _access() -> Any:
        """The dictionary-less accessor: a bare wire model resolves by spelling.

        Imported at the call because `fix.access` composes this module's own
        key rules with the transcription's -- the one place the import runs
        the other way.
        """
        from rekep.fix.access import FieldAccess

        return FieldAccess.spelling_only()

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
        """The entries of the repeating group `count_tag` counts."""
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
        """The entries of a group a log rendered with indexes, in index order."""
        wanted = str(name)
        pattern = _indexed_pattern(wanted)
        entries: dict[int, list[tuple[str, str]]] = {}
        for key, value in self.pairs:
            # An indexed key has a `[` in it, and a wire message has none in
            # any of its keys -- so the reject is a substring test rather than
            # a regex, and a feed of tag-spelled messages pays no regex at all
            # for the groups it does not carry. Measured on market data, where
            # six group lookups a message each fell through to here: 312,000
            # of the 332,000 regex matches in a 4,000-line parse were this.
            if "[" not in key:
                continue
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


def message_bodies(column: Any, named: bool) -> tuple[Any, Any]:
    """A column of log lines cut down to the messages inside them.

    `(bodies, wire)`: what each line carries, and whether it was found by its
    BeginString. Shared rather than repeated, because anything reading a
    line's tokens has to start where the parser starts -- a reading that began
    one character earlier would count `toBridge #ISINCODE` as a key.
    """
    compute = pyarrow.compute
    values = column.cast(pyarrow.string(), safe=False)
    starts = compute.starts_with(values, "8=FIX")
    if compute.all(starts, min_count=0).as_py():
        wire = compute.fill_null(starts, False)
    else:
        begun = compute.struct_field(compute.extract_regex(values, _BEGIN_VECTOR), "msg")
        wire = compute.is_valid(begun)
        values = compute.if_else(wire, begun, values)
    if named:
        # The other kind of message start, and **only** where there was no
        # first one: a line carrying a wire header and a bridge body starts at
        # the header, or the tags that say what it is are cut off with the
        # log's prefix. The scalar parser applies the same guard, so the two
        # agree by construction.
        bridged = compute.struct_field(compute.extract_regex(values, _BRIDGE_VECTOR), "msg")
        values = compute.if_else(
            compute.and_(compute.invert(wire), compute.is_valid(bridged)), bridged, values
        )
    return values, wire


#: One token's marker and key, for counting what a capture spells rather than
#: parsing it. The same token rule `_TOKEN_VECTOR` reads, with the `#` kept:
#: only a parse may shed it, and what a bridge writes `#Foo` and what it
#: writes `Foo` are two different things to count.
_MARKED_KEY_VECTOR = (
    rf"(?s)^{_WS}*(?P<marker>#?)(?P<key>\d+|{_NAME})"
    rf"(?:\[(?:\d+|(?P<select>{_SELECTOR}))\])?"
    rf"(?:\.(?P<member>[A-Za-z0-9_.\-]+))?"
    rf"{_WS}*="
)


def rendered_keys(
    column: Any, separator: str | None = None, *, named: bool | None = None
) -> tuple[Any, Any]:
    """`(marker, key)` for every token of every line, flattened, in kernels.

    What a capture *spells*, which a parse deliberately does not keep: named
    mode sheds the `#`, and the two namespaces a bridge writes -- `#Side`
    before enrichment and `Side` after -- are then indistinguishable. A group
    index is dropped, because `NOPARTYIDS[0]` and `[1]` are one name twice.
    """
    compute = pyarrow.compute
    if isinstance(column, pyarrow.ChunkedArray):
        column = column.combine_chunks()
    if separator is None or named is None:
        sampled = _column_style(column, named)
        separator = sampled[0] if separator is None else separator
        named = sampled[1] if named is None else named
    values, _ = message_bodies(column, named)
    flat = compute.split_pattern(values, separator).values
    parsed = compute.extract_regex(flat, _MARKED_KEY_VECTOR)
    keys = compute.struct_field(parsed, "key")
    member = compute.struct_field(parsed, "member")
    keys = compute.if_else(
        compute.fill_null(compute.greater(compute.binary_length(member), 0), False),
        compute.binary_join_element_wise(keys, compute.fill_null(member, ""), "."),
        keys,
    )
    valid = compute.is_valid(keys)
    return (
        compute.filter(compute.struct_field(parsed, "marker"), valid),
        compute.filter(keys, valid),
    )


def parse_arrow_array(
    column: Any,
    separator: str | None = None,
    *,
    named: bool | None = None,
    entry_separator: str | None = None,
) -> Any:
    """A column of FIX log lines as one `map<string, string>` per row."""
    if separator is None or named is None or entry_separator is None:
        # Sampled from the column as handed over -- once, even for a chunked
        # column, so where a chunk boundary falls can never change what a row
        # parses to. Skipped entirely when the caller said all three.
        #
        # The caller's `named` is handed *in*, because whether there is a
        # second separator to look for depends on it: a wrapped bridge message
        # has a BeginString, so the sample would read it as wire and never look
        # inside an indexed token the caller is about to ask it to read.
        sampled = _column_style(column, named)
        separator = sampled[0] if separator is None else separator
        named = sampled[1] if named is None else named
        entry_separator = sampled[2] if entry_separator is None else entry_separator
    if isinstance(column, pyarrow.ChunkedArray):
        parsed = [
            parse_arrow_array(chunk, separator, named=named, entry_separator=entry_separator)
            for chunk in column.chunks
        ]
        # The explicit type is for the zero-chunk column, which is legal and
        # has nothing to infer from.
        return pyarrow.chunked_array(parsed, type=pyarrow.map_(pyarrow.string(), pyarrow.string()))
    compute = pyarrow.compute
    values, wire = message_bodies(column, named)
    canonical = False
    wire_pattern = _canonical_wire_pattern(separator) if not named else None
    if wire_pattern is not None:
        canonical = bool(
            compute.all(compute.match_substring_regex(values, wire_pattern), min_count=0).as_py()
        )
    tokens = compute.split_pattern(values, separator)
    # `.values`, not `.flatten()`: the boundaries below index into the child
    # array as the offsets wrote it, and `flatten` re-slices around null rows.
    # A kernel output owns its buffers from zero, so the two only agree here.
    flat = tokens.values
    parsed = None
    if named:
        # The extracted key is also the validity test. Running
        # `_PAIR_TOKEN_NAMED` first repeated the same RE2 walk immediately in
        # `_named_pairs`; bridge captures are the expensive parser case.
        parsed = compute.extract_regex(flat, _TOKEN_VECTOR)
        matched = compute.fill_null(compute.is_valid(compute.struct_field(parsed, "key")), False)
    else:
        matched = (
            compute.not_equal(flat, "")
            if canonical
            else compute.match_substring_regex(flat, _PAIR_TOKEN)
        )
        matched = compute.fill_null(matched, False)
    expansion = None
    if named:
        assert parsed is not None
        tags, entries, expansion = _named_pairs(compute.filter(parsed, matched), entry_separator)
    else:
        kept = compute.filter(flat, matched)
        halves = compute.split_pattern(kept, "=", max_splits=1)
        tags = compute.list_element(halves, 0)
        if not canonical:
            tags = compute.utf8_trim_whitespace(tags)
        entries = compute.utf8_trim_whitespace(compute.list_element(halves, 1))
    weights = matched.cast(pyarrow.int32())
    parents = compute.filter(compute.list_parent_indices(tokens), matched)
    if expansion is not None:
        # A token that carried a whole group entry produced several pairs, so
        # what each row is worth is no longer "how many tokens matched" -- the
        # count per token goes back where the mask had a 1, and the row
        # offsets fall out of the same cumulative sum they always did.
        counts, taken = expansion
        weights = compute.replace_with_mask(weights, matched, counts)
        parents = compute.take(parents, taken)
    counted = compute.cumulative_sum(weights)
    bounds = pyarrow.concat_arrays([pyarrow.array([0], pyarrow.int32()), counted])
    offsets = bounds.take(_boundaries(tokens))
    tags, entries, offsets = _until_checksum(tags, entries, offsets, parents, named)
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


@functools.lru_cache(maxsize=len(SEPARATORS) + 1)
def _canonical_wire_pattern(separator: str) -> str | None:
    """Whole-row guard for the numeric wire fast path."""
    if len(separator) != 1:
        return None
    escaped = re.escape(separator)
    value = rf"[^{escaped}\r\n]*"
    return rf"^\d+={value}(?:{escaped}\d+={value})*{escaped}?$"


def _named_pairs(token: Any, entry_separator: str | None = None) -> tuple[Any, Any, Any]:
    """Canonical `(keys, values, expansion)` for named-mode tokens, in kernels."""
    compute = pyarrow.compute
    empty = pyarrow.scalar("")
    # An optional group that did not take part comes back as the *empty
    # string*, not null -- RE2's convention through `extract_regex` -- and no
    # real index or member can be empty, so emptiness is the test throughout.
    key = compute.struct_field(token, "key")
    index = compute.fill_null(compute.struct_field(token, "index"), "")
    select = compute.fill_null(compute.struct_field(token, "select"), "")
    member = compute.fill_null(compute.struct_field(token, "member"), "")
    value = compute.fill_null(compute.struct_field(token, "rest"), "")
    # `INSTRUMENT[EXCHANGE]` selects a member by name where `[0]` selects an
    # entry by position, so it joins the key as the dotted path it is another
    # spelling of -- before anything below reads the key.
    key = compute.if_else(
        compute.not_equal(select, empty),
        compute.binary_join_element_wise(key, select, "."),
        key,
    )
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
    # The entry's own key -- `NoPartyIDs[0]` -- kept separately because the
    # members behind an entry separator all hang off it.
    lead = compute.binary_join_element_wise(key, bracket, "")
    dotted = compute.if_else(
        compute.not_equal(member, empty),
        compute.binary_join_element_wise(".", member, ""),
        empty,
    )
    tags = compute.binary_join_element_wise(lead, dotted, "")
    if entry_separator and compute.any(indexed, min_count=0).as_py():
        expanded = _entry_pairs(tags, lead, value, indexed, entry_separator)
        if expanded is not None:
            return expanded
    return tags, compute.utf8_trim_whitespace(value), None


def _entry_pairs(
    tags: Any, lead: Any, value: Any, indexed: Any, entry_separator: str
) -> tuple[Any, Any, Any] | None:
    """The vectorised `_entry_members`: one token, several members."""
    compute = pyarrow.compute
    empty = pyarrow.scalar("")
    chunks = compute.split_pattern(compute.if_else(indexed, value, empty), entry_separator)
    counts = compute.fill_null(compute.list_value_length(chunks), 1).cast(pyarrow.int32())
    if not compute.any(compute.greater(counts, 1), min_count=0).as_py():
        return None
    taken, first = _expanded(counts)
    flat = chunks.values
    expanded_indexed = compute.take(indexed, taken)
    continuation = compute.and_(expanded_indexed, compute.invert(first))
    # Only continuation chunks can be `member=value`. The old whole-child
    # regex also inspected every ordinary field and every first group member.
    inner = compute.extract_regex(compute.filter(flat, continuation), _MEMBER_VECTOR)
    slots = compute.if_else(
        continuation,
        compute.subtract(
            compute.cumulative_sum(continuation.cast(pyarrow.int32())),
            pyarrow.scalar(1, pyarrow.int32()),
        ),
        pyarrow.scalar(None, pyarrow.int32()),
    )
    member = compute.fill_null(compute.take(compute.struct_field(inner, "member"), slots), "")
    inner_value = compute.take(compute.struct_field(inner, "value"), slots)
    named_member = compute.and_(continuation, compute.not_equal(member, empty))
    expanded_lead = compute.take(lead, taken)
    # A chunk with no `member=` keeps the entry's own key rather than being
    # dropped or guessed at -- the scalar parser's rule, for its reason.
    keys = compute.if_else(
        continuation,
        compute.if_else(
            named_member,
            compute.binary_join_element_wise(expanded_lead, member, "."),
            expanded_lead,
        ),
        compute.take(tags, taken),
    )
    values = compute.if_else(
        expanded_indexed,
        compute.if_else(
            first,
            flat,
            compute.if_else(named_member, compute.fill_null(inner_value, ""), flat),
        ),
        compute.take(value, taken),
    )
    return keys, compute.utf8_trim_whitespace(values), (counts, taken)


def _expanded(counts: Any) -> tuple[Any, Any]:
    """Which token each expanded pair came from, and whether it is that token's first.

    `repeat` in kernels: a list array whose offsets are the running counts has
    exactly one slot per pair, so `list_parent_indices` *is* the repeat -- the
    same construction `parse_arrow_array` builds its row offsets from, one
    level down. A pair is its token's first when its own index equals where
    the token starts.
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
    starts = compute.take(bounds.slice(0, len(bounds) - 1), taken)
    return taken, compute.equal(running, starts)


def _until_checksum(
    tags: Any, entries: Any, offsets: Any, parents: Any, named: bool
) -> tuple[Any, Any, Any]:
    """Each row cut after its first CheckSum <10>, the way the scalar parser cuts."""
    compute = pyarrow.compute
    if named:
        # A rendered feed repeats a few field names across millions of rows.
        # Normalise each distinct spelling once, then expand its boolean result.
        encoded = compute.dictionary_encode(tags)
        distinct = encoded.dictionary
        terminal = compute.replace_substring_regex(distinct, r"^.*\.", "")
        folded = compute.utf8_lower(terminal)
        rendered = compute.and_(
            compute.invert(compute.match_substring(distinct, "[")),
            compute.or_(compute.equal(terminal, CHECKSUM), compute.equal(folded, _CHECKSUM_NAME)),
        )
        distinct_checks = compute.or_(compute.equal(distinct, CHECKSUM), rendered)
        checks = compute.take(distinct_checks, encoded.indices)
    else:
        checks = compute.equal(tags, CHECKSUM)
    if not compute.any(checks, min_count=0).as_py():
        return tags, entries, offsets
    positions = sequence(len(tags))
    row_ends = compute.take(offsets.slice(1), parents)
    before_end = compute.less(positions, compute.subtract(row_ends, 1))
    if not compute.any(compute.and_(checks, before_end), min_count=0).as_py():
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


def _is_checksum(key: str) -> bool:
    """Whether a top-level key is CheckSum <10>, including a component path."""
    if key == CHECKSUM:
        return True
    if "[" in key:
        return False
    terminal = key.rsplit(".", 1)[-1]
    return terminal == CHECKSUM or terminal.lower() == _CHECKSUM_NAME


def _column_style(column: Any, named: bool | None = None) -> tuple[str, bool, str | None]:
    """`(separator, named, entry separator)` off the first non-empty line."""
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
            separator = detect_separator(text)
            reading = _BEGIN.search(text) is None if named is None else named
            entry = detect_entry_separator(text, separator) if reading else None
            return separator, reading, entry
    return SOH, False if named is None else named, None


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


class _Names:
    """One caller's `{name: tag}` mapping, folded once and held by identity.

    Both readings a parse asks of a dictionary come off the same fold: the
    stored key a rendered name resolves to (`from_pairs`), and the tag number
    a whole column of keys resolves to (`tag_arrow_array`). Folding it per
    call was most of what either cost -- a FIX dictionary is six thousand
    names, and a batch resolves a few hundred distinct spellings against it.
    """

    def __init__(self, names: Mapping[str, int | str]) -> None:
        self.source = names
        self.size = len(names)
        #: Folded name to the tag as a key is spelled.
        self.keys = {str(name).strip().lower(): str(tag) for name, tag in names.items()}

    @functools.cached_property
    def tags(self) -> dict[str, int]:
        """The same fold as numbers, for a column resolved to an Arrow tag type.

        Derived rather than built beside `keys`, because a caller that only
        resolves spellings never pays for it -- and a mapping whose tags are
        not numbers is usable for one reading and not the other.
        """
        return {name: int(tag) for name, tag in self.keys.items()}

    @classmethod
    def of(cls, names: Mapping[str, int | str] | None) -> _Names:
        """`names` folded, out of the last few folded, matched by **identity**.

        Tiny and strongly held: a few mappings of a few thousand entries is
        nothing, and holding them is what keeps an id from being recycled onto
        a different object.
        """
        if not names:
            return _NO_NAMES
        for held in _FOLDED:
            if held.source is names and held.size == len(names):
                return held
        held = cls(names)
        _FOLDED.insert(0, held)
        del _FOLDED[_FOLDED_KEPT:]
        return held


#: How much of a key column is cast before the rest of it is. Every message
#: keys its fields one way, so a column that carries a rendered name carries
#: one within a message or two of its start.
_TAG_PROBE = 64

#: The last few dictionaries folded, newest first.
_FOLDED: list[_Names] = []
_FOLDED_KEPT = 4

#: What a caller resolving against no dictionary at all reads through.
_NO_NAMES = _Names({})


def tag_arrow_array(
    maps: Any,
    key_type: pyarrow.DataType | None = None,
    names: Mapping[str, int | str] | None = None,
    *,
    drop_unknown: bool = False,
) -> Any:
    """The same maps with integer tags for keys: `map<int32, string>`."""
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

    The cast is the cheapest reading a column of wire tags has and the most
    expensive failure a column of names has -- pyarrow converts the whole
    column before it raises -- so the head is cast first and only a head that
    reads as tags is worth casting the rest of. A column of names then
    resolves through its *distinct* spellings, once each, and is `take`n back
    across the rows: twelve times what a failed full cast then cost
    (benchmarks/bench_text_file.py).
    """
    try:
        keys.slice(0, _TAG_PROBE).cast(key_type)
        return keys.cast(key_type)
    except (pyarrow.ArrowInvalid, pyarrow.ArrowNotImplementedError):
        pass
    lookup = _Names.of(names).tags
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


# -- pairs -------------------------------------------------------------------


def _resolved_key(key: Any, folded: Mapping[str, str]) -> str | None:
    """One given key as the key the message stores it under, or None to drop it."""
    if isinstance(key, bool):
        return None
    if isinstance(key, int):
        return str(key)
    text = str(key).strip()
    if not text:
        return None
    if text.isascii() and text.isdigit():
        return text
    tag = folded.get(text.lower())
    if tag is not None:
        return tag
    match = _RENDERED_KEY.match(text)
    if match is None:
        return text
    lead, name, index = match.group("lead", "name", "index")
    tag = folded.get(name.lower())
    if tag is None:
        return text
    return f"{lead}{tag}{index or ''}"


def _rendered(value: Any) -> str:
    """One value as the wire spells it."""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "Y" if value else "N"
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    spelling = getattr(value, "into_fix", None)
    if callable(spelling):
        return str(spelling())
    if isinstance(value, float):
        return format(decimal.Decimal(repr(value)), "f")
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return _stamped(value)
    return str(value)


def _stamped(value: datetime.date | datetime.time) -> str:
    """A date, a time or an instant in the spellings the standard fixes.

    `UTCTimestamp` is `YYYYMMDD-HH:MM:SS.ssssss`, `UTCDateOnly` is
    `YYYYMMDD` and `UTCTimeOnly` is `HH:MM:SS.ssssss` -- microseconds because
    that is the finest the format carries a value for, and always present so
    a reader never has to branch on whether the fraction is there.
    """
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y%m%d-%H:%M:%S.%f")
    if isinstance(value, datetime.time):
        return value.strftime("%H:%M:%S.%f")
    return value.strftime("%Y%m%d")


# -- one token ---------------------------------------------------------------


def _parse_token(token: str, named: bool) -> tuple[str, str] | None:
    """One separator-delimited token as a `(key, value)` pair, or noise."""
    head, sign, rest = token.partition("=")
    key = head.strip(_STRIPPED)
    if sign and key.isascii() and key.isdigit():
        # `tag=value`, which is nearly every token of nearly every message and
        # exactly what the regex would have come back with: a digit key takes
        # no index and no member, and both modes admit it. The strip is the
        # pattern's own `_WS` class and not `str.strip`, whose Unicode
        # whitespace would let a non-breaking space through as a tag; the
        # digits are `re.ASCII`'s, so an Arabic-Indic numeral still is not one.
        return key, rest.strip()
    match = _TOKEN.match(token)
    if match is None:
        return None
    marker, key, index, select, member, rest = match.group(
        "marker", "key", "index", "select", "member", "rest"
    )
    if not named and (
        marker is not None
        or index is not None
        or select is not None
        or member is not None
        or not key.isdigit()
    ):
        return None
    if select is not None:
        # `INSTRUMENT[EXCHANGE]` selects a member by name where `[0]` selects
        # an entry by position, so it reads as the dotted path it is another
        # spelling of. One canonical key, whichever way the bridge wrote it.
        key = f"{key}.{select}"
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


def _entry_members(key: str, value: str, entry_separator: str) -> list[tuple[str, str]]:
    """One indexed token that carries a whole group entry, as its members."""
    head, _, rest = value.partition(entry_separator)
    lead = key.rsplit(".", 1)[0]
    built = [(key, head.strip())]
    for chunk in rest.split(entry_separator):
        member = _MEMBER.match(chunk)
        if member is None:
            built.append((lead, chunk.strip()))
        else:
            built.append((f"{lead}.{member['member']}", member["value"].strip()))
    return built


@functools.lru_cache(maxsize=1024)
def _indexed_pattern(wanted: str) -> re.Pattern[str]:
    """`wanted[i]` and `wanted[i].member`, for `indexed_group`.

    Cached because a stream asks for the same few group names on every
    message, and building the pattern is more work than running it.
    """
    return re.compile(rf"^{re.escape(wanted)}\[(\d+)\](?:\.(.+))?$", re.IGNORECASE)
