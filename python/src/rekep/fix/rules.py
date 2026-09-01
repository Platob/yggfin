"""Protocol classification, parsing rules, and log target categories."""

from __future__ import annotations

import dataclasses
import functools
import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.entries import Entry
from rekep.enums import Direction, EventType, Plugin, Protocol
from rekep.fields import Field, column_name, scalar
from rekep.fields.arrays import sequence
from rekep.fix.message import (
    BEGIN_STRING,
    FIX_MSG_TYPE_PATTERN,
    NAMED_KEY,
    XML_DATA_TAG,
    carries_message,
)

#: Read a payload as numbered tags, as both, as rendered names, as XML, or not at all.
CODECS: tuple[str, ...] = ("fix", "fixml", "ul", "xml", "none")

#: The three a payload's own keys decide, and what each one means:
#:
#: - `fix` -- numbered FIX tags and no named key;
#: - `fixml` -- numbered tags and named keys together, whether the names sit in
#:   `XmlData <213>` or inline beside the tags;
#: - `ul` -- named keys and no numbered tag.
#:
#: The wire token `35=UL` is a MsgType and decides nothing here: a numbered-only
#: frame carrying it is `fix`, a frame carrying a named payload beside it is
#: `fixml`, and a bare named document is `ul` whatever its `MSGTYPE` says.
#: Classification reads the *key* of each parsed pair, so a value full of
#: digits never makes a named field numbered.
SHAPES: tuple[str, ...] = ("fix", "fixml", "ul")

#: `codec` -> `parse_arrow_array`'s named mode; None skips parsing. Named for
#: what it maps rather than for the word it maps to: three unrelated `NAMED`
#: constants meant three things, and an import of one read as an import of
#: another.
CODEC_KEYS: dict[str, bool | None] = {
    "fix": False,
    "fixml": True,
    "ul": True,
    "xml": True,
    "none": None,
}

#: What the `protocol` column stores, and so what every kernel here builds: a
#: packed code and never the name it spells.
_PROTOCOL_CODE = Protocol.into_storage_type()

#: How a bridge says which way a payload moved -- `Receiving : 8=FIX...`,
#: `Sending : ...`, `Message received:`, `IN 8=FIX...`, `[OUT] ...` -- counted
#: only where the verb opens the line before the payload's own first token, so
#: prose inside a FIX `Text <58>` or a bridge value never answers. Measured on
#: real capture: every FIX row carries one of these; most FIXML re-log lines
#: carry none, so direction is best-effort there.
#:
#: `in`/`out` answer only where the line or a bracket opens on them and a
#: delimiter closes them, which is the one shape a marker has and none of the
#: shapes the same letters have otherwise: `sending in session 3` and
#: `received out of order` are English, `direct:out` is a route endpoint and
#: `MCFID-IN-XPAR` is a session name. A word boundary is not enough, because a
#: hyphen is one -- and each of those standing in front of the opposite verb
#: turns a right answer into UNKNOWN rather than merely adding a wrong one.
#: `forward` and the session-name fields stay out: each mislabels enrichment
#: snapshots, Jolokia metadata or the route's fixed endpoints as movement.
INBOUND_PATTERN = r"(?i)\b(?:receiv(?:ing|ed))\b|(?:^|[\[({<|])in(?:bound|coming)?[ \t\])}>:|]"
OUTBOUND_PATTERN = r"(?i)\b(?:send(?:ing)?|sent)\b|(?:^|[\[({<|])out(?:bound|going)?[ \t\])}>:|]"

#: Parsed-log target categories. Market rows share one table; known operational
#: traffic stays separate from lines whose transport is not recognised.
MARKET_CATEGORY = "market"
MISC_CATEGORY = "misc"
UNKNOWN_CATEGORY = "unknown"


#: A leading global flag group, the one spelling `joined_pattern` must rewrite.
_LEADING_FLAGS = re.compile(r"^\(\?([a-zA-Z]+)\)")

#: A named capture group's opening, renamed per branch when patterns join.
_NAMED_GROUP = re.compile(r"\(\?P<([^>]+)>")


def joined_pattern(*patterns: str) -> str:
    """One alternation matching wherever any of `patterns` matches.

    Each branch is scoped whole, and a branch's leading global flags become a
    scoped group -- `(?i)x` embeds as `(?i:x)` -- because both engines this
    package writes patterns for accept the scoped form mid-pattern where
    Python `re` rejects the global one. When two or more branches join, each
    branch's named capture groups are made branch-local (`(?P<value>` in the
    second branch becomes `(?P<j1_value>`): the branches matched fine as
    separate patterns, and a name two of them share must not turn the join
    into a pattern neither engine accepts. Empty patterns are skipped: an
    empty alternation branch would match everything, where an empty
    `Rule.pattern` means "no pattern", which is the opposite.
    """
    spelled = [pattern for pattern in patterns if pattern]
    branches = []
    for index, pattern in enumerate(spelled):
        if len(spelled) > 1:
            pattern = _NAMED_GROUP.sub(rf"(?P<j{index}_\1>", pattern)
        flags = ""
        while found := _LEADING_FLAGS.match(pattern):
            flags += found.group(1)
            pattern = pattern[found.end() :]
        letters = "".join(dict.fromkeys(flags))
        branches.append(f"(?{letters}:{pattern})" if letters else f"(?:{pattern})")
    return "|".join(branches)


#: One ULBridge reference envelope. The closing grammar is parsed by
#: `text.entries`; this prefix alone is enough to classify it before the
#: generic key/value splitter mistakes its nested pipes for field separators.
REFERENTIAL_PAYLOAD_PATTERN = r"(?is)(?:^|[^A-Za-z0-9_])Referential[ \t]*\("

#: Where a payload of each shape starts, which is what a direction verb has to
#: precede. Classification reads the parsed keys and this reads the raw line,
#: so the two meet here: the anchor is the first token the payload could open
#: with, and a verb behind it is inside the payload rather than in front of it.
CODEC_ANCHORS: Mapping[str, str] = MappingProxyType(
    {
        "fix": joined_pattern(BEGIN_STRING, FIX_MSG_TYPE_PATTERN),
        "fixml": joined_pattern(BEGIN_STRING, FIX_MSG_TYPE_PATTERN, NAMED_KEY),
        "ul": joined_pattern(REFERENTIAL_PAYLOAD_PATTERN, NAMED_KEY),
        "xml": r"(?is)<(?:\?xml\b[^>]*>\s*<)?[A-Za-z_:][A-Za-z0-9_.:-]*(?:\s|/?>)",
    }
)

#: A bare XML document, a complete event after a transport prefix, an XmlApi
#: line, or a direction prefix whose next token is XML. Complete events may
#: follow any transport text; numbered envelope rules run first, so XML inside
#: FIX `Text <58>` stays with the envelope that owns it.
XML_PAYLOAD_PATTERN = joined_pattern(
    r"(?is)^[ \t]*(?:<\?xml\b[^>]*>\s*)?<[A-Za-z_:][A-Za-z0-9_.:-]*(?:\s|/?>)",
    r"(?is)<event(?:\s|>).*?</event>[ \t\r\n]*$",
    r"(?is)^[^<\r\n]*\bXmlApi\b[^<\r\n]*<"
    r"(?:\?xml\b[^>]*>\s*<)?[A-Za-z_:][A-Za-z0-9_.:-]*(?:\s|/?>)",
    r"(?is)^[ \t]*(?:Receiv(?:ing|ed)|Send(?:ing)?|Sent)[ \t]*:[ \t]*<"
    r"(?:\?xml\b[^>]*>\s*<)?[A-Za-z_:][A-Za-z0-9_.:-]*(?:\s|/?>)",
)


@scalar
class Rule(Convertible):
    """A protocol rule whose regex must work in Python `re` and Arrow RE2."""

    protocol: Annotated[Protocol, Field(dtype=Protocol.into_storage_type())] = Protocol.OTHER
    """What a line matching this rule carries, as the `protocol` column holds it."""

    pattern: str = ""
    """The message regex; alternatives join with `|` (see `joined_pattern`).
    Empty matches every line, which is what makes a fall-through rule."""

    plugin_pattern: str | None = None
    """Matched against `plugin` as well, when the plugin is what tells them apart."""

    separator: str | None = None
    """What the message writes between fields; null detects it per column."""

    entry_separator: str | None = None
    """What one indexed token writes between the members of a group entry; null detects it."""

    extra_entry_separators: tuple[str, ...] = ()
    """Additional literals considered when an indexed-entry separator is detected."""

    pop: dict[str, str] = dataclasses.field(default_factory=dict)
    """Rendered source fields replaced by their target field before FIX resolution."""

    codec: str = "none"
    """How to read the line: one of `CODECS`."""

    def __post_init__(self) -> None:
        """Read the protocol as a code, and keep a direct separator one literal."""
        # Strict here and tolerant at `Rules.rule`: a name the column cannot
        # hold would register as `UNKNOWN`, and two over-long names would then
        # be one protocol. A declaration is read once; a lookup runs per batch.
        declared = Protocol.from_str(self.protocol)
        if declared is Protocol.UNKNOWN:
            raise ValueError(
                f"{self.protocol!r} is no protocol name: at most sixteen bytes of [A-Z0-9._-]"
            )
        self.protocol = declared.family
        if isinstance(self.extra_entry_separators, str):
            self.extra_entry_separators = (self.extra_entry_separators,)
        else:
            self.extra_entry_separators = tuple(self.extra_entry_separators)
        self.pop = dict(self.pop)
        for source, target in self.pop.items():
            if not source or not target:
                raise ValueError("a popped field needs non-empty source and target names")
            if column_name(source) == column_name(target):
                raise ValueError(f"a popped field cannot replace itself: {source!r}")

    def into_dict(self) -> dict[str, Any]:
        """The rule as a document holds it, with the protocol spelled by name.

        A rule set is written and edited by hand, and a packed ASCII code is a
        nineteen-digit integer that says nothing to whoever opens the file.
        """
        return {**Convertible.into_dict(self), "protocol": self.protocol.name}

    @property
    def named(self) -> bool | None:
        """What `parse_arrow_array`'s `named` is for this rule; None is "no message"."""
        return CODEC_KEYS.get(self.codec)


#: The generic structured protocols are named by the shape their codec reads.
#: They carry no pattern, so the keys of a parsed payload decide them; a
#: grammar-specific rule such as `REFERENTIAL` carries its own pattern.
FIX = Rule(protocol=Protocol.FIX, codec="fix")

FIXML = Rule(protocol=Protocol.FIXML, codec="fixml")

UL = Rule(protocol=Protocol.UL, codec="ul", pop={"DetailedCFICode": "CFICode"})

XML = Rule(protocol=Protocol.XML, pattern=XML_PAYLOAD_PATTERN, codec="xml")

REFERENTIAL = Rule(
    protocol=Protocol.REFERENTIAL,
    pattern=REFERENTIAL_PAYLOAD_PATTERN,
    codec="ul",
    pop={"DetailedCFICode": "CFICode"},
)

#: Operational lines whose vocabulary is understood but which carry no market
#: message. Keeping these known lines out of `unknown` makes that table a
#: useful signal that a genuinely new log format arrived.
MISC = Rule(
    protocol=Protocol.MISC,
    pattern=joined_pattern(
        r"(?i)\bheartbeat\b",
        r"(?i)\b(?:connect(?:ed|ion)?|disconnect(?:ed|ion)?|reconnect(?:ed|ion)?)\b",
        r"(?i)\b(?:logon|logout|timeout|retry)\b",
    ),
    codec="none",
)

#: An empty pattern makes this the final fall-through rule.
OTHER = Rule(protocol=Protocol.OTHER, pattern="", codec="none")

#: First match wins. Numbered envelopes lead XML so a complete event inside
#: `Text <58>` remains one FIX field. XML and Referential lead the generic
#: named shape because each contains assignment-like text with its own nesting.
#: Every structured rule leads the operational patterns, so a message saying
#: "heartbeat" remains a message.
DEFAULT_RULES: tuple[Rule, ...] = (FIX, FIXML, XML, REFERENTIAL, UL, MISC, OTHER)


def _default_rules() -> list[Rule]:
    """Fresh default rule instances, so one set's edits stay its own."""
    return [dataclasses.replace(rule) for rule in DEFAULT_RULES]


@dataclasses.dataclass
class Rules(Convertible):
    """Which protocol each line carries, by the first rule that matches.

    First *configured* rule, not first match position in the text: the
    contract is that a specific rule sits in front of a general one and wins,
    which is what lets a `35=0` session rule precede `FIX` even though the
    envelope's `8=FIX` appears earlier in the line. A single-pass combined
    alternation was measured 1.53x faster over 292,750 real rows and rejected
    for exactly that reason -- an alternation's leftmost match decides by
    position, which would make "the rule I put first" inexpressible whenever
    its token sits later in the line than a general rule's. The cost is one
    kernel pass per configured rule instead of one in total.
    """

    @classmethod
    @functools.cache
    def into_default(cls) -> Rules:
        """Shared default rules, built lazily once per concrete class."""
        return cls()

    #: Rules in the order they are tried.
    rules: list[Rule] = dataclasses.field(default_factory=_default_rules)

    def rule(self, protocol: Protocol | str | int) -> Rule:
        """The first rule for one protocol, or `OTHER` when this set has none.

        What the pipeline reads a *slice* of a batch back with: the batch
        carries protocol codes, and how to parse a slice is a property of the
        rule that named it. Tolerant, unlike `Rule`'s own reading: a stored
        batch may carry a code this set no longer declares, and that is a
        line nobody parses rather than a configuration error.
        """
        wanted = Protocol.from_str(protocol).family
        for rule in self.rules:
            # By value, not identity: an open vocabulary evicts a code it
            # learnt at runtime, and the member a rule holds outlives that.
            if rule.protocol == wanted:
                return rule
        return OTHER

    def category_of(
        self, protocol: Protocol | str | int | None, eventtype: int | EventType | None
    ) -> str:
        """Target category for one parsed row."""
        kind = EventType.from_int(eventtype) if eventtype is not None else None
        if kind is not None and kind.rank >= EventType.INTENT.rank:
            return MARKET_CATEGORY
        if kind is EventType.MISC:
            return MISC_CATEGORY
        if Protocol.from_str(protocol).family in self.protocols:
            return MISC_CATEGORY
        return UNKNOWN_CATEGORY

    @property
    def protocols(self) -> frozenset[Protocol]:
        """Recognised protocol codes, excluding the fall-through value."""
        return frozenset(
            rule.protocol for rule in self.rules if rule.protocol is not Protocol.OTHER
        )

    def into_arrow_protocol_array(
        self, messages: Any, plugins: Any = None, entries: Any = None
    ) -> pyarrow.Array:
        """What each row carries, in kernels: one packed `protocol` code per line.

        `entries` is the row's already-parsed key/value pairs, which is what a
        structured rule is decided by -- the message stage hands over the ones
        it just parsed rather than paying for them twice.
        """
        compute = pyarrow.compute
        rows = len(messages)
        found: Any = pyarrow.repeat(
            pyarrow.scalar(OTHER.protocol.into_stored(), _PROTOCOL_CODE), rows
        )
        if not rows:
            return found
        text = messages.cast(pyarrow.string(), safe=False)
        plugin_text = None if plugins is None else Plugin.into_strings_arrow(plugins)
        shapes = (
            payload_shapes(Entry.payload_arrow(messages) if entries is None else entries)
            if any(rule.codec in SHAPES for rule in self.rules)
            else None
        )
        for rule in reversed(self.rules):
            hit = _hit(rule, text, plugin_text, shapes)
            found = compute.if_else(
                hit, pyarrow.scalar(rule.protocol.into_stored(), _PROTOCOL_CODE), found
            )
        # No cast: the seed above and every branch here are already the code the
        # column stores, so there is no width for the loop to have widened.
        return found

    def into_arrow_direction_array(self, messages: Any, protocols: Any) -> pyarrow.Array:
        """Packed transport direction read before the payload.

        The verb counts only where it starts before the row's own payload
        anchor, so a `sent` inside a FIX `Text <58>` or a bridge value never
        becomes a direction. Neither matching is `UNKNOWN`, and so is both: no
        answer beats a guessed one. A protocol whose rules carry no structured
        codec has no anchor and stays `UNKNOWN` whole.
        """
        compute = pyarrow.compute
        rows = len(messages)
        unknown = pyarrow.scalar(int(Direction.UNKNOWN), pyarrow.int32())
        found: Any = pyarrow.repeat(unknown, rows)
        if not rows:
            return found
        text = compute.fill_null(messages.cast(pyarrow.string(), safe=False), "")
        codes = Protocol.into_family_arrow(protocols)
        for protocol, anchor in self._anchors().items():
            selected = compute.fill_null(compute.equal(codes, protocol.into_stored()), False)
            if not compute.any(selected, min_count=0).as_py():
                continue
            payload_at = compute.find_substring_regex(text, pattern=anchor)
            received = _opens(
                compute.find_substring_regex(text, pattern=INBOUND_PATTERN), payload_at
            )
            sent = _opens(compute.find_substring_regex(text, pattern=OUTBOUND_PATTERN), payload_at)
            direction = compute.if_else(
                compute.and_(sent, compute.invert(received)),
                pyarrow.scalar(int(Direction.SENT), pyarrow.int32()),
                compute.if_else(
                    compute.and_(received, compute.invert(sent)),
                    pyarrow.scalar(int(Direction.RECV), pyarrow.int32()),
                    unknown,
                ),
            )
            found = compute.if_else(selected, direction, found)
        return found

    def _anchors(self) -> dict[Protocol, str]:
        """`{protocol: where its payload starts}` for every structured rule.

        Every rule the protocol answers to, not `rule(protocol)`'s first: two
        rules may share a protocol under different codecs, and a verb checked
        against the wrong vocabulary would answer from anywhere.
        """
        found: dict[Protocol, list[str]] = {}
        for rule in self.rules:
            anchor = CODEC_ANCHORS.get(rule.codec)
            if anchor is not None and anchor not in found.setdefault(rule.protocol, []):
                found[rule.protocol].append(anchor)
        return {
            protocol: joined_pattern(*anchors) for protocol, anchors in found.items() if anchors
        }

    def into_arrow_category_array(self, protocols: Any, eventtypes: Any) -> pyarrow.Array:
        """Target category per parsed row, using the scalar rule in kernels."""
        compute = pyarrow.compute
        rows = len(protocols)
        if len(eventtypes) != rows:
            raise ValueError("protocol and eventtype columns must have the same length")
        if not rows:
            return pyarrow.array([], pyarrow.string())

        event_codes = compute.fill_null(eventtypes.cast(pyarrow.int64(), safe=False), 0)
        market = compute.fill_null(
            compute.is_in(
                event_codes,
                value_set=pyarrow.array(
                    sorted(EventType.ranked_at_least(EventType.INTENT)), pyarrow.int64()
                ),
            ),
            False,
        )
        known = compute.fill_null(
            compute.is_in(
                Protocol.into_family_arrow(protocols),
                value_set=pyarrow.array(
                    [protocol.into_stored() for protocol in sorted(self.protocols)],
                    _PROTOCOL_CODE,
                ),
            ),
            False,
        )
        known = compute.or_(known, compute.equal(event_codes, int(EventType.MISC)))
        non_market = compute.if_else(
            known,
            pyarrow.scalar(MISC_CATEGORY),
            pyarrow.scalar(UNKNOWN_CATEGORY),
        )
        return compute.if_else(market, pyarrow.scalar(MARKET_CATEGORY), non_market).cast(
            pyarrow.string(), safe=False
        )


def _opens(verb_at: Any, payload_at: Any) -> Any:
    """Whether a found verb starts before the row's first payload token.

    No payload token is no anchor, not an open door: a row can carry a
    protocol whose patterns never matched its text -- the message stage's
    stored reading rescued it -- and without an anchor a verb could sit
    anywhere in the payload. No answer beats a guessed one.
    """
    compute = pyarrow.compute
    return compute.and_(
        compute.fill_null(compute.greater_equal(verb_at, 0), False),
        compute.fill_null(
            compute.and_(compute.greater_equal(payload_at, 0), compute.less(verb_at, payload_at)),
            False,
        ),
    )


def payload_shapes(entries: Any) -> pyarrow.Array:
    """Which of `SHAPES` each row's parsed pairs make, or nothing.

    The keys decide and the values never do: `Entry.tag` is the numeric
    identity a key was written under, and a key that is not a number carries
    none. So a `#A=1` quoted inside a `Text <58>` value is one entry's text
    rather than a second entry, and a value full of digits stays a value.
    """
    compute = pyarrow.compute
    rows = len(entries)
    empty = pyarrow.scalar("", pyarrow.string())
    if not rows:
        return pyarrow.array([], pyarrow.string())
    if isinstance(entries, pyarrow.ChunkedArray):
        entries = entries.combine_chunks()
    items = compute.list_flatten(entries)
    tags = compute.struct_field(items, "tag")
    parents = compute.list_parent_indices(entries).cast(pyarrow.int64())
    at = sequence(rows)
    # `XmlData <213>` holds a whole message where it holds one, and the FIX
    # stage expands it in the place the tag sat -- so its named keys are the
    # message's own, and a numbered frame carrying one is mixed, not wire.
    carried = compute.and_(
        compute.equal(tags, XML_DATA_TAG),
        carries_message(compute.struct_field(items, "value")),
    )
    numbered, named = (
        compute.is_in(at, value_set=compute.unique(compute.filter(parents, mask)))
        for mask in (
            compute.not_equal(tags, 0),
            compute.or_(compute.equal(tags, 0), carried),
        )
    )
    return compute.if_else(
        compute.and_(numbered, named),
        pyarrow.scalar("fixml", pyarrow.string()),
        compute.if_else(
            numbered,
            pyarrow.scalar("fix", pyarrow.string()),
            compute.if_else(named, pyarrow.scalar("ul", pyarrow.string()), empty),
        ),
    ).cast(pyarrow.string(), safe=False)


def _hit(rule: Rule, text: Any, plugins: Any, shapes: Any) -> Any:
    """One rule's mask over a whole column.

    A rule matches by what it declares: its message pattern and its plugin
    pattern where it has them, and otherwise by the shape its codec names. So
    the built-ins are decided by the keys a payload holds, and a desk that
    writes `plugin_pattern` decides by the plugin instead.
    """
    compute = pyarrow.compute
    mask = compute.is_valid(text)
    if rule.codec in SHAPES and not rule.pattern and not rule.plugin_pattern:
        mask = compute.and_(mask, compute.equal(shapes, rule.codec))
    if rule.pattern:
        mask = compute.and_(
            mask, compute.fill_null(compute.match_substring_regex(text, rule.pattern), False)
        )
    if rule.plugin_pattern:
        if plugins is None:
            return pyarrow.repeat(False, len(text))
        matched = compute.fill_null(
            compute.match_substring_regex(plugins, rule.plugin_pattern), False
        )
        mask = compute.and_(mask, matched)
    return mask
