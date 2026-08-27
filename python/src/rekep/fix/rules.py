"""Protocol classification, parsing rules, and log target categories."""

from __future__ import annotations

import dataclasses
import functools
import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.enums import EventType
from rekep.fields import scalar
from rekep.fix.message import BEGIN_STRING, BRIDGE, BRIDGE_WIRE, WIRE_MSG_TYPE

#: Read wire tags, rendered names, or no message.
CODECS: tuple[str, ...] = ("fix", "ul", "none")

#: `codec` -> `parse_arrow_array`'s named mode; None skips parsing. Named for
#: what it maps rather than for the word it maps to: three unrelated `NAMED`
#: constants meant three things, and an import of one read as an import of
#: another.
CODEC_KEYS: dict[str, bool | None] = {"fix": False, "ul": True, "none": None}

#: Fall-through protocol for a line no configured rule recognizes.
NO_PROTOCOL = "OTHER"

#: How a bridge says which way a payload moved -- `Receiving : 8=FIX...`,
#: `Sending : ...`, `Message received:` -- counted only where the verb opens
#: the line before the payload's own first token, so prose inside a FIX
#: `Text <58>` or a bridge value never answers. Measured on real capture:
#: every FIX row carries one of these; most UL re-log lines carry none, so
#: direction is best-effort there. `incoming`/`outgoing`/`forward`, bare
#: `IN`/`OUT` markers and the session-name fields were all investigated on
#: the same capture and ruled out -- each mislabels enrichment snapshots,
#: Jolokia metadata or the route's fixed endpoints as movement.
INBOUND_PATTERN = r"(?i)\b(?:receiv(?:ing|ed))\b"
OUTBOUND_PATTERN = r"(?i)\b(?:send(?:ing)?|sent)\b"

#: Which protocols carry a direction verb, and the two patterns that read it.
#: A protocol-keyed lookup beside the rules rather than two more `Rule`
#: fields: direction is consulted *after* classification, never folded into
#: it, and a bridge with different wording passes its own mapping to
#: `Rules.into_arrow_direction_array`.
DIRECTION_PATTERNS: Mapping[str, tuple[str, str]] = MappingProxyType(
    {
        "FIX": (INBOUND_PATTERN, OUTBOUND_PATTERN),
        "UL": (INBOUND_PATTERN, OUTBOUND_PATTERN),
    }
)

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


@scalar
class Rule(Convertible):
    """A protocol rule whose regex must work in Python `re` and Arrow RE2."""

    protocol: str = NO_PROTOCOL
    """What a line matching this rule carries, as the `protocol` column holds it."""

    pattern: str = ""
    """The message regex; alternatives join with `|` (see `joined_pattern`).
    Empty matches every line, which is what makes a fall-through rule."""

    plugin_pattern: str | None = None
    """Matched against `plugin_code` as well, when the plugin is what tells them apart."""

    separator: str | None = None
    """What the message writes between fields; null detects it per column."""

    entry_separator: str | None = None
    """What one indexed token writes between the members of a group entry; null detects it."""

    extra_entry_separators: tuple[str, ...] = ()
    """Additional literals considered when an indexed-entry separator is detected."""

    codec: str = "none"
    """How to read the line: `fix`, `ul`, or `none` for "do not"."""

    def __post_init__(self) -> None:
        """Keep direct string input as one literal, never its characters."""
        if isinstance(self.extra_entry_separators, str):
            self.extra_entry_separators = (self.extra_entry_separators,)
        else:
            self.extra_entry_separators = tuple(self.extra_entry_separators)

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Rule:
        """Read a rule document, folding the retired `patterns` list.

        A rule used to carry additional regexes in a `patterns` list beside
        `pattern`. The two spellings collapsed into the one alternation, and a
        stored document from that shape must keep classifying the same lines
        rather than silently losing every pattern past the first.
        """
        spelled = dict(mapping)
        legacy = spelled.pop("patterns", None)
        if legacy:
            plural = [legacy] if isinstance(legacy, str) else list(legacy)
            spelled["pattern"] = joined_pattern(str(spelled.get("pattern") or ""), *plural)
        return super().from_dict(spelled)

    @property
    def named(self) -> bool | None:
        """What `parse_arrow_array`'s `named` is for this rule; None is "no message"."""
        return CODEC_KEYS.get(self.codec)

    def matches(self, message: str | None, plugin: str | None = None) -> bool:
        """Whether one line matches; unavailable message or plugin data does not."""
        if message is None:
            return False
        if self.pattern and _compiled(self.pattern).search(message) is None:
            return False
        if self.plugin_pattern:
            if plugin is None or _compiled(self.plugin_pattern).search(plugin) is None:
                return False
        return True


#: Use parser-owned patterns so classification and parsing cannot drift.
FIX = Rule(protocol="FIX", pattern=joined_pattern(BEGIN_STRING, WIRE_MSG_TYPE), codec="fix")

UL = Rule(protocol="UL", pattern=BRIDGE, codec="ul")

#: More specific than FIX, so this must precede `FIX`. Zero hits across the
#: 292,750-row three-log sample this set was last validated on -- kept anyway,
#: by construction rather than by evidence: its envelope is FIX-shaped, so
#: without it a wrapped bridge message would parse under the wire codec.
UL_WIRE = Rule(protocol="UL", pattern=BRIDGE_WIRE, codec="ul")

#: Operational lines whose vocabulary is understood but which carry no market
#: message. Keeping these known lines out of `unknown` makes that table a
#: useful signal that a genuinely new log format arrived.
MISC = Rule(
    protocol="MISC",
    pattern=joined_pattern(
        r"(?i)\bheartbeat\b",
        r"(?i)\b(?:connect(?:ed|ion)?|disconnect(?:ed|ion)?|reconnect(?:ed|ion)?)\b",
        r"(?i)\b(?:logon|logout|timeout|retry)\b",
    ),
    codec="none",
)

#: An empty pattern makes this the final fall-through rule.
OTHER = Rule(protocol=NO_PROTOCOL, pattern="", codec="none")

#: First match wins; wrapped UL must precede its FIX envelope.
DEFAULT_RULES: tuple[Rule, ...] = (UL_WIRE, FIX, UL, MISC, OTHER)


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

    def categorise(self, message: str | None, plugin: str | None = None) -> Rule:
        """The first rule `message` matches, or `OTHER`."""
        for rule in self.rules:
            if rule.matches(message, plugin):
                return rule
        return OTHER

    def rule(self, protocol: str) -> Rule:
        """The first rule for one protocol, or `OTHER` when this set has none.

        What the pipeline reads a *slice* of a batch back with: the batch
        carries protocol names, and how to parse a slice is a property of the
        rule that named it.
        """
        for rule in self.rules:
            if rule.protocol == protocol:
                return rule
        return OTHER

    def category_of(self, protocol: str | None, etype: int | EventType | None) -> str:
        """Target category for one parsed row."""
        kind = EventType.from_int(etype) if etype is not None else None
        if kind is not None and kind.rank >= EventType.INTENT.rank:
            return MARKET_CATEGORY
        if kind is EventType.MISC:
            return MISC_CATEGORY
        if protocol in self.protocols:
            return MISC_CATEGORY
        return UNKNOWN_CATEGORY

    @property
    def protocols(self) -> frozenset[str]:
        """Recognised protocol names, excluding the fall-through value."""
        return frozenset(rule.protocol for rule in self.rules if rule.protocol != NO_PROTOCOL)

    def into_arrow_protocol_array(self, messages: Any, plugins: Any = None) -> pyarrow.Array:
        """What each row carries, in kernels: one `protocol` name per line."""
        compute = pyarrow.compute
        rows = len(messages)
        found: Any = pyarrow.repeat(pyarrow.scalar(OTHER.protocol, pyarrow.string()), rows)
        if not rows:
            return found
        text = messages.cast(pyarrow.string(), safe=False)
        plugin_text = None if plugins is None else plugins.cast(pyarrow.string(), safe=False)
        for rule in reversed(self.rules):
            hit = _hit(rule, text, plugin_text)
            if hit is None:
                continue
            found = compute.if_else(hit, pyarrow.scalar(rule.protocol, pyarrow.string()), found)
        return found.cast(pyarrow.string(), safe=False)

    def into_arrow_direction_array(
        self,
        messages: Any,
        protocols: Any,
        patterns: Mapping[str, tuple[str, str]] | None = None,
    ) -> pyarrow.Array:
        """True sent, False received, null undirected -- read before the payload.

        The verb counts only where it starts before the first token the row's
        own rule matched, so a `sent` inside a FIX `Text <58>` or a bridge
        value never becomes a direction. Neither matching is null, and so is
        both: no answer beats a guessed one. Protocols outside `patterns` --
        `DIRECTION_PATTERNS` unless a bridge hands its own wording -- stay
        null whole.
        """
        compute = pyarrow.compute
        rows = len(messages)
        found: Any = pyarrow.nulls(rows, pyarrow.bool_())
        if not rows:
            return found
        configured = DIRECTION_PATTERNS if patterns is None else patterns
        text = compute.fill_null(messages.cast(pyarrow.string(), safe=False), "")
        names = protocols.cast(pyarrow.string(), safe=False)
        for protocol, (inbound, outbound) in configured.items():
            selected = compute.fill_null(compute.equal(names, protocol), False)
            if not compute.any(selected, min_count=0).as_py():
                continue
            # Every rule the protocol answers to, not `rule(protocol)`'s first:
            # a rendered bridge line matches `UL`, never `UL_WIRE`, and a verb
            # checked against the wrong vocabulary would answer from anywhere.
            spelled = joined_pattern(
                *dict.fromkeys(rule.pattern for rule in self.rules if rule.protocol == protocol)
            )
            if not spelled:
                continue
            payload_at = compute.find_substring_regex(text, pattern=spelled)
            received = _opens(compute.find_substring_regex(text, pattern=inbound), payload_at)
            sent = _opens(compute.find_substring_regex(text, pattern=outbound), payload_at)
            direction = compute.if_else(
                compute.and_(sent, compute.invert(received)),
                pyarrow.scalar(True),
                compute.if_else(
                    compute.and_(received, compute.invert(sent)),
                    pyarrow.scalar(False),
                    pyarrow.scalar(None, pyarrow.bool_()),
                ),
            )
            found = compute.if_else(selected, direction, found)
        return found

    def into_arrow_category_array(self, protocols: Any, etypes: Any) -> pyarrow.Array:
        """Target category per parsed row, using the scalar rule in kernels."""
        compute = pyarrow.compute
        rows = len(protocols)
        if len(etypes) != rows:
            raise ValueError("protocol and etype columns must have the same length")
        if not rows:
            return pyarrow.array([], pyarrow.string())

        event_codes = compute.fill_null(etypes.cast(pyarrow.int64(), safe=False), 0)
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
                protocols.cast(pyarrow.string(), safe=False),
                value_set=pyarrow.array(sorted(self.protocols), pyarrow.string()),
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


def _hit(rule: Rule, text: Any, plugins: Any) -> Any:
    """One rule's mask over a whole column."""
    compute = pyarrow.compute
    mask = compute.is_valid(text)
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


@functools.lru_cache(maxsize=256)
def _compiled(pattern: str) -> re.Pattern[str]:
    """Compile once with ASCII classes, matching Arrow RE2 semantics."""
    return re.compile(pattern, re.ASCII)
