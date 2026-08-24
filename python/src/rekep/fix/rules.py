"""Protocol classification, parsing rules, and log target categories."""

from __future__ import annotations

import dataclasses
import functools
import re
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.enums import EventType
from rekep.fields import scalar
from rekep.fix.message import BEGIN_STRING, BRIDGE, BRIDGE_WIRE

#: Read wire tags, rendered names, or no message.
CODECS: tuple[str, ...] = ("fix", "ul", "none")

#: `codec` -> `parse_arrow_array`'s named mode; None skips parsing. Named for
#: what it maps rather than for the word it maps to: three unrelated `NAMED`
#: constants meant three things, and an import of one read as an import of
#: another.
CODEC_KEYS: dict[str, bool | None] = {"fix": False, "ul": True, "none": None}

#: Fall-through protocol for a line no configured rule recognizes.
NO_PROTOCOL = "OTHER"

#: Parsed-log target categories. Market rows share one table; known operational
#: traffic stays separate from lines whose transport is not recognised.
MARKET_CATEGORY = "market"
MISC_CATEGORY = "misc"
UNKNOWN_CATEGORY = "unknown"


@scalar
class Rule(Convertible):
    """A protocol rule whose regexes must work in Python `re` and Arrow RE2."""

    protocol: str = NO_PROTOCOL
    """What a line matching this rule carries, as the `protocol` column holds it."""

    pattern: str = ""
    """First message regex; empty with no `patterns` matches every line."""

    plugin_pattern: str | None = None
    """Matched against `plugin_code` as well, when the plugin is what tells them apart."""

    separator: str | None = None
    """What the message writes between fields; null detects it per column."""

    entry_separator: str | None = None
    """What one indexed token writes between the members of a group entry; null detects it."""

    codec: str = "none"
    """How to read the line: `fix`, `ul`, or `none` for "do not"."""

    patterns: list[str] = dataclasses.field(default_factory=list)
    """Additional message regexes; matching any one satisfies the rule."""

    def __post_init__(self) -> None:
        """Keep direct string input as one pattern, never its characters."""
        if isinstance(self.patterns, str):
            self.patterns = [self.patterns]

    @property
    def named(self) -> bool | None:
        """What `parse_arrow_array`'s `named` is for this rule; None is "no message"."""
        return CODEC_KEYS.get(self.codec)

    def matches(self, message: str | None, plugin: str | None = None) -> bool:
        """Whether one line matches; unavailable message or plugin data does not."""
        if message is None:
            return False
        patterns = self.message_patterns
        if patterns and not any(_compiled(pattern).search(message) for pattern in patterns):
            return False
        if self.plugin_pattern:
            if plugin is None or _compiled(self.plugin_pattern).search(plugin) is None:
                return False
        return True

    @property
    def message_patterns(self) -> tuple[str, ...]:
        """All nonempty message patterns, in declaration order."""
        return tuple(filter(None, (self.pattern, *self.patterns)))


#: Use parser-owned patterns so classification and parsing cannot drift.
FIX = Rule(protocol="FIX", pattern=BEGIN_STRING, codec="fix")

UL = Rule(protocol="UL", pattern=BRIDGE, codec="ul")

#: More specific than FIX, so this must precede `FIX`.
UL_WIRE = Rule(protocol="UL", pattern=BRIDGE_WIRE, codec="ul")

#: Operational lines whose vocabulary is understood but which carry no market
#: message. Keeping these known lines out of `unknown` makes that table a
#: useful signal that a genuinely new log format arrived.
MISC = Rule(
    protocol="MISC",
    patterns=[
        r"(?i)\bheartbeat\b",
        r"(?i)\b(?:connect(?:ed|ion)?|disconnect(?:ed|ion)?|reconnect(?:ed|ion)?)\b",
        r"(?i)\b(?:logon|logout|timeout|retry)\b",
    ],
    codec="none",
)

#: Empty patterns make this the final fall-through rule.
OTHER = Rule(protocol=NO_PROTOCOL, pattern="", codec="none")

#: First match wins; wrapped UL must precede its FIX envelope.
DEFAULT_RULES: tuple[Rule, ...] = (UL_WIRE, FIX, UL, MISC, OTHER)


def _default_rules() -> list[Rule]:
    """Fresh default rules, including their mutable pattern lists."""
    return [dataclasses.replace(rule, patterns=list(rule.patterns)) for rule in DEFAULT_RULES]


@dataclasses.dataclass
class Rules(Convertible):
    """Which protocol each line carries, by the first pattern that matches."""

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
        if etype is not None and int(etype) != int(EventType.UNKNOWN):
            return MARKET_CATEGORY
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

    def into_arrow_category_array(self, protocols: Any, etypes: Any) -> pyarrow.Array:
        """Target category per parsed row, using the scalar rule in kernels."""
        compute = pyarrow.compute
        rows = len(protocols)
        if len(etypes) != rows:
            raise ValueError("protocol and etype columns must have the same length")
        if not rows:
            return pyarrow.array([], pyarrow.string())

        event_codes = compute.fill_null(etypes.cast(pyarrow.int32(), safe=False), 0)
        market = compute.not_equal(event_codes, int(EventType.UNKNOWN))
        known = compute.fill_null(
            compute.is_in(
                protocols.cast(pyarrow.string(), safe=False),
                value_set=pyarrow.array(sorted(self.protocols), pyarrow.string()),
            ),
            False,
        )
        non_market = compute.if_else(
            known,
            pyarrow.scalar(MISC_CATEGORY),
            pyarrow.scalar(UNKNOWN_CATEGORY),
        )
        return compute.if_else(market, pyarrow.scalar(MARKET_CATEGORY), non_market).cast(
            pyarrow.string(), safe=False
        )


def _hit(rule: Rule, text: Any, plugins: Any) -> Any:
    """One rule's mask over a whole column."""
    compute = pyarrow.compute
    message_mask = None
    for pattern in rule.message_patterns:
        matched = compute.fill_null(compute.match_substring_regex(text, pattern), False)
        message_mask = matched if message_mask is None else compute.or_(message_mask, matched)
    mask = compute.is_valid(text)
    if message_mask is not None:
        mask = compute.and_(mask, message_mask)
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
