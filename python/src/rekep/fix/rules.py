"""Which protocol a log line carries, and how to read it.

FIX knowledge, not log knowledge: the patterns that say "this is a wire
message" and "this is a bridge message" are the parser's own (`BEGIN_STRING`,
`BRIDGE`), and a second copy of either in `rekep.text` would be a second answer
to one question.
"""

from __future__ import annotations

import dataclasses
import functools
import re
from typing import Any, ClassVar

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import field
from rekep.fix.message import BEGIN_STRING, BRIDGE, BRIDGE_WIRE

#: Read the line as wire tags, as rendered names, or not at all. Three and not
#: two: "this is not a message" is an answer, and it is 60% of a capture --
#: which is why a rule set runs before the parser does
#: (`benchmarks/bench_text_file.py --only messages`).
CODECS: tuple[str, ...] = ("fix", "ul", "none")

#: `codec` -> `parse_arrow_array`'s `named`. None is "nothing to parse", which
#: is not either of the other two and must not be spelled as one.
NAMED: dict[str, bool | None] = {"fix": False, "ul": True, "none": None}

#: What a line carrying nothing this rule set knows is. The fall-through, and
#: the value the `protocol` column holds for most of a capture.
NO_PROTOCOL = "OTHER"


@field
class Rule(Convertible):
    """One pattern, and what a line matching it carries.

    A declaration and nothing else, so a desk whose bridge spells its messages
    differently writes a document rather than patching this package.

    **A pattern is run by two engines** -- `re` on one line, RE2 over a whole
    column -- and they are contracted to agree, so it has to be spellable in
    both: no lookbehind, no lookahead, no backreference. The built-ins are the
    parser's own constants for that reason.
    """

    protocol: str = NO_PROTOCOL
    """What a line matching this rule carries, as the `protocol` column holds it."""

    pattern: str = ""
    """Regular expression matched anywhere in the message; empty matches every line."""

    driver_pattern: str | None = None
    """Matched against `driver_name` as well, when the driver is what tells them apart."""

    separator: str | None = None
    """What the message writes between fields; null detects it per column."""

    entry_separator: str | None = None
    """What one indexed token writes between the members of a group entry; null detects it."""

    codec: str = "none"
    """How to read the line: `fix`, `ul`, or `none` for "do not"."""

    fix_version: str | None = None
    """Which FIX version to resolve names against when the message carries no BeginString."""

    @property
    def named(self) -> bool | None:
        """What `parse_arrow_array`'s `named` is for this rule; None is "no message"."""
        return NAMED.get(self.codec)

    def matches(self, message: str | None, driver: str | None = None) -> bool:
        """Whether one line is this rule's.

        The scalar twin of `into_arrow_protocol_array`. A rule naming a driver
        and handed none does not match: a rule that cannot be evaluated is not
        a rule that matched.
        """
        if message is None:
            return False
        if self.pattern and _compiled(self.pattern).search(message) is None:
            return False
        if self.driver_pattern:
            if driver is None or _compiled(self.driver_pattern).search(driver) is None:
                return False
        return True


#: A wire FIX message: a BeginString anywhere in the line. The parser's own
#: constant, so "what makes this a FIX message" and "where does the message
#: start" can never drift apart.
FIX = Rule(protocol="FIX", pattern=BEGIN_STRING, codec="fix")

#: A UL bridge message: two or more `#NAME=` tokens. Two, because a lone
#: `#FOO=bar` in prose is a sentence -- again the parser's own constant.
UL = Rule(protocol="UL", pattern=BRIDGE, codec="ul")

#: The same message inside a FIX envelope: `8=FIX.4.2|35=UL|#A=1|#B=2`. It
#: answers to the FIX tell too, so it sits **in front of** the FIX rule -- read
#: as a wire message, every named field in it is noise. Same protocol as any
#: other bridge message, and the wire header is not lost: the named codec
#: admits a numeric key, and the message still starts at its BeginString.
UL_WIRE = Rule(protocol="UL", pattern=BRIDGE_WIRE, codec="ul")

#: Everything else, which is most of a capture. An empty pattern matches every
#: line, so this is the fall-through *as a rule*: last in the list, and the
#: answer a custom set that runs out without matching gives.
OTHER = Rule(protocol=NO_PROTOCOL, pattern="", codec="none")

#: The built-ins, in the order they are tried. The wrapped bridge message
#: leads, because it is the only one that answers to two tells and the more
#: specific reading has to get there first.
DEFAULT_RULES: tuple[Rule, ...] = (UL_WIRE, FIX, UL, OTHER)


@dataclasses.dataclass
class Rules(Convertible):
    """Which protocol each line carries, by the first pattern that matches.

    A list of rules and nothing else, so the whole thing is configuration:
    `Rules.from_yaml("rules.yml")` reads one, and it travels in a task document
    with the rest of the job.

    **First match wins, no match is OTHER.** An ordered list lets a specific
    rule sit in front of a general one without either knowing about the other,
    and a line nothing matches is still a line -- parsed as nothing, stored,
    keyed and partitioned like every other. Dropping it, or guessing, is how a
    log stops being a record of what happened.

    One Arrow kernel per rule over the whole column: a handful of passes per
    batch, and nothing per row.
    """

    #: What a FIX-carrying trading log reads under. Assigned below the class,
    #: because it is an instance of it.
    DEFAULT: ClassVar[Rules]

    #: Rules in the order they are tried.
    rules: list[Rule] = dataclasses.field(default_factory=lambda: list(DEFAULT_RULES))

    def categorise(self, message: str | None, driver: str | None = None) -> Rule:
        """The first rule `message` matches, or `OTHER`."""
        for rule in self.rules:
            if rule.matches(message, driver):
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

    def into_arrow_protocol_array(self, messages: Any, drivers: Any = None) -> pyarrow.Array:
        """What each row carries, in kernels: one `protocol` name per line.

        Applied **in reverse**, each rule overwriting what the ones after it
        decided, so the earliest surviving rule is the first match -- one pass
        per rule rather than a scan per row.

        A rule with an empty pattern costs no kernel: it matches every row,
        which is what the array already holds. A rule naming a driver where no
        driver column was handed over is skipped, for `Rule.matches`'s reason.

        A null message matches nothing rather than propagating: a line with no
        payload carries none, which OTHER already says, and the null would land
        in a NOT NULL column.
        """
        compute = pyarrow.compute
        rows = len(messages)
        found: Any = pyarrow.repeat(pyarrow.scalar(OTHER.protocol, pyarrow.string()), rows)
        if not rows:
            return found
        text = messages.cast(pyarrow.string(), safe=False)
        driver_text = None if drivers is None else drivers.cast(pyarrow.string(), safe=False)
        for rule in reversed(self.rules):
            hit = _hit(rule, text, driver_text)
            if hit is None:
                continue
            found = compute.if_else(hit, pyarrow.scalar(rule.protocol, pyarrow.string()), found)
        return found.cast(pyarrow.string(), safe=False)


Rules.DEFAULT = Rules()


def _hit(rule: Rule, text: Any, drivers: Any) -> Any:
    """One rule's mask over a whole column, or None where it costs nothing."""
    compute = pyarrow.compute
    mask = None
    if rule.pattern:
        mask = compute.fill_null(compute.match_substring_regex(text, rule.pattern), False)
    if rule.driver_pattern:
        if drivers is None:
            return None
        matched = compute.fill_null(
            compute.match_substring_regex(drivers, rule.driver_pattern), False
        )
        mask = matched if mask is None else compute.and_(mask, matched)
    return mask


@functools.lru_cache(maxsize=256)
def _compiled(pattern: str) -> re.Pattern[str]:
    """One rule's pattern, compiled once however many lines it is run over.

    `re.ASCII` for the reason every pattern in `rekep.fix` carries it: the
    vectorised twin runs under RE2, whose classes are ASCII-only, and the two
    readings of a rule are contracted to agree.
    """
    return re.compile(pattern, re.DOTALL | re.ASCII)
