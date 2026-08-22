"""What kind of message a log line carries, and how to read it.

Which is FIX knowledge and not log knowledge, which is why it lives here: the
patterns that say "this line is a wire message" and "this line is a bridge
message" are the same two patterns the parser cuts a line with
(`BEGIN_STRING`, `BRIDGE`), and a second copy of either in `rekep.logs` would
be a second answer to one question.
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
from rekep.fix.message import BEGIN_STRING, BRIDGE

#: What a `codec` may say, and what each means to the parser: read the line as
#: wire tags, read it as rendered names, or do not read it at all. Three and
#: not two, because "this line is not a message" is an answer -- 60% of a
#: capture, and the whole reason a rule set runs before the parser does
#: (`benchmarks/bench_text_file.py --only messages`).
CODECS: tuple[str, ...] = ("fix", "ul", "none")

#: `codec` -> what `parse_arrow_array`'s `named` is for it. None is "nothing to
#: parse", which is not the same as either of the other two and must not be
#: spelled as one.
NAMED: dict[str, bool | None] = {"fix": False, "ul": True, "none": None}

#: The Arrow type a `category_id` column is -- the same `int32` every other
#: code here is, so a filter on it is one comparison in one type.
CODE: pyarrow.DataType = pyarrow.int32()


@field
class Rule(Convertible):
    """One pattern, and what a line matching it is.

    A declaration and nothing else, so a desk whose bridge spells its messages
    differently writes a document rather than patching this package -- and the
    document sits beside the schema contracts, loaded the same way
    (`Rules.from_yaml`).

    **A pattern is run by two engines**, Python's `re` on one line and RE2 over
    a whole column, and they are contracted to agree -- so a pattern here has
    to be spellable in both: no lookbehind, no lookahead, no backreference.
    The built-ins are the parser's own constants for exactly that reason.
    """

    name: str = "OTHER"
    """What the category is called, as the `category_name` column holds it."""

    # An integer and not the name, for the same reason every other code here is
    # one: the column survives a rule set this build has never seen, and a
    # filter on it prunes where a set of string literals cannot.
    category_id: int = 0
    """Which category a line matching this rule is, as `category_id` holds it."""

    pattern: str = ""
    """Regular expression matched anywhere in the message; empty matches every line."""

    driver_pattern: str | None = None
    """Matched against `driver_name` as well, when the driver is what tells them apart."""

    separator: str | None = None
    """What the message writes between fields; null detects it per column."""

    entry_separator: str | None = None
    """What one indexed token writes between the members of a group entry; null detects it."""

    codec: str = "none"
    """How to read a line of this category: `fix`, `ul`, or `none` for "do not"."""

    fix_version: str | None = None
    """Which FIX version to resolve names against when the message carries no BeginString."""

    @property
    def named(self) -> bool | None:
        """What `parse_arrow_array`'s `named` is for this rule; None is "no message"."""
        return NAMED.get(self.codec)

    def matches(self, message: str | None, driver: str | None = None) -> bool:
        """Whether one line is this category.

        The scalar twin of one column of `into_arrow_category_arrays`. A rule
        naming a driver and handed none does not match: a rule that cannot be
        evaluated is not a rule that matched.
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
FIX = Rule(
    name="FIX",
    category_id=1,
    pattern=BEGIN_STRING,
    codec="fix",
)

#: A UL bridge message: two or more `#NAME=` tokens. Two, because a lone
#: `#FOO=bar` in prose is a sentence -- again the parser's own constant.
UL = Rule(
    name="UL",
    category_id=2,
    pattern=BRIDGE,
    codec="ul",
)

#: Everything else, which is most of a capture. An empty pattern matches every
#: line, so this is the fall-through *as a rule*: last in the list, and the
#: answer `categorise` gives when a custom set runs out without matching.
OTHER = Rule(name="OTHER", category_id=0, pattern="", codec="none")

#: The three built-ins, in the order they are tried.
DEFAULT_RULES: tuple[Rule, ...] = (FIX, UL, OTHER)


@dataclasses.dataclass
class Rules(Convertible):
    """Which category each line of a log is, by the first pattern that matches.

    A list of rules and nothing else, so the whole thing is configuration:
    `Rules.from_yaml("rules.yml")` reads one, and it travels in a task document
    with the rest of the job.

    **The first match wins, and no match is OTHER.** An ordered list is what
    lets a specific rule sit in front of a general one without either having to
    know about the other, and a line nothing matches is still a line -- parsed
    as nothing, stored, keyed and partitioned like every other, under a
    category that says plainly that it carries no message. Dropping it, or
    guessing, is how a log stops being a record of what happened.

    Vectorised, the matching is one Arrow kernel per rule over the whole
    column, so the cost is a handful of passes per batch rather than anything
    per row.
    """

    #: The rule set a FIX-carrying trading log reads under: wire messages,
    #: bridge messages, everything else. Assigned below the class, because it
    #: is an instance of it.
    DEFAULT: ClassVar[Rules]

    #: Rules in the order they are tried.
    rules: list[Rule] = dataclasses.field(default_factory=lambda: list(DEFAULT_RULES))

    def categorise(self, message: str | None, driver: str | None = None) -> Rule:
        """The first rule `message` matches, or `OTHER`."""
        for rule in self.rules:
            if rule.matches(message, driver):
                return rule
        return OTHER

    def rule(self, category_id: int) -> Rule:
        """The rule a `category_id` names, or `OTHER` when this set has no such id.

        What the pipeline reads a *slice* of a batch back with: the batch
        carries ids, and how to parse a slice is a property of the rule the id
        came from.
        """
        for rule in self.rules:
            if rule.category_id == category_id:
                return rule
        return OTHER

    def into_arrow_category_arrays(
        self, messages: Any, drivers: Any = None
    ) -> tuple[pyarrow.Array, pyarrow.Array]:
        """One `(category_id, category_name)` pair per row, in kernels.

        Applied **in reverse**, each rule overwriting what the ones after it
        decided, so the earliest rule in the list is the one that survives.
        That is the whole of "first match wins", and it is one pass per rule
        rather than a scan per row.

        A rule with an empty pattern is the fall-through and costs no kernel:
        it matches every row, which is what the arrays already hold. A rule
        naming a driver where no driver column was handed over is skipped, for
        `Rule.matches`'s reason -- it cannot be evaluated, so it did not match.

        A null message matches nothing rather than propagating: a line with no
        payload carries no message, which OTHER already says, and letting the
        null through would put one in a NOT NULL column.
        """
        compute = pyarrow.compute
        rows = len(messages)
        ids: Any = pyarrow.repeat(pyarrow.scalar(OTHER.category_id, CODE), rows)
        names: Any = pyarrow.repeat(pyarrow.scalar(OTHER.name, pyarrow.string()), rows)
        if not rows:
            return ids, names
        text = messages.cast(pyarrow.string(), safe=False)
        driver_text = None if drivers is None else drivers.cast(pyarrow.string(), safe=False)
        for rule in reversed(self.rules):
            hit = _hit(rule, text, driver_text)
            if hit is None:
                continue
            ids = compute.if_else(hit, pyarrow.scalar(rule.category_id, CODE), ids)
            names = compute.if_else(hit, pyarrow.scalar(rule.name, pyarrow.string()), names)
        return ids.cast(CODE, safe=False), names.cast(pyarrow.string(), safe=False)


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
