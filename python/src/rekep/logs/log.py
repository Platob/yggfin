"""The shape of one parsed log line, and what decides which event it is."""

from __future__ import annotations

import dataclasses
from typing import Any, ClassVar

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import field
from rekep.market.enums import EventType
from rekep.market.event import Event


@field
class Log(Event):
    """One parsed line of a trading log.

    An `Event` like everything else this package stores, which is what lets a
    parsed log be read beside the orders and books it describes rather than
    beside nothing: `unix` is the instant the line is stamped with, `hash` is
    the digest of the raw line -- so the same capture read twice deduplicates
    itself -- and `etype` is what the line is *about*, decided by `LogRules`.

    `xhash` is the line's own `hash`: a log line is one version of one thing
    and never changes, so its lifecycle is itself. The rest of the envelope --
    `version`, `state`, the previous-version columns -- is constant here and
    costs nothing on disk, where a column of one repeated value encodes away.
    """

    url: str = ""
    """Path of the log the line came from, as its filesystem addresses it."""

    thread_name: str = ""
    """Contents of the first bracketed field."""

    driver_name: str = ""
    """Contents of the second bracketed field -- the emitting module."""

    message: str = ""
    """Payload with the header and level stripped, continuation lines folded in."""


@dataclasses.dataclass
class LogRule(Convertible):
    """One pattern, and the kind of event a line matching it is."""

    pattern: str = ""
    """RE2 regular expression, matched anywhere in the message."""

    etype: EventType = EventType.UNKNOWN
    """What a line matching `pattern` is; readable by name in a configuration."""

    label: str = ""
    """What the rule is for, when the pattern does not say it plainly."""


#: What a FIX-carrying trading log is made of, by the two spellings every one
#: of them uses: the wire `35=` message type, and the name a rendered log
#: prints. Ordered most specific first, because the first match wins and a
#: single line can name more than one of them -- an execution report quoting
#: the order it fills says `ExecutionReport` *and* `NewOrderSingle`.
DEFAULT_RULES: tuple[LogRule, ...] = (
    LogRule(r"35=8(\D|$)|ExecutionReport", EventType.EXECUTION, "a fill, or a report of one"),
    LogRule(
        r"35=[DFG](\D|$)|NewOrderSingle|OrderCancel(Request|Replace)",
        EventType.ORDER,
        "an order, or an amendment to one",
    ),
    LogRule(
        r"35=X(\D|$)|MarketDataIncrementalRefresh",
        EventType.BOOK_SIDE,
        "an incremental book update",
    ),
    LogRule(r"35=W(\D|$)|MarketDataSnapshot", EventType.BOOK, "a full book snapshot"),
    LogRule(r"35=[SR](\D|$)|Quote(Request)?\b", EventType.QUOTE, "a quote, or a request for one"),
    LogRule(r"35=d(\D|$)|SecurityDefinition", EventType.INSTRUMENT, "reference data"),
)


@dataclasses.dataclass
class LogRules(Convertible):
    """Which `EventType` each line of a log is, by the first pattern that matches.

    A list of regular expressions and nothing else, so the whole thing is
    configuration: `LogRules.from_yaml("rules.yml")` reads one, and a desk with
    its own log format writes its own rather than patching this package.

    **The first match wins, and no match is `UNKNOWN`.** Both halves matter. An
    ordered list is what lets a specific rule sit in front of a general one
    without either having to know about the other, and a line nothing matches
    is still a line -- it is stored, keyed and partitioned like every other,
    under a type that says plainly that nobody has classified it. Dropping it,
    or guessing, is how a log stops being a record of what happened.

    The matching is one Arrow kernel per rule over the whole message column, so
    the cost is a handful of passes per batch rather than anything per row.
    """

    #: Rules in the order they are tried. The default reads a FIX trading log.
    rules: list[LogRule] = dataclasses.field(default_factory=lambda: list(DEFAULT_RULES))

    #: The Arrow type an `etype` column is, which is what the codes are cast to.
    CODE: ClassVar[pyarrow.DataType] = pyarrow.int32()

    def etype_arrow(self, messages: Any) -> pyarrow.Array:
        """One `etype` per message: the first rule that matches, else `UNKNOWN`.

        Applied **in reverse**, each rule overwriting what the ones after it
        decided, so the earliest rule in the list is the one that survives.
        That is the whole of "first match wins", and it is one pass per rule
        rather than a scan per row.

        A null message matches nothing rather than propagating: a line with no
        payload is unclassified, which `UNKNOWN` already says, and letting the
        null through would put a null in a NOT NULL column.
        """
        compute = pyarrow.compute
        rows = len(messages)
        found: Any = pyarrow.repeat(pyarrow.scalar(int(EventType.UNKNOWN), self.CODE), rows)
        if not rows:
            return found
        text = messages.cast(pyarrow.string(), safe=False)
        for rule in reversed(self.rules):
            hit = compute.fill_null(compute.match_substring_regex(text, rule.pattern), False)
            found = compute.if_else(hit, pyarrow.scalar(int(rule.etype), self.CODE), found)
        return found.cast(self.CODE, safe=False)

    def etype_arrow_batch(self, batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        """`batch` with its `etype` column decided from its `message` column.

        An empty rule set hands the batch straight back rather than writing a
        column of zeros over one somebody else decided.
        """
        if not self.rules:
            return batch
        index = batch.schema.get_field_index("etype")
        declared = batch.schema.field(index)
        return batch.set_column(
            index,
            declared,
            self.etype_arrow(batch.column("message")).cast(declared.type, safe=False),
        )
