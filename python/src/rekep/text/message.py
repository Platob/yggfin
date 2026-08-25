"""One raw log row, before a protocol reads its payload."""

from __future__ import annotations

import dataclasses
import functools
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Self

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.enums import EventType
from rekep.fields import scalar
from rekep.market.event import Event
from rekep.text.kwargs import Kwarg

_CONTRACT_METADATA = MappingProxyType({"version": "2"})
_EVENT_CODE = pyarrow.int32()


@scalar(slots=True)
class Message(Event):
    """One log header, its provenance, and its protocol-neutral payload."""

    @classmethod
    @functools.cache
    def into_field_metadata(cls) -> Mapping[str, str]:
        """Contract metadata published with raw-message schemas."""
        return _CONTRACT_METADATA

    source_url: str = ""
    """Path of the log the row came from, as its filesystem addresses it."""

    source_rownum: int = 0
    """1-based physical line number of the header; 0 when not read from a file."""

    thread_name: str = ""
    """Contents of the first bracketed header field."""

    plugin_code: str = ""
    """Contents of the second bracketed header field."""

    message: str = ""
    """Payload after the fixed log header, with continuation lines folded in."""

    kwargs: list[Kwarg] = dataclasses.field(default_factory=list)
    """Ordered arguments parsed from the payload without protocol interpretation."""

    def __post_init__(self) -> None:
        """Normalize argument spellings once for typed access."""
        Event.__post_init__(self)
        if self.kwargs is None:
            self.kwargs = []
        if not self.kwargs and self.message:
            self.kwargs = Kwarg.parse_arrow(pyarrow.array([self.message])).to_pylist()[0]
        self.kwargs = [Kwarg.from_stored(entry) for entry in self.kwargs]

    def identify(self) -> Self:
        """Give this raw row its provenance-scoped content identity."""
        if not self.hash:
            self.hash = self.hash_of(self.message, self.source_url, self.source_rownum)
        self.xhash = self.hash
        return self

    @classmethod
    def identified(
        cls, columns: dict[str, Any], schema: pyarrow.Schema, rows: int
    ) -> pyarrow.RecordBatch:
        """Build a batch after assigning raw row identities in Arrow kernels."""
        columns["hash"] = cls.hash_arrow(
            columns["message"], columns["source_url"], columns["source_rownum"]
        )
        columns["xhash"] = columns["hash"]
        return pyarrow.RecordBatch.from_arrays(
            [columns[name] for name in schema.names], schema=schema
        )


@dataclasses.dataclass
class MessageRule(Convertible):
    """One payload pattern and the event type it identifies."""

    pattern: str = ""
    """RE2 regular expression matched anywhere in the payload."""

    etype: EventType = EventType.UNKNOWN
    """Event type assigned to matching messages."""

    label: str = ""
    """Short purpose when the pattern does not explain itself."""

    patterns: list[str] = dataclasses.field(default_factory=list)
    """Additional alternatives; matching any pattern satisfies the rule."""

    @property
    def message_patterns(self) -> tuple[str, ...]:
        """All nonempty patterns in declaration order."""
        return tuple(filter(None, (self.pattern, *self.patterns)))


@dataclasses.dataclass
class MessageRules(Convertible):
    """Ordered payload rules assigning an `EventType` to each message."""

    rules: list[MessageRule] = dataclasses.field(default_factory=list)

    def etype_arrow(self, messages: Any) -> pyarrow.Array:
        """One event code per message; unmatched messages are `UNKNOWN`."""
        compute = pyarrow.compute
        rows = len(messages)
        found: Any = pyarrow.repeat(pyarrow.scalar(int(EventType.UNKNOWN), _EVENT_CODE), rows)
        if not rows:
            return found
        text = messages.cast(pyarrow.string(), safe=False)
        for rule in reversed(self.rules):
            hit = _rule_hit(rule, text)
            found = compute.if_else(hit, pyarrow.scalar(int(rule.etype), _EVENT_CODE), found)
        return found.cast(_EVENT_CODE, safe=False)


def _rule_hit(rule: MessageRule, text: Any) -> Any:
    """One rule's any-pattern mask."""
    compute = pyarrow.compute
    mask = None
    for pattern in rule.message_patterns:
        matched = compute.fill_null(compute.match_substring_regex(text, pattern), False)
        mask = matched if mask is None else compute.or_(mask, matched)
    return compute.is_valid(text) if mask is None else mask
