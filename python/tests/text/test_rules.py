"""Which event a log line is, and what happens to the ones nothing recognises."""

from __future__ import annotations

import pyarrow
import pytest

from rekep.market import EventType
from rekep.text.fixmessage import DEFAULT_RULES, FixMessage, FixMessageRule, FixMessageRules

#: One line per kind the default rules know, in both spellings a log uses.
WIRE = {
    "8=FIX.4.4\x0135=8\x0117=e1\x01": EventType.EXECUTION,
    "8=FIX.4.4\x0135=D\x0111=cl-1\x01": EventType.ORDER,
    "8=FIX.4.4\x0135=F\x0141=cl-1\x01": EventType.ORDER,
    "8=FIX.4.4\x0135=G\x0141=cl-1\x01": EventType.ORDER,
    "8=FIX.4.4\x0135=X\x01268=2\x01": EventType.BOOK,
    "8=FIX.4.4\x0135=W\x01268=2\x01": EventType.BOOK,
    "8=FIX.4.4\x0135=S\x01117=q1\x01": EventType.QUOTE,
    "8=FIX.4.4\x0135=R\x01131=r1\x01": EventType.QUOTE,
    "8=FIX.4.4\x0135=Z\x01117=q1\x01": EventType.QUOTE,
    "8=FIX.4.4\x0135=a\x01117=q1\x01": EventType.QUOTE,
    "8=FIX.4.4\x0135=b\x01117=q1\x01": EventType.QUOTE,
    "8=FIX.4.4\x0135=i\x01117=q1\x01": EventType.QUOTE,
    "8=FIX.4.4\x0135=AG\x01131=r1\x01": EventType.QUOTE,
    "8=FIX.4.4\x0135=AH\x01131=r1\x01": EventType.QUOTE,
    "8=FIX.4.4\x0135=AI\x01117=q1\x01": EventType.QUOTE,
    "8=FIX.4.4\x0135=AJ\x01117=q1\x01": EventType.QUOTE,
    "8=FIX.4.4\x0135=d\x0155=AAPL\x01": EventType.INSTRUMENT,
}
RENDERED = {
    "sent ExecutionReport for cl-1": EventType.EXECUTION,
    "sent NewOrderSingle AAPL 100@10.0": EventType.ORDER,
    "OrderCancelRequest cl-1": EventType.ORDER,
    "OrderCancelReplaceRequest cl-1": EventType.ORDER,
    "MarketDataIncrementalRefresh 12 entries": EventType.BOOK,
    "MarketDataSnapshotFullRefresh AAPL": EventType.BOOK,
    "QuoteRequest from desk": EventType.QUOTE,
    "received MassQuoteAcknowledgement": EventType.QUOTE,
    "sent QuoteStatusReport": EventType.QUOTE,
    "sent RFQRequest": EventType.QUOTE,
    "SecurityDefinition AAPL": EventType.INSTRUMENT,
}
NOTHING = [
    "heartbeat",
    "connection established to venue",
    "",
    "35=99|something else",
    "took 5ms",
]


def codes(messages: list[str | None], rules: FixMessageRules | None = None) -> list[int]:
    """The codes `rules` gives `messages`. `is None`, never `or`: an empty rule
    set is a real answer and must not fall through to the default one."""
    using = FixMessageRules() if rules is None else rules
    return using.etype_arrow(pyarrow.array(messages, type=pyarrow.string())).to_pylist()


@pytest.mark.parametrize("message,expected", WIRE.items(), ids=lambda value: str(value)[:24])
def test_a_wire_message_is_read_by_its_message_type(message: str, expected: EventType) -> None:
    assert codes([message]) == [int(expected)]


@pytest.mark.parametrize("message,expected", RENDERED.items(), ids=lambda value: str(value)[:24])
def test_a_rendered_message_is_read_by_its_name(message: str, expected: EventType) -> None:
    assert codes([message]) == [int(expected)]


@pytest.mark.parametrize("message", NOTHING, ids=lambda value: value[:20] or "empty")
def test_a_line_nothing_matches_is_unknown_rather_than_dropped(message: str) -> None:
    """A line nobody classified is still a line, and says so."""
    assert codes([message]) == [int(EventType.UNKNOWN)]


def test_a_null_message_is_unknown_and_not_null() -> None:
    """`etype` is NOT NULL, so a null payload must not propagate into it."""
    assert codes([None, "heartbeat"]) == [0, 0]


def test_an_empty_pattern_matches_empty_text_but_not_a_null_message() -> None:
    rules = FixMessageRules(rules=[FixMessageRule("", EventType.ORDER)])
    assert codes([None, ""], rules) == [int(EventType.UNKNOWN), int(EventType.ORDER)]


def test_the_first_rule_that_matches_wins() -> None:
    """Which is what lets a specific rule sit in front of a general one."""
    rules = FixMessageRules(
        rules=[
            FixMessageRule(r"URGENT", EventType.EXECUTION),
            FixMessageRule(r"order", EventType.ORDER),
        ]
    )
    assert codes(["URGENT order"], rules) == [int(EventType.EXECUTION)]
    assert codes(["ordinary order"], rules) == [int(EventType.ORDER)]

    reversed_rules = FixMessageRules(rules=list(reversed(rules.rules)))
    assert codes(["URGENT order"], reversed_rules) == [int(EventType.ORDER)], "order decides"


def test_a_line_naming_two_kinds_takes_the_more_specific_one() -> None:
    """An execution report quoting the order it fills names both; it is a fill."""
    assert codes(["ExecutionReport for NewOrderSingle cl-1"]) == [int(EventType.EXECUTION)]


def test_no_rules_leaves_everything_unknown_without_running_anything() -> None:
    empty = FixMessageRules(rules=[])
    assert not empty.rules
    assert codes(list(WIRE), empty) == [0] * len(WIRE)


def test_no_rows_is_no_rows() -> None:
    built = FixMessageRules().etype_arrow(pyarrow.array([], type=pyarrow.string()))
    assert len(built) == 0 and built.type == pyarrow.int32()


def test_the_codes_are_the_type_the_column_is() -> None:
    built = FixMessageRules().etype_arrow(pyarrow.array(["35=8|"], type=pyarrow.string()))
    assert built.type == FixMessage.into_field().field("etype").arrow_type == pyarrow.int32()


def test_the_default_rules_are_read_by_a_wide_column_too() -> None:
    """pyiceberg hands strings back as `large_string`, which is a different type."""
    wide = pyarrow.array(list(WIRE), type=pyarrow.large_string())
    assert FixMessageRules().etype_arrow(wide).to_pylist() == [int(kind) for kind in WIRE.values()]


def test_every_default_rule_says_what_it_is_for() -> None:
    for rule in DEFAULT_RULES:
        assert rule.pattern and rule.label, rule
        assert rule.etype is not EventType.UNKNOWN, "a rule matching nothing in particular"


def test_default_log_rules_and_pattern_lists_are_isolated() -> None:
    first, second = FixMessageRules(), FixMessageRules()
    first.rules[0].patterns.append("first only")
    assert "first only" not in second.rules[0].patterns
    assert first.rules[0] is not second.rules[0]
    assert first.rules[0].patterns is not second.rules[0].patterns


def test_rules_round_trip_as_a_document() -> None:
    """The whole point of them being data: a desk writes its own in a file."""
    rules = FixMessageRules(rules=[FixMessageRule(r"\bboom\b", EventType.EXECUTION, "a fill")])
    assert FixMessageRules.from_json(rules.into_json()) == rules


def test_a_rule_names_its_event_type_rather_than_numbering_it() -> None:
    """`etype: ORDER` in a configuration, not `etype: 110`."""
    loaded = FixMessageRules.from_dict({"rules": [{"pattern": "x", "etype": "ORDER"}]})
    assert loaded.rules[0].etype is EventType.ORDER
    assert FixMessageRules.from_dict({"rules": [{"pattern": "x", "etype": 110}]}) == loaded
