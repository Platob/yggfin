"""Which protocol a line carries, decided the same way one at a time and a column at a time."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fix import BEGIN_STRING, BRIDGE, BRIDGE_WIRE, NO_PROTOCOL, Rule, Rules
from rekep.fix.rules import (
    DEFAULT_RULES,
    MARKET_CATEGORY,
    MISC,
    MISC_CATEGORY,
    NAMED,
    OTHER,
    UL,
    UL_WIRE,
    UNKNOWN_CATEGORY,
)
from rekep.market import EventType

SOH = "\x01"

#: One message of each protocol, in the spellings the sample capture uses.
LINES = {
    "After Enrichment -> ACCOUNT=ACCT-000117 CLIENTID=MCFP2 VENUE=XPAR": "OTHER",
    "sending >> 8=FIX.4.2|9=176|35=D|10=203| << queued seq=1092": "FIX",
    "recv 8=FIX4^A9=61^A35=0^A10=017^A on session 3": "FIX",
    f"raw 8=FIX.4.4{SOH}9=224{SOH}35=8{SOH}10=118{SOH}": "FIX",
    "toBridge #ISINCODE=XX|#SYMBOL=TTF|#SIDE=1": "UL",
    "sending >> 8=FIX.4.2|35=UL|#SYMBOL=TTF|#SIDE=1|10=044|": "UL",
    "8=FIX.4.4|35=8|58=quoting #A=1 and #B=2|10=1|": "FIX",
    "Message rejected because : ignoring OMSSales expiry message": "OTHER",
    "no level printed by this driver": "OTHER",
    "heartbeat emitted seq=7": "MISC",
}

#: Derived from the rule set, then pinned, so a renamed built-in cannot move
#: both sides of the assertions below together.
EXPECTED_RULES = 5
EXPECTED_PROTOCOLS = 4
#: The bridge is the one protocol two built-ins carry, which is the point of
#: `protocol` being a name and not a rule.
EXPECTED_UL_RULES = 2
DEFAULT = Rules.into_default()


def test_the_default_set_is_the_built_ins_in_order() -> None:
    """The wrapped bridge message leads: it is the only one with two tells."""
    assert len(DEFAULT_RULES) == EXPECTED_RULES
    assert [rule.protocol for rule in DEFAULT.rules] == [
        "UL",
        "FIX",
        "UL",
        "MISC",
        "OTHER",
    ]
    assert len({rule.protocol for rule in DEFAULT_RULES}) == EXPECTED_PROTOCOLS
    assert OTHER.protocol == NO_PROTOCOL


def test_the_built_in_patterns_are_the_parser_s_own() -> None:
    """One answer to "where does a message start", not two that drift apart."""
    assert DEFAULT.rule("FIX").pattern == BEGIN_STRING
    assert {rule.pattern for rule in DEFAULT.rules if rule.protocol == "UL"} == {
        BRIDGE,
        BRIDGE_WIRE,
    }


@pytest.mark.parametrize(("message", "expected"), LINES.items(), ids=lambda v: str(v)[:28])
def test_a_line_lands_in_the_protocol_the_rules_claim(message: str, expected: str) -> None:
    assert DEFAULT.categorise(message).protocol == expected


def test_the_column_agrees_with_the_line_for_line_reading() -> None:
    """The two readings are contracted to agree, so they are compared here."""
    protocols = DEFAULT.into_arrow_protocol_array(pyarrow.array(list(LINES)))
    scalar = [DEFAULT.categorise(line) for line in LINES]
    assert protocols.to_pylist() == [rule.protocol for rule in scalar]
    assert protocols.to_pylist() == list(LINES.values())
    assert protocols.type == pyarrow.string()


def test_two_rules_sharing_a_protocol_both_classify_as_it() -> None:
    """A protocol names what a line carries, not which pattern spotted it.

    The bridge has two tells -- bare and wrapped in a FIX envelope -- and a
    reader asking for bridge messages must get both without knowing that.
    """
    bare = "toBridge #ISINCODE=XX|#SYMBOL=TTF"
    wrapped = "sending >> 8=FIX.4.2|35=UL|#SYMBOL=TTF|#SIDE=1|10=044|"
    assert DEFAULT.categorise(bare) == UL
    assert DEFAULT.categorise(wrapped) == UL_WIRE
    assert DEFAULT.categorise(bare).protocol == DEFAULT.categorise(wrapped).protocol
    assert sum(rule.protocol == "UL" for rule in DEFAULT_RULES) == EXPECTED_UL_RULES
    lines = pyarrow.array([bare, wrapped])
    assert DEFAULT.into_arrow_protocol_array(lines).to_pylist() == ["UL", "UL"]


def test_the_first_rule_for_a_protocol_is_the_one_it_reads_back() -> None:
    """A slice is parsed by a rule, and two carry `UL`, so the order decides."""
    assert DEFAULT.rule("UL") == UL_WIRE
    assert DEFAULT.rule("FIX").codec == "fix"
    assert DEFAULT.rule(NO_PROTOCOL) == OTHER


def test_a_wrapped_bridge_message_is_read_as_a_bridge_message() -> None:
    """It answers to both tells, so the order of the rules is what decides it."""
    wrapped = "8=FIX.4.2|35=UL|#A=1|#B=2"
    assert DEFAULT.categorise(wrapped).protocol == "UL"
    assert DEFAULT.categorise(wrapped).named is True
    assert DEFAULT.categorise("8=FIX.4.2|35=ULX|#A=1|#B=2").protocol == "FIX"
    assert DEFAULT.categorise("8=FIX.4.2|135=UL|#A=1|#B=2").protocol == "FIX"


def test_a_lone_marked_key_in_prose_is_not_a_bridge_message() -> None:
    """Two `#NAME=` tokens or it is a sentence, which is what the rule says."""
    assert DEFAULT.categorise("retry #FOO=bar and move on").protocol == "MISC"
    assert DEFAULT.categorise("send #FOO=bar #BAZ=1").protocol == "UL"


def test_a_rule_accepts_several_message_patterns() -> None:
    rules = Rules(rules=[Rule(protocol="OPS", patterns=[r"\bready\b", r"\bstopped\b"]), OTHER])
    messages = pyarrow.array(["service ready", "service stopped", "service busy"])
    scalar = [rules.categorise(message).protocol for message in messages.to_pylist()]
    assert scalar == ["OPS", "OPS", NO_PROTOCOL]
    assert rules.into_arrow_protocol_array(messages).to_pylist() == scalar


def test_dot_does_not_cross_a_newline_unless_the_pattern_requests_it() -> None:
    messages = pyarrow.array(["a\nb"])
    for pattern, expected in ((r"a.b", NO_PROTOCOL), (r"(?s)a.b", "OWN")):
        rules = Rules(rules=[Rule(protocol="OWN", pattern=pattern)])
        scalar = rules.categorise(messages[0].as_py()).protocol
        assert scalar == expected
        assert rules.into_arrow_protocol_array(messages).to_pylist() == [scalar]


def test_the_legacy_positional_rule_signature_keeps_its_bindings() -> None:
    rule = Rule("OWN", "message", "^Driver$", "|", ";", "ul", "4.2")
    assert (
        rule.protocol,
        rule.pattern,
        rule.driver_pattern,
        rule.separator,
        rule.entry_separator,
        rule.codec,
        rule.fix_version,
        rule.patterns,
    ) == ("OWN", "message", "^Driver$", "|", ";", "ul", "4.2", [])
    assert rule.matches("message", "Driver")


def test_a_plural_pattern_passed_as_one_string_stays_one_pattern() -> None:
    rule = Rule(protocol="OWN", patterns=r"ready|stopped")  # type: ignore[arg-type]
    assert rule.patterns == [r"ready|stopped"]
    assert rule.matches("service stopped")


def test_the_misc_rule_recognises_known_operational_lines() -> None:
    messages = pyarrow.array(["heartbeat 7", "connection established", "opaque status"])
    assert DEFAULT.into_arrow_protocol_array(messages).to_pylist() == [
        "MISC",
        "MISC",
        NO_PROTOCOL,
    ]
    assert DEFAULT.categorise("heartbeat 7") == MISC


def test_default_rule_instances_and_pattern_lists_are_isolated() -> None:
    assert Rules.into_default() is DEFAULT
    first, second = Rules(), Rules()
    first.rules[3].patterns.append("first only")
    assert "first only" not in second.rules[3].patterns
    assert "first only" not in DEFAULT.rules[3].patterns
    assert first.rules[3] is not second.rules[3]
    assert first.rules[3].patterns is not second.rules[3].patterns


def test_a_null_message_is_other_rather_than_null() -> None:
    """`protocol` is NOT NULL, so a null payload must not propagate into it."""
    protocols = DEFAULT.into_arrow_protocol_array(
        pyarrow.array([None, "heartbeat"], pyarrow.string())
    )
    assert protocols.to_pylist() == [NO_PROTOCOL, "MISC"]
    assert protocols.null_count == 0
    assert DEFAULT.categorise(None) is OTHER


def test_an_empty_pattern_matches_every_nonnull_message_in_both_paths() -> None:
    rules = Rules(rules=[Rule(protocol="ALL")])
    messages = pyarrow.array([None, ""])
    assert [rules.categorise(message).protocol for message in messages.to_pylist()] == [
        NO_PROTOCOL,
        "ALL",
    ]
    assert rules.into_arrow_protocol_array(messages).to_pylist() == [NO_PROTOCOL, "ALL"]


def test_no_rows_is_no_rows() -> None:
    protocols = DEFAULT.into_arrow_protocol_array(pyarrow.array([], pyarrow.string()))
    assert len(protocols) == 0
    assert protocols.type == pyarrow.string()


def test_categories_agree_one_row_and_one_column_at_a_time() -> None:
    protocols = ["FIX", "UL", "MISC", NO_PROTOCOL, "SBE", None]
    etypes = [EventType.ORDER, EventType.UNKNOWN, EventType.UNKNOWN, EventType.UNKNOWN, 0, None]
    scalar = [
        DEFAULT.category_of(protocol, etype)
        for protocol, etype in zip(protocols, etypes, strict=True)
    ]
    assert scalar == [
        MARKET_CATEGORY,
        MISC_CATEGORY,
        MISC_CATEGORY,
        UNKNOWN_CATEGORY,
        UNKNOWN_CATEGORY,
        UNKNOWN_CATEGORY,
    ]
    vector = DEFAULT.into_arrow_category_array(
        pyarrow.array(protocols),
        pyarrow.array([None if etype is None else int(etype) for etype in etypes]),
    )
    assert vector.to_pylist() == scalar


def test_the_first_rule_that_matches_wins() -> None:
    """Which is what lets a specific rule sit in front of a general one."""
    rules = Rules(rules=[Rule(protocol="SESSION", pattern=r"35=0", codec="fix"), *DEFAULT_RULES])
    line = "recv 8=FIX.4.4|35=0|10=017|"
    assert rules.categorise(line).protocol == "SESSION"
    assert DEFAULT.categorise(line).protocol == "FIX", "and the default set still says FIX"
    assert rules.into_arrow_protocol_array(pyarrow.array([line])).to_pylist() == ["SESSION"]


def test_a_rule_may_be_told_apart_by_its_driver() -> None:
    rules = Rules(rules=[Rule(protocol="BRIDGE", driver_pattern="^ULBridge$")])
    assert rules.categorise("anything", "ULBridge").protocol == "BRIDGE"
    assert rules.categorise("anything", "FixSession_XPAR").protocol == NO_PROTOCOL
    messages = pyarrow.array([None, "a", "b"])
    drivers = pyarrow.array(["ULBridge", "ULBridge", "other"])
    protocols = rules.into_arrow_protocol_array(messages, drivers)
    scalar = [
        rules.categorise(message, driver).protocol
        for message, driver in zip(messages.to_pylist(), drivers.to_pylist(), strict=True)
    ]
    assert protocols.to_pylist() == scalar == [NO_PROTOCOL, "BRIDGE", NO_PROTOCOL]


def test_a_rule_naming_a_driver_with_no_driver_column_does_not_match() -> None:
    """A rule that cannot be evaluated is not a rule that matched."""
    rules = Rules(rules=[Rule(protocol="BRIDGE", driver_pattern="^ULBridge$")])
    assert rules.into_arrow_protocol_array(pyarrow.array(["a"])).to_pylist() == [NO_PROTOCOL]
    assert rules.categorise("a").protocol == NO_PROTOCOL


def test_a_codec_says_how_a_line_of_that_protocol_is_read() -> None:
    assert NAMED[DEFAULT.rule("FIX").codec] is False
    assert NAMED[DEFAULT.rule("UL").codec] is True
    assert DEFAULT.rule(NO_PROTOCOL).named is None, "and OTHER is not read at all"


def test_an_unknown_protocol_reads_back_as_other() -> None:
    """A batch may carry a name this set has lost, and it still has to be read."""
    assert DEFAULT.rule("SBE") is OTHER
    assert DEFAULT.rule("") is OTHER


def test_a_rule_set_round_trips_as_a_document(tmp_path: Path) -> None:
    """A rule set is data, so it has to survive being written down."""
    path = tmp_path / "rules.yml"
    DEFAULT.into_yaml(path)
    assert Rules.from_yaml(path) == DEFAULT


def test_a_loaded_rule_set_overrides_the_default(tmp_path: Path) -> None:
    """A desk with its own bridge writes a document rather than patching this."""
    path = tmp_path / "rules.yml"
    Rules(
        rules=[Rule(protocol="OWN", pattern=r"toBridge", codec="ul", separator="|"), OTHER]
    ).into_yaml(path)
    loaded = Rules.from_yaml(path)
    line = "toBridge #ISINCODE=XX|#SYMBOL=TTF"
    assert DEFAULT.categorise(line).protocol == "UL"
    assert loaded.categorise(line).protocol == "OWN"
    assert loaded.categorise(line).separator == "|"
    protocols = loaded.into_arrow_protocol_array(pyarrow.array([line, "prose"]))
    assert protocols.to_pylist() == ["OWN", NO_PROTOCOL]


def test_a_rule_is_a_field_class_like_every_other_declaration() -> None:
    """Which is what puts a rule set beside the schema contracts."""
    assert Rule.into_field().name == "Rule"
    assert Rule.into_field().names[:2] == ["protocol", "pattern"]
    assert Rule.into_field().field("driver_pattern").nullable
    assert not Rule.into_field().field("protocol").nullable
