"""Which protocol a line carries, decided the same way one at a time and a column at a time."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fix import BEGIN_STRING, BRIDGE, BRIDGE_WIRE, NO_PROTOCOL, Rule, Rules
from rekep.fix.rules import DEFAULT_RULES, NAMED, OTHER, UL, UL_WIRE

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
}

#: Derived from the rule set, then pinned, so a renamed built-in cannot move
#: both sides of the assertions below together.
EXPECTED_RULES = 4
EXPECTED_PROTOCOLS = 3
#: The bridge is the one protocol two built-ins carry, which is the point of
#: `protocol` being a name and not a rule.
EXPECTED_UL_RULES = 2


def test_the_default_set_is_the_built_ins_in_order() -> None:
    """The wrapped bridge message leads: it is the only one with two tells."""
    assert len(DEFAULT_RULES) == EXPECTED_RULES
    assert [rule.protocol for rule in Rules.DEFAULT.rules] == ["UL", "FIX", "UL", "OTHER"]
    assert len({rule.protocol for rule in DEFAULT_RULES}) == EXPECTED_PROTOCOLS
    assert OTHER.protocol == NO_PROTOCOL


def test_the_built_in_patterns_are_the_parser_s_own() -> None:
    """One answer to "where does a message start", not two that drift apart."""
    assert Rules.DEFAULT.rule("FIX").pattern == BEGIN_STRING
    assert {rule.pattern for rule in Rules.DEFAULT.rules if rule.protocol == "UL"} == {
        BRIDGE,
        BRIDGE_WIRE,
    }


@pytest.mark.parametrize(("message", "expected"), LINES.items(), ids=lambda v: str(v)[:28])
def test_a_line_lands_in_the_protocol_the_rules_claim(message: str, expected: str) -> None:
    assert Rules.DEFAULT.categorise(message).protocol == expected


def test_the_column_agrees_with_the_line_for_line_reading() -> None:
    """The two readings are contracted to agree, so they are compared here."""
    protocols = Rules.DEFAULT.into_arrow_protocol_array(pyarrow.array(list(LINES)))
    scalar = [Rules.DEFAULT.categorise(line) for line in LINES]
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
    assert Rules.DEFAULT.categorise(bare) is UL
    assert Rules.DEFAULT.categorise(wrapped) is UL_WIRE
    assert Rules.DEFAULT.categorise(bare).protocol == Rules.DEFAULT.categorise(wrapped).protocol
    assert sum(rule.protocol == "UL" for rule in DEFAULT_RULES) == EXPECTED_UL_RULES
    lines = pyarrow.array([bare, wrapped])
    assert Rules.DEFAULT.into_arrow_protocol_array(lines).to_pylist() == ["UL", "UL"]


def test_the_first_rule_for_a_protocol_is_the_one_it_reads_back() -> None:
    """A slice is parsed by a rule, and two carry `UL`, so the order decides."""
    assert Rules.DEFAULT.rule("UL") is UL_WIRE
    assert Rules.DEFAULT.rule("FIX").codec == "fix"
    assert Rules.DEFAULT.rule(NO_PROTOCOL) is OTHER


def test_a_wrapped_bridge_message_is_read_as_a_bridge_message() -> None:
    """It answers to both tells, so the order of the rules is what decides it."""
    wrapped = "8=FIX.4.2|35=UL|#A=1|#B=2"
    assert Rules.DEFAULT.categorise(wrapped).protocol == "UL"
    assert Rules.DEFAULT.categorise(wrapped).named is True
    assert Rules.DEFAULT.categorise("8=FIX.4.2|35=ULX|#A=1|#B=2").protocol == "FIX"
    assert Rules.DEFAULT.categorise("8=FIX.4.2|135=UL|#A=1|#B=2").protocol == "FIX"


def test_a_lone_marked_key_in_prose_is_not_a_bridge_message() -> None:
    """Two `#NAME=` tokens or it is a sentence, which is what the rule says."""
    assert Rules.DEFAULT.categorise("retry #FOO=bar and move on").protocol == NO_PROTOCOL
    assert Rules.DEFAULT.categorise("send #FOO=bar #BAZ=1").protocol == "UL"


def test_a_null_message_is_other_rather_than_null() -> None:
    """`protocol` is NOT NULL, so a null payload must not propagate into it."""
    protocols = Rules.DEFAULT.into_arrow_protocol_array(
        pyarrow.array([None, "heartbeat"], pyarrow.string())
    )
    assert protocols.to_pylist() == [NO_PROTOCOL, NO_PROTOCOL]
    assert protocols.null_count == 0
    assert Rules.DEFAULT.categorise(None) is OTHER


def test_no_rows_is_no_rows() -> None:
    protocols = Rules.DEFAULT.into_arrow_protocol_array(pyarrow.array([], pyarrow.string()))
    assert len(protocols) == 0
    assert protocols.type == pyarrow.string()


def test_the_first_rule_that_matches_wins() -> None:
    """Which is what lets a specific rule sit in front of a general one."""
    rules = Rules(rules=[Rule(protocol="SESSION", pattern=r"35=0", codec="fix"), *DEFAULT_RULES])
    line = "recv 8=FIX.4.4|35=0|10=017|"
    assert rules.categorise(line).protocol == "SESSION"
    assert Rules.DEFAULT.categorise(line).protocol == "FIX", "and the default set still says FIX"
    assert rules.into_arrow_protocol_array(pyarrow.array([line])).to_pylist() == ["SESSION"]


def test_a_rule_may_be_told_apart_by_its_driver() -> None:
    rules = Rules(rules=[Rule(protocol="BRIDGE", driver_pattern="^ULBridge$")])
    assert rules.categorise("anything", "ULBridge").protocol == "BRIDGE"
    assert rules.categorise("anything", "FixSession_XPAR").protocol == NO_PROTOCOL
    protocols = rules.into_arrow_protocol_array(
        pyarrow.array(["a", "b"]), pyarrow.array(["ULBridge", "other"])
    )
    assert protocols.to_pylist() == ["BRIDGE", NO_PROTOCOL]


def test_a_rule_naming_a_driver_with_no_driver_column_does_not_match() -> None:
    """A rule that cannot be evaluated is not a rule that matched."""
    rules = Rules(rules=[Rule(protocol="BRIDGE", driver_pattern="^ULBridge$")])
    assert rules.into_arrow_protocol_array(pyarrow.array(["a"])).to_pylist() == [NO_PROTOCOL]
    assert rules.categorise("a").protocol == NO_PROTOCOL


def test_a_codec_says_how_a_line_of_that_protocol_is_read() -> None:
    assert NAMED[Rules.DEFAULT.rule("FIX").codec] is False
    assert NAMED[Rules.DEFAULT.rule("UL").codec] is True
    assert Rules.DEFAULT.rule(NO_PROTOCOL).named is None, "and OTHER is not read at all"


def test_an_unknown_protocol_reads_back_as_other() -> None:
    """A batch may carry a name this set has lost, and it still has to be read."""
    assert Rules.DEFAULT.rule("SBE") is OTHER
    assert Rules.DEFAULT.rule("") is OTHER


def test_a_rule_set_round_trips_as_a_document(tmp_path: Path) -> None:
    """A rule set is data, so it has to survive being written down."""
    path = tmp_path / "rules.yml"
    Rules.DEFAULT.into_yaml(path)
    assert Rules.from_yaml(path) == Rules.DEFAULT


def test_a_loaded_rule_set_overrides_the_default(tmp_path: Path) -> None:
    """A desk with its own bridge writes a document rather than patching this."""
    path = tmp_path / "rules.yml"
    Rules(
        rules=[Rule(protocol="OWN", pattern=r"toBridge", codec="ul", separator="|"), OTHER]
    ).into_yaml(path)
    loaded = Rules.from_yaml(path)
    line = "toBridge #ISINCODE=XX|#SYMBOL=TTF"
    assert Rules.DEFAULT.categorise(line).protocol == "UL"
    assert loaded.categorise(line).protocol == "OWN"
    assert loaded.categorise(line).separator == "|"
    protocols = loaded.into_arrow_protocol_array(pyarrow.array([line, "prose"]))
    assert protocols.to_pylist() == ["OWN", NO_PROTOCOL]


def test_a_rule_is_a_field_class_like_every_other_declaration() -> None:
    """Which is what puts a rule set beside the schema contracts."""
    assert Rule.FIELD.name == "Rule"
    assert Rule.FIELD.names[:2] == ["protocol", "pattern"]
    assert Rule.FIELD.field("driver_pattern").nullable
    assert not Rule.FIELD.field("protocol").nullable
