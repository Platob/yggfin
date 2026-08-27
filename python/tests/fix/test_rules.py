"""Which protocol a line carries, decided the same way one at a time and a column at a time."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fix import BEGIN_STRING, BRIDGE, BRIDGE_WIRE, NO_PROTOCOL, Rule, Rules
from rekep.fix.message import WIRE_MSG_TYPE
from rekep.fix.rules import (
    CODEC_KEYS,
    DEFAULT_RULES,
    MARKET_CATEGORY,
    MISC,
    MISC_CATEGORY,
    OTHER,
    UL,
    UL_WIRE,
    UNKNOWN_CATEGORY,
    joined_pattern,
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
    "no level printed by this plugin": "OTHER",
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
    assert DEFAULT.rule("FIX").pattern == joined_pattern(BEGIN_STRING, WIRE_MSG_TYPE)
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


def test_a_rule_joins_several_patterns_into_one_alternation() -> None:
    """`joined_pattern` is the one spelling for "any of these", both paths."""
    rules = Rules(
        rules=[Rule(protocol="OPS", pattern=joined_pattern(r"\bready\b", r"\bstopped\b")), OTHER]
    )
    messages = pyarrow.array(["service ready", "service stopped", "service busy"])
    scalar = [rules.categorise(message).protocol for message in messages.to_pylist()]
    assert scalar == ["OPS", "OPS", NO_PROTOCOL]
    assert rules.into_arrow_protocol_array(messages).to_pylist() == scalar


def test_joined_pattern_scopes_a_branch_s_leading_flags() -> None:
    """A global `(?i)` is illegal mid-pattern in Python `re`; the join rewrites
    it into the scoped form both engines accept, and the flag stays local to
    its own branch rather than leaking across the alternation."""
    joined = joined_pattern(r"(?i)ready", r"stopped")
    assert joined == r"(?i:ready)|(?:stopped)"
    rules = Rules(rules=[Rule(protocol="OPS", pattern=joined), OTHER])
    lines = ["service READY", "service STOPPED", "service stopped"]
    scalar = [rules.categorise(line).protocol for line in lines]
    assert scalar == ["OPS", NO_PROTOCOL, "OPS"]
    assert rules.into_arrow_protocol_array(pyarrow.array(lines)).to_pylist() == scalar
    assert joined_pattern("", r"x", "") == r"(?:x)", "empty branches are no branches"
    assert joined_pattern("") == ""


def test_joined_branches_keep_their_named_groups_apart() -> None:
    """Two branches spelling the same capture name matched fine as separate
    patterns; the join renames per branch rather than becoming a pattern
    neither engine accepts. One branch stays verbatim."""
    joined = joined_pattern(r"(?P<v>ready)", r"(?P<v>stopped)")
    assert joined == r"(?:(?P<j0_v>ready))|(?:(?P<j1_v>stopped))"
    rules = Rules(rules=[Rule(protocol="OPS", pattern=joined), OTHER])
    lines = ["ready", "stopped", "busy"]
    scalar = [rules.categorise(line).protocol for line in lines]
    assert scalar == ["OPS", "OPS", NO_PROTOCOL]
    assert rules.into_arrow_protocol_array(pyarrow.array(lines)).to_pylist() == scalar
    assert joined_pattern(r"(?P<v>x)") == r"(?:(?P<v>x))"


def test_dot_does_not_cross_a_newline_unless_the_pattern_requests_it() -> None:
    messages = pyarrow.array(["a\nb"])
    for pattern, expected in ((r"a.b", NO_PROTOCOL), (r"(?s)a.b", "OWN")):
        rules = Rules(rules=[Rule(protocol="OWN", pattern=pattern)])
        scalar = rules.categorise(messages[0].as_py()).protocol
        assert scalar == expected
        assert rules.into_arrow_protocol_array(messages).to_pylist() == [scalar]


def test_a_stored_document_with_the_retired_patterns_list_still_reads() -> None:
    """`patterns` collapsed into `pattern`; a document from that shape keeps
    classifying the same lines rather than silently losing every regex past
    the first, and a plural spelling stored as one string stays one branch."""
    loaded = Rules.from_dict(
        {
            "rules": [
                {"protocol": "OPS", "pattern": r"\bready\b", "patterns": [r"\bstopped\b"]},
                {"protocol": "OWN", "patterns": r"paused|resumed"},
                {"protocol": NO_PROTOCOL},
            ]
        }
    )
    assert loaded.rule("OPS").pattern == joined_pattern(r"\bready\b", r"\bstopped\b")
    assert loaded.rule("OWN").pattern == joined_pattern(r"paused|resumed")
    lines = ["service ready", "service stopped", "service paused", "service busy"]
    assert [loaded.categorise(line).protocol for line in lines] == [
        "OPS",
        "OPS",
        "OWN",
        NO_PROTOCOL,
    ]
    assert Rules.from_dict(loaded.into_dict()) == loaded, "and the new shape round-trips"


def test_the_misc_rule_recognises_known_operational_lines() -> None:
    messages = pyarrow.array(["heartbeat 7", "connection established", "opaque status"])
    assert DEFAULT.into_arrow_protocol_array(messages).to_pylist() == [
        "MISC",
        "MISC",
        NO_PROTOCOL,
    ]
    assert DEFAULT.categorise("heartbeat 7") == MISC


def test_default_rule_instances_are_isolated() -> None:
    assert Rules.into_default() is DEFAULT
    first, second = Rules(), Rules()
    first.rules[3].pattern = "first only"
    assert second.rules[3].pattern != "first only"
    assert DEFAULT.rules[3].pattern != "first only"
    assert MISC.pattern != "first only"
    assert first.rules[3] is not second.rules[3]


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
    protocols = ["FIX", NO_PROTOCOL, "UL", "MISC", NO_PROTOCOL, "SBE", None]
    etypes = [
        EventType.ORDER,
        EventType.MISC,
        EventType.UNKNOWN,
        EventType.UNKNOWN,
        EventType.UNKNOWN,
        0,
        None,
    ]
    scalar = [
        DEFAULT.category_of(protocol, etype)
        for protocol, etype in zip(protocols, etypes, strict=True)
    ]
    assert scalar == [
        MARKET_CATEGORY,
        MISC_CATEGORY,
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


def test_categories_agree_on_codes_no_member_spells() -> None:
    """Case-variant packed bytes, a previous release's ordinal ids, junk:
    the scalar rule and the kernel answer identically on every one, because
    `from_int` answers only on the compiled codes the kernel's sets hold."""
    respelled = int.from_bytes(b"order", "big", signed=True)
    etypes = [respelled, 110, 210, 410, 999, -1, 0]
    for protocol in (None, "FIX"):
        scalar = [DEFAULT.category_of(protocol, etype) for etype in etypes]
        vector = DEFAULT.into_arrow_category_array(
            pyarrow.array([protocol] * len(etypes), pyarrow.string()),
            pyarrow.array(etypes, pyarrow.int64()),
        )
        assert vector.to_pylist() == scalar
    assert DEFAULT.category_of("FIX", respelled) == MISC_CATEGORY
    assert DEFAULT.category_of(None, respelled) == UNKNOWN_CATEGORY


def test_the_first_rule_that_matches_wins() -> None:
    """Which is what lets a specific rule sit in front of a general one."""
    rules = Rules(rules=[Rule(protocol="SESSION", pattern=r"35=0", codec="fix"), *DEFAULT_RULES])
    line = "recv 8=FIX.4.4|35=0|10=017|"
    assert rules.categorise(line).protocol == "SESSION"
    assert DEFAULT.categorise(line).protocol == "FIX", "and the default set still says FIX"
    assert rules.into_arrow_protocol_array(pyarrow.array([line])).to_pylist() == ["SESSION"]


def test_a_rule_may_be_told_apart_by_its_plugin() -> None:
    rules = Rules(rules=[Rule(protocol="BRIDGE", plugin_pattern="^ULBridge$")])
    assert rules.categorise("anything", "ULBridge").protocol == "BRIDGE"
    assert rules.categorise("anything", "FixSession_XPAR").protocol == NO_PROTOCOL
    messages = pyarrow.array([None, "a", "b"])
    plugins = pyarrow.array(["ULBridge", "ULBridge", "other"])
    protocols = rules.into_arrow_protocol_array(messages, plugins)
    scalar = [
        rules.categorise(message, plugin).protocol
        for message, plugin in zip(messages.to_pylist(), plugins.to_pylist(), strict=True)
    ]
    assert protocols.to_pylist() == scalar == [NO_PROTOCOL, "BRIDGE", NO_PROTOCOL]


def test_a_rule_naming_a_plugin_with_no_plugin_column_does_not_match() -> None:
    """A rule that cannot be evaluated is not a rule that matched."""
    rules = Rules(rules=[Rule(protocol="BRIDGE", plugin_pattern="^ULBridge$")])
    assert rules.into_arrow_protocol_array(pyarrow.array(["a"])).to_pylist() == [NO_PROTOCOL]
    assert rules.categorise("a").protocol == NO_PROTOCOL


def test_a_codec_says_how_a_line_of_that_protocol_is_read() -> None:
    assert CODEC_KEYS[DEFAULT.rule("FIX").codec] is False
    assert CODEC_KEYS[DEFAULT.rule("UL").codec] is True
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


def test_configured_entry_separator_candidates_round_trip_as_literals() -> None:
    configured = Rules.from_dict(
        {
            "rules": [
                {
                    "protocol": "VENDOR",
                    "codec": "ul",
                    "extra_entry_separators": [".*", "\x1e\x1f"],
                }
            ]
        }
    )

    assert configured.rule("VENDOR").extra_entry_separators == (".*", "\x1e\x1f")
    assert Rules.from_dict(configured.into_dict()) == configured


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
    assert Rule.into_field().field("plugin_pattern").nullable
    assert not Rule.into_field().field("protocol").nullable
