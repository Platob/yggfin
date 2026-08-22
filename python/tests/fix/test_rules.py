"""Which category a line is, decided the same way one at a time and a column at a time."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fix import BEGIN_STRING, BRIDGE, Rule, Rules
from rekep.fix.rules import CODE, DEFAULT_RULES, NAMED, OTHER

SOH = "\x01"

#: One message of each category, in the spellings the sample capture uses.
LINES = {
    "After Enrichment -> ACCOUNT=ACCT-000117 CLIENTID=MCFP2 VENUE=XPAR": "OTHER",
    "sending >> 8=FIX.4.2|9=176|35=D|10=203| << queued seq=1092": "FIX",
    "recv 8=FIX4^A9=61^A35=0^A10=017^A on session 3": "FIX",
    f"raw 8=FIX.4.4{SOH}9=224{SOH}35=8{SOH}10=118{SOH}": "FIX",
    "toBridge #ISINCODE=XX|#SYMBOL=TTF|#SIDE=1": "UL",
    "Message rejected because : ignoring OMSSales expiry message": "OTHER",
    "no level printed by this driver": "OTHER",
}

#: Derived from the rule set, then pinned, so a renamed built-in cannot move
#: both sides of the assertions below together.
EXPECTED_RULES = 3


def test_the_default_set_is_the_three_built_ins() -> None:
    assert len(DEFAULT_RULES) == EXPECTED_RULES
    assert [rule.name for rule in Rules.DEFAULT.rules] == ["FIX", "UL", "OTHER"]
    assert [rule.category_id for rule in Rules.DEFAULT.rules] == [1, 2, 0]


def test_the_built_in_patterns_are_the_parser_s_own() -> None:
    """One answer to "where does a message start", not two that drift apart."""
    assert Rules.DEFAULT.rule(1).pattern == BEGIN_STRING
    assert Rules.DEFAULT.rule(2).pattern == BRIDGE


@pytest.mark.parametrize(("message", "expected"), LINES.items(), ids=lambda v: str(v)[:28])
def test_a_line_lands_in_the_category_the_rules_claim(message: str, expected: str) -> None:
    assert Rules.DEFAULT.categorise(message).name == expected


def test_the_column_agrees_with_the_line_for_line_reading() -> None:
    """The two readings are contracted to agree, so they are compared here."""
    ids, names = Rules.DEFAULT.into_arrow_category_arrays(pyarrow.array(list(LINES)))
    scalar = [Rules.DEFAULT.categorise(line) for line in LINES]
    assert ids.to_pylist() == [rule.category_id for rule in scalar]
    assert names.to_pylist() == [rule.name for rule in scalar]
    assert ids.type == CODE


def test_a_lone_marked_key_in_prose_is_not_a_bridge_message() -> None:
    """Two `#NAME=` tokens or it is a sentence, which is what the rule says."""
    assert Rules.DEFAULT.categorise("retry #FOO=bar and move on").name == "OTHER"
    assert Rules.DEFAULT.categorise("send #FOO=bar #BAZ=1").name == "UL"


def test_a_null_message_is_other_rather_than_null() -> None:
    """`category_id` is NOT NULL, so a null payload must not propagate into it."""
    ids, names = Rules.DEFAULT.into_arrow_category_arrays(
        pyarrow.array([None, "heartbeat"], pyarrow.string())
    )
    assert ids.to_pylist() == [0, 0]
    assert names.to_pylist() == ["OTHER", "OTHER"]
    assert Rules.DEFAULT.categorise(None) is OTHER


def test_no_rows_is_no_rows() -> None:
    ids, names = Rules.DEFAULT.into_arrow_category_arrays(pyarrow.array([], pyarrow.string()))
    assert len(ids) == 0 and ids.type == CODE
    assert len(names) == 0


def test_the_first_rule_that_matches_wins() -> None:
    """Which is what lets a specific rule sit in front of a general one."""
    rules = Rules(
        rules=[
            Rule(name="SESSION", category_id=9, pattern=r"35=0", codec="fix"),
            *DEFAULT_RULES,
        ]
    )
    line = "recv 8=FIX.4.4|35=0|10=017|"
    assert rules.categorise(line).name == "SESSION"
    ids, _ = rules.into_arrow_category_arrays(pyarrow.array([line]))
    assert ids.to_pylist() == [9]


def test_a_rule_may_be_told_apart_by_its_driver() -> None:
    rules = Rules(rules=[Rule(name="BRIDGE", category_id=7, driver_pattern="^ULBridge$")])
    assert rules.categorise("anything", "ULBridge").name == "BRIDGE"
    assert rules.categorise("anything", "FixSession_XPAR").name == "OTHER"
    ids, _ = rules.into_arrow_category_arrays(
        pyarrow.array(["a", "b"]), pyarrow.array(["ULBridge", "other"])
    )
    assert ids.to_pylist() == [7, 0]


def test_a_rule_naming_a_driver_with_no_driver_column_does_not_match() -> None:
    """A rule that cannot be evaluated is not a rule that matched."""
    rules = Rules(rules=[Rule(name="BRIDGE", category_id=7, driver_pattern="^ULBridge$")])
    ids, _ = rules.into_arrow_category_arrays(pyarrow.array(["a"]))
    assert ids.to_pylist() == [0]
    assert rules.categorise("a").name == "OTHER"


def test_a_codec_says_how_a_line_of_that_category_is_read() -> None:
    assert NAMED[Rules.DEFAULT.rule(1).codec] is False
    assert NAMED[Rules.DEFAULT.rule(2).codec] is True
    assert Rules.DEFAULT.rule(0).named is None, "and OTHER is not read at all"


def test_an_unknown_category_id_reads_back_as_other() -> None:
    assert Rules.DEFAULT.rule(404) is OTHER


def test_a_rule_set_round_trips_as_a_document(tmp_path: Path) -> None:
    """A rule set is data, so it has to survive being written down."""
    path = tmp_path / "rules.yml"
    Rules.DEFAULT.into_yaml(path)
    assert Rules.from_yaml(path) == Rules.DEFAULT


def test_a_loaded_rule_set_overrides_the_default(tmp_path: Path) -> None:
    """A desk with its own bridge writes a document rather than patching this."""
    path = tmp_path / "rules.yml"
    Rules(
        rules=[
            Rule(name="OWN", category_id=42, pattern=r"toBridge", codec="ul", separator="|"),
            OTHER,
        ]
    ).into_yaml(path)
    loaded = Rules.from_yaml(path)
    line = "toBridge #ISINCODE=XX|#SYMBOL=TTF"
    assert Rules.DEFAULT.categorise(line).name == "UL"
    assert loaded.categorise(line).name == "OWN"
    assert loaded.categorise(line).separator == "|"
    ids, names = loaded.into_arrow_category_arrays(pyarrow.array([line, "prose"]))
    assert ids.to_pylist() == [42, 0]
    assert names.to_pylist() == ["OWN", "OTHER"]


def test_a_rule_is_a_field_class_like_every_other_declaration() -> None:
    """Which is what puts a rule set beside the schema contracts."""
    assert Rule.FIELD.name == "Rule"
    assert Rule.FIELD.names[:3] == ["name", "category_id", "pattern"]
    assert Rule.FIELD.field("driver_pattern").nullable
    assert not Rule.FIELD.field("category_id").nullable
