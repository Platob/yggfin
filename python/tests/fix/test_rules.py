"""Which protocol a line carries, decided once from the keys its payload holds."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.enums.codes import Direction, Protocol
from rekep.fix import Rule, Rules
from rekep.fix.rules import (
    CODEC_ANCHORS,
    CODEC_KEYS,
    CODECS,
    DEFAULT_RULES,
    MARKET_CATEGORY,
    MISC,
    MISC_CATEGORY,
    OTHER,
    SHAPES,
    UNKNOWN_CATEGORY,
    joined_pattern,
    payload_shapes,
)
from rekep.market import EventType
from rekep.text import Message

SOH = "\x01"

#: One message of each protocol, in the spellings the sample capture uses. The
#: three structured protocols are told apart by the keys their payload holds:
#: numbered tags alone, named keys alone, or both together.
LINES = {
    "sending >> 8=FIX.4.2|9=176|35=D|10=203| << queued seq=1092": "FIX",
    "recv 8=FIX4^A9=61^A35=0^A10=017^A on session 3": "FIX",
    f"raw 8=FIX.4.4{SOH}9=224{SOH}35=8{SOH}10=118{SOH}": "FIX",
    "8=FIX.4.4|35=8|58=quoting #A=1 and #B=2|10=1|": "FIX",
    "sending >> 8=FIX.4.2|35=UL|#SYMBOL=TTF|#SIDE=1|10=044|": "FIXML",
    "8=FIX.4.4|35=D|11=ORDER-1|SYMBOL=AAPL|SIDE=1|10=000": "FIXML",
    "toBridge #ISINCODE=XX|#SYMBOL=TTF|#SIDE=1": "UL",
    "ACCOUNT=A1|MSGTYPE=D|CLORDID=ORDER-1|SYMBOL=AAPL|SIDE=1": "UL",
    "After Enrichment -> ACCOUNT=ACCT-000117 CLIENTID=MCFP2 VENUE=XPAR": "OTHER",
    "Message rejected because : ignoring OMSSales expiry message": "OTHER",
    "no level printed by this plugin": "OTHER",
    "heartbeat emitted seq=7": "MISC",
}

#: Derived from the rule set, then pinned, so a renamed built-in cannot move
#: both sides of the assertions below together.
EXPECTED_RULES = 5
EXPECTED_PROTOCOLS = 5
DEFAULT = Rules.into_default()


def protocols_of(*messages: str | None) -> list[str]:
    """The protocol each message carries, spelled out, through the one classifier.

    The column holds packed codes, so reading them back as their spellings is
    both what a consumer does and a round trip of the packing.
    """
    return spelled(DEFAULT.into_arrow_protocol_array(pyarrow.array(messages, pyarrow.string())))


def spelled(protocols: pyarrow.Array) -> list[str]:
    """One protocol column read back as the names it packs."""
    return [Protocol.from_int(code).code for code in protocols.to_pylist()]


def packed(*protocols: str | None) -> pyarrow.Array:
    """One protocol column as the classifier writes it: packed codes, never names."""
    return pyarrow.array(
        [None if name is None else int(Protocol.from_str(name)) for name in protocols],
        pyarrow.int64(),
    )


def test_the_default_set_is_the_built_ins_in_order() -> None:
    """The three shapes lead, so a FIX frame saying "heartbeat" stays a frame."""
    assert len(DEFAULT_RULES) == EXPECTED_RULES
    assert [rule.protocol.code for rule in DEFAULT.rules] == ["FIX", "FIXML", "UL", "MISC", "OTHER"]
    assert len({rule.protocol for rule in DEFAULT_RULES}) == EXPECTED_PROTOCOLS
    assert {rule.protocol for rule in DEFAULT_RULES} == set(Protocol) - {Protocol.UNKNOWN}
    assert OTHER.protocol is Protocol.OTHER
    assert [rule.codec for rule in DEFAULT.rules] == [*SHAPES, "none", "none"]


@pytest.mark.parametrize(("message", "expected"), LINES.items(), ids=lambda v: str(v)[:28])
def test_a_line_lands_in_the_protocol_its_keys_claim(message: str, expected: str) -> None:
    assert protocols_of(message) == [expected]


def test_a_batch_classifies_every_row_where_it_stands() -> None:
    """One pass over a mixed batch, and every row keeps its own position."""
    assert protocols_of(*LINES) == list(LINES.values())


@pytest.mark.parametrize(
    ("message", "expected", "msgtype"),
    [
        ("8=FIX.4.4|35=D|11=ORDER-1|55=AAPL|54=1|38=10|10=000", "FIX", "D"),
        (SOH.join(("8=FIX.4.2", "35=8", "150=2", "39=2", "10=000")), "FIX", "8"),
        ("8=FIX.4.2|35=UL|49=A|56=B|10=000", "FIX", "UL"),
        ("8=FIX.4.4|35=D|11=ORDER-1|SYMBOL=AAPL|SIDE=1|10=000", "FIXML", "D"),
        ("8=FIX.4.2|35=UL|#SYMBOL=TTF|#SIDE=1|10=044", "FIXML", "UL"),
        ("ACCOUNT=A1|MSGTYPE=D|CLORDID=ORDER-1|SYMBOL=AAPL|SIDE=1", "UL", "D"),
        ("EXECTYPE=fill|CURRENCY=NOK|COUNTERAMOUNT=1200", "UL", None),
        ("MSGTYPE=UL|ACCOUNT=A1|SYMBOL=AAPL", "UL", "UL"),
    ],
    ids=(
        "wire-tags",
        "wire-soh",
        "wire-35-UL",
        "wire-with-named",
        "wire-with-marked",
        "named-document",
        "named-without-msgtype",
        "named-msgtype-UL",
    ),
)
def test_the_shape_decides_the_protocol_and_msgtype_stays_its_own(
    message: str, expected: str, msgtype: str | None
) -> None:
    """`35=UL` is a MsgType. A numbered-only frame carrying it is FIX; the same
    frame with a named payload beside it is FIXML; a bare named document is UL
    whatever its `MSGTYPE` says. The discriminator survives all three."""
    parsed = Message.parse_arrow(pyarrow.array([message], pyarrow.string()))
    assert spelled(parsed["protocol"]) == [expected]
    assert parsed["msgtype"].to_pylist() == [msgtype]


def test_a_named_document_inside_xmldata_makes_the_frame_mixed() -> None:
    """`XmlData <213>` carries the message, so its named keys are the frame's."""
    document = "EXECTYPE=fill|CURRENCY=NOK|COUNTERAMOUNT=1200"
    framed = f"8=FIX.4.2|35=UL|212={len(document)}|213={document}|10=000"
    assert protocols_of(framed) == ["FIXML"]
    assert protocols_of("8=FIX.4.2|35=UL|213=<order id='1'/>|10=000") == ["FIX"]


def test_a_value_full_of_digits_is_still_a_value() -> None:
    """Classification reads the key, so what a value holds decides nothing."""
    assert protocols_of("COUNTERAMOUNT=1200|QTY=10|PRICE=41.25") == ["UL"]
    assert protocols_of("8=FIX.4.4|35=8|58=quoting #A=1 and #B=2|10=1|") == ["FIX"]


def test_the_scalar_row_and_the_column_agree() -> None:
    """One classifier: a row built from text answers what its batch answers."""
    for line, expected in LINES.items():
        assert Message(message=line).protocol.code == expected


def test_a_declared_pattern_decides_instead_of_the_shape() -> None:
    """Which is what lets a session rule sit in front of the general one."""
    rules = Rules(rules=[Rule(protocol="SESSION", pattern=r"35=0", codec="fix"), *DEFAULT_RULES])
    line = "recv 8=FIX.4.4|35=0|10=017|"
    assert spelled(rules.into_arrow_protocol_array(pyarrow.array([line]))) == ["SESSION"]
    assert protocols_of(line) == ["FIX"], "and the default set still says FIX"


def test_a_pattern_rule_needs_no_shape() -> None:
    """`joined_pattern` is the one spelling for "any of these"."""
    rules = Rules(
        rules=[Rule(protocol="OPS", pattern=joined_pattern(r"\bready\b", r"\bstopped\b")), OTHER]
    )
    messages = pyarrow.array(["service ready", "service stopped", "service busy"])
    assert spelled(rules.into_arrow_protocol_array(messages)) == ["OPS", "OPS", "OTHER"]


def test_joined_pattern_scopes_a_branch_s_leading_flags() -> None:
    """A global `(?i)` is illegal mid-pattern in Python `re`; the join rewrites
    it into the scoped form both engines accept, and the flag stays local to
    its own branch rather than leaking across the alternation."""
    joined = joined_pattern(r"(?i)ready", r"stopped")
    assert joined == r"(?i:ready)|(?:stopped)"
    rules = Rules(rules=[Rule(protocol="OPS", pattern=joined), OTHER])
    lines = ["service READY", "service STOPPED", "service stopped"]
    assert spelled(rules.into_arrow_protocol_array(pyarrow.array(lines))) == [
        "OPS",
        "OTHER",
        "OPS",
    ]
    assert joined_pattern("", r"x", "") == r"(?:x)", "empty branches are no branches"
    assert joined_pattern("") == ""


def test_joined_branches_keep_their_named_groups_apart() -> None:
    """Two branches spelling the same capture name matched fine as separate
    patterns; the join renames per branch rather than becoming a pattern
    neither engine accepts. One branch stays verbatim."""
    joined = joined_pattern(r"(?P<v>ready)", r"(?P<v>stopped)")
    assert joined == r"(?:(?P<j0_v>ready))|(?:(?P<j1_v>stopped))"
    rules = Rules(rules=[Rule(protocol="OWN", pattern=joined), OTHER])
    lines = ["ready", "stopped", "busy"]
    assert spelled(rules.into_arrow_protocol_array(pyarrow.array(lines))) == [
        "OWN",
        "OWN",
        "OTHER",
    ]
    assert joined_pattern(r"(?P<v>x)") == r"(?:(?P<v>x))"


def test_dot_does_not_cross_a_newline_unless_the_pattern_requests_it() -> None:
    messages = pyarrow.array(["a\nb"])
    for pattern, expected in ((r"a.b", "OTHER"), (r"(?s)a.b", "OWN")):
        rules = Rules(rules=[Rule(protocol="OWN", pattern=pattern)])
        assert spelled(rules.into_arrow_protocol_array(messages)) == [expected]


def test_the_misc_rule_recognises_known_operational_lines() -> None:
    assert protocols_of("heartbeat 7", "connection established", "opaque status") == [
        "MISC",
        "MISC",
        "OTHER",
    ]
    assert DEFAULT.rule("MISC") == MISC


def test_a_lone_marked_key_in_prose_is_not_a_document() -> None:
    """Two tokens or it is a sentence, which is what the payload rule says."""
    assert protocols_of("retry #FOO=bar and move on", "send #FOO=bar #BAZ=1") == ["MISC", "UL"]


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
    assert protocols.to_pylist() == [int(Protocol.OTHER), int(Protocol.MISC)]
    assert protocols.null_count == 0


def test_an_empty_pattern_matches_every_nonnull_message() -> None:
    rules = Rules(rules=[Rule(protocol="ALL")])
    messages = pyarrow.array([None, ""])
    assert spelled(rules.into_arrow_protocol_array(messages)) == ["OTHER", "ALL"]


def test_no_rows_is_no_rows() -> None:
    protocols = DEFAULT.into_arrow_protocol_array(pyarrow.array([], pyarrow.string()))
    assert len(protocols) == 0
    assert protocols.type == pyarrow.int64()

    directions = DEFAULT.into_arrow_direction_array(pyarrow.array([], pyarrow.string()), protocols)
    assert len(directions) == 0
    assert directions.type == pyarrow.int32()

    assert len(payload_shapes(pyarrow.array([], Message.into_field().field("entries").dtype))) == 0


def test_direction_is_a_closed_packed_vocabulary() -> None:
    assert int(Direction.SENT) == int.from_bytes(b"SENT", "big", signed=True)
    assert int(Direction.RECV) == int.from_bytes(b"RECV", "big", signed=True)
    assert Direction.from_str("sent") is Direction.SENT
    assert Direction.from_int(int.from_bytes(b"NOPE", "big", signed=True)) is Direction.UNKNOWN
    with pytest.raises(TypeError, match="closed set"):
        Direction.register("BOTH")


def test_every_structured_protocol_reads_direction_the_same_way() -> None:
    """One anchor per codec, so a verb answers in front of any of the three."""
    assert set(CODEC_ANCHORS) == set(SHAPES)
    assert set(DEFAULT._anchors()) == {Protocol.FIX, Protocol.FIXML, Protocol.UL}


def test_direction_words_produce_packed_codes_before_the_payload() -> None:
    messages = pyarrow.array(
        [
            "Receiving : 8=FIX.4.4|35=D|11=A|10=000|",
            "Sending : 8=FIX.4.4|35=D|11=B|10=000|",
            "Received then sent 8=FIX.4.4|35=D|11=C|10=000|",
            "8=FIX.4.4|35=8|58=order sent late|10=000|",
            "Sending an operational status",
            "Receiving : ACCOUNT=A1|SYMBOL=TTF",
            None,
        ],
        pyarrow.string(),
    )
    protocols = packed("FIX", "FIX", "FIX", "FIX", "OTHER", "UL", "FIX")

    directions = DEFAULT.into_arrow_direction_array(messages, protocols)

    assert directions.type == pyarrow.int32()
    assert directions.to_pylist() == [
        int(Direction.RECV),
        int(Direction.SENT),
        int(Direction.UNKNOWN),
        int(Direction.UNKNOWN),
        int(Direction.UNKNOWN),
        int(Direction.RECV),
        int(Direction.UNKNOWN),
    ]


def test_categories_agree_one_row_and_one_column_at_a_time() -> None:
    protocols = ["FIX", "OTHER", "FIXML", "UL", "OTHER", "SBE", None]
    eventtypes = [
        EventType.ORDER,
        EventType.MISC,
        EventType.UNKNOWN,
        EventType.UNKNOWN,
        EventType.UNKNOWN,
        0,
        None,
    ]
    scalar = [
        DEFAULT.category_of(protocol, eventtype)
        for protocol, eventtype in zip(protocols, eventtypes, strict=True)
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
        packed(*protocols),
        pyarrow.array([None if eventtype is None else int(eventtype) for eventtype in eventtypes]),
    )
    assert vector.to_pylist() == scalar


def test_categories_agree_on_codes_no_member_spells() -> None:
    """Case-variant packed bytes, a previous release's ordinal ids, junk:
    the scalar rule and the kernel answer identically on every one, because
    `from_int` answers only on the compiled codes the kernel's sets hold."""
    respelled = int.from_bytes(b"order", "big", signed=True)
    eventtypes = [respelled, 110, 210, 410, 999, -1, 0]
    for protocol in (None, "FIX"):
        scalar = [DEFAULT.category_of(protocol, eventtype) for eventtype in eventtypes]
        vector = DEFAULT.into_arrow_category_array(
            packed(*[protocol] * len(eventtypes)),
            pyarrow.array(eventtypes, pyarrow.int64()),
        )
        assert vector.to_pylist() == scalar
    assert DEFAULT.category_of("FIX", respelled) == MISC_CATEGORY
    assert DEFAULT.category_of(None, respelled) == UNKNOWN_CATEGORY


def test_a_rule_may_be_told_apart_by_its_plugin() -> None:
    rules = Rules(rules=[Rule(protocol="BRIDGE", plugin_pattern="^ULBridge$")])
    messages = pyarrow.array([None, "a", "b"])
    plugins = pyarrow.array(["ULBridge", "ULBridge", "other"])
    protocols = rules.into_arrow_protocol_array(messages, plugins)
    assert spelled(protocols) == ["OTHER", "BRIDGE", "OTHER"]


def test_a_rule_naming_a_plugin_with_no_plugin_column_does_not_match() -> None:
    """A rule that cannot be evaluated is not a rule that matched."""
    rules = Rules(rules=[Rule(protocol="BRIDGE", plugin_pattern="^ULBridge$")])
    assert spelled(rules.into_arrow_protocol_array(pyarrow.array(["a"]))) == ["OTHER"]


def test_a_codec_says_how_a_line_of_that_protocol_is_read() -> None:
    assert CODECS == ("fix", "fixml", "ul", "none")
    assert CODEC_KEYS[DEFAULT.rule("FIX").codec] is False
    assert CODEC_KEYS[DEFAULT.rule("FIXML").codec] is True
    assert CODEC_KEYS[DEFAULT.rule("UL").codec] is True
    assert DEFAULT.rule(Protocol.OTHER).named is None, "and OTHER is not read at all"


def test_an_unknown_protocol_reads_back_as_other() -> None:
    """A batch may carry a name this set has lost, and it still has to be read."""
    assert DEFAULT.rule("SBE") is OTHER
    assert DEFAULT.rule("") is OTHER
    assert DEFAULT.rule("a name no column could hold") is OTHER


def test_a_slice_is_read_back_by_the_code_its_column_stores() -> None:
    """Which is how the batch path asks: `groups_of` yields packed codes."""
    assert DEFAULT.rule(int(Protocol.FIX)).codec == "fix"
    assert DEFAULT.rule(Protocol.MISC) == MISC
    assert DEFAULT.rule(int(Protocol.from_str("SBE"))) is OTHER


def test_a_protocol_is_an_open_vocabulary_of_eight_ascii_bytes() -> None:
    """A desk's own name is a code without a release here -- up to the width
    the column stores, and a rule that names a wider one is refused rather
    than quietly collapsing onto `UNKNOWN` beside every other over-long name."""
    own = Rule(protocol="venue", codec="ul")
    assert own.protocol is Protocol.from_str("VENUE")
    assert int(own.protocol) == int.from_bytes(b"VENUE\0\0\0", "big", signed=True)
    with pytest.raises(ValueError, match="eight printable ASCII bytes"):
        Rule(protocol="VENUEBRIDGE")
    with pytest.raises(ValueError, match="no protocol name"):
        Rule(protocol="")


def test_a_rule_set_round_trips_as_a_document(tmp_path: Path) -> None:
    """A rule set is data, so it has to survive being written down."""
    path = tmp_path / "rules.yml"
    DEFAULT.into_yaml(path)
    assert Rules.from_yaml(path) == DEFAULT


def test_a_written_rule_spells_its_protocol_rather_than_packing_it() -> None:
    """The document is hand-edited, and a packed code is nineteen digits of
    nothing to whoever opens the file."""
    assert DEFAULT.rules[0].into_dict()["protocol"] == "FIX"
    assert b"protocol: FIX\n" in DEFAULT.into_yaml()
    assert Rules.from_dict({"rules": [{"protocol": "VENUE"}]}).rules[0].protocol.code == "VENUE"


def test_configured_entry_separator_candidates_round_trip_as_literals() -> None:
    configured = Rules.from_dict(
        {
            "rules": [
                {
                    "protocol": "VENDOR",
                    "codec": "fixml",
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
    assert protocols_of(line) == ["UL"]
    assert loaded.rule("OWN").separator == "|"
    protocols = loaded.into_arrow_protocol_array(pyarrow.array([line, "prose"]))
    assert spelled(protocols) == ["OWN", "OTHER"]


def test_a_rule_is_a_field_class_like_every_other_declaration() -> None:
    """Which is what puts a rule set beside the schema contracts."""
    assert Rule.into_field().name == "Rule"
    assert Rule.into_field().names[:2] == ["protocol", "pattern"]
    assert Rule.into_field().field("plugin_pattern").nullable
    assert not Rule.into_field().field("protocol").nullable
    assert Rule.into_field().field("protocol").dtype == Protocol.into_arrow_type().index_type
