"""Which protocol a line carries, decided once from the keys its payload holds."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest
from pyiceberg.expressions import And, EqualTo, In, Not, Or

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
#: Three key-shaped protocols are told apart by the keys their payload holds;
#: XML is told by its document envelope.
LINES = {
    "sending >> 8=FIX.4.2|9=176|35=D|10=203| << queued seq=1092": "FIX",
    "recv 8=FIX4^A9=61^A35=0^A10=017^A on session 3": "FIX",
    f"raw 8=FIX.4.4{SOH}9=224{SOH}35=8{SOH}10=118{SOH}": "FIX",
    "8=FIX.4.4|35=8|58=quoting #A=1 and #B=2|10=1|": "FIX",
    "sending >> 8=FIX.4.2|35=UL|#SYMBOL=TTF|#SIDE=1|10=044|": "FIXML",
    "8=FIX.4.4|35=D|11=ORDER-1|SYMBOL=AAPL|SIDE=1|10=000": "FIXML",
    "toBridge #ISINCODE=XX|#SYMBOL=TTF|#SIDE=1": "UL",
    "ACCOUNT=A1|MSGTYPE=D|CLORDID=ORDER-1|SYMBOL=AAPL|SIDE=1": "UL",
    "Referential(dbi|equity|dbi;GB00BN7SWP63_XLON_GBX|[quantity-type=])": "REFER",
    "<Order ClOrdID='XML-1'><Instrument><Symbol>IBM</Symbol></Instrument></Order>": "XML",
    "Receiving XmlApi: <Execution ExecID='E1'><LastQty>2</LastQty></Execution>": "XML",
    "After Enrichment -> ACCOUNT=ACCT-000117 CLIENTID=MCFP2 VENUE=XPAR": "UL",
    "Message rejected because : ignoring OMSSales expiry message": "OTHER",
    "no level printed by this plugin": "OTHER",
    "heartbeat emitted seq=7": "MISC",
}

#: Derived from the rule set, then pinned, so a renamed built-in cannot move
#: both sides of the assertions below together.
EXPECTED_RULES = 7
EXPECTED_PROTOCOLS = 7
DEFAULT = Rules.into_default()


def protocols_of(*messages: str | None) -> list[str]:
    """The protocol each message carries, spelled out, through the one classifier.

    The column holds packed codes, so reading them back as their spellings is
    both what a consumer does and a round trip of the packing.
    """
    return spelled(DEFAULT.into_arrow_protocol_array(pyarrow.array(messages, pyarrow.string())))


def spelled(protocols: pyarrow.Array) -> list[str]:
    """One protocol column read back as the names it packs."""
    return [Protocol.from_stored(code).code for code in protocols.to_pylist()]


def packed(*protocols: str | None) -> pyarrow.Array:
    """One protocol column as the classifier writes it: packed codes, never names."""
    return pyarrow.array(
        [None if name is None else Protocol.from_str(name).into_stored() for name in protocols],
        Protocol.into_storage_type(),
    )


def test_the_default_set_is_the_built_ins_in_order() -> None:
    """Structured rules lead, so payload prose cannot replace its envelope."""
    assert len(DEFAULT_RULES) == EXPECTED_RULES
    assert [rule.protocol.code for rule in DEFAULT.rules] == [
        "FIX",
        "FIXML",
        "XML",
        "REFER",
        "UL",
        "MISC",
        "OTHER",
    ]
    assert len({rule.protocol for rule in DEFAULT_RULES}) == EXPECTED_PROTOCOLS
    assert {rule.protocol for rule in DEFAULT_RULES} == set(Protocol) - {Protocol.UNKNOWN}
    assert OTHER.protocol is Protocol.OTHER
    assert [rule.codec for rule in DEFAULT.rules] == [
        "fix",
        "fixml",
        "xml",
        "ul",
        "ul",
        "none",
        "none",
    ]


@pytest.mark.parametrize(("message", "expected"), LINES.items(), ids=lambda v: str(v)[:28])
def test_a_line_lands_in_the_protocol_its_keys_claim(message: str, expected: str) -> None:
    assert protocols_of(message) == [expected]


def test_a_batch_classifies_every_row_where_it_stands() -> None:
    """One pass over a mixed batch, and every row keeps its own position."""
    assert protocols_of(*LINES) == list(LINES.values())


def test_an_unmarked_rendered_prefix_keeps_every_field() -> None:
    line = "After Enrichment -> ACCOUNT=ACCT-000117 CLIENTID=MCFP2 VENUE=XPAR"

    found = Message.parse_arrow(pyarrow.array([line]))
    entries = found["entries"][0].as_py()

    assert [entry["key"] for entry in entries] == ["ACCOUNT", "CLIENTID", "VENUE"]
    assert protocols_of(line) == ["UL"]


@pytest.mark.parametrize(
    ("message", "expected", "msgtype"),
    [
        ("8=FIX.4.4|35=D|11=ORDER-1|55=AAPL|54=1|38=10|10=000", "FIX4.4", "D"),
        (
            SOH.join(("8=FIX.4.2", "35=8", "150=2", "39=2", "10=000")),
            "FIX4.2",
            "8",
        ),
        ("8=FIX.4.2|35=UL|49=A|56=B|10=000", "FIX4.2", "UL"),
        (
            "8=FIX.4.4|35=D|11=ORDER-1|SYMBOL=AAPL|SIDE=1|10=000",
            "FIXML4.4",
            "D",
        ),
        ("8=FIX.4.2|35=UL|#SYMBOL=TTF|#SIDE=1|10=044", "FIXML4.2", "UL"),
        (
            "ACCOUNT=A1|MSGTYPE=D|CLORDID=ORDER-1|SYMBOL=AAPL|SIDE=1",
            "UL5SP2",
            "D",
        ),
        ("EXECTYPE=fill|CURRENCY=NOK|COUNTERAMOUNT=1200", "UL5SP2", None),
        ("MSGTYPE=UL|ACCOUNT=A1|SYMBOL=AAPL", "UL5SP2", "UL"),
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
    """The grammar and its resolved version share one protocol code."""
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
    for line in LINES:
        parsed = Message.parse_arrow(pyarrow.array([line], pyarrow.string()))
        assert Message(body=line).protocol.code == spelled(parsed["protocol"])[0]


def test_xml_inside_a_fix_value_does_not_replace_the_fix_envelope() -> None:
    line = "8=FIX.4.4|35=D|58=<Order ClOrdID='not-an-envelope'/>|10=000|"
    assert protocols_of(line) == ["FIX"]


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
    first.rules[5].pattern = "first only"
    assert second.rules[5].pattern != "first only"
    assert DEFAULT.rules[5].pattern != "first only"
    assert MISC.pattern != "first only"
    assert first.rules[5] is not second.rules[5]


def test_a_null_message_is_other_rather_than_null() -> None:
    """`protocol` is NOT NULL, so a null payload must not propagate into it."""
    protocols = DEFAULT.into_arrow_protocol_array(
        pyarrow.array([None, "heartbeat"], pyarrow.string())
    )
    assert protocols.to_pylist() == [
        Protocol.OTHER.into_stored(),
        Protocol.MISC.into_stored(),
    ]
    assert protocols.null_count == 0


def test_an_empty_pattern_matches_every_nonnull_message() -> None:
    rules = Rules(rules=[Rule(protocol="ALL")])
    messages = pyarrow.array([None, ""])
    assert spelled(rules.into_arrow_protocol_array(messages)) == ["OTHER", "ALL"]


def test_a_rule_that_claims_every_row_leaves_the_rest_of_the_set_nothing() -> None:
    """Classification narrows, so the column can run out before the rules do."""
    rules = Rules(rules=[Rule(protocol="ALL", pattern=r"\bready\b"), *DEFAULT_RULES])
    messages = pyarrow.array(["ready 8=FIX.4.4|35=D|", "heartbeat ready"])
    assert spelled(rules.into_arrow_protocol_array(messages)) == ["ALL", "ALL"]


def test_no_rows_is_no_rows() -> None:
    protocols = DEFAULT.into_arrow_protocol_array(pyarrow.array([], pyarrow.string()))
    assert len(protocols) == 0
    assert protocols.type == Protocol.into_storage_type()

    directions = DEFAULT.into_arrow_direction_array(pyarrow.array([], pyarrow.string()), protocols)
    assert len(directions) == 0
    assert directions.type == pyarrow.int32()

    assert len(payload_shapes(pyarrow.array([], Message.into_field().field("entries").dtype))) == 0


def test_direction_is_an_open_packed_vocabulary() -> None:
    assert int(Direction.SENT) == int.from_bytes(b"SENT", "big", signed=True)
    assert int(Direction.RECV) == int.from_bytes(b"RECV", "big", signed=True)
    assert Direction.from_str("sent") is Direction.SENT
    assert Direction.from_int(int.from_bytes(b"NOPE", "big", signed=True)).code == "NOPE"
    assert Direction.register("BOTH") is Direction.from_str("BOTH")


def test_every_structured_protocol_reads_direction_the_same_way() -> None:
    """One anchor per codec, so a verb answers in front of every payload shape."""
    assert set(CODEC_ANCHORS) == {*SHAPES, "xml"}
    assert set(DEFAULT._anchors()) == {
        Protocol.XML,
        Protocol.FIX,
        Protocol.FIXML,
        Protocol.REFERENTIAL,
        Protocol.UL,
    }


def test_an_in_or_out_marker_is_a_direction_and_the_same_letters_in_prose_are_not() -> None:
    """A bridge that marks the way with `IN`/`OUT` says it as plainly as one
    that spells `Receiving`. The marker is the line or a bracket opening on it
    and a delimiter closing it, because the same two letters are also English,
    a route endpoint and half a session name -- and one of those in front of
    the opposite verb would turn a right answer into no answer at all."""
    marked = [
        ("IN 8=FIX.4.4|35=D|11=A|10=000|", Direction.RECV),
        ("OUT >> 8=FIX.4.4|35=D|11=B|10=000|", Direction.SENT),
        ("[IN] 8=FIX.4.4|35=D|11=C|10=000|", Direction.RECV),
        ("incoming 8=FIX.4.4|35=D|11=D|10=000|", Direction.RECV),
        ("outbound 8=FIX.4.4|35=D|11=E|10=000|", Direction.SENT),
        # The same letters, not marking anything -- and each of these carries
        # the opposite verb, so a looser rule would answer UNKNOWN here.
        ("sending in session 3 >> 8=FIX.4.4|35=D|11=F|10=000|", Direction.SENT),
        ("Received out of order 8=FIX.4.4|35=8|11=G|10=000|", Direction.RECV),
        ("Receiving from direct:out 8=FIX.4.4|35=8|11=H|10=000|", Direction.RECV),
        # And no marker at all is no answer, however the letters fall.
        ("[MCFID-IN-XPAR] 8=FIX.4.4|35=D|11=I|10=000|", Direction.UNKNOWN),
        ("(INFO) [Fix_In] 8=FIX.4.4|35=D|11=J|10=000|", Direction.UNKNOWN),
        ("[IN] [OUT] 8=FIX.4.4|35=D|11=K|10=000|", Direction.UNKNOWN),
    ]
    messages = pyarrow.array([line for line, _ in marked], pyarrow.string())

    directions = DEFAULT.into_arrow_direction_array(messages, packed(*["FIX"] * len(marked)))

    assert directions.to_pylist() == [int(expected) for _, expected in marked]


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


def test_category_filters_are_one_exact_iceberg_partition() -> None:
    versions = ("4.4", "5.0.SP2")
    market = In("eventtype", EventType.ranked_at_least(EventType.INTENT))
    protocols = {protocol.into_stored() for protocol in DEFAULT.protocols}
    protocols.update(
        Protocol.with_version(protocol, version).into_stored()
        for protocol in DEFAULT.protocols
        for version in versions
    )
    known_non_market = Or(
        EqualTo("eventtype", int(EventType.MISC)),
        In("protocol", protocols),
    )

    assert DEFAULT.into_iceberg_category_filter(MARKET_CATEGORY, versions) == market
    assert DEFAULT.into_iceberg_category_filter(MISC_CATEGORY, versions) == And(
        Not(market), known_non_market
    )
    assert DEFAULT.into_iceberg_category_filter(UNKNOWN_CATEGORY, versions) == And(
        Not(market), Not(known_non_market)
    )
    with pytest.raises(ValueError, match="category must be one of"):
        DEFAULT.into_iceberg_category_filter("other")


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
    assert CODECS == ("fix", "fixml", "ul", "xml", "none")
    assert CODEC_KEYS[DEFAULT.rule("FIX").codec] is False
    assert CODEC_KEYS[DEFAULT.rule("FIXML").codec] is True
    assert CODEC_KEYS[DEFAULT.rule("UL").codec] is True
    assert CODEC_KEYS[DEFAULT.rule("REFERENTIAL").codec] is True
    assert CODEC_KEYS[DEFAULT.rule("XML").codec] is True
    assert DEFAULT.rule(Protocol.OTHER).named is None, "and OTHER is not read at all"


def test_the_ul_rule_promotes_its_detailed_cfi_reading() -> None:
    assert DEFAULT.rule("UL").pop == {"DetailedCFICode": "CFICode"}
    assert DEFAULT.rule("REFERENTIAL").pop == {"DetailedCFICode": "CFICode"}


def test_a_pop_rule_requires_two_distinct_field_names() -> None:
    with pytest.raises(ValueError, match="non-empty source and target"):
        Rule(protocol="UL", codec="ul", pop={"": "CFICode"})
    with pytest.raises(ValueError, match="cannot replace itself"):
        Rule(protocol="UL", codec="ul", pop={"CFI_Code": "cficode"})


def test_a_versioned_protocol_uses_its_family_rule() -> None:
    assert DEFAULT.rule("FIX4.4") is DEFAULT.rule(Protocol.FIX)
    assert DEFAULT.rule("FIX.5.0.SP2") is DEFAULT.rule(Protocol.FIX)
    assert DEFAULT.rule("FIXML.5.0.SP2") is DEFAULT.rule(Protocol.FIXML)
    assert Protocol.UL in DEFAULT.protocols


@pytest.mark.parametrize(
    ("declared", "family"),
    [("FIX4.4", Protocol.FIX), ("FIX.5.0.SP2", Protocol.FIX), ("FIXML4.4", Protocol.FIXML)],
)
def test_a_versioned_rule_declaration_stores_its_family(declared: str, family: Protocol) -> None:
    rule = Rule(protocol=declared, codec="fix")

    assert rule.protocol is family
    assert rule.into_dict()["protocol"] == family.code
    assert Rules(rules=[rule, OTHER]).rule(declared) is rule


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


def test_a_protocol_is_an_open_vocabulary_of_sixteen_ascii_bytes() -> None:
    """A desk's own name is a code without a release here -- up to the width
    the column stores, and a rule that names a wider one is refused rather
    than quietly collapsing onto `UNKNOWN` beside every other over-long name."""
    own = Rule(protocol="venue", codec="ul")
    assert own.protocol is Protocol.from_str("VENUE")
    assert own.protocol.into_stored() == b"VENUE".ljust(16, b"\0")
    with pytest.raises(ValueError, match=r"sixteen bytes of \[A-Z0-9"):
        Rule(protocol="VENUE-BRIDGE-LONG")
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
    assert b"protocol: XML\n" in DEFAULT.into_yaml()
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
    assert Rule.into_field().field("protocol").dtype == Protocol.into_storage_type()
