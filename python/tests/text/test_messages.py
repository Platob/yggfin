"""The message layer end to end: a capture in, a protocol, tags and flat columns out."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow
import pytest

from rekep.fix import NO_PROTOCOL, FixCodec, FixRegistry, Rule, Rules
from rekep.fix.columns import COLUMNS
from rekep.market.event import HOUR
from rekep.text import HEADER_PATTERN, FixMessage, TextFile, TextFiles
from rekep.times import unix_of

SAMPLE = Path(__file__).parent.parent / "data" / "app_messages_sample.txt"
SAMPLE_BYTES = SAMPLE.read_bytes()

#: The dictionary this repository publishes, beside `python/`.
DATA = Path(__file__).resolve().parents[3] / "data" / "fix.zip"

#: Derived from the fixture, then pinned, so a regression in `HEADER_PATTERN`
#: cannot move both sides of an assertion together.
RECORDS = [line for line in SAMPLE_BYTES.split(b"\n") if HEADER_PATTERN.match(line)]
CONTINUATIONS = [
    line for line in SAMPLE_BYTES.split(b"\n") if line and not HEADER_PATTERN.match(line)
]
EXPECTED_RECORDS = 11
EXPECTED_CONTINUATIONS = 3

#: Which protocol each record carries, in file order. The whole point of the
#: fixture, so it is spelled out rather than derived from the rules it checks.
EXPECTED_PROTOCOLS = [
    "OTHER",
    "OTHER",
    "FIX",
    "FIX",
    "FIX",
    "UL",
    "UL",
    "OTHER",
    "OTHER",
    "OTHER",
    "UL",
]

#: Derived from the bridge line, then pinned: eleven tokens, two of which carry
#: three members each, so a parser that lost one cannot move both sides.
EXPECTED_BRIDGE_PAIRS = 15

#: Where each column of the flat layer lands, derived from the module that
#: declares it. The count and the uniqueness of it are pinned in
#: `tests/fix/test_columns.py` and the hop group's absence in
#: `tests/fix/test_transcribe.py`, so a tag quietly leaving the list cannot
#: take its column's assertions here with it.
FLAT_NAMES = (*COLUMNS.values(), "isincode")

#: Row indexes worth naming. `WRAPPED` is a bridge message inside a FIX
#: envelope -- a wire header and a `#NAME=` body on one line, which answers to
#: both tells and so is the one the rule order exists for.
PIPED, CARET, SOHED, BRIDGE, WRAPPED, REJECTED, HASHED = 2, 3, 4, 5, 6, 7, 10

#: The wire message of the `PIPED` record, cut out of the prose around it, and
#: how many fields it writes between its BeginString and its CheckSum. Derived
#: so that "every field landed somewhere" is checked against the line and not
#: against the parser's own idea of it.
WIRE = RECORDS[PIPED].decode().split("sending >> ")[1].split(" << ")[0]
EXPECTED_WIRE_FIELDS = 15

CARET_RAW_PAIRS = [
    (8, "FIX4"),
    (9, "61"),
    (35, "0"),
    (34, "1093"),
    (49, "XPAR"),
    (56, "BUYSIDE"),
    (52, "20260814-00:05:01.148"),
    (10, "017"),
]

BRIDGE_RAW_PAIRS = [
    ("ISINCODE", "XX0000084733"),
    ("CFICODE", "FXXXSX"),
    ("SYMBOL", "TTF"),
    ("SIDE", "1"),
    ("ORDERQTY", "1200"),
    ("PRICE", "41.2500"),
    ("NOPARTYIDS", "2"),
    ("NOPARTYIDS[0].PARTYID", "BUYSIDE"),
    ("NOPARTYIDS[0].PARTYIDSOURCE", "D"),
    ("NOPARTYIDS[0].PARTYROLE", "1"),
    ("NOPARTYIDS[1].PARTYID", "XPAR"),
    ("NOPARTYIDS[1].PARTYIDSOURCE", "G"),
    ("NOPARTYIDS[1].PARTYROLE", "17"),
    ("TRANSACTTIME", "20260814-00:05:01.148"),
    ("UNKNOWNVENUEFIELD", "Z9"),
]

HASHED_RAW_PAIRS = [
    ("ISINCODE", "XX0000084733"),
    ("SYMBOL", "TTF"),
    ("SIDE", "1"),
    ("ORDERQTY", "1200"),
    ("NOPARTYIDS", "1"),
    ("NOPARTYIDS[0].PARTYID", "BUYSIDE"),
    ("NOPARTYIDS[0].PARTYROLE", "1"),
    ("UNKNOWNVENUEFIELD", "Z9"),
]


def test_the_sample_is_the_shape_the_tests_assume() -> None:
    assert len(RECORDS) == EXPECTED_RECORDS
    assert len(CONTINUATIONS) == EXPECTED_CONTINUATIONS
    assert SAMPLE_BYTES.count(b"\x01") > 0, "the SOH lines are bytes, not four characters"
    assert b"^A9=61" in SAMPLE_BYTES, "and the caret-A line is the two characters"
    assert len(WIRE.strip("|").split("|")) == EXPECTED_WIRE_FIELDS


@pytest.fixture(scope="module")
def codec() -> FixCodec:
    return FixCodec(registry=FixRegistry(cache_dir=DATA, offline=True))


@pytest.fixture(scope="module")
def table(codec: FixCodec) -> pyarrow.Table:
    """Parsed once for the whole file: an Arrow table is immutable, and the four
    tests that need another codec build their own from `codec.registry`."""
    with TextFile.from_path(SAMPLE, codec=codec) as log:
        return log.read_arrow_table()


# -- the protocols -----------------------------------------------------------


def test_every_line_lands_in_the_protocol_the_rules_claim(table: pyarrow.Table) -> None:
    assert table.num_rows == EXPECTED_RECORDS
    assert table.column("protocol_code").to_pylist() == EXPECTED_PROTOCOLS


def test_the_column_and_the_line_agree_on_every_row(table: pyarrow.Table) -> None:
    """Scalar and vectorised, row for row, on the capture itself."""
    scalar = [Rules.into_default().categorise(one) for one in table.column("message").to_pylist()]
    assert [rule.protocol for rule in scalar] == table.column("protocol_code").to_pylist()


# -- what a line carries -----------------------------------------------------


def test_a_wire_message_yields_its_body_and_nothing_around_it(table: pyarrow.Table) -> None:
    """The line has a prefix *and* a suffix, and neither is in the message."""
    assert table.column("symbol")[PIPED].as_py() == "TTF"
    assert table.column("side")[PIPED].as_py() == "1"
    assert table.column("price")[PIPED].as_py() == 41.25, "`44=41.2500` is a Price, so a number"
    assert _tagged(table.column("kwargs")[PIPED]) == [], "every field of it was worth a column"
    assert _keys(table.column("kwargs")[PIPED]) == []
    assert _named(table.column("kwargs")[PIPED]) == []
    around = str([table.column(name)[PIPED].as_py() for name in FLAT_NAMES])
    assert "sending" not in around and "queued" not in around


def test_every_field_of_a_wire_message_lands_in_one_of_the_three_places(
    table: pyarrow.Table,
) -> None:
    """Nothing drops or duplicates across both pair lists and the columns."""
    resolved = len(_tagged(table.column("kwargs")[PIPED]))
    rest = len(_named(table.column("kwargs")[PIPED]))
    assert resolved + rest + _lifted(table, PIPED) == EXPECTED_WIRE_FIELDS


def test_the_caret_and_the_soh_lines_keep_their_distinct_version_semantics(
    table: pyarrow.Table,
) -> None:
    """Known `FIX.4.4` decodes; malformed `FIX4` remains lossless raw input."""
    assert table.column("begin_string")[SOHED].as_py() == "FIX.4.4"
    assert table.column("msg_seq_num")[SOHED].as_py() == 1094
    assert table.column("check_sum")[SOHED].as_py() == "118"
    assert table.column("msg_type")[SOHED].as_py() == "8"
    assert _tagged(table.column("kwargs")[SOHED]) == []

    assert _tagged(table.column("kwargs")[CARET]) == CARET_RAW_PAIRS
    assert _keys(table.column("kwargs")[CARET]) == [str(tag) for tag, _ in CARET_RAW_PAIRS]
    assert _named(table.column("kwargs")[CARET]) == []
    _assert_no_semantic_columns(table, CARET)


def test_no_flat_tag_is_left_in_the_list_it_was_lifted_out_of(table: pyarrow.Table) -> None:
    """One fact stored twice is one that can disagree with itself.

    A tag that repeats is the exception, and it is not half-lifted either: it
    stays whole in the list, every occurrence of it, and its column is null.
    """
    assert _lifted(table, PIPED), "so the loop below has something to be true about"
    for row in range(table.num_rows):
        if row == CARET:
            continue
        pairs = _tagged(table.column("kwargs")[row])
        if pairs is None:
            continue
        left = [tag for tag, _ in pairs if tag in COLUMNS]
        assert all(left.count(tag) > 1 for tag in left), "only a repeat may stay behind"


def test_every_field_of_the_bridge_line_lands_in_one_of_the_four_places(
    table: pyarrow.Table,
) -> None:
    """A versionless named message remains complete and in wire order."""
    assert _tagged(table.column("kwargs")[BRIDGE]) == []
    assert _named(table.column("kwargs")[BRIDGE]) == BRIDGE_RAW_PAIRS
    assert len(BRIDGE_RAW_PAIRS) == EXPECTED_BRIDGE_PAIRS
    _assert_no_semantic_columns(table, BRIDGE)


def test_the_versionless_bridge_line_does_not_guess_a_dictionary(
    table: pyarrow.Table,
) -> None:
    """Known-looking names are still raw without message-local version evidence."""
    raw = dict(_named(table.column("kwargs")[BRIDGE]))
    assert raw["SYMBOL"] == "TTF"
    assert raw["SIDE"] == "1"
    assert raw["ORDERQTY"] == "1200"
    assert raw["PRICE"] == "41.2500"
    assert _tagged(table.column("kwargs")[BRIDGE]) == []
    _assert_no_semantic_columns(table, BRIDGE)


def test_the_versionless_bridge_group_stays_raw_in_wire_order(table: pyarrow.Table) -> None:
    pairs = _named(table.column("kwargs")[BRIDGE])
    assert pairs[6:13] == BRIDGE_RAW_PAIRS[6:13]
    assert pairs[13] == ("TRANSACTTIME", "20260814-00:05:01.148")
    assert table.column("transact_time")[BRIDGE].as_py() is None
    assert table.column("parties")[BRIDGE].as_py() is None
    _assert_no_semantic_columns(table, BRIDGE)


@pytest.mark.parametrize("reverse", [False, True])
def test_mixed_fix_versions_read_parties_through_one_declaration(
    tmp_path: Path, codec: FixCodec, reverse: bool
) -> None:
    """One component, one member tree: a 4.3 sub-ID is read where 5.0.SP2 puts it.

    4.3 named `PartySubID <523>` directly under `NoPartyIDs` and every version
    after it moved the member into `PtysSubGrp`. The registry keeps the newest
    tree, so a 4.3 message's sub-ID is still lifted -- under the group the
    newest declaration puts it in, which is what `data/fix-conflicts.json`
    records the collapse of.
    """
    messages = [
        "8=FIX.4.3|35=D|453=1|448=P43|523=SUB43|10=001|",
        "8=FIX.5.0SP2|35=D|453=1|448=P50|2376=7|802=1|523=SUB50|803=1|10=002|",
    ]
    if reverse:
        messages.reverse()

    parsed = _lines(tmp_path / f"mixed-{reverse}.txt", codec, "FixSession", messages)
    by_id = {row[0]["party_id"]: row[0]["buffer"] for row in parsed.column("parties").to_pylist()}

    assert by_id["P43"] == [("NoPartySubIDs[0].PartySubID", "SUB43")]
    assert by_id["P50"] == [
        ("PartyRoleQualifier", "7"),
        ("NoPartySubIDs", "1"),
        ("NoPartySubIDs[0].PartySubID", "SUB50"),
        ("NoPartySubIDs[0].PartySubIDType", "1"),
    ]


def test_a_bridge_message_in_a_fix_envelope_keeps_both_halves(table: pyarrow.Table) -> None:
    """`8=FIX.4.2|35=UL|#SYMBOL=TTF` is one message with two spellings in it.

    Read as a wire message every named field is noise; read from the `#` the
    header that says what it is gets cut off with the log's prefix. So it is
    its own rule, and the message still starts at its BeginString.
    """
    assert table.column("begin_string")[WRAPPED].as_py() == "FIX.4.2", "the wire header survives"
    assert table.column("msg_type")[WRAPPED].as_py() == "UL"
    assert table.column("symbol")[WRAPPED].as_py() == "TTF", "and so do the names"
    assert table.column("side")[WRAPPED].as_py() == "1"
    assert table.column("order_qty")[WRAPPED].as_py() == 1200.0
    assert table.column("isincode")[WRAPPED].as_py() == "XX0000084733"
    assert _named(table.column("kwargs")[WRAPPED]) == []


def test_a_bridge_message_separated_by_its_own_markers_reads_the_same(
    table: pyarrow.Table,
) -> None:
    """`#A=1#B=2` puts nothing between its tokens, so the next `#` ends the value.

    Read the character there as the separator -- which is what a bridge with a
    `|` between its tokens wants -- and it is the tail of the value in front of
    it: `#A=1#B=2` came back as `A=''` and `B` glued to whatever followed. It
    parsed, which is how it would have travelled.
    """
    assert _named(table.column("kwargs")[HASHED]) == HASHED_RAW_PAIRS
    assert _tagged(table.column("kwargs")[HASHED]) == []
    assert _keys(table.column("kwargs")[HASHED]) == [key for key, _ in HASHED_RAW_PAIRS]
    _assert_no_semantic_columns(table, HASHED)


def test_a_nested_entry_survives_a_marker_separated_line(table: pyarrow.Table) -> None:
    """Two separators on one line, and neither is the other's."""
    pairs = _named(table.column("kwargs")[HASHED])
    assert pairs[4:7] == [
        ("NOPARTYIDS", "1"),
        ("NOPARTYIDS[0].PARTYID", "BUYSIDE"),
        ("NOPARTYIDS[0].PARTYROLE", "1"),
    ]
    assert table.column("parties")[HASHED].as_py() is None
    _assert_no_semantic_columns(table, HASHED)


def test_a_wire_message_that_only_mentions_a_marker_stays_a_wire_message() -> None:
    """The discriminator is what the sender said it is, not a hash in a Text field."""
    quoted = "8=FIX.4.4|35=8|58=see #A=1 and #B=2|10=1|"
    assert Rules.into_default().categorise(quoted).protocol == "FIX"
    assert Rules.into_default().categorise("8=FIX.4.2|35=ULX|#A=1|#B=2").protocol == "FIX"


def test_versionless_names_are_all_kept_and_never_guessed(table: pyarrow.Table) -> None:
    assert _named(table.column("kwargs")[BRIDGE]) == BRIDGE_RAW_PAIRS
    assert _keys(table.column("kwargs")[BRIDGE]) == [key for key, _ in BRIDGE_RAW_PAIRS]
    assert table.column("isincode")[BRIDGE].as_py() is None
    _assert_no_semantic_columns(table, BRIDGE)


def test_a_line_carrying_no_message_has_no_pairs_at_all(table: pyarrow.Table) -> None:
    """Null, not an empty list: it was never a message and sent no session."""
    for row, protocol in enumerate(EXPECTED_PROTOCOLS):
        if protocol != NO_PROTOCOL:
            continue
        assert table.column("kwargs")[row].as_py() is None
        assert table.column("parties")[row].as_py() is None
        assert _lifted(table, row) == 0


def test_a_stack_trace_still_folds_into_the_row_above_it(table: pyarrow.Table) -> None:
    """The message layer must not have cost the parser its continuations."""
    (folded,) = [
        one for one in table.column("message").to_pylist() if "IllegalStateException" in one
    ]
    assert folded.count("\n") == EXPECTED_CONTINUATIONS


# -- the flat layer, as columns ----------------------------------------------


def test_a_wire_message_lands_its_header_and_trailer_in_columns(table: pyarrow.Table) -> None:
    """Who sent it, to whom, in what order and when -- what a reader filters on."""
    assert table.column("begin_string")[PIPED].as_py() == "FIX.4.2"
    assert table.column("body_length")[PIPED].as_py() == 176
    assert table.column("msg_type")[PIPED].as_py() == "D"
    assert table.column("msg_seq_num")[PIPED].as_py() == 1092
    assert table.column("sender_comp_id")[PIPED].as_py() == "BUYSIDE"
    assert table.column("target_comp_id")[PIPED].as_py() == "XPAR"
    assert table.column("check_sum")[PIPED].as_py() == "203"


def test_a_wire_message_lands_what_it_traded_in_columns(table: pyarrow.Table) -> None:
    """The other half of the flat layer: what a desk queries a fill by."""
    assert table.column("symbol")[SOHED].as_py() == "TTF"
    assert table.column("order_id")[SOHED].as_py() == "ORD-0000038106"
    assert table.column("exec_id")[SOHED].as_py() == "EXE-0000091233"
    assert table.column("ord_status")[SOHED].as_py() == "1"
    assert table.column("exec_type")[SOHED].as_py() == "F"
    assert table.column("last_px")[SOHED].as_py() == 41.25
    assert table.column("last_qty")[SOHED].as_py() == 400.0
    assert table.column("leaves_qty")[SOHED].as_py() == 800.0
    assert table.column("cl_ord_id")[PIPED].as_py() == "ORD-0000038106"
    assert table.column("ord_type")[PIPED].as_py() == "2"
    assert table.column("time_in_force")[PIPED].as_py() == "0"


def test_a_malformed_version_keeps_its_checksum_raw_and_lossless(table: pyarrow.Table) -> None:
    """Unknown `FIX4` cannot type fields, but raw CheckSum still keeps its zero."""
    assert dict(_tagged(table.column("kwargs")[CARET]))[10] == "017"
    assert table.column("check_sum")[CARET].as_py() is None
    _assert_no_semantic_columns(table, CARET)


def test_a_stamp_lands_as_the_microsecond_utc_instant_it_spells(table: pyarrow.Table) -> None:
    sending = table.column("sending_time")[PIPED].as_py()
    assert sending == datetime(2026, 8, 14, 0, 5, 1, 147000, tzinfo=UTC)
    sending_ns = int(sending.timestamp()) * 1_000_000_000 + sending.microsecond * 1000
    # `unix` is when the transaction happened, and this line's only FIX clock
    # is its `SendingTime <52>` -- so that is the rung that answered, and the
    # header's own stamp is a millisecond later in `runix`.
    assert table.column("unix")[PIPED].as_py() == sending_ns
    assert table.column("unix_source")[PIPED].as_py() == "SendingTime"
    assert table.column("runix")[PIPED].as_py() - sending_ns == 1_000_000


def test_a_field_the_message_never_sent_is_null_and_never_a_default(table: pyarrow.Table) -> None:
    """A stamp nobody wrote must not read as the epoch, which a sort puts first."""
    assert table.column("sending_time")[SOHED].as_py() is None, "that line carries no tag 52"
    assert table.column("poss_dup_flag").null_count == EXPECTED_RECORDS
    assert table.column("on_behalf_of_comp_id").null_count == EXPECTED_RECORDS
    assert table.column("last_qty")[PIPED].as_py() is None, "and an order has filled nothing"


def test_a_flag_the_wire_spells_with_a_letter_lands_as_a_boolean(
    tmp_path: Path, codec: FixCodec
) -> None:
    """`43=Y` is a boolean and `52=20260814-...` an instant; Arrow's own cast raises on both."""
    parsed = _one_line(
        tmp_path / "flags.txt",
        codec,
        "FixSession_XPAR",
        "sending >> 8=FIX.4.4|9=99|35=A|34=3|49=BUYSIDE|56=XPAR|"
        "52=20260814-00:05:01.147|43=Y|97=N|10=001|",
    )
    assert parsed.column("poss_dup_flag")[0].as_py() is True
    assert parsed.column("poss_resend")[0].as_py() is False
    assert parsed.column("sending_time")[0].as_py() == datetime(
        2026, 8, 14, 0, 5, 1, 147000, tzinfo=UTC
    )


def test_a_tag_is_lifted_only_where_it_occurs_once_in_its_own_line(
    tmp_path: Path, codec: FixCodec
) -> None:
    """One symbol per line, or none -- never whichever leg was written first.

    Both lines are in one batch, and the count that decides is per `(row,
    tag)`: the multi-leg order keeps every `55` and every `44` in the pair list, and
    the single-leg order beside it still lifts its own. A count taken over the
    batch would have suppressed both.
    """
    parsed = _lines(
        tmp_path / "legs.txt",
        codec,
        "FixSession_XPAR",
        [
            "sending >> 8=FIX.4.4|9=99|35=AB|34=8|49=BUYSIDE|56=XPAR|"
            "555=2|600=TTF|55=SPREAD|44=41.25|555=2|55=OTHER|44=42.50|10=011|",
            "sending >> 8=FIX.4.4|9=88|35=D|34=9|49=BUYSIDE|56=XPAR|55=TTF|54=1|44=41.25|10=012|",
        ],
    )
    assert _tagged(parsed.column("kwargs")[0]) == [
        (555, "2"),
        (600, "TTF"),
        (55, "SPREAD"),
        (44, "41.25"),
        (555, "2"),
        (55, "OTHER"),
        (44, "42.50"),
    ], "every occurrence of a repeated tag, in wire order"
    assert parsed.column("price")[0].as_py() is None, "no one price is the multi-leg order's"
    assert parsed.column("symbol")[0].as_py() is None, "nor one unambiguous symbol"
    assert parsed.column("msg_seq_num")[0].as_py() == 8, "while what was written once still lifted"
    assert parsed.column("msg_type")[0].as_py() == "AB"

    assert _tagged(parsed.column("kwargs")[1]) == [], "and the line beside it lifted all of it"
    assert parsed.column("symbol")[1].as_py() == "TTF"
    assert parsed.column("price")[1].as_py() == 41.25
    assert parsed.column("msg_seq_num")[1].as_py() == 9


def test_a_hop_stays_in_the_pair_list_because_one_row_of_it_is_not_one_value(
    tmp_path: Path, codec: FixCodec
) -> None:
    """A scalar column would keep whichever entry of the group arrived first."""
    parsed = _one_line(
        tmp_path / "hops.txt",
        codec,
        "FixSession_XPAR",
        "sending >> 8=FIX.4.4|9=99|35=A|34=3|49=BUYSIDE|56=XPAR|"
        "627=2|628=HOP1|630=7|628=HOP2|630=8|10=001|",
    )
    assert [tag for tag, _ in _tagged(parsed.column("kwargs")[0])] == [
        627,
        628,
        630,
        628,
        630,
    ]
    hops = [value for tag, value in _tagged(parsed.column("kwargs")[0]) if tag == 628]
    assert hops == ["HOP1", "HOP2"], "both of them, in the order they relayed it"
    assert parsed.column("sender_comp_id")[0].as_py() == "BUYSIDE", "and the scalars still lifted"


def test_versionless_bridge_names_remain_raw_even_when_the_dictionary_knows_them(
    tmp_path: Path, codec: FixCodec
) -> None:
    """A dictionary match is not permission to infer a message's FIX version."""
    parsed = _one_line(
        tmp_path / "named.txt",
        codec,
        "ULBridge",
        "toBridge #ISINCODE=XX00#SYMBOL=TTF#SIDE=1#ACCOUNT=<null>#SENDERCOMPID=BRIDGE1",
    )
    assert _named(parsed.column("kwargs")[0]) == [
        ("ISINCODE", "XX00"),
        ("SYMBOL", "TTF"),
        ("SIDE", "1"),
        ("SENDERCOMPID", "BRIDGE1"),
    ]
    assert _tagged(parsed.column("kwargs")[0]) == []
    assert _keys(parsed.column("kwargs")[0]) == [
        "ISINCODE",
        "SYMBOL",
        "SIDE",
        "SENDERCOMPID",
    ]
    _assert_no_semantic_columns(parsed, 0)


# -- millis and micros in one capture ----------------------------------------


def test_both_stamp_widths_read_as_the_instants_they_spell(table: pyarrow.Table) -> None:
    """The header's own clock, which is `runix` now that `unix` is the transaction."""
    recorded = table.column("runix").to_pylist()
    assert recorded[0] == 1_786_665_901_147_250_000, "micros, with a separator"
    assert recorded[1] == 1_786_665_901_147_000_000, "millis only"
    assert recorded[PIPED] == 1_786_665_901_148_000_000, "millis, comma-separated"
    assert recorded[0] > recorded[1], "a padded 147 is 147 ms, not 147 us -- and so is earlier"


def test_the_capture_reparses_to_the_same_instants(
    tmp_path: Path, codec: FixCodec, table: pyarrow.Table
) -> None:
    """Written back out and read again under the same codec, column for column."""
    copy = tmp_path / "copy.txt"
    TextFile.from_path(copy).append_arrow(table)
    with TextFile.from_path(copy, codec=codec) as again:
        written = again.read_arrow_table()
    assert written.column("unix").to_pylist() == table.column("unix").to_pylist()
    assert written.column("protocol_code").to_pylist() == EXPECTED_PROTOCOLS
    # Named rather than left to a `KeyError` from the loop below: a column the
    # flat layer declares and the shape does not is a missing column, and it
    # should fail as one.
    assert set(FLAT_NAMES) <= set(written.schema.names)
    for name in FLAT_NAMES:
        assert written.column(name).to_pylist() == table.column(name).to_pylist(), name


# -- what a rule set decides -------------------------------------------------


def test_a_rule_set_from_a_document_reclassifies_a_line(tmp_path: Path, codec: FixCodec) -> None:
    path = tmp_path / "rules.yml"
    Rules(
        rules=[
            Rule(protocol="BRIDGE", plugin_pattern="^ULBridge$", codec="ul"),
            Rules.into_default().rule(NO_PROTOCOL),
        ]
    ).into_yaml(path)
    own = FixCodec(registry=codec.registry, rules=Rules.from_yaml(path))
    with TextFile.from_path(SAMPLE, codec=own) as log:
        table = log.read_arrow_table()
    found = table.column("protocol_code").to_pylist()
    assert found[BRIDGE] == "BRIDGE", "the plugin decides now, not the message"
    assert found[PIPED] == NO_PROTOCOL, "and the wire messages are nobody's protocol"
    assert found[REJECTED] == "BRIDGE", "including the bridge's own prose line"
    assert table.column("kwargs")[PIPED].as_py() is None
    assert _lifted(table, PIPED) == 0, "a line nothing reads has no flat layer either"


def test_a_file_that_declares_no_rules_parses_as_it_always_did(codec: FixCodec) -> None:
    """The whole of the compatibility promise, in one assertion."""
    quiet = FixCodec(registry=codec.registry, rules=Rules(rules=[]))
    with TextFile.from_path(SAMPLE, codec=quiet) as log:
        table = log.read_arrow_table()
    assert table.column("protocol_code").to_pylist() == [NO_PROTOCOL] * EXPECTED_RECORDS
    assert table.column("kwargs").to_pylist() == [None] * EXPECTED_RECORDS
    assert table.column("parties").to_pylist() == [None] * EXPECTED_RECORDS
    assert table.column("symbol").to_pylist() == [None] * EXPECTED_RECORDS
    assert table.column("isincode").to_pylist() == [None] * EXPECTED_RECORDS
    assert table.column("code").to_pylist() == [""] * EXPECTED_RECORDS
    assert all(table.column(name).null_count == EXPECTED_RECORDS for name in FLAT_NAMES)


def test_a_sparse_codec_gets_typed_nulls_for_optional_declared_columns(
    tmp_path: Path, codec: FixCodec
) -> None:
    class SparseCodec(FixCodec):
        def into_lifted_columns(
            self, kwargs: object, version: str | None = None
        ) -> tuple[dict[str, object], object]:
            return {}, kwargs

        def into_component_columns(
            self, kwargs: object, version: str | None = None
        ) -> tuple[dict[str, object], object]:
            return {}, kwargs

    sparse = SparseCodec(registry=codec.registry)
    parsed = _one_line(tmp_path / "sparse.txt", sparse, "FixSession", "8=FIX.4.4|35=D|34=7|55=TTF|")

    assert parsed.column("msg_seq_num")[0].as_py() is None
    assert parsed.column("parties")[0].as_py() is None
    assert [tag for tag, _ in _tagged(parsed.column("kwargs")[0])] == [8, 35, 34, 55]


def test_quote_fields_are_typed_once_and_drive_log_correlation(
    tmp_path: Path, codec: FixCodec
) -> None:
    parsed = _one_line(
        tmp_path / "quote.txt",
        codec,
        "FixSession",
        "8=FIX.4.4|35=S|34=7|117=Q-1|131=R-1|55=AAPL|537=1|297=0|132=100|133=101|"
        "134=10|135=12|293=9|294=11|62=20260821-10:05:00|",
    )

    assert parsed.column("msg_seq_num").to_pylist() == [7]
    assert parsed.column("code").to_pylist() == ["Q-1"]
    assert parsed.column("code").to_pylist() == ["Q-1"]
    assert parsed.column("quote_id").to_pylist() == ["Q-1"]
    assert parsed.column("quote_req_id").to_pylist() == ["R-1"]
    assert parsed.column("quote_type").to_pylist() == [1]
    assert parsed.column("quote_status").to_pylist() == [0]
    assert parsed.column("bid_px").to_pylist() == [100.0]
    assert parsed.column("offer_px").to_pylist() == [101.0]
    assert parsed.column("bid_size").to_pylist() == [10.0]
    assert parsed.column("offer_size").to_pylist() == [12.0]
    assert parsed.column("def_bid_size").to_pylist() == [9.0]
    assert parsed.column("def_offer_size").to_pylist() == [11.0]
    assert parsed.column("valid_until_time").to_pylist() == [
        datetime(2026, 8, 21, 10, 5, tzinfo=UTC)
    ]
    assert parsed.column("kwargs").to_pylist() == [[]], "every field of it was worth a column"


def test_repeating_quote_entries_stay_in_wire_order(tmp_path: Path, codec: FixCodec) -> None:
    parsed = _one_line(
        tmp_path / "mass_quote.txt",
        codec,
        "FixSession",
        "8=FIX.4.4|35=i|295=2|299=Q-1|132=100|299=Q-2|132=101|",
    )

    assert parsed.column("no_quote_entries").to_pylist() == [2]
    assert parsed.column("quote_entry_id").to_pylist() == [None]
    assert parsed.column("bid_px").to_pylist() == [None]
    assert _tagged(parsed.column("kwargs")[0]) == [
        (295, "2"),
        (299, "Q-1"),
        (132, "100"),
        (299, "Q-2"),
        (132, "101"),
    ]


def test_a_cold_dictionary_reports_uncertainty_and_never_costs_the_capture(
    tmp_path: Path,
) -> None:
    cold = FixCodec(registry=FixRegistry(cache_dir=tmp_path, offline=True))
    with TextFile.from_path(SAMPLE, codec=cold) as log:
        table = log.read_arrow_table()
    assert table.num_rows == EXPECTED_RECORDS
    assert _tagged(table.column("kwargs")[PIPED]) == [
        (8, "FIX.4.2"),
        (9, "176"),
        (35, "D"),
        (34, "1092"),
        (49, "BUYSIDE"),
        (56, "XPAR"),
        (52, "20260814-00:05:01.147"),
        (11, "ORD-0000038106"),
        (55, "TTF"),
        (54, "1"),
        (38, "1200"),
        (40, "2"),
        (44, "41.2500"),
        (59, "0"),
        (10, "203"),
    ]
    assert len(_keys(table.column("kwargs")[PIPED])) == EXPECTED_WIRE_FIELDS
    assert _tagged(table.column("kwargs")[BRIDGE]) == []
    assert _named(table.column("kwargs")[BRIDGE]) == BRIDGE_RAW_PAIRS
    assert len(_keys(table.column("kwargs")[BRIDGE])) == EXPECTED_BRIDGE_PAIRS
    for row in (PIPED, CARET, SOHED, BRIDGE, WRAPPED, HASHED):
        _assert_no_semantic_columns(table, row)


# -- the same, over a set ----------------------------------------------------


def test_a_folder_of_captures_reads_the_messages_too(tmp_path: Path, codec: FixCodec) -> None:
    """`TextFiles` hands its codec to every file it opens, like everything else."""
    for name in ("app.1.txt", "app.2.txt"):
        (tmp_path / name).write_bytes(SAMPLE_BYTES)
    files = TextFiles.from_folder(tmp_path, codec=codec, static_values={"bridge": "bridge-1"})
    table = files.read_arrow_table()
    assert table.num_rows == EXPECTED_RECORDS * 2
    assert table.column("protocol_code").to_pylist() == EXPECTED_PROTOCOLS * 2
    assert _named(table.column("kwargs")[BRIDGE]) == BRIDGE_RAW_PAIRS
    assert _named(table.column("kwargs")[EXPECTED_RECORDS + BRIDGE]) == BRIDGE_RAW_PAIRS
    _assert_no_semantic_columns(table, BRIDGE)
    _assert_no_semantic_columns(table, EXPECTED_RECORDS + BRIDGE)
    sequences = table.column("msg_seq_num").to_pylist()
    assert sequences[PIPED] == 1092, "the flat layer of the first file"
    assert sequences[EXPECTED_RECORDS + PIPED] == 1092, "and of the second, read the same way"
    assert table.schema.names[-1] == "bridge", "and a static column still lands last"
    assert table.column("bridge").to_pylist() == ["bridge-1"] * (EXPECTED_RECORDS * 2)


# -- values that mean nothing ------------------------------------------------


def test_absent_values_never_reach_a_column(tmp_path: Path, codec: FixCodec) -> None:
    message = "toBridge #SYMBOL=TTF|#SIDE=<null>|#ACCOUNT=|#TEXT=N/A|#ORDERQTY=1200"
    table = _one_line(tmp_path / "absent.txt", codec, "ULBridge", message)
    assert _tagged(table.column("kwargs")[0]) == []
    assert _named(table.column("kwargs")[0]) == [
        ("SYMBOL", "TTF"),
        ("ORDERQTY", "1200"),
    ]
    assert _keys(table.column("kwargs")[0]) == ["SYMBOL", "ORDERQTY"]
    _assert_no_semantic_columns(table, 0)

    keeping = FixCodec(registry=codec.registry, null_values=frozenset())
    kept = _one_line(tmp_path / "kept.txt", keeping, "ULBridge", message)
    assert _named(kept.column("kwargs")[0]) == [
        ("SYMBOL", "TTF"),
        ("SIDE", "<null>"),
        ("ACCOUNT", ""),
        ("TEXT", "N/A"),
        ("ORDERQTY", "1200"),
    ]
    assert _keys(kept.column("kwargs")[0]) == [
        "SYMBOL",
        "SIDE",
        "ACCOUNT",
        "TEXT",
        "ORDERQTY",
    ]
    _assert_no_semantic_columns(kept, 0)


# -- helpers -----------------------------------------------------------------


def _lifted(table: pyarrow.Table, row: int) -> int:
    """How many flat columns that row filled."""
    return sum(table.column(name)[row].as_py() is not None for name in FLAT_NAMES)


def _semantic_floor(table: pyarrow.Table, row: int) -> int:
    """What a row fills before any dictionary is consulted: `MsgType <35>`, or nothing."""
    return 1 if table.column("msg_type")[row].as_py() is not None else 0


def _assert_no_semantic_columns(table: pyarrow.Table, row: int) -> None:
    """Unknown-version rows retain pairs without publishing interpreted values.

    `msg_type` is the one column such a row may still fill: it is one tag off
    the front of a message, found without a dictionary, and a row whose
    version nothing resolved still knows what kind of message it is.
    """
    assert _lifted(table, row) == _semantic_floor(table, row)
    assert _component_fields(table, row) == 0
    assert table.column("parties")[row].as_py() is None


def _component_fields(table: pyarrow.Table, row: int) -> int:
    """Source fields represented by the structured Parties value."""
    parties = table.column("parties")[row].as_py()
    if parties is None:
        return 0
    return 1 + sum(
        sum(party[name] is not None for name in ("party_id", "party_id_source", "party_role"))
        + len(party["buffer"] or ())
        for party in parties
    )


def _entries(scalar: pyarrow.Scalar) -> list[dict[str, object]] | None:
    """One row of `kwargs`, or `None` where the line carried no message."""
    return scalar.as_py()


def _pairs(scalar: pyarrow.Scalar) -> list[tuple[object, str]] | None:
    """Stored fields as the pairs they are addressed by: tag, or rendered key."""
    value = _entries(scalar)
    if value is None:
        return None
    return [(entry["tag"] or _key(entry), entry["value"]) for entry in value]


def _tagged(scalar: pyarrow.Scalar) -> list[tuple[int, str]]:
    """Only the fields the dictionary resolved, as `(tag, value)`."""
    return [(tag, value) for tag, value in _pairs(scalar) or () if isinstance(tag, int)]


def _named(scalar: pyarrow.Scalar) -> list[tuple[str, str]]:
    """Only the fields it did not, as `(rendered key, value)`."""
    return [(key, value) for key, value in _pairs(scalar) or () if isinstance(key, str)]


def _keys(scalar: pyarrow.Scalar) -> list[str]:
    """The spelling every stored field arrived under, resolved or not."""
    return [_key(entry) for entry in _entries(scalar) or ()]


def _key(entry: dict[str, object]) -> str:
    """One stored field's rendered key: its name under whatever led it."""
    lead = entry["namespace"] or entry["comp"]
    return f"{lead}.{entry['key']}" if lead else str(entry["key"])


def _lines(path: Path, codec: FixCodec, plugin: str, messages: list[str]) -> pyarrow.Table:
    """Synthesised log lines through the whole parser, as the one batch they land as."""
    path.write_text(
        "".join(f"2026-08-14 00:05:01.147 [t] [{plugin}] (INFO) {one}\n" for one in messages)
    )
    with TextFile.from_path(path, codec=codec) as log:
        return log.read_arrow_table()


def _one_line(path: Path, codec: FixCodec, plugin: str, message: str) -> pyarrow.Table:
    """One synthesised log line through the whole parser, as the row it lands as."""
    return _lines(path, codec, plugin, [message])


# -- when the transaction happened -------------------------------------------

#: One capture where every rung of the chain answers for a different row, and
#: where the regulatory group and the message's own claim *disagree* -- which
#: is the case the ranking exists for.
TRANSACTED_LINES = (
    # An execution whose group says it executed at 09:29 and whose
    # TransactTime claims 09:30. The group wins, and takes EXECUTION_TIME <1>
    # rather than the BROKER_RECEIPT <4> beside it.
    "2026-08-14 00:05:01.147 [t] [d] (INFO) 8=FIX.4.4|35=8|17=E1|54=1|150=F|32=10|31=9.5|"
    "55=IBM|60=20260814-09:30:00.000|768=2|769=20260814-09:29:00.000|770=1|"
    "769=20260814-09:28:00.000|770=4|10=000\n"
    # The same group shape on an *order*, which prefers TIME_IN <2>: one group,
    # two kinds of row, two answers.
    "2026-08-14 00:05:02.147 [t] [d] (INFO) 8=FIX.4.4|35=D|11=C1|54=1|55=IBM|"
    "60=20260814-09:30:00.000|768=2|769=20260814-09:29:00.000|770=1|"
    "769=20260814-09:27:00.000|770=2|10=000\n"
    # No group, so the message's own claim answers.
    "2026-08-14 00:05:03.147 [t] [d] (INFO) 8=FIX.4.4|35=D|11=C2|54=1|55=IBM|"
    "60=20260814-09:26:00.000|10=000\n"
    # No claim either, so the last FIX clock there is.
    "2026-08-14 00:05:04.147 [t] [d] (INFO) 8=FIX.4.4|35=D|11=C3|54=1|55=IBM|"
    "52=20260814-09:31:00.000|10=000\n"
    # Not a message at all: only the clock that recorded it.
    "2026-08-14 00:05:05.147 [t] [d] (INFO) heartbeat\n"
)

TRANSACTED_SOURCES = [
    "TrdRegTimestamps=1",
    "TrdRegTimestamps=2",
    "TransactTime",
    "SendingTime",
    "recorded",
]


@pytest.fixture
def transacted(tmp_path: Path, codec: FixCodec) -> pyarrow.Table:
    path = tmp_path / "transacted.txt"
    path.write_text(TRANSACTED_LINES)
    with TextFile.from_path(path, codec=codec) as log:
        return log.read_arrow_table()


def test_unix_is_the_transaction_time_and_runix_is_when_it_was_recorded(
    transacted: pyarrow.Table,
) -> None:
    """The whole point: a row's `unix` is when it happened, not when it printed."""
    assert transacted.column("unix_source").to_pylist() == TRANSACTED_SOURCES
    unix = transacted.column("unix").to_pylist()
    assert unix[0] == unix_of("20260814-09:29:00.000"), "the group, not TransactTime"
    assert unix[1] == unix_of("20260814-09:27:00.000"), "an order takes TIME_IN"
    assert unix[2] == unix_of("20260814-09:26:00.000")
    assert unix[3] == unix_of("20260814-09:31:00.000")
    recorded = transacted.column("runix").to_pylist()
    assert recorded == [
        unix_of(f"2026-08-14 00:05:0{index + 1}.147") for index in range(len(recorded))
    ], "the header clock is preserved, row for row"
    assert unix[4] == recorded[4], "a line carrying no message happened when it was written"


def test_the_group_and_transact_time_disagreeing_is_decided_by_the_chain(
    transacted: pyarrow.Table,
) -> None:
    """Both rows carry `TransactTime <60>` at 09:30 and neither is stamped with it."""
    claimed = transacted.column("transact_time").to_pylist()[:2]
    assert all(one is not None for one in claimed), "the claim is still stored"
    unix = transacted.column("unix").to_pylist()[:2]
    assert all(one != unix_of("20260814-09:30:00.000") for one in unix)


def test_unix_hour_follows_the_resolved_time_and_not_the_header(
    transacted: pyarrow.Table,
) -> None:
    """A row moves partition with its transaction time, or a sorted read breaks."""
    for unix, hour in zip(
        transacted.column("unix").to_pylist(),
        transacted.column("unix_hour").to_pylist(),
        strict=True,
    ):
        assert hour == unix - unix % HOUR


def test_the_parse_and_the_translation_resolve_one_row_alike(
    transacted: pyarrow.Table,
) -> None:
    """One resolver, two executions: a column of rows and one message at a time."""
    rows = [FixMessage.from_dict(row) for row in transacted.to_pylist()]
    for row in rows:
        found = row.into_fix_events().transacted
        if not row.message or "8=FIX" not in row.message:
            continue
        assert (found.unix, found.source) == (row.unix, row.unix_source), row.message


# -- the two stages ----------------------------------------------------------

#: One capture reaching all three destinations, in every spelling that matters:
#: a wire message, a rendered one, recognised operational traffic, and a line
#: whose transport nothing recognises.
STAGED_LINES = (
    "2026-08-14 00:05:01.147 [t] [FixSession_XPAR] (INFO) 8=FIX.4.4|35=D|11=C1|54=1|55=IBM|"
    "453=1|448=BUYSIDE|447=D|452=1|60=20260814-09:30:00.000|10=000\n"
    "2026-08-14 00:05:02.147 [t] [ULBridge] (INFO) toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=D|"
    "#SIDE=Buy|#SYMBOL=TTF|#NOPARTYIDS[0].PARTYID=ABC|#TECH.CLIENTID=42\n"
    "2026-08-14 00:05:03.147 [t] [HealthMonitor] (INFO) heartbeat sent on session 3\n"
    "2026-08-14 00:05:04.147 [t] [ExperimentalAdapter] (INFO) opaque payload nobody reads\n"
)


@pytest.fixture
def staged(tmp_path: Path, codec: FixCodec) -> pyarrow.Table:
    """What `parse_messages` writes: structured, and resolved against nothing."""
    path = tmp_path / "staged.txt"
    path.write_text(STAGED_LINES)
    with TextFile.from_path(path, codec=codec, resolved=False) as log:
        return log.read_arrow_table()


@pytest.fixture
def resolved(staged: pyarrow.Table, codec: FixCodec) -> pyarrow.Table:
    """What `parse_fix` makes of it, off the stored columns alone."""
    return pyarrow.Table.from_batches(
        [FixMessage.resolve_arrow_batch(batch, codec) for batch in staged.to_batches()]
    )


@pytest.fixture
def whole(tmp_path: Path, codec: FixCodec) -> pyarrow.Table:
    """What reading the very same capture in one pass produces instead.

    The same path as `staged`, because where a line was read is part of its
    identity: two fixtures over two paths would differ for a good reason and
    hide the bad one.
    """
    path = tmp_path / "staged.txt"
    path.write_text(STAGED_LINES)
    with TextFile.from_path(path, codec=codec, resolved=True) as log:
        return log.read_arrow_table()


def test_one_pass_and_two_stages_agree_on_every_stored_column(
    whole: pyarrow.Table, resolved: pyarrow.Table
) -> None:
    """The route a capture took must not change the row it becomes.

    `hash` is what every merge upserts on, so two routes disagreeing about it
    write one line twice or collapse two lines into one.
    """
    assert whole.schema.equals(resolved.schema)
    for name in whole.schema.names:
        assert whole.column(name).to_pylist() == resolved.column(name).to_pylist(), name


def test_two_identical_lines_in_two_captures_stay_two_rows(tmp_path: Path, codec: FixCodec) -> None:
    """Where a line was read is part of its identity, so a copy is not the same row."""
    digests = []
    for name in ("first.txt", "second.txt"):
        path = tmp_path / name
        path.write_text(STAGED_LINES)
        with TextFile.from_path(path, codec=codec, resolved=False) as log:
            digests.append(log.read_arrow_table().column("hash").to_pylist())
    assert len(digests[0]) == len(STAGED_LINES.splitlines())
    assert not set(digests[0]) & set(digests[1]), "one capture copied is not one capture"


def test_the_message_stage_structures_without_resolving(staged: pyarrow.Table) -> None:
    """`tag == 0` is what says an entry is unresolved, and it is NOT NULL."""
    assert staged.column("protocol_code").to_pylist() == ["FIX", "UL", "MISC", "OTHER"]
    assert staged.column("protocol_version").to_pylist() == ["4.4", "4.4", None, None]
    assert staged.column("protocol_version_source").to_pylist() == [
        "begin_string",
        "begin_string",
        "none",
        "none",
    ]
    assert staged.column("msg_type").to_pylist() == ["D", "D", None, None]
    rendered = staged.column("kwargs").to_pylist()[1]
    assert [entry["tag"] for entry in rendered] == [0] * len(rendered), "nothing resolved a name"
    assert staged.column("side").to_pylist() == [None] * 4, "and nothing was lifted"


def test_the_two_fill_levels_are_one_struct(staged: pyarrow.Table, resolved: pyarrow.Table) -> None:
    """`parse_fix` fills the same column; it does not convert a shape."""
    assert staged.schema.field("kwargs").type == resolved.schema.field("kwargs").type


def test_resolution_changes_only_the_tag_the_key_and_the_value(
    staged: pyarrow.Table, resolved: pyarrow.Table
) -> None:
    """Where a field stood is settled at the message stage and never revised."""
    for before, after in zip(
        staged.column("kwargs").to_pylist(), resolved.column("kwargs").to_pylist(), strict=True
    ):
        if before is None:
            continue
        placed = {(entry["key"], entry["namespace"], entry["comp"]) for entry in before}
        for entry in after or ():
            match = [
                one for one in placed if one[0].lower() in {entry["key"].lower(), one[0].lower()}
            ]
            assert match, entry
        kept = [(entry["namespace"], entry["comp"]) for entry in before]
        # Every entry `parse_fix` still holds stands where the message stage
        # put it: resolution reorders nothing and re-places nothing.
        for entry in after or ():
            assert (entry["namespace"], entry["comp"]) in kept, entry


def test_resolution_reads_the_stored_column_and_never_the_raw_line(
    staged: pyarrow.Table, codec: FixCodec
) -> None:
    """A row with its `message` blanked resolves to exactly the same thing."""
    at = staged.schema.get_field_index("message")
    columns = list(staged.columns)
    columns[at] = pyarrow.nulls(staged.num_rows, staged.schema.field(at).type)
    blanked = pyarrow.Table.from_arrays(columns, schema=staged.schema)
    whole = FixMessage.resolve_arrow_batch(staged.to_batches()[0], codec)
    without = FixMessage.resolve_arrow_batch(blanked.to_batches()[0], codec)
    for name in ("kwargs", "side", "symbol", "msg_type", "protocol_version", "unix"):
        assert whole.column(name).to_pylist() == without.column(name).to_pylist(), name


def test_the_redirection_sends_one_input_to_all_three_tables(
    resolved: pyarrow.Table,
) -> None:
    """One condition, one place: what `etype` the rules made of the line.

    The second row is the interesting one. It resolves against the dictionary
    perfectly well -- `#MSGTYPE=D` becomes tag 35 -- and still lands in
    `misc`, because the event rules read the *raw text* and a bridge spelling
    its type as a named key matches neither `35=D` nor `NewOrderSingle`. What
    routes a row is what kind of event it is, not whether its fields resolved,
    and those are different questions.
    """
    rules = Rules()
    categories = rules.into_arrow_category_array(
        resolved.column("protocol_code").combine_chunks(),
        resolved.column("etype").combine_chunks(),
    )
    assert categories.to_pylist() == ["market", "misc", "misc", "unknown"]


def test_a_misc_row_keeps_its_raw_line_and_a_market_row_gives_it_up() -> None:
    """The one stored shape: two content columns, and which is filled where."""
    market = FixMessage(unix=1, hash=1, message=None, kwargs=[])
    misc = FixMessage(unix=2, hash=2, message="heartbeat", kwargs=None)
    assert market.message is None and market.kwargs == []
    assert misc.message == "heartbeat"
    field = FixMessage.into_field()
    assert field.field("message").nullable, "a market row leaves it null"


def test_protocol_version_agrees_with_the_columns_it_derives_from(
    resolved: pyarrow.Table,
) -> None:
    """A row where the stored version and its own evidence disagree is corrupt."""
    for version, begin, appl in zip(
        resolved.column("protocol_version").to_pylist(),
        resolved.column("begin_string").to_pylist(),
        resolved.column("appl_ver_id").to_pylist(),
        strict=True,
    ):
        if begin is None:
            continue
        if str(begin).upper().startswith("FIXT"):
            assert appl is None or version is not None
            continue
        assert version is not None, begin
        assert version.replace(".", "") in str(begin).replace(".", ""), (version, begin)


def test_a_version_nothing_infers_stays_null_and_says_why(codec: FixCodec) -> None:
    """Null because the message carried none, told apart from null because nothing tried."""
    messages = pyarrow.array(
        ["8=FIXT.1.1|35=D|11=C1|10=000", "nothing about this line is a message"],
        pyarrow.string(),
    )
    columns = codec.into_message_columns(messages)
    assert columns["protocol_version"].to_pylist() == [None, None]
    assert columns["protocol_version_source"].to_pylist() == ["none", "none"], (
        "a FIXT header with no ApplVerID resolves nothing, and neither does a non-message"
    )


def test_a_fixt_message_resolves_through_its_application_version(codec: FixCodec) -> None:
    """FIXT is the transport; `ApplVerID <1128>` says which application version."""
    messages = pyarrow.array(["8=FIXT.1.1|35=D|1128=9|11=C1|10=000"], pyarrow.string())
    columns = codec.into_message_columns(messages)
    assert columns["protocol_version"].to_pylist() == ["5.0.SP2"]
    assert columns["protocol_version_source"].to_pylist() == ["application_version"]


def test_wire_order_survives_both_stages(codec: FixCodec) -> None:
    """Order is the identity: repeating-group entries are told apart by position."""
    line = "8=FIX.4.4|35=D|453=3|448=ONE|448=TWO|448=THREE|10=000"
    columns = codec.into_message_columns(pyarrow.array([line], pyarrow.string()))
    staged = [entry["value"] for entry in columns["kwargs"].to_pylist()[0] if entry["tag"] == 448]
    assert staged == ["ONE", "TWO", "THREE"], "the sequence, not the set"
    done = codec.complete_kwargs(columns["kwargs"], "4.4").to_pylist()[0]
    assert [entry["value"] for entry in done if entry["tag"] == 448] == ["ONE", "TWO", "THREE"]


def test_the_split_key_still_spells_what_the_line_wrote(codec: FixCodec) -> None:
    """`namespace`/`comp` joined to `key` is the rendered key, at both fill levels."""
    line = "send #NOPARTYIDS[0].PARTYID=ABC|#TECH.CLIENTID=42"
    columns = codec.into_message_columns(pyarrow.array([line], pyarrow.string()))
    for level in (columns["kwargs"], codec.complete_kwargs(columns["kwargs"], "4.4")):
        rebuilt = [
            ".".join(
                part for part in (entry["namespace"] or entry["comp"], entry["key"]) if part
            ).upper()
            for entry in level.to_pylist()[0]
        ]
        assert rebuilt == ["NOPARTYIDS[0].PARTYID", "TECH.CLIENTID"]
