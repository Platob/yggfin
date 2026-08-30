"""Raw Message records and their explicit conversion into parsed FixMsg rows."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow
import pytest

from rekep.fix import NO_PROTOCOL, FixCodec, FixRegistry, Rule, Rules
from rekep.fix.columns import COLUMNS
from rekep.fix.message import render_fix_value
from rekep.market.event import HOUR, SECOND
from rekep.text import HEADER_PATTERN, FixMsg, Message, TextFile, TextFiles
from rekep.times import unix_of

SAMPLE = Path(__file__).parent.parent / "data" / "app_messages_sample.txt"
SAMPLE_BYTES = SAMPLE.read_bytes()

#: The dictionary this repository publishes, beside `python/`.
DATA = Path(__file__).resolve().parents[3] / "data" / "fix.zip"


def event_types(registry: FixRegistry | None = None):
    """Registry mapping used by the protocol-neutral text boundary."""
    return (registry or FixRegistry.from_builtin()).msg_type_event_types()


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
    # A bare named document, whatever the plugin renders it for.
    "UL",
    # The same names inside a numbered frame, which makes one mixed message.
    "FIXML",
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

#: The standard header the *raw* stage lifts out of `entries` into columns of
#: its own, tag by column name. Spelled out rather than imported from
#: `SESSION_FIELDS`, so a field quietly leaving that tuple cannot move both
#: sides of an assertion together. A row answers to either spelling: the tag,
#: or the rendered name case-insensitively.
LIFTED_HEADER = {
    "8": "beginstring",
    "9": "bodylength",
    "35": "msgtype",
    "49": "sendercompid",
    "50": "sendersubid",
    "142": "senderlocationid",
    "56": "targetcompid",
    "57": "targetsubid",
    "143": "targetlocationid",
    "115": "onbehalfofcompid",
    "116": "onbehalfofsubid",
    "144": "onbehalfoflocationid",
    "128": "delivertocompid",
    "129": "delivertosubid",
    "145": "delivertolocationid",
    "34": "msgseqnum",
    "369": "lastmsgseqnumprocessed",
    "43": "possdupflag",
    "97": "possresend",
    "52": "sendingtime",
    "122": "origsendingtime",
    "370": "onbehalfofsendingtime",
    "1128": "applverid",
    "1129": "cstmapplverid",
    "1156": "applextid",
    "347": "messageencoding",
    "90": "securedatalen",
    "91": "securedata",
    "93": "signaturelength",
    "89": "signature",
}

#: The header fields this module's own wire lines actually write, in the
#: order the value assertions below read them. A line states part of the
#: header, not all of it, so what the *stage lifts* and what *this fixture
#: carries* are two facts and are counted apart.
WIRE_HEADER = (
    "beginstring",
    "bodylength",
    "msgseqnum",
    "msgtype",
    "sendercompid",
    "sendingtime",
    "targetcompid",
)

#: `CheckSum <10>` is the boundary every lift is measured against -- a field is
#: eligible only where it stands in front of it -- so it is deliberately not
#: among them and stays in `entries` for the FIX stage to read.
UNLIFTED_TRAILER = "10"

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

#: Every field the caret-separated line writes, in wire order.
CARET_RAW_PAIRS = [
    (8, "FIX4"),
    (9, "61"),
    (34, "1093"),
    (49, "XPAR"),
    (56, "BUYSIDE"),
    (52, "20260814-00:05:01.148"),
    (10, "017"),
]

#: What is left of it in `entries`: the checksum alone. Every other field it
#: writes is standard header, and the raw stage lifts all six into columns
#: whatever the payload's version turns out to be.
CARET_RESIDUAL_PAIRS = [(10, "017")]

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
    assert CARET_RESIDUAL_PAIRS == [
        pair for pair in CARET_RAW_PAIRS if str(pair[0]) not in LIFTED_HEADER
    ], "the caret line is standard header and a checksum, and nothing else"
    assert UNLIFTED_TRAILER not in LIFTED_HEADER


@pytest.fixture(scope="module")
def codec() -> FixCodec:
    return FixCodec(registry=FixRegistry(cache_dir=DATA, offline=True))


@pytest.fixture(scope="module")
def raw_table(codec: FixCodec) -> pyarrow.Table:
    """Protocol-neutral source rows read once for the module."""
    with TextFile.from_path(
        SAMPLE,
        msg_type_event_types=event_types(codec.registry),
        protocol_rules=codec.rules,
    ) as log:
        return log.read_arrow_table()


@pytest.fixture(scope="module")
def table(raw_table: pyarrow.Table, codec: FixCodec) -> pyarrow.Table:
    """The source rows parsed once at the FixMsg boundary."""
    return _parsed(raw_table, codec)


def test_retained_fixture_fields_partition_into_entries_and_unmap(
    table: pyarrow.Table,
) -> None:
    """Pinned fixture totals make loss or duplication move a fixed expectation."""
    entries = pyarrow.compute.list_value_length(table.column("entries")).to_pylist()
    unmap = pyarrow.compute.list_value_length(table.column("unmap")).to_pylist()

    assert entries == [None, None, 1, 1, 2, 0, 0, None, None, None, 0]
    assert unmap == [None, None, None, None, None, 15, None, None, None, None, 8]
    assert sum(length or 0 for length in entries) == 4
    assert sum(length or 0 for length in unmap) == 23
    assert sum(length or 0 for length in entries + unmap) == 27


# -- the protocols -----------------------------------------------------------


def test_text_file_outputs_only_the_message_contract(raw_table: pyarrow.Table) -> None:
    assert raw_table.num_rows == EXPECTED_RECORDS
    assert raw_table.schema.names == Message.into_field().names
    assert not {
        "protocolversion",
        "Parties",
    } & set(raw_table.schema.names)
    assert {"protocolcode", *LIFTED_HEADER.values()} <= set(raw_table.schema.names)
    assert "CheckSum" not in raw_table.schema.names, "the boundary is not one of the lifted"
    # Protocol-neutral: this stage reads no numbers and no clocks, so every
    # lifted column is stored as the text the payload spelled.
    for name in LIFTED_HEADER.values():
        assert pyarrow.types.is_string(raw_table.schema.field(name).type), name
    assert _keys(raw_table.column("entries")[PIPED]) == [
        token.split("=", 1)[0]
        for token in WIRE.strip("|").split("|")
        if token.split("=", 1)[0] not in LIFTED_HEADER
    ]
    assert [raw_table.column(name)[PIPED].as_py() for name in WIRE_HEADER] == [
        "FIX.4.2",
        "176",
        "1092",
        "D",
        "BUYSIDE",
        "20260814-00:05:01.147",
        "XPAR",
    ]
    assert UNLIFTED_TRAILER in _keys(raw_table.column("entries")[PIPED])


def test_every_line_lands_in_the_protocol_the_rules_claim(table: pyarrow.Table) -> None:
    assert table.num_rows == EXPECTED_RECORDS
    assert table.column("protocolcode").to_pylist() == EXPECTED_PROTOCOLS


def test_the_stored_column_and_a_rebuilt_row_agree(table: pyarrow.Table) -> None:
    """One classifier, so a row rebuilt from its text answers what the batch did."""
    rebuilt = [Message(message=one) for one in table.column("message").to_pylist()]
    assert [row.protocolcode for row in rebuilt] == table.column("protocolcode").to_pylist()


# -- what a line carries -----------------------------------------------------


def test_a_wire_message_yields_its_body_and_nothing_around_it(table: pyarrow.Table) -> None:
    """The line has a prefix *and* a suffix, and neither is in the message."""
    assert table.column("symbol")[PIPED].as_py() == "TTF"
    assert table.column("side")[PIPED].as_py() == "1"
    assert table.column("price")[PIPED].as_py() == 41.25, "`44=41.2500` is a Price, so a number"
    # `52` used to sit here too, as the sidecar an instant column cannot spell
    # back. The raw stage lifts it now, so it never reaches the FIX stage to
    # be audited and the trailing zeros of `44=41.2500` are the only text left.
    assert _tagged(table.column("entries")[PIPED]) == [(44, "41.2500")]
    assert _keys(table.column("entries")[PIPED]) == ["Price"]
    assert _named(table.column("entries")[PIPED]) == []
    around = str([table.column(name)[PIPED].as_py() for name in FLAT_NAMES])
    assert "sending" not in around and "queued" not in around


def test_every_field_of_a_wire_message_lands_in_one_of_the_three_places(
    table: pyarrow.Table,
) -> None:
    """Nothing drops or duplicates across both pair lists and the columns."""
    pairs = _tagged(table.column("entries")[PIPED])
    resolved = len(pairs) - len(_audit_pairs(table, PIPED, pairs))
    rest = len(_named(table.column("entries")[PIPED]))
    assert resolved + rest + _lifted(table, PIPED) == EXPECTED_WIRE_FIELDS


def test_the_caret_and_the_soh_lines_keep_their_distinct_version_semantics(
    table: pyarrow.Table,
) -> None:
    """Known `FIX.4.4` decodes; malformed `FIX4` still gets only its header read.

    The header lift is the raw stage's and knows no versions, so the caret
    line's six standard fields land in columns exactly as the SOH line's do.
    What separates the two is everything past the header: `FIX.4.4` types its
    body, and `FIX4` names no dictionary, so its body stays raw.
    """
    assert table.column("beginstring")[SOHED].as_py() == "FIX.4.4"
    assert table.column("msgseqnum")[SOHED].as_py() == 1094
    assert table.column("checksum")[SOHED].as_py() == "118"
    assert table.column("msgtype")[SOHED].as_py() == "8"
    assert _tagged(table.column("entries")[SOHED]) == [(31, "41.2500"), (6, "41.2500")]

    assert _tagged(table.column("entries")[CARET]) == CARET_RESIDUAL_PAIRS
    assert _keys(table.column("entries")[CARET]) == [str(tag) for tag, _ in CARET_RESIDUAL_PAIRS]
    assert _named(table.column("entries")[CARET]) == []
    assert table.column("unmap")[CARET].as_py() is None
    assert [table.column(name)[CARET].as_py() for name in WIRE_HEADER] == [
        "FIX4",
        61,
        1093,
        "0",
        "XPAR",
        datetime(2026, 8, 14, 0, 5, 1, 148000, tzinfo=UTC),
        "BUYSIDE",
    ], "every header field the line wrote, and the two integers and the instant typed"
    _assert_no_semantic_columns(table, CARET)


def test_a_flat_tag_stays_only_for_a_repeat_or_a_lossless_audit(table: pyarrow.Table) -> None:
    """A singleton sidecar exists only when typing cannot reproduce its text."""
    assert _lifted(table, PIPED), "so the loop below has something to be true about"
    for row in range(table.num_rows):
        if row == CARET:
            continue
        pairs = _tagged(table.column("entries")[row])
        if pairs is None:
            continue
        left = [tag for tag, _ in pairs if tag in COLUMNS]
        audited = {tag for tag, _ in _audit_pairs(table, row, pairs)}
        assert all(left.count(tag) > 1 or tag in audited for tag in left)


def test_every_field_of_the_bridge_line_lands_in_one_of_the_four_places(
    table: pyarrow.Table,
) -> None:
    """A versionless named message remains complete and in wire order."""
    assert table.column("entries")[BRIDGE].as_py() == []
    assert _tagged(table.column("unmap")[BRIDGE]) == []
    assert _named(table.column("unmap")[BRIDGE]) == BRIDGE_RAW_PAIRS
    assert len(BRIDGE_RAW_PAIRS) == EXPECTED_BRIDGE_PAIRS
    _assert_no_semantic_columns(table, BRIDGE)


def test_the_versionless_bridge_line_does_not_guess_a_dictionary(
    table: pyarrow.Table,
) -> None:
    """Known-looking names are still raw without message-local version evidence."""
    raw = dict(_named(table.column("unmap")[BRIDGE]))
    assert raw["SYMBOL"] == "TTF"
    assert raw["SIDE"] == "1"
    assert raw["ORDERQTY"] == "1200"
    assert raw["PRICE"] == "41.2500"
    assert _tagged(table.column("unmap")[BRIDGE]) == []
    _assert_no_semantic_columns(table, BRIDGE)


def test_the_versionless_bridge_group_stays_raw_in_wire_order(table: pyarrow.Table) -> None:
    pairs = _named(table.column("unmap")[BRIDGE])
    assert pairs[6:13] == BRIDGE_RAW_PAIRS[6:13]
    assert pairs[13] == ("TRANSACTTIME", "20260814-00:05:01.148")
    assert table.column("transacttime")[BRIDGE].as_py() is None
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
    by_id = {row[0]["partyid"]: row[0]["buffer"] for row in parsed.column("parties").to_pylist()}

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
    assert table.column("beginstring")[WRAPPED].as_py() == "FIX.4.2", "the wire header survives"
    assert table.column("msgtype")[WRAPPED].as_py() == "UL"
    assert table.column("symbol")[WRAPPED].as_py() == "TTF", "and so do the names"
    assert table.column("side")[WRAPPED].as_py() == "1"
    assert table.column("orderqty")[WRAPPED].as_py() == 1200.0
    assert table.column("isincode")[WRAPPED].as_py() == "XX0000084733"
    assert _named(table.column("entries")[WRAPPED]) == []


def test_a_bridge_message_separated_by_its_own_markers_reads_the_same(
    table: pyarrow.Table,
) -> None:
    """`#A=1#B=2` puts nothing between its tokens, so the next `#` ends the value.

    Read the character there as the separator -- which is what a bridge with a
    `|` between its tokens wants -- and it is the tail of the value in front of
    it: `#A=1#B=2` came back as `A=''` and `B` glued to whatever followed. It
    parsed, which is how it would have travelled.
    """
    assert table.column("entries")[HASHED].as_py() == []
    assert _named(table.column("unmap")[HASHED]) == HASHED_RAW_PAIRS
    assert _tagged(table.column("unmap")[HASHED]) == []
    assert _keys(table.column("unmap")[HASHED]) == [key for key, _ in HASHED_RAW_PAIRS]
    _assert_no_semantic_columns(table, HASHED)


def test_a_nested_entry_survives_a_marker_separated_line(table: pyarrow.Table) -> None:
    """Two separators on one line, and neither is the other's."""
    pairs = _named(table.column("unmap")[HASHED])
    assert pairs[4:7] == [
        ("NOPARTYIDS", "1"),
        ("NOPARTYIDS[0].PARTYID", "BUYSIDE"),
        ("NOPARTYIDS[0].PARTYROLE", "1"),
    ]
    assert table.column("parties")[HASHED].as_py() is None
    _assert_no_semantic_columns(table, HASHED)


def test_a_wire_message_that_only_mentions_a_marker_stays_a_wire_message() -> None:
    """A marker inside a `Text <58>` value is prose, so the frame stays numbered."""
    quoted = "8=FIX.4.4|35=8|58=see #A=1 and #B=2|10=1|"
    assert Message(message=quoted).protocolcode == "FIX"
    assert Message(message="8=FIX.4.2|35=ULX|#A=1|#B=2").protocolcode == "FIXML", (
        "and marked keys of its own make the same frame mixed"
    )


def test_versionless_names_are_all_kept_and_never_guessed(table: pyarrow.Table) -> None:
    assert _named(table.column("unmap")[BRIDGE]) == BRIDGE_RAW_PAIRS
    assert _keys(table.column("unmap")[BRIDGE]) == [key for key, _ in BRIDGE_RAW_PAIRS]
    assert table.column("isincode")[BRIDGE].as_py() is None
    _assert_no_semantic_columns(table, BRIDGE)


def test_a_line_carrying_no_message_has_no_pairs_at_all(table: pyarrow.Table) -> None:
    """Null, not an empty list: it was never a message and sent no session."""
    for row, protocol in enumerate(EXPECTED_PROTOCOLS):
        if protocol != NO_PROTOCOL:
            continue
        assert table.column("entries")[row].as_py() is None
        assert table.column("unmap")[row].as_py() is None
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
    assert table.column("beginstring")[PIPED].as_py() == "FIX.4.2"
    assert table.column("bodylength")[PIPED].as_py() == 176
    assert table.column("msgtype")[PIPED].as_py() == "D"
    assert table.column("msgseqnum")[PIPED].as_py() == 1092
    assert table.column("sendercompid")[PIPED].as_py() == "BUYSIDE"
    assert table.column("targetcompid")[PIPED].as_py() == "XPAR"
    assert table.column("checksum")[PIPED].as_py() == "203"


def test_a_header_field_written_twice_two_ways_is_lifted_by_neither(tmp_path: Path) -> None:
    """Two readings of one fact is not one statement of it.

    A row spelling `MsgSeqNum <34>` twice with two values has not said what its
    sequence number is, so the column stays null and both readings stay in
    `entries` where a reader can see them disagree. Spelled twice with one
    value it *has* said it, and every occurrence leaves the list.
    """
    staged = _staged_lines(
        tmp_path / "torn.txt",
        "FixSession_XPAR",
        [
            "sending >> 8=FIX.4.4|9=99|35=D|34=7|49=BUYSIDE|34=8|55=TTF|10=001|",
            "sending >> 8=FIX.4.4|9=99|35=D|34=7|49=BUYSIDE|34=7|55=TTF|10=002|",
        ],
    )
    assert staged.column("msgseqnum").to_pylist() == [None, "7"]
    assert _tagged(staged.column("entries")[0]) == [
        (34, "7"),
        (34, "8"),
        (55, "TTF"),
        (10, "001"),
    ], "neither reading is lifted, so both are still there to be read"
    assert _tagged(staged.column("entries")[1]) == [(55, "TTF"), (10, "002")]
    assert staged.column("sendercompid").to_pylist() == ["BUYSIDE", "BUYSIDE"], (
        "and one torn field costs the six beside it nothing"
    )


def test_the_header_lift_stops_at_the_checksum_it_is_measured_against(
    tmp_path: Path,
) -> None:
    """`CheckSum <10>` never lifts itself, and nothing standing behind it lifts either.

    A field is header because of where it is, not only what it is called: the
    trailer is the boundary, so a `49` written after it is something else the
    line said and stays in `entries` beside the checksum that ended the message.
    """
    staged = _staged_lines(
        tmp_path / "trailing.txt",
        "FixSession_XPAR",
        ["sending >> 8=FIX.4.4|9=99|35=D|34=9|55=TTF|10=003|49=LATE|52=20260814-09:30:00.000"],
    )
    assert staged.column("sendercompid")[0].as_py() is None
    assert staged.column("sendingtime")[0].as_py() is None
    assert staged.column("msgseqnum")[0].as_py() == "9", "what stood in front of it still lifted"
    assert _tagged(staged.column("entries")[0]) == [
        (55, "TTF"),
        (10, "003"),
        (49, "LATE"),
        (52, "20260814-09:30:00.000"),
    ]


def test_a_wire_message_lands_what_it_traded_in_columns(table: pyarrow.Table) -> None:
    """The other half of the flat layer: what a desk queries a fill by."""
    assert table.column("symbol")[SOHED].as_py() == "TTF"
    assert table.column("orderid")[SOHED].as_py() == "ORD-0000038106"
    assert table.column("execid")[SOHED].as_py() == "EXE-0000091233"
    assert table.column("ordstatus")[SOHED].as_py() == "1"
    assert table.column("exectype")[SOHED].as_py() == "F"
    assert table.column("lastpx")[SOHED].as_py() == 41.25
    assert table.column("lastqty")[SOHED].as_py() == 400.0
    assert table.column("leavesqty")[SOHED].as_py() == 800.0
    assert table.column("clordid")[PIPED].as_py() == "ORD-0000038106"
    assert table.column("ordtype")[PIPED].as_py() == "2"
    assert table.column("timeinforce")[PIPED].as_py() == "0"


def test_a_malformed_version_keeps_its_checksum_raw_and_lossless(table: pyarrow.Table) -> None:
    """Unknown `FIX4` cannot type fields, but raw CheckSum still keeps its zero."""
    assert dict(_tagged(table.column("entries")[CARET]))[10] == "017"
    assert table.column("checksum")[CARET].as_py() is None
    _assert_no_semantic_columns(table, CARET)


def test_a_stamp_lands_as_the_microsecond_utc_instant_it_spells(table: pyarrow.Table) -> None:
    sending = table.column("sendingtime")[PIPED].as_py()
    assert sending == datetime(2026, 8, 14, 0, 5, 1, 147000, tzinfo=UTC)
    sending_ns = int(sending.timestamp()) * 1_000_000_000 + sending.microsecond * 1000
    # `unix` is when the transaction happened, and this line's only FIX clock
    # is its `SendingTime <52>` -- so that is the rung that answered, and the
    # header's own stamp is a millisecond later in `recunix`.
    assert table.column("unix")[PIPED].as_py() == sending_ns
    assert table.column("unixsource")[PIPED].as_py() == "SendingTime"
    assert table.column("recunix")[PIPED].as_py() - sending_ns == 1_000_000


def test_a_field_the_message_never_sent_is_null_and_never_a_default(table: pyarrow.Table) -> None:
    """A stamp nobody wrote must not read as the epoch, which a sort puts first."""
    assert table.column("sendingtime")[SOHED].as_py() is None, "that line carries no tag 52"
    assert table.column("possdupflag").null_count == EXPECTED_RECORDS
    assert table.column("onbehalfofcompid").null_count == EXPECTED_RECORDS
    assert table.column("lastqty")[PIPED].as_py() is None, "and an order has filled nothing"


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
    assert parsed.column("possdupflag")[0].as_py() is True
    assert parsed.column("possresend")[0].as_py() is False
    assert parsed.column("sendingtime")[0].as_py() == datetime(
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
    assert _tagged(parsed.column("entries")[0]) == [
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
    assert parsed.column("msgseqnum")[0].as_py() == 8, "while what was written once still lifted"
    assert parsed.column("msgtype")[0].as_py() == "AB"

    assert _tagged(parsed.column("entries")[1]) == [], "and the line beside it lifted all of it"
    assert parsed.column("symbol")[1].as_py() == "TTF"
    assert parsed.column("price")[1].as_py() == 41.25
    assert parsed.column("msgseqnum")[1].as_py() == 9


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
    assert [tag for tag, _ in _tagged(parsed.column("entries")[0])] == [
        627,
        628,
        630,
        628,
        630,
    ]
    hops = [value for tag, value in _tagged(parsed.column("entries")[0]) if tag == 628]
    assert hops == ["HOP1", "HOP2"], "both of them, in the order they relayed it"
    assert parsed.column("sendercompid")[0].as_py() == "BUYSIDE", "and the scalars still lifted"


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
    assert parsed.column("entries")[0].as_py() == []
    assert _named(parsed.column("unmap")[0]) == [
        ("ISINCODE", "XX00"),
        ("SYMBOL", "TTF"),
        ("SIDE", "1"),
        ("SENDERCOMPID", "BRIDGE1"),
    ]
    assert _tagged(parsed.column("unmap")[0]) == []
    assert _keys(parsed.column("unmap")[0]) == [
        "ISINCODE",
        "SYMBOL",
        "SIDE",
        "SENDERCOMPID",
    ]
    # `SENDERCOMPID` stays in the list with the rest of them. The header lift
    # reads FIX's tags, not a bridge's names for them: which name a feed writes
    # is data, and a rendered spelling is kept exactly as it arrived.
    assert parsed.column("sendercompid")[0].as_py() is None
    _assert_no_semantic_columns(parsed, 0)


# -- millis and micros in one capture ----------------------------------------


def test_both_stamp_widths_read_as_the_instants_they_spell(table: pyarrow.Table) -> None:
    """The header clock, `recunix`, now that `unix` is the transaction."""
    recorded = table.column("recunix").to_pylist()
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
    with TextFile.from_path(
        copy,
        msg_type_event_types=event_types(codec.registry),
        protocol_rules=codec.rules,
    ) as again:
        written = _parsed(again.read_arrow_table(), codec)
    assert written.column("unix").to_pylist() == table.column("unix").to_pylist()
    assert written.column("protocolcode").to_pylist() == EXPECTED_PROTOCOLS
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
    with TextFile.from_path(
        SAMPLE,
        msg_type_event_types=event_types(codec.registry),
        protocol_rules=own.rules,
    ) as log:
        table = _parsed(log.read_arrow_table(), own)
    found = table.column("protocolcode").to_pylist()
    assert found[BRIDGE] == "BRIDGE", "the plugin decides now, not the message"
    assert found[PIPED] == NO_PROTOCOL, "and the wire messages are nobody's protocol"
    assert found[REJECTED] == "BRIDGE", "including the bridge's own prose line"
    assert table.column("entries")[PIPED].as_py() is None
    assert _lifted(table, PIPED) == len(WIRE_HEADER), (
        "only the standard header the raw stage lifted before any rule ran survives"
    )
    assert table.column("msgtype")[PIPED].as_py() == "D"
    assert table.column("beginstring")[PIPED].as_py() == "FIX.4.2"
    assert table.column("checksum")[PIPED].as_py() is None, "the trailer was never lifted"
    assert table.column("symbol")[PIPED].as_py() is None, "and no rule read the body"


def test_a_file_that_declares_no_rules_interprets_nothing_past_the_header(
    codec: FixCodec,
) -> None:
    """An empty FIX rule set reads no payload; the raw stage still lifted the header."""
    quiet = FixCodec(registry=codec.registry, rules=Rules(rules=[]))
    with TextFile.from_path(
        SAMPLE,
        msg_type_event_types=event_types(codec.registry),
        protocol_rules=quiet.rules,
    ) as log:
        table = _parsed(log.read_arrow_table(), quiet)
    assert table.column("protocolcode").to_pylist() == [NO_PROTOCOL] * EXPECTED_RECORDS
    assert table.column("entries").to_pylist() == [None] * EXPECTED_RECORDS
    assert table.column("parties").to_pylist() == [None] * EXPECTED_RECORDS
    assert table.column("symbol").to_pylist() == [None] * EXPECTED_RECORDS
    assert table.column("isincode").to_pylist() == [None] * EXPECTED_RECORDS
    assert table.column("code").to_pylist() == [""] * EXPECTED_RECORDS
    assert table.column("msgtype").null_count < EXPECTED_RECORDS
    # The header is not the rule set's to withhold: it was lifted upstream of
    # any protocol, so it stands here with no rule in sight.
    assert [table.column(name)[PIPED].as_py() for name in WIRE_HEADER] == [
        "FIX.4.2",
        176,
        1092,
        "D",
        "BUYSIDE",
        datetime(2026, 8, 14, 0, 5, 1, 147000, tzinfo=UTC),
        "XPAR",
    ]
    assert table.column("checksum").null_count == EXPECTED_RECORDS, (
        "the trailer is nobody's header, so nothing lifted it"
    )
    assert all(
        table.column(name).null_count == EXPECTED_RECORDS
        for name in FLAT_NAMES
        if name not in set(LIFTED_HEADER.values())
    )


def test_a_sparse_codec_gets_typed_nulls_for_optional_declared_columns(
    tmp_path: Path, codec: FixCodec
) -> None:
    class SparseCodec(FixCodec):
        def into_lifted_columns(
            self, entries: object, version: str | None = None
        ) -> tuple[dict[str, object], object]:
            return {}, entries

        def into_component_columns(
            self, entries: object, version: str | None = None
        ) -> tuple[dict[str, object], object]:
            return {}, entries

    sparse = SparseCodec(registry=codec.registry)
    parsed = _one_line(tmp_path / "sparse.txt", sparse, "FixSession", "8=FIX.4.4|35=D|34=7|55=TTF|")

    assert parsed.column("symbol")[0].as_py() is None
    assert parsed.column("parties")[0].as_py() is None
    # `8` and `34` are gone from the list and `55` is not: a codec that lifts
    # nothing cannot suppress the header, because the raw stage lifted it
    # before this codec was asked anything.
    assert [tag for tag, _ in _tagged(parsed.column("entries")[0])] == [55]
    assert parsed.column("msgseqnum")[0].as_py() == 7
    assert parsed.column("beginstring")[0].as_py() == "FIX.4.4"
    assert parsed.column("msgtype").to_pylist() == ["D"]


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

    assert parsed.column("msgseqnum").to_pylist() == [7]
    assert parsed.column("code").to_pylist() == ["Q-1"]
    assert parsed.column("code").to_pylist() == ["Q-1"]
    assert parsed.column("quoteid").to_pylist() == ["Q-1"]
    assert parsed.column("quotereqid").to_pylist() == ["R-1"]
    assert parsed.column("quotetype").to_pylist() == [1]
    assert parsed.column("quotestatus").to_pylist() == [0]
    assert parsed.column("bidpx").to_pylist() == [100.0]
    assert parsed.column("offerpx").to_pylist() == [101.0]
    assert parsed.column("bidsize").to_pylist() == [10.0]
    assert parsed.column("offersize").to_pylist() == [12.0]
    assert parsed.column("defbidsize").to_pylist() == [9.0]
    assert parsed.column("defoffersize").to_pylist() == [11.0]
    assert parsed.column("validuntiltime").to_pylist() == [datetime(2026, 8, 21, 10, 5, tzinfo=UTC)]
    assert _tagged(parsed.column("entries")[0]) == [(62, "20260821-10:05:00")]


def test_repeating_quote_entries_stay_in_wire_order(tmp_path: Path, codec: FixCodec) -> None:
    parsed = _one_line(
        tmp_path / "mass_quote.txt",
        codec,
        "FixSession",
        "8=FIX.4.4|35=i|295=2|299=Q-1|132=100|299=Q-2|132=101|",
    )

    assert parsed.column("noquoteentries").to_pylist() == [2]
    assert parsed.column("quoteentryid").to_pylist() == [None]
    assert parsed.column("bidpx").to_pylist() == [None]
    assert _tagged(parsed.column("entries")[0]) == [
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
    with TextFile.from_path(
        SAMPLE,
        msg_type_event_types=event_types(cold.registry),
        protocol_rules=cold.rules,
    ) as log:
        table = _parsed(log.read_arrow_table(), cold)
    assert table.num_rows == EXPECTED_RECORDS
    # A cold dictionary types nothing, so the body is intact -- but the header
    # never needed one, and its six fields plus the discriminator are already
    # in columns. The checksum stays, as it does under a warm dictionary too.
    assert table.column("entries")[PIPED].as_py() == []
    assert _tagged(table.column("unmap")[PIPED]) == [
        (11, "ORD-0000038106"),
        (55, "TTF"),
        (54, "1"),
        (38, "1200"),
        (40, "2"),
        (44, "41.2500"),
        (59, "0"),
        (10, "203"),
    ]
    assert len(_keys(table.column("unmap")[PIPED])) == EXPECTED_WIRE_FIELDS - len(WIRE_HEADER)
    assert table.column("beginstring")[PIPED].as_py() == "FIX.4.2"
    assert table.column("msgseqnum")[PIPED].as_py() == 1092
    assert _tagged(table.column("unmap")[BRIDGE]) == []
    assert _named(table.column("unmap")[BRIDGE]) == BRIDGE_RAW_PAIRS
    assert len(_keys(table.column("unmap")[BRIDGE])) == EXPECTED_BRIDGE_PAIRS
    for row in (PIPED, CARET, SOHED, BRIDGE, WRAPPED, HASHED):
        _assert_no_semantic_columns(table, row)


# -- the same, over a set ----------------------------------------------------


def test_a_folder_of_captures_reads_the_messages_too(tmp_path: Path, codec: FixCodec) -> None:
    """`TextFiles` streams raw Message rows and FIX parses them at one boundary."""
    for name in ("app.1.txt", "app.2.txt"):
        (tmp_path / name).write_bytes(SAMPLE_BYTES)
    files = TextFiles.from_folder(
        tmp_path,
        static_values={"bridge": "bridge-1"},
        msg_type_event_types=event_types(codec.registry),
        protocol_rules=codec.rules,
    )
    table = _parsed(files.read_arrow_table(), codec)
    assert table.num_rows == EXPECTED_RECORDS * 2
    assert table.column("protocolcode").to_pylist() == EXPECTED_PROTOCOLS * 2
    assert _named(table.column("unmap")[BRIDGE]) == BRIDGE_RAW_PAIRS
    assert _named(table.column("unmap")[EXPECTED_RECORDS + BRIDGE]) == BRIDGE_RAW_PAIRS
    _assert_no_semantic_columns(table, BRIDGE)
    _assert_no_semantic_columns(table, EXPECTED_RECORDS + BRIDGE)
    sequences = table.column("msgseqnum").to_pylist()
    assert sequences[PIPED] == 1092, "the flat layer of the first file"
    assert sequences[EXPECTED_RECORDS + PIPED] == 1092, "and of the second, read the same way"
    assert table.schema.names[-1] == "bridge", "and a static column still lands last"
    assert table.column("bridge").to_pylist() == ["bridge-1"] * (EXPECTED_RECORDS * 2)


# -- values that mean nothing ------------------------------------------------


def test_absent_values_never_reach_a_column(tmp_path: Path, codec: FixCodec) -> None:
    message = "toBridge #SYMBOL=TTF|#SIDE=<null>|#ACCOUNT=|#TEXT=N/A|#ORDERQTY=1200"
    table = _one_line(tmp_path / "absent.txt", codec, "ULBridge", message)
    assert table.column("entries")[0].as_py() == []
    assert _tagged(table.column("unmap")[0]) == []
    assert _named(table.column("unmap")[0]) == [
        ("SYMBOL", "TTF"),
        ("ORDERQTY", "1200"),
    ]
    assert _keys(table.column("unmap")[0]) == ["SYMBOL", "ORDERQTY"]
    _assert_no_semantic_columns(table, 0)

    keeping = FixCodec(registry=codec.registry, null_values=frozenset())
    kept = _one_line(tmp_path / "kept.txt", keeping, "ULBridge", message)
    assert _named(kept.column("unmap")[0]) == [
        ("SYMBOL", "TTF"),
        ("SIDE", "<null>"),
        ("ACCOUNT", ""),
        ("TEXT", "N/A"),
        ("ORDERQTY", "1200"),
    ]
    assert _keys(kept.column("unmap")[0]) == [
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


def _audit_pairs(
    table: pyarrow.Table, row: int, pairs: list[tuple[int, str]]
) -> list[tuple[int, str]]:
    """Raw sidecars whose typed columns cannot reproduce their spelling."""
    found = []
    for tag, raw in pairs:
        column = COLUMNS.get(tag)
        typed = None if column is None else table.column(column)[row].as_py()
        if typed is not None and render_fix_value(typed) != raw:
            found.append((tag, raw))
    return found


def _semantic_floor(table: pyarrow.Table, row: int) -> int:
    """What a row fills before any dictionary is consulted: the standard header."""
    return sum(table.column(name)[row].as_py() is not None for name in LIFTED_HEADER.values())


def _assert_no_semantic_columns(table: pyarrow.Table, row: int) -> None:
    """Unknown-version rows retain pairs without publishing interpreted values.

    The seven standard header fields are the ones such a row may still fill:
    they are lifted off the front of the message by the raw stage, before any
    protocol reads it, and a row whose version nothing resolved still knows
    who sent what kind of message to whom and when. `CheckSum <10>` is not
    among them, so it stays with the body a dictionary would have had to type.
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
        sum(party[name] is not None for name in ("PartyID", "PartyIDSource", "PartyRole"))
        + len(party["buffer"] or ())
        for party in parties
    )


def _entries(scalar: pyarrow.Scalar) -> list[dict[str, object]] | None:
    """One row of `entries`, or `None` where the line carried no message."""
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
    lead = entry.get("comp")
    return f"{lead}.{entry['key']}" if lead else str(entry["key"])


def _write_lines(path: Path, plugin: str, messages: list[str]) -> None:
    """Synthesised payloads behind the fixed log header this file reads."""
    path.write_text(
        "".join(f"2026-08-14 00:05:01.147 [t] [{plugin}] (INFO) {one}\n" for one in messages)
    )


def _staged_lines(path: Path, plugin: str, messages: list[str]) -> pyarrow.Table:
    """Synthesised log lines as the raw Message rows they stage as.

    Stops one stage short of `_lines`: no codec has read the payload, so what
    stands in a column here is what the raw stage put there itself.
    """
    _write_lines(path, plugin, messages)
    with TextFile.from_path(
        path,
        msg_type_event_types=event_types(),
        protocol_rules=Rules.into_default(),
    ) as log:
        return log.read_arrow_table()


def _lines(path: Path, codec: FixCodec, plugin: str, messages: list[str]) -> pyarrow.Table:
    """Synthesised log lines converted from Message to FixMsg in one batch."""
    _write_lines(path, plugin, messages)
    with TextFile.from_path(
        path,
        msg_type_event_types=event_types(codec.registry),
        protocol_rules=codec.rules,
    ) as log:
        return _parsed(log.read_arrow_table(), codec)


def _one_line(path: Path, codec: FixCodec, plugin: str, message: str) -> pyarrow.Table:
    """One synthesised log line through the whole parser, as the row it lands as."""
    return _lines(path, codec, plugin, [message])


def _parsed(messages: pyarrow.Table, codec: FixCodec) -> pyarrow.Table:
    """Convert raw Message batches through the public FixMsg boundary."""
    return pyarrow.Table.from_batches(
        [FixMsg.from_message_batch(batch, codec) for batch in messages.to_batches()]
    )


def _parsed_lines(codec: FixCodec, *lines: str) -> pyarrow.Table:
    """Build raw rows in memory and parse them only at the FixMsg boundary."""
    messages = Message.into_arrow_reader(Message(message=line) for line in lines).read_all()
    parsed = Message.parse_arrow(
        messages.column("message"),
        event_types(codec.registry),
        messages.column("plugincode"),
        protocol_rules=codec.rules,
    )
    messages = messages.set_column(
        messages.schema.get_field_index("eventtype"), "eventtype", parsed["eventtype"]
    )
    messages = messages.set_column(
        messages.schema.get_field_index("protocolcode"),
        "protocolcode",
        parsed["protocolcode"],
    )
    return _parsed(messages, codec)


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
    with TextFile.from_path(
        path,
        msg_type_event_types=event_types(codec.registry),
        protocol_rules=codec.rules,
    ) as log:
        return _parsed(log.read_arrow_table(), codec)


def test_unix_is_the_transaction_time_and_recunix_is_when_it_was_recorded(
    transacted: pyarrow.Table,
) -> None:
    """The whole point: a row's `unix` is when it happened, not when it printed."""
    assert transacted.column("unixsource").to_pylist() == TRANSACTED_SOURCES
    unix = transacted.column("unix").to_pylist()
    assert unix[0] == unix_of("20260814-09:29:00.000"), "the group, not TransactTime"
    assert unix[1] == unix_of("20260814-09:27:00.000"), "an order takes TIME_IN"
    assert unix[2] == unix_of("20260814-09:26:00.000")
    assert unix[3] == unix_of("20260814-09:31:00.000")
    recorded = transacted.column("recunix").to_pylist()
    assert recorded == [
        unix_of(f"2026-08-14 00:05:0{index + 1}.147") for index in range(len(recorded))
    ], "the header clock is preserved, row for row"
    assert unix[4] == recorded[4], "a line carrying no message happened when it was written"


def test_the_group_and_transact_time_disagreeing_is_decided_by_the_chain(
    transacted: pyarrow.Table,
) -> None:
    """Both rows carry `TransactTime <60>` at 09:30 and neither is stamped with it."""
    claimed = transacted.column("transacttime").to_pylist()[:2]
    assert all(one is not None for one in claimed), "the claim is still stored"
    unix = transacted.column("unix").to_pylist()[:2]
    assert all(one != unix_of("20260814-09:30:00.000") for one in unix)


def test_unix_partition_follows_the_resolved_time_and_not_the_header(
    transacted: pyarrow.Table,
) -> None:
    """A row moves partition with its transaction time, or a sorted read breaks."""
    for unix, hour in zip(
        transacted.column("unix").to_pylist(),
        transacted.column("unixpartition").to_pylist(),
        strict=True,
    ):
        assert hour == (unix - unix % HOUR) // SECOND


def test_the_parse_and_the_translation_resolve_one_row_alike(
    transacted: pyarrow.Table,
) -> None:
    """One resolver, two executions: a column of rows and one message at a time."""
    rows = [FixMsg.from_dict(row) for row in transacted.to_pylist()]
    for row in rows:
        found = row.into_fix_events().transacted
        if not row.message or "8=FIX" not in row.message:
            continue
        assert (found.unix, found.source) == (row.unix, row.unixsource), row.message


# -- the raw/parsed boundary -------------------------------------------------

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
def staged(tmp_path: Path) -> pyarrow.Table:
    """What `parse_messages` writes: raw Message rows."""
    path = tmp_path / "staged.txt"
    path.write_text(STAGED_LINES)
    with TextFile.from_path(
        path,
        msg_type_event_types=event_types(),
        protocol_rules=Rules.into_default(),
    ) as log:
        return log.read_arrow_table()


@pytest.fixture
def resolved(staged: pyarrow.Table, codec: FixCodec) -> pyarrow.Table:
    """What `parse_fix` makes of the raw rows."""
    return _parsed(staged, codec)


def test_two_captures_share_payload_identities_across_clocks(tmp_path: Path) -> None:
    """Capture time anchors `hash` without changing payload identity."""
    tables = []
    for name, text in (
        ("first.txt", STAGED_LINES),
        ("second.txt", STAGED_LINES.replace("2026-08-14", "2026-08-15")),
    ):
        path = tmp_path / name
        path.write_text(text)
        with TextFile.from_path(path) as log:
            tables.append(log.read_arrow_table())
    assert tables[0].num_rows == len(STAGED_LINES.splitlines())
    assert tables[0].column("vhash").equals(tables[1].column("vhash"))
    assert tables[0].column("xhash").equals(tables[1].column("xhash"))
    assert not tables[0].column("hash").equals(tables[1].column("hash"))


def test_the_message_stage_keeps_raw_source_facts_and_unresolved_arguments(
    staged: pyarrow.Table,
) -> None:
    assert staged.schema.names == Message.into_field().names
    expected = [line.split("(INFO) ", 1)[1] for line in STAGED_LINES.splitlines()]
    assert staged.column("message").to_pylist() == expected
    assert "entries" in staged.schema.names
    assert staged.column("msgtype").to_pylist() == ["D", "D", None, None]
    assert not {"Side", "Parties"} & set(staged.schema.names)
    assert staged.column("protocolcode").to_pylist() == ["FIX", "UL", "MISC", "OTHER"]
    # `8=FIX.4.4|35=D` opened the line and neither is here: `entries` starts at
    # the first field the header lift left behind.
    assert [entry["value"] for entry in staged.column("entries")[0].as_py()[:3]] == [
        "C1",
        "1",
        "IBM",
    ]
    # The second line spells its header in rendered names, which the lift does
    # not read: it answers to FIX's tags, and a bridge's own name for a field
    # stays in `entries` where the FIX stage decides what it is.
    assert staged.column("beginstring").to_pylist() == ["FIX.4.4", None, None, None]
    assert _keys(staged.column("entries")[1]) == [
        "BEGINSTRING",
        "SIDE",
        "SYMBOL",
        "NOPARTYIDS[0].PARTYID",
        "TECH.CLIENTID",
    ], "only `#MSGTYPE=` lifted; every other rendered name is untouched"
    # Protocol-neutral: this stage neither counts nor parses a clock, so the
    # trailer's `10=000` is text in `entries` and nothing was cast.
    assert staged.column("msgseqnum").to_pylist() == [None] * 4
    assert staged.column("entries")[0].as_py()[-1]["value"] == "000"


def test_fix_conversion_adds_the_canonical_fix_contract(
    staged: pyarrow.Table, resolved: pyarrow.Table
) -> None:
    assert staged.schema.names == Message.into_field().names
    assert resolved.schema.names == FixMsg.into_field().names
    assert resolved.column("msgtype").to_pylist() == ["D", "D", None, None]
    assert resolved.column("side").to_pylist() == ["1", "1", None, None]
    assert resolved.column("parties")[0].as_py()[0]["partyid"] == "BUYSIDE"


def test_fix_conversion_never_parses_the_raw_message_again(
    staged: pyarrow.Table, codec: FixCodec
) -> None:
    class CountingCodec(FixCodec):
        parsed_rows = 0

        def into_pairs(self, messages, protocol=NO_PROTOCOL):
            self.parsed_rows += len(messages)
            return super().into_pairs(messages, protocol)

    counting = CountingCodec(registry=codec.registry)
    parsed = _parsed(staged, counting)

    assert parsed.num_rows == staged.num_rows
    assert counting.parsed_rows == 0


def test_the_redirection_sends_one_input_to_all_three_tables(
    resolved: pyarrow.Table,
) -> None:
    """One condition, one place: what `eventtype` the rules made of the line.

    The second row is the interesting one: its named `#MSGTYPE=D` is promoted
    before FIX transcription, so it takes the same market route as wire tag 35.
    """
    rules = Rules()
    categories = rules.into_arrow_category_array(
        resolved.column("protocolcode").combine_chunks(),
        resolved.column("eventtype").combine_chunks(),
    )
    assert categories.to_pylist() == ["market", "market", "misc", "misc"]


def test_a_misc_row_keeps_its_raw_line_and_a_market_row_gives_it_up() -> None:
    """The one stored shape: two content columns, and which is filled where."""
    market = FixMsg(unix=1, hash=1, message=None, entries=[])
    misc = FixMsg(unix=2, hash=2, message="heartbeat", entries=None)
    assert market.message is None and market.entries == []
    assert misc.message == "heartbeat"
    field = FixMsg.into_field()
    assert field.field("message").nullable, "a market row leaves it null"


def test_protocol_version_agrees_with_the_columns_it_derives_from(
    resolved: pyarrow.Table,
) -> None:
    """A row where the stored version and its own evidence disagree is corrupt."""
    for version, begin, appl in zip(
        resolved.column("protocolversion").to_pylist(),
        resolved.column("beginstring").to_pylist(),
        resolved.column("applverid").to_pylist(),
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
    parsed = _parsed_lines(codec, *messages.to_pylist())
    assert parsed.column("protocolversion").to_pylist() == [None, None]
    assert parsed.column("protocolversionsource").to_pylist() == ["none", "none"], (
        "a FIXT header with no ApplVerID resolves nothing, and neither does a non-message"
    )


def test_a_fixt_message_resolves_through_its_application_version(codec: FixCodec) -> None:
    """FIXT is the transport; `ApplVerID <1128>` says which application version."""
    messages = pyarrow.array(["8=FIXT.1.1|35=D|1128=9|11=C1|10=000"], pyarrow.string())
    parsed = _parsed_lines(codec, *messages.to_pylist())
    assert parsed.column("protocolversion").to_pylist() == ["5.0.SP2"]
    assert parsed.column("protocolversionsource").to_pylist() == ["application_version"]


def test_wire_order_survives_dictionary_completion(codec: FixCodec) -> None:
    """Structuring and dictionary completion both preserve wire order."""
    line = "8=FIX.4.4|35=D|453=3|448=ONE|448=TWO|448=THREE|10=000"
    pairs = codec.into_pairs(pyarrow.array([line], pyarrow.string()), "FIX")
    columns = {"entries": codec.into_message_entries(pairs)}
    structured = [
        entry["value"] for entry in columns["entries"].to_pylist()[0] if entry["tag"] == 448
    ]
    assert structured == ["ONE", "TWO", "THREE"], "the sequence, not the set"
    done = codec.complete_entries(columns["entries"], "4.4").to_pylist()[0]
    assert [entry["value"] for entry in done if entry["tag"] == 448] == ["ONE", "TWO", "THREE"]


def test_the_split_key_still_spells_what_the_line_wrote(codec: FixCodec) -> None:
    """An indexed `comp` joined to `key` survives dictionary completion."""
    line = "send #NOPARTYIDS[0].PARTYID=ABC|#TECH.CLIENTID=42"
    pairs = codec.into_pairs(pyarrow.array([line], pyarrow.string()), "FIXML")
    columns = {"entries": codec.into_message_entries(pairs)}
    for level in (columns["entries"], codec.complete_entries(columns["entries"], "4.4")):
        rebuilt = [
            ".".join(part for part in (entry["comp"], entry["key"]) if part).upper()
            for entry in level.to_pylist()[0]
        ]
        assert rebuilt == ["NOPARTYIDS[0].PARTYID", "TECH.CLIENTID"]
