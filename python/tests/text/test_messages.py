"""The message layer end to end: a capture in, a protocol, tags and flat columns out."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fix import (
    COMMON,
    FLAT,
    NO_PROTOCOL,
    SESSION,
    FixCodec,
    FixRegistry,
    Rule,
    Rules,
    unix_of,
)
from rekep.fix.columns import COLUMNS
from rekep.text import HEADER_PATTERN, TextFile, TextFiles

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
#: declares it. Pinned, because a tag quietly leaving the list would take its
#: column's assertions with it.
FLAT_NAMES = tuple(COLUMNS.values())
EXPECTED_SESSION_COLUMNS = 33
EXPECTED_COMMON_COLUMNS = 26
EXPECTED_FLAT_COLUMNS = 59

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


def test_the_sample_is_the_shape_the_tests_assume() -> None:
    assert len(RECORDS) == EXPECTED_RECORDS
    assert len(CONTINUATIONS) == EXPECTED_CONTINUATIONS
    assert SAMPLE_BYTES.count(b"\x01") > 0, "the SOH lines are bytes, not four characters"
    assert b"^A9=61" in SAMPLE_BYTES, "and the caret-A line is the two characters"
    assert len(WIRE.strip("|").split("|")) == EXPECTED_WIRE_FIELDS


@pytest.fixture
def codec() -> FixCodec:
    return FixCodec(registry=FixRegistry(cache_dir=DATA, offline=True), fix_version="4.4")


@pytest.fixture
def table(codec: FixCodec) -> pyarrow.Table:
    with TextFile.from_path(SAMPLE, codec=codec) as log:
        return log.read_arrow_table()


# -- the protocols -----------------------------------------------------------


def test_every_line_lands_in_the_protocol_the_rules_claim(table: pyarrow.Table) -> None:
    assert table.num_rows == EXPECTED_RECORDS
    assert table.column("protocol").to_pylist() == EXPECTED_PROTOCOLS


def test_the_column_and_the_line_agree_on_every_row(table: pyarrow.Table) -> None:
    """Scalar and vectorised, row for row, on the capture itself."""
    scalar = [Rules.DEFAULT.categorise(one) for one in table.column("message").to_pylist()]
    assert [rule.protocol for rule in scalar] == table.column("protocol").to_pylist()


# -- what a line carries -----------------------------------------------------


def test_a_wire_message_yields_its_body_and_nothing_around_it(table: pyarrow.Table) -> None:
    """The line has a prefix *and* a suffix, and neither is in the message."""
    assert table.column("symbol")[PIPED].as_py() == "TTF"
    assert table.column("side")[PIPED].as_py() == "1"
    assert table.column("price")[PIPED].as_py() == 41.25, "`44=41.2500` is a Price, so a number"
    assert table.column("fix_tags")[PIPED].as_py() == [], "every field of it was worth a column"
    assert table.column("keyval")[PIPED].as_py() == []
    around = str([table.column(name)[PIPED].as_py() for name in FLAT_NAMES])
    assert "sending" not in around and "queued" not in around


def test_every_field_of_a_wire_message_lands_in_one_of_the_three_places(
    table: pyarrow.Table,
) -> None:
    """Nothing dropped, nothing counted twice, across both maps and the columns."""
    resolved = len(table.column("fix_tags")[PIPED].as_py())
    rest = len(table.column("keyval")[PIPED].as_py())
    assert resolved + rest + _lifted(table, PIPED) == EXPECTED_WIRE_FIELDS


def test_the_caret_and_the_soh_lines_parse_to_the_same_shape(table: pyarrow.Table) -> None:
    """One capture, three separators, and a batch is sampled once by contract."""
    for row in (CARET, SOHED):
        assert table.column("begin_string")[row].as_py().startswith("FIX")
        assert table.column("seq")[row].as_py() > 0
        assert table.column("check_sum")[row].as_py() is not None, "every one ends at its CheckSum"
    assert table.column("msg_type")[CARET].as_py() == "0"
    assert table.column("msg_type")[SOHED].as_py() == "8"
    assert table.column("fix_tags")[CARET].as_py() == [], "a heartbeat is session and nothing else"


def test_no_flat_tag_is_left_in_the_map_it_was_lifted_out_of(table: pyarrow.Table) -> None:
    """One fact stored twice is one that can disagree with itself.

    A tag that repeats is the exception, and it is not half-lifted either: it
    stays whole in the map, every occurrence of it, and its column is null.
    """
    assert _lifted(table, PIPED), "so the loop below has something to be true about"
    for row in range(table.num_rows):
        pairs = table.column("fix_tags")[row].as_py()
        if pairs is None:
            continue
        left = [tag for tag, _ in pairs if tag in COLUMNS]
        assert all(left.count(tag) > 1 for tag in left), "only a repeat may stay behind"


def test_every_field_of_the_bridge_line_lands_in_one_of_the_three_places(
    table: pyarrow.Table,
) -> None:
    """Nothing dropped, nothing counted twice -- the whole of the split."""
    resolved = len(table.column("fix_tags")[BRIDGE].as_py())
    rest = len(table.column("keyval")[BRIDGE].as_py())
    assert resolved + rest + _lifted(table, BRIDGE) == EXPECTED_BRIDGE_PAIRS


def test_the_bridge_line_resolves_the_names_the_dictionary_knows(table: pyarrow.Table) -> None:
    """A name is a tag, and a tag the flat layer names is a column of its own."""
    assert table.column("symbol")[BRIDGE].as_py() == "TTF"
    assert table.column("side")[BRIDGE].as_py() == "1"
    assert table.column("order_qty")[BRIDGE].as_py() == 1200.0
    assert dict(table.column("fix_tags")[BRIDGE].as_py())[453] == "2", "and the group stayed put"


def test_the_bridge_group_keeps_both_entries_in_wire_order(table: pyarrow.Table) -> None:
    tags = [tag for tag, _ in table.column("fix_tags")[BRIDGE].as_py()]
    entries = tags[tags.index(453) :]
    assert entries[:7] == [453, 448, 447, 452, 448, 447, 452]
    assert entries[7:] == [], "and what followed the group is a column now"
    assert table.column("transact_unix")[BRIDGE].as_py() == unix_of("20260814-00:05:01.148")
    parties = [value for tag, value in table.column("fix_tags")[BRIDGE].as_py() if tag == 448]
    assert parties == ["BUYSIDE", "XPAR"]


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
    assert dict(table.column("keyval")[WRAPPED].as_py()) == {"ISINCODE": "XX0000084733"}


def test_a_bridge_message_separated_by_its_own_markers_reads_the_same(
    table: pyarrow.Table,
) -> None:
    """`#A=1#B=2` puts nothing between its tokens, so the next `#` ends the value.

    Read the character there as the separator -- which is what a bridge with a
    `|` between its tokens wants -- and it is the tail of the value in front of
    it: `#A=1#B=2` came back as `A=''` and `B` glued to whatever followed. It
    parsed, which is how it would have travelled.
    """
    assert table.column("symbol")[HASHED].as_py() == "TTF"
    assert table.column("side")[HASHED].as_py() == "1"
    assert table.column("order_qty")[HASHED].as_py() == 1200.0
    tags = dict(table.column("fix_tags")[HASHED].as_py())
    assert tags[453] == "1", "and a group counted in it survives"
    assert dict(table.column("keyval")[HASHED].as_py()) == {
        "ISINCODE": "XX0000084733",
        "UNKNOWNVENUEFIELD": "Z9",
    }


def test_a_nested_entry_survives_a_marker_separated_line(table: pyarrow.Table) -> None:
    """Two separators on one line, and neither is the other's."""
    tags = [tag for tag, _ in table.column("fix_tags")[HASHED].as_py()]
    assert tags[tags.index(453) :] == [453, 448, 452], "the entry members, in wire order"


def test_a_wire_message_that_only_mentions_a_marker_stays_a_wire_message() -> None:
    """The discriminator is what the sender said it is, not a hash in a Text field."""
    quoted = "8=FIX.4.4|35=8|58=see #A=1 and #B=2|10=1|"
    assert Rules.DEFAULT.categorise(quoted).protocol == "FIX"
    assert Rules.DEFAULT.categorise("8=FIX.4.2|35=ULX|#A=1|#B=2").protocol == "FIX"


def test_a_name_no_dictionary_has_is_kept_and_never_guessed(table: pyarrow.Table) -> None:
    assert dict(table.column("keyval")[BRIDGE].as_py()) == {
        "ISINCODE": "XX0000084733",
        "UNKNOWNVENUEFIELD": "Z9",
    }


def test_a_line_carrying_no_message_has_no_pairs_at_all(table: pyarrow.Table) -> None:
    """Null, not an empty map: it was never a message, and it sent no session either."""
    for row, protocol in enumerate(EXPECTED_PROTOCOLS):
        if protocol != NO_PROTOCOL:
            continue
        assert table.column("fix_tags")[row].as_py() is None
        assert table.column("keyval")[row].as_py() is None
        assert _lifted(table, row) == 0


def test_a_stack_trace_still_folds_into_the_row_above_it(table: pyarrow.Table) -> None:
    """The message layer must not have cost the parser its continuations."""
    (folded,) = [
        one for one in table.column("message").to_pylist() if "IllegalStateException" in one
    ]
    assert folded.count("\n") == EXPECTED_CONTINUATIONS


# -- the flat layer, as columns ----------------------------------------------


def test_the_flat_layer_is_a_column_each_and_no_repeating_group(
    table: pyarrow.Table,
) -> None:
    """`NoHops <627>` is header too, and it is the one that must stay in the map."""
    assert len(SESSION) == EXPECTED_SESSION_COLUMNS
    assert len(COMMON) == EXPECTED_COMMON_COLUMNS
    assert len(FLAT) == EXPECTED_FLAT_COLUMNS
    assert len(FLAT_NAMES) == EXPECTED_FLAT_COLUMNS, "and no two tags share a column"
    assert set(FLAT_NAMES) <= set(table.schema.names)
    assert 627 not in COLUMNS, "one row of a repeating group is not one value"


def test_a_wire_message_lands_its_header_and_trailer_in_columns(table: pyarrow.Table) -> None:
    """Who sent it, to whom, in what order and when -- what a reader filters on."""
    assert table.column("begin_string")[PIPED].as_py() == "FIX.4.2"
    assert table.column("body_length")[PIPED].as_py() == 176
    assert table.column("msg_type")[PIPED].as_py() == "D"
    assert table.column("seq")[PIPED].as_py() == 1092
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


def test_a_checksum_keeps_the_leading_zero_that_makes_it_verify(table: pyarrow.Table) -> None:
    """Three digits, so a string: `017` read as `17` is a checksum that fails."""
    assert table.column("check_sum")[CARET].as_py() == "017"


def test_a_stamp_lands_as_the_nanoseconds_it_spells(table: pyarrow.Table) -> None:
    """Nanoseconds and not a timestamp, so a latency is a subtraction."""
    sending = table.column("sending_unix")[PIPED].as_py()
    assert sending == unix_of("20260814-00:05:01.147")
    assert sending == 1_786_665_901_147_000_000
    assert table.column("unix")[PIPED].as_py() - sending == 1_000_000, "a millisecond on the wire"


def test_a_field_the_message_never_sent_is_null_and_never_a_default(table: pyarrow.Table) -> None:
    """A stamp nobody wrote must not read as the epoch, which a sort puts first."""
    assert table.column("sending_unix")[SOHED].as_py() is None, "that line carries no tag 52"
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
    assert parsed.column("sending_unix")[0].as_py() == unix_of("20260814-00:05:01.147")


def test_a_tag_is_lifted_only_where_it_occurs_once_in_its_own_line(
    tmp_path: Path, codec: FixCodec
) -> None:
    """One symbol per line, or none -- never whichever leg was written first.

    Both lines are in one batch, and the count that decides is per `(row,
    tag)`: the multi-leg order keeps every `55` and every `44` in the map, and
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
    assert parsed.column("fix_tags")[0].as_py() == [
        (555, "2"),
        (600, "TTF"),
        (55, "SPREAD"),
        (44, "41.25"),
        (555, "2"),
        (55, "OTHER"),
        (44, "42.50"),
    ], "every occurrence of a repeated tag, in wire order"
    assert parsed.column("price")[0].as_py() is None, "no one price is the multi-leg order's"
    assert parsed.column("symbol")[0].as_py() == "", "nor one symbol -- and `symbol` is NOT NULL"
    assert parsed.column("seq")[0].as_py() == 8, "while what was written once still lifted"
    assert parsed.column("msg_type")[0].as_py() == "AB"

    assert parsed.column("fix_tags")[1].as_py() == [], "and the line beside it lifted all of it"
    assert parsed.column("symbol")[1].as_py() == "TTF"
    assert parsed.column("price")[1].as_py() == 41.25
    assert parsed.column("seq")[1].as_py() == 9


def test_a_hop_stays_in_the_map_because_one_row_of_it_is_not_one_value(
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
    assert [tag for tag, _ in parsed.column("fix_tags")[0].as_py()] == [627, 628, 630, 628, 630]
    hops = [value for tag, value in parsed.column("fix_tags")[0].as_py() if tag == 628]
    assert hops == ["HOP1", "HOP2"], "both of them, in the order they relayed it"
    assert parsed.column("sender_comp_id")[0].as_py() == "BUYSIDE", "and the scalars still lifted"


def test_a_bridge_name_the_dictionary_answers_for_is_lifted_like_a_tag(
    tmp_path: Path, codec: FixCodec
) -> None:
    """The flat layer is the tags, however the line spelled them."""
    parsed = _one_line(
        tmp_path / "named.txt",
        codec,
        "ULBridge",
        "toBridge #ISINCODE=XX00#SYMBOL=TTF#SIDE=1#ACCOUNT=<null>#SENDERCOMPID=BRIDGE1",
    )
    assert parsed.column("sender_comp_id")[0].as_py() == "BRIDGE1"
    assert parsed.column("symbol")[0].as_py() == "TTF"
    assert parsed.column("side")[0].as_py() == "1"
    assert parsed.column("account")[0].as_py() is None, "and the account said it had none"
    assert parsed.column("fix_tags")[0].as_py() == [], "a name it answers for is a column"
    assert parsed.column("keyval")[0].as_py() == [("ISINCODE", "XX00")]


# -- millis and micros in one capture ----------------------------------------


def test_both_stamp_widths_read_as_the_instants_they_spell(table: pyarrow.Table) -> None:
    unix = table.column("unix").to_pylist()
    assert unix[0] == 1_786_665_901_147_250_000, "micros, with a separator"
    assert unix[1] == 1_786_665_901_147_000_000, "millis only"
    assert unix[PIPED] == 1_786_665_901_148_000_000, "millis, comma-separated"
    assert unix[0] > unix[1], "a padded 147 is 147 ms, not 147 us -- and so is earlier"


def test_the_capture_reparses_to_the_same_instants(
    tmp_path: Path, codec: FixCodec, table: pyarrow.Table
) -> None:
    """Written back out and read again under the same codec, column for column."""
    copy = tmp_path / "copy.txt"
    TextFile.from_path(copy).write_arrow(table)
    with TextFile.from_path(copy, codec=codec) as again:
        written = again.read_arrow_table()
    assert written.column("unix").to_pylist() == table.column("unix").to_pylist()
    assert written.column("protocol").to_pylist() == EXPECTED_PROTOCOLS
    for name in FLAT_NAMES:
        assert written.column(name).to_pylist() == table.column(name).to_pylist(), name


# -- what a rule set decides -------------------------------------------------


def test_a_rule_set_from_a_document_reclassifies_a_line(tmp_path: Path, codec: FixCodec) -> None:
    path = tmp_path / "rules.yml"
    Rules(
        rules=[
            Rule(protocol="BRIDGE", driver_pattern="^ULBridge$", codec="ul"),
            Rules.DEFAULT.rule(NO_PROTOCOL),
        ]
    ).into_yaml(path)
    own = FixCodec(registry=codec.registry, rules=Rules.from_yaml(path), fix_version="4.4")
    with TextFile.from_path(SAMPLE, codec=own) as log:
        table = log.read_arrow_table()
    found = table.column("protocol").to_pylist()
    assert found[BRIDGE] == "BRIDGE", "the driver decides now, not the message"
    assert found[PIPED] == NO_PROTOCOL, "and the wire messages are nobody's protocol"
    assert found[REJECTED] == "BRIDGE", "including the bridge's own prose line"
    assert table.column("fix_tags")[PIPED].as_py() is None
    assert _lifted(table, PIPED) == 0, "a line nothing reads has no flat layer either"


def test_a_file_that_declares_no_rules_parses_as_it_always_did(codec: FixCodec) -> None:
    """The whole of the compatibility promise, in one assertion."""
    quiet = FixCodec(registry=codec.registry, rules=Rules(rules=[]))
    with TextFile.from_path(SAMPLE, codec=quiet) as log:
        table = log.read_arrow_table()
    assert table.column("protocol").to_pylist() == [NO_PROTOCOL] * EXPECTED_RECORDS
    assert table.column("fix_tags").to_pylist() == [None] * EXPECTED_RECORDS
    assert table.column("keyval").to_pylist() == [None] * EXPECTED_RECORDS
    assert table.column("symbol").to_pylist() == [""] * EXPECTED_RECORDS, "NOT NULL, so its default"
    nullable = [name for name in FLAT_NAMES if name != "symbol"]
    assert all(table.column(name).null_count == EXPECTED_RECORDS for name in nullable)


def test_a_cold_dictionary_costs_the_tags_and_never_the_capture(tmp_path: Path) -> None:
    cold = FixCodec(registry=FixRegistry(cache_dir=tmp_path, offline=True), fix_version="4.4")
    with TextFile.from_path(SAMPLE, codec=cold) as log:
        table = log.read_arrow_table()
    assert table.num_rows == EXPECTED_RECORDS
    assert table.column("symbol")[PIPED].as_py() == "TTF", "a tag is a tag regardless"
    assert table.column("msg_type")[PIPED].as_py() == "D", "and so is the flat layer"
    assert table.column("fix_tags")[BRIDGE].as_py() == [], "a name resolves to nothing"
    assert _lifted(table, BRIDGE) == 0, "including the names the flat layer is made of"
    assert len(table.column("keyval")[BRIDGE].as_py()) == EXPECTED_BRIDGE_PAIRS


# -- the same, over a set ----------------------------------------------------


def test_a_folder_of_captures_reads_the_messages_too(tmp_path: Path, codec: FixCodec) -> None:
    """`TextFiles` hands its codec to every file it opens, like everything else."""
    for name in ("app.1.txt", "app.2.txt"):
        (tmp_path / name).write_bytes(SAMPLE_BYTES)
    files = TextFiles.from_folder(tmp_path, codec=codec, static_values={"bridge": "bridge-1"})
    table = files.read_arrow_table()
    assert table.num_rows == EXPECTED_RECORDS * 2
    assert table.column("protocol").to_pylist() == EXPECTED_PROTOCOLS * 2
    assert table.column("symbol")[BRIDGE].as_py() == "TTF"
    sequences = table.column("seq").to_pylist()
    assert sequences[PIPED] == 1092, "the flat layer of the first file"
    assert sequences[EXPECTED_RECORDS + PIPED] == 1092, "and of the second, read the same way"
    assert table.schema.names[-1] == "bridge", "and a static column still lands last"
    assert table.column("bridge").to_pylist() == ["bridge-1"] * (EXPECTED_RECORDS * 2)


# -- values that mean nothing ------------------------------------------------


def test_absent_values_never_reach_a_column(tmp_path: Path, codec: FixCodec) -> None:
    message = "toBridge #SYMBOL=TTF|#SIDE=<null>|#ACCOUNT=|#TEXT=N/A|#ORDERQTY=1200"
    table = _one_line(tmp_path / "absent.txt", codec, "ULBridge", message)
    assert table.column("symbol")[0].as_py() == "TTF"
    assert table.column("order_qty")[0].as_py() == 1200.0
    assert _lifted(table, 0) == 2, "and the three that said nothing filled nothing"
    assert table.column("fix_tags")[0].as_py() == []
    assert table.column("keyval")[0].as_py() == []

    keeping = FixCodec(registry=codec.registry, fix_version="4.4", null_values=frozenset())
    kept = _one_line(tmp_path / "kept.txt", keeping, "ULBridge", message)
    assert _lifted(kept, 0) == 5, "each spelling a value now, and each value a column"
    assert kept.column("side")[0].as_py() == "<null>"


# -- helpers -----------------------------------------------------------------


def _lifted(table: pyarrow.Table, row: int) -> int:
    """How many flat columns that row filled, which is how many left the map.

    `symbol` is `Event`'s own and NOT NULL, so a row that never mentioned tag
    55 carries the declared default rather than a null; every other flat column
    says "the message did not say" with one.
    """
    filled = sum(table.column(name)[row].as_py() is not None for name in FLAT_NAMES)
    return filled - (table.column("symbol")[row].as_py() == "")


def _lines(path: Path, codec: FixCodec, driver: str, messages: list[str]) -> pyarrow.Table:
    """Synthesised log lines through the whole parser, as the one batch they land as."""
    path.write_text(
        "".join(f"2026-08-14 00:05:01.147 [t] [{driver}] (INFO) {one}\n" for one in messages)
    )
    with TextFile.from_path(path, codec=codec) as log:
        return log.read_arrow_table()


def _one_line(path: Path, codec: FixCodec, driver: str, message: str) -> pyarrow.Table:
    """One synthesised log line through the whole parser, as the row it lands as."""
    return _lines(path, codec, driver, [message])
