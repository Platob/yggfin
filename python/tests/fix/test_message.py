"""FIX line parsing: one message at a time, and whole columns at once."""

import re

import pyarrow
import pytest

from rekep.fix import (
    BRIDGE,
    MARKER,
    SOH,
    FixPairs,
    detect_entry_separator,
    detect_separator,
    parse_arrow_array,
    rendered_keys,
    tag_arrow_array,
)
from rekep.fix.message import _TAG_PROBE, _Names

PIPE = "8=FIX.4.2|9=2058|35=8|49=BRK|54=1|58=hello world|10=045"
SOHED = PIPE.replace("|", SOH)
CARET = "8=FIX.4.4^A9=100^A35=D^A10=001"
NOISY = "2026-08-14 00:05:01 [T] [ULBridge] (INFO) sent 8=FIX4.2|9=12|35=D|58=a=b|10=033| took=5ms"

#: Derived from the fixture, then pinned: the pipe message splits into this
#: many fields, and a broken separator rule cannot move both sides together.
EXPECTED_FIELDS = 7


# -- separators --------------------------------------------------------------


def test_the_separator_is_whatever_follows_the_beginstring() -> None:
    assert detect_separator(PIPE) == "|"
    assert detect_separator(SOHED) == SOH
    assert detect_separator(CARET) == "^A"
    assert detect_separator("8=FIX.4.2;9=1;10=000") == ";"


def test_without_a_beginstring_the_first_candidate_present_wins() -> None:
    assert detect_separator("35=D|54=1") == "|"
    assert detect_separator("35=D\x0154=1") == SOH
    assert detect_separator("no separators at all") == SOH


def test_tag_18_never_reads_as_a_beginstring() -> None:
    assert detect_separator("18=1|8=FIX.4.2\x019=1") == SOH, "the message's own 8= decides"


# -- one message -------------------------------------------------------------


@pytest.mark.parametrize("line", [PIPE, SOHED, CARET])
def test_the_two_spellings_parse_identically(line: str) -> None:
    parsed = FixPairs.from_text(line)
    assert parsed.get(8).startswith("FIX")
    assert parsed.get(9) in {"2058", "100"}


def test_the_pipe_message_has_every_field() -> None:
    parsed = FixPairs.from_text(PIPE)
    assert len(parsed) == EXPECTED_FIELDS
    assert parsed.get(58) == "hello world"
    assert parsed.get(10) == "045"


def test_log_noise_around_the_message_is_shed() -> None:
    parsed = FixPairs.from_text(NOISY)
    assert parsed.pairs[0] == ("8", "FIX4.2")
    assert parsed.get(58) == "a=b", "only the first = splits tag from value"
    assert parsed.pairs[-1] == ("10", "033"), "the checksum ends the message"


def test_bytes_parse_like_text() -> None:
    assert FixPairs.from_text(PIPE.encode()).pairs == FixPairs.from_text(PIPE).pairs


def test_generic_building_dispatches_text_to_the_message_parser() -> None:
    assert FixPairs.from_(PIPE) == FixPairs.from_text(PIPE)
    assert FixPairs.into_redirects() is FixPairs.into_redirects()


def test_a_message_renders_back_and_parses_again() -> None:
    parsed = FixPairs.from_text(PIPE)
    again = FixPairs.from_text(parsed.into_text(), SOH)
    assert again.pairs == parsed.pairs


def test_repeated_tags_keep_every_value_in_order() -> None:
    parsed = FixPairs.from_text("8=FIX.4.2|146=2|55=AAA|55=BBB|10=000")
    assert parsed.values(55) == ["AAA", "BBB"]
    assert parsed.get(55) == "AAA"


def test_a_message_iterates_as_its_pairs() -> None:
    """`__iter__` is what makes a message usable in a `for`, and it had no test.

    Coverage found it: one `return iter(self.pairs)` the whole suite never
    reached, on the one method a caller writing `for tag, value in message`
    depends on.
    """
    parsed = FixPairs.from_text("8=FIX.4.2|35=8|54=1|10=045")
    assert list(parsed) == parsed.pairs
    assert dict(parsed)["54"] == "1"
    assert len(parsed) == len(list(parsed))


# -- repeating groups --------------------------------------------------------

MDS = "8=FIX.4.2|35=W|268=2|269=0|270=1.1|271=5|269=1|270=1.2|271=7|55=EURUSD|10=000"


def test_group_entries_split_on_the_delimiter_tag() -> None:
    entries = FixPairs.from_text(MDS).group(268, members=[269, 270, 271])
    assert entries == [
        [("269", "0"), ("270", "1.1"), ("271", "5")],
        [("269", "1"), ("270", "1.2"), ("271", "7")],
    ]


def test_without_members_the_last_entry_ends_at_a_repeat_or_runs_on() -> None:
    entries = FixPairs.from_text(MDS).group(268)
    assert entries[0] == [("269", "0"), ("270", "1.1"), ("271", "5")]
    assert entries[1][:3] == [("269", "1"), ("270", "1.2"), ("271", "7")]


def test_the_count_caps_the_entries() -> None:
    lied = "8=FIX.4.2|268=1|269=0|270=1.1|269=1|270=9.9|10=000"
    assert len(FixPairs.from_text(lied).group(268)) == 1


def test_a_missing_or_zero_group_is_empty() -> None:
    parsed = FixPairs.from_text("8=FIX.4.2|268=0|10=000")
    assert parsed.group(268) == []
    assert parsed.group(999) == []


# -- rendered names and indexed groups ---------------------------------------

RENDERED = (
    "Side=1 | Price=41.25 | NoPartyIDs[0]=PartyID=BRK | NoPartyIDs[0]=PartyRole=1"
    " | NoPartyIDs[1]=PartyID=CLI | PartyID[1]=dup | took=5ms"
)

#: Derived from the fixture, then pinned: the rendered line carries this many
#: pairs, so a broken grammar cannot move both sides of an assertion together.
EXPECTED_RENDERED = 7


def test_named_mode_is_automatic_without_a_beginstring() -> None:
    parsed = FixPairs.from_text(RENDERED)
    assert len(parsed) == EXPECTED_RENDERED
    assert parsed.get("Side") == "1"
    assert parsed.get("took") == "5ms", "a rendered line is pairs, noise included"


def test_a_beginstring_keeps_the_wire_rule() -> None:
    """The same rendered spellings beside a wire message stay noise."""
    parsed = FixPairs.from_text("8=FIX.4.2|54=1|Side=2|PartyID[0]=X|10=000")
    assert parsed.pairs == [("8", "FIX.4.2"), ("54", "1"), ("10", "000")]
    assert FixPairs.from_text(RENDERED, named=False).pairs == []


def test_group_entries_store_under_canonical_indexed_keys() -> None:
    parsed = FixPairs.from_text(RENDERED)
    assert ("NoPartyIDs[0].PartyID", "BRK") in parsed.pairs
    assert ("NoPartyIDs[1].PartyID", "CLI") in parsed.pairs
    assert ("PartyID[1]", "dup") in parsed.pairs


def test_the_canonical_spelling_parses_to_the_same_pairs() -> None:
    """`G[0]=M=v` and `G[0].M=v` are two prints of one field."""
    equal_signs = FixPairs.from_text("NoPartyIDs[0]=PartyID=BRK", "|")
    dotted = FixPairs.from_text("NoPartyIDs[0].PartyID=BRK", "|")
    assert equal_signs.pairs == dotted.pairs == [("NoPartyIDs[0].PartyID", "BRK")]


def test_a_dotted_component_path_is_one_plain_key() -> None:
    parsed = FixPairs.from_text("Instrument.Symbol=AAPL|Qty=100")
    assert parsed.pairs == [("Instrument.Symbol", "AAPL"), ("Qty", "100")]


def test_a_rendered_message_round_trips_through_its_canonical_text() -> None:
    parsed = FixPairs.from_text(RENDERED)
    assert FixPairs.from_text(parsed.into_text(), SOH).pairs == parsed.pairs


def test_indexed_group_folds_the_keys_back_into_entries() -> None:
    entries = FixPairs.from_text(RENDERED).indexed_group("NoPartyIDs")
    assert entries == [
        [("PartyID", "BRK"), ("PartyRole", "1")],
        [("PartyID", "CLI")],
    ]


def test_indexed_group_is_case_insensitive_and_takes_bare_entries() -> None:
    parsed = FixPairs.from_text(RENDERED)
    assert parsed.indexed_group("nopartyids") == parsed.indexed_group("NoPartyIDs")
    assert parsed.indexed_group("PartyID") == [[("PartyID", "dup")]]
    assert parsed.indexed_group("absent") == []


def test_get_and_values_reach_through_the_rendered_spellings() -> None:
    parsed = FixPairs.from_text(RENDERED)
    assert parsed.get("side") == "1", "case-insensitive on the plain name"
    assert parsed.get("PartyID") == "BRK", "the first group entry answers first"
    assert parsed.values("PartyID") == ["BRK", "CLI", "dup"]
    assert parsed.values("Side") == ["1"], "an exact key never mixes with rendered ones"


def test_an_indexed_value_with_an_equals_reads_as_member_and_value() -> None:
    """The one ambiguity of the format, resolved the way the group form says."""
    parsed = FixPairs.from_text("PartyID[0]=a=b", "|")
    assert parsed.pairs == [("PartyID[0].a", "b")]


def test_a_dotted_digit_key_keeps_its_dot_in_both_parsers() -> None:
    """Only a digit key can capture a member without an index; the dot is the key's."""
    assert FixPairs.from_text("54.5=x", "|", named=True).pairs == [("54.5", "x")]
    vector = parse_arrow_array(pyarrow.array(["54.5=x"]), "|", named=True)
    assert vector.to_pylist() == [[("54.5", "x")]]


def test_a_vertical_tab_pads_a_token_in_both_parsers() -> None:
    """Python's ASCII `\\s` holds `\\x0b`, RE2's does not: the explicit class does."""
    for line in ("\x0bSide=1", "Side\x0b=1", "N[0]=\x0bM=v"):
        scalar = FixPairs.from_text(line, "|", named=True).pairs
        vector = parse_arrow_array(pyarrow.array([line]), "|", named=True).to_pylist()[0]
        assert scalar == vector and scalar, line


def test_non_ascii_digits_are_noise_to_both_parsers() -> None:
    """The standard's digits are ASCII, and so is RE2's `\\d`: the scalar agrees."""
    line = "٥٤=1|54=2"
    assert FixPairs.from_text(line, "|", named=False).pairs == [("54", "2")]
    vector = parse_arrow_array(pyarrow.array([line]), "|", named=False)
    assert vector.to_pylist() == [[("54", "2")]]


def test_get_returns_the_default_when_nothing_matches() -> None:
    parsed = FixPairs.from_text(RENDERED)
    assert parsed.get("absent") is None
    assert parsed.get("absent", "d") == "d"
    assert parsed.values("absent") == []


# -- a bridge's own spellings -----------------------------------------------


#: One bridge message, as the sample capture writes one: `#`-marked keys, a
#: whole group entry nested in a single token behind a second separator, and a
#: field no dictionary has.
BRIDGE_LINE = "toBridge " + "|".join(
    [
        "#ISINCODE=XX0000084733",
        "#SYMBOL=TTF",
        "#SIDE=1",
        "#NOPARTYIDS=2",
        "#NOPARTYIDS[0]=" + SOH.join(["PARTYID=BUYSIDE", "PARTYIDSOURCE=D", "PARTYROLE=1"]),
        "#NOPARTYIDS[1]=" + SOH.join(["PARTYID=XPAR", "PARTYIDSOURCE=G", "PARTYROLE=17"]),
        "#UNKNOWNVENUEFIELD=Z9",
    ]
)

#: Derived from the line, then pinned: seven tokens, two of which carry three
#: members each, so a parser that lost a member cannot move both sides.
EXPECTED_BRIDGE_PAIRS = 11


def test_the_bridge_fixture_is_the_shape_the_tests_assume() -> None:
    assert BRIDGE_LINE.count("|") + 1 == 7
    assert len(FixPairs.from_text(BRIDGE_LINE).pairs) == EXPECTED_BRIDGE_PAIRS


def test_a_marked_key_drops_its_marker() -> None:
    """`#` says where a key starts, not which field it is."""
    parsed = FixPairs.from_text("#SIDE=1|#SYMBOL=TTF")
    assert parsed.pairs == [("SIDE", "1"), ("SYMBOL", "TTF")]


def test_a_marked_key_is_log_noise_in_tag_mode() -> None:
    """A bridge's `#54=x` is a rendered key spelled with digits, not tag 54."""
    assert FixPairs.from_text("8=FIX.4.2|#54=1|55=TTF|", named=False).get(54) is None
    assert FixPairs.from_text("8=FIX.4.2|#54=1|55=TTF|", named=False).get(55) == "TTF"


def test_the_plugin_s_own_prefix_never_glues_onto_the_first_key() -> None:
    """The same rule `8=FIX` gets, for the same reason."""
    parsed = FixPairs.from_text(BRIDGE_LINE)
    assert parsed.pairs[0] == ("ISINCODE", "XX0000084733")


def test_one_marked_key_in_prose_is_not_a_message_start() -> None:
    """Two `#NAME=` or it is a sentence, which is what `BRIDGE` says."""
    assert FixPairs.from_text("Account=A|note=see #ref for details").get("Account") == "A"


def test_a_bridge_that_writes_nothing_between_its_tokens_is_separated_by_the_marker() -> None:
    """`#A=1#B=2` has no delimiter, so the `#` of the next key ends the value.

    The character there is the tail of the value in front of it, and reading it
    as the separator gave `A` an empty value and glued `B` to whatever came
    next -- silently, because the result still parsed.
    """
    assert detect_separator("#A=1#B=2") == MARKER
    assert FixPairs.from_text("#A=1#B=2").pairs == [("A", "1"), ("B", "2")]
    assert FixPairs.from_text("toBridge #A=1#B=2#C=3").pairs == [
        ("A", "1"),
        ("B", "2"),
        ("C", "3"),
    ]


@pytest.mark.parametrize("between", ["|", ";", "^A", SOH])
def test_a_candidate_between_the_tokens_is_still_the_separator(between: str) -> None:
    """Only a candidate, though: anything else is a value, not a delimiter."""
    assert detect_separator(f"#A=1{between}#B=2") == between
    assert FixPairs.from_text(f"#A=1{between}#B=2").pairs == [("A", "1"), ("B", "2")]


def test_both_separators_on_one_line_and_neither_is_the_other_s() -> None:
    """A marker-separated line can still nest an entry behind a second one."""
    line = "toBridge #NOPARTYIDS[0]=PARTYID=x" + SOH + "PARTYROLE=1#SIDE=1"
    assert detect_separator(line) == MARKER
    assert detect_entry_separator(line, MARKER) == SOH
    assert FixPairs.from_text(line).pairs == [
        ("NOPARTYIDS[0].PARTYID", "x"),
        ("NOPARTYIDS[0].PARTYROLE", "1"),
        ("SIDE", "1"),
    ]


def test_the_outer_separator_is_read_off_the_second_marked_key() -> None:
    """A nested SOH would otherwise win the candidate scan and eat the line."""
    assert detect_separator(BRIDGE_LINE) == "|"
    assert detect_entry_separator(BRIDGE_LINE, "|") == SOH


def test_a_nested_group_entry_becomes_its_members() -> None:
    """Under the canonical keys the one-member-per-token spelling produces."""
    found = dict(FixPairs.from_text(BRIDGE_LINE).pairs)
    assert found["NOPARTYIDS[0].PARTYID"] == "BUYSIDE"
    assert found["NOPARTYIDS[0].PARTYIDSOURCE"] == "D"
    assert found["NOPARTYIDS[0].PARTYROLE"] == "1"
    assert found["NOPARTYIDS[1].PARTYROLE"] == "17"


def test_a_nested_entry_and_a_printed_one_parse_to_the_same_pairs() -> None:
    """No new key spelling: the two spellings of a group are one shape."""
    nested = "#NOPARTYIDS[0]=" + SOH.join(["PARTYID=BUYSIDE", "PARTYROLE=1"]) + "|#SIDE=1"
    printed = "#NOPARTYIDS[0]=PARTYID=BUYSIDE|#NOPARTYIDS[0].PARTYROLE=1|#SIDE=1"
    assert FixPairs.from_text(nested).pairs == FixPairs.from_text(printed).pairs


def test_a_group_entry_reads_back_as_entries() -> None:
    entries = FixPairs.from_text(BRIDGE_LINE).indexed_group("NOPARTYIDS")
    assert [dict(entry)["PARTYID"] for entry in entries] == ["BUYSIDE", "XPAR"]


def test_a_second_separator_is_only_read_inside_an_indexed_token() -> None:
    """`Text=a;b` is a value with a semicolon in it, not two fields."""
    assert detect_entry_separator("Text=a;b|Side=1", "|") is None
    assert FixPairs.from_text("Text=a;b|Side=1").pairs == [("Text", "a;b"), ("Side", "1")]


def test_a_stated_entry_separator_is_used_as_given() -> None:
    line = "#NOPARTYIDS[0]=PARTYID=x;PARTYROLE=1|#SIDE=1"
    assert dict(FixPairs.from_text(line, "|", entry_separator=";").pairs) == {
        "NOPARTYIDS[0].PARTYID": "x",
        "NOPARTYIDS[0].PARTYROLE": "1",
        "SIDE": "1",
    }


def test_a_malformed_member_is_kept_rather_than_dropped() -> None:
    """A parser that loses the malformed half of a line loses the record."""
    line = "#NOPARTYIDS[0]=PARTYID=x" + SOH + "garbage|#SIDE=1"
    assert FixPairs.from_text(line).pairs == [
        ("NOPARTYIDS[0].PARTYID", "x"),
        ("NOPARTYIDS[0]", "garbage"),
        ("SIDE", "1"),
    ]


@pytest.mark.parametrize(
    "line",
    [
        BRIDGE_LINE,
        "#A=1|#B=2",
        "#A=1#B=2",
        "toBridge #A=1#B=2#C=3",
        "#NOPARTYIDS[0]=PARTYID=x" + SOH + "PARTYROLE=1#SIDE=1",
        "#NOPARTYIDS[0]=PARTYID=x" + SOH + "PARTYROLE=1|#SIDE=1",
        "#NOPARTYIDS[0]=PARTYID=x" + SOH + "garbage|#SIDE=1",
        "Side=1 | Price=41.25",
        "8=FIX.4.2|35=D|55=TTF|10=203|",
        "prose with nothing in it",
    ],
    ids=lambda value: value[:26],
)
def test_the_two_parsers_agree_on_every_bridge_spelling(line: str) -> None:
    """The contract this module is built on, asserted on the shapes it added."""
    column = pyarrow.array([line])
    assert parse_arrow_array(column).to_pylist()[0] == FixPairs.from_text(line).pairs


def test_a_column_of_bridge_lines_agrees_row_for_row() -> None:
    """One style per call, so the sampled reading has to hold for every row."""
    lines = [BRIDGE_LINE, "#A=1|#B=2", "#NOPARTYIDS[0]=PARTYID=x" + SOH + "PARTYROLE=1|#SIDE=1"]
    column = pyarrow.array(lines)
    expected = [FixPairs.from_text(line, "|").pairs for line in lines]
    assert parse_arrow_array(column).to_pylist() == expected


def test_a_bridge_column_with_no_nesting_pays_for_no_expansion() -> None:
    """The skip is the point: a column without entries takes the old path."""
    lines = ["#A=1|#B=2", "#C=3|#D=4"]
    column = pyarrow.array(lines)
    assert parse_arrow_array(column).to_pylist() == [
        [("A", "1"), ("B", "2")],
        [("C", "3"), ("D", "4")],
    ]


def test_a_null_line_among_bridge_lines_stays_null() -> None:
    column = pyarrow.array([BRIDGE_LINE, None, "#A=1|#B=2"])
    parsed = parse_arrow_array(column).to_pylist()
    assert parsed[1] is None
    assert len(parsed[0]) == EXPECTED_BRIDGE_PAIRS


# -- whole columns -----------------------------------------------------------


def test_a_column_parses_to_one_map_per_row() -> None:
    maps = parse_arrow_array(pyarrow.array([PIPE, NOISY]))
    assert pyarrow.types.is_map(maps.type)
    assert maps.to_pylist()[0] == FixPairs.from_text(PIPE).pairs


def test_the_vectorised_and_scalar_parsers_agree() -> None:
    lines = [PIPE, NOISY, "35=D|54=1", "plain text"]
    maps = parse_arrow_array(pyarrow.array(lines), "|").to_pylist()
    for line, row in zip(lines, maps, strict=True):
        assert row == FixPairs.from_text(line, "|").pairs


def test_the_checksum_ends_each_row_in_the_vectorised_parser_too() -> None:
    """Pair-shaped log noise after `10=` must not land, exactly as in `from_text`."""
    lines = [
        "8=FIX.4.2|9=1|10=045|11=5|took=3ms",
        "8=FIX.4.2|10=001",
        "8=FIX.4.2|9=2|58=x",
    ]
    maps = parse_arrow_array(pyarrow.array(lines)).to_pylist()
    assert maps[0] == [("8", "FIX.4.2"), ("9", "1"), ("10", "045")]
    assert maps[1][-1] == ("10", "001")
    assert maps[2] == [("8", "FIX.4.2"), ("9", "2"), ("58", "x")]
    for line, row in zip(lines, maps, strict=True):
        assert row == FixPairs.from_text(line).pairs


@pytest.mark.parametrize("checksum", ["CheckSum", "CHECKSUM", "Trailer.CheckSum", "Trailer.10"])
def test_a_named_checksum_ends_scalar_and_vector_messages(checksum: str) -> None:
    line = f"#BeginString=FIXT.1.1|#{checksum}=000|#ApplVerID=9|#Symbol=X"
    expected = [("BeginString", "FIXT.1.1"), (checksum, "000")]
    assert FixPairs.from_text(line, "|", named=True).pairs == expected
    assert parse_arrow_array(pyarrow.array([line]), "|", named=True).to_pylist() == [expected]


@pytest.mark.parametrize("checksum", ["CheckSum", "10"])
def test_an_indexed_member_named_checksum_does_not_end_the_outer_message(checksum: str) -> None:
    line = f"#BeginString=FIXT.1.1|#NoFoo[0].{checksum}=000|#ApplVerID=9|#Symbol=X"
    scalar = FixPairs.from_text(line, "|", named=True).pairs
    vector = parse_arrow_array(pyarrow.array([line]), "|", named=True).to_pylist()[0]
    assert scalar == vector
    assert scalar[-2:] == [("ApplVerID", "9"), ("Symbol", "X")]


def test_null_stays_null_and_noise_becomes_an_empty_map() -> None:
    maps = parse_arrow_array(pyarrow.array([PIPE, None, "no fix here"]))
    listed = maps.to_pylist()
    assert listed[1] is None
    assert listed[2] == []


def test_a_chunked_column_parses_chunk_by_chunk() -> None:
    chunked = pyarrow.chunked_array([[PIPE], [SOHED.replace(SOH, "|")]])
    maps = parse_arrow_array(chunked)
    assert [len(row) for row in maps.to_pylist()] == [EXPECTED_FIELDS, EXPECTED_FIELDS]


def test_an_empty_column_parses_to_an_empty_map_column() -> None:
    assert parse_arrow_array(pyarrow.array([], pyarrow.string())).to_pylist() == []


def test_a_named_column_parses_like_the_scalar_parser() -> None:
    lines = [RENDERED, None, "Instrument.Symbol=AAPL|Qty=100", "", "no pairs at all"]
    maps = parse_arrow_array(pyarrow.array(lines), "|").to_pylist()
    for line, row in zip(lines, maps, strict=True):
        expected = None if line is None else FixPairs.from_text(line, "|").pairs
        assert row == expected


def test_named_mode_is_sampled_from_the_column() -> None:
    """No BeginString in the first line means the column is rendered pairs."""
    sampled = parse_arrow_array(pyarrow.array([RENDERED])).to_pylist()
    assert sampled[0][0] == ("Side", "1")
    wire = parse_arrow_array(pyarrow.array([PIPE, RENDERED])).to_pylist()
    assert wire[1] == [], "a wire column keeps the digits-only rule for every row"


def test_named_can_be_forced_either_way() -> None:
    assert parse_arrow_array(pyarrow.array([RENDERED]), "|", named=False).to_pylist() == [[]]
    forced = parse_arrow_array(pyarrow.array(["35=D|54=1"]), "|", named=True).to_pylist()
    assert forced == [[("35", "D"), ("54", "1")]], "digits parse the same in named mode"


def test_the_style_sample_skips_null_and_empty_leading_rows() -> None:
    column = pyarrow.array([None, "", RENDERED])
    assert parse_arrow_array(column, "|").to_pylist()[2][0] == ("Side", "1")


def test_a_binary_column_with_invalid_utf8_still_parses() -> None:
    """`from_text` decodes with `replace`; the column sampler must not crash first."""
    column = pyarrow.array([b"\xff\xfe|54=1", b"8=FIX.4.2|9=1|10=000"], pyarrow.binary())
    maps = parse_arrow_array(column, "|", named=False).to_pylist()
    assert maps[0] == [("54", "1")]
    assert maps[1][0] == ("8", "FIX.4.2")


def test_a_newline_inside_a_message_survives_the_begin_cut() -> None:
    line = "junk 8=FIX.4.2|58=a\nb|10=000"
    maps = parse_arrow_array(pyarrow.array([line])).to_pylist()
    assert maps[0] == FixPairs.from_text(line).pairs
    assert ("58", "a\nb") in maps[0]


def test_a_zero_chunk_column_parses_and_casts() -> None:
    empty = pyarrow.chunked_array([], pyarrow.string())
    parsed = parse_arrow_array(empty)
    assert parsed.type == pyarrow.map_(pyarrow.string(), pyarrow.string())
    assert tag_arrow_array(parsed).type == pyarrow.map_(pyarrow.int32(), pyarrow.string())


# -- tag numbers -------------------------------------------------------------

NAMES = {"Side": 54, "Price": 44, "PartyID": 448, "PartyRole": 452, "Symbol": 55, "Qty": 38}


def test_tag_arrow_array_casts_numeric_keys_in_place() -> None:
    maps = parse_arrow_array(pyarrow.array([PIPE, None, "no fix here"]))
    tagged = tag_arrow_array(maps)
    assert tagged.type == pyarrow.map_(pyarrow.int32(), pyarrow.string())
    listed = tagged.to_pylist()
    assert listed[0][:2] == [(8, "FIX.4.2"), (9, "2058")]
    assert listed[1] is None and listed[2] == []


def test_tag_arrow_array_takes_another_key_width() -> None:
    maps = parse_arrow_array(pyarrow.array([PIPE]))
    assert tag_arrow_array(maps, pyarrow.int64()).type == pyarrow.map_(
        pyarrow.int64(), pyarrow.string()
    )


def test_rendered_keys_resolve_through_names_by_their_member() -> None:
    maps = parse_arrow_array(pyarrow.array([RENDERED]), "|")
    tagged = tag_arrow_array(maps, names=NAMES, drop_unknown=True).to_pylist()
    assert tagged[0] == [
        (54, "1"),
        (44, "41.25"),
        (448, "BRK"),
        (452, "1"),
        (448, "CLI"),
        (448, "dup"),
    ], "the index and the group are where a field sits, not what it is"


def test_an_unresolvable_key_is_refused_by_name() -> None:
    maps = parse_arrow_array(pyarrow.array([RENDERED]), "|")
    with pytest.raises(KeyError, match="took"):
        tag_arrow_array(maps, names=NAMES)


def test_drop_unknown_rebuilds_the_layout_around_the_dropped_entries() -> None:
    maps = parse_arrow_array(pyarrow.array(["a=1|Side=2", None, "b=3"]), "|")
    tagged = tag_arrow_array(maps, names=NAMES, drop_unknown=True).to_pylist()
    assert tagged == [[(54, "2")], None, []]


def test_tag_arrow_array_honours_a_sliced_input() -> None:
    maps = parse_arrow_array(pyarrow.array([RENDERED, None, "Qty=9"]), "|")
    sliced = tag_arrow_array(maps.slice(1, 2), names=NAMES, drop_unknown=True).to_pylist()
    assert sliced == [None, [(38, "9")]]


def test_tag_arrow_array_is_chunk_transparent_and_empty_safe() -> None:
    maps = parse_arrow_array(pyarrow.chunked_array([[PIPE], []]))
    assert tag_arrow_array(maps).to_pylist()[0][0] == (8, "FIX.4.2")
    empty = parse_arrow_array(pyarrow.array([], pyarrow.string()))
    assert tag_arrow_array(empty).to_pylist() == []


def test_a_number_the_key_type_cannot_hold_is_unknown_not_a_crash() -> None:
    """An epoch-millis key is not a FIX tag; it must take the refuse/drop path."""
    maps = pyarrow.MapArray.from_arrays(
        pyarrow.array([0, 2], pyarrow.int32()),
        pyarrow.array(["54", "1786665901147250000"]),
        pyarrow.array(["1", "x"]),
    )
    with pytest.raises(KeyError, match="1786665901147250000"):
        tag_arrow_array(maps)
    assert tag_arrow_array(maps, drop_unknown=True).to_pylist() == [[(54, "1")]]
    assert tag_arrow_array(maps, pyarrow.int64()).to_pylist() == [
        [(54, "1"), (1786665901147250000, "x")]
    ]


def test_a_name_past_the_cast_probe_still_resolves() -> None:
    """The branch where the probe reads as tags and the whole column does not.

    `_tag_numbers` casts the head of a key column before the rest of it, so a
    column whose first keys are all digits takes the cast -- and has to fall
    through to the dictionary when a name turns up beyond the probe.
    """
    keys = ["54"] * (_TAG_PROBE * 2) + ["Price"]
    maps = pyarrow.MapArray.from_arrays(
        pyarrow.array([0, len(keys)], pyarrow.int32()),
        pyarrow.array(keys),
        pyarrow.array(["1"] * (_TAG_PROBE * 2) + ["9.5"]),
    )
    tagged = tag_arrow_array(maps, names={"Price": 44}).to_pylist()
    assert tagged[0][-1] == (44, "9.5")
    assert {tag for tag, _ in tagged[0]} == {54, 44}


def test_one_folded_dictionary_serves_both_readings_of_it() -> None:
    """`_Names` is walked once per mapping, and answers keys and tags alike."""
    names = {" Side ": 54, "Price": "44"}
    held = _Names.of(names)
    assert _Names.of(names) is held, "the same mapping folds once"
    assert held.keys == {"side": "54", "price": "44"}
    assert held.tags == {"side": 54, "price": 44}
    assert _Names.of({}).keys == {} and _Names.of(None).keys == {}


def test_mixed_digit_and_named_keys_resolve_together() -> None:
    """The dictionary path must still read a digit key, and a digit member."""
    maps = pyarrow.MapArray.from_arrays(
        pyarrow.array([0, 3], pyarrow.int32()),
        pyarrow.array(["54", "Price", "NoPartyIDs[0].269"]),
        pyarrow.array(["1", "9.5", "0"]),
    )
    tagged = tag_arrow_array(maps, names={"Price": 44}).to_pylist()
    assert tagged == [[(54, "1"), (44, "9.5"), (269, "0")]]


# -- the fast path past the pattern ------------------------------------------

#: Tokens where a hand-rolled split could disagree with `_TOKEN`: the
#: whitespace the pattern calls whitespace and the whitespace it does not, the
#: digits `re.ASCII` calls digits and the ones it does not, keys with an index
#: or a member, and values that carry the separator or a newline.
TOKENS = [
    "54=1",
    " 54 = 1 ",
    "\t54\t=\tv\t",
    "\x0b54=1",
    " 54=1",
    "5 4=1",
    "٣=1",
    "5³=1",
    "0054=1",
    "54=",
    "54=  ",
    "=1",
    "=",
    "",
    "54",
    "54.5=x",
    "54[0]=x",
    "54[0].a=b",
    "Side=1",
    "NoPartyIDs[0].PartyID=BRK",
    "54=a=b",
    "54=x|y",
    "54=\n1\n",
]


def by_pattern(token: str, named: bool) -> tuple[str, str] | None:
    """`_parse_token` with nothing but the regex -- what the fast path must match."""
    from rekep.fix.message import _MEMBER, _TOKEN

    match = _TOKEN.match(token)
    if match is None:
        return None
    key, index, member, rest = match.group("key", "index", "member", "rest")
    if not named and (index is not None or member is not None or not key.isdigit()):
        return None
    if index is None:
        return (f"{key}.{member}" if member else key), rest.strip()
    if member is None:
        inner = _MEMBER.match(rest)
        if inner is not None:
            member, rest = inner.group("member", "value")
    return (f"{key}[{index}].{member}" if member else f"{key}[{index}]"), rest.strip()


@pytest.mark.parametrize("token", TOKENS, ids=lambda token: repr(token))
@pytest.mark.parametrize("named", [False, True], ids=["tags", "names"])
def test_the_token_fast_path_answers_what_the_pattern_answers(token: str, named: bool) -> None:
    """A `tag=value` token skips the regex, and must not skip its rules with it."""
    from rekep.fix.message import _parse_token

    assert _parse_token(token, named) == by_pattern(token, named)


def test_a_key_behind_unicode_whitespace_is_not_a_tag() -> None:
    """`str.strip()` would eat a non-breaking space and hand back a tag the
    pattern rejects, which is the one way this fast path could be wrong."""
    from rekep.fix.message import _parse_token

    assert _parse_token("\u00a054=1", False) is None, "not whitespace to the pattern"
    assert _parse_token("\x0b54=1", False) == ("54", "1"), "and a vertical tab is"


# -- a bracket that names a member rather than an entry ----------------------
#
# `NoPartyIDs[0]` selects an entry by position and `Instrument[Exchange]`
# selects a member by name. Only the first was ever read, so a line whose keys
# were all of the second kind did not tokenise -- and a line whose keys do not
# tokenise is not a bridge message, so every field on it went too.


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        ("#INSTRUMENT[EXCHANGE]=XTST", [("INSTRUMENT.EXCHANGE", "XTST")]),
        ("#INSTRUMENT[EXCHANGE].MIC=XTST", [("INSTRUMENT.EXCHANGE.MIC", "XTST")]),
        ("#NOPARTYIDS[0].PARTYID=PARTY-TEST-A", [("NOPARTYIDS[0].PARTYID", "PARTY-TEST-A")]),
        ("#INSTRUMENT.EXCHANGE=XTST", [("INSTRUMENT.EXCHANGE", "XTST")]),
    ],
)
def test_a_named_bracket_reads_as_the_dotted_path_it_spells(
    token: str, expected: list[tuple[str, str]]
) -> None:
    """One canonical key, whichever of the two ways a bridge wrote it."""
    line = f"toBridge {token}|#SIDE=1"
    assert FixPairs.from_text(line).pairs == [*expected, ("SIDE", "1")]


def test_the_two_parsers_agree_about_a_named_bracket() -> None:
    """Scalar and vectorised are contracted to agree, on this like everything else."""
    lines = [
        "toBridge #INSTRUMENT[EXCHANGE]=XTST|#INSTRUMENT[SYMBOL]=SYM-TEST|#SIDE=1",
        "toBridge #NOPARTYIDS[0].PARTYID=PARTY-TEST-A|#NOPARTYIDS[1].PARTYID=PARTY-TEST-B",
        "toBridge #INSTRUMENT[EXCHANGE]=XTST|#NOPARTYIDS[0].PARTYID=PARTY-TEST-A|#SIDE=1",
    ]
    parsed = parse_arrow_array(pyarrow.array(lines))
    for line, row in zip(lines, parsed.to_pylist(), strict=True):
        assert FixPairs.from_text(line).pairs == [tuple(pair) for pair in row], line


def test_a_line_of_named_brackets_is_a_bridge_message() -> None:
    """The classification rule and the token rule are one rule, or a line is lost."""
    assert re.search(BRIDGE, "toBridge #INSTRUMENT[EXCHANGE]=XTST|#INSTRUMENT[SYMBOL]=SYM")
    assert re.search(BRIDGE, "toBridge #NOPARTYIDS[0].PARTYID=A|#NOPARTYIDS[0].PARTYROLE=1")
    assert not re.search(BRIDGE, "a sentence mentioning #hashtag and nothing else")


def test_a_wire_message_still_refuses_a_bracketed_key() -> None:
    """Tag mode is digits only; a bracket is a rendered spelling, not a tag."""
    assert FixPairs.from_text("8=FIX.4.2\x01INSTRUMENT[EXCHANGE]=XTST\x0154=1\x01").pairs == [
        ("8", "FIX.4.2"),
        ("54", "1"),
    ]


# -- what a capture spells, as against what a parse keeps --------------------


def test_rendered_keys_keeps_the_marker_a_parse_sheds() -> None:
    """`#Side` and `Side` are two namespaces, and a parse deliberately loses which."""
    line = "toBridge #ISINCODE=FAKE-ISIN-0001|#SIDE=1|SIDE=1|#NOPARTYIDS[0].PARTYID=PARTY-TEST-A"
    marker, keys = rendered_keys(pyarrow.array([line]), "|", named=True)
    assert list(zip(marker.to_pylist(), keys.to_pylist(), strict=True)) == [
        ("#", "ISINCODE"),
        ("#", "SIDE"),
        ("", "SIDE"),
        ("#", "NOPARTYIDS.PARTYID"),
    ], "the index goes, because one name written twice is one name"


def test_rendered_keys_starts_where_the_parser_starts() -> None:
    """A line's own prose is not a key, and the cut is the parser's own."""
    _, keys = rendered_keys(
        pyarrow.array(["Sending order to venue#CLORDID=ORD-TEST-01|#SIDE=1"]), "|", named=True
    )
    assert keys.to_pylist() == ["CLORDID", "SIDE"]


def test_rendered_keys_reads_a_wire_message_as_its_tags() -> None:
    marker, keys = rendered_keys(
        pyarrow.array(["8=FIX.4.4\x0135=8\x0154=1\x0110=000\x01"]), SOH, named=False
    )
    assert keys.to_pylist() == ["8", "35", "54", "10"]
    assert set(marker.to_pylist()) == {""}
