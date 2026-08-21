"""FIX line parsing: one message at a time, and whole columns at once."""

import pyarrow
import pytest

from rekep.fix import SOH, FixMessage, detect_separator, parse_arrow_array, tag_arrow_array

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
    parsed = FixMessage.from_text(line)
    assert parsed.begin_string.startswith("FIX")
    assert parsed.get(9) in {"2058", "100"}


def test_the_pipe_message_has_every_field() -> None:
    parsed = FixMessage.from_text(PIPE)
    assert len(parsed) == EXPECTED_FIELDS
    assert parsed.get(58) == "hello world"
    assert parsed.get(10) == "045"


def test_log_noise_around_the_message_is_shed() -> None:
    parsed = FixMessage.from_text(NOISY)
    assert parsed.pairs[0] == ("8", "FIX4.2")
    assert parsed.get(58) == "a=b", "only the first = splits tag from value"
    assert parsed.pairs[-1] == ("10", "033"), "the checksum ends the message"


def test_bytes_parse_like_text() -> None:
    assert FixMessage.from_text(PIPE.encode()).pairs == FixMessage.from_text(PIPE).pairs


def test_a_message_renders_back_and_parses_again() -> None:
    parsed = FixMessage.from_text(PIPE)
    again = FixMessage.from_text(parsed.into_text(), SOH)
    assert again.pairs == parsed.pairs


def test_repeated_tags_keep_every_value_in_order() -> None:
    parsed = FixMessage.from_text("8=FIX.4.2|146=2|55=AAA|55=BBB|10=000")
    assert parsed.values(55) == ["AAA", "BBB"]
    assert parsed.get(55) == "AAA"


# -- repeating groups --------------------------------------------------------

MDS = "8=FIX.4.2|35=W|268=2|269=0|270=1.1|271=5|269=1|270=1.2|271=7|55=EURUSD|10=000"


def test_group_entries_split_on_the_delimiter_tag() -> None:
    entries = FixMessage.from_text(MDS).group(268, members=[269, 270, 271])
    assert entries == [
        [("269", "0"), ("270", "1.1"), ("271", "5")],
        [("269", "1"), ("270", "1.2"), ("271", "7")],
    ]


def test_without_members_the_last_entry_ends_at_a_repeat_or_runs_on() -> None:
    entries = FixMessage.from_text(MDS).group(268)
    assert entries[0] == [("269", "0"), ("270", "1.1"), ("271", "5")]
    assert entries[1][:3] == [("269", "1"), ("270", "1.2"), ("271", "7")]


def test_the_count_caps_the_entries() -> None:
    lied = "8=FIX.4.2|268=1|269=0|270=1.1|269=1|270=9.9|10=000"
    assert len(FixMessage.from_text(lied).group(268)) == 1


def test_a_missing_or_zero_group_is_empty() -> None:
    parsed = FixMessage.from_text("8=FIX.4.2|268=0|10=000")
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
    parsed = FixMessage.from_text(RENDERED)
    assert len(parsed) == EXPECTED_RENDERED
    assert parsed.get("Side") == "1"
    assert parsed.get("took") == "5ms", "a rendered line is pairs, noise included"


def test_a_beginstring_keeps_the_wire_rule() -> None:
    """The same rendered spellings beside a wire message stay noise."""
    parsed = FixMessage.from_text("8=FIX.4.2|54=1|Side=2|PartyID[0]=X|10=000")
    assert parsed.pairs == [("8", "FIX.4.2"), ("54", "1"), ("10", "000")]
    assert FixMessage.from_text(RENDERED, named=False).pairs == []


def test_group_entries_store_under_canonical_indexed_keys() -> None:
    parsed = FixMessage.from_text(RENDERED)
    assert ("NoPartyIDs[0].PartyID", "BRK") in parsed.pairs
    assert ("NoPartyIDs[1].PartyID", "CLI") in parsed.pairs
    assert ("PartyID[1]", "dup") in parsed.pairs


def test_the_canonical_spelling_parses_to_the_same_pairs() -> None:
    """`G[0]=M=v` and `G[0].M=v` are two prints of one field."""
    equal_signs = FixMessage.from_text("NoPartyIDs[0]=PartyID=BRK", "|")
    dotted = FixMessage.from_text("NoPartyIDs[0].PartyID=BRK", "|")
    assert equal_signs.pairs == dotted.pairs == [("NoPartyIDs[0].PartyID", "BRK")]


def test_a_dotted_component_path_is_one_plain_key() -> None:
    parsed = FixMessage.from_text("Instrument.Symbol=AAPL|Qty=100")
    assert parsed.pairs == [("Instrument.Symbol", "AAPL"), ("Qty", "100")]


def test_a_rendered_message_round_trips_through_its_canonical_text() -> None:
    parsed = FixMessage.from_text(RENDERED)
    assert FixMessage.from_text(parsed.into_text(), SOH).pairs == parsed.pairs


def test_indexed_group_folds_the_keys_back_into_entries() -> None:
    entries = FixMessage.from_text(RENDERED).indexed_group("NoPartyIDs")
    assert entries == [
        [("PartyID", "BRK"), ("PartyRole", "1")],
        [("PartyID", "CLI")],
    ]


def test_indexed_group_is_case_insensitive_and_takes_bare_entries() -> None:
    parsed = FixMessage.from_text(RENDERED)
    assert parsed.indexed_group("nopartyids") == parsed.indexed_group("NoPartyIDs")
    assert parsed.indexed_group("PartyID") == [[("PartyID", "dup")]]
    assert parsed.indexed_group("absent") == []


def test_get_and_values_reach_through_the_rendered_spellings() -> None:
    parsed = FixMessage.from_text(RENDERED)
    assert parsed.get("side") == "1", "case-insensitive on the plain name"
    assert parsed.get("PartyID") == "BRK", "the first group entry answers first"
    assert parsed.values("PartyID") == ["BRK", "CLI", "dup"]
    assert parsed.values("Side") == ["1"], "an exact key never mixes with rendered ones"


def test_an_indexed_value_with_an_equals_reads_as_member_and_value() -> None:
    """The one ambiguity of the format, resolved the way the group form says."""
    parsed = FixMessage.from_text("PartyID[0]=a=b", "|")
    assert parsed.pairs == [("PartyID[0].a", "b")]


def test_a_dotted_digit_key_keeps_its_dot_in_both_parsers() -> None:
    """Only a digit key can capture a member without an index; the dot is the key's."""
    assert FixMessage.from_text("54.5=x", "|", named=True).pairs == [("54.5", "x")]
    vector = parse_arrow_array(pyarrow.array(["54.5=x"]), "|", named=True)
    assert vector.to_pylist() == [[("54.5", "x")]]


def test_a_vertical_tab_pads_a_token_in_both_parsers() -> None:
    """Python's ASCII `\\s` holds `\\x0b`, RE2's does not: the explicit class does."""
    for line in ("\x0bSide=1", "Side\x0b=1", "N[0]=\x0bM=v"):
        scalar = FixMessage.from_text(line, "|", named=True).pairs
        vector = parse_arrow_array(pyarrow.array([line]), "|", named=True).to_pylist()[0]
        assert scalar == vector and scalar, line


def test_non_ascii_digits_are_noise_to_both_parsers() -> None:
    """The standard's digits are ASCII, and so is RE2's `\\d`: the scalar agrees."""
    line = "٥٤=1|54=2"
    assert FixMessage.from_text(line, "|", named=False).pairs == [("54", "2")]
    vector = parse_arrow_array(pyarrow.array([line]), "|", named=False)
    assert vector.to_pylist() == [[("54", "2")]]


def test_get_returns_the_default_when_nothing_matches() -> None:
    parsed = FixMessage.from_text(RENDERED)
    assert parsed.get("absent") is None
    assert parsed.get("absent", "d") == "d"
    assert parsed.values("absent") == []


# -- whole columns -----------------------------------------------------------


def test_a_column_parses_to_one_map_per_row() -> None:
    maps = parse_arrow_array(pyarrow.array([PIPE, NOISY]))
    assert pyarrow.types.is_map(maps.type)
    assert maps.to_pylist()[0] == FixMessage.from_text(PIPE).pairs


def test_the_vectorised_and_scalar_parsers_agree() -> None:
    lines = [PIPE, NOISY, "35=D|54=1", "plain text"]
    maps = parse_arrow_array(pyarrow.array(lines), "|").to_pylist()
    for line, row in zip(lines, maps, strict=True):
        assert row == FixMessage.from_text(line, "|").pairs


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
        assert row == FixMessage.from_text(line).pairs


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
        expected = None if line is None else FixMessage.from_text(line, "|").pairs
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
    assert maps[0] == FixMessage.from_text(line).pairs
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


def test_mixed_digit_and_named_keys_resolve_together() -> None:
    """The dictionary path must still read a digit key, and a digit member."""
    maps = pyarrow.MapArray.from_arrays(
        pyarrow.array([0, 3], pyarrow.int32()),
        pyarrow.array(["54", "Price", "NoPartyIDs[0].269"]),
        pyarrow.array(["1", "9.5", "0"]),
    )
    tagged = tag_arrow_array(maps, names={"Price": 44}).to_pylist()
    assert tagged == [[(54, "1"), (44, "9.5"), (269, "0")]]
