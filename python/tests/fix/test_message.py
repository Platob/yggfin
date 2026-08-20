"""FIX line parsing: one message at a time, and whole columns at once."""

import pyarrow
import pytest

from rekep.fix import SOH, FixMessage, detect_separator, parse_arrow_array

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
