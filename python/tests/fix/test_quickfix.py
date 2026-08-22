"""The QuickFIX spec read as a source: the file name, the fields, the session layer.

The spec is the same standard the dictionary describes, written for programs
instead of people. What it has and the dictionary does not is the *symbol* of
each enumerated value -- `1` is `BUY` -- and which fields the standard header
and trailer hold, with which of them required. What it does not have is prose.
So these check the halves it answers for, and nothing about descriptions.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rekep.fix.quickfix import SPEC_VERSIONS, parse_session, parse_spec, spec_name

SPEC = (Path(__file__).parent / "fixtures" / "FIX44.xml").read_text()

#: Derived from the fixture, then pinned: four fields, three of them enumerated.
EXPECTED_FIELDS = 4
EXPECTED_VALUES = 6


def test_the_fixture_is_the_shape_the_tests_assume() -> None:
    parsed = parse_spec(SPEC)
    assert len(parsed) == EXPECTED_FIELDS
    assert sum(len(field.values) for field in parsed.values()) == EXPECTED_VALUES


# -- which file is which version ----------------------------------------------


@pytest.mark.parametrize(
    "version,name",
    [
        ("4.0", "FIX40.xml"),
        ("4.4", "FIX44.xml"),
        ("5.0", "FIX50.xml"),
        ("5.0.SP2", "FIX50SP2.xml"),
        ("FIXT1.1", "FIXT11.xml"),
        ("fixt1.1", "FIXT11.xml"),
        ("FIX.4.2", "FIX42.xml"),
    ],
)
def test_a_version_names_its_spec_file(version: str, name: str) -> None:
    """The punctuation is decoration on both sides; `FIXT` is a protocol."""
    assert spec_name(version) == name


def test_every_version_this_package_knows_has_a_spelling() -> None:
    """A version in the list with no file name would 404 at the first fetch."""
    assert len(SPEC_VERSIONS) == 9
    assert all(spec_name(version).endswith(".xml") for version in SPEC_VERSIONS)
    assert len({spec_name(version) for version in SPEC_VERSIONS}) == len(SPEC_VERSIONS)


# -- what a field is ----------------------------------------------------------


def test_a_field_comes_back_with_its_tag_name_and_datatype() -> None:
    parsed = parse_spec(SPEC)
    assert parsed[54].name == "Side"
    assert parsed[54].datatype == "CHAR"
    assert parsed[103].name == "OrdRejReason"
    assert parsed[103].datatype == "INT"


def test_an_enumerated_value_comes_back_as_its_symbol() -> None:
    """`description=` in the spec is the value's *name*, which is the point of it."""
    parsed = parse_spec(SPEC)
    assert parsed[54].values == {"1": "BUY", "2": "SELL", "7": "UNDISCLOSED"}
    assert parsed[43].values == {"Y": "POSSIBLE_DUPLICATE", "N": "ORIGINAL_TRANSMISSION"}


def test_a_field_with_no_enumeration_has_no_values() -> None:
    assert parse_spec(SPEC)[103].values == {}


def test_a_document_that_is_not_a_spec_reads_as_nothing() -> None:
    """Empty, never an exception: this is the enriching source, and a scrape
    that could not read it still has a whole dictionary."""
    assert parse_spec("") == {}
    assert parse_spec("<fix><fields>") == {}
    assert parse_spec("not xml at all") == {}
    assert parse_session("<broken") == ()


def test_a_field_missing_its_number_or_name_is_skipped_rather_than_guessed() -> None:
    document = (
        "<fix><fields>"
        "<field number='1' name='Account' type='STRING'/>"
        "<field name='NoNumber' type='STRING'/>"
        "<field number='x' name='NotATag' type='STRING'/>"
        "<field number='2'/>"
        "</fields></fix>"
    )
    assert set(parse_spec(document)) == {1}


# -- what every message carries -----------------------------------------------


def test_the_session_layer_is_the_header_then_the_trailer() -> None:
    session = parse_session(SPEC)
    assert [name for name, _ in session] == [
        "BeginString",
        "MsgSeqNum",
        "PossDupFlag",
        "CheckSum",
        "Signature",
    ]


def test_the_session_layer_says_which_fields_a_message_must_carry() -> None:
    session = dict(parse_session(SPEC))
    assert session["BeginString"] is True
    assert session["MsgSeqNum"] is True
    assert session["CheckSum"] is True
    assert session["PossDupFlag"] is False
    assert session["Signature"] is False


def test_a_group_inside_the_header_is_not_a_session_field() -> None:
    """`HopGrp` is a repeating group, and one row of it is not one value."""
    assert "HopGrp" not in dict(parse_session(SPEC))
