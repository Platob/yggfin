"""`FixMsg.from_pairs`: what a key may be, and what a value comes out as."""

from __future__ import annotations

import datetime

import pytest

from rekep import FixMsg, Kwarg
from rekep.fix.entries import fold

#: A small dictionary, so the tests say which names they rely on.
TAGS = {
    "MsgType": 35,
    "Side": 54,
    "Price": 44,
    "OrderQty": 38,
    "TransactTime": 60,
    "PartyID": 448,
    "Symbol": 55,
    "AggressorIndicator": 1057,
}


def _raw(message: FixMsg, field: int | str) -> str | None:
    """Raw text from the shared typed accessor."""
    reading = message.get(field)
    return reading.raw if reading else None


def _raws(message: FixMsg, field: int | str) -> list[str]:
    """Every raw occurrence from the shared typed accessor."""
    return [reading.raw for reading in message.readings(field)]


# -- keys --------------------------------------------------------------------


def test_a_tag_is_already_a_key() -> None:
    built = FixMsg.from_pairs([(54, "1"), ("44", "100.5")], TAGS)
    assert built.pairs == [("54", "1"), ("44", "100.5")]


@pytest.mark.parametrize("spelled", ["Side", "side", "SIDE", "sIdE", " Side "])
def test_a_name_resolves_however_it_is_cased(spelled: str) -> None:
    assert FixMsg.from_pairs([(spelled, "1")], TAGS).pairs == [("54", "1")]


@pytest.mark.parametrize("spelled", ["msg_type", "msg-type", "MSG_TYPE", "Msg Type"])
def test_a_renderer_s_separators_are_a_spelling_of_their_own(spelled: str) -> None:
    """A separator is part of the name, so an unknown spelling is kept, not guessed at."""
    assert FixMsg.from_pairs([(spelled, "D")], TAGS).pairs == [(spelled, "D")]


def test_a_component_path_names_the_field_at_the_end_of_it() -> None:
    """`Instrument.Symbol` is a Symbol; the path says where it sits, not what it is."""
    assert FixMsg.from_pairs([("Instrument.Symbol", "AAPL")], TAGS).pairs == [
        ("Instrument.55", "AAPL")
    ]


def test_an_entry_index_survives_onto_the_stored_key() -> None:
    """Which entry a field came from is data, and dropping it merges the entries."""
    built = FixMsg.from_pairs([("PartyID[0]", "A"), ("PartyID[1]", "B")], TAGS)
    assert built.pairs == [("448[0]", "A"), ("448[1]", "B")]
    assert _raws(built, 448) == ["A", "B"]


def test_a_group_entry_keeps_both_halves_of_where_it_sits() -> None:
    built = FixMsg.from_pairs([("NoPartyIDs[0].PartyID", "A")], TAGS)
    assert built.pairs == [("NoPartyIDs[0].448", "A")]
    assert _raw(built, "PartyID") == _raw(built, 448) == "A"


def test_a_numeric_terminal_component_key_keeps_its_location_and_identity() -> None:
    built = FixMsg.from_pairs([("NoPartyIDs[0].PartyID", "A")], TAGS)

    assert built.kwargs == [
        Kwarg(
            tag=448,
            key="448",
            value="A",
            comp="NoPartyIDs[0]",
        )
    ]
    assert built.get(448).raw == "A"
    assert built.get("PartyID").raw == "A"
    assert built.get("NoPartyIDs[0].PartyID").raw == "A"


def test_an_unknown_name_is_kept_exactly_as_it_arrived() -> None:
    """Every venue sends fields no dictionary has, and dropping them loses data."""
    built = FixMsg.from_pairs([("VenueOwnThing", "x"), ("Side", "1")], TAGS)
    assert built.pairs == [("VenueOwnThing", "x"), ("54", "1")]
    assert _raw(built, "VenueOwnThing") == "x"


def test_without_a_dictionary_every_name_stays_a_name() -> None:
    """And `get` still finds it, because the rendered spellings are its fallback."""
    built = FixMsg.from_pairs([("Side", "1")])
    assert built.pairs == [("Side", "1")]
    assert _raw(built, "Side") == "1" and _raw(built, "side") == "1"


def test_a_key_that_is_nothing_at_all_drops_its_pair() -> None:
    assert FixMsg.from_pairs([("", "x"), ("   ", "y"), (True, "z")], TAGS).pairs == []


# -- values ------------------------------------------------------------------


def test_text_is_itself() -> None:
    assert _raw(FixMsg.from_pairs([("Side", "1")], TAGS), 54) == "1"


def test_a_boolean_is_the_y_or_n_a_fix_reader_accepts() -> None:
    """`True` is what Python prints and what no FIX parser reads."""
    built = FixMsg.from_pairs([("AggressorIndicator", True)], TAGS)
    assert _raw(built, 1057) == "Y"
    assert _raw(FixMsg.from_pairs([("AggressorIndicator", False)], TAGS), 1057) == "N"


def test_a_float_is_never_written_in_exponent_notation() -> None:
    """FIX `Price` is digits with an optional point; `1e-07` is not a price."""
    built = FixMsg.from_pairs([("Price", 1e-07), ("OrderQty", 1e18)], TAGS)
    assert _raw(built, 44) == "0.0000001"
    assert "e" not in _raw(built, 38) and _raw(built, 38) == "1000000000000000000"


def test_a_float_keeps_the_value_it_round_trips_as() -> None:
    for value in (100.5, 0.1, 1 / 3, 1e-7, 123456789.123456):
        text = _raw(FixMsg.from_pairs([("Price", value)], TAGS), 44)
        assert float(text) == value, text


def test_an_integer_is_its_digits() -> None:
    assert _raw(FixMsg.from_pairs([("OrderQty", 10)], TAGS), 38) == "10"


def test_an_instant_is_the_utctimestamp_the_standard_fixes() -> None:
    stamped = datetime.datetime(2026, 8, 21, 10, 30, 0, 123456)
    assert (
        _raw(FixMsg.from_pairs([("TransactTime", stamped)], TAGS), 60) == "20260821-10:30:00.123456"
    )


def test_a_date_and_a_time_get_their_own_spellings() -> None:
    assert _raw(FixMsg.from_pairs([("TransactTime", datetime.date(2026, 8, 21))], TAGS), 60) == (
        "20260821"
    )
    assert _raw(FixMsg.from_pairs([("TransactTime", datetime.time(10, 30))], TAGS), 60) == (
        "10:30:00.000000"
    )


def test_a_value_that_knows_its_fix_spelling_is_asked_for_it() -> None:
    """Which is how a banded code renders as the character it was read from."""
    from rekep.market import Side

    assert _raw(FixMsg.from_pairs([("Side", Side.BUY)], TAGS), 54) == "1"


def test_a_none_value_drops_its_pair_rather_than_writing_an_empty_field() -> None:
    """`54=` is a malformed message, not an absent side."""
    assert FixMsg.from_pairs([("Side", None), ("Price", 1.0)], TAGS).pairs == [("44", "1.0")]


def test_bytes_arrive_as_the_text_they_encode() -> None:
    assert _raw(FixMsg.from_pairs([("Symbol", b"AAPL")], TAGS), 55) == "AAPL"


@pytest.mark.parametrize(
    ("source", "text"),
    [
        (b"35=D|11=A|", None),
        ("35=D|58=file.json", "file.json"),
    ],
)
def test_generic_scalar_dispatch_always_treats_text_and_bytes_as_fix(
    source: str | bytes, text: str | None
) -> None:
    built = FixMsg.from_(source)

    assert built.MsgType == "D"
    assert built.get("Text").raw == text


# -- what a message is -------------------------------------------------------


def test_order_and_repetition_are_kept_because_they_are_the_message() -> None:
    """A repeating group *is* tags repeating, so a mapping would lose it."""
    built = FixMsg.from_pairs([("448", "A"), ("448", "B"), ("54", "1")], TAGS)
    assert built.pairs == [("448", "A"), ("448", "B"), ("54", "1")]
    assert _raws(built, 448) == ["A", "B"]


def test_what_from_pairs_builds_reads_back_the_same_through_from_text() -> None:
    """The two ways in have to agree, or a round trip through a log changes a message."""
    built = FixMsg.from_pairs(
        [("MsgType", "D"), ("Side", 1), ("NoPartyIDs[0].PartyID", "A"), ("Custom", "x")], TAGS
    )
    assert FixMsg.from_text(built.into_text("|"), named=True).pairs == built.pairs


def test_the_fold_is_case_and_nothing_else() -> None:
    """Pinned directly, because every name rule above rests on it."""
    assert fold("MsgType") == fold("MSGTYPE") == "msgtype"
    assert fold("msg_type") != fold("MsgType"), "a separator is part of a name"
    assert fold("Side") != fold("Sides"), "it folds spelling, not meaning"
