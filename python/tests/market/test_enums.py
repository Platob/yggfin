"""The codes are a contract with data already on disk, so they are pinned here.

A `Ranged` value is written into a column and read back years later by a build
that has moved on. That makes renumbering a member a silent rewrite of what
stored rows mean -- the one failure this package's contracts exist to prevent
-- so the table below is a literal, checked against the enum. It is meant to be
awkward to change: adding a line is fine, editing one is a migration.
"""

from __future__ import annotations

import pyarrow
import pytest

import rekep.enums
import rekep.enums.ranged as enum_module
from rekep.enums import (
    MIC,
    AssetKind,
    Currency,
    EventType,
    IdSource,
    MarketKind,
    OptionKind,
    Ranged,
    Side,
    State,
    TimeInForce,
)

RANGED = (
    State,
    MarketKind,
    AssetKind,
    OptionKind,
)

PACKED = (Side, TimeInForce, EventType)


def test_every_public_code_is_a_code_and_every_base_is_a_base() -> None:
    """Two modules: `ranged` is what a code is built on, `codes` is the codes."""
    codes = (
        MIC,
        AssetKind,
        Currency,
        EventType,
        IdSource,
        MarketKind,
        OptionKind,
        Side,
        State,
        TimeInForce,
    )
    assert {kind.__module__ for kind in codes} == {"rekep.enums.codes"}
    assert Ranged.__module__ == "rekep.enums.ranged"
    assert {name for name in dir(rekep.enums) if not name.startswith("_")} == {
        *(kind.__name__ for kind in codes),
        "Ranged",
        "codes",
        "ranged",
    }


def test_a_mic_is_exactly_its_four_ascii_bytes_in_int32() -> None:
    xpar = MIC.from_str("XPAR")
    assert int(xpar) == int.from_bytes(b"XPAR", "big") == 1_481_654_610
    assert xpar.into_str() == xpar.into_fix() == str(xpar) == "XPAR"


def test_a_valid_unlisted_mic_registers_once_and_round_trips_from_storage() -> None:
    first = MIC.from_str(" 21xx ")
    assert first is MIC.from_str("21XX")
    assert MIC.from_code(int(first)) is first
    assert first.code == "21XX", "digits are valid ISO 10383 code characters"


def test_an_invalid_mic_is_unknown_instead_of_a_truncated_collision() -> None:
    assert MIC.from_str(None) is MIC.UNKNOWN
    assert MIC.from_str("XPA") is MIC.UNKNOWN
    assert MIC.from_str("ABCDE") is MIC.UNKNOWN
    assert MIC.from_code(-1) is MIC.UNKNOWN


def test_currency_is_three_letters_plus_an_ascii_decimal_digit() -> None:
    assert int(Currency.EUR) == int.from_bytes(b"EUR0", "big")
    assert Currency.EUR.code == Currency.EUR.into_fix() == "EUR"
    assert Currency.EUR.packed_code == "EUR0" and Currency.EUR.decimals == 0
    cents = Currency.from_str("EUR2")
    assert cents.packed_code == "EUR2" and cents.decimals == 2
    assert Currency.from_code(int(cents)) is cents
    assert Currency.from_str("\U0001f4b6") is Currency.UNKNOWN
    assert Currency.from_code(-1, Currency.EUR) is Currency.EUR


def test_currency_registration_is_normalised_and_bounded() -> None:
    assert Currency.from_str(" usd ") is Currency.USD
    assert Currency.from_str("TOO-LONG") is Currency.UNKNOWN
    registered = Currency.register("EUR", decimals=3, aliases=("EURO-3",))
    assert Currency.from_str("euro-3") is registered
    for value in range(enum_module._ASCII_REGISTERED_LIMIT + len(Currency) + 1):
        code = "".join(chr(65 + digit) for digit in (value // 676, value // 26 % 26, value % 26))
        Currency.from_str(code)
    assert len(enum_module._ASCII_REGISTERED[Currency]) == enum_module._ASCII_REGISTERED_LIMIT


def test_generic_packed_codes_are_strict_ascii() -> None:
    assert MIC.from_str("\U0001f4b6") is MIC.UNKNOWN
    assert MIC.schema_metadata()["encoding"] == "ascii-big-endian"


def test_mic_columns_pack_in_kernels_and_keep_invalid_values_null() -> None:
    packed = MIC.arrow_from_strings(
        pyarrow.array(["XPAR", "bad", None, ""]),
        pyarrow.array([None, "xnas", "XCME", "toolong"]),
    )
    assert packed.type == pyarrow.int32()
    assert packed.to_pylist() == [
        int(MIC.from_str("XPAR")),
        int(MIC.from_str("XNAS")),
        int(MIC.from_str("XCME")),
        None,
    ]


#: What `State` means on disk. Written out rather than derived, so a renumbering
#: fails here instead of in a year's worth of stored orders.
STATE_CODES = {
    "UNKNOWN": 0,
    "PENDING": 100,
    "PENDING_NEW": 110,
    "OPEN": 200,
    "NEW": 210,
    "ACCEPTED": 220,
    "PENDING_REPLACE": 230,
    "PENDING_CANCEL": 240,
    "SUSPENDED": 250,
    "STOPPED": 260,
    "PARTIAL": 300,
    "PARTIALLY_FILLED": 310,
    "DONE": 400,
    "FILLED": 410,
    "DONE_FOR_DAY": 420,
    "CALCULATED": 430,
    "CLOSED": 500,
    "CANCELLED": 510,
    "REPLACED": 520,
    "EXPIRED": 530,
    "INTERNAL_EXPIRED": 540,
    "FAILED": 600,
    "REJECTED": 610,
    "INTERNAL_REJECTED": 620,
}

#: The same, for the side of a market. `BID` and `ASK` are aliases and so do
#: not appear: an alias has no value of its own to pin.
SIDE_CODES = {
    "UNKNOWN": 0,
    "BUY": int.from_bytes(b"BUY\0", "big"),
    "BUY_MINUS": int.from_bytes(b"BYMN", "big"),
    "BORROW": int.from_bytes(b"BORR", "big"),
    "SUBSCRIBE": int.from_bytes(b"SUBS", "big"),
    "SELL": int.from_bytes(b"SELL", "big"),
    "SELL_PLUS": int.from_bytes(b"SLPL", "big"),
    "SELL_SHORT": int.from_bytes(b"SHRT", "big"),
    "SELL_SHORT_EXEMPT": int.from_bytes(b"SHEX", "big"),
    "LEND": int.from_bytes(b"LEND", "big"),
    "REDEEM": int.from_bytes(b"REDM", "big"),
    "CROSS": int.from_bytes(b"CROS", "big"),
    "CROSS_SHORT": int.from_bytes(b"CRSH", "big"),
    "CROSS_SHORT_EXEMPT": int.from_bytes(b"CRSE", "big"),
    "AS_DEFINED": int.from_bytes(b"ASDF", "big"),
    "OPPOSITE": int.from_bytes(b"OPPO", "big"),
    "UNDISCLOSED": int.from_bytes(b"UNDS", "big"),
}


def test_the_state_codes_are_the_ones_on_disk() -> None:
    assert {member.name: int(member) for member in State} == STATE_CODES


def test_the_side_codes_are_the_ones_on_disk() -> None:
    assert {member.name: int(member) for member in Side} == SIDE_CODES


def test_packed_side_aliases_and_unknown_codes_are_stable() -> None:
    assert Side.BID is Side.BUY and Side.ASK is Side.SELL
    assert Side.from_str("bid") is Side.BUY
    assert Side.from_str("long") is Side.BUY
    assert Side.from_str("offer") is Side.SELL
    assert Side.from_code(int.from_bytes(b"NOPE", "big")) is Side.UNKNOWN
    assert Side.from_fix("?", Side.SELL) is Side.SELL


def test_time_in_force_uses_fixed_ascii_mnemonics_and_semantic_order() -> None:
    assert int(TimeInForce.IOC).to_bytes(4, "big") == b"IOC\0"
    assert int(TimeInForce.GTC).to_bytes(4, "big") == b"GTC\0"
    assert TimeInForce.from_str("immediate_or_cancel") is TimeInForce.IOC
    assert TimeInForce.from_str("good_till_cancelled") is TimeInForce.GTC
    assert TimeInForce.from_code(int.from_bytes(b"NOPE", "big")) is TimeInForce.UNKNOWN
    assert TimeInForce.IOC < TimeInForce.SESSION <= TimeInForce.DAY < TimeInForce.RESTING


@pytest.mark.parametrize("declared", (*RANGED, *PACKED), ids=lambda cls: cls.__name__)
def test_zero_is_unknown_everywhere(declared: type) -> None:
    """Every code column reads `0` as "nothing was said", with no exception."""
    assert declared(0).name == "UNKNOWN"


@pytest.mark.parametrize("declared", (*RANGED, *PACKED), ids=lambda cls: cls.__name__)
def test_no_two_members_share_a_fix_character(declared: type) -> None:
    """A shared character would make `from_fix` pick one meaning and drop the other."""
    codes = [member.into_fix() for member in declared if member.into_fix()]
    assert len(codes) == len(set(codes)), sorted(codes)


#: The enums that are a FIX field read as a code. `EventType` is ours -- no FIX
#: field says whether a row is an order or a book -- so it is not in here, and
#: the test below would otherwise pass on it by iterating over nothing.
FIX_CODED = (
    Side,
    TimeInForce,
    *(ranged for ranged in RANGED if ranged not in (EventType, State)),
)


@pytest.mark.parametrize("ranged", FIX_CODED, ids=lambda cls: cls.__name__)
def test_every_fix_character_round_trips(ranged: type[Ranged]) -> None:
    """`from_fix` is the exact inverse of `into_fix`, for every member that has one."""
    coded = [member for member in ranged if member.into_fix()]
    assert coded, f"{ranged.__name__} declares no FIX characters"
    for member in coded:
        assert ranged.from_fix(member.into_fix()) is member


def test_market_kind_fix_values_are_tag_scoped() -> None:
    assert MarketKind.from_fix("J", tag=40) is MarketKind.MARKET_IF_TOUCHED
    assert MarketKind.from_fix("J", tag=150) is MarketKind.CLEARING_HOLD
    assert MarketKind.from_fix("J") is MarketKind.UNKNOWN
    assert MarketKind.from_fix("F", tag=150) is MarketKind.TRADE
    assert MarketKind.TRADE.into_fix(150) == "F"


def test_time_in_force_covers_the_fix_latest_code_set() -> None:
    expected = {
        "0": TimeInForce.DAY,
        "1": TimeInForce.GTC,
        "2": TimeInForce.AT_OPEN,
        "3": TimeInForce.IOC,
        "4": TimeInForce.FOK,
        "5": TimeInForce.GTX,
        "6": TimeInForce.GTD,
        "7": TimeInForce.AT_CLOSE,
        "8": TimeInForce.GOOD_THROUGH_CROSSING,
        "9": TimeInForce.AT_CROSSING,
        "A": TimeInForce.GFT,
        "B": TimeInForce.GFA,
        "C": TimeInForce.GFM,
    }
    assert {member.into_fix(): member for member in TimeInForce if member.into_fix()} == expected


def test_market_kind_covers_the_fix_latest_order_and_execution_codes() -> None:
    mappings = MarketKind.fix_mapping()
    assert set(mappings[40]) == set("123456789ABCDEFGHIJKLMPQRST")
    assert set(mappings[150]) == set("03456789ABCDEFGHIJKLMN")
    expected = {
        "5": MarketKind.MARKET_ORDER,
        "6": MarketKind.MARKET_ORDER,
        "8": MarketKind.LIMIT_ORDER,
        "9": MarketKind.PEGGED_ORDER,
        "A": MarketKind.MARKET_ORDER,
        "C": MarketKind.MARKET_ORDER,
        "F": MarketKind.LIMIT_ORDER,
        "G": MarketKind.MARKET_ORDER,
        "H": MarketKind.PREVIOUSLY_QUOTED,
        "I": MarketKind.LIMIT_ORDER,
        "L": MarketKind.PEGGED_ORDER,
        "M": MarketKind.PEGGED_ORDER,
        "Q": MarketKind.PREVIOUSLY_QUOTED,
        "R": MarketKind.STOP_ORDER,
        "S": MarketKind.STOP_LIMIT,
        "T": MarketKind.MARKET_TO_LIMIT,
    }
    assert {code: mappings[40][code] for code in expected} == expected
    assert mappings[150]["M"] is MarketKind.LOCKED
    assert mappings[150]["N"] is MarketKind.RELEASED


def test_protocol_neutral_enums_claim_no_fix_field() -> None:
    """State characters depend on the FIX field that carries them."""
    assert not any(member.into_fix() for member in EventType)
    assert not any(member.into_fix() for member in State)
    assert EventType.from_fix("0") is EventType.UNKNOWN


@pytest.mark.parametrize("ranged", RANGED, ids=lambda cls: cls.__name__)
def test_every_band_floor_is_itself_a_member(ranged: type[Ranged]) -> None:
    """`from_code` degrades an unknown value to its band, so the band must exist."""
    for member in ranged:
        assert member.band in set(ranged), f"{member.name} sits in a band nothing names"


@pytest.mark.parametrize("ranged", RANGED, ids=lambda cls: cls.__name__)
def test_nothing_reaches_into_the_private_range(ranged: type[Ranged]) -> None:
    """Everything from `PRIVATE` up belongs to whoever runs the feed, not to us."""
    assert max(int(member) for member in ranged) < Ranged.PRIVATE


@pytest.mark.parametrize("ranged", RANGED, ids=lambda cls: cls.__name__)
def test_band_arithmetic_agrees_with_the_member(ranged: type[Ranged]) -> None:
    """`band_of` works on a raw code and must give what the member itself says."""
    for member in ranged:
        assert ranged.band_of(int(member)) == member.band


def test_an_unknown_code_degrades_to_its_band_and_keeps_the_band_true() -> None:
    """A state this build has never seen still answers "is it over?" correctly."""
    invented = int(State.DONE) + 90  # a terminal state a later release might add
    assert invented not in set(State)
    assert State.from_code(invented) is State.DONE
    assert State.from_code(invented).is_terminal
    assert State.band_of(invented) >= State.TERMINAL


def test_a_code_in_no_band_reads_as_unknown_rather_than_raising() -> None:
    """A column is an integer, so a reader must survive anything that lands in it."""
    assert State.from_code(9999) is State.UNKNOWN
    assert State.from_code(None) is State.UNKNOWN
    assert State.from_code("nonsense") is State.UNKNOWN
    assert State.from_code(9999, default=State.REJECTED) is State.REJECTED
    assert State.from_fix("~") is State.UNKNOWN


def test_the_terminal_boundary_is_crossed_from_both_sides() -> None:
    """The one predicate every reader writes, checked on both of its branches."""
    live = [member for member in State if member.is_live]
    over = [member for member in State if member.is_terminal]
    assert State.PARTIALLY_FILLED in live and State.PENDING_CANCEL in live
    assert State.FILLED in over and State.REJECTED in over and State.CANCELLED in over
    assert State.PENDING_NEW not in live and State.PENDING_NEW not in over
    assert max(live) < State.TERMINAL <= min(over)


def test_a_pending_amendment_is_live_because_the_order_still_is() -> None:
    """The distinction the band layout exists to get right."""
    assert State.PENDING_REPLACE.is_live
    assert State.PENDING_CANCEL.is_live
    assert not State.PENDING_NEW.is_live


def test_bid_is_buy_and_ask_is_sell() -> None:
    """One code per direction: two spellings would split a filter in half."""
    assert Side.BID is Side.BUY
    assert Side.ASK is Side.SELL
    assert int(Side.BID) == int(Side.BUY)


def test_the_side_band_carries_the_sign() -> None:
    assert Side.BUY.sign == 1 and Side.BUY_MINUS.sign == 1
    assert Side.SELL.sign == -1 and Side.SELL_SHORT.sign == -1
    assert Side.CROSS.sign == 0 and Side.UNKNOWN.sign == 0
    assert Side.BID.opposite is Side.SELL and Side.ASK.opposite is Side.BUY
    assert Side.CROSS.opposite is Side.CROSS


def test_only_resting_validities_rest() -> None:
    assert not TimeInForce.IOC.rests and not TimeInForce.FOK.rests
    assert TimeInForce.DAY.rests and TimeInForce.GTC.rests and TimeInForce.GTD.rests


def test_a_derivative_is_everything_above_the_derivative_band() -> None:
    assert AssetKind.OPTION.is_derivative and AssetKind.FUTURE.is_derivative
    assert AssetKind.REPO.is_derivative and AssetKind.SPREAD.is_derivative
    assert not AssetKind.EQUITY.is_derivative and not AssetKind.INDEX.is_derivative


def test_an_option_kind_reads_the_fix_characters_it_is_written_as() -> None:
    assert OptionKind.from_fix("0") is OptionKind.PUT
    assert OptionKind.from_fix("1") is OptionKind.CALL
    assert OptionKind.from_fix("") is OptionKind.UNKNOWN


def test_event_type_stores_a_readable_mnemonic_with_ranked_bands() -> None:
    """The stored value is the mnemonic; band order rides in ranks, and a
    pushed scan filters on the finite code sets the ranks spell."""
    assert EventType.ORDER.code == "ORDR"
    assert int(EventType.ORDER) == int.from_bytes(b"ORDR", "big", signed=True)
    assert EventType.ORDER.band is EventType.INTENT
    assert EventType.MISC.band is EventType.UNKNOWN
    market = EventType.ranked_at_least(EventType.INTENT)
    assert set(market) == {
        int(member) for member in EventType if member not in (EventType.UNKNOWN, EventType.MISC)
    }
    assert set(EventType.ranked_below(EventType.INTENT)) == {
        int(EventType.UNKNOWN),
        int(EventType.MISC),
    }


def test_the_event_types_partition_the_shapes_by_what_they_assert() -> None:
    """An intent may never happen, a fact cannot be undone, a state is a picture."""
    assert EventType.ORDER.band == EventType.INTENT
    assert EventType.EXECUTION.band == EventType.FACT
    assert EventType.BOOK.band == EventType.STATE
    assert EventType.INSTRUMENT.band == EventType.INSTRUMENT_STATE


def test_the_removed_book_side_code_is_not_reused() -> None:
    assert 310 not in {int(member) for member in EventType}


def test_only_a_state_is_a_snapshot() -> None:
    assert EventType.BOOK.is_snapshot
    assert not EventType.ORDER.is_snapshot and not EventType.EXECUTION.is_snapshot
    assert EventType.INSTRUMENT.is_snapshot, "reference data is a picture too"


def test_from_fix_reads_a_word_spelling_of_a_compiled_member() -> None:
    """Bridges render `SIDE=buy` and `TIMEINFORCE=gtd` where the wire says
    `1` and `6`; the exact code stays first and case-sensitive, and the word
    resolves only to a member that was compiled in -- never registering one."""
    assert Side.from_fix("buy") is Side.BUY
    assert Side.from_fix("Sell") is Side.SELL
    assert Side.from_fix("2") is Side.SELL
    assert TimeInForce.from_fix("gtd") is TimeInForce.GTD
    assert TimeInForce.from_fix("ioc") is TimeInForce.IOC

    before = len(Side._value2member_map_)
    assert Side.from_fix("weird-code", Side.UNKNOWN) is Side.UNKNOWN
    assert len(Side._value2member_map_) == before, "an unknown wire value registers nothing"
