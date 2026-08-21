"""The codes are a contract with data already on disk, so they are pinned here.

A `Ranged` value is written into a column and read back years later by a build
that has moved on. That makes renumbering a member a silent rewrite of what
stored rows mean -- the one failure this package's contracts exist to prevent
-- so the table below is a literal, checked against the enum. It is meant to be
awkward to change: adding a line is fine, editing one is a migration.
"""

from __future__ import annotations

import pytest

from rekep.market.enums import (
    AssetKind,
    EventType,
    ExecKind,
    OptionKind,
    OrderKind,
    Ranged,
    Side,
    State,
    TimeInForce,
    UpdateAction,
)

RANGED = (
    State,
    Side,
    TimeInForce,
    OrderKind,
    ExecKind,
    UpdateAction,
    AssetKind,
    OptionKind,
    EventType,
)

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
    "FAILED": 600,
    "REJECTED": 610,
}

#: The same, for the side of a market. `BID` and `ASK` are aliases and so do
#: not appear: an alias has no value of its own to pin.
SIDE_CODES = {
    "UNKNOWN": 0,
    "BUY": 100,
    "BUY_MINUS": 110,
    "BORROW": 120,
    "SUBSCRIBE": 130,
    "SELL": 200,
    "SELL_PLUS": 210,
    "SELL_SHORT": 220,
    "SELL_SHORT_EXEMPT": 230,
    "LEND": 240,
    "REDEEM": 250,
    "CROSS": 300,
    "CROSS_SHORT": 310,
    "CROSS_SHORT_EXEMPT": 320,
    "AS_DEFINED": 330,
    "OPPOSITE": 340,
    "UNDISCLOSED": 350,
}


def test_the_state_codes_are_the_ones_on_disk() -> None:
    assert {member.name: int(member) for member in State} == STATE_CODES


def test_the_side_codes_are_the_ones_on_disk() -> None:
    assert {member.name: int(member) for member in Side} == SIDE_CODES


@pytest.mark.parametrize("ranged", RANGED, ids=lambda cls: cls.__name__)
def test_zero_is_unknown_everywhere(ranged: type[Ranged]) -> None:
    """Every code column reads `0` as "nothing was said", with no exception."""
    assert ranged(0).name == "UNKNOWN"


@pytest.mark.parametrize("ranged", RANGED, ids=lambda cls: cls.__name__)
def test_no_two_members_share_a_fix_character(ranged: type[Ranged]) -> None:
    """A shared character would make `from_fix` pick one meaning and drop the other."""
    codes = [member.into_fix() for member in ranged if member.into_fix()]
    assert len(codes) == len(set(codes)), sorted(codes)


#: The enums that are a FIX field read as a code. `EventType` is ours -- no FIX
#: field says whether a row is an order or a book -- so it is not in here, and
#: the test below would otherwise pass on it by iterating over nothing.
FIX_CODED = tuple(ranged for ranged in RANGED if ranged is not EventType)


@pytest.mark.parametrize("ranged", FIX_CODED, ids=lambda cls: cls.__name__)
def test_every_fix_character_round_trips(ranged: type[Ranged]) -> None:
    """`from_fix` is the exact inverse of `into_fix`, for every member that has one."""
    coded = [member for member in ranged if member.into_fix()]
    assert coded, f"{ranged.__name__} declares no FIX characters"
    for member in coded:
        assert ranged.from_fix(member.into_fix()) is member


def test_the_one_enum_that_is_ours_claims_no_fix_field() -> None:
    """No FIX field says whether a row is an order or a book, so none is claimed."""
    assert not any(member.into_fix() for member in EventType)
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


def test_only_the_kinds_that_move_shares_are_above_the_trade_band() -> None:
    """Summing quantity without this filter counts every acknowledgement as a fill."""
    moving = {member for member in ExecKind if member.moves_shares}
    assert moving == {
        ExecKind.TRADE,
        ExecKind.TRADED,
        ExecKind.PARTIAL_FILL,
        ExecKind.FILL,
        ExecKind.AMEND,
        ExecKind.TRADE_CORRECT,
        ExecKind.TRADE_CANCEL,
    }
    assert not ExecKind.ACK.moves_shares and not ExecKind.CANCELLED.moves_shares


def test_every_ranged_deletion_counts_as_a_removal() -> None:
    """`== DELETE` misses the ranged ones, which is what the band is for."""
    assert {member for member in UpdateAction if member.removes} == {
        UpdateAction.REMOVE,
        UpdateAction.DELETE,
        UpdateAction.DELETE_THRU,
        UpdateAction.DELETE_FROM,
    }


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


def test_the_event_types_partition_the_shapes_by_what_they_assert() -> None:
    """An intent may never happen, a fact cannot be undone, a state is a picture."""
    assert EventType.ORDER.band == EventType.INTENT
    assert EventType.EXECUTION.band == EventType.FACT
    assert EventType.BOOK.band == EventType.BOOK_SIDE.band == EventType.STATE
    assert EventType.INSTRUMENT.band == EventType.REFERENCE


def test_only_a_state_is_a_snapshot() -> None:
    assert EventType.BOOK.is_snapshot and EventType.BOOK_SIDE.is_snapshot
    assert not EventType.ORDER.is_snapshot and not EventType.EXECUTION.is_snapshot
    assert EventType.INSTRUMENT.is_snapshot, "reference data is a picture too"
