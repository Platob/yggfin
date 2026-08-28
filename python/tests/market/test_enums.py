"""The codes are a contract with data already on disk, so they are pinned here.

A code is written into a column and read back years later by a build that
has moved on. That makes recoding a member a silent rewrite of what stored
rows mean -- the one failure this package's contracts exist to prevent
-- so the table below is a literal, checked against the enum. It is meant to be
awkward to change: adding a line is fine, editing one is a migration.
"""

from __future__ import annotations

import pyarrow
import pytest

import rekep.enums
import rekep.enums.ascii_codes as enum_module
from rekep.enums import (
    MIC,
    AssetKind,
    Currency,
    EventType,
    MarketKind,
    OptionKind,
    Side,
    State,
    TimeInForce,
)

#: The vocabularies that rank their members in hundred-wide bands, so a
#: detailed code says what it broadly means without the stored value being
#: an ordinal.
BANDED = (
    State,
    MarketKind,
    AssetKind,
    OptionKind,
    EventType,
)

PACKED = (Side, TimeInForce, EventType)


def test_every_public_code_is_a_code_and_every_base_is_a_base() -> None:
    """Two modules: `ascii_codes` is what a code is built on, `codes` is the codes."""
    codes = (
        MIC,
        AssetKind,
        Currency,
        EventType,
        MarketKind,
        OptionKind,
        Side,
        State,
        TimeInForce,
    )
    assert {kind.__module__ for kind in codes} == {"rekep.enums.codes"}
    assert enum_module.Ascii32.__module__ == "rekep.enums.ascii_codes"
    assert {name for name in dir(rekep.enums) if not name.startswith("_")} == {
        *(kind.__name__ for kind in codes),
        "Ascii32",
        "Ascii64",
        "ascii_codes",
        "codes",
    }


def test_a_mic_is_exactly_its_four_ascii_bytes_in_int32() -> None:
    xpar = MIC.from_str("XPAR")
    assert int(xpar) == int.from_bytes(b"XPAR", "big") == 1_481_654_610
    assert xpar.into_str() == xpar.into_fix() == str(xpar) == "XPAR"


def test_a_valid_unlisted_mic_registers_once_and_round_trips_from_storage() -> None:
    first = MIC.from_str(" 21xx ")
    assert first is MIC.from_str("21XX")
    assert MIC.from_int(int(first)) is first
    assert first.code == "21XX", "digits are valid ISO 10383 code characters"


def test_an_invalid_mic_is_unknown_instead_of_a_truncated_collision() -> None:
    assert MIC.from_str(None) is MIC.UNKNOWN
    assert MIC.from_str("XPA") is MIC.UNKNOWN
    assert MIC.from_str("ABCDE") is MIC.UNKNOWN
    assert MIC.from_int(-1) is MIC.UNKNOWN


def test_currency_is_three_letters_padded_like_every_other_ascii_code() -> None:
    """Trailing NULs, so the stored integer orders as the text does."""
    assert int(Currency.EUR) == int.from_bytes(b"EUR\0", "big")
    assert int(Currency.from_str("EUA")) < int(Currency.EUR), "and sorts alphabetically"
    assert Currency.EUR.code == Currency.EUR.into_fix() == "EUR"
    assert Currency.from_int(int(Currency.EUR)) is Currency.EUR
    assert Currency.from_str("EUR2") is Currency.UNKNOWN, "no decimal digit rides in the code"
    assert Currency.from_str("\U0001f4b6") is Currency.UNKNOWN
    assert Currency.from_int(-1, Currency.EUR) is Currency.EUR


def test_currency_registration_is_normalised_and_bounded() -> None:
    assert Currency.from_str(" usd ") is Currency.USD
    assert Currency.from_str("TOO-LONG") is Currency.UNKNOWN
    registered = Currency.register("SLE", aliases=("LEONE",))
    assert Currency.from_str("leone") is registered
    for value in range(enum_module._ASCII_REGISTERED_LIMIT + len(Currency) + 1):
        code = "".join(chr(65 + digit) for digit in (value // 676, value // 26 % 26, value % 26))
        Currency.from_str(code)
    assert len(enum_module._ASCII_REGISTERED[Currency]) == enum_module._ASCII_REGISTERED_LIMIT


def test_a_closed_set_refuses_registration_and_an_open_one_reads_exact_bytes() -> None:
    """One base for every ASCII code: openness is the only knob."""
    with pytest.raises(TypeError, match="closed set"):
        Side.register("MID")
    respelled = int.from_bytes(b"\0usd", "big")
    assert Currency.from_int(respelled) is Currency.UNKNOWN, "stored bytes are never respelled"


def test_an_ascii_enum_declares_one_arrow_dictionary_type() -> None:
    """A plain value type every engine speaks: packed integers indexing the
    readable codes, one cached instance per enum, nothing registered."""
    declared = Currency.into_arrow_type()
    assert declared is Currency.into_arrow_type()
    assert declared == pyarrow.dictionary(pyarrow.int32(), pyarrow.utf8())
    assert EventType.into_arrow_type() == pyarrow.dictionary(pyarrow.int64(), pyarrow.utf8())


def test_a_vocabulary_that_does_not_band_is_its_own_band() -> None:
    """`band` is on every code now, and a code ranked by its own packed value
    declares no floors -- so it answers with itself rather than raising."""
    assert Side.BUY.band is Side.BUY
    assert Currency.USD.band is Currency.USD
    assert MIC.XOFF.band is MIC.XOFF
    assert State.FILLED.band is State.DONE, "while a ranked one still bands"
    assert TimeInForce.IOC.band is TimeInForce.IMMEDIATE, "ranks are what band, not width"


def test_a_code_column_renders_as_the_enum_spelled_out() -> None:
    """The dictionary type an enum declares is one an array can actually be:
    Arrow indexes by position, so the codes resolve to their spellings."""
    stored = pyarrow.array([int(State.FILLED), int(State.NEW), 999], pyarrow.int64())
    rendered = State.into_arrow_array(stored)
    assert rendered.type == State.into_arrow_type()
    assert rendered.to_pylist() == [State.FILLED.code, State.NEW.code, None]
    narrow = Currency.into_arrow_array(pyarrow.array([int(Currency.USD)], pyarrow.int32()))
    assert narrow.type == Currency.into_arrow_type()
    assert narrow.to_pylist() == ["USD"]


def test_wire_aliases_resolve_alike_in_the_scalar_and_the_kernel() -> None:
    """`$` lands as USD whichever path parsed the message."""
    from rekep.text.fixmsg import _currency_arrow

    spellings = ["$", "US$", "USD", " eur ", "TRY", "bad!"]
    kernel = _currency_arrow(pyarrow.array(spellings)).to_pylist()
    scalar = [int(Currency.from_fix(value)) for value in spellings]
    assert kernel == scalar
    assert kernel[0] == kernel[1] == int(Currency.USD)


def test_ascii_int64_packs_eight_bytes_into_int64_storage() -> None:
    class Route(enum_module.Ascii64):
        UNKNOWN = 0
        SMART = "SMART"
        DARKPOOL = "DARKPOOL"

    assert int(Route.DARKPOOL) == int.from_bytes(b"DARKPOOL", "big", signed=True)
    assert int(Route.SMART) == int.from_bytes(b"SMART\0\0\0", "big", signed=True)
    assert Route.from_str(" smart ") is Route.SMART
    assert Route.from_int(int(Route.DARKPOOL)) is Route.DARKPOOL
    assert Route.from_str("TOOLONGCODE") is Route.UNKNOWN
    assert Route.into_arrow_type().index_type == pyarrow.int64()


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


#: What `State` means on disk: each mnemonic as the integer of its own bytes.
#: Written out rather than derived, so a recoding fails here instead of in a
#: year's worth of stored orders.
STATE_CODES = {
    "UNKNOWN": 0,
    "PENDING": int.from_bytes(b"10PENDNG".ljust(8, b"\0"), "big"),
    "PENDING_NEW": int.from_bytes(b"11PNDNEW".ljust(8, b"\0"), "big"),
    "OPEN": int.from_bytes(b"20OPEN".ljust(8, b"\0"), "big"),
    "NEW": int.from_bytes(b"21NEW".ljust(8, b"\0"), "big"),
    "ACCEPTED": int.from_bytes(b"22ACCEPT".ljust(8, b"\0"), "big"),
    "PENDING_REPLACE": int.from_bytes(b"23PNDRPL".ljust(8, b"\0"), "big"),
    "PENDING_CANCEL": int.from_bytes(b"24PNDCNL".ljust(8, b"\0"), "big"),
    "SUSPENDED": int.from_bytes(b"25SUSPND".ljust(8, b"\0"), "big"),
    "STOPPED": int.from_bytes(b"26STOPPD".ljust(8, b"\0"), "big"),
    "PARTIAL": int.from_bytes(b"30PARTL".ljust(8, b"\0"), "big"),
    "PARTIALLY_FILLED": int.from_bytes(b"31PRTFIL".ljust(8, b"\0"), "big"),
    "DONE": int.from_bytes(b"40DONE".ljust(8, b"\0"), "big"),
    "FILLED": int.from_bytes(b"41FILLED".ljust(8, b"\0"), "big"),
    "DONE_FOR_DAY": int.from_bytes(b"42DONEDY".ljust(8, b"\0"), "big"),
    "CALCULATED": int.from_bytes(b"43CALCD".ljust(8, b"\0"), "big"),
    "CLOSED": int.from_bytes(b"50CLOSED".ljust(8, b"\0"), "big"),
    "CANCELLED": int.from_bytes(b"51CANCLD".ljust(8, b"\0"), "big"),
    "REPLACED": int.from_bytes(b"52REPLCD".ljust(8, b"\0"), "big"),
    "EXPIRED": int.from_bytes(b"53EXPIRD".ljust(8, b"\0"), "big"),
    "INTERNAL_EXPIRED": int.from_bytes(b"54INTEXP".ljust(8, b"\0"), "big"),
    "FAILED": int.from_bytes(b"60FAILED".ljust(8, b"\0"), "big"),
    "REJECTED": int.from_bytes(b"61REJCTD".ljust(8, b"\0"), "big"),
    "INTERNAL_REJECTED": int.from_bytes(b"62INTREJ".ljust(8, b"\0"), "big"),
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
    assert Side.from_int(int.from_bytes(b"NOPE", "big")) is Side.UNKNOWN
    assert Side.from_fix("?", Side.SELL) is Side.SELL


def test_time_in_force_uses_fixed_ascii_mnemonics_and_semantic_order() -> None:
    assert int(TimeInForce.IOC).to_bytes(4, "big") == b"IOC\0"
    assert int(TimeInForce.GTC).to_bytes(4, "big") == b"GTC\0"
    assert TimeInForce.from_str("immediate_or_cancel") is TimeInForce.IOC
    assert TimeInForce.from_str("good_till_cancelled") is TimeInForce.GTC
    assert TimeInForce.from_int(int.from_bytes(b"NOPE", "big")) is TimeInForce.UNKNOWN
    assert TimeInForce.IOC < TimeInForce.SESSION <= TimeInForce.DAY < TimeInForce.RESTING


@pytest.mark.parametrize("declared", (*BANDED, *PACKED), ids=lambda cls: cls.__name__)
def test_zero_is_unknown_everywhere(declared: type) -> None:
    """Every code column reads `0` as "nothing was said", with no exception."""
    assert declared(0).name == "UNKNOWN"


@pytest.mark.parametrize("declared", (*BANDED, *PACKED), ids=lambda cls: cls.__name__)
def test_no_two_members_share_a_fix_character(declared: type) -> None:
    """A shared character would make `from_fix` pick one meaning and drop the other."""
    codes = [member.into_fix() for member in declared if member.into_fix()]
    assert len(codes) == len(set(codes)), sorted(codes)


#: The enums that are one FIX field read as a code, and the field each one is.
#: They declare no codes of their own: the dictionary enumerates that field's
#: values and the codes are read from it.
#:
#: `EventType`, `State` and `MarketKind` are not here. The first is ours -- no
#: FIX field says whether a row is an order or a book -- and the other two are
#: read from several tags at once. `AssetKind` is not here either: its letters
#: are ISO 10962's, and `CFICode <461>` enumerates nothing.
FIX_CODED = ((Side, "Side"), (TimeInForce, "TimeInForce"), (OptionKind, "PutOrCall"))


@pytest.mark.parametrize(
    ("declared", "field"), FIX_CODED, ids=lambda one: getattr(one, "__name__", one)
)
def test_every_fix_character_round_trips(declared: type, field: str) -> None:
    """`from_fix` is the exact inverse of `into_fix`, for every member that has one."""
    coded = [member for member in declared if member.into_fix()]
    assert coded, f"{declared.__name__} declares no FIX characters"
    for member in coded:
        assert declared.from_fix(member.into_fix()) is member


@pytest.mark.parametrize(
    ("declared", "field"), FIX_CODED, ids=lambda one: getattr(one, "__name__", one)
)
def test_the_codes_are_the_dictionary_s_and_are_not_written_down_here(
    declared: type, field: str
) -> None:
    """Each of these used to carry its wire code beside its spelling, which made
    the enum a second copy of one field's enumerated values -- and two copies
    can disagree in the direction that mis-parses a message."""
    from rekep.fix import FixRegistry

    entry = FixRegistry.from_builtin().scalar(field)
    assert declared.FIX_FIELD == field
    assert entry.fix.tag, f"{field} is in the packaged projection"
    for code, member in declared._fix_codes().items():
        assert entry.fix.value_of(code) is not None, f"{code} is one the dictionary declares"
        assert member.into_fix() == code


def test_an_iso_category_letter_is_not_a_fix_value() -> None:
    """`AssetKind` is coded on ISO 10962, which `CFICode <461>` carries as an
    unenumerated string -- so the dictionary cannot answer for it and the ten
    letters stay written down, under a name that says which standard they are."""
    from rekep.fix import FixRegistry

    assert AssetKind.from_cfi("E") is AssetKind.EQUITY
    assert AssetKind.EQUITY.cfi_category == "E"
    assert AssetKind.from_cfi("") is AssetKind.UNKNOWN
    assert AssetKind.FIX_FIELD == "", "it codes no FIX field"
    assert not FixRegistry.from_builtin().scalar("CFICode").fix.enumerated


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
    assert State.from_fix("0") is State.UNKNOWN


@pytest.mark.parametrize("declared", BANDED, ids=lambda cls: cls.__name__)
def test_every_band_floor_is_itself_a_member(declared: type) -> None:
    """A member's band is the floor its rank sits in, so the floor must exist."""
    for member in declared:
        assert member.band in set(declared), f"{member.name} sits in a band nothing names"


@pytest.mark.parametrize("declared", BANDED, ids=lambda cls: cls.__name__)
def test_a_rank_is_a_band_offset_and_the_codes_are_unique(declared: type) -> None:
    """Ranks order the vocabulary; the packed codes identify it."""
    assert max(member.rank for member in declared) < enum_module.PRIVATE_RANK
    assert len({int(member) for member in declared}) == len(list(declared))
    for member in declared:
        assert member.band.rank == member.rank // declared.WIDTH * declared.WIDTH


def test_a_code_no_state_spells_reads_as_unknown_rather_than_raising() -> None:
    """A column is an integer, so a reader must survive anything that lands in it."""
    assert State.from_int(9999) is State.UNKNOWN
    assert State.from_int(None) is State.UNKNOWN
    assert State.from_int("nonsense") is State.UNKNOWN
    assert State.from_int(9999, default=State.REJECTED) is State.REJECTED
    assert State.from_fix("~") is State.UNKNOWN


def test_the_terminal_boundary_is_crossed_from_both_sides() -> None:
    """The one predicate every reader writes, checked on both of its branches."""
    live = [member for member in State if member.is_live]
    over = [member for member in State if member.is_terminal]
    assert State.PARTIALLY_FILLED in live and State.PENDING_CANCEL in live
    assert State.FILLED in over and State.REJECTED in over and State.CANCELLED in over
    assert State.PENDING_NEW not in live and State.PENDING_NEW not in over
    assert max(member.rank for member in live) < State.TERMINAL
    assert State.TERMINAL <= min(member.rank for member in over)
    assert set(State.live_codes()) == {int(member) for member in live}
    assert set(State.terminal_codes()) == {int(member) for member in over}


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
    assert EventType.ORDER.code == "ORDER"
    assert int(EventType.ORDER) == int.from_bytes(b"ORDER\0\0\0", "big", signed=True)
    assert EventType.EXECUTION.code == "EXECUTED", "eight bytes buy the explicit spelling"
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


def test_a_stored_event_code_decodes_exactly_or_not_at_all() -> None:
    """The mnemonic set is closed: near-miss bytes are not respelled into a
    member, so a Python answer and a pushed code-set filter keep the same
    rows."""
    respelled = int.from_bytes(b"order".ljust(8, b"\0"), "big", signed=True)
    assert EventType.from_int(respelled) is EventType.UNKNOWN
    assert EventType(respelled) is EventType.UNKNOWN
    assert EventType.from_int(int(EventType.ORDER)) is EventType.ORDER
    assert respelled not in EventType.ranked_at_least(EventType.INTENT)
    assert respelled not in EventType.ranked_below(EventType.INTENT)


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
