"""Every stable protocol and market code, over the two bases.

One module rather than one per enum: they share nothing but their base, none
of them refers to another, and a reader looking for the spelling of a code
should not have to guess which file it is in.
"""

from __future__ import annotations

import enum
import functools
import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Self

from rekep.enums.ascii_codes import Ascii32, Ascii64

#: ISO 10962's category letter -- the first character of a `CFICode <461>` --
#: for each kind that has one.
#:
#: Not a FIX code, which is why it is a table and not a lookup: `CFICode` is a
#: six-character string and the dictionary enumerates no values for it at all,
#: so this classification is ISO 10962's and the registry cannot answer it.
#: FIX's own classification of the same instruments is `SecurityType <167>`,
#: read separately in `rekep.market.fix`.
_CFI_CATEGORIES: Mapping[str, str] = MappingProxyType(
    {
        "EQUITY": "E",
        "DEBT": "D",
        "FUND": "C",
        "CURRENCY": "T",
        "COMMODITY": "J",
        "INDEX": "M",
        "FUTURE": "F",
        "OPTION": "O",
        "SWAP": "S",
        "WARRANT": "R",
    }
)


class AssetKind(Ascii64):
    """Tradable asset kind, banded by settlement."""

    UNKNOWN = 0
    CASH = "CASH", 100
    EQUITY = "EQUITY", 110
    DEBT = "DEBT", 120
    FUND = "FUND", 130
    CURRENCY = "CURRENCY", 140
    COMMODITY = "COMMDTY", 150
    INDEX = "INDEX", 160
    DERIVATIVE = "DERIV", 200
    FUTURE = "FUTURE", 210
    OPTION = "OPTION", 220
    SWAP = "SWAP", 230
    WARRANT = "WARRANT", 240
    FORWARD = "FORWARD", 250
    STRUCTURED = "STRUCTD", 300
    SPREAD = "SPREAD", 310
    MULTILEG = "MULTILEG", 320
    BASKET = "BASKET", 330
    FINANCING = "FINANCE", 400
    REPO = "REPO", 410
    LOAN = "LOAN", 420

    @property
    def cfi_category(self) -> str:
        """ISO 10962's category letter for this kind, or empty where it has none."""
        return _CFI_CATEGORIES.get(self.name, "")

    @classmethod
    @functools.cache
    def _by_cfi(cls) -> Mapping[str, AssetKind]:
        return MappingProxyType({letter: cls[name] for name, letter in _CFI_CATEGORIES.items()})

    @classmethod
    def from_cfi(cls, category: Any) -> AssetKind:
        """The kind one ISO 10962 category letter names, or `UNKNOWN`.

        Spelled apart from `from_fix` because a CFI letter is not a FIX value:
        `E` is an equity to ISO 10962 and nothing at all to the dictionary.
        """
        letter = str(category or "").strip().upper()[:1]
        return cls._by_cfi().get(letter, cls.UNKNOWN)

    @property
    def is_derivative(self) -> bool:
        """Whether derivative-specific instrument fields apply."""
        return self.rank >= AssetKind.DERIVATIVE.rank


class EventType(Ascii64):
    """Event kind stored as an eight-byte ASCII mnemonic, banded by rank.

    Eight bytes buy explicit spellings -- `ORDER`, `QUOTE`, `EXECUTED` --
    where four forced abbreviations. The stored value is the readable
    mnemonic; the band order the row predicates reason over rides in each
    member's rank, so a kind question compares ranks and a storage scan
    filters on the finite code sets `ranked_at_least`/`ranked_below` spell.
    """

    UNKNOWN = 0
    MISC = "MISC", 10
    INTENT = "INTENT", 100
    ORDER = "ORDER", 110
    QUOTE = "QUOTE", 120
    FACT = "FACT", 200
    EXECUTION = "EXECUTED", 210
    STATE = "STATE", 300
    BOOK = "BOOK", 320
    INSTRUMENT_STATE = "ISTATE", 400
    INSTRUMENT = "INSTRMT", 410

    @property
    def is_snapshot(self) -> bool:
        """Whether the row is a state rather than an occurrence."""
        return self._rank >= EventType.STATE._rank


class MarketKind(Ascii64):
    """Order pricing and execution semantics, in stable bands."""

    UNKNOWN = 0
    MARKET = "MARKET", 100
    MARKET_ORDER = "MKTORDER", 110
    MARKET_IF_TOUCHED = "MKTIFTCH", 120
    MARKET_TO_LIMIT = "MKTTOLMT", 130
    LIMIT = "LIMIT", 200
    LIMIT_ORDER = "LMTORDER", 210
    LIMIT_ON_CLOSE = "LMTCLOSE", 220
    LIMIT_OR_BETTER = "LMTBETTR", 230
    STOP = "STOP", 300
    STOP_ORDER = "STPORDER", 310
    STOP_LIMIT = "STOPLMT", 320
    PEGGED = "PEGGED", 400
    PEGGED_ORDER = "PEGORDER", 410
    PREVIOUSLY_QUOTED = "PREVQUOT", 420
    PREVIOUSLY_INDICATED = "PREVINDC", 430
    EXECUTION = "EXEC", 500
    ORDER_STATUS = "ORDSTAT", 510
    TRADE = "TRADE", 520
    TRADE_CORRECT = "TRDCORRC", 530
    TRADE_CANCEL = "TRDCANCL", 540
    LOCKED = "LOCKED", 550
    RELEASED = "RELEASED", 560
    CLEARING = "CLEARING", 600
    CLEARING_HOLD = "CLRHOLD", 610
    RELEASED_TO_CLEARING = "RELTOCLR", 620
    ACTIVATION = "ACTIVATN", 700
    TRIGGERED = "TRIGGERD", 710

    @classmethod
    def fix_mapping(cls) -> dict[int, dict[str, MarketKind]]:
        """Return FIX tag and wire spelling mappings."""
        return {
            tag: {code: cls(member) for code, member in values.items()}
            for tag, values in _MARKET_KIND_FIX.items()
        }

    @classmethod
    def from_fix(
        cls, code: Any, default: Self | None = None, *, tag: int | str | None = None
    ) -> Self:
        """Read a tag-scoped FIX value; ambiguous values are unknown."""
        spelling = str(code).strip() if code is not None else ""
        if tag is not None:
            try:
                member = _MARKET_KIND_FIX.get(int(tag), {}).get(spelling)
            except (TypeError, ValueError):
                member = None
            return cls(member) if member is not None else default or cls.UNKNOWN
        matches = {
            member for values in _MARKET_KIND_FIX.values() if (member := values.get(spelling))
        }
        return cls(matches.pop()) if len(matches) == 1 else default or cls.UNKNOWN

    def into_fix(self, tag: int | str | None = None) -> str:
        """Return the unique wire spelling under `tag`, when one exists."""
        if tag is not None:
            try:
                mapping = _MARKET_KIND_FIX.get(int(tag), {})
            except (TypeError, ValueError):
                return ""
            codes = {code for code, member in mapping.items() if member == self}
            return codes.pop() if len(codes) == 1 else ""
        codes = {
            code
            for values in _MARKET_KIND_FIX.values()
            for code, member in values.items()
            if member == self
            and len(
                {
                    candidate.get(code)
                    for candidate in _MARKET_KIND_FIX.values()
                    if code in candidate
                }
            )
            == 1
        }
        return codes.pop() if len(codes) == 1 else ""


_MARKET_KIND_FIX: dict[int, dict[str, int]] = {
    40: {
        "1": MarketKind.MARKET_ORDER,
        "5": MarketKind.MARKET_ORDER,
        "6": MarketKind.MARKET_ORDER,
        "A": MarketKind.MARKET_ORDER,
        "C": MarketKind.MARKET_ORDER,
        "G": MarketKind.MARKET_ORDER,
        "J": MarketKind.MARKET_IF_TOUCHED,
        "K": MarketKind.MARKET_TO_LIMIT,
        "T": MarketKind.MARKET_TO_LIMIT,
        "2": MarketKind.LIMIT_ORDER,
        "8": MarketKind.LIMIT_ORDER,
        "F": MarketKind.LIMIT_ORDER,
        "I": MarketKind.LIMIT_ORDER,
        "B": MarketKind.LIMIT_ON_CLOSE,
        "7": MarketKind.LIMIT_OR_BETTER,
        "3": MarketKind.STOP_ORDER,
        "R": MarketKind.STOP_ORDER,
        "4": MarketKind.STOP_LIMIT,
        "S": MarketKind.STOP_LIMIT,
        "9": MarketKind.PEGGED_ORDER,
        "L": MarketKind.PEGGED_ORDER,
        "M": MarketKind.PEGGED_ORDER,
        "P": MarketKind.PEGGED_ORDER,
        "D": MarketKind.PREVIOUSLY_QUOTED,
        "H": MarketKind.PREVIOUSLY_QUOTED,
        "Q": MarketKind.PREVIOUSLY_QUOTED,
        "E": MarketKind.PREVIOUSLY_INDICATED,
    },
    150: {
        "0": MarketKind.ORDER_STATUS,
        "3": MarketKind.ORDER_STATUS,
        "4": MarketKind.ORDER_STATUS,
        "5": MarketKind.ORDER_STATUS,
        "6": MarketKind.ORDER_STATUS,
        "7": MarketKind.ORDER_STATUS,
        "8": MarketKind.ORDER_STATUS,
        "9": MarketKind.ORDER_STATUS,
        "A": MarketKind.ORDER_STATUS,
        "B": MarketKind.ORDER_STATUS,
        "C": MarketKind.ORDER_STATUS,
        "D": MarketKind.ORDER_STATUS,
        "E": MarketKind.ORDER_STATUS,
        "F": MarketKind.TRADE,
        "G": MarketKind.TRADE_CORRECT,
        "H": MarketKind.TRADE_CANCEL,
        "I": MarketKind.ORDER_STATUS,
        "J": MarketKind.CLEARING_HOLD,
        "K": MarketKind.RELEASED_TO_CLEARING,
        "L": MarketKind.TRIGGERED,
        "M": MarketKind.LOCKED,
        "N": MarketKind.RELEASED,
    },
}


class OptionKind(Ascii64):
    """Option direction read from FIX `PutOrCall <201>`."""

    FIX_FIELD = enum.nonmember("PutOrCall")

    UNKNOWN = 0
    PUT = "PUT", 100
    CALL = "CALL", 200


class State(Ascii64):
    """Event lifecycle, ordered by completion.

    Each code carries its rank as a two-digit prefix, so the stored value
    sorts exactly as the lifecycle does: `21NEW` before `41FILLED`, every
    live state below every terminal one. "Still live" and "finished" are
    rank questions, and a storage scan filters on the code sets
    `ranked_at_least`, `ranked_below` and `ranked_between` spell -- or on a
    range, which the ordering now makes honest.
    """

    #: The rank the terminal states begin at.
    TERMINAL = enum.nonmember(400)

    UNKNOWN = 0
    """Nothing has been stated."""
    PENDING = "10PENDNG", 100
    """Band floor: requested but not acknowledged."""
    PENDING_NEW = "11PNDNEW", 110
    """Awaiting first venue acknowledgement."""
    OPEN = "20OPEN", 200
    """Band floor: live at the venue."""
    NEW = "21NEW", 210
    """Acknowledged and working."""
    ACCEPTED = "22ACCEPT", 220
    """Accepted but not yet working."""
    PENDING_REPLACE = "23PNDRPL", 230
    """Amendment pending while the original remains live."""
    PENDING_CANCEL = "24PNDCNL", 240
    """Cancellation pending while the order remains live."""
    SUSPENDED = "25SUSPND", 250
    """Held by the venue and resumable."""
    STOPPED = "26STOPPD", 260
    """Stopped at a price awaiting a trade."""
    PARTIAL = "30PARTL", 300
    """Band floor: live and partly complete."""
    PARTIALLY_FILLED = "31PRTFIL", 310
    """Some quantity traded; the rest remains live."""
    DONE = "40DONE", 400
    """Band floor and first terminal state."""
    FILLED = "41FILLED", 410
    """Every share traded."""
    DONE_FOR_DAY = "42DONEDY", 420
    """Over for the session."""
    CALCULATED = "43CALCD", 430
    """Priced and closed by the venue."""
    CLOSED = "50CLOSED", 500
    """Band floor: over without completion."""
    CANCELLED = "51CANCLD", 510
    """Withdrawn before completion."""
    REPLACED = "52REPLCD", 520
    """Superseded by an amendment."""
    EXPIRED = "53EXPIRD", 530
    """Reached expiry while live."""
    INTERNAL_EXPIRED = "54INTEXP", 540
    """Expired locally after one day without a newer observation."""
    FAILED = "60FAILED", 600
    """Band floor: refused."""
    REJECTED = "61REJCTD", 610
    """Refused; reason fields explain why."""
    INTERNAL_REJECTED = "62INTREJ", 620
    """Refused by this pipeline before it could change market state."""

    @classmethod
    @functools.cache
    def fix_mapping(cls) -> Mapping[int, Mapping[str, State]]:
        """Tag-scoped lifecycle meanings written into a built FIX registry."""
        declared = {
            35: {
                "D": cls.PENDING_NEW,
                "F": cls.PENDING_CANCEL,
                "G": cls.PENDING_REPLACE,
                "9": cls.UNKNOWN,
            },
            39: {
                "0": cls.NEW,
                "1": cls.PARTIALLY_FILLED,
                "2": cls.FILLED,
                "3": cls.DONE_FOR_DAY,
                "4": cls.CANCELLED,
                "5": cls.REPLACED,
                "6": cls.PENDING_CANCEL,
                "7": cls.STOPPED,
                "8": cls.REJECTED,
                "9": cls.SUSPENDED,
                "A": cls.PENDING_NEW,
                "B": cls.CALCULATED,
                "C": cls.EXPIRED,
                "D": cls.ACCEPTED,
                "E": cls.PENDING_REPLACE,
            },
            150: {
                "0": cls.NEW,
                "1": cls.PARTIALLY_FILLED,
                "2": cls.FILLED,
                "3": cls.DONE_FOR_DAY,
                "4": cls.CANCELLED,
                "5": cls.REPLACED,
                "6": cls.PENDING_CANCEL,
                "7": cls.STOPPED,
                "8": cls.REJECTED,
                "9": cls.SUSPENDED,
                "A": cls.PENDING_NEW,
                "B": cls.CALCULATED,
                "C": cls.EXPIRED,
                "E": cls.PENDING_REPLACE,
                "F": cls.PARTIALLY_FILLED,
                "G": cls.REPLACED,
                "H": cls.CANCELLED,
            },
            279: {
                "0": cls.NEW,
                "1": cls.OPEN,
                "2": cls.CANCELLED,
                "3": cls.CANCELLED,
                "4": cls.CANCELLED,
                "5": cls.OPEN,
            },
            297: {
                "0": cls.ACCEPTED,
                "1": cls.CANCELLED,
                "2": cls.CANCELLED,
                "3": cls.CANCELLED,
                "4": cls.CANCELLED,
                "5": cls.REJECTED,
                "6": cls.CANCELLED,
                "7": cls.EXPIRED,
                "9": cls.REJECTED,
                "10": cls.PENDING,
                "11": cls.CANCELLED,
                "12": cls.OPEN,
                "13": cls.OPEN,
                "14": cls.CANCELLED,
                "15": cls.CANCELLED,
                "16": cls.OPEN,
                "17": cls.CANCELLED,
                "18": cls.OPEN,
                "19": cls.PENDING_CANCEL,
                "21": cls.FILLED,
                "22": cls.FILLED,
                "23": cls.EXPIRED,
            },
            694: {
                "1": cls.FILLED,
                "2": cls.OPEN,
                "3": cls.EXPIRED,
                "4": cls.OPEN,
                "5": cls.CANCELLED,
                "6": cls.CANCELLED,
                "7": cls.CANCELLED,
                "8": cls.CANCELLED,
                "9": cls.OPEN,
                "10": cls.OPEN,
                "11": cls.ACCEPTED,
                "12": cls.CANCELLED,
            },
        }
        return MappingProxyType({tag: MappingProxyType(values) for tag, values in declared.items()})

    @property
    def is_live(self) -> bool:
        """Whether the event is working at the venue."""
        return State.OPEN.rank <= self.rank < State.TERMINAL

    @property
    def is_terminal(self) -> bool:
        """Whether no further lifecycle transition is expected."""
        return self.rank >= State.TERMINAL

    @classmethod
    def live_codes(cls) -> tuple[int, ...]:
        """Stored codes of the live states, for a pushed scan filter."""
        return tuple(int(member) for member in cls if member.is_live)

    @classmethod
    def terminal_codes(cls) -> tuple[int, ...]:
        """Stored codes of the terminal states, for a pushed scan filter."""
        return tuple(int(member) for member in cls if member.is_terminal)


class MIC(Ascii32):
    """ISO 10383 code stored as four ASCII bytes in one `int32`."""

    _PATTERN = enum.nonmember(re.compile(r"^[A-Z0-9]{4}$"))

    UNKNOWN = 0
    """No valid market identifier was present."""

    XOFF = "XOFF"
    """Off-market transaction."""

    XXXX = "XXXX"
    """No market, including an unlisted instrument."""

    @classmethod
    def _valid(cls, text: str) -> bool:
        return bool(cls._PATTERN.fullmatch(text))

    @classmethod
    def _registers_unknown(cls) -> bool:
        return True

    @classmethod
    def schema_metadata(cls) -> dict[str, str]:
        return {**super().schema_metadata(), "pattern": "[A-Z0-9]{4}"}

    @classmethod
    def arrow_from_strings(cls, *values: Any) -> Any:
        """Pack the first valid MIC across string columns with Arrow kernels."""
        if not values:
            raise ValueError("at least one MIC source column is required")
        import pyarrow
        import pyarrow.compute as compute

        alphabet = pyarrow.array(list("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
        encoded = []
        for value in values:
            text = compute.utf8_upper(compute.utf8_trim_whitespace(value.cast(pyarrow.string())))
            valid = compute.fill_null(
                compute.match_substring_regex(text, cls._PATTERN.pattern), False
            )
            packed = pyarrow.repeat(pyarrow.scalar(0, pyarrow.int32()), len(text))
            for index, multiplier in enumerate((1 << 24, 1 << 16, 1 << 8, 1)):
                character = compute.utf8_slice_codeunits(text, start=index, stop=index + 1)
                position = compute.index_in(character, value_set=alphabet)
                byte = compute.if_else(
                    compute.less(position, 10),
                    compute.add(position, 48),
                    compute.add(position, 55),
                ).cast(pyarrow.int32())
                packed = compute.add(packed, compute.multiply(byte, multiplier)).cast(
                    pyarrow.int32()
                )
            encoded.append(compute.if_else(valid, packed, pyarrow.scalar(None, pyarrow.int32())))
        return compute.coalesce(*encoded)


class Currency(Ascii32):
    """ISO 4217 alphabetic code stored as three ASCII letters.

    Packed like every other ASCII code -- NUL-padded to the storage width --
    so the fourth byte is simply zero. No decimal count rides in the value:
    a minor-unit convention is venue data, not part of the code.
    """

    _PATTERN = enum.nonmember(re.compile(r"^[A-Z]{3}$"))

    @classmethod
    def _valid(cls, text: str) -> bool:
        return bool(cls._PATTERN.fullmatch(text))

    @classmethod
    def _registers_unknown(cls) -> bool:
        return True

    @classmethod
    def _built_in_aliases(cls) -> dict[str, str]:
        return {
            "$": "USD",
            "US$": "USD",
            "\u20ac": "EUR",
            "\u00a3": "GBP",
            "\u00a5": "JPY",
        }

    @classmethod
    def schema_metadata(cls) -> dict[str, str]:
        return {**super().schema_metadata(), "pattern": "[A-Z]{3}"}

    UNKNOWN = 0
    """No currency was present."""

    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    JPY = "JPY"
    CHF = "CHF"
    CAD = "CAD"
    AUD = "AUD"
    NZD = "NZD"
    CNY = "CNY"
    HKD = "HKD"
    SGD = "SGD"
    SEK = "SEK"
    NOK = "NOK"
    DKK = "DKK"
    PLN = "PLN"
    CZK = "CZK"
    HUF = "HUF"
    MXN = "MXN"
    BRL = "BRL"
    ZAR = "ZAR"
    INR = "INR"
    KRW = "KRW"
    TWD = "TWD"
    XAU = "XAU"
    XAG = "XAG"
    XPT = "XPT"
    XPD = "XPD"
    XTS = "XTS"
    XXX = "XXX"


class Side(Ascii32):
    """Direction stored as a four-byte ASCII mnemonic."""

    FIX_FIELD = enum.nonmember("Side")

    UNKNOWN = 0
    """No side stated."""
    BUY = "BUY"
    """Buying and book bid."""
    BID = "BUY"
    """Alias of `BUY`."""
    BUY_MINUS = "BYMN"
    """Buy not above the last differing price."""
    BORROW = "BORR"
    """Borrowing collateral."""
    SUBSCRIBE = "SUBS"
    """Subscribing to a fund."""
    SELL = "SELL"
    """Selling and book ask."""
    ASK = "SELL"
    """Alias of `SELL`."""
    SELL_PLUS = "SLPL"
    """Sell not below the last differing price."""
    SELL_SHORT = "SHRT"
    """Selling stock not held."""
    SELL_SHORT_EXEMPT = "SHEX"
    """Exempt short sale."""
    LEND = "LEND"
    """Lending collateral."""
    REDEEM = "REDM"
    """Redeeming a fund holding."""
    CROSS = "CROS"
    """Both sides are the same participant."""
    CROSS_SHORT = "CRSH"
    """Cross with a short sell leg."""
    CROSS_SHORT_EXEMPT = "CRSE"
    """Cross with an exempt short leg."""
    AS_DEFINED = "ASDF"
    """Direction defined by the multileg instrument."""
    OPPOSITE = "OPPO"
    """Opposite of the multileg definition."""
    UNDISCLOSED = "UNDS"
    """Direction withheld."""

    @classmethod
    def _built_in_aliases(cls) -> dict[str, str]:
        return {"LONG": "BUY", "OFFER": "SELL", "SHORT": "SELL_SHORT"}

    @property
    def sign(self) -> int:
        """Return +1 buying, -1 selling or 0 otherwise."""
        if self in (Side.BUY, Side.BUY_MINUS, Side.BORROW, Side.SUBSCRIBE):
            return 1
        if self in (
            Side.SELL,
            Side.SELL_PLUS,
            Side.SELL_SHORT,
            Side.SELL_SHORT_EXEMPT,
            Side.LEND,
            Side.REDEEM,
        ):
            return -1
        return 0

    @property
    def opposite(self) -> Side:
        """Return the plain opposite; neutral sides return themselves."""
        if self.sign > 0:
            return Side.SELL
        if self.sign < 0:
            return Side.BUY
        return self


class TimeInForce(Ascii32):
    """Order lifetime stored as a ranked four-byte ASCII mnemonic."""

    FIX_FIELD = enum.nonmember("TimeInForce")

    UNKNOWN = 0, 0
    """Venue default."""
    IMMEDIATE = "IMMD", 100
    """Ordering marker for non-resting instructions."""
    IOC = "IOC", 110
    """Trade what can immediately and cancel the rest."""
    FOK = "FOK", 120
    """Trade all immediately or none."""
    SESSION = "SESS", 200
    """Ordering marker for session-valid instructions."""
    DAY = "DAY", 210
    """Good for the session."""
    AT_OPEN = "OPEN", 220
    """Opening auction only."""
    AT_CLOSE = "CLOS", 230
    """Closing auction only."""
    GTX = "GTX", 240
    """Good until crossing."""
    GOOD_THROUGH_CROSSING = "GTCR", 250
    """Valid through the next crossing phase."""
    AT_CROSSING = "ATCR", 260
    """Valid only during crossing."""
    GFA = "GFA", 270
    """Good for one auction."""
    RESTING = "REST", 300
    """Ordering marker for cross-session instructions."""
    GTC = "GTC", 310
    """Good until cancelled."""
    GTD = "GTD", 320
    """Good until `Event.eunix`."""
    GFT = "GFT", 330
    """Good for a duration resolved into `Event.eunix`."""
    GFM = "GFM", 340
    """Good for the current month."""

    @classmethod
    def _built_in_aliases(cls) -> dict[str, str]:
        """Spellings this vocabulary answers to beside its own member names.

        The five the dictionary uses that this package abbreviates are here
        because a spelling is an enum's business; the wire code each one
        reaches is the dictionary's, and is read from it.
        """
        return {
            "IMMEDIATE_OR_CANCEL": "IOC",
            "FILL_OR_KILL": "FOK",
            "GOOD_TIL_CANCELLED": "GTC",
            "GOOD_TILL_CANCELLED": "GTC",
            "GOOD_TIL_DATE": "GTD",
            "GOOD_TILL_DATE": "GTD",
            "AT_THE_OPENING": "AT_OPEN",
            "AT_THE_CLOSE": "AT_CLOSE",
            "GOOD_FOR_AUCTION": "GFA",
            "GOOD_FOR_TIME": "GFT",
            "GOOD_FOR_MONTH": "GFM",
        }

    def _rank_of(self, other: Any) -> int | Any:
        return other._rank if isinstance(other, TimeInForce) else NotImplemented

    def __lt__(self, other: Any) -> bool:
        rank = self._rank_of(other)
        return int(self) < other if rank is NotImplemented else self._rank < rank

    def __le__(self, other: Any) -> bool:
        rank = self._rank_of(other)
        return int(self) <= other if rank is NotImplemented else self._rank <= rank

    def __gt__(self, other: Any) -> bool:
        rank = self._rank_of(other)
        return int(self) > other if rank is NotImplemented else self._rank > rank

    def __ge__(self, other: Any) -> bool:
        rank = self._rank_of(other)
        return int(self) >= other if rank is NotImplemented else self._rank >= rank

    @property
    def rests(self) -> bool:
        """Whether an unfilled order remains in the book."""
        return self >= TimeInForce.SESSION
