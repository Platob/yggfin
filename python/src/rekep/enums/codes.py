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

from rekep.enums.ascii_codes import AsciiInt32, AsciiInt64


class AssetKind(AsciiInt64):
    """Tradable asset kind, banded by settlement."""

    UNKNOWN = 0
    CASH = "CASH", "", 100
    EQUITY = "EQUITY", "E", 110
    DEBT = "DEBT", "D", 120
    FUND = "FUND", "C", 130
    CURRENCY = "CURRENCY", "T", 140
    COMMODITY = "COMMDTY", "J", 150
    INDEX = "INDEX", "M", 160
    DERIVATIVE = "DERIV", "", 200
    FUTURE = "FUTURE", "F", 210
    OPTION = "OPTION", "O", 220
    SWAP = "SWAP", "S", 230
    WARRANT = "WARRANT", "R", 240
    FORWARD = "FORWARD", "", 250
    STRUCTURED = "STRUCTD", "", 300
    SPREAD = "SPREAD", "", 310
    MULTILEG = "MULTILEG", "", 320
    BASKET = "BASKET", "", 330
    FINANCING = "FINANCE", "", 400
    REPO = "REPO", "", 410
    LOAN = "LOAN", "", 420

    @property
    def is_derivative(self) -> bool:
        """Whether derivative-specific instrument fields apply."""
        return self.rank >= AssetKind.DERIVATIVE.rank


class EventType(AsciiInt64):
    """Event kind stored as an eight-byte ASCII mnemonic, banded by rank.

    Eight bytes buy explicit spellings -- `ORDER`, `QUOTE`, `EXECUTED` --
    where four forced abbreviations. The stored value is the readable
    mnemonic; the band order the row predicates reason over rides in each
    member's rank, so a kind question compares ranks and a storage scan
    filters on the finite code sets `ranked_at_least`/`ranked_below` spell.
    """

    UNKNOWN = 0
    MISC = "MISC", "", 10
    INTENT = "INTENT", "", 100
    ORDER = "ORDER", "", 110
    QUOTE = "QUOTE", "", 120
    FACT = "FACT", "", 200
    EXECUTION = "EXECUTED", "", 210
    STATE = "STATE", "", 300
    BOOK = "BOOK", "", 320
    INSTRUMENT_STATE = "ISTATE", "", 400
    INSTRUMENT = "INSTRMT", "", 410

    @classmethod
    def _built_in_aliases(cls) -> dict[str, str]:
        """The four-byte mnemonics an earlier release stored, by what they meant.

        Eight bytes bought the explicit spellings; a store or a config still
        written in the abbreviations resolves to the same members.
        """
        return {
            "ORDR": "ORDER",
            "INTE": "INTENT",
            "QUOT": "QUOTE",
            "EXEC": "EXECUTION",
            "STAT": "STATE",
            "ISTA": "INSTRUMENT_STATE",
            "INST": "INSTRUMENT",
        }

    @property
    def is_snapshot(self) -> bool:
        """Whether the row is a state rather than an occurrence."""
        return self._rank >= EventType.STATE._rank


class IdSource(AsciiInt64):
    """Instrument identifier scheme, banded by issuer."""

    UNKNOWN = 0
    REGISTERED = "REGISTRD", "", 100
    ISIN = "ISIN", "4", 110
    CUSIP = "CUSIP", "1", 120
    SEDOL = "SEDOL", "2", 130
    COMMON = "COMMON", "G", 140
    VENDOR = "VENDOR", "", 200
    RIC = "RIC", "5", 210
    BLOOMBERG = "BLOOMBRG", "A", 220
    LOCAL = "LOCAL", "", 300
    WERTPAPIER = "WERTPAPR", "B", 310
    DUTCH = "DUTCH", "C", 320
    VALOREN = "VALOREN", "D", 330
    SICOVAM = "SICOVAM", "E", 340
    BELGIAN = "BELGIAN", "F", 350
    QUIK = "QUIK", "3", 360
    VENUE = "VENUE", "", 400
    EXCHANGE = "EXCHANGE", "8", 410
    CTA = "CTA", "9", 420
    OPRA = "OPRA", "J", 430
    CLEARING = "CLEARING", "H", 440
    MARKETPLACE = "MKTPLACE", "M", 450
    OTHER = "OTHER", "", 500
    CURRENCY = "CURRENCY", "6", 510
    COUNTRY = "COUNTRY", "7", 520
    ISDA_SPEC = "ISDASPEC", "I", 530
    ISDA_URL = "ISDAURL", "K", 540
    CREDIT_LETTER = "CRDTLTTR", "L", 550

    @property
    def is_registered(self) -> bool:
        """Whether identifiers in this scheme are globally issued."""
        return self.band is IdSource.REGISTERED


class MarketKind(AsciiInt64):
    """Order pricing and execution semantics, in stable bands."""

    UNKNOWN = 0
    MARKET = "MARKET", "", 100
    MARKET_ORDER = "MKTORDER", "", 110
    MARKET_IF_TOUCHED = "MKTIFTCH", "", 120
    MARKET_TO_LIMIT = "MKTTOLMT", "", 130
    LIMIT = "LIMIT", "", 200
    LIMIT_ORDER = "LMTORDER", "", 210
    LIMIT_ON_CLOSE = "LMTCLOSE", "", 220
    LIMIT_OR_BETTER = "LMTBETTR", "", 230
    STOP = "STOP", "", 300
    STOP_ORDER = "STPORDER", "", 310
    STOP_LIMIT = "STOPLMT", "", 320
    PEGGED = "PEGGED", "", 400
    PEGGED_ORDER = "PEGORDER", "", 410
    PREVIOUSLY_QUOTED = "PREVQUOT", "", 420
    PREVIOUSLY_INDICATED = "PREVINDC", "", 430
    EXECUTION = "EXEC", "", 500
    ORDER_STATUS = "ORDSTAT", "", 510
    TRADE = "TRADE", "", 520
    TRADE_CORRECT = "TRDCORRC", "", 530
    TRADE_CANCEL = "TRDCANCL", "", 540
    LOCKED = "LOCKED", "", 550
    RELEASED = "RELEASED", "", 560
    CLEARING = "CLEARING", "", 600
    CLEARING_HOLD = "CLRHOLD", "", 610
    RELEASED_TO_CLEARING = "RELTOCLR", "", 620
    ACTIVATION = "ACTIVATN", "", 700
    TRIGGERED = "TRIGGERD", "", 710

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


class OptionKind(AsciiInt64):
    """Option direction read from FIX `PutOrCall <201>`."""

    UNKNOWN = 0
    PUT = "PUT", "0", 100
    CALL = "CALL", "1", 200


class State(AsciiInt64):
    """Event lifecycle, ordered by completion.

    The stored value is the readable mnemonic; the completion order rides in
    each member's rank, so "still live" and "finished" are rank questions
    and a storage scan filters on the code sets `ranked_at_least`,
    `ranked_below` and `ranked_between` spell.
    """

    #: The rank the terminal states begin at.
    TERMINAL = enum.nonmember(400)

    UNKNOWN = 0
    """Nothing has been stated."""
    PENDING = "PENDING", "", 100
    """Band floor: requested but not acknowledged."""
    PENDING_NEW = "PENDNEW", "", 110
    """Awaiting first venue acknowledgement."""
    OPEN = "OPEN", "", 200
    """Band floor: live at the venue."""
    NEW = "NEW", "", 210
    """Acknowledged and working."""
    ACCEPTED = "ACCEPTED", "", 220
    """Accepted but not yet working."""
    PENDING_REPLACE = "PENDRPLC", "", 230
    """Amendment pending while the original remains live."""
    PENDING_CANCEL = "PENDCNCL", "", 240
    """Cancellation pending while the order remains live."""
    SUSPENDED = "SUSPEND", "", 250
    """Held by the venue and resumable."""
    STOPPED = "STOPPED", "", 260
    """Stopped at a price awaiting a trade."""
    PARTIAL = "PARTIAL", "", 300
    """Band floor: live and partly complete."""
    PARTIALLY_FILLED = "PARTFILL", "", 310
    """Some quantity traded; the rest remains live."""
    DONE = "DONE", "", 400
    """Band floor and first terminal state."""
    FILLED = "FILLED", "", 410
    """Every share traded."""
    DONE_FOR_DAY = "DONEDAY", "", 420
    """Over for the session."""
    CALCULATED = "CALCULTD", "", 430
    """Priced and closed by the venue."""
    CLOSED = "CLOSED", "", 500
    """Band floor: over without completion."""
    CANCELLED = "CANCELED", "", 510
    """Withdrawn before completion."""
    REPLACED = "REPLACED", "", 520
    """Superseded by an amendment."""
    EXPIRED = "EXPIRED", "", 530
    """Reached expiry while live."""
    INTERNAL_EXPIRED = "INTEXPRD", "", 540
    """Expired locally after one day without a newer observation."""
    FAILED = "FAILED", "", 600
    """Band floor: refused."""
    REJECTED = "REJECTED", "", 610
    """Refused; reason fields explain why."""
    INTERNAL_REJECTED = "INTREJCT", "", 620
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


class MIC(AsciiInt32):
    """ISO 10383 code stored as four ASCII bytes in one `int32`."""

    _PATTERN = enum.nonmember(re.compile(r"^[A-Z0-9]{4}$"))

    UNKNOWN = 0, ""
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


class Currency(AsciiInt32):
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

    @classmethod
    def _from_superseded(cls, packed: int) -> Self | None:
        """A currency an earlier generation stored, including the `CCCn` one.

        Beyond the paddings every code shares, one generation wrote three
        letters and an ASCII decimal-count digit into the fourth byte; the
        letters name the currency and the digit -- a minor-unit convention
        this enum no longer stores -- drops.
        """
        found = super()._from_superseded(packed)
        if found is not None:
            return found
        if 0 <= packed < 1 << 32:
            raw = packed.to_bytes(4, "big")
            if 0x30 <= raw[3] <= 0x39:
                try:
                    letters = raw[:3].decode("ascii")
                except UnicodeDecodeError:
                    return None
                parsed = cls._from_text(letters)
                if parsed is not cls.UNKNOWN:
                    return parsed
        return None

    UNKNOWN = 0, ""
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


class Side(AsciiInt32):
    """Direction stored as a four-byte ASCII mnemonic."""

    UNKNOWN = 0
    """No side stated."""
    BUY = "BUY", "1"
    """Buying and book bid."""
    BID = "BUY", "1"
    """Alias of `BUY`."""
    BUY_MINUS = "BYMN", "3"
    """Buy not above the last differing price."""
    BORROW = "BORR", "G"
    """Borrowing collateral."""
    SUBSCRIBE = "SUBS", "D"
    """Subscribing to a fund."""
    SELL = "SELL", "2"
    """Selling and book ask."""
    ASK = "SELL", "2"
    """Alias of `SELL`."""
    SELL_PLUS = "SLPL", "4"
    """Sell not below the last differing price."""
    SELL_SHORT = "SHRT", "5"
    """Selling stock not held."""
    SELL_SHORT_EXEMPT = "SHEX", "6"
    """Exempt short sale."""
    LEND = "LEND", "F"
    """Lending collateral."""
    REDEEM = "REDM", "E"
    """Redeeming a fund holding."""
    CROSS = "CROS", "8"
    """Both sides are the same participant."""
    CROSS_SHORT = "CRSH", "9"
    """Cross with a short sell leg."""
    CROSS_SHORT_EXEMPT = "CRSE", "A"
    """Cross with an exempt short leg."""
    AS_DEFINED = "ASDF", "B"
    """Direction defined by the multileg instrument."""
    OPPOSITE = "OPPO", "C"
    """Opposite of the multileg definition."""
    UNDISCLOSED = "UNDS", "7"
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


class TimeInForce(AsciiInt32):
    """Order lifetime stored as a ranked four-byte ASCII mnemonic."""

    UNKNOWN = 0, "", 0
    """Venue default."""
    IMMEDIATE = "IMMD", "", 100
    """Ordering marker for non-resting instructions."""
    IOC = "IOC", "3", 110
    """Trade what can immediately and cancel the rest."""
    FOK = "FOK", "4", 120
    """Trade all immediately or none."""
    SESSION = "SESS", "", 200
    """Ordering marker for session-valid instructions."""
    DAY = "DAY", "0", 210
    """Good for the session."""
    AT_OPEN = "OPEN", "2", 220
    """Opening auction only."""
    AT_CLOSE = "CLOS", "7", 230
    """Closing auction only."""
    GTX = "GTX", "5", 240
    """Good until crossing."""
    GOOD_THROUGH_CROSSING = "GTCR", "8", 250
    """Valid through the next crossing phase."""
    AT_CROSSING = "ATCR", "9", 260
    """Valid only during crossing."""
    GFA = "GFA", "B", 270
    """Good for one auction."""
    RESTING = "REST", "", 300
    """Ordering marker for cross-session instructions."""
    GTC = "GTC", "1", 310
    """Good until cancelled."""
    GTD = "GTD", "6", 320
    """Good until `Event.eunix`."""
    GFT = "GFT", "A", 330
    """Good for a duration resolved into `Event.eunix`."""
    GFM = "GFM", "C", 340
    """Good for the current month."""

    @classmethod
    def _built_in_aliases(cls) -> dict[str, str]:
        return {
            "IMMEDIATE_OR_CANCEL": "IOC",
            "FILL_OR_KILL": "FOK",
            "GOOD_TIL_CANCELLED": "GTC",
            "GOOD_TILL_CANCELLED": "GTC",
            "GOOD_TIL_DATE": "GTD",
            "GOOD_TILL_DATE": "GTD",
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
