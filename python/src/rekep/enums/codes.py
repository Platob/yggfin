"""Every stable protocol and market code, over the two bases.

One module rather than one per enum: they share nothing but their base, none
of them refers to another, and a reader looking for the spelling of a code
should not have to guess which file it is in.
"""

from __future__ import annotations

import enum
import functools
import json
import re
from collections import OrderedDict
from collections.abc import Iterable
from typing import Any, Self

from rekep.enums.ranged import (
    _ASCII_ALIASES,
    _ASCII_REGISTERED,
    _ASCII_REGISTERED_LIMIT,
    Ranged,
    _AsciiInt32,
    _FixedAsciiInt32,
)


class AssetKind(Ranged):
    """Tradable asset kind banded by settlement."""

    UNKNOWN = 0
    CASH = 100
    EQUITY = 110, "E"
    DEBT = 120, "D"
    FUND = 130, "C"
    CURRENCY = 140, "T"
    COMMODITY = 150, "J"
    INDEX = 160, "M"
    DERIVATIVE = 200
    FUTURE = 210, "F"
    OPTION = 220, "O"
    SWAP = 230, "S"
    WARRANT = 240, "R"
    FORWARD = 250
    STRUCTURED = 300
    SPREAD = 310
    MULTILEG = 320
    BASKET = 330
    FINANCING = 400
    REPO = 410
    LOAN = 420

    @property
    def is_derivative(self) -> bool:
        """Whether derivative-specific instrument fields apply."""
        return self >= AssetKind.DERIVATIVE


class EventType(Ranged):
    """Event kind banded by what the row asserts."""

    UNKNOWN = 0
    INTENT = 100
    ORDER = 110
    QUOTE = 120
    FACT = 200
    EXECUTION = 210
    STATE = 300
    BOOK = 320
    INSTRUMENT_STATE = 400
    INSTRUMENT = 410

    @property
    def is_snapshot(self) -> bool:
        """Whether the row is a state rather than an occurrence."""
        return self >= EventType.STATE


class IdSource(Ranged):
    """Instrument identifier scheme banded by issuer."""

    UNKNOWN = 0
    REGISTERED = 100
    ISIN = 110, "4"
    CUSIP = 120, "1"
    SEDOL = 130, "2"
    COMMON = 140, "G"
    VENDOR = 200
    RIC = 210, "5"
    BLOOMBERG = 220, "A"
    LOCAL = 300
    WERTPAPIER = 310, "B"
    DUTCH = 320, "C"
    VALOREN = 330, "D"
    SICOVAM = 340, "E"
    BELGIAN = 350, "F"
    QUIK = 360, "3"
    VENUE = 400
    EXCHANGE = 410, "8"
    CTA = 420, "9"
    OPRA = 430, "J"
    CLEARING = 440, "H"
    MARKETPLACE = 450, "M"
    OTHER = 500
    CURRENCY = 510, "6"
    COUNTRY = 520, "7"
    ISDA_SPEC = 530, "I"
    ISDA_URL = 540, "K"
    CREDIT_LETTER = 550, "L"

    @property
    def is_registered(self) -> bool:
        """Whether identifiers in this scheme are globally issued."""
        return self.band == IdSource.REGISTERED


class MarketKind(Ranged):
    """Order pricing and execution semantics in stable bands."""

    UNKNOWN = 0
    MARKET = 100
    MARKET_ORDER = 110
    MARKET_IF_TOUCHED = 120
    MARKET_TO_LIMIT = 130
    LIMIT = 200
    LIMIT_ORDER = 210
    LIMIT_ON_CLOSE = 220
    LIMIT_OR_BETTER = 230
    STOP = 300
    STOP_ORDER = 310
    STOP_LIMIT = 320
    PEGGED = 400
    PEGGED_ORDER = 410
    PREVIOUSLY_QUOTED = 420
    PREVIOUSLY_INDICATED = 430
    EXECUTION = 500
    ORDER_STATUS = 510
    TRADE = 520
    TRADE_CORRECT = 530
    TRADE_CANCEL = 540
    LOCKED = 550
    RELEASED = 560
    CLEARING = 600
    CLEARING_HOLD = 610
    RELEASED_TO_CLEARING = 620
    ACTIVATION = 700
    TRIGGERED = 710

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


class OptionKind(Ranged):
    """Option direction read from FIX `PutOrCall <201>`."""

    UNKNOWN = 0
    PUT = 100, "0"
    CALL = 200, "1"


class State(Ranged):
    """Event lifecycle ordered by completion."""

    TERMINAL = enum.nonmember(400)

    UNKNOWN = 0
    """Nothing has been stated."""
    PENDING = 100
    """Band floor: requested but not acknowledged."""
    PENDING_NEW = 110
    """Awaiting first venue acknowledgement."""
    OPEN = 200
    """Band floor: live at the venue."""
    NEW = 210
    """Acknowledged and working."""
    ACCEPTED = 220
    """Accepted but not yet working."""
    PENDING_REPLACE = 230
    """Amendment pending while the original remains live."""
    PENDING_CANCEL = 240
    """Cancellation pending while the order remains live."""
    SUSPENDED = 250
    """Held by the venue and resumable."""
    STOPPED = 260
    """Stopped at a price awaiting a trade."""
    PARTIAL = 300
    """Band floor: live and partly complete."""
    PARTIALLY_FILLED = 310
    """Some quantity traded; the rest remains live."""
    DONE = 400
    """Band floor and first terminal state."""
    FILLED = 410
    """Every share traded."""
    DONE_FOR_DAY = 420
    """Over for the session."""
    CALCULATED = 430
    """Priced and closed by the venue."""
    CLOSED = 500
    """Band floor: over without completion."""
    CANCELLED = 510
    """Withdrawn before completion."""
    REPLACED = 520
    """Superseded by an amendment."""
    EXPIRED = 530
    """Reached expiry while live."""
    INTERNAL_EXPIRED = 540
    """Expired locally after one day without a newer observation."""
    FAILED = 600
    """Band floor: refused."""
    REJECTED = 610
    """Refused; reason fields explain why."""
    INTERNAL_REJECTED = 620
    """Refused by this pipeline before it could change market state."""

    @property
    def is_live(self) -> bool:
        """Whether the event is working at the venue."""
        return State.OPEN <= self < State.TERMINAL

    @property
    def is_terminal(self) -> bool:
        """Whether no further lifecycle transition is expected."""
        return self >= State.TERMINAL


class MIC(_AsciiInt32):
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


class Currency(_AsciiInt32):
    """Three uppercase letters plus one ASCII decimal-count digit."""

    _PATTERN = enum.nonmember(re.compile(r"^[A-Z]{3}[0-9]$"))

    def __new__(cls, value: int | str, decimals: int = 0) -> Self:
        if not isinstance(value, str):
            member = int.__new__(cls, int(value))
            member._value_ = int(value)
            member._code = ""
            member._packed_code = ""
            member._decimals = 0
            member._fix_code = ""
            member._rank = int(value)
            return member
        raw = value.strip().upper()
        code = raw[:3]
        count = int(raw[3]) if len(raw) == 4 and raw[3].isdigit() else int(decimals)
        text = f"{code}{count}"
        packed = cls._pack(text)
        member = int.__new__(cls, packed)
        member._value_ = packed
        member._code = code
        member._packed_code = text
        member._decimals = count
        member._fix_code = code
        member._rank = packed
        return member

    @property
    def decimals(self) -> int:
        """Decimal count encoded by the fourth ASCII digit."""
        return self._decimals

    @property
    def packed_code(self) -> str:
        """Four ASCII characters written into the `int32`."""
        return self._packed_code

    @classmethod
    def from_str(cls, value: Any, decimals: int | None = None) -> Self:
        """Parse `CCC`/`CCCn`; an omitted decimal count is zero."""
        if isinstance(value, cls):
            return value
        if isinstance(value, int):
            return cls.from_code(value)
        raw = str(value) if value is not None else ""
        return cls._from_text(raw) if decimals is None else cls.register(raw, decimals=decimals)

    @classmethod
    @functools.lru_cache(maxsize=4_096)
    def _from_text(cls, raw: str) -> Self:
        return cls.register(raw)

    @classmethod
    def register(
        cls, value: str, *, decimals: int | None = None, aliases: Iterable[str] = ()
    ) -> Self:
        """Register one `CCCn` value and optional source aliases."""
        raw = cls._normalise(value)
        alias_map = {**cls._built_in_aliases(), **_ASCII_ALIASES.get(cls, {})}
        raw = alias_map.get(raw, raw)
        named = cls.__members__.get(raw)
        if named is not None:
            raw = named.packed_code
        if len(raw) == 3:
            count = 0 if decimals is None else decimals
            text = f"{raw}{count}"
        elif len(raw) == 4 and raw[3].isdigit():
            count = int(raw[3]) if decimals is None else decimals
            text = f"{raw[:3]}{count}"
        else:
            return cls.UNKNOWN
        if not isinstance(count, int) or not 0 <= count <= 9 or not cls._PATTERN.fullmatch(text):
            return cls.UNKNOWN
        packed = cls._pack(text)
        registered = _ASCII_REGISTERED.setdefault(cls, OrderedDict())
        known = cls._value2member_map_.get(packed) or registered.get(packed)
        if known is None:
            known = int.__new__(cls, packed)
            known._name_ = text
            known._value_ = packed
            known._code = text[:3]
            known._packed_code = text
            known._decimals = count
            known._fix_code = text[:3]
            known._rank = packed
            registered[packed] = known
            registered.move_to_end(packed)
            if len(registered) > _ASCII_REGISTERED_LIMIT:
                registered.popitem(last=False)
        if aliases:
            configured = _ASCII_ALIASES.setdefault(cls, {})
            configured.update({cls._normalise(alias): text for alias in aliases})
            cls._from_text.cache_clear()
        return known

    @classmethod
    def from_code(cls, value: Any, default: Self | None = None) -> Self:
        """Decode `CCCn`, returning `UNKNOWN` for malformed values."""
        try:
            packed = int(value)
        except (TypeError, ValueError):
            return default if default is not None else cls.UNKNOWN
        registered = _ASCII_REGISTERED.setdefault(cls, OrderedDict())
        known = cls._value2member_map_.get(packed) or registered.get(packed)
        if known is not None:
            return known
        if packed < 0 or packed >= 1 << 31:
            return default if default is not None else cls.UNKNOWN
        try:
            text = packed.to_bytes(4, "big").decode("ascii")
        except (OverflowError, UnicodeDecodeError):
            return default if default is not None else cls.UNKNOWN
        parsed = cls.register(text)
        return default if parsed is cls.UNKNOWN and default is not None else parsed

    @classmethod
    def _built_in_aliases(cls) -> dict[str, str]:
        return {
            "$": "USD0",
            "US$": "USD0",
            "\u20ac": "EUR0",
            "\u00a3": "GBP0",
            "\u00a5": "JPY0",
        }

    @classmethod
    def schema_metadata(cls) -> dict[str, str]:
        return {
            "encoding": "ascii-currency-decimals",
            "byte_width": "4",
            "layout": "CCCn",
            "decimal_byte": "ascii-digit",
            "aliases": json.dumps(cls._built_in_aliases(), separators=(",", ":"), sort_keys=True),
        }

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


class Side(_FixedAsciiInt32):
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


class TimeInForce(_FixedAsciiInt32):
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
