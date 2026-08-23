"""Currency code with an encoded decimal count."""

from __future__ import annotations

import enum
import functools
import json
import re
from collections import OrderedDict
from collections.abc import Iterable
from typing import Any, Self

from rekep.enums._ascii import (
    _ASCII_ALIASES,
    _ASCII_REGISTERED,
    _ASCII_REGISTERED_LIMIT,
    _AsciiInt32,
)


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
