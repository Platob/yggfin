"""Standardized market-event semantics."""

from __future__ import annotations

from typing import Any, Self

from rekep.enums.ranged import Ranged


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
