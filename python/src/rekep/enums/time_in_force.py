"""Order lifetime code."""

from __future__ import annotations

from typing import Any

from rekep.enums._ascii import _FixedAsciiInt32


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
