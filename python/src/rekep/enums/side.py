"""Market direction code."""

from __future__ import annotations

from rekep.enums._ascii import _FixedAsciiInt32


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
