"""Tradable asset classification."""

from rekep.enums.ranged import Ranged


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
