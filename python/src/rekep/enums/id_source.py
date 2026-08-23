"""Instrument identifier scheme."""

from rekep.enums.ranged import Ranged


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
