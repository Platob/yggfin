"""Market event category."""

from rekep.enums.ranged import Ranged


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
