"""Option direction."""

from rekep.enums.ranged import Ranged


class OptionKind(Ranged):
    """Option direction read from FIX `PutOrCall <201>`."""

    UNKNOWN = 0
    PUT = 100, "0"
    CALL = 200, "1"
