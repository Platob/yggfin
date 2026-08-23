"""Generic financial lifecycle state."""

from __future__ import annotations

import enum

from rekep.enums.ranged import Ranged


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
