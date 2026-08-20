"""The shape of one parsed log line."""

from __future__ import annotations

import datetime
from typing import Annotated

from rekep.convert import Convertible
from rekep.fields import Field, field


@field
class Log(Convertible):
    """One parsed line of a trading log."""

    # Repeated per row rather than held once per batch, so batches from many
    # logs can be concatenated and still say where each row came from.
    url: str
    """Path of the log the line came from, as its filesystem addresses it."""

    # An integer rather than a timestamp type, so the column survives any
    # downstream that is picky about time units.
    unix: Annotated[int, Field(metadata={"unit": "nanosecond", "epoch": "1970-01-01"})]
    """Timestamp as whole nanoseconds since the epoch, in the log's own zone."""

    # Derived from `unix`, denormalised so a reader can filter on a real date
    # column instead of re-deriving one per query.
    date: datetime.date
    """Calendar day of the timestamp, naive UTC."""

    time: datetime.time
    """Time of day of the timestamp, naive UTC."""

    thread_name: str
    """Contents of the first bracketed field."""

    driver: str
    """Contents of the second bracketed field -- the emitting module."""

    message: str
    """Payload with the header and level stripped, continuation lines folded in."""

    hash64: int
    """Signed 64-bit hash of the raw line, for matching lines across captures."""
