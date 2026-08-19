"""The shape of one parsed log line."""

from __future__ import annotations

import datetime
from typing import Annotated

from rekep.records import Arrow, Record, record


@record
class Log(Record):
    """One parsed line of a trading log."""

    # Repeated per row rather than held once per batch, so batches from many
    # logs can be concatenated and still say where each row came from.
    url: str
    """Path of the log the line came from, as its filesystem addresses it."""

    # An integer rather than a timestamp type, so the column survives any
    # downstream that is picky about time units.
    unix: Annotated[int, Arrow(metadata={"unit": "nanosecond", "epoch": "1970-01-01"})]
    """Timestamp as whole nanoseconds since the epoch, naive UTC."""

    # Derived from `unix`, denormalised so the lake partitions on a real date
    # column instead of every reader re-deriving one.
    date: Annotated[datetime.date, Arrow(partition=True)]
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
