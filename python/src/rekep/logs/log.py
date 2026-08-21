"""The shape of one parsed log line."""

from __future__ import annotations

import datetime
from typing import Annotated

import pyarrow

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
    # downstream that is picky about time units. Part of the key beside
    # `h64`: the hash alone identifies a line, but a key that leads with
    # time is one an engine can prune on, since it correlates with the
    # partition.
    recorded_at_unix: Annotated[
        int, Field.primary_key(metadata={"unit": "nanosecond", "epoch": "1970-01-01"})
    ]
    """Timestamp as whole nanoseconds since the epoch, in the log's own zone."""

    # Derived from `recorded_at_unix`, denormalised so a store partitions on a
    # real date column instead of every reader re-deriving one.
    recorded_at_date: Annotated[datetime.date, Field.partition_key()]
    """Calendar day of the timestamp, naive UTC."""

    recorded_at_time: datetime.time
    """Time of day of the timestamp, naive UTC."""

    thread_name: str
    """Contents of the first bracketed field."""

    driver_name: str
    """Contents of the second bracketed field -- the emitting module."""

    # Placeholders the parser fills with zero and empty text, NOT NULL on
    # purpose: categorisation happens downstream, and a store must never have
    # to widen the column when it does.
    category_id: Annotated[int, Field(arrow_type=pyarrow.int32())]
    """Numeric message category; 0 until a categoriser assigns one."""

    category_name: str
    """Message category label; empty until a categoriser assigns one."""

    message: str
    """Payload with the header and level stripped, continuation lines folded in."""

    h64: Annotated[int, Field.primary_key()]
    """Signed xxh3-64 of the raw line, for matching lines across captures."""
