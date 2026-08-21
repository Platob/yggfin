"""The shape of one parsed log line."""

from __future__ import annotations

import datetime
from typing import Annotated

import pyarrow

from rekep.convert import Convertible
from rekep.fields import Field, field
from rekep.ids import HASH_BITS, TIME_BITS


@field
class Log(Convertible):
    """One parsed line of a trading log."""

    # The row's identity, and the only key anything downstream needs: the
    # millisecond in the high 42 bits, the hash of the raw line folded into the
    # low 21, sign bit clear. One integer that sorts by time, so it is also the
    # sort column a range predicate prunes on and the watermark an incremental
    # load carries -- see `rekep.ids`.
    id: Annotated[
        int,
        Field.primary_key(
            metadata={
                "unit": "millisecond",
                "epoch": "1970-01-01",
                "time_bits": str(TIME_BITS),
                "hash_bits": str(HASH_BITS),
            }
        ),
    ]
    """Sortable row id: the millisecond, then the hash of the raw line."""

    # Repeated per row rather than held once per batch, so batches from many
    # logs can be concatenated and still say where each row came from.
    url: str
    """Path of the log the line came from, as its filesystem addresses it."""

    # An integer rather than a timestamp type, so the column survives any
    # downstream that is picky about time units. Not a key: `id` carries the
    # same millisecond in its high bits and a tiebreak below it, so a key on
    # this column would only be the same order, one column wider.
    recorded_at_unix: Annotated[int, Field(metadata={"unit": "nanosecond", "epoch": "1970-01-01"})]
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

    # The whole digest, kept beside the id: the id holds 21 folded bits of it,
    # which is enough to order and to dedup, and not enough to prove two lines
    # from two captures are the same line. This column is what proves it.
    hash64: int
    """Signed xxh3-64 of the raw line, the hash the id folds into its low bits."""
