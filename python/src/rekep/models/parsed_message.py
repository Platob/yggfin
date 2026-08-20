"""The shape of one message parsed as pipe-delimited key=value pairs."""

from __future__ import annotations

import dataclasses
import datetime
from typing import Annotated

from rekep.records import Arrow, Record, record


@record
class ParsedMessage(Record):
    """One log line's message, parsed as `|`-delimited `key=value` segments.

    Built for FIX-shaped payloads (`8=FIX.4.4|9=112|35=D|...`), but the
    parser behind it is generic: any pipe-separated run of `key=value`
    segments decodes the same way, a leading `#` stripped from the key.
    `protocol` is a small enrichment on top -- the `8=` tag (FIX's
    BeginString) pulled out as its own column when the message carries one.
    """

    url: str
    """Path of the log the line came from."""

    unix: Annotated[int, Arrow(metadata={"unit": "nanosecond", "epoch": "1970-01-01"})]
    """Timestamp as whole nanoseconds since the epoch, naive UTC."""

    date: Annotated[datetime.date, Arrow(partition=True)]
    """Calendar day of the timestamp -- the lake partitions on it."""

    hash64: Annotated[int, Arrow(key=True)]
    """Signed 64-bit hash of the raw message; the primary key, upsert-stable
    across reruns of the same file."""

    protocol: str | None = None
    """Value of the `8=` tag, when the message opens with one."""

    fields: dict[str, str] = dataclasses.field(default_factory=dict)
    """Every `key=value` segment, `#`-stripped from the key; empty when the
    message is not pipe/key-value shaped."""
