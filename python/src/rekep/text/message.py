"""One physical text record before protocol parsing."""

from __future__ import annotations

import datetime
from typing import Annotated, Any, Self

import pyarrow
from yggdryl import TextOptions, scalar

from rekep.convert import Convertible
from rekep.fields import derived_from, digest_key, partition_key, primary_key
from rekep.times import MESSAGE_HEADER, datetime_of


@scalar(slots=True)
class Message(Convertible):
    """One ULBridge text line, before the FIX codec reads its body."""

    url: Annotated[str, primary_key()] = ""
    """Canonical URI of the source text object."""

    rownum: Annotated[int, primary_key()] = 0
    """1-based physical line number within the source object."""

    timestamp: datetime.datetime | None = None
    """UTC instant captured from the line header, at microsecond resolution."""

    timepartition: Annotated[
        datetime.datetime | None,
        partition_key("hour"),
        derived_from("timestamp"),
    ] = None
    """Timestamp partitioned by its UTC hour in Iceberg."""

    threadId: int | None = None
    """Bridge thread identifier captured from the line header."""

    sessionUid: str | None = None
    """Bridge session identifier captured from a message-context header."""

    msgCtxId: str | None = None
    """Bridge message-context identifier captured from the line header."""

    seqNum: int | None = None
    """Bridge sequence number captured from a message-context header."""

    pluginid: str | None = None
    """Bridge plugin that logged the line, and the dialect it is read under."""

    level: str | None = None
    """Severity spelling captured from the line header."""

    bodyhash: Annotated[
        bytes | None,
        digest_key(["body"], dtype=pyarrow.binary(16)),
    ] = None
    """XXH3-128 digest of the exact body bytes, filled during field apply."""

    body: bytes = b""
    """Exact bytes after the matched line-header prefix."""

    def __post_init__(self) -> None:
        """Normalize the raw scalar values once."""
        self.url = str(self.url)
        self.rownum = int(self.rownum)
        if self.timestamp is not None:
            timestamp = datetime_of(self.timestamp)
            if timestamp is None:
                raise ValueError(f"timestamp={self.timestamp!r} is not an instant")
            self.timestamp = timestamp
        if self.threadId is not None:
            self.threadId = int(self.threadId)
        if self.seqNum is not None:
            self.seqNum = int(self.seqNum)
        if isinstance(self.body, str):
            self.body = self.body.encode("utf-8")
        elif not isinstance(self.body, bytes):
            self.body = bytes(self.body)

    @classmethod
    def text_options(cls) -> TextOptions:
        """The native text read that produces this exact contract."""
        options = TextOptions()
        options.start_rownum = 1
        options.parse_mtime = False
        options.rowheader = MESSAGE_HEADER
        options.timezone = "UTC"
        options.safe = False
        options.field = cls.into_field()
        return options

    @classmethod
    def from_text(cls, text: str | bytes, **declared: Any) -> Self:
        """Build one raw record without interpreting its body."""
        declared["body"] = text.encode("utf-8") if isinstance(text, str) else bytes(text)
        return cls(**declared)
