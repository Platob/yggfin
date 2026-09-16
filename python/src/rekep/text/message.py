"""One physical text record before protocol parsing."""

from __future__ import annotations

import datetime
from typing import Annotated, Any

import pyarrow
from yggdryl import TextOptions, scalar

from rekep.annotations import Self
from rekep.convert import Convertible
from rekep.fields import derived_from, digest_key, partition_key, primary_key
from rekep.times import ULBRIDGE_ROWHEADER, datetime_of


@scalar(slots=True)
class Message(Convertible):
    """One ULBridge text line, before the FIX codec reads its body.

    Every column but `timepartition` and `bodyhash` is named for what the
    native text read already calls it, and the bridge's own captures are named
    for the FIX columns they fill -- so a stored row goes on through the codec
    without one spelling being translated into another.
    """

    sourceurl: Annotated[str, primary_key()] = ""
    """Canonical URI of the source text object, filling `sourceurl` (65026)."""

    rownum: Annotated[int, primary_key()] = 0
    """1-based physical line number within the source object."""

    timestamp: datetime.datetime | None = None
    """UTC instant captured from the line header, at microsecond resolution.

    Capture context, and only that: it is the clock the bridge stamped the
    *line* with, so it dates no message on its own. What dates a message is
    stated by the FIX seam, which offers this instant as the `SendingTime` a
    message carrying none of its own takes.
    """

    timepartition: Annotated[
        datetime.datetime | None,
        partition_key("hour"),
        derived_from("timestamp"),
    ] = None
    """Timestamp partitioned by its UTC hour in Iceberg."""

    threadId: int | None = None
    """Bridge thread identifier captured from the line header."""

    bridgesessionid: str | None = None
    """Bridge session instance the line was handled on, filling 65032.

    The session *instance*, which is the bracket's own first part -- never
    `sendersessionid` (65007), which is what a bridge row spells for the
    counterparty session the message names. Two connections to one
    counterparty are two instances, so they are two facts.
    """

    msgctxid: str | None = None
    """Bridge message-context identifier, filling `msgctxid` (65008)."""

    msgseqnum: int | None = None
    """Bridge sequence number, filling `MsgSeqNum` (34) where a frame stated none."""

    pluginid: str | None = None
    """Bridge plugin that wrote the line, filling `pluginid` (65009)."""

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
        self.sourceurl = str(self.sourceurl)
        self.rownum = int(self.rownum)
        if self.timestamp is not None:
            timestamp = datetime_of(self.timestamp)
            if timestamp is None:
                raise ValueError(f"timestamp={self.timestamp!r} is not an instant")
            self.timestamp = timestamp
        if self.threadId is not None:
            self.threadId = int(self.threadId)
        if self.msgseqnum is not None:
            self.msgseqnum = int(self.msgseqnum)
        if isinstance(self.body, str):
            self.body = self.body.encode("utf-8")
        elif not isinstance(self.body, bytes):
            self.body = bytes(self.body)

    @classmethod
    def text_options(cls) -> TextOptions:
        """The native text read that produces this exact contract.

        The bridge read of `rekep.fix.fix_text_options`, with this class's own
        field on it -- the two are pinned equal by `test_message.py`, because a
        capture this read frames and that one does not is a column the codec
        silently stops filling. It is spelled twice rather than imported so a
        raw text row stays readable without the dictionary behind it.
        """
        options = TextOptions()
        options.start_rownum = 1
        options.parse_mtime = False
        options.rowheader = ULBRIDGE_ROWHEADER
        options.timezone = "UTC"
        options.safe = False
        options.field = cls.into_field()
        return options

    @classmethod
    def from_text(cls, text: str | bytes, **declared: Any) -> Self:
        """Build one raw record without interpreting its body."""
        declared["body"] = text.encode("utf-8") if isinstance(text, str) else bytes(text)
        return cls(**declared)
