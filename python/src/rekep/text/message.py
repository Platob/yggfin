"""One physical text record before protocol parsing."""

from __future__ import annotations

import datetime
from typing import Annotated, Any

import pyarrow
from yggdryl import TextOptions, scalar

from rekep.annotations import Self
from rekep.convert import Convertible
from rekep.fields import HOUR, derived_from, digest_key, partition_key, primary_key
from rekep.times import ULBRIDGE_ROWHEADER, datetime_of


@scalar(slots=True)
class Message(Convertible):
    """One ULBridge text line, before the FIX codec reads its body.

    Every column but `timepartition` and `bodyhash` is named for what the
    native text read already calls it, and the bridge's own captures are named
    for the FIX columns they fill -- so a stored row goes on through the codec
    without one spelling being translated into another.
    """

    sourceurl: str = ""
    """Canonical URI of the source text object, filling `sourceurl` (65026)."""

    rownum: int = 0
    """1-based physical line number within the source object."""

    timestamp: datetime.datetime | None = None
    """UTC instant captured from the line header, at microsecond resolution.

    Capture context, and only that: it is the clock the bridge stamped the
    *line* with, so it dates no message and never reaches one. What dates a
    message is what the message states -- its `TransactTime`, else its
    `SendingTime`, else the one instant the codec is pinned with -- so the
    same bytes logged at three hops settle on one instant however each line
    was stamped.
    """

    timepartition: Annotated[
        datetime.datetime | None,
        partition_key(HOUR),
        derived_from("timestamp"),
    ] = None
    """Timestamp partitioned by its UTC hour in Iceberg.

    `logs.messages` is laid out by it and `fix.messages` carries it as an
    ordinary column: a settled message is laid out by the instant it happened
    at, not by the instant a bridge printed the line.
    """

    threadId: int | None = None
    """Bridge thread identifier captured from the line header."""

    msgsessionid: str | None = None
    """Bridge session instance the line was handled on, filling `msgsessionid` (65032).

    The session *instance*, which is the bracket's own first part -- never
    what the message says about the counterparty session it names. Two
    connections to one counterparty are two instances, so they are two facts,
    and the event's own capture record holds this one.
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
        bytes,
        digest_key(["body"], dtype=pyarrow.binary(16)),
        primary_key(),
    ] = b""
    """XXH3-128 digest of the exact *body* bytes, computed beside them on the read.

    The key of `logs.messages`, and a digest of the bytes alone: identical
    bytes are one row whatever session carried them, whichever object they
    were read from and however often a capture is re-read. It is not the
    message's `hashcode`, which covers the settled event -- its facts, its
    text, its metadata and its entry tree -- and answers the same code for two
    different lines that state the same message. The bytes and the message are
    two questions, so they are two columns and neither stands in for the other.
    """

    body: bytes = b""
    """Exact bytes after the matched line-header prefix.

    `logs.messages` is where they live and the only place: `fix.messages`
    references them by `bodyhash` rather than repeating them, because a row
    there is an event and these bytes are one line's.
    """

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

    #: What a row header does not fill, because something else does: the two
    #: the traversal names, and the payload it frames.
    READ_COLUMNS = frozenset({"sourceurl", "rownum", "body"})

    @classmethod
    def captures(cls) -> frozenset[str]:
        """The columns a row header is expected to capture into this contract.

        Every member this class declares except the three the read itself
        fills and the ones a field apply derives -- and a derived member says
        so, by naming the columns it is derived from, so adding one does not
        mean remembering to exclude it here.
        """
        return frozenset(
            member.name
            for member in cls.into_field()
            if member.name not in cls.READ_COLUMNS
            and not member.partition.sources
            and not member.digest.sources
        )

    @classmethod
    def text_options(cls, rowheader: str | None = None) -> TextOptions:
        """The native text read that produces this exact contract.

        The bridge read of `rekep.fix.fix_text_options`, with this class's own
        field on it -- the two are pinned equal by `test_message.py`, because a
        capture this read frames and that one does not is a column the codec
        silently stops filling. It is spelled twice rather than imported so a
        raw text row stays readable without the dictionary behind it.

        `rowheader` reads a bridge that writes these same facts in a layout of
        its own: a different clock precision, a bracket ordered differently, a
        level this logger omits. What it may not do is rename them. The
        columns are the contract, and a capture named anything else is dropped
        in silence by the read -- a whole column of nulls and no error -- so
        the names are checked here, where the mismatch is still legible.
        """
        options = TextOptions()
        options.start_rownum = 1
        options.parse_mtime = False
        options.rowheader = ULBRIDGE_ROWHEADER if rowheader is None else rowheader
        options.timezone = "UTC"
        options.safe = False
        options.field = cls.into_field()
        if rowheader is not None:
            cls._check_captures(options)
        return options

    @classmethod
    def _check_captures(cls, options: TextOptions) -> None:
        """Refuse a header whose captures are not this contract's columns."""
        declared = cls.captures()
        found = frozenset(options.capture_names)
        if found == declared:
            return
        unknown = sorted(found - declared)
        missing = sorted(declared - found)
        said = [f"captures nothing for {', '.join(missing)}" if missing else ""]
        said += [f"captures {', '.join(unknown)}, which no column holds" if unknown else ""]
        raise ValueError(f"row header {' and '.join(part for part in said if part)}")

    @classmethod
    def from_text(cls, text: str | bytes, **declared: Any) -> Self:
        """Build one raw record without interpreting its body."""
        declared["body"] = text.encode("utf-8") if isinstance(text, str) else bytes(text)
        return cls(**declared)
