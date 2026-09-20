"""One physical text record before protocol parsing."""

from __future__ import annotations

import datetime
from typing import Annotated, Any

import pyarrow
from yggdryl import TextOptions, scalar

from rekep.annotations import Self
from rekep.convert import Convertible
from rekep.fields import (
    HOUR,
    Field,
    derived_from,
    field_options,
    partition_key,
    primary_key,
)
from rekep.times import ULBRIDGE_ROWHEADER, datetime_of


@scalar(slots=True)
class Message(Convertible):
    """One ULBridge text line, before the FIX codec reads its body.

    Every column but `timepartition` is named for what the native text read
    already calls it, and the bridge's own captures are named for the FIX
    columns they fill -- so a stored row goes on through the codec without one
    spelling being translated into another.
    """

    sourceurl: str = ""
    """Canonical URI of the source text object, filling `sourceurl` (65026)."""

    rownum: int = 0
    """1-based physical line number within the source object."""

    timestamp: datetime.datetime | None = None
    """UTC instant captured from the line header, at microsecond resolution.

    Capture context, and only that: it is the clock the bridge stamped the
    *line* with, so it dates no message and never reaches one. What dates a
    message is what the message states -- the `SendingTime` it carries, else
    the one instant the codec is pinned with, which the walk then replaces by
    the `TransactTime` it states -- so the same bytes logged at three hops
    settle on one instant however each line was stamped.
    """

    timepartition: Annotated[
        datetime.datetime | None,
        partition_key(HOUR),
        derived_from("timestamp"),
    ] = None
    """Timestamp partitioned by its UTC hour in Iceberg.

    `logs.messages` is laid out by it and the two FIX tables carry it as an
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
    """Bridge plugin that wrote the line.

    Carried in front of a FIX row under this name: the row's own column for
    the plugin is `msgpluginid` (65009), and this capture is spelled as the
    bridge's header brackets it.
    """

    level: str | None = None
    """Severity spelling captured from the line header."""

    currhashcode: Annotated[
        int,
        field_options(dtype=pyarrow.int64()),
        primary_key(),
    ] = 0
    """The line's own content code, as the native text read states it.

    The key of `logs.messages`. A text line is an event of the graph and the
    read codes its content, so identical bytes answer one code whatever
    session carried them, whichever object they were read from and however
    often a capture is re-read -- and nothing here computes a second digest
    beside the one the read already states.

    It is the line's code and not a message's. A FIX row's `currhashcode`
    covers the settled event -- its facts, its text, its metadata and its
    entry tree -- so two lines that state one message share that code and not
    this one, which is why a FIX row holds neither this column nor the bytes
    it was read from.

    A content code is unsigned and the read states it that way. Iceberg's
    only sixty-four-bit integer is signed, so this contract declares the type
    a table holds and `read_field` widens it back for the read alone: the same
    eight bytes cross the boundary through
    `rekep.fields.stored_arrow_reader`, and half of them read back negative.
    """

    body: bytes = b""
    """The whole line as the native read retains it, the row header included.

    The captures above are read off it, not cut out of it. `logs.messages` is
    where the bytes live and the only place, and so is the code beside them:
    neither FIX table holds either, because a row there is an event and both
    of these are one line's. It names the line it was read from instead, with
    `sourceurl`, `rownum` and, exactly, the line's `curruuid`.
    """

    curruuid: Annotated[bytes | None, field_options(dtype=pyarrow.binary(16))] = None
    """The line's own identity, as the native text read states it.

    A text line is an event of the graph, and the read stamps every row with
    the sixteen columns an event opens with; this is the one of them a
    message keeps. A message parsed out of a stored line names it here and
    nowhere else -- as its `srcuuids`, exact provenance rather than an
    identity recomputed from the bytes -- so a FIX row joins the line it was
    read from on this column, whatever the line was stamped with. Not the
    key: identical bytes are one row on `currhashcode`, and the identity a
    table keeps is the line that landed. Sixteen ordered bytes rather than the
    `uuid` Iceberg would store, for the reason the FIX row gives.

    Declared last and nullable, because a table that already exists takes a
    new column only at its end and only as an optional one: an Iceberg schema
    cannot add a required column to rows that never held it. The read fills
    it on every row; a row that states none -- one made by hand, or one a
    table held before this column -- leaves the parse to recompute the
    identity from the line's bytes and instant, which is equal only while
    those are.
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
        if self.curruuid is not None and not isinstance(self.curruuid, bytes):
            self.curruuid = bytes(self.curruuid)

    #: What a row header does not fill, because something else does: the two
    #: the traversal names, the payload it frames, and the identity and code
    #: the read states over the line.
    READ_COLUMNS = frozenset({"sourceurl", "rownum", "body", "curruuid", "currhashcode"})

    @classmethod
    def captures(cls) -> frozenset[str]:
        """The columns a row header is expected to capture into this contract.

        Every member this class declares except the five the read itself
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
    def read_field(cls) -> Field:
        """This contract as the read states it, rather than as a table holds it.

        One column apart: a content code is unsigned and the read answers it
        that way, while Iceberg's only sixty-four-bit integer is signed. The
        read is given the wide type, because a code above 2**63 has no `int64`
        to be cast to and the native apply refuses it rather than wrapping;
        `rekep.fields.stored_arrow_reader` views the same eight bytes back
        into the stored type on the way to the table.
        """
        declared = cls.into_field().into_arrow_schema()
        return Field.from_arrow_schema(
            pyarrow.schema(
                [
                    member.with_type(pyarrow.uint64()) if member.name == "currhashcode" else member
                    for member in declared
                ],
                metadata=declared.metadata,
            ),
            name=cls.__name__,
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
        options.field = cls.read_field()
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
