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
    field_options,
    partition_key,
    primary_key,
)
from rekep.times import EPOCH, ULBRIDGE_ROWHEADER, datetime_of

#: What bytes that are not UTF-8 read as, and the one place it is spelled: the
#: C1 range as windows-1252 fills it, and the five bytes that standard leaves
#: undefined as the control they stand for. `latin-1` decodes every other byte
#: to itself, so the two together are the whole mapping.
_WINDOWS_1252 = str.maketrans(
    {
        code: bytes([code]).decode("cp1252") if code not in (0x81, 0x8D, 0x8F, 0x90, 0x9D) else code
        for code in range(0x80, 0xA0)
    }
)


def decoded(raw: bytes) -> str:
    """`raw` as the native text read decodes a record: UTF-8, and windows-1252
    for each run of bytes that is not.

    One line is not written in one encoding -- a bridge relays what a venue
    sent it -- so the fallback is per run and not per record: a record holding
    both answers both, and nothing is refused or filled with replacement
    characters. A row built by hand out of the same bytes is therefore the row
    the read would have answered, which is the whole point of building one.
    """
    read: list[str] = []
    index = 0
    while index < len(raw):
        try:
            read.append(raw[index:].decode("utf-8"))
            break
        except UnicodeDecodeError as unexplained:
            read.append(raw[index : index + unexplained.start].decode("utf-8"))
            run = raw[index + unexplained.start : index + unexplained.end]
            read.append(run.decode("latin-1").translate(_WINDOWS_1252))
            index += unexplained.end
    return "".join(read)


@scalar(slots=True)
class Message(Convertible):
    """One ULBridge text line, before the FIX codec reads its body.

    Every column is one the native text read already states, under the name
    the read states it with: the event it settles over the line, the two
    columns its traversal names, the line itself, and the bridge's own
    captures. Four of those are named for the fields a parse fills from them,
    so a stored row goes on through the codec without one spelling being
    translated into another; the two the graph names nothing for --
    `msgthreadid` and `loglevel` -- take the same spelling anyway, because a
    column is read by what it holds and not by where it ends up. Nothing here
    is a second reading of a fact the read has already settled.
    """

    currunix: Annotated[datetime.datetime, partition_key(HOUR)] = EPOCH
    """When the line happened, as the native text read settles it.

    The event clock of the graph, and what `logs.messages` is laid out by:
    the hour of it and nothing beside it, exactly as both FIX tables are laid
    out by the hour of theirs. The row already carries the instant, so a
    materialized copy of it would be a second owner of one fact.

    It is the clock the bridge stamped the *line* with, read off the header's
    `mtime` capture, so it dates no message and never reaches one. What dates
    a message is what the message states -- the `SendingTime` it carries,
    else the one instant the codec is pinned with, which the walk then
    replaces by the `TransactTime` it states -- so the same bytes logged at
    three hops settle on one instant however each line was stamped.

    A line the header could not date settles at `EPOCH`, the same pin an
    undated message takes in `fix.bronze`: one hour of one partition, read
    back into every window, and the same instant on every re-read of those
    bytes -- which is what keeps the identity below a replay answers again.
    """

    curruuid: Annotated[
        bytes,
        field_options(dtype=pyarrow.binary(16)),
        primary_key(),
    ] = bytes(16)
    """The line's own identity, as the native text read states it.

    A text line is an event of the graph, and the read stamps every row with
    the columns an event opens with; this is the one of them a message keeps.
    A message parsed out of a stored line names it here and nowhere else --
    as its `srcuuids`, exact provenance rather than an identity recomputed
    from the bytes -- so a FIX row joins the line it was read from on this
    column, whatever the line was stamped with.

    The only key of `logs.messages`; `currhashcode` remains content metadata.
    Held as sixteen ordered bytes rather than the `uuid` Iceberg would store,
    for the reason the FIX row gives. A row made by hand rather than by the
    read states the sixteen zero bytes, which name no line.
    """

    currhashcode: Annotated[
        int,
        field_options(dtype=pyarrow.int64()),
    ] = 0
    """The line's own content code, as the native text read states it.

    A text line is an event of the graph and the read codes its content, so
    identical bytes answer one code -- and nothing here computes a second
    digest beside the one the read already states. It is content metadata,
    not table identity; `curruuid` is the only key.

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

    sourceurl: str = ""
    """Canonical URI of the source text object, stored only on the raw line."""

    rownum: int = 0
    """1-based physical line number within the source object."""

    body: str = ""
    """The whole line as the native read retains it, the row header included.

    Text, because that is what the read decoded it to. The captures below are
    read off it, not cut out of it. `logs.messages` is where the line lives
    and the only place, and so is the code beside it: neither FIX table holds
    either, because a row there is an event and both of these are one line's.
    A FIX row names the raw line's `curruuid` in `srcuuids`; capture details
    are looked up on that raw identity.
    """

    msgthreadid: int | None = None
    """Bridge thread the line was handled on, captured from its header.

    The graph names no thread and neither does the dictionary, so this column
    and `loglevel` stay on `logs.messages` and reach a FIX row only through
    `srcuuids`. They are spelled like the bracket's other parts regardless:
    what a column holds is what names it.
    """

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

    msgpluginid: str | None = None
    """Bridge plugin that wrote the line, filling `msgpluginid` (65009).

    The crate's own field: the plugin that logged the line inside a bridge,
    as the bridge names it. A parse reads it off the line under this name and
    no other, so a header capturing it as anything else leaves the column
    empty on every FIX row it produces.
    """

    loglevel: str | None = None
    """Severity the bridge logged the line at, as its header spells it."""

    def __post_init__(self) -> None:
        """Normalize the raw scalar values once."""
        self.sourceurl = str(self.sourceurl)
        self.rownum = int(self.rownum)
        currunix = datetime_of(self.currunix)
        if currunix is None:
            raise ValueError(f"currunix={self.currunix!r} is not an instant")
        self.currunix = currunix
        if not isinstance(self.curruuid, bytes):
            self.curruuid = bytes(self.curruuid)
        if self.msgthreadid is not None:
            self.msgthreadid = int(self.msgthreadid)
        if self.msgseqnum is not None:
            self.msgseqnum = int(self.msgseqnum)
        if isinstance(self.body, (bytes, bytearray, memoryview)):
            self.body = decoded(bytes(self.body))
        elif not isinstance(self.body, str):
            self.body = str(self.body)

    #: The capture that fills no column of its own: the record clock, which
    #: the read settles `currunix` from rather than storing twice. A header
    #: names it `mtime` because that is the column the native read fills with
    #: it, and this contract keeps the settled instant alone.
    RECORD_CLOCK = "mtime"

    #: What a row header does not fill, because something else does: the two
    #: the traversal names, the payload it frames, and the event the read
    #: settles over the line.
    READ_COLUMNS = frozenset(
        {"sourceurl", "rownum", "body", "currunix", "curruuid", "currhashcode"}
    )

    @classmethod
    def captures(cls) -> frozenset[str]:
        """The columns a row header is expected to capture into this contract.

        Every member this class declares except the ones the read itself
        settles and the ones a field apply derives -- and a derived member
        says so, by naming the columns it is derived from, so adding one does
        not mean remembering to exclude it here. The record clock is the one
        capture no column holds, because the column it fills is the settled
        `currunix` and not a second copy of the reading.
        """
        return frozenset(
            {cls.RECORD_CLOCK}
            | {
                member.name
                for member in cls.into_field()
                if member.name not in cls.READ_COLUMNS
                and not member.partition.sources
                and not member.digest.sources
            }
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
        its own: a different clock precision, a bracket ordered differently,
        other text around them. What it may not do is state a different set of
        facts. The names the read fills from are the contract, and a header
        that renames one leaves it dropped in silence -- a whole column of
        nulls and no error, or a clock that settles nothing -- while one that
        omits it leaves a column no bridge fills. Both are checked here, where
        the mismatch is still legible.
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
        """Refuse a header whose captures are not the ones this read fills from."""
        declared = cls.captures()
        found = frozenset(options.capture_names)
        if found == declared:
            return
        unknown = sorted(found - declared)
        missing = sorted(declared - found)
        said = [f"captures nothing for {', '.join(missing)}" if missing else ""]
        said += [
            f"captures {', '.join(unknown)}, which this read fills nothing from" if unknown else ""
        ]
        raise ValueError(f"row header {' and '.join(part for part in said if part)}")

    @classmethod
    def from_text(cls, text: str | bytes, **declared: Any) -> Self:
        """Build one raw record without interpreting its body."""
        declared["body"] = decoded(text) if isinstance(text, bytes) else text
        return cls(**declared)
