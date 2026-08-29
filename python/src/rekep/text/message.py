"""One raw log row, before a protocol reads its payload."""

from __future__ import annotations

import functools
import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, Any, Self

import pyarrow
import pyarrow.compute

from rekep import txhash
from rekep.enums import EventType
from rekep.fields import DISPLAY, Field, column_name, column_names, scalar
from rekep.fields.arrays import build_list, dense_counts, null_mask, scattered, sequence
from rekep.fix.columns import DECLARATIONS, SESSION
from rekep.fix.message import NOT_SEPARATOR, parse_pairs
from rekep.market.event import MICROSECOND, Event
from rekep.market.identity import hash_bytes, hash_bytes_arrow
from rekep.text.entries import ENTRIES, Entry

#: The standard header is lifted out of `entries` into columns of its own,
#: and a lifted column is read back out of the list wherever it is empty --
#: a null column and a column a projection dropped being the same absence.
_CONTRACT_METADATA = MappingProxyType({"version": "1"})
_EVENT_CODE = pyarrow.int64()
_NO_PROTOCOL = "OTHER"
# Every separator `fix.message.SEPARATORS` declares, in that order: a
# multi-character candidate leads anything it contains, and a discriminator the
# raw stage cannot see is a MsgType filter that silently drops the row.
_DISCRIMINATOR_END = r"[ \t\r\n\f\x0b]*(?:\x04\x03|\^A|[\x01|^;#]|$)"
_TOKEN_START = r"(?:^|\x04\x03|\^A|[\x01|^;#])"
_MSG_TYPE_VALUE = r"^[A-Za-z0-9]+$"
_MSG_TYPE_VALUE_RE = re.compile(_MSG_TYPE_VALUE, re.ASCII)
_CHECKSUM_KEYS = tuple(
    column_name(name) for name in ("10", "CheckSum", "Trailer.10", "Trailer.CheckSum")
)

#: The standard header and trailer a raw row lifts out of `entries`, by the FIX
#: tag each of them is written under. They are lifted here because they are
#: *parsed* here -- on a fifteen-field NewOrderSingle they are nearly half of
#: every entry the row carries, and the FIX stage would otherwise walk the same
#: list again looking for facts it already has.
#:
#: Two of what the standard declares this stage cannot lift, and both stay
#: entries for the FIX stage to read from there like any other field:
#:
#: `CheckSum <10>` is the boundary every other lift is measured against -- a
#: field is eligible only where it stands in front of it -- and a marker that
#: lifted itself out could no longer say it was last.
#:
#: `XmlData <213>` is a message more often than it is a document: real bridge
#: traffic writes `key=value` pairs in it, and `FixCodec.into_payload_pairs`
#: expands those in the place the tag sat. That expansion reads the tag out of
#: the fields the row still carries, so lifting it here would leave a nested
#: order unread and its own column holding the whole payload as bytes.
#: `XmlDataLen <212>` leaves with it: a length says where the value after it
#: ends, so the two are one token and lifting only the length would separate
#: them the next time the row is written out.
#:
#: Which fields, and which tag each answers to, are the FIX stage's own
#: declaration read once here rather than copied: `fix.columns` selects the
#: session names this package promotes and takes every tag from the registry,
#: so a tag is never written down twice. The order is that declaration's, so
#: the two stages carry the header in the same order and the FIX stage reads
#: it off the columns rather than walking `entries` again.
#:
#: Read at import and never per message. This stage still interprets nothing:
#: a lift is a tag standing before the checksum, and the value it lands is the
#: text the payload spelled.
#: What the standard declares but this stage leaves in `entries`, and why is
#: above. Named rather than tagged, because the reason is about the field.
_UNLIFTED: frozenset[str] = frozenset({"CheckSum", "XmlDataLen", "XmlData"})

SESSION_NAMES: tuple[tuple[str, str], ...] = tuple(
    (DECLARATIONS[tag].fix.canonical, str(tag))
    for tag, _ in SESSION
    if DECLARATIONS[tag].fix.canonical not in _UNLIFTED
)

#: The same, as the columns carry them: folded, beside the tag. A column is
#: matched by its fold, so the dictionary's spelling is kept once above and
#: read back off `fix:display` rather than respelled at each use.
SESSION_FIELDS: tuple[tuple[str, str], ...] = tuple(
    (column_name(name), tag) for name, tag in SESSION_NAMES
)

#: The one session field whose value the standard constrains, and the one whose
#: `U`-prefixed wire spelling defers to a rendered name beside it.
_MSG_TYPE = column_name("MsgType")


def _session(name: str) -> Field:
    """One lifted header column: text as the payload spelled it, displayed as
    the dictionary spells it.

    The display and nothing else. This stage lifts by syntax -- a tag, before
    the checksum -- and reads no dictionary, so the column claims no protocol
    reading; what it does need is the spelling a reader knows the field by,
    which the fold removed from its name.
    """
    return Field(metadata={DISPLAY: name})


#: `{folded spelling: column}` for every session field, which is the lookup a
#: parse actually does -- one probe per entry rather than one pass per field.
#:
#: The **tag** only, except for the discriminator. A bridge that renders its
#: header writes its own names -- `#BeginString=`, `#SendingTime=` -- and this
#: stage keeps a rendered spelling exactly as it arrived, because which name a
#: feed uses is data. `MsgType` is the one field that has always answered to
#: both, and it keeps doing so: a `35=U1` wrapper naming its real type beside
#: it is the whole reason the rendered spelling is read at all.
_SESSION_BY_KEY: Mapping[str, str] = MappingProxyType(
    {**{tag: name for name, tag in SESSION_FIELDS}, "msgtype": _MSG_TYPE}
)

_RENDERED_DECORATION = r"[^A-Za-z0-9=\x01\x03\x04|;^#]*"


def _rendered_name(name: str) -> str:
    """A folded field name as the rendered-key grammar can spell it."""
    return _RENDERED_DECORATION.join(re.escape(character) for character in column_name(name))


_CHECKSUM_TOKEN = (
    rf"(?is){_TOKEN_START}[ \t\r\n\f\x0b]*#?"
    rf"(?:10|{_rendered_name('CheckSum')}|{_rendered_name('Trailer.10')}|"
    rf"{_rendered_name('Trailer.CheckSum')})[ \t\r\n\f\x0b]*="
)

# Finding one discriminator is deliberately cheaper than tokenising the whole
# payload. Captures can contain long prose and stack traces; only a row that
# names a message kind is allowed into the key/value splitter.
_WIRE_MSG_TYPE = (
    rf"(?s){_TOKEN_START}[ \t\r\n\f\x0b]*#?35[ \t\r\n\f\x0b]*="
    rf"[ \t\r\n\f\x0b]*(?P<value>[A-Za-z0-9]+){_DISCRIMINATOR_END}"
)
_NAMED_MSG_TYPE = (
    rf"(?is){_TOKEN_START}[ \t\r\n\f\x0b]*#?{_rendered_name('MsgType')}[ \t\r\n\f\x0b]*="
    rf"[ \t\r\n\f\x0b]*(?P<value>[A-Za-z0-9]+){_DISCRIMINATOR_END}"
)
# The version is whatever the BeginString value holds, which is everything up
# to the separator -- `NOT_SEPARATOR`, the same class the FIX parsers read one
# with. A dotted version alone was stricter than the shipped classification
# rule, so `8=FIX4` -- which this repository's own fixture writes -- reached
# the store as OTHER with no direction while `Rules.into_default()` called it
# FIX. One spelling of one rule.
_FIX_BEGIN = (
    r"(?s)(?:^|[^A-Za-z0-9_.\-])#?8[ \t\r\n\f\x0b]*="
    rf"[ \t\r\n\f\x0b]*FIXT?{NOT_SEPARATOR}*{_DISCRIMINATOR_END}"
)


@scalar(slots=True)
class Message(Event):
    """One log header, its provenance, and its protocol-neutral payload."""

    @classmethod
    @functools.cache
    def into_field_metadata(cls) -> Mapping[str, str]:
        """Contract metadata published with raw-message schemas."""
        return _CONTRACT_METADATA

    sourceurl: Annotated[str, Field.column("Source URL")] = ""
    """Path of the log the row came from, as its filesystem addresses it."""

    sourcerownum: Annotated[int, Field.column("Source Rownum")] = 0
    """1-based physical line number of the header; 0 when not read from a file."""

    threadname: Annotated[str, Field.column("Thread Name")] = ""
    """Contents of the first bracketed header field."""

    plugincode: Annotated[str, Field.column("Plugin Code")] = ""
    """Contents of the second bracketed header field."""

    message: str = ""
    """Payload after the fixed log header, with continuation lines folded in."""

    protocolcode: Annotated[str, Field.column("Protocol Code")] = _NO_PROTOCOL
    """Protocol syntax detected without interpreting its fields."""

    # The whole standard header and trailer, in `SESSION_NAMES` order, which
    # is the FIX stage's own -- so the two stages carry the header in the same
    # order and the FIX stage reads it off these columns instead of walking
    # `entries` again. Every one of them is the text the payload spelled: this
    # stage reads no numbers, names no zone and decodes no flag, so a length
    # is `"176"` and a flag is `"Y"`.
    beginstring: Annotated[str | None, _session("BeginString")] = None
    """Protocol the session negotiated, as the payload spells it."""

    bodylength: Annotated[str | None, _session("BodyLength")] = None
    """Declared body length, kept as text: this stage reads no numbers."""

    msgtype: Annotated[str | None, _session("MsgType")] = None
    """First FIX message discriminator when the payload names one."""

    sendercompid: Annotated[str | None, _session("SenderCompID")] = None
    """Who the payload says sent it."""

    sendersubid: Annotated[str | None, _session("SenderSubID")] = None
    """Which desk or trader of the sender the payload names."""

    senderlocationid: Annotated[str | None, _session("SenderLocationID")] = None
    """Where the payload says the sender sent it from."""

    targetcompid: Annotated[str | None, _session("TargetCompID")] = None
    """Who the payload says it was for."""

    targetsubid: Annotated[str | None, _session("TargetSubID")] = None
    """Which desk or trader of the target the payload names."""

    targetlocationid: Annotated[str | None, _session("TargetLocationID")] = None
    """Where the payload says the target is."""

    onbehalfofcompid: Annotated[str | None, _session("OnBehalfOfCompID")] = None
    """Firm the message originated with, where a hub relayed it."""

    onbehalfofsubid: Annotated[str | None, _session("OnBehalfOfSubID")] = None
    """Which desk or trader of that originating firm the payload names."""

    onbehalfoflocationid: Annotated[str | None, _session("OnBehalfOfLocationID")] = None
    """Where the payload says that originating firm sent it from."""

    delivertocompid: Annotated[str | None, _session("DeliverToCompID")] = None
    """Firm the message is ultimately for, where a hub is to relay it."""

    delivertosubid: Annotated[str | None, _session("DeliverToSubID")] = None
    """Which desk or trader of that firm the payload names."""

    delivertolocationid: Annotated[str | None, _session("DeliverToLocationID")] = None
    """Where the payload says that firm is."""

    msgseqnum: Annotated[str | None, _session("MsgSeqNum")] = None
    """Sequence number the payload states, as text."""

    lastmsgseqnumprocessed: Annotated[str | None, _session("LastMsgSeqNumProcessed")] = None
    """Last sequence number the sender says it had processed, as text."""

    possdupflag: Annotated[str | None, _session("PossDupFlag")] = None
    """Whether the payload flags itself a possible retransmission, as spelled."""

    possresend: Annotated[str | None, _session("PossResend")] = None
    """Whether the payload flags itself a possible resend, as spelled."""

    sendingtime: Annotated[str | None, _session("SendingTime")] = None
    """When the payload says it was sent, in the payload's own spelling."""

    origsendingtime: Annotated[str | None, _session("OrigSendingTime")] = None
    """When a resent payload says it first went out, as spelled."""

    onbehalfofsendingtime: Annotated[str | None, _session("OnBehalfOfSendingTime")] = None
    """When the originating firm sent it, where a relayed payload says."""

    applverid: Annotated[str | None, _session("ApplVerID")] = None
    """Application version a FIXT session names for this message."""

    cstmapplverid: Annotated[str | None, _session("CstmApplVerID")] = None
    """Custom application extension the payload names, where it names one."""

    applextid: Annotated[str | None, _session("ApplExtID")] = None
    """Extension pack the payload names, as text."""

    messageencoding: Annotated[str | None, _session("MessageEncoding")] = None
    """Encoding the payload declares for its `Encoded*` fields."""

    securedatalen: Annotated[str | None, _session("SecureDataLen")] = None
    """Declared length of `securedata`, kept as the text in front of it."""

    securedata: Annotated[str | None, _session("SecureData")] = None
    """Encrypted block the payload carried, taken by the length in front of it."""

    signaturelength: Annotated[str | None, _session("SignatureLength")] = None
    """Declared length of `signature`, kept as the text in front of it."""

    signature: Annotated[str | None, _session("Signature")] = None
    """Signature the payload carried, taken by the length in front of it."""

    entries: list[Entry] = None  # type: ignore[assignment]
    """Ordered payload arguments other than the promoted message discriminator."""

    # Resolved by `parse_arrow`, where the raw line and its protocol reading
    # coexist -- the verb before the payload's own first token is the
    # direction, and prose inside the payload never answers. Resolved *here*
    # because `parse_fix` reads these rows back with `message` projected out:
    # the FIX stage re-answers any row still carrying its text and preserves
    # this answer where the text is gone. Null for every row no directed
    # protocol claims, and for the many bridge re-log lines that repeat a
    # payload without repeating `Receiving`/`Sending`.
    direction: bool | None = None
    """True where the line says it was sent, False received; null undirected."""

    def __post_init__(self) -> None:
        """Normalize arguments and promote the protocol-neutral discriminator."""
        Event.__post_init__(self)
        implicit_entries = self.entries is None
        if implicit_entries:
            self.entries = []
        # `protocolcode` and `direction` are read off the raw *text*, so a row
        # carrying one answers them whoever tokenized its arguments:
        # `from_text(line, message=line)` stored `OTHER` and no direction
        # because it had passed its own `entries` in. Everything else here is
        # read off the arguments, and an explicit list of them is the answer.
        if self.message and (implicit_entries or self.protocolcode == _NO_PROTOCOL):
            parsed = self.parse_arrow(pyarrow.array([self.message]))
            if self.protocolcode == _NO_PROTOCOL:
                self.protocolcode = parsed["protocolcode"][0].as_py()
            if self.direction is None:
                self.direction = parsed["direction"][0].as_py()
        if implicit_entries and self.message:
            self.entries = parsed["entries"][0].as_py()
            for name, _ in SESSION_FIELDS:
                if getattr(self, name) is None:
                    setattr(self, name, parsed[name][0].as_py())
            if self.eventtype == EventType.UNKNOWN:
                self.eventtype = EventType(parsed["eventtype"][0].as_py())

        self.entries = [Entry.from_stored(entry) for entry in self.entries]
        session, self.entries = _scalar_session_values(self.entries)
        for name, _ in SESSION_FIELDS:
            if getattr(self, name) is None:
                setattr(self, name, session.get(name))
        if self.msgtype is None and self.eventtype == EventType.UNKNOWN:
            self.eventtype = EventType.MISC

    @classmethod
    def from_text(
        cls,
        text: str | bytes,
        separator: str | None = None,
        *,
        named: bool | None = None,
        entry_separator: str | None = None,
        **declared: Any,
    ) -> Self:
        """One payload's ordered fields as a raw row, discriminator promoted.

        Tokenized by `parse_pairs`, which reads the payload as its protocol:
        tag mode keeps only numeric keys, where the column parser's
        protocol-neutral `Entry.parse_arrow` keeps every key it can split. The
        two disagree on a wire message carrying a named enrichment key, and
        that is the difference between the two readings, not a defect in
        either.

        The raw text itself is retained only where a caller declares
        `message=` -- the pairs carry every field -- and the syntax columns
        `protocolcode`, `eventtype` and `direction` are read off that text, so
        they stay unset without it.
        """
        pairs = parse_pairs(text, separator, named=named, entry_separator=entry_separator)
        return cls(entries=list(pairs), **declared)

    @classmethod
    def parse_arrow(
        cls,
        messages: Any,
        msg_type_event_types: Mapping[str, EventType | int | str] | None = None,
        plugins: Any | None = None,
        protocol_rules: Any | None = None,
    ) -> dict[str, Any]:
        """Promote discriminators and parse only structured payload rows."""
        if isinstance(messages, pyarrow.ChunkedArray):
            offsets, parts = 0, []
            for chunk in messages.chunks:
                plugin_chunk = None if plugins is None else plugins.slice(offsets, len(chunk))
                parts.append(
                    cls.parse_arrow(
                        chunk,
                        msg_type_event_types,
                        plugin_chunk,
                        protocol_rules,
                    )
                )
                offsets += len(chunk)
            return {
                **{
                    name: pyarrow.chunked_array([part[name] for part in parts], pyarrow.string())
                    for name, _ in SESSION_FIELDS
                },
                "eventtype": pyarrow.chunked_array(
                    [part["eventtype"] for part in parts], _EVENT_CODE
                ),
                "protocolcode": pyarrow.chunked_array(
                    [part["protocolcode"] for part in parts], pyarrow.string()
                ),
                "entries": pyarrow.chunked_array([part["entries"] for part in parts], ENTRIES),
                "direction": pyarrow.chunked_array(
                    [part["direction"] for part in parts], pyarrow.bool_()
                ),
            }

        rows = len(messages)
        if not rows:
            return {
                **{name: pyarrow.nulls(0, pyarrow.string()) for name, _ in SESSION_FIELDS},
                "eventtype": pyarrow.array([], _EVENT_CODE),
                "protocolcode": pyarrow.array([], pyarrow.string()),
                "entries": pyarrow.array([], type=ENTRIES),
                "direction": pyarrow.array([], pyarrow.bool_()),
            }

        compute = pyarrow.compute
        text = compute.fill_null(messages.cast(pyarrow.string(), safe=False), "")
        wire_values, wire_probe, named_probe, begins_fix, probed_msg_types = _msg_type_probe(text)
        candidates = compute.or_(
            compute.or_(compute.or_(wire_probe, named_probe), begins_fix),
            Entry.looks_structured_arrow(text),
        )
        entries = _candidate_entries(text, candidates)
        session, entries = _session_columns(entries)
        msg_types = compute.coalesce(session[_MSG_TYPE], probed_msg_types)
        event_types = _event_types(msg_types, msg_type_event_types)
        protocols = (
            protocol_rules.into_arrow_protocol_array(text, plugins)
            if protocol_rules is not None
            else _protocol_codes(wire_values, wire_probe, named_probe, begins_fix)
        )
        # Direction is resolved here, where the raw line and its protocol
        # last coexist: `parse_fix` reads the stored rows with `message`
        # projected out, so an answer not stored now is an answer lost. The
        # FIX stage re-resolves any row still carrying its text -- the same
        # computation -- and preserves this one where the text is gone.
        from rekep.fix.rules import Rules

        resolver = protocol_rules if protocol_rules is not None else Rules.into_default()
        if hasattr(resolver, "into_arrow_direction_array"):
            direction = resolver.into_arrow_direction_array(text, protocols)
        else:
            # A duck-typed classifier that only knows protocols leaves the
            # verbs to the default vocabulary.
            direction = Rules.into_default().into_arrow_direction_array(text, protocols)
        return {
            **session,
            "eventtype": event_types,
            "protocolcode": protocols,
            _MSG_TYPE: msg_types,
            "entries": entries,
            "direction": direction,
        }

    @classmethod
    def msg_types_arrow(cls, messages: Any) -> Any:
        """Probe top-level message discriminators without splitting payload fields."""
        if isinstance(messages, pyarrow.ChunkedArray):
            return pyarrow.chunked_array(
                [cls.msg_types_arrow(chunk) for chunk in messages.chunks], pyarrow.string()
            )
        text = pyarrow.compute.fill_null(messages.cast(pyarrow.string(), safe=False), "")
        return _msg_type_probe(text)[-1]

    def identify(self) -> Self:
        """Give this raw row the identity of its exact payload."""
        if not self.vhash:
            self.vhash = hash_bytes(self.message.encode("utf-8"))
        if not self.hash:
            self.hash = txhash.couple128(self.unix // MICROSECOND, self.vhash)
        self.xhash = self.vhash
        return self

    @classmethod
    def identified(
        cls, columns: dict[str, Any], schema: pyarrow.Schema, rows: int
    ) -> pyarrow.RecordBatch:
        """Build a batch after assigning raw row identities in Arrow kernels."""
        columns["vhash"] = hash_bytes_arrow(columns["message"])
        columns["hash"] = txhash.couple128_arrow(
            cls._clock_micros(columns["unix"]), columns["vhash"]
        )
        columns["xhash"] = columns["vhash"]
        return pyarrow.RecordBatch.from_arrays(
            [columns[name] for name in schema.names], schema=schema
        )


def _scalar_session_values(entries: list[Entry]) -> tuple[dict[str, str], list[Entry]]:
    """Every standard header field before the checksum, and what is left.

    The scalar twin of `_session_columns`, rule for rule: a field spelled
    twice with two readings is not lifted, and a `U`-prefixed wire
    discriminator defers to a rendered name beside it.
    """
    claimed: dict[str, list[int]] = {}
    residual: list[int] = []
    # The discriminator's two spellings are claimed apart, because each has to
    # agree with itself before the rule between them applies.
    spellings: dict[str, list[int]] = {"35": [], "msgtype": []}
    ended = False
    for index, entry in enumerate(entries):
        folded = column_name(entry.key)
        if folded in _CHECKSUM_KEYS:
            ended = True
        column = None if ended else _SESSION_BY_KEY.get(folded)
        if column == _MSG_TYPE:
            if _MSG_TYPE_VALUE_RE.fullmatch(entry.value) is None:
                column = None
            else:
                spellings["35" if folded == "35" else "msgtype"].append(index)
        if column is None:
            residual.append(index)
        else:
            claimed.setdefault(column, []).append(index)

    def agreed(where: list[int]) -> str | None:
        """The one value those entries state, or None when they state two."""
        values = {entries[index].value for index in where}
        if len(values) == 1:
            return values.pop()
        # Two readings of one fact is not one statement of it: both stay where
        # a reader can see them, and the column says nothing.
        residual.extend(where)
        return None

    found: dict[str, str] = {}
    for column, where in claimed.items():
        if column == _MSG_TYPE:
            # The discriminator has a rule of its own for its two spellings, so
            # they disagreeing is expected rather than torn.
            wire = agreed(spellings["35"]) if spellings["35"] else None
            named = agreed(spellings["msgtype"]) if spellings["msgtype"] else None
            hybrid = wire and wire.startswith("U") and named
            value = named if hybrid else (wire or named)
            if value is not None:
                found[column] = value
            continue
        value = agreed(where)
        if value is not None:
            found[column] = value
    residual.sort()
    return found, [entries[index] for index in residual]


def _session_columns(stored: pyarrow.Array) -> tuple[dict[str, pyarrow.Array], pyarrow.Array]:
    """Lift every standard header field out of `entries`, before each checksum.

    One pass for all of them: the eligible window is computed once and each
    field is a mask over it. A field a row spells twice with two different
    values is left where it is and its column stays null -- the same rule the
    FIX stage applies when it lifts, because a bridge that writes one fact
    twice on purpose is telling the reader something a first-wins pop would
    throw away.
    """
    rows = len(stored)
    empty = {name: pyarrow.nulls(rows, pyarrow.string()) for name, _ in SESSION_FIELDS}
    if not rows:
        return {name: pyarrow.nulls(0, pyarrow.string()) for name in empty}, stored
    compute = pyarrow.compute
    entries = compute.list_flatten(stored)
    if not len(entries):
        return empty, stored

    parents = compute.list_parent_indices(stored).cast(pyarrow.int64())
    positions = sequence(len(entries))
    keys = compute.struct_field(entries, "key")
    values = compute.struct_field(entries, "value")
    normalized = column_names(keys)
    checksums = compute.fill_null(
        compute.is_in(normalized, value_set=pyarrow.array(_CHECKSUM_KEYS)), False
    )
    checksum_at = _first_by_parent(positions, parents, checksums, rows)
    before_checksum = compute.fill_null(
        compute.less(positions, compute.take(checksum_at, parents)), True
    )
    named_values = compute.fill_null(compute.match_substring_regex(values, _MSG_TYPE_VALUE), False)

    found: dict[str, pyarrow.Array] = {}
    claimed = pyarrow.array([False] * len(entries), type=pyarrow.bool_())
    for name, tag in SESSION_FIELDS:
        spelled = compute.equal(normalized, tag)
        if name == _MSG_TYPE:
            spelled = compute.or_(spelled, compute.equal(normalized, "msgtype"))
        eligible = compute.and_(before_checksum, spelled)
        if name == _MSG_TYPE:
            # The discriminator has a rule of its own for its two spellings --
            # a `U`-prefixed wire type defers to a rendered name beside it --
            # so disagreement between them is expected rather than torn, and
            # its value is the one the standard constrains. *Within* one
            # spelling the general rule holds: `35=D` beside `35=8` is two
            # readings of one fact, so neither leaves `entries` and the column
            # falls back to the raw line's own first discriminator.
            eligible = compute.and_(eligible, named_values)
            found[name], mask = _wire_or_named(values, parents, normalized, eligible, rows)
            claimed = compute.or_(claimed, mask)
            continue
        first, mask = _agreed_by_parent(values, parents, eligible, rows)
        found[name] = first
        claimed = compute.or_(claimed, mask)
    keep = compute.invert(claimed)
    residual = build_list(
        ENTRIES,
        dense_counts(compute.filter(parents, keep), rows),
        compute.filter(entries, keep),
        null_mask(stored),
    )
    return found, residual


def _wire_or_named(
    values: pyarrow.Array,
    parents: pyarrow.Array,
    normalized: pyarrow.Array,
    eligible: pyarrow.Array,
    rows: int,
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """`(the discriminator, which entries it claims)`, tag deferring to a name.

    A bridge that wraps its own message in `35=U1` and then names the real
    type beside it means the name; everything else means the tag. Each
    spelling has to agree with itself first, so a row spelling `35=` twice
    with two values leaves both where a reader can see them.
    """
    compute = pyarrow.compute
    wire, wire_mask = _agreed_by_parent(
        values, parents, compute.and_(eligible, compute.equal(normalized, "35")), rows
    )
    named, named_mask = _agreed_by_parent(
        values, parents, compute.and_(eligible, compute.equal(normalized, "msgtype")), rows
    )
    wrapped = compute.and_(
        compute.fill_null(compute.starts_with(wire, "U"), False),
        compute.is_valid(named),
    )
    return (
        compute.if_else(wrapped, named, compute.coalesce(wire, named)),
        compute.or_(wire_mask, named_mask),
    )


def _agreed_by_parent(
    values: pyarrow.Array,
    parents: pyarrow.Array,
    eligible: pyarrow.Array,
    rows: int,
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """`(first value per row, which entries it claims)` -- nothing when they disagree.

    A row spelling one header field twice with two readings has not stated it
    once, so neither reading is lifted and both stay where a reader can see
    them.
    """
    compute = pyarrow.compute
    first = _first_by_parent(values, parents, eligible, rows)
    disagrees = compute.and_(
        eligible, compute.fill_null(compute.not_equal(values, compute.take(first, parents)), True)
    )
    torn = compute.greater(dense_counts(compute.filter(parents, disagrees), rows), 0)
    per_entry = compute.take(torn, parents)
    return (
        compute.if_else(torn, pyarrow.nulls(rows, values.type), first),
        compute.and_(eligible, compute.invert(per_entry)),
    )


def _first_by_parent(
    values: pyarrow.Array,
    parents: pyarrow.Array,
    selected: pyarrow.Array,
    rows: int,
) -> pyarrow.Array:
    """First selected value for each dense parent row."""
    selected_parents = pyarrow.compute.filter(parents, selected)
    if not len(selected_parents):
        return pyarrow.nulls(rows, values.type)
    previous = pyarrow.concat_arrays(
        [
            pyarrow.array([-1], pyarrow.int64()),
            selected_parents.slice(0, len(selected_parents) - 1),
        ]
    )
    first = pyarrow.compute.not_equal(selected_parents, previous)
    return pyarrow.compute.scatter(
        pyarrow.compute.filter(values, selected).filter(first),
        pyarrow.compute.filter(selected_parents, first),
        max_index=rows - 1,
    )


def _candidate_entries(text: pyarrow.Array, candidates: pyarrow.Array) -> pyarrow.Array:
    """Parse candidate rows and scatter empty lists into skipped prose rows."""
    compute = pyarrow.compute
    rows = len(text)
    if not compute.any(candidates, min_count=0).as_py():
        return pyarrow.repeat(pyarrow.scalar([], ENTRIES), rows)
    if compute.all(candidates, min_count=0).as_py():
        return Entry.parse_arrow(text)

    positions = sequence(rows)
    selected_at = compute.filter(positions, candidates)
    skipped_at = compute.filter(positions, compute.invert(candidates))
    parsed = Entry.parse_arrow(compute.filter(text, candidates))
    skipped = pyarrow.repeat(pyarrow.scalar([], ENTRIES), len(skipped_at))
    return scattered([parsed, skipped], [selected_at, skipped_at])


def _event_types(
    msg_types: pyarrow.Array,
    declared: Mapping[str, EventType | int | str] | None,
) -> pyarrow.Array:
    """Map known discriminators, separating absent from unknown values."""
    compute = pyarrow.compute
    rows = len(msg_types)
    unknown = pyarrow.scalar(int(EventType.UNKNOWN), _EVENT_CODE)
    found: Any = pyarrow.repeat(unknown, rows)
    if declared:
        keys = [str(key) for key in declared]
        codes = pyarrow.array([_event_code(value) for value in declared.values()], _EVENT_CODE)
        indices = compute.index_in(msg_types, value_set=pyarrow.array(keys, pyarrow.string()))
        found = compute.fill_null(compute.take(codes, indices), unknown)
    return compute.if_else(
        compute.is_null(msg_types),
        pyarrow.scalar(int(EventType.MISC), _EVENT_CODE),
        found,
    ).cast(_EVENT_CODE, safe=False)


def _protocol_codes(
    wire_values: pyarrow.Array,
    wire_probe: pyarrow.Array,
    named_probe: pyarrow.Array,
    begins_fix: pyarrow.Array,
) -> pyarrow.Array:
    """Protocol syntax classified before the payload is discarded."""
    compute = pyarrow.compute
    wrapped = compute.and_(
        compute.fill_null(compute.starts_with(wire_values, "U"), False), named_probe
    )
    named = compute.or_(wrapped, compute.and_(named_probe, compute.invert(wire_probe)))
    return compute.if_else(
        named,
        pyarrow.scalar("UL"),
        compute.if_else(
            compute.or_(begins_fix, wire_probe),
            pyarrow.scalar("FIX"),
            pyarrow.scalar(_NO_PROTOCOL),
        ),
    ).cast(pyarrow.string(), safe=False)


def _before_checksum(candidate_at: pyarrow.Array, checksum_at: pyarrow.Array) -> pyarrow.Array:
    """A discriminator exists and precedes the first checksum token."""
    compute = pyarrow.compute
    exists = compute.greater_equal(candidate_at, 0)
    return compute.and_(
        exists,
        compute.or_(compute.less(checksum_at, 0), compute.less(candidate_at, checksum_at)),
    )


def _msg_type_probe(
    text: pyarrow.Array,
) -> tuple[pyarrow.Array, pyarrow.Array, pyarrow.Array, pyarrow.Array, pyarrow.Array]:
    """Wire values, syntax masks, and the first valid top-level discriminator."""
    compute = pyarrow.compute
    wire_values = compute.struct_field(compute.extract_regex(text, _WIRE_MSG_TYPE), "value")
    named_values = compute.struct_field(compute.extract_regex(text, _NAMED_MSG_TYPE), "value")
    checksum_at = compute.find_substring_regex(text, _CHECKSUM_TOKEN)
    wire_probe = _before_checksum(compute.find_substring_regex(text, _WIRE_MSG_TYPE), checksum_at)
    named_probe = _before_checksum(compute.find_substring_regex(text, _NAMED_MSG_TYPE), checksum_at)
    missing = pyarrow.scalar(None, pyarrow.string())
    wire_values = compute.if_else(wire_probe, wire_values, missing)
    named_values = compute.if_else(named_probe, named_values, missing)
    begins_fix = compute.fill_null(compute.match_substring_regex(text, _FIX_BEGIN), False)
    wrapped = compute.and_(
        compute.fill_null(compute.starts_with(wire_values, "U"), False), named_probe
    )
    msg_types = compute.if_else(wrapped, named_values, compute.coalesce(wire_values, named_values))
    return wire_values, wire_probe, named_probe, begins_fix, msg_types


def _event_code(value: EventType | int | str) -> int:
    """One configurable event spelling as its stable stored integer.

    A member, its name, its mnemonic, or its stored code. A spelling no
    member answers to is refused rather than written into the column as a
    dead code every reader maps to `UNKNOWN`.
    """
    if isinstance(value, EventType):
        return int(value)
    try:
        code = int(value)
    except (TypeError, ValueError):
        member = EventType(str(value))
        if member is EventType.UNKNOWN and str(value).strip().upper() != "UNKNOWN":
            raise ValueError(f"unknown EventType spelling {value!r}") from None
        return int(member)
    member = EventType.from_int(code)
    if member is EventType.UNKNOWN and code != 0:
        raise ValueError(f"no EventType stores id {code}")
    return int(member)
