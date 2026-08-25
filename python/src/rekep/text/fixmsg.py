"""The shape of one parsed log line, what decides which event it is, and what fills it."""

from __future__ import annotations

import dataclasses
import datetime
import functools
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Annotated, Any

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.enums import MIC, EventType, IdSource, OptionKind, Side
from rekep.fields import Field, scalar
from rekep.fields.arrays import (
    build_list,
    build_map,
    dense_counts,
    groups_of,
    interleave,
    scattered,
    sequence,
)
from rekep.fix.access import Entry, FieldAccess, Reading
from rekep.fix.columns import (
    DECLARATIONS,
    IDENTIFIER_FIELDS,
    ISIN_CODE,
    KWARGS,
    FixKwarg,
)
from rekep.fix.components import (
    PARTIES,
    SIDE_TRD_REG_TIMESTAMPS,
    TRD_REG_TIMESTAMPS,
    Party,
    SideTrdRegTimestamp,
    TrdRegTimestamp,
)
from rekep.fix.fields import cast_arrow_fix
from rekep.fix.registry import FixRegistry
from rekep.fix.rules import NO_PROTOCOL
from rekep.fix.transcribe import NO_SOURCE
from rekep.market.event import CODES_TYPE, Event, unix_partition_arrow
from rekep.market.identity import NIL
from rekep.text.message import Message, MessageRule, MessageRules

_EVENT_CODE = pyarrow.int32()
_CONTRACT_METADATA = MappingProxyType({"version": "4"})
_INSTRUMENT_PLUGIN = "rekep.instrument"
_INSTRUMENT_PROTOCOL = "REKEP"

# A `MsgType <35>` of our own, in the range FIX reserves for exactly this: `U`
# followed by digits is user-defined, so a synthesized instrument can never
# collide with a standard type a future FIX version adds. It is a type and not
# a marker beside one: these rows go back out as FIX messages, and a consumer
# holding only the message has no `etype` -- it has tag 35. Reusing `d` would
# have made a synthesized instrument indistinguishable from a
# `SecurityDefinition` a real bridge sent.
_INSTRUMENT_MSG_TYPE = "U1"
_INSTRUMENT_KIND = "rekep.kind"
_INSTRUMENT_XHASH = "rekep.xhash"


@functools.cache
def _row_access() -> FieldAccess:
    """The accessor a stored row reads through: the cross-version dictionary."""
    return FieldAccess.of(FixRegistry.from_builtin(), None)


@scalar(slots=True)
class FixMsg(Message):
    """One raw message transcribed under the FIX registry."""

    @classmethod
    @functools.cache
    def into_field_metadata(cls) -> Mapping[str, str]:
        """Contract metadata published with parsed log schemas."""
        return _CONTRACT_METADATA

    @classmethod
    @functools.cache
    def into_instrument_plugin(cls) -> str:
        """Plugin marker for normalized instrument lifecycle rows."""
        return _INSTRUMENT_PLUGIN

    @classmethod
    @functools.cache
    def into_instrument_protocol(cls) -> str:
        """Protocol marker for normalized internal rows."""
        return _INSTRUMENT_PROTOCOL

    @classmethod
    @functools.cache
    def into_instrument_msg_type(cls) -> str:
        """`MsgType <35>` a synthesized instrument row carries."""
        return _INSTRUMENT_MSG_TYPE

    @classmethod
    def into_message_rules(cls) -> MessageRules:
        """Fresh event rules for FIX wire and rendered message names."""
        return MessageRules(
            rules=[
                dataclasses.replace(rule, patterns=list(rule.patterns)) for rule in DEFAULT_RULES
            ]
        )

    xhash: int = NIL
    """Digest of `code`, or the raw-line digest when no correlation code exists."""

    code: str = ""
    """Best lifecycle identifier present on this line."""

    # One order, and it prefers the identifier that survives an amendment:
    # `OrigClOrdID <41>` names the order a replacement replaces, and a
    # lifecycle that moved to the new `ClOrdID <11>` on every amendment would
    # be one lifecycle per amendment. Later fallbacks give reference and
    # market-data lines a readable lifecycle without inventing a
    # source-specific field.
    @classmethod
    @functools.cache
    def into_code_columns(cls) -> tuple[str, ...]:
        """Parsed columns tried for a lifecycle identifier, best first."""
        return (
            "OrderID",
            "OrigClOrdID",
            "ClOrdID",
            "ExecID",
            "QuoteEntryID",
            "QuoteID",
            "QuoteReqID",
            "SecurityID",
            "ISINCODE",
            "Symbol",
        )

    @classmethod
    @functools.cache
    def into_identifier_columns(cls) -> tuple[str, ...]:
        """Parsed identifier columns retained in `codes`, in lookup order."""
        return tuple(stored for stored, _, _ in IDENTIFIER_FIELDS)

    @classmethod
    @functools.cache
    def into_symbol_columns(cls) -> tuple[str, ...]:
        """Instrument identifiers used when FIX omits `Symbol <55>`."""
        return ("Symbol", "SecurityID", "ISINCODE")

    def __post_init__(self) -> None:
        """Normalize retained FIX fields without changing null/list semantics."""
        Event.__post_init__(self)
        if self.kwargs is not None:
            self.kwargs = [FixKwarg.from_stored(entry) for entry in self.kwargs]

    def identify(self) -> FixMsg:
        """Give the parsed event its lifecycle and version identities."""
        return Event.identify(self)

    # Nullable, and null on `fix.market`: there `kwargs` carries every
    # field the line held, so keeping the raw string beside it would store the
    # same content twice. An all-null column run-length and dictionary encodes
    # to nothing on disk, which is what makes one stored shape across the
    # three tables affordable -- the same reasoning `_zeros` applies to the
    # envelope members a parsed line leaves unset.
    message: str | None = None
    """Payload with the header and level stripped; null where `kwargs` holds it all."""

    protocol_code: str = NO_PROTOCOL
    """Which protocol the line carries; OTHER is a line that carries none."""

    # Without it nothing downstream can tell a real transaction time from a
    # print time, and that distinction is the whole point of resolving one.
    # Empty means no clock answered at all, which is a row with no time.
    unix_source: str = ""
    """Which rung of `TRANSACTED` gave `unix`; `recorded` is the log's own clock."""

    # One column, not a FIX-specific one: every protocol with versions has a
    # version, and a `fix_version` beside it would duplicate itself the first
    # time a second versioned protocol appeared. Resolved once, at the message
    # stage, so nothing downstream re-derives it.
    protocol_version: str | None = None
    """Which version of `protocol_code` the line is read under; null when unresolved."""

    # Null because the message carried no version, or null because nothing
    # tried? A consumer cannot tell the two apart from the value, and they are
    # different facts about the row.
    protocol_version_source: str = NO_SOURCE
    """What resolved `protocol_version`: a BeginString, an application version, or nothing."""

    MsgSeqNum: Annotated[int | None, DECLARATIONS[34]] = None
    """`MsgSeqNum <34>`: wire order among messages with equal timestamps."""

    # A list preserves repeated keys and wire order. Null means no parsed
    # message; an empty list means a message with nothing left after lifting.
    kwargs: list[FixKwarg] | None = None
    """Every field the message carried and no column took, as the dictionary read it."""

    Parties: Annotated[
        list[Party] | None,
        Field(
            arrow_type=PARTIES,
            metadata={"fix:component": "Parties"},
        ),
    ] = None
    """FIX Parties entries; null when the component is absent."""

    TrdRegTimestamps: Annotated[
        list[TrdRegTimestamp] | None,
        Field(
            arrow_type=TRD_REG_TIMESTAMPS,
            metadata={"fix:component": "TrdRegTimestamps"},
        ),
    ] = None
    """FIX TrdRegTimestamps entries; null when the component is absent."""

    SideTrdRegTS: Annotated[
        list[SideTrdRegTimestamp] | None,
        Field(
            arrow_type=SIDE_TRD_REG_TIMESTAMPS,
            metadata={"fix:component": "SideTrdRegTS"},
        ),
    ] = None
    """FIX SideTrdRegTS entries -- the per-side regulatory clock; null when absent."""

    ISINCODE: Annotated[str | None, ISIN_CODE] = None
    """ISIN carried by a rendered `ISINCODE` field."""

    # -- what a message says, flattened ---------------------------------------
    #
    # Flat fields keep the registry's exact name, type, description and
    # metadata. A lifted fact is removed from `kwargs`; repeated facts stay.

    # The envelope itself.

    BeginString: Annotated[str | None, DECLARATIONS[8]] = None
    """`BeginString <8>`: which FIX version the message says it is."""

    BodyLength: Annotated[int | None, DECLARATIONS[9]] = None
    """`BodyLength <9>`, as the message counted it."""

    MsgType: Annotated[str | None, DECLARATIONS[35]] = None
    """`MsgType <35>`: what the message is, on the wire."""

    CheckSum: Annotated[str | None, DECLARATIONS[10]] = None
    """`CheckSum <10>`: three digits, so a string -- `010` read as `10` no longer verifies."""

    # Who sent it, and to whom.

    SenderCompID: Annotated[str | None, DECLARATIONS[49]] = None
    """`SenderCompID <49>`: who sent it."""

    SenderSubID: Annotated[str | None, DECLARATIONS[50]] = None
    """`SenderSubID <50>`: which desk of theirs."""

    SenderLocationID: Annotated[str | None, DECLARATIONS[142]] = None
    """`SenderLocationID <142>`."""

    TargetCompID: Annotated[str | None, DECLARATIONS[56]] = None
    """`TargetCompID <56>`: who it was sent to."""

    TargetSubID: Annotated[str | None, DECLARATIONS[57]] = None
    """`TargetSubID <57>`."""

    TargetLocationID: Annotated[str | None, DECLARATIONS[143]] = None
    """`TargetLocationID <143>`."""

    # And on whose behalf, when a hub relayed it.

    OnBehalfOfCompID: Annotated[str | None, DECLARATIONS[115]] = None
    """`OnBehalfOfCompID <115>`: who the sender was speaking for."""

    OnBehalfOfSubID: Annotated[str | None, DECLARATIONS[116]] = None
    """`OnBehalfOfSubID <116>`."""

    OnBehalfOfLocationID: Annotated[str | None, DECLARATIONS[144]] = None
    """`OnBehalfOfLocationID <144>`."""

    DeliverToCompID: Annotated[str | None, DECLARATIONS[128]] = None
    """`DeliverToCompID <128>`: who it is ultimately for."""

    DeliverToSubID: Annotated[str | None, DECLARATIONS[129]] = None
    """`DeliverToSubID <129>`."""

    DeliverToLocationID: Annotated[str | None, DECLARATIONS[145]] = None
    """`DeliverToLocationID <145>`."""

    # Where it sits in the session's stream, and whether it is a repeat.

    LastMsgSeqNumProcessed: Annotated[int | None, DECLARATIONS[369]] = None
    """`LastMsgSeqNumProcessed <369>`: how far the sender had read."""

    PossDupFlag: Annotated[bool | None, DECLARATIONS[43]] = None
    """`PossDupFlag <43>`: a retransmission of a message already sent."""

    PossResend: Annotated[bool | None, DECLARATIONS[97]] = None
    """`PossResend <97>`: the same business content under a new sequence."""

    # FIX documents these instants as UTC; microseconds are Iceberg-compatible.

    SendingTime: Annotated[datetime.datetime | None, DECLARATIONS[52]] = None
    """`SendingTime <52>`: when it was transmitted."""

    OrigSendingTime: Annotated[datetime.datetime | None, DECLARATIONS[122]] = None
    """`OrigSendingTime <122>`: the original transmission, on a resend."""

    OnBehalfOfSendingTime: Annotated[datetime.datetime | None, DECLARATIONS[370]] = None
    """`OnBehalfOfSendingTime <370>`."""

    # Which application version speaks, under FIXT.

    ApplVerID: Annotated[str | None, DECLARATIONS[1128]] = None
    """`ApplVerID <1128>`."""

    CstmApplVerID: Annotated[str | None, DECLARATIONS[1129]] = None
    """`CstmApplVerID <1129>`."""

    ApplExtID: Annotated[int | None, DECLARATIONS[1156]] = None
    """`ApplExtID <1156>`."""

    # How the payload is written, when it is not plain ASCII.

    MessageEncoding: Annotated[str | None, DECLARATIONS[347]] = None
    """`MessageEncoding <347>`."""

    XmlDataLen: Annotated[int | None, DECLARATIONS[212]] = None
    """`XmlDataLen <212>`."""

    XmlData: Annotated[bytes | None, DECLARATIONS[213]] = None
    """`XmlData <213>`, as the bytes it is."""

    # And how it is sealed.

    SecureDataLen: Annotated[int | None, DECLARATIONS[90]] = None
    """`SecureDataLen <90>`."""

    SecureData: Annotated[bytes | None, DECLARATIONS[91]] = None
    """`SecureData <91>`, as the bytes it is."""

    SignatureLength: Annotated[int | None, DECLARATIONS[93]] = None
    """`SignatureLength <93>`."""

    Signature: Annotated[bytes | None, DECLARATIONS[89]] = None
    """`Signature <89>`, as the bytes it is."""

    # What was traded.

    Symbol: Annotated[str | None, DECLARATIONS[55]] = None
    """`Symbol <55>`: ticker symbol."""

    SecurityID: Annotated[str | None, DECLARATIONS[48]] = None
    """`SecurityID <48>`, under the scheme `SecurityIDSource` names."""

    SecurityIDSource: Annotated[str | None, DECLARATIONS[22]] = None
    """`SecurityIDSource <22>`: which scheme `SecurityID` is in -- `4` is ISIN."""

    SecurityType: Annotated[str | None, DECLARATIONS[167]] = None
    """`SecurityType <167>`."""

    CFICode: Annotated[str | None, DECLARATIONS[461]] = None
    """`CFICode <461>`: what kind of instrument it is, as ISO 10962 spells it."""

    SecurityExchange: Annotated[str | None, DECLARATIONS[207]] = None
    """`SecurityExchange <207>`: the market the instrument is listed on."""

    Currency: Annotated[str | None, DECLARATIONS[15]] = None
    """`Currency <15>`, which is what the prices below are in."""

    # Who asked, and under which identifiers.

    Account: Annotated[str | None, DECLARATIONS[1]] = None
    """`Account <1>`."""

    ClOrdID: Annotated[str | None, DECLARATIONS[11]] = None
    """`ClOrdID <11>`: the client's own identifier for the order."""

    OrigClOrdID: Annotated[str | None, DECLARATIONS[41]] = None
    """`OrigClOrdID <41>`: which order an amendment or cancel is about."""

    OrderID: Annotated[str | None, DECLARATIONS[37]] = None
    """`OrderID <37>`: the venue's identifier for it."""

    ExecID: Annotated[str | None, DECLARATIONS[17]] = None
    """`ExecID <17>`: the venue's identifier for this execution report."""

    # On what terms.

    Side: Annotated[str | None, DECLARATIONS[54]] = None
    """`Side <54>`: `1` buy, `2` sell, and the rest of the standard's codes."""

    OrdType: Annotated[str | None, DECLARATIONS[40]] = None
    """`OrdType <40>`: `1` market, `2` limit, ..."""

    TimeInForce: Annotated[str | None, DECLARATIONS[59]] = None
    """`TimeInForce <59>`: `0` day, `1` GTC, `3` IOC, ..."""

    # Where it stands.

    OrdStatus: Annotated[str | None, DECLARATIONS[39]] = None
    """`OrdStatus <39>`: where the order stands."""

    ExecType: Annotated[str | None, DECLARATIONS[150]] = None
    """`ExecType <150>`: what this report is reporting."""

    # For how much, at what price.

    OrderQty: Annotated[float | None, DECLARATIONS[38]] = None
    """`OrderQty <38>`: how much was asked for."""

    Price: Annotated[float | None, DECLARATIONS[44]] = None
    """`Price <44>`: the limit, when there is one."""

    AvgPx: Annotated[float | None, DECLARATIONS[6]] = None
    """`AvgPx <6>`: the average of what has filled so far."""

    CumQty: Annotated[float | None, DECLARATIONS[14]] = None
    """`CumQty <14>`: how much has filled."""

    LeavesQty: Annotated[float | None, DECLARATIONS[151]] = None
    """`LeavesQty <151>`: how much is still working."""

    LastPx: Annotated[float | None, DECLARATIONS[31]] = None
    """`LastPx <31>`: the price of this fill."""

    LastQty: Annotated[float | None, DECLARATIONS[32]] = None
    """`LastQty <32>`: the size of this fill."""

    # When it happened, and whatever was said about it.

    TransactTime: Annotated[datetime.datetime | None, DECLARATIONS[60]] = None
    """`TransactTime <60>`: when the business event happened, in UTC."""

    Text: Annotated[str | None, DECLARATIONS[58]] = None
    """`Text <58>`: whatever the counterparty wrote, often the reject reason."""

    # Quote identity, terms and lifecycle. Repeating mass-quote entries remain
    # in `kwargs`; a value is lifted only when it occurs once on the line.

    QuoteID: Annotated[str | None, DECLARATIONS[117]] = None
    """`QuoteID <117>`: quote lifecycle identifier."""

    QuoteReqID: Annotated[str | None, DECLARATIONS[131]] = None
    """`QuoteReqID <131>`: request this quote answers."""

    QuoteType: Annotated[int | None, DECLARATIONS[537]] = None
    """`QuoteType <537>`: indicative, tradeable or restricted quote kind."""

    QuoteStatus: Annotated[int | None, DECLARATIONS[297]] = None
    """`QuoteStatus <297>`: quote acknowledgement state."""

    QuoteRejectReason: Annotated[int | None, DECLARATIONS[300]] = None
    """`QuoteRejectReason <300>` when a quote is rejected."""

    QuoteRespType: Annotated[int | None, DECLARATIONS[694]] = None
    """`QuoteRespType <694>`: quote response action."""

    QuoteCancelType: Annotated[int | None, DECLARATIONS[298]] = None
    """`QuoteCancelType <298>`: scope of a quote cancellation."""

    BidPx: Annotated[float | None, DECLARATIONS[132]] = None
    """`BidPx <132>`: quoted bid price."""

    OfferPx: Annotated[float | None, DECLARATIONS[133]] = None
    """`OfferPx <133>`: quoted offer price."""

    BidSize: Annotated[float | None, DECLARATIONS[134]] = None
    """`BidSize <134>`: quoted bid quantity."""

    OfferSize: Annotated[float | None, DECLARATIONS[135]] = None
    """`OfferSize <135>`: quoted offer quantity."""

    DefBidSize: Annotated[float | None, DECLARATIONS[293]] = None
    """`DefBidSize <293>`: default bid quantity for a quote set."""

    DefOfferSize: Annotated[float | None, DECLARATIONS[294]] = None
    """`DefOfferSize <294>`: default offer quantity for a quote set."""

    ValidUntilTime: Annotated[datetime.datetime | None, DECLARATIONS[62]] = None
    """`ValidUntilTime <62>`: quote expiry in UTC."""

    NoQuoteSets: Annotated[int | None, DECLARATIONS[296]] = None
    """`NoQuoteSets <296>`: quote-set group count."""

    NoQuoteEntries: Annotated[int | None, DECLARATIONS[295]] = None
    """`NoQuoteEntries <295>`: quote-entry group count."""

    QuoteSetID: Annotated[str | None, DECLARATIONS[302]] = None
    """`QuoteSetID <302>`: quote-set identifier."""

    QuoteEntryID: Annotated[str | None, DECLARATIONS[299]] = None
    """`QuoteEntryID <299>`: stable quote-entry identifier."""

    @classmethod
    def from_instrument(cls, instrument: Any, **declared: Any) -> FixMsg:
        """Carry one normalized instrument version in the parsed-log stream."""
        from rekep.market.instrument import Instrument

        if not isinstance(instrument, Instrument):
            raise TypeError(f"instrument must be Instrument, got {type(instrument).__name__}")
        known = instrument if instrument.hash else dataclasses.replace(instrument).identify()
        values = {member.name: getattr(known, member.name) for member in dataclasses.fields(Event)}
        values.update(
            {
                "etype": EventType.INSTRUMENT,
                "source_url": "",
                "source_rownum": 0,
                "thread_name": "",
                "plugin_code": cls.into_instrument_plugin(),
                # Null for the same reason a market row's is: `kwargs` below
                # carries every fact this row states, so a raw line beside it
                # would be the same content twice -- and there was no line.
                "message": None,
                "protocol_code": cls.into_instrument_protocol(),
                "MsgType": cls.into_instrument_msg_type(),
                "Symbol": known.symbol or None,
                "SecurityID": known.security_id,
                "SecurityIDSource": known.security_id_source,
                "ISINCODE": known.isin_code,
                "SecurityType": known.security_type,
                "CFICode": known.cfi,
                "SecurityExchange": known.exchange,
                "Currency": None if known.currency is None else known.currency.into_fix(),
                "kwargs": _stored_entries(_instrument_pairs(known)),
            }
        )
        values.update(declared)
        return cls(**values)

    def into_dict(self) -> dict[str, Any]:
        """Plain values with the stored fields in Arrow's list-struct spelling."""
        encoded = Convertible.into_dict(self)
        encoded["kwargs"] = _stored_entries(self.kwargs)
        return encoded

    def get(self, field: int | str) -> Reading:
        """One field off this row, whichever of the four ways it is named.

        The one accessor (fix/access.py) reads the promoted columns first and
        the stored `kwargs` after them, so a lifted fact and a residual one
        answer through one call. The `Reading` carries the stored value and
        the typed reading together.
        """
        return _row_access().reading(self._field_entries(), field)

    def readings(self, field: int | str) -> list[Reading]:
        """Every value of `field` on this row, in stored order."""
        return _row_access().readings(self._field_entries(), field)

    def _field_entries(self) -> list[Entry]:
        """What the accessor scans: lifted columns in schema order, then `kwargs`.

        A list of ready `Entry` views, so a caller reading several dozen
        fields off one row builds them once: `entries_of` passes a ready entry
        straight through, and rebuilding the row per ask was the whole cost of
        decoding a normalized instrument (benchmarks/bench_market.py).
        """
        own = type(self)
        entries = [
            Entry(tag=int(tag), name=name, value=value)
            for name, tag in own.into_tagged_columns()
            if (value := getattr(self, name, None)) is not None
        ]
        entries += [
            Entry(name=spelled, value=value)
            for name, spelled in own.into_named_columns()
            if (value := getattr(self, name, None)) is not None
        ]
        entries += [Entry.from_stored(stored) for stored in self.kwargs or ()]
        return entries

    @classmethod
    @functools.cache
    def into_named_columns(cls) -> tuple[tuple[str, str], ...]:
        """`(attribute, registry spelling)` for lifted columns FIX never numbered."""
        return tuple(
            (member.name, spelled)
            for member in cls.into_field().fields
            if not member.fix.get("tag") and (spelled := member.fix.get("name"))
        )

    @property
    def is_instrument_version(self) -> bool:
        """Whether this row is a normalized instrument lifecycle version.

        One column answers it, and it is the one a message carries: these rows
        are reinjected as FIX, and a consumer holding only the message has no
        `etype` to dispatch on. `MsgType <35>` survives that round trip.
        """
        return self.MsgType == type(self).into_instrument_msg_type()

    # -- the FIX stage --------------------------------------------------------

    @classmethod
    def from_message_arrow_batch(
        cls, batch: pyarrow.RecordBatch, codec: Any, rules: MessageRules
    ) -> pyarrow.RecordBatch:
        """Transcribe one raw `Message` batch under a FIX codec and event rules."""
        if not isinstance(batch, pyarrow.RecordBatch):
            raise TypeError(f"FixMsg conversion needs a RecordBatch, got {type(batch).__name__}")
        rows = batch.num_rows
        columns = {name: batch.column(name) for name in batch.schema.names}
        messages = columns["message"]
        protocols = codec.categorise(messages, columns.get("plugin_code"))
        parts, positions = [], []
        for protocol, where in groups_of(protocols):
            rule = codec.rules.rule(protocol.as_py())
            if rule.named is None:
                parts.append(pyarrow.nulls(len(where), KWARGS))
            else:
                kwargs = (
                    columns["kwargs"]
                    if len(where) == rows
                    else pyarrow.compute.take(columns["kwargs"], where)
                )
                pairs = codec.drop_null_values(
                    codec.into_payload_pairs(codec.into_pairs_from_kwargs(kwargs, protocol.as_py()))
                )
                parts.append(codec.into_message_kwargs(pairs))
            positions.append(where)
        kwargs = scattered(parts, positions) if parts else pyarrow.nulls(rows, KWARGS)
        protocol_version, protocol_version_source = codec.versions_of_kwargs(kwargs)
        columns.update(
            {
                "etype": rules.etype_arrow(messages),
                "protocol_code": protocols,
                "protocol_version": protocol_version,
                "protocol_version_source": protocol_version_source,
                "kwargs": kwargs,
                # MsgType is the one field a versionless message can still
                # state unambiguously: tag 35 names itself on the wire.
                "MsgType": FieldAccess.first_named(kwargs, 35, "MsgType", rows),
            }
        )
        schema = cls._message_schema(batch.schema)
        for field in schema:
            columns.setdefault(field.name, pyarrow.nulls(rows, field.type))
        columns.update(cls._resolved_batch_columns(columns, codec, rows))
        columns["mic"] = _mic_arrow(columns, messages, rows)
        return cls.identified(columns, schema, rows)

    @classmethod
    def _message_schema(cls, source: pyarrow.Schema) -> pyarrow.Schema:
        """The FixMsg contract followed by caller-declared raw columns."""
        schema = cls.into_field().into_arrow_schema()
        own = set(schema.names)
        raw = set(Message.into_field().names)
        collisions = [name for name in source.names if name in own and name not in raw]
        if collisions:
            raise ValueError(
                f"raw message columns collide with FixMsg fields {collisions}; "
                "rename the caller-declared columns"
            )
        extra = [field for field in source if field.name not in own]
        return pyarrow.schema([*schema, *extra], metadata=schema.metadata)

    @classmethod
    def _resolved_batch_columns(
        cls, columns: Mapping[str, Any], codec: Any, rows: int
    ) -> dict[str, Any]:
        """Resolve each version-homogeneous slice and restore batch order."""
        compute = pyarrow.compute
        versions = compute.fill_null(columns["protocol_version"], "")
        parts, positions = [], []
        for version, where in groups_of(versions):
            taken = {
                name: column if len(where) == rows else compute.take(column, where)
                for name, column in columns.items()
            }
            parts.append(cls._resolved_columns(taken, codec, version.as_py() or None, len(where)))
            positions.append(where)
        if not parts:
            return {}
        return {name: scattered([part[name] for part in parts], positions) for name in parts[0]}

    @classmethod
    def _resolved_columns(
        cls, columns: Mapping[str, Any], codec: Any, version: str | None, rows: int
    ) -> dict[str, Any]:
        """One homogeneous slice: `kwargs` completed, and what it gives up to columns."""
        kwargs = codec.complete_kwargs(columns["kwargs"], version)
        components, kwargs = codec.into_component_columns(kwargs, version)
        lifted, kwargs = codec.into_lifted_columns(kwargs, version)
        found: dict[str, Any] = {"kwargs": kwargs, **components, **lifted}
        # A lifted value only fills a column already read directly where it is empty:
        # `MsgType` is read off the front of the message before any of this,
        # and the wire is the authority on what it says.
        for name, column in found.items():
            stored = columns.get(name)
            if name != "kwargs" and stored is not None and stored.null_count < rows:
                found[name] = pyarrow.compute.coalesce(cast_arrow_fix(column, stored.type), stored)
        return found

    @classmethod
    def identified(
        cls, columns: dict[str, Any], schema: pyarrow.Schema, rows: int
    ) -> pyarrow.RecordBatch:
        """The envelope a row earns: its instrument, its time, its identity.

        The FIX conversion ends here so `hash`, transaction time and lifecycle
        identifiers are derived only after the full registry projection exists.
        """
        from rekep.market.transacted import resolve_arrow

        compute = pyarrow.compute
        columns["Symbol"] = cls.symbol_arrow(columns, rows)
        columns["code"] = cls.code_arrow(columns, rows)
        columns["codes"] = cls.codes_arrow(columns, rows)
        columns["reason"] = compute.coalesce(columns.get("Text"), columns["reason"])
        columns["unix"], columns["unix_source"] = resolve_arrow(columns, columns["runix"], rows)
        columns["unix_partition"] = unix_partition_arrow(columns["unix"])
        columns["cunix"] = columns["unix"]
        columns["hash"] = cls.version_hash_arrow(columns, rows)
        linked = compute.not_equal(columns["code"], "")
        columns["xhash"] = compute.if_else(linked, cls.hash_arrow(columns["code"]), columns["hash"])
        # `cast_arrow_fix` and not a plain cast, because the session columns
        # arrive as the text the wire carried: `20260814-09:30:00.123` is an
        # instant and `Y` is a boolean, and Arrow's own cast raises on both.
        return pyarrow.RecordBatch.from_arrays(
            [cast_arrow_fix(columns[name], schema.field(name).type) for name in schema.names],
            schema=schema,
        )

    @classmethod
    @functools.cache
    def into_digest_columns(cls) -> tuple[str, ...]:
        """What a stored row's `hash` is taken over, in this order.

        The **parsed** values and never the raw line, so a message reformatted
        but not changed hashes alike. `runix` is deliberately out and `unix`
        deliberately in: when a line was written down is not what it says, and
        a re-parse that resolves the instant from a different rung has learnt
        something new about the row.
        """
        return (
            "unix",
            "unix_source",
            "source_url",
            "source_rownum",
            "protocol_code",
            "protocol_version",
            "MsgType",
            "kwargs",
        )

    @classmethod
    def version_hash_arrow(cls, columns: Mapping[str, Any], rows: int) -> pyarrow.Array:
        """One digest per row, over the parsed values rather than the raw line.

        A row that could not be read as a message has no parsed values, so it
        hashes on the raw line instead -- which is the one stated exception to
        the rule, and honest: for such a row the raw string *is* the content.
        """
        compute = pyarrow.compute
        parsed = [_digest_text(columns.get(name), rows) for name in cls.into_digest_columns()]
        digests = cls.hash_arrow(*parsed)
        stored = columns.get("kwargs")
        if stored is None:
            return digests
        unread = compute.is_null(stored)
        if not compute.any(unread, min_count=0).as_py():
            return digests
        raw = cls.hash_arrow(
            _digest_text(columns.get("message"), rows),
            _digest_text(columns.get("source_url"), rows),
            _digest_text(columns.get("source_rownum"), rows),
        )
        return compute.if_else(unread, raw, digests)

    @classmethod
    def code_arrow(cls, columns: Mapping[str, Any], rows: int) -> pyarrow.Array:
        """Best readable lifecycle identifier available in parsed FIX columns."""
        return _first_text(columns, cls.into_code_columns(), rows)

    @classmethod
    def codes_arrow(cls, columns: Mapping[str, Any], rows: int) -> pyarrow.Array:
        """Every parsed identifier as one ordered Arrow map per row."""
        compute = pyarrow.compute
        residual = (
            FieldAccess.first_arrow_fields(
                columns.get("kwargs"),
                tuple((tag, field) for _, field, tag in IDENTIFIER_FIELDS),
                rows,
            )
            if columns.get("kwargs") is not None
            else {}
        )
        available = []
        for stored, field, _ in IDENTIFIER_FIELDS:
            promoted = columns.get(field)
            fallback = residual.get(field)
            if promoted is None and fallback is None:
                continue
            values = [
                cast_arrow_fix(column, pyarrow.string())
                for column in (promoted, fallback)
                if column is not None
            ]
            available.append((stored, compute.coalesce(*values)))
        names, values = zip(*available, strict=True) if available else ((), ())
        if not rows or not names:
            return build_map(
                CODES_TYPE,
                pyarrow.repeat(pyarrow.scalar(0, pyarrow.int64()), rows),
                pyarrow.array([], pyarrow.string()),
                pyarrow.array([], pyarrow.string()),
            )
        flat, member = interleave(list(values), rows)
        present = compute.fill_null(
            compute.and_(
                compute.is_valid(flat),
                compute.greater(compute.binary_length(compute.utf8_trim_whitespace(flat)), 0),
            ),
            False,
        )
        width = len(names)
        running = compute.cumulative_sum(present.cast(pyarrow.int64()))
        ends = compute.take(
            running,
            compute.add(
                compute.multiply(sequence(rows), pyarrow.scalar(width, pyarrow.int64())),
                pyarrow.scalar(width - 1, pyarrow.int64()),
            ),
        )
        before = pyarrow.concat_arrays(
            [pyarrow.array([0], pyarrow.int64()), ends.slice(0, rows - 1)]
        )
        sizes = compute.subtract(ends, before)
        return build_map(
            CODES_TYPE,
            sizes,
            compute.filter(compute.take(pyarrow.array(names), member), present),
            compute.filter(flat, present),
        )

    @classmethod
    def symbol_arrow(cls, columns: Mapping[str, Any], rows: int) -> pyarrow.Array:
        """Most relevant readable instrument identifier on each row."""
        found = _first_text(columns, cls.into_symbol_columns(), rows)
        return pyarrow.compute.if_else(
            pyarrow.compute.equal(found, ""), pyarrow.nulls(rows, pyarrow.string()), found
        )

    @classmethod
    @functools.cache
    def into_tagged_columns(cls) -> tuple[tuple[str, str], ...]:
        """`(attribute, wire tag)` for every declared column FIX numbers.

        Read off the declaration once per class rather than per row: which
        columns carry a tag is a fact about the shape, and asking each of a
        hundred fields for its metadata again on every line was the largest
        single cost of turning a parsed log back into FIX.
        """
        return tuple(
            (member.name, tag)
            for member in cls.into_field().fields
            if (tag := member.fix.get("tag")) is not None
        )

    def into_fix_events(self, **declared: Any) -> Any:
        """Rebuild the FIX market reader from promoted columns and residual pairs."""
        from rekep.market.fix import FixEvents

        carried = {"runix": self.runix or self.unix, "mic": self.mic, **declared}
        pairs: list[tuple[Any, Any]] = [
            (tag, value)
            for name, tag in type(self).into_tagged_columns()
            if (value := getattr(self, name, None)) is not None
        ]
        pairs.extend(_stored_pairs(self.kwargs))
        if pairs:
            built = FixEvents.from_pairs(pairs, **carried)
        elif self.message:
            built = FixEvents.from_text(self.message, **carried)
        else:
            built = FixEvents(**carried)
        return self._transacted(built)

    def _transacted(self, built: Any) -> Any:
        """Hand the reader the transaction time this row already resolved.

        Not re-derived, and it could not be: the regulatory groups the chain
        reads first are lifted into typed columns of their own, so a reader
        rebuilt from the residual pairs alone has lost them and would fall
        down the chain to a weaker clock. The parse stage resolved it once and
        stored which rung answered; this is that answer being consumed.
        """
        from rekep.market.transacted import Transacted

        if self.unix_source:
            built.__dict__["transacted"] = Transacted(self.unix, self.unix_source)
        return built

    def into_market_events(self, **declared: Any) -> Iterator[Any]:
        """Translate this parsed row into its ordered market events."""
        for event in self.into_fix_events(**declared):
            if self.reason and not event.reason:
                event.reason = self.reason
                event.hash = NIL
                event.identify()
            yield event

    @classmethod
    def into_market_arrow_batches(
        cls,
        source: pyarrow.RecordBatch | pyarrow.RecordBatchReader | Iterable[pyarrow.RecordBatch],
        batch_row_size: int | None = 65_536,
        **declared: Any,
    ) -> Iterator[tuple[type[Any], pyarrow.RecordBatch]]:
        """Adapt parsed rows into typed Order and Execution Arrow batches.

        This is a streaming boundary, not a second kernel translator: scalar
        `FixEvents` remains the authority for groups, normalization, and event
        identity. Each event type retains message order. `None` drains each
        type only at the end for a caller requiring one atomic commit.
        """
        from rekep.market.orders import Execution, Order

        if batch_row_size is not None and batch_row_size <= 0:
            raise ValueError("batch_row_size must be positive")
        batches = (source,) if isinstance(source, pyarrow.RecordBatch) else source
        event_types = (Order, Execution)
        held: dict[type[Any], list[Any]] = {event_type: [] for event_type in event_types}

        def drained(event_type: type[Any]) -> Iterator[tuple[type[Any], pyarrow.RecordBatch]]:
            events = held[event_type]
            if not events:
                return
            yield from (
                (event_type, batch)
                for batch in event_type.into_arrow_reader(
                    events, batch_row_size=batch_row_size or len(events)
                )
            )
            events.clear()

        for message in cls.from_arrow_reader(batches):
            for event in message.into_market_events(**declared):
                if isinstance(event, Order):
                    event_type = Order
                elif isinstance(event, Execution):
                    event_type = Execution
                else:
                    continue
                events = held[event_type]
                events.append(event)
                if batch_row_size is not None and len(events) >= batch_row_size:
                    yield from drained(event_type)
        for event_type in event_types:
            yield from drained(event_type)

    def into_instruments(self, **declared: Any) -> Iterator[Any]:
        """Yield distinct instrument facts, synthesizing a symbol-only row when needed."""
        if self.is_instrument_version and self.get(_INSTRUMENT_KIND):
            yield self._normalized_instrument()
            return
        translated = tuple(self.into_fix_events(**declared).into_instruments())
        if not translated:
            synthetic = self._flat_instrument()
            translated = () if synthetic is None else (synthetic,)
        if self.is_instrument_version:
            yield from (self._instrument_version(instrument) for instrument in translated)
        else:
            yield from translated

    def into_instrument(self, **declared: Any) -> Any | None:
        """Build one normalized instrument version or the best facts on this row."""
        return next(self.into_instruments(**declared), None)

    @classmethod
    def into_instrument_arrow_batch(cls, batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        """Decode normalized instrument rows as one Arrow batch."""
        from rekep.market.instrument import Instrument

        if not isinstance(batch, pyarrow.RecordBatch):
            raise TypeError(
                f"instrument conversion needs a RecordBatch, got {type(batch).__name__}"
            )
        target = Instrument.into_field()
        columns = {name: batch.column(name) for name in target.names if name in batch.schema.names}
        normalized = _NormalizedInstrumentFields.from_array(batch.column("kwargs"), batch.num_rows)
        columns.update(
            {
                "etype": pyarrow.repeat(
                    pyarrow.scalar(int(EventType.INSTRUMENT), _EVENT_CODE), batch.num_rows
                ),
                "symbol": pyarrow.compute.fill_null(batch.column("Symbol"), ""),
                "kind": _stored_code(normalized.first(_INSTRUMENT_KIND)),
                "security_id": batch.column("SecurityID"),
                "security_id_source": batch.column("SecurityIDSource"),
                "isin_code": batch.column("ISINCODE"),
                "alt_ids": normalized.alt_ids(target.field("alt_ids").arrow_type),
                "security_type": batch.column("SecurityType"),
                "cfi": batch.column("CFICode"),
                "exchange": batch.column("SecurityExchange"),
                "currency": _currency_arrow(batch.column("Currency")),
                "multiplier": cast_arrow_fix(
                    normalized.first("ContractMultiplier"), pyarrow.float64()
                ),
                "tick": cast_arrow_fix(normalized.first("MinPriceIncrement"), pyarrow.float64()),
                "lot": cast_arrow_fix(normalized.first("RoundLot"), pyarrow.float64()),
                "maturity": cast_arrow_fix(normalized.first("MaturityDate"), pyarrow.date32()),
                "strike": cast_arrow_fix(normalized.first("StrikePrice"), pyarrow.float64()),
                "option_kind": _fix_enum_arrow(normalized.first("PutOrCall"), OptionKind),
                "label": normalized.first("SecurityDesc"),
                "legs": normalized.legs(target.field("legs").arrow_type),
            }
        )
        raw = pyarrow.RecordBatch.from_arrays(
            [columns[name] for name in target.names], names=target.names
        )
        return target.cast_arrow_batch(raw)

    def _flat_instrument(self) -> Any | None:
        """Build only the instrument facts already promoted on this row."""
        from rekep.market.instrument import Instrument

        symbol = self.Symbol or self.SecurityID or self.ISINCODE or ""
        if not symbol:
            return None
        return Instrument(
            symbol=symbol,
            security_id=self.SecurityID,
            security_id_source=self.SecurityIDSource,
            isin_code=self.ISINCODE,
            security_type=self.SecurityType,
            cfi=self.CFICode,
            exchange=self.SecurityExchange,
            currency=self.Currency,
        )

    def _normalized_instrument(self) -> Any:
        """Decode one package-authored instrument through the columnar path."""
        from rekep.market.instrument import Instrument

        source = next(iter(type(self).into_arrow_reader((self,), batch_row_size=1)))
        row = type(self).into_instrument_arrow_batch(source).to_pylist()[0]
        return Instrument.from_dict(row)

    def _instrument_version(self, instrument: Any) -> Any:
        """Put decoded facts back on the lifecycle envelope this FixMsg carries."""
        return dataclasses.replace(
            instrument,
            unix=self.unix,
            unix_partition=self.unix_partition,
            etype=EventType.INSTRUMENT,
            cunix=self.cunix,
            runix=self.runix,
            eunix=self.eunix,
            sunix=self.sunix,
            hash=self.hash,
            xhash=self.xhash,
            linked_events=list(self.linked_events),
            version=self.version,
            state=self.state,
            code=self.code,
            codes=dict(self.codes),
            prev_unix=self.prev_unix,
            parent_hash=None if self.parent_hash is None else list(self.parent_hash),
            mic=self.mic,
            reason=self.reason,
        )


#: What a FIX-carrying trading log is made of, by the two spellings every one
#: of them uses: the wire `35=` message type, and the name a rendered log
#: prints. Ordered most specific first, because the first match wins and a
#: single line can name more than one of them -- an execution report quoting
#: the order it fills says `ExecutionReport` *and* `NewOrderSingle`.
DEFAULT_RULES: tuple[MessageRule, ...] = (
    MessageRule(
        r"35=8(\D|$)",
        EventType.EXECUTION,
        "a fill, or a report of one",
        [r"ExecutionReport"],
    ),
    MessageRule(
        r"35=[DFG](\D|$)",
        EventType.ORDER,
        "an order, or an amendment to one",
        [r"NewOrderSingle", r"OrderCancel(Request|Replace)"],
    ),
    MessageRule(
        r"35=X(\D|$)",
        EventType.BOOK,
        "an incremental book update",
        [r"MarketDataIncrementalRefresh"],
    ),
    MessageRule(
        r"35=W(\D|$)",
        EventType.BOOK,
        "a full book snapshot",
        [r"MarketDataSnapshot"],
    ),
    MessageRule(
        r"35=(?:AG|AH|AI|AJ|[RSZabi])(?:[^A-Za-z0-9]|$)",
        EventType.QUOTE,
        "a quote lifecycle message",
        [
            r"\b(?:MassQuote(?:Acknowledgement)?|Quote(?:RequestReject|StatusRequest|"
            r"StatusReport|Response|Cancel|Request)?|RFQRequest)\b"
        ],
    ),
    MessageRule(
        r"35=d(\D|$)",
        EventType.INSTRUMENT,
        "reference data",
        [r"SecurityDefinition"],
    ),
)


def _first_text(columns: Mapping[str, Any], names: Sequence[str], rows: int) -> pyarrow.Array:
    """First nonblank value in `names`, preserving its original spelling."""
    compute = pyarrow.compute
    found: Any = pyarrow.nulls(rows, pyarrow.string())
    for name in names:
        value = columns.get(name)
        if value is None or value.null_count == rows:
            continue
        value = value.cast(pyarrow.string(), safe=False)
        present = compute.and_(
            compute.is_valid(value),
            compute.not_equal(compute.utf8_trim_whitespace(value), ""),
        )
        use = compute.and_(compute.is_null(found), compute.fill_null(present, False))
        found = compute.if_else(use, value, found)
        if found.null_count == 0:
            break
    return compute.fill_null(found, "")


def _mic_arrow(columns: Mapping[str, Any], messages: Any, rows: int) -> pyarrow.Array:
    """ISO exchange fields, then direction-aware FIX session endpoints."""
    compute = pyarrow.compute
    missing = pyarrow.nulls(rows, pyarrow.string())
    stored = columns.get("kwargs")
    tags = (
        FieldAccess.first_arrow_tags(stored, (30, 100, 275, 1301), rows)
        if stored is not None
        else {}
    )
    explicit = [
        tags.get(30, missing),
        columns.get("SecurityExchange", missing),
        tags.get(100, missing),
        tags.get(275, missing),
        tags.get(1301, missing),
    ]
    explicit = [value for value in explicit if value.null_count < rows]
    venue = MIC.arrow_from_strings(*explicit) if explicit else pyarrow.nulls(rows, pyarrow.int32())
    sender_source = columns.get("SenderCompID", missing)
    target_source = columns.get("TargetCompID", missing)
    sender = (
        MIC.arrow_from_strings(sender_source)
        if sender_source.null_count < rows
        else pyarrow.nulls(rows, pyarrow.int32())
    )
    target = (
        MIC.arrow_from_strings(target_source)
        if target_source.null_count < rows
        else pyarrow.nulls(rows, pyarrow.int32())
    )
    if sender.null_count == rows:
        return compute.coalesce(venue, target)
    if target.null_count == rows:
        return compute.coalesce(venue, sender)
    text = messages.cast(pyarrow.string(), safe=False)
    outbound = compute.fill_null(
        compute.match_substring_regex(text, r"(?i)\b(?:send|sending|sent|outbound)\b"), False
    )
    inbound = compute.fill_null(
        compute.match_substring_regex(text, r"(?i)\b(?:recv|receive|received|receiving|inbound)\b"),
        False,
    )
    directed = compute.if_else(
        outbound,
        target,
        compute.if_else(inbound, sender, pyarrow.nulls(rows, pyarrow.int32())),
    )
    return compute.coalesce(venue, directed, target, sender)


_NORMALIZED_GROUP = re.compile(r"^(?P<group>NoLegs|NoSecurityAltID)\[(?P<index>[0-9]+)\]")
_GROUP_STRIDE = 1 << 32


@dataclasses.dataclass(frozen=True)
class _NormalizedInstrumentFields:
    """Column views over the normalized fields stored in `FixMsg.kwargs`."""

    rows: int
    parents: pyarrow.Array
    keys: pyarrow.Array
    values: pyarrow.Array
    paths: pyarrow.Array
    groups: pyarrow.Array
    group_ids: pyarrow.Array

    @classmethod
    def from_array(cls, stored: pyarrow.Array, rows: int) -> _NormalizedInstrumentFields:
        compute = pyarrow.compute
        entries = compute.list_flatten(stored)
        parents = compute.list_parent_indices(stored).cast(pyarrow.int64())
        keys = compute.struct_field(entries, "key")
        values = compute.struct_field(entries, "value")
        namespace = compute.struct_field(entries, "namespace")
        component = compute.struct_field(entries, "comp")
        prefix = compute.coalesce(component, namespace)
        prefixed = compute.fill_null(compute.greater(compute.binary_length(prefix), 0), False)
        paths = compute.if_else(
            prefixed,
            compute.binary_join_element_wise(prefix, keys, "."),
            keys,
        )
        group_parts = compute.extract_regex(paths, _NORMALIZED_GROUP.pattern)
        groups = compute.struct_field(group_parts, "group")
        indices = cast_arrow_fix(compute.struct_field(group_parts, "index"), pyarrow.int64())
        group_ids = compute.add(
            compute.multiply(parents, pyarrow.scalar(_GROUP_STRIDE, pyarrow.int64())),
            compute.fill_null(indices, 0),
        )
        return cls(rows, parents, keys, values, paths, groups, group_ids)

    def first(self, path: str) -> pyarrow.Array:
        """First value of one exact normalized path per row."""
        compute = pyarrow.compute
        matches = compute.fill_null(compute.equal(self.paths, path), False)
        if not compute.any(matches, min_count=0).as_py():
            return pyarrow.nulls(self.rows, pyarrow.string())
        parents = compute.filter(self.parents, matches)
        values = compute.filter(self.values, matches)
        return compute.take(values, compute.index_in(sequence(self.rows), value_set=parents))

    def _roots(
        self, group: str, key: str, *, nonblank: bool = False
    ) -> tuple[pyarrow.Array, pyarrow.Array, pyarrow.Array]:
        """Stored group ids, row ids and delimiter values in wire order."""
        compute = pyarrow.compute
        matches = compute.and_(compute.equal(self.groups, group), compute.equal(self.keys, key))
        if nonblank:
            matches = compute.and_(
                matches,
                compute.fill_null(
                    compute.greater(
                        compute.binary_length(compute.utf8_trim_whitespace(self.values)), 0
                    ),
                    False,
                ),
            )
        matches = compute.fill_null(matches, False)
        return (
            compute.filter(self.group_ids, matches),
            compute.filter(self.parents, matches),
            compute.filter(self.values, matches),
        )

    def _member(self, roots: pyarrow.Array, group: str, key: str) -> pyarrow.Array:
        """First `key` aligned to each selected normalized group entry."""
        compute = pyarrow.compute
        matches = compute.fill_null(
            compute.and_(compute.equal(self.groups, group), compute.equal(self.keys, key)),
            False,
        )
        ids = compute.filter(self.group_ids, matches)
        values = compute.filter(self.values, matches)
        if not len(values):
            return pyarrow.nulls(len(roots), pyarrow.string())
        return compute.take(values, compute.index_in(roots, value_set=ids))

    def alt_ids(self, arrow_type: pyarrow.DataType | None) -> pyarrow.Array:
        """Alternative identifiers as one nullable Arrow map per row."""
        assert arrow_type is not None
        roots, parents, values = self._roots("NoSecurityAltID", "SecurityAltID", nonblank=True)
        sizes = dense_counts(parents, self.rows)
        sources = _id_source_name_arrow(
            self._member(roots, "NoSecurityAltID", "SecurityAltIDSource")
        )
        return build_map(
            arrow_type,
            sizes,
            sources,
            values,
            mask=pyarrow.compute.equal(sizes, 0),
        )

    def legs(self, arrow_type: pyarrow.DataType | None) -> pyarrow.Array:
        """Normalized leg groups as one nullable Arrow list per row."""
        assert arrow_type is not None
        roots, parents, xhash = self._roots("NoLegs", "xhash")
        sizes = dense_counts(parents, self.rows)

        def member(name: str) -> pyarrow.Array:
            return self._member(roots, "NoLegs", name)

        item = arrow_type.value_type
        columns = {
            "xhash": pyarrow.compute.fill_null(cast_arrow_fix(xhash, pyarrow.int64()), 0),
            "symbol": pyarrow.compute.fill_null(member("LegSymbol"), ""),
            "side": _fix_enum_arrow(member("LegSide"), Side),
            "ratio": cast_arrow_fix(member("LegRatioQty"), pyarrow.float64()),
            "kind": _stored_code(member("kind")),
            "security_id": member("LegSecurityID"),
            "security_id_source": member("LegSecurityIDSource"),
            "cfi": member("LegCFICode"),
            "security_type": member("LegSecurityType"),
            "exchange": member("LegSecurityExchange"),
            "currency": _currency_arrow(member("LegCurrency")),
            "multiplier": cast_arrow_fix(member("LegContractMultiplier"), pyarrow.float64()),
            "maturity": cast_arrow_fix(member("LegMaturityDate"), pyarrow.date32()),
            "strike": cast_arrow_fix(member("LegStrikePrice"), pyarrow.float64()),
            "option_kind": _fix_enum_arrow(member("LegPutOrCall"), OptionKind),
        }
        entries = pyarrow.StructArray.from_arrays(
            [columns[item.field(index).name] for index in range(item.num_fields)],
            fields=[item.field(index) for index in range(item.num_fields)],
        )
        return build_list(
            arrow_type,
            sizes,
            entries,
            mask=pyarrow.compute.equal(sizes, 0),
        )


def _stored_code(values: pyarrow.Array) -> pyarrow.Array:
    """A stored stable integer code, with malformed values degraded to unknown."""
    return pyarrow.compute.fill_null(cast_arrow_fix(values, pyarrow.int32()), 0)


@functools.cache
def _fix_enum_arrays(enum_type: type[Any]) -> tuple[pyarrow.Array, pyarrow.Array]:
    """FIX spellings and stable integer values for one enum declaration."""
    declared = {spelling: int(member) for member in enum_type if (spelling := member.into_fix())}
    return (
        pyarrow.array(declared, pyarrow.string()),
        pyarrow.array(declared.values(), pyarrow.int32()),
    )


def _fix_enum_arrow(values: pyarrow.Array, enum_type: type[Any]) -> pyarrow.Array:
    """FIX enum spellings decoded through cached Arrow lookup arrays."""
    compute = pyarrow.compute
    spellings, codes = _fix_enum_arrays(enum_type)
    text = compute.utf8_trim_whitespace(values.cast(pyarrow.string(), safe=False))
    return compute.fill_null(
        compute.take(codes, compute.index_in(text, value_set=spellings)), 0
    ).cast(pyarrow.int32())


@functools.cache
def _id_source_arrays() -> tuple[pyarrow.Array, pyarrow.Array]:
    """FIX identifier-source spellings and their persisted names."""
    declared = {spelling: member.name for member in IdSource if (spelling := member.into_fix())}
    return pyarrow.array(declared, pyarrow.string()), pyarrow.array(
        declared.values(), pyarrow.string()
    )


def _id_source_name_arrow(values: pyarrow.Array) -> pyarrow.Array:
    """Identifier source spellings as stable names, preserving unknown codes."""
    compute = pyarrow.compute
    spellings, names = _id_source_arrays()
    trimmed = compute.utf8_trim_whitespace(values.cast(pyarrow.string(), safe=False))
    known = compute.take(names, compute.index_in(trimmed, value_set=spellings))
    fallback = compute.if_else(
        compute.fill_null(compute.greater(compute.binary_length(values), 0), False),
        values,
        pyarrow.scalar(IdSource.UNKNOWN.name),
    )
    return compute.coalesce(known, fallback)


def _currency_arrow(values: pyarrow.Array) -> pyarrow.Array:
    """Canonical normalized currency text packed into its persisted int32."""
    compute = pyarrow.compute
    text = values.cast(pyarrow.string(), safe=False)
    canonical = compute.utf8_upper(compute.utf8_trim_whitespace(text))
    valid = compute.fill_null(compute.match_substring_regex(canonical, r"^[A-Z]{3}$"), False)
    packed_text = compute.binary_join_element_wise(canonical, "0", "")
    alphabet = pyarrow.array(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
    packed = pyarrow.repeat(pyarrow.scalar(0, pyarrow.int32()), len(values))
    for index, multiplier in enumerate((1 << 24, 1 << 16, 1 << 8, 1)):
        if index == 3:
            byte = pyarrow.repeat(pyarrow.scalar(ord("0"), pyarrow.int32()), len(values))
        else:
            character = compute.utf8_slice_codeunits(packed_text, start=index, stop=index + 1)
            byte = compute.add(compute.index_in(character, value_set=alphabet), 65).cast(
                pyarrow.int32()
            )
        packed = compute.add(packed, compute.multiply(byte, multiplier)).cast(pyarrow.int32())
    unknown = compute.if_else(
        compute.is_null(values),
        pyarrow.scalar(None, pyarrow.int32()),
        pyarrow.scalar(0, pyarrow.int32()),
    )
    return compute.if_else(valid, packed, unknown)


def _instrument_pairs(instrument: Any) -> list[tuple[str, str]] | None:
    """Registry-shaped fields not already promoted on a normalized FixMsg."""
    values = (
        (_INSTRUMENT_KIND, int(instrument.kind)),
        ("ContractMultiplier", instrument.multiplier),
        ("MinPriceIncrement", instrument.tick),
        ("RoundLot", instrument.lot),
        ("MaturityDate", instrument.maturity),
        ("StrikePrice", instrument.strike),
        ("PutOrCall", instrument.option_kind),
        ("SecurityDesc", instrument.label),
    )
    pairs = [(name, rendered) for name, value in values if (rendered := _fix_text(value))]

    alternatives = dict(instrument.alt_ids or {})
    if instrument.isin_code and not (
        instrument.security_id == instrument.isin_code
        and _id_source(instrument.security_id_source) == "4"
    ):
        alternatives.setdefault("ISIN", instrument.isin_code)
    if alternatives:
        pairs.append(("NoSecurityAltID", str(len(alternatives))))
        for index, (source, value) in enumerate(sorted(alternatives.items())):
            root = f"NoSecurityAltID[{index}]"
            pairs.extend(
                (
                    (f"{root}.SecurityAltID", str(value)),
                    (f"{root}.SecurityAltIDSource", _id_source(source)),
                )
            )

    if instrument.legs:
        pairs.append(("NoLegs", str(len(instrument.legs))))
        for index, leg in enumerate(instrument.legs):
            root = f"NoLegs[{index}]"
            members = (
                (_INSTRUMENT_XHASH, leg.xhash),
                (_INSTRUMENT_KIND, int(leg.kind)),
                ("LegSymbol", leg.symbol),
                ("LegSide", leg.side),
                ("LegRatioQty", leg.ratio),
                ("LegSecurityID", leg.security_id),
                ("LegSecurityIDSource", leg.security_id_source),
                ("LegCFICode", leg.cfi),
                ("LegSecurityType", leg.security_type),
                ("LegSecurityExchange", leg.exchange),
                ("LegCurrency", leg.currency),
                ("LegContractMultiplier", leg.multiplier),
                ("LegMaturityDate", leg.maturity),
                ("LegStrikePrice", leg.strike),
                ("LegPutOrCall", leg.option_kind),
            )
            pairs.extend(
                (f"{root}.{name}", rendered)
                for name, value in members
                if (rendered := _fix_text(value))
            )
    return pairs or None


def _stored_entries(entries: Sequence[Any] | None) -> list[dict[str, Any]] | None:
    """Stored fields in the spelling Arrow accepts without a shape pass."""
    return (
        None if entries is None else [FixKwarg.from_stored(entry).into_dict() for entry in entries]
    )


def _fix_text(value: Any) -> str:
    """One normalized value in the spelling a named FIX pair accepts."""
    if value is None:
        return ""
    if isinstance(value, datetime.date):
        return value.strftime("%Y%m%d")
    if isinstance(value, bool):
        return "Y" if value else "N"
    spelling = getattr(value, "into_fix", None)
    return str(spelling() if callable(spelling) else value)


def _id_source(value: Any) -> str:
    """An identifier scheme name or wire character as its FIX value."""
    from rekep.enums import IdSource

    text = "" if value is None else str(value)
    named = IdSource.__members__.get(text.strip().upper())
    return named.into_fix() if named is not None and named.into_fix() else text


def _stored_pairs(entries: Sequence[Any] | None) -> Iterator[tuple[Any, Any]]:
    """Stored fields as the pairs a FIX reader addresses them by.

    The tag where the dictionary found one, and the rendered key -- name with
    whatever stood in front of it joined back -- where it did not. A plain
    `(key, value)` tuple is accepted as itself, so a caller writing a
    `FixMsg` by hand need not spell the whole struct out.
    """
    for entry in entries or ():
        if not isinstance(entry, Mapping):
            key, value = entry
            yield key, value
            continue
        if entry.get("tag"):
            yield entry["tag"], entry.get("value")
            continue
        lead = entry.get("namespace") or entry.get("comp")
        name = entry["key"]
        yield (f"{lead}.{name}" if lead else name), entry.get("value")


def _digest_text(column: Any, rows: int) -> pyarrow.Array:
    """One column as the text its digest is taken over.

    Text for every member, so the tuple hashes the same way whatever Arrow
    type each column happens to be -- and a stored list of fields hashes as
    the fields it holds, in order, rather than as an address.
    """
    compute = pyarrow.compute
    if column is None:
        return pyarrow.repeat("", rows)
    if isinstance(column, pyarrow.ChunkedArray):
        column = column.combine_chunks()
    if pyarrow.types.is_list(column.type):
        return _stored_text(column, rows)
    return compute.fill_null(column.cast(pyarrow.string(), safe=False), "")


def _stored_text(column: Any, rows: int) -> pyarrow.Array:
    """A `KWARGS` column as one string per row: every field, in wire order."""
    compute = pyarrow.compute
    entries = compute.list_flatten(column)
    spelled = compute.binary_join_element_wise(
        compute.fill_null(compute.struct_field(entries, "tag").cast(pyarrow.string()), ""),
        compute.fill_null(compute.struct_field(entries, "comp"), ""),
        compute.fill_null(compute.struct_field(entries, "namespace"), ""),
        compute.fill_null(compute.struct_field(entries, "key"), ""),
        compute.fill_null(compute.struct_field(entries, "value"), ""),
        "\x1f",
    )
    lengths = compute.fill_null(compute.list_value_length(column), 0).cast(pyarrow.int32())
    offsets = pyarrow.concat_arrays(
        [pyarrow.array([0], pyarrow.int32()), compute.cumulative_sum(lengths)]
    )
    listed = pyarrow.ListArray.from_arrays(offsets, spelled)
    joined = compute.binary_join(listed, "\x1e")
    del rows
    return compute.fill_null(joined, "")
