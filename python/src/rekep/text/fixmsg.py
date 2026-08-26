"""The shape of one parsed log line, what decides which event it is, and what fills it."""

from __future__ import annotations

import dataclasses
import datetime
import functools
import re
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
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
    Kwarg,
)
from rekep.fix.components import (
    LEGS,
    PARTIES,
    SECURITY_ALT_IDS,
    SIDE_TRD_REG_TIMESTAMPS,
    TRD_REG_TIMESTAMPS,
    Leg,
    Party,
    SideTrdRegTimestamp,
    TrdRegTimestamp,
)
from rekep.fix.components import (
    # Aliased because `FixMsg` names its column after the member that opens an
    # entry, and an annotation spelling the bare class name would resolve to
    # that column's own default under `get_type_hints`.
    SecurityAltID as SecurityAltIDEntry,
)
from rekep.fix.fields import cast_arrow_fix
from rekep.fix.message import (
    group_pairs,
    indexed_group_pairs,
    normalized_pairs,
    parse_pairs,
    render_fix_value,
)
from rekep.fix.registry import FixRegistry
from rekep.fix.rules import NO_PROTOCOL
from rekep.fix.transcribe import NO_SOURCE, infer_version_from_pairs
from rekep.market.event import CODES_TYPE, Event, unix_partition_arrow
from rekep.market.identity import NIL
from rekep.text.message import Message

_EVENT_CODE = pyarrow.int32()
_CONTRACT_METADATA = MappingProxyType({"version": "1"})
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
_COMPONENT_GROUPS: tuple[tuple[str, str, type[Any]], ...] = (
    ("Parties", "NoPartyIDs", Party),
    ("TrdRegTimestamps", "NoTrdRegTimestamps", TrdRegTimestamp),
    ("SideTrdRegTS", "NoSideTrdRegTS", SideTrdRegTimestamp),
    ("SecurityAltID", "NoSecurityAltID", SecurityAltIDEntry),
    ("Legs", "NoLegs", Leg),
)

#: The parsed columns that hold one structured component each. What the
#: market translator checks before taking its flat shortcut, published here so
#: a component added above is one edit and not a hunt for hardcoded tuples.
COMPONENT_COLUMNS: tuple[str, ...] = tuple(column for column, _, _ in _COMPONENT_GROUPS)


@functools.cache
def _row_access() -> FieldAccess:
    """The accessor a stored row reads through: the cross-version dictionary."""
    return FieldAccess.of(FixRegistry.from_builtin(), None)


@scalar(slots=True)
class FixMsg(Message):
    """One raw message transcribed under the FIX registry."""

    @classmethod
    def from_(cls, source: Any, *args: Any, **kwargs: Any) -> FixMsg:
        """Build text and bytes as FIX payloads; document readers stay explicit."""
        if isinstance(source, str | bytes):
            return cls.from_text(source, *args, **kwargs)
        return super().from_(source, *args, **kwargs)

    @classmethod
    @functools.cache
    def into_redirects(cls) -> Mapping[Any, str]:
        """Generic conversions plus direct text parsing."""
        return MappingProxyType({**Message.into_redirects(), str: "text", bytes: "text"})

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
            self.kwargs = [Kwarg.from_stored(entry) for entry in self.kwargs]
        if (
            self.protocol_version is None
            and self.protocol_code == NO_PROTOCOL
            and (self.BeginString or self.kwargs)
        ):
            evidence: list[tuple[str, Any]] = []
            if self.BeginString:
                evidence.append(("8", self.BeginString))
            if self.ApplVerID:
                evidence.append(("1128", self.ApplVerID))
            evidence.extend(_stored_pairs(self.kwargs))
            version, source = infer_version_from_pairs(evidence)
            if version is not None:
                self.protocol_version = version
                self.protocol_version_source = source
                if self.protocol_code == NO_PROTOCOL:
                    self.protocol_code = "FIX"

    def identify(self) -> FixMsg:
        """Give the parsed event its lifecycle and version identities."""
        return Event.identify(self)

    # Nullable, and null on `fix.market`: typed columns plus `kwargs` carry
    # every field the line held, so keeping the raw string beside them would
    # store the same content twice. An all-null column run-length and dictionary encodes
    # to nothing on disk, which is what makes one stored shape across the
    # three tables affordable -- the same reasoning `_zeros` applies to the
    # envelope members a parsed line leaves unset.
    message: str | None = None
    """Payload text; null where parsed columns retain every field."""

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
    # message; an empty list means no residual or raw audit sidecar remains.
    kwargs: list[Kwarg] | None = None
    """Unlifted fields and lossless raw audit sidecars for typed columns."""

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
    # metadata. A lifted fact stays in `kwargs` only where typing loses text.

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

    # Last, and lists: what the instrument's two repeating groups carry. Last
    # because Iceberg counts leaf columns in declaration order for the bounds
    # it collects, and this contract already crosses that cutoff -- a nested
    # member declared earlier would push flat columns past it. The three
    # components above predate the cutoff being crossed; new ones go here.

    SecurityAltID: Annotated[
        list[SecurityAltIDEntry] | None,
        Field(
            arrow_type=SECURITY_ALT_IDS,
            metadata={"fix:component": "SecurityAltID"},
        ),
    ] = None
    """FIX SecAltIDGrp entries -- every other identifier; null when absent."""

    Legs: Annotated[
        list[Leg] | None,
        Field(
            arrow_type=LEGS,
            metadata={"fix:component": "Legs"},
        ),
    ] = None
    """FIX InstrmtLegGrp entries -- a multileg's legs; null when absent."""

    @classmethod
    def from_text(
        cls,
        text: str | bytes,
        separator: str | None = None,
        *,
        named: bool | None = None,
        entry_separator: str | None = None,
        **declared: Any,
    ) -> FixMsg:
        """Build a scalar parsed row from one ordered FIX payload."""
        pairs = parse_pairs(
            text,
            separator,
            named=named,
            entry_separator=entry_separator,
        )
        return cls._from_fix_pairs(pairs, **declared)

    @classmethod
    def from_pairs(
        cls,
        pairs: Iterable[tuple[Any, Any]],
        names: Mapping[str, int | str] | None = None,
        **declared: Any,
    ) -> FixMsg:
        """Build a scalar parsed row from ordered named or numbered fields."""
        return cls._from_fix_pairs(normalized_pairs(pairs, names), **declared)

    @classmethod
    def _from_fix_pairs(cls, pairs: Sequence[tuple[str, str]], **declared: Any) -> FixMsg:
        """Promote the discriminator while retaining every ordered field."""
        msg_type = declared.pop("MsgType", None)
        staged = Message(MsgType=msg_type, kwargs=list(pairs))
        if "etype" not in declared:
            if staged.MsgType is None:
                declared["etype"] = EventType.MISC
            else:
                declared["etype"] = (
                    FixRegistry.from_builtin()
                    .msg_type_event_types()
                    .get(staged.MsgType, EventType.UNKNOWN)
                )
        return cls(MsgType=staged.MsgType, kwargs=list(pairs), **declared)

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

    def into_fix_pairs(self, access: FieldAccess | None = None) -> list[tuple[str, str]]:
        """Ordered FIX fields projected once from columns, components, and `kwargs`.

        When an accessor resolves both a numeric field and a rendered field to
        one registry identity, the rendered occurrence is authoritative. Its
        position relative to other rendered fields is unchanged.
        """
        resolver = access or _row_access()
        pairs, resolved = self._canonical_pairs(resolver)
        return [
            (
                (tagged if access is not None else source)[0],
                _fix_text(resolver.canonical_value(tagged[0], source[1])),
            )
            for source, tagged in zip(pairs, resolved, strict=True)
        ]

    def _canonical_pairs(
        self, access: FieldAccess
    ) -> tuple[list[tuple[str, Any]], list[tuple[str, Any]]]:
        """Source-spelled and tag-resolved views, retaining stored value types."""
        stored = [(str(key), value) for key, value in _stored_pairs(self.kwargs)]
        stored_resolved = access.tagged_pairs(stored)
        stored_identities = {
            _pair_identity(pair[0])
            for source, pair in zip(stored, stored_resolved, strict=True)
            if not _component_key(source[0])
        }

        promoted = [
            (str(tag), value)
            for name, tag in type(self).into_tagged_columns()
            if (value := getattr(self, name, None)) is not None
        ]
        promoted.extend(
            (spelled, value)
            for name, spelled in type(self).into_named_columns()
            if (value := getattr(self, name, None)) is not None
        )
        promoted_resolved = access.tagged_pairs(promoted)
        promoted = [
            source
            for source, pair in zip(promoted, promoted_resolved, strict=True)
            if _pair_identity(pair[0]) not in stored_identities
        ]

        components: list[tuple[str, Any]] = []
        for column, count_name, row_type in _COMPONENT_GROUPS:
            entries = getattr(self, column, None)
            if entries is None:
                continue
            count = _tag_of(count_name)
            if _pair_identity(str(count)) not in stored_identities:
                components.extend(_component_pairs(count, entries, row_type))

        pairs = [*promoted, *components, *stored]
        resolved = access.tagged_pairs(pairs)
        named = {
            _pair_identity(pair[0])
            for source, pair in zip(pairs, resolved, strict=True)
            if not _numeric_key(source[0])
            and not _component_key(source[0])
            and _numeric_key(pair[0])
        }
        if not named:
            return pairs, resolved

        group_version = access.version or self.protocol_version
        if access.registry is not None and group_version is None:
            try:
                group_version = infer_version_from_pairs(stored, access.registry)[0]
            except (KeyError, OSError, ValueError):
                group_version = None
        group_counts = (
            access.registry.group_count_tags(group_version)
            if access.registry is not None
            else frozenset()
        )
        group_at = next(
            (
                index
                for index, pair in enumerate(stored_resolved)
                if _numeric_key(pair[0]) and int(pair[0]) in group_counts
            ),
            None,
        )
        protected = (
            [False] * len(promoted)
            + [True] * len(components)
            + [
                _component_key(pair[0]) or (group_at is not None and index >= group_at)
                for index, pair in enumerate(stored)
            ]
        )
        keep = [
            not (_numeric_key(source[0]) and _pair_identity(pair[0]) in named and not guarded)
            for source, pair, guarded in zip(pairs, resolved, protected, strict=True)
        ]
        return (
            [pair for pair, kept in zip(pairs, keep, strict=True) if kept],
            [pair for pair, kept in zip(resolved, keep, strict=True) if kept],
        )

    def group(
        self, count_tag: int | str, members: Collection[int | str] | None = None
    ) -> list[list[tuple[str, str]]]:
        """The entries of the repeating group `count_tag` counts."""
        return group_pairs(self.pairs, count_tag, members)

    def indexed_group(self, name: int | str) -> list[list[tuple[str, str]]]:
        """Rendered indexed group entries in index order."""
        return indexed_group_pairs(self.pairs, name)

    @property
    def pairs(self) -> list[tuple[str, str]]:
        """Canonical ordered text fields used by scalar and stored rows."""
        return self.into_fix_pairs()

    def __len__(self) -> int:
        return len(self.pairs)

    def __iter__(self) -> Iterator[tuple[str, str]]:
        return iter(self.pairs)

    def into_text(self, separator: str = "\x01") -> str:
        """Render ordered fields with the requested FIX separator."""
        return separator.join(f"{key}={value}" for key, value in self.pairs)

    def _field_entries(self) -> list[Entry]:
        """The canonical field sequence as accessor-ready entries.

        A list of ready `Entry` views, so a caller reading several dozen
        fields off one row builds them once: `entries_of` passes a ready entry
        straight through, and rebuilding the row per ask was the whole cost of
        decoding a normalized instrument (benchmarks/bench_market.py).
        """
        pairs, _ = self._canonical_pairs(_row_access())
        return [Entry.from_pair(key, value) for key, value in pairs]

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
        cls, batch: pyarrow.RecordBatch, codec: Any
    ) -> pyarrow.RecordBatch:
        """Transcribe one classified raw `Message` batch under a FIX codec."""
        if not isinstance(batch, pyarrow.RecordBatch):
            raise TypeError(f"FixMsg conversion needs a RecordBatch, got {type(batch).__name__}")
        rows = batch.num_rows
        columns = {name: batch.column(name) for name in batch.schema.names}
        messages = columns.get("message")
        if messages is not None:
            protocols = codec.categorise(messages, columns.get("plugin_code"))
            # Direction reads the verb before the payload, so it is resolved
            # here, where the classification saying which token opens the
            # payload was just computed -- and written back onto the batch,
            # appended where a stored batch predates the column, so the
            # partial fast path's slices carry it too. Only a row that
            # still has its text answers fresh: a projected row dropped the
            # message, and the stored answer is the only one there is.
            direction = codec.rules.into_arrow_direction_array(messages, protocols)
            stored_direction = columns.get("direction")
            if stored_direction is not None:
                direction = pyarrow.compute.if_else(
                    pyarrow.compute.is_valid(messages), direction, stored_direction
                )
            columns["direction"] = direction
            if "direction" in batch.schema.names:
                at = batch.schema.get_field_index("direction")
                batch = batch.set_column(at, batch.schema.field(at), direction)
            else:
                batch = batch.append_column(
                    Message.into_field().into_arrow_schema().field("direction"), direction
                )
        else:
            protocols = columns.get("protocol_code")
            if protocols is None:
                raise ValueError(
                    "a projected Message batch needs protocol_code; reparse the v4 message "
                    "contract before dropping message"
                )
        from rekep.text.fixmsg_arrow import flat_fixmsg_positions, into_flat_fixmsg_batch

        flat = into_flat_fixmsg_batch(cls, batch, codec, columns, protocols)
        if flat is not None:
            return flat
        fast_parts: list[pyarrow.RecordBatch] = []
        fast_positions: list[pyarrow.Array] = []
        for where in flat_fixmsg_positions(codec, columns, protocols):
            taken = _take_record_batch(batch, where)
            taken_columns = {name: taken.column(name) for name in taken.schema.names}
            translated = into_flat_fixmsg_batch(
                cls,
                taken,
                codec,
                taken_columns,
                pyarrow.compute.take(protocols, where),
            )
            if translated is not None:
                fast_parts.append(translated)
                fast_positions.append(where)
        if fast_parts:
            claimed = pyarrow.concat_arrays(fast_positions)
            all_rows = sequence(rows)
            fallback_at = pyarrow.compute.filter(
                all_rows,
                pyarrow.compute.invert(pyarrow.compute.is_in(all_rows, value_set=claimed)),
            )
            if len(fallback_at):
                fallback = _take_record_batch(batch, fallback_at)
                fallback_columns = {name: fallback.column(name) for name in fallback.schema.names}
                fast_parts.append(
                    cls._from_message_arrow_batch_reference(
                        fallback,
                        codec,
                        fallback_columns,
                        pyarrow.compute.take(protocols, fallback_at),
                    )
                )
                fast_positions.append(fallback_at)
            return _scatter_record_batches(fast_parts, fast_positions)
        return cls._from_message_arrow_batch_reference(batch, codec, columns, protocols)

    @classmethod
    def _from_message_arrow_batch_reference(
        cls,
        batch: pyarrow.RecordBatch,
        codec: Any,
        columns: dict[str, pyarrow.Array],
        protocols: pyarrow.Array,
    ) -> pyarrow.RecordBatch:
        """Transcribe rows through the registry's complete configurable path."""
        rows = batch.num_rows
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
                "protocol_code": protocols,
                "protocol_version": protocol_version,
                "protocol_version_source": protocol_version_source,
                "kwargs": kwargs,
            }
        )
        schema = cls._message_schema(batch.schema)
        for field in schema:
            columns.setdefault(field.name, pyarrow.nulls(rows, field.type))
        columns.update(cls._resolved_batch_columns(columns, codec, rows))
        columns["mic"] = _mic_arrow(columns, rows)
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
        kwargs = cls._prefer_named_kwargs(
            columns["kwargs"], kwargs, codec.registry.group_count_tags(version)
        )
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

    @staticmethod
    def _prefer_named_kwargs(
        source: Any, resolved: Any, group_count_tags: Collection[int] = ()
    ) -> Any:
        """Drop flat numeric copies shadowed by named fields of one identity.

        Indexed component members remain repetitions: their shared tag does
        not make two group entries duplicates.
        """
        if isinstance(source, pyarrow.ChunkedArray):
            source = source.combine_chunks()
        if isinstance(resolved, pyarrow.ChunkedArray):
            resolved = resolved.combine_chunks()
        rows = len(resolved)
        if not rows or resolved.null_count == rows:
            return resolved

        compute = pyarrow.compute
        source_entries = compute.list_flatten(source)
        entries = compute.list_flatten(resolved)
        if not len(entries):
            return resolved
        source_tags = compute.struct_field(source_entries, "tag")
        tags = compute.struct_field(entries, "tag")
        source_components = compute.struct_field(source_entries, "comp")
        named = compute.and_(
            compute.and_(compute.equal(source_tags, 0), compute.is_null(source_components)),
            compute.greater(tags, 0),
        )
        if not compute.any(named, min_count=0).as_py():
            return resolved

        parents = compute.list_parent_indices(resolved).cast(pyarrow.int64())
        identities = compute.add(
            compute.multiply(parents, pyarrow.scalar(1 << 32, pyarrow.int64())),
            tags.cast(pyarrow.int64()),
        )
        named_identities = compute.filter(identities, named)
        numeric = compute.and_(compute.greater(source_tags, 0), compute.is_null(source_components))
        if group_count_tags:
            positions = sequence(len(entries))
            counted = compute.is_in(
                source_tags,
                value_set=pyarrow.array(sorted(group_count_tags), source_tags.type),
            )
            count_parents = compute.filter(parents, counted)
            count_positions = compute.filter(positions, counted)
            first_counts = compute.take(
                count_positions,
                compute.index_in(sequence(rows), value_set=count_parents),
            )
            protected = compute.fill_null(
                compute.greater_equal(positions, compute.take(first_counts, parents)), False
            )
            numeric = compute.and_(numeric, compute.invert(protected))
        duplicate = compute.and_(
            numeric,
            compute.fill_null(compute.is_in(identities, value_set=named_identities), False),
        )
        if not compute.any(duplicate, min_count=0).as_py():
            return resolved

        keep = compute.invert(duplicate)
        kept_parents = compute.filter(parents, keep)
        sizes = dense_counts(kept_parents, rows)
        values = pyarrow.StructArray.from_arrays(
            [
                compute.filter(compute.struct_field(entries, field.name), keep)
                for field in entries.type
            ],
            fields=list(entries.type),
        )
        mask = compute.is_null(resolved) if resolved.null_count else None
        return build_list(KWARGS, sizes, values, mask)

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
        recomputed = cls.hash_arrow(
            _digest_text(columns.get("message"), rows),
            _digest_text(columns.get("source_url"), rows),
            _digest_text(columns.get("source_rownum"), rows),
        )
        incoming = columns.get("hash")
        raw = (
            recomputed
            if incoming is None
            else compute.if_else(
                compute.and_(compute.is_valid(incoming), compute.not_equal(incoming, NIL)),
                incoming,
                recomputed,
            )
        )
        return compute.if_else(unread, raw, digests)

    @classmethod
    def code_arrow(cls, columns: Mapping[str, Any], rows: int) -> pyarrow.Array:
        """Best readable lifecycle identifier available in parsed FIX columns."""
        return _first_text(columns, cls.into_code_columns(), rows)

    @classmethod
    def codes_arrow(
        cls,
        columns: Mapping[str, Any],
        rows: int,
        tags: Mapping[str, int] | None = None,
    ) -> pyarrow.Array:
        """Every parsed identifier as one ordered Arrow map per row."""
        compute = pyarrow.compute
        identified = tuple(
            (stored, field, tag if tags is None else tags.get(field, tag))
            for stored, field, tag in IDENTIFIER_FIELDS
        )
        residual = (
            FieldAccess.first_arrow_fields(
                columns.get("kwargs"),
                tuple((tag, field) for _, field, tag in identified if tag > 0),
                rows,
            )
            if columns.get("kwargs") is not None
            else {}
        )
        available = []
        for stored, field, _ in identified:
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
        """Expose this parsed row through the FIX market translator."""
        from rekep.market.fix import FixEvents

        carried = {"runix": self.runix or self.unix, "mic": self.mic, **declared}
        return self._transacted(FixEvents(message=self, **carried))

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

        Flat standard messages use Arrow kernels; grouped, custom, and uncommon
        shapes fall back to the scalar authority. Each event type retains message
        order. `None` drains each type only at the end for one atomic commit.
        """
        from rekep.market.fix_arrow import flat_market_parts, flat_market_positions
        from rekep.market.orders import Execution, Order

        if batch_row_size is not None and batch_row_size <= 0:
            raise ValueError("batch_row_size must be positive")
        batches = (source,) if isinstance(source, pyarrow.RecordBatch) else source
        event_types = (Order, Execution)
        pending: dict[type[Any], list[pyarrow.RecordBatch]] = {
            event_type: [] for event_type in event_types
        }
        pending_rows = {event_type: 0 for event_type in event_types}

        def combined(event_type: type[Any]) -> pyarrow.RecordBatch:
            parts = pending[event_type]
            table = pyarrow.Table.from_batches(
                parts, schema=event_type.into_field().into_arrow_schema()
            ).combine_chunks()
            parts.clear()
            pending_rows[event_type] = 0
            return table.to_batches(max_chunksize=table.num_rows)[0]

        def pushed(
            event_type: type[Any], batch: pyarrow.RecordBatch
        ) -> Iterator[tuple[type[Any], pyarrow.RecordBatch]]:
            if batch_row_size is None:
                pending[event_type].append(batch)
                pending_rows[event_type] += batch.num_rows
                return
            offset = 0
            while offset < batch.num_rows:
                take = min(batch_row_size - pending_rows[event_type], batch.num_rows - offset)
                pending[event_type].append(batch.slice(offset, take))
                pending_rows[event_type] += take
                offset += take
                if pending_rows[event_type] == batch_row_size:
                    yield event_type, combined(event_type)

        def scalar_parts(
            batch: pyarrow.RecordBatch, source_at: pyarrow.Array
        ) -> Iterator[tuple[type[Any], pyarrow.RecordBatch, pyarrow.Array, pyarrow.Array]]:
            events: dict[type[Any], list[Any]] = {event_type: [] for event_type in event_types}
            origins: dict[type[Any], list[int]] = {event_type: [] for event_type in event_types}
            ranks: dict[type[Any], list[int]] = {event_type: [] for event_type in event_types}
            for origin, message in zip(
                source_at.to_pylist(), cls.from_arrow_reader([batch]), strict=True
            ):
                row_ranks = {event_type: 0 for event_type in event_types}
                for event in message.into_market_events(**declared):
                    event_type = type(event)
                    if event_type not in events:
                        continue
                    events[event_type].append(event)
                    origins[event_type].append(origin)
                    ranks[event_type].append(row_ranks[event_type])
                    row_ranks[event_type] += 1
            for event_type in event_types:
                if not events[event_type]:
                    continue
                converted = list(
                    event_type.into_arrow_reader(
                        events[event_type], batch_row_size=len(events[event_type])
                    )
                )
                table = pyarrow.Table.from_batches(
                    converted, schema=event_type.into_field().into_arrow_schema()
                ).combine_chunks()
                yield (
                    event_type,
                    table.to_batches(max_chunksize=table.num_rows)[0],
                    pyarrow.array(origins[event_type], pyarrow.int64()),
                    pyarrow.array(ranks[event_type], pyarrow.int64()),
                )

        def ordered(
            event_type: type[Any],
            parts: Sequence[pyarrow.RecordBatch],
            origins: Sequence[pyarrow.Array],
            ranks: Sequence[pyarrow.Array],
        ) -> pyarrow.RecordBatch:
            table = pyarrow.Table.from_batches(
                parts, schema=event_type.into_field().into_arrow_schema()
            ).combine_chunks()
            keys = pyarrow.table(
                {
                    "source": pyarrow.concat_arrays(origins),
                    "rank": pyarrow.concat_arrays(ranks),
                }
            )
            order = pyarrow.compute.sort_indices(
                keys, sort_keys=[("source", "ascending"), ("rank", "ascending")]
            )
            table = table.take(order).combine_chunks()
            return table.to_batches(max_chunksize=table.num_rows)[0]

        for incoming in batches:
            batch = cls.into_field().cast_arrow_batch(incoming)
            flat = flat_market_parts(batch, declared)
            if flat is not None:
                for event_type, translated in zip(event_types, flat[:2], strict=True):
                    if translated is None:
                        continue
                    yield from pushed(event_type, translated)
                continue
            translated_parts: dict[type[Any], list[pyarrow.RecordBatch]] = {
                event_type: [] for event_type in event_types
            }
            translated_at: dict[type[Any], list[pyarrow.Array]] = {
                event_type: [] for event_type in event_types
            }
            translated_ranks: dict[type[Any], list[pyarrow.Array]] = {
                event_type: [] for event_type in event_types
            }
            claimed: list[pyarrow.Array] = []
            for where in flat_market_positions(batch, declared):
                taken = _take_record_batch(batch, where)
                translated = flat_market_parts(taken, declared)
                if translated is None:
                    continue
                claimed.append(where)
                for event_type, event_batch, local_at in zip(
                    event_types,
                    translated[:2],
                    translated[2:],
                    strict=True,
                ):
                    if event_batch is None:
                        continue
                    translated_parts[event_type].append(event_batch)
                    translated_at[event_type].append(pyarrow.compute.take(where, local_at))
                    translated_ranks[event_type].append(
                        pyarrow.repeat(pyarrow.scalar(0, pyarrow.int64()), event_batch.num_rows)
                    )
            all_rows = sequence(batch.num_rows)
            claimed_at = (
                pyarrow.concat_arrays(claimed) if claimed else pyarrow.array([], pyarrow.int64())
            )
            fallback_at = pyarrow.compute.filter(
                all_rows,
                pyarrow.compute.invert(pyarrow.compute.is_in(all_rows, value_set=claimed_at)),
            )
            if len(fallback_at):
                fallback = _take_record_batch(batch, fallback_at)
                for event_type, event_batch, origins, ranks in scalar_parts(fallback, fallback_at):
                    translated_parts[event_type].append(event_batch)
                    translated_at[event_type].append(origins)
                    translated_ranks[event_type].append(ranks)
            for event_type in event_types:
                if translated_parts[event_type]:
                    yield from pushed(
                        event_type,
                        ordered(
                            event_type,
                            translated_parts[event_type],
                            translated_at[event_type],
                            translated_ranks[event_type],
                        ),
                    )
        for event_type in event_types:
            if pending_rows[event_type]:
                yield event_type, combined(event_type)

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


def _take_record_batch(batch: pyarrow.RecordBatch, where: pyarrow.Array) -> pyarrow.RecordBatch:
    """Take rows while retaining the source schema and its metadata."""
    return pyarrow.RecordBatch.from_arrays(
        [pyarrow.compute.take(column, where) for column in batch.columns],
        schema=batch.schema,
    )


def _scatter_record_batches(
    parts: Sequence[pyarrow.RecordBatch], positions: Sequence[pyarrow.Array]
) -> pyarrow.RecordBatch:
    """Restore disjoint translated row slices to their source order."""
    schema = parts[0].schema
    return pyarrow.RecordBatch.from_arrays(
        [
            scattered([part.column(index) for part in parts], positions)
            for index in range(len(schema))
        ],
        schema=schema,
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


def _mic_arrow(columns: Mapping[str, Any], rows: int) -> pyarrow.Array:
    """ISO exchange fields, the stored venue, then FIX session endpoints."""
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
    stored_mic = columns.get("mic")
    if stored_mic is None:
        stored_mic = pyarrow.nulls(rows, pyarrow.int32())
    elif stored_mic.type != pyarrow.int32():
        stored_mic = stored_mic.cast(pyarrow.int32(), safe=False)
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
    return compute.coalesce(venue, stored_mic, target, sender)


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
    return None if entries is None else [Kwarg.from_stored(entry).into_dict() for entry in entries]


@functools.cache
def _tags_by_name() -> Mapping[str, int]:
    """Canonical FIX names to the tags declared by this parsed-row contract."""
    return MappingProxyType({member.name.casefold(): tag for tag, member in DECLARATIONS.items()})


def _tag_of(name: str) -> int:
    """The contract tag of one canonical FIX name."""
    return _tags_by_name()[name.casefold()]


def _component_pairs(
    count_tag: int, entries: Sequence[Any], row_type: type[Any]
) -> list[tuple[str, Any]]:
    """One typed component restored as a valid count-led repeating group."""
    pairs: list[tuple[str, Any]] = [(str(count_tag), len(entries))]
    members = tuple(member for member in row_type.into_field().fields if member.name != "buffer")
    for entry in entries:
        values = entry if isinstance(entry, Mapping) else None
        buffered = dict(
            (values.get("buffer") if values is not None else getattr(entry, "buffer", None)) or {}
        )
        for index, member in enumerate(members):
            value = (
                values.get(member.name) if values is not None else getattr(entry, member.name, None)
            )
            if value is None and member.name in buffered:
                value = buffered.pop(member.name)
            if index == 0 and value is None:
                raise ValueError(f"{row_type.__name__} entry lacks delimiter {member.name!r}")
            if value is not None:
                pairs.append((str(member.fix["tag"]), value))
        for key, value in buffered.items():
            pairs.append((str(key), value))
    return pairs


def _fix_text(value: Any) -> str:
    """One normalized value in the spelling a named FIX pair accepts."""
    return "" if value is None else render_fix_value(value)


def _numeric_key(value: Any) -> bool:
    """Whether a pair key is a numeric FIX tag."""
    text = str(value)
    return text.isascii() and text.isdigit() and len(text) <= 9


def _component_key(value: Any) -> bool:
    """Whether a rendered key belongs to one indexed group entry."""
    lead, separator, _ = str(value).rpartition(".")
    index = lead.rpartition("[")[2].removesuffix("]")
    return bool(separator) and lead.endswith("]") and index.isdigit()


def _pair_identity(key: Any) -> tuple[str, int | str]:
    """Stable resolved identity for duplicate selection."""
    text = str(key)
    return ("tag", int(text)) if _numeric_key(text) else ("name", text.casefold())


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
            entry = Kwarg.from_stored(entry)
        lead = entry.get("namespace") or entry.get("comp")
        name = entry["key"]
        if lead:
            yield f"{lead}.{name}", entry.get("value")
        elif "[" in name:
            yield name, entry.get("value")
        elif entry.get("tag"):
            yield entry["tag"], entry.get("value")
        else:
            yield name, entry.get("value")


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
