"""The shape of one parsed log line, what decides which event it is, and what fills it."""

from __future__ import annotations

import dataclasses
import datetime
import functools
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Annotated, Any

import pyarrow
import pyarrow.compute

from rekep import txhash
from rekep.enums import MIC, Direction, EventType, Protocol
from rekep.fields import Field, column_name, scalar
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
    DECLARED,
    ENTRIES,
    IDENTIFIER_FIELDS,
    ISIN_CODE,
    PARENT_CL_ORD_ID,
    PARENT_ORDER_ID,
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
from rekep.fix.fields import cast_arrow_fix, scalar_fix_value
from rekep.fix.message import (
    group_pairs,
    indexed_group_pairs,
    normalized_pairs,
    render_fix_value,
)
from rekep.fix.registry import FixRegistry
from rekep.fix.transcribe import NO_SOURCE, FixCodec, infer_version_from_pairs
from rekep.market.event import ALTIDS_TYPE, Event, unix_partition_arrow
from rekep.market.fields import MarketConvertible
from rekep.market.identity import NIL, hash_bytes_arrow, hash_int_of
from rekep.market.ticker import SymbolTicker
from rekep.text.message import SESSION_FIELDS, Message

_CONTRACT_METADATA = MappingProxyType({"version": "1"})
_PROTOCOL_CODE = Protocol.into_arrow_type().index_type


@functools.cache
def _component_groups() -> tuple[tuple[str, str, type[Any]], ...]:
    """`(column, count group, entry class)` off the codec's component extractors.

    Derived and not declared twice: `FixCodec.into_components` is the one
    mapping of structured columns to extractors, and each extractor already
    names its group and its row shape.
    """
    return tuple(
        (column, extractor.group, extractor.into_row())
        for column, extractor in FixCodec.into_components().items()
    )


#: The parsed columns that hold one structured component each. What
#: `FixMsg.into_first_values` checks before taking its flat shortcut.
COMPONENT_COLUMNS: tuple[str, ...] = tuple(FixCodec.into_components())


@functools.lru_cache(maxsize=8)
def _codec_of(registry: FixRegistry, _revision: int) -> FixCodec:
    """One shared codec per dictionary generation, so batches share its memos.

    Keyed on the store revision like `MarketTags._of`: a codec snapshots its
    tag indexes and component declarations, so a mutated registry earns a
    fresh one rather than serving stale reads.
    """
    return FixCodec(registry=registry)


@scalar(slots=True)
class FixMsg(Message):
    """One raw message transcribed under the FIX registry."""

    # The dictionary this row resolves through is reader state, not row
    # content -- the same stored row reads under another feed's dictionary
    # without changing -- so the link is private and never a column.
    __registry: FixRegistry | None = None

    @classmethod
    @functools.cache
    def into_registry(cls) -> FixRegistry:
        """The dictionary an unlinked row resolves through: the packaged one."""
        return FixRegistry.from_builtin()

    @classmethod
    def into_codec(cls, registry: FixRegistry | None = None) -> FixCodec:
        """One conversion needs one dictionary: the codec derives from it.

        The packaged registry when none is given, so an unconfigured
        transcription still reads offline.
        """
        selected = registry if registry is not None else cls.into_registry()
        return _codec_of(selected, selected.revision)

    @property
    def registry(self) -> FixRegistry:
        """The privately linked dictionary, or the packaged default."""
        return getattr(self, "_FixMsg__registry", None) or type(self).into_registry()

    def link_registry(self, registry: FixRegistry | None) -> FixMsg:
        """Privately link the dictionary every read on this row resolves through."""
        self.__registry = registry
        self._partition_scalar_entries()
        return self

    def _residual_entries(self) -> list[Entry]:
        """Retained payload fields, whichever side of registry resolution holds them."""
        return [*(self.entries or ()), *(self.unmap or ())]

    def _partition_scalar_entries(self) -> None:
        """Partition one retained payload under this row's linked dictionary."""
        if self.entries is None and self.unmap is None:
            return
        codec = type(self).into_codec(self.registry)
        stored = pyarrow.array([_stored_entries(self._residual_entries())], type=ENTRIES)
        entries, unmap = type(self)._partition_entries(stored, codec, self.protocolversion)
        self.entries = [Entry.from_stored(entry) for entry in entries[0].as_py() or ()]
        unresolved = unmap[0].as_py()
        self.unmap = (
            None if unresolved is None else [Entry.from_stored(entry) for entry in unresolved]
        )

    def _row_access(self) -> FieldAccess:
        """The accessor this row reads through: its dictionary, cross-version."""
        return FieldAccess.of(self.registry, None)

    @classmethod
    def from_(cls, source: Any, *args: Any, **kwargs: Any) -> FixMsg:
        """Build text and bytes as FIX payloads; document readers stay explicit.

        The text check leads so a payload spelled like a document name still
        parses as a payload; everything else dispatches through the redirect
        table -- spelled without `super()`, whose `__class__` cell predates
        the class `slots=True` rebuilt.
        """
        if isinstance(source, str | bytes):
            return cls.from_text(source, *args, **kwargs)
        return getattr(cls, f"from_{cls.redirect_of(source)}")(source, *args, **kwargs)

    @classmethod
    @functools.cache
    def into_redirects(cls) -> Mapping[Any, str]:
        """Generic conversions plus direct text parsing and raw-row transcription."""
        return MappingProxyType(
            {**Message.into_redirects(), str: "text", bytes: "text", Message: "message"}
        )

    @classmethod
    @functools.cache
    def into_field_metadata(cls) -> Mapping[str, str]:
        """Contract metadata published with parsed log schemas."""
        return _CONTRACT_METADATA

    xhash: Annotated[int, Field(dtype=pyarrow.int64())] = NIL
    """Digest of `code`, or `vhash` when no correlation code exists."""

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
            "orderid",
            "origclordid",
            "clordid",
            "execid",
            "quoteentryid",
            "quoteid",
            "quotereqid",
            "symbolticker",
        )

    def __post_init__(self) -> None:
        """Normalize retained FIX fields without changing null/list semantics."""
        Event.__post_init__(self)
        # Both packed enums Message declares, because this reaches
        # `Event.__post_init__` and not Message's, so nothing else reads them
        # off a spelling. A column takes the code, never the word.
        self.direction = Direction.from_str(self.direction)
        _supplied = self.protocol
        self.protocol = Protocol.from_str(self.protocol)
        if self.protocol is Protocol.UNKNOWN and _supplied not in (None, Protocol.OTHER):
            raise ValueError(f"{_supplied!r} is no protocol name")
        if self.entries is not None:
            self.entries = [Entry.from_stored(entry) for entry in self.entries]
        if self.unmap:
            self.unmap = [Entry.from_stored(entry) for entry in self.unmap]
        else:
            self.unmap = None
        self.symbolticker = (
            SymbolTicker.from_str(self.symbolticker)
            if self.symbolticker
            else SymbolTicker.from_fixmsg(self)
        ).into_str()
        if (
            self.protocolversion is None
            and self.protocol is Protocol.OTHER
            and (self.beginstring or self.entries or self.unmap)
        ):
            evidence: list[tuple[str, Any]] = []
            if self.beginstring:
                evidence.append(("8", self.beginstring))
            if self.applverid:
                evidence.append(("1128", self.applverid))
            evidence.extend(_stored_pairs(self._residual_entries()))
            version, source = infer_version_from_pairs(evidence)
            if version is not None:
                self.protocolversion = version
                self.protocolversionsource = source
                if self.protocol is Protocol.OTHER:
                    self.protocol = Protocol.FIX

    def identify(self) -> FixMsg:
        """Give the parsed event the identities its registry projection earns."""
        if self.hash and self.vhash and self.xhash:
            return self
        staged_values = {
            member.name: getattr(self, member.name) for member in dataclasses.fields(Message)
        }
        staged_values["message"] = self.message or ""
        if self.entries is not None or self.unmap is not None:
            retained = list(_stored_pairs(self._residual_entries()))
            checksum = str(_tag_of("CheckSum"))
            # Partitioning separates fields that preceded the trailer; restore
            # its framing position before the raw transcription reads it again.
            staged_values["entries"] = [
                *[pair for pair in retained if str(pair[0]) != checksum],
                *[pair for pair in retained if str(pair[0]) == checksum],
            ]
        else:
            staged_values["entries"] = None
        parsed = type(self).from_message_batch(
            [Message(**staged_values)], type(self).into_codec(self.registry)
        )

        def value(name: str) -> Any:
            return parsed.column(name)[0].as_py()

        self.unix = value("unix")
        self.unixpartition = value("unixpartition")
        self.creaunix = value("creaunix")
        self.unixsource = value("unixsource")
        self.code = value("code")
        self.altids = dict(value("altids") or ())
        self.reason = value("reason")
        self.symbolticker = value("symbolticker")
        self.vhash = value("vhash")
        self.hash = hash_int_of(value("hash")) or NIL
        self.xhash = value("xhash")
        return self

    # Nullable, and null on `fix.market`: typed columns plus the two residual
    # lists carry every field the line held, so keeping the raw string beside them would
    # store the same content twice. An all-null column run-length and dictionary encodes
    # to nothing on disk, which is what makes one stored shape across the
    # three tables affordable -- the same reasoning `_zeros` applies to the
    # envelope members a parsed line leaves unset.
    message: str | None = None
    """Payload text; null where parsed columns retain every field."""

    protocol: Protocol = Protocol.OTHER
    """Which protocol the line carries; OTHER is a line that carries none."""

    # Without it nothing downstream can tell a real transaction time from a
    # print time, and that distinction is the whole point of resolving one.
    # Empty means no clock answered at all, which is a row with no time.
    unixsource: Annotated[str, Field.column("Unix Source")] = ""
    """Which rung of `TRANSACTED` gave `unix`; `recorded` is the log's own clock."""

    # One column, not a FIX-specific one: every protocol with versions has a
    # version, and a `fix_version` beside it would duplicate itself the first
    # time a second versioned protocol appeared. Resolved once, at the message
    # stage, so nothing downstream re-derives it.
    protocolversion: Annotated[str | None, Field.column("Protocol Version")] = None
    """Which version of `protocol` the line is read under; null when unresolved."""

    # Null because the message carried no version, or null because nothing
    # tried? A consumer cannot tell the two apart from the value, and they are
    # different facts about the row.
    protocolversionsource: Annotated[str, Field.column("Protocol Version Source")] = NO_SOURCE
    """What resolved `protocolversion`: a BeginString, an application version, or nothing."""

    msgseqnum: Annotated[int | None, DECLARED["MsgSeqNum"]] = None
    """`MsgSeqNum <34>`: wire order among messages with equal timestamps."""

    # A list preserves repeated keys and wire order. Null means no parsed
    # message; an empty list means no residual or raw audit sidecar remains.
    entries: list[Entry] | None = None
    """Unlifted fields and lossless raw audit sidecars for typed columns."""

    unmap: Annotated[list[Entry] | None, Field.column("Unmapped")] = None
    """Registry-unresolved entries partition payload with `entries`; null means all resolved."""

    parties: Annotated[
        list[Party] | None,
        Field(
            dtype=PARTIES,
            metadata={"fix:component": "Parties"},
        ),
    ] = None
    """FIX Parties entries; null when the component is absent."""

    trdregtimestamps: Annotated[
        list[TrdRegTimestamp] | None,
        Field(
            dtype=TRD_REG_TIMESTAMPS,
            metadata={"fix:component": "TrdRegTimestamps"},
        ),
    ] = None
    """FIX TrdRegTimestamps entries; null when the component is absent."""

    sidetrdregts: Annotated[
        list[SideTrdRegTimestamp] | None,
        Field(
            dtype=SIDE_TRD_REG_TIMESTAMPS,
            metadata={"fix:component": "SideTrdRegTS"},
        ),
    ] = None
    """FIX SideTrdRegTS entries -- the per-side regulatory clock; null when absent."""

    isincode: Annotated[str | None, ISIN_CODE, Field.column("ISIN Code")] = None
    """ISIN carried by a rendered `ISINCODE` field."""

    parentclordid: Annotated[str | None, PARENT_CL_ORD_ID] = None
    """Client order identity of the parent in a replace chain, bridge-rendered."""

    parentorderid: Annotated[str | None, PARENT_ORDER_ID] = None
    """Venue order identity of the parent in a replace chain, bridge-rendered."""

    # -- what a message says, flattened ---------------------------------------
    #
    # Flat fields keep the registry's exact name, type, description and
    # metadata. A lifted fact stays in `entries` only where typing loses text.

    # The envelope itself.

    beginstring: Annotated[str | None, DECLARED["BeginString"]] = None
    """`BeginString <8>`: which FIX version the message says it is."""

    bodylength: Annotated[int | None, DECLARED["BodyLength"]] = None
    """`BodyLength <9>`, as the message counted it."""

    msgtype: Annotated[str | None, DECLARED["MsgType"]] = None
    """`MsgType <35>`: what the message is, on the wire."""

    checksum: Annotated[str | None, DECLARED["CheckSum"]] = None
    """`CheckSum <10>`: three digits, so a string -- `010` read as `10` no longer verifies."""

    # Who sent it, and to whom.

    sendercompid: Annotated[str | None, DECLARED["SenderCompID"]] = None
    """`SenderCompID <49>`: who sent it."""

    sendersubid: Annotated[str | None, DECLARED["SenderSubID"]] = None
    """`SenderSubID <50>`: which desk of theirs."""

    senderlocationid: Annotated[str | None, DECLARED["SenderLocationID"]] = None
    """`SenderLocationID <142>`."""

    targetcompid: Annotated[str | None, DECLARED["TargetCompID"]] = None
    """`TargetCompID <56>`: who it was sent to."""

    targetsubid: Annotated[str | None, DECLARED["TargetSubID"]] = None
    """`TargetSubID <57>`."""

    targetlocationid: Annotated[str | None, DECLARED["TargetLocationID"]] = None
    """`TargetLocationID <143>`."""

    # And on whose behalf, when a hub relayed it.

    onbehalfofcompid: Annotated[str | None, DECLARED["OnBehalfOfCompID"]] = None
    """`OnBehalfOfCompID <115>`: who the sender was speaking for."""

    onbehalfofsubid: Annotated[str | None, DECLARED["OnBehalfOfSubID"]] = None
    """`OnBehalfOfSubID <116>`."""

    onbehalfoflocationid: Annotated[str | None, DECLARED["OnBehalfOfLocationID"]] = None
    """`OnBehalfOfLocationID <144>`."""

    delivertocompid: Annotated[str | None, DECLARED["DeliverToCompID"]] = None
    """`DeliverToCompID <128>`: who it is ultimately for."""

    delivertosubid: Annotated[str | None, DECLARED["DeliverToSubID"]] = None
    """`DeliverToSubID <129>`."""

    delivertolocationid: Annotated[str | None, DECLARED["DeliverToLocationID"]] = None
    """`DeliverToLocationID <145>`."""

    # Where it sits in the session's stream, and whether it is a repeat.

    lastmsgseqnumprocessed: Annotated[int | None, DECLARED["LastMsgSeqNumProcessed"]] = None
    """`LastMsgSeqNumProcessed <369>`: how far the sender had read."""

    possdupflag: Annotated[bool | None, DECLARED["PossDupFlag"]] = None
    """`PossDupFlag <43>`: a retransmission of a message already sent."""

    possresend: Annotated[bool | None, DECLARED["PossResend"]] = None
    """`PossResend <97>`: the same business content under a new sequence."""

    # FIX documents these instants as UTC; microseconds are Iceberg-compatible.

    sendingtime: Annotated[datetime.datetime | None, DECLARED["SendingTime"]] = None
    """`SendingTime <52>`: when it was transmitted."""

    origsendingtime: Annotated[datetime.datetime | None, DECLARED["OrigSendingTime"]] = None
    """`OrigSendingTime <122>`: the original transmission, on a resend."""

    onbehalfofsendingtime: Annotated[
        datetime.datetime | None, DECLARED["OnBehalfOfSendingTime"]
    ] = None
    """`OnBehalfOfSendingTime <370>`."""

    # Which application version speaks, under FIXT.

    applverid: Annotated[str | None, DECLARED["ApplVerID"]] = None
    """`ApplVerID <1128>`."""

    cstmapplverid: Annotated[str | None, DECLARED["CstmApplVerID"]] = None
    """`CstmApplVerID <1129>`."""

    applextid: Annotated[int | None, DECLARED["ApplExtID"]] = None
    """`ApplExtID <1156>`."""

    # How the payload is written, when it is not plain ASCII.

    messageencoding: Annotated[str | None, DECLARED["MessageEncoding"]] = None
    """`MessageEncoding <347>`."""

    xmldatalen: Annotated[int | None, DECLARED["XmlDataLen"]] = None
    """`XmlDataLen <212>`."""

    xmldata: Annotated[bytes | None, DECLARED["XmlData"]] = None
    """`XmlData <213>`, as the bytes it is."""

    # And how it is sealed.

    securedatalen: Annotated[int | None, DECLARED["SecureDataLen"]] = None
    """`SecureDataLen <90>`."""

    securedata: Annotated[bytes | None, DECLARED["SecureData"]] = None
    """`SecureData <91>`, as the bytes it is."""

    signaturelength: Annotated[int | None, DECLARED["SignatureLength"]] = None
    """`SignatureLength <93>`."""

    signature: Annotated[bytes | None, DECLARED["Signature"]] = None
    """`Signature <89>`, as the bytes it is."""

    # What was traded.

    symbolticker: Annotated[str, Field.column("Symbol Ticker")] = ""
    """Canonical spelling derived from the FIX instrument identifiers."""

    symbol: Annotated[str | None, DECLARED["Symbol"]] = None
    """`Symbol <55>`: ticker symbol."""

    securityid: Annotated[str | None, DECLARED["SecurityID"]] = None
    """`SecurityID <48>`, under the scheme `SecurityIDSource` names."""

    securityidsource: Annotated[str | None, DECLARED["SecurityIDSource"]] = None
    """`SecurityIDSource <22>`: which scheme `SecurityID` is in -- `4` is ISIN."""

    securitytype: Annotated[str | None, DECLARED["SecurityType"]] = None
    """`SecurityType <167>`."""

    cficode: Annotated[str | None, DECLARED["CFICode"]] = None
    """`CFICode <461>`: what kind of instrument it is, as ISO 10962 spells it."""

    securityexchange: Annotated[str | None, DECLARED["SecurityExchange"]] = None
    """`SecurityExchange <207>`: the market the instrument is listed on."""

    currency: Annotated[str | None, DECLARED["Currency"]] = None
    """`Currency <15>`, which is what the prices below are in."""

    maturitydate: Annotated[datetime.date | None, DECLARED["MaturityDate"]] = None
    """`MaturityDate <541>`, the day a dated contract expires."""

    maturitymonthyear: Annotated[str | None, DECLARED["MaturityMonthYear"]] = None
    """`MaturityMonthYear <200>`, the older spelling of the same expiry."""

    strikeprice: Annotated[float | None, DECLARED["StrikePrice"]] = None
    """`StrikePrice <202>`, in `currency`."""

    putorcall: Annotated[int | None, DECLARED["PutOrCall"]] = None
    """`PutOrCall <201>`: 0 is a put and 1 is a call."""

    contractmultiplier: Annotated[float | None, DECLARED["ContractMultiplier"]] = None
    """`ContractMultiplier <231>`, how many units one contract carries."""

    minpriceincrement: Annotated[float | None, DECLARED["MinPriceIncrement"]] = None
    """`MinPriceIncrement <969>`, the venue's tick."""

    roundlot: Annotated[float | None, DECLARED["RoundLot"]] = None
    """`RoundLot <561>`, the tradable unit."""

    securitydesc: Annotated[str | None, DECLARED["SecurityDesc"]] = None
    """`SecurityDesc <107>`, the venue's own words for the contract."""

    # Who asked, and under which identifiers.

    account: Annotated[str | None, DECLARED["Account"]] = None
    """`Account <1>`."""

    clordid: Annotated[str | None, DECLARED["ClOrdID"]] = None
    """`ClOrdID <11>`: the client's own identifier for the order."""

    origclordid: Annotated[str | None, DECLARED["OrigClOrdID"]] = None
    """`OrigClOrdID <41>`: which order an amendment or cancel is about."""

    orderid: Annotated[str | None, DECLARED["OrderID"]] = None
    """`OrderID <37>`: the venue's identifier for it."""

    execid: Annotated[str | None, DECLARED["ExecID"]] = None
    """`ExecID <17>`: the venue's identifier for this execution report."""

    # On what terms.

    side: Annotated[str | None, DECLARED["Side"]] = None
    """`Side <54>`: `1` buy, `2` sell, and the rest of the standard's codes."""

    ordtype: Annotated[str | None, DECLARED["OrdType"]] = None
    """`OrdType <40>`: `1` market, `2` limit, ..."""

    timeinforce: Annotated[str | None, DECLARED["TimeInForce"]] = None
    """`TimeInForce <59>`: `0` day, `1` GTC, `3` IOC, ..."""

    # Where it stands.

    ordstatus: Annotated[str | None, DECLARED["OrdStatus"]] = None
    """`OrdStatus <39>`: where the order stands."""

    exectype: Annotated[str | None, DECLARED["ExecType"]] = None
    """`ExecType <150>`: what this report is reporting."""

    # For how much, at what price.

    orderqty: Annotated[float | None, DECLARED["OrderQty"]] = None
    """`OrderQty <38>`: how much was asked for."""

    price: Annotated[float | None, DECLARED["Price"]] = None
    """`Price <44>`: the limit, when there is one."""

    avgpx: Annotated[float | None, DECLARED["AvgPx"]] = None
    """`AvgPx <6>`: the average of what has filled so far."""

    cumqty: Annotated[float | None, DECLARED["CumQty"]] = None
    """`CumQty <14>`: how much has filled."""

    leavesqty: Annotated[float | None, DECLARED["LeavesQty"]] = None
    """`LeavesQty <151>`: how much is still working."""

    lastpx: Annotated[float | None, DECLARED["LastPx"]] = None
    """`LastPx <31>`: the price of this fill."""

    lastqty: Annotated[float | None, DECLARED["LastQty"]] = None
    """`LastQty <32>`: the size of this fill."""

    # When it happened, and whatever was said about it.

    transacttime: Annotated[datetime.datetime | None, DECLARED["TransactTime"]] = None
    """`TransactTime <60>`: when the business event happened, in UTC."""

    text: Annotated[str | None, DECLARED["Text"]] = None
    """`Text <58>`: whatever the counterparty wrote, often the reject reason."""

    # Quote identity, terms and lifecycle. Repeating mass-quote entries remain
    # in `entries`; a value is lifted only when it occurs once on the line.

    quoteid: Annotated[str | None, DECLARED["QuoteID"]] = None
    """`QuoteID <117>`: quote lifecycle identifier."""

    quotereqid: Annotated[str | None, DECLARED["QuoteReqID"]] = None
    """`QuoteReqID <131>`: request this quote answers."""

    quotetype: Annotated[int | None, DECLARED["QuoteType"]] = None
    """`QuoteType <537>`: indicative, tradeable or restricted quote kind."""

    quotestatus: Annotated[int | None, DECLARED["QuoteStatus"]] = None
    """`QuoteStatus <297>`: quote acknowledgement state."""

    quoterejectreason: Annotated[int | None, DECLARED["QuoteRejectReason"]] = None
    """`QuoteRejectReason <300>` when a quote is rejected."""

    quoteresptype: Annotated[int | None, DECLARED["QuoteRespType"]] = None
    """`QuoteRespType <694>`: quote response action."""

    quotecanceltype: Annotated[int | None, DECLARED["QuoteCancelType"]] = None
    """`QuoteCancelType <298>`: scope of a quote cancellation."""

    bidpx: Annotated[float | None, DECLARED["BidPx"]] = None
    """`BidPx <132>`: quoted bid price."""

    offerpx: Annotated[float | None, DECLARED["OfferPx"]] = None
    """`OfferPx <133>`: quoted offer price."""

    bidsize: Annotated[float | None, DECLARED["BidSize"]] = None
    """`BidSize <134>`: quoted bid quantity."""

    offersize: Annotated[float | None, DECLARED["OfferSize"]] = None
    """`OfferSize <135>`: quoted offer quantity."""

    defbidsize: Annotated[float | None, DECLARED["DefBidSize"]] = None
    """`DefBidSize <293>`: default bid quantity for a quote set."""

    defoffersize: Annotated[float | None, DECLARED["DefOfferSize"]] = None
    """`DefOfferSize <294>`: default offer quantity for a quote set."""

    validuntiltime: Annotated[datetime.datetime | None, DECLARED["ValidUntilTime"]] = None
    """`ValidUntilTime <62>`: quote expiry in UTC."""

    noquotesets: Annotated[int | None, DECLARED["NoQuoteSets"]] = None
    """`NoQuoteSets <296>`: quote-set group count."""

    noquoteentries: Annotated[int | None, DECLARED["NoQuoteEntries"]] = None
    """`NoQuoteEntries <295>`: quote-entry group count."""

    quotesetid: Annotated[str | None, DECLARED["QuoteSetID"]] = None
    """`QuoteSetID <302>`: quote-set identifier."""

    quoteentryid: Annotated[str | None, DECLARED["QuoteEntryID"]] = None
    """`QuoteEntryID <299>`: stable quote-entry identifier."""

    # Last, and lists: what the instrument's two repeating groups carry. Last
    # because Iceberg counts leaf columns in declaration order for the bounds
    # it collects, and this contract already crosses that cutoff -- a nested
    # member declared earlier would push flat columns past it. The three
    # components above predate the cutoff being crossed; new ones go here.

    securityaltid: Annotated[
        list[SecurityAltIDEntry] | None,
        Field(
            dtype=SECURITY_ALT_IDS,
            metadata={"fix:component": "SecurityAltID"},
        ),
    ] = None
    """FIX SecAltIDGrp entries -- every other identifier; null when absent."""

    legs: Annotated[
        list[Leg] | None,
        Field(
            dtype=LEGS,
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
        registry: FixRegistry | None = None,
        **declared: Any,
    ) -> FixMsg:
        """Build a scalar parsed row from one ordered FIX payload.

        `Message.from_text` owns the tokenization and the discriminator; this
        is `from_message` over that staged raw row.
        """
        staged = Message.from_text(text, separator, named=named, entry_separator=entry_separator)
        return cls.from_message(staged, registry=registry, **declared)

    @classmethod
    def from_pairs(
        cls,
        pairs: Iterable[tuple[Any, Any]],
        names: Mapping[str, int | str] | None = None,
        *,
        registry: FixRegistry | None = None,
        **declared: Any,
    ) -> FixMsg:
        """Build a scalar parsed row from ordered named or numbered fields."""
        staged = Message(entries=normalized_pairs(pairs, names))
        return cls.from_message(staged, registry=registry, **declared)

    @classmethod
    def from_message(
        cls,
        source: Message,
        *,
        registry: FixRegistry | None = None,
        **declared: Any,
    ) -> FixMsg:
        """Transcribe one raw row: the scalar half of `from_message_batch`.

        The raw stage already tokenized the payload and promoted the
        discriminator, so the row carries over whole -- envelope, provenance
        and residual arguments. `eventtype` classifies under the registry only
        where the raw stage left it unknown, and identity resets: a parsed
        row hashes over its parsed values, not the raw line's digest.
        """
        if not isinstance(source, Message):
            raise TypeError(f"source must be Message, got {type(source).__name__}")
        values = {
            member.name: getattr(source, member.name) for member in dataclasses.fields(Message)
        }
        values.update(
            {
                "message": source.message or None,
                "entries": list(source.entries or ()),
                "linkedhashes": list(source.linkedhashes),
                "altids": dict(source.altids),
                "parenthash": None if source.parenthash is None else list(source.parenthash),
                "hash": NIL,
                "vhash": NIL,
                "xhash": NIL,
            }
        )
        values.update(_session_values(source))
        values.update(declared)
        msg_type = values.get("msgtype")
        if (
            "eventtype" not in declared
            and msg_type is not None
            and source.eventtype == EventType.UNKNOWN
        ):
            values["eventtype"] = (
                (registry or cls.into_registry())
                .msg_type_event_types()
                .get(msg_type, EventType.UNKNOWN)
            )
        return cls(**values).link_registry(registry)

    def into_dict(self) -> dict[str, Any]:
        """Plain values with the stored fields in Arrow's list-struct spelling."""
        encoded = Event.into_dict(self)
        encoded["entries"] = _stored_entries(self.entries)
        encoded["unmap"] = _stored_entries(self.unmap)
        return encoded

    def into_row(self) -> dict[str, Any]:
        """The stored row, with Arrow's list-struct spelling for retained fields."""
        encoded = MarketConvertible.into_row(self)
        encoded["entries"] = _stored_entries(self.entries)
        encoded["unmap"] = _stored_entries(self.unmap)
        return encoded

    def get(self, field: int | str) -> Reading:
        """One field off this row, whichever of the four ways it is named.

        The one accessor (fix/access.py) reads the promoted columns first and
        the stored residual lists after them, so a lifted fact and a residual one
        answer through one call. The `Reading` carries the stored value and
        the typed reading together.
        """
        return self._row_access().reading(self._field_entries(), field)

    def readings(self, field: int | str) -> list[Reading]:
        """Every value of `field` on this row, in stored order."""
        return self._row_access().readings(self._field_entries(), field)

    def into_fix_pairs(self, access: FieldAccess | None = None) -> list[tuple[str, str]]:
        """Ordered FIX fields projected once from columns, components, and retained entries.

        When an accessor resolves both a numeric field and a rendered field to
        one registry identity, the rendered occurrence is authoritative. Its
        position relative to other rendered fields is unchanged.
        """
        resolver = access or self._row_access()
        _, pairs, resolved = self._canonical_fields(resolver)
        return [
            (
                (tagged if access is not None else source)[0],
                _fix_text(resolver.canonical_value(tagged[0], source[1])),
            )
            for source, tagged in zip(pairs, resolved, strict=True)
        ]

    def _canonical_fields(
        self, access: FieldAccess
    ) -> tuple[list[Entry], list[tuple[str, Any]], list[tuple[str, Any]]]:
        """Structured entries with source-spelled and tag-resolved keys.

        One aligned pass, retaining stored value types: the dedup rules run
        on the rendered keys, and every surviving position keeps its ready
        `Entry` -- so a field read never re-splits a spelling the stored
        shape already holds apart.
        """
        stored_entries = self._residual_entries()
        stored = [(entry.spelling, entry.value) for entry in stored_entries]
        stored_resolved = access.tagged_pairs(stored)
        stored_identities = {
            _pair_identity(pair[0])
            for source, pair in zip(stored, stored_resolved, strict=True)
            if not _component_key(source[0])
        }

        promoted_entries = [
            Entry.of(tag=int(tag), key=str(tag), value=value)
            for name, tag in type(self).into_tagged_columns()
            if (value := getattr(self, name, None)) is not None
        ]
        promoted_entries.extend(
            Entry.of(key=spelled, value=value)
            for name, spelled in type(self).into_named_columns()
            if (value := getattr(self, name, None)) is not None
        )
        promoted = [(entry.spelling, entry.value) for entry in promoted_entries]
        promoted_resolved = access.tagged_pairs(promoted)
        keep_promoted = [
            _pair_identity(pair[0]) not in stored_identities for pair in promoted_resolved
        ]
        promoted_entries = [
            entry for entry, kept in zip(promoted_entries, keep_promoted, strict=True) if kept
        ]
        promoted = [pair for pair, kept in zip(promoted, keep_promoted, strict=True) if kept]

        component_fields: list[tuple[str, Entry]] = []
        for column, count_name, row_type in _component_groups():
            entries = getattr(self, column, None)
            if entries is None:
                continue
            count = _tag_of(count_name)
            if _pair_identity(str(count)) not in stored_identities:
                component_fields.extend(_component_fields(count, entries, row_type))
        component_records = [entry for _, entry in component_fields]
        components = [(key, entry.value) for key, entry in component_fields]

        # The promoted discriminator re-enters at its wire-legal position:
        # after the leading BeginString/BodyLength run the raw stage retained.
        # The projection then keeps the wire's own order, and a
        # rendered row re-parses whole -- a `35=` in front of the `8=` anchor
        # would be shed as log noise.
        head = 0
        while head < len(stored) and stored[head][0] in ("8", "9"):
            head += 1
        if head:
            at = next((index for index, pair in enumerate(promoted) if pair[0] == "35"), None)
            if at is not None:
                stored = [*stored[:head], promoted[at], *stored[head:]]
                stored_entries = [
                    *stored_entries[:head],
                    promoted_entries[at],
                    *stored_entries[head:],
                ]
                promoted = [*promoted[:at], *promoted[at + 1 :]]
                promoted_entries = [*promoted_entries[:at], *promoted_entries[at + 1 :]]
                stored_resolved = access.tagged_pairs(stored)

        fields = [*promoted_entries, *component_records, *stored_entries]
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
            return fields, pairs, resolved

        group_version = access.version or self.protocolversion
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
            [entry for entry, kept in zip(fields, keep, strict=True) if kept],
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

    def resolved_version(self, registry: FixRegistry | None = None) -> str | None:
        """Which protocol version this row reads under, inferred when unresolved."""
        if self.protocolversion is not None:
            return self.protocolversion
        try:
            return infer_version_from_pairs(self.pairs, registry or self.registry)[0]
        except (OSError, ValueError):
            return None

    @property
    def has_indexed_entries(self) -> bool:
        """Whether a rendered group path survives only in source spelling."""
        return any(entry.comp or "[" in entry.key for entry in self._residual_entries())

    def component_records(self, column: str) -> list[dict[str, str]] | None:
        """One resolved component column, each entry first-value-by-name.

        None when the parse stage did not resolve the column, which is what
        sends a scalar-built message down the pair-walking fallback. A stored
        row hands entries back as mappings and a constructed one as `@scalar`
        rows; both answer here, typed members rendered back to the wire's
        spelling so the same readers serve both paths, and `buffer` merged
        after them because a member kept as text was one a column could not
        hold.
        """
        entries = getattr(self, column, None)
        if entries is None:
            return None
        found: list[dict[str, str]] = []
        for entry in entries:
            if isinstance(entry, Mapping):
                values = dict(entry)
            else:
                values = {
                    member.name: getattr(entry, member.name, None)
                    for member in dataclasses.fields(entry)
                }
            resolved: dict[str, str] = {}
            for name, value in values.items():
                if name != "buffer" and value is not None:
                    resolved.setdefault(name, render_fix_value(value))
            for name, value in dict(values.get("buffer") or {}).items():
                resolved.setdefault(name, value)
            found.append(resolved)
        return found

    def into_first_values(self, access: FieldAccess | None = None) -> dict[str, Any] | None:
        """Promoted columns and simple numeric residuals without a FIX round trip.

        The first occurrence of each wire key wins, as a flat message read
        does. None when a component column or a rendered entry survives on
        this row, which is what sends a reader down the canonical projection.
        """
        if any(getattr(self, name, None) is not None for name in COMPONENT_COLUMNS):
            return None
        if self.unmap is not None:
            return None
        entries = self.entries or ()
        if any(entry.comp or not entry.tag for entry in entries):
            return None

        resolver = access or self._row_access()
        stored = [(str(entry.tag), entry.value) for entry in entries]
        stored_tags = {tag for tag, _ in stored}
        found: dict[str, Any] = {}
        for name, tag in type(self).into_tagged_columns():
            value = getattr(self, name, None)
            if value is None or tag in stored_tags:
                continue
            found[tag] = render_fix_value(resolver.canonical_value(tag, value))
        for name, spelling in type(self).into_named_columns():
            value = getattr(self, name, None)
            if value is not None:
                found[spelling] = render_fix_value(resolver.canonical_value(spelling, value))
        for tag, value in stored:
            found.setdefault(tag, render_fix_value(resolver.canonical_value(tag, value)))
        return found

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
        """The canonical field sequence as accessor-ready entries."""
        fields, _, _ = self._canonical_fields(self._row_access())
        return fields

    @classmethod
    @functools.cache
    def into_named_columns(cls) -> tuple[tuple[str, str], ...]:
        """`(attribute, registry spelling)` for lifted columns FIX never numbered."""
        return tuple(
            (member.name, spelled)
            for member in cls.into_field().fields
            if not member.fix.get("tag") and (spelled := member.fix.get("name"))
        )

    # -- the FIX stage --------------------------------------------------------

    @classmethod
    def from_message_batch(
        cls,
        source: pyarrow.RecordBatch | Iterable[Message],
        codec: FixCodec | FixRegistry | None = None,
    ) -> pyarrow.RecordBatch:
        """Transcribe raw messages as one parsed Arrow batch.

        The vectorized half of the one conversion path -- raw `Message` to
        `FixMsg` to typed market events; `from_message` is the row-by-row
        half. `source` is one raw-contract RecordBatch or an iterable of
        scalar `Message` rows. A feed's `FixRegistry` is all the conversion
        needs -- the codec derives from it -- and a full `FixCodec` is for
        the feeds whose rules or field declarations differ; the default
        reads the packaged registry.
        """
        selected = codec if isinstance(codec, FixCodec) else cls.into_codec(codec)
        if isinstance(source, pyarrow.RecordBatch):
            return cls._from_message_batch(source, selected)
        rows = list(source)
        for row in rows:
            if not isinstance(row, Message):
                raise TypeError(f"from_message_batch takes Message rows, got {type(row).__name__}")
        schema = Message.into_field().into_arrow_schema()
        if not rows:
            staged = pyarrow.RecordBatch.from_pylist([], schema=schema)
        else:
            table = pyarrow.Table.from_batches(
                Message.into_arrow_reader(rows, batch_row_size=len(rows)), schema=schema
            ).combine_chunks()
            staged = table.to_batches(max_chunksize=table.num_rows)[0]
        return cls._from_message_batch(staged, selected)

    @classmethod
    def _from_message_batch(cls, batch: pyarrow.RecordBatch, codec: Any) -> pyarrow.RecordBatch:
        """Transcribe one classified raw `Message` batch under a FIX codec."""
        if not isinstance(batch, pyarrow.RecordBatch):
            raise TypeError(f"FixMsg conversion needs a RecordBatch, got {type(batch).__name__}")
        # A batch scanned back out of Iceberg carries `large_string` where the
        # raw contract says `string`, and the vectorized path below joins those
        # columns against constants it builds itself -- which Arrow refuses
        # across the two widths. The declaration is the one reading, so the
        # batch is brought onto it here rather than at each kernel. Narrowed
        # and not merged: this stage is read with `message` projected away, and
        # filling a column the reader did not select would invent the text it
        # deliberately left behind.
        batch = Message.into_field().narrowed(batch.schema).cast_arrow_batch(batch)
        rows = batch.num_rows
        columns = {name: batch.column(name) for name in batch.schema.names}
        columns.update(_session_batch_columns(columns))
        messages = columns.get("message")
        if messages is not None:
            # Protocol and direction are both read off the raw line, so both are
            # answered here and by the same rule: a row that still carries its
            # text is classified again under *this* codec's rules -- the ones it
            # is then parsed with -- and a row whose text a projection dropped
            # keeps the answer the message stage stored, because there is no
            # other. Direction is written back onto the batch, appended where
            # the batch has no such column, so the fast path's slices carry it.
            carries_text = pyarrow.compute.fill_null(
                pyarrow.compute.greater(pyarrow.compute.binary_length(messages), 0), False
            )
            protocols = codec.rules.into_arrow_protocol_array(messages, columns.get("plugincode"))
            direction = codec.rules.into_arrow_direction_array(messages, protocols)
            stored_protocols = columns.get("protocol")
            if stored_protocols is not None:
                protocols = pyarrow.compute.if_else(
                    carries_text,
                    protocols,
                    pyarrow.compute.fill_null(
                        stored_protocols.cast(_PROTOCOL_CODE, safe=False), Protocol.OTHER
                    ),
                )
            stored_direction = columns.get("direction")
            if stored_direction is not None:
                direction = pyarrow.compute.if_else(carries_text, direction, stored_direction)
            columns["direction"] = direction
            if "direction" in batch.schema.names:
                at = batch.schema.get_field_index("direction")
                batch = batch.set_column(at, batch.schema.field(at), direction)
            else:
                batch = batch.append_column(
                    Message.into_field().into_arrow_schema().field("direction"), direction
                )
        else:
            protocols = columns.get("protocol")
            if protocols is None:
                raise ValueError(
                    "a projected Message batch needs protocol; reparse the "
                    "messages before dropping message"
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
                    cls._from_message_batch_reference(
                        fallback,
                        codec,
                        fallback_columns,
                        pyarrow.compute.take(protocols, fallback_at),
                    )
                )
                fast_positions.append(fallback_at)
            return _scatter_record_batches(fast_parts, fast_positions)
        return cls._from_message_batch_reference(batch, codec, columns, protocols)

    @classmethod
    def _from_message_batch_reference(
        cls,
        batch: pyarrow.RecordBatch,
        codec: Any,
        columns: dict[str, pyarrow.Array],
        protocols: pyarrow.Array,
    ) -> pyarrow.RecordBatch:
        """Transcribe rows through the registry's complete configurable path."""
        rows = batch.num_rows
        # Every way into this method hands over columns taken straight off a
        # batch, including the fast path's fallback slice, so the header
        # columns the raw stage lifted are read as this stage stores them here
        # rather than at each caller.
        columns = {**columns, **_session_batch_columns(columns)}
        parts, positions = [], []
        for protocol, where in groups_of(protocols):
            rule = codec.rules.rule(protocol.as_py())
            if rule.named is None:
                parts.append(pyarrow.nulls(len(where), ENTRIES))
            else:
                entries = (
                    columns["entries"]
                    if len(where) == rows
                    else pyarrow.compute.take(columns["entries"], where)
                )
                pairs = codec.drop_null_values(
                    codec.into_payload_pairs(
                        codec.into_pairs_from_entries(entries, protocol.as_py())
                    )
                )
                parts.append(codec.into_message_entries(pairs))
            positions.append(where)
        entries = scattered(parts, positions) if parts else pyarrow.nulls(rows, ENTRIES)
        protocolversion, protocolversionsource = codec.versions_of_entries(
            entries, columns.get("beginstring"), columns.get("applverid")
        )
        columns.update(
            {
                "protocol": protocols,
                "protocolversion": protocolversion,
                "protocolversionsource": protocolversionsource,
                "entries": entries,
            }
        )
        schema = cls._message_schema(batch.schema)
        for field in schema:
            columns.setdefault(field.name, pyarrow.nulls(rows, field.type))
        columns.update(cls._resolved_batch_columns(columns, codec, rows))
        columns["mic"] = _mic_arrow(columns, rows)
        return cls.identified(columns, schema, rows, codec.registry)

    @classmethod
    def _message_schema(cls, source: pyarrow.Schema) -> pyarrow.Schema:
        """The FixMsg contract followed by caller-declared raw columns.

        A caller's column collides by what it folds to, not by how it is
        spelled: `OrigClOrdID` and `origclordid` are one column asked for
        twice, and appending both would leave the shape with two.
        """
        schema = cls.into_field().into_arrow_schema()
        own = {column_name(name) for name in schema.names}
        raw = {column_name(name) for name in Message.into_field().names}
        collisions = [name for name in source.names if column_name(name) in own - raw]
        if collisions:
            raise ValueError(
                f"raw message columns collide with FixMsg fields {collisions}; "
                "rename the caller-declared columns"
            )
        extra = [field for field in source if column_name(field.name) not in own]
        return pyarrow.schema([*schema, *extra], metadata=schema.metadata)

    @classmethod
    def _resolved_batch_columns(
        cls, columns: Mapping[str, Any], codec: Any, rows: int
    ) -> dict[str, Any]:
        """Resolve each version-homogeneous slice and restore batch order."""
        compute = pyarrow.compute
        versions = compute.fill_null(columns["protocolversion"], "")
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
        """One homogeneous slice: `entries` completed, and what it gives up to columns."""
        entries = codec.complete_entries(columns["entries"], version)
        entries = cls._prefer_named_entries(
            columns["entries"], entries, codec.registry.group_count_tags(version)
        )
        components, entries = codec.into_component_columns(entries, version)
        lifted, entries = codec.into_lifted_columns(entries, version)
        entries, unmap = cls._partition_entries(entries, codec, version)
        found: dict[str, Any] = {
            **components,
            **lifted,
            "entries": entries,
            "unmap": unmap,
        }
        # A lifted value only fills a column already read directly where it is empty:
        # `MsgType` is read off the front of the message before any of this,
        # and the wire is the authority on what it says.
        for name, column in found.items():
            stored = columns.get(name)
            if name not in {"entries", "unmap"} and stored is not None and stored.null_count < rows:
                found[name] = pyarrow.compute.coalesce(cast_arrow_fix(column, stored.type), stored)
        return found

    @staticmethod
    def _partition_entries(
        entries: Any, codec: Any, version: str | None
    ) -> tuple[pyarrow.Array, pyarrow.Array]:
        """Retained fields split by identity in the selected version's registry index."""
        if isinstance(entries, pyarrow.ChunkedArray):
            entries = entries.combine_chunks()
        rows = len(entries)
        compute = pyarrow.compute
        items = compute.list_flatten(entries)
        parents = compute.list_parent_indices(entries).cast(pyarrow.int64())
        tags = compute.struct_field(items, "tag")
        keys = compute.struct_field(items, "key")
        comp = compute.struct_field(items, "comp")
        whole = compute.if_else(
            compute.is_valid(comp),
            compute.binary_join_element_wise(compute.fill_null(comp, ""), keys, "."),
            keys,
        )
        index = codec.index_of(version)
        _, named_hit, _, _ = index.resolve_with_match(whole)
        known = compute.if_else(
            compute.not_equal(tags, 0),
            compute.fill_null(compute.is_in(tags, value_set=codec.known_tags), False),
            compute.fill_null(named_hit, False),
        )
        mapped = _entry_subset(
            parents,
            items,
            known,
            rows,
            compute.is_null(entries) if entries.null_count else None,
        )
        unknown = compute.invert(known)
        unknown_sizes = dense_counts(compute.filter(parents, unknown), rows)
        unmap = _entry_subset(
            parents,
            items,
            unknown,
            rows,
            compute.equal(unknown_sizes, 0),
            unknown_sizes,
        )
        return mapped, unmap

    @staticmethod
    def _prefer_named_entries(
        source: Any, resolved: Any, group_count_tags: Collection[int] = ()
    ) -> Any:
        """Drop a flat numeric copy a named field of one identity repeats.

        A wrapper re-renders its own payload, so `54=1` beside `Side=1` is one
        field written twice and the rendered spelling is what the row keeps.
        Two *different* values are not a repetition but a conflict, and both
        stay: which of them the sender meant is not this stage's to decide.

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
        # The first named occurrence of an identity is what a numeric copy is
        # compared against: `named_values` and `named_identities` come off one
        # mask, so `index_in` indexes both.
        values = compute.struct_field(entries, "value")
        named_values = compute.filter(values, named)
        repeated = compute.fill_null(
            compute.equal(
                values, compute.take(named_values, compute.index_in(identities, named_identities))
            ),
            False,
        )
        duplicate = compute.and_(
            numeric,
            compute.and_(
                compute.fill_null(compute.is_in(identities, value_set=named_identities), False),
                repeated,
            ),
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
        return build_list(ENTRIES, sizes, values, mask)

    @classmethod
    def identified(
        cls,
        columns: dict[str, Any],
        schema: pyarrow.Schema,
        rows: int,
        registry: FixRegistry | None = None,
    ) -> pyarrow.RecordBatch:
        """The envelope a row earns: its instrument, its time, its identity.

        The FIX conversion ends here so `hash`, transaction time and lifecycle
        identifiers are derived only after the full registry projection exists.
        """
        from rekep.market.transacted import resolve_arrow

        compute = pyarrow.compute
        columns["symbolticker"] = SymbolTicker.into_arrow_array(columns, rows, registry)
        columns["code"] = cls.code_arrow(columns, rows)
        columns["altids"] = cls.altids_arrow(columns, rows)
        columns["reason"] = compute.coalesce(columns.get("text"), columns["reason"])
        columns["unix"], columns["unixsource"] = resolve_arrow(columns, columns["recunix"], rows)
        columns["unixpartition"] = unix_partition_arrow(columns["unix"])
        columns["creaunix"] = columns["unix"]
        columns["vhash"] = cls.version_vhash_arrow(columns, rows)
        columns["hash"] = txhash.couple128_arrow(
            cls._clock_micros(columns["unix"]), columns["vhash"]
        )
        linked = compute.not_equal(columns["code"], "")
        columns["xhash"] = compute.if_else(
            linked, cls.hash_arrow(columns["code"]), columns["vhash"]
        )
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
        """What a stored row's `vhash` is taken over, in this order.

        The **parsed** values and never the raw line, so a message reformatted
        but not changed hashes alike. Every clock is excluded; `unixsource`
        remains because it states which field supplied the event time.
        """
        return (
            "unixsource",
            "sourceurl",
            "sourcerownum",
            "protocol",
            "protocolversion",
            "msgtype",
            "entries",
            "unmap",
        )

    @classmethod
    def version_vhash_arrow(cls, columns: Mapping[str, Any], rows: int) -> pyarrow.Array:
        """One value hash per row, over parsed values rather than the raw line.

        A row that could not be read as a message has no parsed values, so it
        hashes on the raw line instead -- which is the one stated exception to
        the rule, and honest: for such a row the raw string *is* the content.
        """
        compute = pyarrow.compute
        parsed = [_digest_text(columns.get(name), rows) for name in cls.into_digest_columns()]
        digests = cls.hash_arrow(*parsed)
        stored = columns.get("entries")
        unmapped = columns.get("unmap")
        if stored is None and unmapped is None:
            return digests
        unread = compute.and_(
            compute.is_null(stored) if stored is not None else pyarrow.repeat(True, rows),
            compute.is_null(unmapped) if unmapped is not None else pyarrow.repeat(True, rows),
        )
        if not compute.any(unread, min_count=0).as_py():
            return digests
        recomputed = hash_bytes_arrow(_digest_text(columns.get("message"), rows))
        incoming = columns.get("vhash")
        raw = (
            recomputed
            if incoming is None
            else compute.if_else(
                compute.and_(
                    compute.is_valid(incoming),
                    compute.not_equal(incoming, pyarrow.scalar(NIL, pyarrow.int64())),
                ),
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
    def altids_arrow(
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
                columns.get("entries"),
                tuple((tag, field) for _, field, tag in identified if tag > 0),
                rows,
            )
            if columns.get("entries") is not None
            else {}
        )
        available = []
        for stored, field, _ in identified:
            # The column carries the folded name, which is what `stored` is;
            # the residual is keyed by the dictionary's spelling, because that
            # is what was asked of `entries`.
            promoted = columns.get(stored)
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
                ALTIDS_TYPE,
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
            ALTIDS_TYPE,
            sizes,
            compute.filter(compute.take(pyarrow.array(names), member), present),
            compute.filter(flat, present),
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

        carried = {
            "recunix": self.recunix or self.unix,
            "mic": self.mic,
            "registry": getattr(self, "_FixMsg__registry", None),
            **declared,
        }
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

        if self.unixsource:
            built.__dict__["transacted"] = Transacted(self.unix, self.unixsource)
        return built

    def into_market_events(self, **declared: Any) -> Iterator[Any]:
        """Translate this parsed row into its ordered market events."""
        for event in self.into_fix_events(**declared):
            if self.reason and not event.reason:
                event.reason = self.reason
                event.vhash = event.hash = NIL
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
        """Yield grouped instrument facts, or one declared row projection."""
        translated = tuple(self.into_fix_events(**declared).into_instruments())
        if translated:
            yield from translated
            return
        from rekep.market.instrument import Instrument

        instrument = Instrument.from_(self, **declared)
        if instrument.symbolticker:
            yield instrument

    def into_instrument(self, **declared: Any) -> Any | None:
        """Build the first instrument carried by this row, when any."""
        return next(self.into_instruments(**declared), None)


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
    stored = columns.get("entries")
    tags = (
        FieldAccess.first_arrow_tags(stored, (30, 100, 275, 1301), rows)
        if stored is not None
        else {}
    )
    explicit = [
        tags.get(30, missing),
        columns.get("securityexchange", missing),
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
    sender_source = columns.get("sendercompid", missing)
    target_source = columns.get("targetcompid", missing)
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


def _stored_entries(entries: Sequence[Any] | None) -> list[dict[str, Any]] | None:
    """Stored fields in the spelling Arrow accepts without a shape pass."""
    return None if entries is None else [Entry.from_stored(entry).into_dict() for entry in entries]


def _entry_subset(
    parents: pyarrow.Array,
    items: pyarrow.Array,
    keep: pyarrow.Array,
    rows: int,
    mask: pyarrow.Array | None,
    sizes: pyarrow.Array | None = None,
) -> pyarrow.Array:
    """One filtered side of an `ENTRIES` column, preserving child order."""
    selected = pyarrow.StructArray.from_arrays(
        [
            pyarrow.compute.filter(pyarrow.compute.struct_field(items, field.name), keep)
            for field in items.type
        ],
        fields=list(items.type),
    )
    if sizes is None:
        sizes = dense_counts(pyarrow.compute.filter(parents, keep), rows)
    return build_list(ENTRIES, sizes, selected, mask)


@functools.cache
def _tags_by_name() -> Mapping[str, int]:
    """Canonical FIX names to the tags declared by this parsed-row contract."""
    return MappingProxyType(
        {column_name(name): member.fix.tag for name, member in DECLARED.items()}
    )


def _tag_of(name: str) -> int:
    """The contract tag of one canonical FIX name."""
    return _tags_by_name()[column_name(name)]


def _component_fields(
    count_tag: int, entries: Sequence[Any], row_type: type[Any]
) -> list[tuple[str, Entry]]:
    """One typed component restored as a valid count-led repeating group.

    `(spelling, entry)` per field: a constructed member spells as its tag,
    and a `buffer` key keeps its stored spelling byte for byte.
    """
    fields: list[tuple[str, Entry]] = [
        (str(count_tag), Entry.of(tag=count_tag, key=str(count_tag), value=len(entries)))
    ]
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
                tag = int(member.fix["tag"])
                fields.append((str(tag), Entry.of(tag=tag, key=str(tag), value=value)))
        for key, value in buffered.items():
            fields.append((str(key), Entry.from_pair(str(key), value)))
    return fields


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
    return ("tag", int(text)) if _numeric_key(text) else ("name", column_name(text))


@functools.cache
def _session_types() -> Mapping[str, Any]:
    """Which of the lifted header fields this stage stores as something else.

    The raw stage is protocol-neutral and keeps every one of them as the text
    the payload spelled; a `BodyLength` is a number here and a `SendingTime` an
    instant, so the two stages disagree on exactly three of the seven and this
    is the list of them.
    """
    declared = FixMsg.into_field()
    return {
        name: declared.field(name).dtype
        for name, _ in SESSION_FIELDS
        if not pyarrow.types.is_string(declared.field(name).dtype)
    }


def _session_batch_columns(columns: Mapping[str, Any]) -> dict[str, Any]:
    """The header columns the raw stage lifted, read as this stage stores them.

    The raw stage is protocol-neutral and keeps every one of them as text; a
    `BodyLength` is a number here and a `SendingTime` an instant, so the three
    that differ are cast once for the whole batch instead of being scanned for
    again in `entries`.
    """
    found: dict[str, Any] = {}
    for name, dtype in _session_types().items():
        column = columns.get(name)
        if column is None:
            continue
        if pyarrow.types.is_string(column.type):
            found[name] = cast_arrow_fix(column, dtype)
    return found


def _session_values(source: Message) -> dict[str, Any]:
    """The header fields the raw stage already lifted, as this stage holds them.

    Read off the columns rather than out of `entries`: the raw stage parsed
    them once, and scanning the list again for facts already in hand is what
    this exists to stop.
    """
    typed = _session_types()
    found: dict[str, Any] = {}
    for name, _ in SESSION_FIELDS:
        value = getattr(source, name, None)
        if value is None:
            continue
        dtype = typed.get(name)
        found[name] = value if dtype is None else scalar_fix_value(value, dtype)
    return found


def _stored_pairs(entries: Sequence[Any] | None) -> Iterator[tuple[Any, Any]]:
    """Stored fields as the pairs a FIX reader addresses them by.

    The tag where the dictionary found one, and the rendered key -- name with
    whatever stood in front of it joined back -- where it did not. A plain
    `(key, value)` tuple is accepted as itself, so a caller writing a
    `FixMsg` by hand need not spell the whole struct out.
    """
    for entry in entries or ():
        if not isinstance(entry, Mapping):
            entry = Entry.from_stored(entry)
        lead = entry.get("comp")
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
    """A `ENTRIES` column as one string per row: every field, in wire order."""
    compute = pyarrow.compute
    entries = compute.list_flatten(column)
    spelled = compute.binary_join_element_wise(
        compute.fill_null(compute.struct_field(entries, "tag").cast(pyarrow.string()), ""),
        compute.fill_null(compute.struct_field(entries, "comp"), ""),
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
