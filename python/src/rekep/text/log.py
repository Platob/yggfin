"""The shape of one parsed log line, what decides which event it is, and what fills it."""

from __future__ import annotations

import dataclasses
import datetime
import functools
from collections.abc import Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import Annotated, Any, Protocol, runtime_checkable

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.enums import EventType
from rekep.fields import Field, scalar
from rekep.fix.columns import DECLARATIONS, ISIN_CODE
from rekep.fix.components import PARTIES, Party
from rekep.fix.rules import NO_PROTOCOL
from rekep.fix.transcribe import FIX_MISS_TAGS, FIX_TAGS, KEYVAL
from rekep.market.event import Event
from rekep.market.identity import NIL

_EVENT_CODE = pyarrow.int32()
_CONTRACT_METADATA = MappingProxyType({"version": "1"})
_INSTRUMENT_DRIVER = "rekep.instrument"
_INSTRUMENT_PROTOCOL = "REKEP"
_INSTRUMENT_KIND = "rekep.kind"
_INSTRUMENT_XHASH = "rekep.xhash"


@scalar(slots=True)
class Log(Event):
    """One parsed line of a trading log."""

    @classmethod
    @functools.cache
    def into_field_metadata(cls) -> Mapping[str, str]:
        """Contract metadata published with parsed log schemas."""
        return _CONTRACT_METADATA

    @classmethod
    @functools.cache
    def into_instrument_driver(cls) -> str:
        """Driver marker for normalized instrument lifecycle rows."""
        return _INSTRUMENT_DRIVER

    @classmethod
    @functools.cache
    def into_instrument_protocol(cls) -> str:
        """Protocol marker for normalized internal rows."""
        return _INSTRUMENT_PROTOCOL

    xhash: int = NIL
    """Digest of `xcode`, or the raw-line digest when no correlation code exists."""

    code: str = ""
    """Best readable record identifier present on this line."""

    xcode: str = ""
    """Best direct lifecycle correlation identifier present on this line."""

    # `code` names this record; `xcode` prefers the identifier that survives an
    # order amendment. Later fallbacks still give reference and market-data
    # lines a readable lifecycle without inventing a source-specific field.
    @classmethod
    @functools.cache
    def into_code_columns(cls) -> tuple[str, ...]:
        """Parsed columns tried for a readable record identifier."""
        return (
            "order_id",
            "cl_ord_id",
            "orig_cl_ord_id",
            "exec_id",
            "quote_entry_id",
            "quote_id",
            "quote_req_id",
            "security_id",
            "isincode",
            "symbol",
        )

    @classmethod
    @functools.cache
    def into_xcode_columns(cls) -> tuple[str, ...]:
        """Parsed columns tried for a lifecycle identifier."""
        return (
            "order_id",
            "orig_cl_ord_id",
            "cl_ord_id",
            "exec_id",
            "quote_entry_id",
            "quote_id",
            "quote_req_id",
            "security_id",
            "isincode",
            "symbol",
        )

    @classmethod
    @functools.cache
    def into_symbol_columns(cls) -> tuple[str, ...]:
        """Instrument identifiers used when FIX omits `Symbol <55>`."""
        return ("symbol", "security_id", "isincode")

    url: str = ""
    """Path of the log the line came from, as its filesystem addresses it."""

    thread_name: str = ""
    """Contents of the first bracketed field."""

    driver_name: str = ""
    """Contents of the second bracketed field -- the emitting module."""

    message: str = ""
    """Payload with the header and level stripped, continuation lines folded in."""

    protocol: str = NO_PROTOCOL
    """Which protocol the line carries; OTHER is a line that carries none."""

    # Lists preserve repeated keys and wire order. Null means no parsed
    # message; an empty list means a message with nothing left after lifting.
    fix_tags: Annotated[list[tuple[int, str]] | None, Field(arrow_type=FIX_TAGS)] = None
    """The message's fields under the tags FIX gives them, in wire order."""

    fix_miss_tags: Annotated[list[str] | None, Field(arrow_type=FIX_MISS_TAGS)] = None
    """Ordered raw keys absent from the selected FIX registry version."""

    keyval: Annotated[list[tuple[str, str]] | None, Field(arrow_type=KEYVAL)] = None
    """The fields no FIX tag answers for, spelled as the log spelled them."""

    parties: Annotated[
        list[Party] | None,
        Field(
            arrow_type=PARTIES,
            metadata={"fix:component": "Parties"},
        ),
    ] = None
    """FIX Parties entries; null when the component is absent."""

    isincode: Annotated[str | None, ISIN_CODE] = None
    """ISIN carried by a rendered `ISINCODE` field."""

    # -- what a message says, flattened ---------------------------------------
    #
    # Flat fields keep the registry's exact name, type, description and
    # metadata. A lifted fact is removed from `fix_tags`; repeated facts stay.

    # The envelope itself.

    begin_string: Annotated[str | None, DECLARATIONS[8]] = None
    """`BeginString <8>`: which FIX version the message says it is."""

    body_length: Annotated[int | None, DECLARATIONS[9]] = None
    """`BodyLength <9>`, as the message counted it."""

    msg_type: Annotated[str | None, DECLARATIONS[35]] = None
    """`MsgType <35>`: what the message is, on the wire."""

    check_sum: Annotated[str | None, DECLARATIONS[10]] = None
    """`CheckSum <10>`: three digits, so a string -- `010` read as `10` no longer verifies."""

    # Who sent it, and to whom.

    sender_comp_id: Annotated[str | None, DECLARATIONS[49]] = None
    """`SenderCompID <49>`: who sent it."""

    sender_sub_id: Annotated[str | None, DECLARATIONS[50]] = None
    """`SenderSubID <50>`: which desk of theirs."""

    sender_location_id: Annotated[str | None, DECLARATIONS[142]] = None
    """`SenderLocationID <142>`."""

    target_comp_id: Annotated[str | None, DECLARATIONS[56]] = None
    """`TargetCompID <56>`: who it was sent to."""

    target_sub_id: Annotated[str | None, DECLARATIONS[57]] = None
    """`TargetSubID <57>`."""

    target_location_id: Annotated[str | None, DECLARATIONS[143]] = None
    """`TargetLocationID <143>`."""

    # And on whose behalf, when a hub relayed it.

    on_behalf_of_comp_id: Annotated[str | None, DECLARATIONS[115]] = None
    """`OnBehalfOfCompID <115>`: who the sender was speaking for."""

    on_behalf_of_sub_id: Annotated[str | None, DECLARATIONS[116]] = None
    """`OnBehalfOfSubID <116>`."""

    on_behalf_of_location_id: Annotated[str | None, DECLARATIONS[144]] = None
    """`OnBehalfOfLocationID <144>`."""

    deliver_to_comp_id: Annotated[str | None, DECLARATIONS[128]] = None
    """`DeliverToCompID <128>`: who it is ultimately for."""

    deliver_to_sub_id: Annotated[str | None, DECLARATIONS[129]] = None
    """`DeliverToSubID <129>`."""

    deliver_to_location_id: Annotated[str | None, DECLARATIONS[145]] = None
    """`DeliverToLocationID <145>`."""

    # Where it sits in the session's stream, and whether it is a repeat.

    last_msg_seq_num_processed: Annotated[int | None, DECLARATIONS[369]] = None
    """`LastMsgSeqNumProcessed <369>`: how far the sender had read."""

    poss_dup_flag: Annotated[bool | None, DECLARATIONS[43]] = None
    """`PossDupFlag <43>`: a retransmission of a message already sent."""

    poss_resend: Annotated[bool | None, DECLARATIONS[97]] = None
    """`PossResend <97>`: the same business content under a new sequence."""

    # FIX documents these instants as UTC; microseconds are Iceberg-compatible.

    sending_time: Annotated[datetime.datetime | None, DECLARATIONS[52]] = None
    """`SendingTime <52>`: when it was transmitted."""

    orig_sending_time: Annotated[datetime.datetime | None, DECLARATIONS[122]] = None
    """`OrigSendingTime <122>`: the original transmission, on a resend."""

    on_behalf_of_sending_time: Annotated[datetime.datetime | None, DECLARATIONS[370]] = None
    """`OnBehalfOfSendingTime <370>`."""

    # Which application version speaks, under FIXT.

    appl_ver_id: Annotated[str | None, DECLARATIONS[1128]] = None
    """`ApplVerID <1128>`."""

    cstm_appl_ver_id: Annotated[str | None, DECLARATIONS[1129]] = None
    """`CstmApplVerID <1129>`."""

    appl_ext_id: Annotated[int | None, DECLARATIONS[1156]] = None
    """`ApplExtID <1156>`."""

    # How the payload is written, when it is not plain ASCII.

    message_encoding: Annotated[str | None, DECLARATIONS[347]] = None
    """`MessageEncoding <347>`."""

    xml_data_len: Annotated[int | None, DECLARATIONS[212]] = None
    """`XmlDataLen <212>`."""

    xml_data: Annotated[bytes | None, DECLARATIONS[213]] = None
    """`XmlData <213>`, as the bytes it is."""

    # And how it is sealed.

    secure_data_len: Annotated[int | None, DECLARATIONS[90]] = None
    """`SecureDataLen <90>`."""

    secure_data: Annotated[bytes | None, DECLARATIONS[91]] = None
    """`SecureData <91>`, as the bytes it is."""

    signature_length: Annotated[int | None, DECLARATIONS[93]] = None
    """`SignatureLength <93>`."""

    signature: Annotated[bytes | None, DECLARATIONS[89]] = None
    """`Signature <89>`, as the bytes it is."""

    # What was traded.

    symbol: Annotated[str | None, DECLARATIONS[55]] = None
    """`Symbol <55>`: ticker symbol."""

    security_id: Annotated[str | None, DECLARATIONS[48]] = None
    """`SecurityID <48>`, under the scheme `SecurityIDSource` names."""

    security_id_source: Annotated[str | None, DECLARATIONS[22]] = None
    """`SecurityIDSource <22>`: which scheme `SecurityID` is in -- `4` is ISIN."""

    security_type: Annotated[str | None, DECLARATIONS[167]] = None
    """`SecurityType <167>`."""

    cfi_code: Annotated[str | None, DECLARATIONS[461]] = None
    """`CFICode <461>`: what kind of instrument it is, as ISO 10962 spells it."""

    security_exchange: Annotated[str | None, DECLARATIONS[207]] = None
    """`SecurityExchange <207>`: the market the instrument is listed on."""

    currency: Annotated[str | None, DECLARATIONS[15]] = None
    """`Currency <15>`, which is what the prices below are in."""

    # Who asked, and under which identifiers.

    account: Annotated[str | None, DECLARATIONS[1]] = None
    """`Account <1>`."""

    cl_ord_id: Annotated[str | None, DECLARATIONS[11]] = None
    """`ClOrdID <11>`: the client's own identifier for the order."""

    orig_cl_ord_id: Annotated[str | None, DECLARATIONS[41]] = None
    """`OrigClOrdID <41>`: which order an amendment or cancel is about."""

    order_id: Annotated[str | None, DECLARATIONS[37]] = None
    """`OrderID <37>`: the venue's identifier for it."""

    exec_id: Annotated[str | None, DECLARATIONS[17]] = None
    """`ExecID <17>`: the venue's identifier for this execution report."""

    # On what terms.

    side: Annotated[str | None, DECLARATIONS[54]] = None
    """`Side <54>`: `1` buy, `2` sell, and the rest of the standard's codes."""

    ord_type: Annotated[str | None, DECLARATIONS[40]] = None
    """`OrdType <40>`: `1` market, `2` limit, ..."""

    time_in_force: Annotated[str | None, DECLARATIONS[59]] = None
    """`TimeInForce <59>`: `0` day, `1` GTC, `3` IOC, ..."""

    # Where it stands.

    ord_status: Annotated[str | None, DECLARATIONS[39]] = None
    """`OrdStatus <39>`: where the order stands."""

    exec_type: Annotated[str | None, DECLARATIONS[150]] = None
    """`ExecType <150>`: what this report is reporting."""

    # For how much, at what price.

    order_qty: Annotated[float | None, DECLARATIONS[38]] = None
    """`OrderQty <38>`: how much was asked for."""

    price: Annotated[float | None, DECLARATIONS[44]] = None
    """`Price <44>`: the limit, when there is one."""

    vwap: Annotated[float | None, DECLARATIONS[6]] = None
    """`AvgPx <6>`: the average of what has filled so far."""

    cum_qty: Annotated[float | None, DECLARATIONS[14]] = None
    """`CumQty <14>`: how much has filled."""

    leaves_qty: Annotated[float | None, DECLARATIONS[151]] = None
    """`LeavesQty <151>`: how much is still working."""

    last_px: Annotated[float | None, DECLARATIONS[31]] = None
    """`LastPx <31>`: the price of this fill."""

    last_qty: Annotated[float | None, DECLARATIONS[32]] = None
    """`LastQty <32>`: the size of this fill."""

    # When it happened, and whatever was said about it.

    transact_time: Annotated[datetime.datetime | None, DECLARATIONS[60]] = None
    """`TransactTime <60>`: when the business event happened, in UTC."""

    text: Annotated[str | None, DECLARATIONS[58]] = None
    """`Text <58>`: whatever the counterparty wrote, often the reject reason."""

    # Quote identity, terms and lifecycle. Repeating mass-quote entries remain
    # in `fix_tags`; a value is lifted only when it occurs once on the line.

    quote_id: Annotated[str | None, DECLARATIONS[117]] = None
    """`QuoteID <117>`: quote lifecycle identifier."""

    quote_req_id: Annotated[str | None, DECLARATIONS[131]] = None
    """`QuoteReqID <131>`: request this quote answers."""

    quote_type: Annotated[int | None, DECLARATIONS[537]] = None
    """`QuoteType <537>`: indicative, tradeable or restricted quote kind."""

    quote_status: Annotated[int | None, DECLARATIONS[297]] = None
    """`QuoteStatus <297>`: quote acknowledgement state."""

    quote_reject_reason: Annotated[int | None, DECLARATIONS[300]] = None
    """`QuoteRejectReason <300>` when a quote is rejected."""

    quote_resp_type: Annotated[int | None, DECLARATIONS[694]] = None
    """`QuoteRespType <694>`: quote response action."""

    quote_cancel_type: Annotated[int | None, DECLARATIONS[298]] = None
    """`QuoteCancelType <298>`: scope of a quote cancellation."""

    bid_px: Annotated[float | None, DECLARATIONS[132]] = None
    """`BidPx <132>`: quoted bid price."""

    offer_px: Annotated[float | None, DECLARATIONS[133]] = None
    """`OfferPx <133>`: quoted offer price."""

    bid_size: Annotated[float | None, DECLARATIONS[134]] = None
    """`BidSize <134>`: quoted bid quantity."""

    offer_size: Annotated[float | None, DECLARATIONS[135]] = None
    """`OfferSize <135>`: quoted offer quantity."""

    def_bid_size: Annotated[float | None, DECLARATIONS[293]] = None
    """`DefBidSize <293>`: default bid quantity for a quote set."""

    def_offer_size: Annotated[float | None, DECLARATIONS[294]] = None
    """`DefOfferSize <294>`: default offer quantity for a quote set."""

    valid_until_time: Annotated[datetime.datetime | None, DECLARATIONS[62]] = None
    """`ValidUntilTime <62>`: quote expiry in UTC."""

    no_quote_sets: Annotated[int | None, DECLARATIONS[296]] = None
    """`NoQuoteSets <296>`: quote-set group count."""

    no_quote_entries: Annotated[int | None, DECLARATIONS[295]] = None
    """`NoQuoteEntries <295>`: quote-entry group count."""

    quote_set_id: Annotated[str | None, DECLARATIONS[302]] = None
    """`QuoteSetID <302>`: quote-set identifier."""

    quote_entry_id: Annotated[str | None, DECLARATIONS[299]] = None
    """`QuoteEntryID <299>`: stable quote-entry identifier."""

    @classmethod
    def from_instrument(cls, instrument: Any, **declared: Any) -> Log:
        """Carry one normalized instrument version in the parsed-log stream."""
        from rekep.market.instrument import Instrument

        if not isinstance(instrument, Instrument):
            raise TypeError(f"instrument must be Instrument, got {type(instrument).__name__}")
        known = instrument if instrument.hash else dataclasses.replace(instrument).identify()
        values = {member.name: getattr(known, member.name) for member in dataclasses.fields(Event)}
        values.update(
            {
                "etype": EventType.INSTRUMENT,
                "url": "",
                "thread_name": "",
                "driver_name": cls.into_instrument_driver(),
                "message": "",
                "protocol": cls.into_instrument_protocol(),
                "msg_type": "d",
                "symbol": known.symbol or None,
                "security_id": known.security_id,
                "security_id_source": known.security_id_source,
                "isincode": known.isin_code,
                "security_type": known.security_type,
                "cfi_code": known.cfi,
                "security_exchange": known.exchange,
                "currency": None if known.currency is None else known.currency.into_fix(),
                "keyval": _instrument_pairs(known),
            }
        )
        values.update(declared)
        return cls(**values)

    def into_dict(self) -> dict[str, Any]:
        """Plain values with ordered pairs in Arrow's list-struct spelling."""
        encoded = Convertible.into_dict(self)
        for name in ("fix_tags", "keyval"):
            entries = getattr(self, name)
            if entries is not None:
                encoded[name] = _pair_entries(tuple(_stored_pairs(entries)))
        return encoded

    @property
    def is_instrument_version(self) -> bool:
        """Whether this row is a normalized instrument lifecycle version."""
        return (
            self.etype is EventType.INSTRUMENT
            and self.driver_name == type(self).into_instrument_driver()
        )

    @classmethod
    def code_arrow(cls, columns: Mapping[str, Any], rows: int) -> pyarrow.Array:
        """Best readable record identifier available in parsed FIX columns."""
        return _first_text(columns, cls.into_code_columns(), rows)

    @classmethod
    def symbol_arrow(cls, columns: Mapping[str, Any], rows: int) -> pyarrow.Array:
        """Most relevant readable instrument identifier on each row."""
        found = _first_text(columns, cls.into_symbol_columns(), rows)
        return pyarrow.compute.if_else(
            pyarrow.compute.equal(found, ""), pyarrow.nulls(rows, pyarrow.string()), found
        )

    @classmethod
    def xcode_arrow(cls, columns: Mapping[str, Any], rows: int) -> pyarrow.Array:
        """Best readable lifecycle identifier available in parsed FIX columns."""
        return _first_text(columns, cls.into_xcode_columns(), rows)

    def into_fix_events(self, **declared: Any) -> Any:
        """Rebuild the FIX market reader from promoted columns and residual pairs."""
        from rekep.market.fix import FixEvents

        carried = {"runix": self.unix, "mic": self.mic, **declared}
        pairs: list[tuple[Any, Any]] = []
        for member in type(self).into_field().fields:
            tag = member.fix.get("tag")
            value = getattr(self, member.name, None)
            if tag is not None and value is not None:
                pairs.append((tag, value))
        pairs.extend(_stored_pairs(self.fix_tags))
        pairs.extend(_stored_pairs(self.keyval))
        if pairs:
            return FixEvents.from_pairs(pairs, **carried)
        return (
            FixEvents.from_text(self.message, **carried) if self.message else FixEvents(**carried)
        )

    def into_market_events(self, **declared: Any) -> Iterator[Any]:
        """Translate this parsed row into its ordered market events."""
        for event in self.into_fix_events(**declared):
            if self.error and not event.error:
                event.error = self.error
                event.hash = NIL
                event.identify()
            yield event

    def into_instruments(self, **declared: Any) -> Iterator[Any]:
        """Yield distinct instrument facts, synthesizing a symbol-only row when needed."""
        normalized = dict(_stored_pairs(self.keyval)) if self.is_instrument_version else None
        if normalized is not None and _INSTRUMENT_KIND in normalized:
            yield self._instrument_version(self._normalized_instrument(normalized))
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

    def _flat_instrument(self) -> Any | None:
        """Build only the instrument facts already promoted on this row."""
        from rekep.market.instrument import Instrument

        symbol = self.symbol or self.security_id or self.isincode or ""
        if not symbol:
            return None
        return Instrument(
            symbol=symbol,
            security_id=self.security_id,
            security_id_source=self.security_id_source,
            isin_code=self.isincode,
            security_type=self.security_type,
            cfi=self.cfi_code,
            exchange=self.security_exchange,
            currency=self.currency,
        )

    def _normalized_instrument(self, pairs: Mapping[str, str]) -> Any:
        """Decode one package-authored instrument row without rebuilding FIX state."""
        from rekep.enums import AssetKind, IdSource, OptionKind, Side
        from rekep.market.instrument import Instrument, Leg

        alternatives: dict[str, str] = {}
        for index in range(_pair_count(pairs, "NoSecurityAltID")):
            root = f"NoSecurityAltID[{index}]"
            value = pairs.get(f"{root}.SecurityAltID")
            source = pairs.get(f"{root}.SecurityAltIDSource")
            if not value:
                continue
            scheme = IdSource.from_fix(source, IdSource.UNKNOWN)
            alternatives.setdefault(
                (
                    scheme.name
                    if scheme is not IdSource.UNKNOWN
                    else (source or IdSource.UNKNOWN.name)
                ),
                value,
            )

        legs = []
        for index in range(_pair_count(pairs, "NoLegs")):
            root = f"NoLegs[{index}]"

            def get(name: str, prefix: str = f"{root}.") -> str | None:
                return pairs.get(f"{prefix}{name}")

            cfi, security_type = get("LegCFICode"), get("LegSecurityType")
            fallback_kind = AssetKind.from_fix(cfi[:1], AssetKind.UNKNOWN) if cfi else None
            legs.append(
                Leg(
                    xhash=_pair_int(get(_INSTRUMENT_XHASH)) or NIL,
                    symbol=get("LegSymbol") or "",
                    side=Side.from_fix(get("LegSide"), Side.UNKNOWN),
                    ratio=_pair_float(get("LegRatioQty")),
                    kind=AssetKind.from_code(get(_INSTRUMENT_KIND), fallback_kind),
                    security_id=get("LegSecurityID"),
                    security_id_source=get("LegSecurityIDSource"),
                    cfi=cfi,
                    security_type=security_type,
                    exchange=get("LegSecurityExchange"),
                    currency=get("LegCurrency"),
                    multiplier=_pair_float(get("LegContractMultiplier")),
                    maturity=_pair_date(get("LegMaturityDate")),
                    strike=_pair_float(get("LegStrikePrice")),
                    option_kind=OptionKind.from_fix(get("LegPutOrCall"), OptionKind.UNKNOWN),
                )
            )

        fallback_kind = (
            AssetKind.from_fix(self.cfi_code[:1], AssetKind.UNKNOWN) if self.cfi_code else None
        )
        return Instrument(
            symbol=self.symbol or "",
            kind=AssetKind.from_code(pairs.get(_INSTRUMENT_KIND), fallback_kind),
            security_id=self.security_id,
            security_id_source=self.security_id_source,
            isin_code=self.isincode,
            alt_ids=alternatives or None,
            security_type=self.security_type,
            cfi=self.cfi_code,
            exchange=self.security_exchange,
            currency=self.currency,
            multiplier=_pair_float(pairs.get("ContractMultiplier")),
            tick=_pair_float(pairs.get("MinPriceIncrement")),
            lot=_pair_float(pairs.get("RoundLot")),
            maturity=_pair_date(pairs.get("MaturityDate")),
            strike=_pair_float(pairs.get("StrikePrice")),
            option_kind=OptionKind.from_fix(pairs.get("PutOrCall"), OptionKind.UNKNOWN),
            label=pairs.get("SecurityDesc"),
            legs=legs or None,
        )

    def _instrument_version(self, instrument: Any) -> Any:
        """Put decoded facts back on the lifecycle envelope this Log carries."""
        return dataclasses.replace(
            instrument,
            unix=self.unix,
            unix_hour=self.unix_hour,
            etype=EventType.INSTRUMENT,
            cunix=self.cunix,
            runix=self.runix,
            eunix=self.eunix,
            sunix=self.sunix,
            hash=self.hash,
            xhash=self.xhash,
            linked_xhash=None if self.linked_xhash is None else list(self.linked_xhash),
            version=self.version,
            state=self.state,
            code=self.code,
            xcode=self.xcode,
            seq=self.seq,
            prev_hash=self.prev_hash,
            prev_state=self.prev_state,
            prev_unix=self.prev_unix,
            parent_hash=None if self.parent_hash is None else list(self.parent_hash),
            mic=self.mic,
            error=self.error,
        )


@runtime_checkable
class MessageCodec(Protocol):
    """What a source calls to turn a message column into the columns a row carries."""

    def categorise(self, messages: Any, drivers: Any = None) -> Any:
        """One `protocol` name per row."""
        ...

    def into_pairs(self, messages: Any, protocol: str = NO_PROTOCOL) -> Any:
        """Ordered key/value pairs per row, as the line spells them.

        Addressed by the name `categorise` gave the row, because that is what
        the batch carries. Null, not an empty list, for a protocol that reads
        nothing.
        """
        ...

    def into_fix_pairs(self, pairs: Any, version: str | None = None) -> tuple[Any, Any, Any]:
        """Resolved tags, residual pairs, and registry-missed raw keys."""
        ...

    def version_of(
        self, message: str | None, protocol: str = NO_PROTOCOL
    ) -> tuple[str | None, str]:
        """Which protocol version a message is read under, and where that came from.

        The one a protocol without versions answers `(None, "none")` to. Here
        rather than inside `into_fix_pairs` so one resolved version is handed
        down to each homogeneous slice.
        """
        ...

    def into_flat_columns(
        self, tags: Any, version: str | None = None
    ) -> tuple[dict[str, Any], Any]:
        """The fields worth a column of their own, lifted out of `tags`.

        `{column: array}` and the residual pairs. A protocol with nothing
        to lift returns `({}, tags)` and nothing above it changes.
        """
        ...

    def into_component_columns(
        self, tags: Any, version: str | None = None
    ) -> tuple[dict[str, Any], Any]:
        """Structured components and the residual ordered tags."""
        ...


@dataclasses.dataclass
class LogRule(Convertible):
    """One pattern, and the kind of event a line matching it is."""

    pattern: str = ""
    """RE2 regular expression, matched anywhere in the message."""

    etype: EventType = EventType.UNKNOWN
    """What a line matching `pattern` is; readable by name in a configuration."""

    label: str = ""
    """What the rule is for, when the pattern does not say it plainly."""

    patterns: list[str] = dataclasses.field(default_factory=list)
    """Additional regexes; matching any message pattern satisfies the rule."""

    @property
    def message_patterns(self) -> tuple[str, ...]:
        """All nonempty patterns, in declaration order."""
        return tuple(filter(None, (self.pattern, *self.patterns)))


#: What a FIX-carrying trading log is made of, by the two spellings every one
#: of them uses: the wire `35=` message type, and the name a rendered log
#: prints. Ordered most specific first, because the first match wins and a
#: single line can name more than one of them -- an execution report quoting
#: the order it fills says `ExecutionReport` *and* `NewOrderSingle`.
DEFAULT_RULES: tuple[LogRule, ...] = (
    LogRule(
        r"35=8(\D|$)",
        EventType.EXECUTION,
        "a fill, or a report of one",
        [r"ExecutionReport"],
    ),
    LogRule(
        r"35=[DFG](\D|$)",
        EventType.ORDER,
        "an order, or an amendment to one",
        [r"NewOrderSingle", r"OrderCancel(Request|Replace)"],
    ),
    LogRule(
        r"35=X(\D|$)",
        EventType.BOOK,
        "an incremental book update",
        [r"MarketDataIncrementalRefresh"],
    ),
    LogRule(
        r"35=W(\D|$)",
        EventType.BOOK,
        "a full book snapshot",
        [r"MarketDataSnapshot"],
    ),
    LogRule(
        r"35=(?:AG|AH|AI|AJ|[RSZabi])(?:[^A-Za-z0-9]|$)",
        EventType.QUOTE,
        "a quote lifecycle message",
        [
            r"\b(?:MassQuote(?:Acknowledgement)?|Quote(?:RequestReject|StatusRequest|"
            r"StatusReport|Response|Cancel|Request)?|RFQRequest)\b"
        ],
    ),
    LogRule(
        r"35=d(\D|$)",
        EventType.INSTRUMENT,
        "reference data",
        [r"SecurityDefinition"],
    ),
)


def _default_log_rules() -> list[LogRule]:
    """Fresh default rules, including their mutable pattern lists."""
    return [dataclasses.replace(rule, patterns=list(rule.patterns)) for rule in DEFAULT_RULES]


@dataclasses.dataclass
class LogRules(Convertible):
    """Which `EventType` each line of a log is, by the first pattern that matches."""

    #: Rules in the order they are tried. The default reads a FIX trading log.
    rules: list[LogRule] = dataclasses.field(default_factory=_default_log_rules)

    def etype_arrow(self, messages: Any) -> pyarrow.Array:
        """One `etype` per message: the first rule that matches, else `UNKNOWN`."""
        compute = pyarrow.compute
        rows = len(messages)
        found: Any = pyarrow.repeat(pyarrow.scalar(int(EventType.UNKNOWN), _EVENT_CODE), rows)
        if not rows:
            return found
        text = messages.cast(pyarrow.string(), safe=False)
        for rule in reversed(self.rules):
            hit = _log_rule_hit(rule, text)
            found = compute.if_else(hit, pyarrow.scalar(int(rule.etype), _EVENT_CODE), found)
        return found.cast(_EVENT_CODE, safe=False)


def _log_rule_hit(rule: LogRule, text: Any) -> Any:
    """One log rule's any-pattern mask."""
    compute = pyarrow.compute
    mask = None
    for pattern in rule.message_patterns:
        matched = compute.fill_null(compute.match_substring_regex(text, pattern), False)
        mask = matched if mask is None else compute.or_(mask, matched)
    return pyarrow.compute.is_valid(text) if mask is None else mask


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


def _instrument_pairs(instrument: Any) -> list[tuple[str, str]] | None:
    """Registry-shaped fields not already promoted on a normalized Log."""
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


def _pair_count(pairs: Mapping[str, str], name: str) -> int:
    return max(_pair_int(pairs.get(name)), 0)


def _pair_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _pair_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _pair_date(value: Any) -> datetime.date | None:
    try:
        return None if value is None else datetime.datetime.strptime(value, "%Y%m%d").date()
    except (TypeError, ValueError):
        return None


def _pair_entries(
    pairs: Sequence[tuple[Any, Any]] | None,
) -> list[dict[str, Any]] | None:
    """Pairs in the struct spelling Arrow accepts without a shape pass."""
    return None if pairs is None else [{"key": key, "value": value} for key, value in pairs]


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
    """Stored list entries as ordered pairs, before or after an Arrow round trip."""
    for entry in entries or ():
        if isinstance(entry, Mapping):
            yield entry["key"], entry["value"]
        else:
            key, value = entry
            yield key, value
