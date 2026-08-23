"""FIX messages as market events: what a venue said, as rows this package stores."""

from __future__ import annotations

import dataclasses
import datetime
import functools
import json
import types
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, TypeVar

from rekep.convert import Convertible
from rekep.enums import (
    MIC,
    AssetKind,
    Currency,
    IdSource,
    MarketKind,
    OptionKind,
    Side,
    State,
    TimeInForce,
)
from rekep.fields import StructField
from rekep.fix import infer_version_from_pairs
from rekep.fix.fields import EPOCH_ORDINAL, NANOS, SECONDS_A_DAY, unix_of
from rekep.fix.message import FixMessage
from rekep.fix.quickfix import (
    SpecComponent,
    SpecComponentRef,
    SpecFieldRef,
    SpecGroup,
    SpecMember,
)
from rekep.fix.registry import FixRegistry
from rekep.market.event import MarketEvent
from rekep.market.instrument import Instrument, Leg
from rekep.market.orders import Execution, Order, _quantity_transition

TMarketEvent = TypeVar("TMarketEvent", bound=MarketEvent)

#: Protocol controls read by translation but not necessarily stored as their
#: own model column. The bundled registry supplies their tags and datatypes.
CARRIED_FIELDS: tuple[str, ...] = (
    "BodyLength",
    "CheckSum",
    "MsgType",
    "SenderCompID",
    "TargetCompID",
    "SendingTime",
    "OrigSendingTime",
    "OrigTime",
    "PossDupFlag",
    "TransactTime",
    "TradeDate",
    "ExpireTime",
    "ExpireDate",
    "ExposureDuration",
    "ExposureDurationUnit",
    "OrdStatus",
    "OrdType",
    "ExecType",
    "ExecTransType",
    "CxlRejReason",
    "NoMDEntries",
    "MDEntryType",
    "MDEntryPx",
    "MDEntrySize",
    "MDEntryDate",
    "MDEntryTime",
    "MDEntryID",
    "MDUpdateAction",
    "NumberOfOrders",
    "TrdMatchID",
    "ExDestination",
    # The repeating groups an instrument is read out of. A group's own
    # `NumInGroup` count and the members that land somewhere other than a
    # column of their own -- an alternative identifier becomes a key of
    # `alt_ids`, so its two tags are read and stored and are not extras.
    "NoSecurityAltID",
    "SecurityAltID",
    "SecurityAltIDSource",
    "NoLegs",
    # The older way to say when a contract expires, which `maturity` falls back
    # to. A venue that sends it usually sends no `MaturityDate <541>` at all.
    "MaturityMonthYear",
    "LegMaturityMonthYear",
    # Quotes, including the two default sizes used by mass-quote sets.
    "QuoteID",
    "QuoteReqID",
    "QuoteType",
    "QuoteStatus",
    "QuoteRejectReason",
    "QuoteRespType",
    "QuoteCancelType",
    "BidPx",
    "OfferPx",
    "BidSize",
    "OfferSize",
    "DefBidSize",
    "DefOfferSize",
    "ValidUntilTime",
    "NoQuoteSets",
    "NoQuoteEntries",
    "QuoteSetID",
    "QuoteEntryID",
)

#: Encoding fields, omitted from market metadata; BeginString remains evidence.
FRAMING_FIELDS = frozenset({"BodyLength", "CheckSum"})

#: Where `MarketEvent.unix` comes from, **best first**, and why each is where
#: it is. Every one is a real FIX field, and the order is the standard's own
#: definitions rather than a preference:
#:
#: 1. `TransactTime <60>` -- "timestamp when the business transaction
#:    represented by the message occurred". The thing being asked for.
#: 2. `MDEntryDate <272>` + `MDEntryTime <273>` -- a market-data entry's own
#:    instant, split across two fields because that is how FIX carries it.
#:    Read per *entry*, so two entries of one refresh keep their own times.
#: 3. `OrigTime <42>` -- "time of message origination", which for a relayed
#:    or republished message is nearer the transaction than the relay's own
#:    transmission.
#: 4. `OrigSendingTime <122>` -- on a `PossDupFlag <43>` resend, when the
#:    message *first* went out. Still transmission, but the original one.
#: 5. `SendingTime <52>` -- transmission. Last, and only because a row with
#:    no time at all sorts nowhere: it is the recording clock, and it is what
#:    `runix` holds regardless.
TRANSACTED: tuple[Any, ...] = (
    "TransactTime",
    ("MDEntryDate", "MDEntryTime"),
    "OrigTime",
    "OrigSendingTime",
    "SendingTime",
)

#: MsgType <35> values that carry an order, and the state each *asserts* when
#: the message itself does not say. A request is not an acknowledgement: a
#: NewOrderSingle is what a participant asked for, and the venue has not
#: agreed to anything yet, so it is `PENDING_NEW` rather than `NEW`.
ORDERED: dict[str, State] = {
    "D": State.PENDING_NEW,
    "F": State.PENDING_CANCEL,
    "G": State.PENDING_REPLACE,
    "9": State.UNKNOWN,
}

#: MsgType <35> values that carry a book or a trade, entry by entry.
ENTRIED = frozenset({"W", "X"})

#: Quote messages. A quote is emitted as one indicative order per populated
#: side, so the same book fold accepts orders, depth and quotes.
QUOTED = frozenset({"S", "AI", "AJ", "Z"})

#: MDEntryType <269> to the side of the book an entry belongs to. Everything
#: else it enumerates -- an index value, a settlement price, a session high,
#: an imbalance -- is a statistic about the market rather than an order in it,
#: and is not a market event this package stores.
ENTRY_SIDES: dict[str, Side] = {"0": Side.BID, "1": Side.ASK}

#: Every field `FixEvents.instrument` reads, the two repeating groups included.
#: An entry of a refresh that names none of them is not describing another
#: instrument -- it is describing a level of the one the header named -- so it
#: takes the header's instrument whole rather than building a poorer copy.
INSTRUMENT_FIELDS: frozenset[str] = frozenset(
    {
        "Currency",
        "SecurityIDSource",
        "SecurityID",
        "Symbol",
        "ExDestination",
        "SecurityDesc",
        "SecurityType",
        "MaturityMonthYear",
        "PutOrCall",
        "StrikePrice",
        "SecurityExchange",
        "ContractMultiplier",
        "NoSecurityAltID",
        "CFICode",
        "MaturityDate",
        "NoLegs",
        "RoundLot",
        "MinPriceIncrement",
    }
)

INSTRUMENT_MESSAGE_FIELDS: frozenset[str] = INSTRUMENT_FIELDS | frozenset(
    {
        "BeginString",
        "MsgType",
        "NoMDEntries",
        "NoQuoteSets",
        "NoQuoteEntries",
        "QuoteSetID",
        "QuoteEntryID",
        *(
            field
            for source in TRANSACTED
            for field in (source if isinstance(source, tuple) else (source,))
        ),
    }
)

#: The MDEntryType <269> that is a trade rather than a resting interest.
ENTRY_TRADE = "2"

#: FIX characters are field-specific: `1` is partially filled in OrdStatus
#: and a changed level in MDUpdateAction. Standardise them here and keep the
#: raw pair in event metadata.
FIX_STATES: dict[str, dict[str, State]] = {
    "OrdStatus": {
        "0": State.NEW,
        "1": State.PARTIALLY_FILLED,
        "2": State.FILLED,
        "3": State.DONE_FOR_DAY,
        "4": State.CANCELLED,
        "5": State.REPLACED,
        "6": State.PENDING_CANCEL,
        "7": State.STOPPED,
        "8": State.REJECTED,
        "9": State.SUSPENDED,
        "A": State.PENDING_NEW,
        "B": State.CALCULATED,
        "C": State.EXPIRED,
        "D": State.ACCEPTED,
        "E": State.PENDING_REPLACE,
    },
    # Only trade-bearing ExecType values create an Execution row. OrdStatus
    # carries the order lifecycle in the same message.
    "ExecType": {
        "1": State.FILLED,
        "2": State.FILLED,
        "F": State.FILLED,
        "G": State.REPLACED,
        "H": State.CANCELLED,
    },
    "MDUpdateAction": {
        "0": State.NEW,
        "1": State.OPEN,
        "2": State.CANCELLED,
        "3": State.CANCELLED,
        "4": State.CANCELLED,
        "5": State.OPEN,
    },
    "QuoteStatus": {
        "0": State.ACCEPTED,
        "1": State.CANCELLED,
        "2": State.CANCELLED,
        "3": State.CANCELLED,
        "4": State.CANCELLED,
        "5": State.REJECTED,
        "6": State.CANCELLED,
        "7": State.EXPIRED,
        "9": State.REJECTED,
        "10": State.PENDING,
        "11": State.CANCELLED,
        "12": State.OPEN,
        "13": State.OPEN,
        "14": State.CANCELLED,
        "15": State.CANCELLED,
        "16": State.OPEN,
        "17": State.CANCELLED,
        "18": State.OPEN,
        "19": State.PENDING_CANCEL,
        "21": State.FILLED,
        "22": State.FILLED,
        "23": State.EXPIRED,
    },
    "QuoteRespType": {
        "1": State.FILLED,
        "2": State.OPEN,
        "3": State.EXPIRED,
        "4": State.OPEN,
        "5": State.CANCELLED,
        "6": State.CANCELLED,
        "7": State.CANCELLED,
        "8": State.CANCELLED,
        "9": State.OPEN,
        "10": State.OPEN,
        "11": State.ACCEPTED,
        "12": State.CANCELLED,
    },
}

# ExecType describes the report when OrdStatus is absent. Trade corrections
# and cancellations (G/H) describe an Execution lifecycle, not the Order, and
# therefore cannot safely stand in for the missing order status.
EXEC_ORDER_STATES: Mapping[str, State] = types.MappingProxyType(
    {
        "0": State.NEW,
        "1": State.PARTIALLY_FILLED,
        "2": State.FILLED,
        "3": State.DONE_FOR_DAY,
        "4": State.CANCELLED,
        "5": State.REPLACED,
        "6": State.PENDING_CANCEL,
        "7": State.STOPPED,
        "8": State.REJECTED,
        "9": State.SUSPENDED,
        "A": State.PENDING_NEW,
        "B": State.CALCULATED,
        "C": State.EXPIRED,
        "E": State.PENDING_REPLACE,
    }
)

# Standardisation is intentionally many-to-one for order kinds and lifecycle
# states. Keep the wire spelling beside the standard code for an audit.
RAW_METADATA_FIELDS = frozenset(FIX_STATES) | {
    "OrdType",
    "CxlQty",
    "ExpireTime",
    "ExpireDate",
    "ExposureDuration",
    "ExposureDurationUnit",
    "QuoteCancelType",
    "QuoteType",
    "QuoteRejectReason",
}

_EXPOSURE_UNIT_NS = types.MappingProxyType(
    {
        0: NANOS,
        1: 100_000_000,
        2: 10_000_000,
        3: 1_000_000,
        4: 1_000,
        5: 1,
        10: 60 * NANOS,
        11: 3_600 * NANOS,
        12: SECONDS_A_DAY * NANOS,
        13: 7 * SECONDS_A_DAY * NANOS,
    }
)


@functools.cache
def _standard_market_tags() -> Mapping[str, int]:
    """Tags selected by canonical name from the packaged registry."""
    registry = FixRegistry.from_builtin()
    found = {name: int(registry.scalar(name).fix["tag"]) for name in CARRIED_FIELDS}
    for shape in (MarketEvent, Order, Execution, Instrument):
        _declared_tags(shape.into_field(), found)
    return types.MappingProxyType(found)


@functools.cache
def market_tags(
    registry: FixRegistry | None = None, version: str | None = None
) -> Mapping[str, int]:
    """Market field names to tags, overridden by a selected registry version."""
    standard = _standard_market_tags()
    if version is None:
        return standard
    registry = registry or FixRegistry.from_builtin()
    try:
        configured = registry.tags(version)
    except (KeyError, OSError, ValueError):
        return standard
    found = dict(standard)
    for name in tuple(found):
        if tag := configured.get(name.lower()):
            found[name] = tag
    for name, tag in configured.items():
        found.setdefault(name, tag)
    return types.MappingProxyType(found)


@functools.cache
def market_fields() -> frozenset[str]:
    """Canonical fields market translation reads from promoted log columns."""
    return frozenset({"BeginString", *_standard_market_tags()})


def _declared_tags(struct: StructField, into: dict[str, int]) -> None:
    """Every `fix:` tag under `struct`, nested members included."""
    for member in struct.fields:
        tag = member.fix.get("tag")
        name = member.fix.get("name")
        if tag and name:
            into.setdefault(str(name), int(tag))
        if member.fields:
            _declared_tags(member, into)


def _field_key(value: Any) -> str:
    """A FIX name in the spelling used by registry and rendered keys."""
    return "".join(character.lower() for character in str(value) if character.isalnum())


def _version_key(value: Any) -> str:
    """A BeginString or registry version in one comparison spelling."""
    text = str(value).strip().upper()
    if text.startswith("FIX."):
        text = text[4:]
    return "".join(character for character in text if character.isalnum())


@dataclasses.dataclass
class FixEvents(Convertible):
    """The market events one FIX message carries, in the order it carries them."""

    @classmethod
    @functools.cache
    def into_redirects(cls) -> Mapping[Any, str]:
        """Conversions inferred for a FIX event translator."""
        return types.MappingProxyType({**super().into_redirects(), str: "text"})

    message: FixMessage = dataclasses.field(default_factory=FixMessage)
    """The message being read."""

    venue: str | None = None
    """Which feed this came off, when the reader knows and the message does not."""

    mic: MIC | None = None
    """ISO venue supplied by the parsed log, when it already resolved one."""

    runix: int = 0
    """When the line was recorded, which is the reader's clock and not the venue's."""

    registry: FixRegistry | None = None
    """Optional dictionary overriding standard tags for this feed."""

    fix_version: str | None = None
    """Registry version; otherwise inferred from BeginString when possible."""

    # -- building -----------------------------------------------------------

    @classmethod
    def from_text(cls, text: str | bytes, **carried: Any) -> FixEvents:
        """Events out of one log line, however it spells its separator."""
        return cls(message=FixMessage.from_text(text), **carried)

    @classmethod
    def from_pairs(
        cls,
        pairs: Iterable[tuple[Any, Any]],
        names: Mapping[str, int | str] | None = None,
        **carried: Any,
    ) -> FixEvents:
        """Events out of named or numbered pairs; unknown names remain metadata."""
        # With no explicit mapping, keep names until the instance can infer the
        # message version and apply its registry. Resolving newest-first here
        # would silently use the wrong tag for an older/custom version.
        return cls(message=FixMessage.from_pairs(pairs, names), **carried)

    # -- reading ------------------------------------------------------------

    @functools.cached_property
    def version(self) -> str | None:
        """Configured version, or the application version inferred from the message."""
        if self.fix_version is not None:
            return self.fix_version
        try:
            return infer_version_from_pairs(self.message.pairs, self.registry)[0]
        except (OSError, ValueError):
            return None

    @functools.cached_property
    def tags(self) -> Mapping[str, int]:
        """The field-name index selected for this message."""
        if self.version is None:
            return types.MappingProxyType({})
        return market_tags(self.registry, self.version)

    @functools.cached_property
    def _tags_by_name(self) -> Mapping[str, int]:
        return types.MappingProxyType({_field_key(name): tag for name, tag in self.tags.items()})

    @functools.cached_property
    def _names_by_tag(self) -> Mapping[str, str]:
        return types.MappingProxyType(
            {
                self.tag_of(name): name
                for name in _standard_market_tags()
                if self.tag_of(name) != name
            }
        )

    def tag_of(self, field: int | str) -> str:
        """A canonical field name or numeric tag as the selected wire tag."""
        text = str(field)
        if text.isascii() and text.isdigit():
            return text
        tag = self.tags.get(text)
        if tag is None:
            tag = self._tags_by_name.get(_field_key(text))
        return str(tag) if tag is not None else text

    @functools.cached_property
    def by_tag(self) -> dict[str, Any]:
        """The message as one value per key, first occurrence winning."""
        pairs = self.message.pairs
        if any(not (key.isascii() and key.isdigit()) for key, _ in pairs):
            # Only a message that actually spells a key as a name pays for the
            # resolution. A wire message is already all tags, which is most of
            # a feed, and running the pass over it re-resolved eighteen keys
            # to themselves -- 29% of the conversion, for nothing.
            pairs = FixMessage.from_pairs(pairs, self.tags).pairs
        found: dict[str, Any] = {}
        for key, value in pairs:
            found.setdefault(key, value)
        return found

    def get(self, field: int | str) -> Any:
        """The message's first value for one canonical name or numeric tag."""
        wanted = self.tag_of(field)
        found = self.by_tag.get(wanted)
        if found is not None or wanted in self.by_tag:
            return found
        folded = _field_key(str(field))
        return next(
            (value for key, value in self.by_tag.items() if _field_key(key) == folded), None
        )

    def state_of(self, field: str, default: State = State.UNKNOWN) -> State:
        """Standard state for one field-specific FIX code."""
        return FIX_STATES.get(field, {}).get(self.get(field) or "", default)

    @functools.cached_property
    def _message_kind(self) -> str:
        """Declared MsgType, or the narrow field-based fallback."""
        return self.get("MsgType") or self._inferred()

    def __iter__(self) -> Iterator[MarketEvent]:
        """Every market event the message carries, in the order it carries them."""
        if self.version is None:
            return
        kind = self._message_kind
        if kind in ENTRIED:
            yield from self._entries(kind)
        elif kind == "i":
            yield from self._mass_quotes()
        elif kind in QUOTED:
            yield from self._quotes(kind)
        elif kind == "8":
            yield from self._reported()
        elif kind == "AE":
            yield self.into_execution()
        elif kind in ORDERED:
            yield self.into_order(ORDERED[kind])

    def into_instruments(self) -> Iterator[Instrument]:
        """Distinct repeating-entry instruments, or the header fallback."""
        return (instrument for _, instrument in self.into_instrument_observations())

    def into_instrument_observations(self) -> Iterator[tuple[int, Instrument]]:
        """Distinct `(entry time, instrument)` facts without constructing events."""
        if self.version is None:
            return
        seen: dict[int, list[Instrument]] = {}
        for reader in self._instrument_readers():
            instrument = reader.instrument
            if not instrument.identities():
                continue
            versions = seen.setdefault(instrument.xhash, [])
            if instrument in versions:
                continue
            versions.append(instrument)
            yield reader.unix, instrument

    def _instrument_readers(self) -> Iterator[FixEvents]:
        """Entry projections when present, otherwise the message header."""
        if self._message_kind in ENTRIED:
            entries = self._group_entries("NoMDEntries")
            for entry in entries:
                yield self._inside(entry)
            if not entries:
                yield self
        elif self._message_kind == "i":
            yield from self._quote_readers()
        else:
            yield self

    def _inferred(self) -> str:
        """What a message with no MsgType <35> is, from the fields it carries.

        The fields are the evidence, most specific first: a market-data entry
        type means a refresh, an `ExecType <150>` or an `ExecID <17>` means an
        execution report, and an order's own identifiers mean an order. An
        empty string for anything else, which dispatches to nothing.
        """
        get = self.get
        if get("MDEntryType") is not None:
            return "X"
        if any(get(field) is not None for field in ("BidPx", "OfferPx", "QuoteStatus")):
            return "S"
        if get("ExecType") is not None or get("ExecID") is not None or get("ExecRefID") is not None:
            return "8"
        if get("ClOrdID") is not None or get("OrdType") is not None or get("OrdStatus") is not None:
            return "D"
        return ""

    def _reported(self) -> Iterator[MarketEvent]:
        """An ExecutionReport <8>: the order's new state, and the fill if there was one."""
        order = self.into_order(self.state_of("OrdStatus"))
        yield order
        if self.state_of("ExecType") is not State.UNKNOWN:
            # Completed *from the order*, not from a previous report: the
            # running totals a venue leaves out of a fill -- how much is done
            # now, how much is left, what the average is -- are all statements
            # about the order this report is on, and the order row is the one
            # thing here that already holds them.
            yield self.into_execution(order)

    def _entries(self, kind: str) -> Iterator[MarketEvent]:
        """One market-data refresh, entry by entry."""
        for entry in self._group_entries("NoMDEntries"):
            inside = self._inside(entry)
            entry_type = inside.get("MDEntryType")
            if entry_type in ENTRY_SIDES:
                yield inside.into_entry_order(ENTRY_SIDES[entry_type], snapshot=kind == "W")
            elif entry_type == ENTRY_TRADE:
                yield inside.into_entry_execution()

    def _inside(self, entry: list[tuple[str, str]]) -> FixEvents:
        """A repeating-group entry completed by its message header."""
        inside = FixEvents(
            message=FixMessage(pairs=entry),
            venue=self.venue,
            mic=self.mic,
            runix=self.runix,
            registry=self.registry,
            fix_version=self.version,
        )
        inside.__dict__["version"] = self.version
        inside.__dict__["tags"] = self.tags
        own = inside.by_tag
        if all(inside.get(field) is None for field in INSTRUMENT_FIELDS):
            inside.__dict__["instrument"] = self.instrument
        inside.__dict__["by_tag"] = {**self.by_tag, **own}
        return inside

    def _mass_quotes(self) -> Iterator[Order]:
        """A MassQuote <i>, one two-sided quote entry at a time."""
        for quote in self._quote_readers():
            yield from quote._quotes("i")

    def _quote_readers(self) -> Iterator[FixEvents]:
        """MassQuote entries flattened through their enclosing quote set."""
        sets = _group_segments(
            self.message.pairs,
            self.tag_of("NoQuoteSets"),
            self._quote_group_delimiters[0],
        )
        if sets:
            for quote_set in sets:
                prefix, entries = _group_segments(
                    quote_set,
                    self.tag_of("NoQuoteEntries"),
                    self._quote_group_delimiters[1],
                    with_prefix=True,
                )
                for entry in entries:
                    yield self._inside([*prefix, *entry])
            return
        entries = self._group_entries("NoQuoteEntries")
        if not entries:
            yield self
            return
        for entry in entries:
            yield self._inside(entry)

    @functools.cached_property
    def _quote_group_delimiters(self) -> tuple[str, str]:
        """Outer and inner delimiters from the selected FIX declaration."""
        fallback = (self.tag_of("QuoteSetID"), self.tag_of("QuoteEntryID"))
        registry = self.registry
        if registry is None:
            return fallback
        versions = (self.version,) if self.version else registry.versions
        for version in versions:
            try:
                components = registry.components(version)
            except (KeyError, OSError, ValueError):
                continue
            by_name = {component.name.lower(): component for component in components}
            root = by_name.get("quotsetgrp")
            if root is None:
                continue
            outer = _declared_group(root.members, "NoQuoteSets", by_name)
            inner = (
                _declared_group(outer.members, "NoQuoteEntries", by_name)
                if outer is not None
                else None
            )
            outer_name = _first_declared_name(outer.members, by_name) if outer else None
            inner_name = _first_declared_name(inner.members, by_name) if inner else None
            if outer_name and inner_name:
                return self.tag_of(outer_name), self.tag_of(inner_name)
        return fallback

    def _quotes(self, kind: str) -> Iterator[Order]:
        """One FIX quote as an indicative order for each represented side."""
        get = self.get
        state = self._quote_state(kind)
        sides = (
            (Side.BID, "BidPx", "BidSize", "DefBidSize"),
            (Side.ASK, "OfferPx", "OfferSize", "DefOfferSize"),
        )
        populated = [
            side
            for side in sides
            if get(side[1]) is not None or get(side[2]) is not None or get(side[3]) is not None
        ]
        # A status/response commonly carries only QuoteID. It updates the side
        # it names, or both lifecycles created by the original two-sided quote.
        if not populated and kind in {"AI", "AJ", "Z"} and (get("QuoteEntryID") or get("QuoteID")):
            named_side = Side.from_fix(get("Side"), Side.UNKNOWN)
            populated = [side for side in sides if side[0] is named_side] or list(sides)
        for side, px, qty, default_qty in populated:
            yield self.into_quote_order(
                side,
                state,
                px=_number(get(px)),
                qty=_number(get(qty) or get(default_qty)),
            )

    def _quote_state(self, kind: str) -> State:
        """The standard lifecycle meaning asserted by one quote message."""
        if kind == "Z":
            return State.CANCELLED
        if kind == "AI":
            return self.state_of("QuoteStatus")
        if kind == "AJ":
            return self.state_of("QuoteRespType")
        return self.state_of("QuoteStatus", State.OPEN)

    # -- converting ---------------------------------------------------------

    def into_order(self, state: State = State.UNKNOWN) -> Order:
        """The order this message is about, in the state the message puts it in."""
        get = self.get
        unix = self.unix
        tif = TimeInForce.from_fix(get("TimeInForce"), TimeInForce.DAY)
        duration = _integer(get("ExposureDuration"))
        exec_type = get("ExecType")
        if state is State.UNKNOWN:
            state = EXEC_ORDER_STATES.get(exec_type, State.UNKNOWN)
        order_qty = _number(get("OrderQty"))
        cumulative = _number(get("CumQty"))
        leaves = _number(get("LeavesQty"))
        if state is State.REPLACED:
            state = (
                State.FILLED
                if leaves == 0 and order_qty is not None and cumulative == order_qty
                else State.PARTIALLY_FILLED
                if cumulative is not None and cumulative > 0
                else State.NEW
            )
        transition = _quantity_transition(
            state,
            execution_state=self.state_of("ExecType"),
            order_qty=order_qty,
            cum_qty=cumulative,
            leaves_qty=leaves,
            last_qty=_number(get("LastQty")),
            cancel_qty=_number(get("CxlQty")),
        )
        return self._finish(
            Order(
                unix=unix,
                cunix=unix,
                runix=self.runix or unix,
                eunix=self._expires(tif, unix, duration),
                state=transition.state,
                side=Side.from_fix(get("Side"), Side.UNKNOWN),
                px=_number(get("Price")),
                qty=transition.current_qty,
                prev_qty=transition.previous_qty,
                kind=MarketKind.from_fix(get("OrdType"), MarketKind.UNKNOWN, tag=40),
                tif=tif,
                stop_px=_number(get("StopPx")),
                hidden_qty=_hidden_qty(transition.current_qty, _number(get("MaxFloor"))),
                vwap=_number(get("AvgPx")),
                order_id=get("OrderID"),
                client_order_id=get("ClOrdID"),
                prev_client_order_id=get("OrigClOrdID"),
                reason_code=_integer(get("OrdRejReason") or get("CxlRejReason")),
                reason=get("Text"),
                **self._shared(),
            )
        )

    def into_quote_order(
        self,
        side: Side,
        state: State,
        *,
        px: float | None,
        qty: float | None,
    ) -> Order:
        """One side of a FIX quote as an indicative limit order."""
        get = self.get
        unix = self.unix
        return self._finish(
            Order(
                unix=unix,
                cunix=unix,
                runix=self.runix or unix,
                eunix=_unix_value(get("ValidUntilTime")),
                state=state,
                side=side,
                px=px,
                qty=qty,
                kind=MarketKind.LIMIT_ORDER,
                indicative=True,
                order_id=get("QuoteEntryID") or get("QuoteID"),
                client_order_id=get("QuoteReqID"),
                reason_code=_integer(get("QuoteRejectReason")),
                reason=get("Text"),
                **self._shared(),
            )
        )

    def into_execution(self, order: Order | None = None) -> Execution:
        """What traded, as the report says it. `px` is `LastPx <31>`, not `Price <44>`."""
        get = self.get
        unix = self.unix
        return self._finish(
            Execution(
                unix=unix,
                cunix=unix,
                runix=self.runix or unix,
                state=self.state_of("ExecType"),
                kind=MarketKind.from_fix(get("ExecType"), MarketKind.UNKNOWN, tag=150),
                side=Side.from_fix(get("Side"), Side.UNKNOWN),
                px=_number(get("LastPx")),
                qty=_number(get("LastQty")),
                exec_id=get("ExecID"),
                exec_ref_id=get("ExecRefID"),
                trade_id=get("TradeID") or get("TrdMatchID"),
                linked_events=(
                    [(order.unix, order.xhash)] if order is not None and order.xhash else []
                ),
                parent_hash=[order.hash] if order is not None and order.hash else [],
                order_id=get("OrderID"),
                client_order_id=get("ClOrdID"),
                prev_client_order_id=get("OrigClOrdID"),
                filled_qty=_number(get("CumQty")),
                leaves_qty=_number(get("LeavesQty")),
                vwap=_number(get("AvgPx")),
                aggressor=_flag(get("AggressorIndicator")),
                reason_code=_integer(get("ExecRestatementReason")),
                reason=get("Text"),
                **self._shared(),
            ),
            order,
        )

    def into_entry_order(self, side: Side, snapshot: bool = False) -> Order:
        """One market-data entry as the resting interest it describes.

        A price level with a size *is* an order, aggregated or not, and reading
        it as one is what lets a book be folded from a feed and from an order
        stream by the same code. `MDEntryID <278>` is the venue's own handle on
        that interest, so it is the lifecycle identity when there is one.
        """
        get = self.get
        unix = self.unix
        return self._finish(
            Order(
                unix=unix,
                cunix=unix,
                runix=self.runix or unix,
                state=self.state_of("MDUpdateAction", State.NEW if snapshot else State.OPEN),
                side=side,
                px=_number(get("MDEntryPx")),
                qty=_number(get("MDEntrySize")),
                kind=MarketKind.LIMIT_ORDER,
                indicative=True,
                # An entry with no id of its own is a *level*, not an order, so
                # the price is what persists across its updates: that is what
                # `MDUpdateAction <279>` addresses when it says Change or Delete,
                # and it is what makes a level's own lifecycle findable.
                order_id=get("MDEntryID")
                or (f"{side.name}@{get('MDEntryPx')}" if get("MDEntryPx") else None),
                **self._shared(),
            )
        )

    def into_entry_execution(self) -> Execution:
        """One market-data entry of type Trade <2> as the execution it reports."""
        get = self.get
        unix = self.unix
        return self._finish(
            Execution(
                unix=unix,
                cunix=unix,
                runix=self.runix or unix,
                state=State.FILLED,
                kind=MarketKind.TRADE,
                side=Side.from_fix(get("Side"), Side.UNKNOWN),
                px=_number(get("MDEntryPx")),
                qty=_number(get("MDEntrySize")),
                exec_id=get("MDEntryID"),
                trade_id=get("TradeID") or get("TrdMatchID"),
                **self._shared(),
            )
        )

    def _finish(self, event: TMarketEvent, previous: MarketEvent | None = None) -> TMarketEvent:
        """Attach transient reference data before deriving and identifying."""
        event.attach_instrument(self.instrument)
        finished = event.with_previous(previous)
        if finished is None:
            raise AssertionError("a newly translated event cannot be unchanged")
        return finished

    @functools.cached_property
    def instrument(self) -> Instrument:
        """What the message says is being traded, groups and all.

        Cached like `by_tag`, and for the same reason: a message is *one*
        instrument, and an `ExecutionReport` yields two events off it while a
        refresh yields one per entry. Reading eighteen tags and two repeating
        groups once per event was that work N times -- a fifth of the cost of
        reading a five-entry refresh.
        """
        get = self.get
        cfi = get("CFICode")
        security_type = get("SecurityType")
        return Instrument(
            symbol=get("Symbol") or "",
            kind=_classified(cfi, security_type),
            security_id=get("SecurityID"),
            security_id_source=get("SecurityIDSource"),
            alt_ids=self.into_alt_ids() or None,
            security_type=security_type,
            cfi=cfi,
            exchange=get("SecurityExchange") or get("ExDestination"),
            currency=_currency(get("Currency")),
            multiplier=_number(get("ContractMultiplier")),
            tick=_number(get("MinPriceIncrement")),
            lot=_number(get("RoundLot")),
            maturity=_date(get("MaturityDate")) or _month_year(get("MaturityMonthYear")),
            strike=_number(get("StrikePrice")),
            option_kind=OptionKind.from_fix(get("PutOrCall"), OptionKind.UNKNOWN),
            label=get("SecurityDesc"),
            legs=self.into_legs() or None,
        )

    def into_alt_ids(self) -> dict[str, str]:
        """Every alternative identifier the message carried, by the scheme's name."""
        found: dict[str, str] = {}
        for entry in self._group("NoSecurityAltID"):
            named = entry.get("SecurityAltID")
            raw_scheme = entry.get("SecurityAltIDSource")
            scheme = IdSource.from_fix(raw_scheme, IdSource.UNKNOWN)
            if not named:
                continue
            key = scheme.name if scheme is not IdSource.UNKNOWN else (raw_scheme or "")
            found.setdefault(key or IdSource.UNKNOWN.name, named)
        return found

    def into_legs(self) -> list[Leg]:
        """The legs of a multileg instrument, from the `NoLegs <555>` group.

        Every member is the instrument field with a `Leg` in front of it --
        `LegSymbol <600>` is `Symbol <55>` for the leg -- so the reading is the
        same one, against a different set of tags.
        """
        built = []
        for entry in self._group("NoLegs"):
            cfi, security_type = entry.get("LegCFICode"), entry.get("LegSecurityType")
            built.append(
                Leg(
                    symbol=entry.get("LegSymbol") or "",
                    side=Side.from_fix(entry.get("LegSide"), Side.UNKNOWN),
                    ratio=_number(entry.get("LegRatioQty")),
                    kind=_classified(cfi, security_type),
                    security_id=entry.get("LegSecurityID"),
                    security_id_source=entry.get("LegSecurityIDSource"),
                    cfi=cfi,
                    security_type=security_type,
                    exchange=entry.get("LegSecurityExchange"),
                    currency=_currency(entry.get("LegCurrency")),
                    multiplier=_number(entry.get("LegContractMultiplier")),
                    maturity=_date(entry.get("LegMaturityDate"))
                    or _month_year(entry.get("LegMaturityMonthYear")),
                    strike=_number(entry.get("LegStrikePrice")),
                    option_kind=OptionKind.from_fix(entry.get("LegPutOrCall"), OptionKind.UNKNOWN),
                )
            )
        return built

    def _group_entries(self, name: str) -> list[list[tuple[str, str]]]:
        """One repeating group under its configured count tag or rendered name."""
        return self.message.group(self.tag_of(name)) or self.message.indexed_group(name)

    def _group(self, name: str) -> list[dict[str, str]]:
        """One repeating group's entries, each as first-value-by-tag."""
        found = []
        for entry in self._group_entries(name):
            resolved: dict[str, str] = {}
            for key, value in FixMessage.from_pairs(entry, self.tags).pairs:
                resolved.setdefault(self._names_by_tag.get(key, key), value)
            found.append(resolved)
        return found

    # -- what every event carries ------------------------------------------

    @property
    def unix(self) -> int:
        """When the transaction happened, by `TRANSACTED` -- not when it was sent.

        Falls back to the log's `runix` when the message carries no FIX clock.
        """
        get = self.get
        recorded = _unix_value(get("SendingTime"))
        if recorded is None:
            recorded = self.runix or None
        for source in TRANSACTED:
            if isinstance(source, tuple):
                date, time = source
                found = _unix_value(get(date))
                clock = _unix_value(get(time), day=found if found is not None else recorded)
                found = clock if clock is not None else found
            else:
                found = _unix_value(get(source))
            if found is not None:
                return found
        return recorded or 0

    def _expires(self, tif: TimeInForce, unix: int, duration: int | None) -> int | None:
        """Exact expiry, from UTC time first and a fixed GFT duration second."""
        explicit = unix_of(self.get("ExpireTime"))
        if explicit is not None:
            return explicit
        if tif is not TimeInForce.GFT or duration is None or duration <= 0:
            return None
        raw_unit = self.get("ExposureDurationUnit")
        unit = 0 if raw_unit is None else _integer(raw_unit)
        factor = _EXPOSURE_UNIT_NS.get(unit)
        if factor is None:
            return None
        expires = unix + duration * factor
        return expires if -(1 << 63) <= expires < 1 << 63 else None

    @functools.cached_property
    def _shared_values(self) -> dict[str, Any]:
        """Shared envelope values that are immutable during translation."""
        instrument = self.instrument
        mic = self._mic()
        return {
            "code": instrument.symbol,
            "seq": _integer(self.get("MsgSeqNum")),
            "mic": mic,
            "error": self._error(),
            "instrument_xhash": instrument.xhash,
            "px_unit": instrument.currency.into_str() if instrument.currency else "",
            "ccy": instrument.currency,
        }

    def _error(self) -> str | None:
        """FIX text or the first structured reject/restatement reason."""
        if text := self.get("Text"):
            return str(text)
        for name in (
            "OrdRejReason",
            "CxlRejReason",
            "QuoteRejectReason",
            "ExecRestatementReason",
        ):
            value = self.get(name)
            if value is None:
                continue
            registry = self.registry or FixRegistry.from_builtin()
            try:
                member = (
                    registry.scalar(name, version=self.version)
                    if self.version
                    else registry.scalar(name)
                )
            except (KeyError, OSError, ValueError):
                member = FixRegistry.from_builtin().scalar(name)
            try:
                label = json.loads(member.fix.get("values", "{}"))[str(value)]
            except (KeyError, TypeError, ValueError):
                label = None
            return f"{name}={value}: {label}" if label else f"{name}={value}"
        return None

    def _shared(self) -> dict[str, Any]:
        """The shared envelope with event-owned metadata."""
        return {**self._shared_values, "metadata": dict(self.extras)}

    def _mic(self) -> MIC | None:
        """First valid ISO code: exchange fields, configured feed, then session peers."""
        for value in (
            self.get("LastMkt"),
            self.get("SecurityExchange"),
            self.get("ExDestination"),
            self.venue,
            self.mic,
            self.get("SenderCompID"),
            self.get("TargetCompID"),
        ):
            found = MIC.from_str(value)
            if found is not MIC.UNKNOWN:
                return found
        return None

    @functools.cached_property
    def extras(self) -> dict[str, str]:
        """Every field the shapes have no column for, under the key it arrived as."""
        claimed = frozenset(self.tag_of(name) for name in _standard_market_tags()) | frozenset(
            self.tag_of(field) for field in FRAMING_FIELDS
        )
        audited = frozenset(self.tag_of(field) for field in RAW_METADATA_FIELDS)
        return {
            key: (
                value
                if isinstance(value, str)
                else FixMessage.from_pairs([(key, value)]).pairs[0][1]
            )
            for key, value in self.by_tag.items()
            if key not in claimed or key in audited
        }


def _group_segments(
    pairs: Sequence[tuple[str, str]],
    count_tag: str,
    delimiter: str,
    *,
    with_prefix: bool = False,
) -> Any:
    """Split one declared group without collapsing repeated ordered fields."""
    count_at = next((index for index, pair in enumerate(pairs) if pair[0] == count_tag), None)
    if count_at is None:
        return ([], []) if with_prefix else []
    try:
        count = int(pairs[count_at][1])
    except (TypeError, ValueError):
        count = 0
    starts = [index for index in range(count_at + 1, len(pairs)) if pairs[index][0] == delimiter]
    selected = starts[: max(count, 0)]
    entries = [
        list(pairs[start : starts[index + 1] if index + 1 < len(starts) else len(pairs)])
        for index, start in enumerate(selected)
    ]
    if not with_prefix:
        return entries
    prefix_end = selected[0] if selected else len(pairs)
    return list(pairs[:prefix_end]), entries


def _declared_group(
    members: Sequence[SpecMember],
    wanted: str,
    components: Mapping[str, SpecComponent],
    seen: frozenset[str] = frozenset(),
) -> SpecGroup | None:
    """Find a nested group through component references without cycles."""
    for member in members:
        if isinstance(member, SpecGroup):
            if member.name.lower() == wanted.lower():
                return member
            if found := _declared_group(member.members, wanted, components, seen):
                return found
        elif isinstance(member, SpecComponentRef):
            key = member.name.lower()
            component = components.get(key)
            if component is not None and key not in seen:
                if found := _declared_group(component.members, wanted, components, seen | {key}):
                    return found
    return None


def _first_declared_name(
    members: Sequence[SpecMember],
    components: Mapping[str, SpecComponent],
    seen: frozenset[str] = frozenset(),
) -> str | None:
    """The first physical field after recursive component expansion."""
    for member in members:
        if isinstance(member, SpecFieldRef | SpecGroup):
            return member.name
        if isinstance(member, SpecComponentRef):
            key = member.name.lower()
            component = components.get(key)
            if component is not None and key not in seen:
                if found := _first_declared_name(component.members, components, seen | {key}):
                    return found
    return None


#: `SecurityType <167>` to what an instrument settles as, for the venues that
#: send no `CFICode <461>`. FIX enumerates a hundred and eighteen of these and
#: most of them are one kind of bond, so this maps the **bands** rather than
#: the list: a value not here classifies as nothing, which is what `UNKNOWN`
#: is for and better than a guess. Read off the dictionary in `data/fix.zip`,
#: and checked against it by `tests/market/test_fix.py`.
SECURITY_TYPES: dict[str, AssetKind] = {
    # Equity
    "CS": AssetKind.EQUITY,
    "PS": AssetKind.EQUITY,
    # Collective investment
    "MF": AssetKind.FUND,
    # Derivatives
    "FUT": AssetKind.FUTURE,
    "OPT": AssetKind.OPTION,
    "OOF": AssetKind.OPTION,
    "OOP": AssetKind.OPTION,
    "OOC": AssetKind.OPTION,
    "WAR": AssetKind.WARRANT,
    "MLEG": AssetKind.MULTILEG,
    # Swaps, which FIX spells one per underlying
    "CDS": AssetKind.SWAP,
    "IRS": AssetKind.SWAP,
    "FXSWAP": AssetKind.SWAP,
    # Currency
    "FXSPOT": AssetKind.CURRENCY,
    "FXFWD": AssetKind.FORWARD,
    "FXNDF": AssetKind.FORWARD,
    "FORWARD": AssetKind.FORWARD,
    "CASH": AssetKind.CURRENCY,
    # Financing
    "REPO": AssetKind.REPO,
    "BUYSELL": AssetKind.REPO,
    "SECLOAN": AssetKind.LOAN,
    "SECPLEDGE": AssetKind.LOAN,
    "TERM": AssetKind.LOAN,
    "RVLV": AssetKind.LOAN,
    "RVLVTRM": AssetKind.LOAN,
    "BRIDGE": AssetKind.LOAN,
    "SWING": AssetKind.LOAN,
    # Debt: the long tail, by what a reader would call it
    "CORP": AssetKind.DEBT,
    "CB": AssetKind.DEBT,
    "TBOND": AssetKind.DEBT,
    "TNOTE": AssetKind.DEBT,
    "TBILL": AssetKind.DEBT,
    "TIPS": AssetKind.DEBT,
    "MUNI": AssetKind.DEBT,
    "GO": AssetKind.DEBT,
    "REV": AssetKind.DEBT,
    "MTN": AssetKind.DEBT,
    "CP": AssetKind.DEBT,
    "CD": AssetKind.DEBT,
    "ABS": AssetKind.DEBT,
    "MBS": AssetKind.DEBT,
    "CMO": AssetKind.DEBT,
    "FRN": AssetKind.DEBT,
    "EUCORP": AssetKind.DEBT,
    "EUSOV": AssetKind.DEBT,
    "BRADY": AssetKind.DEBT,
}


def _classified(cfi: str | None, security_type: str | None) -> AssetKind:
    """What an instrument settles as, from its CFI code or from FIX's own word.

    The CFI first, because ISO 10962's category letter *is* what `AssetKind` is
    coded on and it classifies exactly. `SecurityType <167>` after it, because
    a venue that sends no CFI very often sends `CS`, `FUT` or `OPT` instead --
    and a reading that stopped at the CFI left every one of those `UNKNOWN`.
    """
    if cfi:
        found = AssetKind.from_fix(cfi[:1], AssetKind.UNKNOWN)
        if found is not AssetKind.UNKNOWN:
            return found
    if security_type:
        return SECURITY_TYPES.get(security_type.strip().upper(), AssetKind.UNKNOWN)
    return AssetKind.UNKNOWN


def _month_year(text: str | None) -> datetime.date | None:
    """`MaturityMonthYear <200>` as a date -- the first of the month it names."""
    if not text:
        return None
    trimmed = text.strip()
    if len(trimmed) < 6 or not trimmed[:6].isdigit():
        return None
    day = trimmed[6:8]
    try:
        return datetime.date(int(trimmed[:4]), int(trimmed[4:6]), int(day) if day.isdigit() else 1)
    except ValueError:
        return None


def _unix_value(value: Any, day: int | None = None) -> int | None:
    """FIX text or a typed parsed-log clock as epoch nanoseconds."""
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.UTC).replace(tzinfo=None)
        date = (value.toordinal() - EPOCH_ORDINAL) * SECONDS_A_DAY * NANOS
        clock = (value.hour * 3600 + value.minute * 60 + value.second) * NANOS
        return date + clock + value.microsecond * 1_000
    if isinstance(value, datetime.date):
        return (value.toordinal() - EPOCH_ORDINAL) * SECONDS_A_DAY * NANOS
    if isinstance(value, datetime.time):
        base = 0 if day is None else day - day % (SECONDS_A_DAY * NANOS)
        return (
            base
            + (value.hour * 3600 + value.minute * 60 + value.second) * NANOS
            + value.microsecond * 1_000
        )
    return unix_of(value, day=day)


def _currency(text: str | None) -> Currency | None:
    """A present FIX currency as its packed code; absence stays null."""
    return None if text is None else Currency.from_fix(text)


def _number(text: str | None) -> float | None:
    """A FIX `Price`, `Qty` or `float` as a float; None for anything that is not.

    None rather than zero, and that is the whole point: a price the venue did
    not send and a price of zero are different facts, and a market order has
    no limit at all.
    """
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _hidden_qty(qty: float | None, displayed: float | None) -> float | None:
    """Reserve quantity implied by FIX MaxFloor; absent when it cannot be known."""
    if qty is None or displayed is None:
        return None
    return max(qty - displayed, 0.0)


def _integer(text: Any) -> int | None:
    """A FIX `int`, `SeqNum` or reject code as an int; None for anything else."""
    if text is None or text == "":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _flag(text: str | None) -> bool | None:
    """A FIX `Boolean`: `Y` or `N`, and None for a venue that sent neither."""
    if not text:
        return None
    first = text.strip()[:1].upper()
    return True if first == "Y" else False if first == "N" else None


def _date(text: str | None) -> datetime.date | None:
    """A FIX `LocalMktDate` or `UTCDateOnly` as a date."""
    stamped = unix_of(text)
    if stamped is None:
        return None
    return datetime.date.fromordinal(stamped // (SECONDS_A_DAY * NANOS) + EPOCH_ORDINAL)
