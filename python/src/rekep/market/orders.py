"""What a participant asked for, and what actually traded."""

from __future__ import annotations

import dataclasses
import datetime
import functools
from collections.abc import Iterator
from types import MappingProxyType
from typing import Annotated, Any

import pyarrow

from rekep.enums import Currency, EventType, State, TimeInForce
from rekep.fields import Field, column_name, scalar
from rekep.market.event import Event, MarketEvent, _declared_value_parts, _local_timestamp
from rekep.market.fields import fix_tag
from rekep.market.identity import NIL

# Exact source fields stay on Order/Execution. These two namespaces are only
# the lookup meaning of those fields: equal text in OrderID and ClOrdID is not
# evidence that two orders are the same lifecycle.
VENUE_ORDER_CODE = "order"
CLIENT_ORDER_CODE = "client_order"

_ORDER_CODE_FIELDS = MappingProxyType(
    {
        "orderid": (VENUE_ORDER_CODE, "OrderID"),
        "secondaryorderid": (VENUE_ORDER_CODE, "SecondaryOrderID"),
        "quoteentryid": (VENUE_ORDER_CODE, "QuoteEntryID"),
        "quoteid": (VENUE_ORDER_CODE, "QuoteID"),
        "mdentryid": (VENUE_ORDER_CODE, "MDEntryID"),
        "mdentryrefid": (VENUE_ORDER_CODE, "MDEntryRefID"),
        "origclordid": (CLIENT_ORDER_CODE, "OrigClOrdID"),
        "clordid": (CLIENT_ORDER_CODE, "ClOrdID"),
        "secondaryclordid": (CLIENT_ORDER_CODE, "SecondaryClOrdID"),
        "quotereqid": (CLIENT_ORDER_CODE, "QuoteReqID"),
    }
)


@functools.lru_cache(maxsize=128)
def _code_name(name: str) -> str:
    """One source identifier name in the spelling the lookup contract reads."""
    return column_name(name)


@dataclasses.dataclass(frozen=True, slots=True)
class _QuantityTransition:
    """Previous and current live quantity asserted by one order transition."""

    previous_qty: float | None
    current_qty: float | None
    state: State


def _quantity_transition(
    state: State,
    *,
    execution_state: State = State.UNKNOWN,
    previous_qty: float | None = None,
    order_qty: float | None = None,
    cum_qty: float | None = None,
    leavesqty: float | None = None,
    last_qty: float | None = None,
    cancel_qty: float | None = None,
) -> _QuantityTransition:
    """Normalize source quantities into the order's before and after state."""
    previous = _quantity(previous_qty)
    total = _quantity(order_qty)
    cumulative = _quantity(cum_qty)
    leaves = _quantity(leavesqty)
    last = _quantity(last_qty)
    cancelled = _quantity(cancel_qty)

    normalized = state
    if normalized is State.UNKNOWN and execution_state is State.FILLED:
        completely_filled = leaves == 0 or (
            total is not None and cumulative is not None and cumulative >= total
        )
        normalized = State.FILLED if completely_filled else State.PARTIALLY_FILLED

    if normalized.is_terminal:
        if previous is None:
            if leaves is not None and last is not None:
                previous = leaves + last
            elif total is not None and cumulative is not None:
                previous = total if normalized is State.FILLED else max(total - cumulative, 0.0)
            elif cancelled is not None:
                previous = cancelled
            elif total is not None:
                previous = total
            elif last is not None:
                previous = last
            elif cumulative is not None:
                previous = cumulative
        return _QuantityTransition(previous, 0.0, normalized)

    if leaves is not None:
        current = leaves
    elif total is not None and cumulative is not None:
        current = max(total - cumulative, 0.0)
    elif previous is not None and last is not None and normalized.rank >= State.PARTIAL.rank:
        current = max(previous - last, 0.0)
    elif total is not None and last is not None and normalized.rank >= State.PARTIAL.rank:
        current = max(total - last, 0.0)
    elif previous is not None:
        current = previous
    else:
        current = total

    if normalized is State.PARTIALLY_FILLED and execution_state is State.FILLED and current == 0:
        normalized = State.FILLED

    if previous is None and normalized.rank >= State.PARTIAL.rank:
        if leaves is not None and last is not None:
            previous = leaves + last
        elif current is not None and last is not None:
            previous = current + last
        elif leaves is not None and cumulative is not None:
            previous = leaves + cumulative
        elif total is not None and current != total:
            previous = total
    return _QuantityTransition(previous, current, normalized)


def _quantity(value: float | None) -> float | None:
    """One finite source quantity, with negative remaining values clamped."""
    if value is None:
        return None
    return max(float(value), 0.0)


@scalar(slots=True)
class Order(MarketEvent):
    """One version of one order: what was asked for, and how far it has got."""

    @classmethod
    @functools.cache
    def into_event_type(cls) -> EventType:
        """Event kind fixed by this shape."""
        return EventType.ORDER

    # The uniform event slot is `LastPx`, but for an order it carries the
    # limit from `Price <44>`. The subclass metadata records that provenance.
    lastpx: Annotated[float | None, fix_tag("Price")] = None
    """Current order limit, or null for a market order."""

    lastqty: Annotated[float | None, Field.column("LastQty")] = None
    """Current remaining quantity after this transition; null when indeterminable."""

    prevqty: Annotated[float | None, Field.column("PrevQty")] = None
    """Quantity before this transition, reconstructed when no prior Order was observed."""

    timeinforce: Annotated[TimeInForce, fix_tag("TimeInForce")] = TimeInForce.UNKNOWN
    """How long it lives. `GTD` expires at `expunix`, where every expiry here lives."""

    stoppx: Annotated[float | None, fix_tag("StopPx")] = None
    """Trigger price of a stop order; `lastpx` is the limit once triggered."""

    hiddenqty: Annotated[float | None, Field.column("HiddenQty")] = None
    """Current quantity hidden from the displayed book; null when unstated."""

    vwap: float | None = None
    """Average price of what has been done, weighted by quantity."""

    indicative: bool = False
    """Whether this interest is a quote rather than a firm order."""

    orderid: Annotated[str | None, fix_tag("OrderID")] = None
    """Identifier the venue gave the order."""

    clordid: Annotated[str | None, fix_tag("ClOrdID")] = None
    """Identifier the sender gave this version of the order."""

    origclordid: Annotated[str | None, fix_tag("OrigClOrdID")] = None
    """Identifier the sender gave the version this one replaced."""

    clordlinkid: Annotated[str | None, fix_tag("ClOrdLinkID")] = None
    """Identifier linking the order versions of one intent across replace chains."""

    # Bridge-rendered identities FIX never numbered: the same registry
    # annotation, resolved through a namespace record rather than a tag.
    parentclordid: Annotated[str | None, fix_tag("ParentClOrdID")] = None
    """Client order identity of the parent in a replace chain, where a bridge says it."""

    parentorderid: Annotated[str | None, fix_tag("ParentOrderID")] = None
    """Venue order identity of the parent in a replace chain, where a bridge says it."""

    cxlrejreason: Annotated[int | None, fix_tag("CxlRejReason", dtype=pyarrow.int32())] = None
    """Why a cancel or amend was refused, in FIX's own codes; null off a reject."""

    cxlrejresponseto: Annotated[str | None, fix_tag("CxlRejResponseTo")] = None
    """Which request a reject answers: a cancel, or a cancel/replace."""

    def complete_from(self, previous: Event) -> None:
        """An order completed from its last version, by what a market actually means."""
        same_named_life = self._continues_named_life(previous)
        linked_life = (
            previous._linked_order_life(self) if isinstance(previous, Execution) else ("", "")
        )
        MarketEvent.complete_from(self, previous)
        # By name, for the reason `_carry` gives. `vwap` means the same thing
        # wherever it appears, unlike the abstract price and quantity slots.
        _carry(
            self,
            previous,
            "stoppx",
            "vwap",
            "orderid",
        )
        if self.lastqty is None and isinstance(previous, Execution):
            self.lastqty = previous.leavesqty
        if self.hiddenqty is None and isinstance(previous, Order):
            displayed = (
                None
                if previous.lastqty is None or previous.hiddenqty is None
                else max(previous.lastqty - previous.hiddenqty, 0.0)
            )
            if displayed is not None and self.lastqty is not None:
                self.hiddenqty = max(self.lastqty - displayed, 0.0)
        _carry_code(self, previous, "timeinforce")
        named = getattr(previous, "clordid", None)
        if self.clordid is None:
            self.clordid = named
        elif self.origclordid is None and named not in (None, self.clordid):
            self.origclordid = named
        anchor, source = (
            (previous.code or previous.life_code(), previous.life_code_source())
            if same_named_life
            else linked_life
        )
        if anchor:
            # A later acknowledgement may introduce the venue's OrderID. The
            # exact field keeps it, while the lifecycle stays on its first
            # readable anchor. Clear the incoming identity so the envelope
            # hashes that anchor again.
            self.code = anchor
            self.codesource = source
            self.xhash = NIL

    def derive(self) -> None:
        """What an order's own numbers say about each other."""
        if self.expires_on_arrival and self.expunix is None:
            self.expunix = self.unix
        if self.state.is_terminal:
            if self.prevqty is None and self.lastqty is not None:
                self.prevqty = self.lastqty
            self.lastqty = 0.0
            self.hiddenqty = 0.0
        MarketEvent.derive(self)

    def _remember_previous(self, previous: Event) -> None:
        """Prefer an observed prior Order quantity over source reconstruction."""
        MarketEvent._remember_previous(self, previous)
        if isinstance(previous, Order):
            self.prevqty = previous.lastqty

    @property
    def expires_on_arrival(self) -> bool:
        """Whether FIX says unfilled quantity can never rest."""
        return TimeInForce.IMMEDIATE <= self.timeinforce < TimeInForce.SESSION

    def life_code(self) -> str:
        """The order identifier that survives amendments, or nothing."""
        return self.code or self._named_life_code()

    def life_code_source(self) -> str:
        """The exact order field that supplied the readable identifier."""
        if self.code:
            return self.codesource or "Code"
        source, _ = self._named_life_key()
        return source

    def _named_life_code(self) -> str:
        """The strongest typed order identifier this version carries."""
        return self._named_life_key()[1]

    def _named_life_key(self) -> tuple[str, str]:
        """Reader-facing source name and strongest order identifier."""
        return next(((source, value) for _, source, value in self._code_fields_of(self)), ("", ""))

    @classmethod
    def lookup_altids_of(cls, event: MarketEvent) -> Iterator[tuple[str, str]]:
        """Typed order identifiers on `event`, strongest first and once each.

        Venue identifiers lead client identifiers; exact columns are inserted
        at their strength within that order so hand-built rows remain indexed.
        Parsed `altids` retains identifiers not promoted to dedicated columns.
        """
        yield from ((namespace, value) for namespace, _, value in cls._code_fields_of(event))

    @classmethod
    def _code_fields_of(cls, event: MarketEvent) -> Iterator[tuple[str, str, str]]:
        """Typed order identifiers with the exact field that supplied each value."""
        found: set[tuple[str, str]] = set()
        parsed: list[tuple[str, str, str]] = []
        for name, value in event.altids.items():
            field = _ORDER_CODE_FIELDS.get(_code_name(name))
            if field is not None and value:
                namespace, source = field
                parsed.append((namespace, source, str(value)))
        candidates = [
            (VENUE_ORDER_CODE, "OrderID", getattr(event, "orderid", None)),
            *(key for key in parsed if key[0] == VENUE_ORDER_CODE),
            (CLIENT_ORDER_CODE, "OrigClOrdID", getattr(event, "origclordid", None)),
            (CLIENT_ORDER_CODE, "ClOrdID", getattr(event, "clordid", None)),
            *(key for key in parsed if key[0] == CLIENT_ORDER_CODE),
        ]
        for namespace, source, value in candidates:
            if value and (key := (namespace, str(value))) not in found:
                found.add(key)
                yield namespace, source, str(value)

    def _continues_named_life(self, previous: Event) -> bool:
        """Whether FIX identifiers link this row to the preceding Order."""
        if not isinstance(previous, Order):
            return False
        if self.orderid and previous.orderid and self.orderid != previous.orderid:
            return False
        same_order = self.orderid and self.orderid == previous.orderid
        same_client_version = self.clordid and self.clordid == previous.clordid
        amends_client_version = self.origclordid and self.origclordid in (
            previous.clordid,
            previous.origclordid,
        )
        return bool(same_order or same_client_version or amends_client_version)

    def version_parts(self) -> tuple[Any, ...]:
        """An order's version moves with what it asked for, and how far it got."""
        return (
            *MarketEvent.version_parts(self),
            self.timeinforce,
            self.stoppx,
            self.hiddenqty,
            self.vwap,
            self.indicative,
            self.orderid,
            self.clordid,
            self.origclordid,
            self.clordlinkid,
            self.parentclordid,
            self.parentorderid,
            self.cxlrejreason,
            self.cxlrejresponseto,
        )


@scalar(slots=True)
class Execution(MarketEvent):
    """One fill, correction or cancellation reported against an order."""

    # An exact event hash is intentionally not reversible to its lifecycle
    # code. Keep the order anchor only while an in-memory event chain crosses
    # this report; persisted folding resolves the exact hash through its index.
    __order_code: str = ""
    __order_codesource: str = ""

    @classmethod
    @functools.cache
    def into_event_type(cls) -> EventType:
        """Event kind fixed by this shape."""
        return EventType.EXECUTION

    # The shared names remain stable while this declaration states the exact
    # report fields. An execution carries the last fill, not the order limit
    # or its remaining quantity.
    lastpx: Annotated[float | None, fix_tag("LastPx")] = None
    """What traded on this report -- the fill's price, not the order's limit."""

    lastqty: Annotated[float | None, fix_tag("LastQty")] = None
    """What traded on this report -- the fill's quantity, not the order's."""

    execid: Annotated[str | None, fix_tag("ExecID")] = None
    """Identifier the venue gave this report."""

    execrefid: Annotated[str | None, fix_tag("ExecRefID")] = None
    """Original execution amended or cancelled by this report."""

    tradeid: Annotated[str | None, fix_tag("TradeID")] = None
    """Identifier the venue gave the trade, which both sides of it share."""

    orderid: Annotated[str | None, fix_tag("OrderID")] = None
    """Identifier the venue gave that order."""

    clordid: Annotated[str | None, fix_tag("ClOrdID")] = None
    """Identifier the sender gave the version of the order that traded."""

    origclordid: Annotated[str | None, fix_tag("OrigClOrdID")] = None
    """Identifier the sender gave the preceding order version."""

    cumqty: Annotated[float | None, fix_tag("CumQty")] = None
    """Quantity done on the order as of this report, including this fill."""

    leavesqty: Annotated[float | None, fix_tag("LeavesQty")] = None
    """Quantity still working after this report."""

    vwap: float | None = None
    """Average price of everything done on the order, as of this report."""

    aggressorindicator: Annotated[bool | None, fix_tag("AggressorIndicator")] = None
    """Whether this side took liquidity; null when the venue does not say."""

    # How the trade settles. Common on real TradeCaptureReports, and money is
    # wrong without them: a fill priced in one currency can settle in another,
    # on a date the venue names rather than the trade's own.

    settldate: Annotated[datetime.datetime | None, fix_tag("SettlDate")] = None
    """When the trade settles; null when the venue leaves it to convention."""

    settltype: Annotated[str | None, fix_tag("SettlType")] = None
    """How the settlement date was chosen, in FIX's own codes."""

    settlcurrency: Annotated[Currency | None, fix_tag("SettlCurrency")] = None
    """ISO 4217 currency the trade settles in, when it differs from `currency`."""

    settlcurrfxratecalc: Annotated[str | None, fix_tag("SettlCurrFxRateCalc")] = None
    """Whether the settlement FX rate multiplies or divides."""

    def __post_init__(self) -> None:
        """Normalize settlement to the timestamp its FIX-backed column stores."""
        self.settldate = _local_timestamp(self.settldate)
        if self.settlcurrency is not None:
            currency = Currency.from_str(self.settlcurrency)
            self.settlcurrency = None if currency is Currency.UNKNOWN else currency
        MarketEvent.__post_init__(self)

    def complete_from(self, previous: Event) -> None:
        """A report completed from the one before it on the same order."""
        if isinstance(previous, Order):
            self.__order_code = previous.code or previous.life_code()
            self.__order_codesource = previous.life_code_source()
        elif isinstance(previous, Execution):
            self.__order_code, self.__order_codesource = (
                previous.__order_code,
                previous.__order_codesource,
            )
        MarketEvent.complete_from(self, previous)
        # By name, for the reason `_carry` gives.
        _carry(
            self,
            previous,
            "orderid",
            "clordid",
            "origclordid",
            "aggressorindicator",
        )
        same_report_life = (
            isinstance(previous, Execution)
            and self.state in (State.REPLACED, State.CANCELLED)
            and self.execrefid is not None
            and self.execrefid in (previous.execid, previous.execrefid, previous.code)
        )
        if same_report_life and previous.code:
            self.code = previous.code
            self.codesource = previous.codesource
            self.xhash = NIL
        done, left, average = _totals_of(previous)
        known_done = done
        delta = None
        revised_average = average
        if same_report_life and isinstance(previous, Execution):
            prior_qty = previous.lastqty
            replacement_qty = 0.0 if self.state is State.CANCELLED else self.lastqty
            if prior_qty is not None and replacement_qty is not None:
                delta = replacement_qty - prior_qty
                revised_average = (
                    _replaced_average(
                        average,
                        known_done,
                        previous.lastpx,
                        prior_qty,
                        self.lastpx,
                        replacement_qty,
                    )
                    if known_done is not None
                    else (
                        average
                        if replacement_qty == prior_qty and self.lastpx == previous.lastpx
                        else None
                    )
                )
        elif self.state is State.FILLED and self.lastqty is not None:
            if known_done is None and self.cumqty is not None and self.cumqty >= self.lastqty:
                known_done = self.cumqty - self.lastqty
            delta = self.lastqty
            revised_average = _weighted(average, known_done, self.lastpx, self.lastqty)
        if self.cumqty is None:
            self.cumqty = (
                max(known_done + delta, 0.0)
                if delta is not None and known_done is not None
                else known_done
            )
        if self.leavesqty is None and left is not None:
            self.leavesqty = max(left - delta, 0.0) if delta is not None else left
        if self.vwap is None:
            self.vwap = revised_average

    def _linked_order_life(self, current: Order) -> tuple[str, str]:
        """Transient order anchor retained while folding an in-memory report chain."""
        current_keys = set(Order.lookup_altids_of(current))
        report_keys = set(Order.lookup_altids_of(self))
        if not current_keys or current_keys.isdisjoint(report_keys):
            return "", ""
        return self.__order_code, self.__order_codesource

    def life_code(self) -> str:
        """The report identifier that survives corrections, or nothing."""
        return self.code or self._named_life_code()

    def life_code_source(self) -> str:
        """The exact execution field that supplied the readable identifier."""
        if self.code:
            return self.codesource or "Code"
        return self._named_life_key()[0]

    def _named_life_code(self) -> str:
        """The strongest execution identifier this version carries itself."""
        return self._named_life_key()[1]

    def _named_life_key(self) -> tuple[str, str]:
        """Reader-facing source name and strongest execution identifier."""
        if self.state in (State.REPLACED, State.CANCELLED) and self.execrefid:
            return "ExecRefID", self.execrefid
        if self.execid:
            return "ExecID", self.execid
        if self.tradeid:
            return "TradeID", self.tradeid
        return "", ""

    def version_parts(self) -> tuple[Any, ...]:
        """An execution's version moves when what it says about the trade does."""
        return (
            *MarketEvent.version_parts(self),
            self.execid,
            self.execrefid,
            self.tradeid,
            self.orderid,
            self.clordid,
            self.origclordid,
            self.cumqty,
            self.leavesqty,
            self.vwap,
            self.aggressorindicator,
            *_declared_value_parts(self.settldate),
            self.settltype,
            self.settlcurrency,
            self.settlcurrfxratecalc,
        )


def _carry(into: Event, previous: Event, *names: str) -> None:
    """Fill each of `names` on `into` from `previous`, where it is absent there.

    By name, because an event's version chain crosses classes: an
    `ExecutionReport <8>` produces an `Order` and an `Execution`, so the
    version before either of them is regularly the other. Neither is a base of
    the other, and a `getattr` that misses is the same as a value that was
    never sent.
    """
    for name in names:
        if getattr(into, name, None) is None:
            carried = getattr(previous, name, None)
            if carried is not None:
                setattr(into, name, carried)


def _carry_code(into: Event, previous: Event, *names: str) -> None:
    """Fill each banded code in `names` from `previous`, where the codes agree."""
    for name in names:
        current = getattr(into, name)
        if current != 0:
            continue
        carried = getattr(previous, name, None)
        if isinstance(carried, type(current)):
            setattr(into, name, carried)


def _totals_of(previous: MarketEvent) -> tuple[float | None, float | None, float | None]:
    """Known `(filled, remaining, vwap)` before an execution report."""
    if isinstance(previous, Order):
        # A first live order has traded nothing and its current quantity is
        # what remains. Later order versions deliberately carry no cumulative
        # total; an execution must then use the source's explicit totals.
        done = 0.0 if previous.prevqty is None and previous.state.is_live else None
        return done, previous.lastqty, previous.vwap
    return previous.cumqty, previous.leavesqty, previous.vwap


def _weighted(
    average: float | None, done: float | None, px: float | None, qty: float | None
) -> float | None:
    """The average price of everything done, once this fill is part of it.

    None when this fill has no price, because an average that silently skipped
    a fill is worse than an absent one. A first fill is its own average, which
    falls out of the arithmetic with nothing done before it.
    """
    if qty is None or qty == 0:
        return average
    if px is None or done is None:
        return None
    if done == 0:
        return px
    if average is None:
        return None
    total = done + qty
    return px if not total else (average * done + px * qty) / total


def _replaced_average(
    average: float | None,
    done: float,
    old_px: float | None,
    old_qty: float,
    new_px: float | None,
    new_qty: float,
) -> float | None:
    """Average after one referenced fill is replaced or removed."""
    total = done - old_qty + new_qty
    if total <= 0:
        return None
    if old_px is None:
        return average if new_qty == old_qty and new_px is None else None
    if average is None:
        if done != old_qty:
            return None
        notional = old_px * old_qty
    else:
        notional = average * done
    notional -= old_px * old_qty
    if new_qty:
        if new_px is None:
            return None
        notional += new_px * new_qty
    return notional / total
