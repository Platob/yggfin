"""What a participant asked for, and what actually traded."""

from __future__ import annotations

import dataclasses
import datetime
import functools
from collections.abc import Iterator
from types import MappingProxyType
from typing import Annotated, Any

from rekep.enums import Currency, EventType, ManualIndicator, State, TimeInForce
from rekep.fields import Field, column_name, scalar
from rekep.market.event import Event, MarketEvent, _declared_value_parts, _local_timestamp
from rekep.market.fields import fix_tag
from rekep.market.identity import NIL

# These namespaces preserve the meaning of each `altids` entry: equal text in
# OrderID and ClOrdID is not evidence that two orders are the same lifecycle.
VENUE_ORDER_CODE = "order"
CLIENT_ORDER_CODE = "client_order"

_ORDER_CODE_FIELDS = MappingProxyType(
    {
        "globalorderid": (VENUE_ORDER_CODE, "GlobalOrderId"),
        "rootorderid": (VENUE_ORDER_CODE, "RootOrderId"),
        "orderid": (VENUE_ORDER_CODE, "OrderID"),
        "secondaryorderid": (VENUE_ORDER_CODE, "SecondaryOrderID"),
        "quoteentryid": (VENUE_ORDER_CODE, "QuoteEntryID"),
        "quoteid": (VENUE_ORDER_CODE, "QuoteID"),
        "mdentryid": (VENUE_ORDER_CODE, "MDEntryID"),
        "mdentryrefid": (VENUE_ORDER_CODE, "MDEntryRefID"),
        "parentorderid": (VENUE_ORDER_CODE, "ParentOrderID"),
        "origclordid": (CLIENT_ORDER_CODE, "OrigClOrdID"),
        "clordid": (CLIENT_ORDER_CODE, "ClOrdID"),
        "clordlinkid": (CLIENT_ORDER_CODE, "ClOrdLinkID"),
        "parentclordid": (CLIENT_ORDER_CODE, "ParentClOrdID"),
        "rootoriginatororderid": (CLIENT_ORDER_CODE, "RootOriginatorOrderId"),
        "secondaryclordid": (CLIENT_ORDER_CODE, "SecondaryClOrdID"),
        "quotereqid": (CLIENT_ORDER_CODE, "QuoteReqID"),
    }
)

_ORDER_CODE_PRIORITY = (
    "orderid",
    "globalorderid",
    "rootorderid",
    "secondaryorderid",
    "quoteentryid",
    "quoteid",
    "mdentryid",
    "mdentryrefid",
    "origclordid",
    "clordid",
    "rootoriginatororderid",
    "secondaryclordid",
    "quotereqid",
    "clordlinkid",
    "parentclordid",
    "parentorderid",
)

_EXECUTION_CODE_PRIORITY = (
    "execid",
    "secondaryexecid",
    "tradeid",
    "trdmatchid",
    "mdentryid",
)


@functools.lru_cache(maxsize=128)
def _code_name(name: str) -> str:
    """One source identifier name in the spelling the lookup contract reads."""
    return column_name(name)


def _altid(event: Event, name: str) -> str:
    """One folded identifier from an event's only identifier store."""
    value = event.altids.get(_code_name(name))
    return str(value) if value else ""


def _set_altid(event: Event, name: str, value: str | None) -> None:
    """Store one non-empty identifier under its folded source name."""
    if value:
        event.altids[_code_name(name)] = str(value)


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

    manualindicator: Annotated[ManualIndicator, fix_tag("ManualOrderIndicator")] = (
        ManualIndicator.UNKNOWN
    )
    """Whether a person entered the order; unknown when the source does not say."""

    def complete_from(self, previous: Event) -> None:
        """An order completed from its last version, by what a market actually means."""
        same_named_life = self._continues_named_life(previous)
        linked_life = previous._linked_order_life(self) if isinstance(previous, Execution) else ""
        MarketEvent.complete_from(self, previous)
        # By name, for the reason `_carry` gives. `vwap` means the same thing
        # wherever it appears, unlike the abstract price and quantity slots.
        _carry(
            self,
            previous,
            "stoppx",
            "vwap",
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
        _carry_code(self, previous, "timeinforce", "manualindicator")
        named = _altid(previous, "clordid")
        current = _altid(self, "clordid")
        if not current:
            _set_altid(self, "clordid", named)
        elif not _altid(self, "origclordid") and named not in ("", current):
            _set_altid(self, "origclordid", named)
        anchor = previous.code or previous.life_code() if same_named_life else linked_life
        if anchor:
            # A later acknowledgement may introduce the venue's OrderID. The
            # exact field keeps it, while the lifecycle stays on its first
            # readable anchor. Clear the incoming identity so the envelope
            # hashes that anchor again.
            self.code = anchor
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

    def _named_life_code(self) -> str:
        """The strongest typed order identifier this version carries."""
        return self._named_life_key()[1]

    def _named_life_key(self) -> tuple[str, str]:
        """Reader-facing source name and strongest order identifier."""
        return next(((source, value) for _, source, value in self._code_fields_of(self)), ("", ""))

    @classmethod
    def lookup_altids_of(cls, event: MarketEvent) -> Iterator[tuple[str, str]]:
        """Typed order identifiers on `event`, strongest first and once each.

        Venue identifiers lead client identifiers. `altids` is the only
        persisted store, so hand-built and parsed rows follow the same rule.
        """
        yield from ((namespace, value) for namespace, _, value in cls._code_fields_of(event))

    @classmethod
    def _code_fields_of(cls, event: MarketEvent) -> Iterator[tuple[str, str, str]]:
        """Typed order identifiers with the exact field that supplied each value."""
        found: set[tuple[str, str]] = set()
        for name in _ORDER_CODE_PRIORITY:
            value = _altid(event, name)
            field = _ORDER_CODE_FIELDS.get(name)
            if field is None or not value:
                continue
            namespace, source = field
            if value and (key := (namespace, str(value))) not in found:
                found.add(key)
                yield namespace, source, str(value)

    def _continues_named_life(self, previous: Event) -> bool:
        """Whether FIX identifiers link this row to the preceding Order."""
        if not isinstance(previous, Order):
            return False
        orderid = _altid(self, "orderid")
        previous_orderid = _altid(previous, "orderid")
        clordid = _altid(self, "clordid")
        previous_clordid = _altid(previous, "clordid")
        origclordid = _altid(self, "origclordid")
        previous_origclordid = _altid(previous, "origclordid")
        moved_venue_id = bool(orderid and previous_orderid and orderid != previous_orderid)
        amended = bool(origclordid and origclordid in (previous_clordid, previous_origclordid))
        if amended:
            return True
        if not moved_venue_id and (
            (orderid and orderid == previous_orderid) or (clordid and clordid == previous_clordid)
        ):
            return True

        current = tuple(self._code_fields_of(self))
        held = tuple(self._code_fields_of(previous))
        # A reused ClOrdID cannot reconcile two contradictory venue IDs. A
        # second stable identity can: it is lifecycle evidence rather than an
        # accidental text match.
        stable_sources = {
            "GlobalOrderId",
            "RootOrderId",
            "RootOriginatorOrderId",
            "SecondaryOrderID",
        }
        if moved_venue_id:
            current_keys = {
                (namespace, value)
                for namespace, source, value in current
                if source in stable_sources
            }
            held_keys = {
                (namespace, value) for namespace, source, value in held if source in stable_sources
            }
        else:
            current_keys = {(namespace, value) for namespace, _, value in current}
            held_keys = {(namespace, value) for namespace, _, value in held}
        return not current_keys.isdisjoint(held_keys)

    def version_parts(self) -> tuple[Any, ...]:
        """An order's version moves with what it asked for, and how far it got."""
        return (
            *MarketEvent.version_parts(self),
            self.timeinforce,
            self.stoppx,
            self.hiddenqty,
            self.vwap,
            self.indicative,
            self.manualindicator,
        )


@scalar(slots=True)
class Execution(MarketEvent):
    """One fill, correction or cancellation reported against an order."""

    # An exact event hash is intentionally not reversible to its lifecycle
    # code. Keep the order anchor only while an in-memory event chain crosses
    # this report; persisted folding resolves the exact hash through its index.
    __order_code: str = ""

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

    cumqty: Annotated[float | None, fix_tag("CumQty")] = None
    """Quantity done on the order as of this report, including this fill."""

    leavesqty: Annotated[float | None, fix_tag("LeavesQty")] = None
    """Quantity still working after this report."""

    vwap: float | None = None
    """Average price of everything done on the order, as of this report."""

    aggressorindicator: Annotated[bool | None, fix_tag("AggressorIndicator")] = None
    """Whether this side took liquidity; null when the venue does not say."""

    manualindicator: Annotated[ManualIndicator, fix_tag("ManualOrderIndicator")] = (
        ManualIndicator.UNKNOWN
    )
    """Whether a person entered the reported trade; unknown when unstated."""

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
        elif isinstance(previous, Execution):
            self.__order_code = previous.__order_code
        MarketEvent.complete_from(self, previous)
        # By name, for the reason `_carry` gives.
        _carry(
            self,
            previous,
            "aggressorindicator",
        )
        _carry_code(self, previous, "manualindicator")
        execrefid = _altid(self, "execrefid")
        same_report_life = (
            isinstance(previous, Execution)
            and self.state in (State.REPLACED, State.CANCELLED)
            and bool(execrefid)
            and execrefid
            in (_altid(previous, "execid"), _altid(previous, "execrefid"), previous.code)
        )
        if same_report_life and previous.code:
            self.code = previous.code
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

    def _linked_order_life(self, current: Order) -> str:
        """Transient order anchor retained while folding an in-memory report chain."""
        current_keys = set(Order.lookup_altids_of(current))
        report_keys = set(Order.lookup_altids_of(self))
        if not current_keys or current_keys.isdisjoint(report_keys):
            return ""
        return self.__order_code

    def life_code(self) -> str:
        """The report identifier that survives corrections, or nothing."""
        return self.code or self._named_life_code()

    def _named_life_code(self) -> str:
        """The strongest execution identifier this version carries itself."""
        return self._named_life_key()[1]

    def _named_life_key(self) -> tuple[str, str]:
        """Reader-facing source name and strongest execution identifier."""
        if self.state in (State.REPLACED, State.CANCELLED) and (
            execrefid := _altid(self, "execrefid")
        ):
            return "ExecRefID", execrefid
        for name in _EXECUTION_CODE_PRIORITY:
            if value := _altid(self, name):
                return name, value
        return "", ""

    def version_parts(self) -> tuple[Any, ...]:
        """An execution's version moves when what it says about the trade does."""
        return (
            *MarketEvent.version_parts(self),
            self.cumqty,
            self.leavesqty,
            self.vwap,
            self.aggressorindicator,
            self.manualindicator,
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
