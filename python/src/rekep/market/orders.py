"""What a participant asked for, and what actually traded."""

from __future__ import annotations

import functools
from typing import Annotated, Any

import pyarrow

from rekep.enums import EventType, State, TimeInForce
from rekep.fields import Field, scalar
from rekep.market.event import Event, MarketEvent
from rekep.market.fields import fix_tag
from rekep.market.identity import NIL


@scalar(slots=True)
class Order(MarketEvent):
    """One version of one order: what was asked for, and how far it has got."""

    @classmethod
    @functools.cache
    def into_event_type(cls) -> EventType:
        """Event kind fixed by this shape."""
        return EventType.ORDER

    tif: Annotated[TimeInForce, fix_tag("TimeInForce")] = TimeInForce.UNKNOWN
    """How long it lives. `GTD` expires at `eunix`, where every expiry here lives."""

    exposure_duration: Annotated[int | None, fix_tag("ExposureDuration")] = None
    """FIX GFT duration; null when absent."""

    exposure_duration_unit: Annotated[
        int | None, fix_tag("ExposureDurationUnit"), Field(arrow_type=pyarrow.int32())
    ] = None
    """FIX GFT duration unit; null means the FIX default of seconds."""

    stop_px: Annotated[float | None, fix_tag("StopPx")] = None
    """Trigger price of a stop order; `px` is the limit that applies once triggered."""

    hidden_qty: float | None = None
    """Current quantity hidden from the displayed book; null when unstated."""

    vwap: Annotated[float | None, fix_tag("AvgPx")] = None
    """Average price of what has been done, weighted by quantity."""

    indicative: bool = False
    """Whether this interest is a quote rather than a firm order."""

    order_id: Annotated[str | None, fix_tag("OrderID")] = None
    """Identifier the venue gave the order."""

    client_order_id: Annotated[str | None, fix_tag("ClOrdID")] = None
    """Identifier the sender gave this version of the order."""

    prev_client_order_id: Annotated[str | None, fix_tag("OrigClOrdID")] = None
    """Identifier the sender gave the version this one replaced."""

    # int32 rather than int64: a reject code is a small number in every
    # dictionary that defines one, and it is not an enum here because past the
    # handful FIX standardises every venue numbers its own.
    reason_code: Annotated[
        int | None, fix_tag("OrdRejReason"), Field(arrow_type=pyarrow.int32())
    ] = None
    """Why the order was refused or restated, as the venue numbers it."""

    reason: Annotated[str | None, fix_tag("Text")] = None
    """Free text the venue sent with the refusal or the restatement."""

    def complete_from(self, previous: Event) -> None:
        """An order completed from its last version, by what a market actually means."""
        same_named_life = self._continues_named_life(previous)
        MarketEvent.complete_from(self, previous)
        # By name, for the reason `_carry` gives. `vwap` means the same thing
        # wherever it appears, unlike the abstract price and quantity slots.
        _carry(
            self,
            previous,
            "stop_px",
            "hidden_qty",
            "vwap",
            "order_id",
            "exposure_duration",
            "exposure_duration_unit",
        )
        if self.qty is None and isinstance(previous, Execution):
            self.qty = previous.leaves_qty
        _carry_code(self, previous, "tif")
        named = getattr(previous, "client_order_id", None)
        if self.client_order_id is None:
            self.client_order_id = named
        elif self.prev_client_order_id is None and named not in (None, self.client_order_id):
            self.prev_client_order_id = named
        anchor = (
            previous.xcode or previous.life_code()
            if same_named_life
            else self._parent_order_life_code(previous)
        )
        if anchor:
            # A later acknowledgement may introduce the venue's OrderID. The
            # exact field keeps it, while the lifecycle stays on its first
            # readable anchor. Rehash even when its text already agrees:
            # completion may just have supplied the instrument or venue scope.
            self.xcode = anchor
            self.xhash = NIL

    def derive(self) -> None:
        """What an order's own numbers say about each other."""
        if self.expires_on_arrival:
            self.eunix = self.unix
        if self.state.is_terminal:
            self.qty = 0.0
            self.hidden_qty = 0.0
        MarketEvent.derive(self)

    @property
    def expires_on_arrival(self) -> bool:
        """Whether FIX says unfilled quantity can never rest."""
        return TimeInForce.IMMEDIATE <= self.tif < TimeInForce.SESSION

    def life_parts(self) -> tuple[Any, ...]:
        """An order's lifecycle is the identifier that survives its amendments."""
        named = self._named_life_code()
        if not named and (not self.xcode or self.xcode == self.code):
            return MarketEvent.life_parts(self)
        return (self.instrument_xhash, self.mic, self.xcode or named, self.side)

    def life_code(self) -> str:
        """The order identifier that survives amendments, then the market fallback."""
        return self.xcode or self._named_life_code() or MarketEvent.life_code(self)

    def _named_life_code(self) -> str:
        """The strongest order identifier this version carries itself."""
        return self.order_id or self.prev_client_order_id or self.client_order_id or ""

    def _continues_named_life(self, previous: Event) -> bool:
        """Whether FIX identifiers link this row to the preceding Order."""
        if not isinstance(previous, Order):
            return False
        if self.order_id and previous.order_id and self.order_id != previous.order_id:
            return False
        same_order = self.order_id and self.order_id == previous.order_id
        same_client_version = (
            self.client_order_id and self.client_order_id == previous.client_order_id
        )
        amends_client_version = self.prev_client_order_id and self.prev_client_order_id in (
            previous.client_order_id,
            previous.prev_client_order_id,
        )
        return bool(same_order or same_client_version or amends_client_version)

    def _parent_order_life_code(self, previous: Event) -> str:
        """An order code whose scoped hash matches a parent Execution's order."""
        target = previous.primary_linked_xhash
        if not target:
            return ""
        candidates = dict.fromkeys(
            (
                self.order_id,
                self.prev_client_order_id,
                self.client_order_id,
                getattr(previous, "order_id", None),
                getattr(previous, "prev_client_order_id", None),
                getattr(previous, "client_order_id", None),
            )
        )
        for candidate in candidates:
            if (
                candidate
                and self.hash_of(
                    self.instrument_xhash,
                    self.mic,
                    candidate,
                    self.side,
                )
                == target
            ):
                return candidate
        return ""

    def version_parts(self) -> tuple[Any, ...]:
        """An order's version moves with what it asked for, and how far it got."""
        return (
            *MarketEvent.version_parts(self),
            self.client_order_id,
            self.hidden_qty,
            self.vwap,
            self.indicative,
        )


@scalar(slots=True)
class Execution(MarketEvent):
    """One fill, correction or cancellation reported against an order."""

    @classmethod
    @functools.cache
    def into_event_type(cls) -> EventType:
        """Event kind fixed by this shape."""
        return EventType.EXECUTION

    # The abstract slots, re-declared for the one thing a subclass owns about
    # them: which FIX field they actually hold. `MarketEvent` tags them
    # `Price <44>` and `OrderQty <38>` because that is what an order's are,
    # and a report's are `LastPx` and `LastQty` -- the *last fill*, not the
    # order. Re-declaring keeps the column exactly where it was (a dataclass
    # field re-annotated keeps its position) and stops the schema naming a
    # field it does not carry.
    px: Annotated[float | None, fix_tag("LastPx")] = None
    """What traded on this report -- the fill's price, not the order's limit."""

    qty: Annotated[float | None, fix_tag("LastQty")] = None
    """What traded on this report -- the fill's quantity, not the order's."""

    exec_id: Annotated[str | None, fix_tag("ExecID")] = None
    """Identifier the venue gave this report."""

    exec_ref_id: Annotated[str | None, fix_tag("ExecRefID")] = None
    """Original execution amended or cancelled by this report."""

    trade_id: Annotated[str | None, fix_tag("TradeID")] = None
    """Identifier the venue gave the trade, which both sides of it share."""

    order_id: Annotated[str | None, fix_tag("OrderID")] = None
    """Identifier the venue gave that order."""

    client_order_id: Annotated[str | None, fix_tag("ClOrdID")] = None
    """Identifier the sender gave the version of the order that traded."""

    prev_client_order_id: Annotated[str | None, fix_tag("OrigClOrdID")] = None
    """Identifier the sender gave the preceding order version."""

    filled_qty: Annotated[float | None, fix_tag("CumQty")] = None
    """Quantity done on the order as of this report, including this fill."""

    leaves_qty: Annotated[float | None, fix_tag("LeavesQty")] = None
    """Quantity still working after this report."""

    vwap: Annotated[float | None, fix_tag("AvgPx")] = None
    """Average price of everything done on the order, as of this report."""

    aggressor: Annotated[bool | None, fix_tag("AggressorIndicator")] = None
    """Whether this side took liquidity; null when the venue does not say."""

    reason_code: Annotated[
        int | None, fix_tag("ExecRestatementReason"), Field(arrow_type=pyarrow.int32())
    ] = None
    """Why a restatement or a refusal happened, as the venue numbers it."""

    reason: Annotated[str | None, fix_tag("Text")] = None
    """Free text the venue sent with the report."""

    def complete_from(self, previous: Event) -> None:
        """A report completed from the one before it on the same order."""
        MarketEvent.complete_from(self, previous)
        # By name, for the reason `_carry` gives.
        _carry(
            self,
            previous,
            "order_id",
            "client_order_id",
            "prev_client_order_id",
            "aggressor",
        )
        same_report_life = (
            isinstance(previous, Execution)
            and self.state in (State.REPLACED, State.CANCELLED)
            and self.exec_ref_id is not None
            and self.exec_ref_id in (previous.exec_id, previous.exec_ref_id, previous.xcode)
        )
        if previous.is_order():
            self.link_to(previous.xhash, primary=True)
        if same_report_life and previous.xcode:
            self.xcode = previous.xcode
            self.xhash = NIL
        done, left, average = _totals_of(previous)
        known_done = done
        delta = None
        revised_average = average
        if same_report_life and isinstance(previous, Execution):
            prior_qty = previous.qty
            replacement_qty = 0.0 if self.state is State.CANCELLED else self.qty
            if prior_qty is not None and replacement_qty is not None:
                delta = replacement_qty - prior_qty
                revised_average = (
                    _replaced_average(
                        average,
                        known_done,
                        previous.px,
                        prior_qty,
                        self.px,
                        replacement_qty,
                    )
                    if known_done is not None
                    else (
                        average if replacement_qty == prior_qty and self.px == previous.px else None
                    )
                )
        elif self.state is State.FILLED and self.qty is not None:
            if known_done is None and previous.is_order() and average is None:
                known_done = 0.0
            delta = self.qty
            revised_average = _weighted(average, known_done, self.px, self.qty)
        if self.filled_qty is None:
            self.filled_qty = (
                max(known_done + delta, 0.0)
                if delta is not None and known_done is not None
                else known_done
            )
        if self.leaves_qty is None and left is not None:
            self.leaves_qty = max(left - delta, 0.0) if delta is not None else left
        if self.vwap is None:
            self.vwap = revised_average

    def life_parts(self) -> tuple[Any, ...]:
        """An execution's lifecycle is the report the venue identified it by.

        `ExecID <17>` first, which the standard makes unique per report;
        `TradeID <1003>` after it, which both sides of a trade share and which
        is what a trade-capture report carries instead. A correction uses
        `ExecRefID <19>` to stay on the report it amends.
        """
        named = self._named_life_code()
        if not named and (not self.xcode or self.xcode == self.code):
            return MarketEvent.life_parts(self)
        return (self.instrument_xhash, self.mic, self.xcode or named, self.side)

    def life_code(self) -> str:
        """The report identifier that survives corrections, then the market fallback."""
        return self.xcode or self._named_life_code() or MarketEvent.life_code(self)

    def _named_life_code(self) -> str:
        """The strongest execution identifier this version carries itself."""
        if self.state in (State.REPLACED, State.CANCELLED) and self.exec_ref_id:
            return self.exec_ref_id
        return self.exec_id or self.trade_id or ""

    def version_parts(self) -> tuple[Any, ...]:
        """An execution's version moves when what it says about the trade does."""
        return (*MarketEvent.version_parts(self), self.exec_id, self.filled_qty, self.vwap)


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
        done = 0.0 if previous.prev_qty is None and previous.state.is_live else None
        return done, previous.qty, previous.vwap
    return previous.filled_qty, previous.leaves_qty, previous.vwap


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
