"""What a participant asked for, and what actually traded."""

from __future__ import annotations

from typing import Annotated, Any, ClassVar

import pyarrow

from rekep.fields import Field, field
from rekep.market.enums import EventType, ExecKind, OrderKind, State, TimeInForce
from rekep.market.event import Event, MarketEvent
from rekep.market.fields import fix_tag


@field
class Order(MarketEvent):
    """One version of one order: what was asked for, and how far it has got.

    `px` is the limit and `qty` is the quantity asked for. An order with
    `kind` in the `MARKET` band has no `px`, and that is the type saying so
    rather than a zero standing in for it.

    The running totals -- `filled_qty`, `leaves_qty`, `avg_px` -- are carried
    on the row even though they are derivable from the executions, because
    deriving them means a windowed aggregate over every fill of the lifecycle
    to answer "how much is left", which is a question asked once per tick. The
    venue already computed them and put them in the message; dropping them
    only moves the work.

    `prev_client_order_id` is FIX `OrigClOrdID <41>` and is the other half of
    what `prev_hash` says: the identity the *venue* knew this order by before
    the amendment, which is what reconciling against the venue's own records
    needs and what a content hash cannot supply.
    """

    EVENT_TYPE: ClassVar[EventType] = EventType.ORDER

    kind: Annotated[OrderKind, fix_tag("OrdType", 40)] = OrderKind.UNKNOWN
    """How the order is priced; the band says whether `px` and `stop_px` mean anything."""

    tif: Annotated[TimeInForce, fix_tag("TimeInForce", 59)] = TimeInForce.UNKNOWN
    """How long it lives. `GTD` expires at `eunix`, where every expiry here lives."""

    stop_px: Annotated[float | None, fix_tag("StopPx", 99)] = None
    """Trigger price of a stop order; `px` is the limit that applies once triggered."""

    display_qty: Annotated[float | None, fix_tag("MaxFloor", 111)] = None
    """How much of `qty` the book shows; the rest is hidden."""

    filled_qty: Annotated[float | None, fix_tag("CumQty", 14)] = None
    """Quantity done so far, as the venue counts it."""

    leaves_qty: Annotated[float | None, fix_tag("LeavesQty", 151)] = None
    """Quantity still working; zero on anything terminal."""

    avg_px: Annotated[float | None, fix_tag("AvgPx", 6)] = None
    """Average price of what has been done, weighted by quantity."""

    order_id: Annotated[str | None, fix_tag("OrderID", 37)] = None
    """Identifier the venue gave the order."""

    client_order_id: Annotated[str | None, fix_tag("ClOrdID", 11)] = None
    """Identifier the sender gave this version of the order."""

    prev_client_order_id: Annotated[str | None, fix_tag("OrigClOrdID", 41)] = None
    """Identifier the sender gave the version this one replaced."""

    # int32 rather than int64: a reject code is a small number in every
    # dictionary that defines one, and it is not an enum here because past the
    # handful FIX standardises every venue numbers its own.
    reason_code: Annotated[
        int | None, fix_tag("OrdRejReason", 103), Field(arrow_type=pyarrow.int32())
    ] = None
    """Why the order was refused or restated, as the venue numbers it."""

    reason: Annotated[str | None, fix_tag("Text", 58)] = None
    """Free text the venue sent with the refusal or the restatement."""

    def complete_from(self, previous: Event) -> None:
        """An order completed from its last version, by what a market actually means.

        Beyond carrying forward what the message did not repeat, three rules
        that are arithmetic rather than copying:

        - **`prev_client_order_id` is the identifier that was replaced.** FIX
          calls it `OrigClOrdID <41>` and requires a new `ClOrdID <11>` per
          version, so when this version has a different one and did not say
          what it replaced, the version before it *is* the answer.
        - **A fully filled order filled what it asked for.** `State.FILLED`
          means all of it, so a report that says the state and not `CumQty
          <14>` has still said how much was done: what the version before
          asked for. `derive` is about to zero the quantity on this row, so
          this is the only place that number is still readable.
        """
        super().complete_from(previous)
        # Read by name and not behind an `isinstance`, because an order's own
        # chain interleaves with executions: one `ExecutionReport <8>` yields
        # both, so the version before an order is very often a fill. Neither
        # class is a base of the other, and both carry these fields.
        # `filled_qty` and `avg_px` are named fields and mean the same thing
        # wherever they appear, unlike the abstract slots -- so they carry
        # across shapes, and a fill's running totals complete the next order
        # version of the same order.
        _carry(self, previous, "stop_px", "display_qty", "filled_qty", "avg_px", "order_id")
        if self.leaves_qty is None and self.qty is None:
            # Carried only when `derive` below cannot work it out, which is
            # the cross-shape case: an order following a fill has no quantity
            # of its own to subtract from. Where it *does* -- an order
            # following an order -- carrying would win over the derivation and
            # keep reporting the quantity that was left before this version's
            # own `CumQty <14>` said otherwise.
            self.leaves_qty = getattr(previous, "leaves_qty", None)
        if self.kind is OrderKind.UNKNOWN:
            self.kind = getattr(previous, "kind", self.kind) or self.kind
        if self.tif is TimeInForce.UNKNOWN:
            self.tif = getattr(previous, "tif", self.tif) or self.tif
        named = getattr(previous, "client_order_id", None)
        if self.client_order_id is None:
            self.client_order_id = named
        elif self.prev_client_order_id is None and named not in (None, self.client_order_id):
            self.prev_client_order_id = named
        if self.filled_qty is None and self.state is State.FILLED:
            # Filled means all of it, so how much was done is what was asked
            # -- which this version no longer carries, because the rule below
            # has just zeroed it.
            self.filled_qty = getattr(previous, "qty", None)

    def derive(self) -> None:
        """What an order's own numbers say about each other.

        **`leaves_qty` is what is left**: `qty - filled_qty`, with nothing
        filled counting as nothing filled. A venue that sends `CumQty <14>`
        and not `LeavesQty <151>` has still said how much is working, and
        deriving it here is what stops every reader deriving it differently.

        **A terminal order rests nothing.** `leaves_qty` and `qty` both go to
        zero, because there is no quantity on the row any more: the order is
        done, cancelled or expired, and a book folding it has to take its
        liquidity out rather than leave it standing. What was asked for is not
        lost -- it is on the version before, which `prev_hash` names, and
        `filled_qty` says how much of it happened.
        """
        if self.state.is_terminal:
            self.leaves_qty = 0.0
            self.qty = 0.0
        elif self.leaves_qty is None and self.qty is not None:
            self.leaves_qty = max(self.qty - (self.filled_qty or 0.0), 0.0)
        super().derive()

    def life_parts(self) -> tuple[Any, ...]:
        """An order's lifecycle is the identifier that survives its amendments.

        `OrderID <37>` first, because the venue assigns it once and keeps it
        across a cancel/replace -- which is the definition of the lifecycle.
        `ClOrdID <11>` does *not* survive one: the standard requires a new
        one per version. So when only client identifiers are there,
        `OrigClOrdID <41>` is preferred, which puts a replacement on the same
        lifecycle as the version it replaced. One hop is exact; a chain of
        replacements walks back one link per version, which is why
        `with_previous` carries the lifecycle forward instead of re-deriving
        it, and why a venue that sends `OrderID` never needs any of this.
        """
        named = self.order_id or self.prev_client_order_id or self.client_order_id
        if not named:
            return super().life_parts()
        return (self.instrument_hash, self.venue or "", named)

    def version_parts(self) -> tuple[Any, ...]:
        """An order's version moves with what it asked for, and how far it got."""
        return (*super().version_parts(), self.client_order_id, self.filled_qty)


@field
class Execution(MarketEvent):
    """One thing that happened to an order -- usually, but not always, a trade.

    `px` is FIX `LastPx <31>` and `qty` is `LastQty <32>`: what traded on this
    report, not what the order asked for. `kind >= ExecKind.TRADE` is what
    separates the reports where shares moved from the acknowledgements that
    share the same message, and summing `qty` without that filter counts every
    ack as a fill.

    `order_xhash` is the order's lifecycle, flat and typed, beside the generic
    `parent_hash` list. The list is the truth about lineage; the flat column is
    what a join uses, because no engine under this package joins on an array
    without exploding it first, and an explode of a fills table is a shuffle
    nobody needs to pay for a link that is always single-valued.
    """

    EVENT_TYPE: ClassVar[EventType] = EventType.EXECUTION

    # The abstract slots, re-declared for the one thing a subclass owns about
    # them: which FIX field they actually hold. `MarketEvent` tags them
    # `Price <44>` and `OrderQty <38>` because that is what an order's are,
    # and a report's are `LastPx` and `LastQty` -- the *last fill*, not the
    # order. Re-declaring keeps the column exactly where it was (a dataclass
    # field re-annotated keeps its position) and stops the schema naming a
    # field it does not carry.
    px: Annotated[float | None, fix_tag("LastPx", 31)] = None
    """What traded on this report -- the fill's price, not the order's limit."""

    qty: Annotated[float | None, fix_tag("LastQty", 32)] = None
    """What traded on this report -- the fill's quantity, not the order's."""

    kind: Annotated[ExecKind, fix_tag("ExecType", 150)] = ExecKind.UNKNOWN
    """What this report says happened; `>= ExecKind.TRADE` means shares moved."""

    exec_id: Annotated[str | None, fix_tag("ExecID", 17)] = None
    """Identifier the venue gave this report."""

    trade_id: Annotated[str | None, fix_tag("TradeID", 1003)] = None
    """Identifier the venue gave the trade, which both sides of it share."""

    order_xhash: int | None = None
    """Lifecycle of the order this happened to -- the join key, single-valued."""

    order_id: Annotated[str | None, fix_tag("OrderID", 37)] = None
    """Identifier the venue gave that order."""

    client_order_id: Annotated[str | None, fix_tag("ClOrdID", 11)] = None
    """Identifier the sender gave the version of the order that traded."""

    filled_qty: Annotated[float | None, fix_tag("CumQty", 14)] = None
    """Quantity done on the order as of this report, including this fill."""

    leaves_qty: Annotated[float | None, fix_tag("LeavesQty", 151)] = None
    """Quantity still working after this report."""

    avg_px: Annotated[float | None, fix_tag("AvgPx", 6)] = None
    """Average price of everything done on the order, as of this report."""

    aggressor: Annotated[bool | None, fix_tag("AggressorIndicator", 1057)] = None
    """Whether this side took liquidity; null when the venue does not say."""

    reason_code: Annotated[
        int | None, fix_tag("ExecRestatementReason", 378), Field(arrow_type=pyarrow.int32())
    ] = None
    """Why a restatement or a refusal happened, as the venue numbers it."""

    reason: Annotated[str | None, fix_tag("Text", 58)] = None
    """Free text the venue sent with the report."""

    def complete_from(self, previous: Event) -> None:
        """A report completed from the one before it on the same order.

        `previous` here is the last report of the *order*, not of this
        execution -- which is what makes the running totals derivable at all.
        The three that are arithmetic:

        - **`filled_qty` accumulates.** A venue that sends `LastQty <32>` and
          not `CumQty <14>` has still said how much is now done: what was done
          before, plus this fill.
        - **`leaves_qty` decreases by the same fill**, which is the other half
          of the same statement.
        - **`avg_px` is re-weighted**, not copied: the average of everything
          done is the previous average over the previous quantity, plus this
          fill over its own, divided by the total. Copying it forward would
          leave every partial fill reporting the first one's price.

        All three only fill where the venue said nothing, and all three need
        this fill to have moved shares -- an acknowledgement changes no total,
        and adding its quantity is how a fills table starts overcounting.
        """
        super().complete_from(previous)
        # By name, for the reason `Order.complete_from` gives: the version
        # before a fill is as often an order as another fill.
        _carry(self, previous, "order_xhash", "order_id", "client_order_id", "aggressor")
        if self.kind is ExecKind.UNKNOWN:
            self.kind = getattr(previous, "kind", None) or self.kind
        if self.order_xhash is None and previous.is_order():
            self.order_xhash = previous.xhash
        done, left, average = _totals_of(previous)
        moved = self.kind.moves_shares and self.qty is not None
        if self.filled_qty is None:
            self.filled_qty = (done or 0.0) + self.qty if moved else done
        if self.leaves_qty is None and left is not None:
            self.leaves_qty = max(left - self.qty, 0.0) if moved else left
        if self.avg_px is None:
            self.avg_px = _weighted(average, done, self.px, self.qty) if moved else average

    def life_parts(self) -> tuple[Any, ...]:
        """An execution's lifecycle is the report the venue identified it by.

        `ExecID <17>` first, which the standard makes unique per report;
        `TradeID <1003>` after it, which both sides of a trade share and which
        is what a trade-capture report carries instead. A fill amended later
        (`ExecType` `G`/`H`) carries the *same* identifier, which is exactly
        right: a correction is another version of one execution, not a
        second one.
        """
        named = self.exec_id or self.trade_id
        if not named:
            return super().life_parts()
        return (self.instrument_hash, self.venue or "", named)

    def version_parts(self) -> tuple[Any, ...]:
        """An execution's version moves when what it says about the trade does."""
        return (*super().version_parts(), self.kind, self.exec_id, self.filled_qty)


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


def _totals_of(previous: MarketEvent) -> tuple[float | None, float | None, float | None]:
    """`(filled_qty, leaves_qty, avg_px)` of whatever the previous version was.

    An execution's previous version is the last report of the order, which may
    be an `Order` or another `Execution` -- both carry the three running
    totals, and neither is a base of the other, so this reads them by name
    rather than by type.
    """
    return (
        getattr(previous, "filled_qty", None),
        getattr(previous, "leaves_qty", None),
        getattr(previous, "avg_px", None),
    )


def _weighted(
    average: float | None, done: float | None, px: float | None, qty: float | None
) -> float | None:
    """The average price of everything done, once this fill is part of it.

    None when this fill has no price, because an average that silently skipped
    a fill is worse than an absent one. A first fill is its own average, which
    falls out of the arithmetic with nothing done before it.
    """
    if px is None or qty is None:
        return average
    if not average or not done:
        return px
    total = done + qty
    return px if not total else (average * done + px * qty) / total
