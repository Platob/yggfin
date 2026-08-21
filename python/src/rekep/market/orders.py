"""What a participant asked for, and what actually traded."""

from __future__ import annotations

from typing import Annotated, Any, ClassVar

import pyarrow

from rekep.fields import Field, field
from rekep.market.enums import EventType, ExecKind, OrderKind, TimeInForce
from rekep.market.event import MarketEvent
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
