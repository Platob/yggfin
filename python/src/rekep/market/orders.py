"""What a participant asked for, and what actually traded."""

from __future__ import annotations

import uuid
from typing import Annotated

import pyarrow

from rekep.fields import Field, field
from rekep.market.enums import ExecKind, OrderKind, TimeInForce
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
    what `prev_h128` says: the identity the *venue* knew this order by before
    the amendment, which is what reconciling against the venue's own records
    needs and what a content hash cannot supply.
    """

    kind: Annotated[OrderKind, fix_tag("OrdType", 40)]
    """How the order is priced; the band says whether `px` and `stop_px` mean anything."""

    tif: Annotated[TimeInForce, fix_tag("TimeInForce", 59)]
    """How long it lives. `GTD` expires at `eunix`, where every expiry here lives."""

    stop_px: Annotated[float | None, fix_tag("StopPx", 99)]
    """Trigger price of a stop order; `px` is the limit that applies once triggered."""

    display_qty: Annotated[float | None, fix_tag("MaxFloor", 111)]
    """How much of `qty` the book shows; the rest is hidden."""

    filled_qty: Annotated[float | None, fix_tag("CumQty", 14)]
    """Quantity done so far, as the venue counts it."""

    leaves_qty: Annotated[float | None, fix_tag("LeavesQty", 151)]
    """Quantity still working; zero on anything terminal."""

    avg_px: Annotated[float | None, fix_tag("AvgPx", 6)]
    """Average price of what has been done, weighted by quantity."""

    order_id: Annotated[str | None, fix_tag("OrderID", 37)]
    """Identifier the venue gave the order."""

    client_order_id: Annotated[str | None, fix_tag("ClOrdID", 11)]
    """Identifier the sender gave this version of the order."""

    prev_client_order_id: Annotated[str | None, fix_tag("OrigClOrdID", 41)]
    """Identifier the sender gave the version this one replaced."""

    # int32 rather than int64: a reject code is a small number in every
    # dictionary that defines one, and it is not an enum here because past the
    # handful FIX standardises every venue numbers its own.
    reason_code: Annotated[
        int | None, fix_tag("OrdRejReason", 103), Field(arrow_type=pyarrow.int32())
    ]
    """Why the order was refused or restated, as the venue numbers it."""

    reason: Annotated[str | None, fix_tag("Text", 58)]
    """Free text the venue sent with the refusal or the restatement."""


@field
class Execution(MarketEvent):
    """One thing that happened to an order -- usually, but not always, a trade.

    `px` is FIX `LastPx <31>` and `qty` is `LastQty <32>`: what traded on this
    report, not what the order asked for. `kind >= ExecKind.TRADE` is what
    separates the reports where shares moved from the acknowledgements that
    share the same message, and summing `qty` without that filter counts every
    ack as a fill.

    `order_xh128` is the order's lifecycle, flat and typed, beside the generic
    `parent_h128` list. The list is the truth about lineage; the flat column is
    what a join uses, because no engine under this package joins on an array
    without exploding it first, and an explode of a fills table is a shuffle
    nobody needs to pay for a link that is always single-valued.
    """

    kind: Annotated[ExecKind, fix_tag("ExecType", 150)]
    """What this report says happened; `>= ExecKind.TRADE` means shares moved."""

    exec_id: Annotated[str | None, fix_tag("ExecID", 17)]
    """Identifier the venue gave this report."""

    trade_id: Annotated[str | None, fix_tag("TradeID", 1003)]
    """Identifier the venue gave the trade, which both sides of it share."""

    order_xh128: uuid.UUID | None
    """Lifecycle of the order this happened to -- the join key, single-valued."""

    order_id: Annotated[str | None, fix_tag("OrderID", 37)]
    """Identifier the venue gave that order."""

    client_order_id: Annotated[str | None, fix_tag("ClOrdID", 11)]
    """Identifier the sender gave the version of the order that traded."""

    filled_qty: Annotated[float | None, fix_tag("CumQty", 14)]
    """Quantity done on the order as of this report, including this fill."""

    leaves_qty: Annotated[float | None, fix_tag("LeavesQty", 151)]
    """Quantity still working after this report."""

    avg_px: Annotated[float | None, fix_tag("AvgPx", 6)]
    """Average price of everything done on the order, as of this report."""

    aggressor: Annotated[bool | None, fix_tag("AggressorIndicator", 1057)]
    """Whether this side took liquidity; null when the venue does not say."""

    reason_code: Annotated[
        int | None, fix_tag("ExecRestatementReason", 378), Field(arrow_type=pyarrow.int32())
    ]
    """Why a restatement or a refusal happened, as the venue numbers it."""

    reason: Annotated[str | None, fix_tag("Text", 58)]
    """Free text the venue sent with the report."""
