"""A side of a book, both sides of one, and the derived prices computed in kernels."""

from __future__ import annotations

import uuid
from typing import Annotated, Any, ClassVar, Self

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field, FieldBuilder, field
from rekep.market.enums import EventType, Side, UpdateAction
from rekep.market.event import UNIX, MarketEvent
from rekep.market.fields import MarketFieldBuilder, fix_tag
from rekep.market.identity import NIL
from rekep.market.orders import Execution, Order


@field
class Level(Convertible):
    """One live price level: what is there, and how much of it."""

    FIELD_BUILDER: ClassVar[type[FieldBuilder]] = MarketFieldBuilder

    px: Annotated[float, fix_tag("MDEntryPx", 270)] = 0.0
    """Price of the level."""

    qty: Annotated[float, fix_tag("MDEntrySize", 271)] = 0.0
    """Quantity resting at that price."""

    orders: Annotated[
        int | None, fix_tag("NumberOfOrders", 346), Field(arrow_type=pyarrow.int32())
    ] = None
    """How many orders make up the level; null when the venue does not say."""


@field
class LevelUpdate(Convertible):
    """One change to a level, as an incremental feed sends it.

    `px` and `qty` are nullable here and not on `Level`, because a deletion
    says which level went and not what was in it -- and the ranged deletions
    (`DELETE_THRU`, `DELETE_FROM`) name a boundary rather than a level at all.
    """

    FIELD_BUILDER: ClassVar[type[FieldBuilder]] = MarketFieldBuilder

    action: Annotated[UpdateAction, fix_tag("MDUpdateAction", 279)] = UpdateAction.UNKNOWN
    """What this does to the book; `>= UpdateAction.REMOVE` takes liquidity out."""

    px: Annotated[float | None, fix_tag("MDEntryPx", 270)] = None
    """Price the update applies at."""

    qty: Annotated[float | None, fix_tag("MDEntrySize", 271)] = None
    """Quantity after the update; null on a deletion."""

    orders: Annotated[
        int | None, fix_tag("NumberOfOrders", 346), Field(arrow_type=pyarrow.int32())
    ] = None
    """How many orders make up the level after the update."""

    position: Annotated[
        int | None, fix_tag("MDEntryPositionNo", 290), Field(arrow_type=pyarrow.int32())
    ] = None
    """Depth the update applies at, counting from the top; for venues that index."""


@field
class LevelExecution(Convertible):
    """One trade against this side, as the book saw it.

    Small on purpose: it is the *trace* of an execution against these levels,
    not the execution itself. `xhash` is the link to the row that is, in the
    executions table, with everything the report carried -- putting a whole
    `Execution` here would nest a second event envelope inside every book row
    for the two numbers a book actually needs.
    """

    FIELD_BUILDER: ClassVar[type[FieldBuilder]] = MarketFieldBuilder

    unix: Annotated[int, Field(metadata=UNIX)] = 0
    """When it traded, in whole nanoseconds since the epoch."""

    px: Annotated[float, fix_tag("LastPx", 31)] = 0.0
    """What it traded at."""

    qty: Annotated[float, fix_tag("LastQty", 32)] = 0.0
    """How much traded."""

    xhash: uuid.UUID | None = None
    """Lifecycle of the execution this came from -- the join into that table."""

    aggressor: Annotated[bool | None, fix_tag("AggressorIndicator", 1057)] = None
    """Whether this side was the one taking liquidity."""


@field
class BookSide(MarketEvent):
    """One side of one book: what is live, what changed, and what traded.

    `side` is `BID` or `ASK`, which are the same codes as `BUY` and `SELL`
    because a bid *is* a buy. `px` and `qty` are the **best level's** -- the
    top of this side -- and `total_qty` is every level's.

    **State, delta and trace are all on the row**, and that is the point of the
    shape. A feed sends one or two of the three: a snapshot says what is there,
    an incremental says what changed, a trade stream says what took liquidity
    out. Keeping only snapshots loses what moved; keeping only increments makes
    every reader replay from the last snapshot before it can answer anything;
    keeping neither trades means "was that level filled or pulled?" needs a
    join against another table on a timestamp. Carrying all three means a
    consumer reads state without replaying and reconstructs causation without a
    second stream -- and the cost is three list columns that are null on the
    rows that do not have them.

    A row where `alive` is null is an increment that was never resolved to a
    state; `summarise_arrow` leaves those rows exactly as it found them rather
    than deriving nulls over whatever the producer put there.
    """

    EVENT_TYPE: ClassVar[EventType] = EventType.BOOK_SIDE

    #: What `summarise_arrow` redirects to, keyed by what it was handed.
    SUMMARIES: ClassVar[dict[Any, str]] = {
        pyarrow.RecordBatch: "arrow_batch",
        pyarrow.Table: "arrow_table",
    }

    #: What `append_event` redirects to, keyed by the kind of event.
    APPENDS: ClassVar[dict[Any, str]] = {Order: "order", Execution: "execution"}

    # Declared before the three lists, because Iceberg collects column bounds
    # for the first `write.metadata.metrics.max-inferred-column-defaults`
    # leaves in pre-order and nothing under a list gets bounds at all.
    depth: Annotated[int | None, Field(arrow_type=pyarrow.int32())] = None
    """How many levels are live -- `len(alive)`, flat, so a filter can use it."""

    total_qty: float | None = None
    """Sum of `qty` over every live level."""

    alive: list[Level] | None = None
    """Every live level, best first; null on a row that carries only a delta."""

    updates: list[LevelUpdate] | None = None
    """The changes that produced this version, in the order the venue sent them."""

    executions: list[LevelExecution] | None = None
    """The trades that took liquidity out of this side, in the order they printed."""

    # -- building a side out of events ---------------------------------------

    def append_event(self, event: Any) -> Self:
        """`append_order` or `append_execution`, inferred from what it was handed."""
        return getattr(self, f"append_{self.redirect_of(event, self.APPENDS)}")(event)

    def append_order(self, order: Order) -> Self:
        """Add an order's resting quantity to this side, and record the update.

        **Aggregated, not per order.** The level at `order.px` moves by what
        the order rests for -- `display_qty` if the venue hides part of it,
        `leaves_qty` if it said how much is left, `qty` otherwise, and nothing
        at all once the order is terminal. What this does *not* do is track
        which order contributed what, so replacing an order means appending the
        cancel and then the replacement, not appending the new one twice.

        That is the honest shape of an aggregated book, and it is what a venue
        that publishes levels rather than orders gives you anyway.
        """
        self._facing(order)
        if order.px is None:
            raise ValueError(
                f"order {order.xhash} has no price and so never rests: "
                "a market order is an execution against this side, not a level on it"
            )
        resting = 0.0 if order.state.is_terminal else _resting(order)
        return self._moved(order, order.px, resting)

    def append_execution(self, execution: Execution) -> Self:
        """Take a fill's quantity out of this side, and record the trade.

        Only a report that moved shares changes the book: an acknowledgement
        and a restatement share the same message type, and subtracting their
        quantity is how a book ends up empty by lunchtime.
        """
        self._facing(execution)
        if not execution.kind.moves_shares or execution.px is None or execution.qty is None:
            return self
        traded = LevelExecution(
            unix=execution.unix,
            px=execution.px,
            qty=execution.qty,
            xhash=execution.xhash,
            aggressor=execution.aggressor,
        )
        self.executions = [*(self.executions or []), traded]
        return self._moved(execution, execution.px, -abs(execution.qty))

    def _facing(self, event: MarketEvent) -> None:
        """Refuse an event that does not belong on this side.

        A sell order does not rest on the bid; it *takes* from it, which is an
        execution and a different method. Left unchecked the direction was a
        factor in the arithmetic, so a mismatched order quietly removed
        liquidity instead of being refused -- and an event with no side at all
        moved a level by nothing and recorded a `DELETE` of a level that had
        never existed. Both were silent, and both were wrong.
        """
        if not self.side.sign:
            raise ValueError(
                f"a book side is a bid or an ask, not {self.side.name}: "
                "set `side` before appending anything to it"
            )
        if event.side.sign != self.side.sign:
            raise ValueError(
                f"a {event.side.name} event does not belong on the {self.side.name} side of a book"
            )

    def _moved(self, event: MarketEvent, px: float, delta: float) -> Self:
        """One level moved by `delta`, with the update and the lineage recorded."""
        levels = {level.px: level for level in (self.alive or [])}
        standing = levels.get(px)
        quantity = (standing.qty if standing else 0.0) + delta
        if quantity > 0:
            levels[px] = Level(px=px, qty=quantity, orders=standing.orders if standing else None)
            action = UpdateAction.CHANGE if standing else UpdateAction.NEW
        else:
            levels.pop(px, None)
            action = UpdateAction.DELETE
        self.alive = sorted(levels.values(), key=lambda level: -level.px * self.side.sign)
        self.updates = [
            *(self.updates or []),
            LevelUpdate(
                action=action,
                px=px,
                qty=None if action is UpdateAction.DELETE else quantity,
                orders=None,
                position=None,
            ),
        ]
        return self._versioned(event)

    def _versioned(self, event: MarketEvent) -> Self:
        """The side moved on one version, remembering what moved it.

        Every append is a new version of the same `xhash`, so the lineage a
        reader walks back is the same one whether the side was built here or
        assembled from a feed: `prev_hash` is the version before, `parent_hash`
        gains the event that caused this one, and the derived columns follow.
        """
        self.prev_hash, self.prev_state, self.prev_unix = self.hash, self.state, self.unix
        self.version += 1
        self.unix = max(self.unix, event.unix)
        # Set, never appended to: `parent_hash` is what *this version* was
        # built from, and the version before it is already on the row as
        # `prev_hash`. Accumulating instead put one entry per append on a
        # single row -- two hundred appends, two hundred parents -- which is
        # both unbounded and a different claim from the one the column makes.
        self.parent_hash = [event.hash]
        self.depth = len(self.alive or [])
        self.total_qty = sum(level.qty for level in (self.alive or []))
        best = (self.alive or [None])[0]
        self.px = best.px if best else None
        self.qty = best.qty if best else None
        self.hash = self.hash_of(self.xhash, self.version, self.unix, self.px, self.qty)
        return self

    # -- the derived columns, in kernels -------------------------------------

    @classmethod
    def summarise_arrow(cls, source: Any) -> Any:
        """`summarise_arrow_batch` or `_table`, inferred from what it was handed."""
        return getattr(cls, f"summarise_{cls.redirect_of(source, cls.SUMMARIES)}")(source)

    @classmethod
    def summarise_arrow_batch(cls, batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        """`px`, `qty`, `depth` and `total_qty` recomputed from `alive`.

        Flat columns, derived once by whoever writes the data, because the
        alternative is every reader reaching into a list to ask what the best
        price is -- and a nested access is the one thing no engine under this
        package prunes on. `px > 100` skips files; `alive[0].px > 100` reads
        them all and throws the rows away.
        """
        return _with(batch, **_derived(batch, "alive", ("px", "qty", "depth", "total_qty")))

    @classmethod
    def summarise_arrow_table(cls, table: pyarrow.Table) -> pyarrow.Table:
        """`summarise_arrow_batch` over a whole table, batch by batch."""
        return pyarrow.Table.from_batches(
            [cls.summarise_arrow_batch(batch) for batch in table.to_batches()], schema=table.schema
        )


@field
class Book(MarketEvent):
    """Both sides of one book, flat, and the prices that only exist across them.

    **The sides are unnested.** Each one contributes five scalars and its three
    lists, prefixed `bid_`/`ask_`, rather than a whole `BookSide` event nested
    inside. A nested side put a second and third copy of the fifteen-column
    event envelope in every book row -- and, worse, pushed this shape to 140
    leaf columns, past the 100 that Iceberg collects bounds for. `bid_hash` and
    `ask_hash` keep the provenance a nested side was carrying: which exact
    version of each side this book was built from, which is also what
    `parent_hash` holds.

    `side` is `UNKNOWN`: a book does not have one. `px` is the **mid** and
    `qty` is the size at the touch, so the abstract slots stay meaningful
    rather than being null on every row of the largest table here.

    **The flat pair `(px, spread)` is the best bid and offer**, exactly and
    without duplicating them: bid is `px - spread / 2`, ask is `px +
    spread / 2`. That is also why there is no `crossed` flag -- `spread < 0`
    is crossed, `spread == 0` is locked, and both are one range predicate on a
    column an engine already has statistics for.

    Flat, rather than reached through a nested side, because a nested price is
    not a filter anything here can use. Iceberg writes **no bounds at all for a
    field under a list or a map**, so nothing about a level in `bid_alive` can
    ever skip a file; Doris pushes a predicate down only for a top-level scalar
    column. Deriving these once at write time is what makes the same query
    cheap on all three engines instead of only on Spark.
    """

    EVENT_TYPE: ClassVar[EventType] = EventType.BOOK

    #: What `summarise_arrow` redirects to, keyed by what it was handed.
    SUMMARIES: ClassVar[dict[Any, str]] = {
        pyarrow.RecordBatch: "arrow_batch",
        pyarrow.Table: "arrow_table",
    }

    #: What `append_event` redirects to, keyed by the kind of event.
    APPENDS: ClassVar[dict[Any, str]] = {Order: "order", Execution: "execution"}

    spread: float | None = None
    """`ask_px - bid_px`; negative is crossed, zero is locked."""

    # The size-weighted price, which is the one number that says where the book
    # thinks the instrument is when the two sides are not the same size. Each
    # price is weighted by the *opposite* side's quantity, because a large bid
    # against a small offer means the next trade is likelier at the offer.
    micro_px: float | None = None
    """Microprice: `(bid_px * ask_qty + ask_px * bid_qty) / (bid_qty + ask_qty)`."""

    imbalance: float | None = None
    """`(bid_qty - ask_qty) / (bid_qty + ask_qty)`, in `[-1, 1]`; positive is bid-heavy."""

    bid_hash: uuid.UUID | None = None
    """Which version of the buy side this book was built from."""

    bid_px: float | None = None
    """Best bid; also `px - spread / 2`."""

    bid_qty: float | None = None
    """Quantity at the best bid."""

    bid_depth: Annotated[int | None, Field(arrow_type=pyarrow.int32())] = None
    """How many levels are live on the buy side."""

    bid_total_qty: float | None = None
    """Sum of `qty` over every live buy level."""

    ask_hash: uuid.UUID | None = None
    """Which version of the sell side this book was built from."""

    ask_px: float | None = None
    """Best offer; also `px + spread / 2`."""

    ask_qty: float | None = None
    """Quantity at the best offer."""

    ask_depth: Annotated[int | None, Field(arrow_type=pyarrow.int32())] = None
    """How many levels are live on the sell side."""

    ask_total_qty: float | None = None
    """Sum of `qty` over every live sell level."""

    # Every list last, together: nothing under one of them carries statistics,
    # so their position costs nothing, and keeping them out of the way leaves
    # every scalar above inside the bounds budget.
    bid_alive: list[Level] | None = None
    """Every live buy level, best first."""

    bid_updates: list[LevelUpdate] | None = None
    """The changes that produced this version of the buy side."""

    bid_executions: list[LevelExecution] | None = None
    """The trades that took liquidity out of the buy side."""

    ask_alive: list[Level] | None = None
    """Every live sell level, best first."""

    ask_updates: list[LevelUpdate] | None = None
    """The changes that produced this version of the sell side."""

    ask_executions: list[LevelExecution] | None = None
    """The trades that took liquidity out of the sell side."""

    # -- building a book out of events ---------------------------------------

    def append_event(self, event: Any) -> Self:
        """`append_order` or `append_execution`, inferred from what it was handed."""
        return getattr(self, f"append_{self.redirect_of(event, self.APPENDS)}")(event)

    def append_order(self, order: Order) -> Self:
        """Add an order to whichever side it belongs on."""
        return self._through(order, "append_order")

    def append_execution(self, execution: Execution) -> Self:
        """Take a fill out of whichever side it hit."""
        return self._through(execution, "append_execution")

    def _through(self, event: MarketEvent, method: str) -> Self:
        """Route `event` to its side, apply it there, and read the side back flat.

        The sides are columns here and a `BookSide` there, so routing goes
        through the real thing rather than through a second implementation of
        what an order does to a level: `into_side` lifts the flat columns into
        the side they came from, the side's own method moves it, and
        `from_side` puts the result back. One walk, one set of rules, whichever
        shape is holding them.
        """
        if not event.side.sign:
            raise ValueError(
                f"a {event.side.name} event names no side of the book to go on: "
                "set `side` on it, or append it to the side you mean directly"
            )
        name = "bid" if event.side.sign > 0 else "ask"
        return self.from_side(name, getattr(self.into_side(name), method)(event))

    # -- the sides, lifted and put back --------------------------------------

    def into_side(self, name: str) -> BookSide:
        """One side of this book as the `BookSide` its columns describe."""
        if name not in ("bid", "ask"):
            raise ValueError(f"a book has a bid and an ask, not a {name!r}")
        given = {
            "etype": EventType.BOOK_SIDE,
            "side": Side.BID if name == "bid" else Side.ASK,
            # A side has a lifecycle of its own, derived from the book's so it
            # is stable and reproducible without being shared: lifting the bid
            # and the ask out of one book gave both the book's own `xhash`,
            # which made `bid_hash` and `ask_hash` versions of the same thing.
            "xhash": BookSide.hash_of(self.xhash, name),
            "hash": getattr(self, f"{name}_hash") or NIL,
        } | {
            column: getattr(self, f"{name}_{column}")
            for column in ("px", "qty", "depth", "total_qty", "alive", "updates", "executions")
        }
        # Everything else is the book's own envelope, copied by name rather
        # than listed here, so a column added to `MarketEvent` reaches the side
        # without this method changing.
        envelope = {
            member.name: getattr(self, member.name)
            for member in MarketEvent.FIELD.fields
            if member.name not in given
        }
        return BookSide(**envelope, **given)

    def from_side(self, name: str, side: BookSide) -> Self:
        """`side` written back into this book's flat columns, and the book re-priced."""
        for column in ("hash", "px", "qty", "depth", "total_qty", "alive", "updates", "executions"):
            setattr(self, f"{name}_{column}", getattr(side, column))
        self.prev_hash, self.prev_state, self.prev_unix = self.hash, self.state, self.unix
        self.version += 1
        self.unix = max(self.unix, side.unix)
        # The side this version was built from, and only that -- the reasoning
        # is on `BookSide._versioned`. The other side is unchanged and is
        # already named by the `bid_hash`/`ask_hash` pair.
        self.parent_hash = [side.hash]
        self._priced()
        self.hash = self.hash_of(self.xhash, self.version, self.unix, self.px, self.spread)
        return self

    def _priced(self) -> None:
        """The five prices across the sides, for one book rather than a column of them."""
        bid_px, bid_qty = self.bid_px, self.bid_qty
        ask_px, ask_qty = self.ask_px, self.ask_qty
        self.px = None if bid_px is None or ask_px is None else (bid_px + ask_px) / 2
        self.spread = None if bid_px is None or ask_px is None else ask_px - bid_px
        size = None if bid_qty is None or ask_qty is None else bid_qty + ask_qty
        self.qty = size
        if not size:
            self.micro_px = self.imbalance = None
            return
        self.micro_px = (
            None
            if bid_px is None or ask_px is None
            else (bid_px * ask_qty + ask_px * bid_qty) / size
        )
        self.imbalance = (bid_qty - ask_qty) / size

    # -- the derived columns, in kernels -------------------------------------

    @classmethod
    def summarise_arrow(cls, source: Any) -> Any:
        """`summarise_arrow_batch` or `_table`, inferred from what it was handed."""
        return getattr(cls, f"summarise_{cls.redirect_of(source, cls.SUMMARIES)}")(source)

    @classmethod
    def summarise_arrow_batch(cls, batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        """Each side derived from its own levels, then the prices across them.

        **Both halves, in that order**, because the second needs the first: a
        book assembled from two feeds carries levels and nothing derived, so
        reading `bid_px` before deriving it gives null and every price across
        the sides comes out null with it. Doing only the cross-side half was
        the first version of this, and the benchmark caught it by deriving a
        batch of books into a column of nulls.

        Everything is kernels over whole columns, and no row is looked at in
        Python. A one-sided book derives to null everywhere a price is
        missing, which is what a mid across a side that is not there means;
        the division guards on the size rather than letting Arrow return an
        infinity, because an infinite microprice is a number and a null is the
        truth.
        """
        compute = pyarrow.compute
        for name in ("bid", "ask"):
            batch = _with(
                batch,
                **_derived(
                    batch,
                    f"{name}_alive",
                    (f"{name}_px", f"{name}_qty", f"{name}_depth", f"{name}_total_qty"),
                ),
            )
        bid_px, bid_qty = batch.column("bid_px"), batch.column("bid_qty")
        ask_px, ask_qty = batch.column("ask_px"), batch.column("ask_qty")
        size = compute.add(bid_qty, ask_qty)
        weighted = compute.greater(size, 0)
        return _with(
            batch,
            px=compute.divide(compute.add(bid_px, ask_px), pyarrow.scalar(2.0)),
            qty=size,
            spread=compute.subtract(ask_px, bid_px),
            micro_px=compute.if_else(
                weighted,
                compute.divide(
                    compute.add(
                        compute.multiply(bid_px, ask_qty), compute.multiply(ask_px, bid_qty)
                    ),
                    size,
                ),
                pyarrow.scalar(None, pyarrow.float64()),
            ),
            imbalance=compute.if_else(
                weighted,
                compute.divide(compute.subtract(bid_qty, ask_qty), size),
                pyarrow.scalar(None, pyarrow.float64()),
            ),
        )

    @classmethod
    def summarise_arrow_table(cls, table: pyarrow.Table) -> pyarrow.Table:
        """`summarise_arrow_batch` over a whole table, batch by batch."""
        return pyarrow.Table.from_batches(
            [cls.summarise_arrow_batch(batch) for batch in table.to_batches()], schema=table.schema
        )


# -- helpers ----------------------------------------------------------------


def _resting(order: Order) -> float:
    """What an order actually sits in the book for.

    The venue's own numbers in the order it knows them: what it shows
    (`display_qty`) is what the book sees of an iceberg, what is left
    (`leaves_qty`) is what a partly filled order still rests for, and the
    quantity asked for is what a fresh one does.
    """
    for quantity in (order.display_qty, order.leaves_qty, order.qty):
        if quantity is not None:
            return float(quantity)
    return 0.0


def _combined(column: Any) -> Any:
    """A column as one Array, because the offsets of a chunk index its own child."""
    return column.combine_chunks() if isinstance(column, pyarrow.ChunkedArray) else column


def _with(batch: Any, **columns: Any) -> Any:
    """`batch` with each named column replaced, its declared type and comment kept."""
    for name, column in columns.items():
        index = batch.schema.get_field_index(name)
        declared = batch.schema.field(index)
        batch = batch.set_column(index, declared, column.cast(declared.type, safe=False))
    return batch


def _derived(batch: Any, levels: str, into: tuple[str, str, str, str]) -> dict[str, Any]:
    """The four columns a list of levels determines: best price, best size, depth, total.

    One walk, whichever shape asked for it -- a `BookSide` deriving its own
    columns and a `Book` deriving each of its two sides both come through here,
    so there is one set of rules about empty lists and null ones rather than
    two that drift.

    Two things it does not do, both of which are how a first version got this
    wrong:

    - It never calls `list_element`, which raises `Index 0 is out of bounds` on
      an empty list -- and an empty side of a book is a normal Tuesday, not an
      error. The best row of each list is taken by its own offset instead,
      which is null exactly where the list is empty.
    - It never sums by prefix. A running total over the whole batch minus
      another running total gives the right answer only until the totals get
      big, and a book's own quantities are small differences of large ones by
      then. The sum is grouped, which is exact.
    """
    compute = pyarrow.compute
    alive = _combined(batch.column(levels))
    known = compute.is_valid(alive)
    lengths = compute.list_value_length(alive)
    top = compute.if_else(
        compute.greater(lengths, 0), alive.offsets[:-1], pyarrow.scalar(None, pyarrow.int32())
    )
    members = alive.values
    best_px, best_qty, depth, total = into
    return {
        best_px: compute.if_else(
            known, compute.take(members.field("px"), top), batch.column(best_px)
        ),
        best_qty: compute.if_else(
            known, compute.take(members.field("qty"), top), batch.column(best_qty)
        ),
        depth: compute.if_else(known, lengths, batch.column(depth)),
        total: compute.if_else(known, _list_sums(alive, "qty", lengths), batch.column(total)),
    }


def _list_sums(alive: Any, name: str, lengths: Any) -> Any:
    """One exact sum per list, grouped rather than differenced.

    `list_parent_indices` says which row each flattened level belongs to, so
    the whole batch is one hash aggregate; the result is scattered back by
    `index_in`, and a list that is empty sums to zero rather than to null --
    a side with no levels holds nothing, which is a quantity and not a
    question.

    The row identifiers are built with `repeat` and `cumulative_sum` rather
    than from a Python `range`, so nothing here allocates per row.
    """
    compute = pyarrow.compute
    rows = len(alive)
    totals = (
        pyarrow.table(
            {
                "row": compute.list_parent_indices(alive),
                "value": compute.list_flatten(alive).field(name),
            }
        )
        .group_by("row")
        .aggregate([("value", "sum")])
    )
    identifiers = compute.subtract(
        compute.cumulative_sum(pyarrow.repeat(pyarrow.scalar(1, pyarrow.int64()), rows)), 1
    )
    where = compute.index_in(identifiers, value_set=totals.column("row").combine_chunks())
    summed = compute.take(totals.column("value_sum").combine_chunks(), where)
    return compute.if_else(compute.equal(lengths, 0), pyarrow.scalar(0.0), summed)
