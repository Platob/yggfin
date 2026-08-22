"""A side of a book, both sides of one, and the derived prices computed in kernels."""

from __future__ import annotations

import bisect
import copy
import dataclasses
from collections.abc import Iterable, Iterator
from typing import Annotated, Any, ClassVar, Self

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field, FieldBuilder, field
from rekep.market.enums import EventType, Side, State, UpdateAction
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

    xhash: int | None = None
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

    # Re-declared for their real FIX field, and for nothing else: a dataclass
    # member re-annotated keeps its position, so the column does not move.
    # `MarketEvent` tags them `Price <44>`/`OrderQty <38>`, which is an
    # *order's* price and quantity; a book level's are a market-data entry's.
    px: Annotated[float | None, fix_tag("MDEntryPx", 270)] = None
    """The best level's price on this side -- the top of the book, this way up."""

    qty: Annotated[float | None, fix_tag("MDEntrySize", 271)] = None
    """The size at that best level; `total_qty` is every level's."""

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

    def append_event(self, event: Any) -> Self | None:
        """`append_order` or `append_execution`, inferred from what it was handed."""
        return getattr(self, f"append_{self.redirect_of(event, self.APPENDS)}")(event)

    def append_order(self, order: Order) -> Self | None:
        """Add an order's resting quantity to this side, and record the update.

        **Aggregated, not per order.** The level at `order.px` moves by what
        the order rests for -- `display_qty` if the venue hides part of it,
        `leaves_qty` if it said how much is left, `qty` otherwise, and nothing
        at all once the order is terminal. What this does *not* do is track
        which order contributed what, so replacing an order means appending the
        cancel and then the replacement, not appending the new one twice.

        That is the honest shape of an aggregated book, and it is what a venue
        that publishes levels rather than orders gives you anyway.

        **None when nothing moved**, and the side is left exactly as it was --
        no version, no update, no new hash. An order that rests for nothing is
        the common case of that: a terminal one, or one the venue sent with no
        quantity at all. A caller that writes what it gets back therefore
        writes one row per real change rather than one per message, which is
        the difference between a book table and a copy of the feed.
        """
        self._facing(order)
        if order.px is None:
            raise ValueError(
                f"order {order.xhash} has no price and so never rests: "
                "a market order is an execution against this side, not a level on it"
            )
        resting = 0.0 if order.state.is_terminal else _resting(order)
        return self._moved(order, order.px, resting)

    def append_execution(self, execution: Execution) -> Self | None:
        """Take a fill's quantity out of this side, and record the trade.

        Only a report that moved shares changes the book: an acknowledgement
        and a restatement share the same message type, and subtracting their
        quantity is how a book ends up empty by lunchtime. Anything that moved
        none of them returns None, having changed nothing.
        """
        self._facing(execution)
        if not execution.kind.moves_shares or execution.px is None or execution.qty is None:
            return None
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

    def _moved(self, event: MarketEvent, px: float, delta: float) -> Self | None:
        """One level moved by `delta`, with the update and the lineage recorded.

        None when `delta` moves nothing that is there: a zero delta on a level
        the side does not hold is a message about liquidity that was already
        gone, and versioning the side for it would write a row that differs
        from the one before it only in its hash.
        """
        levels = {level.px: level for level in (self.alive or [])}
        if not delta and px not in levels:
            return None
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
        # `NIL` means nothing hashed this yet, so the first version replaced
        # nothing and says so with a null rather than with sixteen zero bytes
        # that a reader would have to know to recognise.
        self.prev_hash = None if self.hash == NIL else self.hash
        self.prev_state, self.prev_unix = self.state, self.unix
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

    # Re-declared to carry **no** FIX tag, which is the honest thing to say:
    # a mid and a touch size are computed from two sides, and FIX has no field
    # for either. The inherited `Price <44>` would label them as an order's
    # limit, which is a schema claiming a provenance it does not have.
    px: float | None = None
    """The mid, `(bid_px + ask_px) / 2`; null until both sides have a price."""

    qty: float | None = None
    """The size at the touch, `bid_qty + ask_qty`; null until both sides have one."""

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

    bid_hash: int | None = None
    """Which version of the buy side this book was built from."""

    bid_px: float | None = None
    """Best bid; also `px - spread / 2`."""

    bid_qty: float | None = None
    """Quantity at the best bid."""

    bid_depth: Annotated[int | None, Field(arrow_type=pyarrow.int32())] = None
    """How many levels are live on the buy side."""

    bid_total_qty: float | None = None
    """Sum of `qty` over every live buy level."""

    ask_hash: int | None = None
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

    @classmethod
    def from_events(cls, events: Iterable[MarketEvent]) -> Iterator[Self]:
        """One book per instant that changed it, folded from **one instrument's** stream.

        The input is orders and executions in time order, for a single
        instrument, and the output is the book after each instant that moved
        it -- one row per instant, never one per message, because several
        events at the same nanosecond are one state of the book and writing
        three rows with the same `unix` is writing the feed rather than the
        book.

        **One instrument, and it is checked.** A stream carrying two of them
        folds into a book that is neither, silently and forever; that is the
        kind of wrong that a partition exists to prevent, and
        `instrument_hash` is `bucket[16]` for exactly this reason. Read a
        partition and hand it here.

        **Sorted, and that is checked too.** A fold is a fold: an event out of
        order asks the book to un-happen something, and there is no honest
        answer. Iceberg keeps these tables sorted by `unix` for the same
        reason, so a partition read back is already in the order this wants.

        What is kept between events is the **live orders**, not the levels: a
        venue that restates an order has to replace what that order was
        resting for, and a level cannot say which order contributed what.
        `Resting` holds them, sorted by price then by descending quantity,
        which is what makes the best bid and offer a read rather than a scan.
        """
        bid, ask = Resting(side=Side.BID), Resting(side=Side.ASK)
        instrument, unix, previous, about = None, None, None, None
        moved = False
        for event in events:
            if instrument is None:
                instrument = event.instrument_hash
            elif event.instrument_hash != instrument:
                raise ValueError(
                    f"a book is one instrument's: got {event.instrument_hash} after "
                    f"{instrument}. Partition the stream on `instrument_hash` first"
                )
            if unix is not None and event.unix < unix:
                raise ValueError(
                    f"a book is folded in time order: {event.unix} came after {unix}. "
                    "Sort the stream on `unix` first"
                )
            if unix is not None and event.unix != unix and moved:
                previous = cls._settled(bid, ask, unix, about, previous)
                yield previous
                moved = False
            unix = event.unix
            moved = cls._folded(bid, ask, event) or moved
            # After folding, never before: a book is described by the events
            # it holds, and reading the event that *triggered* the yield gave
            # every row the units of the instant after it.
            about = event
        if moved and unix is not None and about is not None:
            yield cls._settled(bid, ask, unix, about, previous)

    @classmethod
    def _folded(cls, bid: Resting, ask: Resting, event: MarketEvent) -> bool:
        """One event applied to the side it belongs on. True when the book moved.

        A shape that is itself a book is skipped rather than refused: a stream
        read off a table may carry the book rows this produced beside the
        orders that produced them, and folding a book into a book is not a
        thing that means anything.
        """
        if event.is_order():
            side = bid if event.side.sign > 0 else ask if event.side.sign < 0 else None
            # The price is *not* checked here: a cancellation carries none, and
            # gets one from the version it cancels. `Resting.apply` decides,
            # after completing.
            return False if side is None else side.apply(event)
        if event.is_execution():
            return cls._traded(bid, ask, event)
        return False

    @classmethod
    def _traded(cls, bid: Resting, ask: Resting, execution: Execution) -> bool:
        """A fill taken out of the side it hit, if this report moved shares at all."""
        if not execution.kind.moves_shares or execution.px is None or execution.qty is None:
            return False
        side = cls._hit(bid, ask, execution)
        if side is None:
            return False
        side.take(execution, abs(execution.qty))
        return True

    @classmethod
    def _hit(cls, bid: Resting, ask: Resting, execution: Execution) -> Resting | None:
        """Which side a fill took liquidity out of, by what the report actually says.

        Three readings, strongest first, because a feed gives different ones:

        1. **The report's own side.** An execution's `side` is the side of the
           order it reports, and a filled buy order was resting on the bid --
           so its liquidity leaves the bid. Exact whenever a venue sends it.
        2. **The order it names.** A report with no side but an
           `order_xhash` that is live on one side names that side.
        3. **Its price against the touch.** A market-data trade print carries
           neither, which is most prints: a trade at or below the mid took
           from the bid, above it from the ask. That is the tick rule, and it
           is the honest answer when the venue has not given a better one.

        None when the book is empty, which is a print against liquidity this
        fold never saw -- there is nothing to take it out of.
        """
        if execution.side.sign > 0:
            return bid
        if execution.side.sign < 0:
            return ask
        named = execution.order_xhash
        if named:
            if named in bid.orders:
                return bid
            if named in ask.orders:
                return ask
        best_bid, best_ask = bid.best, ask.best
        if best_bid is None and best_ask is None:
            return None
        if best_bid is None:
            return ask
        if best_ask is None:
            return bid
        mid = ((best_bid.px or 0.0) + (best_ask.px or 0.0)) / 2
        return bid if (execution.px or 0.0) <= mid else ask

    @classmethod
    def _settled(
        cls, bid: Resting, ask: Resting, unix: int, about: MarketEvent, previous: Self | None
    ) -> Self:
        """The book as it stands, as a new row -- and the delta handed over with it.

        A **new** `Book` per instant and never the same one mutated: these are
        versions, and a caller collecting them into a batch would otherwise
        get one object repeated. `with_previous` does the versioning, so a
        book folded here carries the same `prev_hash`/`prev_state` chain as
        one assembled by `append_event`.

        `about` is the last event folded in, and it is where the book learns
        what it is a book *of*: the instrument, the symbol and the units are
        the stream's, not something a book row could work out for itself.
        """
        book = cls(
            unix=unix,
            instrument=about.instrument,
            instrument_hash=about.instrument_hash,
            symbol=about.symbol,
            px_unit=about.px_unit,
            qty_unit=about.qty_unit,
            venue=about.venue,
            state=State.OPEN if (bid.orders or ask.orders) else State.CLOSED,
        )
        # The lifecycle only, and not `identify`: the content hash is derived
        # at the end of `with_previous` below, once the sides are on the row,
        # and computing it here as well hashed every book twice.
        book.xhash = book.life_hash()
        parents = []
        for name, resting in (("bid", bid), ("ask", ask)):
            side = resting.into_side(unix, BookSide.hash_of(book.xhash, name)).identify()
            for column in ("hash", "px", "qty", "depth", "total_qty"):
                setattr(book, f"{name}_{column}", getattr(side, column))
            for column in ("alive", "updates", "executions"):
                setattr(book, f"{name}_{column}", getattr(side, column))
            parents.append(side.hash)
            resting.cleared()
        book.parent_hash = parents
        # The prices across the sides are `Book.derive`'s, which
        # `with_previous` runs once every layer has filled -- so they are not
        # computed here as well, and the content hash it ends with is of a row
        # that already has them.
        return book.with_previous(previous)

    def append_event(self, event: Any) -> Self | None:
        """`append_order` or `append_execution`, inferred from what it was handed."""
        return getattr(self, f"append_{self.redirect_of(event, self.APPENDS)}")(event)

    def append_order(self, order: Order) -> Self | None:
        """Add an order to whichever side it belongs on; None if nothing moved."""
        return self._through(order, "append_order")

    def append_execution(self, execution: Execution) -> Self | None:
        """Take a fill out of whichever side it hit; None if nothing moved."""
        return self._through(execution, "append_execution")

    def _through(self, event: MarketEvent, method: str) -> Self | None:
        """Route `event` to its side, apply it there, and read the side back flat.

        The sides are columns here and a `BookSide` there, so routing goes
        through the real thing rather than through a second implementation of
        what an order does to a level: `into_side` lifts the flat columns into
        the side they came from, the side's own method moves it, and
        `from_side` puts the result back. One walk, one set of rules, whichever
        shape is holding them.

        A side that says nothing moved stops here: the book is not versioned,
        the other side is not touched, and the caller gets None. Passing it on
        would write a book row whose only difference from the last one is that
        a message arrived.
        """
        if not event.side.sign:
            raise ValueError(
                f"a {event.side.name} event names no side of the book to go on: "
                "set `side` on it, or append it to the side you mean directly"
            )
        name = "bid" if event.side.sign > 0 else "ask"
        moved = getattr(self.into_side(name), method)(event)
        return None if moved is None else self.from_side(name, moved)

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
        # `NIL` means nothing hashed this yet, so the first version replaced
        # nothing and says so with a null rather than with sixteen zero bytes
        # that a reader would have to know to recognise.
        self.prev_hash = None if self.hash == NIL else self.hash
        self.prev_state, self.prev_unix = self.state, self.unix
        self.version += 1
        self.unix = max(self.unix, side.unix)
        # The side this version was built from, and only that -- the reasoning
        # is on `BookSide._versioned`. The other side is unchanged and is
        # already named by the `bid_hash`/`ask_hash` pair.
        self.parent_hash = [side.hash]
        self._priced()
        self.hash = self.hash_of(self.xhash, self.version, self.unix, self.px, self.spread)
        return self

    def derive(self) -> None:
        """A book's prices are computed across its sides, and never carried.

        The one place a book has to override the completion rules: `px` and
        `qty` are abstract slots that `MarketEvent.complete_from` fills from
        the previous version of the same shape, which is right for an order
        whose limit the venue stopped repeating and wrong for a mid. A book
        whose ask has just emptied has *no* mid, and inheriting the last one
        makes a one-sided market look two-sided for as long as it lasts.
        """
        self._priced()
        super().derive()

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


# -- folding a book out of one instrument's events ---------------------------


class _Level:
    """One price level: what stands at it, and where it sorts.

    A slotted class and not a tuple or a list of floats, because it is written
    on every order and read on every snapshot: a slot read is an offset where
    a dict probe is a hash, and a named field is legible where `totals[1]` is
    not. Not a `@field` class either -- nothing here is ever a column, which
    is what `Level` is for.
    """

    __slots__ = ("px", "key", "qty", "members", "frozen")

    def __init__(self, px: float, key: float) -> None:
        self.px = px
        """The price itself, as the venue quoted it."""
        self.key = key
        """`px` turned the way this side sorts, which is what `keys` holds."""
        self.qty = 0.0
        """Everything resting here, kept as a running total."""
        self.members: dict[int, None] = {}
        """Which orders are here, by lifecycle, in the order they arrived.

        A dict and not a list, because an order leaves a level as often as it
        joins one and a list removal is a scan. Insertion-ordered, which is
        what makes "the order a venue would have filled them in" a real answer
        for the orders whose quantity ties.
        """
        self.frozen: Level | None = None
        """This level as a row carries it, built once and shared until it moves.

        A snapshot of a two hundred level book built two hundred `Level`
        objects, and one order moving one level rebuilt all two hundred: at
        0.52 us each that was 104 us of `into_levels`' 125 us. A level that did
        not move rebuilds to an object equal to the one before it, so it hands
        back the same one -- and nothing mutates a `Level` once it is on a row,
        which is what makes sharing it across rows sound.
        """

    def into_level(self) -> Level:
        """This level as a row carries it, from the cache when it has not moved."""
        if self.frozen is None:
            self.frozen = Level(px=self.px, qty=self.qty, orders=len(self.members))
        return self.frozen


@dataclasses.dataclass
class Resting:
    """Every order standing on one side of a book, kept in best-first order.

    The state a fold needs and a `BookSide` row does not carry: which orders
    are live, so that a restatement replaces what that order was resting for
    rather than adding to it. `BookSide.append_order` is aggregated by design
    -- it moves a level by a delta and keeps no idea which order contributed
    what -- which is right for a venue that publishes levels and wrong for one
    that publishes orders. This is the other half.

    **The order is the structure, not a sort.** `keys` is the price of every
    live level, kept sorted by `bisect.insort` as levels come and go, so the
    best is `keys[0]` and a snapshot is one walk. What it replaced was a
    `sorted()` per snapshot and a `min()` over every live order per read of
    the touch, which is the difference between a structure that costs the
    depth and one that costs the book:

    | live orders | `best` before | `best` now |
    | --- | --- | --- |
    | 50 | 23 us | ~0.5 us |
    | 2,000 | 1,030 us | ~0.5 us |
    | 8,000 | 4,350 us | ~0.5 us |

    Sorted **by price, then by descending quantity**: price first because that
    is what a book is, size second because at one price the larger interest is
    the one a taker meets first on most venues. The second key is applied
    where it is asked for -- inside one level, over that level's own members
    -- and never over every order at once, which is what made it free.

    A bid sorts down and an ask up, and neither branches: `facing` is `-1` on
    a bid and `+1` on an ask, and `key = px * facing` is ascending on both.
    Multiplying by a sign is exact in binary floating point, so the price
    comes back out of the key with nothing lost.
    """

    side: Side
    """Which side this is; its sign is what turns the sort around."""

    orders: dict[int, Order] = dataclasses.field(default_factory=dict)
    """Every live order, by lifecycle, in no particular order."""

    named: dict[str, int] = dataclasses.field(default_factory=dict)
    """Which lifecycle each venue or client identifier belongs to."""

    levels: dict[float, _Level] = dataclasses.field(default_factory=dict)
    """Every live level, by price. Derived state, kept as the orders move.

    Incrementally on purpose: a book is snapshotted after every instant that
    changed it, and re-aggregating two hundred orders per snapshot is the same
    arithmetic over and over for one order's worth of change.
    """

    keys: list[float] = dataclasses.field(default_factory=list)
    """`level.key` for every live level, **sorted**, best first.

    The whole point of the structure. Maintained by `bisect.insort` on an
    insert and by one `pop` on a delete -- both a `memmove` over a list of
    floats, which at any real depth is faster than the comparisons a sort
    would do.
    """

    total_qty: float = 0.0
    """Everything resting on this side, running rather than summed per snapshot."""

    updates: list[LevelUpdate] = dataclasses.field(default_factory=list)
    """What has changed since the last book was yielded."""

    executions: list[LevelExecution] = dataclasses.field(default_factory=list)
    """What has traded against this side since the last book was yielded."""

    def __post_init__(self) -> None:
        """Read the side's direction once: it is used on every order."""
        self.facing = -self.side.sign

    # -- what the orders come to --------------------------------------------

    @property
    def sign(self) -> int:
        """`+1` on a bid and `-1` on an ask -- what turns the sort around."""
        return self.side.sign

    @property
    def depth(self) -> int:
        """How many live levels this side has."""
        return len(self.keys)

    @property
    def best_level(self) -> _Level | None:
        """The level at the touch, or None when nothing is resting. O(1)."""
        return self.levels[self.keys[0] * self.facing] if self.keys else None

    @property
    def best(self) -> Order | None:
        """The order at the touch, or None when nothing is resting.

        The largest at the best price, which is the second key of the
        ordering -- read over that level's own members and never over every
        live order, which is what a `min()` across the book used to cost.
        """
        level = self.best_level
        if level is None:
            return None
        return max((self.orders[x] for x in level.members), key=_resting, default=None)

    @property
    def sorted_orders(self) -> list[Order]:
        """Every live order, best first: by price, then by descending quantity.

        A walk of `keys` and, inside each level, of its members -- so the only
        sorting left is per level, over the handful of orders standing at one
        price. Kept because it is the honest reading of "the orders, in order",
        and because `take` walks it when a fill names no order.
        """
        found: list[Order] = []
        facing = self.facing
        for key in self.keys:
            members = self.levels[key * facing].members
            if len(members) == 1:
                found.append(self.orders[next(iter(members))])
                continue
            found.extend(sorted((self.orders[x] for x in members), key=_resting, reverse=True))
        return found

    def into_levels(self) -> list[Level]:
        """The live orders aggregated to price levels, best first.

        Which is what a book row publishes: a level is every order at one
        price, and `orders` is how many of them there are -- the one number an
        aggregated feed cannot give you and an order-by-order fold can.

        One walk of `keys`, in the order they are already in. No sort, no
        aggregation, and no rebuilding: the totals were kept as the orders
        moved, and a level that did not move hands back the object it handed
        back last time.
        """
        levels = self.levels
        facing = self.facing
        return [levels[key * facing].into_level() for key in self.keys]

    def into_side(self, unix: int, xhash: int) -> BookSide:
        """This side as the `BookSide` a book row carries flat."""
        best = self.best_level
        return BookSide(
            unix=unix,
            xhash=xhash,
            side=self.side,
            state=State.OPEN if best is not None else State.CLOSED,
            px=best.px if best is not None else None,
            qty=best.qty if best is not None else None,
            depth=len(self.keys),
            total_qty=self.total_qty,
            alive=self.into_levels(),
            updates=list(self.updates),
            executions=list(self.executions),
        )

    def cleared(self) -> None:
        """Forget the delta, keeping the state. Called once a book has been yielded."""
        self.updates.clear()
        self.executions.clear()

    # -- moving it ----------------------------------------------------------

    def apply(self, order: Order) -> bool:
        """One order's latest version, put where it belongs. True if the side moved.

        The order is completed from the version this side already holds, which
        is what makes a venue's restatement usable: a report that says only
        "partially filled, 4 done" arrives here with no price, and leaves with
        the price the order has had all along.

        An order that ends up resting for nothing is removed rather than kept
        at zero -- a level of zero is not a level, and `alive` is what is
        alive. False when nothing about the side changed.
        """
        standing = self.standing(order)
        # A copy, because what is stored here outlives the call and is
        # mutated by it -- `_reduce` writes a new `leaves_qty` onto a resting
        # order when a trade takes part of it. Folding the caller's own object
        # would edit an event it still holds, and replaying one stream twice
        # would give two different books.
        #
        # `copy.copy` and not `dataclasses.replace`, which re-runs `__init__`
        # and `__post_init__` over forty fields to produce the same object:
        # 11.1 us against 3.3 us, and nothing here needs the re-run because
        # `completed_from` sets everything that would change. The copy shares
        # the caller's `metadata` and `instrument`, which nothing in a fold
        # writes to, and `parent_hash` is assigned rather than appended to.
        #
        # `completed_from` and not `with_previous`: a fold does not publish
        # order rows, so it does not need their content hashes, and hashing
        # every version of every live order was half the per-order cost.
        settled = copy.copy(order).completed_from(standing)
        if settled.px is None:
            # No price even after completing: a market order, which rests
            # nowhere -- it is an execution against a side, not a level on
            # it. Skipped rather than refused, because an order stream really
            # does carry them and one of them is not a reason to abandon the
            # book. `BookSide.append_order` raises instead, because there a
            # caller named the side and meant it.
            return False
        before = _resting(standing) if standing else 0.0
        after = 0.0 if settled.state.is_terminal else _resting(settled)
        if standing is not None:
            self._leave(standing)
            if standing.xhash != settled.xhash:
                # Completing gave this version an identity the standing one
                # did not have -- a venue filled in, an id learnt -- and it
                # was found by name rather than by lifecycle. One order is one
                # entry, so the identity it had goes with the level it left.
                self._forget(standing.xhash)
        if after <= 0:
            self._forget(settled.xhash)
        else:
            self.orders[settled.xhash] = settled
            self._join(settled, after)
            for spelling in (settled.order_id, settled.client_order_id):
                if spelling:
                    self.named[spelling] = settled.xhash
        moved = after - before
        if not moved and (standing is None or standing.px == settled.px):
            return False
        self.updates.append(
            LevelUpdate(
                action=_action_of(before, after),
                px=settled.px,
                qty=None if after <= 0 else after,
                orders=None,
                position=None,
            )
        )
        return True

    def standing(self, order: Order) -> Order | None:
        """The live version of `order`, by lifecycle or by the name the venue gave it.

        By name as well, and that is not redundant: a lifecycle is hashed from
        the instrument, the venue and the identifier, so a report that carries
        the identifier and omits the venue -- which venues do, because they
        know which one they are -- hashes to a different lifecycle than the
        order it continues. The identifier is the venue's own and unique to
        it, so it settles the question the hash cannot.
        """
        found = self.orders.get(order.xhash) if order.xhash else None
        if found is not None:
            return found
        for spelling in (order.order_id, order.client_order_id):
            if spelling and spelling in self.named:
                return self.orders.get(self.named[spelling])
        return None

    def remove(self, xhash: int) -> None:
        """Take one order out of the book, and every name that pointed at it."""
        gone = self.orders.get(xhash)
        if gone is None:
            return
        self._leave(gone)
        self._forget(xhash)

    # -- the structure itself ------------------------------------------------

    def _join(self, order: Order, quantity: float) -> None:
        """Put `order` on its level for `quantity`, making the level if it is new.

        A new level is one `bisect.insort` into `keys` -- a binary search and
        a `memmove`, which at any depth a book reaches beats re-sorting.
        """
        px = order.px
        level = self.levels.get(px)
        if level is None:
            key = px * self.facing
            level = self.levels[px] = _Level(px, key)
            bisect.insort(self.keys, key)
        level.qty += quantity
        level.members[order.xhash] = None
        level.frozen = None
        self.total_qty += quantity

    def _leave(self, order: Order) -> None:
        """Take `order` off its level, dropping the level when it empties.

        A level that reaches zero is dropped rather than kept at zero: a level
        of nothing is not a level, and leaving it would put an empty price in
        every `alive` list from then on.
        """
        level = self.levels.get(order.px)
        if level is None:
            return
        quantity = _resting(order)
        level.qty -= quantity
        self.total_qty -= quantity
        level.members.pop(order.xhash, None)
        level.frozen = None
        if not level.members or level.qty <= 0:
            # The whole level went with it, so the running totals follow: a
            # level whose members are gone but whose quantity has drifted --
            # a float sum that did not land on zero -- must not leave that
            # drift in `total_qty` for the rest of the fold.
            self.total_qty -= level.qty
            del self.levels[order.px]
            self.keys.pop(bisect.bisect_left(self.keys, level.key))

    def _forget(self, xhash: int) -> None:
        """Drop one order and the names that pointed at it, level already left."""
        gone = self.orders.pop(xhash, None)
        if gone is None:
            return
        for spelling in (gone.order_id, gone.client_order_id):
            if spelling and self.named.get(spelling) == xhash:
                del self.named[spelling]

    def take(self, execution: Execution, traded: float) -> None:
        """Record a trade against this side, and take `traded` out of it.

        The order the fill names is reduced first, because that is exact.
        Where the report names no order -- a market-data trade print, which is
        most of them -- the quantity comes off the resting orders best first,
        which is the same order a venue would have filled them in.

        Walked level by level rather than over a sorted copy of the book: a
        fill usually clears one level, and building an ordered list of every
        live order to take one bite out of the top of it was the cost of a
        trade on a deep book.
        """
        self.executions.append(
            LevelExecution(
                unix=execution.unix,
                px=execution.px or 0.0,
                qty=execution.qty or 0.0,
                xhash=execution.xhash,
                aggressor=execution.aggressor,
            )
        )
        hit = self.orders.get(execution.order_xhash or 0)
        if hit is not None:
            self._reduce(hit, traded)
            return
        while traded > 0 and self.keys:
            top = self.keys[0]
            level = self.levels[top * self.facing]
            # A list, because reducing an order can empty the level and delete
            # the dict this is walking. Sorted inside the level and nowhere
            # else: the largest interest at one price is met first.
            for xhash in sorted(level.members, key=lambda x: -_resting(self.orders[x])):
                if traded <= 0:
                    break
                resting = self.orders.get(xhash)
                if resting is not None:
                    traded = self._reduce(resting, traded)
            if self.keys and self.keys[0] == top:
                # The touch did not move, so there is nothing further to take
                # off it. Without this the loop could spin on a level that
                # would not clear.
                break

    def _reduce(self, order: Order, traded: float) -> float:
        """Take what `traded` can from one order; the rest is returned for the next."""
        standing = _resting(order)
        taken = min(standing, traded)
        left = standing - taken
        if left <= 0:
            self.remove(order.xhash)
        else:
            level = self.levels.get(order.px)
            if level is not None:
                level.qty -= taken
                level.frozen = None
                self.total_qty -= taken
            order.leaves_qty = left
        self.updates.append(
            LevelUpdate(
                action=_action_of(standing, left),
                px=order.px,
                qty=None if left <= 0 else left,
                orders=None,
                position=None,
            )
        )
        return traded - taken


def _action_of(before: float, after: float) -> UpdateAction:
    """What moving a level from `before` to `after` did to it."""
    if after <= 0:
        return UpdateAction.DELETE
    return UpdateAction.CHANGE if before > 0 else UpdateAction.NEW
