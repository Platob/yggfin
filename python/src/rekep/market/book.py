"""A side of a book, both sides of one, and the derived prices computed in kernels."""

from __future__ import annotations

from typing import Annotated, Any, ClassVar

import pyarrow
import pyarrow.compute

from rekep.convert import Convertible
from rekep.fields import Field, FieldBuilder, field
from rekep.market.enums import UpdateAction
from rekep.market.event import MarketEvent
from rekep.market.fields import MarketFieldBuilder, fix_tag


@field
class Level(Convertible):
    """One live price level: what is there, and how much of it."""

    FIELD_BUILDER: ClassVar[type[FieldBuilder]] = MarketFieldBuilder

    px: Annotated[float, fix_tag("MDEntryPx", 270)]
    """Price of the level."""

    qty: Annotated[float, fix_tag("MDEntrySize", 271)]
    """Quantity resting at that price."""

    orders: Annotated[int | None, fix_tag("NumberOfOrders", 346), Field(arrow_type=pyarrow.int32())]
    """How many orders make up the level; null when the venue does not say."""


@field
class LevelUpdate(Convertible):
    """One change to a level, as an incremental feed sends it.

    `px` and `qty` are nullable here and not on `Level`, because a deletion
    says which level went and not what was in it -- and the ranged deletions
    (`DELETE_THRU`, `DELETE_FROM`) name a boundary rather than a level at all.
    """

    FIELD_BUILDER: ClassVar[type[FieldBuilder]] = MarketFieldBuilder

    action: Annotated[UpdateAction, fix_tag("MDUpdateAction", 279)]
    """What this does to the book; `>= UpdateAction.REMOVE` takes liquidity out."""

    px: Annotated[float | None, fix_tag("MDEntryPx", 270)]
    """Price the update applies at."""

    qty: Annotated[float | None, fix_tag("MDEntrySize", 271)]
    """Quantity after the update; null on a deletion."""

    orders: Annotated[int | None, fix_tag("NumberOfOrders", 346), Field(arrow_type=pyarrow.int32())]
    """How many orders make up the level after the update."""

    position: Annotated[
        int | None, fix_tag("MDEntryPositionNo", 290), Field(arrow_type=pyarrow.int32())
    ]
    """Depth the update applies at, counting from the top; for venues that index."""


@field
class BookSide(MarketEvent):
    """One side of one book, carrying both what is live and what changed.

    `side` is `BID` or `ASK`, which are the same codes as `BUY` and `SELL`
    because a bid *is* a buy. `px` and `qty` are the **best level's** -- the
    top of this side -- and `total_qty` is every level's.

    **Both the state and the delta are on the row**, and that is the point of
    the shape. A feed sends one or the other: a snapshot says what is there, an
    incremental says what changed. Keeping only snapshots loses what moved;
    keeping only increments makes every reader replay from the last snapshot
    before it can answer anything. Carrying both means a consumer reads state
    without replaying and reconstructs causation without a second stream --
    and the cost is a list column that is null on the rows that do not have it.

    A row where `alive` is null is an increment that was never resolved to a
    state; `summarise_arrow` leaves those rows exactly as it found them rather
    than deriving nulls over whatever the producer put there.
    """

    #: What `summarise_arrow` redirects to, keyed by what it was handed.
    SUMMARIES: ClassVar[dict[Any, str]] = {
        pyarrow.RecordBatch: "arrow_batch",
        pyarrow.Table: "arrow_table",
    }

    alive: list[Level] | None
    """Every live level, best first; null on a row that carries only a delta."""

    updates: list[LevelUpdate] | None
    """The changes that produced this version, in the order the venue sent them."""

    depth: Annotated[int | None, Field(arrow_type=pyarrow.int32())]
    """How many levels are live -- `len(alive)`, flat, so a filter can use it."""

    total_qty: float | None
    """Sum of `qty` over every live level."""

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

        Everything here is a kernel over whole columns. Two things it does not
        do, both of which are how a first version got this wrong:

        - It never calls `list_element`, which raises `Index 0 is out of
          bounds` on an empty list -- and an empty side of a book is a normal
          Tuesday, not an error. The best row of each list is taken by its own
          offset instead, which is null exactly where the list is empty.
        - It never sums by prefix. A running total over the whole batch minus
          another running total gives the right answer only until the totals
          get big, and a book's own quantities are small differences of large
          ones by then. The sum is grouped, which is exact.
        """
        alive = _combined(batch.column("alive"))
        known = pyarrow.compute.is_valid(alive)
        lengths = pyarrow.compute.list_value_length(alive)
        top = pyarrow.compute.if_else(
            pyarrow.compute.greater(lengths, 0),
            alive.offsets[:-1],
            pyarrow.scalar(None, pyarrow.int32()),
        )
        levels = alive.values
        return _with(
            batch,
            px=pyarrow.compute.if_else(
                known, pyarrow.compute.take(levels.field("px"), top), batch.column("px")
            ),
            qty=pyarrow.compute.if_else(
                known, pyarrow.compute.take(levels.field("qty"), top), batch.column("qty")
            ),
            depth=pyarrow.compute.if_else(known, lengths, batch.column("depth")),
            total_qty=pyarrow.compute.if_else(
                known, _list_sums(alive, "qty", lengths), batch.column("total_qty")
            ),
        )

    @classmethod
    def summarise_arrow_table(cls, table: pyarrow.Table) -> pyarrow.Table:
        """`summarise_arrow_batch` over a whole table, batch by batch."""
        return pyarrow.Table.from_batches(
            [cls.summarise_arrow_batch(batch) for batch in table.to_batches()], schema=table.schema
        )


@field
class Book(MarketEvent):
    """Both sides of one book, and the prices that only exist across them.

    Each side is the whole `BookSide` event, identity included, so a book says
    exactly which version of each side it was built from -- `bid.h128` and
    `ask.h128`, which are also what `parent_h128` holds. A snapshot that only
    kept the levels would be a number nobody can reproduce; keeping the two
    envelopes costs columns that repeat and therefore compress, and buys a
    row that can be checked against the sides it came from.

    `side` is `UNKNOWN`: a book does not have one. `px` is the **mid** and
    `qty` is the size at the touch, so the abstract slots stay meaningful
    rather than being null on every row of the largest table here.

    **The flat pair `(px, spread)` is the best bid and offer**, exactly and
    without duplicating them: bid is `px - spread / 2`, ask is `px +
    spread / 2`. That is also why there is no `crossed` flag -- `spread < 0`
    is crossed, `spread == 0` is locked, and both are one range predicate on a
    column an engine already has statistics for.

    Flat, rather than reached through `bid.px`, because a nested price is not
    a filter anything here can use. Iceberg writes **no bounds at all for a
    field under a list or a map**, so nothing about a level in `alive` can ever
    skip a file; Doris pushes a predicate down only for a top-level scalar
    column, so `bid.px` reaches the scan as a row filter and `spread` reaches
    it as file pruning. Deriving these once at write time is what makes the
    same query cheap on all three engines instead of only on Spark.
    """

    #: What `summarise_arrow` redirects to, keyed by what it was handed.
    SUMMARIES: ClassVar[dict[Any, str]] = {
        pyarrow.RecordBatch: "arrow_batch",
        pyarrow.Table: "arrow_table",
    }

    # Declared *before* the two sides, and the order is load-bearing. Iceberg
    # collects column bounds for the first `write.metadata.metrics
    # .max-inferred-column-defaults` leaves in pre-order, 100 by default, and a
    # book is 140 leaves. Behind `bid` and `ask` these three landed at 138, 139
    # and 140 -- past the cutoff, so the columns this whole shape exists to make
    # prunable would have shipped with no bounds at all, and every filter on
    # them would have read every file while looking like it worked.
    spread: float | None
    """`ask.px - bid.px`; negative is crossed, zero is locked."""

    # The size-weighted price, which is the one number that says where the book
    # thinks the instrument is when the two sides are not the same size. Each
    # price is weighted by the *opposite* side's quantity, because a large bid
    # against a small offer means the next trade is likelier at the offer.
    micro_px: float | None
    """Microprice: `(bid.px * ask.qty + ask.px * bid.qty) / (bid.qty + ask.qty)`."""

    imbalance: float | None
    """`(bid.qty - ask.qty) / (bid.qty + ask.qty)`, in `[-1, 1]`; positive is bid-heavy."""

    bid: BookSide
    """The buy side, best first."""

    ask: BookSide
    """The sell side, best first."""

    # -- the derived columns, in kernels -------------------------------------

    @classmethod
    def summarise_arrow(cls, source: Any) -> Any:
        """`summarise_arrow_batch` or `_table`, inferred from what it was handed."""
        return getattr(cls, f"summarise_{cls.redirect_of(source, cls.SUMMARIES)}")(source)

    @classmethod
    def summarise_arrow_batch(cls, batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        """Both sides summarised from their levels, then the prices across them.

        **Both halves, in that order**, because the second needs the first: a
        book that has just been assembled from two feeds carries levels and
        nothing derived, so reading `bid.px` before deriving it gives null and
        every price across the sides comes out null with it. Doing only the
        cross-side half was the first version of this, and the benchmark
        caught it by deriving a batch of books into a column of nulls.

        Everything is kernels over whole columns, and no row is looked at in
        Python. A one-sided book derives to null everywhere a price is
        missing, which is what a mid across a side that is not there means;
        the division guards on the size rather than letting Arrow return an
        infinity, because an infinite microprice is a number and a null is the
        truth.
        """
        compute = pyarrow.compute
        batch = _with(
            batch,
            bid=cls._side(batch.column("bid")),
            ask=cls._side(batch.column("ask")),
        )
        bid_px = compute.struct_field(batch.column("bid"), "px")
        bid_qty = compute.struct_field(batch.column("bid"), "qty")
        ask_px = compute.struct_field(batch.column("ask"), "px")
        ask_qty = compute.struct_field(batch.column("ask"), "qty")
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

    @classmethod
    def _side(cls, column: Any) -> pyarrow.Array:
        """One nested side summarised, as the batch it already is.

        A struct column and a record batch are the same thing in Arrow -- the
        same child arrays under a different name -- so a side is summarised by
        the code that summarises a side, rather than by a second walk of the
        same levels written for the nested case.
        """
        array = _combined(column)
        summarised = BookSide.summarise_arrow_batch(pyarrow.RecordBatch.from_struct_array(array))
        return summarised.to_struct_array()


# -- helpers ----------------------------------------------------------------


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
