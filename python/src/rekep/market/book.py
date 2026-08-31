"""Compact price levels and deterministic book folding."""

from __future__ import annotations

import bisect
import copy
import dataclasses
import functools
import heapq
import itertools
import math
from collections import deque
from collections.abc import Iterable, Iterator
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Any, Self

import pyarrow
import pyarrow.compute

from rekep.enums import (
    EventType,
    MarketKind,
    Side,
    State,
)
from rekep.fields import Field, scalar
from rekep.market.event import DAY, HOUR, MarketEvent
from rekep.market.fields import MarketConvertible, fix_tag
from rekep.market.identity import HASH, NIL, frame, hash_bytes, stored_member
from rekep.market.instrument import Instrument
from rekep.market.orders import CLIENT_ORDER_CODE, Execution, Order, _quantity_transition

if TYPE_CHECKING:
    from rekep.fix.registry import FixRegistry
    from rekep.text.fixmsg import FixMsg

_ARROW_DISPATCH = MappingProxyType(
    {
        pyarrow.RecordBatch: "arrow_batch",
        pyarrow.Table: "arrow_table",
    }
)

# Amortize heap rebuilds while bounding stale revisions independently of replay length.
_DEADLINE_STALE_BUFFER = 64
# Keep the stable root and current identifiers; bound only historical lookup aliases.
_ORDER_ALIAS_LIMIT = 64
# Exact event links can target an earlier version while its order still rests.
_ORDER_HASH_LIMIT = 64


@scalar(slots=True, weakref_slot=True)
class Level(MarketConvertible):
    """One compact price level."""

    px: Annotated[float, fix_tag("MDEntryPx")] = 0.0
    """Price of the level."""

    qty: Annotated[float, fix_tag("MDEntrySize")] = 0.0
    """Quantity resting at that price."""


@scalar(slots=True)
class Book(MarketEvent):
    """Both sides of one book, flat, and the prices that only exist across them."""

    @classmethod
    @functools.cache
    def into_event_type(cls) -> EventType:
        """Event kind fixed by this shape."""
        return EventType.BOOK

    hash: Annotated[int, Field.primary_key(dtype=HASH)] = NIL
    """Time-anchored composition of `unix` and the live book's `vhash`."""

    # Re-declared to carry **no** FIX tag, which is the honest thing to say:
    # a mid and a touch size are computed from two sides, and FIX has no field
    # for either. The inherited `LastPx <31>` would claim the mid was a trade.
    lastpx: float | None = None
    """The mid, `(bidpx + askpx) / 2`; null until both sides have a price."""

    lastqty: Annotated[float | None, Field.column("LastQty")] = None
    """The size at the touch, `bidqty + askqty`; null until both sides have one."""

    spread: float | None = None
    """`askpx - bidpx`; negative is crossed, zero is locked."""

    # The size-weighted price, which is the one number that says where the book
    # thinks the instrument is when the two sides are not the same size. Each
    # price is weighted by the *opposite* side's quantity, because a large bid
    # against a small offer means the next trade is likelier at the offer.
    vwap: float | None = None
    """Top-of-book weighted price using each side's opposing quantity."""

    execpx: Annotated[float | None, Field.column("ExecPx")] = None
    """Most recent filled execution price observed by this book."""

    prevexecpx: Annotated[float | None, Field.column("PrevExecPx")] = None
    """Execution price on the preceding book version."""

    imbalance: float | None = None
    """`(bidqty - askqty) / (bidqty + askqty)`, in `[-1, 1]`; positive is bid-heavy."""

    bidpx: Annotated[float | None, Field.column("BidPx")] = None
    """Best bid; also `px - spread / 2`."""

    prevbidpx: Annotated[float | None, Field.column("PrevBidPx")] = None
    """Best bid on the preceding book version."""

    bidqty: Annotated[float | None, Field.column("BidQty")] = None
    """Quantity at the best bid."""

    prevbidqty: Annotated[float | None, Field.column("PrevBidQty")] = None
    """Best-bid quantity on the preceding book version."""

    biddepth: Annotated[int, Field(dtype=pyarrow.int32()), Field.column("BidDepth")] = 0
    """How many levels are live on the buy side."""

    askpx: Annotated[float | None, Field.column("AskPx")] = None
    """Best offer; also `px + spread / 2`."""

    prevaskpx: Annotated[float | None, Field.column("PrevAskPx")] = None
    """Best offer on the preceding book version."""

    askqty: Annotated[float | None, Field.column("AskQty")] = None
    """Quantity at the best offer."""

    prevaskqty: Annotated[float | None, Field.column("PrevAskQty")] = None
    """Best-offer quantity on the preceding book version."""

    askdepth: Annotated[int, Field(dtype=pyarrow.int32()), Field.column("AskDepth")] = 0
    """How many levels are live on the sell side."""

    bidlevels: Annotated[list[Level], Field.column("BidLevels")] = dataclasses.field(
        default_factory=list
    )
    """Changed buy levels on deltas; every live buy level on snapshots, best first."""

    asklevels: Annotated[list[Level], Field.column("AskLevels")] = dataclasses.field(
        default_factory=list
    )
    """Changed sell levels on deltas; every live sell level on snapshots, best first."""

    deltas: list[Order] = dataclasses.field(default_factory=list)
    """Order state transitions belonging to this book delta."""

    executions: list[Execution] = dataclasses.field(default_factory=list)
    """Execution evidence belonging to this book delta."""

    bidalive: Annotated[list[Order], Field.column("BidAlive")] = dataclasses.field(
        default_factory=list
    )
    """Complete living bid orders, populated only on snapshots."""

    askalive: Annotated[list[Order], Field.column("AskAlive")] = dataclasses.field(
        default_factory=list
    )
    """Complete living ask orders, populated only on snapshots."""

    __bid_order_vhashes: tuple[int, ...] = ()
    __ask_order_vhashes: tuple[int, ...] = ()
    __bid_order_frame: bytes | None = None
    __ask_order_frame: bytes | None = None

    def __post_init__(self) -> None:
        """Normalize collection and depth invariants once at construction."""
        if self.deltas is None:
            self.deltas = []
        if self.executions is None:
            self.executions = []
        if self.bidlevels is None:
            self.bidlevels = []
        if self.asklevels is None:
            self.asklevels = []
        if self.bidalive is None:
            self.bidalive = []
        if self.askalive is None:
            self.askalive = []
        self.biddepth = 0 if self.biddepth is None else self.biddepth
        self.askdepth = 0 if self.askdepth is None else self.askdepth
        if self.bidalive or self.askalive:
            self._remember_alive_vhashes(*_alive_vhashes(self.bidalive, self.askalive))
        MarketEvent.__post_init__(self)

    def version_parts(self) -> tuple[Any, ...]:
        """Identify this instant's complete live order state."""
        bid = self.__bid_order_vhashes
        ask = self.__ask_order_vhashes
        return (
            *self._version_prefix_parts(len(bid)),
            *bid,
            len(ask),
            *ask,
        )

    def _version_prefix_parts(self, bid_count: int) -> tuple[Any, ...]:
        """Current book values before the cached live-side value hashes."""
        return (
            *MarketEvent.version_parts(self),
            self.spread,
            self.vwap,
            self.execpx,
            self.imbalance,
            self.bidpx,
            self.bidqty,
            self.biddepth,
            self.askpx,
            self.askqty,
            self.askdepth,
            bid_count,
        )

    def _remember_alive_vhashes(
        self,
        bid: Iterable[int],
        ask: Iterable[int],
        *,
        bid_frame: bytes | None = None,
        ask_frame: bytes | None = None,
    ) -> None:
        """Retain ordered live Order value hashes as private Book identity inputs."""
        self.__bid_order_vhashes = tuple(bid)
        self.__ask_order_vhashes = tuple(ask)
        self.__bid_order_frame = bid_frame
        self.__ask_order_frame = ask_frame

    def _version_vhash(self) -> int:
        """Hash the live sides from their cached byte-exact identity frames."""
        bid = self.__bid_order_vhashes
        ask = self.__ask_order_vhashes
        bid_frame = self.__bid_order_frame
        if bid_frame is None:
            bid_frame = self.__bid_order_frame = vhashes_frame(bid)
        ask_frame = self.__ask_order_frame
        if ask_frame is None:
            ask_frame = self.__ask_order_frame = vhashes_frame(ask)
        return hash_bytes(
            b"".join(
                (
                    frame(
                        (
                            type(self).__name__,
                            *self._version_prefix_parts(len(bid)),
                        )
                    ),
                    bid_frame,
                    frame((len(ask),)),
                    ask_frame,
                )
            )
        )

    def complete_from(self, previous: MarketEvent) -> None:
        """Complete the book without carrying an earlier delta's relations."""
        links = list(self.linkhashes)
        MarketEvent.complete_from(self, previous)
        self.linkhashes = links
        if isinstance(previous, Book) and self.execpx is None:
            self.execpx = previous.execpx

    def with_previous(self, previous: MarketEvent | None) -> Self | None:
        """Finish a known delta directly; snapshots retain generic completion."""
        if (
            self.snapunix is not None
            or not self.xhash
            or (
                previous is not None
                and (not isinstance(previous, Book) or previous.xhash != self.xhash)
            )
        ):
            completed = MarketEvent.with_previous(self, previous)
            if completed is not None and completed.snapunix is not None:
                completed.forget_delta()
            return completed
        if previous is not None:
            self._keep_creation(previous)
            if not self.recunix:
                self.recunix = previous.recunix
            if self.expunix is None:
                self.expunix = previous.expunix
            if self.lastmkt is None:
                self.lastmkt = previous.lastmkt
            if self.into_instrument() is None:
                instrument = previous.into_instrument()
                if instrument is not None:
                    self.attach_instrument(instrument)
            if not self.pxunit:
                self.pxunit = previous.pxunit
            if self.currency is None:
                self.currency = previous.currency
            if not self.qtyunit:
                self.qtyunit = previous.qtyunit
            if self.execpx is None:
                self.execpx = previous.execpx
            self.code = previous.code or self.code
            if previous.code:
                self.codesource = previous.codesource or self.codesource
            self.version = previous.version + 1
            self.prevunix = previous.unix
            self._remember_previous(previous)
        self.derive()
        self._materialize_life_code()
        if self.linkhashes:
            self._drop_self_link()
        self.vhash = self._version_vhash()
        self.hash = NIL
        return self.identify()

    @classmethod
    def from_fixmsgs(cls, logs: Iterable[FixMsg], **declared: Any) -> Iterator[Self]:
        """Fold an already sorted parsed-log stream into books."""
        return iter(BookIterator(logs=logs, **declared))

    @classmethod
    def from_events(cls, events: Iterable[MarketEvent], **declared: Any) -> Iterator[Self]:
        """Fold translated events directly, for protocol adapters and tests."""
        return iter(BookIterator.from_events(events, **declared))

    @classmethod
    def into_arrow_reader(
        cls, events: Iterable[Self], batch_row_size: int = 65_536
    ) -> pyarrow.RecordBatchReader:
        """Serialize books in bounded, column-built Arrow batches."""
        if batch_row_size <= 0:
            raise ValueError("batch_row_size must be positive")
        schema = cls.into_field().into_arrow_schema()

        def batches() -> Iterator[pyarrow.RecordBatch]:
            held: list[Self] = []
            for event in events:
                held.append(event)
                if len(held) >= batch_row_size:
                    yield _book_arrow_batch(held, schema)
                    held.clear()
            if held:
                yield _book_arrow_batch(held, schema)

        return pyarrow.RecordBatchReader.from_batches(schema, batches())

    def forget_delta(self) -> None:
        """A picture keeps levels but clears its event deltas."""
        MarketEvent.forget_delta(self)
        self.prevbidpx = None
        self.prevbidqty = None
        self.prevaskpx = None
        self.prevaskqty = None
        self.prevexecpx = None
        self.deltas = []
        self.executions = []

    def _remember_previous(self, previous: MarketEvent) -> None:
        """Retain both derived prices and the preceding touch."""
        MarketEvent._remember_previous(self, previous)
        if isinstance(previous, Book):
            if self.prevbidpx is None:
                self.prevbidpx = previous.bidpx
            if self.prevbidqty is None:
                self.prevbidqty = previous.bidqty
            if self.prevaskpx is None:
                self.prevaskpx = previous.askpx
            if self.prevaskqty is None:
                self.prevaskqty = previous.askqty
            if self.prevexecpx is None:
                self.prevexecpx = previous.execpx

    def derive(self) -> None:
        """A book's prices are computed across its sides, and never carried."""
        self._summarise()
        MarketEvent.derive(self)

    def _summarise(self) -> None:
        """Fill flat side summaries from present level lists, then price the book."""
        for execution in reversed(self.executions):
            if execution.state is State.FILLED and execution.lastpx is not None:
                self.execpx = execution.lastpx
                break
        if self.snapunix is not None:
            for name in ("bid", "ask"):
                levels = getattr(self, f"{name}levels")
                best = levels[0] if levels else None
                setattr(self, f"{name}px", None if best is None else best.px)
                setattr(self, f"{name}qty", None if best is None else best.qty)
                setattr(self, f"{name}depth", len(levels))
        self._priced()

    def _priced(self) -> None:
        """The five prices across the sides, for one book rather than a column of them."""
        bidpx, bidqty = self.bidpx, self.bidqty
        askpx, askqty = self.askpx, self.askqty
        self.lastpx = None if bidpx is None or askpx is None else (bidpx + askpx) / 2
        self.spread = None if bidpx is None or askpx is None else askpx - bidpx
        size = None if bidqty is None or askqty is None else bidqty + askqty
        self.lastqty = size
        if not size:
            self.vwap = self.imbalance = None
            return
        self.vwap = (
            None if bidpx is None or askpx is None else (bidpx * askqty + askpx * bidqty) / size
        )
        self.imbalance = (bidqty - askqty) / size

    # -- the derived columns, in kernels -------------------------------------

    @classmethod
    def summarise_arrow(cls, source: Any) -> Any:
        """`summarise_arrow_batch` or `_table`, inferred from what it was handed."""
        return getattr(cls, f"summarise_{cls.redirect_of(source, _ARROW_DISPATCH)}")(source)

    @classmethod
    def summarise_arrow_batch(cls, batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        """Each side derived from its own levels, then the prices across them."""
        compute = pyarrow.compute
        for name in ("bid", "ask"):
            batch = _with(
                batch,
                **_derived(
                    batch,
                    f"{name}levels",
                    (f"{name}px", f"{name}qty", f"{name}depth"),
                ),
            )
        bidpx, bidqty = batch.column("bidpx"), batch.column("bidqty")
        askpx, askqty = batch.column("askpx"), batch.column("askqty")
        size = compute.add(bidqty, askqty)
        weighted = compute.greater(size, 0)
        return _with(
            batch,
            lastpx=compute.divide(compute.add(bidpx, askpx), pyarrow.scalar(2.0)),
            lastqty=size,
            spread=compute.subtract(askpx, bidpx),
            vwap=compute.if_else(
                weighted,
                compute.divide(
                    compute.add(compute.multiply(bidpx, askqty), compute.multiply(askpx, bidqty)),
                    size,
                ),
                pyarrow.scalar(None, pyarrow.float64()),
            ),
            imbalance=compute.if_else(
                weighted,
                compute.divide(compute.subtract(bidqty, askqty), size),
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


def vhashes_frame(vhashes: tuple[int, ...]) -> bytes:
    """A run of value hashes in their native signed 64-bit frame."""
    return frame(vhashes) if vhashes else b""


def _alive_vhashes(
    bid: Iterable[Order], ask: Iterable[Order]
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """The two sides' live orders as the hashes a book is identified by.

    For a book that already carries its live rows -- one read back, or one
    just snapshotted. A fold reads the hashes off its own state instead, and
    never builds the rows to get them.
    """
    return tuple(order.vhash for order in bid), tuple(order.vhash for order in ask)


def _resting(order: Order) -> float:
    """Displayed live quantity after subtracting explicitly hidden interest."""
    lastqty = order.lastqty
    if lastqty is None:
        return 0.0
    hidden = order.hiddenqty
    return max(lastqty - (hidden if hidden is not None and hidden > 0 else 0.0), 0.0)


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


def _derived(batch: Any, levels: str, into: tuple[str, str, str]) -> dict[str, Any]:
    """The best price, best size, and depth a list of levels determines."""
    compute = pyarrow.compute
    alive = _combined(batch.column(levels))
    lengths = compute.list_value_length(alive)
    top = compute.if_else(
        compute.greater(lengths, 0), alive.offsets[:-1], pyarrow.scalar(None, pyarrow.int32())
    )
    members = alive.values
    best_px, best_qty, depth = into
    derive = compute.is_valid(batch.column("snapunix"))
    return {
        best_px: compute.if_else(
            derive, compute.take(members.field("px"), top), batch.column(best_px)
        ),
        best_qty: compute.if_else(
            derive, compute.take(members.field("qty"), top), batch.column(best_qty)
        ),
        depth: compute.if_else(derive, lengths, batch.column(depth)),
    }


_BOOK_STRUCT_LISTS = frozenset(
    {"bidlevels", "asklevels", "deltas", "executions", "bidalive", "askalive"}
)


def _book_arrow_batch(books: list[Book], schema: pyarrow.Schema) -> pyarrow.RecordBatch:
    """Build one Book batch by columns, including its nested struct lists."""
    columns = []
    for declared in schema:
        name = declared.name
        values = [stored_member(name, getattr(book, name)) for book in books]
        if name in _BOOK_STRUCT_LISTS:
            columns.append(_struct_list_arrow(values, declared.type))
        else:
            columns.append(pyarrow.array(values, type=declared.type))
    return pyarrow.RecordBatch.from_arrays(columns, schema=schema)


def _struct_list_arrow(rows: list[list[Any]], declared: pyarrow.DataType) -> pyarrow.Array:
    """Build one non-null list-of-struct column without row documents."""
    offsets = [0, *itertools.accumulate(len(row) for row in rows)]
    values = list(itertools.chain.from_iterable(rows))
    struct = declared.value_type
    children = [
        pyarrow.array(
            [stored_member(member.name, getattr(value, member.name)) for value in values],
            type=member.type,
        )
        for member in struct
    ]
    flattened = pyarrow.StructArray.from_arrays(children, fields=list(struct))
    return pyarrow.ListArray.from_arrays(
        pyarrow.array(offsets, type=pyarrow.int32()), flattened, type=declared
    )


# -- folding a book out of one instrument's events ---------------------------


@dataclasses.dataclass(slots=True)
class _LevelState:
    """Mutable quantity and insertion-ordered membership for one live price."""

    px: float
    qty: float = 0.0
    members: dict[int, None] = dataclasses.field(default_factory=dict)

    #: Members in descending resting quantity. Inserts and revisions keep this
    #: ordered, so a busy shared price never re-sorts all its orders per event.
    resting: list[int] = dataclasses.field(default_factory=list, repr=False)

    #: Stable tie order for equal quantities; replacing a member earns a new one.
    priority: dict[int, int] = dataclasses.field(default_factory=dict, repr=False)
    next_priority: int = dataclasses.field(default=0, repr=False)

    #: Those members' value hashes, in the same order. Cached beside the
    #: order for the same reason and cleared with it: a book is identified by
    #: this and by nothing else about the orders standing behind it.
    vhashes: tuple[int, ...] | None = dataclasses.field(default=None, repr=False)

    #: The same value hashes in their byte-exact identity frames. Joining the
    #: unchanged levels avoids reframing a whole side when one price moves.
    frame: bytes | None = dataclasses.field(default=None, repr=False)


@dataclasses.dataclass
class _Side:
    """Every order standing on one side of a book, kept in best-first order."""

    side: Side
    """Which side this is; its sign is what turns the sort around."""

    orders: dict[int, Order] = dataclasses.field(default_factory=dict)
    """Every live order, by lifecycle, in no particular order."""

    named: dict[tuple[str, str], dict[int | None, int]] = dataclasses.field(default_factory=dict)
    """Lifecycle for each typed identifier and known venue scope."""

    aliases: dict[int, dict[tuple[int | None, str, str], None]] = dataclasses.field(
        default_factory=dict
    )
    """Current lookup keys per lifecycle, cached for bounded replacement and removal."""

    hashes: dict[int, int] = dataclasses.field(default_factory=dict)
    """Live lifecycle for each retained exact event hash."""

    version_hashes: dict[int, dict[int, None]] = dataclasses.field(default_factory=dict)
    """Bounded exact event hashes retained for each live lifecycle."""

    levels: dict[float, _LevelState] = dataclasses.field(default_factory=dict)
    """Live levels by price, updated incrementally as orders move."""

    keys: list[float] = dataclasses.field(default_factory=list)
    """Best-first sorted prices multiplied by this side's direction."""

    alive: list[_LevelState] = dataclasses.field(default_factory=list)
    """Live levels in the same best-first order as `keys`."""

    changed: dict[float, None] = dataclasses.field(default_factory=dict)
    """Prices changed since the last emitted book, in first-touch order."""

    max_order_age_ns: int | None = None
    """Maximum unchanged lifetime included in each order's indexed deadline."""

    _deadlines: list[tuple[int, int, int]] = dataclasses.field(
        default_factory=list, init=False, repr=False
    )
    _deadline_tokens: dict[int, int] = dataclasses.field(
        default_factory=dict, init=False, repr=False
    )
    _deadline_values: dict[int, int] = dataclasses.field(
        default_factory=dict, init=False, repr=False
    )
    _next_deadline_token: int = dataclasses.field(default=0, init=False, repr=False)
    _order_vhashes_cache: tuple[int, ...] | None = dataclasses.field(
        default=None, init=False, repr=False
    )
    _order_frame_cache: bytes | None = dataclasses.field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Cache direction and index any supplied live orders."""
        self.facing = -self.side.sign
        for order in self.orders.values():
            self._index_deadline(order)

    @classmethod
    def from_snapshot(
        cls,
        side: Side,
        levels: Iterable[Level],
        orders: Iterable[Order],
        max_order_age_ns: int | None = None,
    ) -> _Side:
        """Restore a side from compact levels and generic live order rows."""
        restored = cls(side=side, max_order_age_ns=max_order_age_ns)
        captured_orders = list(orders)
        wrong_side = [order.xhash for order in captured_orders if order.side.sign != side.sign]
        if wrong_side:
            raise ValueError(
                f"recovery {side.name.lower()}_alive contains orders for another side: "
                + ", ".join(map(str, wrong_side))
            )
        terminal = [order.xhash for order in captured_orders if order.state.is_terminal]
        if terminal:
            raise ValueError(
                f"recovery {side.name.lower()}_alive contains terminal orders: "
                + ", ".join(map(str, terminal))
            )
        captured_levels: dict[float, Level] = {}
        for captured in levels:
            if captured.qty <= 0:
                raise ValueError(f"recovery snapshot level {captured.px} must have positive qty")
            if captured.px in captured_levels:
                raise ValueError(f"recovery snapshot contains level {captured.px} more than once")
            captured_levels[captured.px] = captured
        recovered_xhash: set[int] = set()
        for order in captured_orders:
            if not order.xhash:
                raise ValueError(
                    f"recovery {side.name.lower()}_alive contains an unidentified Order"
                )
            if order.xhash in recovered_xhash:
                raise ValueError(
                    f"recovery {side.name.lower()}_alive contains Order "
                    f"{order.xhash} more than once"
                )
            quantity = _resting(order)
            if order.lastpx is None or quantity <= 0:
                raise ValueError(
                    f"recovery Order {order.xhash} has no positive resting price and quantity"
                )
            if order.lastpx not in captured_levels:
                raise ValueError(
                    f"live recovery Order {order.xhash} at {order.lastpx} is absent from the levels"
                )
            recovered_xhash.add(order.xhash)
            restored._remember(order)
            restored._join(order, quantity)
        for captured in captured_levels.values():
            recovered = restored.levels.get(captured.px)
            if recovered is None:
                raise ValueError(f"recovery level {captured.px} has positive qty but no live Order")
            if not math.isclose(recovered.qty, captured.qty):
                raise ValueError(
                    f"recovery level {captured.px} has qty {captured.qty}, "
                    f"but its Orders total {recovered.qty}"
                )
        restored.changed.clear()
        return restored

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
    def best_level(self) -> _LevelState | None:
        """The level at the touch, or None when nothing is resting. O(1)."""
        return self.alive[0] if self.alive else None

    @property
    def best(self) -> Order | None:
        """The order at the touch, or None when nothing is resting.

        The largest at the best price, which is the second key of the
        ordering -- read over that level's own members and never over every
        live order, which is what a `min()` across the book used to cost.
        """
        level = self.best_level
        if level is None or not level.resting:
            return None
        return self.orders[level.resting[0]]

    @property
    def sorted_orders(self) -> list[Order]:
        """Every live order, best first: by price, then by descending quantity.

        A walk of `alive` and, inside each level, of its members in the order
        that level last settled into -- so the only sorting left is per level
        and per change to it. Kept because it is the honest reading of "the
        orders, in order".
        """
        orders = self.orders
        return [orders[x] for level in self.alive for x in self._resting_members(level)]

    def order_vhashes(self) -> tuple[int, ...]:
        """Every live order's value hash, best first.

        What a book is identified by, without building the list of orders it
        would otherwise be read off: a book row keeps the value hashes and nothing
        else about the orders standing behind it. Per level and cached, so an
        instrument with a hundred live levels pays for the one an event moved.
        """
        cached = self._order_vhashes_cache
        if cached is not None:
            return cached
        found: list[int] = []
        orders = self.orders
        for level in self.alive:
            vhashes = level.vhashes
            if vhashes is None:
                vhashes = level.vhashes = tuple(
                    orders[x].vhash for x in self._resting_members(level)
                )
            found.extend(vhashes)
        cached = self._order_vhashes_cache = tuple(found)
        return cached

    def order_value_identity(self) -> tuple[tuple[int, ...], bytes]:
        """Ordered live value hashes and their cached v1 frames.

        `order_vhashes` above fills every live level's hashes, and `_moved`
        drops a level's two caches together with the side's, so only the
        frames can still be missing here.
        """
        vhashes = self.order_vhashes()
        encoded = self._order_frame_cache
        if encoded is None:
            for level in self.alive:
                if level.frame is None:
                    level.frame = vhashes_frame(level.vhashes)
            encoded = self._order_frame_cache = b"".join(level.frame for level in self.alive)
        return vhashes, encoded

    @staticmethod
    def _resting_members(level: _LevelState) -> list[int]:
        """One level's incrementally ordered members."""
        return level.resting

    def _place(self, level: _LevelState, xhash: int, quantity: float) -> None:
        """Insert or revise one member in stable descending quantity order."""
        try:
            level.resting.remove(xhash)
        except ValueError:
            pass
        priority = level.priority.get(xhash)
        if priority is None:
            level.next_priority += 1
            priority = level.priority[xhash] = level.next_priority
        candidate = (-quantity, priority)
        low, high = 0, len(level.resting)
        while low < high:
            middle = (low + high) // 2
            current = level.resting[middle]
            key = (-_resting(self.orders[current]), level.priority[current])
            if key < candidate:
                low = middle + 1
            else:
                high = middle
        level.resting.insert(low, xhash)
        self._moved(level)

    def _unplace(self, level: _LevelState, xhash: int) -> None:
        """Remove one member from a level's resting order."""
        try:
            level.resting.remove(xhash)
        except ValueError:
            return
        level.priority.pop(xhash, None)
        self._moved(level)

    def _touch(self, px: float, level: _LevelState | None = None) -> None:
        """Record a price as changed, and forget what its members settled into."""
        self.changed[px] = None
        self._moved(px if level is None else level)

    def _moved(self, where: float | _LevelState | None) -> None:
        """Forget one level's cached order value hashes, by price or by level."""
        level = self.levels.get(where) if isinstance(where, float) else where
        if level is not None:
            level.vhashes = level.frame = None
            self._order_vhashes_cache = None
            self._order_frame_cache = None

    def into_levels(self) -> list[Level]:
        """The live orders aggregated to price levels, best first."""
        return [Level(px=level.px, qty=level.qty) for level in self.alive]

    def into_orders(self) -> list[Order]:
        """Independent live order rows needed to resume a compact snapshot."""
        return [copy.copy(order) for order in self.sorted_orders]

    def into_changed_levels(self) -> list[Level]:
        """Post-change levels, including zero-quantity deletions, best first."""
        levels = []
        for px in self.changed:
            level = self.levels.get(px)
            if level is None:
                levels.append(Level(px=px, qty=0.0))
            else:
                levels.append(Level(px=level.px, qty=level.qty))
        if len(levels) < 2:
            return levels
        return sorted(levels, key=lambda level: -level.px * self.side.sign)

    def cleared(self) -> None:
        """Forget the delta, keeping the state. Called once a book has been yielded."""
        self.changed.clear()

    def expire(self, unix: int) -> list[Order]:
        """Remove and return orders whose indexed deadline has been reached."""
        expired = []
        while self._deadlines and self._deadlines[0][0] <= unix:
            deadline, xhash, token = heapq.heappop(self._deadlines)
            if self._deadline_tokens.get(xhash) != token:
                continue
            order = self.orders.get(xhash)
            if order is None:
                continue
            current = self._deadline_of(order)
            if current is None or current[0] != deadline:
                continue
            expired.append(self._expire_order(order, unix, current[1], deadline))
        return expired

    def has_due(self, unix: int) -> bool:
        """Whether the earliest indexed deadline is observable by `unix`."""
        self._discard_stale_deadlines()
        return bool(self._deadlines and self._deadlines[0][0] <= unix)

    def purge(self, unix: int, reason: str) -> list[Order]:
        """Expire every order still resting, best first, and return them.

        The list is built before the first expiry, because expiring one takes
        it off its level and can drop the level the walk is standing in.
        """
        resting = list(self.sorted_orders)
        return [self._expire_order(order, unix, reason, unix) for order in resting]

    def bound(self, max_alive: int, unix: int) -> list[Order]:
        """Keep the best `max_alive` orders and return auditable evictions."""
        if max_alive < 0:
            raise ValueError("max_alive must be non-negative")
        excess = len(self.orders) - max_alive
        if excess <= 0:
            return []
        evicted = self._evictions(excess)
        reason = f"order exceeded max_side_alive={max_alive} by price-time priority"
        return [self._expire_order(order, unix, reason, unix) for order in evicted]

    def _evictions(self, excess: int) -> list[Order]:
        """Worst price first, ordering only equal-price candidates by time."""
        evicted: list[Order] = []
        for level in reversed(self.alive):
            remaining = excess - len(evicted)
            if remaining <= 0:
                break
            # Prices are already worst-first in this traversal. Only equal-price
            # orders need ordering, and the latest loses first.
            members = (self.orders[xhash] for xhash in level.members)
            if remaining == 1:
                evicted.append(max(members, key=self._time_priority))
            elif remaining < len(level.members):
                evicted.extend(heapq.nlargest(remaining, members, key=self._time_priority))
            else:
                evicted.extend(sorted(members, key=self._time_priority, reverse=True))
        return evicted

    @staticmethod
    def _time_priority(order: Order) -> tuple[int, int]:
        """Time priority within one price, with lifecycle as stable tie-break."""
        return order.creaunix or order.unix, order.xhash

    def _expire_order(self, order: Order, unix: int, reason: str, deadline: int) -> Order:
        """Remove one live order and build its synthetic terminal version."""
        self._leave(order)
        self._forget(order.xhash)
        terminal = copy.copy(order)
        terminal.unix = unix
        terminal.expunix = deadline
        terminal.state = State.INTERNAL_EXPIRED
        if not terminal.reason:
            terminal.reason = reason
        terminal.with_previous(order)
        return terminal

    # -- moving it ----------------------------------------------------------

    def apply(self, order: Order) -> bool:
        """One order's latest version, put where it belongs. True if the side moved."""
        moved, _ = self._applied(order)
        return moved

    def _applied(self, order: Order) -> tuple[bool, Order | None]:
        """Apply one order and return the completed version used by the fold."""
        standing = self.standing(order)
        # A full duplicate needs none of completion's copying, derivation or
        # hashing. Partial restatements deliberately miss these cheap guards
        # and continue below so absent economics can be completed.
        if (
            standing is not None
            and order.xhash == standing.xhash
            and order.vhash
            and order.vhash == standing.vhash
        ):
            return False, None
        # A copy, because what is stored here outlives the call and is
        # mutated by it -- `_reduce` writes a new `qty` onto a resting
        # order when a trade takes part of it. Folding the caller's own object
        # would edit an event it still holds, and replaying one stream twice
        # would give two different books.
        #
        # `copy.copy` and not `dataclasses.replace`, which re-runs `__init__`
        # and `__post_init__` over forty fields to produce the same object:
        # 11.1 us against 3.3 us, and nothing here needs the re-run because
        # `completed_from` sets everything that would change. The copy shares
        # the caller's metadata and transient reference, which the fold never
        # mutates, and `parenthash` is assigned rather than appended to.
        #
        # Complete once here so both the book and its auditable delta use the
        # same linked version; publishing the raw partial report loses terms.
        settled = copy.copy(order)
        # Completion records identifiers in `altids`; isolate the fold's copy
        # from the immutable input event before that map is extended.
        settled.altids = dict(order.altids)
        if standing is not None and settled.xhash and settled.xhash == standing.xhash:
            settled._completed_from_same_lifecycle(standing)
        elif standing is not None:
            # A typed identifier hit is the continuity proof. Synchronize
            # before generic completion so a newly assigned source code cannot
            # split the established lifecycle.
            settled.code = standing.code
            settled.xhash = standing.xhash
            settled.completed_from(standing)
        elif not settled.xhash or not settled.vhash or not settled.hash:
            settled.completed_from(None)
        settled = BookIterator.validate(settled)
        if not settled.vhash or not settled.hash:
            settled.identify()
        if (
            standing is not None
            and settled.xhash == standing.xhash
            and settled.vhash == standing.vhash
        ):
            return False, None
        if settled.state is State.INTERNAL_REJECTED:
            return False, settled
        if settled.lastpx is None:
            # No price even after completing: a market order, which rests
            # nowhere -- it is an execution against a side, not a level on
            # it. Skipped rather than refused, because an order stream really
            # does carry them and one of them is not a reason to abandon the
            # book.
            return False, settled
        if standing is not None and settled.state in (
            State.PENDING_CANCEL,
            State.PENDING_REPLACE,
        ):
            return False, self._hold_pending(standing, settled)
        before = _resting(standing) if standing else 0.0
        after = (
            0.0 if settled.state.is_terminal or settled.expires_on_arrival else _resting(settled)
        )
        if (
            standing is not None
            and settled.xhash == standing.xhash
            and settled.lastpx == standing.lastpx
            and after > 0
            and (level := self.levels.get(standing.lastpx)) is not None
        ):
            moved = after - before
            if moved:
                level.qty += moved
                self.changed[standing.lastpx] = None
            # Match the former leave/join path's equal-price priority without
            # deleting and bisecting the level itself.
            level.members.pop(settled.xhash, None)
            level.members[settled.xhash] = None
            level.priority.pop(settled.xhash, None)
            self._remember(settled)
            self._place(level, settled.xhash, after)
            return bool(moved), settled
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
            self._remember(settled)
            self._join(settled, after)
        moved = after - before
        if not moved and (standing is None or standing.lastpx == settled.lastpx):
            return False, settled
        return True, settled

    def _hold_pending(self, standing: Order, requested: Order) -> Order:
        """Publish a pending request while the acknowledged economics keep resting."""
        working = copy.copy(requested)
        for name in (
            "lastpx",
            "lastqty",
            "hiddenqty",
            "notional",
            "expunix",
            "timeinforce",
            "stoppx",
            "kind",
        ):
            setattr(working, name, getattr(standing, name))
        working.vhash = working.hash = NIL
        working.identify()

        old_xhash = standing.xhash
        if working.xhash != old_xhash:
            level = self.levels.get(standing.lastpx)
            if level is not None and old_xhash in level.members:
                level.members = {
                    working.xhash if xhash == old_xhash else xhash: None for xhash in level.members
                }
                at = level.resting.index(old_xhash)
                level.resting[at] = working.xhash
                level.priority[working.xhash] = level.priority.pop(old_xhash)
                self._moved(level)
            self._rekey(old_xhash, working.xhash)
        self._remember(working)
        return requested

    def revise_quantity(
        self,
        order: Order,
        current_qty: float | None,
        state: State | None = None,
    ) -> bool:
        """Revise a pending report Order without creating a second version."""
        standing = self.orders.get(order.xhash)
        before = _resting(standing) if standing is not None else 0.0
        if standing is not None:
            self._leave(standing)

        if state is not None:
            order.state = state
        displayed = (
            None
            if order.lastqty is None or order.hiddenqty is None
            else max(order.lastqty - order.hiddenqty, 0.0)
        )
        order.lastqty = current_qty
        if displayed is not None and current_qty is not None:
            order.hiddenqty = max(current_qty - displayed, 0.0)
        order.notional = None
        order.vhash = order.hash = NIL
        order.derive()
        order.identify()

        if standing is None:
            return False
        standing.lastqty = order.lastqty
        standing.hiddenqty = order.hiddenqty
        standing.notional = order.notional
        standing.state = order.state
        standing.vhash = order.vhash
        standing.hash = order.hash
        after = 0.0 if order.state.is_terminal or order.expires_on_arrival else _resting(order)
        if after <= 0:
            self._forget(standing.xhash)
        else:
            self._remember(standing)
            self._join(standing, after)
        return before != after

    def standing(self, event: Order | Execution) -> Order | None:
        """The live order matched by exact event links, then typed identifiers."""
        if event.is_order() and event.xhash:
            found = self.orders.get(event.xhash)
            if found is not None:
                return found
        for event_hash in event.linkhashes:
            xhash = self.hashes.get(event_hash)
            found = self.orders.get(xhash) if xhash is not None else None
            if found is not None:
                return found
        aliases = tuple(Order.lookup_altids_of(event))
        scope = int(event.lastmkt) if event.lastmkt else None
        venue = {value for namespace, value in aliases if namespace != CLIENT_ORDER_CODE}
        for key in aliases:
            scoped = self.named.get(key)
            if not scoped:
                continue
            if scope is not None:
                identity = scoped.get(scope, scoped.get(None))
            else:
                identity = next(iter(scoped.values())) if len(scoped) == 1 else None
            found = self.orders.get(identity) if identity is not None else None
            if found is None:
                continue
            if key[0] == CLIENT_ORDER_CODE and venue:
                standing_venue = {
                    value
                    for _, namespace, value in self.aliases.get(found.xhash, ())
                    if namespace != CLIENT_ORDER_CODE
                }
                if standing_venue and venue.isdisjoint(standing_venue):
                    continue
            return found
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
        px = order.lastpx
        level = self.levels.get(px)
        if level is None:
            key = px * self.facing
            level = self.levels[px] = _LevelState(px=px)
            at = bisect.bisect_left(self.keys, key)
            self.keys.insert(at, key)
            self.alive.insert(at, level)
        level.qty += quantity
        level.members[order.xhash] = None
        self._place(level, order.xhash, quantity)
        self._touch(px, level)

    def _leave(self, order: Order) -> None:
        """Take `order` off its level, dropping the level when it empties.

        A level that reaches zero is dropped rather than kept at zero: a level
        of nothing is not a level, and leaving it would put an empty price in
        every `alive` list from then on.
        """
        level = self.levels.get(order.lastpx)
        if level is None:
            return
        quantity = _resting(order)
        level.qty -= quantity
        level.members.pop(order.xhash, None)
        self._unplace(level, order.xhash)
        self._touch(order.lastpx, level)
        if not level.members or level.qty <= 0:
            del self.levels[order.lastpx]
            at = bisect.bisect_left(self.keys, order.lastpx * self.facing)
            self.keys.pop(at)
            self.alive.pop(at)

    def _forget(self, xhash: int) -> None:
        """Drop one order and every cached code that still points at it."""
        self.orders.pop(xhash, None)
        self._deadline_tokens.pop(xhash, None)
        self._deadline_values.pop(xhash, None)
        for event_hash in self.version_hashes.pop(xhash, ()):
            if self.hashes.get(event_hash) == xhash:
                del self.hashes[event_hash]
        for scope, namespace, value in self.aliases.pop(xhash, ()):
            key = (namespace, value)
            scoped = self.named.get(key)
            if scoped is None or scoped.get(scope) != xhash:
                continue
            del scoped[scope]
            if not scoped:
                del self.named[key]
        self._compact_deadlines()

    def _rekey(self, old: int, new: int) -> None:
        """Move one lifecycle's state and code cache without scanning either index."""
        self.orders.pop(old, None)
        self._deadline_tokens.pop(old, None)
        self._deadline_values.pop(old, None)
        aliases = self.aliases.pop(old, {})
        if aliases:
            self.aliases[new] = aliases
            for scope, namespace, value in aliases:
                scoped = self.named.get((namespace, value))
                if scoped is not None and scoped.get(scope) == old:
                    scoped[scope] = new
        versions = self.version_hashes.pop(old, {})
        if versions:
            self.version_hashes[new] = versions
            for event_hash in versions:
                if self.hashes.get(event_hash) == old:
                    self.hashes[event_hash] = new
        self._compact_deadlines()

    def _remember(self, order: Order) -> None:
        """Store one live order, its bounded code index, and its lazy deadline.

        A changed version of a resting order has a new value hash even where it
        stands at the same price for the same quantity, so its level forgets
        what it settled into whether or not the level itself moved.
        """
        xhash = order.xhash
        self.orders[xhash] = order
        if order.hash:
            versions = self.version_hashes.setdefault(xhash, {})
            versions[order.hash] = None
            self.hashes[order.hash] = xhash
            while len(versions) > _ORDER_HASH_LIMIT:
                expired = next(iter(versions))
                del versions[expired]
                if self.hashes.get(expired) == xhash:
                    del self.hashes[expired]
        scope = int(order.lastmkt) if order.lastmkt else None
        current = dict.fromkeys((scope, *key) for key in Order.lookup_altids_of(order))
        previous = self.aliases.get(xhash, {})
        remembered = {
            alias: None
            for alias in previous
            if self.named.get((alias[1], alias[2]), {}).get(alias[0]) in (None, xhash)
        }
        remembered.update(current)
        pinned = set(current)
        if order.code:
            pinned.update(alias for alias in remembered if alias[2] == order.code)
        for alias in tuple(remembered):
            if len(remembered) <= _ORDER_ALIAS_LIMIT:
                break
            if alias not in pinned:
                del remembered[alias]
        for old_scope, namespace, value in previous.keys() - remembered.keys():
            key = (namespace, value)
            scoped = self.named.get(key)
            if scoped is None or scoped.get(old_scope) != xhash:
                continue
            del scoped[old_scope]
            if not scoped:
                del self.named[key]
        self.aliases[xhash] = remembered
        for current_scope, namespace, value in remembered:
            if (current_scope, namespace, value) not in current:
                scoped = self.named.get((namespace, value), {})
                if scoped.get(current_scope) not in (None, xhash):
                    continue
            self.named.setdefault((namespace, value), {})[current_scope] = xhash
        self._moved(order.lastpx)
        self._index_deadline(order)

    def _index_deadline(self, order: Order) -> None:
        """Push the order's earliest expiry; older entries become stale tokens."""
        current = self._deadline_of(order)
        if current is None:
            self._deadline_tokens.pop(order.xhash, None)
            self._deadline_values.pop(order.xhash, None)
            self._compact_deadlines()
            return
        deadline = current[0]
        if self._deadline_values.get(order.xhash) == deadline:
            return
        self._next_deadline_token += 1
        token = self._next_deadline_token
        self._deadline_tokens[order.xhash] = token
        self._deadline_values[order.xhash] = deadline
        heapq.heappush(self._deadlines, (deadline, order.xhash, token))
        self._compact_deadlines()

    def _discard_stale_deadlines(self) -> None:
        """Pop invalid heap heads before a due-time query reads one."""
        while self._deadlines:
            _, xhash, token = self._deadlines[0]
            if self._deadline_tokens.get(xhash) == token:
                return
            heapq.heappop(self._deadlines)

    def _compact_deadlines(self) -> None:
        """Bound lazy stale entries while keeping deadline updates O(log n)."""
        active = len(self._deadline_tokens)
        if not active:
            self._deadlines.clear()
            return
        if len(self._deadlines) <= active * 2 + _DEADLINE_STALE_BUFFER:
            return
        self._deadlines = [
            entry for entry in self._deadlines if self._deadline_tokens.get(entry[1]) == entry[2]
        ]
        heapq.heapify(self._deadlines)

    def _deadline_of(self, order: Order) -> tuple[int, str] | None:
        """The earliest explicit or configured age deadline and its audit reason."""
        explicit = order.expunix
        age = (
            None
            if self.max_order_age_ns is None
            else (order.creaunix or order.unix) + self.max_order_age_ns
        )
        if explicit is None and age is None:
            return None
        if explicit is not None and (age is None or explicit <= age):
            return explicit, "order reached its explicit expiry"
        assert age is not None
        return age, "order exceeded the configured live age"

    def take(self, execution: Execution, traded: float) -> None:
        """Record a trade against this side, and take `traded` out of it."""
        hit = self.standing(execution)
        if hit is not None:
            if execution.leavesqty is not None:
                # ExecutionReport carries the post-trade total. The Order row
                # emitted from that same report may already have applied it;
                # subtracting LastQty again double-counts the fill.
                hidden = max(hit.hiddenqty or 0.0, 0.0)
                post_trade = max(execution.leavesqty - hidden, 0.0)
                traded = max(_resting(hit) - post_trade, 0.0)
            if traded > 0:
                self._reduce(hit, traded)
            return
        while traded > 0 and self.alive:
            top = self.keys[0]
            level = self.alive[0]
            # A tuple, because reducing an order can empty the level and edit
            # the resting order this is walking.
            for xhash in tuple(level.resting):
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
        self._touch(order.lastpx)
        if left <= 0:
            self.remove(order.xhash)
        else:
            level = self.levels.get(order.lastpx)
            if level is not None:
                level.qty -= taken
            if order.lastqty is not None:
                revised = copy.copy(order)
                revised.lastqty = max(revised.lastqty - taken, 0.0)
                revised.vhash = revised.hash = NIL
                revised.derive()
                revised.identify()
                self._remember(revised)
                if level is not None:
                    self._place(level, order.xhash, _resting(revised))
        return traded - taken


# -- folding a book out of a stream ------------------------------------------


@dataclasses.dataclass
class _Folding:
    """One instrument's mutable state inside a `BookIterator`.

    Everything a fold has to remember between events, and nothing that is a
    row: the two sides, what has been emitted, and the identities that are
    constant for the whole instrument and so are computed once rather than per
    book.
    """

    symbolticker: str
    """Canonical nonblank ticker that owns this state."""

    bid: _Side
    """The bid side's live orders, best first."""

    ask: _Side
    """The ask side's live orders, best first."""

    xhash: int = NIL
    """The book's lifecycle. One instrument is one book, so this never moves."""

    creaunix: int = 0
    """The first upstream creation clock retained across book versions."""

    code: str = ""
    """Readable code the book lifecycle was first identified by."""

    codesource: str = ""
    """Field that supplied the book lifecycle's readable code."""

    unix: int | None = None
    """The instant the events being folded belong to; None before the first."""

    previous: Book | None = None
    """The last book emitted, which the next one is a version of."""

    about: MarketEvent | None = None
    """The last event folded in -- where a book learns what it is a book of."""

    instrument: Instrument = dataclasses.field(default_factory=Instrument)
    """Transient instrument facts carried by the latest event that stated them."""

    emitted: int | None = None
    """`unix` of the last book emitted, which is what an hourly snapshot counts from."""

    moved: bool = False
    """Whether anything has happened since the last book was emitted."""

    deltas: list[Order] = dataclasses.field(default_factory=list)
    """Complete order transitions awaiting the next delta row."""

    executions: list[Execution] = dataclasses.field(default_factory=list)
    """Complete execution evidence awaiting the next delta row."""

    reported: dict[int, Order] = dataclasses.field(default_factory=dict)
    """Generated Orders by source hash until their report's Execution arrives."""

    reported_names: dict[tuple[str, str], int] = dataclasses.field(default_factory=dict)
    """Pending typed source identifiers to their Order source hash."""

    unpublished: set[int] = dataclasses.field(default_factory=set)
    """Report Orders deferred because LastQty is needed to distinguish their change."""

    linkhashes: dict[int, None] = dataclasses.field(default_factory=dict)
    """Delta relations accumulated once in insertion order."""

    parent_hashes: dict[int, None] = dataclasses.field(default_factory=dict)
    """Delta parent versions accumulated once in insertion order."""


@dataclasses.dataclass
class BookIterator:
    """Fold one sorted parsed-log stream into books."""

    logs: Iterable[FixMsg] = ()
    """Parsed logs sorted by event time, `msgseqnum`, and hash; read once."""

    registry: FixRegistry | None = None
    """FIX dictionary that owns dispatch and lifecycle value mappings."""

    snapshot_every: int = HOUR
    """Emit the book on every multiple of this; `0` emits only what changed."""

    snapshot_until: int | None = None
    """On flush, complete boundaries before this exclusive instant; None does not guess."""

    snapshots: Iterable[Book] = ()
    """Prior snapshots, normalized to the latest version of each book."""

    max_order_age_ns: int | None = DAY
    """Expire unchanged orders after one day; None uses explicit `expunix` only."""

    max_side_alive: int | None = None
    """Keep at most this many live orders per side; None keeps every order."""

    # Not a bound like the two above but a decision about the end of the
    # stream, which neither of them can express: a window that ends is not the
    # same as an order that aged out, and a reader of the last book cannot tell
    # a still-resting order from one nobody ever cancelled. False -- the
    # default -- leaves them resting, which is what a run that will be resumed
    # from its snapshots wants.
    purge_alive: bool = False
    """Expire whatever is still resting when the stream ends, as auditable versions."""

    folding: dict[str, _Folding] = dataclasses.field(default_factory=dict)
    """Mutable fold state keyed by canonical nonblank `symbolticker`."""

    def __post_init__(self) -> None:
        if self.snapshot_every < 0:
            raise ValueError("snapshot_every must be non-negative")
        if self.max_side_alive is not None and self.max_side_alive < 0:
            raise ValueError("max_side_alive must be non-negative")
        if self.max_order_age_ns is not None and self.max_order_age_ns < 0:
            raise ValueError("max_order_age_ns must be non-negative")
        self._source: Iterator[MarketEvent] | None = None
        self._event_input: Iterable[MarketEvent] | None = None
        self._finished = False
        self._books: deque[Book] = deque()
        self._unix: int | None = None
        self._swept: int | None = None
        latest: dict[str, Book] = {}
        for snapshot in self.snapshots:
            ticker = snapshot.symbolticker
            if not ticker:
                raise ValueError("a recovery snapshot requires a nonblank `symbolticker`")
            current = latest.get(ticker)
            if current is None or (snapshot.unix, snapshot.version, snapshot.hash) > (
                current.unix,
                current.version,
                current.hash,
            ):
                latest[ticker] = snapshot
        self.snapshots = tuple(
            sorted(latest.values(), key=lambda row: (row.unix, row.symbolticker))
        )
        for snapshot in self.snapshots:
            self._restore(snapshot)

    @classmethod
    def from_events(cls, events: Iterable[MarketEvent], **declared: Any) -> BookIterator:
        """Build a fold over one ordered stream of translated event versions."""
        built = cls(**declared)
        built._event_input = events
        return built

    # -- the stream ----------------------------------------------------------

    def __iter__(self) -> Iterator[Book]:
        """The books, which is what a fold is usually asked for."""
        return self.books

    @property
    def books(self) -> Iterator[Book]:
        """Every book this stream produces, in the order the events arrived."""
        return self._drain()

    def _drain(self) -> Iterator[Book]:
        """Hand back held books, then fold only until another is available."""
        while True:
            while self._books:
                yield self._books.popleft()
            if not self._advance():
                return

    def _advance(self) -> bool:
        """Fold one more event, or close the stream. False when there is no more.

        `_finished` and not "the source is None", which is the same question
        asked in a way that answers wrong: the source is also None before the
        first pull, so closing on it made the next pull start the stream over
        -- and a finite stream folded forever.
        """
        if self._finished:
            return False
        if self._source is None:
            self._source = iter(self._events())
        event = next(self._source, None)
        if event is None:
            self._finished = True
            self._source = None
            books = len(self._books)
            self._flush()
            self._order_output(books)
            return bool(self._books)
        books = len(self._books)
        self._feed(event)
        self._order_output(books)
        return True

    def _events(self) -> Iterator[MarketEvent]:
        """Translate each parsed market row once."""
        if self._event_input is not None:
            yield from self._event_input
            return
        for log in self.logs:
            if log.eventtype is EventType.INSTRUMENT:
                continue
            yield from log.into_market_events(registry=self.registry)

    def _order_output(self, books: int) -> None:
        """Keep checkpoint commits globally boundary-ordered across instruments."""
        if len(self._books) - books > 1 or (
            books and len(self._books) > books and self._books[-2].unix > self._books[-1].unix
        ):
            ordered = sorted(self._books, key=lambda row: row.unix)
            self._books.clear()
            self._books.extend(ordered)

    # -- folding --------------------------------------------------------------

    def _feed(self, event: MarketEvent) -> None:
        """One event into the state of the instrument it is about."""
        if self._unix is not None and event.unix < self._unix:
            raise ValueError(
                f"a book is folded in time order: {event.unix} came after {self._unix}. "
                "Sort the stream on `unix` first"
            )
        self._unix = event.unix
        state = self._state_of(event)
        # An active instrument has one row per instant. Exact-boundary events
        # become that boundary's delta; only inactive instruments snapshot on
        # the boundary through `_sweep`.
        self._settle(state, event.unix)
        self._refresh_instrument(state, event)
        self._sweep(event.unix, state)
        state.moved = self._expire(state, event.unix) or state.moved
        source_hash = event.hash
        paired = self._paired_execution(state, event) if isinstance(event, Execution) else None
        if paired is not None:
            folded, settled = paired
        else:
            folded, settled = _folded(state.bid, state.ask, event)
        if settled is None:
            if isinstance(event, Order):
                # A report Order can look unchanged until its following
                # Execution supplies LastQty. Keep it only for that pair.
                self._remember_reported(
                    state,
                    source_hash,
                    event,
                    unpublished=True,
                )
                state.unix = event.unix
            return
        state.unix = settled.unix
        if isinstance(settled, Order):
            state.deltas.append(settled)
            self._remember_reported(state, source_hash, settled)
            bounded = self.max_side_alive is not None and self._bound(state, settled.unix)
        elif isinstance(settled, Execution):
            state.executions.append(settled)
            bounded = False
        else:
            bounded = False
        if isinstance(settled, Order | Execution):
            self._remember_pending(state, settled)
        state.moved = folded or bounded or isinstance(settled, Order | Execution) or state.moved
        # After folding, never before: a book is described by the events it
        # holds, and reading the event that *triggered* the emission gave
        # every row the units of the instant after it.
        state.about = settled

    @staticmethod
    def _remember_reported(
        state: _Folding,
        source_hash: int,
        order: Order,
        *,
        unpublished: bool = False,
    ) -> None:
        """Index one pending report Order by source, lifecycle, and names."""
        key = source_hash or order.hash
        if not key:
            return
        state.reported[key] = order
        for code in Order.lookup_altids_of(order):
            state.reported_names[code] = key
        if unpublished:
            state.unpublished.add(key)

    @staticmethod
    def _reported_for_execution(state: _Folding, execution: Execution) -> tuple[int, Order] | None:
        """Find this instant's resulting Order by source, lifecycle, or name."""
        for parent in execution.parenthash or ():
            if parent in state.reported:
                return parent, state.reported[parent]

        candidates = [
            linked if linked in state.reported else None for linked in execution.linkhashes
        ]
        candidates.extend(
            state.reported_names.get(code) for code in Order.lookup_altids_of(execution)
        )
        for key in dict.fromkeys(candidate for candidate in candidates if candidate is not None):
            order = state.reported.get(key)
            if order is None or order.unix != execution.unix:
                continue
            if order.state.rank < State.PARTIAL.rank:
                continue
            return key, order
        return None

    def _paired_execution(
        self, state: _Folding, execution: Execution
    ) -> tuple[bool, Execution] | None:
        """Apply a report's generated Order once and retain its Execution as evidence."""
        source_execution_hash = execution.hash
        matched = self._reported_for_execution(state, execution)
        if matched is None:
            return None
        source_parent, paired = matched

        side = state.bid if paired.side.sign > 0 else state.ask if paired.side.sign < 0 else None
        if side is None:
            side = (
                state.bid
                if state.bid.standing(paired) is not None
                else state.ask
                if state.ask.standing(paired) is not None
                else None
            )
        published = source_parent not in state.unpublished
        paired_moved = False
        if not published and side is not None:
            standing = side.standing(paired)
            previous_qty = standing.lastqty if standing is not None else paired.prevqty
            transition = _quantity_transition(
                paired.state,
                execution_state=execution.state,
                previous_qty=previous_qty,
                leavesqty=execution.leavesqty,
                last_qty=execution.lastqty if execution.state is State.FILLED else None,
                order_qty=paired.lastqty,
            )
            candidate = copy.copy(paired)
            candidate.prevqty = transition.previous_qty
            candidate.lastqty = transition.current_qty
            candidate.vhash = candidate.hash = NIL
            paired_moved, resulting = side._applied(candidate)
            if resulting is not None:
                paired = resulting
                self._remember_reported(
                    state,
                    source_parent,
                    paired,
                )
                state.unpublished.discard(source_parent)
                state.deltas.append(paired)
                self._remember_pending(state, paired)

        last_qty = execution.lastqty if execution.state is State.FILLED else None
        transition = _quantity_transition(
            paired.state,
            execution_state=execution.state,
            previous_qty=(
                paired.prevqty
                if paired.prevqty is not None
                else paired.lastqty
                if last_qty is not None
                else None
            ),
            leavesqty=execution.leavesqty,
            last_qty=last_qty,
            order_qty=paired.lastqty if paired.prevqty is None and last_qty is None else None,
        )
        old_hash = paired.hash
        old_vhash = paired.vhash
        previous_changed = paired.prevqty is None and transition.previous_qty is not None
        state_changed = paired.state is not transition.state
        if previous_changed:
            paired.prevqty = transition.previous_qty
        moved = False
        if side is not None and (
            transition.current_qty != paired.lastqty or previous_changed or state_changed
        ):
            moved = side.revise_quantity(paired, transition.current_qty, transition.state)
        elif previous_changed or state_changed:
            paired.state = transition.state
            paired.lastqty = transition.current_qty
            paired.derive()
            paired.vhash = paired.hash = NIL
            paired.identify()
        if paired.vhash != old_vhash:
            state.parent_hashes.pop(old_hash, None)
            state.parent_hashes[paired.hash] = None
            state.linkhashes.pop(old_hash, None)
            state.linkhashes[paired.hash] = None

        stale_order_hashes = {source_parent, old_hash, paired.hash}
        execution.linkhashes = [
            linked for linked in execution.linkhashes if linked not in stale_order_hashes
        ]
        execution.parenthash = list(
            dict.fromkeys(
                paired.hash if state.reported.get(parent) is paired else parent
                for parent in execution.parenthash or ()
            )
        )
        execution.completed_from(paired)
        execution.identify()
        settled = self.validate(execution)
        paired.linkhashes = [
            linked for linked in paired.linkhashes if linked != source_execution_hash
        ]
        paired.link_to(settled)
        settled.link_to(paired, primary=True)
        return moved or paired_moved, settled

    @staticmethod
    def _remember_pending(state: _Folding, event: Order | Execution) -> None:
        """Accumulate one delta's event and parent relations without rescanning it."""
        if event.hash:
            state.linkhashes[event.hash] = None
        if event.hash:
            state.parent_hashes[event.hash] = None

    def _sweep(self, unix: int, folded: _Folding) -> None:
        """Fill in the hourly rows of every *other* instrument the clock passed."""
        every = self.snapshot_every
        if not every or len(self.folding) < 2:
            return
        boundary = unix - unix % every
        if self._swept is not None and boundary <= self._swept:
            return
        self._swept = boundary
        for state in self.folding.values():
            if state is not folded:
                self._settle(state, unix, inclusive=True)
                if self._expire(state, unix):
                    state.unix = unix
                    state.moved = True

    def _flush(self) -> None:
        """Emit the last delta and any explicitly bounded quiet-tail snapshots."""
        for state in self.folding.values():
            if state.unix is not None:
                until = state.unix + 1
                if self.snapshot_until is not None:
                    until = max(until, self.snapshot_until)
                if self.purge_alive and self._purge(state, state.unix):
                    # At `state.unix` and not at `until`: the orders were alive
                    # up to the last event, so the version that ends them
                    # belongs to that instant and not to a boundary past it.
                    state.moved = True
                self._settle(state, until)

    def _purge(self, state: _Folding, unix: int) -> bool:
        """End every order still resting on both sides, as `purge_alive` asks."""
        reason = "order was still resting when the stream ended"
        expired = [*state.bid.purge(unix, reason), *state.ask.purge(unix, reason)]
        state.deltas.extend(expired)
        for event in expired:
            self._remember_pending(state, event)
        return bool(expired)

    def _state_of(self, event: MarketEvent) -> _Folding:
        """The fold this event belongs to, by its canonical ticker identity."""
        ticker = event.symbolticker
        if not ticker:
            raise ValueError("a book event requires a nonblank `symbolticker`")
        known = self.folding.get(ticker)
        if known is not None:
            event.instrumentxhash = known.instrument.xhash
            return known
        state = self._started(event)
        event.instrumentxhash = state.instrument.xhash
        self.folding[ticker] = state
        return state

    def _started(self, event: MarketEvent) -> _Folding:
        """The state one instrument's fold starts from, identities and all."""
        ticker = event.symbolticker
        parsed = event.into_instrument()
        instrument = (
            parsed
            if parsed is not None and parsed.symbolticker == ticker
            else Instrument(symbolticker=ticker, currency=event.currency)
        )
        creation = event.creaunix or event.unix
        lifecycle = Book(
            unix=event.unix,
            creaunix=creation,
            plugin=event.plugin,
            instrumentxhash=instrument.xhash,
            symbolticker=ticker,
            altids=dict(event.altids),
            currency=event.currency,
        )
        lifecycle.attach_instrument(instrument)
        xhash = lifecycle.life_hash()
        state = _Folding(
            symbolticker=ticker,
            bid=_Side(side=Side.BID, max_order_age_ns=self.max_order_age_ns),
            ask=_Side(side=Side.ASK, max_order_age_ns=self.max_order_age_ns),
            xhash=xhash,
            creaunix=creation,
            code=lifecycle.life_code(),
            codesource=lifecycle.life_code_source(),
            instrument=instrument,
        )
        return state

    def _restore(self, snapshot: Book) -> None:
        """Resume one fold from the complete live state of a prior snapshot."""
        if snapshot.snapunix is None:
            raise ValueError("a recovery seed must be a book snapshot with `snapunix`")
        if snapshot.deltas or snapshot.executions:
            raise ValueError("a recovery snapshot cannot carry delta events")
        if snapshot.biddepth != len(snapshot.bidlevels):
            raise ValueError("recovery snapshot biddepth does not match bidlevels")
        if snapshot.askdepth != len(snapshot.asklevels):
            raise ValueError("recovery snapshot askdepth does not match asklevels")
        ticker = snapshot.symbolticker
        if not ticker:
            raise ValueError("a recovery snapshot requires a nonblank `symbolticker`")
        parsed = snapshot.into_instrument()
        instrument = (
            parsed
            if parsed is not None and parsed.symbolticker == ticker
            else Instrument(symbolticker=ticker, currency=snapshot.currency)
        )
        snapshot.instrumentxhash = instrument.xhash
        snapshot.attach_instrument(instrument)
        state = _Folding(
            symbolticker=ticker,
            bid=_Side.from_snapshot(
                Side.BID,
                snapshot.bidlevels,
                snapshot.bidalive,
                self.max_order_age_ns,
            ),
            ask=_Side.from_snapshot(
                Side.ASK,
                snapshot.asklevels,
                snapshot.askalive,
                self.max_order_age_ns,
            ),
            xhash=snapshot.xhash,
            creaunix=snapshot.creaunix,
            code=snapshot.code,
            codesource=snapshot.codesource,
            unix=snapshot.unix,
            previous=snapshot,
            about=snapshot,
            instrument=instrument,
            emitted=snapshot.unix,
        )
        state.moved = self._bound(state, snapshot.unix)
        self.folding[ticker] = state
        self._unix = max(self._unix or snapshot.unix, snapshot.unix)
        self._swept = max(self._swept or snapshot.unix, snapshot.unix)

    def _expire(self, state: _Folding, unix: int) -> bool:
        """Expire stale orders on both sides before events at `unix` apply."""
        bid_due = state.bid.has_due(unix)
        ask_due = state.ask.has_due(unix)
        if not bid_due and not ask_due:
            return False
        expired = state.bid.expire(unix) if bid_due else []
        if ask_due:
            expired.extend(state.ask.expire(unix))
        state.deltas.extend(expired)
        for event in expired:
            self._remember_pending(state, event)
        return bool(expired)

    def _bound(self, state: _Folding, unix: int) -> bool:
        """Apply the configured per-side live-order bound."""
        if self.max_side_alive is None:
            return False
        expired = []
        if len(state.bid.orders) > self.max_side_alive:
            expired.extend(state.bid.bound(self.max_side_alive, unix))
        if len(state.ask.orders) > self.max_side_alive:
            expired.extend(state.ask.bound(self.max_side_alive, unix))
        state.deltas.extend(expired)
        for event in expired:
            self._remember_pending(state, event)
        return bool(expired)

    @staticmethod
    def validate(event: MarketEvent) -> MarketEvent:
        """Reject incomplete book inputs while preserving an existing reason."""
        reasons: list[str] = []
        terminal_changed = False
        if isinstance(event, Order) and event.state.is_terminal:
            terminal_changed = event.lastqty != 0.0 or event.hiddenqty != 0.0
            event.lastqty = 0.0
            event.hiddenqty = 0.0
        validates_interest = (
            event.is_order()
            and not event.state.is_terminal
            and event.state not in (State.UNKNOWN, State.PENDING_CANCEL)
        )
        if validates_interest:
            quantity = event.lastqty
            kind = getattr(event, "kind", MarketKind.UNKNOWN)
            if not event.side.sign:
                reasons.append("side is missing")
            price_required = kind.band is MarketKind.LIMIT or kind is MarketKind.STOP_LIMIT
            invalid_price = event.lastpx is not None and not math.isfinite(event.lastpx)
            if price_required and event.lastpx is None or invalid_price:
                reasons.append("required price is missing or non-finite")
            if quantity is None or not math.isfinite(quantity) or quantity <= 0:
                reasons.append("quantity is missing or non-positive")
        elif event.is_execution() and event.state is State.FILLED:
            if event.lastpx is None or not math.isfinite(event.lastpx):
                reasons.append("price is missing or non-finite")
            if event.lastqty is None or not math.isfinite(event.lastqty) or event.lastqty <= 0:
                reasons.append("quantity is missing or non-positive")
        if not reasons:
            if terminal_changed:
                event.vhash = event.hash = NIL
                event.identify()
            return event
        event.state = State.INTERNAL_REJECTED
        if isinstance(event, Order):
            event.lastqty = 0.0
            event.hiddenqty = 0.0
        if not event.reason:
            event.reason = "rejected for book: " + "; ".join(reasons)
        event.vhash = event.hash = NIL
        return event.identify()

    def _refresh_instrument(self, state: _Folding, event: MarketEvent) -> None:
        """Keep transient instrument facts when this event carries them."""
        parsed = event.into_instrument()
        if parsed is not None and parsed.symbolticker == state.symbolticker:
            state.instrument = parsed

    # -- emitting -------------------------------------------------------------

    def _settle(self, state: _Folding, unix: int, *, inclusive: bool = False) -> None:
        """Emit whatever `unix` completes: the instant that ended, then the hours."""
        if state.unix is not None and unix != state.unix and state.moved:
            self._emit(state, state.unix)
            state.moved = False
        elif state.unix is not None and unix != state.unix and state.unpublished:
            state.reported.clear()
            state.reported_names.clear()
            state.unpublished.clear()
        self._snapshots(state, unix, inclusive=inclusive)

    def _snapshots(self, state: _Folding, unix: int, *, inclusive: bool = False) -> None:
        """One book on every boundary between the last emission and `unix`.

        Every boundary, and not just the latest: an hourly table whose rows
        skip the hours nothing happened in is a table you have to scan
        backwards to read, which is the thing hourly rows exist to avoid. A
        gap of ten hours is ten rows, each a picture of the same state, each
        saying so in `snapunix`.
        """
        every = self.snapshot_every
        if not every or state.previous is None or state.emitted is None:
            return
        boundary = state.emitted - state.emitted % every + every
        while boundary < unix or (inclusive and boundary == unix):
            # A deadline is state, not a later event's side effect. Apply it at
            # the first observable boundary after it was reached, before a
            # recovery picture can repeat an order that is already dead. The
            # changed row at that boundary replaces the unchanged snapshot,
            # just as a venue event exactly on a boundary does, and carries the
            # synthetic EXPIRED delta once.
            if self._expire(state, boundary):
                state.unix = boundary
                state.moved = True
                self._emit(state, boundary)
                state.moved = False
                boundary += every
                continue
            if not self._snapshot_book(state, boundary):
                break
            boundary += every

    def _snapshot_book(self, state: _Folding, unix: int) -> bool:
        """Append one full recovery picture at an otherwise quiet instant."""
        previous = state.previous
        if previous is None:
            return False
        taken = previous.make_snapshot(unix)
        if taken is None:
            return False
        taken.bidlevels = state.bid.into_levels()
        taken.asklevels = state.ask.into_levels()
        taken.deltas = []
        taken.executions = []
        taken.bidalive = state.bid.into_orders()
        taken.askalive = state.ask.into_orders()
        bid_vhashes, bid_frame = state.bid.order_value_identity()
        ask_vhashes, ask_frame = state.ask.order_value_identity()
        taken._remember_alive_vhashes(
            bid_vhashes,
            ask_vhashes,
            bid_frame=bid_frame,
            ask_frame=ask_frame,
        )
        alive = [*taken.bidalive, *taken.askalive]
        taken.linkhashes = list(dict.fromkeys(order.hash for order in alive if order.hash))
        taken.parenthash = [order.hash for order in alive if order.hash] or None
        linked = taken.with_previous(previous)
        if linked is None:
            return False
        state.previous = linked
        state.emitted = unix
        self._books.append(linked)
        return True

    def _emit(self, state: _Folding, unix: int) -> None:
        """The book as it stands, as a new row, and the delta handed over with it."""
        settled = _settled(state, unix)
        if settled is None:
            return
        state.previous = settled
        state.emitted = unix
        self._books.append(settled)


def _folded(bid: _Side, ask: _Side, event: MarketEvent) -> tuple[bool, MarketEvent | None]:
    """Apply one event and return `(moved, settled event)`.

    A shape that is itself a book is skipped rather than refused: a stream
    read off a table may carry the book rows this produced beside the
    orders that produced them, and folding a book into a book is not a
    thing that means anything.
    """
    if event.is_order():
        side = bid if event.side.sign > 0 else ask if event.side.sign < 0 else None
        if side is None:
            side = bid if bid.standing(event) is not None else ask if ask.standing(event) else None
        # The price is *not* checked here: a cancellation carries none, and
        # gets one from the version it cancels. `_Side.apply` decides,
        # after completing.
        if side is None:
            settled = BookIterator.validate(event)
            return False, settled
        moved, settled = side._applied(event)
        return moved, settled
    if event.is_execution():
        settled = BookIterator.validate(event)
        return _traded(bid, ask, settled), settled
    return False, event


def _traded(bid: _Side, ask: _Side, execution: Execution) -> bool:
    """A new fill taken from its side; amendments need their referenced delta."""
    if (
        execution.state is State.INTERNAL_REJECTED
        or execution.state is not State.FILLED
        or execution.lastpx is None
        or execution.lastqty is None
    ):
        return False
    side = _hit(bid, ask, execution)
    if side is None:
        return False
    side.take(execution, abs(execution.lastqty))
    return True


def _hit(bid: _Side, ask: _Side, execution: Execution) -> _Side | None:
    """Which side a fill took liquidity out of, by what the report actually says."""
    if execution.side.sign > 0:
        return bid
    if execution.side.sign < 0:
        return ask
    if bid.standing(execution) is not None:
        return bid
    if ask.standing(execution) is not None:
        return ask
    best_bid, best_ask = bid.best_level, ask.best_level
    if best_bid is None and best_ask is None:
        return None
    if best_bid is None:
        return ask
    if best_ask is None:
        return bid
    mid = ((best_bid.px or 0.0) + (best_ask.px or 0.0)) / 2
    return bid if (execution.lastpx or 0.0) <= mid else ask


def _settled(state: _Folding, unix: int) -> Book | None:
    """The book as it stands, as a new row -- and the delta handed over with it."""
    about = state.about
    # Transient reference facts never form a second stream; the canonical
    # ticker columns remain on every persisted market row.
    previous = state.previous
    bid_best = state.bid.best_level
    ask_best = state.ask.best_level
    bidlevels = state.bid.into_changed_levels() if state.bid.changed else []
    asklevels = state.ask.into_changed_levels() if state.ask.changed else []
    book = Book(
        unix=unix,
        creaunix=state.creaunix,
        plugin=about.plugin,
        instrumentxhash=state.instrument.xhash,
        symbolticker=state.symbolticker,
        code=state.code,
        codesource=state.codesource,
        altids=dict(about.altids),
        pxunit=about.pxunit,
        currency=about.currency,
        qtyunit=about.qtyunit,
        lastmkt=about.lastmkt,
        state=State.OPEN if (state.bid.keys or state.ask.keys) else State.CLOSED,
        xhash=state.xhash,
        linkhashes=list(state.linkhashes),
        parenthash=list(state.parent_hashes) or None,
        bidpx=None if bid_best is None else bid_best.px,
        bidqty=None if bid_best is None else bid_best.qty,
        biddepth=state.bid.depth,
        askpx=None if ask_best is None else ask_best.px,
        askqty=None if ask_best is None else ask_best.qty,
        askdepth=state.ask.depth,
        bidlevels=bidlevels,
        asklevels=asklevels,
        deltas=list(state.deltas),
        executions=list(state.executions),
    )
    bid_vhashes, bid_frame = state.bid.order_value_identity()
    ask_vhashes, ask_frame = state.ask.order_value_identity()
    book._remember_alive_vhashes(
        bid_vhashes,
        ask_vhashes,
        bid_frame=bid_frame,
        ask_frame=ask_frame,
    )
    book.attach_instrument(state.instrument)
    state.bid.cleared()
    state.ask.cleared()
    state.deltas.clear()
    state.executions.clear()
    state.reported.clear()
    state.reported_names.clear()
    state.unpublished.clear()
    state.linkhashes.clear()
    state.parent_hashes.clear()
    # The prices across the sides are `Book.derive`'s, which `with_previous`
    # runs once every layer has filled -- so they are not computed here as
    # well, and the value hash it ends with is of a row that already has them.
    return book.with_previous(previous)
