"""Compact price levels and deterministic book folding."""

from __future__ import annotations

import bisect
import copy
import dataclasses
import functools
import heapq
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
from rekep.market.identity import NIL
from rekep.market.instrument import Instrument
from rekep.market.orders import Execution, Order

if TYPE_CHECKING:
    from rekep.text.log import Log

_ARROW_DISPATCH = MappingProxyType(
    {
        pyarrow.RecordBatch: "arrow_batch",
        pyarrow.Table: "arrow_table",
    }
)


@scalar(slots=True, weakref_slot=True)
class Level(MarketConvertible):
    """One compact price level and the event lifecycles linked to it."""

    px: Annotated[float, fix_tag("MDEntryPx")] = 0.0
    """Price of the level."""

    qty: Annotated[float, fix_tag("MDEntrySize")] = 0.0
    """Quantity resting at that price."""

    order_xhash: list[int] = dataclasses.field(default_factory=list)
    """Live order lifecycles at this price, in arrival order."""

    exec_xhash: list[int] = dataclasses.field(default_factory=list)
    """Execution lifecycles that changed this level in this delta."""


@scalar(slots=True)
class Book(MarketEvent):
    """Both sides of one book, flat, and the prices that only exist across them."""

    @classmethod
    @functools.cache
    def into_event_type(cls) -> EventType:
        """Event kind fixed by this shape."""
        return EventType.BOOK

    hash: Annotated[int, Field.primary_key()] = NIL
    """Digest of `(unix, instrument_xhash)`, unique for one book instant."""

    # Re-declared to carry **no** FIX tag, which is the honest thing to say:
    # a mid and a touch size are computed from two sides, and FIX has no field
    # for either. The inherited `Price <44>` would label them as an order's
    # limit, which is a schema claiming a provenance it does not have.
    px: float | None = None
    """The mid, `(bid_px + ask_px) / 2`; null until both sides have a price."""

    qty: float | None = None
    """The size at the touch, `bid_qty + ask_qty`; null until both sides have one."""

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

    bid_px: float | None = None
    """Best bid; also `px - spread / 2`."""

    bid_qty: float | None = None
    """Quantity at the best bid."""

    bid_depth: Annotated[int | None, Field(arrow_type=pyarrow.int32())] = None
    """How many levels are live on the buy side."""

    bid_total_qty: float | None = None
    """Sum of `qty` over every live buy level."""

    ask_px: float | None = None
    """Best offer; also `px + spread / 2`."""

    ask_qty: float | None = None
    """Quantity at the best offer."""

    ask_depth: Annotated[int | None, Field(arrow_type=pyarrow.int32())] = None
    """How many levels are live on the sell side."""

    ask_total_qty: float | None = None
    """Sum of `qty` over every live sell level."""

    bid_levels: list[Level] | None = None
    """Changed buy levels on deltas; every live buy level on snapshots, best first."""

    ask_levels: list[Level] | None = None
    """Changed sell levels on deltas; every live sell level on snapshots, best first."""

    order_events: list[Order] | None = None
    """Order inputs on deltas; every live order on recovery snapshots."""

    execution_events: list[Execution] | None = None
    """Complete execution inputs folded into this delta; empty on snapshots."""

    def version_parts(self) -> tuple[Any, ...]:
        """Identify the one book row allowed for an instrument and instant."""
        return self.unix, self.instrument_xhash

    def _carry_side(self, previous: Book, name: str) -> None:
        """Copy one unchanged side's flat summary from the prior book."""
        for suffix in ("px", "qty", "depth", "total_qty"):
            setattr(self, f"{name}_{suffix}", getattr(previous, f"{name}_{suffix}"))

    @classmethod
    def from_logs(cls, logs: Iterable[Log], **declared: Any) -> Iterator[Self]:
        """Fold an already sorted parsed-log stream into books."""
        return iter(BookIterator(logs=logs, **declared))

    @classmethod
    def from_events(
        cls, events: Iterable[MarketEvent | Instrument], **declared: Any
    ) -> Iterator[Self]:
        """Fold translated events directly, for protocol adapters and tests."""
        return iter(BookIterator.from_events(events, **declared))

    def into_orders(self) -> Iterator[Order]:
        """Complete order rows carried by this book delta or snapshot."""
        return iter(self.order_events or ())

    def into_executions(self) -> Iterator[Execution]:
        """Complete execution rows carried by this book delta."""
        return iter(self.execution_events or ())

    def forget_delta(self) -> None:
        """A picture keeps levels but clears their execution traces and event deltas."""
        MarketEvent.forget_delta(self)
        for name in ("bid_levels", "ask_levels"):
            levels = getattr(self, name)
            if levels is not None:
                setattr(
                    self,
                    name,
                    [dataclasses.replace(level, exec_xhash=[]) for level in levels],
                )
        self.order_events = []
        self.execution_events = []

    def derive(self) -> None:
        """A book's prices are computed across its sides, and never carried."""
        self._summarise()
        MarketEvent.derive(self)

    def _summarise(self) -> None:
        """Fill flat side summaries from present level lists, then price the book."""
        for name in ("bid", "ask"):
            levels = getattr(self, f"{name}_levels")
            # A delta carries only changed levels and already carries the full
            # flat summary. A null depth marks an un-summarised full picture.
            if levels is None or getattr(self, f"{name}_depth") is not None:
                continue
            best = levels[0] if levels else None
            setattr(self, f"{name}_px", None if best is None else best.px)
            setattr(self, f"{name}_qty", None if best is None else best.qty)
            setattr(self, f"{name}_depth", len(levels))
            setattr(self, f"{name}_total_qty", sum(level.qty for level in levels))
        self._priced()

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
                    f"{name}_levels",
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
    """Displayed live quantity after subtracting explicitly hidden interest."""
    if order.qty is None:
        return 0.0
    return max(float(order.qty) - max(float(order.hidden_qty or 0.0), 0.0), 0.0)


def _names_of(event: MarketEvent) -> tuple[str | None, str | None, str | None]:
    """Source order identifiers carried by an order or execution."""
    return (
        getattr(event, "order_id", None),
        getattr(event, "client_order_id", None),
        getattr(event, "prev_client_order_id", None),
    )


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
    """The four columns a list of levels determines: best price, best size, depth, total."""
    compute = pyarrow.compute
    alive = _combined(batch.column(levels))
    lengths = compute.list_value_length(alive)
    top = compute.if_else(
        compute.greater(lengths, 0), alive.offsets[:-1], pyarrow.scalar(None, pyarrow.int32())
    )
    members = alive.values
    best_px, best_qty, depth, total = into
    derive = compute.and_(compute.is_valid(alive), compute.is_null(batch.column(depth)))
    return {
        best_px: compute.if_else(
            derive, compute.take(members.field("px"), top), batch.column(best_px)
        ),
        best_qty: compute.if_else(
            derive, compute.take(members.field("qty"), top), batch.column(best_qty)
        ),
        depth: compute.if_else(derive, lengths, batch.column(depth)),
        total: compute.if_else(derive, _list_sums(alive, "qty", lengths), batch.column(total)),
    }


def _list_sums(alive: Any, name: str, lengths: Any) -> Any:
    """One exact sum per list, grouped rather than differenced."""
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


def _summarise_side(book: Book, name: str, side: _Side) -> None:
    """Write one dirty side's cached touch, depth and total onto a book."""
    best = side.best_level
    setattr(book, f"{name}_px", None if best is None else best.px)
    setattr(book, f"{name}_qty", None if best is None else best.qty)
    setattr(book, f"{name}_depth", side.depth)
    setattr(book, f"{name}_total_qty", side.total_qty)


# -- folding a book out of one instrument's events ---------------------------


@dataclasses.dataclass
class _Side:
    """Every order standing on one side of a book, kept in best-first order."""

    side: Side
    """Which side this is; its sign is what turns the sort around."""

    orders: dict[int, Order] = dataclasses.field(default_factory=dict)
    """Every live order, by lifecycle, in no particular order."""

    named: dict[str, int] = dataclasses.field(default_factory=dict)
    """Which lifecycle each venue or client identifier belongs to."""

    levels: dict[float, Level] = dataclasses.field(default_factory=dict)
    """Live levels by price, updated incrementally as orders move."""

    keys: list[float] = dataclasses.field(default_factory=list)
    """Best-first sorted prices multiplied by this side's direction."""

    alive: list[Level] = dataclasses.field(default_factory=list)
    """Live levels in the same best-first order as `keys`."""

    total_qty: float = 0.0
    """Everything resting on this side, running rather than summed per snapshot."""

    changed: dict[float, None] = dataclasses.field(default_factory=dict)
    """Prices changed since the last emitted book, in first-touch order."""

    executions: dict[float, list[int]] = dataclasses.field(default_factory=dict)
    """Execution lifecycles linked to each changed price."""

    # Only explicit expiries are indexed; max-age expiry still examines all orders.
    _expiry_keys: list[int] = dataclasses.field(default_factory=list, init=False, repr=False)
    _expiring: dict[int, dict[int, None]] = dataclasses.field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        """Cache direction and index any explicitly supplied live orders."""
        self.facing = -self.side.sign
        for order in self.orders.values():
            self._index_expiry(order)

    @classmethod
    def from_snapshot(
        cls,
        side: Side,
        levels: Iterable[Level],
        orders: Iterable[Order],
    ) -> _Side:
        """Restore a side from compact levels and generic live order rows."""
        restored = cls(side=side)
        by_xhash = {
            order.xhash: order
            for order in orders
            if order.side.sign == side.sign and order.xhash and not order.state.is_terminal
        }
        linked: set[int] = set()
        for captured in levels:
            if captured.qty <= 0:
                raise ValueError(f"recovery snapshot level {captured.px} must have positive qty")
            members = list(dict.fromkeys(captured.order_xhash))
            if captured.qty > 0 and not members:
                raise ValueError(
                    f"recovery level {captured.px} has positive qty but no linked Order"
                )
            missing = [xhash for xhash in members if xhash not in by_xhash]
            if missing:
                raise ValueError(
                    "a recovery level requires every linked live Order row: "
                    + ", ".join(map(str, missing))
                )
            repeated = [xhash for xhash in members if xhash in linked]
            if repeated:
                raise ValueError(
                    "a recovery Order is linked from more than one level: "
                    + ", ".join(map(str, repeated))
                )
            linked.update(members)
            recovered_qty = sum(_resting(by_xhash[xhash]) for xhash in members)
            if members and not math.isclose(recovered_qty, captured.qty):
                raise ValueError(
                    f"recovery level {captured.px} has qty {captured.qty}, "
                    f"but its Orders total {recovered_qty}"
                )
            for xhash in members:
                order = by_xhash[xhash]
                if order.px != captured.px:
                    raise ValueError(
                        f"recovery Order {xhash} rests at {order.px}, not level {captured.px}"
                    )
                quantity = _resting(order)
                if not order.xhash or order.px is None or quantity <= 0:
                    continue
                restored._remember(order)
                restored._join(order, quantity)
                for spelling in _names_of(order):
                    if spelling:
                        restored.named[spelling] = order.xhash
        extra = [xhash for xhash in by_xhash if xhash not in linked]
        if extra:
            raise ValueError(
                "a live recovery Order is absent from the compact levels: "
                + ", ".join(map(str, extra))
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
    def best_level(self) -> Level | None:
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
        if level is None:
            return None
        return max((self.orders[x] for x in level.order_xhash), key=_resting, default=None)

    @property
    def sorted_orders(self) -> list[Order]:
        """Every live order, best first: by price, then by descending quantity.

        A walk of `alive` and, inside each level, of its members -- so the only
        sorting left is per level, over the handful of orders standing at one
        price. Kept because it is the honest reading of "the orders, in order".
        """
        found: list[Order] = []
        for level in self.alive:
            members = level.order_xhash
            if len(members) == 1:
                found.append(self.orders[members[0]])
                continue
            found.extend(sorted((self.orders[x] for x in members), key=_resting, reverse=True))
        return found

    def into_levels(self) -> list[Level]:
        """The live orders aggregated to price levels, best first."""
        return [
            dataclasses.replace(
                level,
                order_xhash=list(level.order_xhash),
                exec_xhash=[],
            )
            for level in self.alive
        ]

    def into_orders(self) -> list[Order]:
        """Independent live order rows needed to resume a compact snapshot."""
        return [copy.copy(order) for order in self.sorted_orders]

    def into_changed_levels(self) -> list[Level]:
        """Post-change levels, including zero-quantity deletions, best first."""
        levels = []
        for px in self.changed:
            level = self.levels.get(px)
            executions = list(dict.fromkeys(self.executions.get(px, ())))
            if level is None:
                levels.append(Level(px=px, qty=0.0, exec_xhash=executions))
            else:
                levels.append(
                    dataclasses.replace(
                        level,
                        order_xhash=list(level.order_xhash),
                        exec_xhash=executions,
                    )
                )
        return sorted(levels, key=lambda level: -level.px * self.side.sign)

    def cleared(self) -> None:
        """Forget the delta, keeping the state. Called once a book has been yielded."""
        self.changed.clear()
        self.executions.clear()

    def expire(self, unix: int, max_age_ns: int | None = None) -> list[Order]:
        """Remove and return terminal versions of orders stale at `unix`."""
        expired: list[tuple[Order, str, int]] = []
        orders: Iterable[Order]
        if max_age_ns is None:
            until = bisect.bisect_right(self._expiry_keys, unix)
            orders = (
                self.orders[xhash]
                for expiry in self._expiry_keys[:until]
                for xhash in self._expiring[expiry]
            )
        else:
            orders = self.orders.values()
        for order in orders:
            explicit = order.eunix is not None and order.eunix <= unix
            origin = order.cunix or order.unix
            too_old = max_age_ns is not None and unix - origin >= max_age_ns
            if explicit or too_old:
                reason = (
                    "order reached its explicit expiry"
                    if explicit
                    else "order exceeded the configured live age"
                )
                deadline = order.eunix if explicit else origin + max_age_ns
                expired.append((order, reason, deadline))
        return [
            self._expire_order(order, unix, reason, deadline) for order, reason, deadline in expired
        ]

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
            members = (self.orders[xhash] for xhash in level.order_xhash)
            if remaining == 1:
                evicted.append(max(members, key=self._time_priority))
            elif remaining < len(level.order_xhash):
                evicted.extend(heapq.nlargest(remaining, members, key=self._time_priority))
            else:
                evicted.extend(sorted(members, key=self._time_priority, reverse=True))
        return evicted

    @staticmethod
    def _time_priority(order: Order) -> tuple[int, int]:
        """Time priority within one price, with lifecycle as stable tie-break."""
        return order.cunix or order.unix, order.xhash

    def _expire_order(self, order: Order, unix: int, reason: str, deadline: int) -> Order:
        """Remove one live order and build its synthetic terminal version."""
        self._leave(order)
        self._forget(order.xhash)
        terminal = copy.copy(order)
        terminal.unix = unix
        terminal.eunix = deadline
        terminal.state = State.INTERNAL_EXPIRED
        if not terminal.error:
            terminal.error = reason
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
        # mutates, and `parent_hash` is assigned rather than appended to.
        #
        # Complete once here so both the book and its auditable delta use the
        # same linked version; publishing the raw partial report loses terms.
        settled = BookIterator.validate(copy.copy(order).completed_from(standing))
        if standing is not None and settled.same_as(standing):
            return False, None
        settled.identify()
        if settled.state is State.INTERNAL_REJECTED:
            return False, settled
        if settled.px is None:
            # No price even after completing: a market order, which rests
            # nowhere -- it is an execution against a side, not a level on
            # it. Skipped rather than refused, because an order stream really
            # does carry them and one of them is not a reason to abandon the
            # book.
            return False, settled
        before = _resting(standing) if standing else 0.0
        after = (
            0.0 if settled.state.is_terminal or settled.expires_on_arrival else _resting(settled)
        )
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
            for spelling in _names_of(settled):
                if spelling:
                    self.named[spelling] = settled.xhash
        moved = after - before
        if not moved and (standing is None or standing.px == settled.px):
            return False, copy.copy(settled)
        return True, copy.copy(settled)

    def standing(self, event: Order | Execution) -> Order | None:
        """The live order matched by lifecycle links, then source identifiers."""
        if event.is_order() and event.xhash:
            found = self.orders.get(event.xhash)
            if found is not None:
                return found
        for identity in event.linked_xhash or ():
            found = self.orders.get(identity)
            if found is not None:
                return found
        for spelling in _names_of(event):
            identity = self.named.get(spelling) if spelling else None
            if identity is not None:
                return self.orders.get(identity)
        if not getattr(event, "order_id", None):
            client_order_id = getattr(event, "client_order_id", None)
            if client_order_id:
                return next(
                    (
                        order
                        for order in self.orders.values()
                        if order.client_order_id == client_order_id
                    ),
                    None,
                )
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
            level = self.levels[px] = Level(px=px, qty=0.0)
            at = bisect.bisect_left(self.keys, key)
            self.keys.insert(at, key)
            self.alive.insert(at, level)
        level.qty += quantity
        if order.xhash not in level.order_xhash:
            level.order_xhash.append(order.xhash)
        self.total_qty += quantity
        self.changed[px] = None

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
        if order.xhash in level.order_xhash:
            level.order_xhash.remove(order.xhash)
        self.changed[order.px] = None
        if not level.order_xhash or level.qty <= 0:
            # The whole level went with it, so the running totals follow: a
            # level whose members are gone but whose quantity has drifted --
            # a float sum that did not land on zero -- must not leave that
            # drift in `total_qty` for the rest of the fold.
            self.total_qty -= level.qty
            del self.levels[order.px]
            at = bisect.bisect_left(self.keys, order.px * self.facing)
            self.keys.pop(at)
            self.alive.pop(at)

    def _forget(self, xhash: int) -> None:
        """Drop one order and the names that pointed at it, level already left."""
        gone = self.orders.pop(xhash, None)
        if gone is None:
            return
        self._unindex_expiry(gone)
        for spelling in _names_of(gone):
            if spelling and self.named.get(spelling) == xhash:
                del self.named[spelling]

    def _remember(self, order: Order) -> None:
        """Store one live order and keep its explicit-expiry index exact."""
        previous = self.orders.get(order.xhash)
        changed_expiry = previous is None or previous.eunix != order.eunix
        if previous is not None and changed_expiry:
            self._unindex_expiry(previous)
        self.orders[order.xhash] = order
        if changed_expiry:
            self._index_expiry(order)

    def _index_expiry(self, order: Order) -> None:
        """Index one live order when it carries an explicit expiry."""
        if order.eunix is None:
            return
        bucket = self._expiring.get(order.eunix)
        if bucket is None:
            at = bisect.bisect_left(self._expiry_keys, order.eunix)
            self._expiry_keys.insert(at, order.eunix)
            bucket = self._expiring[order.eunix] = {}
        bucket[order.xhash] = None

    def _unindex_expiry(self, order: Order) -> None:
        """Remove an order's current explicit-expiry entry."""
        if order.eunix is None:
            return
        bucket = self._expiring.get(order.eunix)
        if bucket is None:
            return
        bucket.pop(order.xhash, None)
        if bucket:
            return
        del self._expiring[order.eunix]
        at = bisect.bisect_left(self._expiry_keys, order.eunix)
        if at < len(self._expiry_keys) and self._expiry_keys[at] == order.eunix:
            self._expiry_keys.pop(at)

    def take(self, execution: Execution, traded: float) -> None:
        """Record a trade against this side, and take `traded` out of it."""
        hit = self.standing(execution)
        if hit is not None:
            if execution.leaves_qty is not None:
                # ExecutionReport carries the post-trade total. The Order row
                # emitted from that same report may already have applied it;
                # subtracting LastQty again double-counts the fill.
                hidden = max(hit.hidden_qty or 0.0, 0.0)
                post_trade = max(execution.leaves_qty - hidden, 0.0)
                traded = max(_resting(hit) - post_trade, 0.0)
            if traded > 0:
                self._reduce(hit, traded, execution.xhash)
            return
        while traded > 0 and self.alive:
            top = self.keys[0]
            level = self.alive[0]
            # A list, because reducing an order can empty the level and delete
            # the dict this is walking. Sorted inside the level and nowhere
            # else: the largest interest at one price is met first.
            for xhash in sorted(
                level.order_xhash,
                key=lambda x: -_resting(self.orders[x]),
            ):
                if traded <= 0:
                    break
                resting = self.orders.get(xhash)
                if resting is not None:
                    traded = self._reduce(resting, traded, execution.xhash)
            if self.keys and self.keys[0] == top:
                # The touch did not move, so there is nothing further to take
                # off it. Without this the loop could spin on a level that
                # would not clear.
                break

    def _reduce(self, order: Order, traded: float, execution_xhash: int) -> float:
        """Take what `traded` can from one order; the rest is returned for the next."""
        standing = _resting(order)
        taken = min(standing, traded)
        left = standing - taken
        if execution_xhash:
            self.executions.setdefault(order.px, []).append(execution_xhash)
        self.changed[order.px] = None
        if left <= 0:
            self.remove(order.xhash)
        else:
            level = self.levels.get(order.px)
            if level is not None:
                level.qty -= taken
                self.total_qty -= taken
                self.changed[order.px] = None
            if order.qty is not None:
                order.qty = max(order.qty - taken, 0.0)
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

    instrument_xhash: int
    """Which instrument this is the state of."""

    bid: _Side
    """The bid side's live orders, best first."""

    ask: _Side
    """The ask side's live orders, best first."""

    xhash: int = NIL
    """The book's lifecycle. One instrument is one book, so this never moves."""

    xcode: str = ""
    """Readable instrument code the book lifecycle was first identified by."""

    unix: int | None = None
    """The instant the events being folded belong to; None before the first."""

    previous: Book | None = None
    """The last book emitted, which the next one is a version of."""

    about: MarketEvent | None = None
    """The last event folded in -- where a book learns what it is a book of."""

    instrument: Instrument = dataclasses.field(default_factory=Instrument)
    """Latest instrument facts visible to the folded event."""

    emitted: int | None = None
    """`unix` of the last book emitted, which is what an hourly snapshot counts from."""

    moved: bool = False
    """Whether anything has happened since the last book was emitted."""

    order_events: list[Order] = dataclasses.field(default_factory=list)
    """Complete order inputs awaiting the next delta row."""

    execution_events: list[Execution] = dataclasses.field(default_factory=list)
    """Complete execution inputs awaiting the next delta row."""


@dataclasses.dataclass
class BookIterator:
    """Fold one sorted parsed-log stream into books."""

    logs: Iterable[Log] = ()
    """Parsed logs sorted by event time, sequence and hash; read once."""

    snapshot_every: int = HOUR
    """Emit the book on every multiple of this; `0` emits only what changed."""

    snapshot_until: int | None = None
    """On flush, complete boundaries before this exclusive instant; None does not guess."""

    snapshots: Iterable[Book] = ()
    """Latest prior book snapshots used to resume live orders."""

    max_order_age_ns: int | None = DAY
    """Expire unchanged orders after one day; None uses explicit `eunix` only."""

    max_side_alive: int | None = None
    """Keep at most this many live orders per side; None keeps every order."""

    folding: dict[int, _Folding] = dataclasses.field(default_factory=dict)
    """Mutable fold state keyed by first-seen instrument identity; later identities alias to it."""

    def __post_init__(self) -> None:
        if self.snapshot_every < 0:
            raise ValueError("snapshot_every must be non-negative")
        if self.max_side_alive is not None and self.max_side_alive < 0:
            raise ValueError("max_side_alive must be non-negative")
        self._source: Iterator[MarketEvent] | None = None
        self._event_input: Iterable[MarketEvent | Instrument] | None = None
        self._finished = False
        self._books: deque[Book] = deque()
        self._unix: int | None = None
        self._swept: int | None = None
        self._aliases: dict[int, int] = {}
        self._life_aliases: dict[int, int] = {}
        self._hash_aliases: dict[int, int] = {}
        self._instruments: dict[int, Instrument] = {}
        for snapshot in self.snapshots:
            self._restore(snapshot)

    @classmethod
    def from_events(
        cls, events: Iterable[MarketEvent | Instrument], **declared: Any
    ) -> BookIterator:
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
        """Index instrument rows and translate the other parsed rows once."""
        if self._event_input is not None:
            for event in self._event_input:
                if isinstance(event, Instrument):
                    self._index_instrument(event)
                else:
                    yield event
            return
        for log in self.logs:
            if log.etype is EventType.INSTRUMENT:
                if log.is_instrument_version:
                    for instrument in log.into_instruments():
                        self._index_instrument(instrument)
                continue
            yield from log.into_market_events()

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
        folded, settled = _folded(state.bid, state.ask, event)
        if settled is None:
            return
        state.unix = settled.unix
        if settled.is_order():
            state.order_events.append(settled)
            bounded = self.max_side_alive is not None and self._bound(state, settled.unix)
        elif settled.is_execution():
            state.execution_events.append(settled)
            bounded = False
        else:
            bounded = False
        state.moved = (
            folded or bounded or settled.is_order() or settled.is_execution() or state.moved
        )
        # After folding, never before: a book is described by the events it
        # holds, and reading the event that *triggered* the emission gave
        # every row the units of the instant after it.
        state.about = settled

    def _sweep(self, unix: int, folded: _Folding) -> None:
        """Fill in the hourly rows of every *other* instrument the clock passed."""
        every = self.snapshot_every
        if not every:
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
                self._settle(state, until)

    def _state_of(self, event: MarketEvent) -> _Folding:
        """The fold this event belongs to, by its flat or transient identities.

        A venue that starts sending an ISIN halfway through a capture changes
        what `Instrument.identify` returns, and a fold keyed on that alone
        opened a second book for the same instrument. Every identity is aliased
        onto the first one seen, which is also what the rows carry -- so the
        partition an instrument's rows land in does not move either.
        """
        known = self._aliases.get(event.instrument_xhash)
        if known is not None:
            self._canonicalize(event, known)
            return self.folding[known]
        instrument = event.into_instrument()
        for identity in instrument.identities() if instrument is not None else ():
            known = self._aliases.get(identity)
            if known is not None:
                # Learnt under another spelling: alias this one too, so the
                # next message carrying it is one probe rather than a walk.
                self._aliases[event.instrument_xhash] = known
                self._canonicalize(event, known)
                return self.folding[known]
        stored = self._instrument_of(event, instrument)
        observed = event.instrument_xhash
        canonical = stored.xhash if stored is not None else observed
        self._canonicalize(event, canonical)
        state = self.folding[canonical] = self._started(event, stored)
        self._aliases[canonical] = canonical
        self._aliases[observed] = canonical
        for identity in instrument.identities() if instrument is not None else ():
            self._aliases.setdefault(identity, canonical)
        if stored is not None:
            self._remember(state, stored)
        return state

    def _started(self, event: MarketEvent, stored: Instrument | None = None) -> _Folding:
        """The state one instrument's fold starts from, identities and all."""
        lifecycle = Book(
            unix=event.unix,
            instrument_xhash=event.instrument_xhash,
            code=event.code,
            ccy=event.ccy,
        )
        parsed = event.into_instrument()
        instrument = (
            stored
            if stored is not None
            else parsed
            or Instrument(xhash=event.instrument_xhash, symbol=event.code, currency=event.ccy)
        )
        lifecycle.attach_instrument(instrument)
        xhash = lifecycle.life_hash()
        state = _Folding(
            instrument_xhash=event.instrument_xhash,
            bid=_Side(side=Side.BID),
            ask=_Side(side=Side.ASK),
            xhash=xhash,
            xcode=lifecycle.life_code(),
            instrument=instrument,
        )
        return state

    def _canonicalize(self, event: MarketEvent, instrument_xhash: int) -> None:
        """Rewrite an aliased input and its relations onto the fold's canonical identities."""
        linked = [self._alias_of(self._life_aliases, value) for value in event.linked_xhash or ()]
        parents = [self._alias_of(self._hash_aliases, value) for value in event.parent_hash or ()]
        previous = self._alias_of(self._hash_aliases, event.prev_hash)
        linked = list(dict.fromkeys(linked))
        parents = list(dict.fromkeys(parents))
        changed = (
            event.instrument_xhash != instrument_xhash
            or linked != list(event.linked_xhash or ())
            or parents != list(event.parent_hash or ())
            or previous != event.prev_hash
        )
        if not changed:
            return
        old_xhash, old_hash = event.xhash, event.hash
        event.instrument_xhash = instrument_xhash
        event.linked_xhash = linked or None
        event.parent_hash = parents or None
        event.prev_hash = previous
        event.xhash = NIL
        event.hash = NIL
        event.identify()
        if old_xhash and old_xhash != event.xhash:
            self._life_aliases[old_xhash] = event.xhash
        if old_hash and old_hash != event.hash:
            self._hash_aliases[old_hash] = event.hash

    @staticmethod
    def _alias_of(aliases: dict[int, int], value: int | None) -> int | None:
        """Resolve a short alias chain; None remains absent."""
        while value in aliases:
            value = aliases[value]
        return value

    def _restore(self, snapshot: Book) -> None:
        """Resume one fold from the complete live state of a prior snapshot."""
        if snapshot.sunix is None:
            raise ValueError("a recovery seed must be a book snapshot with `sunix`")
        canonical = snapshot.instrument_xhash
        known = self._instruments.get(canonical)
        instrument = (
            known
            if known is not None
            else snapshot.into_instrument() or Instrument(xhash=canonical, symbol=snapshot.code)
        )
        snapshot.attach_instrument(instrument)
        state = _Folding(
            instrument_xhash=canonical,
            bid=_Side.from_snapshot(
                Side.BID,
                snapshot.bid_levels or (),
                snapshot.order_events or (),
            ),
            ask=_Side.from_snapshot(
                Side.ASK,
                snapshot.ask_levels or (),
                snapshot.order_events or (),
            ),
            xhash=snapshot.xhash,
            xcode=snapshot.xcode,
            unix=snapshot.unix,
            previous=snapshot,
            about=snapshot,
            instrument=instrument,
            emitted=snapshot.unix,
        )
        state.moved = self._bound(state, snapshot.unix)
        self.folding[canonical] = state
        self._aliases[canonical] = canonical
        self._remember(state, state.instrument)
        self._unix = max(self._unix or snapshot.unix, snapshot.unix)
        self._swept = max(self._swept or snapshot.unix, snapshot.unix)

    def _expire(self, state: _Folding, unix: int) -> bool:
        """Expire stale orders on both sides before events at `unix` apply."""
        expired = state.bid.expire(unix, self.max_order_age_ns)
        expired += state.ask.expire(unix, self.max_order_age_ns)
        state.order_events.extend(expired)
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
        state.order_events.extend(expired)
        return bool(expired)

    @staticmethod
    def validate(event: MarketEvent) -> MarketEvent:
        """Reject incomplete book inputs while preserving an existing error."""
        reasons: list[str] = []
        validates_interest = (
            event.is_order()
            and not event.state.is_terminal
            and event.state not in (State.UNKNOWN, State.PENDING_CANCEL)
        )
        if validates_interest:
            quantity = event.qty
            kind = getattr(event, "kind", MarketKind.UNKNOWN)
            if not event.side.sign:
                reasons.append("side is missing")
            price_required = kind.band == MarketKind.LIMIT or kind is MarketKind.STOP_LIMIT
            invalid_price = event.px is not None and not math.isfinite(event.px)
            if price_required and event.px is None or invalid_price:
                reasons.append("required price is missing or non-finite")
            if quantity is None or not math.isfinite(quantity) or quantity <= 0:
                reasons.append("quantity is missing or non-positive")
        elif event.is_execution() and event.state is State.FILLED:
            if event.px is None or not math.isfinite(event.px):
                reasons.append("price is missing or non-finite")
            if event.qty is None or not math.isfinite(event.qty) or event.qty <= 0:
                reasons.append("quantity is missing or non-positive")
        if not reasons:
            return event
        event.state = State.INTERNAL_REJECTED
        if not getattr(event, "error", None):
            event.error = "rejected for book: " + "; ".join(reasons)
        event.hash = NIL
        return event.identify()

    def _remember(self, state: _Folding, known: Instrument) -> None:
        """Alias every authoritative identity onto its fold."""
        for identity in known.identities():
            self._aliases.setdefault(identity, state.instrument_xhash)

    def _index_instrument(self, known: Instrument) -> None:
        """Index one full instrument version by all of its identities."""
        for identity in (known.xhash, *known.identities()):
            self._instruments[identity] = known

    def _instrument_of(
        self, event: MarketEvent, instrument: Instrument | None
    ) -> Instrument | None:
        """Known instrument reached through the event's canonical or parsed identity."""
        stored = self._instruments.get(event.instrument_xhash)
        if stored is not None or instrument is None:
            return stored
        return next(
            (
                self._instruments[identity]
                for identity in instrument.identities()
                if identity in self._instruments
            ),
            None,
        )

    def _refresh_instrument(self, state: _Folding, event: MarketEvent) -> None:
        """Use the latest instrument version visible at this event."""
        known = self._instrument_of(event, event.into_instrument())
        if known is None:
            return
        state.instrument = known
        self._remember(state, known)

    # -- emitting -------------------------------------------------------------

    def _settle(self, state: _Folding, unix: int, *, inclusive: bool = False) -> None:
        """Emit whatever `unix` completes: the instant that ended, then the hours."""
        if state.unix is not None and unix != state.unix and state.moved:
            self._emit(state, state.unix)
            state.moved = False
        self._snapshots(state, unix, inclusive=inclusive)

    def _snapshots(self, state: _Folding, unix: int, *, inclusive: bool = False) -> None:
        """One book on every boundary between the last emission and `unix`.

        Every boundary, and not just the latest: an hourly table whose rows
        skip the hours nothing happened in is a table you have to scan
        backwards to read, which is the thing hourly rows exist to avoid. A
        gap of ten hours is ten rows, each a picture of the same state, each
        saying so in `sunix`.
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
        taken.bid_levels = state.bid.into_levels()
        taken.ask_levels = state.ask.into_levels()
        taken.order_events = [*state.bid.into_orders(), *state.ask.into_orders()]
        taken.execution_events = []
        taken.linked_xhash = [order.xhash for order in taken.order_events if order.xhash] or None
        taken.parent_hash = [order.hash for order in taken.order_events if order.hash] or None
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
        or execution.px is None
        or execution.qty is None
    ):
        return False
    side = _hit(bid, ask, execution)
    if side is None:
        return False
    side.take(execution, abs(execution.qty))
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
    best_bid, best_ask = bid.best, ask.best
    if best_bid is None and best_ask is None:
        return None
    if best_bid is None:
        return ask
    if best_ask is None:
        return bid
    mid = ((best_bid.px or 0.0) + (best_ask.px or 0.0)) / 2
    return bid if (execution.px or 0.0) <= mid else ask


def _settled(state: _Folding, unix: int) -> Book | None:
    """The book as it stands, as a new row -- and the delta handed over with it."""
    about = state.about
    # The instrument the fold has *accumulated*, not whatever the last message
    # happened to spell: a book row says what it is a book of, and the last
    # message may have named the instrument more poorly than an earlier one.
    # `instrument_xhash` is the fold's canonical key for the same reason -- a
    # row whose partition moved when a venue started sending an ISIN would
    # split one instrument's history across two of them.
    book = Book(
        unix=unix,
        instrument_xhash=state.instrument_xhash,
        code=state.instrument.symbol or about.code,
        xcode=state.xcode,
        px_unit=about.px_unit,
        ccy=about.ccy,
        qty_unit=about.qty_unit,
        mic=about.mic,
        seq=about.seq,
        state=State.OPEN if (state.bid.keys or state.ask.keys) else State.CLOSED,
    )
    book.attach_instrument(state.instrument)
    book.xhash = state.xhash
    book.order_events = list(state.order_events)
    book.execution_events = list(state.execution_events)
    events = [*book.order_events, *book.execution_events]
    book.linked_xhash = list(dict.fromkeys(event.xhash for event in events if event.xhash)) or None
    book.parent_hash = list(dict.fromkeys(event.hash for event in events if event.hash)) or None
    previous = state.previous
    for name, side in (("bid", state.bid), ("ask", state.ask)):
        if previous is None or side.changed:
            _summarise_side(book, name, side)
        else:
            book._carry_side(previous, name)
        setattr(book, f"{name}_levels", side.into_changed_levels() if side.changed else None)
        side.cleared()
    state.order_events.clear()
    state.execution_events.clear()
    # The prices across the sides are `Book.derive`'s, which `with_previous`
    # runs once every layer has filled -- so they are not computed here as
    # well, and the content hash it ends with is of a row that already has them.
    return book.with_previous(state.previous)
