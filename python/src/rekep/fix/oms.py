"""Structured OMS order facts carried by XML capture entries."""

from __future__ import annotations

import datetime
import functools
from collections.abc import Mapping, Sequence
from typing import Annotated, Any

import pyarrow
import pyarrow.compute as compute

from rekep.fields import Field, column_names, scalar
from rekep.fields.arrays import build_list, dense_counts, sequence
from rekep.fix.columns import DECLARED, ENTRIES
from rekep.fix.components import ComponentGroup
from rekep.fix.fields import cast_arrow_field

_ORDER_VIEW = (
    r"(?i)^(?P<orderpath>event\[[0-9]+\]"
    r"(?:\.action\[[0-9]+\])?\.order\[[0-9]+\])(?P<tail>.*)$"
)
_EVENT_VIEW = r"(?i)^(?P<eventpath>event\[[0-9]+\])"
_ACTION_VIEW = (
    r"(?i)^event\[[0-9]+\]\."
    r"(?P<actionpath>action\[[0-9]+\])\.order\[[0-9]+\]$"
)
_ULLINK_VIEW = (
    r"(?i)^(?P<blockpath>event\[[0-9]+\]"
    r"(?:\.action\[[0-9]+\])?\.order\[[0-9]+\]"
    r"\.data\[[0-9]+\]\.ullink\[[0-9]+\]\."
    r"(?P<source>visible|invisible|persisted)\[[0-9]+\])(?P<tail>.*)$"
)

# OMS XML names the business fields but carries no FIX application version.
# The package default makes that interpretation reproducible across re-reads.
OMS_FIX_VERSION = "4.4"


@scalar
class OmsUllink:
    """One declared ULLINK fact set and the visibility block that supplied it."""

    source: str = ""
    """`visible`, `invisible`, or `persisted`."""

    orderid: Annotated[str | None, DECLARED["OrderID"]] = None
    """Venue order identifier supplied inside this block."""

    clordid: Annotated[str | None, DECLARED["ClOrdID"]] = None
    """Client order identifier supplied inside this block."""

    execid: Annotated[str | None, DECLARED["ExecID"]] = None
    """Execution identifier supplied inside this block."""

    account: Annotated[str | None, DECLARED["Account"]] = None
    """Account supplied inside this block."""

    orderqty: Annotated[float | None, DECLARED["OrderQty"]] = None
    """Requested quantity supplied inside this block."""

    cumqty: Annotated[float | None, DECLARED["CumQty"]] = None
    """Cumulative executed quantity supplied inside this block."""

    leavesqty: Annotated[float | None, DECLARED["LeavesQty"]] = None
    """Remaining quantity supplied inside this block."""

    lastqty: Annotated[float | None, DECLARED["LastQty"]] = None
    """Latest executed quantity supplied inside this block."""

    price: Annotated[float | None, DECLARED["Price"]] = None
    """Order limit supplied inside this block."""

    avgpx: Annotated[float | None, DECLARED["AvgPx"]] = None
    """Average executed price supplied inside this block."""

    lastpx: Annotated[float | None, DECLARED["LastPx"]] = None
    """Latest executed price supplied inside this block."""

    timeinforce: Annotated[str | None, DECLARED["TimeInForce"]] = None
    """Time-in-force supplied inside this block."""

    transacttime: Annotated[datetime.datetime | None, DECLARED["TransactTime"]] = None
    """Business instant supplied inside this block."""

    creationtime: Annotated[datetime.datetime | None, DECLARED["CreationTime"]] = None
    """Lifecycle creation instant supplied inside this block."""


OMS_ULLINKS = pyarrow.list_(pyarrow.field("item", OmsUllink.into_field().dtype, nullable=False))


@scalar
class OmsOrder:
    """One XML OMS order with the event or action that owns it."""

    orderpath: str = ""
    """Indexed XML path that distinguishes peer order facts."""

    owner: str = ""
    """`event` for a direct child and `action` for an action-owned order."""

    eventid: str | None = None
    """Identifier on the enclosing XML event."""

    eventkind: str | None = None
    """Type on the enclosing XML event."""

    actionid: str | None = None
    """Identifier on the owning action; null for an event-owned order."""

    actiontype: str | None = None
    """Type on the owning action; null for an event-owned order."""

    actionuserid: str | None = None
    """User on the owning action; null for an event-owned order."""

    instrumentid: str | None = None
    """Source instrument key consumed by `Instrument.from_instrument_keys_arrow`."""

    orderid: Annotated[str | None, DECLARED["OrderID"]] = None
    """Venue order identifier, including XML `order/@id`."""

    clordid: Annotated[str | None, DECLARED["ClOrdID"]] = None
    """Client order identifier, including XML `order/@clientid`."""

    execid: Annotated[str | None, DECLARED["ExecID"]] = None
    """Execution report identifier when the XML order states one."""

    account: Annotated[str | None, DECLARED["Account"]] = None
    """Account attached to the order."""

    securityexchange: Annotated[str | None, DECLARED["SecurityExchange"]] = None
    """Listing venue, including XML `order/@exchangeid`."""

    currency: Annotated[str | None, DECLARED["Currency"]] = None
    """Price currency supplied by the order."""

    side: Annotated[str | None, DECLARED["Side"]] = None
    """Order side in source or FIX spelling."""

    ordtype: Annotated[str | None, DECLARED["OrdType"]] = None
    """Order type in source or FIX spelling."""

    timeinforce: Annotated[str | None, DECLARED["TimeInForce"]] = None
    """Time-in-force in source or FIX spelling."""

    ordstatus: Annotated[str | None, DECLARED["OrdStatus"]] = None
    """OMS execution state retained in the standard order-status slot."""

    orderqty: Annotated[float | None, DECLARED["OrderQty"]] = None
    """Requested quantity."""

    price: Annotated[float | None, DECLARED["Price"]] = None
    """Order limit."""

    avgpx: Annotated[float | None, DECLARED["AvgPx"]] = None
    """Average executed price."""

    cumqty: Annotated[float | None, DECLARED["CumQty"]] = None
    """Cumulative executed quantity."""

    leavesqty: Annotated[float | None, DECLARED["LeavesQty"]] = None
    """Remaining quantity."""

    lastpx: Annotated[float | None, DECLARED["LastPx"]] = None
    """Latest executed price."""

    lastqty: Annotated[float | None, DECLARED["LastQty"]] = None
    """Latest executed quantity."""

    grosstradeamt: Annotated[float | None, DECLARED["GrossTradeAmt"]] = None
    """Source capital or capitalization in the order currency."""

    transacttime: Annotated[datetime.datetime | None, DECLARED["TransactTime"]] = None
    """Order instant, falling back to the enclosing event timestamp."""

    creationtime: Annotated[datetime.datetime | None, DECLARED["CreationTime"]] = None
    """Lifecycle creation instant."""

    expiretime: Annotated[datetime.datetime | None, DECLARED["ExpireTime"]] = None
    """Order deadline when the source supplies one."""

    reason: str | None = None
    """Termination reason; null outside a terminating fact."""

    ullink: Annotated[list[OmsUllink] | None, Field(dtype=OMS_ULLINKS)] = None
    """Declared values from visible, invisible, and persisted ULLINK blocks."""


OMS_ORDERS = pyarrow.list_(pyarrow.field("item", OmsOrder.into_field().dtype, nullable=False))


# `(relative component path, source keys)` declarations. The path is part of
# the contract: equal words under a sibling element do not become equal facts.
_ORDER_SOURCES: Mapping[str, tuple[tuple[str, tuple[str, ...]], ...]] = {
    "instrumentid": ((r"^$", ("instrumentid", "underlying")),),
    "orderid": ((r"^$", ("id", "orderid")),),
    "clordid": ((r"^$", ("clientid", "clordid")),),
    "execid": ((r"^$", ("execid",)),),
    "account": ((r"^$", ("account",)),),
    "securityexchange": ((r"^$", ("exchangeid", "securityexchange")),),
    "currency": ((r"^$", ("currency",)),),
    "side": ((r"^$", ("side",)),),
    "ordtype": ((r"^$", ("ordtype", "ordertype")),),
    "timeinforce": (
        (r"^$", ("timeinforce",)),
        (r"^\.timeinforce\[[0-9]+\]$", ("timeinforce", "type", "value")),
    ),
    "ordstatus": ((r"^$", ("executionstate", "ordstatus", "status")),),
    "orderqty": (
        (r"^$", ("quantity", "orderqty")),
        (r"^\.qty\[[0-9]+\]$", ("quantity", "orderqty", "total", "value")),
    ),
    "cumqty": (
        (r"^$", ("cumqty", "cumulativequantity", "executedquantity")),
        (r"^\.qty\[[0-9]+\]$", ("cumqty", "cumulative", "executed")),
    ),
    "leavesqty": (
        (r"^$", ("leavesqty", "openquantity")),
        (r"^\.qty\[[0-9]+\]$", ("leavesqty", "leaves", "open")),
    ),
    "lastqty": (
        (r"^$", ("lastqty",)),
        (r"^\.qty\[[0-9]+\]$", ("lastqty", "last")),
    ),
    "price": (
        (r"^$", ("price",)),
        (r"^\.price\[[0-9]+\]$", ("price", "limit", "value")),
    ),
    "avgpx": (
        (r"^$", ("avgpx", "averageprice")),
        (r"^\.price\[[0-9]+\]$", ("avgpx", "avg", "average")),
    ),
    "lastpx": (
        (r"^$", ("lastpx",)),
        (r"^\.price\[[0-9]+\]$", ("lastpx", "last")),
    ),
    "grosstradeamt": (
        (r"^$", ("grosstradeamt", "capital", "capitalization")),
        (r"^\.price\[[0-9]+\]$", ("grosstradeamt", "capital", "capitalization")),
    ),
    "transacttime": (
        (r"^$", ("transacttime", "timestamp")),
        (r"^\.time\[[0-9]+\]$", ("transacttime", "time", "value")),
    ),
    "creationtime": (
        (r"^$", ("creationtime",)),
        (r"^\.time\[[0-9]+\]$", ("creationtime", "creation", "created")),
    ),
    "expiretime": (
        (r"^$", ("expiretime",)),
        (r"^\.timeinforce\[[0-9]+\]$", ("expiretime", "expiry", "expires")),
    ),
    "reason": ((r"^\.terminate\[[0-9]+\]$", ("reason",)),),
}

_ULLINK_SOURCES: Mapping[str, tuple[str, ...]] = {
    "orderid": ("orderid",),
    "clordid": ("clordid", "clientid"),
    "execid": ("execid",),
    "account": ("account",),
    "orderqty": ("orderqty", "quantity"),
    "cumqty": ("cumqty",),
    "leavesqty": ("leavesqty",),
    "lastqty": ("lastqty",),
    "price": ("price",),
    "avgpx": ("avgpx", "averageprice"),
    "lastpx": ("lastpx",),
    "timeinforce": ("timeinforce",),
    "transacttime": ("transacttime", "timestamp"),
    "creationtime": ("creationtime",),
}


class OmsOrders(ComponentGroup):
    """Lift indexed XML orders while retaining every undeclared XML entry."""

    component = "event"
    group = "order"

    @classmethod
    @functools.cache
    def into_row(cls) -> type[OmsOrder]:
        """The nested order declaration."""
        return OmsOrder

    @classmethod
    @functools.cache
    def into_projection(cls) -> tuple[tuple[str, str], ...]:
        """The FIX member each financial column represents."""
        return tuple(
            (name, OmsOrder.into_field().field(name).fix.get("name", name))
            for name in _ORDER_SOURCES
        )

    def into_arrow_arrays_with_errors(self, tags: Any) -> tuple[Any, Any, Any]:
        """Extract OMS orders through Arrow kernels; malformed values remain residual."""
        if isinstance(tags, pyarrow.ChunkedArray):
            tags = tags.combine_chunks()
        if not isinstance(tags, pyarrow.Array) or tags.type != ENTRIES:
            actual = getattr(tags, "type", type(tags).__name__)
            raise TypeError(f"{type(self).__name__} needs {ENTRIES}, got {actual}")
        orders, residual = _orders_arrow(tags)
        return orders, residual, pyarrow.nulls(len(tags), pyarrow.string())


def _orders_arrow(tags: pyarrow.Array) -> tuple[pyarrow.Array, pyarrow.Array]:
    """XML entry lists split into declared order structs and residual entries."""
    rows = len(tags)
    items = compute.list_flatten(tags)
    if not len(items):
        return pyarrow.nulls(rows, OMS_ORDERS), tags
    parents = compute.list_parent_indices(tags).cast(pyarrow.int64())
    positions = sequence(len(items))
    values = compute.struct_field(items, "value")
    keys = column_names(compute.struct_field(items, "key"))
    comps = compute.fill_null(compute.struct_field(items, "comp"), "")
    view = compute.extract_regex(comps, _ORDER_VIEW)
    orderpaths = compute.struct_field(view, "orderpath")
    tails = compute.utf8_lower(compute.struct_field(view, "tail"))
    inside = compute.is_valid(orderpaths)
    item_ids = _identities(parents, orderpaths)
    order_ids = compute.unique(compute.filter(item_ids, inside))
    if not len(order_ids):
        return pyarrow.nulls(rows, OMS_ORDERS), tags
    first_positions = compute.index_in(order_ids, value_set=item_ids)
    order_parents = compute.take(parents, first_positions)
    orderpaths = compute.take(orderpaths, first_positions)
    eventpaths = compute.struct_field(compute.extract_regex(orderpaths, _EVENT_VIEW), "eventpath")
    actionpaths = compute.struct_field(
        compute.extract_regex(orderpaths, _ACTION_VIEW), "actionpath"
    )

    consumed = pyarrow.repeat(pyarrow.scalar(False), len(items))
    order_field = OmsOrder.into_field()
    columns: dict[str, pyarrow.Array] = {
        "orderpath": orderpaths,
        "owner": compute.if_else(
            compute.is_valid(actionpaths),
            pyarrow.scalar("action"),
            pyarrow.scalar("event"),
        ),
    }
    for name, sources in _ORDER_SOURCES.items():
        matched = pyarrow.repeat(pyarrow.scalar(False), len(items))
        for tail_pattern, source_keys in sources:
            path_match = compute.fill_null(
                compute.match_substring_regex(tails, tail_pattern), False
            )
            key_match = compute.is_in(keys, value_set=pyarrow.array(source_keys))
            matched = compute.or_(
                matched, compute.and_(inside, compute.and_(path_match, key_match))
            )
        columns[name], selected = _first_values(
            order_ids,
            item_ids,
            values,
            positions,
            matched,
            order_field.field(name),
        )
        consumed = compute.or_(consumed, selected)

    event_ids = _ancestor_ids(parents, comps)
    for name, source_keys in (
        ("eventid", ("id",)),
        ("eventkind", ("type",)),
    ):
        columns[name], selected = _ancestor_values(
            parents,
            comps,
            keys,
            values,
            positions,
            order_parents,
            eventpaths,
            event_ids,
            source_keys,
            order_field.field(name),
        )
        consumed = compute.or_(consumed, selected)
    event_time, selected = _ancestor_values(
        parents,
        comps,
        keys,
        values,
        positions,
        order_parents,
        eventpaths,
        event_ids,
        ("timestamp",),
        order_field.field("transacttime"),
    )
    columns["transacttime"] = compute.coalesce(columns["transacttime"], event_time)
    consumed = compute.or_(consumed, selected)

    action_targets = compute.if_else(
        compute.is_valid(actionpaths),
        compute.binary_join_element_wise(eventpaths, ".", actionpaths, ""),
        pyarrow.scalar(None, pyarrow.string()),
    )
    for name, source_keys in (
        ("actionid", ("id",)),
        ("actiontype", ("type",)),
        ("actionuserid", ("userid",)),
    ):
        columns[name], selected = _ancestor_values(
            parents,
            comps,
            keys,
            values,
            positions,
            order_parents,
            action_targets,
            event_ids,
            source_keys,
            order_field.field(name),
        )
        consumed = compute.or_(consumed, selected)

    columns["ullink"], ullink_consumed = _ullink_arrow(
        parents, positions, keys, values, comps, order_ids
    )
    consumed = compute.or_(consumed, ullink_consumed)
    order_struct = pyarrow.StructArray.from_arrays(
        [columns[field.name] for field in order_field.fields],
        fields=order_field.arrow_fields,
    )
    sizes = dense_counts(order_parents, rows)
    orders = build_list(OMS_ORDERS, sizes, order_struct, compute.equal(sizes, 0))
    residual = _residual(tags, items, parents, consumed, rows)
    return orders, residual


def _ullink_arrow(
    parents: pyarrow.Array,
    positions: pyarrow.Array,
    keys: pyarrow.Array,
    values: pyarrow.Array,
    comps: pyarrow.Array,
    order_ids: pyarrow.Array,
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """Three ULLINK sources projected through one member declaration."""
    view = compute.extract_regex(comps, _ULLINK_VIEW)
    blockpaths = compute.struct_field(view, "blockpath")
    tails = compute.struct_field(view, "tail")
    sources = compute.utf8_lower(compute.struct_field(view, "source"))
    inside = compute.is_valid(blockpaths)
    block_item_ids = _identities(parents, blockpaths)
    block_ids = compute.unique(compute.filter(block_item_ids, inside))
    if not len(block_ids):
        return (
            pyarrow.nulls(len(order_ids), OMS_ULLINKS),
            pyarrow.repeat(pyarrow.scalar(False), len(values)),
        )
    first_positions = compute.index_in(block_ids, value_set=block_item_ids)
    block_sources = compute.take(sources, first_positions)
    block_paths = compute.take(blockpaths, first_positions)
    order_view = compute.extract_regex(block_paths, _ORDER_VIEW)
    block_order_paths = compute.struct_field(order_view, "orderpath")
    block_parents = compute.take(parents, first_positions)
    block_order_ids = _identities(block_parents, block_order_paths)
    block_at = compute.index_in(block_order_ids, value_set=order_ids).cast(pyarrow.int64())

    ullink_field = OmsUllink.into_field()
    columns: dict[str, pyarrow.Array] = {"source": block_sources}
    consumed = pyarrow.repeat(pyarrow.scalar(False), len(values))
    known = pyarrow.repeat(pyarrow.scalar(False), len(block_ids))
    direct = compute.fill_null(compute.equal(tails, ""), False)
    for name, source_keys in _ULLINK_SOURCES.items():
        matched = compute.and_(
            inside,
            compute.and_(direct, compute.is_in(keys, pyarrow.array(source_keys))),
        )
        columns[name], selected = _first_values(
            block_ids,
            block_item_ids,
            values,
            positions,
            matched,
            ullink_field.field(name),
        )
        known = compute.or_(known, compute.is_valid(columns[name]))
        consumed = compute.or_(consumed, selected)

    kept_at = compute.indices_nonzero(known).cast(pyarrow.int64())
    if not len(kept_at):
        return (
            pyarrow.nulls(len(order_ids), OMS_ULLINKS),
            pyarrow.repeat(pyarrow.scalar(False), len(values)),
        )
    entries = pyarrow.StructArray.from_arrays(
        [compute.take(columns[field.name], kept_at) for field in ullink_field.fields],
        fields=ullink_field.arrow_fields,
    )
    kept_parents = compute.take(block_at, kept_at)
    sizes = dense_counts(kept_parents, len(order_ids))
    return build_list(OMS_ULLINKS, sizes, entries, compute.equal(sizes, 0)), consumed


def _ancestor_values(
    parents: pyarrow.Array,
    comps: pyarrow.Array,
    keys: pyarrow.Array,
    values: pyarrow.Array,
    positions: pyarrow.Array,
    order_parents: pyarrow.Array,
    target_paths: pyarrow.Array,
    item_ids: pyarrow.Array,
    source_keys: Sequence[str],
    field: Field,
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """One exact ancestor attribute repeated onto each order it owns."""
    target_ids = _identities(order_parents, target_paths)
    matched = compute.is_in(keys, value_set=pyarrow.array(source_keys))
    return _first_values(target_ids, item_ids, values, positions, matched, field)


def _ancestor_ids(parents: pyarrow.Array, comps: pyarrow.Array) -> pyarrow.Array:
    """Entry identities used by exact event and action ancestor reads."""
    return _identities(parents, comps)


def _identities(parents: pyarrow.Array, paths: pyarrow.Array) -> pyarrow.Array:
    """One collision-free identity for a component inside one source row."""
    return compute.binary_join_element_wise(
        parents.cast(pyarrow.string()),
        "\x00",
        compute.fill_null(paths, ""),
        "",
    )


def _first_values(
    target_ids: pyarrow.Array,
    item_ids: pyarrow.Array,
    values: pyarrow.Array,
    positions: pyarrow.Array,
    matched: pyarrow.Array,
    field: Field,
) -> tuple[pyarrow.Array, pyarrow.Array]:
    """First readable value per identity and the source positions it consumed."""
    selected_ids = compute.filter(item_ids, matched)
    selected_values = compute.filter(values, matched)
    selected_positions = compute.filter(positions, matched)
    if pyarrow.types.is_string(selected_values.type) or pyarrow.types.is_large_string(
        selected_values.type
    ):
        selected_values = compute.if_else(compute.equal(selected_values, ""), None, selected_values)
    converted = cast_arrow_field(selected_values, field, field.dtype)
    readable = compute.is_valid(converted)
    readable_ids = compute.filter(selected_ids, readable)
    at = compute.index_in(target_ids, value_set=readable_ids)
    converted = compute.take(compute.filter(converted, readable), at)
    chosen = compute.drop_null(compute.take(compute.filter(selected_positions, readable), at))
    consumed = compute.is_in(positions, value_set=chosen)
    return converted, compute.fill_null(consumed, False)


def _residual(
    source: pyarrow.Array,
    items: pyarrow.StructArray,
    parents: pyarrow.Array,
    consumed: pyarrow.Array,
    rows: int,
) -> pyarrow.Array:
    """Every entry no declared OMS column now holds."""
    keep = compute.invert(compute.fill_null(consumed, False))
    sizes = dense_counts(compute.filter(parents, keep), rows)
    fields = [ENTRIES.value_type.field(index) for index in range(ENTRIES.value_type.num_fields)]
    values = pyarrow.StructArray.from_arrays(
        [compute.filter(compute.struct_field(items, field.name), keep) for field in fields],
        fields=fields,
    )
    return build_list(
        ENTRIES,
        sizes,
        values,
        compute.is_null(source) if source.null_count else None,
    )
