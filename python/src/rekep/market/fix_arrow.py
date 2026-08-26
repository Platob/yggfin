"""Arrow translation for flat FIX order and execution messages."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

import pyarrow
import pyarrow.compute as compute

from rekep.enums import Currency, EventType, MarketKind, Side, State, TimeInForce
from rekep.fields.arrays import build_list, build_map, dense_counts, interleave, sequence
from rekep.fix.access import FieldAccess
from rekep.fix.fields import cast_arrow_fix
from rekep.market.fix import MarketTags
from rekep.market.identity import NIL, hash_arrow
from rekep.market.orders import Execution, Order

_REASON_FIELDS = (
    "OrdRejReason",
    "CxlRejReason",
    "QuoteRejectReason",
    "ExecRestatementReason",
)
_COMPLEX_FIELDS = (
    "CFICode",
    "SecurityType",
    "ContractMultiplier",
    "NoSecurityAltID",
    "NoLegs",
    "ExpireTime",
    "ExpireDate",
    "ExposureDuration",
    "ExposureDurationUnit",
    *_REASON_FIELDS,
)
_READ_FIELDS = (
    "AggressorIndicator",
    "AvgPx",
    "ClOrdID",
    "CumQty",
    "Currency",
    "CxlQty",
    "ExecID",
    "ExecRefID",
    "ExecType",
    "LastPx",
    "LastQty",
    "LeavesQty",
    "MaxFloor",
    "OrderID",
    "OrderQty",
    "OrdStatus",
    "OrdType",
    "OrigClOrdID",
    "Price",
    "Side",
    "StopPx",
    "Symbol",
    "TimeInForce",
    "TradeID",
    "TrdMatchID",
    *_COMPLEX_FIELDS,
)
_FLOAT_FIELDS = (
    "AvgPx",
    "CumQty",
    "CxlQty",
    "LastPx",
    "LastQty",
    "LeavesQty",
    "MaxFloor",
    "OrderQty",
    "Price",
    "StopPx",
)
_METADATA_COLUMNS = {
    "MsgSeqNum": "34",
    "BeginString": "8",
    "OrdType": "40",
    "OrdStatus": "39",
    "ExecType": "150",
}


def into_flat_market_batches(
    batch: pyarrow.RecordBatch, declared: Mapping[str, Any]
) -> tuple[pyarrow.RecordBatch | None, pyarrow.RecordBatch | None] | None:
    """Translate one all-flat standard FIX batch, or return None for scalar fallback."""
    translated = flat_market_parts(batch, declared)
    return None if translated is None else translated[:2]


def flat_market_parts(
    batch: pyarrow.RecordBatch, declared: Mapping[str, Any]
) -> (
    tuple[
        pyarrow.RecordBatch | None,
        pyarrow.RecordBatch | None,
        pyarrow.Array,
        pyarrow.Array,
    ]
    | None
):
    """Translate a flat batch and retain each output row's source position."""
    if set(declared) - {"registry", "fix_version"} or not batch.num_rows:
        return None
    columns = {name: batch.column(name) for name in batch.schema.names}
    msg_type = columns.get("MsgType")
    version = columns.get("protocol_version")
    kwargs = columns.get("kwargs")
    if msg_type is None or version is None or kwargs is None:
        return None
    if version.null_count:
        return None
    configured_version = declared.get("fix_version")
    if configured_version is None:
        versions = compute.unique(version)
        if len(versions) != 1:
            return None
        configured_version = versions[0].as_py()
    tags = MarketTags.of(declared.get("registry"), configured_version)
    order_types = tuple(
        kind
        for kind, handler in tags.handlers.items()
        if handler == "order" and kind in tags.ordered
    )
    report_types = tuple(
        kind for kind, handler in tags.handlers.items() if handler == "executionreport"
    )
    execution_types = tuple(
        kind for kind, handler in tags.handlers.items() if handler == "execution"
    )
    supported_types = (*order_types, *report_types, *execution_types)
    if not supported_types:
        return None
    supported = compute.is_in(msg_type, value_set=pyarrow.array(supported_types))
    if supported.null_count or not compute.all(supported, min_count=0).as_py():
        return None
    if any(
        (column := columns.get(name)) is not None and column.null_count < batch.num_rows
        for name in ("TrdRegTimestamps", "SideTrdRegTS")
    ):
        return None
    entries = compute.list_flatten(kwargs)
    if len(entries):
        tag = compute.struct_field(entries, "tag")
        namespace = compute.struct_field(entries, "namespace")
        component = compute.struct_field(entries, "comp")
        if (
            tag.null_count
            or compute.any(compute.less_equal(tag, 0), min_count=0).as_py()
            or namespace.null_count < len(namespace)
            or component.null_count < len(component)
        ):
            return None
        parents = compute.list_parent_indices(kwargs).cast(pyarrow.int64())
        identities = compute.add(
            compute.multiply(parents, pyarrow.scalar(1 << 32, pyarrow.int64())),
            tag.cast(pyarrow.int64()),
        )
        if len(compute.unique(identities)) != len(identities):
            return None

    values = _Values(columns, kwargs, tags, batch.num_rows)
    if any(values.present(name) for name in _COMPLEX_FIELDS):
        return None
    symbol = values.text("Symbol", fallback="")
    if compute.any(compute.match_substring_regex(symbol, r"^[A-Za-z]{3}/[A-Za-z]{3}$")).as_py():
        return None
    tif_text = values.text("TimeInForce")
    expiring = compute.fill_null(
        compute.is_in(tif_text, value_set=pyarrow.array(["6", "A"])), False
    )
    if compute.any(expiring, min_count=0).as_py():
        return None
    for name in _FLOAT_FIELDS:
        value = values.number(name)
        if value.null_count < len(value):
            finite = compute.fill_null(compute.is_finite(value), True)
            if not compute.all(finite, min_count=0).as_py():
                return None

    exec_state = values.mapped("ExecType", tags.states["ExecType"], State.UNKNOWN)
    reports = compute.is_in(msg_type, value_set=pyarrow.array(report_types))
    report_executions = compute.and_(reports, compute.not_equal(exec_state, int(State.UNKNOWN)))
    for name in ("CumQty", "LeavesQty", "AvgPx"):
        missing = compute.and_(report_executions, compute.is_null(values.number(name)))
        if compute.any(missing, min_count=0).as_py():
            return None

    shared = _Shared(values, tags)
    order_at = compute.indices_nonzero(
        compute.is_in(msg_type, value_set=pyarrow.array((*order_types, *report_types)))
    ).cast(pyarrow.int64())
    execution_at = compute.indices_nonzero(
        compute.or_(
            compute.is_in(msg_type, value_set=pyarrow.array(execution_types)),
            report_executions,
        )
    ).cast(pyarrow.int64())
    reports_set = pyarrow.array(report_types)
    orders = _orders(values, shared, tags, order_at, reports_set) if len(order_at) else None
    executions = (
        _executions(values, shared, tags, execution_at, reports_set, orders, order_at)
        if len(execution_at)
        else None
    )
    return orders, executions, order_at, execution_at


def flat_market_positions(
    batch: pyarrow.RecordBatch, declared: Mapping[str, Any]
) -> Iterator[pyarrow.Array]:
    """Yield version-homogeneous rows accepted by flat market translation."""
    if set(declared) - {"registry", "fix_version"} or not batch.num_rows:
        return
    columns = {name: batch.column(name) for name in batch.schema.names}
    msg_type = columns.get("MsgType")
    versions = columns.get("protocol_version")
    kwargs = columns.get("kwargs")
    if msg_type is None or versions is None or kwargs is None:
        return
    positions = sequence(batch.num_rows)
    configured = declared.get("fix_version")
    grouped_versions = (
        ((pyarrow.scalar(configured), positions),)
        if configured is not None
        else (
            (
                version,
                compute.filter(positions, compute.equal(versions, version)),
            )
            for version in compute.drop_null(compute.unique(versions)).sort()
        )
    )
    for version, version_at in grouped_versions:
        tags = MarketTags.of(declared.get("registry"), version.as_py())
        supported = tuple(
            kind
            for kind, handler in tags.handlers.items()
            if (
                (handler == "order" and kind in tags.ordered)
                or handler in {"executionreport", "execution"}
            )
        )
        if not supported:
            continue
        candidate = compute.filter(
            version_at,
            compute.is_in(
                compute.take(msg_type, version_at),
                value_set=pyarrow.array(supported),
            ),
        )
        if not len(candidate):
            continue
        taken = {name: compute.take(column, candidate) for name, column in columns.items()}
        eligible = _eligible_market_rows(taken, tags, len(candidate))
        where = compute.filter(candidate, eligible)
        if len(where):
            yield where


def _eligible_market_rows(
    columns: Mapping[str, pyarrow.Array], tags: MarketTags, rows: int
) -> pyarrow.Array:
    """Identify rows for which the flat translator mirrors scalar semantics."""
    eligible = pyarrow.repeat(pyarrow.scalar(True), rows)
    kwargs = columns["kwargs"]
    for name in ("TrdRegTimestamps", "SideTrdRegTS"):
        column = columns.get(name)
        if column is not None:
            eligible = compute.and_(eligible, compute.is_null(column))

    entries = compute.list_flatten(kwargs)
    if len(entries):
        tag = compute.struct_field(entries, "tag")
        namespace = compute.struct_field(entries, "namespace")
        component = compute.struct_field(entries, "comp")
        parents = compute.list_parent_indices(kwargs).cast(pyarrow.int64())
        valid = compute.and_(compute.is_valid(tag), compute.greater(tag, 0))
        valid = compute.and_(valid, compute.is_null(namespace))
        valid = compute.and_(valid, compute.is_null(component))
        eligible = compute.and_(
            eligible,
            compute.invert(_marked_market_rows(parents, compute.invert(valid), rows)),
        )
        duplicated = _duplicate_market_rows(parents, tag, rows)
        eligible = compute.and_(eligible, compute.invert(duplicated))

    values = _Values(columns, kwargs, tags, rows)
    for name in _COMPLEX_FIELDS:
        eligible = compute.and_(eligible, compute.is_null(values.raw(name, pyarrow.string())))
    symbol = values.text("Symbol", fallback="")
    eligible = compute.and_(
        eligible,
        compute.invert(compute.match_substring_regex(symbol, r"^[A-Za-z]{3}/[A-Za-z]{3}$")),
    )
    expiring = compute.fill_null(
        compute.is_in(values.text("TimeInForce"), value_set=pyarrow.array(["6", "A"])),
        False,
    )
    eligible = compute.and_(eligible, compute.invert(expiring))
    for name in _FLOAT_FIELDS:
        value = values.number(name)
        eligible = compute.and_(eligible, compute.fill_null(compute.is_finite(value), True))

    msg_type = columns["MsgType"]
    reports = pyarrow.array(
        [kind for kind, handler in tags.handlers.items() if handler == "executionreport"]
    )
    report_rows = compute.is_in(msg_type, value_set=reports)
    exec_state = values.mapped("ExecType", tags.states["ExecType"], State.UNKNOWN)
    report_executions = compute.and_(report_rows, compute.not_equal(exec_state, int(State.UNKNOWN)))
    for name in ("CumQty", "LeavesQty", "AvgPx"):
        missing = compute.and_(report_executions, compute.is_null(values.number(name)))
        eligible = compute.and_(eligible, compute.invert(missing))
    return compute.fill_null(eligible, False)


def _marked_market_rows(parents: pyarrow.Array, marked: pyarrow.Array, rows: int) -> pyarrow.Array:
    """Mark rows having at least one selected residual entry."""
    found = compute.unique(compute.filter(parents, compute.fill_null(marked, True)))
    return compute.is_in(sequence(rows), value_set=found)


def _duplicate_market_rows(parents: pyarrow.Array, tags: pyarrow.Array, rows: int) -> pyarrow.Array:
    """Mark rows carrying one residual numeric tag more than once."""
    valid = compute.and_(compute.is_valid(tags), compute.greater(tags, 0))
    valid_parents = compute.filter(parents, valid)
    valid_tags = compute.filter(tags, valid).cast(pyarrow.int64())
    identities = compute.add(
        compute.multiply(valid_parents, pyarrow.scalar(1 << 32, pyarrow.int64())),
        valid_tags,
    )
    counted = compute.value_counts(identities)
    repeated = compute.filter(counted.field("values"), compute.greater(counted.field("counts"), 1))
    if not len(repeated):
        return pyarrow.repeat(pyarrow.scalar(False), rows)
    found = compute.index_in(repeated, value_set=identities)
    return compute.is_in(sequence(rows), value_set=compute.take(valid_parents, found))


class _Values:
    """Typed column views over one FixMsg batch."""

    def __init__(
        self,
        columns: Mapping[str, pyarrow.Array],
        kwargs: pyarrow.Array,
        tags: MarketTags,
        rows: int,
    ):
        self.columns = columns
        self.rows = rows
        requested = tuple((tags.tags[name], name) for name in _READ_FIELDS if name in tags.tags)
        self.residual = FieldAccess.first_arrow_fields(kwargs, requested, rows)
        self._cache: dict[tuple[str, str], pyarrow.Array] = {}

    def raw(self, name: str, arrow_type: pyarrow.DataType) -> pyarrow.Array:
        key = (name, str(arrow_type))
        found = self._cache.get(key)
        if found is not None:
            return found
        available = []
        for column in (self.columns.get(name), self.residual.get(name)):
            if column is not None:
                available.append(cast_arrow_fix(column, arrow_type))
        found = compute.coalesce(*available) if available else pyarrow.nulls(self.rows, arrow_type)
        self._cache[key] = found
        return found

    def text(self, name: str, fallback: str | None = None) -> pyarrow.Array:
        found = self.raw(name, pyarrow.string())
        return compute.fill_null(found, fallback) if fallback is not None else found

    def number(self, name: str) -> pyarrow.Array:
        return self.raw(name, pyarrow.float64())

    def present(self, name: str) -> bool:
        return self.raw(name, pyarrow.string()).null_count < self.rows

    def mapped(self, name: str, mapping: Mapping[str, Any], default: Any) -> pyarrow.Array:
        return _mapped(self.text(name), mapping, default)


class _Shared:
    """Market envelope columns shared by orders and executions."""

    def __init__(self, values: _Values, tags: MarketTags):
        from rekep.text.fixmsg import FixMsg

        columns = values.columns
        rows = values.rows
        self.rows = rows
        self.unix = columns["unix"].cast(pyarrow.int64(), safe=False)
        self.unix_partition = columns["unix_partition"].cast(pyarrow.int32(), safe=False)
        runix = columns["runix"].cast(pyarrow.int64(), safe=False)
        self.runix = compute.if_else(compute.equal(runix, 0), self.unix, runix)
        self.reason = columns["reason"].cast(pyarrow.string(), safe=False)
        self.mic = columns["mic"].cast(pyarrow.int32(), safe=False)
        self.symbol = values.text("Symbol", fallback="")
        self.instrument_xhash = compute.if_else(
            compute.equal(self.symbol, ""),
            pyarrow.scalar(NIL, pyarrow.int64()),
            hash_arrow("symbol", "", self.symbol),
        )
        self.codes = FixMsg.codes_arrow(columns, rows, tags.tags)
        self.metadata = _metadata(values, tags)
        currency = values.text("Currency")
        self.ccy, self.px_unit = _currencies(currency)

    def take(self, value: pyarrow.Array, where: pyarrow.Array) -> pyarrow.Array:
        return compute.take(value, where)


def _orders(
    values: _Values,
    shared: _Shared,
    tags: MarketTags,
    where: pyarrow.Array,
    report_types: pyarrow.Array,
) -> pyarrow.RecordBatch:
    msg_type = compute.take(values.columns["MsgType"], where)
    unix = shared.take(shared.unix, where)
    state = _mapped(msg_type, tags.ordered, State.UNKNOWN)
    ord_status = shared.take(
        values.mapped("OrdStatus", tags.states["OrdStatus"], State.UNKNOWN), where
    )
    exec_state = shared.take(
        values.mapped("ExecType", tags.states["ExecType"], State.UNKNOWN), where
    )
    state = compute.if_else(compute.is_in(msg_type, value_set=report_types), ord_status, state)
    state = compute.if_else(
        compute.equal(state, int(State.UNKNOWN)),
        shared.take(values.mapped("ExecType", tags.exec_order_states, State.UNKNOWN), where),
        state,
    ).cast(pyarrow.int32())
    total = shared.take(values.number("OrderQty"), where)
    cumulative = shared.take(values.number("CumQty"), where)
    leaves = shared.take(values.number("LeavesQty"), where)
    last = shared.take(values.number("LastQty"), where)
    cancelled = shared.take(values.number("CxlQty"), where)
    state = _replaced_state(state, total, cumulative, leaves)
    state, previous, current = _quantity_transition(
        state, exec_state, total, cumulative, leaves, last, cancelled
    )
    terminal = compute.greater_equal(state, int(State.DONE))
    previous = compute.if_else(
        compute.and_(terminal, compute.and_(compute.is_null(previous), compute.is_valid(current))),
        current,
        previous,
    )
    current = compute.if_else(terminal, pyarrow.scalar(0.0), current)
    side = shared.take(values.mapped("Side", Side._fix_codes(), Side.UNKNOWN), where)
    kind = shared.take(
        values.mapped("OrdType", MarketKind.fix_mapping()[40], MarketKind.UNKNOWN), where
    )
    tif = shared.take(
        values.mapped("TimeInForce", TimeInForce._fix_codes(), TimeInForce.DAY), where
    )
    immediate = compute.is_in(
        tif,
        value_set=pyarrow.array([int(TimeInForce.IOC), int(TimeInForce.FOK)], pyarrow.int32()),
    )
    eunix = compute.if_else(immediate, unix, pyarrow.nulls(len(where), pyarrow.int64()))
    displayed = shared.take(values.number("MaxFloor"), where)
    hidden = compute.if_else(
        compute.and_(compute.is_valid(current), compute.is_valid(displayed)),
        compute.max_element_wise(compute.subtract(current, displayed), 0.0),
        pyarrow.scalar(None, pyarrow.float64()),
    )
    hidden = compute.if_else(terminal, pyarrow.scalar(0.0), hidden)
    order_id = shared.take(values.text("OrderID"), where)
    client_id = shared.take(values.text("ClOrdID"), where)
    previous_client_id = shared.take(values.text("OrigClOrdID"), where)
    named = _first_nonempty(order_id, previous_client_id, client_id)
    symbol = shared.take(shared.symbol, where)
    code = _first_nonempty(named, symbol, fallback="")
    instrument_xhash = shared.take(shared.instrument_xhash, where)
    mic = shared.take(shared.mic, where)
    named_life = compute.not_equal(named, "")
    xhash = compute.if_else(
        named_life,
        Order.hash_arrow(instrument_xhash, mic, named, side),
        Order.hash_arrow(instrument_xhash, code, side),
    )
    absent_life = compute.and_(compute.equal(instrument_xhash, NIL), compute.equal(code, ""))
    xhash = compute.if_else(absent_life, pyarrow.scalar(NIL, pyarrow.int64()), xhash)
    px = shared.take(values.number("Price"), where)
    ccy = shared.take(shared.ccy, where)
    reason = shared.take(shared.reason, where)
    vwap = shared.take(values.number("AvgPx"), where)
    null_float = pyarrow.nulls(len(where), pyarrow.float64())
    event_hash = Order.hash_arrow(
        xhash,
        0,
        unix,
        state,
        mic,
        0,
        reason,
        kind,
        side,
        px,
        null_float,
        ccy,
        current,
        previous,
        null_float,
        null_float,
        client_id,
        hidden,
        vwap,
        False,
    )
    columns: dict[str, pyarrow.Array] = {
        "unix": unix,
        "unix_partition": shared.take(shared.unix_partition, where),
        "etype": _constant(len(where), int(EventType.ORDER), pyarrow.int32()),
        "cunix": unix,
        "runix": shared.take(shared.runix, where),
        "eunix": eunix,
        "hash": event_hash,
        "xhash": xhash,
        "linked_events": _empty_lists(
            len(where), Order.into_field().field("linked_events").arrow_type
        ),
        "version": _constant(len(where), 0, pyarrow.int64()),
        "state": state,
        "code": code,
        "codes": shared.take(shared.codes, where),
        "mic": mic,
        "reason": reason,
        "instrument_xhash": instrument_xhash,
        "instrument_code": symbol,
        "kind": kind,
        "side": side,
        "px": px,
        "px_unit": shared.take(shared.px_unit, where),
        "ccy": ccy,
        "qty": current,
        "prev_qty": previous,
        "qty_unit": _constant(len(where), "", pyarrow.string()),
        "metadata": shared.take(shared.metadata, where),
        "tif": tif,
        "stop_px": shared.take(values.number("StopPx"), where),
        "hidden_qty": hidden,
        "vwap": vwap,
        "indicative": _constant(len(where), False, pyarrow.bool_()),
        "order_id": order_id,
        "client_order_id": client_id,
        "prev_client_order_id": previous_client_id,
    }
    return _batch(Order, columns, len(where))


def _executions(
    values: _Values,
    shared: _Shared,
    tags: MarketTags,
    where: pyarrow.Array,
    report_types: pyarrow.Array,
    orders: pyarrow.RecordBatch | None,
    order_at: pyarrow.Array,
) -> pyarrow.RecordBatch:
    rows = len(where)
    msg_type = compute.take(values.columns["MsgType"], where)
    reported = compute.is_in(msg_type, value_set=report_types)
    unix = shared.take(shared.unix, where)
    state = shared.take(values.mapped("ExecType", tags.states["ExecType"], State.UNKNOWN), where)
    side = shared.take(values.mapped("Side", Side._fix_codes(), Side.UNKNOWN), where)
    kind = shared.take(
        values.mapped("ExecType", MarketKind.fix_mapping()[150], MarketKind.UNKNOWN), where
    )
    exec_id = shared.take(values.text("ExecID"), where)
    exec_ref_id = shared.take(values.text("ExecRefID"), where)
    trade_id = shared.take(
        _first_nonempty(values.text("TradeID"), values.text("TrdMatchID")), where
    )
    corrected = compute.and_(
        compute.is_in(
            state,
            value_set=pyarrow.array([int(State.REPLACED), int(State.CANCELLED)], pyarrow.int32()),
        ),
        compute.fill_null(compute.not_equal(exec_ref_id, ""), False),
    )
    named = compute.if_else(corrected, exec_ref_id, _first_nonempty(exec_id, trade_id))
    symbol = shared.take(shared.symbol, where)
    code = _first_nonempty(named, symbol, fallback="")
    instrument_xhash = shared.take(shared.instrument_xhash, where)
    mic = shared.take(shared.mic, where)
    named_life = compute.not_equal(named, "")
    xhash = compute.if_else(
        named_life,
        Execution.hash_arrow(instrument_xhash, mic, named, side),
        Execution.hash_arrow(instrument_xhash, code, side),
    )
    absent_life = compute.and_(compute.equal(instrument_xhash, NIL), compute.equal(code, ""))
    xhash = compute.if_else(absent_life, pyarrow.scalar(NIL, pyarrow.int64()), xhash)
    order_id = shared.take(values.text("OrderID"), where)
    client_id = shared.take(values.text("ClOrdID"), where)
    previous_client_id = shared.take(values.text("OrigClOrdID"), where)
    px = shared.take(values.number("LastPx"), where)
    qty = shared.take(values.number("LastQty"), where)
    filled = shared.take(values.number("CumQty"), where)
    leaves = shared.take(values.number("LeavesQty"), where)
    vwap = shared.take(values.number("AvgPx"), where)
    aggressor_text = shared.take(values.text("AggressorIndicator"), where)
    aggressor_head = compute.utf8_upper(
        compute.utf8_slice_codeunits(compute.utf8_trim_whitespace(aggressor_text), 0, 1)
    )
    aggressor = compute.if_else(
        compute.equal(aggressor_head, "Y"),
        pyarrow.scalar(True),
        compute.if_else(
            compute.equal(aggressor_head, "N"),
            pyarrow.scalar(False),
            pyarrow.scalar(None, pyarrow.bool_()),
        ),
    )
    order_by_source = _order_lookup(orders, order_at, where)
    order_unix = order_by_source["unix"]
    order_xhash = order_by_source["xhash"]
    order_hash = order_by_source["hash"]
    linked_sizes = compute.if_else(reported, 1, 0).cast(pyarrow.int64())
    linked_values = pyarrow.StructArray.from_arrays(
        [compute.filter(order_unix, reported), compute.filter(order_xhash, reported)],
        fields=list(Execution.into_field().field("linked_events").arrow_type.value_type),
    )
    linked = build_list(
        Execution.into_field().field("linked_events").arrow_type,
        linked_sizes,
        linked_values,
    )
    parent = build_list(
        Execution.into_field().field("parent_hash").arrow_type,
        linked_sizes,
        compute.filter(order_hash, reported),
    )
    ccy = shared.take(shared.ccy, where)
    reason = shared.take(shared.reason, where)
    null_float = pyarrow.nulls(rows, pyarrow.float64())
    no_link_hash = Execution.hash_arrow(
        xhash,
        0,
        unix,
        state,
        mic,
        0,
        reason,
        kind,
        side,
        px,
        null_float,
        ccy,
        qty,
        null_float,
        null_float,
        null_float,
        exec_id,
        filled,
        vwap,
    )
    linked_hash = Execution.hash_arrow(
        xhash,
        0,
        unix,
        state,
        mic,
        1,
        order_unix,
        order_xhash,
        reason,
        kind,
        side,
        px,
        null_float,
        ccy,
        qty,
        null_float,
        null_float,
        null_float,
        exec_id,
        filled,
        vwap,
    )
    event_hash = compute.if_else(reported, linked_hash, no_link_hash)
    columns: dict[str, pyarrow.Array] = {
        "unix": unix,
        "unix_partition": shared.take(shared.unix_partition, where),
        "etype": _constant(rows, int(EventType.EXECUTION), pyarrow.int32()),
        "cunix": unix,
        "runix": shared.take(shared.runix, where),
        "hash": event_hash,
        "xhash": xhash,
        "linked_events": linked,
        "version": _constant(rows, 0, pyarrow.int64()),
        "state": state,
        "code": code,
        "codes": shared.take(shared.codes, where),
        "parent_hash": parent,
        "mic": mic,
        "reason": reason,
        "instrument_xhash": instrument_xhash,
        "instrument_code": symbol,
        "kind": kind,
        "side": side,
        "px": px,
        "px_unit": shared.take(shared.px_unit, where),
        "ccy": ccy,
        "qty": qty,
        "qty_unit": _constant(rows, "", pyarrow.string()),
        "metadata": shared.take(shared.metadata, where),
        "exec_id": exec_id,
        "exec_ref_id": exec_ref_id,
        "trade_id": trade_id,
        "order_id": order_id,
        "client_order_id": client_id,
        "prev_client_order_id": previous_client_id,
        "filled_qty": filled,
        "leaves_qty": leaves,
        "vwap": vwap,
        "aggressor": aggressor,
    }
    return _batch(Execution, columns, rows)


def _order_lookup(
    orders: pyarrow.RecordBatch | None, order_at: pyarrow.Array, execution_at: pyarrow.Array
) -> dict[str, pyarrow.Array]:
    rows = len(execution_at)
    if orders is None:
        return {name: pyarrow.nulls(rows, pyarrow.int64()) for name in ("unix", "xhash", "hash")}
    locations = compute.index_in(execution_at, value_set=order_at)
    return {
        name: compute.take(orders.column(name), locations) for name in ("unix", "xhash", "hash")
    }


def _quantity_transition(
    state: pyarrow.Array,
    execution_state: pyarrow.Array,
    total: pyarrow.Array,
    cumulative: pyarrow.Array,
    leaves: pyarrow.Array,
    last: pyarrow.Array,
    cancelled: pyarrow.Array,
) -> tuple[pyarrow.Array, pyarrow.Array, pyarrow.Array]:
    zero = pyarrow.scalar(0.0)
    total = _nonnegative(total)
    cumulative = _nonnegative(cumulative)
    leaves = _nonnegative(leaves)
    last = _nonnegative(last)
    cancelled = _nonnegative(cancelled)
    filled = compute.equal(execution_state, int(State.FILLED))
    completely_filled = compute.or_(
        compute.fill_null(compute.equal(leaves, 0.0), False),
        compute.fill_null(
            compute.and_(
                compute.and_(compute.is_valid(total), compute.is_valid(cumulative)),
                compute.greater_equal(cumulative, total),
            ),
            False,
        ),
    )
    infer = compute.and_(compute.equal(state, int(State.UNKNOWN)), filled)
    state = compute.if_else(
        infer,
        compute.if_else(completely_filled, int(State.FILLED), int(State.PARTIALLY_FILLED)),
        state,
    ).cast(pyarrow.int32())
    terminal = compute.greater_equal(state, int(State.DONE))
    total_minus_cumulative = compute.max_element_wise(compute.subtract(total, cumulative), zero)
    previous_terminal = compute.coalesce(
        compute.if_else(
            compute.and_(compute.is_valid(leaves), compute.is_valid(last)),
            compute.add(leaves, last),
            pyarrow.scalar(None, pyarrow.float64()),
        ),
        compute.if_else(
            compute.and_(compute.is_valid(total), compute.is_valid(cumulative)),
            compute.if_else(compute.equal(state, int(State.FILLED)), total, total_minus_cumulative),
            pyarrow.scalar(None, pyarrow.float64()),
        ),
        cancelled,
        total,
        last,
        cumulative,
    )
    partial = compute.greater_equal(state, int(State.PARTIAL))
    current = compute.coalesce(
        leaves,
        compute.if_else(
            compute.and_(compute.is_valid(total), compute.is_valid(cumulative)),
            total_minus_cumulative,
            pyarrow.scalar(None, pyarrow.float64()),
        ),
        compute.if_else(
            compute.and_(partial, compute.and_(compute.is_valid(total), compute.is_valid(last))),
            compute.max_element_wise(compute.subtract(total, last), zero),
            pyarrow.scalar(None, pyarrow.float64()),
        ),
        total,
    )
    previous_partial = compute.coalesce(
        compute.if_else(
            compute.and_(compute.is_valid(leaves), compute.is_valid(last)),
            compute.add(leaves, last),
            pyarrow.scalar(None, pyarrow.float64()),
        ),
        compute.if_else(
            compute.and_(compute.is_valid(current), compute.is_valid(last)),
            compute.add(current, last),
            pyarrow.scalar(None, pyarrow.float64()),
        ),
        compute.if_else(
            compute.and_(compute.is_valid(leaves), compute.is_valid(cumulative)),
            compute.add(leaves, cumulative),
            pyarrow.scalar(None, pyarrow.float64()),
        ),
        compute.if_else(
            compute.and_(compute.is_valid(total), compute.not_equal(current, total)),
            total,
            pyarrow.scalar(None, pyarrow.float64()),
        ),
    )
    previous = compute.if_else(
        terminal,
        previous_terminal,
        compute.if_else(partial, previous_partial, pyarrow.scalar(None, pyarrow.float64())),
    )
    state = compute.if_else(
        compute.and_(
            compute.equal(state, int(State.PARTIALLY_FILLED)),
            compute.and_(filled, compute.fill_null(compute.equal(current, 0.0), False)),
        ),
        int(State.FILLED),
        state,
    ).cast(pyarrow.int32())
    current = compute.if_else(terminal, zero, current)
    return state, previous, current


def _replaced_state(
    state: pyarrow.Array, total: pyarrow.Array, cumulative: pyarrow.Array, leaves: pyarrow.Array
) -> pyarrow.Array:
    replaced = compute.equal(state, int(State.REPLACED))
    filled = compute.and_(
        compute.fill_null(compute.equal(leaves, 0.0), False),
        compute.fill_null(
            compute.and_(compute.is_valid(total), compute.equal(cumulative, total)), False
        ),
    )
    partial = compute.fill_null(compute.greater(cumulative, 0.0), False)
    normalized = compute.if_else(
        filled,
        int(State.FILLED),
        compute.if_else(partial, int(State.PARTIALLY_FILLED), int(State.NEW)),
    )
    return compute.if_else(replaced, normalized, state).cast(pyarrow.int32())


def _metadata(values: _Values, tags: MarketTags) -> pyarrow.Array:
    rows = values.rows
    candidates: list[pyarrow.Array] = []
    names: list[str] = []
    for name, tag in _METADATA_COLUMNS.items():
        column = values.columns.get(name)
        if column is None:
            continue
        candidates.append(cast_arrow_fix(column, pyarrow.string()))
        names.append(str(tags.tags.get(name, int(tag))))
    promoted, member = interleave(candidates, rows)
    promoted_present = compute.is_valid(promoted)
    promoted_parents = compute.divide(sequence(rows * len(candidates)), len(candidates))
    promoted_ranks = member.cast(pyarrow.int64())
    promoted_keys = compute.take(pyarrow.array(names), member)

    stored = values.columns["kwargs"]
    entries = compute.list_flatten(stored)
    residual_parents = compute.list_parent_indices(stored).cast(pyarrow.int64())
    residual_tags = compute.struct_field(entries, "tag")
    residual_keys = residual_tags.cast(pyarrow.string())
    claimed = pyarrow.array(sorted(tags.claimed))
    audited = pyarrow.array(sorted(tags.audited))
    residual_keep = compute.or_(
        compute.invert(compute.is_in(residual_keys, value_set=claimed)),
        compute.is_in(residual_keys, value_set=audited),
    )
    residual_values = compute.struct_field(entries, "value")
    residual_ranks = compute.add(sequence(len(entries)), len(candidates))

    parents = pyarrow.concat_arrays(
        [
            compute.filter(promoted_parents, promoted_present),
            compute.filter(residual_parents, residual_keep),
        ]
    )
    keys = pyarrow.concat_arrays(
        [
            compute.filter(promoted_keys, promoted_present),
            compute.filter(residual_keys, residual_keep),
        ]
    )
    items = pyarrow.concat_arrays(
        [
            compute.filter(promoted, promoted_present),
            compute.filter(residual_values, residual_keep),
        ]
    )
    ranks = pyarrow.concat_arrays(
        [
            compute.filter(promoted_ranks, promoted_present),
            compute.filter(residual_ranks, residual_keep),
        ]
    )
    if len(parents):
        stride = len(entries) + len(candidates) + 1
        order = compute.array_sort_indices(compute.add(compute.multiply(parents, stride), ranks))
        parents = compute.take(parents, order)
        keys = compute.take(keys, order)
        items = compute.take(items, order)
    arrow_type = Order.into_field().field("metadata").arrow_type
    return build_map(arrow_type, dense_counts(parents, rows), keys, items)


def _currencies(source: pyarrow.Array) -> tuple[pyarrow.Array, pyarrow.Array]:
    unique = compute.drop_null(compute.unique(source))
    if not len(unique):
        return pyarrow.nulls(len(source), pyarrow.int32()), pyarrow.repeat(
            pyarrow.scalar(""), len(source)
        )
    members = [Currency.from_fix(value.as_py()) for value in unique]
    positions = compute.index_in(source, value_set=unique)
    ccy = compute.take(
        pyarrow.array([int(member) for member in members], pyarrow.int32()), positions
    )
    unit = compute.take(pyarrow.array([member.into_str() for member in members]), positions)
    return ccy, compute.fill_null(unit, "")


def _mapped(source: pyarrow.Array, mapping: Mapping[str, Any], default: Any) -> pyarrow.Array:
    keys = pyarrow.array(list(mapping), pyarrow.string())
    values = pyarrow.array([int(value) for value in mapping.values()], pyarrow.int32())
    if not len(keys):
        return _constant(len(source), int(default), pyarrow.int32())
    positions = compute.index_in(source, value_set=keys)
    return compute.fill_null(compute.take(values, positions), int(default)).cast(pyarrow.int32())


def _first_nonempty(*columns: pyarrow.Array, fallback: str | None = None) -> pyarrow.Array:
    rows = len(columns[0])
    found = pyarrow.nulls(rows, pyarrow.string())
    for column in columns:
        present = compute.fill_null(
            compute.and_(compute.is_valid(column), compute.not_equal(column, "")), False
        )
        found = compute.if_else(compute.and_(compute.is_null(found), present), column, found)
    return compute.fill_null(found, fallback) if fallback is not None else found


def _nonnegative(column: pyarrow.Array) -> pyarrow.Array:
    """Clamp present quantities without turning absence into zero."""
    return compute.if_else(
        compute.is_valid(column),
        compute.max_element_wise(column, 0.0),
        pyarrow.scalar(None, pyarrow.float64()),
    )


def _empty_lists(rows: int, arrow_type: pyarrow.DataType) -> pyarrow.Array:
    return build_list(
        arrow_type,
        pyarrow.repeat(pyarrow.scalar(0, pyarrow.int64()), rows),
        pyarrow.array([], type=arrow_type.value_type),
    )


def _constant(rows: int, value: Any, arrow_type: pyarrow.DataType) -> pyarrow.Array:
    return pyarrow.repeat(pyarrow.scalar(value, arrow_type), rows)


def _batch(
    shape: type[Any], columns: Mapping[str, pyarrow.Array], rows: int
) -> pyarrow.RecordBatch:
    schema = shape.into_field().into_arrow_schema()
    arrays = []
    for field in schema:
        value = columns.get(field.name)
        if value is None:
            value = pyarrow.nulls(rows, field.type)
        elif value.type != field.type:
            value = value.cast(field.type, safe=False)
        arrays.append(value)
    return pyarrow.RecordBatch.from_arrays(arrays, schema=schema)
