"""Arrow translation for flat FIX order and execution messages."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any

import pyarrow
import pyarrow.compute as compute

from rekep import txhash
from rekep.enums import Currency, EventType, MarketKind, Side, State, TimeInForce
from rekep.fields import column_names, encoded_key
from rekep.fields.arrays import build_list, build_map, dense_counts, interleave, sequence
from rekep.fields.names import column_name
from rekep.fix.access import FieldAccess
from rekep.fix.fields import cast_arrow_fix
from rekep.market.fix import (
    _TRADE_EVIDENCE_FIELDS,
    CANCEL_REJECT_HANDLER,
    EXECUTION_HANDLERS,
    EXECUTION_REPORT_HANDLER,
    EXECUTION_REQUEST_HANDLER,
    ORDER_HANDLERS,
    MarketTags,
)
from rekep.market.identity import (
    HASH,
    NIL,
    arrow_of,
    framed_arrow,
    hash_arrow,
    hash_bytes_arrow,
)
from rekep.market.instrument import Instrument
from rekep.market.orders import Execution, Order
from rekep.market.ticker import SymbolTicker

_REASON_FIELDS = (
    "OrdRejReason",
    "CxlRejReason",
    "CxlRejResponseTo",
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
    "XmlData",
    "SecureData",
    "Signature",
    *_REASON_FIELDS,
)
_READ_FIELDS = (
    "AggressorIndicator",
    "ClOrdID",
    "ClOrdLinkID",
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
    "SecurityExchange",
    "SecurityID",
    "SecurityIDSource",
    "SettlCurrency",
    "SettlCurrFxRateCalc",
    "SettlDate",
    "SettlType",
    "Side",
    "StopPx",
    "Symbol",
    "TimeInForce",
    "TradeID",
    "TrdMatchID",
    *_COMPLEX_FIELDS,
)
_FLOAT_FIELDS = (
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
#: Wire codes with the word spellings a bridge renders beside them, exact
#: codes winning any collision -- the flat mirror of `from_fix`'s fallback.
_SIDE_CODES = {**Side.worded_codes(), **Side._fix_codes()}
_TIF_CODES = {**TimeInForce.worded_codes(), **TimeInForce._fix_codes()}
#: Resolved component columns whose presence sends a row to the scalar
#: translator. The regulatory clocks steer transaction time; the instrument
#: groups feed `altids` and `legs` -- their count tags in `entries` used to
#: mark these rows, and the resolved column is where that presence lives now.
#: `Parties` is deliberately absent: order and execution rows never read it.
_COMPONENT_EXCLUSIONS = (
    "trdregtimestamps",
    "sidetrdregts",
    "securityaltid",
    "legs",
)


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
    columns = _with_instrument_columns({name: batch.column(name) for name in batch.schema.names})
    msg_type = columns.get("msgtype")
    version = columns.get("protocolversion")
    entries = columns.get("entries")
    if msg_type is None or version is None or entries is None:
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
        if handler in ORDER_HANDLERS and kind in tags.ordered
    )
    report_types = tuple(
        kind for kind, handler in tags.handlers.items() if handler == EXECUTION_REPORT_HANDLER
    )
    execution_types = tuple(
        kind for kind, handler in tags.handlers.items() if handler in EXECUTION_HANDLERS
    )
    supported_types = (*order_types, *report_types, *execution_types)
    if not supported_types:
        return None
    supported = compute.is_in(msg_type, value_set=pyarrow.array(supported_types))
    if supported.null_count or not compute.all(supported, min_count=0).as_py():
        return None
    if any(
        (column := columns.get(name)) is not None and column.null_count < batch.num_rows
        for name in _COMPONENT_EXCLUSIONS
    ):
        return None
    items = compute.list_flatten(entries)
    if len(items):
        tag = compute.struct_field(items, "tag")
        component = compute.struct_field(items, "comp")
        if (
            tag.null_count
            or compute.any(compute.less_equal(tag, 0), min_count=0).as_py()
            or component.null_count < len(component)
        ):
            return None
        parents = compute.list_parent_indices(entries).cast(pyarrow.int64())
        identities = compute.add(
            compute.multiply(parents, pyarrow.scalar(1 << 32, pyarrow.int64())),
            tag.cast(pyarrow.int64()),
        )
        if len(compute.unique(identities)) != len(identities):
            return None

    values = _Values(columns, entries, tags, batch.num_rows)
    if any(values.present(name) for name in _COMPLEX_FIELDS):
        return None
    if compute.any(
        compute.is_valid(SymbolTicker.currency_arrow(_ticker_array(values, tags))), min_count=0
    ).as_py():
        return None
    if compute.any(_expiring_rows(values), min_count=0).as_py():
        return None
    for name in _FLOAT_FIELDS:
        value = values.number(name)
        if value.null_count < len(value):
            finite = compute.fill_null(compute.is_finite(value), True)
            if not compute.all(finite, min_count=0).as_py():
                return None

    exec_state = values.mapped("ExecType", tags.execution_states, State.UNKNOWN)
    reports = compute.is_in(msg_type, value_set=pyarrow.array(report_types))
    report_executions = compute.and_(reports, compute.not_equal(exec_state, int(State.UNKNOWN)))
    for name in ("CumQty", "LeavesQty"):
        missing = compute.and_(report_executions, compute.is_null(values.number(name)))
        if compute.any(missing, min_count=0).as_py():
            return None

    shared = _Shared(values, tags)
    order_at = compute.indices_nonzero(
        compute.is_in(msg_type, value_set=pyarrow.array((*order_types, *report_types)))
    ).cast(pyarrow.int64())
    execution_rows = compute.is_in(msg_type, value_set=pyarrow.array(execution_types))
    request_types = tuple(
        kind for kind, handler in tags.handlers.items() if handler == EXECUTION_REQUEST_HANDLER
    )
    if request_types:
        # A report request with no trade content is a query, mirroring the
        # scalar reader: it fabricates no execution row.
        requests = compute.is_in(msg_type, value_set=pyarrow.array(request_types))
        execution_rows = compute.and_(
            execution_rows,
            compute.or_(compute.invert(requests), _trade_evidence_rows(values)),
        )
    execution_at = compute.indices_nonzero(compute.or_(execution_rows, report_executions)).cast(
        pyarrow.int64()
    )
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
    msg_type = columns.get("msgtype")
    versions = columns.get("protocolversion")
    entries = columns.get("entries")
    if msg_type is None or versions is None or entries is None:
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
                (handler in ORDER_HANDLERS and kind in tags.ordered)
                or handler == EXECUTION_REPORT_HANDLER
                or handler in EXECUTION_HANDLERS
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


def _trade_evidence_rows(values: _Values) -> pyarrow.Array:
    """Rows whose report request actually carries a trade, not just criteria."""
    evidence = pyarrow.repeat(pyarrow.scalar(False), values.rows)
    for name in _TRADE_EVIDENCE_FIELDS:
        text = values.text(name)
        evidence = compute.or_(evidence, compute.fill_null(compute.not_equal(text, ""), False))
    return evidence


def _expiring_rows(values: _Values) -> pyarrow.Array:
    """Rows whose time-in-force reads an expiry the flat translation cannot.

    The literal codes and their word spellings both count: a `gtd` a bridge
    wrote out resolves to the same expiring code the scalar reader sees, and
    a row it resolves on needs `ExpireDate <432>`/`ExpireTime <126>`, which
    only the scalar path reads.
    """
    text = values.text("TimeInForce")
    literal = compute.is_in(text, value_set=pyarrow.array(["6", "A"]))
    worded = compute.equal(
        _mapped(text, _TIF_CODES, TimeInForce.UNKNOWN),
        int(TimeInForce.GTD),
    )
    return compute.fill_null(compute.or_(literal, worded), False)


def _eligible_market_rows(
    columns: Mapping[str, pyarrow.Array], tags: MarketTags, rows: int
) -> pyarrow.Array:
    """Identify rows for which the flat translator mirrors scalar semantics."""
    columns = _with_instrument_columns(columns)
    eligible = pyarrow.repeat(pyarrow.scalar(True), rows)
    entries = columns["entries"]
    for name in _COMPONENT_EXCLUSIONS:
        column = columns.get(name)
        if column is not None:
            eligible = compute.and_(eligible, compute.is_null(column))

    items = compute.list_flatten(entries)
    if len(items):
        tag = compute.struct_field(items, "tag")
        component = compute.struct_field(items, "comp")
        parents = compute.list_parent_indices(entries).cast(pyarrow.int64())
        valid = compute.and_(compute.is_valid(tag), compute.greater(tag, 0))
        valid = compute.and_(valid, compute.is_null(component))
        eligible = compute.and_(
            eligible,
            compute.invert(_marked_market_rows(parents, compute.invert(valid), rows)),
        )
        duplicated = _duplicate_market_rows(parents, tag, rows)
        eligible = compute.and_(eligible, compute.invert(duplicated))

    values = _Values(columns, entries, tags, rows)
    for name in _COMPLEX_FIELDS:
        eligible = compute.and_(eligible, compute.is_null(values.raw(name, pyarrow.string())))
    eligible = compute.and_(
        eligible,
        compute.invert(compute.is_valid(SymbolTicker.currency_arrow(_ticker_array(values, tags)))),
    )
    eligible = compute.and_(eligible, compute.invert(_expiring_rows(values)))
    for name in _FLOAT_FIELDS:
        value = values.number(name)
        eligible = compute.and_(eligible, compute.fill_null(compute.is_finite(value), True))

    msg_type = columns["msgtype"]
    reports = pyarrow.array(
        [kind for kind, handler in tags.handlers.items() if handler == EXECUTION_REPORT_HANDLER]
    )
    report_rows = compute.is_in(msg_type, value_set=reports)
    exec_state = values.mapped("ExecType", tags.execution_states, State.UNKNOWN)
    report_executions = compute.and_(report_rows, compute.not_equal(exec_state, int(State.UNKNOWN)))
    for name in ("CumQty", "LeavesQty"):
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


def _with_instrument_columns(
    columns: Mapping[str, pyarrow.Array],
) -> dict[str, pyarrow.Array]:
    """Expose nested component members to the field reader without copying them."""
    found = dict(columns)
    instrument = found.get("instrument")
    if instrument is None:
        return found
    if isinstance(instrument, pyarrow.ChunkedArray):
        instrument = instrument.combine_chunks()
    for field in instrument.type:
        member = compute.struct_field(instrument, field.name)
        if pyarrow.types.is_string(member.type) or pyarrow.types.is_large_string(member.type):
            member = compute.if_else(compute.equal(member, ""), None, member)
        elif pyarrow.types.is_integer(member.type):
            member = compute.if_else(compute.equal(member, 0), None, member)
        found.setdefault(field.name, member)
    return found


class _Values:
    """Typed column views over one FixMsg batch."""

    def __init__(
        self,
        columns: Mapping[str, pyarrow.Array],
        entries: pyarrow.Array,
        tags: MarketTags,
        rows: int,
    ):
        self.columns = _with_instrument_columns(columns)
        self.rows = rows
        requested = tuple((tags.tags[name], name) for name in _READ_FIELDS if name in tags.tags)
        self.residual = FieldAccess.first_arrow_fields(entries, requested, rows)
        self._cache: dict[tuple[str, str], pyarrow.Array] = {}

    def raw(self, name: str, dtype: pyarrow.DataType) -> pyarrow.Array:
        """One FIX field, from its own column or from what `entries` still holds.

        Asked for by the dictionary's spelling and found under the folded one:
        a column is `parentclordid` and the field is `ParentClOrdID`, and the
        residual read is keyed by the name the caller asked with.
        """
        key = (name, str(dtype))
        found = self._cache.get(key)
        if found is not None:
            return found
        available = []
        for column in (self.columns.get(column_name(name)), self.residual.get(name)):
            if column is not None:
                available.append(cast_arrow_fix(column, dtype))
        found = compute.coalesce(*available) if available else pyarrow.nulls(self.rows, dtype)
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
        self.unixpartition = columns["unixpartition"].cast(pyarrow.int32(), safe=False)
        recunix = columns["recunix"].cast(pyarrow.int64(), safe=False)
        self.recunix = compute.if_else(compute.equal(recunix, 0), self.unix, recunix)
        self.reason = columns["reason"].cast(pyarrow.string(), safe=False)
        self.mic = columns["mic"].cast(pyarrow.int32(), safe=False)
        self.symbolticker = _ticker_array(values, tags)
        self.instrumentxhash = compute.if_else(
            compute.equal(self.symbolticker, ""),
            pyarrow.scalar(NIL, pyarrow.int64()),
            hash_arrow(self.symbolticker),
        )
        self.altids = FixMsg.altids_arrow(columns, rows, tags.tags)
        self.metadata = _metadata(values, tags)
        stated_currency, stated_unit = _currencies(values.text("Currency"))
        component_currency = values.columns.get("currency")
        if component_currency is None:
            self.currency, self.pxunit = stated_currency, stated_unit
        else:
            component_currency, component_unit = _currencies(component_currency)
            self.currency = compute.coalesce(component_currency, stated_currency)
            self.pxunit = compute.if_else(
                compute.equal(component_unit, ""), stated_unit, component_unit
            )

    def take(self, value: pyarrow.Array, where: pyarrow.Array) -> pyarrow.Array:
        return compute.take(value, where)


def _ticker_array(values: _Values, tags: MarketTags) -> pyarrow.Array:
    """Canonical ticker normalized by the component that owns its parts."""
    component = Instrument.from_fix_arrow(
        {
            "symbolticker": values.columns.get("symbolticker"),
            "symbol": values.text("Symbol"),
            "securityid": values.text("SecurityID"),
            "securityidsource": values.text("SecurityIDSource"),
            "securityexchange": values.text("SecurityExchange"),
        },
        values.rows,
        registry=tags.registry,
    )
    return compute.struct_field(component, "symbolticker")


def _orders(
    values: _Values,
    shared: _Shared,
    tags: MarketTags,
    where: pyarrow.Array,
    report_types: pyarrow.Array,
) -> pyarrow.RecordBatch:
    msg_type = compute.take(values.columns["msgtype"], where)
    unix = shared.take(shared.unix, where)
    state = _mapped(msg_type, tags.ordered, State.UNKNOWN)
    ord_status = shared.take(
        values.mapped("OrdStatus", tags.states["OrdStatus"], State.UNKNOWN), where
    )
    exec_state = shared.take(values.mapped("ExecType", tags.execution_states, State.UNKNOWN), where)
    state = compute.if_else(compute.is_in(msg_type, value_set=report_types), ord_status, state)
    # A cancel-reject's real state is `OrdStatus <39>` -- where the order
    # stands after the refusal -- mirroring the scalar dispatch's read.
    reject_types = tuple(
        kind for kind, handler in tags.handlers.items() if handler == CANCEL_REJECT_HANDLER
    )
    if reject_types:
        state = compute.if_else(
            compute.and_(
                compute.is_in(msg_type, value_set=pyarrow.array(reject_types)),
                compute.not_equal(ord_status, int(State.UNKNOWN)),
            ),
            ord_status,
            state,
        )
    state = compute.if_else(
        compute.equal(state, int(State.UNKNOWN)),
        shared.take(values.mapped("ExecType", tags.exec_type_fallbacks, State.UNKNOWN), where),
        state,
    ).cast(_code_type(State.UNKNOWN))
    total = shared.take(values.number("OrderQty"), where)
    cumulative = shared.take(values.number("CumQty"), where)
    leaves = shared.take(values.number("LeavesQty"), where)
    last = shared.take(values.number("LastQty"), where)
    cancelled = shared.take(values.number("CxlQty"), where)
    state = _replaced_state(state, total, cumulative, leaves)
    state, previous, current = _quantity_transition(
        state, exec_state, total, cumulative, leaves, last, cancelled
    )
    terminal = _ranked_at_least(state, State.DONE)
    previous = compute.if_else(
        compute.and_(terminal, compute.and_(compute.is_null(previous), compute.is_valid(current))),
        current,
        previous,
    )
    current = compute.if_else(terminal, pyarrow.scalar(0.0), current)
    side = shared.take(values.mapped("Side", _SIDE_CODES, Side.UNKNOWN), where)
    kind = shared.take(values.mapped("OrdType", tags.order_kinds, MarketKind.UNKNOWN), where)
    timeinforce = shared.take(values.mapped("TimeInForce", _TIF_CODES, TimeInForce.DAY), where)
    immediate = compute.is_in(
        timeinforce,
        value_set=pyarrow.array([int(TimeInForce.IOC), int(TimeInForce.FOK)], pyarrow.int32()),
    )
    expunix = compute.if_else(immediate, unix, pyarrow.nulls(len(where), pyarrow.int64()))
    displayed = shared.take(values.number("MaxFloor"), where)
    hidden = compute.if_else(
        compute.and_(compute.is_valid(current), compute.is_valid(displayed)),
        compute.max_element_wise(compute.subtract(current, displayed), 0.0),
        pyarrow.scalar(None, pyarrow.float64()),
    )
    hidden = compute.if_else(terminal, pyarrow.scalar(0.0), hidden)
    orderid = shared.take(values.text("OrderID"), where)
    client_id = shared.take(values.text("ClOrdID"), where)
    previous_client_id = shared.take(values.text("OrigClOrdID"), where)
    named = _first_nonempty(orderid, previous_client_id, client_id, fallback="")
    symbolticker = shared.take(shared.symbolticker, where)
    code = named
    instrumentxhash = shared.take(shared.instrumentxhash, where)
    mic = shared.take(shared.mic, where)
    named_life = compute.not_equal(named, "")
    xhash = compute.if_else(
        named_life,
        Order.hash_arrow(arrow_of(instrumentxhash), mic, named, side),
        pyarrow.scalar(NIL, pyarrow.int64()),
    )
    px = shared.take(values.number("Price"), where)
    currency = shared.take(shared.currency, where)
    reason = shared.take(shared.reason, where)
    vwap = pyarrow.nulls(len(where), pyarrow.float64())
    null_float = pyarrow.nulls(len(where), pyarrow.float64())
    eventtype = _constant(len(where), int(EventType.ORDER), pyarrow.int64())
    altids = shared.take(shared.altids, where)
    pxunit = shared.take(shared.pxunit, where)
    qtyunit = _constant(len(where), "", pyarrow.string())
    metadata = shared.take(shared.metadata, where)
    clordlinkid = shared.take(values.text("ClOrdLinkID"), where)
    parentclordid = shared.take(values.text("ParentClOrdID"), where)
    parentorderid = shared.take(values.text("ParentOrderID"), where)
    null_text = pyarrow.nulls(len(where), pyarrow.string())
    vhash = _value_hash_arrow(
        Order,
        (arrow_of(xhash), eventtype, state, mic, 0, code, reason),
        altids,
        (
            arrow_of(instrumentxhash),
            symbolticker,
            kind,
            side,
            px,
            pxunit,
            currency,
            current,
            qtyunit,
            null_float,
        ),
        metadata,
        (
            timeinforce,
            shared.take(values.number("StopPx"), where),
            hidden,
            vwap,
            False,
            orderid,
            client_id,
            previous_client_id,
            clordlinkid,
            parentclordid,
            parentorderid,
            pyarrow.nulls(len(where), pyarrow.int32()),
            null_text,
        ),
    )
    event_hash = txhash.couple128_arrow(Order._clock_micros(unix), vhash)
    columns: dict[str, pyarrow.Array] = {
        "unix": unix,
        "unixpartition": shared.take(shared.unixpartition, where),
        "eventtype": eventtype,
        "creaunix": unix,
        "recunix": shared.take(shared.recunix, where),
        "expunix": expunix,
        "hash": event_hash,
        "vhash": vhash,
        "xhash": xhash,
        "linkedhashes": _empty_lists(len(where), Order.into_field().field("linkedhashes").dtype),
        "version": _constant(len(where), 0, pyarrow.int64()),
        "state": state,
        "code": code,
        "altids": altids,
        "prevhash": pyarrow.nulls(len(where), HASH),
        "mic": mic,
        "reason": reason,
        "instrumentxhash": instrumentxhash,
        "symbolticker": symbolticker,
        "kind": kind,
        "side": side,
        "px": px,
        "pxunit": pxunit,
        "currency": currency,
        "qty": current,
        "prevqty": previous,
        "qtyunit": qtyunit,
        "metadata": metadata,
        "timeinforce": timeinforce,
        "stoppx": shared.take(values.number("StopPx"), where),
        "hiddenqty": hidden,
        "vwap": vwap,
        "indicative": _constant(len(where), False, pyarrow.bool_()),
        "orderid": orderid,
        "clordid": client_id,
        "origclordid": previous_client_id,
        # The reject-only columns stay null here: a row carrying either is
        # `_REASON_FIELDS`-complex and translates through the scalar path.
        "clordlinkid": clordlinkid,
        # Namespace identities live in their resolved columns, never as tags.
        "parentclordid": parentclordid,
        "parentorderid": parentorderid,
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
    msg_type = compute.take(values.columns["msgtype"], where)
    reported = compute.is_in(msg_type, value_set=report_types)
    unix = shared.take(shared.unix, where)
    state = shared.take(values.mapped("ExecType", tags.execution_states, State.UNKNOWN), where)
    side = shared.take(values.mapped("Side", _SIDE_CODES, Side.UNKNOWN), where)
    kind = shared.take(values.mapped("ExecType", tags.execution_kinds, MarketKind.UNKNOWN), where)
    execid = shared.take(values.text("ExecID"), where)
    execrefid = shared.take(values.text("ExecRefID"), where)
    tradeid = shared.take(_first_nonempty(values.text("TradeID"), values.text("TrdMatchID")), where)
    corrected = compute.and_(
        compute.is_in(
            state,
            value_set=pyarrow.array(
                [int(State.REPLACED), int(State.CANCELLED)], _code_type(State.UNKNOWN)
            ),
        ),
        compute.fill_null(compute.not_equal(execrefid, ""), False),
    )
    named = compute.fill_null(
        compute.if_else(corrected, execrefid, _first_nonempty(execid, tradeid)), ""
    )
    symbolticker = shared.take(shared.symbolticker, where)
    code = named
    instrumentxhash = shared.take(shared.instrumentxhash, where)
    mic = shared.take(shared.mic, where)
    named_life = compute.not_equal(named, "")
    xhash = compute.if_else(
        named_life,
        Execution.hash_arrow(arrow_of(instrumentxhash), mic, named, side),
        pyarrow.scalar(NIL, pyarrow.int64()),
    )
    orderid = shared.take(values.text("OrderID"), where)
    client_id = shared.take(values.text("ClOrdID"), where)
    previous_client_id = shared.take(values.text("OrigClOrdID"), where)
    px = shared.take(values.number("LastPx"), where)
    qty = shared.take(values.number("LastQty"), where)
    filled = shared.take(values.number("CumQty"), where)
    leaves = shared.take(values.number("LeavesQty"), where)
    first_fill = compute.and_(
        compute.equal(state, int(State.FILLED)),
        compute.and_(
            compute.and_(compute.is_valid(filled), compute.is_valid(qty)),
            compute.equal(filled, qty),
        ),
    )
    vwap = compute.if_else(first_fill, px, pyarrow.scalar(None, pyarrow.float64()))
    aggressor_text = shared.take(values.text("AggressorIndicator"), where)
    aggressor_head = compute.utf8_upper(
        compute.utf8_slice_codeunits(compute.utf8_trim_whitespace(aggressor_text), 0, 1)
    )
    aggressorindicator = compute.if_else(
        compute.equal(aggressor_head, "Y"),
        pyarrow.scalar(True),
        compute.if_else(
            compute.equal(aggressor_head, "N"),
            pyarrow.scalar(False),
            pyarrow.scalar(None, pyarrow.bool_()),
        ),
    )
    order_by_source = _order_lookup(orders, order_at, where)
    order_xhash = order_by_source["xhash"]
    order_hash = order_by_source["hash"]
    order_lifecycle = order_xhash
    linked_sizes = compute.if_else(reported, 1, 0).cast(pyarrow.int64())
    linked = build_list(
        Execution.into_field().field("linkedhashes").dtype,
        linked_sizes,
        compute.filter(order_lifecycle, reported),
    )
    parent = build_list(
        Execution.into_field().field("parenthash").dtype,
        linked_sizes,
        compute.filter(order_hash, reported),
    )
    currency = shared.take(shared.currency, where)
    reason = shared.take(shared.reason, where)
    null_float = pyarrow.nulls(rows, pyarrow.float64())
    eventtype = _constant(rows, int(EventType.EXECUTION), pyarrow.int64())
    altids = shared.take(shared.altids, where)
    pxunit = shared.take(shared.pxunit, where)
    qtyunit = _constant(rows, "", pyarrow.string())
    metadata = shared.take(shared.metadata, where)
    settldate = shared.take(values.raw("SettlDate", pyarrow.date32()), where)
    settltype = shared.take(values.text("SettlType"), where)
    settlcurrency = shared.take(values.text("SettlCurrency"), where)
    settlcurrfxratecalc = shared.take(values.text("SettlCurrFxRateCalc"), where)
    market_values = (
        arrow_of(instrumentxhash),
        symbolticker,
        kind,
        side,
        px,
        pxunit,
        currency,
        qty,
        qtyunit,
        null_float,
    )
    execution_values = (
        execid,
        execrefid,
        tradeid,
        orderid,
        client_id,
        previous_client_id,
        filled,
        leaves,
        vwap,
        aggressorindicator,
        settldate.cast(pyarrow.string()),
        settltype,
        settlcurrency,
        settlcurrfxratecalc,
    )
    no_link_vhash = _value_hash_arrow(
        Execution,
        (arrow_of(xhash), eventtype, state, mic, 0, code, reason),
        altids,
        market_values,
        metadata,
        execution_values,
    )
    linked_vhash = _value_hash_arrow(
        Execution,
        (arrow_of(xhash), eventtype, state, mic, 1, order_lifecycle, code, reason),
        altids,
        market_values,
        metadata,
        execution_values,
    )
    vhash = compute.if_else(reported, linked_vhash, no_link_vhash)
    event_hash = txhash.couple128_arrow(Execution._clock_micros(unix), vhash)
    columns: dict[str, pyarrow.Array] = {
        "unix": unix,
        "unixpartition": shared.take(shared.unixpartition, where),
        "eventtype": eventtype,
        "creaunix": unix,
        "recunix": shared.take(shared.recunix, where),
        "hash": event_hash,
        "vhash": vhash,
        "xhash": xhash,
        "linkedhashes": linked,
        "version": _constant(rows, 0, pyarrow.int64()),
        "state": state,
        "code": code,
        "altids": altids,
        "prevhash": pyarrow.nulls(rows, HASH),
        "parenthash": parent,
        "mic": mic,
        "reason": reason,
        "instrumentxhash": instrumentxhash,
        "symbolticker": symbolticker,
        "kind": kind,
        "side": side,
        "px": px,
        "pxunit": pxunit,
        "currency": currency,
        "qty": qty,
        "qtyunit": qtyunit,
        "metadata": metadata,
        "execid": execid,
        "execrefid": execrefid,
        "tradeid": tradeid,
        "orderid": orderid,
        "clordid": client_id,
        "origclordid": previous_client_id,
        "cumqty": filled,
        "leavesqty": leaves,
        "vwap": vwap,
        "aggressorindicator": aggressorindicator,
        "settldate": settldate,
        "settltype": settltype,
        "settlcurrency": settlcurrency,
        "settlcurrfxratecalc": settlcurrfxratecalc,
    }
    return _batch(Execution, columns, rows)


def _order_lookup(
    orders: pyarrow.RecordBatch | None, order_at: pyarrow.Array, execution_at: pyarrow.Array
) -> dict[str, pyarrow.Array]:
    rows = len(execution_at)
    if orders is None:
        return {
            "xhash": pyarrow.nulls(rows, pyarrow.int64()),
            "hash": pyarrow.nulls(rows, HASH),
        }
    locations = compute.index_in(execution_at, value_set=order_at)
    return {name: compute.take(orders.column(name), locations) for name in ("xhash", "hash")}


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
    ).cast(_code_type(State.UNKNOWN))
    terminal = _ranked_at_least(state, State.DONE)
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
    partial = _ranked_at_least(state, State.PARTIAL)
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
    ).cast(_code_type(State.UNKNOWN))
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
    return compute.if_else(replaced, normalized, state).cast(_code_type(State.UNKNOWN))


def _metadata(values: _Values, tags: MarketTags) -> pyarrow.Array:
    from rekep.text.fixmsg import FixMsg

    rows = values.rows
    candidates: list[pyarrow.Array] = []
    names: list[str] = []
    for name, default_tag in FixMsg.into_tagged_columns():
        column = values.columns.get(name)
        if column is None or column.null_count == rows:
            continue
        tag = tags.lookup_tags.get(name, default_tag)
        if tag in tags.claimed and tag not in tags.audited:
            continue
        candidates.append(cast_arrow_fix(column, pyarrow.string()))
        names.append(tag)

    promoted_parents = pyarrow.array([], pyarrow.int64())
    promoted_keys = pyarrow.array([], pyarrow.string())
    promoted_items = pyarrow.array([], pyarrow.string())
    promoted_ranks = pyarrow.array([], pyarrow.int64())
    if candidates:
        promoted, member = interleave(candidates, rows)
        promoted_present = compute.is_valid(promoted)
        promoted_parents = compute.filter(
            compute.divide(sequence(rows * len(candidates)), len(candidates)), promoted_present
        )
        promoted_keys = compute.filter(compute.take(pyarrow.array(names), member), promoted_present)
        promoted_items = compute.filter(promoted, promoted_present)
        promoted_ranks = compute.filter(member.cast(pyarrow.int64()), promoted_present)

    stored = values.columns["entries"]
    items = compute.list_flatten(stored)
    residual_parents = compute.list_parent_indices(stored).cast(pyarrow.int64())
    residual_tags = compute.struct_field(items, "tag")
    residual_keys = residual_tags.cast(pyarrow.string())
    claimed = pyarrow.array(sorted(tags.claimed))
    audited = pyarrow.array(sorted(tags.audited))
    residual_keep = compute.or_(
        compute.invert(compute.is_in(residual_keys, value_set=claimed)),
        compute.is_in(residual_keys, value_set=audited),
    )
    residual_values = compute.struct_field(items, "value")
    residual_ranks = compute.add(sequence(len(items)), len(candidates))

    kept_residual_parents = compute.filter(residual_parents, residual_keep)
    kept_residual_tags = compute.filter(residual_tags, residual_keep)
    if len(promoted_parents) and len(kept_residual_parents):
        promoted_identities = compute.add(
            compute.multiply(promoted_parents, pyarrow.scalar(1 << 32, pyarrow.int64())),
            promoted_keys.cast(pyarrow.int64()),
        )
        residual_identities = compute.add(
            compute.multiply(kept_residual_parents, pyarrow.scalar(1 << 32, pyarrow.int64())),
            kept_residual_tags.cast(pyarrow.int64()),
        )
        keep_promoted = compute.invert(
            compute.is_in(promoted_identities, value_set=residual_identities)
        )
        promoted_parents = compute.filter(promoted_parents, keep_promoted)
        promoted_keys = compute.filter(promoted_keys, keep_promoted)
        promoted_items = compute.filter(promoted_items, keep_promoted)
        promoted_ranks = compute.filter(promoted_ranks, keep_promoted)

    parents = pyarrow.concat_arrays(
        [
            promoted_parents,
            kept_residual_parents,
        ]
    )
    keys = pyarrow.concat_arrays(
        [
            promoted_keys,
            compute.filter(residual_keys, residual_keep),
        ]
    )
    items = pyarrow.concat_arrays(
        [
            promoted_items,
            compute.filter(residual_values, residual_keep),
        ]
    )
    ranks = pyarrow.concat_arrays(
        [
            promoted_ranks,
            compute.filter(residual_ranks, residual_keep),
        ]
    )
    if len(parents):
        stride = len(items) + len(candidates) + 1
        order = compute.array_sort_indices(compute.add(compute.multiply(parents, stride), ranks))
        parents = compute.take(parents, order)
        keys = compute.take(keys, order)
        items = compute.take(items, order)
    dtype = Order.into_field().field("metadata").dtype
    return build_map(dtype, dense_counts(parents, rows), keys, items)


def _value_hash_arrow(
    shape: type[Order] | type[Execution],
    event: tuple[Any, ...],
    altids: pyarrow.Array,
    market: tuple[Any, ...],
    metadata: pyarrow.Array,
    specific: tuple[Any, ...],
) -> pyarrow.Array:
    """Hash complete non-clock market values around their canonical maps."""
    framed = (
        framed_arrow(shape.__name__, *event),
        _mapping_frame_arrow(altids),
        framed_arrow(*market),
        _mapping_frame_arrow(metadata),
        framed_arrow(*specific),
    )
    joined = compute.binary_join_element_wise(
        *framed,
        pyarrow.scalar(b"", pyarrow.binary()),
        null_handling="replace",
        null_replacement=b"",
    )
    return hash_bytes_arrow(joined)


def _mapping_frame_arrow(values: pyarrow.Array) -> pyarrow.Array:
    """Optional map entries as deterministic identity-frame segments."""
    item = pyarrow.struct(
        [
            pyarrow.field("key", values.type.key_type, nullable=False),
            pyarrow.field("value", values.type.item_type, nullable=values.type.item_field.nullable),
        ]
    )
    listed = values.cast(pyarrow.list_(item), safe=False)
    counts = compute.fill_null(compute.list_value_length(listed), 0).cast(pyarrow.int64())
    entries = compute.list_flatten(listed)
    parents = compute.list_parent_indices(listed).cast(pyarrow.int64())
    keys = compute.struct_field(entries, "key")
    items = compute.struct_field(entries, "value")
    if len(entries):
        order = compute.sort_indices(
            pyarrow.record_batch([parents, keys], names=["parent", "key"]),
            sort_keys=[("parent", "ascending"), ("key", "ascending")],
        )
        keys = compute.take(keys, order)
        items = compute.take(items, order)
    entry_frames = (
        framed_arrow(keys, items) if len(entries) else pyarrow.array([], pyarrow.binary())
    )
    grouped = build_list(pyarrow.list_(pyarrow.binary()), counts, entry_frames)
    payload = compute.binary_join(grouped, pyarrow.scalar(b"", pyarrow.binary()))
    return compute.binary_join_element_wise(
        framed_arrow(compute.is_valid(values), counts),
        payload,
        pyarrow.scalar(b"", pyarrow.binary()),
    )


def _currencies(source: pyarrow.Array) -> tuple[pyarrow.Array, pyarrow.Array]:
    if pyarrow.types.is_integer(source.type):
        currency = source.cast(_code_type(Currency.UNKNOWN), safe=False)
        unique = compute.drop_null(compute.unique(currency))
        if not len(unique):
            return currency, pyarrow.repeat(pyarrow.scalar(""), len(source))
        units = pyarrow.array([Currency.from_int(value.as_py()).into_str() for value in unique])
        return currency, compute.fill_null(
            compute.take(units, compute.index_in(currency, value_set=unique)), ""
        )
    unique = compute.drop_null(compute.unique(source))
    if not len(unique):
        return pyarrow.nulls(len(source), _code_type(Currency.UNKNOWN)), pyarrow.repeat(
            pyarrow.scalar(""), len(source)
        )
    members = [Currency.from_fix(value.as_py()) for value in unique]
    positions = compute.index_in(source, value_set=unique)
    currency = compute.take(
        pyarrow.array([int(member) for member in members], _code_type(Currency.UNKNOWN)), positions
    )
    unit = compute.take(pyarrow.array([member.into_str() for member in members]), positions)
    return currency, compute.fill_null(unit, "")


def _ranked_at_least(codes: pyarrow.Array, floor: Any) -> pyarrow.Array:
    """Rows whose stored code ranks at or above `floor`.

    The stored value is a mnemonic, not an ordinal, so "at least this far
    along" is membership of the finite set the ranks name rather than a
    comparison of the packed bytes.
    """
    wanted = pyarrow.array(sorted(type(floor).ranked_at_least(floor)), _code_type(floor))
    return compute.fill_null(compute.is_in(codes, value_set=wanted), False)


def _code_type(default: Any) -> pyarrow.DataType:
    """The Arrow width one stable code's column stores, off the code itself."""
    declared = type(default)
    into_arrow_type = getattr(declared, "into_arrow_type", None)
    return pyarrow.int32() if into_arrow_type is None else into_arrow_type().index_type


def _mapped(source: pyarrow.Array, mapping: Mapping[str, Any], default: Any) -> pyarrow.Array:
    stored = _code_type(default)
    keys = pyarrow.array(list(mapping), pyarrow.string())
    values = pyarrow.array([int(value) for value in mapping.values()], stored)
    if not len(keys):
        return _constant(len(source), int(default), stored)
    # Trimmed like the scalar reading strips, so `"1 "` is the code it is on
    # both paths.
    source = compute.utf8_trim_whitespace(source)
    positions = compute.index_in(source, value_set=keys)
    found = compute.take(values, positions)
    if found.null_count > source.null_count:
        # A word spelling arrives in whatever case the bridge wrote. Fold the
        # word keys once and fill only what the exact pass left behind. Only
        # words: a one- or two-character key is a wire code, case-sensitive
        # by the standard and read by the exact pass alone -- the same reach
        # the scalar `_coded` lookup has. A key two words collide on is
        # dropped rather than guessed.
        folded: dict[str, int] = {}
        collided: set[str] = set()
        for key, value in mapping.items():
            if len(key) <= 2:
                continue
            normalized = encoded_key(key)
            if normalized in folded and folded[normalized] != int(value):
                collided.add(normalized)
            folded.setdefault(normalized, int(value))
        for normalized in collided:
            folded.pop(normalized, None)
        if folded:
            worded = compute.index_in(
                column_names(compute.utf8_trim_whitespace(source)),
                value_set=pyarrow.array(list(folded), pyarrow.string()),
            )
            fallback = compute.take(pyarrow.array(list(folded.values()), stored), worded)
            found = compute.coalesce(found, fallback)
    return compute.fill_null(found, int(default)).cast(stored)


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


def _empty_lists(rows: int, dtype: pyarrow.DataType) -> pyarrow.Array:
    return build_list(
        dtype,
        pyarrow.repeat(pyarrow.scalar(0, pyarrow.int64()), rows),
        pyarrow.array([], type=dtype.value_type),
    )


def _constant(rows: int, value: Any, dtype: pyarrow.DataType) -> pyarrow.Array:
    return pyarrow.repeat(pyarrow.scalar(value, dtype), rows)


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
