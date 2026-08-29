"""`BookIterator`: one stream of events in and one book stream out."""

from __future__ import annotations

import dataclasses
import datetime
from pathlib import Path

import pyarrow
import pytest

from rekep import FixCodec, Message
from rekep.fix import FixFieldValue, FixRegistry, record_copy
from rekep.fix.columns import ISIN_SCHEME
from rekep.market import (
    MIC,
    AssetKind,
    Book,
    BookIterator,
    Currency,
    EventType,
    Execution,
    Instrument,
    MarketEvent,
    MarketKind,
    Order,
    Side,
    State,
)
from rekep.market.book import _resting, _Side
from rekep.market.event import DAY, HOUR
from rekep.market.fix_arrow import into_flat_market_batches
from rekep.market.identity import NIL, hash_bytes_of
from rekep.text import FixMsg

DATA = Path(__file__).resolve().parents[3] / "data" / "fix"

#: An instant on an hour boundary, so a snapshot's `unix` is legible.
BASE = (1_787_000_000_000_000_000 // HOUR) * HOUR

BTC = Instrument(symbol="BTC-USD", securityexchange="XCME")
ETH = Instrument(symbol="ETH-USD", securityexchange="XCME")
#: The same instrument, as a later message spells it: more is known.
BTC_RICH = Instrument(
    symbol="BTC-USD",
    securityexchange="XCME",
    currency="USD",
    cficode="FFICSX",
    kind=AssetKind.FUTURE,
)


def initial[EventT: MarketEvent](event: EventT, instrument: Instrument = BTC) -> EventT:
    """Attach transient reference data and require an initial version."""
    built = event.attach_instrument(instrument).with_previous(None)
    assert built is not None
    return built


def order(unix: int, about: Instrument, side: Side, px: float, qty: float, named: str, **given):
    declared = {
        "unix": unix,
        "side": side,
        "px": px,
        "qty": qty,
        "orderid": named,
        "state": State.NEW,
    }
    return initial(Order(**{**declared, **given}), about)


def with_instruments(events, instruments):
    """One time-sorted stream, with reference versions visible at equal instants."""
    return sorted(
        [*events, *instruments],
        key=lambda event: (
            event.unix,
            0 if isinstance(event, Instrument) else 1,
            event.hash,
        ),
    )


# -- one stream in, one out --------------------------------------------------


def test_a_negative_snapshot_interval_is_refused_before_iteration() -> None:
    with pytest.raises(ValueError, match="snapshot_every"):
        BookIterator(snapshot_every=-1)


def test_orders_expire_after_one_unchanged_day_by_default() -> None:
    assert BookIterator().max_order_age_ns == DAY


def test_instrument_versioning_is_owned_outside_the_book_fold() -> None:
    events = [order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")]
    iterating = BookIterator.from_events(events)
    assert [one.unix for one in iterating.books] == [BASE]
    assert [one.code for one in Instrument.from_events(events)] == ["BTC-USD"]


def test_sorted_logs_feed_instruments_and_books_without_a_task_adapter() -> None:
    log = FixMsg(
        unix=BASE,
        msgtype="D",
        symbol="BTC-USD",
        clordid="B1",
        side="1",
        ordtype="2",
        price=100.0,
        orderqty=2.0,
        beginstring="FIX.4.4",
    )

    (instrument,) = Instrument.from_fixmsgs([log], snapshot_every=0)
    (book,) = BookIterator(logs=[instrument.into_fixmsg(), log], snapshot_every=0)

    assert instrument.symbol == book.code == "BTC-USD"
    assert book.instrumentxhash == instrument.xhash
    assert book.bidpx == 100.0 and book.bidqty == 2.0


def test_book_translation_uses_the_selected_fix_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix", offline=True)
    seen = []

    def translated(_message, **declared):
        seen.append(declared.get("registry"))
        return iter(())

    monkeypatch.setattr(FixMsg, "into_market_events", translated)

    assert list(BookIterator(logs=[FixMsg(eventtype=EventType.ORDER)], registry=registry)) == []
    assert seen == [registry]


def test_log_symbol_uses_the_best_available_instrument_spelling() -> None:
    columns = {
        "symbol": pyarrow.array(["AAPL", None, None]),
        "securityid": pyarrow.array(["ignored", "US0378331005", None]),
        "isincode": pyarrow.array([None, "ignored", "FR0000120271"]),
    }
    assert FixMsg.symbol_arrow(columns, 3).to_pylist() == [
        "AAPL",
        "US0378331005",
        "FR0000120271",
    ]


def test_log_altids_retain_lifecycle_identifiers_in_lookup_order() -> None:
    columns = {
        "orderid": pyarrow.array(["ORD-1", None]),
        "origclordid": pyarrow.array(["CL-0", None]),
        "clordid": pyarrow.array(["CL-1", "CL-2"]),
        "execid": pyarrow.array(["EX-1", None]),
        "quotesetid": pyarrow.array([None, "SET-1"]),
        "symbol": pyarrow.array(["AAPL", "MSFT"]),
    }

    altids = FixMsg.altids_arrow(columns, 2).to_pylist(maps_as_pydicts="strict")

    assert list(altids[0].items()) == [
        ("orderid", "ORD-1"),
        ("origclordid", "CL-0"),
        ("clordid", "CL-1"),
        ("execid", "EX-1"),
    ]
    assert list(altids[1].items()) == [
        ("clordid", "CL-2"),
        ("quotesetid", "SET-1"),
    ]


def test_log_altids_read_unpromoted_identifiers_from_parsed_entries() -> None:
    log = FixMsg(
        entries=[
            (198, "ORD-SECONDARY"),
            (526, "CL-SECONDARY"),
            (527, "EXEC-SECONDARY"),
            (19, "EXEC-REF"),
            (1003, "TRADE"),
            (880, "MATCH"),
            (278, "MD-ENTRY"),
            (280, "MD-REF"),
        ]
    )
    batch = pyarrow.Table.from_pylist(
        [log.into_row()], schema=FixMsg.into_field().into_arrow_schema()
    ).to_batches()[0]

    (altids,) = FixMsg.altids_arrow({"entries": batch.column("entries")}, 1).to_pylist(
        maps_as_pydicts="strict"
    )

    assert list(altids.items()) == [
        ("secondaryorderid", "ORD-SECONDARY"),
        ("secondaryclordid", "CL-SECONDARY"),
        ("secondaryexecid", "EXEC-SECONDARY"),
        ("execrefid", "EXEC-REF"),
        ("tradeid", "TRADE"),
        ("trdmatchid", "MATCH"),
        ("mdentryid", "MD-ENTRY"),
        ("mdentryrefid", "MD-REF"),
    ]


def test_log_altids_match_rendered_unpromoted_identifier_names() -> None:
    log = FixMsg(
        entries=[
            ("SecondaryExecID", "EXEC-NAMED"),
            ("MDEntryRefID", "MD-NAMED"),
        ]
    )
    batch = pyarrow.Table.from_pylist(
        [log.into_row()], schema=FixMsg.into_field().into_arrow_schema()
    ).to_batches()[0]

    (altids,) = FixMsg.altids_arrow({"entries": batch.column("entries")}, 1).to_pylist(
        maps_as_pydicts="strict"
    )

    assert list(altids.items()) == [
        ("secondaryexecid", "EXEC-NAMED"),
        ("mdentryrefid", "MD-NAMED"),
    ]


def test_log_altids_fill_null_promoted_identifiers_from_residual_fields() -> None:
    logs = [
        FixMsg(entries=[(37, "ORDER-RESIDUAL")]),
        FixMsg(orderid="ORDER-PROMOTED", entries=[(37, "ORDER-IGNORED")]),
    ]
    table = pyarrow.Table.from_pylist(
        [log.into_row() for log in logs],
        schema=FixMsg.into_field().into_arrow_schema(),
    )
    columns = {name: table.column(name) for name in table.schema.names}

    altids = FixMsg.altids_arrow(columns, 2).to_pylist(maps_as_pydicts="strict")

    assert [row["orderid"] for row in altids] == [
        "ORDER-RESIDUAL",
        "ORDER-PROMOTED",
    ]


def test_market_arrow_batches_match_scalar_orders_and_executions() -> None:
    def message(offset: int, message_type: str, **given) -> FixMsg:
        return FixMsg(
            unix=BASE + offset,
            unixsource="TransactTime",
            beginstring="FIX.4.4",
            msgtype=message_type,
            symbol="BTC-USD",
            mic=MIC.from_str("XCME"),
            **given,
        )

    logs = [
        message(
            1,
            "D",
            clordid="CL-1",
            side="1",
            ordtype="2",
            orderqty=5.0,
            price=100.0,
            entries=[(9999, "order-meta")],
            reason="order reason",
        ),
        message(
            2,
            "8",
            orderid="ORD-1",
            clordid="CL-1",
            execid="EXEC-1",
            side="1",
            ordtype="2",
            ordstatus="1",
            exectype="F",
            orderqty=5.0,
            price=100.0,
            lastpx=100.5,
            lastqty=2.0,
            cumqty=2.0,
            leavesqty=3.0,
            avgpx=100.5,
            entries=[(1003, "TRADE-1"), (9998, "report-meta")],
        ),
        message(
            3,
            "AE",
            execid="EXEC-2",
            side="2",
            lastpx=101.0,
            lastqty=1.0,
            entries=[(1003, "TRADE-2")],
        ),
        message(4, "0"),
        message(
            5,
            "W",
            entries=[
                (268, "4"),
                (269, "0"),
                (278, "BID-1"),
                (270, "100"),
                (271, "5"),
                (269, "1"),
                (278, "ASK-1"),
                (270, "101"),
                (271, "4"),
                (269, "2"),
                (278, "TRADE-ENTRY"),
                (270, "100.5"),
                (271, "2"),
                (269, "4"),
                (270, "99"),
            ],
        ),
        message(
            6,
            "S",
            quoteid="QUOTE-1",
            bidpx=99.0,
            bidsize=3.0,
            offerpx=102.0,
            offersize=4.0,
        ),
        message(
            7,
            "i",
            entries=[
                (296, "1"),
                (302, "SET-1"),
                (295, "2"),
                (299, "QUOTE-2"),
                (132, "99"),
                (134, "3"),
                (133, "101"),
                (135, "4"),
                (299, "QUOTE-3"),
                (132, "98"),
                (134, "2"),
                (133, "102"),
                (135, "5"),
            ],
        ),
    ]
    schema = FixMsg.into_field().into_arrow_schema()
    source = pyarrow.Table.from_pylist([log.into_row() for log in logs], schema=schema)
    source_batches = source.to_batches(max_chunksize=2)
    stored = list(FixMsg.from_arrow_reader(source_batches))
    expected = {Order: [], Execution: []}
    for log in stored:
        for event in log.into_market_events():
            expected[type(event)].append(event)

    found = {Order: [], Execution: []}
    reader = pyarrow.RecordBatchReader.from_batches(schema, source_batches)
    for event_type, batch in FixMsg.into_market_arrow_batches(reader, batch_row_size=2):
        assert batch.schema.equals(event_type.into_field().into_arrow_schema(), check_metadata=True)
        found[event_type].append(batch)

    assert [batch.num_rows for batch in found[Order]] == [2, 2, 2, 2, 2]
    assert [batch.num_rows for batch in found[Execution]] == [2, 1]
    atomic = list(
        FixMsg.into_market_arrow_batches(
            pyarrow.RecordBatchReader.from_batches(schema, source_batches),
            batch_row_size=None,
        )
    )
    assert [(event_type, batch.num_rows) for event_type, batch in atomic] == [
        (Order, 10),
        (Execution, 3),
    ]
    for event_type in (Order, Execution):
        expected_table = pyarrow.Table.from_batches(
            list(event_type.into_arrow_reader(expected[event_type], batch_row_size=2)),
            schema=event_type.into_field().into_arrow_schema(),
        )
        found_table = pyarrow.Table.from_batches(
            found[event_type], schema=event_type.into_field().into_arrow_schema()
        )
        assert found_table.equals(expected_table)
    report_order = expected[Order][1]
    report_execution = expected[Execution][0]
    assert report_execution.parenthash == [report_order.hash]
    assert report_execution.linkedhashes == [report_order.xhash]
    assert report_execution.altids["tradeid"] == "TRADE-1"
    assert report_execution.metadata["9998"] == "report-meta"


def test_an_empty_market_arrow_batch_emits_nothing() -> None:
    schema = FixMsg.into_field().into_arrow_schema()
    empty = pyarrow.RecordBatch.from_arrays(
        [pyarrow.array([], field.type) for field in schema], schema=schema
    )

    assert list(FixMsg.into_market_arrow_batches(empty)) == []


def test_flat_fix_arrow_translation_matches_the_scalar_reference() -> None:
    def message(offset: int, message_type: str, **given) -> FixMsg:
        return FixMsg(
            unix=BASE + offset,
            unixsource="TransactTime",
            beginstring="FIX.4.4",
            msgtype=message_type,
            symbol="ETH-USD",
            mic=MIC.from_str("XPAR"),
            **given,
        )

    logs = [
        message(
            1,
            "D",
            clordid="C-1",
            side="1",
            ordtype="2",
            orderqty=10.0,
            price=100.0,
        ),
        message(2, "F", origclordid="C-1", clordid="C-2", side="1", orderqty=10.0),
        message(
            3,
            "G",
            origclordid="C-1",
            clordid="C-3",
            side="1",
            ordtype="2",
            orderqty=12.0,
            cumqty=2.0,
            leavesqty=10.0,
            price=99.5,
        ),
        message(
            4,
            "8",
            orderid="O-1",
            clordid="C-3",
            execid="E-1",
            side="1",
            ordstatus="1",
            exectype="F",
            orderqty=12.0,
            price=99.5,
            lastpx=99.25,
            lastqty=2.0,
            cumqty=2.0,
            leavesqty=10.0,
            avgpx=99.25,
            entries=[(1057, "Y"), (9998, "audit")],
        ),
        message(
            5,
            "AE",
            execid="E-2",
            exectype="F",
            side="2",
            lastpx=99.75,
            lastqty=3.0,
            entries=[(1003, "T-2")],
        ),
        message(
            6,
            "8",
            orderid="O-2",
            execid="E-3",
            side="2",
            ordstatus="2",
            exectype="F",
            orderqty=2.0,
            price=101.0,
            lastpx=101.25,
            lastqty=2.0,
            cumqty=2.0,
            leavesqty=0.0,
            avgpx=101.25,
        ),
        message(
            7,
            "AE",
            execid="E-4",
            exectype="G",
            side="2",
            lastpx=99.5,
            lastqty=2.0,
            cumqty=5.0,
            leavesqty=0.0,
            avgpx=99.6,
            entries=[(19, "E-2")],
        ),
    ]
    schema = FixMsg.into_field().into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pylist([message.into_row() for message in logs], schema)
    registry = FixRegistry(cache_dir=DATA, offline=True)
    expected = {Order: [], Execution: []}
    for message in FixMsg.from_arrow_reader([batch]):
        for event in message.into_market_events(registry=registry):
            expected[type(event)].append(event)

    assert into_flat_market_batches(batch, {"registry": registry}) is not None
    found = {Order: [], Execution: []}
    for event_type, translated in FixMsg.into_market_arrow_batches(
        batch, batch_row_size=2, registry=registry
    ):
        found[event_type].append(translated)

    for event_type in (Order, Execution):
        expected_table = pyarrow.Table.from_batches(
            list(event_type.into_arrow_reader(expected[event_type]))
        )
        found_table = pyarrow.Table.from_batches(found[event_type])
        different = [
            name
            for name in found_table.schema.names
            if not found_table[name].equals(expected_table[name])
        ]
        assert not different, {
            name: (found_table[name].to_pylist(), expected_table[name].to_pylist())
            for name in different
        }


def test_mixed_market_batch_keeps_supported_rows_fast_and_ordered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rekep.market.fix_arrow as fix_arrow

    def message(offset: int, message_type: str, **given) -> FixMsg:
        return FixMsg(
            unix=BASE + offset,
            beginstring="FIX.4.4",
            msgtype=message_type,
            symbol="ETH-USD",
            mic=MIC.from_str("XPAR"),
            **given,
        )

    logs = [
        message(1, "D", clordid="C-1", side="1", ordtype="2", orderqty=10.0),
        message(2, "W", entries=[(268, "0")]),
        message(3, "AE", execid="E-1", exectype="F", side="2", lastpx=99.0, lastqty=1.0),
        message(4, "S", quoteid="Q-1", bidpx=98.0, offerpx=100.0),
        message(
            5,
            "8",
            orderid="O-1",
            clordid="C-1",
            execid="E-2",
            side="1",
            ordstatus="1",
            exectype="F",
            orderqty=10.0,
            lastpx=99.5,
            lastqty=2.0,
            cumqty=2.0,
            leavesqty=8.0,
            avgpx=99.5,
        ),
        message(6, "0"),
        message(7, "F", origclordid="C-1", clordid="C-2", side="1"),
    ]
    schema = FixMsg.into_field().into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pylist([message.into_row() for message in logs], schema)
    registry = FixRegistry(cache_dir=DATA, offline=True)
    expected = {Order: [], Execution: []}
    for message in FixMsg.from_arrow_reader([batch]):
        for event in message.into_market_events(registry=registry):
            if type(event) in expected:
                expected[type(event)].append(event)

    original = fix_arrow.flat_market_parts
    activated: list[bool] = []

    def observed(*args, **kwargs):
        translated = original(*args, **kwargs)
        activated.append(translated is not None)
        return translated

    monkeypatch.setattr(fix_arrow, "flat_market_parts", observed)
    found = {Order: [], Execution: []}
    for event_type, translated in FixMsg.into_market_arrow_batches(
        batch, batch_row_size=2, registry=registry
    ):
        found[event_type].append(translated)

    assert activated == [False, True]
    for event_type in (Order, Execution):
        expected_table = pyarrow.Table.from_batches(
            list(event_type.into_arrow_reader(expected[event_type]))
        )
        found_table = pyarrow.Table.from_batches(found[event_type])
        assert found_table.equals(expected_table)


def test_flat_fix_arrow_uses_custom_message_names_and_states(tmp_path: Path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix", offline=True)
    builtin = FixRegistry.from_builtin()
    msg_type = builtin.field("MsgType")
    ord_status = builtin.field("OrdStatus")
    exec_type = builtin.field("ExecType")
    assert msg_type is not None and ord_status is not None and exec_type is not None
    configured = {
        "MsgType": record_copy(msg_type),
        "OrdStatus": record_copy(ord_status),
        "ExecType": record_copy(exec_type),
    }
    configured["MsgType"].fix.enumerated = [
        FixFieldValue(value="Q", meaning="NewOrderSingle", aliases=("NEW_ORDER_SINGLE",)),
        FixFieldValue(value="R", meaning="ExecutionReport", aliases=("EXECUTION_REPORT",)),
    ]
    configured["MsgType"].fix.event_types = {"Q": EventType.ORDER, "R": EventType.EXECUTION}
    configured["MsgType"].fix.states = {"Q": State.PENDING_NEW}
    configured["OrdStatus"].fix.states = {**ord_status.fix.states, "Z": State.PARTIALLY_FILLED}
    configured["ExecType"].fix.states = {**exec_type.fix.states, "T": State.FILLED}
    fields = (
        "MsgType",
        "Symbol",
        "ClOrdID",
        "Side",
        "OrdType",
        "OrderQty",
        "Price",
        "OrderID",
        "ExecID",
        "OrdStatus",
        "ExecType",
        "LastPx",
        "LastQty",
        "CumQty",
        "LeavesQty",
        "AvgPx",
    )
    wire_tags: dict[str, int] = {}
    for index, name in enumerate(fields):
        entry = configured.get(name) or builtin.field(name)
        assert entry is not None
        if name != "MsgType":
            entry = record_copy(entry)
            entry.fix.tag = 9000 + index
        assert entry.fix.tag is not None
        wire_tags[name] = entry.fix.tag
        registry.add_field(entry)
    logs = [
        FixMsg(
            unix=BASE + 1,
            protocolversion="4.4",
            msgtype="Q",
            entries=[
                (wire_tags["Symbol"], "AAPL"),
                (wire_tags["ClOrdID"], "CUSTOM-1"),
                (wire_tags["Side"], "1"),
                (wire_tags["OrdType"], "2"),
                (wire_tags["OrderQty"], "5"),
                (wire_tags["Price"], "100"),
            ],
        ),
        FixMsg(
            unix=BASE + 2,
            protocolversion="4.4",
            msgtype="R",
            entries=[
                (wire_tags["Symbol"], "AAPL"),
                (wire_tags["OrderID"], "ORDER-1"),
                (wire_tags["ClOrdID"], "CUSTOM-1"),
                (wire_tags["ExecID"], "EXEC-1"),
                (wire_tags["Side"], "1"),
                (wire_tags["OrdType"], "2"),
                (wire_tags["OrdStatus"], "Z"),
                (wire_tags["ExecType"], "T"),
                (wire_tags["OrderQty"], "5"),
                (wire_tags["Price"], "100"),
                (wire_tags["LastPx"], "100.25"),
                (wire_tags["LastQty"], "2"),
                (wire_tags["CumQty"], "2"),
                (wire_tags["LeavesQty"], "3"),
                (wire_tags["AvgPx"], "100.25"),
            ],
        ),
    ]
    schema = FixMsg.into_field().into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pylist([message.into_row() for message in logs], schema)
    expected = {Order: [], Execution: []}
    for message in FixMsg.from_arrow_reader([batch]):
        for event in message.into_market_events(registry=registry):
            expected[type(event)].append(event)

    assert into_flat_market_batches(batch, {"registry": registry}) is not None
    found = {Order: [], Execution: []}
    for event_type, translated in FixMsg.into_market_arrow_batches(batch, registry=registry):
        found[event_type].append(translated)

    for event_type in (Order, Execution):
        expected_table = pyarrow.Table.from_batches(
            list(event_type.into_arrow_reader(expected[event_type]))
        )
        found_table = pyarrow.Table.from_batches(found[event_type])
        different = [
            name
            for name in found_table.schema.names
            if not found_table[name].equals(expected_table[name])
        ]
        assert not different, {
            name: (found_table[name].to_pylist(), expected_table[name].to_pylist())
            for name in different
        }
    assert [order.state for order in expected[Order]] == [
        State.PENDING_NEW,
        State.PARTIALLY_FILLED,
    ]
    assert expected[Execution][0].state is State.FILLED
    assert expected[Order][0].altids["clordid"] == "CUSTOM-1"


@pytest.mark.parametrize(
    ("exec_type", "execution_state"),
    [("G", State.REPLACED), ("H", State.CANCELLED)],
)
def test_flat_fix_arrow_keeps_trade_revision_order_state_unknown(
    exec_type: str, execution_state: State
) -> None:
    message = FixMsg(
        unix=BASE,
        protocolversion="4.4",
        msgtype="8",
        symbol="AAPL",
        orderid="ORDER-1",
        clordid="CLIENT-1",
        execid="EXEC-1",
        exectype=exec_type,
        side="1",
        orderqty=5.0,
        lastpx=100.25,
        lastqty=2.0,
        cumqty=2.0,
        leavesqty=3.0,
        avgpx=100.25,
    )
    schema = FixMsg.into_field().into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pylist([message.into_row()], schema)

    translated = into_flat_market_batches(batch, {"registry": FixRegistry.from_builtin()})
    assert translated is not None
    orders, executions = translated
    assert orders is not None and executions is not None
    assert orders.column("state")[0].as_py() == int(State.UNKNOWN)
    assert executions.column("state")[0].as_py() == int(execution_state)


def test_parsed_fixmsg_keeps_raw_unused_values_in_scalar_and_arrow_metadata() -> None:
    line = (
        "8=FIX.4.4|35=8|37=ORDER-1|11=CLIENT-1|17=EXEC-1|55=AAPL|54=1|"
        "39=1|150=F|38=2|31=10.5|32=2|14=2|151=0|6=0010.5000|"
        "60=20260825-09:30:03.5|10=000|"
    )
    raw = next(iter(Message.into_arrow_reader([Message(message=line)])))
    registry = FixRegistry.from_builtin()
    parsed = FixMsg.from_message_batch(raw, FixCodec(registry=registry))

    message = next(FixMsg.from_arrow_reader([parsed]))
    scalar = list(message.into_market_events(registry=registry))
    assert scalar and all(event.metadata["6"] == "0010.5000" for event in scalar)

    translated = into_flat_market_batches(parsed, {"registry": registry})
    assert translated is not None
    for batch in translated:
        if batch is not None:
            for metadata in batch["metadata"].to_pylist():
                assert metadata.count(("6", "0010.5000")) == 1


def test_reference_input_is_read_only_and_books_remain_the_only_output() -> None:
    events = [order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")]
    instruments = list(Instrument.from_events(events))
    iterating = BookIterator.from_events(with_instruments(events, instruments))
    assert [one.unix for one in iterating.books] == [BASE]
    assert instruments[0].version == 0, "folding did not version or replace its input"


def test_checkpoint_rows_are_globally_boundary_ordered_across_instruments() -> None:
    events = [
        order(BASE + 1, BTC, Side.BID, 100.0, 1.0, "B1"),
        order(BASE + 2, ETH, Side.BID, 10.0, 1.0, "E1"),
        order(BASE + 3 * HOUR + 1, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    streams = (
        (list(BookIterator.from_events(events)), "instrumentxhash"),
        (list(Instrument.from_events(events)), "xhash"),
    )
    for snapshots, key in streams:
        snapshots = [row for row in snapshots if row.snapunix is not None]
        assert [row.unix for row in snapshots] == sorted(row.unix for row in snapshots)
        for boundary in (BASE + HOUR, BASE + 2 * HOUR, BASE + 3 * HOUR):
            assert {getattr(row, key) for row in snapshots if row.unix == boundary} == {
                BTC.xhash,
                ETH.xhash,
            }


def test_book_iteration_never_queues_a_parallel_reference_stream() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC_RICH, Side.BID, 99.0, 4.0, "B2"),
        order(BASE + 20, ETH, Side.ASK, 10.5, 3.0, "E1"),
    ]
    expected_books = [one.into_dict() for one in BookIterator.from_events(events).books]
    pulled: list[int] = []

    iterating: BookIterator

    def source():
        for event in events:
            pulled.append(event.unix)
            yield event

    iterating = BookIterator.from_events(source())
    assert [one.into_dict() for one in iterating] == expected_books
    assert pulled == [event.unix for event in events]


def test_an_out_of_order_rotated_segment_keeps_a_distinct_instrument() -> None:
    instruments = list(
        Instrument.from_observations(
            [(BASE + 10, BTC), (BASE, ETH)],
            snapshot_every=0,
        )
    )

    assert {known.xhash for known in instruments} == {BTC.xhash, ETH.xhash}
    assert [known.unix for known in instruments] == [BASE + 10, BASE + 10]


def test_conflicting_security_ids_collapse_under_the_same_exact_symbol() -> None:
    first = Instrument(
        symbol="ABC",
        securityexchange="XPAR",
        securityid="111111111",
        securityidsource="1",
    )
    second = Instrument(
        symbol="ABC",
        securityexchange="XPAR",
        securityid="222222222",
        securityidsource="1",
    )

    instruments = list(
        Instrument.from_observations(
            [(BASE, first), (BASE + 1, second)],
            snapshot_every=0,
        )
    )

    (known,) = instruments
    assert known.securityid == "111111111", "the first nonempty fact remains authoritative"
    assert known.xhash == first.xhash == second.xhash
    assert known.version == 0


def test_a_weak_symbol_can_still_be_enriched_by_its_first_security_id() -> None:
    weak = Instrument(symbol="ABC", securityexchange="XPAR")
    strong = Instrument(
        symbol="ABC",
        securityexchange="XPAR",
        securityid="111111111",
        securityidsource="1",
    )

    bare, enriched = Instrument.from_observations(
        [(BASE, weak), (BASE + 1, strong)],
        snapshot_every=0,
    )

    assert enriched.securityid == "111111111"
    assert enriched.xhash == bare.xhash
    assert enriched.version == bare.version + 1


def test_instrument_altids_hold_reference_schemes_and_lifecycle_fields() -> None:
    known = Instrument(
        symbol="ABC",
        altids={ISIN_SCHEME: "FR0000120271", "clordid": "CLIENT-1"},
    )
    observed = Instrument(
        symbol="ABC",
        altids={"CUSIP": "012345678", "clordid": "CLIENT-2"},
    )

    enriched = known.enriched_with(observed)

    assert known.into_isin() == "FR0000120271"
    assert enriched is not None
    assert enriched.altids == {
        ISIN_SCHEME: "FR0000120271",
        "clordid": "CLIENT-1",
        "CUSIP": "012345678",
    }
    assert [field.name for field in Instrument.into_field().fields].count("altids") == 1


def test_a_security_id_only_instrument_is_unidentified_and_skipped() -> None:
    unidentified = Instrument(securityid="US1234567890", securityidsource="4")
    assert unidentified.xhash == NIL and unidentified.identities() == ()
    assert list(Instrument.from_observations([(BASE, unidentified)], snapshot_every=0)) == []


def test_different_symbols_do_not_alias_through_a_shared_security_id() -> None:
    first = Instrument(
        symbol="BTC-USD",
        securityid="US1234567890",
        securityidsource="4",
    )
    second = Instrument(
        symbol="XBT-USD",
        securityid="US1234567890",
        securityidsource="4",
    )

    instruments = list(
        Instrument.from_observations(
            [(BASE, first), (BASE + 1, second)],
            snapshot_every=0,
        )
    )

    assert [known.symbol for known in instruments] == ["BTC-USD", "XBT-USD"]
    assert len({known.xhash for known in instruments}) == 2
    assert [known.version for known in instruments] == [0, 0]


def test_iterating_the_iterator_is_iterating_its_books() -> None:
    events = [order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")]
    assert [type(one) for one in BookIterator.from_events(events)] == [Book]


def test_the_source_is_read_once_and_not_started_over() -> None:
    """A generator source would be silently half-consumed by a second pass, and a
    list source would be folded forever."""
    events = iter([order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")])
    iterating = BookIterator.from_events(events)
    assert len(list(iterating.books)) == 1
    assert list(iterating.books) == [], "drained, and it says so"


# -- the state is per instrument ---------------------------------------------


def test_one_iterator_folds_every_instrument_on_its_own() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, ETH, Side.BID, 10.0, 3.0, "E1"),
        order(BASE + 20, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    iterating = BookIterator.from_events(events, snapshot_every=0)
    found = list(iterating.books)
    assert {one.code for one in found} == {"BTC-USD", "ETH-USD"}
    assert len(iterating.folding) == 2, "one mutable state per instrument, and no more"


def test_an_instrument_s_book_never_sees_another_s_liquidity() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, ETH, Side.BID, 999.0, 99.0, "E1"),
        order(BASE + 20, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    books = {one.code: one for one in BookIterator.from_events(events, snapshot_every=0)}
    assert books["BTC-USD"].bidpx == 100.0 and books["BTC-USD"].bidqty == 5.0
    assert books["ETH-USD"].bidpx == 999.0


def test_a_stream_out_of_order_is_refused() -> None:
    """A fold asks the book to un-happen something, and there is no honest answer."""
    events = [
        order(BASE + 100, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE, ETH, Side.BID, 10.0, 3.0, "E1"),
    ]
    with pytest.raises(ValueError, match="time order"):
        list(BookIterator.from_events(events))


def test_the_order_is_checked_across_instruments_and_not_within_one() -> None:
    """The clock is the stream's, which is what makes an hourly boundary the same
    instant for every instrument in it."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE, ETH, Side.BID, 10.0, 3.0, "E1"),
        order(BASE + 1, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    assert len(list(BookIterator.from_events(events, snapshot_every=0))) == 3


# -- reference ownership -----------------------------------------------------


def test_an_instrument_is_published_when_it_is_learnt_and_not_per_message() -> None:
    """A feed repeats the instrument on every message; a row per message would be
    the feed again rather than the reference data in it."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.BID, 99.0, 5.0, "B2"),
        order(BASE + 20, BTC, Side.BID, 98.0, 5.0, "B3"),
    ]
    assert len(list(Instrument.from_events(events))) == 1


def test_a_message_that_knows_more_publishes_another_version() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC_RICH, Side.BID, 99.0, 5.0, "B2"),
    ]
    bare, rich = Instrument.from_events(events)
    assert bare.cficode is None and bare.kind is AssetKind.UNKNOWN
    assert rich.cficode == "FFICSX" and rich.kind is AssetKind.FUTURE
    assert rich.currency is Currency.USD
    assert rich.xhash == bare.xhash, "the same instrument, and an identity that did not move"
    assert rich.version == bare.version + 1 and rich.prevunix == bare.unix
    assert rich.prevhash == bare.hash
    assert rich.hash != bare.hash, "two versions of what is known, and two rows"


def test_learning_never_retracts_what_was_already_known() -> None:
    """Instrument data arrives in whatever order a venue felt like sending it, and a
    later message that omits a field has not withdrawn it."""
    events = [
        order(BASE, BTC_RICH, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.BID, 99.0, 5.0, "B2"),
    ]
    (only,) = Instrument.from_events(events)
    assert only.cficode == "FFICSX"


def test_equal_reference_repeats_skip_enrichment_but_a_late_field_does_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The equality shortcut must still notice the last reference-data member."""
    repeated = dataclasses.replace(BTC)
    richer = dataclasses.replace(BTC, securitydesc="Bitcoin future")
    enriched_with = Instrument.enriched_with
    examined: list[Instrument] = []

    def counted(self: Instrument, other: Instrument) -> Instrument | None:
        examined.append(other)
        return enriched_with(self, other)

    monkeypatch.setattr(Instrument, "enriched_with", counted)
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, repeated, Side.BID, 99.0, 4.0, "B2"),
        order(BASE + 20, richer, Side.BID, 98.0, 3.0, "B3"),
    ]

    found = list(Instrument.from_events(events))

    assert examined == [richer], "initial and equal states need no enrichment walk"
    assert [one.securitydesc for one in found] == [None, "Bitcoin future"]


# -- the hourly grid ----------------------------------------------------------


def test_a_gap_of_hours_is_filled_hour_by_hour() -> None:
    """A table whose hourly rows skip the hours nothing happened in is one you have
    to scan backwards to read, which is what hourly rows exist to avoid."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 3 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator.from_events(events))
    assert [(one.unix - BASE) // HOUR for one in found] == [0, 1, 2, 3, 3]
    assert [one.snapunix is not None for one in found] == [False, True, True, True, False]


def test_a_snapshot_says_what_it_is_a_picture_of() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 2 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator.from_events(events))
    taken = [one for one in found if one.snapunix is not None]
    assert [one.snapunix for one in taken] == [BASE + 60, BASE + 60]
    assert [one.unix for one in taken] == [BASE + HOUR, BASE + 2 * HOUR]
    assert all(one.unix - one.snapunix > 0 for one in taken), "staleness, without a join"


def test_instrument_state_is_published_on_every_book_boundary() -> None:
    events = [
        order(BASE + 60, BTC_RICH, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 3 * HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1"),
    ]
    books = list(BookIterator.from_events(events))
    boundaries = {one.unix for one in books if one.snapunix is not None}
    instruments = list(Instrument.from_events(events))

    assert [one.unix for one in instruments] == [
        BASE + 60,
        BASE + HOUR,
        BASE + 2 * HOUR,
        BASE + 3 * HOUR,
    ]
    assert {one.unix for one in instruments if one.snapunix is not None} == boundaries
    assert all(one.cficode == BTC_RICH.cficode for one in instruments)


def test_expiry_delta_does_not_skip_the_instrument_boundary() -> None:
    expiring = order(
        BASE + 60,
        BTC,
        Side.BID,
        100.0,
        5.0,
        "B1",
        expunix=BASE + HOUR - 10,
    )
    clock = order(BASE + 2 * HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1")
    events = [expiring, clock]
    records = list(BookIterator.from_events(events))
    instruments = list(Instrument.from_events(events))
    assert [one.unix for one in instruments] == [BASE + 60, BASE + HOUR, BASE + 2 * HOUR]
    expired = next(one for one in records if one.unix == BASE + HOUR)
    assert expired.biddepth == 0


def test_recovery_continues_full_instrument_snapshots() -> None:
    events = [
        order(BASE + 60, BTC_RICH, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1"),
    ]
    instruments = list(Instrument.from_events(events))
    known = next(one for one in instruments if one.unix == BASE + HOUR)
    later = [order(BASE + 3 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2")]
    resumed = Instrument.from_events(
        later,
        instruments=[Instrument.from_dict(known.into_dict())],
    )

    recovered = list(resumed)
    assert [one.unix for one in recovered] == [BASE + 2 * HOUR, BASE + 3 * HOUR]
    assert all(one.cficode == BTC_RICH.cficode for one in recovered)


def test_instrument_recovery_breaks_equal_versions_by_hash() -> None:
    def seed(unix: int, label: str) -> Instrument:
        known = dataclasses.replace(BTC, unix=unix, securitydesc=label).with_previous(None)
        assert known is not None
        return known

    old = seed(BASE, "old")
    peers = [seed(BASE + 60, "first"), seed(BASE + 60, "second")]
    low, high = sorted(peers, key=lambda row: row.hash)

    (snapshot,) = Instrument.from_observations(
        (),
        instruments=[low, old, high],
        snapshot_until=BASE + HOUR + 1,
    )

    assert snapshot.securitydesc == high.securitydesc


def test_every_instrument_gets_the_hour_and_not_only_the_one_that_traded() -> None:
    """The hour is a property of the clock: a book nothing happened to for three
    hours still stood there for three hours."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 70, ETH, Side.BID, 10.0, 3.0, "E1"),
        order(BASE + 3 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator.from_events(events))
    hours = {}
    for one in found:
        hours.setdefault(one.code, []).append((one.unix - BASE) // HOUR)
    assert hours["BTC-USD"] == [0, 1, 2, 3, 3]
    assert hours["ETH-USD"] == [0, 1, 2, 3]


def test_an_exact_boundary_snapshots_only_the_inactive_instrument() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + HOUR, ETH, Side.BID, 10.0, 1.0, "E1"),
    ]
    records = list(BookIterator.from_events(events))
    instruments = list(Instrument.from_events(events))

    btc_books = [one for one in records if one.code == "BTC-USD"]
    btc_refs = [one for one in instruments if one.code == "BTC-USD"]
    eth_books = [one for one in records if one.code == "ETH-USD"]
    assert [one.unix for one in btc_books] == [BASE + 60, BASE + HOUR]
    assert [one.unix for one in btc_refs] == [BASE + 60, BASE + HOUR]
    assert [one.unix for one in eth_books] == [BASE + HOUR]


def test_an_active_exact_boundary_is_one_final_delta() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + HOUR, BTC, Side.ASK, 101.0, 1.0, "A1"),
        order(BASE + HOUR, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    records = list(BookIterator.from_events(events))
    instruments = list(Instrument.from_events(events))

    boundary_books = [one for one in records if one.unix == BASE + HOUR]
    boundary_refs = [one for one in instruments if one.unix == BASE + HOUR]
    assert len(boundary_books) == 1
    (delta,) = boundary_books
    assert delta.snapunix is None
    assert delta.biddepth == 2 and delta.askdepth == 1
    assert len(boundary_refs) == 1 and boundary_refs[0].snapunix == BASE + 60


def test_an_exact_boundary_keeps_one_book_identity() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100, 5.0, "B1"),
        order(BASE + HOUR, BTC, Side.ASK, 101.0, 1.0, "A1"),
        order(BASE + HOUR, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    books = list(BookIterator.from_events(events).books)

    assert len({(book.unix, book.instrumentxhash) for book in books}) == len(books)
    assert [book.hash for book in books] == [
        book.txhash_of(*book.version_parts()) for book in books
    ]


def test_the_snapshots_are_versions_of_the_book_they_picture() -> None:
    """The book at 14:00 and the book at 15:00 are two rows of one book."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 2 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator.from_events(events))
    assert [one.version for one in found] == [0, 1, 2, 3]
    assert len({one.xhash for one in found}) == 1, "one book"
    assert len({one.hash for one in found}) == len(found), "and four versions of it"
    for before, after in zip(found, found[1:], strict=False):
        assert after.prevunix == before.unix
        assert after.prevhash == before.hash


def test_turning_the_grid_off_leaves_only_what_changed() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 5 * HOUR, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator.from_events(events, snapshot_every=0))
    assert len(found) == 2 and all(one.snapunix is None for one in found)


def test_the_stream_ends_without_guessing_how_long_the_book_stood() -> None:
    """A snapshot fills the gap between two events; past the last one there is no
    gap, only a guess."""
    events = [order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1")]
    found = list(BookIterator.from_events(events))
    assert len(found) == 1 and found[0].snapunix is None


# -- a side that did not move -------------------------------------------------


def test_a_side_that_did_not_move_carries_no_levels_delta() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
        order(BASE + 20, BTC, Side.ASK, 100.4, 2.0, "A2"),
    ]
    first, second, third = BookIterator.from_events(events, snapshot_every=0).books
    assert first.bidlevels and second.bidlevels == [] and third.bidlevels == []
    assert second.asklevels and third.asklevels, "the ask changed on both later rows"


def test_a_side_that_did_not_move_carries_no_delta() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    _, second = BookIterator.from_events(events, snapshot_every=0).books
    assert second.bidlevels == []
    assert len(second.asklevels) == 1


def test_a_side_that_did_not_move_still_reports_its_state() -> None:
    """Carried, not dropped: a book row says what both sides are, always."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    _, second = BookIterator.from_events(events, snapshot_every=0).books
    assert second.bidpx == 100.0 and second.bidqty == 5.0 and second.biddepth == 1
    assert second.bidlevels == [], "an unchanged side has no levels delta"
    assert second.spread == pytest.approx(0.5), "and the prices across the sides follow"


def test_each_delta_reads_both_incremental_side_summaries() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
        order(BASE + 20, BTC, Side.ASK, 100.4, 2.0, "A2"),
        order(BASE + 30, BTC, Side.BID, 99.0, 3.0, "B2"),
    ]

    found = list(BookIterator.from_events(events, snapshot_every=0))

    assert [
        (
            row.bidpx,
            row.bidqty,
            row.biddepth,
            row.askpx,
            row.askqty,
            row.askdepth,
        )
        for row in found
    ] == [
        (100.0, 5.0, 1, None, None, 0),
        (100.0, 5.0, 1, 100.5, 7.0, 1),
        (100.0, 5.0, 1, 100.4, 2.0, 2),
        (100.0, 5.0, 2, 100.4, 2.0, 2),
    ]
    assert found[1].px == pytest.approx(100.25)
    assert found[2].vwap == pytest.approx((100.0 * 2.0 + 100.4 * 5.0) / 7.0)


def test_a_trade_counts_as_the_side_moving() -> None:
    """It takes liquidity out, and a row that said otherwise would be wrong."""
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 10, BTC, Side.ASK, 100.5, 7.0, "A1"),
        initial(
            Execution(
                unix=BASE + 20,
                side=Side.BID,
                px=100.0,
                qty=2.0,
                state=State.FILLED,
                execid="EX-1",
            )
        ),
    ]
    first, _, third = BookIterator.from_events(events, snapshot_every=0).books
    assert first.bidqty == 5.0 and third.bidqty == 3.0
    assert [(level.px, level.qty) for level in third.bidlevels] == [(100.0, 3.0)]
    assert [(one.px, one.qty) for one in third.executions] == [(100.0, 2.0)]


def test_a_trade_amendment_is_not_folded_as_a_fresh_fill() -> None:
    events = [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "B1"),
        initial(
            Execution(
                unix=BASE + 10,
                side=Side.BID,
                px=100.0,
                qty=2.0,
                state=State.FILLED,
                execid="EX-1",
            )
        ),
        initial(
            Execution(
                unix=BASE + 20,
                side=Side.BID,
                px=100.0,
                qty=2.0,
                state=State.CANCELLED,
                execid="EX-2",
                execrefid="EX-1",
            )
        ),
    ]

    found = list(BookIterator.from_events(events, snapshot_every=0))

    assert len(found) == 3 and found[-1].bidqty == 3.0
    assert found[-1].executions[0].state is State.CANCELLED


# -- a picture has no delta ---------------------------------------------------


def test_a_snapshot_shows_the_book_and_not_what_changed_to_produce_it() -> None:
    """Carrying the delta forward made a consumer summing those columns count one
    level insertion once per hourly row -- four times over a three-hour quiet
    patch -- for an insertion that happened once."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 3 * HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    found = list(BookIterator.from_events(events))
    for one in found:
        if one.snapunix is None:
            continue
        assert [level.px for level in one.bidlevels] == [100.0], "the state is still there"
        assert one.deltas == [] and one.executions == []
        assert (
            one.prevbidpx,
            one.prevbidqty,
            one.prevaskpx,
            one.prevaskqty,
            one.prevexecpx,
        ) == (None, None, None, None, None)
        assert [order.orderid for order in one.bidalive] == ["B1"]
        assert one.linkedhashes == [order.xhash for order in one.bidalive]


def test_forgetting_the_delta_does_not_empty_the_row_it_pictures() -> None:
    """A snapshot is a `copy.copy`, so its lists are the subject's own until
    something replaces them: clearing in place would empty the book below it."""
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + HOUR + 60, BTC, Side.BID, 99.0, 1.0, "B2"),
    ]
    first, snapshot, _ = BookIterator.from_events(events).books
    assert len(first.bidlevels) == 1, "the row that was pictured still has its delta"
    assert snapshot.bidlevels is not first.bidlevels
    assert snapshot.bidlevels == first.bidlevels


# -- recovery, validation and expiry ---------------------------------------


def test_only_snapshots_carry_the_full_state_needed_to_resume() -> None:
    events = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + HOUR + 60, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    changed, snapshot, latest = BookIterator.from_events(events).books

    assert changed.bidlevels and latest.bidlevels == []
    assert snapshot.deltas == [] and snapshot.executions == []
    assert [one.orderid for one in snapshot.bidalive] == ["B1"]


def test_a_snapshot_restores_names_levels_and_live_quantities() -> None:
    before = [
        order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + HOUR + 60, BTC, Side.ASK, 100.5, 7.0, "A1"),
    ]
    seed = next(one for one in BookIterator.from_events(before) if one.snapunix is not None)
    after = order(BASE + HOUR + 70, BTC, Side.BID, 99.0, 3.0, "B2")

    iterator = BookIterator.from_events([after], snapshots=[seed], snapshot_every=0)
    (resumed,) = iterator

    assert resumed.biddepth == 2
    assert resumed.bidpx == 100.0 and resumed.askpx is None
    assert [one.orderid for one in resumed.deltas] == ["B2"]
    assert sum(level.qty for level in iterator.folding[BTC.xhash].bid.alive) == 8.0


def test_book_recovery_normalizes_candidates_by_unix_version_and_hash() -> None:
    def seeds(named: str) -> list[Book]:
        books = BookIterator.from_events(
            [
                order(BASE + 60, BTC, Side.BID, 100.0, 5.0, named),
                order(BASE + 2 * HOUR + 60, BTC, Side.ASK, 101.0, 1.0, f"A-{named}"),
            ]
        )
        return [book for book in books if book.snapunix is not None]

    first, second = seeds("B1"), seeds("B2")
    old = first[0]
    low, high = sorted((first[-1], second[-1]), key=lambda row: row.hash)
    assert (low.unix, low.version) == (high.unix, high.version)

    restored = BookIterator(snapshots=[high, old, low], snapshot_every=0)

    assert restored.snapshots == (high,)
    assert restored.folding[BTC.xhash].previous is high


def test_recovery_rebuilds_the_same_order_framed_hash_as_an_uninterrupted_fold() -> None:
    placed = order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1")
    clock = order(BASE + HOUR + 60, BTC, Side.ASK, 100.5, 7.0, "A1")
    after = order(BASE + HOUR + 70, BTC, Side.BID, 99.0, 3.0, "B2")
    uninterrupted = list(BookIterator.from_events([placed, clock, after], snapshot_every=0))[-1]
    seed = next(
        one for one in BookIterator.from_events([placed, clock]) if one.snapunix is not None
    )
    persisted = Book.from_dict(seed.into_dict())

    recovered = list(
        BookIterator.from_events(
            [clock, after],
            snapshots=[persisted],
            snapshot_every=0,
        )
    )[-1]
    expected = (
        after.unix,
        hash_bytes_of(BTC.xhash),
        2,
        hash_bytes_of(placed.hash),
        hash_bytes_of(after.hash),
        1,
        hash_bytes_of(clock.hash),
    )

    assert uninterrupted.version_parts() == recovered.version_parts() == expected
    assert uninterrupted.hash == recovered.hash == uninterrupted.txhash_of(*expected)


def test_the_live_state_a_book_is_identified_by_follows_every_revision() -> None:
    """A side caches what each level settled into; a revision has to clear it.

    The cache is what keeps a book with a hundred live levels from re-sorting
    and re-hashing all of them on every event -- so the case worth pinning is
    the one that changes an order without changing its level: same price, same
    resting quantity, a new version and therefore a new content hash.
    """

    def live(where: _Side) -> tuple[int, ...]:
        """What the side says it is identified by, read the long way round."""
        return tuple(one.hash for one in where.sorted_orders)

    side = _Side(side=Side.BID)
    placed = order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")
    assert side.apply(placed)
    before = side.order_hashes()
    assert before == live(side) == (placed.hash,)

    revised = order(BASE + 10, BTC, Side.BID, 100.0, 5.0, "B1", state=State.PARTIALLY_FILLED)
    assert revised.px == placed.px and _resting(revised) == _resting(placed)
    side.apply(revised)
    after = side.order_hashes()
    assert after == live(side) and after != before, (
        "the level forgot what it had settled into -- even though nothing about "
        "the level itself moved, which is exactly the case a stale cache would miss"
    )

    beside = order(BASE + 20, BTC, Side.BID, 100.0, 9.0, "B2")
    assert side.apply(beside)
    assert side.order_hashes() == live(side) == (beside.hash, *after), "biggest first"


def test_order_lookup_falls_back_to_a_live_client_id_without_an_order_id() -> None:
    placed = initial(
        Order(
            unix=BASE,
            side=Side.BID,
            px=100.0,
            qty=5.0,
            clordid="client-1",
            state=State.NEW,
        ),
        BTC,
    )
    side = _Side(side=Side.BID)
    assert side.apply(placed)
    # There is deliberately no linear fallback when the required index is corrupt.
    side.named.clear()

    found = side.standing(Order(clordid="client-1"))

    assert found is None


def test_a_restored_order_continues_the_persisted_version_chain() -> None:
    placed = order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1")
    clock = order(BASE + HOUR + 60, BTC, Side.ASK, 100.5, 7.0, "A1")
    seed = next(
        one for one in BookIterator.from_events([placed, clock]) if one.snapunix is not None
    )
    amended = order(BASE + HOUR + 70, BTC, Side.BID, 100.0, 4.0, "B1")

    (resumed,) = BookIterator.from_events([amended], snapshots=[seed], snapshot_every=0)

    (audited,) = resumed.deltas
    seeded = next(one for one in seed.bidalive if one.orderid == "B1")
    assert audited.prevunix == seeded.unix == placed.unix
    assert audited.prevhash == seeded.hash
    assert audited.version == seeded.version + 1


def test_recovery_rebuilds_every_typed_alias_of_a_mutating_order() -> None:
    placed = initial(
        Order(
            unix=BASE + 10,
            side=Side.BID,
            px=100.0,
            qty=5.0,
            clordid="CL-1",
            altids={"clordid": "CL-1"},
            state=State.NEW,
        ),
        BTC,
    )
    amended = initial(
        Order(
            unix=BASE + 20,
            side=Side.BID,
            px=100.0,
            qty=4.0,
            orderid="ORD-1",
            clordid="CL-2",
            origclordid="CL-1",
            altids={
                "orderid": "ORD-1",
                "origclordid": "CL-1",
                "clordid": "CL-2",
            },
            state=State.OPEN,
        ),
        BTC,
    )
    clock = order(BASE + HOUR + 30, BTC, Side.ASK, 101.0, 1.0, "A1")
    seed = next(
        book
        for book in BookIterator.from_events([placed, amended, clock])
        if book.snapunix is not None
    )

    restored = BookIterator(snapshots=[seed])
    side = restored.folding[BTC.xhash].bid
    lifecycle = next(iter(side.orders))

    for name, code in (
        ("clordid", "CL-1"),
        ("origclordid", "CL-1"),
        ("clordid", "CL-2"),
        ("orderid", "ORD-1"),
    ):
        assert side.standing(Order(altids={name: code})).xhash == lifecycle

    side.apply(
        initial(
            Order(
                unix=BASE + HOUR + 40,
                side=Side.BID,
                orderid="ORD-1",
                altids={"orderid": "ORD-1"},
                state=State.CANCELLED,
            ),
            BTC,
        )
    )
    assert side.named == {} and side.aliases == {}


def test_recovery_refuses_a_live_level_it_cannot_reconstruct() -> None:
    placed = order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1")
    clock = order(BASE + HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1")
    seed = next(
        one for one in BookIterator.from_events([placed, clock]) if one.snapunix is not None
    )
    broken = dataclasses.replace(seed, bidalive=[])

    with pytest.raises(ValueError, match="no live Order"):
        BookIterator(snapshots=[broken])


def test_recovery_refuses_a_live_order_absent_from_the_levels() -> None:
    placed = order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1")
    clock = order(BASE + HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1")
    seed = next(
        one for one in BookIterator.from_events([placed, clock]) if one.snapunix is not None
    )
    broken = dataclasses.replace(seed, biddepth=0, bidlevels=[])

    with pytest.raises(ValueError, match="absent from the levels"):
        BookIterator(snapshots=[broken])


def test_recovery_refuses_a_level_quantity_that_disagrees_with_its_orders() -> None:
    placed = order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1")
    clock = order(BASE + HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1")
    seed = next(
        one for one in BookIterator.from_events([placed, clock]) if one.snapunix is not None
    )
    broken = dataclasses.replace(
        seed,
        bidlevels=[dataclasses.replace(seed.bidlevels[0], qty=4.0)],
    )

    with pytest.raises(ValueError, match="Orders total"):
        BookIterator(snapshots=[broken])


def test_recovery_refuses_a_delta_deletion_as_a_full_level() -> None:
    placed = order(BASE + 60, BTC, Side.BID, 100.0, 5.0, "B1")
    clock = order(BASE + HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1")
    seed = next(
        one for one in BookIterator.from_events([placed, clock]) if one.snapunix is not None
    )
    broken = dataclasses.replace(
        seed,
        bidlevels=[dataclasses.replace(seed.bidlevels[0], qty=0.0)],
    )

    with pytest.raises(ValueError, match="must have positive qty"):
        BookIterator(snapshots=[broken])


def test_recovery_rebuilds_the_explicit_expiry_index() -> None:
    placed = order(
        BASE + 60,
        BTC,
        Side.BID,
        100.0,
        5.0,
        "B1",
        expunix=BASE + HOUR + 10,
    )
    clock = order(BASE + HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1")
    snapshot = next(
        one for one in BookIterator.from_events([placed, clock]).books if one.snapunix is not None
    )
    assert snapshot.bidlevels and snapshot.bidalive and placed.expunix is not None

    restored = _Side.from_snapshot(
        Side.BID,
        snapshot.bidlevels,
        snapshot.bidalive,
    )

    assert restored._deadlines[0][0] == placed.expunix
    (expired,) = restored.expire(placed.expunix)
    assert expired.xhash == placed.xhash and restored.orders == {}
    assert restored._deadlines == [] and restored._deadline_tokens == {}


def test_recovery_applies_the_side_bound_as_an_auditable_delta() -> None:
    before = [
        order(BASE + 60, BTC, Side.BID, 100.0, 1.0, "B1"),
        order(BASE + 61, BTC, Side.BID, 99.0, 1.0, "B2"),
        order(BASE + 62, BTC, Side.BID, 98.0, 1.0, "B3"),
        order(BASE + HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1"),
    ]
    seed = next(one for one in BookIterator.from_events(before) if one.snapunix is not None)

    iterator = BookIterator(snapshots=[seed], snapshot_every=0, max_side_alive=2)
    (bounded,) = iterator.books

    expired = [one for one in bounded.deltas if one.state is State.INTERNAL_EXPIRED]
    assert [one.orderid for one in expired] == ["B3"]
    assert bounded.biddepth == 2
    assert sum(level.qty for level in iterator.folding[BTC.xhash].bid.alive) == 2.0


def test_different_symbol_references_do_not_alias_one_book() -> None:
    first = Instrument(
        symbol="BTC-USD",
        securityexchange="XCME",
        securityid="US1234567890",
        securityidsource="4",
    )
    second = Instrument(
        symbol="XBT-USD",
        securityexchange="XCME",
        securityid="US1234567890",
        securityidsource="4",
    )
    events = [
        order(BASE, first, Side.BID, 100.0, 5.0, "B1"),
        order(BASE + 1, second, Side.BID, 99.0, 2.0, "B2"),
    ]
    instruments = list(Instrument.from_events(events))
    folding = BookIterator.from_events(
        with_instruments(events, instruments),
        snapshot_every=0,
    )

    books = list(folding.books)
    assert len(folding.folding) == 2
    assert {book.instrumentxhash for book in books} == {first.xhash, second.xhash}


def test_a_same_symbol_reference_keeps_the_book_and_nested_order_on_one_identity() -> None:
    canonical = Instrument(symbol="BTC-USD", securityexchange="XCME")
    richer = Instrument(
        symbol="BTC-USD",
        securityexchange="XCME",
        securityid="US1234567890",
        securityidsource="4",
    )
    known = dataclasses.replace(canonical, unix=BASE - HOUR).with_previous(None)
    assert known is not None and richer.xhash == known.xhash
    folding = BookIterator.from_events(
        with_instruments(
            [order(BASE, richer, Side.BID, 100.0, 1.0, "B1")],
            [Instrument.from_dict(known.into_dict())],
        ),
        snapshot_every=0,
    )

    (book,) = folding
    instrument = known
    (nested,) = book.deltas
    assert instrument.xhash == book.instrumentxhash == nested.instrumentxhash
    assert book.instrumentxhash == canonical.xhash
    assert nested.xhash == Order.hash_of(
        hash_bytes_of(canonical.xhash), nested.mic, "B1", nested.side
    )


def test_a_same_symbol_reference_preserves_execution_links_and_parent_versions() -> None:
    canonical = Instrument(symbol="BTC-USD", securityexchange="XCME")
    richer = Instrument(
        symbol="BTC-USD",
        securityexchange="XCME",
        securityid="US1234567890",
        securityidsource="4",
    )
    known = dataclasses.replace(canonical, unix=BASE - HOUR).with_previous(None)
    assert known is not None
    placed = order(BASE, richer, Side.BID, 100.0, 1.0, "B1")
    fill = (
        Execution(
            unix=BASE + 1,
            state=State.FILLED,
            side=Side.BID,
            px=100.0,
            qty=1.0,
            execid="X1",
            linkedhashes=[placed.xhash],
            parenthash=[placed.hash],
        )
        .attach_instrument(richer)
        .with_previous(placed)
    )
    assert fill is not None
    books = list(
        BookIterator.from_events(
            with_instruments([placed, fill], [known]),
            snapshot_every=0,
        ).books
    )

    nested_order = books[0].deltas[0]
    nested_fill = books[-1].executions[0]
    assert nested_fill.primary_linked_hash == nested_order.xhash
    assert nested_order.hash in nested_fill.parenthash


@pytest.mark.parametrize("explicit", [False, True], ids=["max-age", "expunix"])
def test_stale_orders_expire_into_an_auditable_terminal_event(explicit: bool) -> None:
    expiring = order(
        BASE,
        BTC,
        Side.BID,
        100.0,
        5.0,
        "B1",
        expunix=BASE + 10 if explicit else None,
    )
    clock = order(BASE + 20, BTC, Side.ASK, 101.0, 1.0, "A1")
    iterator = BookIterator.from_events(
        [expiring, clock],
        snapshot_every=0,
        max_order_age_ns=None if explicit else 10,
    )

    latest = list(iterator)[-1]
    expired = [one for one in latest.deltas if one.orderid == "B1"]
    assert len(expired) == 1 and expired[0].state is State.INTERNAL_EXPIRED
    assert expired[0].expunix == BASE + 10
    assert expired[0].reason and latest.biddepth == 0


def test_an_inactive_instrument_snapshots_before_its_expiry_is_applied() -> None:
    btc = order(
        BASE + 60,
        BTC,
        Side.BID,
        100.0,
        5.0,
        "B1",
        expunix=BASE + HOUR + 10,
    )
    eth_clock = order(BASE + 2 * HOUR, ETH, Side.BID, 10.0, 1.0, "E1")

    btc_books = [one for one in BookIterator.from_events([btc, eth_clock]) if one.code == "BTC-USD"]

    snapshot = next(one for one in btc_books if one.snapunix is not None)
    expired = next(
        one
        for one in btc_books
        if any(event.state is State.INTERNAL_EXPIRED for event in one.deltas)
    )
    recovered = btc_books[-1]
    assert snapshot.unix == BASE + HOUR and snapshot.bidqty == 5.0
    assert expired.unix == eth_clock.unix and expired.biddepth == 0
    assert expired.deltas[-1].state is State.INTERNAL_EXPIRED
    assert recovered is expired and recovered.snapunix is None
    assert len({(book.unix, book.instrumentxhash) for book in btc_books}) == len(btc_books)


@pytest.mark.parametrize(
    ("explicit_offset", "max_age"),
    [
        (-10, None),
        (0, None),
        (None, HOUR - 70),
        (None, HOUR - 60),
    ],
    ids=["explicit-before", "explicit-at", "max-age-before", "max-age-at"],
)
def test_expiry_is_applied_before_a_crossed_snapshot_boundary(
    explicit_offset: int | None, max_age: int | None
) -> None:
    """A boundary never republishes interest whose known lifetime has ended."""
    if explicit_offset is not None:
        deadline = BASE + HOUR + explicit_offset
    else:
        assert max_age is not None
        deadline = BASE + 60 + max_age
    expiring = order(
        BASE + 60,
        BTC,
        Side.BID,
        100.0,
        5.0,
        "B1",
        expunix=deadline if explicit_offset is not None else None,
    )
    clock = order(BASE + 3 * HOUR + 60, BTC, Side.ASK, 101.0, 1.0, "A1")

    found = list(
        BookIterator.from_events(
            [expiring, clock],
            max_order_age_ns=max_age,
        )
    )

    boundary = [one for one in found if one.unix == BASE + HOUR]
    assert boundary and all(one.biddepth == 0 for one in boundary)
    assert len(boundary) == 1 and boundary[0].snapunix is None
    expired = [
        event
        for book in found
        for event in book.deltas
        if event.orderid == "B1" and event.state is State.INTERNAL_EXPIRED
    ]
    assert len(expired) == 1
    assert expired[0].expunix == deadline
    assert [one.unix for one in found] == sorted(one.unix for one in found)
    assert not any(one.unix == BASE + 2 * HOUR for one in found), (
        "a closed book stops producing unchanged snapshots"
    )


def test_an_inactive_instrument_expires_before_its_crossed_snapshot() -> None:
    btc = order(
        BASE + 60,
        BTC,
        Side.BID,
        100.0,
        5.0,
        "B1",
        expunix=BASE + HOUR - 10,
    )
    eth_clock = order(BASE + 3 * HOUR + 60, ETH, Side.BID, 10.0, 1.0, "E1")

    btc_books = [one for one in BookIterator.from_events([btc, eth_clock]) if one.code == "BTC-USD"]

    assert [one.unix for one in btc_books] == sorted(one.unix for one in btc_books)
    boundary = next(one for one in btc_books if one.unix == BASE + HOUR)
    assert boundary.biddepth == 0
    expired = [
        event
        for book in btc_books
        for event in book.deltas
        if event.orderid == "B1" and event.state is State.INTERNAL_EXPIRED
    ]
    assert len(expired) == 1
    assert all(one.biddepth == 0 for one in btc_books if one.unix >= BASE + HOUR)


def test_an_incomplete_limit_order_is_rejected_but_not_lost() -> None:
    incomplete = order(
        BASE,
        BTC,
        Side.BID,
        None,
        5.0,
        "B1",
        kind=MarketKind.LIMIT_ORDER,
    )

    (book,) = BookIterator.from_events([incomplete], snapshot_every=0)

    (audited,) = book.deltas
    assert audited.state is State.INTERNAL_REJECTED and "required price" in audited.reason
    assert book.biddepth == 0 and book.bidpx is None


def test_a_new_order_without_a_side_is_rejected_instead_of_silently_ignored() -> None:
    incomplete = order(BASE, BTC, Side.UNKNOWN, 100.0, 5.0, "B1")

    (book,) = BookIterator.from_events([incomplete], snapshot_every=0)

    (audited,) = book.deltas
    assert audited.state is State.INTERNAL_REJECTED and "side is missing" in audited.reason
    assert book.biddepth == book.askdepth == 0


def test_pending_new_is_validated_but_pending_cancel_may_omit_terms() -> None:
    incomplete = order(
        BASE,
        BTC,
        Side.BID,
        None,
        None,
        "NEW",
        kind=MarketKind.LIMIT_ORDER,
        state=State.PENDING_NEW,
    )
    standing = order(BASE + 10, BTC, Side.BID, 100.0, 5.0, "B1")
    cancel = order(
        BASE + 20,
        BTC,
        Side.BID,
        None,
        None,
        "B1",
        state=State.PENDING_CANCEL,
    )

    books = list(BookIterator.from_events([incomplete, standing, cancel], snapshot_every=0))

    assert books[0].deltas[0].state is State.INTERNAL_REJECTED
    assert books[-1].bidqty == 5.0
    assert books[-1].deltas[0].state is State.PENDING_CANCEL
    assert books[-1].deltas[0].reason is None


def test_a_rejected_replace_never_removes_the_standing_order() -> None:
    standing = order(BASE, BTC, Side.BID, 100.0, 5.0, "B1")
    malformed = order(
        BASE + 10,
        BTC,
        Side.BID,
        float("nan"),
        5.0,
        "B1",
        kind=MarketKind.LIMIT_ORDER,
        reason="upstream detail",
    )

    latest = list(BookIterator.from_events([standing, malformed], snapshot_every=0))[-1]

    assert latest.bidpx == 100.0 and latest.bidqty == 5.0
    assert latest.deltas[0].state is State.INTERNAL_REJECTED
    assert latest.deltas[0].reason == "upstream detail"


def test_negative_prices_are_valid_but_nonpositive_quantities_are_not() -> None:
    priced = order(
        BASE,
        BTC,
        Side.BID,
        -37.0,
        5.0,
        "B1",
        kind=MarketKind.LIMIT_ORDER,
    )
    bad_fill = initial(
        Execution(
            unix=BASE + 10,
            side=Side.BID,
            px=-37.0,
            qty=-1.0,
            state=State.FILLED,
            execid="E1",
        )
    )

    latest = list(BookIterator.from_events([priced, bad_fill], snapshot_every=0))[-1]

    assert latest.bidqty == 5.0
    assert latest.executions[0].state is State.INTERNAL_REJECTED
    assert "quantity" in latest.executions[0].reason


def test_a_fill_with_authoritative_leaves_is_not_subtracted_twice() -> None:
    placed = order(BASE, BTC, Side.BID, 100.0, 1_200.0, "B1")
    remaining = order(
        BASE + 10,
        BTC,
        Side.BID,
        100.0,
        800.0,
        "B1",
        state=State.PARTIALLY_FILLED,
    )
    fill = initial(
        Execution(
            unix=BASE + 10,
            side=Side.BID,
            px=100.0,
            qty=400.0,
            leavesqty=800.0,
            cumqty=400.0,
            state=State.FILLED,
            execid="E1",
            linkedhashes=[placed.xhash],
        )
    )

    latest = list(BookIterator.from_events([placed, remaining, fill], snapshot_every=0))[-1]

    assert latest.bidqty == 800.0 and latest.biddepth == 1
    assert latest.deltas[0].px == 100.0 and latest.deltas[0].qty == 800.0
    assert latest.deltas[0].prevqty == 1_200.0
    assert latest.deltas[0].version == 1 and latest.deltas[0].prevunix == placed.unix
    assert latest.deltas[0].prevhash == placed.hash
    assert latest.executions[0].qty == 400.0


def test_a_book_says_which_instrument_it_is_of_readably_and_by_hash() -> None:
    """A hash joins and a person reads, so a book row carries both."""
    events = _resting_stream()
    assert [one.instrumentcode for one in events] == [BTC.symbol, BTC.symbol]
    books = list(BookIterator.from_events(events, snapshot_every=0))
    assert {one.instrumentcode for one in books} == {BTC.symbol}
    assert {one.instrumentxhash for one in books} == {BTC.xhash}


# -- what happens to what is still resting when the stream ends ---------------


def _resting_stream() -> list[MarketEvent]:
    """Two live orders and nothing that ends them."""
    return [
        order(BASE, BTC, Side.BID, 100.0, 5.0, "b-1"),
        order(BASE + 1, BTC, Side.ASK, 101.0, 4.0, "a-1"),
    ]


def test_orders_still_resting_at_the_end_are_kept_by_default() -> None:
    """A run that will be resumed from its snapshots wants them exactly as they are."""
    books = list(BookIterator.from_events(_resting_stream(), snapshot_every=0))
    last = books[-1]
    assert (last.biddepth, last.askdepth) == (1, 1)
    assert [one.state for one in last.deltas] == [State.NEW]


def test_purge_alive_ends_them_as_auditable_versions() -> None:
    """A window that ends is not an order that aged out, so it says so per order."""
    books = list(BookIterator.from_events(_resting_stream(), snapshot_every=0, purge_alive=True))
    purged = [one for book in books for one in book.deltas if one.state is State.INTERNAL_EXPIRED]
    assert len(purged) == 2, "one terminal version per side, and no more"
    assert {one.qty for one in purged} == {0.0}
    assert all("still resting when the stream ended" in (one.reason or "") for one in purged)
    assert (books[-1].biddepth, books[-1].askdepth) == (0, 0)


def test_purging_leaves_the_lifecycles_it_ended_linked_to_their_book() -> None:
    """An expiry nobody can join back to its order is an expiry nobody can audit."""
    books = list(BookIterator.from_events(_resting_stream(), snapshot_every=0, purge_alive=True))
    last = books[-1]
    purged = [one for one in last.deltas if one.state is State.INTERNAL_EXPIRED]
    linked = set(last.linkedhashes)
    assert {one.xhash for one in purged} <= linked


def test_purge_alive_on_an_empty_stream_emits_nothing() -> None:
    assert list(BookIterator.from_events([], purge_alive=True)) == []


def test_resolved_instrument_components_send_a_row_to_the_scalar_translator() -> None:
    """A row whose legs or alt-ids live in resolved columns skips the flat path.

    Before componentization these rows carried `NoSecurityAltID <454>` or
    `NoLegs <555>` in `entries`, which is what `_COMPLEX_FIELDS` read to send
    them to the scalar translator. The groups are lifted with their count tags
    now, so the resolved column is the only remaining evidence -- and the
    routing must not quietly change with the storage. The batch still
    translates, through the scalar reference, to exactly what the scalar
    events say.
    """
    from rekep.fix.components import SecurityAltID as SecurityAltIDEntry
    from rekep.market.fix_arrow import flat_market_positions

    registry = FixRegistry(cache_dir=DATA, offline=True)
    plain = FixMsg(
        unix=BASE + 1,
        unixsource="TransactTime",
        beginstring="FIX.4.4",
        msgtype="D",
        symbol="ETH-USD",
        mic=MIC.from_str("XPAR"),
        clordid="C-1",
        side="1",
        ordtype="2",
        orderqty=10.0,
        price=100.0,
    )
    identified = dataclasses.replace(
        plain,
        clordid="C-2",
        securityaltid=[SecurityAltIDEntry(securityaltid="US0378331005", securityaltidsource="4")],
    )
    # A refused extraction -- the count lies -- leaves the group in `entries`
    # and the column null, so this row rides the pre-existing entries check.
    refused = dataclasses.replace(
        plain,
        clordid="C-3",
        entries=[(555, "9"), (600, "AAPL"), (624, "1")],
    )
    schema = FixMsg.into_field().into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pylist(
        [plain.into_row(), identified.into_row(), refused.into_row()], schema
    )

    assert into_flat_market_batches(batch, {"registry": registry}) is None
    positions = [
        where.to_pylist() for where in flat_market_positions(batch, {"registry": registry})
    ]
    assert positions == [[0]], "only the row with no group evidence at all stays flat"

    expected = [
        event
        for message in FixMsg.from_arrow_reader([batch])
        for event in message.into_market_events(registry=registry)
        if isinstance(event, Order)
    ]
    found = [
        translated
        for event_type, translated in FixMsg.into_market_arrow_batches(batch, registry=registry)
        if event_type is Order
    ]
    assert [order.clordid for order in expected] == ["C-1", "C-2", "C-3"]
    expected_table = pyarrow.Table.from_batches(list(Order.into_arrow_reader(expected)))
    assert pyarrow.Table.from_batches(found).equals(expected_table)


def test_flat_translation_reads_the_new_lifecycle_and_settlement_columns() -> None:
    """A cancel-reject's OrdStatus, an intent link and settlement facts read
    identically flat and scalar -- and the batch is flat-eligible, so the
    equality is between two live paths and not one falling back."""
    registry = FixRegistry(cache_dir=DATA, offline=True)
    linked = FixMsg(
        unix=BASE + 1,
        unixsource="TransactTime",
        beginstring="FIX.4.4",
        msgtype="D",
        symbol="ETH-USD",
        mic=MIC.from_str("XPAR"),
        clordid="C-1",
        side="1",
        ordtype="2",
        orderqty=10.0,
        price=100.0,
        parentclordid="P-1",
        parentorderid="V-9",
        entries=[(583, "LINK-1")],
    )
    rejected = dataclasses.replace(
        linked,
        msgtype="9",
        clordid="C-2",
        origclordid="C-1",
        ordstatus="2",
        parentclordid=None,
        parentorderid=None,
        entries=None,
    )
    settled = FixMsg(
        unix=BASE + 3,
        unixsource="TransactTime",
        beginstring="FIX.4.4",
        msgtype="AE",
        symbol="ETH-USD",
        mic=MIC.from_str("XPAR"),
        execid="E-1",
        side="2",
        lastpx=99.5,
        lastqty=3.0,
        entries=[(64, "20260818"), (63, "W2"), (120, "USD"), (156, "M")],
    )
    schema = FixMsg.into_field().into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pylist(
        [linked.into_row(), rejected.into_row(), settled.into_row()], schema
    )

    translated = into_flat_market_batches(batch, {"registry": registry})
    assert translated is not None, "every row here is flat-eligible"
    orders, executions = translated

    assert orders.column("clordlinkid").to_pylist() == ["LINK-1", None]
    assert orders.column("parentclordid").to_pylist() == ["P-1", None]
    assert orders.column("parentorderid").to_pylist() == ["V-9", None]
    assert orders.column("state").to_pylist()[1] == int(State.FILLED), (
        "the reject reads where the order stands from OrdStatus"
    )
    assert executions.column("settldate").to_pylist() == [datetime.date(2026, 8, 18)]
    assert executions.column("settltype").to_pylist() == ["W2"]
    assert executions.column("settlcurrency").to_pylist() == ["USD"]
    assert executions.column("settlcurrfxratecalc").to_pylist() == ["M"]

    expected = {Order: [], Execution: []}
    for message in FixMsg.from_arrow_reader([batch]):
        for event in message.into_market_events(registry=registry):
            if type(event) in expected:
                expected[type(event)].append(event)
    expected_orders = pyarrow.Table.from_batches(list(Order.into_arrow_reader(expected[Order])))
    assert pyarrow.Table.from_batches([orders]).equals(expected_orders)
    expected_executions = pyarrow.Table.from_batches(
        list(Execution.into_arrow_reader(expected[Execution]))
    )
    assert pyarrow.Table.from_batches([executions]).equals(expected_executions)
