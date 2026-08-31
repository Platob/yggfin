"""OMS XML component extraction and market translation."""

from __future__ import annotations

import pyarrow

from rekep.enums import EventType, State
from rekep.fix.oms import OmsOrder, OmsOrders
from rekep.fix.rules import Rules
from rekep.market.orders import Execution, Order
from rekep.text.fixmsg import FixMsg
from rekep.text.message import Message

OMS_XML = """\
<event id="EVT-20260828-0001" instance="SIM" type="orderdelta"
 timestamp="20260828-15:23:31.657">
  <order id="ORD-A19X" clientid="CLI-Z72"
   instrumentid="dbi;GB00SYNTH001_XLON_GBP" exchangeid="XLON" side="sell"
   executionstate="filled" quantity="71" cumqty="71" leavesqty="0"
   lastqty="71" lastpx="20.7">
    <price value="20.7" avg="20.6" capital="1469.7"/>
    <data><ullink>
      <visible orderid="VEN-A19X" account="ACCT-Q7" mystery="keep-v"/>
      <invisible price="20.7" mystery="keep-i"/>
      <persisted clordid="CLI-Z72" mystery="keep-p"/>
    </ullink></data>
  </order>
  <action id="ACT-T88" userid="robot_7" type="order.terminate">
    <order id="ORD-A19X" clientid="CLI-Z72"
     instrumentid="dbi;GB00SYNTH001_XLON_GBP" exchangeid="XLON" side="sell"
     executionstate="terminated" quantity="71" cumqty="71" leavesqty="0">
      <terminate reason="expired"/>
    </order>
  </action>
</event>
"""


def _parsed() -> pyarrow.RecordBatch:
    """The synthetic document through the two supported parsing stages."""
    return FixMsg.from_message_batch([Message(body=OMS_XML)])


def test_oms_orders_preserve_owner_provenance_and_one_ullink_shape() -> None:
    parsed = _parsed()
    orders = parsed.column("omsorders")[0].as_py()

    assert len(orders) == 2
    direct, terminating = orders
    assert (direct["orderpath"], direct["owner"], direct["actionid"]) == (
        "event[0].order[0]",
        "event",
        None,
    )
    assert (
        terminating["orderpath"],
        terminating["owner"],
        terminating["actionid"],
        terminating["actiontype"],
        terminating["actionuserid"],
    ) == (
        "event[0].action[0].order[0]",
        "action",
        "ACT-T88",
        "order.terminate",
        "robot_7",
    )
    assert [order["eventid"] for order in orders] == ["EVT-20260828-0001"] * 2
    assert direct["transacttime"].isoformat() == "2026-08-28T15:23:31.657000+00:00"
    assert (
        direct["orderqty"],
        direct["cumqty"],
        direct["leavesqty"],
        direct["lastqty"],
        direct["price"],
        direct["avgpx"],
        direct["lastpx"],
        direct["grosstradeamt"],
    ) == (71.0, 71.0, 0.0, 71.0, 20.7, 20.6, 20.7, 1469.7)
    assert terminating["reason"] == "expired"

    ullink = direct["ullink"]
    assert len(ullink) == 3
    assert [entry["source"] for entry in ullink] == [
        "visible",
        "invisible",
        "persisted",
    ]
    assert (ullink[0]["orderid"], ullink[1]["price"], ullink[2]["clordid"]) == (
        "VEN-A19X",
        20.7,
        "CLI-Z72",
    )
    assert OmsOrders.component == "event" and OmsOrders.group == "order"
    assert OmsOrder.into_field().field("price").fix.name == "Price"

    residual = [
        *parsed.column("entries")[0].as_py(),
        *parsed.column("unmap")[0].as_py(),
    ]
    assert [(entry["key"], entry["value"]) for entry in residual] == [
        ("instance", "SIM"),
        ("mystery", "keep-v"),
        ("mystery", "keep-i"),
        ("mystery", "keep-p"),
    ]


def test_oms_market_rows_keep_the_order_lifecycle_and_require_trade_evidence() -> None:
    converted = dict(
        FixMsg.into_market_arrow_batches(
            [_parsed()],
            batch_row_size=None,
            fix_version="4.4",
        )
    )
    orders = converted[Order]
    executions = converted[Execution]

    assert (orders.num_rows, executions.num_rows) == (2, 1)
    assert orders.column("unix").to_pylist() == [1787930611657000000] * 2
    assert orders.column("state").to_pylist() == [int(State.FILLED), int(State.EXPIRED)]
    assert orders.column("code").to_pylist() == ["ORD-A19X", "ORD-A19X"]
    assert orders.column("xhash")[0].as_py() == orders.column("xhash")[1].as_py()
    assert orders.column("hash")[0].as_py() != orders.column("hash")[1].as_py()
    assert orders.column("symbolticker").to_pylist() == ["XLON:0:GB00SYNTH001"] * 2
    assert orders.column("vwap").to_pylist() == [None, None]

    assert executions.column("state").to_pylist() == [int(State.FILLED)]
    assert executions.column("execid").to_pylist() == ["EVT-20260828-0001"]
    assert executions.column("lastpx").to_pylist() == [20.7]
    assert executions.column("lastqty").to_pylist() == [71.0]
    assert executions.column("vwap").to_pylist() == [20.7]
    assert orders.column("linkhashes")[0].as_py() == [executions.column("hash")[0].as_py()]
    assert orders.column("linkhashes")[1].as_py() == []
    assert executions.column("linkhashes")[0].as_py() == [orders.column("hash")[0].as_py()]


def test_invalid_oms_numbers_stay_residual_without_failing_the_row() -> None:
    parsed = FixMsg.from_message_batch(
        [
            Message(
                body=(
                    '<event id="E-1"><order id="O-1" quantity="not-a-number" '
                    'instrumentid="dbi;GB00SYNTH002_XPAR_EUR">'
                    '<qty value="12"/></order></event>'
                )
            )
        ]
    )

    order = parsed.column("omsorders")[0].as_py()[0]
    assert order["orderid"] == "O-1"
    assert order["orderqty"] == 12.0
    assert parsed.column("error").to_pylist() == [None]
    residual = [
        *parsed.column("entries")[0].as_py(),
        *parsed.column("unmap")[0].as_py(),
    ]
    assert [(entry["key"], entry["value"]) for entry in residual] == [("quantity", "not-a-number")]


def test_oms_orders_classify_only_unclassified_rows_as_market_orders() -> None:
    parsed = FixMsg.from_message_batch(
        [
            Message(body=OMS_XML),
            Message(body="an operational note"),
            Message(body=OMS_XML, eventtype=EventType.EXECUTION),
        ]
    )

    assert parsed.column("eventtype").to_pylist() == [
        int(EventType.ORDER),
        int(EventType.MISC),
        int(EventType.EXECUTION),
    ]
    assert Rules().into_arrow_category_array(
        parsed.column("protocol"), parsed.column("eventtype")
    ).to_pylist() == ["market", "misc", "market"]


def test_oms_market_translation_accepts_storage_empty_component_lists() -> None:
    parsed = _parsed()
    storage_empty = {"trdregtimestamps", "sidetrdregts", "securityaltid", "legs"}
    persisted = pyarrow.RecordBatch.from_arrays(
        [
            pyarrow.array([[]], field.type)
            if field.name in storage_empty
            else parsed.column(field.name)
            for field in parsed.schema
        ],
        schema=parsed.schema,
    )

    converted = dict(
        FixMsg.into_market_arrow_batches([persisted], batch_row_size=None, fix_version="4.4")
    )
    assert converted[Order].num_rows == 2
    assert converted[Execution].num_rows == 1
