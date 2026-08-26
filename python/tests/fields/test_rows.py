"""Column-built dataclass batches: byte equality with the document path, and
the member shapes only the columnar path can serialize -- real dates and
datetimes inside nested dataclass lists."""

import dataclasses
import datetime

import pyarrow
import pytest

from rekep import FixMsg
from rekep.enums import Currency, Side, State
from rekep.fields import scalar
from rekep.fields.rows import dataclass_arrow_batch
from rekep.fix.components import TrdRegTimestamp
from rekep.market import Instrument, Leg, Order

UNIX = 1_710_374_400_000_000_000


def _orders(rows: int) -> list[Order]:
    return [
        Order(
            unix=UNIX + index,
            side=Side.BID if index % 2 else Side.ASK,
            px=100.0 + index * 0.01,
            qty=1.0 + index % 3,
            order_id=f"O{index}",
            state=State.NEW,
            codes={"symbol": f"S{index}"},
            ccy=Currency.USD,
            linked_events=[(UNIX, index + 2)] if index % 2 else [],
        ).identify()
        for index in range(rows)
    ]


@pytest.mark.parametrize("rows", [0, 1, 7])
def test_columnar_batch_equals_the_document_batch(rows: int) -> None:
    events = _orders(rows)
    schema = Order.into_field().into_arrow_schema()
    built = dataclass_arrow_batch(events, schema)
    expected = pyarrow.RecordBatch.from_pylist(
        [event.into_dict() for event in events], schema=schema
    )
    assert built.schema.equals(schema, check_metadata=True)
    assert built.equals(expected)


def test_events_round_trip_through_the_reader() -> None:
    events = _orders(5)
    back = list(Order.from_arrow_reader(Order.into_arrow_reader(events, batch_row_size=2)))
    assert [one.into_dict() for one in back] == [one.into_dict() for one in events]


def test_nullable_dataclass_lists_and_dates_round_trip() -> None:
    # `legs` crosses null, empty and filled rows; `maturity` is a real date,
    # which the dict-per-row path could never hand to `from_pylist`.
    instruments = [
        Instrument(
            unix=UNIX,
            symbol=f"CAL-{index}",
            exchange="XPAR",
            currency=Currency.EUR,
            maturity=datetime.date(2027, 6, 1),
            alt_ids={"RIC": f"C{index}.N"} if index == 2 else None,
            legs=[None, [], [Leg(symbol=f"JUN-{index}", side=Side.BUY)]][index],
        ).identify()
        for index in range(3)
    ]
    back = list(Instrument.from_arrow_reader(Instrument.into_arrow_reader(instruments)))
    assert [one.into_dict() for one in back] == [one.into_dict() for one in instruments]
    assert back[0].legs is None and back[1].legs == [] and back[2].legs is not None
    assert back[2].maturity == datetime.date(2027, 6, 1)


def test_temporal_component_members_round_trip() -> None:
    stamped = datetime.datetime(2026, 8, 14, 0, 5, 1, 147000, tzinfo=datetime.UTC)
    message = FixMsg(
        unix=UNIX,
        message="8=FIX.4.4|35=8|37=ORD-1|55=BTC-USD|10=000",
        TrdRegTimestamps=[TrdRegTimestamp(TrdRegTimestamp=stamped, TrdRegTimestampType=1)],
    )
    back = list(FixMsg.from_arrow_reader(FixMsg.into_arrow_reader([message])))
    assert back[0].TrdRegTimestamps[0].TrdRegTimestamp == stamped
    assert back[0].TrdRegTimestamps[0].TrdRegTimestampType == 1


def test_a_single_member_shape_builds() -> None:
    @scalar
    @dataclasses.dataclass
    class One:
        value: int = 0

    schema = One.into_field().into_arrow_schema()
    built = dataclass_arrow_batch([One(value=3), One(value=4)], schema)
    assert built.to_pylist() == [{"value": 3}, {"value": 4}]
