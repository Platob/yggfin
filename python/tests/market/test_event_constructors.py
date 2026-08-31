"""Declaration-driven FIX constructors shared by every market event."""

from __future__ import annotations

import datetime

import pytest

from rekep.entries import Entry
from rekep.enums import Currency, EventType, OptionKind, Side, TimeInForce
from rekep.market import Execution, Instrument, Leg, Order
from rekep.market.identity import NIL
from rekep.text import FixMsg


def test_promoted_columns_precede_entries_and_explicit_values_precede_both() -> None:
    source = FixMsg(
        unix=11,
        recunix=12,
        eventtype=EventType.EXECUTION,
        hash=101,
        vhash=102,
        xhash=103,
        linkxhashes=[104],
        code="source-row",
        price=10.5,
        side="1",
        timeinforce="6",
        orderid="ORDER-1",
        altids={"clordid": "CLIENT-1"},
        entries=[Entry.of(tag=44, value="20.5"), Entry.of(tag=99, value="9.5")],
    )

    promoted = Order.from_fixmsg(source)
    overridden = Order.from_fixmsg(source, price=30, side=Side.SELL)

    assert (promoted.unix, promoted.recunix, promoted.altids) == (
        11,
        12,
        {"clordid": "CLIENT-1"},
    )
    assert (promoted.price, promoted.stoppx) == (10.5, 9.5)
    assert promoted.side is Side.BUY and promoted.timeinforce is TimeInForce.GTD
    assert promoted.orderid == "ORDER-1"
    assert (promoted.hash, promoted.vhash, promoted.xhash, promoted.linkxhashes) == (0, 0, 0, [])
    assert promoted.code == "" and promoted.eventtype is EventType.ORDER
    assert overridden.price == 30.0 and overridden.side is Side.SELL


def test_generic_dispatch_reads_numeric_entries_through_the_registry() -> None:
    entries = [
        Entry.of(tag=44, value="12.5"),
        Entry.of(tag=54, value="Buy"),
        Entry.of(tag=59, value="GoodTillDate"),
        Entry.of(tag=37, value="ORDER-2"),
    ]

    built = Order.from_(entries, version="4.4", unix=23)
    overridden = Order.from_entries(entries, version="4.4", price=13)

    assert (built.unix, built.price, built.orderid) == (23, 12.5, "ORDER-2")
    assert built.side is Side.BUY and built.timeinforce is TimeInForce.GTD
    assert overridden.price == 13.0


def test_subclass_declarations_select_their_exact_fix_fields() -> None:
    source = FixMsg(
        price=99,
        orderqty=10,
        lastpx=101.5,
        lastqty=2,
        execid="EXEC-1",
        side="2",
    )

    order = Order.from_fixmsg(source)
    execution = Execution.from_fixmsg(source)

    assert order.price == 99.0
    assert order.lastqty is None
    assert (execution.price, execution.lastqty, execution.execid) == (101.5, 2.0, "EXEC-1")
    assert execution.side is Side.SELL


def test_promoted_components_convert_by_their_declared_fix_fields() -> None:
    source = FixMsg(
        unix=31,
        protocol="FIX4.4",
        instrument=Instrument(
            symbol="SPREAD",
            legs=[
                Leg(
                    symbol="LEG-A",
                    side=Side.BUY,
                    ratio=2,
                    currency="USD",
                    maturitydate=datetime.datetime(2027, 3, 19),
                    putorcall=OptionKind.CALL,
                )
            ],
        ),
    )

    built = Instrument.from_fixmsg(source)

    assert built is not None and built.symbolticker == "SPREAD"
    assert built.legs is not None and len(built.legs) == 1
    leg = built.legs[0]
    assert (leg.symbol, leg.ratio, leg.maturitydate) == (
        "LEG-A",
        2.0,
        datetime.datetime(2027, 3, 19),
    )
    assert leg.side is Side.BUY and leg.currency is Currency.USD
    assert leg.putorcall is OptionKind.CALL


def test_indexed_raw_entries_build_nested_declared_components() -> None:
    entries = [
        Entry.of(tag=55, value="SPREAD"),
        Entry.of(tag=600, value="LEG-A", comp="Strategies[0].NoLegs[0]"),
        Entry.of(tag=624, value="2", comp="Strategies[0].NoLegs[0]"),
        Entry.of(tag=623, value="3", comp="Strategies[0].NoLegs[0]"),
        Entry.of(tag=600, value="LEG-B", comp="Strategies[1].NoLegs[0]"),
        Entry.of(tag=624, value="1", comp="Strategies[1].NoLegs[0]"),
    ]

    built = Instrument.from_entries(entries, version="4.4")

    assert built.symbolticker == "SPREAD"
    assert built.legs is not None
    assert [(leg.symbol, leg.side, leg.ratio) for leg in built.legs] == [
        ("LEG-A", Side.SELL, 3.0),
        ("LEG-B", Side.BUY, None),
    ]


def test_registry_scalars_are_projected_to_the_declared_python_types() -> None:
    instrument = Instrument.from_entries(
        [Entry.of(tag=55, value="OPTION"), Entry.of(tag=201, value="1")],
        version="4.4",
    )
    execution = Execution.from_entries([Entry.of(tag=64, value="20260818")], version="4.4")

    assert instrument.putorcall is OptionKind.CALL
    assert execution.settldate == datetime.datetime(2026, 8, 18)
    assert type(execution.settldate) is datetime.datetime


def test_a_local_settlement_rule_can_retain_a_clock() -> None:
    execution = Execution.from_entries(
        [Entry.of(tag=64, value="20260818-16:30:00.250")], version="4.4"
    )
    assert execution.settldate == datetime.datetime(2026, 8, 18, 16, 30, 0, 250000)


def test_aware_local_settlement_matches_scalar_and_arrow_identity() -> None:
    east = datetime.timezone(datetime.timedelta(hours=2))
    execution = Execution(
        unix=1_700_000_000_000_000_000,
        execid="EXEC-1",
        settldate=datetime.datetime(2026, 8, 18, 16, 30, 0, 250000, tzinfo=east),
    ).identify()

    assert execution.settldate == datetime.datetime(2026, 8, 18, 14, 30, 0, 250000)
    (stored,) = Execution.from_arrow_reader([Execution.into_arrow_batch([execution])])
    stored.vhash = NIL
    stored.hash = NIL
    stored.identify()

    assert stored.settldate == execution.settldate
    assert stored.vhash == execution.vhash


def test_from_fixmsg_refuses_an_unparsed_source() -> None:
    with pytest.raises(TypeError, match="source must be FixMsg, got object"):
        Order.from_fixmsg(object())
