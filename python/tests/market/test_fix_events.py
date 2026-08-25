"""FIX messages read as market events: which timestamp, which shape, which fields.

The rules under test are the standard's own, so where one is not obvious the
test says which FIX definition it rests on.
"""

from __future__ import annotations

import datetime

import pytest

from rekep.fix import FixPairs, FixRegistry, fix_field
from rekep.market import (
    MIC,
    AssetKind,
    Currency,
    Execution,
    MarketKind,
    OptionKind,
    Order,
    Side,
    State,
    TimeInForce,
)
from rekep.market.fix import (
    CARRIED_FIELDS,
    FIX_STATES,
    TRANSACTED,
    FixEvents,
    MarketTags,
    market_tags,
    unix_of,
)

#: One filled ExecutionReport, spelled the way a log prints one.
FILLED = (
    "8=FIX.4.4|9=289|35=8|49=XCME|56=ACME|34=1090|52=20260821-10:30:00.250|"
    "37=ORD-9|11=CL-7|41=CL-6|17=EX-3|150=F|39=1|55=BTC-USD|207=XCME|15=USD|"
    "54=1|38=10|44=100.5|40=2|59=1|32=4|31=100.25|14=4|151=6|6=100.25|"
    "60=20260821-10:29:59.998|1057=Y|9999=venue-thing|10=123"
)


def events(line: str, **carried: object) -> list[object]:
    return list(reader(line, **carried))


def reader(line: str, **carried: object) -> FixEvents:
    """One message as the translator reads it, under a pinned dictionary version."""
    carried.setdefault("fix_version", "4.4")
    return FixEvents.from_text(line, **carried)


# -- when it happened --------------------------------------------------------


def test_a_timestamp_is_read_as_whole_nanoseconds_since_the_epoch() -> None:
    assert unix_of("20260821-10:30:00.123456789") == 1787308200123456789
    assert unix_of("20260821-10:30:00") == 1787308200000000000


def test_a_fraction_is_scaled_by_its_own_width() -> None:
    """`.5` is half a second and `.000000001` is a nanosecond; both are decimals."""
    assert unix_of("20260821-00:00:00.5") - unix_of("20260821") == 500_000_000
    assert unix_of("20260821-00:00:00.000000001") - unix_of("20260821") == 1


def test_epoch_fix_time_is_not_mistaken_for_an_absent_clock() -> None:
    reader = FixEvents.from_text("35=D|52=19700101-00:00:00", runix=123, fix_version="4.4")
    assert reader.unix == 0


def test_a_date_alone_and_a_time_alone_are_both_read() -> None:
    """One reading for all three spellings, because feeds disagree with their own
    dictionary about which of them a field is declared as."""
    day = unix_of("20260821")
    assert day == 1787270400000000000
    assert unix_of("10:30:00", day=day) == day + 37_800_000_000_000


def test_a_time_with_no_day_is_that_time_on_the_epoch_rather_than_a_guess() -> None:
    assert unix_of("10:30:00") == 37_800_000_000_000


def test_a_day_that_is_not_a_whole_day_still_places_a_time_on_it() -> None:
    """`day` is any instant in the day, so passing an event's own `unix` works."""
    noon = unix_of("20260821-12:00:00")
    assert unix_of("10:30:00", day=noon) == unix_of("20260821-10:30:00")


@pytest.mark.parametrize(
    "text", ["", None, "nonsense", "20260231-10:00:00", "99:99:99", "20260821-25:00:00"]
)
def test_anything_that_is_not_a_timestamp_reads_as_absent(text: str | None) -> None:
    """Zero would be the epoch, which is a real instant a sort puts first."""
    assert unix_of(text) is None


def test_a_leap_second_is_a_time_because_the_standard_admits_one() -> None:
    """`SS` is 00-60 in FIX, and 60 is the leap second -- not a parse error.

    It lands on the next midnight, which is what counting whole seconds since
    the epoch does with one: the count has no leap second in it either.
    """
    assert unix_of("20260630-23:59:60") == unix_of("20260701")


def test_the_transaction_time_is_preferred_over_the_transmission_time() -> None:
    """FIX defines `TransactTime <60>` as when the business transaction occurred and
    `SendingTime <52>` as when the message was transmitted. They are not the same
    instant, and reading them as one is how a latency measurement comes out zero."""
    order, _ = events(FILLED, runix=7)
    assert order.unix == unix_of("20260821-10:29:59.998")
    assert order.unix != unix_of("20260821-10:30:00.250")


def test_the_recording_clock_is_the_readers_and_stays_separate() -> None:
    order, _ = events(FILLED, runix=1_787_308_200_400_000_000)
    assert order.runix == 1_787_308_200_400_000_000
    assert order.runix > order.unix, "recorded after it happened, which is the point"


def test_the_fix_sequence_is_not_repeated_on_market_events() -> None:
    order, fill = events(FILLED)
    assert not hasattr(order, "seq") and not hasattr(fill, "seq")


def test_without_a_transaction_time_the_message_falls_down_the_declared_order() -> None:
    """Every step of `TRANSACTED`, in order, each one dropped in turn."""
    header = "35=D|55=AAPL|11=CL-1|"
    assert events(header + "60=20260821-10:00:00|42=20260821-09:00:00")[0].unix == unix_of(
        "20260821-10:00:00"
    )
    assert events(header + "42=20260821-09:00:00|122=20260821-08:00:00")[0].unix == unix_of(
        "20260821-09:00:00"
    )
    assert events(header + "122=20260821-08:00:00|52=20260821-11:00:00")[0].unix == unix_of(
        "20260821-08:00:00"
    )
    assert events(header + "52=20260821-11:00:00")[0].unix == unix_of("20260821-11:00:00")


def test_a_message_with_no_time_at_all_says_so_with_a_zero() -> None:
    assert events("35=D|55=AAPL|11=CL-1")[0].unix == 0


def test_a_message_with_no_fix_clock_uses_the_recorded_log_instant() -> None:
    recorded = unix_of("20260821-10:30:00")
    assert events("35=D|55=AAPL|11=CL-1", runix=recorded)[0].unix == recorded


def test_the_declared_order_is_the_one_the_reader_uses() -> None:
    """Pinned, because the rule *is* the constant: a reordering here is a silent
    change of which clock every downstream row is stamped with."""
    assert tuple(rung.name for rung in TRANSACTED) == (
        "TrdRegTimestamps",
        "SideTrdRegTS",
        "TransactTime",
        "MDEntry",
        "OrigTime",
        "OrigSendingTime",
        "SendingTime",
    )
    regulatory = [rung for rung in TRANSACTED if rung.is_column]
    assert [rung.column for rung in regulatory] == [
        "TrdRegTimestamps",
        "SideTrdRegTS",
    ], "the regulatory record leads, because it is the strongest claim in the message"
    assert [rung.fields for rung in TRANSACTED if not rung.is_column] == [
        ("TransactTime",),
        ("MDEntryDate", "MDEntryTime"),
        ("OrigTime",),
        ("OrigSendingTime",),
        ("SendingTime",),
    ]


def test_a_regulatory_stamp_outranks_the_messages_own_claim() -> None:
    """`TransactTime <60>` is what the message says; the group is what was recorded."""
    header = "35=D|55=AAPL|11=CL-1|60=20260821-10:00:00|"
    group = "768=1|769=20260821-09:00:00|770=2|"
    order = events(header + group)[0]
    assert order.unix == unix_of("20260821-09:00:00")


def test_which_regulatory_stamp_counts_depends_on_what_the_row_is() -> None:
    """One group, two kinds of row, two answers -- which is why the table exists.

    An order happened when it arrived (`TIME_IN <2>`); an execution happened
    when it executed (`EXECUTION_TIME <1>`). Reading either as "the group's
    first entry" would stamp one of them with the other's instant.
    """
    group = "768=2|769=20260821-09:00:00|770=1|769=20260821-08:00:00|770=2|"
    ordered = events("35=D|55=AAPL|11=CL-1|" + group)[0]
    assert ordered.unix == unix_of("20260821-08:00:00"), "an order takes TIME_IN"
    reported = events("35=8|55=AAPL|11=CL-1|17=E1|150=F|39=2|" + group)[0]
    assert reported.unix == unix_of("20260821-09:00:00"), "a report takes EXECUTION_TIME"


def test_a_group_carrying_no_ranked_type_still_answers() -> None:
    """A regulatory stamp nobody ranked is still nearer than a transmission clock."""
    order = events("35=D|55=AAPL|11=CL-1|52=20260821-11:00:00|768=1|769=20260821-09:00:00|770=3|")[
        0
    ]
    assert order.unix == unix_of("20260821-09:00:00")


def test_the_rung_that_answered_is_recorded() -> None:
    """Without it nothing downstream tells a transaction time from a print time."""
    header = "35=D|55=AAPL|11=CL-1|"
    assert reader(header + "768=1|769=20260821-09:00:00|770=2|").transacted.source == (
        "TrdRegTimestamps=2"
    )
    assert reader(header + "60=20260821-10:00:00").transacted.source == "TransactTime"
    assert reader(header + "52=20260821-11:00:00").transacted.source == "SendingTime"
    assert reader(header, runix=unix_of("20260821-10:30:00")).transacted.source == "recorded"
    assert reader(header).transacted.source == "", "no clock anywhere"


# -- an execution report is two events ---------------------------------------


def test_a_filled_report_is_both_the_order_and_the_fill() -> None:
    """FIX uses one message for "your order is now partially filled" and "here is
    the fill that did it"; they are two rows, and storing one loses the other."""
    order, fill = events(FILLED)
    assert isinstance(order, Order) and isinstance(fill, Execution)


def test_the_order_carries_what_remains_and_the_fill_what_moved() -> None:
    """`LeavesQty <151>` is live interest; `LastQty <32>` is what just traded."""
    order, fill = events(FILLED)
    assert (order.px, order.qty) == (100.5, 6.0)
    assert (fill.px, fill.qty) == (100.25, 4.0)


def test_the_order_comes_first_because_the_fill_points_at_it() -> None:
    order, fill = events(FILLED)
    assert fill.linked_events == [(order.unix, order.xhash)]
    assert fill.parent_hash == [order.hash]


def test_a_report_hashes_only_the_completed_fill_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = Execution.hash_of.__func__
    calls = 0

    def counted(cls: type[Execution], *parts: object) -> int:
        nonlocal calls
        calls += 1
        return original(cls, *parts)

    monkeypatch.setattr(Execution, "hash_of", classmethod(counted))

    _, fill = events(FILLED)

    assert fill.hash and calls == 1


def test_an_acknowledgement_produces_no_execution_row() -> None:
    """`ExecType <150>` `0` is New: nothing traded, and summing acks as fills is
    exactly the mistake the field-specific state mapping prevents."""
    acked = FILLED.replace("|150=F|", "|150=0|").replace("|39=1|", "|39=0|")
    (only,) = events(acked)
    assert isinstance(only, Order) and only.state is State.NEW


@pytest.mark.parametrize("code", ["F", "G", "H"])
def test_every_trade_exectype_produces_a_fill(code: str) -> None:
    """Trade, Trade Correct and Trade Cancel all say something about a trade."""
    assert len(events(FILLED.replace("|150=F|", f"|150={code}|"))) == 2


def test_the_order_state_is_the_ordstatus_the_venue_sent() -> None:
    order, _ = events(FILLED)
    assert order.state is State.PARTIALLY_FILLED


def test_an_executions_own_state_is_about_the_fill_not_the_order() -> None:
    """A fill is done the instant it exists; the order it belongs to may not be."""
    _, fill = events(FILLED)
    assert fill.state is State.FILLED
    assert fill.state is not State.PARTIALLY_FILLED


def test_a_trade_cancel_and_a_correct_read_as_what_they_do_to_the_fill() -> None:
    _, cancelled = events(FILLED.replace("|150=F|", "|150=H|"))
    _, corrected = events(FILLED.replace("|150=F|", "|150=G|"))
    assert cancelled.state is State.CANCELLED
    assert corrected.state is State.REPLACED


def test_a_trade_correction_keeps_its_execid_and_anchors_on_execrefid() -> None:
    report = FILLED.replace("|17=EX-3|", "|17=EX-4|19=EX-3|").replace("|150=F|", "|150=G|")
    _, corrected = events(report)

    assert corrected.exec_id == "EX-4" and corrected.exec_ref_id == "EX-3"
    assert corrected.code == "EX-3"


def test_a_lifecycle_exectype_reads_through_the_code_it_shares_with_ordstatus() -> None:
    """`ExecType <150>` and `OrdStatus <39>` use the same characters for the same
    lifecycle events, so nothing has to restate the mapping."""
    report = FILLED.replace("|150=F|", "|150=4|").replace("|39=1|", "|39=4|")
    order, *rest = events(report)
    assert order.state is State.CANCELLED and not rest


# -- an order request --------------------------------------------------------


@pytest.mark.parametrize(
    "kind,state",
    [("D", State.PENDING_NEW), ("F", State.PENDING_CANCEL), ("G", State.PENDING_REPLACE)],
)
def test_a_request_is_pending_because_the_venue_has_not_agreed_yet(kind: str, state: State) -> None:
    """A NewOrderSingle is what a participant asked for, not an acknowledgement."""
    (order,) = events(f"35={kind}|55=AAPL|11=CL-1|54=1|38=100|44=10.0|60=20260821-10:00:00")
    assert order.state is state


def test_an_order_carries_every_slot_the_message_filled() -> None:
    order, _ = events(FILLED)
    assert order.kind is MarketKind.LIMIT_ORDER and order.tif is TimeInForce.GTC
    assert order.side is Side.BUY
    assert (order.qty, order.vwap) == (6.0, 100.25)
    assert not hasattr(order, "filled_qty") and not hasattr(order, "leaves_qty")
    assert (order.order_id, order.client_order_id) == ("ORD-9", "CL-7")
    assert order.prev_client_order_id == "CL-6"


def test_every_parsed_identifier_is_retained_in_isolated_code_maps() -> None:
    report = FILLED.replace(
        "|17=EX-3|",
        "|17=EX-3|527=EX-SECONDARY|19=EX-2|1003=TR-1|278=MD-1|280=MD-0|",
    )
    order, execution = events(report)
    shared = {
        "order_id": "ORD-9",
        "orig_cl_ord_id": "CL-6",
        "cl_ord_id": "CL-7",
        "exec_id": "EX-3",
        "secondary_exec_id": "EX-SECONDARY",
        "exec_ref_id": "EX-2",
        "trade_id": "TR-1",
        "md_entry_id": "MD-1",
        "md_entry_ref_id": "MD-0",
        "symbol": "BTC-USD",
    }

    assert {name: order.codes[name] for name in shared} == shared
    assert {name: execution.codes[name] for name in shared} == shared
    assert order.codes is not execution.codes
    order.codes["local"] = "order-only"
    assert "local" not in execution.codes


def test_a_rendered_identifier_missing_from_the_merged_index_is_still_retained() -> None:
    parsed = FixEvents.from_pairs([("MDEntryRefID", "MD-NAMED")], fix_version="4.4")

    assert parsed._identifier_codes["md_entry_ref_id"] == "MD-NAMED"


def test_order_qty_uses_explicit_or_derived_live_quantity() -> None:
    explicit, _ = events(FILLED)
    derived, _ = events(FILLED.replace("|151=6|", "|"))
    assert explicit.qty == derived.qty == 6.0


def test_an_omitted_time_in_force_is_day_as_fix_specifies() -> None:
    (order,) = events("35=D|55=AAPL|11=CL-1|54=1|38=100|44=10|60=20260821-10:00:00")
    assert order.tif is TimeInForce.DAY and order.eunix is None


def test_a_price_the_venue_did_not_send_is_absent_and_not_zero() -> None:
    """A market order has no limit at all, and zero is a price."""
    (order,) = events("35=D|55=AAPL|11=CL-1|40=1|54=1|38=100|60=20260821-10:00:00")
    assert order.px is None and order.kind is MarketKind.MARKET_ORDER


def test_a_local_market_expiry_date_is_preserved_but_not_guessed_as_utc() -> None:
    (order,) = events("35=D|55=AAPL|11=CL-1|59=6|432=20260821|60=20260821-10:00:00")
    assert order.eunix is None
    assert order.metadata["432"] == "20260821"


def test_an_explicit_expiry_time_wins_over_the_day() -> None:
    (order,) = events(
        "35=D|55=AAPL|11=CL-1|59=6|432=20260821|126=20260821-16:30:00|60=20260821-10:00:00"
    )
    assert order.eunix == unix_of("20260821-16:30:00")


@pytest.mark.parametrize(
    ("unit", "factor"),
    [
        (None, 1_000_000_000),
        ("0", 1_000_000_000),
        ("1", 100_000_000),
        ("2", 10_000_000),
        ("3", 1_000_000),
        ("4", 1_000),
        ("5", 1),
        ("10", 60_000_000_000),
        ("11", 3_600_000_000_000),
        ("12", 86_400_000_000_000),
        ("13", 604_800_000_000_000),
    ],
)
def test_good_for_time_derives_an_exact_expiry(unit: str | None, factor: int) -> None:
    unit_pair = "" if unit is None else f"|1916={unit}"
    (order,) = events(f"35=D|55=AAPL|11=CL-1|59=A|1629=2{unit_pair}|60=20260821-10:00:00")
    assert order.tif is TimeInForce.GFT
    assert order.eunix == order.unix + 2 * factor
    assert order.metadata["1629"] == "2"
    if unit is not None:
        assert order.metadata["1916"] == unit


@pytest.mark.parametrize(
    ("duration", "unit"), [("0", "0"), ("-1", "0"), ("2", "14"), ("2", "15"), ("2", "99")]
)
def test_good_for_time_refuses_non_positive_or_calendar_durations(duration: str, unit: str) -> None:
    (order,) = events(f"35=D|55=AAPL|11=CL-1|59=A|1629={duration}|1916={unit}|60=20260821-10:00:00")
    assert order.eunix is None
    assert order.metadata["1629"] == duration
    assert order.metadata["1916"] == unit


def test_expire_time_wins_over_good_for_time() -> None:
    (order,) = events(
        "35=D|55=AAPL|11=CL-1|59=A|1629=1|1916=13|126=20260821-10:00:01|60=20260821-10:00:00"
    )
    assert order.eunix == unix_of("20260821-10:00:01")


@pytest.mark.parametrize("fix", ["3", "4"], ids=["ioc", "fok"])
def test_immediate_time_in_force_expires_on_arrival(fix: str) -> None:
    (order,) = events(f"35=D|55=AAPL|11=CL-1|54=1|38=100|44=10|59={fix}|60=20260821-10:00:00")
    assert order.eunix == order.unix == unix_of("20260821-10:00:00")


@pytest.mark.parametrize("fix", ["0", "2", "5", "7", "8", "9", "B", "C"])
def test_calendar_dependent_time_in_force_has_no_invented_instant(fix: str) -> None:
    (order,) = events(f"35=D|55=AAPL|11=CL-1|54=1|38=100|44=10|59={fix}|60=20260821-10:00:00")
    assert order.eunix is None


# -- market data -------------------------------------------------------------

REFRESH = (
    "8=FIX.4.4|35=X|49=XCME|52=20260821-10:30:00.250|55=BTC-USD|207=XCME|15=USD|268=3|"
    "279=0|269=0|270=100.0|271=5|272=20260821|273=10:29:59.100|"
    "279=1|269=1|270=100.5|271=7|273=10:29:59.200|"
    "279=0|269=2|270=100.2|271=1|273=10:29:59.300|10=1"
)


def test_a_refresh_is_one_event_per_entry() -> None:
    found = events(REFRESH)
    assert [type(one).__name__ for one in found] == ["Order", "Order", "Execution"]


def test_a_bid_entry_and_an_offer_entry_land_on_the_right_sides() -> None:
    """`MDEntryType <269>` `0` is Bid and `1` is Offer -- and a bid *is* a buy."""
    bid, ask, _ = events(REFRESH)
    assert bid.side is Side.BID and bid.side is Side.BUY
    assert ask.side is Side.ASK and ask.side is Side.SELL


def test_every_entry_keeps_its_own_instant() -> None:
    """`MDEntryDate <272>` and `MDEntryTime <273>` are per entry, and a refresh that
    stamped all three with the header's time would lose the order they arrived in."""
    stamps = [one.unix for one in events(REFRESH)]
    assert stamps == sorted(stamps) and len(set(stamps)) == 3
    assert stamps[0] == unix_of("20260821-10:29:59.100")


def test_an_entry_after_the_first_inherits_the_date_it_was_not_resent() -> None:
    """Only the first entry carries `MDEntryDate <272>`; the rest are times of day."""
    _, ask, _ = events(REFRESH)
    assert ask.unix == unix_of("20260821-10:29:59.200")


def test_an_update_action_decides_what_the_entry_does_to_the_book() -> None:
    """`MDUpdateAction <279>` `2` is Delete, which is the end of that interest."""
    deleted = REFRESH.replace("279=0|269=0|", "279=2|269=0|", 1)
    bid, *_ = events(deleted)
    assert bid.state is State.CANCELLED
    assert bid.metadata["279"] == "2"


def test_a_snapshot_entry_with_no_action_is_present_by_definition() -> None:
    """Which is what a full refresh <W> means, and why it carries no action."""
    (level,) = events("35=W|55=AAPL|268=1|269=0|270=10.0|271=100|52=20260821-10:00:00")
    assert level.state is State.NEW


def test_a_trade_entry_is_an_execution_and_not_a_resting_order() -> None:
    *_, printed = events(REFRESH)
    assert isinstance(printed, Execution)
    assert printed.state is State.FILLED
    assert (printed.px, printed.qty) == (100.2, 1.0)


def test_an_entry_type_that_is_a_statistic_is_not_a_market_event() -> None:
    """A settlement price and a session high are facts about the market, not orders
    in it, and there is no row here that could hold one honestly."""
    assert events("35=X|55=AAPL|268=1|269=6|270=99.5|52=20260821-10:00:00") == []


def test_a_level_with_no_entry_id_is_still_one_lifecycle_across_its_updates() -> None:
    """`MDUpdateAction` Change and Delete address a level by its price, so the price
    is what persists when the venue gives no `MDEntryID <278>`."""
    first = "35=X|55=AAPL|268=1|279=0|269=0|270=10.0|271=5|52=20260821-10:00:00"
    later = "35=X|55=AAPL|268=1|279=1|269=0|270=10.0|271=9|52=20260821-10:00:01"
    assert events(first)[0].xhash == events(later)[0].xhash


def test_an_entry_id_is_the_lifecycle_when_the_venue_gives_one() -> None:
    with_id = "35=X|55=AAPL|268=1|279=0|269=0|270=10.0|271=5|278=E-1|52=20260821-10:00:00"
    moved = "35=X|55=AAPL|268=1|279=1|269=0|270=11.0|271=5|278=E-1|52=20260821-10:00:01"
    assert events(with_id)[0].xhash == events(moved)[0].xhash, "a level that moved is the same one"


# -- what a message says about the instrument --------------------------------


def test_the_instrument_is_read_and_flattened_onto_the_partition_column() -> None:
    order, fill = events(FILLED)
    instrument = order.into_instrument()
    assert instrument is not None
    assert instrument.symbol == "BTC-USD" and order.symbol == "BTC-USD"
    assert instrument.exchange == "XCME" and instrument.currency is Currency.USD
    assert order.instrument_xhash == instrument.xhash != 0
    assert fill.instrument_xhash == order.instrument_xhash


def test_the_price_unit_is_the_instruments_currency() -> None:
    order, _ = events(FILLED)
    assert order.px_unit == "USD" and order.ccy is Currency.USD


def test_fix_exchange_values_become_the_lossless_mic_code() -> None:
    order, fill = events(FILLED)
    assert order.mic is fill.mic is MIC.from_str("XCME")
    assert int(order.mic) == int.from_bytes(b"XCME", "big")


def test_fix_text_becomes_the_event_reason_and_session_ids_are_a_mic_fallback() -> None:
    (order,) = events("35=D|49=BUYSIDE|56=XPAR|11=CL-1|58=invalid price")
    assert order.reason == "invalid price"
    assert order.mic is MIC.from_str("XPAR")


def test_a_structured_reject_reason_fills_reason_when_text_is_absent() -> None:
    (order,) = events("35=8|11=CL-1|39=8|150=0|103=6|60=20260821-10:00:00")
    assert order.state is State.REJECTED
    assert order.reason is not None and order.reason.startswith("OrdRejReason=6: Duplicate Order")


def test_session_direction_does_not_split_one_order_lifecycle() -> None:
    sent = "35=D|49=BUYSIDE|56=XPAR|55=AAPL|11=CL-1|60=20260821-10:00:00"
    received = "35=8|49=XPAR|56=BUYSIDE|55=AAPL|11=CL-1|39=0|150=0|60=20260821-10:00:01"
    (requested,) = events(sent)
    (reported,) = events(received)
    assert requested.mic == reported.mic == MIC.from_str("XPAR")
    assert requested.mic is reported.mic is MIC.from_str("XPAR")
    assert requested.xhash == reported.xhash


def test_configured_mic_precedes_session_peer_fallbacks() -> None:
    (order,) = events(
        "35=D|49=BUY1|56=XPAR|55=AAPL|11=CL-1",
        venue="XCME",
        mic=MIC.from_str("XPAR"),
    )
    assert order.mic is MIC.from_str("XCME")


def test_the_asset_class_is_the_first_character_of_the_cfi_code() -> None:
    """ISO 10962's own category letter, which is what `AssetKind` is coded on."""
    (order,) = events("35=D|55=AAPL|11=C|461=ESVUFR|60=20260821-10:00:00")
    instrument = order.into_instrument()
    assert instrument is not None
    assert instrument.kind is AssetKind.EQUITY
    assert instrument.cfi == "ESVUFR"


def test_an_instrument_with_no_cfi_is_unknown_rather_than_guessed() -> None:
    (order,) = events("35=D|55=AAPL|11=C|60=20260821-10:00:00")
    instrument = order.into_instrument()
    assert instrument is not None and instrument.kind is AssetKind.UNKNOWN


def test_the_option_fields_are_read_where_a_venue_sends_them() -> None:
    (order,) = events(
        "35=D|55=AAPL|11=C|461=OCASPS|201=1|202=150.5|541=20261218|60=20260821-10:00:00"
    )
    instrument = order.into_instrument()
    assert instrument is not None
    assert instrument.option_kind is OptionKind.CALL
    assert instrument.strike == 150.5
    assert instrument.maturity == datetime.date(2026, 12, 18)


# -- what the shapes do not have a column for --------------------------------


def test_a_field_no_column_holds_is_kept_under_the_key_it_arrived_as() -> None:
    """Every venue sends fields no dictionary has; dropping them loses data."""
    order, _ = events(FILLED)
    assert order.metadata["9999"] == "venue-thing"


def test_a_field_a_column_already_holds_is_not_repeated_into_the_extras() -> None:
    order, fill = events(FILLED)
    for claimed in ("54", "44", "38", "31", "32", "55", "60", "52"):
        assert claimed not in order.metadata, claimed
        assert claimed not in fill.metadata, claimed


def test_each_event_owns_its_metadata_mapping() -> None:
    order, fill = events(FILLED)
    expected = dict(fill.metadata)

    assert order.metadata is not fill.metadata
    order.metadata["local"] = "order-only"
    assert fill.metadata == expected


def test_standard_state_keeps_the_original_field_specific_codes() -> None:
    order, fill = events(FILLED)
    assert order.metadata["39"] == fill.metadata["39"] == "1"
    assert order.metadata["150"] == fill.metadata["150"] == "F"
    assert order.metadata["40"] == fill.metadata["40"] == "2"
    assert FIX_STATES["OrdStatus"]["1"] is State.PARTIALLY_FILLED
    assert FIX_STATES["MDUpdateAction"]["1"] is State.OPEN


def test_standard_kind_keeps_each_many_to_one_order_type_spelling() -> None:
    market, with_or_without = (
        events(f"35=D|55=AAPL|11={code}|40={code}|60=20260821-10:00:00")[0] for code in ("5", "6")
    )
    assert market.kind is with_or_without.kind is MarketKind.MARKET_ORDER
    assert market.metadata["40"] == "5" and with_or_without.metadata["40"] == "6"


def test_the_frame_of_the_message_is_not_a_fact_about_the_market() -> None:
    """A byte count and a checksum are properties of the encoding, recomputed by
    anything that re-emits the message."""
    order, _ = events(FILLED)
    assert "9" not in order.metadata and "10" not in order.metadata
    assert order.metadata["8"] == "FIX.4.4", "but which protocol the venue speaks is real"


# -- the way in from pairs ---------------------------------------------------


def test_events_build_from_named_pairs_with_nothing_loaded() -> None:
    """The whole point of `market_tags`: no scrape, no network, no dictionary file."""
    (order,) = FixEvents.from_pairs(
        [
            ("MsgType", "D"),
            ("Symbol", "AAPL"),
            ("ClOrdID", "CL-1"),
            ("Side", Side.BUY),
            ("OrderQty", 100.0),
            ("Price", 10.5),
            ("TransactTime", datetime.datetime(2026, 8, 21, 10, 0, 0)),
            ("MyOwnField", "kept"),
        ],
        fix_version="4.4",
    )
    assert order.side is Side.BUY and order.px == 10.5 and order.qty == 100.0
    assert order.unix == unix_of("20260821-10:00:00")
    assert order.metadata["MyOwnField"] == "kept"


def test_pairs_and_a_wire_line_produce_the_same_events() -> None:
    """Two ways in, one result -- or a round trip through a log changes the data."""
    line = "35=D|55=AAPL|11=CL-1|54=1|38=100|44=10.5|60=20260821-10:00:00"
    from_line = events(line)[0]
    from_pairs = next(
        iter(
            FixEvents.from_pairs(
                [
                    ("MsgType", "D"),
                    ("Symbol", "AAPL"),
                    ("ClOrdID", "CL-1"),
                    ("Side", "1"),
                    ("OrderQty", "100"),
                    ("Price", "10.5"),
                    ("TransactTime", "20260821-10:00:00"),
                ],
                fix_version="4.4",
            )
        )
    )
    assert from_pairs.hash == from_line.hash and from_pairs.xhash == from_line.xhash


def test_an_offline_registry_selects_version_specific_wire_tags(tmp_path) -> None:
    """A custom version must change reads, not only named-pair preprocessing."""
    registry = FixRegistry(cache_dir=tmp_path / "fix", offline=True)
    fields = (
        ("MsgType", "String"),
        ("Symbol", "String"),
        ("ClOrdID", "String"),
        ("Side", "char"),
        ("OrderQty", "Qty"),
        ("Price", "Price"),
        ("TransactTime", "UTCTimestamp"),
        ("DeskCode", "String"),
    )
    for version, first_tag in (("VENUE1", 9001), ("VENUE2", 9101)):
        registry._store_fields(
            version,
            [
                fix_field(name, first_tag + offset, datatype, version=version)
                for offset, (name, datatype) in enumerate(fields)
            ],
        )

    line = (
        "8=FIX.VENUE1|9001=D|9002=AAPL|9003=CUSTOM-1|9004=1|9005=7|"
        "9006=10.5|9007=20260821-10:00:00|9008=ALPHA"
    )
    assert list(FixEvents.from_text(line)) == [], "standard tags cannot read this dialect"

    reader = FixEvents.from_text(line, registry=registry)
    (order,) = list(reader)
    assert reader.version == "VENUE1"
    assert (order.symbol, order.client_order_id, order.side) == ("AAPL", "CUSTOM-1", Side.BUY)
    assert (order.qty, order.px, order.unix) == (7.0, 10.5, unix_of("20260821-10:00:00"))
    assert "9004" not in order.metadata, "the configured Side tag is a claimed column"
    assert order.metadata["9008"] == "ALPHA", "a registry-only field remains auditable"


# -- the dictionary these shapes are their own ------------------------------


def test_the_tag_mapping_comes_from_the_declarations_rather_than_a_list() -> None:
    """Which is why it cannot drift: a column that gains a tag is in it."""
    tags = market_tags()
    assert tags["Side"] == 54 and tags["ExecType"] == 150 and tags["LastPx"] == 31
    assert tags["Symbol"] == 55, "a nested member of `instrument` counts too"
    assert set(CARRIED_FIELDS) <= set(tags)


def test_one_reading_of_a_dictionary_serves_every_message_that_uses_it() -> None:
    """Nothing on `MarketTags` is a fact about a message, so nothing is per message."""
    held = MarketTags.of(None, "4.4")
    assert MarketTags.of(None, "4.4") is held
    assert MarketTags.of(None, None) is not held
    assert held.names_by_tag["54"] == "Side"
    assert "54" in held.claimed, "a field with a column of its own is not metadata"
    assert "9" in held.claimed and "10" in held.claimed, "framing is not data"
    assert held.tags["Side"] == 54
    order = events(FILLED)[0]
    assert "54" not in order.metadata


def test_every_carried_tag_comes_from_the_builtin_registry() -> None:
    registry = FixRegistry.from_builtin()
    tags = market_tags()
    for name in CARRIED_FIELDS:
        assert tags[name] == int(registry.scalar(name).fix["tag"]), name


# -- messages that are not market events -------------------------------------


@pytest.mark.parametrize("line", ["35=0|52=20260821-10:00:00", "35=A|98=0|108=30", "35=5"])
def test_a_session_message_yields_nothing_rather_than_failing(line: str) -> None:
    """A feed is mostly made of them, so an empty iterator is the right answer."""
    assert events(line) == []


def test_a_fragment_with_no_msgtype_is_read_from_the_fields_it_has() -> None:
    """A decoder that only works on complete headers is no use on a log."""
    (order,) = list(
        FixEvents(
            message=FixPairs.from_pairs([("11", "CL-1"), ("54", "1")]),
            fix_version="4.4",
        )
    )
    assert isinstance(order, Order)
    reported = list(
        FixEvents(
            message=FixPairs.from_pairs([("17", "EX-1"), ("150", "F")]),
            fix_version="4.4",
        )
    )
    assert [type(one) for one in reported] == [Order, Execution], (
        "an execution report says the order's state as well, header or no header"
    )


def test_a_fragment_with_no_version_remains_raw() -> None:
    reader = FixEvents(message=FixPairs.from_pairs([("11", "CL-1"), ("54", "1")]))
    assert reader.version is None
    assert list(reader) == []
    assert list(reader.into_instruments()) == []


# -- the instrument an entry is about ----------------------------------------

#: A refresh whose header names the instrument fully -- an ISIN under
#: `NoSecurityAltID <454>` included -- and whose entries name only levels.
IDENTIFIED = (
    "8=FIX.4.4|35=X|49=XCME|52=20260821-10:30:00.000|55=BTC-USD|207=XCME|15=USD|"
    "48=US0378331005|22=4|454=1|455=US0378331005|456=4|"
    "268=2|"
    "279=0|269=0|270=100.5|271=3|278=L1|"
    "279=0|269=1|270=100.7|271=4|278=L2|10=001"
)


def test_an_entry_that_names_no_instrument_takes_the_headers() -> None:
    """An entry of a refresh describes a level, not another instrument -- so it
    is the header's instrument, alternative identifiers and legs included,
    which are read off the pairs and never reached an entry before."""
    reader = FixEvents.from_text(IDENTIFIED, venue="XCME")
    found = list(reader)
    assert len(found) == 2
    for one in found:
        instrument = one.into_instrument()
        assert instrument is reader.instrument, "one message, one instrument"
        assert instrument.alt_ids == {"ISIN": "US0378331005"}
        assert instrument.isin_code == "US0378331005"


def test_an_entry_that_names_its_own_instrument_gets_it() -> None:
    """A refresh may carry entries for several instruments, and then the entry
    is the one that says which."""
    # Without the header's ISIN, because a registered identifier outranks a
    # symbol and both entries would rightly land on the one instrument.
    line = IDENTIFIED.replace("48=US0378331005|22=4|454=1|455=US0378331005|456=4|", "")
    line = line.replace("269=1|270=100.7", "269=1|55=ETH-USD|270=100.7")
    found = list(FixEvents.from_text(line, venue="XCME"))
    instruments = [one.into_instrument() for one in found]
    assert all(instrument is not None for instrument in instruments)
    assert [instrument.symbol for instrument in instruments if instrument] == [
        "BTC-USD",
        "ETH-USD",
    ]
    assert instruments[0].xhash != instruments[1].xhash


def test_instrument_projection_reads_every_md_entry_without_building_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    line = IDENTIFIED.replace("48=US0378331005|22=4|454=1|455=US0378331005|456=4|", "")
    line = line.replace("269=1|270=100.7", "269=1|55=ETH-USD|270=100.7")

    def refused(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("reference extraction must not construct market events")

    monkeypatch.setattr(FixEvents, "into_entry_order", refused)
    found = list(FixEvents.from_text(line, venue="XCME").into_instruments())

    assert [instrument.symbol for instrument in found] == ["BTC-USD", "ETH-USD"]
    assert len({instrument.xhash for instrument in found}) == 2


def test_every_tag_the_instrument_reads_is_declared() -> None:
    """The inheritance guard covers every named instrument field it reads."""
    import inspect
    import re

    from rekep.market.fix import INSTRUMENT_FIELDS

    source = inspect.getsource(FixEvents.instrument.func)
    read = set(re.findall(r'\bget\("([A-Za-z0-9]+)"\)', source))
    assert read, "the reading is there to be read"
    assert read <= INSTRUMENT_FIELDS, f"undeclared: {sorted(read - INSTRUMENT_FIELDS)}"
    assert {"NoSecurityAltID", "NoLegs"} <= INSTRUMENT_FIELDS
