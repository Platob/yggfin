"""FIX messages read as market events: which timestamp, which shape, which fields.

The rules under test are the standard's own, so where one is not obvious the
test says which FIX definition it rests on.
"""

from __future__ import annotations

import datetime

import pytest

from rekep.fix import FixFieldValue, FixRegistry, fix_field, record_copy
from rekep.market import (
    MIC,
    NIL,
    AssetKind,
    Currency,
    Execution,
    Instrument,
    InstrumentUpdate,
    Leg,
    MarketKind,
    OptionKind,
    Order,
    Side,
    State,
    TimeInForce,
)
from rekep.market.fix import (
    CARRIED_FIELDS,
    TRANSACTED,
    FixEvents,
    MarketTags,
    market_tags,
    unix_of,
)
from rekep.text import FixMsg

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
    reader = FixEvents.from_text("35=D|52=19700101-00:00:00", recunix=123, fix_version="4.4")
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
    order, _ = events(FILLED, recunix=7)
    assert order.unix == unix_of("20260821-10:29:59.998")
    assert order.unix != unix_of("20260821-10:30:00.250")


def test_the_recording_clock_is_the_readers_and_stays_separate() -> None:
    order, _ = events(FILLED, recunix=1_787_308_200_400_000_000)
    assert order.recunix == 1_787_308_200_400_000_000
    assert order.recunix > order.unix, "recorded after it happened, which is the point"


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
    assert events("35=D|55=AAPL|11=CL-1", recunix=recorded)[0].unix == recorded


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
        "trdregtimestamps",
        "sidetrdregts",
    ], "the regulatory record leads, because it is the strongest claim in the message"
    assert [rung.fields for rung in TRANSACTED if not rung.is_column] == [
        ("transacttime",),
        ("mdentrydate", "mdentrytime"),
        ("origtime",),
        ("origsendingtime",),
        ("sendingtime",),
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
    assert reader(header, recunix=unix_of("20260821-10:30:00")).transacted.source == "recorded"
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
    assert fill.linkedhashes == [order.xhash]
    assert fill.parenthash == [order.hash]


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

    assert fill.vhash and fill.hash and calls == 1


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


def test_partial_fill_exectype_supplies_order_and_execution_states_without_ordstatus() -> None:
    report = FILLED.replace("|150=F|", "|150=1|").replace("39=1|", "")
    order, fill = events(report)

    assert order.state is State.PARTIALLY_FILLED
    assert fill.state is State.FILLED


@pytest.mark.parametrize(
    ("exec_type", "order_state", "execution_state"),
    [
        ("G", State.UNKNOWN, State.REPLACED),
        ("H", State.UNKNOWN, State.CANCELLED),
    ],
)
def test_trade_revision_exectype_supplies_both_states_without_ordstatus(
    exec_type: str, order_state: State, execution_state: State
) -> None:
    report = FILLED.replace("|150=F|", f"|150={exec_type}|").replace("39=1|", "")
    order, execution = events(report)

    assert order.state is order_state
    assert execution.state is execution_state


def test_a_trade_cancel_and_a_correct_read_as_what_they_do_to_the_fill() -> None:
    _, cancelled = events(FILLED.replace("|150=F|", "|150=H|"))
    _, corrected = events(FILLED.replace("|150=F|", "|150=G|"))
    assert cancelled.state is State.CANCELLED
    assert corrected.state is State.REPLACED


def test_a_trade_correction_keeps_its_execid_and_anchors_on_execrefid() -> None:
    report = FILLED.replace("|17=EX-3|", "|17=EX-4|19=EX-3|").replace("|150=F|", "|150=G|")
    _, corrected = events(report)

    assert corrected.execid == "EX-4" and corrected.execrefid == "EX-3"
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
    order, fill = events(FILLED)
    assert order.kind is MarketKind.LIMIT_ORDER and order.timeinforce is TimeInForce.GTC
    assert order.side is Side.BUY
    assert (order.qty, order.vwap, fill.vwap) == (6.0, None, 100.25)
    assert order.metadata["6"] == fill.metadata["6"] == "100.25"
    assert not hasattr(order, "cumqty") and not hasattr(order, "leavesqty")
    assert (order.orderid, order.clordid) == ("ORD-9", "CL-7")
    assert order.origclordid == "CL-6"


def test_avgpx_is_evidence_not_a_vwap_when_prior_fills_are_unknown() -> None:
    report = FILLED.replace("|32=4|", "|32=2|")
    order, fill = events(report)

    assert order.vwap is None and fill.vwap is None
    assert fill.cumqty == 4.0 and fill.qty == 2.0
    assert order.metadata["6"] == fill.metadata["6"] == "100.25"


def test_every_parsed_identifier_is_retained_in_isolated_altid_maps() -> None:
    report = FILLED.replace(
        "|17=EX-3|",
        "|17=EX-3|527=EX-SECONDARY|19=EX-2|1003=TR-1|278=MD-1|280=MD-0|",
    )
    order, execution = events(report)
    shared = {
        "orderid": "ORD-9",
        "origclordid": "CL-6",
        "clordid": "CL-7",
        "execid": "EX-3",
        "secondaryexecid": "EX-SECONDARY",
        "execrefid": "EX-2",
        "tradeid": "TR-1",
        "mdentryid": "MD-1",
        "mdentryrefid": "MD-0",
    }

    assert {name: order.altids[name] for name in shared} == shared
    assert {name: execution.altids[name] for name in shared} == shared
    assert "symbol" not in order.altids and "symbol" not in execution.altids
    assert order.altids is not execution.altids
    order.altids["local"] = "order-only"
    assert "local" not in execution.altids


def test_instrument_fields_never_identify_an_order_or_execution() -> None:
    (order,) = events("35=D|55=AAPL|48=US0378331005|22=4|54=1|38=1|44=100|60=20260821-10:00:00")
    (execution,) = events(
        "35=AE|55=AAPL|48=US0378331005|22=4|54=1|31=100|32=1|60=20260821-10:00:01"
    )

    assert order.instrumentxhash and execution.instrumentxhash
    assert order.code == execution.code == ""
    assert order.xhash == execution.xhash == NIL
    assert not ({"symbol", "securityid", "isincode"} & order.altids.keys())
    assert not ({"symbol", "securityid", "isincode"} & execution.altids.keys())


def test_a_rendered_identifier_missing_from_the_merged_index_is_still_retained() -> None:
    parsed = FixEvents.from_pairs([("MDEntryRefID", "MD-NAMED")], fix_version="4.4")

    assert parsed._identifier_altids["mdentryrefid"] == "MD-NAMED"


def test_order_qty_uses_explicit_or_derived_live_quantity() -> None:
    explicit, _ = events(FILLED)
    derived, _ = events(FILLED.replace("|151=6|", "|"))
    assert explicit.qty == derived.qty == 6.0


def test_an_omitted_time_in_force_is_day_as_fix_specifies() -> None:
    (order,) = events("35=D|55=AAPL|11=CL-1|54=1|38=100|44=10|60=20260821-10:00:00")
    assert order.timeinforce is TimeInForce.DAY and order.expunix is None


def test_a_price_the_venue_did_not_send_is_absent_and_not_zero() -> None:
    """A market order has no limit at all, and zero is a price."""
    (order,) = events("35=D|55=AAPL|11=CL-1|40=1|54=1|38=100|60=20260821-10:00:00")
    assert order.px is None and order.kind is MarketKind.MARKET_ORDER


def test_a_local_market_expiry_date_is_preserved_but_not_guessed_as_utc() -> None:
    """`ExpireDate <432>` is a day in a place the message never names.

    It reads as the instant the day begins -- every FIX temporal does, which
    is what keeps a zone applicable to it later -- and no zone is applied to
    it here, which is what `expunix` staying null says: the package does not
    know what midnight local means and will not guess.
    """
    (order,) = events("35=D|55=AAPL|11=CL-1|59=6|432=20260821|60=20260821-10:00:00")
    assert order.expunix is None
    assert order.metadata["432"] == "20260821-00:00:00.000000"


def test_an_explicit_expiry_time_wins_over_the_day() -> None:
    (order,) = events(
        "35=D|55=AAPL|11=CL-1|59=6|432=20260821|126=20260821-16:30:00|60=20260821-10:00:00"
    )
    assert order.expunix == unix_of("20260821-16:30:00")


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
    assert order.timeinforce is TimeInForce.GFT
    assert order.expunix == order.unix + 2 * factor
    assert order.metadata["1629"] == "2"
    if unit is not None:
        assert order.metadata["1916"] == unit


@pytest.mark.parametrize(
    ("duration", "unit"), [("0", "0"), ("-1", "0"), ("2", "14"), ("2", "15"), ("2", "99")]
)
def test_good_for_time_refuses_non_positive_or_calendar_durations(duration: str, unit: str) -> None:
    (order,) = events(f"35=D|55=AAPL|11=CL-1|59=A|1629={duration}|1916={unit}|60=20260821-10:00:00")
    assert order.expunix is None
    assert order.metadata["1629"] == duration
    assert order.metadata["1916"] == unit


def test_expire_time_wins_over_good_for_time() -> None:
    (order,) = events(
        "35=D|55=AAPL|11=CL-1|59=A|1629=1|1916=13|126=20260821-10:00:01|60=20260821-10:00:00"
    )
    assert order.expunix == unix_of("20260821-10:00:01")


@pytest.mark.parametrize("fix", ["3", "4"], ids=["ioc", "fok"])
def test_immediate_time_in_force_expires_on_arrival(fix: str) -> None:
    (order,) = events(f"35=D|55=AAPL|11=CL-1|54=1|38=100|44=10|59={fix}|60=20260821-10:00:00")
    assert order.expunix == order.unix == unix_of("20260821-10:00:00")


@pytest.mark.parametrize("fix", ["0", "2", "5", "7", "8", "9", "B", "C"])
def test_calendar_dependent_time_in_force_has_no_invented_instant(fix: str) -> None:
    (order,) = events(f"35=D|55=AAPL|11=CL-1|54=1|38=100|44=10|59={fix}|60=20260821-10:00:00")
    assert order.expunix is None


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


def test_instrument_update_keeps_only_declared_reference_facts() -> None:
    line = (
        "8=FIX.4.4|35=8|37=ORD-9|11=CL-7|55=AAPL|"
        "454=1|455=US0378331005|456=4|60=20260821-10:00:00|10=000"
    )
    update = next(InstrumentUpdate.from_fixmsgs([FixMsg.from_text(line)]))
    assert update.instrument.isincode == "US0378331005"
    assert update.altids == {}
    assert "altids" not in Instrument.into_field().names


def test_the_instrument_is_read_and_flattened_onto_the_partition_column() -> None:
    order, fill = events(FILLED)
    instrument = order.into_instrument()
    assert instrument is not None
    assert instrument.symbol == "BTC-USD"
    assert order.symbolticker == "XCME:BTC-USD"
    assert instrument.securityexchange == "XCME" and instrument.currency is Currency.USD
    assert order.instrumentxhash == instrument.xhash != 0
    assert fill.instrumentxhash == order.instrumentxhash


def test_the_price_unit_is_the_instruments_currency() -> None:
    order, _ = events(FILLED)
    assert order.pxunit == "USD" and order.currency is Currency.USD


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
    assert instrument.cficode == "ESVUFR"


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
    assert instrument.putorcall is OptionKind.CALL
    assert instrument.strikeprice == 150.5
    assert instrument.maturitydate == datetime.date(2026, 12, 18)


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
    registry = FixRegistry.from_builtin()
    assert registry.state_values("OrdStatus")["1"] is State.PARTIALLY_FILLED
    assert registry.state_values("MDUpdateAction")["1"] is State.OPEN


@pytest.mark.parametrize(
    "field,expected",
    [
        (
            "OrdStatus",
            {
                "0": State.NEW,
                "1": State.PARTIALLY_FILLED,
                "2": State.FILLED,
                "3": State.DONE_FOR_DAY,
                "4": State.CANCELLED,
                "5": State.REPLACED,
                "6": State.PENDING_CANCEL,
                "7": State.STOPPED,
                "8": State.REJECTED,
                "9": State.SUSPENDED,
                "A": State.PENDING_NEW,
                "B": State.CALCULATED,
                "C": State.EXPIRED,
                "D": State.ACCEPTED,
                "E": State.PENDING_REPLACE,
            },
        ),
        (
            "ExecType",
            {
                "0": State.NEW,
                "1": State.PARTIALLY_FILLED,
                "2": State.FILLED,
                "3": State.DONE_FOR_DAY,
                "4": State.CANCELLED,
                "5": State.REPLACED,
                "6": State.PENDING_CANCEL,
                "7": State.STOPPED,
                "8": State.REJECTED,
                "9": State.SUSPENDED,
                "A": State.PENDING_NEW,
                "B": State.CALCULATED,
                "C": State.EXPIRED,
                "E": State.PENDING_REPLACE,
                "F": State.PARTIALLY_FILLED,
                "G": State.REPLACED,
                "H": State.CANCELLED,
            },
        ),
        (
            "MDUpdateAction",
            {
                "0": State.NEW,
                "1": State.OPEN,
                "2": State.CANCELLED,
                "3": State.CANCELLED,
                "4": State.CANCELLED,
                "5": State.OPEN,
            },
        ),
        (
            "QuoteStatus",
            {
                "0": State.ACCEPTED,
                "1": State.CANCELLED,
                "2": State.CANCELLED,
                "3": State.CANCELLED,
                "4": State.CANCELLED,
                "5": State.REJECTED,
                "6": State.CANCELLED,
                "7": State.EXPIRED,
                "9": State.REJECTED,
                "10": State.PENDING,
                "11": State.CANCELLED,
                "12": State.OPEN,
                "13": State.OPEN,
                "14": State.CANCELLED,
                "15": State.CANCELLED,
                "16": State.OPEN,
                "17": State.CANCELLED,
                "18": State.OPEN,
                "19": State.PENDING_CANCEL,
                "21": State.FILLED,
                "22": State.FILLED,
                "23": State.EXPIRED,
            },
        ),
        (
            "QuoteRespType",
            {
                "1": State.FILLED,
                "2": State.OPEN,
                "3": State.EXPIRED,
                "4": State.OPEN,
                "5": State.CANCELLED,
                "6": State.CANCELLED,
                "7": State.CANCELLED,
                "8": State.CANCELLED,
                "9": State.OPEN,
                "10": State.OPEN,
                "11": State.ACCEPTED,
                "12": State.CANCELLED,
            },
        ),
    ],
)
def test_every_builtin_fix_state_is_registry_configuration(
    field: str, expected: dict[str, State]
) -> None:
    assert FixRegistry.from_builtin().state_values(field) == expected


def test_every_state_conversion_uses_the_same_registry_configuration() -> None:
    registry = FixRegistry.from_builtin()

    assert registry.state_values("MsgType") == {
        "D": State.PENDING_NEW,
        "F": State.PENDING_CANCEL,
        "G": State.PENDING_REPLACE,
        "9": State.UNKNOWN,
    }
    assert registry.state_values("ExecType")["1"] is State.PARTIALLY_FILLED
    assert registry.state_values("ExecType")["G"] is State.REPLACED


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
    registry = FixRegistry(cache_dir=tmp_path / "fix")
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
    assert (order.symbolticker, order.clordid, order.side) == ("AAPL", "CUSTOM-1", Side.BUY)
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


def test_dictionary_value_names_expand_the_pinned_market_maps() -> None:
    tags = MarketTags.of()
    assert {name: len(values) for name, values in tags.states.items()} == {
        "OrdStatus": 34,
        "ExecType": 40,
        "MDUpdateAction": 12,
        "QuoteStatus": 47,
        "QuoteRespType": 25,
    }
    assert len(tags.execution_states) == 13
    assert len(tags.order_kinds) == 65
    assert len(tags.execution_kinds) == 49
    assert len(tags.exec_type_fallbacks) == 36


def _restated(record, states):
    """One record with different lifecycle states, holding nothing else's."""
    restated = record_copy(record)
    restated.fix.states = states
    return restated


def test_a_registry_mutation_refreshes_its_market_reading(tmp_path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    entry = FixRegistry.from_builtin().field("OrdStatus")
    registry.add_field(_restated(entry, {"0": State.NEW}))
    first = MarketTags.of(registry)
    first_state = first.states["OrdStatus"]["0"]

    registry.update_field(_restated(entry, {"0": State.CANCELLED}))
    second = MarketTags.of(registry)

    assert second is not first
    assert first_state is State.NEW
    assert second.states["OrdStatus"]["0"] is State.CANCELLED


def test_a_sparse_registry_keeps_builtin_trade_exectypes(tmp_path) -> None:
    tags = MarketTags.of(FixRegistry(cache_dir=tmp_path / "fix"), "4.4")

    assert tags.execution_states["G"] is State.REPLACED
    assert tags.execution_states["H"] is State.CANCELLED
    assert "G" not in tags.exec_type_fallbacks
    assert "H" not in tags.exec_type_fallbacks


def test_configured_trade_encodings_create_only_execution_fallbacks(tmp_path) -> None:
    registry = FixRegistry(cache_dir=tmp_path / "fix")
    entry = FixRegistry.from_builtin().field("ExecType")
    assert entry is not None
    configured = record_copy(entry)
    configured.fix.enumerated = [
        FixFieldValue(value="T", meaning="Trade Correct"),
        FixFieldValue(value="U", meaning="Trade Bust"),
    ]
    configured.fix.states = {"T": State.REPLACED, "U": State.REJECTED}
    registry.add_field(configured)

    tags = MarketTags.of(registry, "4.4")
    assert tags.execution_states["G"] is State.REPLACED
    assert tags.execution_states["H"] is State.CANCELLED
    assert tags.execution_states["T"] is State.REPLACED
    assert tags.execution_states["U"] is State.REJECTED
    assert "T" not in tags.exec_type_fallbacks
    assert "U" not in tags.exec_type_fallbacks


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
            message=FixMsg.from_pairs([("11", "CL-1"), ("54", "1")]),
            fix_version="4.4",
        )
    )
    assert isinstance(order, Order)
    reported = list(
        FixEvents(
            message=FixMsg.from_pairs([("17", "EX-1"), ("150", "F")]),
            fix_version="4.4",
        )
    )
    assert [type(one) for one in reported] == [Order, Execution], (
        "an execution report says the order's state as well, header or no header"
    )


def test_a_fragment_with_no_version_remains_raw() -> None:
    reader = FixEvents(message=FixMsg.from_pairs([("11", "CL-1"), ("54", "1")]))
    assert reader.version is None
    assert list(reader) == []
    assert list(InstrumentUpdate.from_fixmsgs([reader.message])) == []


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
    header = next(InstrumentUpdate.from_fixmsgs([reader.message])).instrument
    assert len(found) == 2
    for one in found:
        instrument = one.into_instrument()
        assert instrument == header, "one message, one instrument component"
        assert instrument.isincode == "US0378331005"


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

    monkeypatch.setattr(FixEvents, "_event", refused)
    found = [
        update.instrument for update in InstrumentUpdate.from_fixmsgs([FixMsg.from_text(line)])
    ]

    assert [instrument.symbol for instrument in found] == ["BTC-USD", "ETH-USD"]
    assert len({instrument.xhash for instrument in found}) == 2


def test_resolved_component_columns_feed_alt_ids_and_legs() -> None:
    """A parsed row's typed `SecurityAltID`/`Legs` answer without a pair walk.

    The parse stage lifts both groups out of `entries`, so a stored row has no
    count tag left to re-parse -- the resolved columns are the only place the
    entries live, and the instrument they build must equal the one the same
    wire line builds through the scalar fallback.
    """
    from rekep.fix.components import SecurityAltID as SecurityAltIDEntry

    stored = FixMsg(
        beginstring="FIX.4.4",
        msgtype="d",
        protocolversion="4.4",
        securityaltid=[
            SecurityAltIDEntry(securityaltid="US0378331005", securityaltidsource="4"),
            SecurityAltIDEntry(securityaltid="037833100", securityaltidsource="1"),
        ],
        instrument=Instrument(
            symbol="SPREAD",
            isincode="US0378331005",
            legs=[
                Leg(
                    symbol="AAPL",
                    side=Side.BUY,
                    ratio=1.0,
                    maturitydate=datetime.date(2027, 1, 15),
                    strikeprice=150.5,
                ),
                Leg(symbol="MSFT", side=Side.SELL, ratio=2.0, currency="USD"),
            ],
        ),
    )
    instrument = next(InstrumentUpdate.from_fixmsgs([stored])).instrument

    assert instrument.isincode == "US0378331005"
    assert [(leg.symbol, leg.side, leg.ratio) for leg in instrument.legs] == [
        ("AAPL", Side.BUY, 1.0),
        ("MSFT", Side.SELL, 2.0),
    ]
    assert instrument.legs[0].maturitydate == datetime.date(2027, 1, 15)
    assert instrument.legs[0].strikeprice == 150.5
    assert instrument.legs[1].currency == Currency.USD

    wire = (
        "8=FIX.4.4|35=d|55=SPREAD|454=2|455=US0378331005|456=4|455=037833100|456=1|"
        "555=2|600=AAPL|624=1|623=1|611=20270115|612=150.5|"
        "600=MSFT|624=2|623=2|556=USD"
    )
    assert instrument == next(InstrumentUpdate.from_fixmsgs([FixMsg.from_text(wire)])).instrument


def test_a_two_sided_trade_capture_report_is_one_execution_per_side() -> None:
    """`Side <54>`, `OrderID <37>`, `ClOrdID <11>` live inside each `NoSides
    <552>` entry, so a flat read kept one side's identity and dropped the
    other's. Each side resolves the report-level facts through the merged
    view -- the match id, the price, the quantity and the clock are the
    report's -- and its own identifiers over them."""
    wire = (
        "8=FIX.4.4|35=AE|571=RPT-1|880=M-1|31=99.5|32=7|75=20260814|"
        "55=EUR/USD|60=20260814-09:30:00|552=2|"
        "54=1|37=O-BUY|11=C-BUY|1=ACC-B|"
        "54=2|37=O-SELL|11=C-SELL|1=ACC-S|10=000"
    )
    events = list(FixEvents.from_text(wire))

    assert [type(one) for one in events] == [Execution, Execution]
    assert [(one.side, one.orderid, one.clordid) for one in events] == [
        (Side.BUY, "O-BUY", "C-BUY"),
        (Side.SELL, "O-SELL", "C-SELL"),
    ]
    assert {one.tradeid for one in events} == {"M-1"}
    assert {(one.px, one.qty, one.unix) for one in events} == {(99.5, 7.0, events[0].unix)}
    assert events[0].xhash != events[1].xhash, "two sides, two lifecycles"


def test_a_single_sided_or_flat_trade_capture_report_stays_one_execution() -> None:
    single = (
        "8=FIX.4.4|35=AE|571=R2|880=M-2|31=101|32=3|55=AAPL|60=20260814-10:00:00|"
        "552=1|54=1|37=O-9|11=C-9|10=000"
    )
    (execution,) = list(FixEvents.from_text(single))
    assert (execution.side, execution.orderid, execution.px) == (Side.BUY, "O-9", 101.0)

    flat = (
        "8=FIX.4.4|35=AE|571=R3|880=M-3|31=50|32=1|55=MSFT|54=2|37=O-3|60=20260814-11:00:00|10=000"
    )
    (execution,) = list(FixEvents.from_text(flat))
    assert (execution.side, execution.orderid) == (Side.SELL, "O-3")


def test_a_rendered_indexed_report_splits_sides_the_same_way() -> None:
    """The bridge's `NOSIDES[i]=...` spelling reaches the same two executions,
    with report-level legs untouched by the split."""
    member = "\x04\x03"
    line = (
        "toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=AE|#TRADEREPORTID=RPT-9|#TRDMATCHID=M-9|"
        "#LASTPX=1.0842|#LASTQTY=1000000|#SYMBOL=EUR/USD|#TRANSACTTIME=20260814-09:30:00|"
        f"#NOLEGS=2|#NOLEGS[0]=LEGSYMBOL=EUR/USD-NEAR{member}LEGSIDE=1{member}LEGRATIOQTY=1{member}|"
        f"#NOLEGS[1]=LEGSYMBOL=EUR/USD-FAR{member}LEGSIDE=2{member}LEGRATIOQTY=1{member}|"
        f"#NOSIDES=2|#NOSIDES[0]=SIDE=1{member}ORDERID=O-BUY{member}CLORDID=C-BUY{member}|"
        f"#NOSIDES[1]=SIDE=2{member}ORDERID=O-SELL{member}CLORDID=C-SELL{member}"
    )
    events = list(FixMsg.from_text(line).into_fix_events(fix_version="4.4"))

    assert [(one.side, one.orderid) for one in events] == [
        (Side.BUY, "O-BUY"),
        (Side.SELL, "O-SELL"),
    ]
    assert {one.tradeid for one in events} == {"M-9"}
    instrument = next(InstrumentUpdate.from_fixmsgs([FixMsg.from_text(line)])).instrument
    assert [leg.symbol for leg in instrument.legs] == ["EUR/USD-NEAR", "EUR/USD-FAR"]


def test_a_cancel_reject_reads_where_the_order_stands_from_ordstatus() -> None:
    """`OrderCancelReject <9>` is the one order message whose real state lives
    in `OrdStatus <39>` -- it says where the order stands after the refusal --
    and its coded reason leads the prose instead of being dropped by it."""
    line = (
        "8=FIX.4.4|35=9|37=O-1|11=C-2|41=C-1|39=2|102=1|434=1|"
        "58=Too late to cancel|60=20260814-10:00:00|10=000"
    )
    (order,) = list(FixEvents.from_text(line))

    assert order.state is State.FILLED
    assert order.cxlrejreason == 1
    assert order.cxlrejresponseto == "1"
    assert order.reason is not None
    coded, response, text = order.reason.split("; ")
    assert coded.startswith("CxlRejReason=1")
    assert response.startswith("CxlRejResponseTo=1")
    assert text == "Too late to cancel"


def test_a_cancel_reject_without_ordstatus_stays_unknown() -> None:
    line = "8=FIX.4.4|35=9|37=O-1|11=C-2|41=C-1|60=20260814-10:00:00|10=000"
    (order,) = list(FixEvents.from_text(line))
    assert order.state is State.UNKNOWN


def test_a_quote_request_with_prices_is_read_like_any_other_quote() -> None:
    """`QuoteRequest <R>` used to dispatch to nothing; one carrying prices is
    two indicative sides, exactly as a quote would be."""
    line = (
        "8=FIX.4.4|35=R|131=QR-1|55=EUR/USD|132=1.08|134=1000000|"
        "133=1.081|135=2000000|60=20260814-10:00:00|10=000"
    )
    bid, ask = list(FixEvents.from_text(line))
    assert (bid.side, bid.px, bid.qty) == (Side.BID, 1.08, 1000000.0)
    assert (ask.side, ask.px, ask.qty) == (Side.ASK, 1.081, 2000000.0)


def test_a_mass_quote_acknowledgement_reads_its_entries() -> None:
    """`MassQuoteAcknowledgement <b>` carries the same quote sets `i` does,
    and a rejecting acknowledgement is what says the quote never stood."""
    line = (
        "8=FIX.4.4|35=b|117=Q-1|297=5|296=1|302=S1|295=1|299=E1|55=AAA|"
        "132=9|134=5|60=20260814-10:00:00|10=000"
    )
    (entry,) = list(FixEvents.from_text(line))
    assert entry.state is State.REJECTED
    # Rejected is terminal, so the working quantity is zeroed and the asked
    # size survives as the previous one.
    assert (entry.side, entry.px, entry.qty, entry.prevqty) == (Side.BID, 9.0, 0.0, 5.0)


def test_a_trade_capture_report_request_carrying_a_trade_is_an_execution() -> None:
    line = (
        "8=FIX.4.4|35=AD|568=REQ-1|571=R-1|880=M-1|31=100|32=5|55=AAPL|54=1|"
        "37=O-1|60=20260814-10:00:00|10=000"
    )
    (execution,) = list(FixEvents.from_text(line))
    assert isinstance(execution, Execution)
    assert (execution.side, execution.px, execution.qty, execution.tradeid) == (
        Side.BUY,
        100.0,
        5.0,
        "M-1",
    )


def test_an_execution_carries_how_its_trade_settles() -> None:
    line = (
        "8=FIX.4.4|35=AE|571=R-2|880=M-2|31=1.0842|32=1000000|55=EUR/USD|"
        "64=20260818|63=W2|120=USD|156=M|552=1|54=1|37=O-2|11=C-7|"
        "60=20260814-10:00:00|10=000"
    )
    (execution,) = list(FixEvents.from_text(line))
    assert execution.settldate == datetime.date(2026, 8, 18)
    assert execution.settltype == "W2"
    assert execution.settlcurrency == "USD"
    assert execution.settlcurrfxratecalc == "M"


def test_the_order_intent_link_identifier_is_typed_and_coded() -> None:
    line = "8=FIX.4.4|35=D|11=C-1|583=LINK-9|55=AAPL|54=1|38=5|44=10|60=20260814-10:00:00|10=000"
    (order,) = list(FixEvents.from_text(line))
    assert order.clordlinkid == "LINK-9"
    assert order.altids["clordlinkid"] == "LINK-9"


def test_the_parent_identities_a_bridge_renders_reach_the_order() -> None:
    """`ParentClOrdID`/`ParentOrderID` are registry namespace fields -- FIX
    never numbered them -- and a rendered line's spellings resolve through
    the same records the parsed column is lifted with."""
    line = (
        "toBridge #BEGINSTRING=FIX.4.4|#MSGTYPE=D|#CLORDID=C-2|#PARENTCLORDID=P-1|"
        "#PARENTORDERID=V-9|#SYMBOL=AAPL|#SIDE=1|#ORDERQTY=5|#PRICE=10|"
        "#TRANSACTTIME=20260814-10:00:00"
    )
    (order,) = list(FixEvents.from_text(line))
    assert order.parentclordid == "P-1"
    assert order.parentorderid == "V-9"
    assert order.clordid == "C-2"
    assert "PARENTCLORDID" not in order.metadata, "a field with a column is not metadata"
    assert "PARENTORDERID" not in order.metadata, "under the bridge's spelling either"


def test_word_spelled_ul_values_read_like_their_wire_codes() -> None:
    """`SIDE=buy`, `ORDSTATUS=canceled`, `EXECTYPE=cancel`, `ORDTYPE=limit`
    and `TIMEINFORCE=gtd` are how real bridges render the codes; a scalar
    reader resolves each where it used to record `UNKNOWN`."""
    line = (
        "8=FIX.4.4|35=8|37=O-1|11=C-1|17=E-1|54=buy|39=canceled|150=cancel|"
        "40=limit|59=gtd|38=5|44=10|126=20260820-17:30:00|60=20260814-10:00:00|10=000"
    )
    (order,) = list(FixEvents.from_text(line))
    assert order.state is State.CANCELLED
    assert order.side is Side.BUY
    assert order.kind is MarketKind.LIMIT_ORDER
    assert order.timeinforce is TimeInForce.GTD
    assert order.expunix is not None, "a GTD spelled out still reads its expiry"


def test_a_side_never_answers_with_its_siblings_fields() -> None:
    """The report level falls through to every side; a field one side carried
    alone does not -- the whole-message first-occurrence view would have
    handed side one's client id and account to a side two that sent neither."""
    wire = (
        "8=FIX.4.4|35=AE|571=R-1|880=M-1|31=99.5|32=7|55=EUR/USD|60=20260814-09:30:00|"
        "552=2|54=1|37=O-BUY|11=C-BUY|1=ACC-B|54=2|37=O-SELL|10=000"
    )
    first, second = list(FixEvents.from_text(wire))

    assert (first.clordid, second.clordid) == ("C-BUY", None)
    assert "1" not in (second.metadata or {}).values(), "no borrowed account either"
    assert {one.tradeid for one in (first, second)} == {"M-1"}, (
        "the report level still falls through"
    )
    assert {one.px for one in (first, second)} == {99.5}


def test_a_side_with_nested_parties_does_not_truncate_the_sides_after_it() -> None:
    """A side regularly nests a multi-entry `NoPartyIDs`, whose repeated tags
    would end a first-repeat scan in the middle of side one."""
    wire = (
        "8=FIX.4.4|35=AE|571=R-2|880=M-2|31=100|32=5|55=AAPL|60=20260814-09:30:00|"
        "552=2|54=1|37=O-BUY|453=2|448=DESK-A|447=D|452=1|448=XPAR|447=G|452=17|"
        "54=2|37=O-SELL|10=000"
    )
    events = list(FixEvents.from_text(wire))

    assert [(one.side, one.orderid) for one in events] == [
        (Side.BUY, "O-BUY"),
        (Side.SELL, "O-SELL"),
    ]


def test_a_side_without_its_own_clock_keeps_the_reports_resolution() -> None:
    """A report-level regulatory stamp outranks `TransactTime <60>` on the
    report, and each side that carries no clock of its own keeps that answer
    rather than re-resolving from the entry's weaker fields."""
    wire = (
        "8=FIX.4.4|35=AE|571=R-3|880=M-3|31=100|32=5|55=AAPL|"
        "768=1|769=20260814-09:29:58|770=1|60=20260814-09:30:00|"
        "552=2|54=1|37=O-BUY|54=2|37=O-SELL|10=000"
    )
    outer = FixEvents.from_text(wire)
    events = list(outer)

    assert {one.unix for one in events} == {outer.transacted.unix}
    assert outer.transacted.source.startswith("TrdRegTimestamps")


def test_a_pure_trade_report_query_fabricates_no_execution() -> None:
    """`TradeCaptureReportRequest <AD>` decodes like the report it echoes --
    but a request carrying only criteria is a question, not a trade."""
    query = "8=FIX.4.4|35=AD|568=REQ-7|569=0|263=1|60=20260814-10:00:00|10=000"
    assert list(FixEvents.from_text(query)) == []

    echoed = (
        "8=FIX.4.4|35=AD|568=REQ-8|571=R-1|880=M-1|31=100|32=5|55=AAPL|54=1|"
        "60=20260814-10:00:00|10=000"
    )
    (execution,) = list(FixEvents.from_text(echoed))
    assert (execution.tradeid, execution.px) == ("M-1", 100.0)


def test_a_side_with_its_own_regulatory_stamp_keeps_it() -> None:
    """The report's clock steers only a side with no clock of its own: a side
    carrying `SideTrdRegTS` resolves its own instant, above the report's."""
    wire = (
        "8=FIX.4.4|35=AE|571=R-4|880=M-4|31=100|32=5|55=AAPL|"
        "768=1|769=20260814-09:29:58|770=1|60=20260814-09:30:00|"
        "552=2|54=1|37=O-BUY|"
        "54=2|37=O-SELL|1016=1|1012=20260814-09:29:59|1013=1|10=000"
    )
    first, second = list(FixEvents.from_text(wire))

    assert first.unix == unix_of("20260814-09:29:58"), "no clock of its own, the report's"
    assert second.unix == unix_of("20260814-09:29:59"), "its own stamp, not the report's"
