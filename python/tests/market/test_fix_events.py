"""FIX messages read as market events: which timestamp, which shape, which fields.

The rules under test are the standard's own, so where one is not obvious the
test says which FIX definition it rests on.
"""

from __future__ import annotations

import datetime

import pytest

from rekep.fix import FixMessage
from rekep.market import (
    AssetKind,
    ExecKind,
    Execution,
    OptionKind,
    Order,
    OrderKind,
    Side,
    State,
    TimeInForce,
)
from rekep.market.fix import CARRIED_TAGS, TRANSACTED, FixEvents, market_tags, unix_of

#: One filled ExecutionReport, spelled the way a log prints one.
FILLED = (
    "8=FIX.4.4|9=289|35=8|49=XCME|56=ACME|34=1090|52=20260821-10:30:00.250|"
    "37=ORD-9|11=CL-7|41=CL-6|17=EX-3|150=F|39=1|55=BTC-USD|207=XCME|15=USD|"
    "54=1|38=10|44=100.5|40=2|59=1|32=4|31=100.25|14=4|151=6|6=100.25|"
    "60=20260821-10:29:59.998|1057=Y|9999=venue-thing|10=123"
)


def events(line: str, **carried: object) -> list[object]:
    return list(FixEvents.from_text(line, **carried))


# -- when it happened --------------------------------------------------------


def test_a_timestamp_is_read_as_whole_nanoseconds_since_the_epoch() -> None:
    assert unix_of("20260821-10:30:00.123456789") == 1787308200123456789
    assert unix_of("20260821-10:30:00") == 1787308200000000000


def test_a_fraction_is_scaled_by_its_own_width() -> None:
    """`.5` is half a second and `.000000001` is a nanosecond; both are decimals."""
    assert unix_of("20260821-00:00:00.5") - unix_of("20260821") == 500_000_000
    assert unix_of("20260821-00:00:00.000000001") - unix_of("20260821") == 1


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


def test_the_declared_order_is_the_one_the_reader_uses() -> None:
    """Pinned, because the rule *is* the constant: a reordering here is a silent
    change of which clock every downstream row is stamped with."""
    assert TRANSACTED == (60, (272, 273), 42, 122, 52)


# -- an execution report is two events ---------------------------------------


def test_a_filled_report_is_both_the_order_and_the_fill() -> None:
    """FIX uses one message for "your order is now partially filled" and "here is
    the fill that did it"; they are two rows, and storing one loses the other."""
    order, fill = events(FILLED)
    assert isinstance(order, Order) and isinstance(fill, Execution)


def test_the_order_carries_what_was_asked_and_the_fill_what_moved() -> None:
    """`Price <44>`/`OrderQty <38>` against `LastPx <31>`/`LastQty <32>`."""
    order, fill = events(FILLED)
    assert (order.px, order.qty) == (100.5, 10.0)
    assert (fill.px, fill.qty) == (100.25, 4.0)


def test_the_order_comes_first_because_the_fill_points_at_it() -> None:
    order, fill = events(FILLED)
    assert fill.order_xhash == order.xhash
    assert fill.parent_hash == [order.hash]


def test_an_acknowledgement_produces_no_execution_row() -> None:
    """`ExecType <150>` `0` is New: nothing traded, and summing acks as fills is
    exactly the mistake the `kind` band exists to prevent."""
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
    assert fill.kind is ExecKind.TRADED and fill.state is State.FILLED
    assert fill.state is not State.PARTIALLY_FILLED


def test_a_trade_cancel_and_a_correct_read_as_what_they_do_to_the_fill() -> None:
    _, cancelled = events(FILLED.replace("|150=F|", "|150=H|"))
    _, corrected = events(FILLED.replace("|150=F|", "|150=G|"))
    assert cancelled.state is State.CANCELLED
    assert corrected.state is State.REPLACED


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
    assert order.kind is OrderKind.LIMIT_ORDER and order.tif is TimeInForce.GTC
    assert order.side is Side.BUY
    assert (order.filled_qty, order.leaves_qty, order.avg_px) == (4.0, 6.0, 100.25)
    assert (order.order_id, order.client_order_id) == ("ORD-9", "CL-7")
    assert order.prev_client_order_id == "CL-6"


def test_a_price_the_venue_did_not_send_is_absent_and_not_zero() -> None:
    """A market order has no limit at all, and zero is a price."""
    (order,) = events("35=D|55=AAPL|11=CL-1|40=1|54=1|38=100|60=20260821-10:00:00")
    assert order.px is None and order.kind is OrderKind.MARKET_ORDER


def test_an_expiry_given_only_as_a_day_lasts_through_that_day() -> None:
    """A GTD order dated today is good *through* today; midnight would retire it
    before it ever traded."""
    (order,) = events("35=D|55=AAPL|11=CL-1|59=6|432=20260821|60=20260821-10:00:00")
    assert order.eunix == unix_of("20260822") - 1


def test_an_explicit_expiry_time_wins_over_the_day() -> None:
    (order,) = events(
        "35=D|55=AAPL|11=CL-1|59=6|432=20260821|126=20260821-16:30:00|60=20260821-10:00:00"
    )
    assert order.eunix == unix_of("20260821-16:30:00")


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


def test_a_snapshot_entry_with_no_action_is_present_by_definition() -> None:
    """Which is what a full refresh <W> means, and why it carries no action."""
    (level,) = events("35=W|55=AAPL|268=1|269=0|270=10.0|271=100|52=20260821-10:00:00")
    assert level.state is State.NEW


def test_a_trade_entry_is_an_execution_and_not_a_resting_order() -> None:
    *_, printed = events(REFRESH)
    assert isinstance(printed, Execution)
    assert printed.kind is ExecKind.TRADED and printed.state is State.FILLED
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
    assert order.instrument.symbol == "BTC-USD" and order.symbol == "BTC-USD"
    assert order.instrument.exchange == "XCME" and order.instrument.currency == "USD"
    assert order.instrument_hash == order.instrument.xhash != 0
    assert fill.instrument_hash == order.instrument_hash


def test_the_price_unit_is_the_instruments_currency() -> None:
    order, _ = events(FILLED)
    assert order.px_unit == "USD"


def test_the_asset_class_is_the_first_character_of_the_cfi_code() -> None:
    """ISO 10962's own category letter, which is what `AssetKind` is coded on."""
    (order,) = events("35=D|55=AAPL|11=C|461=ESVUFR|60=20260821-10:00:00")
    assert order.instrument.kind is AssetKind.EQUITY
    assert order.instrument.cfi == "ESVUFR"


def test_an_instrument_with_no_cfi_is_unknown_rather_than_guessed() -> None:
    (order,) = events("35=D|55=AAPL|11=C|60=20260821-10:00:00")
    assert order.instrument.kind is AssetKind.UNKNOWN


def test_the_option_fields_are_read_where_a_venue_sends_them() -> None:
    (order,) = events(
        "35=D|55=AAPL|11=C|461=OCASPS|201=1|202=150.5|541=20261218|60=20260821-10:00:00"
    )
    assert order.instrument.option_kind is OptionKind.CALL
    assert order.instrument.strike == 150.5
    assert order.instrument.maturity == datetime.date(2026, 12, 18)


# -- what the shapes do not have a column for --------------------------------


def test_a_field_no_column_holds_is_kept_under_the_key_it_arrived_as() -> None:
    """Every venue sends fields no dictionary has; dropping them loses data."""
    order, _ = events(FILLED)
    assert order.metadata["9999"] == "venue-thing"


def test_a_field_a_column_already_holds_is_not_repeated_into_the_extras() -> None:
    order, fill = events(FILLED)
    for claimed in ("54", "44", "38", "31", "32", "150", "39", "55", "60", "52"):
        assert claimed not in order.metadata, claimed
        assert claimed not in fill.metadata, claimed


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
        ]
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
                ]
            )
        )
    )
    assert from_pairs.hash == from_line.hash and from_pairs.xhash == from_line.xhash


# -- the dictionary these shapes are their own ------------------------------


def test_the_tag_mapping_comes_from_the_declarations_rather_than_a_list() -> None:
    """Which is why it cannot drift: a column that gains a tag is in it."""
    tags = market_tags()
    assert tags["Side"] == 54 and tags["ExecType"] == 150 and tags["LastPx"] == 31
    assert tags["Symbol"] == 55, "a nested member of `instrument` counts too"
    assert set(CARRIED_TAGS) <= set(tags)


def test_every_carried_tag_is_the_number_the_dictionary_gives_that_name() -> None:
    """The one hand-written list of tag numbers in the module, checked against the
    published dictionary rather than against memory -- because a transposed tag
    reads a real field under the wrong name and nothing ever says so."""
    published = _published_tags()
    for name, tag in CARRIED_TAGS.items():
        assert published.get(name) == tag, f"{name} is <{published.get(name)}>, not <{tag}>"


# -- messages that are not market events -------------------------------------


@pytest.mark.parametrize("line", ["35=0|52=20260821-10:00:00", "35=A|98=0|108=30", "35=5"])
def test_a_session_message_yields_nothing_rather_than_failing(line: str) -> None:
    """A feed is mostly made of them, so an empty iterator is the right answer."""
    assert events(line) == []


def test_a_fragment_with_no_msgtype_is_read_from_the_fields_it_has() -> None:
    """A decoder that only works on complete headers is no use on a log."""
    (order,) = list(FixEvents(message=FixMessage.from_pairs([("11", "CL-1"), ("54", "1")])))
    assert isinstance(order, Order)
    reported = list(FixEvents(message=FixMessage.from_pairs([("17", "EX-1"), ("150", "F")])))
    assert [type(one) for one in reported] == [Order, Execution], (
        "an execution report says the order's state as well, header or no header"
    )


def _published_tags() -> dict[str, int]:
    """Every FIX field name to its tag, from `data/fix.zip` and not from the code.

    Read with `zipfile` rather than through `FixRegistry`, so this does not
    depend on the code it is checking -- exactly as `test_fix.py` does.
    """
    import json
    import zipfile
    from pathlib import Path

    archive = Path(__file__).resolve().parents[3] / "data" / "fix.zip"
    found: dict[str, int] = {}
    with zipfile.ZipFile(archive) as opened:
        for member in sorted(opened.namelist(), reverse=True):
            if member == "versions.json":
                continue
            for entry in json.loads(opened.read(member))["fields"]:
                found.setdefault(entry["name"], int(entry["metadata"]["fix:tag"]))
    return found


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
        assert one.instrument is reader.instrument, "one message, one instrument"
        assert one.instrument.alt_ids == {"ISIN": "US0378331005"}
        assert one.instrument.isin_code == "US0378331005"


def test_an_entry_that_names_its_own_instrument_gets_it() -> None:
    """A refresh may carry entries for several instruments, and then the entry
    is the one that says which."""
    # Without the header's ISIN, because a registered identifier outranks a
    # symbol and both entries would rightly land on the one instrument.
    line = IDENTIFIED.replace("48=US0378331005|22=4|454=1|455=US0378331005|456=4|", "")
    line = line.replace("269=1|270=100.7", "269=1|55=ETH-USD|270=100.7")
    found = list(FixEvents.from_text(line, venue="XCME"))
    assert [one.instrument.symbol for one in found] == ["BTC-USD", "ETH-USD"]
    assert found[0].instrument.xhash != found[1].instrument.xhash


def test_every_tag_the_instrument_reads_is_declared() -> None:
    """`INSTRUMENT_TAGS` decides when an entry may skip building its own, so a
    tag added to the reading and not to the set would let an entry inherit an
    instrument that is not its."""
    import inspect
    import re

    from rekep.market.fix import INSTRUMENT_TAGS

    source = inspect.getsource(FixEvents.instrument.func)
    read = set(re.findall(r"\bget\((\d+)\)", source))
    assert read, "the reading is there to be read"
    assert read <= INSTRUMENT_TAGS, f"undeclared: {sorted(read - INSTRUMENT_TAGS)}"
    assert {"454", "555"} <= INSTRUMENT_TAGS, "the two groups it also reads"
