"""FIX quotes use the same compact order lifecycle as firm interest."""

from __future__ import annotations

from pathlib import Path

from rekep.fix import FixRegistry
from rekep.market import MarketKind, Order, Side, State
from rekep.market.fix import FixEvents, unix_of

FIX_DATA = Path(__file__).resolve().parents[3] / "data" / "fix"


def events(line: str) -> list[Order]:
    return list(FixEvents.from_text(line, fix_version="4.4"))


def mass_quote(registry: FixRegistry) -> str:
    """A two-set message spelled from the local 4.4 dictionary."""
    tags = registry.tags("4.4")
    fields = (
        ("BeginString", "FIX.4.4"),
        ("MsgType", "i"),
        ("MsgSeqNum", 19),
        ("NoQuoteSets", 2),
        ("QuoteSetID", "SET-1"),
        ("TotNoQuoteEntries", 2),
        ("NoQuoteEntries", 2),
        ("QuoteEntryID", "ENTRY-1"),
        ("Symbol", "AAPL"),
        ("BidPx", 100),
        ("OfferPx", 101),
        ("BidSize", 10),
        ("OfferSize", 11),
        ("QuoteEntryID", "ENTRY-2"),
        ("Symbol", "MSFT"),
        ("BidPx", 200),
        ("OfferPx", 201),
        ("BidSize", 20),
        ("OfferSize", 21),
        ("QuoteSetID", "SET-2"),
        ("TotNoQuoteEntries", 1),
        ("NoQuoteEntries", 1),
        ("QuoteEntryID", "ENTRY-3"),
        ("Symbol", "NVDA"),
        ("BidPx", 300),
        ("OfferPx", 301),
        ("BidSize", 30),
        ("OfferSize", 31),
        ("TransactTime", "20260821-10:00:00"),
        ("CheckSum", "001"),
    )
    return "|".join(f"{tags[name.lower()]}={value}" for name, value in fields)


def test_a_two_sided_quote_becomes_two_distinct_indicative_orders() -> None:
    bid, ask = events(
        "35=S|117=Q-1|131=REQ-1|55=AAPL|132=100.0|133=101.0|"
        "134=10|135=12|537=1|62=20260821-10:05:00|60=20260821-10:00:00"
    )

    assert all(isinstance(row, Order) and row.indicative for row in (bid, ask))
    assert (bid.side, bid.px, bid.qty) == (Side.BID, 100.0, 10.0)
    assert (ask.side, ask.px, ask.qty) == (Side.ASK, 101.0, 12.0)
    assert bid.kind is ask.kind is MarketKind.LIMIT_ORDER
    assert bid.order_id == ask.order_id == "Q-1"
    assert bid.client_order_id == ask.client_order_id == "REQ-1"
    assert bid.xhash != ask.xhash
    assert bid.eunix == ask.eunix == unix_of("20260821-10:05:00")
    assert bid.metadata["537"] == ask.metadata["537"] == "1"


def test_named_quote_fields_resolve_through_the_builtin_registry() -> None:
    bid, ask = list(
        FixEvents.from_pairs(
            [
                ("MsgType", "S"),
                ("QuoteID", "Q-1"),
                ("Symbol", "AAPL"),
                ("BidPx", 100),
                ("OfferPx", 101),
                ("BidSize", 10),
                ("OfferSize", 12),
            ],
            fix_version="4.4",
        )
    )
    assert (bid.px, bid.qty, ask.px, ask.qty) == (100.0, 10.0, 101.0, 12.0)


def test_a_quote_status_without_prices_updates_both_quote_sides() -> None:
    opened = events("35=S|117=Q-1|55=AAPL|132=100|133=101|134=10|135=12|60=20260821-10:00:00")
    cancelled = events("35=AI|117=Q-1|55=AAPL|297=17|60=20260821-10:01:00")

    assert len(cancelled) == 2
    assert {row.xhash for row in cancelled} == {row.xhash for row in opened}
    assert all(row.state is State.CANCELLED and row.qty == 0.0 for row in cancelled)


def test_a_quote_response_can_target_one_side() -> None:
    (expired,) = events("35=AJ|117=Q-1|55=AAPL|54=1|694=3|60=20260821-10:01:00")
    assert expired.side is Side.BID
    assert expired.state is State.EXPIRED
    assert expired.indicative


def test_a_quote_reject_code_becomes_an_auditable_error() -> None:
    rejected = events("35=AI|117=Q-1|55=AAPL|297=5|300=8|60=20260821-10:01:00")
    assert len(rejected) == 2
    assert all(row.reason_code == 8 for row in rejected)
    assert all(row.error == "QuoteRejectReason=8: Invalid price" for row in rejected)
    assert all(row.metadata["300"] == "8" for row in rejected)


def test_firm_orders_and_depth_rows_are_distinguished() -> None:
    (firm,) = events("35=D|11=C-1|55=AAPL|54=1|38=5|44=100|60=20260821-10:00:00")
    (depth,) = events("35=X|55=AAPL|268=1|279=0|269=0|270=100|271=5|278=BID-1|60=20260821-10:00:00")
    assert not firm.indicative
    assert depth.indicative


def test_a_mass_quote_emits_every_quote_entry_side() -> None:
    rows = events(
        "35=i|117=MASS-1|295=2|"
        "299=ENTRY-1|55=AAPL|132=100|133=101|134=10|135=11|"
        "299=ENTRY-2|55=MSFT|132=200|133=201|134=20|135=21|"
        "60=20260821-10:00:00"
    )
    assert [(row.code, row.side, row.order_id) for row in rows] == [
        ("AAPL", Side.BID, "ENTRY-1"),
        ("AAPL", Side.ASK, "ENTRY-1"),
        ("MSFT", Side.BID, "ENTRY-2"),
        ("MSFT", Side.ASK, "ENTRY-2"),
    ]
    assert len({row.xhash for row in rows}) == 4


def test_nested_mass_quote_sets_emit_every_entry_in_wire_order() -> None:
    registry = FixRegistry(cache_dir=FIX_DATA, offline=True)
    rows = list(FixEvents.from_text(mass_quote(registry), registry=registry, fix_version="4.4"))

    assert [(row.code, row.side, row.order_id, row.px, row.qty) for row in rows] == [
        ("AAPL", Side.BID, "ENTRY-1", 100.0, 10.0),
        ("AAPL", Side.ASK, "ENTRY-1", 101.0, 11.0),
        ("MSFT", Side.BID, "ENTRY-2", 200.0, 20.0),
        ("MSFT", Side.ASK, "ENTRY-2", 201.0, 21.0),
        ("NVDA", Side.BID, "ENTRY-3", 300.0, 30.0),
        ("NVDA", Side.ASK, "ENTRY-3", 301.0, 31.0),
    ]
    assert [row.seq for row in rows] == [19] * 6


def test_nested_mass_quote_sets_project_each_instrument_once_in_wire_order() -> None:
    registry = FixRegistry(cache_dir=FIX_DATA, offline=True)
    found = list(
        FixEvents.from_text(
            mass_quote(registry), registry=registry, fix_version="4.4"
        ).into_instruments()
    )

    assert [instrument.symbol for instrument in found] == ["AAPL", "MSFT", "NVDA"]
