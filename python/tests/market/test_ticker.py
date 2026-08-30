import pyarrow
import pytest

from rekep.entries import Entry
from rekep.enums import AssetKind, Currency, SecurityIDSource
from rekep.market.ticker import SymbolTicker
from rekep.text.fixmsg import FixMsg


def test_the_readable_symbol_precedes_the_security_identifier() -> None:
    """First rung of the ladder: what the desk, the venue and a reader all call
    the instrument, under the venue that named it."""
    message = FixMsg(
        protocolversion="4.2",
        securityexchange="xnas",
        securityidsource="4",
        securityid="US0378331005",
        symbol="AAPL",
    )

    ticker = SymbolTicker.from_fixmsg(message)

    assert str(ticker) == "XNAS:AAPL"
    assert ticker.into_str() == str(ticker)
    assert SymbolTicker.from_str(str(ticker)) == ticker


def test_the_identifier_is_the_rung_for_a_line_carrying_no_symbol() -> None:
    """Second rung, and the whole reason the first one is not the only one."""
    ticker = SymbolTicker.from_entries(
        [
            Entry(key="SecurityExchange", value="xnas"),
            Entry(key="SecurityIDSource", value="4"),
            Entry(key="SecurityID", value="US0378331005"),
        ],
        version="4.2",
    )

    assert str(ticker) == "XNAS:ISINNumber:US0378331005"
    assert SymbolTicker.from_str(str(ticker)) == ticker


def test_unknown_venue_is_omitted_from_an_identifier() -> None:
    ticker = SymbolTicker.from_entries(
        [
            Entry(key="SecurityExchange", value="XXXX"),
            Entry(key="SecurityIDSource", value="1"),
            Entry(key="SecurityID", value="037833100"),
        ],
        version="4.2",
    )

    assert str(ticker) == "CUSIP:037833100"
    assert SymbolTicker.from_str(str(ticker)) == ticker


def test_entries_resolve_numeric_tags_through_the_registry() -> None:
    ticker = SymbolTicker.from_entries(
        [
            Entry(key="207", value="XPAR"),
            Entry(key="55", value="AIR"),
        ],
        version="4.4",
    )

    assert str(ticker) == "XPAR:AIR"


@pytest.mark.parametrize("symbol", ["EUR/NOK", "eurnok", "EUR.NOK"])
def test_fx_spellings_share_one_ticker(symbol: str) -> None:
    ticker = SymbolTicker.from_entries([Entry(key="Symbol", value=symbol)])

    assert str(ticker) == "EUR/NOK"
    assert ticker.kind is AssetKind.CURRENCY
    assert ticker.currency is Currency.NOK
    assert SymbolTicker.from_str(str(ticker)) == ticker


def test_fx_detection_requires_iso_4217_codes() -> None:
    valid = SymbolTicker.from_str("AEDOMR")
    invented = SymbolTicker.from_str("ABC/XYZ")

    assert str(valid) == "AED/OMR"
    assert valid.kind is AssetKind.CURRENCY
    assert valid.currency is Currency.from_str("OMR")
    assert str(invented) == "ABC/XYZ"
    assert invented.kind is AssetKind.UNKNOWN
    assert invented.currency is None


def test_fx_ticker_preserves_a_known_venue() -> None:
    ticker = SymbolTicker.from_entries(
        [
            Entry(key="SecurityExchange", value="XPAR"),
            Entry(key="Symbol", value="eurnok"),
        ]
    )

    assert str(ticker) == "XPAR:EUR/NOK"
    assert ticker.kind is AssetKind.CURRENCY
    assert ticker.currency is Currency.NOK
    assert SymbolTicker.from_str(str(ticker)) == ticker


def test_a_scheme_is_stored_as_a_code_and_still_spelled_by_the_dictionary() -> None:
    """The column holds four bytes; the ticker keeps the dictionary's name, so
    reading the scheme as a code moved no stored ticker."""
    ticker = SymbolTicker.from_entries(
        [
            Entry(key="SecurityIDSource", value="4"),
            Entry(key="SecurityID", value="US0378331005"),
        ]
    )

    assert str(ticker) == "ISINNumber:US0378331005"
    assert SecurityIDSource.from_str("4") is SecurityIDSource.ISIN
    assert str(SecurityIDSource.ISIN) == "ISIN", "which is what a column stores"


def test_an_identifier_without_its_scheme_is_no_rung_at_all() -> None:
    """`US0378331005` and `037833100` are one instrument under two schemes, so
    an identifier missing its own cannot say which it holds."""
    assert (
        str(
            SymbolTicker.from_entries(
                [
                    Entry(key="SecurityID", value="US0378331005"),
                    Entry(key="Symbol", value="AAPL"),
                ]
            )
        )
        == "AAPL"
    )
    bare = SymbolTicker.from_entries([Entry(key="SecurityID", value="US0378331005")])
    assert str(bare) == ""
    assert bare.kind is AssetKind.UNKNOWN
    assert bare.currency is None


@pytest.mark.parametrize("symbol", ["", "[N/A]"])
def test_venue_alone_is_not_an_instrument_identifier(symbol: str) -> None:
    ticker = SymbolTicker.from_entries(
        [
            Entry(key="SecurityExchange", value="XPAR"),
            Entry(key="Symbol", value=symbol),
        ]
    )

    assert str(ticker) == ""


def test_private_identifier_source_is_preserved() -> None:
    ticker = SymbolTicker.from_entries(
        [
            Entry(key="SecurityIDSource", value="VENUE"),
            Entry(key="SecurityID", value="PRIVATE-1"),
        ]
    )

    assert str(ticker) == "VENUE:PRIVATE-1"
    assert SymbolTicker.from_str(str(ticker)) == ticker
    assert (
        str(
            SymbolTicker.from_entries(
                [
                    Entry(key="SecurityIDSource", value="VENUE"),
                    Entry(key="SecurityID", value="PRIVATE-1"),
                    Entry(key="Symbol", value="DISPLAY"),
                ]
            )
        )
        == "DISPLAY"
    ), "and a symbol beside it still leads"


def test_a_symbol_carrying_a_colon_stays_a_symbol() -> None:
    """The builder holds the parts, so it never re-reads its own spelling --
    which would take `4:X` for the scheme and identifier it looks like."""
    assert str(SymbolTicker.from_entries([Entry(key="Symbol", value="4:X")])) == "4:X"
    assert str(SymbolTicker.from_entries([Entry(key="Symbol", value="BRN:JAN26")])) == "BRN:JAN26"


def test_parse_cache_is_bounded_and_reuses_a_ticker() -> None:
    one = SymbolTicker.from_str("EUR/NOK")
    other = SymbolTicker.from_str("EUR/NOK")

    assert one is other
    assert SymbolTicker.from_str.cache_info().maxsize == 65_536


def test_arrow_tickers_follow_the_scalar_canonical_spelling() -> None:
    columns = {
        "securityid": pyarrow.array(["US0378331005", None, None]),
        "securityidsource": pyarrow.array(["4", None, None]),
        "symbol": pyarrow.array(["AAPL", "eurnok", "ABC/XYZ"]),
        "securityexchange": pyarrow.array(["xnas", "xpar", "XXXX"]),
    }

    found = SymbolTicker.into_arrow_array(columns, 3)

    assert found.to_pylist() == [
        "XNAS:AAPL",
        "XPAR:EUR/NOK",
        "ABC/XYZ",
    ]
    assert SymbolTicker.currency_arrow(found).to_pylist() == [None, int(Currency.NOK), None]


def test_parts_a_caller_already_holds_need_no_search_among_entries() -> None:
    """`from_values` and `from_entries` answer alike on the same four parts.

    A shape whose members *are* the parts -- `Instrument`, `Leg` -- was
    building a synthetic entry list so a registry-backed reader could find
    them again and hand back what it was given. Same ticker, without the
    search.
    """
    parts = {
        "securityexchange": "xnas",
        "securityidsource": "4",
        "securityid": "US0378331005",
        "symbol": "AAPL",
    }
    searched = SymbolTicker.from_entries(
        [Entry(key=key.capitalize(), value=value) for key, value in parts.items()],
        version="4.2",
    )

    assert SymbolTicker.from_values(**parts, version="4.2") == searched
    assert str(searched) == "XNAS:AAPL"


def test_a_stored_ticker_is_the_fallback_when_no_part_names_one() -> None:
    """The rung below the ladder: what a row already carries, kept as it is."""
    ticker = SymbolTicker.from_values(symbolticker="XPAR:TTF")

    assert str(ticker) == "XPAR:TTF"
    assert str(SymbolTicker.from_values(symbol="TTF", symbolticker="XPAR:OTHER")) == "TTF"
    assert str(SymbolTicker.from_values()) == ""


def test_a_scheme_a_member_spells_reaches_the_same_name_as_its_wire_code() -> None:
    """`SecurityIDSource.ISIN`, `"4"` and `"ISIN"` are one scheme in a ticker."""
    spellings = (SecurityIDSource.ISIN, "4", "ISIN", "ISINNumber")

    built = {
        str(SymbolTicker.from_values(securityid="XX0000084733", securityidsource=spelled))
        for spelled in spellings
    }

    assert built == {"ISINNumber:XX0000084733"}
