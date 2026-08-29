"""What a message teaches an instrument: identifiers, classification, and legs.

Every tag number here is checked against the published dictionary by
`test_fix.py`; what these pin is the *reading* of them.
"""

from __future__ import annotations

import datetime

import pytest

from rekep.fix.columns import ISIN_SCHEME, id_scheme
from rekep.market import AssetKind, Currency, FixEvents, Instrument, Leg, OptionKind, Side
from rekep.market.fix import SECURITY_TYPES, _classified, _month_year


def instrument_of(line: str) -> Instrument:
    (only, *rest) = FixEvents.from_text(line, venue="XCME", fix_version="4.4")
    assert not rest, "the fixture is meant to be one event"
    instrument = only.into_instrument()
    assert instrument is not None
    return instrument


HEAD = "35=D|49=XCME|52=20260821-10:30:00.000|11=CL-1|60=20260821-10:30:00.000"


# -- the ISIN ----------------------------------------------------------------


def test_the_isin_is_read_from_the_identifier_the_instrument_leads_with() -> None:
    """`SecurityID <48>` under `SecurityIDSource <22>` of `4`."""
    found = instrument_of(f"{HEAD}|55=AAPL|48=US0378331005|22=4")
    assert found.isincode == "US0378331005"


def test_the_isin_is_read_from_the_alternative_identifiers_too() -> None:
    """A venue uses whichever of the two places it prefers, and both are FIX."""
    found = instrument_of(f"{HEAD}|55=AAPL|48=037833100|22=1|454=1|455=US0378331005|456=4")
    assert found.isincode == "US0378331005"
    assert found.securityid == "037833100", "and what it led with is still there"


def test_an_identifier_in_no_scheme_is_not_an_isin() -> None:
    assert instrument_of(f"{HEAD}|55=AAPL|48=US0378331005").isincode is None
    assert instrument_of(f"{HEAD}|55=AAPL|48=037833100|22=1").isincode is None


def test_every_alternative_identifier_is_kept_under_the_scheme_that_issued_it() -> None:
    """Reference schemes use registry names and lifecycle aliases use field names."""
    found = instrument_of(
        f"{HEAD}|55=AAPL|454=3|455=US0378331005|456=4|455=037833100|456=1|455=AAPL.OQ|456=5"
    )
    assert found.altids == {
        "clordid": "CL-1",
        "ISINNumber": "US0378331005",
        "CUSIP": "037833100",
        "RICCode": "AAPL.OQ",
    }


def test_a_scheme_this_build_has_never_seen_keeps_the_character_it_came_as() -> None:
    """The only honest key left for it, and better than dropping the identifier."""
    found = instrument_of(f"{HEAD}|55=AAPL|454=1|455=whatever|456=Z")
    assert found.altids == {"clordid": "CL-1", "Z": "whatever"}


def test_an_instrument_without_reference_alternatives_keeps_lifecycle_altids() -> None:
    assert instrument_of(f"{HEAD}|55=AAPL").altids == {"clordid": "CL-1"}


def test_the_two_identifier_source_tags_share_one_enumeration() -> None:
    """Which is what lets one reading serve both -- and the dictionary names them.

    `SecurityIDSource <22>` enumerates every scheme there is, so this package
    compiles none of them: a scheme is stored under the name the dictionary
    gives it, and only the one this package asks a question about -- which
    instrument carries an ISIN -- is named in code.
    """
    assert id_scheme("4") == ISIN_SCHEME == "ISINNumber"
    assert id_scheme("8") == "ExchangeSymbol"
    assert id_scheme("A") == "BloombergSymbol"
    assert id_scheme(ISIN_SCHEME) == ISIN_SCHEME, "by its name as well as its code"
    assert id_scheme("") == "" and id_scheme(None) == ""


# -- what it settles as ------------------------------------------------------


def test_the_cfi_code_classifies_exactly_because_that_is_what_it_is_for() -> None:
    """ISO 10962's category letter is what `AssetKind` is coded on."""
    assert instrument_of(f"{HEAD}|55=AAPL|461=ESVUFR").kind is AssetKind.EQUITY
    assert instrument_of(f"{HEAD}|55=ESZ6|461=FFICSX").kind is AssetKind.FUTURE


def test_the_security_type_classifies_where_the_cfi_is_absent() -> None:
    """A venue that sends no CFI very often sends `CS`, `FUT` or `OPT` instead, and
    a reading that stopped at the CFI left every one of those unknown."""
    assert instrument_of(f"{HEAD}|55=AAPL|167=CS").kind is AssetKind.EQUITY
    assert instrument_of(f"{HEAD}|55=ESZ6|167=FUT").kind is AssetKind.FUTURE
    assert instrument_of(f"{HEAD}|55=SPX|167=MLEG").kind is AssetKind.MULTILEG


@pytest.mark.parametrize("symbol", ("EUR/USD", "EURUSD", "EUR.USD"))
def test_a_currency_pair_symbol_supplies_its_ticker_class_and_quote_currency(
    symbol: str,
) -> None:
    found = instrument_of(f"{HEAD}|55={symbol}")
    assert found.symbolticker == "EUR/USD"
    assert found.kind is AssetKind.CURRENCY
    assert found.currency is Currency.USD


def test_the_cfi_wins_over_the_security_type_when_both_are_there() -> None:
    assert _classified("ESVUFR", "FUT") is AssetKind.EQUITY


def test_a_cfi_that_classifies_as_nothing_falls_through_to_the_security_type() -> None:
    """Rather than stopping at a letter this build has no member for."""
    assert _classified("ZZZZZZ", "CS") is AssetKind.EQUITY


def test_a_type_that_is_in_neither_list_is_unknown_rather_than_a_guess() -> None:
    assert _classified(None, "NOTATHING") is AssetKind.UNKNOWN
    assert _classified(None, None) is AssetKind.UNKNOWN


def test_the_security_type_map_only_holds_values_the_dictionary_defines() -> None:
    """A key that names no FIX value is a rule that can never fire."""
    import json
    import zipfile
    from pathlib import Path

    archive = Path(__file__).resolve().parents[3] / "data" / "fix.zip"
    with zipfile.ZipFile(archive) as opened:
        # SecurityType is tag 167, so it is in the shard holding tags 0 to 499.
        shard = json.loads(opened.read("fields/000000.json"))
    # A record is a field document: the enumerated values are one packed JSON
    # string under `fix`, because Arrow field metadata is bytes to bytes.
    values = json.loads(shard["167"]["fix"].get("values") or "[]")
    published = {one["value"] for one in values}
    unknown = sorted(set(SECURITY_TYPES) - published)
    assert not unknown, f"no FIX version defines SecurityType {unknown}"


# -- when it expires ---------------------------------------------------------


def test_a_maturity_date_is_read_as_given() -> None:
    found = instrument_of(f"{HEAD}|55=ESZ6|541=20261218")
    assert found.maturitydate == datetime.date(2026, 12, 18)


def test_a_month_year_maturity_is_the_month_it_names() -> None:
    """The older of the two ways to say it, and a venue that sends it usually sends
    no `MaturityDate <541>` at all -- so reading it is the difference between a
    dated future and an undated one."""
    assert instrument_of(f"{HEAD}|55=ESZ6|200=202612").maturitydate == datetime.date(2026, 12, 1)


def test_a_month_year_with_a_day_in_it_keeps_the_day() -> None:
    assert _month_year("20261218") == datetime.date(2026, 12, 18)


def test_the_exact_date_wins_over_the_month() -> None:
    found = instrument_of(f"{HEAD}|55=ESZ6|200=202612|541=20261218")
    assert found.maturitydate == datetime.date(2026, 12, 18)


@pytest.mark.parametrize("text", ["", None, "2026", "abcdef", "202613", "20261232"])
def test_a_month_year_that_is_not_one_reads_as_absent(text: str | None) -> None:
    assert _month_year(text) is None


# -- the legs ----------------------------------------------------------------

SPREAD = (
    f"{HEAD}|55=SPREAD|207=XCME|15=USD|167=MLEG|555=2|"
    "600=ESZ6|602=US1234567890|603=4|624=1|623=2|609=FUT|610=202612|616=XCME|556=USD|614=50|"
    "600=ESH7|624=2|623=1|609=FUT|611=20270320|612=4500|1358=1|616=XCME|556=USD|614=50"
)


def test_a_multileg_instrument_carries_its_legs() -> None:
    found = instrument_of(SPREAD)
    assert found.kind is AssetKind.MULTILEG
    assert [one.symbol for one in found.legs] == ["ESZ6", "ESH7"]


def test_a_leg_says_which_way_the_strategy_takes_it_and_how_much() -> None:
    near, far = instrument_of(SPREAD).legs
    assert (near.side, near.ratio) == (Side.BUY, 2.0)
    assert (far.side, far.ratio) == (Side.SELL, 1.0)


def test_a_leg_is_read_with_the_same_rules_as_the_instrument_it_is_of() -> None:
    """Every member is the instrument field with a `Leg` in front of it."""
    near, far = instrument_of(SPREAD).legs
    assert near.kind is AssetKind.FUTURE and near.contractmultiplier == 50.0
    assert near.maturitydate == datetime.date(2026, 12, 1), "its month-year, read the same way"
    assert far.maturitydate == datetime.date(2027, 3, 20)
    assert far.strikeprice == 4500.0 and far.putorcall is OptionKind.CALL
    assert near.currency is Currency.USD and near.securityexchange == "XCME"


def test_a_leg_identifies_the_way_an_instrument_does_so_it_joins_to_one() -> None:
    near, far = instrument_of(SPREAD).legs
    assert near.xhash != far.xhash
    instrument = Instrument(
        symbol="ESZ6", securityexchange="XCME", securityid="US1234567890", securityidsource="4"
    )
    assert near.symbolticker == instrument.symbolticker
    assert near.xhash == instrument.xhash


def test_a_currency_pair_leg_uses_the_same_symbol_rules_as_an_instrument() -> None:
    leg = Leg(symbol="EUR/USD", xhash=7)
    instrument = Instrument(symbol=leg.symbol)
    assert (leg.symbolticker, leg.xhash, leg.kind, leg.currency) == (
        instrument.symbolticker,
        instrument.xhash,
        AssetKind.CURRENCY,
        Currency.USD,
    )


def test_an_instrument_that_is_not_multileg_carries_no_legs() -> None:
    assert instrument_of(f"{HEAD}|55=AAPL|167=CS").legs is None


def test_the_legs_are_not_repeated_into_the_metadata() -> None:
    """A field a column holds is not an extra, and a leg's fields are columns."""
    (order,) = FixEvents.from_text(f"{SPREAD}|9999=mine", venue="XCME", fix_version="4.4")
    assert order.metadata == {"9999": "mine"}


def test_a_group_rendered_with_indexes_reads_the_same_as_one_in_wire_order() -> None:
    """A log prints whichever its bridge felt like, and both are the same message."""
    rendered = (
        "MsgType=D|Symbol=SPREAD|ClOrdID=C|SecurityType=MLEG|"
        "NoLegs[0].LegSymbol=ESZ6|NoLegs[0].LegSide=1|NoLegs[0].LegRatioQty=2|"
        "NoLegs[1].LegSymbol=ESH7|NoLegs[1].LegSide=2|NoLegs[1].LegRatioQty=1|"
        "TransactTime=20260821-10:30:00.000"
    )
    (order,) = FixEvents.from_text(rendered, venue="XCME", fix_version="4.4")
    instrument = order.into_instrument()
    assert instrument is not None
    assert [(one.symbol, one.side, one.ratio) for one in instrument.legs] == [
        ("ESZ6", Side.BUY, 2.0),
        ("ESH7", Side.SELL, 1.0),
    ]
