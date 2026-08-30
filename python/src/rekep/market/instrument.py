"""What is being traded, as reference data publishes it."""

from __future__ import annotations

import dataclasses
import datetime
import functools
from collections.abc import Iterable, Iterator
from typing import Annotated, Any

import pyarrow

from rekep.enums import (
    Ascii32,
    AssetKind,
    Currency,
    EventType,
    OptionKind,
    SecurityIDSource,
    Side,
)
from rekep.fields import Field, scalar
from rekep.fix.columns import ISIN_SCHEME, isin_identity
from rekep.fix.registry import FixRegistry
from rekep.market.event import UNIX, Event, _declared_value_parts
from rekep.market.fields import MarketConvertible, fix_tag
from rekep.market.identity import HASH, NIL, hash_bytes_of, hash_of
from rekep.market.ticker import SymbolTicker


@scalar(slots=True, weakref_slot=True)
class Leg(MarketConvertible):
    """One leg of a multileg instrument: a spread's near and far, an option's pair."""

    xhash: Annotated[int, Field(dtype=pyarrow.int64())] = NIL
    """The instrument this leg is of, derived the same way any other one is."""

    symbolticker: Annotated[str, Field.column("Symbol Ticker")] = ""
    """Canonical instrument spelling derived from the leg's FIX identifiers."""

    symbol: Annotated[str, fix_tag("LegSymbol")] = ""
    """Identifier as the venue spells the leg."""

    side: Annotated[Side, fix_tag("LegSide")] = Side.UNKNOWN
    """Which way the strategy takes this leg; `side.sign` turns it into `+1`/`-1`."""

    ratio: Annotated[float | None, fix_tag("LegRatioQty")] = None
    """How many of this leg one unit of the strategy is; the leg's weight."""

    kind: AssetKind = AssetKind.UNKNOWN
    """What the leg settles as, read from `LegCFICode <608>` or `LegSecurityType <609>`."""

    securityid: Annotated[str | None, fix_tag("LegSecurityID")] = None
    """Identifier in the scheme `securityidsource` names."""

    securityidsource: Annotated[str | None, fix_tag("LegSecurityIDSource")] = None
    """Which scheme `securityid` is in, as FIX numbers them."""

    cficode: Annotated[str | None, fix_tag("LegCFICode")] = None
    """ISO 10962 classification of the leg."""

    securitytype: Annotated[str | None, fix_tag("LegSecurityType")] = None
    """What the venue calls this leg, from FIX's own list."""

    securityexchange: Annotated[str | None, fix_tag("LegSecurityExchange")] = None
    """Where the leg is listed, when it differs from the strategy's venue."""

    currency: Annotated[Currency | None, fix_tag("LegCurrency")] = None
    """ISO 4217 currency the leg is priced in."""

    contractmultiplier: Annotated[float | None, fix_tag("LegContractMultiplier")] = None
    """Units of the underlying one leg contract represents."""

    maturitydate: Annotated[datetime.date | None, fix_tag("LegMaturityDate")] = None
    """When the leg expires; null for anything that does not."""

    strikeprice: Annotated[float | None, fix_tag("LegStrikePrice")] = None
    """Exercise price, where the leg is an option."""

    putorcall: Annotated[OptionKind, fix_tag("LegPutOrCall")] = OptionKind.UNKNOWN
    """Which way the leg points, where it is an option."""

    def __post_init__(self) -> None:
        """Derive `xhash` the way an instrument does, so a leg joins to one."""
        self.normalize_float_members()
        if self.currency is not None:
            self.currency = Currency.from_str(self.currency)
        ticker = SymbolTicker.from_values(
            symbolticker=self.symbolticker,
            symbol=self.symbol,
            securityid=self.securityid,
            securityidsource=self.securityidsource,
            securityexchange=self.securityexchange,
        )
        self.symbolticker = ticker.into_str()
        if ticker.kind is AssetKind.CURRENCY:
            if self.kind == AssetKind.UNKNOWN:
                self.kind = ticker.kind
            if self.currency is None:
                self.currency = ticker.currency
        self.xhash = hash_of(self.symbolticker) if self.symbolticker else NIL


@scalar(slots=True)
class Instrument(Event):
    """One flat reference-data record for a canonical ticker."""

    unix: Annotated[int, Field(metadata=UNIX), Field.sort_key()] = 0
    """When the reference facts were observed, in nanoseconds since the epoch."""

    hash: Annotated[int, Field(dtype=HASH)] = NIL
    """Time-anchored composition of `unix` and `vhash`."""

    @classmethod
    @functools.cache
    def into_event_type(cls) -> EventType:
        """Instrument records use one event kind."""
        return EventType.INSTRUMENT

    # Not a partition: bucketing a hash splits every hour into as many files as
    # buckets, and the hour already prunes the read this would prune.
    xhash: Annotated[int, Field(dtype=pyarrow.int64())] = NIL
    """Digest of `symbolticker`; zero when the ticker is empty."""

    symbolticker: Annotated[str, Field.primary_key(), Field.column("Symbol Ticker")] = ""
    """Canonical spelling selected from the FIX instrument identifiers."""

    symbol: Annotated[str, fix_tag("Symbol")] = ""
    """Human-readable spelling carried by `Symbol <55>`."""

    kind: AssetKind = AssetKind.UNKNOWN
    """What it settles as, read from the first character of the CFI code."""

    securityid: Annotated[str | None, fix_tag("SecurityID")] = None
    """Identifier in the scheme `securityidsource` names -- an ISIN, a CUSIP, a FIGI."""

    securityidsource: Annotated[SecurityIDSource | None, fix_tag("SecurityIDSource")] = None
    """Which scheme `securityid` is in, as its code; `ISIN` is FIX's `4`."""

    # Flat, and derived from whichever of the two places FIX carries it in --
    # `SecurityID <48>` under source `4`, or an entry of the `NoSecurityAltID
    # <454>` group. Flat because it is what a human looks an instrument up by
    # and what a reference-data join keys on, and neither can reach into a map
    # on any engine below Arrow.
    isincode: Annotated[str | None, Field(metadata={"iso": "6166"}), Field.column("ISIN Code")] = (
        None
    )
    """ISO 6166 identifier, wherever the message carried it; null when it did not."""

    securitytype: Annotated[str | None, fix_tag("SecurityType")] = None
    """What the venue calls it, from FIX's own list -- `CS`, `FUT`, `OPT`, `MLEG`."""

    cficode: Annotated[str | None, fix_tag("CFICode")] = None
    """Full ISO 10962 classification; `kind` is its first character, decoded."""

    securityexchange: Annotated[str | None, fix_tag("SecurityExchange")] = None
    """ISO 10383 market identifier the instrument is listed on."""

    currency: Annotated[Currency | None, fix_tag("Currency")] = None
    """ISO 4217 currency the instrument is priced in."""

    # Persisted rather than joined for it, because it is what turns a price and
    # a quantity into money: without it every consumer of a notional needs the
    # reference table, and the ones that forget are wrong by a factor nobody
    # notices until settlement.
    contractmultiplier: Annotated[float | None, fix_tag("ContractMultiplier")] = None
    """Units of the underlying one contract represents; 1 for cash instruments."""

    minpriceincrement: Annotated[float | None, fix_tag("MinPriceIncrement")] = None
    """Smallest price change the venue accepts, which is what makes a spread countable."""

    roundlot: Annotated[float | None, fix_tag("RoundLot")] = None
    """Quantity increment the venue trades in."""

    maturitydate: Annotated[datetime.date | None, fix_tag("MaturityDate")] = None
    """When the contract expires; null for anything that does not."""

    strikeprice: Annotated[float | None, fix_tag("StrikePrice")] = None
    """Exercise price of an option."""

    putorcall: Annotated[OptionKind, fix_tag("PutOrCall")] = OptionKind.UNKNOWN
    """Which way the option points; `UNKNOWN` for everything that is not one."""

    securitydesc: Annotated[str | None, fix_tag("SecurityDesc")] = None
    """Human description, as reference data publishes it."""

    # Last, and a list: a multileg instrument is a handful of legs and every
    # other instrument has none. Last because Iceberg counts leaf columns in
    # declaration order for the bounds it collects, and a nested member
    # declared earlier pushes a flat one past the cutoff -- see
    # `docs/market/index.md`.
    legs: list[Leg] | None = None
    """The legs of a multileg instrument, in the order the venue sent them."""

    def __post_init__(self) -> None:
        """Normalize facts and derive identity from the canonical ticker."""
        self.normalize_float_members()
        if self.currency is not None:
            self.currency = Currency.from_str(self.currency)
        # Before the ticker, which is built from the pair. The group-carried
        # ISIN follows it, because `altids` is filled by then and an identifier
        # the message stated outright outranks one read out of a group.
        self.securityid, self.securityidsource, self.isincode = isin_identity(
            self.securityid, self.securityidsource, self.isincode
        )
        if self.isincode is None:
            self.isincode = self.into_isin()
        ticker = SymbolTicker.from_values(
            symbolticker=self.symbolticker,
            symbol=self.symbol,
            securityid=self.securityid,
            securityidsource=self.securityidsource,
            securityexchange=self.securityexchange,
        )
        self.symbolticker = ticker.into_str()
        if ticker.kind is AssetKind.CURRENCY:
            if self.kind == AssetKind.UNKNOWN:
                self.kind = ticker.kind
            if self.currency is None:
                self.currency = ticker.currency

        self.xhash = self.into_xhash()
        self.code = self.symbolticker
        Event.__post_init__(self)
        self._materialize_life_code()

    def into_isin(self) -> str | None:
        """The ISO 6166 identifier this instrument carries, from either place."""
        if self.securityid and self.securityidsource is SecurityIDSource.ISIN:
            return self.securityid
        return (self.altids or {}).get(ISIN_SCHEME)

    def enriched_with(self, other: Instrument) -> Instrument | None:
        """This record plus facts only the other observation knows."""
        filled = {}
        merged_altids = dict(self.altids)
        for key, value in other.altids.items():
            merged_altids.setdefault(key, value)
        if merged_altids != self.altids:
            filled["altids"] = merged_altids
        event_members = set(Event.into_field().names)
        for member in dataclasses.fields(self):
            name = member.name
            if name in event_members or name == "xhash":
                continue
            mine, theirs = getattr(self, name), getattr(other, name)
            if theirs in (None, "", NIL) or theirs == mine:
                continue
            # A code that is `UNKNOWN` is not knowledge, and the zero every
            # stable code starts at is what says so.
            if isinstance(mine, Ascii32) and (not theirs or mine):
                continue
            if mine in (None, "", NIL) or not mine:
                filled[name] = theirs
        if not filled:
            return None
        return dataclasses.replace(self, **filled, vhash=NIL, hash=NIL)

    def into_xhash(self) -> int:
        """The canonical ticker's identity, or zero when absent."""
        return hash_of(self.symbolticker) if self.symbolticker else NIL

    def life_code(self) -> str:
        """The canonical ticker that names this reference record."""
        return self.symbolticker

    def life_parts(self) -> tuple[Any, ...]:
        """An instrument identity exists only when its canonical ticker does."""
        return (hash_bytes_of(self.xhash),) if self.xhash else ()

    def version_parts(self) -> tuple[Any, ...]:
        """Current declared reference values in the framed hash domain."""
        event_members = set(Event.into_field().names)
        values = {
            member.name: getattr(self, member.name)
            for member in type(self).into_field().fields
            if member.name not in event_members
        }
        return (*Event.version_parts(self), *_declared_value_parts(values))

    @classmethod
    def from_events(cls, events: Iterable[Any]) -> Iterator[Instrument]:
        """Flat reference records merged from transient market-event facts."""

        def observed() -> Iterator[Instrument | None]:
            for event in events:
                instrument = event.into_instrument()
                if instrument is not None and instrument.unix != event.unix:
                    instrument = dataclasses.replace(instrument, unix=event.unix, hash=NIL)
                yield instrument

        return _flat_instruments(observed())

    @classmethod
    def from_fixmsgs(
        cls,
        logs: Iterable[Any],
        *,
        registry: FixRegistry | None = None,
    ) -> Iterator[Instrument]:
        """Flat reference records merged from parsed FIX messages."""

        def observed() -> Iterator[Instrument]:
            for log in logs:
                yield from log.into_instruments(registry=registry)

        return _flat_instruments(observed())


def _flat_instruments(observed: Iterable[Instrument | None]) -> Iterator[Instrument]:
    """One deterministically enriched record per canonical ticker."""
    # Input order owns conflicts; later observations fill gaps but never revise facts.
    order: list[str] = []
    records: dict[str, Instrument] = {}
    for instrument in observed:
        if instrument is None or not instrument.symbolticker:
            continue
        instrument.identify()
        ticker = instrument.symbolticker
        known = records.get(ticker)
        if known is None:
            order.append(ticker)
            records[ticker] = instrument
            continue
        if known.vhash == instrument.vhash:
            continue
        enriched = known.enriched_with(instrument)
        if enriched is not None:
            records[ticker] = enriched.identify()
    # Identified where it was stored, and nothing has touched it since, so
    # there is nothing here to give an identity to.
    yield from (records[ticker] for ticker in order)
