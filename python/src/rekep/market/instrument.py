"""What is being traded, as reference data publishes it."""

from __future__ import annotations

import dataclasses
import datetime
from typing import Annotated, ClassVar

from rekep.convert import Convertible
from rekep.fields import Field, FieldBuilder, field
from rekep.market.enums import AssetKind, IdSource, OptionKind, Ranged, Side
from rekep.market.fields import MarketFieldBuilder, fix_tag
from rekep.market.identity import NIL, hash_of


@field
class Leg(Convertible):
    """One leg of a multileg instrument: a spread's near and far, an option's pair.

    FIX carries these in the `NoLegs <555>` group, whose members are the
    instrument fields with a `Leg` in front of them -- `LegSymbol <600>` is
    `Symbol <55>` for the leg. So this is `Instrument`'s own shape, cut down to
    what a leg actually varies: what it is, which way it points, and how much
    of it there is per unit of the strategy.

    Not an `Instrument`, and deliberately: a leg nested inside an instrument
    that nested a leg is a recursion Arrow has no type for, and the leg of a
    spread is a reference to a security rather than a second copy of the
    reference data for it. `xhash` is that reference.
    """

    FIELD_BUILDER: ClassVar[type[FieldBuilder]] = MarketFieldBuilder

    xhash: int = NIL
    """The instrument this leg is of, derived the same way any other one is."""

    symbol: Annotated[str, fix_tag("LegSymbol", 600)] = ""
    """Identifier as the venue spells the leg."""

    side: Annotated[Side, fix_tag("LegSide", 624)] = Side.UNKNOWN
    """Which way the strategy takes this leg; `side.sign` turns it into `+1`/`-1`."""

    ratio: Annotated[float | None, fix_tag("LegRatioQty", 623)] = None
    """How many of this leg one unit of the strategy is; the leg's weight."""

    kind: AssetKind = AssetKind.UNKNOWN
    """What the leg settles as, read from `LegCFICode <608>` or `LegSecurityType <609>`."""

    security_id: Annotated[str | None, fix_tag("LegSecurityID", 602)] = None
    """Identifier in the scheme `security_id_source` names."""

    security_id_source: Annotated[str | None, fix_tag("LegSecurityIDSource", 603)] = None
    """Which scheme `security_id` is in, as FIX numbers them."""

    cfi: Annotated[str | None, fix_tag("LegCFICode", 608)] = None
    """ISO 10962 classification of the leg."""

    security_type: Annotated[str | None, fix_tag("LegSecurityType", 609)] = None
    """What the venue calls this leg, from FIX's own list."""

    exchange: Annotated[str | None, fix_tag("LegSecurityExchange", 616)] = None
    """Where the leg is listed, when it differs from the strategy's venue."""

    currency: Annotated[str | None, fix_tag("LegCurrency", 556)] = None
    """ISO 4217 currency the leg is priced in."""

    multiplier: Annotated[float | None, fix_tag("LegContractMultiplier", 614)] = None
    """Units of the underlying one leg contract represents."""

    maturity: Annotated[datetime.date | None, fix_tag("LegMaturityDate", 611)] = None
    """When the leg expires; null for anything that does not."""

    strike: Annotated[float | None, fix_tag("LegStrikePrice", 612)] = None
    """Exercise price, where the leg is an option."""

    option_kind: Annotated[OptionKind, fix_tag("LegPutOrCall", 1358)] = OptionKind.UNKNOWN
    """Which way the leg points, where it is an option."""

    def __post_init__(self) -> None:
        """Derive `xhash` the way an instrument does, so a leg joins to one."""
        if not self.xhash:
            self.xhash = Instrument(
                symbol=self.symbol,
                exchange=self.exchange,
                security_id=self.security_id,
                security_id_source=self.security_id_source,
            ).xhash


@field
class Instrument(Convertible):
    """One tradable instrument, identified by sixteen bytes rather than by a symbol.

    A symbol is not an identity: it is reused after a delisting, respelled per
    venue and per vendor, and changed outright by a corporate action. `xhash`
    is what an event joins on and what survives all three; the symbol beside
    it is what a human reads and what a venue sent.

    Almost every member is nullable, and that is the shape of reference data
    rather than an oversight: an instrument is known by whoever has looked it
    up, a feed that only sends a symbol still produces valid rows, and a
    contract that demanded a CFI code would refuse the first venue that
    publishes none. What is NOT NULL is what a producer cannot fail to know --
    the identity, the symbol it was called, and the class it was treated as.
    """

    FIELD_BUILDER: ClassVar[type[FieldBuilder]] = MarketFieldBuilder

    xhash: Annotated[int, Field.primary_key()] = NIL
    """Stable identity of the instrument, which outlives every symbol it has had."""

    symbol: Annotated[str, fix_tag("Symbol", 55)] = ""
    """Identifier as the venue spells it -- readable, and never an identity."""

    kind: AssetKind = AssetKind.UNKNOWN
    """What it settles as, read from the first character of the CFI code."""

    security_id: Annotated[str | None, fix_tag("SecurityID", 48)] = None
    """Identifier in the scheme `security_id_source` names -- an ISIN, a CUSIP, a FIGI."""

    security_id_source: Annotated[str | None, fix_tag("SecurityIDSource", 22)] = None
    """Which scheme `security_id` is in, as FIX numbers them (`4` is ISIN)."""

    # Flat, and derived from whichever of the two places FIX carries it in --
    # `SecurityID <48>` under source `4`, or an entry of the `NoSecurityAltID
    # <454>` group. Flat because it is what a human looks an instrument up by
    # and what a reference-data join keys on, and neither can reach into a map
    # on any engine below Arrow.
    isin_code: Annotated[str | None, Field(metadata={"iso": "6166"})] = None
    """ISO 6166 identifier, wherever the message carried it; null when it did not."""

    # A map and not a struct: which schemes a venue sends is not known when the
    # shape is written, and a column per scheme would be forty nulls wide.
    # Nullable, like `MarketEvent.metadata` and for the same reason: most
    # instruments carry no alternative at all, and a null says that where an
    # empty map says "it sent an empty list of them".
    alt_ids: dict[str, str] | None = None
    """Every other identifier the message carried, keyed by `IdSource`'s name."""

    security_type: Annotated[str | None, fix_tag("SecurityType", 167)] = None
    """What the venue calls it, from FIX's own list -- `CS`, `FUT`, `OPT`, `MLEG`."""

    cfi: Annotated[str | None, fix_tag("CFICode", 461)] = None
    """Full ISO 10962 classification; `kind` is its first character, decoded."""

    exchange: Annotated[str | None, fix_tag("SecurityExchange", 207)] = None
    """ISO 10383 market identifier the instrument is listed on."""

    currency: Annotated[str | None, fix_tag("Currency", 15)] = None
    """ISO 4217 currency the instrument is priced in."""

    # Persisted rather than joined for it, because it is what turns a price and
    # a quantity into money: without it every consumer of a notional needs the
    # reference table, and the ones that forget are wrong by a factor nobody
    # notices until settlement.
    multiplier: Annotated[float | None, fix_tag("ContractMultiplier", 231)] = None
    """Units of the underlying one contract represents; 1 for cash instruments."""

    tick: Annotated[float | None, fix_tag("MinPriceIncrement", 969)] = None
    """Smallest price change the venue accepts, which is what makes a spread countable."""

    lot: Annotated[float | None, fix_tag("RoundLot", 561)] = None
    """Quantity increment the venue trades in."""

    maturity: Annotated[datetime.date | None, fix_tag("MaturityDate", 541)] = None
    """When the contract expires; null for anything that does not."""

    strike: Annotated[float | None, fix_tag("StrikePrice", 202)] = None
    """Exercise price of an option."""

    option_kind: Annotated[OptionKind, fix_tag("PutOrCall", 201)] = OptionKind.UNKNOWN
    """Which way the option points; `UNKNOWN` for everything that is not one."""

    label: Annotated[str | None, fix_tag("SecurityDesc", 107)] = None
    """Human description, as reference data publishes it."""

    # Last, and a list: a multileg instrument is a handful of legs and every
    # other instrument has none. Last because Iceberg counts leaf columns in
    # declaration order for the bounds it collects, and a nested member
    # declared earlier pushes a flat one past the cutoff -- see
    # `docs/market.md`.
    legs: list[Leg] | None = None
    """The legs of a multileg instrument, in the order the venue sent them."""

    def __post_init__(self) -> None:
        """Derive `xhash` from the strongest identifier present, unless given one.

        A caller with a reference-data system already knows the identity, and
        what it says wins. Everything else is a producer that only has what the
        venue sent, and it still has to produce rows that join -- so the
        identity is derived, by `identify`, from what is actually there.
        """
        if self.isin_code is None:
            self.isin_code = self.into_isin()
        if not self.xhash:
            self.xhash = self.identify()

    def into_isin(self) -> str | None:
        """The ISO 6166 identifier this instrument carries, from either place.

        FIX puts an ISIN in one of two places and a venue uses whichever it
        prefers: `SecurityID <48>` when `SecurityIDSource <22>` is `4`, or an
        entry of the `NoSecurityAltID <454>` group whose `SecurityAltIDSource
        <456>` is -- the two tags share one enumeration, which is what lets one
        reading serve both.

        `alt_ids` is keyed by `IdSource`'s own name, so the alternative is a
        probe rather than a scan.
        """
        if self.security_id and IdSource.from_fix(self.security_id_source) is IdSource.ISIN:
            return self.security_id
        return (self.alt_ids or {}).get(IdSource.ISIN.name)

    def enriched_with(self, other: Instrument) -> Instrument | None:
        """This instrument plus whatever `other` knows and it does not, or None.

        None when it learnt nothing, and that is what makes it usable on a
        stream: a feed repeats the instrument on every message, so a row per
        message would be the feed again rather than the reference data in it.
        A row comes out only where something was actually learnt.

        **Filling, never correcting.** What this instrument already says
        stands: reference data arrives in the order a venue felt like sending
        it, and a later message that omits a field has not retracted it. A
        producer that means to correct something replaces the row.

        **The identity does not move.** An instrument enriched with a tick or
        a maturity is the same instrument, and an `xhash` that changed when a
        field was learnt would break every join to it -- which is why
        `identify` keys on what is issued rather than on what is known.
        """
        filled = {}
        for member in dataclasses.fields(self):
            if member.name == "xhash":
                continue
            mine, theirs = getattr(self, member.name), getattr(other, member.name)
            if theirs in (None, "", NIL) or theirs == mine:
                continue
            # A code that is `UNKNOWN` is not knowledge, and the zero every
            # `Ranged` starts at is what says so.
            if isinstance(mine, Ranged) and (not theirs or mine):
                continue
            if mine in (None, "", NIL) or not mine:
                filled[member.name] = theirs
        if not filled:
            return None
        return dataclasses.replace(self, **filled, xhash=self.xhash or other.xhash)

    def identify(self) -> int:
        """The identity `self` is entitled to, from the strongest key it carries.

        In order, because that is the order of how much a key is worth:

        1. a **registered identifier** -- `security_id` in the scheme
           `security_id_source` names (ISIN, CUSIP, FIGI). It is issued rather
           than chosen, so two vendors spelling the same instrument differently
           still land on one identity.
        2. the **symbol, scoped to its venue** -- `exchange` then `symbol`.
           A bare symbol is not unique across venues (`BTC-USD` is several
           different contracts), so the venue is part of the key; a feed that
           names no exchange gets the empty scope, consistently.
        3. **nothing** -- an instrument with neither is `NIL`, which is a
           visible "unidentified" rather than a hash of emptiness that would
           silently merge every unnamed instrument into one.

        Each branch leads with a constant naming the scheme, so a symbol that
        happens to read like an ISIN cannot collide with the ISIN.
        """
        if self.security_id and self.security_id_source:
            return hash_of("id", self.security_id_source, self.security_id)
        if self.symbol:
            return hash_of("symbol", self.exchange or "", self.symbol)
        return NIL
