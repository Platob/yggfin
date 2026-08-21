"""What is being traded, as reference data publishes it."""

from __future__ import annotations

import datetime
from typing import Annotated, ClassVar

from rekep.convert import Convertible
from rekep.fields import Field, FieldBuilder, field
from rekep.market.enums import AssetKind, OptionKind
from rekep.market.fields import MarketFieldBuilder, fix_tag
from rekep.market.identity import NIL, hash_of


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

    def __post_init__(self) -> None:
        """Derive `xhash` from the strongest identifier present, unless given one.

        A caller with a reference-data system already knows the identity, and
        what it says wins. Everything else is a producer that only has what the
        venue sent, and it still has to produce rows that join -- so the
        identity is derived, by `identify`, from what is actually there.
        """
        if not self.xhash:
            self.xhash = self.identify()

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
