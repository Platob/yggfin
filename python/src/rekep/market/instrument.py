"""What is being traded, as reference data publishes it."""

from __future__ import annotations

import dataclasses
import datetime
import functools
from collections.abc import Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import Annotated, Any

import pyarrow
import pyarrow.compute as compute

from rekep import txhash
from rekep.entries import ENTRIES, Entry
from rekep.enums import (
    Ascii32,
    AssetKind,
    Currency,
    EventType,
    OptionKind,
    Protocol,
    SecurityIDSource,
    Side,
)
from rekep.fields import Field, column_name, column_names, scalar
from rekep.fields.arrays import (
    build_list,
    dense_counts,
    list_parts,
    null_mask,
    sequence,
    struct_columns,
)
from rekep.fix.columns import ISIN_SCHEME, isin_identity
from rekep.fix.fields import cast_arrow_fix, scalar_fix_temporal
from rekep.fix.registry import FixRegistry
from rekep.market.event import (
    UNIX,
    Event,
    _declared_temporal_arrow,
    _declared_value_parts,
    _local_timestamp,
    unix_partition_arrow,
)
from rekep.market.fields import MarketConvertible, fix_tag
from rekep.market.identity import (
    HASH,
    NIL,
    framed_arrow,
    hash_bytes_arrow,
)
from rekep.market.ticker import SymbolTicker


@scalar(slots=True, weakref_slot=True)
class Leg(MarketConvertible):
    """One leg of a multileg instrument: a spread's near and far, an option's pair."""

    symbolticker: Annotated[str, Field.column("SymbolTicker")] = ""
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

    maturitydate: Annotated[datetime.datetime | None, fix_tag("LegMaturityDate")] = None
    """When the leg expires; null for anything that does not."""

    strikeprice: Annotated[float | None, fix_tag("LegStrikePrice")] = None
    """Exercise price, where the leg is an option."""

    putorcall: Annotated[OptionKind, fix_tag("LegPutOrCall")] = OptionKind.UNKNOWN
    """Which way the leg points, where it is an option."""

    def __post_init__(self) -> None:
        """Normalize the reference facts once."""
        self.maturitydate = _local_timestamp(self.maturitydate)
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

    @property
    def xhash(self) -> int:
        """Stable reference digest of `symbolticker`; zero when it is empty."""
        return Event.xhash_of(self.symbolticker)

    @classmethod
    def from_fix_arrow(
        cls,
        source: pyarrow.Array | Mapping[str, Any],
        rows: int | None = None,
        *,
        registry: FixRegistry | None = None,
    ) -> pyarrow.StructArray:
        """Normalize FIX leg columns as one market-leg struct."""
        columns = struct_columns(source) if isinstance(source, pyarrow.Array) else dict(source)
        rows = _row_count(columns) if rows is None else rows
        selected_registry = registry or FixRegistry.from_builtin()
        columns = _registry_instrument_columns(columns, selected_registry, rows, cls)
        ticker = SymbolTicker.into_arrow_array(columns, rows, selected_registry)
        currency = _enum_arrow(columns.get("currency"), rows, Currency, "from_fix", nullable=True)
        kind = _classified_arrow(columns.get("cficode"), columns.get("securitytype"), rows)
        pair_currency = SymbolTicker.currency_arrow(ticker)
        kind = compute.if_else(
            compute.and_(compute.equal(kind, 0), compute.is_valid(pair_currency)),
            pyarrow.scalar(int(AssetKind.CURRENCY), pyarrow.int64()),
            kind,
        )
        currency = compute.coalesce(currency, pair_currency)
        values: dict[str, Any] = {
            "symbolticker": ticker,
            "symbol": compute.fill_null(_text(columns.get("symbol"), rows), ""),
            "side": _enum_arrow(columns.get("side"), rows, Side, "from_fix"),
            "ratio": columns.get("ratio", columns.get("ratioqty")),
            "kind": kind,
            "securityid": _text(columns.get("securityid"), rows),
            "securityidsource": _text(columns.get("securityidsource"), rows),
            "cficode": _text(columns.get("cficode"), rows),
            "securitytype": _text(columns.get("securitytype"), rows),
            "securityexchange": _text(columns.get("securityexchange"), rows),
            "currency": currency,
            "contractmultiplier": columns.get("contractmultiplier"),
            "maturitydate": _maturity_arrow(
                columns.get("maturitydate"), columns.get("maturitymonthyear"), rows
            ),
            "strikeprice": columns.get("strikeprice"),
            "putorcall": _enum_arrow(
                columns.get("putorcall"), rows, OptionKind, "from_fix", integer_is_fix=True
            ),
        }
        return _struct_of(cls, values, rows)


@scalar(slots=True)
class TickRule(MarketConvertible):
    """One price threshold and the tick increment that applies from it."""

    starttickpricerange: Annotated[float | None, fix_tag("StartTickPriceRange")] = None
    """Lowest price at which this increment applies; null for the first open band."""

    tickincrement: Annotated[float, fix_tag("TickIncrement")] = 0.0
    """Smallest price change accepted inside this band."""

    def __post_init__(self) -> None:
        """Normalize numeric spellings once."""
        self.normalize_float_members()


@scalar(slots=True)
class Instrument(MarketConvertible):
    """FIX instrument facts independent of any event that carried them."""

    @classmethod
    @functools.cache
    def into_redirects(cls) -> Mapping[Any, str]:
        """Generic conversions from parsed rows and reference updates."""
        from rekep.text.fixmsg import FixMsg

        return MappingProxyType(
            {
                **MarketConvertible.into_redirects(),
                InstrumentUpdate: "update",
                FixMsg: "fixmsg",
            }
        )

    symbolticker: Annotated[str, fix_tag("SymbolTicker")] = ""
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
    isincode: Annotated[str | None, Field(metadata={"iso": "6166"}), fix_tag("ISINCode")] = None
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

    quantitytype: Annotated[int | None, fix_tag("QuantityType", dtype=pyarrow.int32())] = None
    """FIX quantity convention used by the instrument reference."""

    maturitydate: Annotated[datetime.datetime | None, fix_tag("MaturityDate")] = None
    """When the contract expires; null for anything that does not."""

    strikeprice: Annotated[float | None, fix_tag("StrikePrice")] = None
    """Exercise price of an option."""

    putorcall: Annotated[OptionKind, fix_tag("PutOrCall")] = OptionKind.UNKNOWN
    """Which way the option points; `UNKNOWN` for everything that is not one."""

    securitydesc: Annotated[str | None, fix_tag("SecurityDesc")] = None
    """Human description, as reference data publishes it."""

    # Nested members stay last because Iceberg counts leaf columns in
    # declaration order for the bounds it collects; see docs/market/index.md.
    legs: list[Leg] | None = None
    """The legs of a multileg instrument, in the order the venue sent them."""

    tickladder: list[TickRule] | None = None
    """Price bands in ascending source order, each carrying its active increment."""

    def __post_init__(self) -> None:
        """Normalize facts and settle the canonical ticker once."""
        self.maturitydate = _local_timestamp(self.maturitydate)
        self.normalize_float_members()
        if self.tickladder is not None:
            self.tickladder = [
                rule if isinstance(rule, TickRule) else TickRule.from_dict(rule)
                for rule in self.tickladder
            ]
        if self.currency is not None:
            self.currency = Currency.from_str(self.currency)
        self.securityid, self.securityidsource, self.isincode = isin_identity(
            self.securityid, self.securityidsource, self.isincode
        )
        if self.securityid and self.securityidsource is SecurityIDSource.ISIN:
            self.isincode = self.securityid
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

    @property
    def xhash(self) -> int:
        """Digest of `symbolticker`; zero when the ticker is empty."""
        return Event.xhash_of(self.symbolticker)

    def enriched_with(self, other: Instrument) -> Instrument | None:
        """These facts plus values only the other observation knows."""
        filled: dict[str, Any] = {}
        for member in dataclasses.fields(self):
            name = member.name
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
        return dataclasses.replace(self, **filled)

    @classmethod
    def from_referential_entries(
        cls,
        entries: Iterable[Entry | Mapping[str, Any] | tuple[Any, Any]],
        *,
        registry: FixRegistry | None = None,
    ) -> Instrument:
        """Build one component from normalized Referential entries."""
        stored = [Entry.from_stored(entry).into_dict() for entry in entries]
        found = cls.from_referential_arrow(pyarrow.array([stored], type=ENTRIES), registry=registry)
        return cls.from_dict(found[0].as_py())

    @classmethod
    def from_instrument_key(
        cls,
        key: str,
        *,
        venue: str | None = None,
        kind: AssetKind | str | None = None,
        registry: FixRegistry | None = None,
    ) -> Instrument:
        """Build one component from a `dbi;<isin>_<mic>_<ccy>` identity."""
        found = cls.from_instrument_keys_arrow(
            pyarrow.array([key]), venue=venue, kind=kind, registry=registry
        )
        return cls.from_dict(found[0].as_py())

    @classmethod
    def from_instrument_keys_arrow(
        cls,
        keys: Any,
        *,
        venue: Any = None,
        kind: Any = None,
        registry: FixRegistry | None = None,
    ) -> pyarrow.StructArray:
        """Build components from OMS/ULBridge instrument-key columns."""
        if isinstance(keys, pyarrow.ChunkedArray):
            keys = keys.combine_chunks()
        elif not isinstance(keys, pyarrow.Array):
            keys = pyarrow.array(keys if isinstance(keys, list | tuple) else [keys])
        columns: dict[str, Any] = {"instrumentkey": keys}
        if venue is not None:
            columns["securityexchange"] = venue
        if kind is not None:
            columns["referentialkind"] = kind
        return cls.from_fix_arrow(columns, len(keys), registry=registry)

    @classmethod
    def from_referential_arrow(
        cls,
        entries: pyarrow.Array | pyarrow.ChunkedArray,
        *,
        registry: FixRegistry | None = None,
    ) -> pyarrow.StructArray:
        """Build components from Referential entry rows without materializing rows."""
        if isinstance(entries, pyarrow.ChunkedArray):
            entries = entries.combine_chunks()
        return cls.from_fix_arrow({"entries": entries}, len(entries), registry=registry)

    @classmethod
    def from_fix_arrow(
        cls,
        source: pyarrow.RecordBatch | Mapping[str, Any],
        rows: int | None = None,
        *,
        registry: FixRegistry | None = None,
    ) -> pyarrow.StructArray:
        """Normalize promoted FIX columns as one nested component."""
        columns = (
            {name: source.column(name) for name in source.schema.names}
            if isinstance(source, pyarrow.RecordBatch)
            else dict(source)
        )
        rows = (
            source.num_rows
            if isinstance(source, pyarrow.RecordBatch)
            else (_row_count(columns) if rows is None else rows)
        )
        selected_registry = registry or FixRegistry.from_builtin()
        columns = _registry_instrument_columns(columns, selected_registry, rows)
        nested = columns.get("instrument")
        referential = _referential_columns_arrow(columns, rows, selected_registry)
        referential_rows = compute.is_valid(referential["instrumentkey"])
        if nested is not None:
            if isinstance(nested, pyarrow.ChunkedArray):
                nested = nested.combine_chunks()
            if nested.null_count < rows and not compute.any(referential_rows, min_count=0).as_py():
                return cls.into_field().cast_arrow_array(nested)
        identifiers = _identifier_arrow(columns, rows)
        identifiers = {
            "securityid": compute.coalesce(identifiers["securityid"], referential["securityid"]),
            "securityidsource": compute.coalesce(
                identifiers["securityidsource"], referential["securityidsource"]
            ),
            "isincode": compute.coalesce(identifiers["isincode"], referential["isincode"]),
        }
        columns.update(identifiers)
        columns["securityexchange"] = compute.coalesce(
            _text(columns.get("securityexchange"), rows),
            referential["securityexchange"],
        )
        ticker_columns = dict(columns)
        ticker_columns["securityidsource"] = identifiers["securityidsource"]
        ticker = SymbolTicker.into_arrow_array(ticker_columns, rows, selected_registry)
        currency = _enum_arrow(columns.get("currency"), rows, Currency, "from_fix", nullable=True)
        currency = compute.coalesce(currency, referential["currency"])
        pair_currency = SymbolTicker.currency_arrow(ticker)
        kind = _classified_arrow(columns.get("cficode"), columns.get("securitytype"), rows)
        kind = compute.if_else(compute.equal(kind, 0), referential["kind"], kind)
        kind = compute.if_else(
            compute.and_(compute.equal(kind, 0), compute.is_valid(pair_currency)),
            pyarrow.scalar(int(AssetKind.CURRENCY), pyarrow.int64()),
            kind,
        )
        values: dict[str, Any] = {
            "symbolticker": ticker,
            "symbol": compute.fill_null(_text(columns.get("symbol"), rows), ""),
            "kind": kind,
            "securityid": identifiers["securityid"],
            "securityidsource": _enum_arrow(
                identifiers["securityidsource"],
                rows,
                SecurityIDSource,
                "from_str",
                nullable=True,
            ),
            "isincode": identifiers["isincode"],
            "securitytype": _text(columns.get("securitytype"), rows),
            "cficode": _text(columns.get("cficode"), rows),
            "securityexchange": columns["securityexchange"],
            "currency": compute.coalesce(currency, pair_currency),
            "contractmultiplier": columns.get("contractmultiplier"),
            "minpriceincrement": columns.get("minpriceincrement"),
            "roundlot": columns.get("roundlot"),
            "quantitytype": compute.coalesce(
                _quantity_type_arrow(columns.get("quantitytype"), rows, selected_registry),
                referential["quantitytype"],
            ),
            "maturitydate": _maturity_arrow(
                columns.get("maturitydate"), columns.get("maturitymonthyear"), rows
            ),
            "strikeprice": columns.get("strikeprice"),
            "putorcall": _enum_arrow(
                columns.get("putorcall"), rows, OptionKind, "from_fix", integer_is_fix=True
            ),
            "securitydesc": _text(columns.get("securitydesc"), rows),
            "legs": _legs_arrow(columns.get("legs"), rows, selected_registry),
            "tickladder": referential["tickladder"],
        }
        built = _struct_of(cls, values, rows)
        if nested is None or nested.null_count == rows:
            return built
        nested = cls.into_field().cast_arrow_array(nested)
        enriched = _enriched_instrument_arrow(nested, built)
        return compute.if_else(
            compute.is_valid(nested),
            compute.if_else(referential_rows, enriched, nested),
            built,
        )

    @classmethod
    def from_update(cls, source: InstrumentUpdate) -> Instrument:
        """Extract the component carried by one reference-data update."""
        if not isinstance(source, InstrumentUpdate):
            raise TypeError(f"source must be InstrumentUpdate, got {type(source).__name__}")
        return source.instrument

    @classmethod
    def from_update_arrow_batch(cls, source: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        """Project update rows back to the component schema without row materialization."""
        batch = InstrumentUpdate.into_field().cast_arrow_batch(source)
        component = batch.column("instrument")
        return pyarrow.RecordBatch.from_arrays(
            [component.field(index) for index in range(component.type.num_fields)],
            schema=cls.into_field().into_arrow_schema(),
        )

    @classmethod
    def from_fixmsg(
        cls,
        source: Any,
        *,
        registry: FixRegistry | None = None,
    ) -> Instrument | None:
        """Build the first component carried by one parsed FIX row."""
        update = next(InstrumentUpdate.from_fixmsgs((source,), registry=registry), None)
        return None if update is None else update.instrument

    @classmethod
    def from_fix_events(cls, source: Any) -> Instrument:
        """Build one component from a FIX translator's scoped entry view."""
        get = source.get
        cfi = get("CFICode")
        securitytype = get("SecurityType")
        altids = source._security_altids
        built = cls.from_entries(
            source._event_entries,
            registry=source.registry,
            version=source.version,
            kind=_classified(cfi, securitytype),
            isincode=altids.get(ISIN_SCHEME),
            maturitydate=_date(get("MaturityDate")) or _month_year(get("MaturityMonthYear")),
        )
        if source.message.protocol.family is Protocol.REFERENTIAL:
            referential = cls.from_referential_entries(
                (*source.message.entries, *source.message.unmap),
                registry=source.registry,
            )
            built = referential.enriched_with(built) or referential
        built.legs = _normalized_legs(source, built.legs or source._declared_legs())
        parent = source.__dict__.get("_parent_reference")
        if parent is None:
            promoted = source._versioned_message.instrument
            return promoted.enriched_with(built) or promoted
        if not built.symbolticker:
            return parent
        fallback = dataclasses.replace(parent, legs=None)
        enriched = built.enriched_with(fallback)
        if enriched is None:
            return built
        # Header identifiers participate in ticker selection; settle the
        # combined facts once instead of retaining the child-only spelling.
        return dataclasses.replace(enriched, symbolticker="")


@scalar(slots=True)
class InstrumentUpdate(Event):
    """One observed version of an instrument component."""

    @classmethod
    @functools.cache
    def into_redirects(cls) -> Mapping[Any, str]:
        """Generic event conversions plus the event-free component."""
        return MappingProxyType({**Event.into_redirects(), Instrument: "instrument"})

    unix: Annotated[int, Field(metadata=UNIX)] = 0
    """When the reference facts were observed, in nanoseconds since the epoch."""

    hash: Annotated[int, Field(dtype=HASH)] = NIL
    """Time-anchored composition of `unix` and `vhash`."""

    # A current-reference table replaces one lifecycle at a time. The nested
    # ticker cannot be an Iceberg identifier field, so the event's top-level
    # lifecycle key is the merge key every engine can use.
    xhash: Annotated[int, Field.primary_key(dtype=HASH), Field.column("XHash")] = NIL
    """Direct XXH3-128 digest of the component's canonical ticker."""

    # Last because Iceberg counts nested leaves in declaration order for the
    # bounds it collects; see docs/market/index.md.
    instrument: Instrument = dataclasses.field(default_factory=Instrument)
    """Reference facts observed by this update."""

    def __post_init__(self) -> None:
        """Make the envelope identity agree with its component."""
        if not isinstance(self.instrument, Instrument):
            self.instrument = Instrument.from_dict(self.instrument)
        self.code = self.instrument.symbolticker
        self.codesource = "SymbolTicker" if self.code else ""
        Event.__post_init__(self)
        self._materialize_life_code()
        self.xhash = self.life_hash()
        self._drop_self_link()

    @classmethod
    @functools.cache
    def into_event_type(cls) -> EventType:
        """Reference-data updates use one event kind."""
        return EventType.INSTRUMENT

    @classmethod
    def from_instrument(
        cls,
        source: Instrument,
        *,
        unix: int = 0,
        creaunix: int | None = None,
        recunix: int | None = None,
        **event_values: Any,
    ) -> InstrumentUpdate:
        """Wrap one component in its observation envelope."""
        if not isinstance(source, Instrument):
            source = Instrument.from_dict(source)
        return cls(
            instrument=source,
            unix=unix,
            creaunix=unix if creaunix is None else creaunix,
            recunix=unix if recunix is None else recunix,
            **event_values,
        )

    @classmethod
    def from_fixmsg(
        cls,
        source: Any,
        *,
        registry: FixRegistry | None = None,
        **overrides: Any,
    ) -> InstrumentUpdate | None:
        """Build the first reference update carried by one parsed FIX row."""
        from rekep.text.fixmsg import FixMsg

        if not isinstance(source, FixMsg):
            raise TypeError(f"source must be FixMsg, got {type(source).__name__}")
        update = next(cls.from_fixmsgs((source,), registry=registry), None)
        if update is None or not overrides:
            return update
        explicit_identity = "vhash" in overrides or "hash" in overrides
        if not explicit_identity:
            overrides.update(vhash=NIL, hash=NIL)
        updated = dataclasses.replace(update, **overrides)
        return updated if explicit_identity else updated.identify()

    @classmethod
    def from_instrument_arrow_batch(
        cls,
        source: pyarrow.RecordBatch | pyarrow.StructArray,
        *,
        unix: Any = 0,
        creaunix: Any | None = None,
        recunix: Any | None = None,
        plugin: Any = "",
    ) -> pyarrow.RecordBatch:
        """Wrap component columns in identified update envelopes with Arrow kernels."""
        if isinstance(source, pyarrow.RecordBatch):
            component_batch = Instrument.into_field().cast_arrow_batch(source)
            component = pyarrow.StructArray.from_arrays(
                component_batch.columns, fields=list(component_batch.schema)
            )
            rows = component_batch.num_rows
        elif isinstance(source, pyarrow.StructArray):
            rows = len(source)
            raw = pyarrow.RecordBatch.from_arrays(
                [source.field(index) for index in range(source.type.num_fields)],
                names=[source.type.field(index).name for index in range(source.type.num_fields)],
            )
            component_batch = Instrument.into_field().cast_arrow_batch(raw)
            component = pyarrow.StructArray.from_arrays(
                component_batch.columns, fields=list(component_batch.schema)
            )
        else:
            raise TypeError(
                f"source must be RecordBatch or StructArray, got {type(source).__name__}"
            )

        clock = _broadcast(unix, rows, pyarrow.int64())
        ticker = compute.struct_field(component, "symbolticker")
        creation = clock if creaunix is None else _broadcast(creaunix, rows, pyarrow.int64())
        xhash = cls.xhash_arrow(ticker)
        field = cls.into_field()
        values = _default_columns(field, rows)
        values.update(
            {
                "unix": clock,
                "unixpartition": unix_partition_arrow(clock),
                "eventtype": _broadcast(int(EventType.INSTRUMENT), rows, pyarrow.int64()),
                "creaunix": creation,
                "recunix": clock if recunix is None else _broadcast(recunix, rows, pyarrow.int64()),
                "plugin": _broadcast(plugin, rows, pyarrow.string()),
                "xhash": xhash,
                "code": ticker,
                "codesource": compute.if_else(compute.equal(ticker, ""), "", "SymbolTicker"),
                "instrument": component,
            }
        )
        values["vhash"] = _update_vhash_arrow(cls, values, component)
        values["hash"] = txhash.couple128_arrow(cls._clock_micros(clock), values["vhash"])
        return pyarrow.RecordBatch.from_arrays(
            [field.field(name).cast_arrow_array(values[name]) for name in field.names],
            schema=field.into_arrow_schema(),
        )

    def life_code(self) -> str:
        """The canonical ticker that names this reference lifecycle."""
        return self.instrument.symbolticker

    def version_parts(self) -> tuple[Any, ...]:
        """Envelope values followed by the complete component declaration."""
        return (*Event.version_parts(self), *_declared_value_parts(self.instrument))

    def enriched_with(self, other: InstrumentUpdate) -> InstrumentUpdate | None:
        """This update plus facts only the other observation knows."""
        instrument = self.instrument.enriched_with(other.instrument)
        if instrument is None:
            return None
        return dataclasses.replace(self, instrument=instrument, vhash=NIL, hash=NIL)

    @classmethod
    def from_events(cls, events: Iterable[Any]) -> Iterator[InstrumentUpdate]:
        """Reference updates merged from transient market-event facts."""

        def observed() -> Iterator[InstrumentUpdate | None]:
            for event in events:
                instrument = event.into_instrument()
                yield (
                    None
                    if instrument is None
                    else cls.from_instrument(
                        instrument,
                        unix=event.unix,
                        creaunix=event.creaunix,
                        recunix=event.recunix,
                        plugin=event.plugin,
                    )
                )

        return cls.enriched(observed())

    @classmethod
    def from_fixmsgs(
        cls,
        logs: Iterable[Any],
        *,
        registry: FixRegistry | None = None,
    ) -> Iterator[InstrumentUpdate]:
        """Reference updates merged from parsed FIX messages."""

        def observed() -> Iterator[InstrumentUpdate]:
            for log in logs:
                if getattr(log, "error", None) or log.protocol.family is Protocol.OTHER:
                    continue
                translated = log.into_fix_events(registry=registry)
                for reader in translated._instrument_readers():
                    instrument = Instrument.from_fix_events(reader)
                    if instrument.symbolticker:
                        yield cls.from_instrument(
                            instrument,
                            unix=reader.unix,
                            creaunix=reader.creation_unix,
                            recunix=reader.recorded_unix,
                            plugin=reader.message.plugin,
                        )

        return cls.enriched(observed())

    @classmethod
    def enriched(cls, observed: Iterable[InstrumentUpdate | None]) -> Iterator[InstrumentUpdate]:
        """One deterministically enriched update per canonical ticker."""
        # Input order owns conflicts; later observations fill gaps but never
        # revise facts already stated by the first observation.
        order: list[str] = []
        records: dict[str, InstrumentUpdate] = {}
        for update in observed:
            if update is None or not update.instrument.symbolticker:
                continue
            update.identify()
            ticker = update.instrument.symbolticker
            known = records.get(ticker)
            if known is None:
                order.append(ticker)
                records[ticker] = update
                continue
            if known.vhash == update.vhash:
                continue
            instrument = known.instrument.enriched_with(update.instrument)
            if instrument is not None:
                records[ticker] = dataclasses.replace(
                    known, instrument=instrument, vhash=NIL, hash=NIL
                ).identify()
        yield from (records[ticker] for ticker in order)

    @classmethod
    def versioned(
        cls,
        observed: Iterable[InstrumentUpdate],
        stored: Mapping[str, InstrumentUpdate],
    ) -> Iterator[InstrumentUpdate]:
        """Observations that add a fact to the stored lifecycle."""
        for row in observed:
            known = stored.get(row.instrument.symbolticker)
            if known is not None and known.vhash == row.vhash:
                continue
            enriched = row if known is None else known.enriched_with(row)
            if enriched is not None:
                yield enriched.identify()


#: `SecurityType <167>` fallbacks for feeds that send no CFI code.
SECURITY_TYPES: dict[str, AssetKind] = {
    "CS": AssetKind.EQUITY,
    "PS": AssetKind.EQUITY,
    "MF": AssetKind.FUND,
    "FUT": AssetKind.FUTURE,
    "OPT": AssetKind.OPTION,
    "OOF": AssetKind.OPTION,
    "OOP": AssetKind.OPTION,
    "OOC": AssetKind.OPTION,
    "WAR": AssetKind.WARRANT,
    "MLEG": AssetKind.MULTILEG,
    "CDS": AssetKind.SWAP,
    "IRS": AssetKind.SWAP,
    "FXSWAP": AssetKind.SWAP,
    "FXSPOT": AssetKind.CURRENCY,
    "FXFWD": AssetKind.FORWARD,
    "FXNDF": AssetKind.FORWARD,
    "FORWARD": AssetKind.FORWARD,
    "CASH": AssetKind.CURRENCY,
    "REPO": AssetKind.REPO,
    "BUYSELL": AssetKind.REPO,
    "SECLOAN": AssetKind.LOAN,
    "SECPLEDGE": AssetKind.LOAN,
    "TERM": AssetKind.LOAN,
    "RVLV": AssetKind.LOAN,
    "RVLVTRM": AssetKind.LOAN,
    "BRIDGE": AssetKind.LOAN,
    "SWING": AssetKind.LOAN,
    "CORP": AssetKind.DEBT,
    "CB": AssetKind.DEBT,
    "TBOND": AssetKind.DEBT,
    "TNOTE": AssetKind.DEBT,
    "TBILL": AssetKind.DEBT,
    "TIPS": AssetKind.DEBT,
    "MUNI": AssetKind.DEBT,
    "GO": AssetKind.DEBT,
    "REV": AssetKind.DEBT,
    "MTN": AssetKind.DEBT,
    "CP": AssetKind.DEBT,
    "CD": AssetKind.DEBT,
    "ABS": AssetKind.DEBT,
    "MBS": AssetKind.DEBT,
    "CMO": AssetKind.DEBT,
    "FRN": AssetKind.DEBT,
    "EUCORP": AssetKind.DEBT,
    "EUSOV": AssetKind.DEBT,
    "BRADY": AssetKind.DEBT,
}


def _classified(cfi: str | None, securitytype: str | None) -> AssetKind:
    """Instrument kind from the ISO CFI category, then FIX SecurityType."""
    if cfi:
        found = AssetKind.from_cfi(cfi[:1])
        if found is not AssetKind.UNKNOWN:
            return found
    if securitytype:
        return SECURITY_TYPES.get(securitytype.strip().upper(), AssetKind.UNKNOWN)
    return AssetKind.UNKNOWN


def _month_year(text: str | None) -> datetime.datetime | None:
    """`MaturityMonthYear <200>` as the first or explicitly stated day."""
    if not text:
        return None
    trimmed = text.strip()
    if len(trimmed) < 6 or not trimmed[:6].isdigit():
        return None
    day = trimmed[6:8]
    try:
        return datetime.datetime(
            int(trimmed[:4]), int(trimmed[4:6]), int(day) if day.isdigit() else 1
        )
    except ValueError:
        return None


def _date(text: Any) -> datetime.datetime | None:
    """One FIX local-market date or timestamp in its stored naive form."""
    if isinstance(text, datetime.date):
        return _local_timestamp(text)
    parsed = scalar_fix_temporal(text, pyarrow.timestamp("us")) if isinstance(text, str) else None
    return _local_timestamp(parsed)


def _normalized_legs(source: Any, legs: list[Leg] | None) -> list[Leg] | None:
    """Apply CFI classification and month-year fallback to parsed legs."""
    if not legs:
        return legs
    entries = source._group("NoLegs")
    normalized: list[Leg] = []
    for index, leg in enumerate(legs):
        entry = entries[index] if index < len(entries) else {}
        changes: dict[str, Any] = {}
        if leg.kind is AssetKind.UNKNOWN:
            changes["kind"] = _classified(entry.get("LegCFICode"), entry.get("LegSecurityType"))
        if leg.maturitydate is None:
            maturity = _month_year(entry.get("LegMaturityMonthYear"))
            if maturity is not None:
                changes["maturitydate"] = maturity
        normalized.append(dataclasses.replace(leg, **changes) if changes else leg)
    return normalized


def _registry_instrument_columns(
    columns: Mapping[Any, Any],
    registry: FixRegistry,
    rows: int,
    shape: type[MarketConvertible] = Instrument,
) -> dict[str, Any]:
    """Resolve one instrument shape's input columns through the FIX registry."""
    name_items, tag_items = _registry_instrument_plan(registry, registry.revision, shape)
    name_targets = dict(name_items)
    tag_targets = dict(tag_items)

    candidates: dict[str, list[tuple[int, int, Any]]] = {}
    for order, (key, value) in enumerate(columns.items()):
        text = str(key)
        numeric = text.isascii() and text.isdigit()
        if numeric:
            for target, rank in tag_targets.get(int(text), ()):
                candidates.setdefault(target, []).append((rank, order, value))
            continue
        folded = column_name(text)
        target, rank = name_targets.get(folded, (folded, 0))
        if target:
            candidates.setdefault(target, []).append((rank, order, value))

    members = {member.name: member for member in shape.into_field().fields}
    normalized: dict[str, Any] = {}
    for target, found in candidates.items():
        ordered = [value for _, _, value in sorted(found, key=lambda item: item[:2])]
        normalized[target] = _coalesce_instrument_columns(ordered, rows, members.get(target))
    return normalized


def _coalesce_instrument_columns(
    values: list[Any], rows: int, field: Field | None
) -> pyarrow.Array:
    """First non-null alias or tag value per row, in registry priority."""
    arrays = [_instrument_source_array(value, rows) for value in values]
    if len(arrays) == 1:
        return arrays[0]
    if field is not None and field.enum.encoding == "ascii-big-endian":
        # Stored enums are packed integers while a registry alias or numeric
        # tag still carries FIX text. Render both as their text before choosing
        # rows so a packed ISIN code cannot be mistaken for the wire value `4`.
        arrays = [_instrument_enum_text(array, field.enum.byte_width) for array in arrays]
    else:
        concrete = [array.type for array in arrays if not pyarrow.types.is_null(array.type)]
        if concrete and any(not dtype.equals(concrete[0]) for dtype in concrete[1:]):
            dtype = field.dtype if field is not None else pyarrow.string()
            arrays = [
                pyarrow.nulls(rows, dtype)
                if pyarrow.types.is_null(array.type)
                else cast_arrow_fix(array, dtype)
                for array in arrays
            ]
        elif concrete:
            arrays = [
                pyarrow.nulls(rows, concrete[0]) if pyarrow.types.is_null(array.type) else array
                for array in arrays
            ]
    return compute.coalesce(*arrays)


def _instrument_source_array(value: Any, rows: int) -> pyarrow.Array:
    """One registry candidate broadcast to the source batch length."""
    if isinstance(value, pyarrow.ChunkedArray):
        value = value.combine_chunks()
    if isinstance(value, pyarrow.Array):
        if len(value) != rows:
            raise ValueError(f"instrument column has {len(value)} rows; expected {rows}")
        return value
    if isinstance(value, list | tuple):
        if len(value) != rows:
            raise ValueError(f"instrument column has {len(value)} rows; expected {rows}")
        return pyarrow.array(value)
    if value is None or isinstance(value, pyarrow.Scalar) and not value.is_valid:
        return pyarrow.nulls(rows)
    scalar = value if isinstance(value, pyarrow.Scalar) else pyarrow.scalar(value)
    return pyarrow.repeat(scalar, rows)


def _instrument_enum_text(values: pyarrow.Array, byte_width: int | None) -> pyarrow.Array:
    """FIX text for either raw spellings or packed ASCII enum storage."""
    if pyarrow.types.is_null(values.type):
        return pyarrow.nulls(len(values), pyarrow.string())
    if not pyarrow.types.is_integer(values.type) or not byte_width:
        return _text(values, len(values))
    distinct = compute.unique(values)

    def unpack(value: int | None) -> str | None:
        if value is None:
            return None
        try:
            raw = int(value).to_bytes(byte_width, "big", signed=False)
            code = raw.rstrip(b"\x00")
            if not code or any(byte < 0x20 or byte > 0x7E for byte in code):
                return str(value)
            return code.decode("ascii")
        except (OverflowError, ValueError):
            return str(value)

    rendered = pyarrow.array([unpack(value) for value in distinct.to_pylist()], pyarrow.string())
    return compute.take(rendered, compute.index_in(values, value_set=distinct))


@functools.lru_cache(maxsize=64)
def _registry_instrument_plan(
    registry: FixRegistry,
    revision: int,
    shape: type[MarketConvertible],
) -> tuple[
    tuple[tuple[str, tuple[str, int]], ...],
    tuple[tuple[int, tuple[tuple[str, int], ...]], ...],
]:
    """Registry resolution plan for one component declaration and revision."""
    del revision
    records: list[tuple[str, Any]] = []
    tag_targets: dict[int, list[tuple[str, int]]] = {}
    for member in shape.into_field().fields:
        record = registry.resolve(member.fix.canonical)
        if record is None:
            continue
        records.append((member.name, record))
    maturity_name = "LegMaturityMonthYear" if shape is Leg else "MaturityMonthYear"
    maturity = registry.resolve(maturity_name)
    if maturity is not None:
        records.append(("maturitymonthyear", maturity))

    name_targets = {record.fix.folded: (target, 0) for target, record in records}
    for target, record in records:
        for rank, spelling in enumerate(record.fix.spellings()[1:], start=100):
            name_targets.setdefault(column_name(spelling), (target, rank))
        for rank, tag in enumerate(record.fix.tag_priority, start=10):
            tag_targets.setdefault(tag, []).append((target, rank))
    return tuple(name_targets.items()), tuple(
        (tag, tuple(tag_targets[tag])) for tag in sorted(tag_targets)
    )


def _referential_columns_arrow(
    columns: Mapping[str, Any], rows: int, registry: FixRegistry | None
) -> dict[str, pyarrow.Array]:
    """Reference facts normalized from a Referential row's residual entries."""
    sources = _entry_arrays(columns, rows)
    instrument_key = compute.coalesce(
        _text(columns.get("instrumentkey"), rows),
        _first_entry_arrow(sources, "InstrumentKey", rows),
    )
    identity = _instrument_key_columns_arrow(instrument_key, rows)
    stated_kind = _asset_kind_arrow(columns.get("referentialkind"), rows)
    entry_kind = _asset_kind_arrow(_first_entry_arrow(sources, "AssetClass", rows), rows)
    return {
        "instrumentkey": instrument_key,
        **identity,
        "kind": compute.if_else(compute.equal(stated_kind, 0), entry_kind, stated_kind),
        "quantitytype": _quantity_type_arrow(
            _first_entry_arrow(sources, "QuantityType", rows), rows, registry
        ),
        "tickladder": _tick_ladder_arrow(sources, rows),
    }


def _instrument_key_columns_arrow(keys: Any, rows: int) -> dict[str, pyarrow.Array]:
    """Shared Arrow reading of `dbi;<isin>_<mic>_<ccy>` identities."""
    parsed = compute.extract_regex(
        _text(keys, rows),
        r"(?i)^dbi;(?P<isin>[^_]+)_(?P<mic>[^_]+)_(?P<currency>[^_]+)$",
    )
    isin = compute.struct_field(parsed, "isin")
    security_source = compute.if_else(
        compute.is_valid(isin),
        pyarrow.scalar(int(SecurityIDSource.ISIN), SecurityIDSource.into_arrow_type().index_type),
        pyarrow.scalar(None, SecurityIDSource.into_arrow_type().index_type),
    )
    return {
        "securityid": isin,
        "securityidsource": security_source,
        "isincode": isin,
        "securityexchange": compute.struct_field(parsed, "mic"),
        "currency": _enum_arrow(
            compute.struct_field(parsed, "currency"),
            rows,
            Currency,
            "from_str",
            nullable=True,
        ),
    }


def _asset_kind_arrow(source: Any, rows: int) -> pyarrow.Array:
    """Packed AssetKind values or their textual spellings."""
    dtype = AssetKind.into_arrow_type().index_type
    if isinstance(source, pyarrow.ChunkedArray):
        source = source.combine_chunks()
    if isinstance(source, pyarrow.Array) and pyarrow.types.is_integer(source.type):
        return compute.fill_null(source.cast(dtype, safe=False), pyarrow.scalar(0, dtype))
    if isinstance(source, Ascii32):
        return _broadcast(int(source), rows, dtype)
    return _mapped_arrow(source, rows, lambda value: int(AssetKind.from_str(value)), dtype)


def _entry_arrays(columns: Mapping[str, Any], rows: int) -> tuple[pyarrow.Array, ...]:
    """Entry-list columns that can carry unpromoted Referential members."""
    found: list[pyarrow.Array] = []
    for name in ("entries", "unmap"):
        source = columns.get(name)
        if isinstance(source, pyarrow.ChunkedArray):
            source = source.combine_chunks()
        if (
            isinstance(source, pyarrow.Array)
            and len(source) == rows
            and (pyarrow.types.is_list(source.type) or pyarrow.types.is_large_list(source.type))
            and pyarrow.types.is_struct(source.type.value_type)
            and {"key", "value", "comp"}.issubset(source.type.value_type.names)
        ):
            found.append(source)
    return tuple(found)


def _first_entry_arrow(sources: Iterable[pyarrow.Array], name: str, rows: int) -> pyarrow.Array:
    """First terminal key per row across retained then unmapped entries."""
    wanted = column_name(name)
    result = pyarrow.nulls(rows, pyarrow.string())
    for source in sources:
        items = compute.list_flatten(source)
        if not len(items):
            continue
        parents = compute.list_parent_indices(source).cast(pyarrow.int64())
        keys = _text(compute.struct_field(items, "key"), len(items))
        terminal = compute.struct_field(
            compute.extract_regex(keys, r"^(?:.*\.)?(?P<name>[^.]*)$"), "name"
        )
        matched = compute.fill_null(compute.equal(column_names(terminal), wanted), False)
        if not compute.any(matched, min_count=0).as_py():
            continue
        matched_parents = compute.filter(parents, matched)
        matched_values = compute.filter(
            _text(compute.struct_field(items, "value"), len(items)), matched
        )
        positions = compute.index_in(sequence(rows), value_set=matched_parents)
        result = compute.coalesce(result, compute.take(matched_values, positions))
    return result


def _quantity_type_arrow(column: Any, rows: int, registry: FixRegistry | None) -> pyarrow.Array:
    """QuantityType spellings encoded by the selected FIX dictionary."""
    source = _text(column, rows)
    declared = (registry or FixRegistry.from_builtin()).field("QuantityType")
    if declared is not None:
        source = declared.fix.arrow_encode(source)
    valid = compute.fill_null(compute.match_substring_regex(source, r"^[+-]?[0-9]+$"), False)
    numeric = compute.if_else(valid, source, pyarrow.scalar(None, pyarrow.string()))
    return numeric.cast(pyarrow.int32(), safe=False)


def _tick_ladder_arrow(sources: Iterable[pyarrow.Array], rows: int) -> pyarrow.Array:
    """Canonical TickRules entries grouped as the instrument's typed ladder."""
    dtype = Instrument.into_field().field("tickladder").dtype
    ladders = [_tick_ladder_one_arrow(source, rows, dtype) for source in sources]
    return compute.coalesce(*ladders) if ladders else pyarrow.nulls(rows, dtype)


def _tick_ladder_one_arrow(
    source: pyarrow.Array, rows: int, dtype: pyarrow.DataType
) -> pyarrow.Array:
    """One entry-list column projected to nullable ordered tick bands."""
    items = compute.list_flatten(source)
    if not len(items):
        return pyarrow.nulls(rows, dtype)
    parents = compute.list_parent_indices(source).cast(pyarrow.int64())
    keys = column_names(compute.struct_field(items, "key"))
    comps = _text(compute.struct_field(items, "comp"), len(items))
    indexed = compute.extract_regex(comps, r"(?i)(?:^|\.)TickRules\[(?P<index>[0-9]+)\]$")
    has_index = compute.is_valid(indexed)
    start_key = compute.equal(keys, column_name("StartTickPriceRange"))
    tick_key = compute.equal(keys, column_name("TickIncrement"))
    values = _text(compute.struct_field(items, "value"), len(items))
    numeric = compute.fill_null(
        compute.match_substring_regex(
            values,
            r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$",
        ),
        False,
    )
    relevant = compute.and_(has_index, compute.and_(compute.or_(start_key, tick_key), numeric))
    if not compute.any(relevant, min_count=0).as_py():
        return pyarrow.nulls(rows, dtype)
    indices = compute.if_else(
        has_index,
        compute.struct_field(indexed, "index"),
        pyarrow.scalar(None, pyarrow.string()),
    ).cast(pyarrow.int64(), safe=False)
    identities = compute.add(
        compute.multiply(parents, pyarrow.scalar(1 << 32, pyarrow.int64())), indices
    )
    starts = compute.and_(relevant, start_key)
    ticks = compute.and_(relevant, tick_key)
    tick_parents = compute.filter(parents, ticks)
    tick_ids = compute.filter(identities, ticks)
    tick_values = compute.filter(values, ticks).cast(pyarrow.float64(), safe=False)
    start_ids = compute.filter(identities, starts)
    start_values = compute.filter(values, starts).cast(pyarrow.float64(), safe=False)
    start_at = compute.index_in(tick_ids, value_set=start_ids)
    start_values = compute.take(start_values, start_at)
    rule = pyarrow.StructArray.from_arrays(
        [start_values, tick_values], fields=TickRule.into_field().arrow_fields
    )
    sizes = dense_counts(tick_parents, rows)
    return build_list(dtype, sizes, rule, compute.equal(sizes, 0))


def _enriched_instrument_arrow(
    primary: pyarrow.StructArray, fallback: pyarrow.StructArray
) -> pyarrow.StructArray:
    """Nested facts first, with Referential values filling only their gaps."""
    field = Instrument.into_field()
    defaults = Instrument().into_row()
    valid_parent = compute.is_valid(primary)
    columns: list[pyarrow.Array] = []
    for index, member in enumerate(field.fields):
        owned = compute.struct_field(primary, index)
        known = compute.is_valid(owned)
        if pyarrow.types.is_string(member.dtype):
            known = compute.and_(known, compute.not_equal(owned, ""))
        elif pyarrow.types.is_list(member.dtype) or pyarrow.types.is_large_list(member.dtype):
            known = compute.and_(known, compute.greater(compute.list_value_length(owned), 0))
        elif not member.nullable:
            known = compute.and_(
                known,
                compute.not_equal(owned, pyarrow.scalar(defaults[member.name], member.dtype)),
            )
        known = compute.and_(valid_parent, compute.fill_null(known, False))
        columns.append(compute.if_else(known, owned, compute.struct_field(fallback, index)))
    return pyarrow.StructArray.from_arrays(columns, fields=field.arrow_fields)


def _row_count(columns: Mapping[str, Any]) -> int:
    """Length shared by a mapping of Arrow columns."""
    for column in columns.values():
        if isinstance(column, pyarrow.ChunkedArray | pyarrow.Array):
            return len(column)
    return 0


def _broadcast(value: Any, rows: int, dtype: pyarrow.DataType) -> pyarrow.Array:
    """One scalar or already-vector value as exactly `rows` values of `dtype`."""
    if rows == 0:
        return pyarrow.array([], type=dtype)
    if isinstance(value, pyarrow.ChunkedArray):
        value = value.combine_chunks()
    if isinstance(value, pyarrow.Array):
        if len(value) != rows:
            raise ValueError(f"expected {rows} rows, got {len(value)}")
        return value if value.type == dtype else value.cast(dtype, safe=False)
    if isinstance(value, list | tuple):
        if len(value) != rows:
            raise ValueError(f"expected {rows} rows, got {len(value)}")
        return pyarrow.array(value, type=dtype)
    scalar = value if isinstance(value, pyarrow.Scalar) else pyarrow.scalar(value, type=dtype)
    if scalar.type != dtype:
        scalar = scalar.cast(dtype)
    return pyarrow.repeat(scalar, rows)


def _text(column: Any, rows: int) -> pyarrow.Array:
    """Trimmed UTF-8, with an absent input represented by nulls."""
    if column is None:
        return pyarrow.nulls(rows, pyarrow.string())
    if isinstance(column, pyarrow.ChunkedArray):
        column = column.combine_chunks()
    if not isinstance(column, pyarrow.Array):
        column = _broadcast(column, rows, pyarrow.string())
    elif column.type != pyarrow.string():
        column = column.cast(pyarrow.string(), safe=False)
    return compute.utf8_trim_whitespace(column)


def _mapped_arrow(
    column: Any,
    rows: int,
    convert: Any,
    dtype: pyarrow.DataType,
    *,
    nullable: bool = False,
) -> pyarrow.Array:
    """Apply a parser once per distinct spelling and take the results by index."""
    source = _text(column, rows)
    unique = compute.unique(source)
    converted = pyarrow.array(
        [None if value is None else convert(value) for value in unique.to_pylist()],
        type=dtype,
    )
    result = compute.take(converted, compute.index_in(source, value_set=unique))
    return result if nullable else compute.fill_null(result, pyarrow.scalar(0, dtype))


def _enum_arrow(
    column: Any,
    rows: int,
    enum_type: type[Any],
    parser: str,
    *,
    nullable: bool = False,
    integer_is_fix: bool = False,
) -> pyarrow.Array:
    """FIX spellings or already-packed codes as one enum storage column."""
    dtype = enum_type.into_arrow_type().index_type
    if column is None:
        return pyarrow.nulls(rows, dtype) if nullable else _broadcast(0, rows, dtype)
    if isinstance(column, pyarrow.ChunkedArray):
        column = column.combine_chunks()
    if (
        isinstance(column, pyarrow.Array)
        and pyarrow.types.is_integer(column.type)
        and not integer_is_fix
    ):
        cast = column.cast(dtype, safe=False)
        return cast if nullable else compute.fill_null(cast, pyarrow.scalar(0, dtype))
    read = getattr(enum_type, parser)
    return _mapped_arrow(column, rows, lambda value: int(read(value)), dtype, nullable=nullable)


def _classified_arrow(cfi: Any, securitytype: Any, rows: int) -> pyarrow.Array:
    """Vectorized CFI classification with `SecurityType` as its fallback."""
    dtype = AssetKind.into_arrow_type().index_type
    cfi_kind = _mapped_arrow(
        cfi,
        rows,
        lambda value: int(AssetKind.from_cfi(value[:1])) if value else 0,
        dtype,
    )
    fallback = _mapped_arrow(
        securitytype,
        rows,
        lambda value: int(SECURITY_TYPES.get(value.strip().upper(), AssetKind.UNKNOWN)),
        dtype,
    )
    return compute.if_else(compute.equal(cfi_kind, 0), fallback, cfi_kind)


def _maturity_arrow(date: Any, monthyear: Any, rows: int) -> pyarrow.Array:
    """FIX maturity date, filled from month resolution where exact dates are absent."""
    dtype = pyarrow.timestamp("us")

    def parsed(column: Any, convert: Any) -> pyarrow.Array:
        if column is None:
            return pyarrow.nulls(rows, dtype)
        if isinstance(column, pyarrow.ChunkedArray):
            column = column.combine_chunks()
        if isinstance(column, pyarrow.Array) and pyarrow.types.is_temporal(column.type):
            return column.cast(dtype, safe=False)
        if convert is _date:
            return cast_arrow_fix(_text(column, rows), dtype)
        return _mapped_arrow(column, rows, convert, dtype, nullable=True)

    return compute.coalesce(parsed(date, _date), parsed(monthyear, _month_year))


def _identifier_arrow(columns: Mapping[str, Any], rows: int) -> dict[str, pyarrow.Array]:
    """Normalize the primary identifier pair and the ISIN carried beside it."""
    identifier = _text(columns.get("securityid"), rows)
    source_column = columns.get("securityidsource")
    source = (
        _broadcast(source_column, rows, pyarrow.int32())
        if isinstance(source_column, (pyarrow.Array, pyarrow.ChunkedArray))
        and pyarrow.types.is_integer(source_column.type)
        else _text(source_column, rows)
    )
    isin = _text(columns.get("isincode"), rows)
    source_codes = _enum_arrow(source, rows, SecurityIDSource, "from_str", nullable=True)
    stated_isin = compute.fill_null(compute.equal(source_codes, int(SecurityIDSource.ISIN)), False)
    isin = compute.if_else(compute.and_(stated_isin, compute.is_null(isin)), identifier, isin)

    alternatives = columns.get("securityaltid")
    if alternatives is not None:
        if isinstance(alternatives, pyarrow.ChunkedArray):
            alternatives = alternatives.combine_chunks()
        _, entries = list_parts(alternatives)
        if len(entries):
            parents = compute.list_parent_indices(alternatives).cast(pyarrow.int64())
            schemes = compute.struct_field(entries, "securityaltidsource")
            scheme_codes = _enum_arrow(
                schemes, len(entries), SecurityIDSource, "from_str", nullable=True
            )
            keep = compute.fill_null(compute.equal(scheme_codes, int(SecurityIDSource.ISIN)), False)
            matched_parents = compute.filter(parents, keep)
            matched_values = compute.filter(
                _text(compute.struct_field(entries, "securityaltid"), len(entries)), keep
            )
            first = compute.index_in(sequence(rows), value_set=matched_parents)
            isin = compute.coalesce(isin, compute.take(matched_values, first))

    from_isin = compute.and_(compute.is_valid(isin), compute.is_null(identifier))
    identifier = compute.if_else(from_isin, isin, identifier)
    source_codes = compute.if_else(
        from_isin,
        pyarrow.scalar(int(SecurityIDSource.ISIN), source_codes.type),
        source_codes,
    )
    return {
        "securityid": identifier,
        "securityidsource": source_codes,
        "isincode": isin,
    }


def _legs_arrow(source: Any, rows: int, registry: FixRegistry | None = None) -> pyarrow.Array:
    """FIX leg entries normalized inside the component's declared list type."""
    dtype = Instrument.into_field().field("legs").dtype
    if source is None:
        return pyarrow.nulls(rows, dtype)
    if isinstance(source, pyarrow.ChunkedArray):
        source = source.combine_chunks()
    if source.type == dtype:
        return source
    sizes, entries = list_parts(source)
    normalized = Leg.from_fix_arrow(entries, len(entries), registry=registry)
    return build_list(dtype, sizes, normalized, null_mask(source))


def _struct_of(
    cls: type[MarketConvertible], values: Mapping[str, Any], rows: int
) -> pyarrow.StructArray:
    """Class-declared struct assembled from normalized member columns."""
    field = cls.into_field()
    defaults = cls().into_row()
    columns: list[pyarrow.Array] = []
    for member in field.fields:
        value = values.get(member.name)
        if value is None:
            if member.nullable:
                columns.append(pyarrow.nulls(rows, member.dtype))
                continue
            value = defaults[member.name]
        if not isinstance(value, pyarrow.Array | pyarrow.ChunkedArray):
            value = _broadcast(value, rows, member.dtype)
        columns.append(member.cast_arrow_array(value))
    return pyarrow.StructArray.from_arrays(columns, fields=field.arrow_fields)


def _default_columns(field: Any, rows: int) -> dict[str, pyarrow.Array]:
    """One update's declared defaults broadcast without constructing source rows."""
    if rows == 0:
        return {member.name: pyarrow.array([], type=member.dtype) for member in field.fields}
    defaults = InstrumentUpdate().into_row()
    return {
        member.name: pyarrow.repeat(pyarrow.scalar(defaults[member.name], type=member.dtype), rows)
        for member in field.fields
    }


def _joined_frames(*frames: Any) -> pyarrow.Array:
    """Raw identity-frame segments concatenated without framing them again."""
    return compute.binary_join_element_wise(
        *frames,
        pyarrow.scalar(b"", pyarrow.binary()),
        null_handling="replace",
        null_replacement=b"",
    )


def _declared_frame_arrow(values: pyarrow.Array) -> pyarrow.Array:
    """Arrow equivalent of `_declared_value_parts`, including nested lists."""
    kind = values.type
    if pyarrow.types.is_struct(kind):
        parts: list[Any] = [framed_arrow(True, kind.num_fields)[0]]
        for index in range(kind.num_fields):
            parts.extend(
                (
                    framed_arrow(kind.field(index).name)[0],
                    _declared_frame_arrow(compute.struct_field(values, index)),
                )
            )
        framed = _joined_frames(*parts)
        if values.null_count:
            framed = compute.if_else(compute.is_valid(values), framed, framed_arrow(None)[0])
        return framed
    if pyarrow.types.is_list(kind) or pyarrow.types.is_large_list(kind):
        sizes, items = list_parts(values)
        item_frames = _declared_frame_arrow(items)
        grouped = build_list(pyarrow.list_(pyarrow.binary()), sizes, item_frames)
        payload = compute.binary_join(grouped, pyarrow.scalar(b"", pyarrow.binary()))
        framed = _joined_frames(framed_arrow(True, sizes), payload)
        if values.null_count:
            framed = compute.if_else(compute.is_valid(values), framed, framed_arrow(None)[0])
        return framed
    if pyarrow.types.is_date(kind):
        values = values.cast(pyarrow.string())
    elif pyarrow.types.is_timestamp(kind):
        values = _declared_temporal_arrow(values)
    return framed_arrow(values)


def _update_vhash_arrow(
    cls: type[InstrumentUpdate], values: Mapping[str, pyarrow.Array], component: pyarrow.Array
) -> pyarrow.Array:
    """`InstrumentUpdate.version_parts` over whole columns."""
    event = framed_arrow(
        cls.__name__,
        values["eventtype"],
        values["state"],
        values["lastmkt"],
        values["code"],
        values["codesource"],
        values["reason"],
        True,
        0,
    )
    return hash_bytes_arrow(_joined_frames(event, _declared_frame_arrow(component)))
