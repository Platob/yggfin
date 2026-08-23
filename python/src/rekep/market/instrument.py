"""What is being traded, as reference data publishes it."""

from __future__ import annotations

import dataclasses
import datetime
import functools
from collections.abc import Iterable, Iterator
from typing import Annotated, Any

from rekep.enums import AssetKind, Currency, EventType, IdSource, OptionKind, Ranged, Side, State
from rekep.fields import Field, scalar
from rekep.fix.registry import FixRegistry
from rekep.market.event import HOUR, Event
from rekep.market.fields import MarketConvertible, fix_tag
from rekep.market.identity import NIL, hash_of


@scalar(slots=True, weakref_slot=True)
class Leg(MarketConvertible):
    """One leg of a multileg instrument: a spread's near and far, an option's pair."""

    xhash: int = NIL
    """The instrument this leg is of, derived the same way any other one is."""

    symbol: Annotated[str, fix_tag("LegSymbol")] = ""
    """Identifier as the venue spells the leg."""

    side: Annotated[Side, fix_tag("LegSide")] = Side.UNKNOWN
    """Which way the strategy takes this leg; `side.sign` turns it into `+1`/`-1`."""

    ratio: Annotated[float | None, fix_tag("LegRatioQty")] = None
    """How many of this leg one unit of the strategy is; the leg's weight."""

    kind: AssetKind = AssetKind.UNKNOWN
    """What the leg settles as, read from `LegCFICode <608>` or `LegSecurityType <609>`."""

    security_id: Annotated[str | None, fix_tag("LegSecurityID")] = None
    """Identifier in the scheme `security_id_source` names."""

    security_id_source: Annotated[str | None, fix_tag("LegSecurityIDSource")] = None
    """Which scheme `security_id` is in, as FIX numbers them."""

    cfi: Annotated[str | None, fix_tag("LegCFICode")] = None
    """ISO 10962 classification of the leg."""

    security_type: Annotated[str | None, fix_tag("LegSecurityType")] = None
    """What the venue calls this leg, from FIX's own list."""

    exchange: Annotated[str | None, fix_tag("LegSecurityExchange")] = None
    """Where the leg is listed, when it differs from the strategy's venue."""

    currency: Annotated[Currency | None, fix_tag("LegCurrency")] = None
    """ISO 4217 currency the leg is priced in."""

    multiplier: Annotated[float | None, fix_tag("LegContractMultiplier")] = None
    """Units of the underlying one leg contract represents."""

    maturity: Annotated[datetime.date | None, fix_tag("LegMaturityDate")] = None
    """When the leg expires; null for anything that does not."""

    strike: Annotated[float | None, fix_tag("LegStrikePrice")] = None
    """Exercise price, where the leg is an option."""

    option_kind: Annotated[OptionKind, fix_tag("LegPutOrCall")] = OptionKind.UNKNOWN
    """Which way the leg points, where it is an option."""

    def __post_init__(self) -> None:
        """Derive `xhash` the way an instrument does, so a leg joins to one."""
        self.normalize_float_members()
        if self.currency is not None:
            self.currency = Currency.from_str(self.currency)
        if not self.xhash:
            self.xhash = Instrument(
                symbol=self.symbol,
                exchange=self.exchange,
                security_id=self.security_id,
                security_id_source=self.security_id_source,
            ).xhash


@scalar(slots=True)
class Instrument(Event):
    """One version of the facts known about a tradable instrument."""

    @classmethod
    @functools.cache
    def into_event_type(cls) -> EventType:
        """Instrument-state rows share one event kind."""
        return EventType.INSTRUMENT

    xhash: Annotated[int, Field.partition_key("bucket[16]")] = NIL
    """Stable identity of the instrument, which outlives every symbol it has had."""

    symbol: Annotated[str, fix_tag("Symbol")] = ""
    """Identifier as the venue spells it -- readable, and never an identity."""

    kind: AssetKind = AssetKind.UNKNOWN
    """What it settles as, read from the first character of the CFI code."""

    security_id: Annotated[str | None, fix_tag("SecurityID")] = None
    """Identifier in the scheme `security_id_source` names -- an ISIN, a CUSIP, a FIGI."""

    security_id_source: Annotated[str | None, fix_tag("SecurityIDSource")] = None
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

    security_type: Annotated[str | None, fix_tag("SecurityType")] = None
    """What the venue calls it, from FIX's own list -- `CS`, `FUT`, `OPT`, `MLEG`."""

    cfi: Annotated[str | None, fix_tag("CFICode")] = None
    """Full ISO 10962 classification; `kind` is its first character, decoded."""

    exchange: Annotated[str | None, fix_tag("SecurityExchange")] = None
    """ISO 10383 market identifier the instrument is listed on."""

    currency: Annotated[Currency | None, fix_tag("Currency")] = None
    """ISO 4217 currency the instrument is priced in."""

    # Persisted rather than joined for it, because it is what turns a price and
    # a quantity into money: without it every consumer of a notional needs the
    # reference table, and the ones that forget are wrong by a factor nobody
    # notices until settlement.
    multiplier: Annotated[float | None, fix_tag("ContractMultiplier")] = None
    """Units of the underlying one contract represents; 1 for cash instruments."""

    tick: Annotated[float | None, fix_tag("MinPriceIncrement")] = None
    """Smallest price change the venue accepts, which is what makes a spread countable."""

    lot: Annotated[float | None, fix_tag("RoundLot")] = None
    """Quantity increment the venue trades in."""

    maturity: Annotated[datetime.date | None, fix_tag("MaturityDate")] = None
    """When the contract expires; null for anything that does not."""

    strike: Annotated[float | None, fix_tag("StrikePrice")] = None
    """Exercise price of an option."""

    option_kind: Annotated[OptionKind, fix_tag("PutOrCall")] = OptionKind.UNKNOWN
    """Which way the option points; `UNKNOWN` for everything that is not one."""

    label: Annotated[str | None, fix_tag("SecurityDesc")] = None
    """Human description, as reference data publishes it."""

    # Last, and a list: a multileg instrument is a handful of legs and every
    # other instrument has none. Last because Iceberg counts leaf columns in
    # declaration order for the bounds it collects, and a nested member
    # declared earlier pushes a flat one past the cutoff -- see
    # `docs/market.md`.
    legs: list[Leg] | None = None
    """The legs of a multileg instrument, in the order the venue sent them."""

    def __post_init__(self) -> None:
        """Normalize facts and derive a stable identity when none was supplied."""
        self.normalize_float_members()
        if self.currency is not None:
            self.currency = Currency.from_str(self.currency)
        if self.isin_code is None:
            self.isin_code = self.into_isin()
        if not self.xhash:
            self.xhash = self.into_xhash()
        self.code = self.code or self.symbol
        Event.__post_init__(self)
        self._materialize_life_code()

    def into_isin(self) -> str | None:
        """The ISO 6166 identifier this instrument carries, from either place."""
        if self.security_id and IdSource.from_fix(self.security_id_source) is IdSource.ISIN:
            return self.security_id
        return (self.alt_ids or {}).get(IdSource.ISIN.name)

    def enriched_with(self, other: Instrument) -> Instrument | None:
        """This instrument plus whatever `other` knows and it does not, or None."""
        filled = {}
        for name in _INSTRUMENT_MEMBERS:
            mine, theirs = getattr(self, name), getattr(other, name)
            if theirs in (None, "", NIL) or theirs == mine:
                continue
            # A code that is `UNKNOWN` is not knowledge, and the zero every
            # `Ranged` starts at is what says so.
            if isinstance(mine, Ranged) and (not theirs or mine):
                continue
            if mine in (None, "", NIL) or not mine:
                filled[name] = theirs
        if not filled:
            return None
        return dataclasses.replace(self, **filled, xhash=self.xhash or other.xhash)

    def into_xhash(self) -> int:
        """The identity `self` is entitled to, from the strongest key it carries."""
        found = self.identities()
        return found[0] if found else NIL

    def identities(self) -> tuple[int, ...]:
        """Every identity this instrument could be known by, strongest first."""
        found = []
        if self.security_id and self.security_id_source:
            found.append(_identity_hash("id", self.security_id_source, self.security_id))
        if self.isin_code:
            found.append(_identity_hash("id", IdSource.ISIN.into_fix(), self.isin_code))
        if self.symbol:
            found.append(_identity_hash("symbol", self.exchange or "", self.symbol))
        return tuple(dict.fromkeys(found))

    def life_code(self) -> str:
        """Strongest readable identifier carried by this version."""
        if self.xcode:
            return self.xcode
        if self.security_id and self.security_id_source:
            return self.security_id
        return self.into_isin() or self.symbol or Event.life_code(self)

    def life_parts(self) -> tuple[Any, ...]:
        """An instrument lifecycle is its stable identity."""
        return (self.xhash,) if self.xhash else Event.life_parts(self)

    def version_parts(self) -> tuple[Any, ...]:
        """Hash the complete explicitly framed instrument state."""
        return (self.xhash, self.version, self.unix, *_instrument_parts(self))

    def into_log(self, **declared: Any) -> Any:
        """Carry this version as a normalized row in the market Log stream."""
        from rekep.text.log import Log

        return Log.from_instrument(self, **declared)

    @classmethod
    def from_observations(
        cls,
        observations: Iterable[
            tuple[int, Instrument | None] | tuple[int, Instrument | None, int | None]
        ],
        **declared: Any,
    ) -> Iterator[Instrument]:
        """Version ordered instrument observations and hourly snapshots."""
        return iter(_InstrumentIterator(observations=observations, **declared))

    @classmethod
    def from_events(cls, events: Iterable[Any], **declared: Any) -> Iterator[Instrument]:
        """Version transient instrument facts carried by market events."""
        return cls.from_observations(
            ((event.unix, event.into_instrument(), event.seq) for event in events),
            **declared,
        )

    @classmethod
    def from_logs(
        cls,
        logs: Iterable[Any],
        *,
        registry: FixRegistry | None = None,
        **declared: Any,
    ) -> Iterator[Instrument]:
        """Version instrument facts and symbol-only fallbacks from sorted logs."""

        def observations() -> Iterator[tuple[int, Instrument, int | None]]:
            for log in logs:
                for instrument in log.into_instruments(
                    registry=registry,
                ):
                    yield log.unix, instrument, log.seq

        return cls.from_observations(observations(), **declared)


@dataclasses.dataclass
class _InstrumentState:
    """Latest version and its next snapshot boundary."""

    xhash: int
    current: Instrument
    previous: Instrument
    next_snapshot: int | None


@dataclasses.dataclass
class _InstrumentIterator:
    """Deduplicate and version instrument observations in event-time order."""

    observations: Iterable[
        tuple[int, Instrument | None] | tuple[int, Instrument | None, int | None]
    ] = ()
    instruments: Iterable[Instrument] = ()
    snapshot_every: int = HOUR
    snapshot_until: int | None = None

    def __post_init__(self) -> None:
        if self.snapshot_every < 0:
            raise ValueError("snapshot_every must be non-negative")
        self._states: dict[int, _InstrumentState] = {}
        self._aliases: dict[int, int] = {}
        self._unix: int | None = None
        for known in self.instruments:
            self._seed(known)

    def __iter__(self) -> Iterator[Instrument]:
        for observed in self.observations:
            unix, instrument, *carried = observed
            seq = carried[0] if carried else None
            if self._unix is not None and unix < self._unix:
                unix = self._unix
            self._unix = unix
            yield from self._snapshots(unix, inclusive=True)
            if instrument is None or not instrument.identities():
                continue
            state = self._state_of(instrument)
            if state is None:
                known = _observed_at(instrument, unix, seq).with_previous(None)
                if known is None:  # pragma: no cover - first observations always add state
                    continue
                state = _InstrumentState(
                    xhash=known.xhash,
                    current=known,
                    previous=known,
                    next_snapshot=self._next_boundary(unix),
                )
                self._states[state.xhash] = state
                self._remember(state)
                yield known
                continue
            if all(
                getattr(state.current, name) == getattr(instrument, name)
                for name in _INSTRUMENT_MEMBERS
            ):
                continue
            enriched = state.current.enriched_with(instrument)
            if enriched is None:
                continue
            known = _observed_at(enriched, unix, seq, xhash=state.xhash).with_previous(
                state.previous
            )
            if known is None:
                continue
            state.current = known
            state.previous = known
            state.next_snapshot = self._next_boundary(unix)
            self._remember(state)
            yield known
        if self.snapshot_until is not None:
            yield from self._snapshots(self.snapshot_until)

    def _seed(self, known: Instrument) -> None:
        """Keep the latest supplied version for each lifecycle."""
        self._unix = known.unix if self._unix is None else max(self._unix, known.unix)
        current = self._states.get(known.xhash)
        if current is not None and (current.previous.unix, current.previous.version) >= (
            known.unix,
            known.version,
        ):
            return
        state = _InstrumentState(
            xhash=known.xhash,
            current=known,
            previous=known,
            next_snapshot=None if known.state.is_terminal else self._next_boundary(known.unix),
        )
        self._states[known.xhash] = state
        self._remember(state)

    def _state_of(self, instrument: Instrument) -> _InstrumentState | None:
        """Find a lifecycle through any non-conflicting spelling."""
        identities = instrument.identities()
        strong = _strong_identities(instrument, identities)
        for identity in identities:
            canonical = self._aliases.get(identity)
            if canonical is None:
                continue
            state = self._states[canonical]
            known_strong = _strong_identities(state.current)
            if strong and known_strong and strong.isdisjoint(known_strong):
                continue
            return state
        return None

    def _remember(self, state: _InstrumentState) -> None:
        """Alias every known spelling onto the first stable identity."""
        self._aliases[state.xhash] = state.xhash
        for identity in state.current.identities():
            self._aliases.setdefault(identity, state.xhash)

    def _next_boundary(self, unix: int) -> int | None:
        return (
            None
            if not self.snapshot_every
            else unix - unix % self.snapshot_every + self.snapshot_every
        )

    def _snapshots(self, unix: int, *, inclusive: bool = False) -> Iterator[Instrument]:
        """Emit globally ordered snapshots until each lifecycle is terminal."""
        while True:
            pending = [
                state.next_snapshot
                for state in self._states.values()
                if state.next_snapshot is not None
                and (state.next_snapshot < unix or inclusive and state.next_snapshot == unix)
            ]
            if not pending:
                return
            boundary = min(pending)
            for state in sorted(
                (state for state in self._states.values() if state.next_snapshot == boundary),
                key=lambda item: item.xhash,
            ):
                pictured = state.previous.make_snapshot(boundary)
                known = None if pictured is None else pictured.with_previous(state.previous)
                if known is None:
                    state.next_snapshot = (
                        None if state.previous.state.is_terminal else boundary + self.snapshot_every
                    )
                    continue
                state.previous = known
                state.next_snapshot = (
                    None if known.state.is_terminal else boundary + self.snapshot_every
                )
                yield known


# Frozen by the instrument identity protocol, not inferred from dataclass order.
_INSTRUMENT_MEMBERS = (
    "symbol",
    "kind",
    "security_id",
    "security_id_source",
    "isin_code",
    "alt_ids",
    "security_type",
    "cfi",
    "exchange",
    "currency",
    "multiplier",
    "tick",
    "lot",
    "maturity",
    "strike",
    "option_kind",
    "label",
    "legs",
)
_LEG_MEMBERS = (
    "xhash",
    "symbol",
    "side",
    "ratio",
    "kind",
    "security_id",
    "security_id_source",
    "cfi",
    "security_type",
    "exchange",
    "currency",
    "multiplier",
    "maturity",
    "strike",
    "option_kind",
)


def _observed_at(
    instrument: Instrument,
    unix: int,
    seq: int | None,
    *,
    xhash: int | None = None,
) -> Instrument:
    """Copy facts onto a fresh event envelope at one observation instant."""
    return dataclasses.replace(
        instrument,
        unix=unix,
        unix_hour=0,
        etype=EventType.INSTRUMENT,
        cunix=instrument.cunix or unix,
        runix=instrument.runix or unix,
        eunix=None,
        sunix=None,
        hash=NIL,
        xhash=instrument.xhash if xhash is None else xhash,
        linked_events=[],
        version=0,
        state=State.OPEN,
        seq=seq,
        prev_hash=None,
        prev_state=State.UNKNOWN,
        prev_unix=None,
        parent_hash=None,
        error=None,
    )


def _instrument_parts(instrument: Instrument) -> tuple[Any, ...]:
    """Instrument state as scalar identity-v1 parts, including containers."""
    parts: list[Any] = ["rekep-instrument-v1"]
    for name in _INSTRUMENT_MEMBERS:
        value = getattr(instrument, name)
        parts.append(name)
        if name == "alt_ids":
            parts.extend(_map_parts(value))
        elif name == "legs":
            parts.extend(_legs_parts(value))
        else:
            parts.append(_scalar_part(value))
    return tuple(parts)


def _strong_identities(
    instrument: Instrument, identities: tuple[int, ...] | None = None
) -> frozenset[int]:
    """Security identifiers, excluding the exchange-symbol fallback."""
    found = instrument.identities() if identities is None else identities
    return frozenset(found[:-1] if instrument.symbol else found)


def _map_parts(values: dict[str, str] | None) -> tuple[Any, ...]:
    if values is None:
        return (False, 0)
    ordered = sorted(values.items(), key=lambda item: item[0].encode("utf-8"))
    return (True, len(ordered), *(part for pair in ordered for part in pair))


def _legs_parts(legs: list[Any] | None) -> tuple[Any, ...]:
    if legs is None:
        return (False, 0)
    parts: list[Any] = [True, len(legs)]
    for index, leg in enumerate(legs):
        parts.extend(("leg", index))
        for name in _LEG_MEMBERS:
            parts.extend((name, _scalar_part(getattr(leg, name))))
    return tuple(parts)


def _scalar_part(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime.date) else value


@functools.lru_cache(maxsize=65_536)
def _identity_hash(kind: str, scope: str, value: str) -> int:
    """Hash a repeated instrument spelling once."""
    return hash_of(kind, scope, value)
