"""The lifecycle envelope every market row carries, and the priced event on top of it."""

from __future__ import annotations

import copy
import dataclasses
import datetime
import enum
import functools
from collections.abc import Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Any, Self, get_type_hints

import pyarrow
import pyarrow.compute

from rekep import txhash
from rekep.annotations import SEQUENCE_ORIGINS, item_annotation, unwrap_annotated, unwrap_optional
from rekep.enums import MIC, AssetKind, Currency, EventType, MarketKind, Plugin, Side, State
from rekep.fields import Field, column_name, scalar
from rekep.fields.arrays import build_list, dense_counts, null_mask
from rekep.market.fields import MarketConvertible, fix_tag
from rekep.market.identity import (
    HASH,
    NIL,
    NIL_BYTES,
    framed_arrow,
    hash128_bytes,
    hash128_bytes_arrow,
    hash_arrow,
    hash_of,
)
from rekep.market.ticker import SymbolTicker

if TYPE_CHECKING:
    from rekep.fix.registry import FixRegistry
    from rekep.market.instrument import Instrument

#: What a `*unix` column holds, said once. Whole nanoseconds since the epoch,
#: as an integer rather than a timestamp type -- which is the same choice
#: `FixMsg` makes, for the same reason: a width or a zone that a downstream is
#: picky about is a conversion per row, and an integer survives every one of
#: them unchanged.
UNIX: dict[str, str] = {"unit": "ns", "epoch": "1970-01-01"}

#: What the compact partition clock holds. It stays an integer so identity
#: partitions remain portable between Arrow and Iceberg engines.
UNIX_PARTITION: dict[str, str] = {"unit": "second", "epoch": "1970-01-01"}

#: Nanoseconds in a microsecond, the resolution an identity is anchored at.
MICROSECOND = 1_000

#: Nanoseconds in a second.
SECOND = 1_000_000_000

#: Nanoseconds in a day.
DAY = 86_400_000_000_000

#: Nanoseconds in an hour, used to locate `unix`'s partition boundary.
HOUR = 3_600_000_000_000

_CONTRACT_METADATA = MappingProxyType({"version": "2"})

# A parsed row's identity belongs to that row, not to any market event built
# from it. Explicit overrides may still supply these values for a native row.
_SOURCE_IDENTITY_MEMBERS = frozenset(
    (
        "unixpartition",
        "eventtype",
        "snapunix",
        "hash",
        "vhash",
        "xhash",
        "linkhashes",
        "version",
        "state",
        "code",
        "prevunix",
        "prevhash",
        "parenthash",
    )
)

_MISSING = object()

_INSTRUMENT_ALTID_NAMES = frozenset(
    {
        "bloombergcode",
        "cusip",
        "exchangeid",
        "legsecurityid",
        "legsecurityidsource",
        "legsymbol",
        "instrumentid",
        "isin",
        "isincode",
        "ric",
        "securityaltid",
        "securityaltidsource",
        "securityexchange",
        "securityid",
        "securityidsource",
        "sedol",
        "symbol",
        "symbolticker",
        "underlying",
        "underlyingsecurityid",
        "underlyingsecurityidsource",
        "underlyingsymbol",
    }
)


def _market_altids(values: Mapping[str, str]) -> dict[str, str]:
    """Fold lifecycle identifiers and discard instrument reference codes."""
    found: dict[str, str] = {}
    for name, value in values.items():
        folded = column_name(name)
        if folded and folded not in _INSTRUMENT_ALTID_NAMES and value:
            found.setdefault(folded, str(value))
    return found


#: Every readable identity carried by a row, keyed by its folded field name.
#: Lookup code decides which identities are comparable; storage does not drop
#: a code merely because the row also promotes it into a dedicated column.
ALTIDS_TYPE = pyarrow.map_(
    pyarrow.string(), pyarrow.field("value", pyarrow.string(), nullable=False)
)

#: Relations name exact event versions, at the same width as `hash`.
_LINK_HASHES_TYPE = pyarrow.list_(pyarrow.field("item", HASH, nullable=False))

#: The list a `parenthash` is, at the width an identifier is stored in.
_PARENT_HASH_TYPE = pyarrow.list_(pyarrow.field("item", HASH, nullable=False))


@scalar(slots=True, weakref_slot=True)
class Event(MarketConvertible):
    """One immutable version of one thing that happened, and its place in a life."""

    @classmethod
    @functools.cache
    def into_redirects(cls) -> Mapping[Any, str]:
        """Generic conversions plus parsed FIX rows and raw entries."""
        from rekep.entries import Entry
        from rekep.text.fixmsg import FixMsg

        return MappingProxyType(
            {
                **MarketConvertible.into_redirects(),
                FixMsg: "fixmsg",
                Entry: "entries",
                list: "entries",
                tuple: "entries",
            }
        )

    @classmethod
    @functools.cache
    def into_field_metadata(cls) -> Mapping[str, str]:
        """Contract metadata inherited by every event shape."""
        return _CONTRACT_METADATA

    @classmethod
    @functools.cache
    def into_event_type(cls) -> EventType:
        """Event kind fixed by this concrete shape."""
        return EventType.UNKNOWN

    unix: Annotated[int, Field.primary_key(metadata=UNIX)] = 0
    """When the event happened, in whole nanoseconds since the epoch."""

    # Denormalised from `unix` rather than partitioned with an `hour`
    # transform: an identity partition on a real column is the one form every
    # engine below reads the same way, and a transformed one needs Iceberg's
    # Rust core on the writer for no gain a reader can see.
    #
    # An **hour**, and an integer rather than a date. A day of ticks is one
    # partition at day granularity, which prunes nothing inside a session --
    # the query everybody actually writes. Whole epoch seconds keep identity
    # partition paths compact while `unix` retains nanosecond precision.
    #
    # `derived_from` is the third thing this buys: a merge joins on `unix` and
    # `hash`, which names no partition column and so prunes nothing at the
    # manifest list -- it scales with the table, not with the chunk. Saying
    # that `unixpartition` is a function of `unix` lets a merge name it anyway,
    # because rows agreeing on `unix` agree here. Replaying one hour: 20 ms
    # against 93 over 168 hourly partitions, 27 against 164 over 336.
    unixpartition: Annotated[
        int,
        Field.partition_key(dtype=pyarrow.int32(), derived_from="unix", metadata=UNIX_PARTITION),
        Field.column("UnixPartition"),
    ] = 0
    """`unix`'s hour boundary in whole epoch seconds; the partition value."""

    # Third, not last: a read that spans the tables filters on it before
    # anything else, and Iceberg's column bounds are collected in pre-order.
    # Constant within one table, where run-length and dictionary encoding
    # collapse it to nothing.
    eventtype: Annotated[EventType, Field.column("EventType")] = EventType.UNKNOWN
    """Which kind of event this is -- the one column a union of the tables needs."""

    plugin: Annotated[Plugin, Field.column("Plugin")] = Plugin.UNKNOWN
    """Source plugin; UNKNOWN when missing or longer than 16 ASCII bytes."""

    creaunix: Annotated[int, Field(metadata=UNIX), Field.column("CreaUnix")] = 0
    """When the event was created, upstream of anything that carried it."""

    recunix: Annotated[int, Field(metadata=UNIX), Field.column("RecUnix")] = 0
    """When the event was written down here; deliberately not part of `hash`."""

    expunix: Annotated[int | None, Field(metadata=UNIX), Field.column("ExpUnix")] = None
    """When the event stops being true -- an order's expiry, a quote's staleness."""

    # A snapshot's own `unix` is when the picture was taken, because that is
    # what orders it against everything else in the stream. What it is a
    # picture *of* would otherwise be lost: `snapunix` keeps it, so "as of when"
    # and "taken when" are both on the row and a stale snapshot is one
    # subtraction rather than a join against whatever it snapshotted.
    snapunix: Annotated[int | None, Field(metadata=UNIX), Field.column("SnapUnix")] = None
    """`unix` of the event this is a snapshot of; null when it is not one."""

    # The high half is `unix` in whole microseconds; equal clocks are still
    # distinguished by the value identity in the low half.
    hash: Annotated[int, Field.primary_key(dtype=HASH)] = NIL
    """Time-anchored composition of `unix` and `vhash`."""

    vhash: Annotated[int, Field(dtype=pyarrow.int64()), Field.column("ValueHash")] = NIL
    """XXH3-64 of the framed value parts, with every clock excluded."""

    xhash: Annotated[int, Field(dtype=HASH), Field.column("XHash")] = NIL
    """Direct XXH3-128 digest of `code`; all-zero when no readable code exists."""

    linkhashes: Annotated[
        list[int],
        Field(dtype=_LINK_HASHES_TYPE),
        Field.column("LinkHashes"),
    ] = dataclasses.field(default_factory=list)
    """Related exact event `hash` values, with the primary match first."""

    version: int = 0
    """Which version of `xhash` this is, counting up from the first."""

    state: State = State.UNKNOWN
    """Where the lifecycle stands, as a ranked code: `is_terminal` is over."""

    code: str = ""
    """Readable identifier of this lifecycle, shared by every version of it."""

    altids: Annotated[dict[str, str], Field(dtype=ALTIDS_TYPE), Field.column("AltIDs")] = (
        dataclasses.field(default_factory=dict)
    )
    """Other identifiers keyed by reference scheme or lifecycle field name."""

    prevunix: Annotated[int | None, Field(metadata=UNIX), Field.column("PrevUnix")] = None
    """When the previous version happened, so dwell time is a subtraction."""

    prevhash: Annotated[int | None, Field(dtype=HASH), Field.column("PrevHash")] = None
    """The previous version's hash; null on the first version."""

    # Parent hashes record directed construction provenance. Peer relations
    # stay separate because they carry no parent/child direction.
    parenthash: Annotated[
        list[int] | None, Field(dtype=_PARENT_HASH_TYPE), Field.column("ParentHash")
    ] = None
    """Every event this one was built from, in the order they were combined."""

    lastmkt: Annotated[MIC | None, fix_tag("LastMkt")] = None
    """Last execution venue as an ISO 10383 code; null when absent."""

    reason: str | None = None
    """Why this event was rejected or could not be interpreted, when known."""

    def __post_init__(self) -> None:
        """Make the members agree, so everything downstream can assume they do."""
        # Arrow reads a map back as a list of pairs, and a row that made the
        # round trip is the same row: normalize once here rather than at every
        # reader of `altids`.
        if not isinstance(self.altids, dict):
            self.altids = dict(self.altids or ())
        self.plugin = Plugin.from_str(self.plugin)
        self._name_codes()
        if self.lastmkt is not None:
            venue = MIC.from_str(self.lastmkt)
            self.lastmkt = None if venue is MIC.UNKNOWN else venue
        self.normalize_float_members()
        if self.eventtype is EventType.UNKNOWN:
            self.eventtype = type(self).into_event_type()
        self.unixpartition = _unix_partition_of(self.unix)
        self._drop_self_link()

    @classmethod
    def from_fixmsg(
        cls,
        source: Any,
        *,
        registry: FixRegistry | None = None,
        **overrides: Any,
    ) -> Self:
        """Build from promoted FIX columns, then residual entries."""
        from rekep.fix.access import FieldAccess
        from rekep.text.fixmsg import FixMsg

        if not isinstance(source, FixMsg):
            raise TypeError(f"source must be FixMsg, got {type(source).__name__}")
        selected = registry or source.registry
        access = FieldAccess.of(selected, source.resolved_version(selected))
        values = _promoted_values(cls, source, access)
        for name, value in _entry_values(cls, source.entries or (), access).items():
            values.setdefault(name, value)
        values.update(overrides)
        return cls.from_dict(values)

    @classmethod
    def from_arrow_reader(
        cls, source: pyarrow.RecordBatchReader | Iterable[pyarrow.RecordBatch]
    ) -> Iterator[Self]:
        """Build event objects lazily from a schema-checked Arrow stream."""
        reader = cls.into_field().cast_arrow_reader(source)
        for batch in reader:
            yield from (cls.from_dict(row) for row in batch.to_pylist())

    @classmethod
    def into_arrow_reader(
        cls, events: Iterable[Self], batch_row_size: int = 65_536
    ) -> pyarrow.RecordBatchReader:
        """Serialize event objects as bounded Arrow batches."""
        if batch_row_size <= 0:
            raise ValueError("batch_row_size must be positive")
        schema = cls.into_field().into_arrow_schema()

        def batches() -> Iterator[pyarrow.RecordBatch]:
            held: list[Self] = []
            for event in events:
                held.append(event)
                if len(held) >= batch_row_size:
                    yield cls.into_arrow_batch(held)
                    held.clear()
            if held:
                yield cls.into_arrow_batch(held)

        return pyarrow.RecordBatchReader.from_batches(schema, batches())

    # -- what kind of event this is -----------------------------------------

    @classmethod
    def is_a(cls, kind: EventType) -> bool:
        """Whether this shape is `kind`, or anything inside `kind`'s band.

        The one comparison the named questions below are made of, so a band
        (`EventType.STATE`) and a member (`EventType.BOOK`) are both answerable
        without a caller knowing which it was handed.
        """
        declared = cls.into_event_type()
        return declared == kind or (kind.band is kind and declared.band is kind)

    @classmethod
    def is_order(cls) -> bool:
        """Whether this shape is an order."""
        return cls.is_a(EventType.ORDER)

    @classmethod
    def is_execution(cls) -> bool:
        """Whether this shape is an execution report."""
        return cls.is_a(EventType.EXECUTION)

    @classmethod
    def is_book(cls) -> bool:
        """Whether this shape is a whole book."""
        return cls.is_a(EventType.BOOK)

    @classmethod
    def is_snapshot(cls) -> bool:
        """Whether this shape is a picture of something rather than a thing itself."""
        return cls.into_event_type().is_snapshot

    # -- identity -----------------------------------------------------------

    @classmethod
    def hash_of(cls, *parts: Any) -> int:
        """The identifier `parts` name, for this shape.

        The class name goes in front of the parts, so an `Order` and a `Book`
        built from the same code and time cannot land on one identifier --
        which is a collision no amount of hash width prevents, because the
        inputs really are equal.
        """
        return hash_of(cls.__name__, *parts)

    @classmethod
    def hash_arrow(cls, *columns: Any) -> pyarrow.Array:
        """`hash_of` over whole columns: one identifier per row, in kernels."""
        return hash_arrow(cls.__name__, *columns)

    @staticmethod
    def _clock_micros(clock: Any) -> pyarrow.Array:
        """One clock column as whole epoch microseconds, flooring like `//` does."""
        if not pyarrow.types.is_integer(getattr(clock, "type", None)):
            return txhash.epoch_micros_arrow(clock)
        compute = pyarrow.compute
        nanos = clock.cast(pyarrow.int64(), safe=False)
        carry = compute.if_else(
            compute.less(nanos, 0), pyarrow.scalar(MICROSECOND - 1, pyarrow.int64()), 0
        )
        return compute.divide(compute.subtract(nanos, carry), MICROSECOND).cast(pyarrow.int64())

    @staticmethod
    def xhash_of(code: str) -> int:
        """Lifecycle identity, or zero when no readable code names one."""
        return hash128_bytes(code.encode("utf-8")) if code else NIL

    @classmethod
    def xhash_arrow(cls, code: Any) -> pyarrow.Array:
        """`xhash_of` over a whole code column."""
        compute = pyarrow.compute
        text = code.cast(pyarrow.string(), safe=False)
        named = compute.fill_null(compute.not_equal(text, ""), False)
        return compute.if_else(named, hash128_bytes_arrow(text), pyarrow.scalar(NIL_BYTES, HASH))

    def identify(self) -> Self:
        """Give the event the identity its own content earns, where it has none."""
        self._materialize_life_code()
        self.xhash = self.xhash or self.life_hash()
        self._drop_self_link()
        if not self.vhash:
            self.vhash = self.hash_of(*self.version_parts())
        if not self.hash:
            self.hash = txhash.couple128(self.unix // MICROSECOND, self.vhash)
        self._drop_self_link()
        return self

    def with_previous(self, previous: Event | None) -> Self | None:
        """Complete this version, returning None when it changes no stored fact."""
        self.completed_from(previous)
        related = previous is not None and (
            type(self) is not type(previous) or self.xhash != previous.xhash
        )
        self.vhash = self.hash_of(*self.version_parts())
        self.hash = NIL
        if (
            previous is not None
            and type(self) is type(previous)
            and self.xhash == previous.xhash
            and self.vhash == previous.vhash
            and self.snapunix is None
            and previous.snapunix is None
        ):
            return None
        identified = self.identify()
        if related:
            # A relation names exact versions, so neither side may still be
            # waiting for the content hash that identifies it.
            identified.link_to(previous, primary=True)
        return identified

    def completed_from(self, previous: Event | None) -> Self:
        """Complete inherited and derived values without assigning identities."""
        if previous is None:
            self.derive()
            self._materialize_life_code()
            self.xhash = self.xhash or self.life_hash()
            self._drop_self_link()
            self.vhash = self.hash = NIL
            return self
        self.complete_from(previous)
        self.derive()
        self._materialize_life_code()
        same_shape = type(self) is type(previous)
        same_lifecycle = same_shape and bool(
            (self.xhash and self.xhash == previous.xhash)
            or (self.code and self.code == previous.code)
            or not self.code
        )
        if same_lifecycle:
            # The readable code is the lifecycle identity. Keep its original
            # spelling when a later version names the same digest indirectly.
            self.code = previous.code or self.code
            self._keep_creation(previous)
            self.xhash = previous.xhash or self.life_hash()
        else:
            self.xhash = self.life_hash()
        self._drop_self_link()
        if same_lifecycle:
            # The row carried no readable key of its own, so the same
            # lifecycle is the only honest place its readable key can come
            # from. Never copy it before this comparison: completion crosses
            # shapes, and an execution is not named by its order's code.
            self.code = previous.code or self.code
            self._keep_lifecycle_altids(previous)
            self._name_codes()
            self.version = previous.version + 1
            self.prevunix = previous.unix
            self._remember_previous(previous)
        else:
            # Not a version of it: a different thing, built from it. A fill
            # completed from the order it happened to is the case that matters
            # -- it takes the order's running totals and stays version zero of
            # its own life, because a version counter counts one lifecycle.
            # Relations are added by the producer only after both exact event
            # hashes are final; construction provenance is already final here.
            if previous.hash and previous.hash not in (self.parenthash or ()):
                self.parenthash = [*(self.parenthash or ()), previous.hash]
        # Cleared, not kept: every layer has just filled fields the hash is
        # made of, so the identity this row arrived with was of a different
        # row. `identify` refuses to overwrite a hash that is set, which is
        # what makes clearing it the way to ask for a new one.
        self.vhash = self.hash = NIL
        return self

    def _completed_from_same_lifecycle(self, previous: Event) -> Self:
        """Complete a version whose lifecycle hash already matches its predecessor."""
        self.complete_from(previous)
        self._keep_creation(previous)
        self.derive()
        self._materialize_life_code()
        self.xhash = previous.xhash
        self._drop_self_link()
        self.code = previous.code or self.code
        self._keep_lifecycle_altids(previous)
        self._name_codes()
        self.version = previous.version + 1
        self.prevunix = previous.unix
        self._remember_previous(previous)
        self.vhash = self.hash = NIL
        return self

    def _keep_lifecycle_altids(self, previous: Event) -> None:
        """Carry absent identifiers while keeping this version's observed values."""
        merged = dict(previous.altids)
        for name, value in self.altids.items():
            if value:
                merged[name] = value
        self.altids = merged

    def _keep_creation(self, previous: Event) -> None:
        """Keep a lifecycle's known creation; a later row may fill an unknown one."""
        if previous.creaunix:
            self.creaunix = previous.creaunix

    def complete_from(self, previous: Event) -> None:
        """Fill what this version left absent, from the version before it."""
        if previous.linkhashes:
            self.link_to(*previous.linkhashes)
        if not self.unix:
            # A message with no clock at all still belongs somewhere in time,
            # and the only honest answer is where the version before it was.
            self.unix = previous.unix
            self.unixpartition = _unix_partition_of(self.unix)
        if not self.recunix:
            self.recunix = previous.recunix
        if self.expunix is None:
            self.expunix = previous.expunix
        # Not `code`: that names a lifecycle, and completion crosses lifecycles
        # -- an execution completed from its order is not named by the order.
        # The identifiers *beside* it are facts about the same thing under any
        # reading, so a version silent about one keeps what the last one said.
        self.name_altids(previous.altids)
        if self.lastmkt is None:
            self.lastmkt = previous.lastmkt
        if self.state is State.UNKNOWN:
            # A report that says nothing about the state is not saying the
            # state is unknown; it is not mentioning it.
            self.state = previous.state

    def _remember_previous(self, previous: Event) -> None:
        """Store shape-specific transition values after lifecycle matching."""
        self.prevhash = previous.hash or None

    def link_to(self, *events: Event | int, primary: bool = False) -> Self:
        """Relate exact event versions once, optionally ahead of existing links."""
        if not events:
            return self
        given: list[int] = []
        for event in events:
            event_hash = event.hash if isinstance(event, Event) else int(event)
            if event_hash:
                given.append(int(event_hash))
        given = list(dict.fromkeys(given))
        existing = list(self.linkhashes)
        ordered = given + existing if primary else existing + given
        self.linkhashes = list(dict.fromkeys(ordered))
        self._drop_self_link()
        return self

    @property
    def primary_link(self) -> int | None:
        """First related exact event version, when one is known."""
        return self.linkhashes[0] if self.linkhashes else None

    def _drop_self_link(self) -> None:
        """A relation never points back to its own exact event version."""
        if self.hash and self.linkhashes:
            self.linkhashes = [linked for linked in self.linkhashes if linked != self.hash]

    @staticmethod
    def _without_self_links_arrow(linkhashes: Any, hashes: Any) -> pyarrow.Array:
        """Remove each row's own exact hash from its Arrow relation list."""
        compute = pyarrow.compute
        if isinstance(linkhashes, pyarrow.ChunkedArray):
            linkhashes = linkhashes.combine_chunks()
        if isinstance(hashes, pyarrow.ChunkedArray):
            hashes = hashes.combine_chunks()
        values = compute.list_flatten(linkhashes)
        if not len(values):
            return linkhashes
        parents = compute.list_parent_indices(linkhashes).cast(pyarrow.int64())
        self_links = compute.fill_null(compute.equal(values, compute.take(hashes, parents)), False)
        if not compute.any(self_links, min_count=0).as_py():
            return linkhashes
        keep = compute.invert(self_links)
        kept_parents = compute.filter(parents, keep)
        return build_list(
            linkhashes.type,
            dense_counts(kept_parents, len(linkhashes)),
            compute.filter(values, keep),
            null_mask(linkhashes),
        )

    def derive(self) -> None:
        """Fill what this row's own fields already determine.

        Nothing at the envelope's layer: an event's own time, state and
        identity are given, never computed from each other. Subclasses have
        real work here, and this exists so every one of them can call
        `super().derive()` without knowing that.
        """

    def make_snapshot(self, unix: int, *, same_unix: bool = False) -> Self | None:
        """Picture this live state, expiring it after one unchanged day."""
        if not type(self).is_snapshot() or (self.state.is_terminal and not same_unix):
            return None
        if unix < self.unix or (unix == self.unix and not same_unix):
            return None
        if self.snapunix is not None and self.snapunix >= unix:
            return None
        taken = copy.copy(self)
        taken.unix = unix
        taken.unixpartition = _unix_partition_of(unix)
        # What it is a picture *of*: the instant of the state, which for a
        # picture of a picture is the original state and not the middle one.
        # Two snapshots of one unchanged book then agree on what they show and
        # differ only in when they were taken, which is the truth about them.
        taken.snapunix = self.snapunix if self.snapunix is not None else self.unix
        if unix - taken.snapunix >= DAY:
            taken.state = State.INTERNAL_EXPIRED
            taken.expunix = taken.snapunix + DAY
            taken.reason = taken.reason or "expired internally after one day without change"
        taken.forget_delta()
        # Cleared so `identify` derives one: the row differs from the one it
        # was copied from, in the two fields that say it is a picture.
        taken.vhash = taken.hash = NIL
        return taken

    def forget_delta(self) -> None:
        """Drop what *changed*, keeping what *is*. A snapshot has no delta."""

    def life_hash(self) -> int:
        """The lifecycle identity its readable code names."""
        return self.xhash_of(self.life_code())

    def life_code(self) -> str:
        """The readable part that names this lifecycle, without changing it."""
        return self.code

    def _materialize_life_code(self) -> None:
        """Store the readable lifecycle part once, before mutable aliases move."""
        self.code = self.code or self.life_code()
        self._name_codes()

    def _code_values(self) -> Iterator[tuple[str, str | None]]:
        """Every dedicated code column this row carries."""
        yield "code", self.code

    def _name_codes(self) -> None:
        """Keep every dedicated code in the persisted identifier map."""
        for name, value in self._code_values():
            if value:
                self.altids[name] = str(value)

    def name_altid(self, name: str, value: str | None) -> None:
        """Record one identifier this row carried, without displacing one it has."""
        if value and not self.altids.get(name):
            self.altids[name] = value

    def name_altids(self, altids: Mapping[str, str]) -> None:
        """Record several, in one pass and under the same rule."""
        for name, value in altids.items():
            self.name_altid(name, value)

    def version_parts(self) -> tuple[Any, ...]:
        """Current non-clock values in the lifecycle's framed hash domain."""
        # Lifecycle relations and parent provenance navigate between rows;
        # neither rewrites the value identity of a row when a peer is learned.
        return (
            self.eventtype,
            self.state,
            self.lastmkt,
            self.code,
            self.reason,
            *_mapping_parts(self.altids),
        )


@scalar(slots=True)
class MarketEvent(Event):
    """A priced event carrying a flat instrument identity."""

    # Parsed instrument facts are useful while folding but are deliberately
    # not a member: market events persist only the flat identity.
    __instrument: Instrument | None = None

    # Flat and first because an engine pushes a predicate down only for a
    # top-level scalar. The canonical ticker is both the readable key and the
    # one identity used to group market operations.
    #
    # Not a partition, and this is measured rather than argued. The
    # case for bucketing it is real -- `unixpartition` prunes time and not
    # instrument, so a scan for one instrument across a week opens every
    # hour's files. But bucketing does not fix that: the bucket prunes files
    # *inside* an hour, and a scan across N hours still opens at least one
    # file per hour, which is what the query actually pays for. Over 144,000
    # rows across 72 hours and 40 instruments, one instrument's week cost
    # 632 ms on `unixpartition` alone, 650 ms at bucket[8] and 671 ms at
    # bucket[16] -- slower, not faster -- while the file count went 72 to 576
    # to 1,152, the mean file fell from 76 KiB to 25, and the hourly read
    # every consumer writes went 24 ms to 165 to 320. See docs/market/index.md.
    symbolticker: Annotated[str, Field.column("SymbolTicker")] = ""
    """Canonical instrument key; empty when the source cannot identify it."""

    kind: MarketKind = MarketKind.UNKNOWN
    """Standard market semantic, independent of its protocol spelling."""

    side: Annotated[Side, fix_tag("Side")] = Side.UNKNOWN
    """Which way the interest points; `side.sign` turns it into `+1` or `-1`."""

    lastpx: Annotated[float | None, fix_tag("LastPx")] = None
    """The current event price in `pxunit`; each subclass declares its exact source."""

    prevpx: Annotated[float | None, Field.column("PrevPx")] = None
    """Price on the immediately preceding lifecycle version; null on the first."""

    # Ours, and so carrying no FIX tag: it normalises `PriceType <423>`, which
    # is an enumeration of conventions, together with `Currency <15>`, which is
    # the unit -- and a tag naming either would label the column as a field it
    # is not. NOT NULL with an empty placeholder: a
    # producer always knows how it quotes, and a column widened later is a
    # column every reader written before the widening has to re-handle.
    pxunit: Annotated[str, Field.column("PxUnit")] = ""
    """How to read `lastpx`: a currency, or `PCT`, `BPS`, `YIELD`; empty when unstated."""

    currency: Annotated[Currency | None, fix_tag("Currency")] = None
    """Currency of the monetary values; null when the source does not state one."""

    lastqty: Annotated[float | None, fix_tag("LastQty")] = None
    """The quantity on this row, in `qtyunit`; what it means is the subclass's to say."""

    prevqty: Annotated[float | None, Field.column("PrevQty")] = None
    """Quantity on the immediately preceding lifecycle version; null on the first."""

    qtyunit: Annotated[str, Field.column("QtyUnit")] = ""
    """How to read `lastqty`: `SHARES`, `LOTS`, `NOMINAL`; empty when unstated."""

    # Carried rather than derived after persistence because the multiplier is
    # normalized in the separate reference stream.
    notional: float | None = None
    """`lastpx * lastqty * multiplier` in the instrument's currency, as computed."""

    prevnotional: Annotated[float | None, Field.column("PrevNotional")] = None
    """Notional on the immediately preceding lifecycle version; null on the first."""

    # Free-form protocol metadata stays separate from declared columns.
    metadata: dict[str, str] | None = None
    """Protocol fields carried verbatim, exactly as the venue sent them."""

    def __post_init__(self) -> None:
        """Normalize the compact currency code and inherited clocks."""
        self.symbolticker = SymbolTicker.from_str(self.symbolticker).into_str()
        if self.currency is not None:
            self.currency = Currency.from_str(self.currency)
        Event.__post_init__(self)
        self.altids = _market_altids(self.altids)

    def name_altid(self, name: str, value: str | None) -> None:
        """Record one folded lifecycle identifier, never an instrument reference."""
        folded = column_name(name)
        if folded and folded not in _INSTRUMENT_ALTID_NAMES:
            Event.name_altid(self, folded, value)

    def _keep_lifecycle_altids(self, previous: Event) -> None:
        """Carry lifecycle identifiers without reintroducing reference codes."""
        Event._keep_lifecycle_altids(self, previous)
        self.altids = _market_altids(self.altids)

    def attach_instrument(self, instrument: Instrument) -> Self:
        """Use reference data while building without adding it to the event schema."""
        self.__instrument = instrument
        self.symbolticker = self.symbolticker or instrument.symbolticker
        if self.currency is None:
            self.currency = instrument.currency
        return self

    def into_instrument(self) -> Instrument | None:
        """Return transient parsed reference data, absent after persisted reads."""
        return getattr(self, "_MarketEvent__instrument", None)

    def complete_from(self, previous: Event) -> None:
        """The four market slots, carried forward where this version was silent.

        `lastpx` and `lastqty` are filled because a venue restating an order's status
        rarely repeats its limit, and a row with a null price drops out of
        every filter on price.
        """
        Event.complete_from(self, previous)
        if not isinstance(previous, MarketEvent):
            return
        if self.into_instrument() is None:
            known = previous.into_instrument()
            if known is not None:
                self.__instrument = known
        if not self.symbolticker:
            self.symbolticker = previous.symbolticker
        if self.side is Side.UNKNOWN:
            self.side = previous.side
        # `lastpx` and `lastqty` are the abstract slots, and what they hold is the
        # subclass's to say: an order's are what it asked for, an execution's
        # are what traded, a book's are the mid and the touch. So they carry
        # only from a version of the *same shape*. Carrying an execution's
        # `LastQty` into an order's `OrderQty` made a partly filled order
        # claim its current quantity was exactly what had just traded.
        if previous.eventtype == self.eventtype:
            if self.kind is MarketKind.UNKNOWN:
                self.kind = previous.kind
            if self.lastpx is None:
                self.lastpx = previous.lastpx
            if self.lastqty is None:
                self.lastqty = previous.lastqty
        if not self.pxunit:
            self.pxunit = previous.pxunit
        if self.currency is None:
            self.currency = previous.currency
        if not self.qtyunit:
            self.qtyunit = previous.qtyunit

    def _remember_previous(self, previous: Event) -> None:
        """Keep the prior priced values only after lifecycle identity agrees."""
        Event._remember_previous(self, previous)
        if not isinstance(previous, MarketEvent):
            return
        if self.prevpx is None:
            self.prevpx = previous.lastpx
        if self.prevqty is None:
            self.prevqty = previous.lastqty
        if self.prevnotional is None:
            self.prevnotional = previous.notional

    def derive(self) -> None:
        """A notional is a price times a quantity times a multiplier, or nothing."""
        Event.derive(self)
        if self.notional is None:
            self.notional = self.into_notional()

    def into_notional(self) -> float | None:
        """`lastpx * lastqty * multiplier` in the instrument's currency, or None.

        None when any of the three is missing, and that is the point of it
        being a method rather than an expression at the call site: a notional
        computed with a multiplier of "probably one" is wrong by a factor
        nobody notices until settlement, so a contract whose multiplier is
        unknown has no notional rather than a plausible one.
        """
        if self.lastpx is None or self.lastqty is None:
            return None
        instrument = self.into_instrument()
        if instrument is None:
            return None
        multiplier = instrument.contractmultiplier
        if multiplier is None:
            # A cash instrument really does trade one for one, and it is the
            # only class where the multiplier can be assumed rather than read.
            # `band` answers with the band-floor member itself, not a number,
            # so this is `is` and never `==`.
            if instrument.kind.band is not AssetKind.CASH:
                return None
            multiplier = 1.0
        return self.lastpx * self.lastqty * multiplier

    def forget_delta(self) -> None:
        """A snapshot keeps current values, never the transition into them."""
        Event.forget_delta(self)
        self.prevpx = None
        self.prevqty = None
        self.prevnotional = None

    def life_code(self) -> str:
        """The lifecycle identifier, and the instrument ticker when there is none.

        A market event that names no order and no report is still an event
        about one instrument on one side, and the ticker is the readable half
        of that. It is a fallback and never a preference: a row carrying an
        order identifier is named by the order.
        """
        return self.code or self.symbolticker

    def version_parts(self) -> tuple[Any, ...]:
        """Current non-clock market values in the framed hash domain."""
        return (
            *Event.version_parts(self),
            self.symbolticker,
            self.kind,
            self.side,
            self.lastpx,
            self.pxunit,
            self.currency,
            self.lastqty,
            self.qtyunit,
            self.notional,
            *_mapping_parts(self.metadata),
        )

    @classmethod
    def from_books_arrow_batch(cls, books: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        """Flatten this event shape from one batch of carrying books."""
        if cls.is_order():
            column = "deltas"
        elif cls.is_execution():
            column = "executions"
        else:
            raise TypeError(f"{cls.__name__} is not a book event shape")
        from rekep.fields.arrays import struct_columns

        listed = books.column(column)
        parents = pyarrow.compute.list_parent_indices(listed)
        events = pyarrow.compute.list_flatten(listed)
        field = cls.into_field()
        if not len(events):
            return pyarrow.RecordBatch.from_arrays(
                [pyarrow.array([], type=member.dtype) for member in field.fields],
                schema=field.into_arrow_schema(),
            )
        projected = struct_columns(events)
        projected["parenthash"] = _append_list_value(
            projected["parenthash"], books.column("hash").take(parents)
        )
        raw = pyarrow.RecordBatch.from_arrays(
            [projected[name] for name in field.names], names=field.names
        )
        return field.cast_arrow_batch(raw)


def unix_partition_arrow(unix: Any) -> pyarrow.Array:
    """Return `unix`'s hour boundary as whole epoch seconds.

    The columnar twin of what `__post_init__` computes for one row, and here
    rather than in a parser because the rule belongs to the column: a
    partition that stopped being a function of `unix` would break every
    ordered read that prunes on it.
    """
    compute = pyarrow.compute
    hour = pyarrow.scalar(HOUR, pyarrow.int64())
    remainder = compute.subtract(unix, compute.multiply(compute.divide(unix, hour), hour))
    floored = compute.subtract(
        unix,
        compute.if_else(compute.less(remainder, 0), compute.add(remainder, hour), remainder),
    )
    return compute.divide(floored, pyarrow.scalar(SECOND, pyarrow.int64())).cast(pyarrow.int32())


def _unix_partition_of(unix: int) -> int:
    """Return one nanosecond instant's hour boundary as epoch seconds."""
    return (unix - unix % HOUR) // SECOND


def _mapping_parts(values: Mapping[str, Any] | None) -> tuple[Any, ...]:
    """One optional mapping in deterministic key order."""
    if values is None:
        return (False, 0)
    ordered = sorted(values.items(), key=lambda item: item[0].encode("utf-8"))
    return (True, len(ordered), *(part for pair in ordered for part in pair))


def _mapping_frame_arrow(values: pyarrow.Array) -> pyarrow.Array:
    """Optional map entries as deterministic identity-frame segments."""
    item = pyarrow.struct(
        [
            pyarrow.field("key", values.type.key_type, nullable=False),
            pyarrow.field("value", values.type.item_type, nullable=values.type.item_field.nullable),
        ]
    )
    listed = values.cast(pyarrow.list_(item), safe=False)
    counts = pyarrow.compute.fill_null(pyarrow.compute.list_value_length(listed), 0).cast(
        pyarrow.int64()
    )
    entries = pyarrow.compute.list_flatten(listed)
    parents = pyarrow.compute.list_parent_indices(listed).cast(pyarrow.int64())
    keys = pyarrow.compute.struct_field(entries, "key")
    items = pyarrow.compute.struct_field(entries, "value")
    if len(entries):
        order = pyarrow.compute.sort_indices(
            pyarrow.record_batch([parents, keys], names=["parent", "key"]),
            sort_keys=[("parent", "ascending"), ("key", "ascending")],
        )
        keys = pyarrow.compute.take(keys, order)
        items = pyarrow.compute.take(items, order)
    entry_frames = (
        framed_arrow(keys, items) if len(entries) else pyarrow.array([], pyarrow.binary())
    )
    grouped = build_list(pyarrow.list_(pyarrow.binary()), counts, entry_frames)
    payload = pyarrow.compute.binary_join(grouped, pyarrow.scalar(b"", pyarrow.binary()))
    return pyarrow.compute.binary_join_element_wise(
        framed_arrow(pyarrow.compute.is_valid(values), counts),
        payload,
        pyarrow.scalar(b"", pyarrow.binary()),
    )


@functools.cache
def _declared_members(cls: type[MarketConvertible]) -> tuple[tuple[Field, Any], ...]:
    """Arrow members paired with their resolved Python declarations."""
    hints = get_type_hints(cls, include_extras=True)
    return tuple((member, hints[member.name]) for member in cls.into_field().fields)


def _promoted_values(cls: type[MarketConvertible], source: Any, access: Any) -> dict[str, Any]:
    """Declaration-matched promoted values carried by one source row."""
    source_fields = _source_fields(source)
    values: dict[str, Any] = {}
    for member, annotation in _declared_members(cls):
        if issubclass(cls, Event) and member.name in _SOURCE_IDENTITY_MEMBERS:
            continue
        value = _promoted_value(member, source, source_fields, access)
        if value is _MISSING:
            continue
        values[member.name] = _projected_value(member, annotation, value, access)
    return values


def _source_fields(source: Any) -> tuple[Field, ...]:
    """The Arrow declaration a promoted source carries, when it has one."""
    into_field = getattr(type(source), "into_field", None)
    if into_field is None:
        return ()
    try:
        return tuple(into_field().fields)
    except (AttributeError, TypeError):
        return ()


def _promoted_value(
    member: Field,
    source: Any,
    source_fields: tuple[Field, ...],
    access: Any,
) -> Any:
    """First non-empty source member declaring the same field."""
    for candidate in source_fields:
        if not _same_field(member, candidate, access):
            continue
        value = getattr(source, candidate.name, _MISSING)
        if value is not _MISSING and _has_value(value):
            return value
    if isinstance(source, Mapping):
        exact = source.get(member.name, _MISSING)
        if exact is not _MISSING and _has_value(exact):
            return exact
        from rekep.entries import Entry

        pairs = (Entry.of(key=str(key), value=value) for key, value in source.items())
        reading = _reading(member, pairs, access)
        if reading:
            return reading.value
    elif not source_fields:
        exact = getattr(source, member.name, _MISSING)
        if exact is not _MISSING and _has_value(exact):
            return exact
    return _MISSING


def _same_field(target: Field, source: Field, access: Any) -> bool:
    """Whether two declarations name one registry field."""
    target_fix = _fix_backed(target)
    source_fix = _fix_backed(source)
    if target_fix != source_fix:
        return False
    if not target_fix:
        return target.name == source.name
    left = access.resolve(target.fix.canonical)
    right = access.resolve(source.fix.canonical)
    if left.tag is not None and left.tag == right.tag:
        return True
    return bool(left.names & right.names)


def _fix_backed(member: Field) -> bool:
    """Whether a declaration names a registry-owned field."""
    # `fix:name` is also the reader-facing spelling of a generic column. The
    # registry adds the datatype, including for named fields with no tag, so
    # it is the discriminator that cannot turn an envelope column into FIX.
    return bool(member.fix.type)


def _has_value(value: Any) -> bool:
    """Whether a promoted value says more than its missing sentinel."""
    if value is None or value == "":
        return False
    if isinstance(value, enum.Enum) and not int(value):
        return False
    if isinstance(value, Mapping | list | tuple | set | frozenset) and not value:
        return False
    return True


def _entry_values(
    cls: type[MarketConvertible], entries: Iterable[Any], access: Any
) -> dict[str, Any]:
    """Declared values resolved from one materialized entry sequence."""
    materialized = tuple(access.entries_of(entries))
    groups: dict[str, list[Any]] = {}
    for entry in materialized:
        if entry.comp:
            # The full indexed lead is the occurrence identity: nested paths
            # may reuse the same terminal group name and must remain separate.
            groups.setdefault(entry.comp, []).append(entry)
    values: dict[str, Any] = {}
    for member, annotation in _declared_members(cls):
        nested = _nested_type(annotation)
        if nested is not None:
            items = []
            for group in groups.values():
                projected = _entry_values(nested, group, access)
                if projected:
                    items.append(nested.from_dict(projected))
            if items:
                values[member.name] = items
            continue
        reading = _reading(member, materialized, access)
        if reading:
            _, declared = unwrap_annotated(annotation)
            _, declared = unwrap_optional(declared)
            value = (
                reading.raw
                if _fix_backed(member)
                and isinstance(declared, type)
                and issubclass(declared, enum.Enum)
                else reading.value
            )
            values[member.name] = _projected_value(member, annotation, value, access)
    return values


def _reading(member: Field, entries: Iterable[Any], access: Any) -> Any:
    """One declared member read by its registry identity."""
    return access.reading(entries, member.fix.canonical if _fix_backed(member) else member.name)


def _projected_value(member: Field, annotation: Any, value: Any, access: Any) -> Any:
    """One source value converted through its target declaration."""
    _, inner = unwrap_annotated(annotation)
    _, inner = unwrap_optional(inner)
    if getattr(inner, "__origin__", None) in SEQUENCE_ORIGINS:
        item = item_annotation(inner)
        _, item = unwrap_annotated(item)
        _, item = unwrap_optional(item)
        if isinstance(item, type) and issubclass(item, MarketConvertible):
            return [
                one
                if isinstance(one, item)
                else item.from_dict(_promoted_values(item, one, access))
                for one in value
            ]
    if isinstance(inner, type) and issubclass(inner, MarketConvertible):
        if isinstance(value, inner):
            return value
        return inner.from_dict(_promoted_values(inner, value, access))
    if isinstance(inner, type) and issubclass(inner, enum.Enum):
        if isinstance(value, inner):
            return value
        parser = getattr(inner, "from_fix" if _fix_backed(member) else "from_str", None)
        return parser(value) if parser is not None else inner(value)
    if inner is datetime.date and isinstance(value, datetime.datetime):
        return value.date()
    return value


def _nested_type(annotation: Any) -> type[MarketConvertible] | None:
    """The declared market row inside one optional sequence."""
    _, inner = unwrap_annotated(annotation)
    _, inner = unwrap_optional(inner)
    if getattr(inner, "__origin__", None) not in SEQUENCE_ORIGINS:
        return None
    item = item_annotation(inner)
    _, item = unwrap_annotated(item)
    _, item = unwrap_optional(item)
    if isinstance(item, type) and issubclass(item, MarketConvertible):
        return item
    return None


def _declared_value_parts(value: Any) -> tuple[Any, ...]:
    """One declared value recursively reduced to portable identity parts."""
    if isinstance(value, MarketConvertible):
        members = type(value).into_field().fields
        parts: list[Any] = [True, len(members)]
        for member in members:
            parts.extend((member.name, *_declared_value_parts(getattr(value, member.name))))
        return tuple(parts)
    if isinstance(value, Mapping):
        ordered = sorted(value.items(), key=lambda item: item[0].encode("utf-8"))
        parts = [True, len(ordered)]
        for key, item in ordered:
            parts.extend((key, *_declared_value_parts(item)))
        return tuple(parts)
    if isinstance(value, list | tuple):
        return (True, len(value), *(part for item in value for part in _declared_value_parts(item)))
    return (_declared_temporal(value) if isinstance(value, datetime.date) else value,)


def _declared_temporal(value: datetime.date) -> str:
    """One market identity timestamp in the spelling Arrow can reproduce."""
    if not isinstance(value, datetime.datetime):
        return value.isoformat()
    if value.tzinfo is not None:
        value = value.astimezone(datetime.UTC)
        return value.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return value.isoformat(timespec="microseconds")


def _local_timestamp(value: datetime.date | None) -> datetime.datetime | None:
    """One local-market value in the naive timestamp form its schema stores.

    Arrow drops the zone after converting an aware value to UTC for a naive
    timestamp. Normalize before identity is derived so scalar and Arrow paths
    see the same instant.
    """
    if value is None:
        return None
    if not isinstance(value, datetime.datetime):
        return datetime.datetime.combine(value, datetime.time())
    if value.utcoffset() is not None:
        return value.astimezone(datetime.UTC).replace(tzinfo=None)
    return value


def _declared_temporal_arrow(values: pyarrow.Array) -> pyarrow.Array:
    """Vectorized spelling used by `_declared_temporal`."""
    spelled = values.cast(pyarrow.string())
    return pyarrow.compute.replace_substring(spelled, " ", "T", max_replacements=1)


def _append_list_value(array: pyarrow.Array, values: pyarrow.Array) -> pyarrow.Array:
    """Append one carrying-book hash per nested event without row loops."""
    from rekep.fields.arrays import build_list, list_parts, sequence

    sizes, flat = list_parts(array)
    grown = pyarrow.compute.add(sizes, 1)
    parent = pyarrow.compute.list_parent_indices(array)
    old_positions = pyarrow.compute.add(sequence(len(flat)), parent)
    new_positions = pyarrow.compute.subtract(pyarrow.compute.cumulative_sum(grown), 1)
    positions = pyarrow.concat_arrays([old_positions, new_positions])
    ordered = pyarrow.concat_arrays([flat, values]).take(
        pyarrow.compute.array_sort_indices(positions)
    )
    return build_list(array.type, grown, ordered)
