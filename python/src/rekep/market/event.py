"""The lifecycle envelope every market row carries, and the priced event on top of it."""

from __future__ import annotations

import copy
import dataclasses
import functools
import operator
from collections.abc import Iterable, Iterator, Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Any, Self

import pyarrow
import pyarrow.compute

from rekep.enums import MIC, AssetKind, Currency, EventType, MarketKind, Side, State
from rekep.fields import Field, scalar
from rekep.market.fields import MarketConvertible, fix_tag
from rekep.market.identity import NIL, hash_arrow, hash_of

if TYPE_CHECKING:
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

#: Nanoseconds in a second.
SECOND = 1_000_000_000

#: Nanoseconds in a day.
DAY = 86_400_000_000_000

#: Nanoseconds in an hour, used to locate `unix`'s partition boundary.
HOUR = 3_600_000_000_000

_CONTRACT_METADATA = MappingProxyType({"version": "2"})

#: What `codes` holds: lifecycle aliases beside `code`, such as `cl_ord_id`
#: and `exec_id`. Instrument identity has `instrument_xhash` and
#: `instrument_code`; mixing it into this map lets unrelated events match.
CODES_TYPE = pyarrow.map_(
    pyarrow.string(), pyarrow.field("value", pyarrow.string(), nullable=False)
)

_LINKED_EVENTS_TYPE = pyarrow.list_(
    pyarrow.field(
        "item",
        pyarrow.struct(
            [
                pyarrow.field("unix", pyarrow.int64(), nullable=False),
                pyarrow.field("xhash", pyarrow.int64(), nullable=False),
            ]
        ),
        nullable=False,
    )
)


@scalar(slots=True, weakref_slot=True)
class Event(MarketConvertible):
    """One immutable version of one thing that happened, and its place in a life."""

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

    # Sorted on, as well as keyed and partitioned by the hour it falls in.
    # A sort order does not decide which file a row lands in -- the partition
    # does -- it decides where inside the file, which is what narrows the
    # column's min/max in a manifest from "everything this file holds" to a
    # real range. A time filter then reads a few row groups instead of all of
    # them, and that is the filter every reader of an event stream writes.
    unix: Annotated[int, Field.primary_key(metadata=UNIX), Field.sort_key()] = 0
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
    # that `unix_partition` is a function of `unix` lets a merge name it anyway,
    # because rows agreeing on `unix` agree here. Replaying one hour: 20 ms
    # against 93 over 168 hourly partitions, 27 against 164 over 336.
    unix_partition: Annotated[
        int,
        Field.partition_key(dtype=pyarrow.int32(), derived_from="unix", metadata=UNIX_PARTITION),
    ] = 0
    """`unix`'s hour boundary in whole epoch seconds; the partition value."""

    # Third, not last: a read that spans the tables filters on it before
    # anything else, and Iceberg's column bounds are collected in pre-order.
    # Constant within one table, where run-length and dictionary encoding
    # collapse it to nothing.
    etype: EventType = EventType.UNKNOWN
    """Which kind of event this is -- the one column a union of the tables needs."""

    cunix: Annotated[int, Field(metadata=UNIX)] = 0
    """When the event was created, upstream of anything that carried it."""

    runix: Annotated[int, Field(metadata=UNIX)] = 0
    """When the event was written down here; deliberately not part of `hash`."""

    eunix: Annotated[int | None, Field(metadata=UNIX)] = None
    """When the event stops being true -- an order's expiry, a quote's staleness."""

    # A snapshot's own `unix` is when the picture was taken, because that is
    # what orders it against everything else in the stream. What it is a
    # picture *of* would otherwise be lost: `sunix` keeps it, so "as of when"
    # and "taken when" are both on the row and a stale snapshot is one
    # subtraction rather than a join against whatever it snapshotted.
    sunix: Annotated[int | None, Field(metadata=UNIX)] = None
    """`unix` of the event this is a snapshot of; null when it is not one."""

    hash: Annotated[int, Field.primary_key()] = NIL
    """Digest of this version's content: the same version, twice, is one row."""

    xhash: int = NIL
    """Identity of the thing across every version of it -- the lifecycle."""

    linked_events: Annotated[list[tuple[int, int]], Field(dtype=_LINKED_EVENTS_TYPE)] = (
        dataclasses.field(default_factory=list)
    )
    """Related event times and lifecycle identities, primary match first."""

    version: int = 0
    """Which version of `xhash` this is, counting up from the first."""

    state: State = State.UNKNOWN
    """Where the lifecycle stands, as a ranked code: `is_terminal` is over."""

    code: str = ""
    """Readable identifier of this lifecycle, shared by every version of it."""

    codes: Annotated[dict[str, str], Field(dtype=CODES_TYPE)] = dataclasses.field(
        default_factory=dict
    )
    """Every other identifier the source spelled, by the name that carried it."""

    prev_unix: Annotated[int | None, Field(metadata=UNIX)] = None
    """When the previous version happened, so dwell time is a subtraction."""

    # Version digests are distinct from `linked_events` lifecycle relations.
    parent_hash: list[int] | None = None
    """Every event this one was built from, in the order they were combined."""

    mic: MIC | None = None
    """ISO 10383 venue code, packed losslessly into `int32`; null when absent."""

    reason: str | None = None
    """Why this event was rejected or could not be interpreted, when known."""

    def __post_init__(self) -> None:
        """Make the members agree, so everything downstream can assume they do."""
        links = self.linked_events
        if links is None:
            self.linked_events = []
        elif links:
            normalized = list(dict.fromkeys(links))
            if not isinstance(links, list) or normalized != links:
                self.linked_events = normalized
        elif not isinstance(links, list):
            self.linked_events = []
        # Arrow reads a map back as a list of pairs, and a row that made the
        # round trip is the same row: normalize once here rather than at every
        # reader of `codes`.
        if not isinstance(self.codes, dict):
            self.codes = dict(self.codes or ())
        self.normalize_float_members()
        if self.etype is EventType.UNKNOWN:
            self.etype = type(self).into_event_type()
        self.unix_partition = _unix_partition_of(self.unix)
        self._drop_self_link()

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> Self:
        """Rebuild named Arrow link structs as the public tuple representation."""
        values = dict(mapping)
        links = values.get("linked_events")
        if links is not None:
            values["linked_events"] = [
                (link["unix"], link["xhash"]) if isinstance(link, Mapping) else tuple(link)
                for link in links
            ]
        return MarketConvertible.from_dict.__func__(cls, values)

    def into_dict(self) -> dict[str, Any]:
        """Spell tuple links as named structs at the Arrow boundary."""
        values = MarketConvertible.into_dict(self)
        values["linked_events"] = [
            {"unix": unix, "xhash": xhash} for unix, xhash in self.linked_events
        ]
        return values

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
            held: list[dict[str, Any]] = []
            for event in events:
                held.append(event.into_dict())
                if len(held) >= batch_row_size:
                    yield pyarrow.RecordBatch.from_pylist(held, schema=schema)
                    held.clear()
            if held:
                yield pyarrow.RecordBatch.from_pylist(held, schema=schema)

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

    def identify(self) -> Self:
        """Give the event the identity its own content earns, where it has none."""
        self._materialize_life_code()
        self.xhash = self.xhash or self.life_hash()
        self._drop_self_link()
        if not self.hash:
            self.hash = self.hash_of(*self.version_parts())
        return self

    def with_previous(self, previous: Event | None) -> Self | None:
        """Complete this version, returning None when it changes no stored fact."""
        self.completed_from(previous)
        if previous is not None and self.same_as(previous):
            return None
        return self.identify()

    def same_as(self, previous: Event) -> bool:
        """Whether this is only a later observation of the same stored state."""
        if type(self) is not type(previous) or self.xhash != previous.xhash:
            return False
        snapshot = self.sunix is not None or previous.sunix is not None
        values, floats = type(self)._same_as_values(snapshot)
        if values(self) != values(previous):
            return False
        # Tuple equality treats the same NaN object as equal by identity while
        # the member-by-member comparison this replaces did not.
        if floats is None:
            return True
        current = floats(self)
        if not isinstance(current, tuple):
            return current == current
        return all(value == value for value in current)

    @classmethod
    @functools.cache
    def _same_as_values(cls, snapshot: bool) -> tuple[Any, Any | None]:
        """Cached projection of the state compared for this concrete shape."""
        ignored = {
            "cunix",
            "runix",
            "hash",
            "xhash",
            "version",
            "prev_unix",
            "prev_px",
            "prev_qty",
            "prev_notional",
            "prev_bid_px",
            "prev_bid_qty",
            "prev_ask_px",
            "prev_ask_qty",
            "prev_exec_px",
            "parent_hash",
        }
        # Observation time is state only for a snapshot.
        if not snapshot:
            ignored.update(("unix", "unix_partition"))
        names = tuple(
            member.name for member in dataclasses.fields(cls) if member.name not in ignored
        )
        floating = tuple(
            member.name
            for member in cls.into_field().fields
            if member.name in names and pyarrow.types.is_floating(member.dtype)
        )
        return operator.attrgetter(*names), operator.attrgetter(*floating) if floating else None

    def completed_from(self, previous: Event | None) -> Self:
        """`with_previous` without the content hash: everything but `hash`."""
        if previous is None:
            self.derive()
            self._materialize_life_code()
            self.xhash = self.xhash or self.life_hash()
            self._drop_self_link()
            return self
        life_before = self.life_parts()
        self.complete_from(previous)
        self.derive()
        self._materialize_life_code()
        # Read **after** every layer has completed, and that is not
        # incidental: an order version that arrived carrying only its
        # `OrderID <37>` does not know its own instrument or venue until the
        # previous version has given them to it, and those are part of what
        # its lifecycle is. Asked before, it would identify as something else
        # and start its own version count. An event with nothing to be
        # identified by inherits the lifecycle it is completed from, which is
        # the only one available to it.
        # A parsed row may already have been identified before completion
        # supplies its instrument or venue. Its readable key still agrees,
        # but its scoped hash does not; changed lifecycle parts invalidate it.
        if self.xhash and self.life_parts() != life_before:
            self.xhash = NIL
        self.xhash = self.xhash or self.life_hash() or previous.xhash
        self._drop_self_link()
        if self.xhash and self.xhash == previous.xhash:
            # The row carried no readable key of its own, so the same
            # lifecycle is the only honest place its readable key can come
            # from. Never copy it before this comparison: completion crosses
            # shapes, and an execution is not named by its order's code.
            self.code = previous.code or self.code
            self._keep_lifecycle_codes(previous)
            self.version = previous.version + 1
            self.prev_unix = previous.unix
            self._remember_previous(previous)
        elif self.xhash != previous.xhash:
            # Not a version of it: a different thing, built from it. A fill
            # completed from the order it happened to is the case that matters
            # -- it takes the order's running totals and stays version zero of
            # its own life, because a version counter counts one lifecycle.
            if previous.hash and previous.hash not in (self.parent_hash or ()):
                self.parent_hash = [*(self.parent_hash or ()), previous.hash]
            self.link_to(previous)
        # Cleared, not kept: every layer has just filled fields the hash is
        # made of, so the identity this row arrived with was of a different
        # row. `identify` refuses to overwrite a hash that is set, which is
        # what makes clearing it the way to ask for a new one.
        self.hash = NIL
        return self

    def _completed_from_same_lifecycle(self, previous: Event) -> Self:
        """Complete a version whose lifecycle hash already matches its predecessor."""
        self.complete_from(previous)
        self.derive()
        self._materialize_life_code()
        self.xhash = previous.xhash
        self._drop_self_link()
        self.code = previous.code or self.code
        self._keep_lifecycle_codes(previous)
        self.version = previous.version + 1
        self.prev_unix = previous.unix
        self._remember_previous(previous)
        self.hash = NIL
        return self

    def _keep_lifecycle_codes(self, previous: Event) -> None:
        """Carry absent identifiers while keeping this version's observed values."""
        merged = dict(previous.codes)
        for name, value in self.codes.items():
            if value:
                merged[name] = value
        self.codes = merged

    def complete_from(self, previous: Event) -> None:
        """Fill what this version left absent, from the version before it."""
        if previous.linked_events:
            self.link_to(*previous.linked_events)
        if not self.cunix:
            self.cunix = previous.cunix or previous.unix
        if not self.unix:
            # A message with no clock at all still belongs somewhere in time,
            # and the only honest answer is where the version before it was.
            self.unix = previous.unix
            self.unix_partition = _unix_partition_of(self.unix)
        if not self.runix:
            self.runix = previous.runix
        if self.eunix is None:
            self.eunix = previous.eunix
        # Not `code`: that names a lifecycle, and completion crosses lifecycles
        # -- an execution completed from its order is not named by the order.
        # The identifiers *beside* it are facts about the same thing under any
        # reading, so a version silent about one keeps what the last one said.
        self.name_codes(previous.codes)
        if self.mic is None:
            self.mic = previous.mic
        if self.state is State.UNKNOWN:
            # A report that says nothing about the state is not saying the
            # state is unknown; it is not mentioning it.
            self.state = previous.state

    def _remember_previous(self, previous: Event) -> None:
        """Store shape-specific transition values after lifecycle matching."""

    def link_to(self, *events: Event | tuple[int, int], primary: bool = False) -> Self:
        """Relate events once, optionally ahead of existing links."""
        if not events:
            return self
        given = []
        for event in events:
            link = (event.unix, event.xhash) if isinstance(event, Event) else tuple(event)
            if len(link) != 2:
                raise ValueError("a linked event is a (unix, xhash) pair")
            unix, xhash = link
            if xhash:
                given.append((int(unix), int(xhash)))
        given = list(dict.fromkeys(given))
        existing = list(self.linked_events)
        ordered = given + existing if primary else existing + given
        self.linked_events = list(dict.fromkeys(ordered))
        self._drop_self_link()
        return self

    @property
    def primary_linked_event(self) -> tuple[int, int] | None:
        """First related event, when one is known."""
        return self.linked_events[0] if self.linked_events else None

    def _drop_self_link(self) -> None:
        """A relation never points back to its own lifecycle."""
        if self.xhash and self.linked_events:
            self.linked_events = [
                linked for linked in self.linked_events if linked[1] != self.xhash
            ]

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
        if self.sunix is not None and self.sunix >= unix:
            return None
        taken = copy.copy(self)
        taken.unix = unix
        taken.unix_partition = _unix_partition_of(unix)
        # What it is a picture *of*: the instant of the state, which for a
        # picture of a picture is the original state and not the middle one.
        # Two snapshots of one unchanged book then agree on what they show and
        # differ only in when they were taken, which is the truth about them.
        taken.sunix = self.sunix if self.sunix is not None else self.unix
        if unix - taken.sunix >= DAY:
            taken.state = State.INTERNAL_EXPIRED
            taken.eunix = taken.sunix + DAY
            taken.reason = taken.reason or "expired internally after one day without change"
        taken.forget_delta()
        # Cleared so `identify` derives one: the row differs from the one it
        # was copied from, in the two fields that say it is a picture.
        taken.hash = NIL
        return taken

    def forget_delta(self) -> None:
        """Drop what *changed*, keeping what *is*. A snapshot has no delta."""

    def life_hash(self) -> int:
        """The identifier of this event's lifecycle, from what it carries now.

        `NIL` when it carries nothing to be identified by, which is honest:
        hashing emptiness would give every unidentified event one lifecycle.
        Whether that is what the event *ends up* with is `with_previous`'s
        answer, because a version completed from another inherits its.
        """
        parts = self.life_parts()
        if not parts:
            return NIL
        # Cached on the parts rather than computed per event, because a
        # lifecycle is the thing that *repeats*: forty resting levels restated
        # on every refresh, one order amended five times, a trade corrected.
        # A version hash is not cached beside it -- it is different by
        # definition, so it would only evict these.
        try:
            return _life_hash(type(self).__name__, parts)
        except TypeError:
            # A supported bytes-like part may be mutable and therefore cannot
            # key the cache; frame it directly without remembering it.
            return self.hash_of(*parts)

    def life_code(self) -> str:
        """The readable part that names this lifecycle, without changing it."""
        return self.code

    def _materialize_life_code(self) -> None:
        """Store the readable lifecycle part once, before mutable codes move."""
        self.code = self.code or self.life_code()

    def name_code(self, name: str, value: str | None) -> None:
        """Record one identifier this row carried, without displacing one it has."""
        if value and not self.codes.get(name):
            self.codes[name] = value

    def name_codes(self, codes: Mapping[str, str]) -> None:
        """Record several, in one pass and under the same rule."""
        for name, value in codes.items():
            self.name_code(name, value)

    def life_parts(self) -> tuple[Any, ...]:
        """What makes this event's lifecycle the one it is, across every version.

        Empty when nothing does, which `life_hash` reads as `NIL`: an event
        that names no persistent thing is honestly unidentified.
        """
        code = self.life_code()
        return (code,) if code else ()

    def version_parts(self) -> tuple[Any, ...]:
        """What makes this version different from the one before it.

        The lifecycle leads, so two lifecycles cannot collide however alike
        their versions are, and then the counter, the instant and the state --
        the three things every event has that a new version moves.
        """
        links = tuple(self.linked_events)
        return (
            self.xhash,
            self.version,
            self.unix,
            self.state,
            self.mic,
            len(links),
            *(part for link in links for part in link),
            self.reason,
        )


@scalar(slots=True)
class MarketEvent(Event):
    """A priced event carrying a flat instrument identity."""

    # Parsed instrument facts are useful while folding but are deliberately
    # not a member: market events persist only the flat identity.
    __instrument: Instrument | None = None

    # Flat and first. An event stream is read one instrument at a time far more
    # often than it is read whole, and `instrument.xhash` cannot serve that: an
    # engine pushes a predicate down only for a top-level scalar.
    #
    # Not a partition, deliberately. The value is a hash, so bucketing it split
    # every hour into as many files as buckets while the hour itself already
    # prunes the read -- more small files for a filter that was already exact.
    instrument_xhash: int = NIL
    """Instrument lifecycle identity used to join market rows."""

    # Beside the hash rather than only inside `codes`: a hash joins, and a
    # person reads. Every filter, group and error message about an instrument
    # was reaching into a map for the one key this package writes itself.
    #
    # Not a partition either, and this one is measured rather than argued. The
    # case for bucketing it is real -- `unix_partition` prunes time and not
    # instrument, so a scan for one instrument across a week opens every
    # hour's files. But bucketing does not fix that: the bucket prunes files
    # *inside* an hour, and a scan across N hours still opens at least one
    # file per hour, which is what the query actually pays for. Over 144,000
    # rows across 72 hours and 40 instruments, one instrument's week cost
    # 632 ms on `unix_partition` alone, 650 ms at bucket[8] and 671 ms at
    # bucket[16] -- slower, not faster -- while the file count went 72 to 576
    # to 1,152, the mean file fell from 76 KiB to 25, and the hourly read
    # every consumer writes went 24 ms to 165 to 320. See docs/market/index.md.
    instrument_code: str = ""
    """Readable spelling of the instrument `instrument_xhash` names; empty when unstated."""

    kind: MarketKind = MarketKind.UNKNOWN
    """Standard market semantic, independent of its protocol spelling."""

    side: Annotated[Side, fix_tag("Side")] = Side.UNKNOWN
    """Which way the interest points; `side.sign` turns it into `+1` or `-1`."""

    px: Annotated[float | None, fix_tag("Price")] = None
    """The price on this row, in `px_unit`; what it means is the subclass's to say."""

    prev_px: float | None = None
    """Price on the immediately preceding lifecycle version; null on the first."""

    # Ours, and so carrying no FIX tag: it normalises `PriceType <423>`, which
    # is an enumeration of conventions, together with `Currency <15>`, which is
    # the unit -- and a tag naming either would label the column as a field it
    # is not. NOT NULL with an empty placeholder: a
    # producer always knows how it quotes, and a column widened later is a
    # column every reader written before the widening has to re-handle.
    px_unit: str = ""
    """How to read `px`: a currency, or `PCT`, `BPS`, `YIELD`; empty when unstated."""

    ccy: Annotated[Currency | None, fix_tag("Currency")] = None
    """Currency of the monetary values; null when the source does not state one."""

    qty: Annotated[float | None, fix_tag("OrderQty")] = None
    """The quantity on this row, in `qty_unit`; what it means is the subclass's to say."""

    prev_qty: float | None = None
    """Quantity on the immediately preceding lifecycle version; null on the first."""

    qty_unit: str = ""
    """How to read `qty`: `SHARES`, `LOTS`, `NOMINAL`; empty when unstated."""

    # Carried rather than derived after persistence because the multiplier is
    # normalized in the separate reference stream.
    notional: float | None = None
    """`px * qty * multiplier` in the instrument's currency, as the producer computed it."""

    prev_notional: float | None = None
    """Notional on the immediately preceding lifecycle version; null on the first."""

    # Free-form protocol metadata stays separate from declared columns.
    metadata: dict[str, str] | None = None
    """Protocol fields carried verbatim, exactly as the venue sent them."""

    def __post_init__(self) -> None:
        """Normalize the compact currency code and inherited clocks."""
        if self.ccy is not None:
            self.ccy = Currency.from_str(self.ccy)
        Event.__post_init__(self)

    def attach_instrument(self, instrument: Instrument) -> Self:
        """Use reference data while building without adding it to the event schema."""
        self.__instrument = instrument
        self.instrument_xhash = self.instrument_xhash or instrument.xhash
        self.instrument_code = self.instrument_code or instrument.code or instrument.symbol
        if self.ccy is None:
            self.ccy = instrument.currency
        return self

    def into_instrument(self) -> Instrument | None:
        """Return transient parsed reference data, absent after persisted reads."""
        return getattr(self, "_MarketEvent__instrument", None)

    @property
    def symbol(self) -> str:
        """The flat readable instrument spelling."""
        return self.instrument_code

    def complete_from(self, previous: Event) -> None:
        """The four market slots, carried forward where this version was silent.

        `px` and `qty` are filled because a venue restating an order's status
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
        if not self.instrument_xhash:
            self.instrument_xhash = previous.instrument_xhash
        if not self.instrument_code:
            self.instrument_code = previous.instrument_code
        if self.side is Side.UNKNOWN:
            self.side = previous.side
        # `px` and `qty` are the abstract slots, and what they hold is the
        # subclass's to say: an order's are what it asked for, an execution's
        # are what traded, a book's are the mid and the touch. So they carry
        # only from a version of the *same shape*. Carrying an execution's
        # `LastQty` into an order's `OrderQty` made a partly filled order
        # claim its current quantity was exactly what had just traded.
        if previous.etype == self.etype:
            if self.kind is MarketKind.UNKNOWN:
                self.kind = previous.kind
            if self.px is None:
                self.px = previous.px
            if self.qty is None:
                self.qty = previous.qty
        if not self.px_unit:
            self.px_unit = previous.px_unit
        if self.ccy is None:
            self.ccy = previous.ccy
        if not self.qty_unit:
            self.qty_unit = previous.qty_unit

    def _remember_previous(self, previous: Event) -> None:
        """Keep the prior priced values only after lifecycle identity agrees."""
        Event._remember_previous(self, previous)
        if not isinstance(previous, MarketEvent):
            return
        if self.prev_px is None:
            self.prev_px = previous.px
        if self.prev_qty is None:
            self.prev_qty = previous.qty
        if self.prev_notional is None:
            self.prev_notional = previous.notional

    def derive(self) -> None:
        """A notional is a price times a quantity times a multiplier, or nothing."""
        Event.derive(self)
        if self.notional is None:
            self.notional = self.into_notional()

    def into_notional(self) -> float | None:
        """`px * qty * multiplier` in the instrument's currency, or None.

        None when any of the three is missing, and that is the point of it
        being a method rather than an expression at the call site: a notional
        computed with a multiplier of "probably one" is wrong by a factor
        nobody notices until settlement, so a contract whose multiplier is
        unknown has no notional rather than a plausible one.
        """
        if self.px is None or self.qty is None:
            return None
        instrument = self.into_instrument()
        if instrument is None:
            return None
        multiplier = instrument.multiplier
        if multiplier is None:
            # A cash instrument really does trade one for one, and it is the
            # only class where the multiplier can be assumed rather than read.
            # `band` answers with the band-floor member itself, not a number,
            # so this is `is` and never `==`.
            if instrument.kind.band is not AssetKind.CASH:
                return None
            multiplier = 1.0
        return self.px * self.qty * multiplier

    def forget_delta(self) -> None:
        """A snapshot keeps current values, never the transition into them."""
        Event.forget_delta(self)
        self.prev_px = None
        self.prev_qty = None
        self.prev_notional = None

    def life_parts(self) -> tuple[Any, ...]:
        """A market lifecycle is an instrument and a direction, at least.

        Both, because the same identifier means different things on the two
        sides of a book and on two instruments -- and because an event that
        names neither has no lifecycle worth hashing, which is what the empty
        tuple says.
        """
        code = self.life_code()
        if not self.instrument_xhash and not code:
            return ()
        return (self.instrument_xhash, code, self.side)

    def life_code(self) -> str:
        """The lifecycle identifier, and the instrument symbol when there is none.

        A market event that names no order and no report is still an event
        about one instrument on one side, and the symbol is the readable half
        of that. It is a fallback and never a preference: a row carrying an
        order identifier is named by the order.
        """
        return self.code or self.symbol

    def version_parts(self) -> tuple[Any, ...]:
        """A market version moves when its price or its quantity moves."""
        return (
            *Event.version_parts(self),
            self.kind,
            self.side,
            self.px,
            self.prev_px,
            self.ccy,
            self.qty,
            self.prev_qty,
            self.notional,
            self.prev_notional,
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
        projected["parent_hash"] = _append_list_value(
            projected["parent_hash"], books.column("hash").take(parents)
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


@functools.lru_cache(maxsize=65_536)
def _life_hash(shape: str, parts: tuple[Any, ...]) -> int:
    """`hash_of` over a lifecycle's parts, remembered.

    Pure, so the cache cannot be stale: the parts are the identifier, and the
    shape's name is in front of them for the same reason `Event.hash_of` puts
    it there. Bounded, because a feed of one-shot client order ids has no
    repeats to find and should not keep them all.
    """
    return hash_of(shape, *parts)


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
