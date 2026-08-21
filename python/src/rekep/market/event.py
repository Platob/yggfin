"""The lifecycle envelope every market row carries, and the priced event on top of it."""

from __future__ import annotations

import dataclasses
import datetime
from typing import Annotated, Any, ClassVar, Self

import pyarrow

from rekep.convert import Convertible
from rekep.fields import Field, FieldBuilder, field
from rekep.market.enums import AssetKind, EventType, Side, State
from rekep.market.fields import MarketFieldBuilder, fix_tag
from rekep.market.identity import NIL, hash_arrow, hash_of
from rekep.market.instrument import Instrument

#: What a `*unix` column holds, said once. Whole nanoseconds since the epoch,
#: as an integer rather than a timestamp type -- which is the same choice
#: `Log` makes, for the same reason: a width or a zone that a downstream is
#: picky about is a conversion per row, and an integer survives every one of
#: them unchanged.
UNIX: dict[str, str] = {"unit": "nanosecond", "epoch": "1970-01-01"}

#: The day a `unix` of zero falls on, kept because a caller reading a `*unix`
#: back into a calendar still wants somewhere to start.
EPOCH = datetime.date(1970, 1, 1)

#: Nanoseconds in a day.
DAY = 86_400_000_000_000

#: Nanoseconds in an hour, which is what `hunix` truncates `unix` to.
HOUR = 3_600_000_000_000


@field
class Event(Convertible):
    """One immutable version of one thing that happened, and its place in a life.

    Every shape in this package is an `Event`, and the envelope is what makes a
    stream of them a *history* rather than a pile of rows:

    - **`xhash` is the thing; `hash` is this version of it.** An order amended
      four times is four rows sharing one `xhash`, each with its own `hash` and
      an incrementing `version`. Nothing is ever updated in place, so the store
      can be append-only and a reader can ask what was known at any moment
      rather than only what is true now.
    - **`hash` is content, so a capture read twice deduplicates itself.** It is
      the digest of what the event *says* -- never of when it was recorded --
      so the same version arriving down two feeds is one row with one key, and
      re-reading yesterday's capture writes nothing. That is why `runix` is
      outside the digest and outside the key.
    - **The previous version is on the row.** `prev_hash`, `prev_state` and
      `prev_unix` make the history a linked list, so "what changed" and "how
      long did it sit there" are read from one row instead of joined out of a
      window function over the whole lifecycle. Three columns bought against a
      self-join that costs a shuffle of the table.

    The four clocks are separate because they answer different questions and
    disagree constantly -- by microseconds on a good day and by hours when
    something has gone wrong upstream. `unix` is when it happened, `cunix` when
    it was made, `runix` when it was written down, `eunix` when it stops being
    true. Only `unix` is in the key.

    The primary key is `(unix, hash)`: `hash` alone identifies the version, and
    leading with time gives an engine a key that correlates with the partition,
    so a merge prunes to a day instead of scanning the table. `Log` keys itself
    the same way for the same reason.
    """

    FIELD_BUILDER: ClassVar[type[FieldBuilder]] = MarketFieldBuilder

    #: What this shape is, as the `etype` column holds it. Declared on the
    #: class rather than passed per row, because a shape is one kind of event
    #: and a row that disagreed with its own table would be unreadable.
    EVENT_TYPE: ClassVar[EventType] = EventType.UNKNOWN

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
    # An **hour**, and an `int64` rather than a date. A day of ticks is one
    # partition at day granularity, which prunes nothing inside a session --
    # the query everybody actually writes. And the same integer as `unix`
    # means a filter on the two is one comparison in one type: `WHERE hunix =
    # X AND unix BETWEEN ...` prunes the partition and then the file, with no
    # cast between a date and an instant in the middle of it.
    hunix: Annotated[int, Field.partition_key(metadata=UNIX)] = 0
    """`unix` truncated to the hour -- what the data is partitioned on."""

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

    version: int = 0
    """Which version of `xhash` this is, counting up from the first."""

    state: State = State.UNKNOWN
    """Where the lifecycle stands, as a banded code: `>= State.TERMINAL` is over."""

    symbol: Annotated[str, fix_tag("Symbol", 55)] = ""
    """Main readable identifier of the subject, as the venue spells it."""

    seq: Annotated[int | None, fix_tag("MsgSeqNum", 34)] = None
    """Sequence the venue gave the message, which orders what a clock cannot."""

    prev_hash: int | None = None
    """The version this one replaced; null on the first."""

    prev_state: State = State.UNKNOWN
    """The state this version moved out of -- a transition, without the self-join."""

    prev_unix: Annotated[int | None, Field(metadata=UNIX)] = None
    """When the previous version happened, so dwell time is a subtraction."""

    # A list rather than a second parent column: a book is built from two
    # sides, a spread from as many legs as it has, and the count is not a
    # property of the shape. What a join actually uses is the one flat parent a
    # subclass declares -- an execution's `order_xhash` -- because no engine
    # here joins on a list without exploding it first.
    parent_hash: list[int] | None = None
    """Every event this one was built from, in the order they were combined."""

    def __post_init__(self) -> None:
        """Make the members agree, so everything downstream can assume they do.

        Two of them are not independent facts and are therefore never given:

        - **`etype` is the class.** A row whose type disagreed with the table
          holding it would be unreadable, so it is taken from `EVENT_TYPE`
          rather than trusted from a caller. A value explicitly set to
          something other than `UNKNOWN` is left alone, which is how a shape
          that carries more than one kind (a `Quote` on the order table) still
          says so.
        - **`hunix` is `unix`.** It is denormalised for the partition, so
          deriving it here is the difference between one authority and two
          columns that disagree on the row nobody looks at. A modulo rather
          than a `datetime` round trip: it is exact for every representable
          instant, it costs no object, and Python's `%` floors, so an instant
          before the epoch lands in the hour that contains it rather than in
          the one after.
        """
        if self.etype is EventType.UNKNOWN:
            self.etype = type(self).EVENT_TYPE
        self.hunix = self.unix - self.unix % HOUR

    # -- what kind of event this is -----------------------------------------

    @classmethod
    def is_a(cls, kind: EventType) -> bool:
        """Whether this shape is `kind`, or anything inside `kind`'s band.

        The one comparison the named questions below are made of, so a band
        (`EventType.STATE`) and a member (`EventType.BOOK`) are both answerable
        without a caller knowing which it was handed.
        """
        return cls.EVENT_TYPE == kind or (
            kind == EventType.band_of(kind) and cls.EVENT_TYPE.band == kind
        )

    @classmethod
    def is_order(cls) -> bool:
        """Whether this shape is an order."""
        return cls.is_a(EventType.ORDER)

    @classmethod
    def is_execution(cls) -> bool:
        """Whether this shape is an execution report."""
        return cls.is_a(EventType.EXECUTION)

    @classmethod
    def is_book_side(cls) -> bool:
        """Whether this shape is one side of a book."""
        return cls.is_a(EventType.BOOK_SIDE)

    @classmethod
    def is_book(cls) -> bool:
        """Whether this shape is a whole book."""
        return cls.is_a(EventType.BOOK)

    @classmethod
    def is_snapshot(cls) -> bool:
        """Whether this shape is a picture of something rather than a thing itself."""
        return cls.EVENT_TYPE.is_snapshot

    # -- identity -----------------------------------------------------------

    @classmethod
    def hash_of(cls, *parts: Any) -> int:
        """The identifier `parts` name, for this shape.

        The class name goes in front of the parts, so an `Order` and a `Book`
        built from the same symbol and time cannot land on one identifier --
        which is a collision no amount of hash width prevents, because the
        inputs really are equal.
        """
        return hash_of(cls.__name__, *parts)

    @classmethod
    def hash_arrow(cls, *columns: Any) -> pyarrow.Array:
        """`hash_of` over whole columns: one identifier per row, in kernels."""
        return hash_arrow(cls.__name__, *columns)

    def identify(self) -> Self:
        """Give the event the identity its own content earns, where it has none.

        Two identities and one rule each. `xhash` is the **lifecycle** -- the
        thing that persists across every version of this event -- so it is
        hashed from what does not change: which instrument, which side, which
        identifier the venue gave it. `hash` is **this version** -- so it is
        hashed from the lifecycle plus what makes this version different from
        the last one.

        Both are computed by layer: `life_parts` and `version_parts` are what
        a subclass overrides, and the hashing, the class-name prefix and the
        "only when unset" rule stay here. A producer that already knows an
        identity keeps it -- this fills gaps, it does not overwrite.
        """
        self.xhash = self.xhash or self.life_hash()
        if not self.hash:
            self.hash = self.hash_of(*self.version_parts())
        return self

    def with_previous(self, previous: Event | None) -> Self:
        """This event completed from the version before it, and re-identified.

        The whole point of a versioned event: a venue restates only what
        changed, so a report that says "partially filled, 4 done" and nothing
        else is a complete row only once the price, the side, the instrument
        and everything else it did not repeat are carried forward from the
        version it follows.

        What that means field by field is **layered**: `complete_from` is what
        each class overrides, and it fills only what this version left absent.
        A value the message actually sent always wins -- this completes a row,
        it does not correct one.

        There are two halves to filling a row and they are separate methods,
        because only one of them needs a previous version. `complete_from` is
        what the row before implies about this one; `derive` is what *this*
        row implies about itself -- a quantity that is the difference of two
        others, a notional that is a product of three. A first version has no
        previous and still derives.

        What is *not* layered is the versioning, and it is here so no subclass
        can forget a piece of it: the counter moves on, the version before is
        recorded as `prev_hash`/`prev_state`/`prev_unix` so a transition is on
        the row rather than behind a self-join, and the content hash is
        re-derived last -- after every layer has filled, or it would identify
        a row that does not exist yet.
        """
        if previous is None:
            self.derive()
            return self.identify()
        self.complete_from(previous)
        self.derive()
        # Read **after** every layer has completed, and that is not
        # incidental: an order version that arrived carrying only its
        # `OrderID <37>` does not know its own instrument or venue until the
        # previous version has given them to it, and those are part of what
        # its lifecycle is. Asked before, it would identify as something else
        # and start its own version count. An event with nothing to be
        # identified by inherits the lifecycle it is completed from, which is
        # the only one available to it.
        self.xhash = self.xhash or self.life_hash() or previous.xhash
        if self.xhash and self.xhash == previous.xhash:
            self.version = previous.version + 1
            # NIL means the previous version was never hashed, and a null says
            # "no previous version" -- which is the truth about it either way.
            self.prev_hash = previous.hash or None
            self.prev_state = previous.state
            self.prev_unix = previous.unix
        elif previous.hash and previous.hash not in (self.parent_hash or ()):
            # Not a version of it: a different thing, built from it. A fill
            # completed from the order it happened to is the case that matters
            # -- it takes the order's running totals and stays version zero of
            # its own life, because a version counter counts one lifecycle.
            self.parent_hash = [*(self.parent_hash or ()), previous.hash]
        # Cleared, not kept: every layer has just filled fields the hash is
        # made of, so the identity this row arrived with was of a different
        # row. `identify` refuses to overwrite a hash that is set, which is
        # what makes clearing it the way to ask for a new one.
        self.hash = NIL
        return self.identify()

    def complete_from(self, previous: Event) -> None:
        """Fill what this version left absent, from the version before it.

        The envelope's own layer. `cunix` is the one that reads oddly and is
        the most important: it is **get-or-set**, because a lifecycle is
        created once and every later version of it was created at the same
        instant. A version that recomputed it would make "how old is this
        order" mean "how long since the last message about it".

        The identity is deliberately not here: whether this row is a version
        of `previous` or a different thing built from it is `same_life_as`,
        and it can only be asked once every layer has filled.
        """
        if not self.cunix:
            self.cunix = previous.cunix or previous.unix
        if not self.unix:
            # A message with no clock at all still belongs somewhere in time,
            # and the only honest answer is where the version before it was.
            self.unix = previous.unix
            self.hunix = self.unix - self.unix % HOUR
        if not self.runix:
            self.runix = previous.runix
        if self.eunix is None:
            self.eunix = previous.eunix
        if not self.symbol:
            self.symbol = previous.symbol
        if self.seq is None:
            self.seq = previous.seq
        if self.state is State.UNKNOWN:
            # A report that says nothing about the state is not saying the
            # state is unknown; it is not mentioning it.
            self.state = previous.state

    def derive(self) -> None:
        """Fill what this row's own fields already determine.

        Nothing at the envelope's layer: an event's own time, state and
        identity are given, never computed from each other. Subclasses have
        real work here, and this exists so every one of them can call
        `super().derive()` without knowing that.
        """

    def life_hash(self) -> int:
        """The identifier of this event's lifecycle, from what it carries now.

        `NIL` when it carries nothing to be identified by, which is honest:
        hashing emptiness would give every unidentified event one lifecycle.
        Whether that is what the event *ends up* with is `with_previous`'s
        answer, because a version completed from another inherits its.
        """
        parts = self.life_parts()
        return self.hash_of(*parts) if parts else NIL

    def life_parts(self) -> tuple[Any, ...]:
        """What makes this event's lifecycle the one it is, across every version.

        Empty when nothing does, which `life_hash` reads as `NIL`: an event
        that names no persistent thing is honestly unidentified.
        """
        return (self.symbol,) if self.symbol else ()

    def version_parts(self) -> tuple[Any, ...]:
        """What makes this version different from the one before it.

        The lifecycle leads, so two lifecycles cannot collide however alike
        their versions are, and then the counter, the instant and the state --
        the three things every event has that a new version moves.
        """
        return (self.xhash, self.version, self.unix, self.state)


@field
class MarketEvent(Event):
    """An event with a price, a quantity and an instrument -- the shape of a market.

    The four slots below are deliberately *abstract*, and every subclass says
    in its own docstring what it puts in them. That is what stops the package
    growing a `last_px`, a `bid_px`, an `entry_px` and a `limit_px` that all
    mean "the price on this row" and none of which a generic reader can find::

        Order       px is the limit, qty is the order quantity
        Execution   px is what traded (FIX LastPx), qty is how much (LastQty)
        BookSide    px is the best level's price, qty is its size
        Book        px is the mid, qty is the size behind it

    `px` and `qty` are nullable, and that is not laziness: a market order has
    no price, an empty book side has neither, and `0.0` is a real price --
    negative ones are too, as a settlement in April 2020 reminded everybody --
    so a NOT NULL column would have to lie about the difference between "at
    zero" and "not priced".
    """

    # Flat, first, and partitioned on. An event stream is read one instrument
    # at a time far more often than it is read whole, and `instrument.xhash`
    # cannot do this job: Doris pushes a predicate down only for a top-level
    # scalar, and no engine here partitions on a nested member at all.
    #
    # `bucket[16]` rather than the value itself, because the value is a hash:
    # partitioning on it directly is one partition per instrument per hour,
    # which is a hundred thousand partitions a day and a file in each. Sixteen
    # buckets is sixteen files an hour, and a single-instrument read still
    # touches one of them. The count is a deployment choice, not a law.
    instrument_hash: Annotated[int, Field.partition_key("bucket[16]")] = NIL
    """Which instrument this is about -- `instrument.xhash`, flat, for the partition."""

    side: Annotated[Side, fix_tag("Side", 54)] = Side.UNKNOWN
    """Which way the interest points; `side.sign` turns it into `+1` or `-1`."""

    px: Annotated[float | None, fix_tag("Price", 44)] = None
    """The price on this row, in `px_unit`; what it means is the subclass's to say."""

    # Ours, and so carrying no FIX tag: it normalises `PriceType <423>`, which
    # is an enumeration of conventions, together with `Currency <15>`, which is
    # the unit -- and a tag naming either would label the column as a field it
    # is not. NOT NULL with an empty placeholder: a
    # producer always knows how it quotes, and a column widened later is a
    # column every reader written before the widening has to re-handle.
    px_unit: str = ""
    """How to read `px`: a currency, or `PCT`, `BPS`, `YIELD`; empty when unstated."""

    qty: Annotated[float | None, fix_tag("OrderQty", 38)] = None
    """The quantity on this row, in `qty_unit`; what it means is the subclass's to say."""

    qty_unit: str = ""
    """How to read `qty`: `SHARES`, `LOTS`, `NOMINAL`; empty when unstated."""

    # Carried rather than derived, because deriving it needs the instrument's
    # multiplier -- so every consumer that wants money either joins reference
    # data or is quietly wrong on anything but a cash equity.
    notional: float | None = None
    """`px * qty * multiplier` in the instrument's currency, as the producer computed it."""

    venue: Annotated[str | None, fix_tag("LastMkt", 30)] = None
    """Where this event happened, which is not always where the instrument is listed."""

    instrument: Instrument = dataclasses.field(default_factory=Instrument)
    """What was traded. The flat `symbol` above is what a filter uses; this is the rest."""

    # A map and not a struct: what a venue sends is not known when the shape is
    # written, duplicate keys happen, and order is part of what was sent.
    metadata: dict[str, str] | None = None
    """Protocol fields carried verbatim, exactly as the venue sent them."""

    def __post_init__(self) -> None:
        """The envelope's own normalisation, then the instrument the row is about.

        `instrument_hash` is `instrument.xhash` flattened, so the two cannot
        disagree: a nested member nothing partitions on and a flat column
        everything does have to be the same instrument.
        """
        super().__post_init__()
        if self.instrument.xhash:
            self.instrument_hash = self.instrument.xhash

    def complete_from(self, previous: Event) -> None:
        """The four market slots, carried forward where this version was silent.

        `px` and `qty` are filled because a venue restating an order's status
        rarely repeats its limit, and a row with a null price is a row that
        drops out of every filter on price. The instrument is filled for the
        same reason and re-flattened onto `instrument_hash` after, so a
        completed row lands in the partition its lifecycle already lives in.
        """
        super().complete_from(previous)
        if not isinstance(previous, MarketEvent):
            return
        if not self.instrument.xhash and previous.instrument.xhash:
            self.instrument = previous.instrument
        if not self.instrument_hash:
            self.instrument_hash = self.instrument.xhash or previous.instrument_hash
        if self.side is Side.UNKNOWN:
            self.side = previous.side
        # `px` and `qty` are the abstract slots, and what they hold is the
        # subclass's to say: an order's are what it asked for, an execution's
        # are what traded, a book's are the mid and the touch. So they carry
        # only from a version of the *same shape*. Carrying an execution's
        # `LastQty` into an order's `OrderQty` made a partly filled order
        # claim it had asked for exactly what had just traded, and then
        # derived a `leaves_qty` of zero from it.
        if previous.etype == self.etype:
            if self.px is None:
                self.px = previous.px
            if self.qty is None:
                self.qty = previous.qty
        if not self.px_unit:
            self.px_unit = previous.px_unit
        if not self.qty_unit:
            self.qty_unit = previous.qty_unit
        if self.venue is None:
            self.venue = previous.venue

    def derive(self) -> None:
        """A notional is a price times a quantity times a multiplier, or nothing."""
        super().derive()
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
        multiplier = self.instrument.multiplier
        if multiplier is None:
            # A cash instrument really does trade one for one, and it is the
            # only class where the multiplier can be assumed rather than read.
            # `band` is the floor as an `int`, which is what a range predicate
            # compares against -- so this is `==` and never `is`.
            if self.instrument.kind.band != AssetKind.CASH:
                return None
            multiplier = 1.0
        return self.px * self.qty * multiplier

    def life_parts(self) -> tuple[Any, ...]:
        """A market lifecycle is an instrument and a direction, at least.

        Both, because the same identifier means different things on the two
        sides of a book and on two instruments -- and because an event that
        names neither has no lifecycle worth hashing, which is what the empty
        tuple says.
        """
        if not self.instrument_hash and not self.symbol:
            return ()
        return (self.instrument_hash, self.symbol, self.side)

    def version_parts(self) -> tuple[Any, ...]:
        """A market version moves when its price or its quantity moves."""
        return (*super().version_parts(), self.side, self.px, self.qty)
