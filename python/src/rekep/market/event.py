"""The lifecycle envelope every market row carries, and the priced event on top of it."""

from __future__ import annotations

import datetime
import uuid
from typing import Annotated, Any, ClassVar

import pyarrow

from rekep.convert import Convertible
from rekep.fields import Field, FieldBuilder, field
from rekep.market.enums import Side, State
from rekep.market.fields import MarketFieldBuilder, fix_tag
from rekep.market.identity import h128_arrow, h128_of
from rekep.market.instrument import Instrument

#: What a `*unix` column holds, said once. Whole nanoseconds since the epoch,
#: as an integer rather than a timestamp type -- which is the same choice
#: `Log` makes, for the same reason: a width or a zone that a downstream is
#: picky about is a conversion per row, and an integer survives every one of
#: them unchanged.
UNIX: dict[str, str] = {"unit": "nanosecond", "epoch": "1970-01-01"}


@field
class Event(Convertible):
    """One immutable version of one thing that happened, and its place in a life.

    Every shape in this package is an `Event`, and the envelope is what makes a
    stream of them a *history* rather than a pile of rows:

    - **`xh128` is the thing; `h128` is this version of it.** An order amended
      four times is four rows sharing one `xh128`, each with its own `h128` and
      an incrementing `version`. Nothing is ever updated in place, so the store
      can be append-only and a reader can ask what was known at any moment
      rather than only what is true now.
    - **`h128` is content, so a capture read twice deduplicates itself.** It is
      the digest of what the event *says* -- never of when it was recorded --
      so the same version arriving down two feeds is one row with one key, and
      re-reading yesterday's capture writes nothing. That is why `runix` is
      outside the digest and outside the key.
    - **The previous version is on the row.** `prev_h128`, `prev_state` and
      `prev_unix` make the history a linked list, so "what changed" and "how
      long did it sit there" are read from one row instead of joined out of a
      window function over the whole lifecycle. Three columns bought against a
      self-join that costs a shuffle of the table.

    The four clocks are separate because they answer different questions and
    disagree constantly -- by microseconds on a good day and by hours when
    something has gone wrong upstream. `unix` is when it happened, `cunix` when
    it was made, `runix` when it was written down, `eunix` when it stops being
    true. Only `unix` is in the key.

    The primary key is `(unix, h128)`: `h128` alone identifies the version, and
    leading with time gives an engine a key that correlates with the partition,
    so a merge prunes to a day instead of scanning the table. `Log` keys itself
    the same way for the same reason.
    """

    FIELD_BUILDER: ClassVar[type[FieldBuilder]] = MarketFieldBuilder

    unix: Annotated[int, Field.primary_key(metadata=UNIX)]
    """When the event happened, in whole nanoseconds since the epoch."""

    # Denormalised from `unix` rather than partitioned with a `day` transform:
    # an identity partition on a real date column is the one form every engine
    # below reads the same way, and the transformed alternative needs Iceberg's
    # Rust core on the writer for no gain a reader can see.
    date: Annotated[datetime.date, Field.partition_key()]
    """Calendar day of `unix`, naive UTC -- what the data is partitioned on."""

    cunix: Annotated[int, Field(metadata=UNIX)]
    """When the event was created, upstream of anything that carried it."""

    runix: Annotated[int, Field(metadata=UNIX)]
    """When the event was written down here; deliberately not part of `h128`."""

    eunix: Annotated[int | None, Field(metadata=UNIX)]
    """When the event stops being true -- an order's expiry, a quote's staleness."""

    h128: Annotated[uuid.UUID, Field.primary_key()]
    """Digest of this version's content: the same version, twice, is one row."""

    xh128: uuid.UUID
    """Identity of the thing across every version of it -- the lifecycle."""

    version: int
    """Which version of `xh128` this is, counting up from the first."""

    state: State
    """Where the lifecycle stands, as a banded code: `>= State.TERMINAL` is over."""

    symbol: Annotated[str, fix_tag("Symbol", 55)]
    """Main readable identifier of the subject, as the venue spells it."""

    seq: Annotated[int | None, fix_tag("MsgSeqNum", 34)]
    """Sequence the venue gave the message, which orders what a clock cannot."""

    prev_h128: uuid.UUID | None
    """The version this one replaced; null on the first."""

    prev_state: State
    """The state this version moved out of -- a transition, without the self-join."""

    prev_unix: Annotated[int | None, Field(metadata=UNIX)]
    """When the previous version happened, so dwell time is a subtraction."""

    # A list rather than a second parent column: a book is built from two
    # sides, a spread from as many legs as it has, and the count is not a
    # property of the shape. What a join actually uses is the one flat parent a
    # subclass declares -- an execution's `order_xh128` -- because no engine
    # here joins on a list without exploding it first.
    parent_h128: list[uuid.UUID] | None
    """Every event this one was built from, in the order they were combined."""

    @classmethod
    def h128_of(cls, *parts: Any) -> uuid.UUID:
        """The identifier `parts` name, for this shape.

        The class name goes in front of the parts, so an `Order` and a `Book`
        built from the same symbol and time cannot land on one identifier --
        which is a collision no amount of hash width prevents, because the
        inputs really are equal.
        """
        return h128_of(cls.__name__, *parts)

    @classmethod
    def h128_arrow(cls, *columns: Any) -> pyarrow.Array:
        """`h128_of` over whole columns: one identifier per row, in kernels."""
        return h128_arrow(cls.__name__, *columns)


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

    side: Annotated[Side, fix_tag("Side", 54)]
    """Which way the interest points; `side.sign` turns it into `+1` or `-1`."""

    px: Annotated[float | None, fix_tag("Price", 44)]
    """The price on this row, in `px_unit`; what it means is the subclass's to say."""

    # Ours, and so carrying no FIX tag: it normalises `PriceType <423>`, which
    # is an enumeration of conventions, together with `Currency <15>`, which is
    # the unit -- and a tag naming either would label the column as a field it
    # is not. NOT NULL with an empty placeholder, like `Log.category_name`: a
    # producer always knows how it quotes, and a column widened later is a
    # column every reader written before the widening has to re-handle.
    px_unit: str
    """How to read `px`: a currency, or `PCT`, `BPS`, `YIELD`; empty when unstated."""

    qty: Annotated[float | None, fix_tag("OrderQty", 38)]
    """The quantity on this row, in `qty_unit`; what it means is the subclass's to say."""

    qty_unit: str
    """How to read `qty`: `SHARES`, `LOTS`, `NOMINAL`; empty when unstated."""

    # Carried rather than derived, because deriving it needs the instrument's
    # multiplier -- so every consumer that wants money either joins reference
    # data or is quietly wrong on anything but a cash equity.
    notional: float | None
    """`px * qty * multiplier` in the instrument's currency, as the producer computed it."""

    venue: Annotated[str | None, fix_tag("LastMkt", 30)]
    """Where this event happened, which is not always where the instrument is listed."""

    instrument: Instrument
    """What was traded. The flat `symbol` above is what a filter uses; this is the rest."""

    # A map and not a struct: what a venue sends is not known when the shape is
    # written, duplicate keys happen, and order is part of what was sent.
    metadata: dict[str, str] | None
    """Protocol fields carried verbatim, exactly as the venue sent them."""
