"""Market: what happened, as a history rather than a state.

Every shape here is an `Event` -- an immutable version of one thing, keyed by
the sixteen bytes of its own content, linked to the version before it and to
the events it was built from. `MarketEvent` adds the four slots a market needs
(a side, a price, a quantity, an instrument), and `Order`, `Execution`,
`BookSide` and `Book` say what those slots mean for them.

Protocol notions are banded `int32` codes (`enums.py`), identifiers are signed
`int64` digests of a byte frame (`identity.py`), and the derived prices a
reader would otherwise recompute are columns that `Book.summarise_arrow` fills
in kernels. `fix.py` is the way in from a venue: a FIX message, or the pairs
one was rendered as, read as the orders and executions it carries.
"""

from rekep.market.book import Book, BookSide, Level, LevelExecution, LevelUpdate, Resting
from rekep.market.enums import (
    AssetKind,
    EventType,
    ExecKind,
    OptionKind,
    OrderKind,
    Ranged,
    Side,
    State,
    TimeInForce,
    UpdateAction,
)
from rekep.market.event import DAY, EPOCH, UNIX, Event, MarketEvent
from rekep.market.fields import MarketFieldBuilder, fix_tag, unkeyed
from rekep.market.fix import TRANSACTED, FixEvents, market_tags, unix_of
from rekep.market.identity import (
    HASH,
    NIL,
    arrow_of,
    frame,
    hash_arrow,
    hash_bytes,
    hash_of,
    part_bytes,
)
from rekep.market.instrument import Instrument
from rekep.market.orders import Execution, Order

__all__ = [
    "DAY",
    "EPOCH",
    "HASH",
    "NIL",
    "TRANSACTED",
    "UNIX",
    "AssetKind",
    "Book",
    "BookSide",
    "Event",
    "EventType",
    "ExecKind",
    "Execution",
    "FixEvents",
    "Instrument",
    "Level",
    "LevelExecution",
    "LevelUpdate",
    "MarketEvent",
    "MarketFieldBuilder",
    "OptionKind",
    "Order",
    "OrderKind",
    "Ranged",
    "Resting",
    "Side",
    "State",
    "TimeInForce",
    "UpdateAction",
    "arrow_of",
    "frame",
    "fix_tag",
    "hash_arrow",
    "hash_bytes",
    "hash_of",
    "market_tags",
    "part_bytes",
    "unix_of",
    "unkeyed",
]
