"""Market: what happened, as a history rather than a state.

Every shape here is an `Event` -- an immutable version of one thing, keyed by
the sixteen bytes of its own content, linked to the version before it and to
the events it was built from. `MarketEvent` adds the four slots a market needs
(a side, a price, a quantity, an instrument), and `Order`, `Execution`,
`BookSide` and `Book` say what those slots mean for them.

Protocol notions are banded `int32` codes (`enums.py`), identifiers are
`fixed_size_binary[16]` (`identity.py`), and the derived prices a reader would
otherwise recompute are columns that `Book.summarise_arrow` fills in kernels.
"""

from rekep.market.book import Book, BookSide, Level, LevelUpdate
from rekep.market.enums import (
    AssetKind,
    ExecKind,
    OptionKind,
    OrderKind,
    Ranged,
    Side,
    State,
    TimeInForce,
    UpdateAction,
)
from rekep.market.event import UNIX, Event, MarketEvent
from rekep.market.fields import MarketFieldBuilder, fix_tag, unkeyed
from rekep.market.identity import H128, arrow_of, h128_arrow, h128_of, uuids_of
from rekep.market.instrument import Instrument
from rekep.market.orders import Execution, Order

__all__ = [
    "H128",
    "UNIX",
    "AssetKind",
    "Book",
    "BookSide",
    "Event",
    "ExecKind",
    "Execution",
    "Instrument",
    "Level",
    "LevelUpdate",
    "MarketEvent",
    "MarketFieldBuilder",
    "OptionKind",
    "Order",
    "OrderKind",
    "Ranged",
    "Side",
    "State",
    "TimeInForce",
    "UpdateAction",
    "arrow_of",
    "fix_tag",
    "h128_arrow",
    "h128_of",
    "unkeyed",
    "uuids_of",
]
