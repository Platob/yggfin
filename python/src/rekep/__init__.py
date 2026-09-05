"""rekep: trading log parser."""

from importlib.metadata import version as package_version

from rekep.convert import Convertible
from rekep.dataset import Dataset
from rekep.enums import Ascii32, Ascii64, Ascii128, Currency, MarketKind
from rekep.fields import (
    Field,
    FieldBuilder,
    FixedSizeListField,
    LargeListField,
    LargeListViewField,
    ListField,
    ListViewField,
    MapField,
    ProtocolMetadata,
    StructField,
    scalar,
)
from rekep.fix import FieldRule, FieldRules, FixCodec, FixRegistry, Rules
from rekep.market import (
    Book,
    Event,
    Execution,
    FixEvents,
    Instrument,
    InstUpdate,
    MarketEvent,
    Order,
    SymbolTicker,
    TickRule,
)
from rekep.tasks import Task
from rekep.text import Entry, FixMsg, Message
from rekep.times import datetime_of, unix_of

__version__ = package_version("rekep")

__all__ = [
    "Book",
    "Convertible",
    "Ascii32",
    "Ascii64",
    "Ascii128",
    "Currency",
    "Dataset",
    "Event",
    "Execution",
    "Field",
    "FieldBuilder",
    "FixEvents",
    "FixMsg",
    "Entry",
    "FieldRule",
    "FieldRules",
    "FixCodec",
    "FixRegistry",
    "Rules",
    "FixedSizeListField",
    "Instrument",
    "InstUpdate",
    "LargeListField",
    "LargeListViewField",
    "ListField",
    "ListViewField",
    "MapField",
    "MarketEvent",
    "MarketKind",
    "Message",
    "Order",
    "ProtocolMetadata",
    "StructField",
    "SymbolTicker",
    "Task",
    "TickRule",
    "__version__",
    "datetime_of",
    "scalar",
    "unix_of",
]
