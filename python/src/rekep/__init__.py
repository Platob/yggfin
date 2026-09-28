"""Stream captured text lines through FIX into Iceberg data products."""

from importlib.metadata import version as package_version

from yggdryl import (
    DataType,
    IOBase,
    MarketDataKind,
    Scalar,
    Side,
    State,
    TextOptions,
    Uri,
    Url,
)

from rekep.dataset import Dataset
from rekep.fields import Field, scalar
from rekep.fix import FixCodec, FixMsg, FixRegistry
from rekep.storages import Storages
from rekep.times import datetime_of, unix_of

__version__ = package_version("rekep")

__all__ = [
    "DataType",
    "Dataset",
    "Field",
    "FixCodec",
    "FixMsg",
    "FixRegistry",
    "IOBase",
    "MarketDataKind",
    "Scalar",
    "Side",
    "State",
    "Storages",
    "TextOptions",
    "Uri",
    "Url",
    "__version__",
    "datetime_of",
    "scalar",
    "unix_of",
]
