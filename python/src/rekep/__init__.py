"""Stream captured text lines through FIX into Iceberg data products."""

from importlib.metadata import version as package_version

from yggdryl import DataType, IOBase, Scalar, TextOptions, Uri, Url

from rekep.convert import Convertible
from rekep.dataset import Dataset
from rekep.fields import Field, scalar
from rekep.fix import FixCodec, FixMsg, FixRegistry, fix_registry
from rekep.text import Message
from rekep.times import datetime_of, unix_of

__version__ = package_version("rekep")

__all__ = [
    "Convertible",
    "DataType",
    "Dataset",
    "Field",
    "FixCodec",
    "FixMsg",
    "FixRegistry",
    "IOBase",
    "Message",
    "Scalar",
    "TextOptions",
    "Uri",
    "Url",
    "__version__",
    "datetime_of",
    "fix_registry",
    "scalar",
    "unix_of",
]
