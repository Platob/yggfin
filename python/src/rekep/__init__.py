"""Stream raw text records into Iceberg tables."""

from importlib.metadata import version as package_version

from rekep.convert import Convertible
from rekep.dataset import Dataset
from rekep.fields import Field, scalar
from rekep.tasks import Task
from rekep.text import Message
from rekep.times import datetime_of, unix_of

__version__ = package_version("rekep")

__all__ = [
    "Convertible",
    "Dataset",
    "Field",
    "Message",
    "Task",
    "__version__",
    "datetime_of",
    "scalar",
    "unix_of",
]
