"""rekep: trading log parser."""

# Importing is declaring: a class can only be found by name once Python has
# run its `class` statement, so the packages this one ships are imported here
# rather than left to whoever happens to reference them first. A deployment's
# own modules go in $REKEP_MODULES -- see `rekep.classes`.
from rekep import jobs as jobs
from rekep import models as models
from rekep.convert import Convertible
from rekep.dag import Dag
from rekep.dataset import Dataset
from rekep.job import Job, arrow_task
from rekep.models import Log
from rekep.namespace import Namespace, ResourceUri
from rekep.records import Arrow, ArrowFieldBuilder, Record, record
from rekep.run import Run, RunEvent, RunState

__version__ = "0.1.0"

__all__ = [
    "Arrow",
    "ArrowFieldBuilder",
    "Convertible",
    "Dag",
    "Dataset",
    "Job",
    "Log",
    "Namespace",
    "Record",
    "ResourceUri",
    "Run",
    "RunEvent",
    "RunState",
    "__version__",
    "arrow_task",
    "record",
]
