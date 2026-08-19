"""Flows: tasks, dags and data movements, declared as records.

`@task` binds a function to record lineage; `@dag` orders tasks; `Flow` is
the streaming specialisation whose one abstract method is `arrow_transform`.
All three are records, so side files, dumps and orchestrators see the same
declaration the code runs.
"""

from rekep.flows.dag import Dag, dag
from rekep.flows.flow import FLOWS_ROOT, Flow, load, load_all
from rekep.flows.passthrough import Passthrough
from rekep.flows.task import Task, task

__all__ = [
    "FLOWS_ROOT",
    "Dag",
    "Flow",
    "Passthrough",
    "Task",
    "dag",
    "load",
    "load_all",
    "task",
]
