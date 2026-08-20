"""Airflow authoring from jobs, with data lineage declared by record.

Needs the `airflow` extra -- and a POSIX platform, which is Airflow's own
requirement. Nothing here imports Airflow at import time, so the rest of
rekep stays usable anywhere.

**A `Job` is the task.** This package deliberately wraps none of Airflow's
authoring API: no `@dag`, no `@task`, no `DAG` subclass. A job already says
what a task needs, `Job.into_airflow()` hands that to Airflow's own
decorators, and anything Airflow accepts reaches them untouched through
`airflow["dag"]`/`airflow["task"]`. What this package adds is the part
Airflow cannot derive -- the lineage a record declares.
"""

from rekep.airflow.lineage import asset_name, asset_of, asset_uri, metadata_of
from rekep.airflow.service import Airflow, Dags

__all__ = [
    "Airflow",
    "Dags",
    "asset_name",
    "asset_of",
    "asset_uri",
    "metadata_of",
]
