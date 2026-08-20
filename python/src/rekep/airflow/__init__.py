"""Airflow authoring from dags and tasks, with data lineage declared by record.

Needs the `airflow` extra -- and a POSIX platform, which is Airflow's own
requirement. Nothing here imports Airflow at import time, so the rest of
rekep stays usable anywhere.

**Airflow is a projection, not the definition.** `rekep.dag.Dag` is the
graph -- its tasks, its edges, its order, its runner -- and this package
hands one to Airflow's own `DAG` and `@task`, wrapping neither: no `@dag`
decorator of ours, no `DAG` subclass. Anything Airflow accepts reaches it
untouched through `airflow["dag"]`/`airflow["task"]`. What this package adds
is the part Airflow cannot derive -- the lineage a record declares.
"""

from rekep.airflow.lineage import airflow_tags, asset_name, asset_of, asset_uri, metadata_of
from rekep.airflow.service import Airflow, Dags

__all__ = [
    "Airflow",
    "Dags",
    "airflow_tags",
    "asset_name",
    "asset_of",
    "asset_uri",
    "metadata_of",
]
