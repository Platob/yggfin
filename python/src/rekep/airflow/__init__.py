"""Airflow authoring with data lineage declared by record.

Needs the `airflow` extra -- and a POSIX platform, which is Airflow's own
requirement. Importing this package without Airflow raises at the first
decorator use, not at import, so the rest of rekep stays usable anywhere.
"""

from rekep.airflow.decorators import DAG, dag, task
from rekep.airflow.lineage import asset_name, asset_of, asset_uri, metadata_of
from rekep.airflow.service import Airflow, Dags

__all__ = [
    "DAG",
    "Airflow",
    "Dags",
    "asset_name",
    "asset_of",
    "asset_uri",
    "dag",
    "metadata_of",
    "task",
]
