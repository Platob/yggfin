"""`Dag.into_airflow`: airflow[] config reaches the real DAG/task.

Airflow itself needs POSIX, and only resolving `DAG` off `airflow.sdk` hits
the fork-dependent modules Windows lacks, so the skip has to attempt exactly
what the code will. The wiring itself is covered without Airflow in
`test_into_airflow.py`; this file is the "and it is really Airflow" half.
"""

import pytest

try:
    from airflow.sdk import DAG as _  # noqa: F401
except Exception as error:  # pragma: no cover - never taken on POSIX
    pytest.skip(f"Airflow cannot load here: {error}", allow_module_level=True)

from rekep import config  # noqa: E402
from rekep.dag import Dag  # noqa: E402
from rekep.job import Job  # noqa: E402


def test_dag_and_task_kwargs_reach_the_built_dag() -> None:
    config.register(Job(uri="job:/demo", airflow={"task": {"retries": 3}}))
    dag = Dag(
        uri="dag:/demo",
        schedule="@daily",
        tasks=["job:/demo"],
        tags={"stage": "reference"},
        airflow={"dag": {"max_active_runs": 1}},
    )
    try:
        built = dag.into_airflow()
    finally:
        config.clear()
    assert built.max_active_runs == 1
    assert "stage=reference" in {str(tag) for tag in built.tags}
    (task,) = built.tasks
    assert task.retries == 3
