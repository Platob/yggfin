"""The decorators themselves need a working Airflow, which needs POSIX.

`importorskip` is not enough here: `airflow.sdk` itself imports everywhere, and
only resolving `DAG` off it hits the fork-dependent modules that Windows lacks.
The skip has to attempt exactly what the decorators will.
"""

import pytest

try:
    from airflow.sdk import DAG as _  # noqa: F401
except Exception as error:  # pragma: no cover - never taken on POSIX
    pytest.skip(f"Airflow cannot load here: {error}", allow_module_level=True)

from rekep.airflow import DAG, dag, task  # noqa: E402
from rekep.models import Log  # noqa: E402


def test_dag_carries_lineage() -> None:
    with DAG("d", consumes=[Log]) as built:
        pass
    assert "Log" in built.tags
    assert "### Consumes" in built.doc_md


def test_dag_decorator_carries_lineage() -> None:
    @dag(produces=[Log])
    def pipeline() -> None:
        pass

    built = pipeline()
    assert "Log" in built.tags


def test_task_outlets_are_record_assets() -> None:
    @task(produces=[Log])
    def write() -> None:
        pass

    (outlet,) = write().operator.outlets
    assert outlet.uri == "rekep://rekep.models.log.Log"
