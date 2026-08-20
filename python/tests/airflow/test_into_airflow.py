"""`Job.into_airflow` builds against Airflow's own API -- with a stand-in.

A `Job` **is** the task: there is no rekep decorator between it and Airflow
any more, so what is worth pinning is that the job hands the right arguments
to `sdk.DAG` and `sdk.task`. A stand-in for that module says so without
needing Airflow installed, which is what lets this run everywhere.
"""

import sys
import types
from typing import Any

import pytest

from rekep.job import Job
from rekep.models import Log, ParsedMessage


class FakeDag:
    """Enough of an Airflow DAG to be entered and to collect its task."""

    def __init__(self, dag_id: str, **kwargs: Any) -> None:
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.tasks: list[dict[str, Any]] = []

    def __enter__(self) -> "FakeDag":
        FakeDag.current = self
        return self

    def __exit__(self, *_: object) -> None:
        FakeDag.current = None


def fake_task(**kwargs: Any) -> Any:
    def decorate(fn: Any) -> Any:
        def call() -> Any:
            FakeDag.current.tasks.append({**kwargs, "callable": fn})
            return fn

        return call

    return decorate


@pytest.fixture
def sdk(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Stand in for `rekep.airflow.sdk`, which imports Airflow at import."""
    import rekep.airflow
    from rekep.airflow import lineage

    stand_in = types.SimpleNamespace(DAG=FakeDag, task=fake_task)
    monkeypatch.setitem(sys.modules, "rekep.airflow.sdk", stand_in)
    monkeypatch.setattr(rekep.airflow, "sdk", stand_in, raising=False)
    monkeypatch.setattr(lineage, "asset_of", lineage.asset_uri)
    return stand_in


def test_a_job_becomes_one_dag_with_one_task(sdk: Any) -> None:
    built = Job(name="demo", description="what it does", schedule="@daily").into_airflow()
    assert built.dag_id == "demo"
    assert built.kwargs["schedule"] == "@daily"
    assert built.kwargs["description"] == "what it does"
    assert built.kwargs["catchup"] is False
    (task,) = built.tasks
    assert task["task_id"] == "demo"


def test_the_task_runs_the_job_tracked(sdk: Any) -> None:
    job = Job(name="demo")
    (task,) = job.into_airflow().tasks
    assert task["callable"] == job.run_tracked, "the job is the task"


def test_airflow_config_passes_straight_through(sdk: Any) -> None:
    """rekep keeps no list of which kwarg belongs to which; Airflow has one."""
    job = Job(
        name="demo",
        airflow={"dag": {"max_active_runs": 1}, "task": {"retries": 3, "pool": "etl"}},
    )
    built = job.into_airflow()
    assert built.kwargs["max_active_runs"] == 1
    (task,) = built.tasks
    assert (task["retries"], task["pool"]) == (3, "etl")


def test_lineage_is_derived_not_declared(sdk: Any) -> None:
    job = Job(
        name="demo",
        consumes=["rekep.models.Log"],
        produces=["rekep.models.ParsedMessage"],
        tags=["mine"],
    )
    built = job.into_airflow()
    assert set(built.kwargs["tags"]) >= {"mine", "Log", "ParsedMessage", "rekep"}
    assert "### Consumes" in built.kwargs["doc_md"]
    (task,) = built.tasks
    from rekep.airflow import lineage

    assert task["inlets"] == [lineage.asset_uri(Log)]
    assert task["outlets"] == [lineage.asset_uri(ParsedMessage)]


def test_a_job_declaring_no_lineage_gets_no_assets(sdk: Any) -> None:
    (task,) = Job(name="demo").into_airflow().tasks
    assert "inlets" not in task
    assert "outlets" not in task
