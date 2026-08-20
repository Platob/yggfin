"""`Dag.into_airflow` builds against Airflow's own API -- with a stand-in.

A `Dag` is rekep's own graph and Airflow is one projection of it, so what is
worth pinning is that the dag hands the right arguments to `sdk.DAG` and
`sdk.task`, one task per job, wired the way `dependencies` says. A stand-in
for that module says so without needing Airflow installed, which is what lets
this run everywhere.
"""

import sys
import types
from typing import Any

import pytest

from rekep import config
from rekep.dag import Dag
from rekep.job import Job
from rekep.models import Log, ParsedMessage


class FakeDag:
    """Enough of an Airflow DAG to be entered and to collect its tasks."""

    def __init__(self, dag_id: str, **kwargs: Any) -> None:
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.tasks: list[dict[str, Any]] = []
        self.operators: dict[str, FakeOperator] = {}

    def __enter__(self) -> "FakeDag":
        FakeDag.current = self
        return self

    def __exit__(self, *_: object) -> None:
        FakeDag.current = None


class FakeOperator:
    """What `@task(...)(fn)()` hands back: something edges can be set on."""

    def __init__(self, arguments: dict[str, Any]) -> None:
        self.arguments = arguments
        self.upstream: list[str] = []

    def set_upstream(self, other: "FakeOperator") -> None:
        self.upstream.append(other.arguments["task_id"])


def fake_task(**kwargs: Any) -> Any:
    def decorate(fn: Any) -> Any:
        def call() -> Any:
            arguments = {**kwargs, "callable": fn}
            FakeDag.current.tasks.append(arguments)
            operator = FakeOperator(arguments)
            FakeDag.current.operators[arguments["task_id"]] = operator
            return operator

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


@pytest.fixture(autouse=True)
def registry() -> Any:
    """Tasks are resolved by URI, so each test declares its own and clears up."""
    config.clear()
    yield config
    config.clear()


def declare(uri: str, **kwargs: Any) -> Job:
    return config.register(Job(uri=uri, **kwargs))


def test_a_dag_becomes_one_airflow_dag_with_one_task_per_job(sdk: Any) -> None:
    declare("rekep:///jobs/pipeline/extract")
    declare("rekep:///jobs/pipeline/load")
    built = Dag(
        uri="rekep:///dags/pipeline/demo",
        tasks=["rekep:///jobs/pipeline/extract", "rekep:///jobs/pipeline/load"],
        description="what it does",
        schedule="@daily",
    ).into_airflow()
    assert built.dag_id == "demo"
    assert built.kwargs["schedule"] == "@daily"
    assert built.kwargs["description"] == "what it does"
    assert built.kwargs["catchup"] is False
    assert [task["task_id"] for task in built.tasks] == ["extract", "load"]


def test_each_task_runs_its_own_job(sdk: Any) -> None:
    job = declare("rekep:///jobs/pipeline/extract")
    (task,) = Dag(uri="rekep:///dags/pipeline/demo", tasks=[job.uri]).into_airflow().tasks
    assert task["callable"] == job.run, "the job is the task"


def test_dependencies_become_airflow_edges(sdk: Any) -> None:
    declare("rekep:///jobs/pipeline/extract")
    declare("rekep:///jobs/pipeline/load")
    dag = Dag(
        uri="rekep:///dags/pipeline/demo",
        tasks=["rekep:///jobs/pipeline/extract", "rekep:///jobs/pipeline/load"],
        dependencies={"load": ["extract"]},
    )
    built = dag.into_airflow()
    assert built.operators["load"].upstream == ["extract"]
    assert built.operators["extract"].upstream == []


def test_airflow_config_passes_straight_through(sdk: Any) -> None:
    """rekep keeps no list of which kwarg belongs to which; Airflow has one."""
    declare("rekep:///jobs/pipeline/extract", airflow={"task": {"retries": 3, "pool": "etl"}})
    built = Dag(
        uri="rekep:///dags/pipeline/demo",
        tasks=["rekep:///jobs/pipeline/extract"],
        airflow={"dag": {"max_active_runs": 1}},
    ).into_airflow()
    assert built.kwargs["max_active_runs"] == 1
    (task,) = built.tasks
    assert (task["retries"], task["pool"]) == (3, "etl")


def test_a_task_overrides_the_dags_own_task_defaults(sdk: Any) -> None:
    declare("rekep:///jobs/pipeline/extract", airflow={"task": {"retries": 3}})
    declare("rekep:///jobs/pipeline/load")
    built = Dag(
        uri="rekep:///dags/pipeline/demo",
        tasks=["rekep:///jobs/pipeline/extract", "rekep:///jobs/pipeline/load"],
        airflow={"task": {"retries": 1, "pool": "etl"}},
    ).into_airflow()
    extract, load = built.tasks
    assert (extract["retries"], extract["pool"]) == (3, "etl"), "the task's own wins"
    assert load["retries"] == 1, "the dag's default stands where nothing overrides it"


def test_lineage_is_derived_not_declared(sdk: Any) -> None:
    declare(
        "rekep:///jobs/pipeline/parse",
        consumes=["rekep.models.Log"],
        produces=["rekep.models.ParsedMessage"],
    )
    built = Dag(
        uri="rekep:///dags/pipeline/demo",
        tasks=["rekep:///jobs/pipeline/parse"],
        tags={"mine": "yes"},
    ).into_airflow()
    assert set(built.kwargs["tags"]) >= {"mine=yes", "Log=consumes", "ParsedMessage=produces"}
    assert "### Consumes" in built.kwargs["doc_md"]
    (task,) = built.tasks
    from rekep.airflow import lineage

    assert task["inlets"] == [lineage.asset_uri(Log)]
    assert task["outlets"] == [lineage.asset_uri(ParsedMessage)]


def test_a_dag_declaring_no_lineage_gets_no_assets(sdk: Any) -> None:
    declare("rekep:///jobs/pipeline/extract")
    (task,) = (
        Dag(uri="rekep:///dags/pipeline/demo", tasks=["rekep:///jobs/pipeline/extract"])
        .into_airflow()
        .tasks
    )
    assert "inlets" not in task
    assert "outlets" not in task


def test_a_single_job_dag_needs_no_side_file(sdk: Any) -> None:
    """`Dag.from_job` is the one-step pipeline, named after the task it runs."""
    job = declare("rekep:///jobs/pipeline/extract", schedule="@hourly", tags={"stage": "ingestion"})
    built = Dag.from_job(job).into_airflow()
    assert built.dag_id == "extract"
    assert built.kwargs["schedule"] == "@hourly"
    assert built.kwargs["tags"] == ["stage=ingestion"]
    assert [task["task_id"] for task in built.tasks] == ["extract"]
