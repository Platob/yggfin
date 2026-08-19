"""Tasks and dags: rekep's own units, records through and through."""

import logging
import pathlib

import pytest

from rekep.flows import Dag, Passthrough, Task, dag, task
from rekep.models import Log

SAMPLE = pathlib.Path(__file__).parent.parent / "data" / "app_sample.txt"


@task(produces=[Log])
def extract(**context: object) -> int:
    return 3


@task(consumes=[Log], name="report")
def summarise(extract: int = 0, **context: object) -> int:
    return extract * 2


@dag(schedule="@daily", tags=["demo"])
def pipeline() -> list[Task]:
    """Two steps, wired by name."""
    return [extract, summarise]


# -- task -------------------------------------------------------------------


def test_task_binds_the_function_and_keeps_the_path() -> None:
    assert extract.name == "extract"
    assert extract.callable.endswith(".extract")
    assert extract.run() == 3


def test_task_name_override() -> None:
    assert summarise.name == "report"


def test_task_lineage_resolves_to_record_classes() -> None:
    assert extract.produced_records() == [Log]
    assert summarise.consumed_records() == [Log]


def test_task_round_trips_and_resolves_through_the_path() -> None:
    """A task from YAML has no bound function; the dotted path runs it."""
    loaded = Task.from_yaml(extract.into_yaml())
    assert loaded == extract
    assert getattr(loaded, "_Task__fn", None) is None
    assert loaded.run() == 3


def test_a_task_without_any_callable_refuses_to_run() -> None:
    with pytest.raises(ValueError, match="no callable"):
        Task(name="empty").run()


# -- dag --------------------------------------------------------------------


def test_dag_declaration() -> None:
    assert pipeline.name == "pipeline"
    assert pipeline.schedule == "@daily"
    assert pipeline.description == "Two steps, wired by name."
    assert [entry.name for entry in pipeline.tasks] == ["extract", "report"]


def test_dag_lineage_is_the_union() -> None:
    assert pipeline.consumed_records() == [Log]
    assert pipeline.produced_records() == [Log]


def test_dag_run_chains_results_by_name(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="rekep.flows"):
        results = pipeline.run()
    assert results == {"extract": 3, "report": 6}
    assert "task extract: run" in caplog.text


def test_dag_refuses_non_tasks() -> None:
    with pytest.raises(TypeError, match="not a Task"):

        @dag
        def broken() -> list[object]:
            return [object()]


def test_dag_round_trips_as_a_record() -> None:
    assert Dag.from_yaml(pipeline.into_yaml()) == pipeline


def test_dag_run_emits_openlineage_start_and_complete(tmp_path: pathlib.Path) -> None:
    pytest.importorskip("openlineage.client")
    import json

    from rekep.openlineage import OpenLineage

    path = tmp_path / "events.log"
    pipeline.run(lineage=OpenLineage.from_path(path))

    start, complete = (json.loads(line) for line in path.read_text().splitlines())
    assert (start["eventType"], complete["eventType"]) == ("START", "COMPLETE")
    assert start["job"]["name"] == "pipeline"
    assert {d["name"] for d in start["inputs"] + start["outputs"]} == {"Log"}


def test_dag_run_emits_fail_and_reraises(tmp_path: pathlib.Path) -> None:
    pytest.importorskip("openlineage.client")
    import json

    from rekep.openlineage import OpenLineage

    @task
    def boom(**context: object) -> None:
        raise RuntimeError("nope")

    @dag
    def broken() -> list[Task]:
        return [boom]

    path = tmp_path / "events.log"
    with pytest.raises(RuntimeError, match="nope"):
        broken.run(lineage=OpenLineage.from_path(path))

    _, failed = (json.loads(line) for line in path.read_text().splitlines())
    assert failed["eventType"] == "FAIL"
    assert "nope" in failed["run"]["facets"]["errorMessage"]["message"]


# -- flow interop -----------------------------------------------------------


def test_flow_into_dag_carries_config_and_runs() -> None:
    flow = Passthrough(
        name="p",
        schedule="@hourly",
        source=SAMPLE.as_uri(),
        produces=["rekep.models.Log"],
    )
    built = flow.into_dag()
    assert isinstance(built, Dag)
    assert built.schedule == "@hourly"
    assert built.produced_records() == [Log]
    assert built.run() == {"p": 24}, "the flow's reference run is the task body"
