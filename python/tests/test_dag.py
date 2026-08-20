"""The dag: our own graph -- resolved, validated, ordered and run here."""

from __future__ import annotations

import pathlib
from typing import Any

import pytest

from rekep import config
from rekep.dag import Dag, find, load, load_all
from rekep.job import Job, Passthrough
from rekep.models import Log

SAMPLE = pathlib.Path(__file__).parent / "data" / "app_sample.txt"
REPO_DAGS = pathlib.Path(__file__).parents[2] / "stacks" / "dags"
REPO_JOBS = pathlib.Path(__file__).parents[2] / "stacks" / "jobs"


@pytest.fixture(autouse=True)
def registry() -> Any:
    """Tasks resolve through the shared registry, so each test starts empty."""
    config.clear()
    yield config
    config.clear()


def declare(uri: str, **kwargs: Any) -> Job:
    return config.register(Passthrough(uri=uri, **kwargs))


def chain() -> Dag:
    """extract -> transform -> load, declared out of order on purpose."""
    for name in ("extract", "transform", "load"):
        declare(f"job:/pipeline/{name}")
    return Dag(
        uri="dag:/pipeline/demo",
        tasks=["job:/pipeline/load", "job:/pipeline/extract", "job:/pipeline/transform"],
        dependencies={"load": ["transform"], "transform": ["extract"]},
    )


# -- identity ---------------------------------------------------------------


def test_dag_id_and_namespace_read_the_levels_back_out() -> None:
    dag = Dag(uri="dag:/pipeline/trading_logs")
    assert (dag.dag_id(), dag.dag_namespace()) == ("trading_logs", "pipeline")
    assert dag.dag_name() == "pipeline.trading_logs"


def test_uri_is_scoped_to_the_dag_scheme() -> None:
    """A dag and a task may share a name without sharing an identity."""
    assert str(Dag(uri="pipeline/orders").resource_uri()) == "dag:/pipeline/orders"
    assert Dag(uri="dag:/pipeline/orders").resource_uri() != (
        Job(uri="job:/pipeline/orders").resource_uri()
    )


def test_a_task_is_named_by_the_dag_that_runs_it() -> None:
    """OpenLineage's `dag_id.task_id`: two dags may both run a `load`."""
    job = declare("job:/pipeline/load")
    assert Dag(uri="dag:/pipeline/demo").task_name(job) == "demo.load"
    assert Dag(uri="dag:/pipeline/other").task_name("load") == "other.load"


def test_a_dag_is_a_record_and_round_trips() -> None:
    dag = chain()
    assert Dag.from_json(dag.into_json()) == dag


# -- the graph --------------------------------------------------------------


def test_tasks_resolve_to_the_registered_objects() -> None:
    job = declare("job:/pipeline/extract")
    dag = Dag(uri="dag:/pipeline/demo", tasks=["job:/pipeline/extract"])
    assert dag.tasks_by_id() == {"extract": job}
    assert dag.task("extract") is job, "the same object, not a copy"


def test_an_unknown_task_says_what_the_dag_has() -> None:
    dag = chain()
    with pytest.raises(KeyError, match="no task 'nope'"):
        dag.task("nope")


def test_two_tasks_sharing_an_id_are_refused() -> None:
    """`dependencies` names a task by its id, so a duplicate is ambiguous."""
    declare("job:/a/load")
    declare("job:/b/load")
    dag = Dag(uri="dag:/pipeline/demo", tasks=["job:/a/load", "job:/b/load"])
    with pytest.raises(ValueError, match="two tasks called 'load'"):
        dag.tasks_by_id()


def test_order_is_topological() -> None:
    assert chain().order() == ["extract", "transform", "load"]


def test_order_falls_back_to_declaration_order() -> None:
    """Among ready tasks the one written first goes first, so a run is
    reproducible rather than reproducible-on-this-machine."""
    for name in ("c", "a", "b"):
        declare(f"job:/pipeline/{name}")
    dag = Dag(
        uri="dag:/pipeline/demo", tasks=["job:/pipeline/c", "job:/pipeline/a", "job:/pipeline/b"]
    )
    assert dag.order() == ["c", "a", "b"]


def test_a_cycle_is_refused_by_name() -> None:
    declare("job:/pipeline/a")
    declare("job:/pipeline/b")
    dag = Dag(
        uri="dag:/pipeline/demo",
        tasks=["job:/pipeline/a", "job:/pipeline/b"],
        dependencies={"a": ["b"], "b": ["a"]},
    )
    with pytest.raises(ValueError, match="cycle"):
        dag.order()


def test_an_edge_naming_an_undeclared_task_is_refused() -> None:
    declare("job:/pipeline/a")
    dag = Dag(uri="dag:/pipeline/demo", tasks=["job:/pipeline/a"], dependencies={"a": ["nowhere"]})
    with pytest.raises(ValueError, match="nowhere"):
        dag.upstreams()


def test_every_task_appears_in_the_graph_edges_or_not() -> None:
    assert chain().upstreams() == {
        "load": ["transform"],
        "extract": [],
        "transform": ["extract"],
    }


def test_the_graph_reads_both_ways() -> None:
    assert chain().downstreams()["extract"] == ["transform"]


def test_roots_and_leaves_are_where_a_run_starts_and_ends() -> None:
    dag = chain()
    assert dag.roots() == ["extract"]
    assert dag.leaves() == ["load"]


# -- running ----------------------------------------------------------------


def test_run_walks_the_order_and_returns_what_each_task_returned() -> None:
    declare("job:/pipeline/first", source=SAMPLE.as_uri())
    declare("job:/pipeline/second", source=SAMPLE.as_uri())
    dag = Dag(
        uri="dag:/pipeline/demo",
        tasks=["job:/pipeline/second", "job:/pipeline/first"],
        dependencies={"second": ["first"]},
    )
    assert list(dag.run()) == ["first", "second"], "in order, not as declared"
    assert dag.run() == {"first": 24, "second": 24}


def test_a_failing_task_stops_the_run() -> None:
    """The tasks after it declared they needed it; a swallowed failure would
    report success for a pipeline that did not happen."""
    declare("job:/pipeline/first")  # no source: extract() raises
    declare("job:/pipeline/second", source=SAMPLE.as_uri())
    dag = Dag(
        uri="dag:/pipeline/demo",
        tasks=["job:/pipeline/first", "job:/pipeline/second"],
        dependencies={"second": ["first"]},
    )
    with pytest.raises(NotImplementedError, match="override extract"):
        dag.run()


# -- lineage ----------------------------------------------------------------


def test_a_dag_adds_up_its_tasks_lineage() -> None:
    declare("job:/pipeline/extract", produces=["rekep.models.Log"])
    declare("job:/pipeline/count", consumes=["rekep.models.Log"])
    dag = Dag(
        uri="dag:/pipeline/demo",
        tasks=["job:/pipeline/extract", "job:/pipeline/count"],
        dependencies={"count": ["extract"]},
    )
    assert dag.produced_records() == [Log]
    assert dag.consumed_records() == [Log]


def test_a_record_two_tasks_read_is_listed_once() -> None:
    declare("job:/pipeline/a", consumes=["rekep.models.Log"])
    declare("job:/pipeline/b", consumes=["rekep.models.Log"])
    dag = Dag(uri="dag:/pipeline/demo", tasks=["job:/pipeline/a", "job:/pipeline/b"])
    assert dag.consumed_records() == [Log]


# -- building from a single job ---------------------------------------------


def test_from_job_builds_the_one_task_dag() -> None:
    job = declare("job:/pipeline/extract", schedule="@daily", tags={"stage": "ingestion"})
    dag = Dag.from_job(job)
    assert str(dag.resource_uri()) == "dag:/pipeline/extract"
    assert dag.tasks == ["job:/pipeline/extract"]
    assert (dag.schedule, dag.tags) == ("@daily", {"stage": "ingestion"})
    assert dag.order() == ["extract"]


def test_from_job_takes_overrides() -> None:
    job = declare("job:/pipeline/extract")
    assert Dag.from_job(job, uri="dag:/other/name").dag_id() == "name"


# -- side files -------------------------------------------------------------


def test_load_needs_no_class_declared(tmp_path: pathlib.Path) -> None:
    """A graph of references has nothing to subclass for, so `Dag` is the
    default rather than a required ceremony."""
    path = tmp_path / "d.yaml"
    path.write_text("uri: dag:/pipeline/demo\ntasks: [job:/pipeline/a]\n")
    assert type(load(path)) is Dag


def test_load_builds_a_declared_subclass(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "d.yaml"
    path.write_text("dag: rekep.dag.Dag\nuri: dag:/pipeline/demo\n")
    assert type(load(path)) is Dag


def test_load_refuses_a_non_dag_class(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "d.yaml"
    path.write_text("dag: rekep.models.Log\nuri: dag:/x\n")
    with pytest.raises(TypeError, match="not a Dag subclass"):
        load(path)


def test_load_renders_jinja_with_the_environment(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STAGE", "dev")
    path = tmp_path / "d.yaml"
    path.write_text('uri: "dag:/pipeline/demo_{{ env.STAGE }}"\n')
    assert load(path).dag_id() == "demo_dev"


def test_load_all_reads_a_directory_and_registers(tmp_path: pathlib.Path) -> None:
    (tmp_path / "b.yaml").write_text("uri: dag:/b\n")
    (tmp_path / "a.json").write_text('{"uri": "dag:/a"}')
    (tmp_path / "notes.txt").write_text("not a dag")
    assert [dag.dag_id() for dag in load_all(tmp_path)] == ["a", "b"]
    assert find("dag:/a", tmp_path).dag_id() == "a"
    assert find("rekep:/dags/a", tmp_path) is find("dag:/a", tmp_path)


def test_find_says_where_it_looked(tmp_path: pathlib.Path) -> None:
    with pytest.raises(KeyError, match="no dag"):
        find("dag:/nowhere", tmp_path)


# -- the shipped pipeline ---------------------------------------------------


def test_the_shipped_dags_load_and_order(monkeypatch: pytest.MonkeyPatch) -> None:
    """Whatever is committed under stacks/dags must parse, resolve and order."""
    from rekep.render import git_context

    monkeypatch.setenv("REKEP_SOURCE_URL", "file:///dev/null")
    git_context.cache_clear()
    try:
        suffix = git_context()["git_branch_suffix"]
        dags = {dag.dag_id(): dag for dag in load_all(REPO_DAGS)}
        pipeline = dags[f"trading_logs{suffix}"]
        assert pipeline.order(REPO_JOBS) == ["files_to_logs", f"logs_to_records{suffix}"]
        assert pipeline.tags == {"domain": "pipeline", "owner": "data-eng"}
        assert [record.__name__ for record in pipeline.produced_records(REPO_JOBS)] == [
            "Log",
            "ParsedMessage",
        ]
    finally:
        git_context.cache_clear()
