import json
import pathlib
from collections.abc import Iterator

import pyarrow
import pytest

import rekep.job
from rekep.job import Job, Passthrough, arrow_task, load, load_all
from rekep.lineage import Collector
from rekep.models import Log
from rekep.records import record
from rekep.run import RunState

SAMPLE = pathlib.Path(__file__).parent / "data" / "app_sample.txt"
REPO_JOBS = pathlib.Path(__file__).parents[2] / "stacks" / "jobs"


@record
class Doubler(Job):
    """Every batch twice, for telling transform output from input."""

    def arrow_transform(
        self, batches: Iterator[pyarrow.RecordBatch]
    ) -> Iterator[pyarrow.RecordBatch]:
        for batch in batches:
            yield batch
            yield batch


# -- the class ----------------------------------------------------------


def test_bare_job_has_no_transform_but_still_declares() -> None:
    """`Job` is concrete -- not enforced abstract -- so it round-trips."""
    job = Job(name="nope")
    assert Job.from_json(job.into_json()) == job
    with pytest.raises(NotImplementedError, match="arrow_transform"):
        job.run()


def test_passthrough_is_the_identity() -> None:
    batch = pyarrow.RecordBatch.from_pydict({"a": [1, 2, 3]})
    (out,) = list(Passthrough(name="p").arrow_transform(iter([batch])))
    assert out is batch


def test_job_is_a_record() -> None:
    job = Passthrough(name="p", schedule="@daily", consumes=["rekep.models.Log"])
    assert Passthrough.from_json(job.into_json()) == job


def test_lineage_paths_resolve_to_record_classes() -> None:
    job = Passthrough(name="p", produces=["rekep.models.Log"])
    assert job.produced_records() == [Log]
    assert job.consumed_records() == []


def test_a_non_record_lineage_path_is_refused() -> None:
    job = Passthrough(name="p", consumes=["pathlib.Path"])
    with pytest.raises(TypeError, match="not a Record"):
        job.consumed_records()


# -- identity -------------------------------------------------------------


def test_qualified_name_joins_namespace_and_name() -> None:
    assert Job(name="task", namespace="dag").qualified_name() == "dag.task"


def test_qualified_name_without_a_namespace_is_just_the_name() -> None:
    assert Job(name="task").qualified_name() == "task"


def test_uri_is_scoped_to_the_job_scheme() -> None:
    assert str(Job(name="orders", namespace="trading").resource_uri()) == "job:/trading/orders"


# -- bind / @arrow_task -------------------------------------------------


def test_bind_attaches_a_transform_that_run_uses() -> None:
    def double(batches: Iterator[pyarrow.RecordBatch]) -> Iterator[pyarrow.RecordBatch]:
        for batch in batches:
            yield batch
            yield batch

    job = Job(name="bound", source=SAMPLE.as_uri()).bind(double)
    assert job.run() == 48


def test_arrow_task_bare_builds_a_job_named_after_the_function() -> None:
    @arrow_task
    def passthrough(batches: Iterator[pyarrow.RecordBatch]) -> Iterator[pyarrow.RecordBatch]:
        yield from batches

    assert isinstance(passthrough, Job)
    assert passthrough.name == "passthrough"


def test_arrow_task_configured_carries_lineage_and_namespace() -> None:
    @arrow_task(name="etl", namespace="trading", consumes=[Log], produces=[Log])
    def transform(batches: Iterator[pyarrow.RecordBatch]) -> Iterator[pyarrow.RecordBatch]:
        yield from batches

    assert transform.qualified_name() == "trading.etl"
    assert transform.consumed_records() == [Log]
    assert transform.produced_records() == [Log]


def test_arrow_task_config_wins_over_kwargs() -> None:
    configured = Job(name="preloaded", source=SAMPLE.as_uri())

    @arrow_task(config=configured, name="ignored")
    def passthrough(batches: Iterator[pyarrow.RecordBatch]) -> Iterator[pyarrow.RecordBatch]:
        yield from batches

    assert passthrough is configured
    assert passthrough.name == "preloaded"
    assert passthrough.run() == 24


def test_calling_an_arrow_task_runs_it_tracked() -> None:
    @arrow_task(name="counted", namespace="trading", source=SAMPLE.as_uri(), produces=[Log])
    def passthrough(batches: Iterator[pyarrow.RecordBatch]) -> Iterator[pyarrow.RecordBatch]:
        yield from batches

    collector = Collector()
    assert passthrough.with_lineage(collector)() == 24
    assert [e.event_type for e in collector.events] == [RunState.START, RunState.COMPLETE]
    assert collector.events[0].outputs[0].namespace == "trading"
    assert collector.events[0].outputs[0].name == "log"


# -- run_tracked ----------------------------------------------------------


def test_run_tracked_emits_start_then_complete() -> None:
    collector = Collector()
    job = Passthrough(name="p", source=SAMPLE.as_uri()).with_lineage(collector)
    assert job.run_tracked() == 24
    assert [e.event_type for e in collector.events] == [RunState.START, RunState.COMPLETE]


def test_run_tracked_emits_start_then_fail_and_reraises() -> None:
    collector = Collector()
    job = Passthrough(name="p").with_lineage(collector)  # no source: extract() raises
    with pytest.raises(NotImplementedError, match="override extract"):
        job.run_tracked()
    assert [e.event_type for e in collector.events] == [RunState.START, RunState.FAIL]


def test_a_failure_carries_the_error_into_the_run_facets() -> None:
    """A FAIL that does not say what went wrong is worth less than the
    traceback the caller is about to see anyway."""
    collector = Collector()
    with pytest.raises(NotImplementedError):
        Passthrough(name="p").with_lineage(collector).run_tracked()
    (failed,) = collector.of(RunState.FAIL)
    assert "NotImplementedError" in failed.run.facets["errorMessage"]["message"]


def test_runs_are_not_shared_between_instances() -> None:
    mine, theirs = Collector(), Collector()
    Passthrough(name="a", source=SAMPLE.as_uri()).with_lineage(mine).run_tracked()
    Passthrough(name="b", source=SAMPLE.as_uri()).with_lineage(theirs)
    assert mine.events
    assert theirs.events == []


def test_without_a_client_run_tracked_is_just_run() -> None:
    """Not "tracked and discarded" -- no run is built at all."""
    job = Passthrough(name="p", source=SAMPLE.as_uri())
    assert job.lineage() is None
    assert job.run_tracked() == job.run()


# -- run --------------------------------------------------------------------


def test_run_extracts_transforms_and_counts() -> None:
    job = Passthrough(name="p", source=SAMPLE.as_uri())
    assert job.run() == 24


def test_transform_output_is_what_load_sees() -> None:
    assert Doubler(name="d", source=SAMPLE.as_uri()).run() == 48


def test_run_without_a_source_says_what_to_override() -> None:
    with pytest.raises(NotImplementedError, match="override extract"):
        Passthrough(name="p").run()


# -- side files -------------------------------------------------------------


def test_load_builds_the_declared_class(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "job.json"
    path.write_text(json.dumps({"job": "rekep.job.Passthrough", "name": "j"}))
    job = load(path)
    assert isinstance(job, Passthrough)
    assert job.name == "j"


def test_load_renders_jinja_with_the_environment(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BUCKET", "s3://lake")
    path = tmp_path / "job.yaml"
    path.write_text('job: rekep.job.Passthrough\nname: y\nsource: "{{ env.BUCKET }}/app.txt"\n')
    assert load(path).source == "s3://lake/app.txt"


def test_load_passes_extra_context(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "job.yaml"
    path.write_text('job: rekep.job.Passthrough\nname: "{{ suffix }}"\n')
    assert load(path, suffix="rendered").name == "rendered"


def test_load_requires_a_job_key(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "job.yaml"
    path.write_text("name: anonymous\n")
    with pytest.raises(ValueError, match="declares no"):
        load(path)


def test_load_refuses_a_non_job_class(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "job.yaml"
    path.write_text("job: rekep.models.Log\nname: x\n")
    with pytest.raises(TypeError, match="not a Job subclass"):
        load(path)


def test_load_allows_the_bare_job_class(tmp_path: pathlib.Path) -> None:
    """Concrete, not abstract: a purely descriptive job is a valid side file."""
    path = tmp_path / "job.yaml"
    path.write_text("job: rekep.job.Job\nname: x\n")
    job = load(path)
    assert type(job) is Job
    with pytest.raises(NotImplementedError, match="arrow_transform"):
        job.run()


def test_load_all_reads_a_directory(tmp_path: pathlib.Path) -> None:
    (tmp_path / "b.yaml").write_text("job: rekep.job.Passthrough\nname: b\n")
    (tmp_path / "a.json").write_text(json.dumps({"job": "rekep.job.Passthrough", "name": "a"}))
    (tmp_path / "notes.txt").write_text("not a job")
    jobs = load_all(tmp_path)
    assert [job.name for job in jobs] == ["a", "b"], "sorted, and .txt ignored"


def test_the_shipped_side_files_load(monkeypatch: pytest.MonkeyPatch) -> None:
    """Whatever is committed under stacks/jobs must actually parse."""
    monkeypatch.setenv("REKEP_SOURCE_URL", "file:///dev/null")
    jobs = load_all(REPO_JOBS)
    assert jobs, "stacks/jobs has no side files"
    assert all(isinstance(job, rekep.job.Job) for job in jobs)


def test_the_shipped_files_to_logs_declares_its_full_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REKEP_SOURCE_URL", "file:///dev/null")
    jobs = {job.name: job for job in load_all(REPO_JOBS)}
    f2l = jobs["files_to_logs"]
    assert f2l.namespace == "pipeline", "stable across branches, unlike logs_to_records"
    assert f2l.repo_url == "https://github.com/Platob/yggfin"
    assert f2l.script_path == "python/src/rekep/jobs/files_to_logs.py"
    assert f2l.env["LOG_LEVEL"] == "INFO"
    assert f2l.properties["team"] == "trading-platform"
    assert f2l.airflow["dag"]["max_active_runs"] == 1
    assert f2l.airflow["task"]["retries"] == 2


def test_the_shipped_logs_to_records_picks_up_the_branch_suffix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rekep.render import git_context

    monkeypatch.setenv("REKEP_SOURCE_URL", "file:///dev/null")
    git_context.cache_clear()
    try:
        suffix = git_context()["git_branch_suffix"]
        jobs = {job.name: job for job in load_all(REPO_JOBS)}
        l2r = jobs[f"logs_to_records{suffix}"]
        assert l2r.namespace == f"pipeline{suffix}"
        assert l2r.consumed_records() == [Log]
    finally:
        git_context.cache_clear()


# -- python config / airflow / env -----------------------------------------


def test_source_code_location_facet_carries_repo_and_path() -> None:
    job = Job(name="j", repo_url="https://github.com/Platob/yggfin", script_path="a/b.py")
    facet = job.source_code_location_facet()
    assert facet["type"] == "git"
    assert facet["repoUrl"] == "https://github.com/Platob/yggfin"
    assert facet["path"] == "a/b.py"
    assert "version" in facet  # the current git sha, whatever it is here


def test_facets_include_source_code_location_only_when_declared() -> None:
    assert Job(name="j").facets() == {}
    declared = Job(name="j", repo_url="https://github.com/Platob/yggfin")
    assert "sourceCodeLocation" in declared.facets()


def test_env_airflow_and_properties_round_trip() -> None:
    job = Job(
        name="j",
        env={"BUCKET": "s3://lake"},
        properties={"team": "trading"},
        airflow={"dag": {"max_active_runs": 1}, "task": {"retries": 2}},
    )
    assert Job.from_json(job.into_json()) == job
