import json
import pathlib
from collections.abc import Iterator

import pyarrow
import pytest

import rekep.job
from rekep.job import Job, Passthrough, arrow_task, find, load, load_all
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
    job = Job(uri="rekep:///jobs/nope")
    assert Job.from_json(job.into_json()) == job
    with pytest.raises(NotImplementedError, match="arrow_transform"):
        job.run()


def test_passthrough_is_the_identity() -> None:
    batch = pyarrow.RecordBatch.from_pydict({"a": [1, 2, 3]})
    (out,) = list(Passthrough(uri="rekep:///jobs/p").arrow_transform(iter([batch])))
    assert out is batch


def test_job_is_a_record() -> None:
    job = Passthrough(uri="rekep:///jobs/p", schedule="@daily", consumes=["rekep.models.Log"])
    assert Passthrough.from_json(job.into_json()) == job


def test_lineage_paths_resolve_to_record_classes() -> None:
    job = Passthrough(uri="rekep:///jobs/p", produces=["rekep.models.Log"])
    assert job.produced_records() == [Log]
    assert job.consumed_records() == []


def test_a_non_record_lineage_path_is_refused() -> None:
    job = Passthrough(uri="rekep:///jobs/p", consumes=["pathlib.Path"])
    with pytest.raises(TypeError, match="not a Record"):
        job.consumed_records()


# -- identity -------------------------------------------------------------


def test_task_name_joins_every_level_of_the_uri() -> None:
    assert Job(uri="rekep:///jobs/dag/task").task_name() == "dag.task"


def test_task_name_of_a_bare_uri_is_just_the_name() -> None:
    assert Job(uri="rekep:///jobs/task").task_name() == "task"


def test_task_id_and_namespace_read_the_levels_back_out() -> None:
    job = Job(uri="rekep:///jobs/trading/orders")
    assert (job.task_id(), job.task_namespace()) == ("orders", "trading")


def test_an_unqualified_task_lands_in_the_default_namespace() -> None:
    assert Job(uri="rekep:///jobs/orders").task_namespace() == "default"


def test_uri_is_scoped_to_the_job_scheme() -> None:
    """A bare path is a job here, and reads back as one however it was spelled."""
    assert str(Job(uri="trading/orders").resource_uri()) == "rekep:///jobs/trading/orders"
    assert (
        str(Job(uri="rekep:///jobs/trading/orders").resource_uri())
        == "rekep:///jobs/trading/orders"
    )


# -- bind / @arrow_task -------------------------------------------------


def test_bind_attaches_a_transform_that_run_uses() -> None:
    def double(batches: Iterator[pyarrow.RecordBatch]) -> Iterator[pyarrow.RecordBatch]:
        for batch in batches:
            yield batch
            yield batch

    job = Job(uri="rekep:///jobs/bound", source=SAMPLE.as_uri()).bind(double)
    assert job.run() == 48


def test_arrow_task_bare_builds_a_job_named_after_the_function() -> None:
    @arrow_task
    def passthrough(batches: Iterator[pyarrow.RecordBatch]) -> Iterator[pyarrow.RecordBatch]:
        yield from batches

    assert isinstance(passthrough, Job)
    assert passthrough.task_id() == "passthrough"


def test_arrow_task_configured_carries_lineage_and_identity() -> None:
    @arrow_task(uri="rekep:///jobs/trading/etl", consumes=[Log], produces=[Log])
    def transform(batches: Iterator[pyarrow.RecordBatch]) -> Iterator[pyarrow.RecordBatch]:
        yield from batches

    assert transform.task_name() == "trading.etl"
    assert transform.consumed_records() == [Log]
    assert transform.produced_records() == [Log]


def test_arrow_task_config_wins_over_kwargs() -> None:
    configured = Job(uri="rekep:///jobs/preloaded", source=SAMPLE.as_uri())

    @arrow_task(config=configured, uri="rekep:///jobs/ignored")
    def passthrough(batches: Iterator[pyarrow.RecordBatch]) -> Iterator[pyarrow.RecordBatch]:
        yield from batches

    assert passthrough is configured
    assert passthrough.task_id() == "preloaded"
    assert passthrough.run() == 24


def test_calling_an_arrow_task_runs_it() -> None:
    @arrow_task(uri="rekep:///jobs/trading/counted", source=SAMPLE.as_uri(), produces=[Log])
    def passthrough(batches: Iterator[pyarrow.RecordBatch]) -> Iterator[pyarrow.RecordBatch]:
        yield from batches

    assert passthrough() == 24


# -- the run's representation ----------------------------------------------


def test_into_run_event_describes_the_task_and_what_it_moves() -> None:
    """rekep represents a run; it does not emit one. There is no client."""
    job = Passthrough(uri="rekep:///jobs/trading/p", consumes=["rekep.models.Log"])
    event = job.into_run_event(RunState.START)
    assert event.event_type is RunState.START
    assert event.job is job
    assert [(d.namespace, d.name) for d in event.inputs] == [("trading", "log")]
    assert event.outputs == []
    assert event.run.run_id, "a run event carries a run id, stable across its events"


def test_run_events_of_one_run_share_its_id() -> None:
    job = Passthrough(uri="rekep:///jobs/p")
    start = job.into_run_event(RunState.START)
    complete = job.into_run_event(RunState.COMPLETE, start.run)
    assert complete.run.run_id == start.run.run_id


def test_run_events_of_separate_runs_do_not() -> None:
    job = Passthrough(uri="rekep:///jobs/p")
    first, second = job.into_run_event(RunState.START), job.into_run_event(RunState.START)
    assert first.run.run_id != second.run.run_id


# -- run --------------------------------------------------------------------


def test_run_extracts_transforms_and_counts() -> None:
    job = Passthrough(uri="rekep:///jobs/p", source=SAMPLE.as_uri())
    assert job.run() == 24


def test_transform_output_is_what_load_sees() -> None:
    assert Doubler(uri="rekep:///jobs/d", source=SAMPLE.as_uri()).run() == 48


def test_run_without_a_source_says_what_to_override() -> None:
    with pytest.raises(NotImplementedError, match="override extract"):
        Passthrough(uri="rekep:///jobs/p").run()


# -- side files -------------------------------------------------------------


def test_load_builds_the_declared_class(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "job.json"
    path.write_text(json.dumps({"job": "rekep.job.Passthrough", "uri": "rekep:///jobs/j"}))
    job = load(path)
    assert isinstance(job, Passthrough)
    assert job.task_id() == "j"


def test_load_renders_jinja_with_the_environment(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BUCKET", "s3://lake")
    path = tmp_path / "job.yaml"
    path.write_text(
        'job: rekep.job.Passthrough\nuri: rekep:///jobs/y\nsource: "{{ env.BUCKET }}/app.txt"\n'
    )
    assert load(path).source == "s3://lake/app.txt"


def test_load_passes_extra_context(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "job.yaml"
    path.write_text('job: rekep.job.Passthrough\nuri: "rekep:///jobs/{{ suffix }}"\n')
    assert load(path, suffix="rendered").task_id() == "rendered"


def test_load_requires_a_job_key(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "job.yaml"
    path.write_text("uri: rekep:///jobs/anonymous\n")
    with pytest.raises(ValueError, match="declares no"):
        load(path)


def test_load_refuses_a_non_job_class(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "job.yaml"
    path.write_text("job: rekep.models.Log\nuri: rekep:///jobs/x\n")
    with pytest.raises(TypeError, match="not a Job subclass"):
        load(path)


def test_load_allows_the_bare_job_class(tmp_path: pathlib.Path) -> None:
    """Concrete, not abstract: a purely descriptive job is a valid side file."""
    path = tmp_path / "job.yaml"
    path.write_text("job: rekep.job.Job\nuri: rekep:///jobs/x\n")
    job = load(path)
    assert type(job) is Job
    with pytest.raises(NotImplementedError, match="arrow_transform"):
        job.run()


def test_load_all_reads_a_directory(tmp_path: pathlib.Path) -> None:
    (tmp_path / "b.yaml").write_text("job: rekep.job.Passthrough\nuri: rekep:///jobs/b\n")
    (tmp_path / "a.json").write_text(
        json.dumps({"job": "rekep.job.Passthrough", "uri": "rekep:///jobs/a"})
    )
    (tmp_path / "notes.txt").write_text("not a job")
    jobs = load_all(tmp_path)
    assert [job.task_id() for job in jobs] == ["a", "b"], "sorted, and .txt ignored"


def test_find_resolves_a_uri_through_the_registry(tmp_path: pathlib.Path) -> None:
    """Any spelling of the identity finds the one loaded object, not a copy."""
    (tmp_path / "a.yaml").write_text("job: rekep.job.Passthrough\nuri: rekep:///jobs/trading/a\n")
    (declared,) = load_all(tmp_path)
    assert find("rekep:///jobs/trading/a", tmp_path) is declared
    assert find("rekep:///jobs/trading/a", tmp_path) is declared


def test_find_says_where_it_looked(tmp_path: pathlib.Path) -> None:
    with pytest.raises(KeyError, match="no job"):
        find("rekep:///jobs/nowhere", tmp_path)


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
    jobs = {job.task_id(): job for job in load_all(REPO_JOBS)}
    f2l = jobs["files_to_logs"]
    assert f2l.task_namespace() == "pipeline", "stable across branches, unlike logs_to_records"
    assert f2l.repo_url == "https://github.com/Platob/yggfin"
    assert f2l.script_path == "python/src/rekep/jobs/files_to_logs.py"
    assert f2l.env["LOG_LEVEL"] == "INFO"
    assert f2l.properties["team"] == "trading-platform"
    assert f2l.tags == {"domain": "pipeline", "stage": "ingestion"}, "a mapping, not a list"
    assert f2l.airflow["task"]["retries"] == 2


def test_the_shipped_logs_to_records_picks_up_the_branch_suffix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rekep.render import git_context

    monkeypatch.setenv("REKEP_SOURCE_URL", "file:///dev/null")
    git_context.cache_clear()
    try:
        suffix = git_context()["git_branch_suffix"]
        jobs = {job.task_id(): job for job in load_all(REPO_JOBS)}
        l2r = jobs[f"logs_to_records{suffix}"]
        assert l2r.task_namespace() == f"pipeline{suffix}"
        assert l2r.consumed_records() == [Log]
    finally:
        git_context.cache_clear()


# -- python config / airflow / env -----------------------------------------


def test_source_code_location_facet_carries_repo_and_path() -> None:
    job = Job(
        uri="rekep:///jobs/j", repo_url="https://github.com/Platob/yggfin", script_path="a/b.py"
    )
    facet = job.source_code_location_facet()
    assert facet["type"] == "git"
    assert facet["repoUrl"] == "https://github.com/Platob/yggfin"
    assert facet["path"] == "a/b.py"
    assert "version" in facet  # the current git sha, whatever it is here


def test_facets_include_source_code_location_only_when_declared() -> None:
    assert Job(uri="rekep:///jobs/j").facets() == {}
    declared = Job(uri="rekep:///jobs/j", repo_url="https://github.com/Platob/yggfin")
    assert "sourceCodeLocation" in declared.facets()


def test_env_airflow_tags_and_properties_round_trip() -> None:
    job = Job(
        uri="rekep:///jobs/j",
        env={"BUCKET": "s3://lake"},
        properties={"team": "trading"},
        tags={"stage": "ingestion"},
        airflow={"dag": {"max_active_runs": 1}, "task": {"retries": 2}},
    )
    assert Job.from_json(job.into_json()) == job
