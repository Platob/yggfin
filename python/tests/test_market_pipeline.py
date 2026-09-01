"""The two DAGs Airflow schedules, and the routes between their tasks.

The DAG folder is not part of the package, so it is loaded the way Airflow
loads it -- a `DagBag` over the bundle path, which is also what puts
`marimo_operator` on `sys.path`.
"""

from __future__ import annotations

import datetime
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("airflow", reason="the DAGs run under Airflow, which is POSIX-only")

ROOT = Path(__file__).resolve().parents[2]
DAGS = ROOT / "tasks" / "airflow"

#: The graph this repository supports, producer to consumer.
EDGES = {
    "parse_messages": {"route_messages"},
    "route_messages": {"parse_fix_market", "parse_fix_misc", "parse_fix_unknown"},
    "parse_fix_market": {"route_fix_market"},
    "parse_fix_misc": set(),
    "parse_fix_unknown": set(),
    "route_fix_market": {"parse_instruments", "parse_market"},
    "parse_instruments": set(),
    "parse_market": {"route_market"},
    "route_market": {"flatten_orders", "flatten_executions"},
    "flatten_orders": set(),
    "flatten_executions": set(),
}

#: The table each task claims on every run, as an Airflow asset name.
OUTLETS = {
    "parse_messages": ["logs.messages"],
    "parse_fix_market": ["fix.market"],
    "parse_fix_misc": ["fix.misc"],
    "parse_fix_unknown": ["fix.unknown"],
    "parse_instruments": ["market.instruments"],
    "parse_market": [],
    "flatten_orders": ["market.orders"],
    "flatten_executions": ["market.executions"],
    "route_messages": [],
    "route_fix_market": [],
    "route_market": [],
}


@pytest.fixture(scope="module")
def bag() -> Any:
    from airflow.dag_processing.dagbag import BundleDagBag

    loaded = BundleDagBag(DAGS, bundle_path=DAGS)
    assert loaded.import_errors == {}, loaded.import_errors
    return loaded


@pytest.fixture(scope="module")
def pipeline(bag: Any) -> Any:
    return bag.dags["rekep_market_pipeline"]


@pytest.fixture(scope="module")
def maintenance(bag: Any) -> Any:
    return bag.dags["rekep_iceberg_maintenance"]


def module() -> Any:
    """The DAG module, imported the way the bundle path makes it importable."""
    if str(DAGS) not in sys.path:
        sys.path.insert(0, str(DAGS))
    import market_pipeline

    return market_pipeline


# -- what the scheduler reads ------------------------------------------------


def test_the_repository_ships_exactly_two_dags(bag: Any) -> None:
    assert sorted(bag.dags) == ["rekep_iceberg_maintenance", "rekep_market_pipeline"]


def test_the_publishing_dag_keeps_its_schedule_and_its_catch_up_policy(pipeline: Any) -> None:
    # Spelled as a timetable rather than as `@hourly`: a bare cron string
    # follows `[scheduler] create_cron_data_intervals`, off by default, and a
    # zero-width interval reads nothing on every run.
    assert type(pipeline.timetable).__name__ == "CronDataIntervalTimetable"
    assert pipeline.timetable.expression == "0 * * * *"
    assert str(pipeline.timetable.timezone) == "UTC"
    assert pipeline.start_date == datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    assert pipeline.catchup is True
    assert pipeline.max_active_runs == 1
    assert pipeline.render_template_as_native_obj is True
    assert sorted(pipeline.tags) == ["arrow", "iceberg", "marimo", "market", "rekep"]


def test_the_maintenance_dag_runs_nightly_and_retries(maintenance: Any) -> None:
    assert maintenance.schedule == "30 2 * * *"
    assert maintenance.start_date == datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    assert maintenance.catchup is False
    assert maintenance.max_active_runs == 1
    assert sorted(maintenance.tags) == ["iceberg", "maintenance", "marimo", "rekep"]

    (task,) = maintenance.tasks
    assert task.task_id == "optimize_iceberg"
    assert task.retries == 2
    assert task.retry_delay == datetime.timedelta(minutes=10)


def test_the_runtime_parameters_are_the_two_a_person_sets(pipeline: Any, maintenance: Any) -> None:
    assert {name: pipeline.params[name] for name in pipeline.params} == {
        "branch": "root",
        "books": True,
    }
    assert pipeline.params.get_param("books").schema["type"] == "boolean"
    assert pipeline.params.get_param("branch").schema["minLength"] == 1
    assert {name: maintenance.params[name] for name in maintenance.params} == {"branch": "root"}


def test_the_graph_is_the_one_the_workflow_supports(pipeline: Any) -> None:
    assert {task.task_id: set(task.downstream_task_ids) for task in pipeline.tasks} == EDGES


def test_every_task_runs_its_own_document_through_the_one_operator(pipeline: Any) -> None:
    """Eight task instances out of six documents: `parse_fix` runs three times,
    with the category the only value that differs between them."""
    applications = {
        task.task_id: task for task in pipeline.tasks if type(task).__name__ == "MarimoOperator"
    }

    assert len(applications) == 8
    assert len({task.document for task in applications.values()}) == 6
    for task_id, task in applications.items():
        assert (ROOT / task.document).is_file()
        assert task.repository == str(ROOT)
        if task_id.startswith("parse_fix_"):
            assert task.document == "tasks/parse_fix/parse_fix.yml"
            assert task.parameters == {"category": task_id.removeprefix("parse_fix_")}
        else:
            assert task.document == f"tasks/{task_id}/{task_id}.yml"
            assert task.parameters == {}, "Params and the interval merge at execution time"


def test_the_three_fix_runs_are_the_categories_the_package_declares() -> None:
    """The DAG names them so it can parse without importing the package; this
    is what keeps the two spellings from drifting."""
    from rekep.fix.rules import MARKET_CATEGORY, MISC_CATEGORY, UNKNOWN_CATEGORY

    assert module().CATEGORIES == (MARKET_CATEGORY, MISC_CATEGORY, UNKNOWN_CATEGORY)


def test_a_task_claims_only_a_table_it_writes_on_every_run(pipeline: Any) -> None:
    """`parse_market` writes books or events by its `books` parameter, so it
    claims neither: an asset event says the table was written."""
    assert {
        task.task_id: [asset.name for asset in task.outlets] for task in pipeline.tasks
    } == OUTLETS


def test_both_dags_serialize(bag: Any) -> None:
    from airflow.serialization.serialized_objects import DagSerialization

    for dag_id, dag in sorted(bag.dags.items()):
        serialized = DagSerialization.to_dict(dag)
        assert DagSerialization.from_dict(serialized).dag_id == dag_id
        assert "hook" not in json.dumps(serialized), "nothing live reaches the metadata database"


def test_the_dags_import_without_the_stack_a_task_runs_under() -> None:
    """A scheduler parses these; only a worker needs marimo, Iceberg and Arrow."""
    checked = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-c",
            "import sys;"
            f"sys.path.insert(0, {str(DAGS)!r});"
            "import market_pipeline;"
            "print([name for name in ('marimo', 'pyiceberg', 'polars', 'pyarrow', 'rekep')"
            " if name in sys.modules])",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert checked.returncode == 0, checked.stderr
    assert checked.stdout.strip() == "[]", checked.stdout


# -- routing -----------------------------------------------------------------


def result(**held: Any) -> dict[str, Any]:
    """One task result, in the shape every task returns."""
    return {
        "task": "parse_messages",
        "read": 0,
        "written": 0,
        "skipped": 0,
        "sources": {},
        "targets": {},
        "window": {"start": None, "end": None},
        "elapsed_ms": 1,
        **held,
    }


def routed(produced: dict[str, Any], then: dict[str, str]) -> list[str] | None:
    return module().route.function(result=produced, then=then)


def test_a_positive_count_routes_its_consumer() -> None:
    assert routed(result(read=11), {"parse_fix_market": "read"}) == ["parse_fix_market"]


def test_a_zero_count_skips_every_consumer() -> None:
    """`None` is how a branch says "none of them", which Airflow skips."""
    assert routed(result(read=0), {"parse_fix_market": "read"}) is None


def test_a_replay_that_wrote_nothing_still_reaches_its_consumer() -> None:
    """`merge_by` skips stored keys, so `written` is zero on a replay and the
    consumer still has rows to read."""
    then = {f"parse_fix_{category}": "read" for category in module().CATEGORIES}
    assert routed(result(read=11, written=0, skipped=11), then) == list(then)


@pytest.mark.parametrize(
    "produced",
    [result(), result(flatten={}), result(flatten={"executions": 1})],
    ids=["absent", "empty", "another-product"],
)
def test_a_count_the_result_does_not_carry_is_refused(produced: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="has no 'flatten.orders'"):
        routed(produced, {"flatten_orders": "flatten.orders"})


@pytest.mark.parametrize(
    "value",
    [-1, True, False, "2", 2.0, None],
    ids=["negative", "true", "false", "text", "float", "null"],
)
def test_a_count_that_is_not_a_count_is_refused(value: Any) -> None:
    """A boolean is an `int` in Python and is not a count here."""
    with pytest.raises(ValueError, match="not a non-negative count"):
        routed(result(read=value), {"parse_fix_market": "read"})


def test_the_two_readers_of_fix_market_are_routed_together() -> None:
    then = {"parse_instruments": "read", "parse_market": "read"}

    assert routed(result(task="parse_fix_market", read=2), then) == [
        "parse_instruments",
        "parse_market",
    ]
    assert routed(result(task="parse_fix_market", read=0), then) is None


def test_book_mode_routes_each_flattener_on_its_own_count() -> None:
    then = {"flatten_orders": "flatten.orders", "flatten_executions": "flatten.executions"}

    assert routed(result(flatten={"orders": 2, "executions": 1}), then) == [
        "flatten_orders",
        "flatten_executions",
    ]
    assert routed(result(flatten={"orders": 2, "executions": 0}), then) == ["flatten_orders"]
    assert routed(result(flatten={"orders": 0, "executions": 1}), then) == ["flatten_executions"]


def test_direct_market_mode_skips_both_flatteners() -> None:
    """`books: false` writes the Orders and Executions itself; nothing is left
    for the two flatteners to project out of a book."""
    produced = result(
        task="parse_market",
        mode="events",
        read=3,
        written=3,
        flatten={"orders": 0, "executions": 0},
    )

    assert (
        routed(
            produced,
            {"flatten_orders": "flatten.orders", "flatten_executions": "flatten.executions"},
        )
        is None
    )


def test_the_maintenance_result_stays_a_summary_small_enough_for_xcom() -> None:
    """Every table it visited, four counts each -- and no listing of the files."""
    from rekep.logs import Stage

    reports = {
        f"table{index}": {"rewritten": 0, "expired": 0, "deleted": 0, "bytes": 0}
        for index in range(64)
    }
    produced = result(
        task="optimize_iceberg",
        read=64,
        tables=64,
        expired=0,
        deleted=0,
        byte_size=0,
        reports=reports,
    )

    assert Stage.validated(produced) == produced
    assert len(json.dumps(produced)) < 8192
    for report in reports.values():
        assert set(report) == {"rewritten", "expired", "deleted", "bytes"}
