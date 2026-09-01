"""Airflow DAGs executing the repository's task applications."""

from __future__ import annotations

import datetime
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from airflow.sdk import Asset, CronDataIntervalTimetable, Param, dag, task
from marimo_operator import MarimoOperator

#: One closed hour of capture per run, named by the hour it covers and started
#: once that hour has ended. Spelled as a timetable rather than as `@hourly`
#: because a bare cron string follows `[scheduler] create_cron_data_intervals`,
#: which is off by default and gives every run a zero-width interval -- and a
#: zero-width `[start, end)` reads nothing, on every run, silently.
HOURLY = CronDataIntervalTimetable("0 * * * *", timezone="UTC")

#: The checkout holding `python/`, `tasks/` and `data/`. A versioned DAG bundle
#: or an image places this file inside the repository it schedules, so the path
#: math is the whole answer; `repository` is templated for a deployment that
#: keeps the DAG somewhere else.
ROOT = str(Path(__file__).resolve().parents[2])

#: The message categories `parse_fix` runs once each, in the spelling
#: `rekep.fix.rules` gives them. Named here rather than imported, so the DAG
#: parses without the package a worker runs the task under.
CATEGORIES = ("market", "misc", "unknown")

#: The table a task writes on every run, keyed by its Airflow task id and named
#: by the identifier the catalog knows it as. `parse_market` writes books or
#: events depending on its `books` parameter, so it declares none: an asset
#: event is a claim that one named table was written.
WRITES = {
    "parse_messages": "logs.messages",
    **{f"parse_fix_{category}": f"fix.{category}" for category in CATEGORIES},
    "parse_instruments": "market.instruments",
    "flatten_orders": "market.orders",
    "flatten_executions": "market.executions",
}


def marimo_task(name: str, task_id: str | None = None, **kwargs: Any) -> MarimoOperator:
    """One task application, named once.

    `task_id` names the Airflow task when one document runs more than once --
    `parse_fix` is three, one per message category. Everything else is a
    `BaseOperator` argument -- retries, pools, queue, execution timeout,
    callbacks -- and reaches Airflow unchanged.
    """
    task_id = task_id or name
    written = WRITES.get(task_id)
    return MarimoOperator(
        task_id=task_id,
        repository=ROOT,
        document=f"tasks/{name}/{name}.yml",
        doc_md=f"`tasks/{name}/{name}.py`, configured by `tasks/{name}/{name}.yml`.",
        outlets=[Asset(name=written)] if written else [],
        **kwargs,
    )


def _count_at(result: object, path: str) -> int:
    """One non-negative count in a task's returned mapping."""
    value = result
    for name in path.split("."):
        if not isinstance(value, Mapping) or name not in value:
            raise ValueError(f"task result has no {path!r}")
        value = value[name]
    if type(value) is not int or value < 0:
        raise ValueError(f"task result {path!r} is not a non-negative count")
    return value


@task.branch
def route(result: Any, then: dict[str, str]) -> list[str] | None:
    """Choose direct downstream tasks from named counts in a task result."""
    selected = [task_id for task_id, path in then.items() if _count_at(result, path)]
    return selected or None


@dag(
    dag_id="rekep_market_pipeline",
    description="Parse logs and publish market tables.",
    schedule=HOURLY,
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params={
        "branch": Param("root", type="string", minLength=1),
        "books": Param(True, type="boolean"),
    },
    tags=["rekep", "arrow", "iceberg", "market", "marimo"],
)
def market_pipeline() -> None:
    messages = marimo_task("parse_messages")
    # Category is the only per-run input; the application derives its table and
    # the task name it reports from it, so neither can drift from the rows it
    # selected.
    parsed = {
        category: marimo_task(
            "parse_fix", f"parse_fix_{category}", parameters={"category": category}
        )
        for category in CATEGORIES
    }
    instrument_updates = marimo_task("parse_instruments")
    market = marimo_task("parse_market")
    orders = marimo_task("flatten_orders")
    executions = marimo_task("flatten_executions")

    messages_route = route.override(task_id="route_messages")(
        result=messages.output,
        then={f"parse_fix_{category}": "read" for category in CATEGORIES},
    )
    messages_route >> list(parsed.values())

    # Both consumers read `fix.market`; the two terminal FIX tasks have no
    # downstream work and never hold this branch open.
    fix_route = route.override(task_id="route_fix_market")(
        result=parsed["market"].output,
        then={"parse_instruments": "read", "parse_market": "read"},
    )
    fix_route >> [instrument_updates, market]

    market_route = route.override(task_id="route_market")(
        result=market.output,
        then={
            "flatten_orders": "flatten.orders",
            "flatten_executions": "flatten.executions",
        },
    )
    market_route >> [orders, executions]


market_pipeline()


@dag(
    dag_id="rekep_iceberg_maintenance",
    description="Compact Iceberg tables and clean expired history and orphan files.",
    schedule="30 2 * * *",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=10),
    },
    params={"branch": Param("root", type="string", minLength=1)},
    tags=["rekep", "iceberg", "maintenance", "marimo"],
)
def iceberg_maintenance() -> None:
    marimo_task("optimize_iceberg")


iceberg_maintenance()
