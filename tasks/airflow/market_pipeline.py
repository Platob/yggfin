"""Airflow DAG executing the repository's notebook tasks."""

from __future__ import annotations

import datetime
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import scrapbook as sb
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.sdk import Param, dag, task
from rekep.tasks import Task

ROOT = Path(os.environ.get("REKEP_ROOT", Path(__file__).resolve().parents[2])).resolve()
OUTPUT_ROOT = os.environ.get("REKEP_NOTEBOOK_OUTPUT", "/tmp/rekep-notebooks")


def rooted_parameters(parameters: dict[str, Any]) -> dict[str, Any]:
    """Make repository-relative catalog locations worker-independent."""
    rooted = dict(parameters)
    if "project_root" in rooted:
        rooted["project_root"] = str(ROOT)
    properties = dict(rooted.get("catalog_properties", {}))
    uri = properties.get("uri", "")
    sqlite = "sqlite:///"
    if uri.startswith(sqlite):
        path = Path(uri.removeprefix(sqlite))
        if not path.is_absolute():
            properties["uri"] = f"{sqlite}{(ROOT / path).as_posix()}"
    warehouse = properties.get("warehouse", "")
    local = "file://"
    if warehouse.startswith(local):
        path = Path(warehouse.removeprefix(local))
        if not path.is_absolute():
            properties["warehouse"] = f"{local}{(ROOT / path).as_posix()}"
    if properties:
        rooted["catalog_properties"] = properties
    return rooted


def notebook_task(task_id: str, document: str) -> PapermillOperator:
    """Build one operator from an adjacent YAML/notebook pair."""
    path = ROOT / document
    configured = Task.from_yaml(path)
    parameters = {
        **rooted_parameters(configured.parameters),
        "branch": "{{ params.branch }}",
        "start": "{{ data_interval_start.isoformat() }}",
        "end": "{{ data_interval_end.isoformat() }}",
    }
    return PapermillOperator(
        task_id=task_id,
        input_nb=str(configured.into_notebook_path(path)),
        output_nb=f"{OUTPUT_ROOT}/{task_id}-{{{{ ts_nodash }}}}.ipynb",
        parameters=parameters,
        kernel_name="python3",
        log_output=True,
    )


def _count_at(result: object, path: str) -> int:
    """One non-negative count in a notebook's returned mapping."""
    value = result
    for name in path.split("."):
        if not isinstance(value, Mapping) or name not in value:
            raise ValueError(f"notebook result has no {path!r}")
        value = value[name]
    if type(value) is not int or value < 0:
        raise ValueError(f"notebook result {path!r} is not a non-negative count")
    return value


@task.branch
def after_notebook(output: Any, then: dict[str, str]) -> list[str] | None:
    """Choose direct downstream tasks from named counts in a notebook result."""
    url = getattr(output, "url", output)
    notebook = sb.read_notebook(url)
    if "result" not in notebook.scraps:
        raise ValueError(f"{url} has no 'result' scrap")
    result = notebook.scraps["result"].data
    selected = [task_id for task_id, path in then.items() if _count_at(result, path)]
    return selected or None


@dag(
    dag_id="rekep_market_pipeline",
    description="Parse logs and publish market tables.",
    schedule="@hourly",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    catchup=True,
    max_active_runs=1,
    params={"branch": Param("root", type="string", minLength=1)},
    tags=["rekep", "arrow", "iceberg", "market", "notebook"],
)
def market_pipeline() -> None:
    messages = notebook_task("parse_messages", "tasks/parse_messages/parse_messages.yml")
    parsed = notebook_task("parse_fix", "tasks/parse_fix/parse_fix.yml")
    instruments = notebook_task(
        "flatten_instruments", "tasks/flatten_instruments/flatten_instruments.yml"
    )
    market = notebook_task("parse_market", "tasks/parse_market/parse_market.yml")
    orders = notebook_task("flatten_orders", "tasks/flatten_orders/flatten_orders.yml")
    executions = notebook_task(
        "flatten_executions", "tasks/flatten_executions/flatten_executions.yml"
    )

    messages_route = after_notebook.override(task_id="route_messages")(
        output=messages.output, then={"parse_fix": "read"}
    )
    messages_route >> parsed

    fix_route = after_notebook.override(task_id="route_fix")(
        output=parsed.output,
        then={
            "flatten_instruments": "instrument_versions",
            "parse_market": "routed.market",
        },
    )
    fix_route >> [instruments, market]

    market_route = after_notebook.override(task_id="route_market")(
        output=market.output,
        then={
            "flatten_orders": "flatten.orders",
            "flatten_executions": "flatten.executions",
        },
    )
    market_route >> [orders, executions]


market_pipeline()
