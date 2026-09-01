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
    catalog = dict(rooted.get("catalog", {}))
    properties = dict(catalog.get("properties", {}))
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
        catalog["properties"] = properties
        rooted["catalog"] = catalog
    return rooted


def notebook_task(
    task_id: str, document: str, *, category: str | None = None
) -> PapermillOperator:
    """Build one operator from an adjacent YAML/notebook pair."""
    path = ROOT / document
    configured = Task.from_yaml(path)
    parameters = rooted_parameters(configured.parameters)
    if category is not None:
        parameters["category"] = category
    if "branch" in parameters:
        parameters["branch"] = "{{ params.branch }}"
    if "books" in parameters:
        parameters["books"] = "{{ params.books }}"
    if "start" in parameters:
        parameters["start"] = "{{ data_interval_start.isoformat() }}"
    if "end" in parameters:
        parameters["end"] = "{{ data_interval_end.isoformat() }}"
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
    render_template_as_native_obj=True,
    params={
        "branch": Param("root", type="string", minLength=1),
        "books": Param(True, type="boolean"),
    },
    tags=["rekep", "arrow", "iceberg", "market", "notebook"],
)
def market_pipeline() -> None:
    messages = notebook_task(
        "parse_messages", "tasks/parse_messages/parse_messages.yml"
    )
    # Category is the only per-run input; the notebook derives its table and
    # observable task name so neither can drift from the selected rows.
    fix_document = "tasks/parse_fix/parse_fix.yml"
    parsed_market = notebook_task("parse_fix_market", fix_document, category="market")
    parsed_misc = notebook_task("parse_fix_misc", fix_document, category="misc")
    parsed_unknown = notebook_task(
        "parse_fix_unknown", fix_document, category="unknown"
    )
    instrument_updates = notebook_task(
        "parse_instruments", "tasks/parse_instruments/parse_instruments.yml"
    )
    market = notebook_task("parse_market", "tasks/parse_market/parse_market.yml")
    orders = notebook_task("flatten_orders", "tasks/flatten_orders/flatten_orders.yml")
    executions = notebook_task(
        "flatten_executions", "tasks/flatten_executions/flatten_executions.yml"
    )

    messages_route = after_notebook.override(task_id="route_messages")(
        output=messages.output,
        then={
            "parse_fix_market": "read",
            "parse_fix_misc": "read",
            "parse_fix_unknown": "read",
        },
    )
    messages_route >> [parsed_market, parsed_misc, parsed_unknown]

    # Both consumers read `fix.market`; the two terminal FIX tasks have no
    # downstream work and never hold this branch open.
    fix_route = after_notebook.override(task_id="route_fix_market")(
        output=parsed_market.output,
        then={"parse_instruments": "read", "parse_market": "read"},
    )
    fix_route >> [instrument_updates, market]

    market_route = after_notebook.override(task_id="route_market")(
        output=market.output,
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
    tags=["rekep", "iceberg", "maintenance", "notebook"],
)
def iceberg_maintenance() -> None:
    notebook_task("optimize_iceberg", "tasks/optimize_iceberg/optimize_iceberg.yml")


iceberg_maintenance()
