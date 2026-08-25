"""Airflow DAG executing the repository's notebook tasks."""

from __future__ import annotations

import datetime
import os
from pathlib import Path
from typing import Any

from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.sdk import dag

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


@dag(
    dag_id="rekep_market_pipeline",
    description="Parse logs, build books, and publish flat market tables.",
    schedule="@hourly",
    start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    catchup=True,
    max_active_runs=1,
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

    messages >> parsed >> [instruments, market]
    market >> [orders, executions]


market_pipeline()
