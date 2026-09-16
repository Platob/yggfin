"""Airflow DAG for the dbt products derived from the stored FIX rows."""

from __future__ import annotations

import json
from pathlib import Path

from airflow.sdk import Asset, dag
from marimo_operator import MarimoOperator

ROOT = str(Path(__file__).resolve().parents[2])

#: What the ingestion DAG publishes, and what this one waits on. Naming the
#: Asset is the whole schedule: a run starts when `parse_fix` writes.
UPSTREAM = Asset(name="fix.messages")

#: The tables the dbt project commits, in the order its models build them.
PUBLISHED = ("orders.events", "orders.current", "executions.fills")

DEFAULTS = json.loads(
    Path(ROOT, "tasks", "build_dbt", "build_dbt.json").read_text(encoding="utf-8")
)["parameters"]


@dag(
    dag_id="rekep_products",
    description="Build the order and execution products from the stored FIX rows.",
    schedule=[UPSTREAM],
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params=DEFAULTS,
    tags=["rekep", "dbt", "duckdb", "iceberg", "marimo"],
)
def _products() -> None:
    MarimoOperator(
        task_id="build_dbt",
        repository=ROOT,
        document="tasks/build_dbt/build_dbt.json",
        doc_md="`tasks/build_dbt/build_dbt.py`, configured by its adjacent JSON document.",
        outlets=[Asset(name=table) for table in PUBLISHED],
    )


products = _products()
