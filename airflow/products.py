"""Airflow DAG for the dbt products derived from the stored FIX rows."""

from __future__ import annotations

import json
from pathlib import Path

from airflow.sdk import Asset, dag
from rekep_operator import RekepOperator

ROOT = str(Path(__file__).resolve().parents[1])

#: What the ingestion DAG publishes last, and what this one waits on. Naming
#: the Asset is the whole schedule: a run starts when `parse_fix_refined`
#: writes, because a product reads the walked rows and never the parsed ones.
UPSTREAM = Asset(name="fix.refined")

#: The tables the dbt project commits.
PUBLISHED = ("orders.events", "orders.current", "executions.fills")

#: The defaults `build_dbt` ships, read without importing `rekep`.
DEFAULTS = json.loads(
    Path(ROOT, "python", "src", "rekep", "tasks", "build_dbt.json").read_text(encoding="utf-8")
)


@dag(
    dag_id="rekep_products",
    description="Build the order and execution products from the walked FIX rows.",
    schedule=[UPSTREAM],
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params=DEFAULTS,
    tags=["rekep", "dbt", "duckdb", "iceberg"],
)
def _products() -> None:
    RekepOperator(
        task_id="build_dbt",
        task_name="build_dbt",
        repository=ROOT,
        doc_md="`python/src/rekep/tasks/build_dbt.py`, run as `rekep tasks build_dbt run`.",
        outlets=[Asset(name=table) for table in PUBLISHED],
    )


products = _products()
