"""Airflow DAG for the dbt products derived from the stored FIX rows."""

from __future__ import annotations

from airflow.sdk import Asset, dag
from dispatch import defaults, node

#: What the ingestion DAG publishes last, and what this one waits on. Naming
#: the Asset is the whole schedule: a run starts when `parse_fix_refined`
#: writes, because a product reads the walked rows and never the parsed ones.
UPSTREAM = Asset(name="fix.refined")

#: The tables the dbt project commits.
PUBLISHED = ("orders.events", "orders.current", "executions.fills")


@dag(
    dag_id="rekep_products",
    description="Build the order and execution products from the walked FIX rows.",
    schedule=[UPSTREAM],
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params=defaults("build_dbt"),
    tags=["rekep", "dbt", "duckdb", "iceberg"],
)
def _products() -> None:
    node("build_dbt", PUBLISHED)


products = _products()
