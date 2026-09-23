"""Airflow DAG from captured messages to one book snapshot and its events."""

from __future__ import annotations

import json
from pathlib import Path

from airflow.sdk import Asset, dag
from marimo_operator import MarimoOperator

ROOT = str(Path(__file__).resolve().parents[2])


def _defaults(name: str) -> dict[str, object]:
    """The parameters owned by one adjacent task document."""
    document = Path(ROOT, "tasks", name, f"{name}.json")
    return json.loads(document.read_text(encoding="utf-8"))["parameters"]


MESSAGE_DEFAULTS = _defaults("parse_messages")
RAW_DEFAULTS = _defaults("parse_fix_raw")
REFINED_DEFAULTS = _defaults("parse_fix_refined")
BOOK_DEFAULTS = _defaults("parse_books")
EVENT_DEFAULTS = _defaults("parse_orders")
# The book stage owns the snapshot handed to all three readers. It is not a
# DAG Param: a caller cannot redirect just one child to a different commit.
PARAMS = {
    **MESSAGE_DEFAULTS,
    **RAW_DEFAULTS,
    **REFINED_DEFAULTS,
    **BOOK_DEFAULTS,
    **{name: value for name, value in EVENT_DEFAULTS.items() if name != "snapshot_id"},
}


def _task(name: str, target: str, *, upstream_task_id: str | None = None) -> MarimoOperator:
    """One repository Marimo application and the table it publishes."""
    return MarimoOperator(
        task_id=name,
        repository=ROOT,
        document=f"tasks/{name}/{name}.json",
        doc_md=f"`tasks/{name}/{name}.py`, configured by its adjacent JSON document.",
        outlets=[Asset(name=target)],
        upstream_task_id=upstream_task_id,
    )


@dag(
    dag_id="rekep_ingestion",
    description="Parse captured FIX into a book snapshot, then its orders, quotes and executions.",
    # One run a day, covering its own data interval: the operator hands the
    # interval to every task as its `start` and `end`, and a manual trigger
    # of the DAG covers the last complete day the same way. Triggered on an
    # unscheduled DAG, the interval has no width and each task covers the day
    # before the instant it ran instead.
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params=PARAMS,
    tags=["rekep", "arrow", "iceberg", "fix", "marimo"],
)
def _ingestion() -> None:
    messages = _task("parse_messages", "logs.messages")
    raw = _task("parse_fix_raw", "fix.raw")
    refined = _task("parse_fix_refined", "fix.refined")
    books = _task("parse_books", "market.books")
    orders = _task("parse_orders", "market.orders", upstream_task_id="parse_books")
    quotes = _task("parse_quotes", "market.quotes", upstream_task_id="parse_books")
    executions = _task("parse_executions", "market.executions", upstream_task_id="parse_books")
    messages >> raw >> refined >> books >> [orders, quotes, executions]


ingestion = _ingestion()
