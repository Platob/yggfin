"""Airflow DAG from captured messages to one book snapshot and its events."""

from __future__ import annotations

from airflow.sdk import dag
from dispatch import defaults, node

MESSAGE_DEFAULTS = defaults("parse_messages")
RAW_DEFAULTS = defaults("parse_fix_raw")
REFINED_DEFAULTS = defaults("parse_fix_refined")
BOOK_DEFAULTS = defaults("parse_books")
EVENT_DEFAULTS = defaults("parse_orders")
# The book stage owns the snapshot handed to all three readers. It is not a
# DAG Param: a caller cannot redirect just one child to a different commit.
PARAMS = {
    **MESSAGE_DEFAULTS,
    **RAW_DEFAULTS,
    **REFINED_DEFAULTS,
    **BOOK_DEFAULTS,
    **{name: value for name, value in EVENT_DEFAULTS.items() if name != "snapshot_id"},
}


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
    tags=["rekep", "arrow", "iceberg", "fix"],
)
def _ingestion() -> None:
    messages = node("parse_messages", ["logs.messages"])
    raw = node("parse_fix_raw", ["fix.raw"])
    refined = node("parse_fix_refined", ["fix.refined"])
    books = node("parse_books", ["market.books"])
    orders = node("parse_orders", ["market.orders"], upstream_task_id="parse_books")
    quotes = node("parse_quotes", ["market.quotes"], upstream_task_id="parse_books")
    executions = node("parse_executions", ["market.executions"], upstream_task_id="parse_books")
    messages >> raw >> refined >> books >> [orders, quotes, executions]


ingestion = _ingestion()
