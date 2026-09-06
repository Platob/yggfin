"""Airflow DAG for the two streamed ingestion stages."""

from __future__ import annotations

from pathlib import Path

from airflow.sdk import Asset, dag
from marimo_operator import MarimoOperator

ROOT = str(Path(__file__).resolve().parents[2])


def _task(name: str, target: str) -> MarimoOperator:
    """One repository Marimo application and the table it publishes."""
    return MarimoOperator(
        task_id=name,
        repository=ROOT,
        document=f"tasks/{name}/{name}.json",
        doc_md=f"`tasks/{name}/{name}.py`, configured by its adjacent JSON document.",
        outlets=[Asset(name=target)],
    )


@dag(
    dag_id="rekep_ingestion",
    description="Parse captured text into raw messages, then native FIX rows.",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["rekep", "arrow", "iceberg", "fix", "marimo"],
)
def _ingestion() -> None:
    messages = _task("parse_messages", "logs.messages")
    fixed = _task("parse_fix", "fix.messages")
    messages >> fixed


ingestion = _ingestion()
