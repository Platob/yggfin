"""Airflow DAG for the three streamed ingestion stages."""

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
BRONZE_DEFAULTS = _defaults("parse_fix_bronze")
SILVER_DEFAULTS = _defaults("parse_fix_silver")
# One Params mapping over three documents, so a name two of them share --
# `start`, `end`, `catalog`, `registry` -- means one thing on every node. The
# table each stage reads is named for what it reads, `messages` and `bronze`,
# so a run's conf cannot hand one stage the other's source.
PARAMS = {**MESSAGE_DEFAULTS, **BRONZE_DEFAULTS, **SILVER_DEFAULTS}


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
    description="Parse one day of captured text into raw messages, then parsed and walked FIX.",
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
    bronze = _task("parse_fix_bronze", "fix.bronze")
    silver = _task("parse_fix_silver", "fix.silver")
    messages >> bronze >> silver


ingestion = _ingestion()
