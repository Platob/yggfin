"""Airflow DAGs built from job side files.

An Airflow deployment's DAG folder needs only::

    from rekep.airflow.jobs import dags

    globals().update(dags())

Each side file under `stacks/jobs` (see `rekep.job.load`) becomes one DAG
with one task running the job, tagged and documented with the lineage its
`consumes`/`produces` declare.
"""

from __future__ import annotations

import os
from typing import Any

from rekep.airflow import lineage
from rekep.job import JOBS_ROOT, Job, load_all


def dags(root: str | os.PathLike[str] = JOBS_ROOT, **context: Any) -> dict[str, Any]:
    """One built DAG per side file under `root`, keyed by dag id."""
    return {job.name: build(job) for job in load_all(root, **context)}


def build(job: Job) -> Any:
    """The Airflow DAG for one job: one task, lineage tagged and documented."""
    return job.into_airflow()


def documentation(job: Job) -> str:
    """The lineage documentation a job's DAG will carry; needs no Airflow."""
    return lineage.documentation_of(job.consumed_records(), job.produced_records())
