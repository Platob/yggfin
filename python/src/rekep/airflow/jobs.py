"""Airflow DAGs built from job side files.

An Airflow deployment's DAG folder needs only::

    from rekep.airflow.jobs import dags

    globals().update(dags())

Each side file under `stacks/jobs` (see `rekep.job.load`) becomes one DAG
with one task running the job, tagged and documented with the lineage its
`consumes`/`produces` declare. The building is `Job.into_airflow()`'s --
this module only says which jobs and under what keys.
"""

from __future__ import annotations

import os
from typing import Any

from rekep.job import load_all


def dags(root: str | os.PathLike[str] | None = None, **context: Any) -> dict[str, Any]:
    """One built DAG per side file under `root`, keyed by dag id."""
    return {job.name: job.into_airflow() for job in load_all(root, **context)}
