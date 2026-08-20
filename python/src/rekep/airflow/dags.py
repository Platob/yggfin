"""Airflow DAGs built from dag side files.

An Airflow deployment's DAG folder needs only::

    from rekep.airflow.dags import dags

    globals().update(dags())

Each side file under `stacks/dags` (see `rekep.dag.load`) becomes one Airflow
DAG, with one task per job it references, wired in the order its
`dependencies` declare and documented with the lineage those jobs'
`consumes`/`produces` add up to. The building is `Dag.into_airflow()`'s --
this module only says which dags and under what keys.
"""

from __future__ import annotations

import os
from typing import Any

from rekep.dag import load_all


def dags(
    root: str | os.PathLike[str] | None = None,
    jobs_root: str | os.PathLike[str] | None = None,
    **context: Any,
) -> dict[str, Any]:
    """One built Airflow DAG per side file under `root`, keyed by dag id.

    `jobs_root` is where the tasks those dags reference are declared, for a
    deployment that keeps the two folders somewhere other than side by side.
    """
    return {dag.dag_id(): dag.into_airflow(jobs_root) for dag in load_all(root, **context)}
