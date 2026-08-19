"""DAGs built from flow side files.

An Airflow deployment's DAG folder needs only::

    from rekep.airflow.flows import dags

    globals().update(dags())

Each side file under `stacks/flows` (see `rekep.flows.load`) becomes one DAG
with one task running the flow, tagged and documented with the lineage its
`consumes`/`produces` declare.
"""

from __future__ import annotations

import os
from typing import Any

from rekep.airflow import lineage
from rekep.flows import FLOWS_ROOT, Flow, load_all


def dags(root: str | os.PathLike[str] = FLOWS_ROOT, **context: Any) -> dict[str, Any]:
    """One built DAG per side file under `root`, keyed by dag id."""
    return {flow.name: build(flow) for flow in load_all(root, **context)}


def build(flow: Flow) -> Any:
    """The Airflow DAG for one flow, by way of rekep's own dag.

    `Flow -> Dag -> Airflow DAG`: the interop lives on the classes, so a
    hand-built `Dag` and a side-file `Flow` take the same road here.
    """
    return flow.into_dag().into_airflow()


def documentation(flow: Flow) -> str:
    """The lineage documentation a flow's DAG will carry; needs no Airflow."""
    return lineage.documentation_of(flow.consumed_records(), flow.produced_records())
