"""The Airflow authoring API, whichever version is installed.

Airflow 3 moved DAG authoring into `airflow.sdk`; Airflow 2 keeps it spread
across `airflow.models`, `airflow.decorators` and `airflow.datasets`, and calls
an asset a dataset. Both are resolved here once, so nothing else in this package
has to know which one it is talking to.
"""

from __future__ import annotations

from typing import Any

from rekep.require import require

try:  # Airflow 3
    from airflow.sdk import DAG, Asset, dag, task
except ImportError:  # pragma: no cover - exercised only on Airflow 2
    try:
        from airflow.datasets import Dataset as Asset
        from airflow.decorators import dag, task
        from airflow.models.dag import DAG
    except ImportError:
        require("airflow.sdk", "airflow")
        raise

__all__ = ["DAG", "Asset", "asset", "dag", "task"]


def asset(name: str, uri: str, extra: dict[str, Any]) -> Any:
    """Build an Asset, or the Dataset that Airflow 2 calls one.

    Airflow 3 separates an asset's name from its URI; Airflow 2 has only the
    URI. Asking for both first and falling back keeps the same call working on
    either.
    """
    try:
        return Asset(name=name, uri=uri, extra=extra)
    except TypeError:  # pragma: no cover - exercised only on Airflow 2
        return Asset(uri=uri, extra=extra)
