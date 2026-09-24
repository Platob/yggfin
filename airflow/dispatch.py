"""Where each DAG node runs its bundled task: on the worker, or in a pod on EKS.

Unset, `REKEP_EKS_CONFIG` leaves every task on the worker under
`RekepOperator`. Naming a JSON document, it runs every task under
`EksRekepOperator` with the `EksPodOperator` keywords the document gives it:

    {
      "cluster_name": "market-data",
      "image": "123456789012.dkr.ecr.eu-west-1.amazonaws.com/rekep:<commit>",
      "namespace": "rekep",
      "service_account_name": "rekep",
      "region": "eu-west-1",
      "container_resources": {"requests": {"cpu": "2", "memory": "8Gi"}},
      "tasks": {"build_dbt": {"container_resources": {"requests": {"memory": "16Gi"}}}}
    }

A keyword at the top applies to every task, and one under `tasks.<name>`
replaces it whole for that task alone.
"""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from airflow.sdk import Asset, BaseOperator

#: The checkout these DAGs ship from: its `python/src/rekep/tasks/<name>.json`
#: are every task's defaults, and on the worker its locked environment runs them.
ROOT = str(Path(__file__).resolve().parents[1])

#: Where the checkout keeps those defaults.
DEFAULTS = Path(ROOT, "python", "src", "rekep", "tasks")

#: The environment variable naming the EKS dispatch document.
EKS = "REKEP_EKS_CONFIG"


def defaults(name: str) -> dict[str, Any]:
    """The defaults one bundled task ships, read without importing `rekep`."""
    return json.loads((DEFAULTS / f"{name}.json").read_text(encoding="utf-8"))


def node(name: str, targets: Sequence[str], *, upstream_task_id: str | None = None) -> BaseOperator:
    """One bundled task as a DAG node, publishing `targets`, run where `EKS` says."""
    common: dict[str, Any] = {
        "task_id": name,
        "task_name": name,
        "repository": ROOT,
        "outlets": [Asset(name=target) for target in targets],
        "upstream_task_id": upstream_task_id,
    }
    configured = os.environ.get(EKS)
    if not configured:
        from rekep_operator import RekepOperator

        return RekepOperator(
            doc_md=f"`python/src/rekep/tasks/{name}.py`, run as `rekep tasks {name} run`.",
            **common,
        )
    from eks_rekep_operator import EksRekepOperator

    return EksRekepOperator(
        doc_md=f"`python/src/rekep/tasks/{name}.py`, run as `rekep tasks {name} run` on EKS.",
        **common,
        **eks_settings(configured, name),
    )


def eks_settings(document: str | os.PathLike[str], name: str) -> dict[str, Any]:
    """The `EksPodOperator` keywords the dispatch `document` gives task `name`.

    `container_resources` is spelled as the Kubernetes object's `requests` and
    `limits`; every other keyword is passed as the operator takes it, and
    `pod_template_dict` carries whatever else a pod needs.
    """
    settings = json.loads(Path(document).read_text(encoding="utf-8"))
    if not isinstance(settings, dict):
        raise TypeError(f"{document} is a JSON object of EksPodOperator keywords")
    overrides = settings.pop("tasks", {})
    unknown = sorted(task for task in overrides if not (DEFAULTS / f"{task}.json").is_file())
    if unknown:
        raise ValueError(f"{document} overrides no bundled task named {', '.join(unknown)}")
    settings.update(overrides.get(name, {}))
    resources = settings.get("container_resources")
    if isinstance(resources, dict):
        from kubernetes.client import models as k8s

        settings["container_resources"] = k8s.V1ResourceRequirements(**resources)
    return settings
