"""The Airflow operator a bundled task runs under in a pod on an Amazon EKS cluster."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.amazon.aws.operators.eks import EksPodOperator
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from rekep_operator import RekepTask

if TYPE_CHECKING:
    from airflow.sdk import Asset, Context

#: Where the pod's container publishes its result: the file the XCom sidecar
#: hands back once the container has exited.
RESULT = f"{PodDefaults.XCOM_MOUNT_PATH}/return.json"


class EksRekepOperator(RekepTask, EksPodOperator):
    """Run one bundled task as `rekep tasks <name> run` in a pod on an EKS cluster.

    Every other keyword is `EksPodOperator`'s -- `cluster_name`, `namespace`,
    `region`, `aws_conn_id`, `service_account_name`, `env_vars`,
    `container_resources`, ... -- and `image` is one with `rekep` on its PATH.
    The parameters resolve on the worker exactly as `RekepOperator` resolves
    them and reach the container as `--parameter` arguments, each value its
    JSON; the result comes back through the XCom sidecar.
    """

    template_fields: ClassVar[tuple[str, ...]] = tuple(
        dict.fromkeys((*RekepTask.TEMPLATE_FIELDS, *EksPodOperator.template_fields))
    )

    def __init__(
        self,
        *,
        task_name: str,
        repository: str,
        image: str,
        cluster_name: str,
        parameters: dict[str, Any] | None = None,
        upstream_task_id: str | None = None,
        outlets: list[Asset] | None = None,
        **kwargs: Any,
    ) -> None:
        # The command, its arguments and the sidecar are what this operator
        # is: a keyword naming any of them is refused as a second value.
        super().__init__(
            cluster_name=cluster_name,
            image=image,
            cmds=["rekep"],
            arguments=[],
            do_xcom_push=True,
            outlets=outlets or [],
            **kwargs,
        )
        self.task_name = task_name
        self.repository = repository
        self.parameters = dict(parameters or {})
        self.upstream_task_id = upstream_task_id

    def execute(self, context: Context) -> dict[str, Any] | None:
        """Run the task in its pod and return the result it published."""
        # Set after the template fields rendered, so a parameter's own value
        # is never read as a template.
        self.arguments = self._arguments(self._resolved(context))
        result = super().execute(context)
        # A deferred run publishes from `trigger_reentry` instead.
        return result if self.deferrable else self._published(context, result)

    def trigger_reentry(self, context: Context, event: dict[str, Any]) -> dict[str, Any]:
        """Publish the result of a pod the triggerer watched to completion."""
        return self._published(context, super().trigger_reentry(context, event))

    def _arguments(self, parameters: dict[str, Any]) -> list[str]:
        """`rekep tasks <name> run`, every parameter spelled as the JSON it is."""
        arguments = ["tasks", self.task_name, "run"]
        for name, value in parameters.items():
            arguments += ["--parameter", f"{name}={json.dumps(value, ensure_ascii=False)}"]
        return [*arguments, "--result-file", RESULT]
