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
        #: What `trigger_reentry` published when it ran inside `execute`.
        self.reentered: dict[str, Any] | None = None

    def render_template_fields(self, context: Context, jinja_env: Any = None) -> None:
        """Render the task's fields as the DAG does, and the pod's as text.

        Kubernetes takes strings, and a DAG rendering native objects would
        read an environment variable of `8`, or one holding a JSON document,
        as a number or a mapping.
        """
        jinja_env = jinja_env or self.get_template_env()
        self._do_render_template_fields(self, RekepTask.TEMPLATE_FIELDS, context, jinja_env, set())
        pod = [name for name in self.template_fields if name not in RekepTask.TEMPLATE_FIELDS]
        # The operator's own switch outranks the DAG's, for this pass alone.
        native, self.render_template_as_native_obj = self.render_template_as_native_obj, False
        try:
            self._do_render_template_fields(self, pod, context, jinja_env, set())
        finally:
            self.render_template_as_native_obj = native

    def execute(self, context: Context) -> dict[str, Any] | None:
        """Run the task in its pod and return the result it published."""
        # Set after the template fields rendered, so a parameter's own value
        # is never read as a template.
        self.arguments = self._arguments(self._resolved(context))
        result = super().execute(context)
        if not self.deferrable:
            return self._published(context, result)
        # A deferring pod publishes from `trigger_reentry` once it is done;
        # one already done when it would defer was published there already.
        return self.reentered

    def trigger_reentry(self, context: Context, event: dict[str, Any]) -> dict[str, Any]:
        """Publish the result of a pod the triggerer watched to completion."""
        self.reentered = self._published(context, super().trigger_reentry(context, event))
        return self.reentered

    def _arguments(self, parameters: dict[str, Any]) -> list[str]:
        """`rekep tasks <name> run`, every parameter spelled as the JSON it is.

        Kubernetes expands `$(NAME)` in a container's arguments and reads `$$`
        as `$`, so every `$` is doubled and the container reads the value as
        it was resolved.
        """
        arguments = ["tasks", self.task_name, "run"]
        for name, value in parameters.items():
            spelled = json.dumps(value, ensure_ascii=False).replace("$", "$$")
            arguments += ["--parameter", f"{name}={spelled}"]
        return [*arguments, "--result-file", RESULT]
