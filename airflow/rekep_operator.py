"""The Airflow operator a bundled task runs under on the worker, and what every one shares.

`RekepTask` is what an operator running a bundled task resolves and publishes,
wherever the task runs: `RekepOperator` runs it in a child on the worker, and
`eks_rekep_operator.EksRekepOperator` in a pod on an EKS cluster.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.standard.hooks.subprocess import SubprocessHook
from airflow.sdk import BaseOperator
from airflow.sdk.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.sdk import Asset, Context

#: The locked dependency group a task runs under.
GROUP = "runner"

#: Where a checkout keeps each bundled task's defaults, as `<name>.json`.
DEFAULTS = Path("python", "src", "rekep", "tasks")

#: A task name is a module name, so it cannot spell a path out of `DEFAULTS`.
NAME = re.compile(r"[a-z_][a-z0-9_]*")

#: Interval bounds, and the parameter each fills. A task that declares neither
#: name is scheduled the same and is handed nothing extra. An interval with no
#: width -- what Airflow infers for a manual run of an unscheduled DAG -- is
#: no interval, and the task falls back to its own default window. A bound the
#: run's own conf names wins over the interval: a person triggering a run for
#: one day means that day, whatever the schedule would have covered.
INTERVAL = (("start", "data_interval_start"), ("end", "data_interval_end"))

#: Everything but these becomes `_` in the name of an attempt directory, so a
#: run id carrying `:` or `+` cannot leave the directory it is created in.
UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


class RekepTask:
    """The parameters a run of one bundled task takes, and the result it publishes.

    Mixed into an operator ahead of the Airflow class it runs through. The
    operator sets `task_name`, `repository`, `parameters` and
    `upstream_task_id`; `repository` is the checkout the DAG ships from, whose
    `python/src/rekep/tasks/<name>.json` are the defaults the DAG's Params are
    read from too.
    """

    #: What every such operator templates, besides its own Airflow class's.
    TEMPLATE_FIELDS: ClassVar[tuple[str, ...]] = (
        "task_name",
        "repository",
        "parameters",
        "upstream_task_id",
    )

    task_name: str
    repository: str
    parameters: dict[str, Any]
    upstream_task_id: str | None
    outlets: Any

    def _resolved(self, context: Context) -> dict[str, Any]:
        """Every parameter the run takes: defaults, overrides, interval, then the pin."""
        parameters = self._merged(self._defaults(self._checkout()), context)
        if self.upstream_task_id is not None:
            parameters = self._pinned(parameters, context)
        return parameters

    def _published(self, context: Context, result: Any) -> dict[str, Any]:
        """The run's result, validated before it reaches XCom and the outlets."""
        from rekep.logs import Stage

        validated = Stage.validated(result)
        self._recorded(context, validated)
        return validated

    # -- what every run is handed -------------------------------------------

    def _checkout(self) -> Path:
        """The absolute checkout the defaults are read from."""
        if not self.repository:
            raise AirflowException("a task's defaults are a checkout's; name its repository")
        return Path(self.repository).resolve()

    def _defaults(self, repository: Path) -> dict[str, Any]:
        """The defaults the checkout ships for `task_name`, refused before a process starts.

        Read from the checkout the child runs, as the DAG reads its Params,
        rather than from whichever `rekep` the worker imports: the operator,
        the DAG and the child then resolve one set of parameters.
        """
        document = repository / DEFAULTS / f"{self.task_name}.json"
        if not NAME.fullmatch(self.task_name) or not document.is_file():
            raise AirflowException(f"no task is named {self.task_name!r} in {repository}")
        return json.loads(document.read_text(encoding="utf-8"))

    def _merged(self, defaults: dict[str, Any], context: Context) -> dict[str, Any]:
        """The task's defaults, under the operator's, the DAG's Params and its interval.

        Later wins, and only a name the task already declares is set: a task
        that does not take `books` is not handed the scheduler's. The one
        exception is a bound the run's own conf names, which the interval
        leaves alone.
        """
        declared = set(defaults)
        undeclared = sorted(set(self.parameters) - declared)
        if undeclared:
            raise AirflowException(
                f"{self.task_name} takes no {', '.join(undeclared)}; it takes {', '.join(defaults)}"
            )
        parameters = {**defaults, **self.parameters}
        for name, value in (context.get("params") or {}).items():
            if name in declared:
                parameters[name] = value
        conf = getattr(context.get("dag_run"), "conf", None) or {}
        bounds = [context.get(key) for _, key in INTERVAL]
        dated = all(isinstance(moment, datetime.datetime) for moment in bounds)
        if dated and bounds[0] < bounds[1]:
            for (name, _), moment in zip(INTERVAL, bounds, strict=True):
                if name in declared and name not in conf:
                    parameters[name] = moment.isoformat()
        return parameters

    def _pinned(self, parameters: dict[str, Any], context: Context) -> dict[str, Any]:
        """Use the completed upstream stage's window, snapshot and named tables.

        This handoff runs after ordinary scheduler parameters: all fanout
        readers must observe the same commit, even if a later run advances it.
        """
        from rekep.logs import Stage

        required = {"start", "end", "snapshot_id"}
        missing = sorted(required - parameters.keys())
        if missing:
            raise AirflowException(f"{self.task_name} takes no {', '.join(missing)}")
        try:
            upstream = Stage.validated(
                context["task_instance"].xcom_pull(task_ids=self.upstream_task_id)
            )
            window = upstream["window"]
            lower, upper = window["start"], window["end"]
            if type(lower) is not int or type(upper) is not int or lower >= upper:
                raise ValueError("expected an ordered window of epoch nanoseconds")
            snapshot = upstream.get("snapshot_id")
            if type(snapshot) is not int or snapshot < 0:
                raise ValueError("expected a nonnegative snapshot_id")
            sources = {
                name: table for name, table in upstream["targets"].items() if name in parameters
            }
            if not sources:
                raise ValueError("no upstream target names a declared source parameter")
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise AirflowException(
                f"{self.upstream_task_id} published no usable snapshot: {error}"
            ) from error
        return {
            **parameters,
            **sources,
            "start": lower,
            "end": upper,
            "snapshot_id": snapshot,
        }

    def _recorded(self, context: Context, result: dict[str, Any]) -> None:
        """Attach this run's counts to every outlet the task says it wrote."""
        events = context.get("outlet_events")
        if events is None:
            return
        written = set(result["targets"].values())
        for asset in self.outlets:
            if getattr(asset, "name", None) in written:
                events[asset].extra.update(
                    {
                        "task": result["task"],
                        "read": result["read"],
                        "written": result["written"],
                        "skipped": result["skipped"],
                    }
                )


class RekepOperator(RekepTask, BaseOperator):
    """Run one bundled task as `rekep tasks <name> run` in an isolated child.

    Airflow supplies what only a scheduler knows -- declared Params and the data
    interval -- and gets back the one small result mapping the task returned.
    The child runs in the checkout's locked environment, from its root.
    """

    template_fields: ClassVar[tuple[str, ...]] = (*RekepTask.TEMPLATE_FIELDS, "environment")
    template_fields_renderers: ClassVar[dict[str, str]] = {
        "parameters": "json",
        "environment": "json",
    }

    def __init__(
        self,
        *,
        task_name: str,
        repository: str,
        parameters: dict[str, Any] | None = None,
        environment: dict[str, str] | None = None,
        cache_dir: str | None = None,
        upstream_task_id: str | None = None,
        outlets: list[Asset] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(outlets=outlets or [], **kwargs)
        self.task_name = task_name
        self.repository = repository
        self.parameters = dict(parameters or {})
        self.environment = dict(environment or {})
        self.cache_dir = cache_dir
        self.upstream_task_id = upstream_task_id
        #: The running child, for `on_kill`. Never a constructor argument and
        #: never a template field, so nothing live reaches the serialized DAG.
        self.hook: SubprocessHook | None = None

    # -- running ------------------------------------------------------------

    def execute(self, context: Context) -> dict[str, Any]:
        """Run the task and return the result it published."""
        repository = self._rooted()
        parameters = self._resolved(context)
        attempt = Path(tempfile.mkdtemp(prefix=f"{self._attempt(context)}-"))
        try:
            written = attempt / "parameters.json"
            published = attempt / "result.json"
            _secured(written, json.dumps(parameters, ensure_ascii=False))
            self.hook = SubprocessHook()
            outcome = self.hook.run_command(
                self._argv(repository, self.task_name, written, published),
                env=self._environment(),
                cwd=str(repository),
            )
            if outcome.exit_code != 0:
                raise AirflowException(f"{self.task_name} exited with {outcome.exit_code}")
            if not published.is_file():
                raise AirflowException(f"{self.task_name} published no result")
            return self._published(context, json.loads(published.read_text(encoding="utf-8")))
        finally:
            # The parameter document may hold a credential, so it goes whether
            # the task landed or raised.
            shutil.rmtree(attempt, ignore_errors=True)
            self.hook = None

    def on_kill(self) -> None:
        """Stop the child process group running the task."""
        if self.hook is not None:
            self.hook.send_sigterm()

    # -- what the child is handed -------------------------------------------

    def _argv(self, repository: Path, name: str, parameters: Path, result: Path) -> list[str]:
        """The locked offline task command, as a list: nothing reaches a shell."""
        return [
            "uv",
            "run",
            "--project",
            str(repository / "python"),
            "--group",
            GROUP,
            "--no-sync",
            "--offline",
            "--no-progress",
            "--no-env-file",
            "--",
            "rekep",
            "tasks",
            name,
            "run",
            "--parameters-file",
            str(parameters),
            "--result-file",
            str(result),
        ]

    def _environment(self) -> dict[str, str]:
        """The worker's environment, plus what this operator configures."""
        environment = dict(os.environ)
        if self.cache_dir:
            environment["UV_CACHE_DIR"] = self.cache_dir
        environment.update({name: str(value) for name, value in self.environment.items()})
        return environment

    # -- where it runs ------------------------------------------------------

    def _rooted(self) -> Path:
        """The absolute checkout root holding `python/pyproject.toml`."""
        repository = self._checkout()
        if not (repository / "python" / "pyproject.toml").is_file():
            raise AirflowException(f"{repository} is not a rekep checkout")
        return repository

    def _attempt(self, context: Context) -> str:
        """A directory stem naming the attempt that owns it."""
        instance = context.get("task_instance")
        parts = (
            self.dag_id,
            self.task_id,
            str(context.get("run_id") or ""),
            str(getattr(instance, "map_index", -1)),
            str(getattr(instance, "try_number", 0)),
        )
        return UNSAFE.sub("_", "-".join(parts))[:120]


def _secured(path: Path, payload: str) -> None:
    """Write `payload` where only this worker's user can read it back."""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(payload)
