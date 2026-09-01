"""The one Airflow operator this repository's task applications run under."""

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

#: The dependency group holding what a task application imports. `uv` installs
#: it from the lock; nothing resolves while a task runs.
GROUP = "runner"

#: Interval bounds, and the parameter each fills. A task that declares neither
#: name is scheduled the same and is handed nothing extra.
INTERVAL = (("start", "data_interval_start"), ("end", "data_interval_end"))

#: Everything but these becomes `_` in the name of an attempt directory, so a
#: run id carrying `:` or `+` cannot leave the directory it is created in.
UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


class MarimoOperator(BaseOperator):
    """Run one task document's Marimo application in the locked uv environment.

    The child is `rekep task run`, which is what a person runs locally, so a
    laptop and a worker differ in nothing but the machine. Airflow supplies
    what only a scheduler knows -- the declared Params and the data interval --
    and gets back the one small result mapping the task returned.
    """

    template_fields: ClassVar[tuple[str, ...]] = (
        "document",
        "repository",
        "parameters",
        "environment",
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        "parameters": "json",
        "environment": "json",
    }

    def __init__(
        self,
        *,
        document: str,
        repository: str,
        parameters: dict[str, Any] | None = None,
        environment: dict[str, str] | None = None,
        cache_dir: str | None = None,
        outlets: list[Asset] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(outlets=outlets or [], **kwargs)
        self.document = document
        self.repository = repository
        self.parameters = dict(parameters or {})
        self.environment = dict(environment or {})
        self.cache_dir = cache_dir
        #: The running child, for `on_kill`. Never a constructor argument and
        #: never a template field, so nothing live reaches the serialized DAG.
        self.hook: SubprocessHook | None = None

    # -- running ------------------------------------------------------------

    def execute(self, context: Context) -> dict[str, Any]:
        """Run the application and return the result it published."""
        from rekep.logs import Stage
        from rekep.tasks import Task

        repository = self._rooted()
        document = self._document(repository)
        task = Task.from_yaml(str(document))
        # Refuses an application outside its own task directory before a
        # process is started with it.
        task.into_application_path(document)
        parameters = self._merged(task.parameters, context)
        attempt = Path(tempfile.mkdtemp(prefix=f"{self._attempt(context)}-"))
        try:
            written = attempt / "parameters.json"
            published = attempt / "result.json"
            _secured(written, json.dumps(parameters, ensure_ascii=False))
            self.hook = SubprocessHook()
            outcome = self.hook.run_command(
                self._argv(repository, document, written, published),
                env=self._environment(),
                cwd=str(repository),
            )
            if outcome.exit_code != 0:
                raise AirflowException(f"{task.name} exited with {outcome.exit_code}")
            if not published.is_file():
                raise AirflowException(f"{task.name} published no result")
            result = Stage.validated(json.loads(published.read_text(encoding="utf-8")))
            self._recorded(context, result)
            return result
        finally:
            # The parameter document may hold a credential, so it goes whether
            # the task landed or raised.
            shutil.rmtree(attempt, ignore_errors=True)
            self.hook = None

    def on_kill(self) -> None:
        """Stop the child process group: uv, the interpreter, and the task."""
        if self.hook is not None:
            self.hook.send_sigterm()

    # -- what the child is handed -------------------------------------------

    def _merged(self, defaults: dict[str, Any], context: Context) -> dict[str, Any]:
        """The task's defaults, under the DAG's Params, under its interval.

        Later wins, and only a name the document already declares is set: a
        task that does not take `books` is not handed the scheduler's.
        """
        parameters = dict(defaults)
        undeclared = sorted(set(self.parameters) - set(defaults))
        if undeclared:
            raise AirflowException(f"{self.document} declares no {', '.join(undeclared)}")
        parameters.update(self.parameters)
        for name, value in (context.get("params") or {}).items():
            if name in defaults:
                parameters[name] = value
        for name, key in INTERVAL:
            moment = context.get(key)
            if name in defaults and isinstance(moment, datetime.datetime):
                parameters[name] = moment.isoformat()
        return parameters

    def _argv(self, repository: Path, document: Path, parameters: Path, result: Path) -> list[str]:
        """The command, as a list: nothing here reaches a shell.

        `--no-sync --offline` because the environment is the deployment's, made
        once from the lock with `uv sync --locked --group runner`. A task never
        resolves a dependency, reaches an index, or writes to the environment
        it runs in.
        """
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
            "task",
            "run",
            str(document),
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
        """The absolute repository root holding `python/` and `tasks/`."""
        if not self.repository:
            raise AirflowException("a task runs out of a checkout; name its repository")
        repository = Path(self.repository).resolve()
        if not (repository / "python" / "pyproject.toml").is_file():
            raise AirflowException(f"{repository} is not a rekep checkout")
        return repository

    def _document(self, repository: Path) -> Path:
        """The task document, refused when it is outside the repository."""
        named = Path(self.document)
        document = (named if named.is_absolute() else repository / named).resolve()
        if not document.is_relative_to(repository):
            raise AirflowException(f"{self.document} is outside {repository}")
        if not document.is_file():
            raise AirflowException(f"{document} is not a task document")
        return document

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

    # -- what it publishes --------------------------------------------------

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


def _secured(path: Path, payload: str) -> None:
    """Write `payload` where only this worker's user can read it back."""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(payload)
