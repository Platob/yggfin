"""Dag: the graph a set of tasks form, ours rather than an orchestrator's.

A `Job` (`rekep.job`) is one task -- what OpenLineage calls the `task_id`
half of a job's name. A `Dag` is the other half: which tasks belong together,
what has to finish before what, and when the whole thing runs. It is a
`Record` like everything else here, so it round-trips through a side file
under `stacks/dags` and needs no orchestrator installed to be declared,
loaded, validated or read.

**It is a full implementation, not a description of someone else's.** The
graph is resolved and validated here (`upstreams`), ordered here (`order`,
Kahn's algorithm, declaration order as the tie-break so the same dag always
runs in the same sequence), and executed here (`run`). Airflow is one
*projection* of it -- `into_airflow()`, the same relationship a record has
with Iceberg -- not the thing that defines it. A dag whose tasks are all
independent is still a dag; it just has no edges.

Tasks are referenced by URI (`rekep:///jobs/pipeline/files_to_logs`), never restated:
the task's own side file is its declaration, and a graph that copied a task's
config would be a second place for it to disagree with itself. `dependencies`
names them by task id -- the last level of the URI -- because that is what a
reader of the graph already sees.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import pathlib
from collections.abc import Iterable
from typing import Any

from rekep.job import Job
from rekep.job import find as find_job
from rekep.namespace import Namespace, ResourceUri
from rekep.records import Record, record
from rekep.records import registry as side_files

logger = logging.getLogger("rekep.dag")

#: Where dag side files live when nothing says otherwise: the checkout's
#: `stacks/dags` if it has one, else the user's `~/.config/rekep/dags` -- see
#: `rekep.config.folder`. `REKEP_DAGS_ROOT` overrides both.
DAGS_ROOT = os.environ.get("REKEP_DAGS_ROOT")

#: Config extensions a dags directory is scanned for.
EXTENSIONS = side_files.EXTENSIONS


@record
class Dag(Record):
    """A named graph of tasks: what runs, in what order, on what schedule."""

    uri: str
    """This dag's identity as a path: `rekep:///dags/namespace/name`.

    The same spelling a job and a dataset use, scoped to its own service --
    so `rekep:///dags/pipeline/trading_logs` and `rekep:///jobs/pipeline/trading_logs` are two
    resources, not one name used twice."""

    tasks: list[str] = dataclasses.field(default_factory=list)
    """The tasks in this dag, as job URIs (`rekep:///jobs/pipeline/files_to_logs`).

    References, not copies: the task's own side file stays its only
    declaration. Declaration order is the tie-break `order()` falls back on,
    so a dag with no dependencies still runs the way it reads."""

    dependencies: dict[str, list[str]] = dataclasses.field(default_factory=dict)
    """Task id -> the task ids that must finish first.

    Keyed by the downstream task because that is the question being answered
    ("what does this one need?"), and by *id* -- the last level of the URI --
    because that is the name a reader of the graph already has in front of
    them. A task named nowhere here has no upstreams and starts immediately."""

    schedule: str | None = None
    """Cron expression or scheduler alias, None for manual runs."""

    description: str | None = None
    """One line on what this dag does."""

    tags: dict[str, str] = dataclasses.field(default_factory=dict)
    """Extra tags for the orchestrator, on top of the derived lineage tags.

    A mapping like a task's: `domain: trading` says what the tag *is* about,
    which a bare `trading` never does."""

    airflow: dict[str, dict[str, Any]] = dataclasses.field(default_factory=dict)
    """Airflow-specific config under `dag`/`task`, merged into `into_airflow`'s
    calls. A task's own `airflow["task"]` wins over the dag's, which is the
    only ordering that lets a dag set a default at all."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """Generic extra properties: whatever a deployment needs to carry."""

    # -- identity ---------------------------------------------------------

    def resource_uri(self) -> ResourceUri:
        """This dag's identity: `rekep:///dags/namespace/name`."""
        return ResourceUri.parse(self.uri, service="dags")

    def dag_id(self) -> str:
        """This dag's own name, unqualified -- what Airflow calls a dag_id."""
        return self.resource_uri().name()

    def dag_namespace(self) -> str:
        """The namespace this dag is identified under."""
        return self.resource_uri().namespace()

    def dag_name(self) -> str:
        """Every level of the identity joined, the way `Job.task_name` joins its own."""
        return Namespace.of(*self.resource_uri().levels).path()

    def task_name(self, task: Job | str) -> str:
        """`<dag_id>.<task_id>`: OpenLineage's own spelling of a task's full name.

        A task's name is qualified by the dag that runs it, not only by its
        own namespace -- two dags may legitimately run tasks called `load`,
        and only this qualification tells the two runs apart.
        """
        return f"{self.dag_id()}.{task if isinstance(task, str) else task.task_id()}"

    # -- the graph --------------------------------------------------------

    def tasks_by_id(self, root: str | os.PathLike[str] | None = None) -> dict[str, Job]:
        """The declared tasks, resolved from their URIs, in declaration order.

        Resolution goes through `rekep.job.find`, so a task already loaded is
        the *same object* the rest of the process has, and only an unknown
        one costs a folder read. Two tasks sharing an id are refused: the id
        is what `dependencies` names them by, so a duplicate makes the graph
        ambiguous rather than merely untidy.
        """
        found: dict[str, Job] = {}
        for uri in self.tasks:
            task = find_job(uri, root)
            identifier = task.task_id()
            if identifier in found:
                raise ValueError(
                    f"dag {self.dag_id()!r}: two tasks called {identifier!r} "
                    f"({found[identifier].uri}, {task.uri}); dependencies name a task by its "
                    "id, so give one of them a different name"
                )
            found[identifier] = task
        return found

    def task(self, identifier: str, root: str | os.PathLike[str] | None = None) -> Job:
        """The task called `identifier`, or a refusal listing what this dag has."""
        tasks = self.tasks_by_id(root)
        if identifier not in tasks:
            known = ", ".join(tasks) or "none"
            raise KeyError(f"dag {self.dag_id()!r} has no task {identifier!r} (declared: {known})")
        return tasks[identifier]

    def upstreams(self, root: str | os.PathLike[str] | None = None) -> dict[str, list[str]]:
        """Every task's upstream ids, validated against the declared tasks.

        Every task appears, including the ones nothing precedes, so the result
        is the whole graph rather than only its edges. An edge naming a task
        this dag does not declare is refused here -- at load, where the file
        can be fixed -- instead of silently ordering nothing.
        """
        tasks = self.tasks_by_id(root)
        edges: dict[str, list[str]] = {identifier: [] for identifier in tasks}
        for downstream, upstream in self.dependencies.items():
            named = [downstream, *upstream]
            unknown = [name for name in named if name not in tasks]
            if unknown:
                known = ", ".join(tasks) or "none"
                raise ValueError(
                    f"dag {self.dag_id()!r}: dependency names {', '.join(unknown)}, which this "
                    f"dag does not declare (tasks: {known})"
                )
            edges[downstream] = list(upstream)
        return edges

    def downstreams(self, root: str | os.PathLike[str] | None = None) -> dict[str, list[str]]:
        """The same graph read the other way: task id -> what waits on it."""
        edges: dict[str, list[str]] = {}
        for downstream, upstream in self.upstreams(root).items():
            edges.setdefault(downstream, [])
            for name in upstream:
                edges.setdefault(name, []).append(downstream)
        return edges

    def order(self, root: str | os.PathLike[str] | None = None) -> list[str]:
        """The task ids in an order every dependency is satisfied by.

        Kahn's algorithm, with **declaration order as the tie-break**: among
        the tasks that are ready, the one written first goes first, so the
        same dag always produces the same sequence. An order that depended on
        set iteration would make a failure reproduce only sometimes, which is
        the worst kind of failure to debug.

        A cycle has no ready task and no way to pick one, so it is refused by
        naming exactly the tasks still waiting -- which is the cycle, plus
        whatever hangs off it.
        """
        edges = self.upstreams(root)
        pending = dict(edges)
        ordered: list[str] = []
        while pending:
            ready = [
                identifier
                for identifier in edges
                if identifier in pending and not set(pending[identifier]) - set(ordered)
            ]
            if not ready:
                waiting = ", ".join(sorted(pending))
                raise ValueError(
                    f"dag {self.dag_id()!r}: dependencies never resolve for {waiting}; "
                    "there is a cycle in `dependencies`"
                )
            for identifier in ready:
                ordered.append(identifier)
                del pending[identifier]
        return ordered

    def ordered_tasks(self, root: str | os.PathLike[str] | None = None) -> list[Job]:
        """The tasks themselves, in `order()`."""
        tasks = self.tasks_by_id(root)
        return [tasks[identifier] for identifier in self.order(root)]

    def roots(self, root: str | os.PathLike[str] | None = None) -> list[str]:
        """The tasks nothing has to finish before: where a run starts."""
        return [name for name, upstream in self.upstreams(root).items() if not upstream]

    def leaves(self, root: str | os.PathLike[str] | None = None) -> list[str]:
        """The tasks nothing waits on: where a run ends."""
        return [name for name, downstream in self.downstreams(root).items() if not downstream]

    # -- running ----------------------------------------------------------

    def run(self, root: str | os.PathLike[str] | None = None) -> dict[str, Any]:
        """Run every task in `order()`, and return what each returned.

        Sequential and in-process: this is the executor a laptop, a test and
        a cron line want, not a replacement for a scheduler. A failing task
        stops the run -- the tasks after it declared they needed it -- and the
        exception is left to propagate, since a dag that swallowed one would
        report success for a pipeline that did not happen.
        """
        results: dict[str, Any] = {}
        for task in self.ordered_tasks(root):
            logger.info("dag %s: running %s", self.dag_id(), self.task_name(task))
            results[task.task_id()] = task.run()
        return results

    # -- lineage ----------------------------------------------------------

    def consumed_records(self, root: str | os.PathLike[str] | None = None) -> list[type[Record]]:
        """Every record this dag's tasks read, deduplicated, in task order."""
        return _unique(cls for task in self.ordered_tasks(root) for cls in task.consumed_records())

    def produced_records(self, root: str | os.PathLike[str] | None = None) -> list[type[Record]]:
        """Every record this dag's tasks write, deduplicated, in task order."""
        return _unique(cls for task in self.ordered_tasks(root) for cls in task.produced_records())

    # -- interop ----------------------------------------------------------

    def into_airflow(self, root: str | os.PathLike[str] | None = None) -> Any:
        """This dag as a real Airflow DAG: one task per job, edges wired.

        The projection, not the definition. Airflow's own `DAG` and `@task`
        do the building -- rekep wraps neither -- and `airflow["dag"]` /
        `airflow["task"]` reach them untouched, the task's own winning over
        the dag's. What is *derived* is the part Airflow cannot derive
        (`rekep.airflow.lineage`): tags and a Consumes/Produces table for the
        dag, inlets and outlets for each task, from what the records declare.
        """
        from rekep.airflow import lineage, sdk

        tasks = self.tasks_by_id(root)
        defaults = self.airflow.get("task", {})
        with sdk.DAG(
            self.dag_id(),
            **lineage.dag_arguments(
                self.consumed_records(root),
                self.produced_records(root),
                description=self.description,
                schedule=self.schedule,
                tags=dict(self.tags),
                catchup=False,
                **self.airflow.get("dag", {}),
            ),
        ) as built:
            operators = {
                identifier: sdk.task(
                    **lineage.task_arguments(
                        task.consumed_records(),
                        task.produced_records(),
                        task_id=identifier,
                        **{**defaults, **task.airflow.get("task", {})},
                    )
                )(task.run)()
                for identifier, task in tasks.items()
            }
            for identifier, upstream in self.upstreams(root).items():
                for name in upstream:
                    operators[identifier].set_upstream(operators[name])
        return built

    # -- building ---------------------------------------------------------

    @classmethod
    def from_job(cls, task: Job, **overrides: Any) -> Dag:
        """The one-task dag a single job forms, named after the job.

        A task still has to belong to a dag to be scheduled, and a pipeline
        that is genuinely one step should not have to write a second file to
        say so. Schedule, description and tags come from the task, since with
        one task there is nobody else for them to belong to.
        """
        identity = task.resource_uri()
        return cls(
            **{
                "uri": str(ResourceUri.of("dags", *identity.levels)),
                "tasks": [str(identity)],
                "schedule": task.schedule,
                "description": task.description,
                "tags": dict(task.tags),
                "airflow": {"dag": task.airflow.get("dag", {})},
                **overrides,
            }
        )


def _unique(classes: Iterable[type[Record]]) -> list[type[Record]]:
    """The record classes, first occurrence kept -- `dict` is the ordered set."""
    return list(dict.fromkeys(classes))


def load(path: str | os.PathLike[str], **context: Any) -> Dag:
    """Build the dag a side file declares.

    The file may name its class under `dag:` -- for a subclass that overrides
    `run` or `into_airflow` -- and configures it with the rest. Unlike a job
    side file, naming one is optional: a graph of references has nothing to
    subclass for, so `Dag` itself is the default rather than a required
    ceremony. Jinja is rendered before parsing, like everywhere else.
    """
    path = pathlib.Path(path)
    mapping = side_files.parse(path, context)
    dotted = mapping.pop("dag", None)
    cls = _dag_class(str(dotted)) if dotted else Dag
    return cls.from_dict(mapping)


def load_all(root: str | os.PathLike[str] | None = None, **context: Any) -> list[Dag]:
    """Every dag declared under `root`, in file order, and registered."""
    from rekep import config

    folder = config.folder("dags", root if root is not None else DAGS_ROOT)
    return [
        config.register(load(path, **context))
        for path in sorted(folder.glob("*"))
        if path.suffix in EXTENSIONS
    ]


def find(uri: str, root: str | os.PathLike[str] | None = None, **context: Any) -> Dag:
    """The dag `uri` names: from the registry, or by loading the folder."""
    from rekep import config

    found = config.lookup(uri, service="dags")
    if found is None:
        load_all(root, **context)
        found = config.lookup(uri, service="dags")
    if found is None:
        raise KeyError(f"no dag {uri!r} declared under {config.folder('dags', root)}")
    return found


def _dag_class(dotted: str) -> type[Dag]:
    from rekep.imports import locate

    cls = locate(dotted)
    if not (isinstance(cls, type) and issubclass(cls, Dag)):
        raise TypeError(f"{dotted} is not a Dag subclass")
    return cls
