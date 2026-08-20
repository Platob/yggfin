"""Job: the OpenLineage resource for a process that consumes and produces
datasets (https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md).

**A job is one task.** OpenLineage names a job hierarchically -- `dag_id`,
then `task_id` -- and that is exactly what this is: the unit a `Dag`
(`rekep.dag`) schedules, one node of a graph rather than the graph. So the
naming here is task naming: `task_id()` is this task's own name, and
`task_name()` is the full one its levels add up to.

A job is both a record and a program. The record half is deployment
configuration -- identity, schedule, lineage -- loaded from a side file under
`stacks/jobs` and round-trippable like any other record. The program half is
`arrow_transform`: batches in, batches out, all processing in Arrow. The base
implementation raises rather than enforcing an abstract method, so a bare
`Job` still declares and describes lineage -- identity, consumes, produces --
even before anyone gives it something to run; override `arrow_transform` to
make one that actually moves data.

`run` chains the three stages -- `extract` yields source batches,
`arrow_transform` reshapes them, `load` disposes of them -- and each stage
can be overridden alone. What a run *was* is representable without being
emitted anywhere: `into_run_event(state)` builds the OpenLineage `RunEvent`
for this task, inputs and outputs included. There is no client here and no
transport -- rekep says what a run is, whoever collects it says where it goes.

`@arrow_task` is the decorator shortcut: it binds a plain
batches-in/batches-out function as a `Job`'s `arrow_transform`, so a function
becomes a fully-declared task without a `@record class ... (Job)` declaration
for every one-off transform.
"""

from __future__ import annotations

import dataclasses
import os
import pathlib
from collections.abc import Callable, Iterator, Sequence
from typing import Any

import pyarrow

from rekep.imports import locate
from rekep.namespace import Namespace, ResourceUri
from rekep.records import Record, record
from rekep.records import registry as side_files

#: Where job side files live when nothing says otherwise: the checkout's
#: `stacks/jobs` if it has one, else the user's `~/.config/rekep/jobs` -- see
#: `rekep.config.folder`. `REKEP_JOBS_ROOT` overrides both.
JOBS_ROOT = os.environ.get("REKEP_JOBS_ROOT")

#: Config extensions a jobs directory is scanned for.
EXTENSIONS = side_files.EXTENSIONS


@record
class Job(Record):
    """One task: a process definition that consumes and produces datasets.

    `uri` is the whole identity -- `rekep:/jobs/namespace/name`, a path like every
    other resource here -- and `task_id`/`task_namespace`/`task_name` read
    the levels back out of it. Everything else is deployment configuration:
    when to run, what it reads and writes, and -- via `arrow_transform` --
    how it moves data.
    """

    uri: str
    """This task's identity as a path: `rekep:/jobs/namespace/name`.

    One string rather than a name beside a namespace, because they are one
    identity and a resource that can spell itself two ways eventually spells
    itself two different ways. A bare `rekep:/jobs/passthrough` is a less qualified
    name, not a different shape."""

    schedule: str | None = None
    """Cron expression or scheduler alias, None when a `Dag` decides instead."""

    description: str | None = None
    """One line on what this task does."""

    source: str | None = None
    """URL of the log the default `extract` reads, None when overridden."""

    timezone: str | None = None
    """IANA zone the source's wall-clock timestamps are in (`Europe/Paris`).

    A log writes local time and says nothing about which local, so this is
    the one piece of context that cannot be recovered from the file. None
    reads the clock as UTC, which is right only for logs actually written
    that way."""

    tags: dict[str, str] = dataclasses.field(default_factory=dict)
    """Extra tags for the orchestrator, on top of the derived lineage tags.

    A mapping, not a list: a bare `structuring` says nothing about what makes
    it a tag, while `stage: structuring` names the dimension *and* the value,
    which is what makes tags searchable and mergeable. Two declarations of
    the same key are one decision to resolve, not two tags to carry."""

    consumes: list[str] = dataclasses.field(default_factory=list)
    """Dotted paths of the records this task reads."""

    produces: list[str] = dataclasses.field(default_factory=list)
    """Dotted paths of the records this task writes."""

    repo_url: str | None = None
    """Git remote of the repository this task's code lives in."""

    script_path: str | None = None
    """Path to this task's source, relative to the repo root."""

    airflow: dict[str, dict[str, Any]] = dataclasses.field(default_factory=dict)
    """Airflow-specific config, under `dag`/`task` -- each merged as kwargs
    into the matching call in `Dag.into_airflow` (`pool`, `retries`, `owner`,
    `trigger_rule`, `max_active_runs`, ...). Generic on purpose: rekep does
    not maintain a list of which kwarg belongs to Airflow's `DAG` versus its
    `@task`, Airflow does."""

    env: dict[str, str] = dataclasses.field(default_factory=dict)
    """Environment variables this task's execution reads. Values may be Jinja
    (`{{ env.BUCKET }}`, `{{ git_branch_suffix }}`) -- the whole side file is
    rendered before parsing, so this dict arrives already resolved."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """Generic extra properties: whatever a deployment needs to carry that
    is neither lineage nor orchestrator config."""

    __fn: Any = None  # bound arrow_transform, from @arrow_task: state, not schema

    # -- identity -------------------------------------------------------

    def resource_uri(self) -> ResourceUri:
        """This task's identity: `rekep:/jobs/namespace/name`.

        A `ResourceUri`, the one place a job's and a dataset's identifiers
        are built and parsed -- so the two can never collide even when they
        share a namespace and a name, and every spelling resolves to one
        identity.
        """
        return ResourceUri.parse(self.uri, service="jobs")

    def task_id(self) -> str:
        """This task's own name, unqualified -- what Airflow calls a task_id."""
        return self.resource_uri().name()

    def task_namespace(self) -> str:
        """The namespace this task is identified under."""
        return self.resource_uri().namespace()

    def task_name(self) -> str:
        """Every level of the identity joined, `dag_id.task_id`-style.

        OpenLineage names a job by its whole hierarchy rather than its last
        level, and `Namespace` is the recursive path-builder that joins one
        -- the same one a `Dataset`'s location uses. A `Dag` qualifies it
        further (`Dag.task_name`), because a task named inside a dag is that
        dag's task, not a second job that happens to share a name.
        """
        return Namespace.of(*self.resource_uri().levels).path()

    def source_code_location_facet(self) -> dict[str, Any]:
        """OpenLineage `SourceCodeLocationJobFacet`: where this task's code lives.

        `repo_url`/`script_path` are this task's own declaration; `version`
        and `branch` come from `rekep.render.git_context()` -- the same git
        facts side files use for branch-conditional naming -- read fresh
        each call rather than baked in at deploy time.
        """
        from rekep.render import git_context

        context = git_context()
        facet: dict[str, Any] = {"type": "git", "version": context["git_sha"]}
        if self.repo_url:
            facet["repoUrl"] = self.repo_url
        if self.script_path:
            facet["path"] = self.script_path
        if context["git_branch"]:
            facet["branch"] = context["git_branch"]
        return facet

    def facets(self) -> dict[str, Any]:
        """Every static facet this task carries; `sourceCodeLocation` when declared."""
        facets: dict[str, Any] = {}
        if self.repo_url or self.script_path:
            facets["sourceCodeLocation"] = self.source_code_location_facet()
        return facets

    # -- the program ----------------------------------------------------

    def bind(
        self, fn: Callable[[Iterator[pyarrow.RecordBatch]], Iterator[pyarrow.RecordBatch]]
    ) -> Job:
        """This task with `fn` attached as its `arrow_transform`; config stays authoritative."""
        self._Job__fn = fn
        return self

    def arrow_transform(
        self, batches: Iterator[pyarrow.RecordBatch]
    ) -> Iterator[pyarrow.RecordBatch]:
        """Reshape a stream of record batches, one batch at a time.

        Not enforced abstract: a `Job` declared without one still round-trips
        and documents its lineage. A function bound through `@arrow_task` or
        `bind()` runs here; failing that, override this method, or the call
        raises naming the class. Lean on `pyarrow.compute`, never materialise
        the stream.
        """
        bound = getattr(self, "_Job__fn", None)
        if bound is None:
            raise NotImplementedError(f"{type(self).__name__} has no arrow_transform to run")
        yield from bound(batches)

    def extract(self) -> Iterator[pyarrow.RecordBatch]:
        """Source batches; by default, the parsed log at `source`."""
        from rekep.logs import LogFile

        if self.source is None:
            raise NotImplementedError(f"{type(self).__name__} has no source url; override extract")
        with LogFile.from_url(self.source, timezone=self.timezone) as log:
            yield from log.into_arrow_batches()

    def load(self, batches: Iterator[pyarrow.RecordBatch]) -> int:
        """Dispose of the transformed batches; by default, count rows.

        A real sink -- Iceberg append, parquet write -- overrides this. The
        default drains the stream so `run` is exercisable end to end without
        one.
        """
        return sum(batch.num_rows for batch in batches)

    def run(self) -> Any:
        """Extract, transform, load."""
        return self.load(self.arrow_transform(self.extract()))

    def __call__(self) -> Any:
        """Run this task -- what calling an `@arrow_task` does."""
        return self.run()

    # -- lineage ----------------------------------------------------------

    def consumed_records(self) -> list[type[Record]]:
        """The record classes behind `consumes`."""
        return [_record_class(path) for path in self.consumes]

    def produced_records(self) -> list[type[Record]]:
        """The record classes behind `produces`."""
        return [_record_class(path) for path in self.produces]

    def inputs(self) -> list[Any]:
        """The datasets `consumes` names, as run references."""
        from rekep.run import InputDataset

        return [
            InputDataset(namespace=self.task_namespace(), name=cls.doris_table_name())
            for cls in self.consumed_records()
        ]

    def outputs(self) -> list[Any]:
        """The datasets `produces` names, as run references."""
        from rekep.run import OutputDataset

        return [
            OutputDataset(namespace=self.task_namespace(), name=cls.doris_table_name())
            for cls in self.produced_records()
        ]

    def into_run_event(self, state: Any, run: Any = None) -> Any:
        """This task's `RunEvent` in `state`: the representation, not a report.

        rekep describes what a run *is* -- OpenLineage's own shape, built from
        what this task already declares -- and stops there. Nothing here emits
        anything, because a transport is the collector's business and a client
        we do not ship is a client we cannot get wrong; `into_json()` on the
        result is what leaves the process.

        `run` carries an existing `Run` when several events belong to one
        execution, since a run id has to be stable across `START` and whatever
        ends it -- and is where an `errorMessage` facet rides on a `FAIL`.
        """
        from rekep.run import Run, RunEvent, now

        return RunEvent(
            event_type=state,
            event_time=now(),
            run=run if run is not None else Run(),
            job=self,
            inputs=self.inputs(),
            outputs=self.outputs(),
        )


@record
class Passthrough(Job):
    """Copy log batches through unchanged; the wiring reference.

    Exists so a deployment can smoke-test its side files, sources and DAG
    plumbing with a task whose transform provably does nothing.
    """

    def arrow_transform(
        self, batches: Iterator[pyarrow.RecordBatch]
    ) -> Iterator[pyarrow.RecordBatch]:
        yield from batches


def arrow_task(
    fn: Callable[[Iterator[pyarrow.RecordBatch]], Iterator[pyarrow.RecordBatch]] | None = None,
    *,
    config: Job | None = None,
    uri: str | None = None,
    consumes: Sequence[type[Record] | str] = (),
    produces: Sequence[type[Record] | str] = (),
    **job_kwargs: Any,
) -> Any:
    """Bind a batches-in/batches-out function as a `Job`'s `arrow_transform`.

    `@arrow_task` (bare) or `@arrow_task(uri="rekep:/jobs/trading/etl", consumes=[Log])`
    turns a plain function into a fully-declared task -- calling the result
    runs `run()`, extract -> transform -> load. Undeclared, the identity is
    `rekep:/jobs/<function name>`: a decorator that made you name the thing twice
    would be a worse decorator.

    `config=` takes an already-built `Job` (typically one loaded from a side
    file) and binds `fn` onto it, config staying authoritative; everything
    else builds a fresh one. Lineage takes record classes or dotted paths
    interchangeably, like `Job.consumes`/`produces` do.
    """

    def wrap(target: Callable[..., Any]) -> Job:
        job = config or Job(
            uri=uri or f"rekep:/jobs/{target.__name__}",
            consumes=[_dotted(entry) for entry in consumes],
            produces=[_dotted(entry) for entry in produces],
            **job_kwargs,
        )
        return job.bind(target)

    return wrap if fn is None else wrap(fn)


def _dotted(entry: type[Record] | str) -> str:
    if isinstance(entry, str):
        return entry
    return f"{entry.__module__}.{entry.__qualname__}"


def load(path: str | os.PathLike[str], **context: Any) -> Job:
    """Build the task a side file declares.

    The file names its class under the `job` key and configures it with the
    rest; it may use Jinja (`{{ env.BUCKET }}`), rendered with `context` and
    the environment before parsing. Which job classes exist is Python's
    business -- the side file only picks one and fills its fields in.
    """
    path = pathlib.Path(path)
    mapping = side_files.parse(path, context)
    dotted = mapping.pop("job", None)
    if not dotted:
        raise ValueError(f"{path} declares no `job:` class")
    cls = _job_class(str(dotted))
    return cls.from_dict(mapping)


def load_all(root: str | os.PathLike[str] | None = None, **context: Any) -> list[Job]:
    """Every task declared under `root`, in file order, and registered.

    `root` defaults through `rekep.config.folder`: the checkout's
    `stacks/jobs` when it has one, the user's config home when it does not.
    """
    from rekep import config

    folder = config.folder("jobs", root if root is not None else JOBS_ROOT)
    return [
        config.register(load(path, **context))
        for path in sorted(folder.glob("*"))
        if path.suffix in EXTENSIONS
    ]


def find(uri: str, root: str | os.PathLike[str] | None = None, **context: Any) -> Job:
    """The task `uri` names: from the registry, or by loading the folder.

    Not called `load`, which is the file loader above: one takes a path and
    builds whatever it declares, this one takes an identity and finds who
    already answers to it -- and only reads the folder when nobody does.
    """
    from rekep import config

    found = config.lookup(uri, service="jobs")
    if found is None:
        load_all(root, **context)
        found = config.lookup(uri, service="jobs")
    if found is None:
        raise KeyError(f"no job {uri!r} declared under {config.folder('jobs', root)}")
    return found


def _job_class(dotted: str) -> type[Job]:
    cls = locate(dotted)
    if not (isinstance(cls, type) and issubclass(cls, Job)):
        raise TypeError(f"{dotted} is not a Job subclass")
    return cls


def _record_class(dotted: str) -> type[Record]:
    cls = locate(dotted)
    if not (isinstance(cls, type) and issubclass(cls, Record)):
        raise TypeError(f"{dotted} is not a Record")
    return cls
