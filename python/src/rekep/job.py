"""Job: the OpenLineage resource for a process that consumes and produces
datasets (https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md).

A job is both a record and a program. The record half is deployment
configuration -- namespace, name, schedule, lineage -- loaded from a side
file under `stacks/jobs` and round-trippable like any other record. The
program half is `arrow_transform`: batches in, batches out, all processing in
Arrow. The base implementation raises rather than enforcing an abstract
method, so a bare `Job` still declares and describes lineage -- namespace,
name, consumes, produces -- even before anyone gives it something to run;
override `arrow_transform` to make one that actually moves data.

`run` chains the three stages -- `extract` yields source batches,
`arrow_transform` reshapes them, `load` disposes of them -- and each stage
can be overridden alone. `run_tracked` wraps that whole chain in a `Run`,
`START` before and `COMPLETE`/`FAIL` after -- the same internal lineage
bookkeeping `Dataset`'s protocol writers use, kept on the instance
to whatever lineage client is bound -- and to nothing at all when none is.

`@arrow_task` is the decorator shortcut: it binds a plain
batches-in/batches-out function as a `Job`'s `arrow_transform`, so a function
becomes a fully lineage-tracked job without a `@record class ... (Job)`
declaration for every one-off transform.
"""

from __future__ import annotations

import dataclasses
import json
import os
import pathlib
import tomllib
from collections.abc import Callable, Iterator, Sequence
from typing import Any

import pyarrow

from rekep.imports import locate
from rekep.namespace import Namespace, ResourceUri
from rekep.records import Record, record
from rekep.render import render
from rekep.require import require

#: Where job side files live when nothing says otherwise: the checkout's
#: `stacks/jobs` if it has one, else the user's `~/.config/rekep/jobs` -- see
#: `rekep.config.folder`. `REKEP_JOBS_ROOT` overrides both.
JOBS_ROOT = os.environ.get("REKEP_JOBS_ROOT")

#: Config extensions a jobs directory is scanned for.
EXTENSIONS = (".yaml", ".yml", ".toml", ".json")


@record
class Job(Record):
    """A process definition that consumes and produces datasets.

    `namespace` and `name` give it OpenLineage identity; `qualified_name`
    joins them through `Namespace`, the same recursive path-builder a
    `Dataset`'s location uses. Everything else is deployment configuration:
    when to run, what it reads and writes, and -- via `arrow_transform` --
    how it moves data.
    """

    name: str
    """Job identifier; becomes the Airflow dag_id."""

    namespace: str | None = None
    """OpenLineage namespace this job is identified under, scheduler-assigned."""

    schedule: str | None = None
    """Cron expression or Airflow schedule alias, None for manual runs."""

    description: str | None = None
    """One line on what this job does."""

    uri: str | None = None
    """This job's identity as a path: `job:/namespace/name`.

    An override for `namespace`/`name`, in the same spelling a dataset uses,
    for a declaration that would rather name itself once than twice."""

    source: str | None = None
    """URL of the log the default `extract` reads, None when overridden."""

    timezone: str | None = None
    """IANA zone the source's wall-clock timestamps are in (`Europe/Paris`).

    A log writes local time and says nothing about which local, so this is
    the one piece of context that cannot be recovered from the file. None
    reads the clock as UTC, which is right only for logs actually written
    that way."""

    tags: list[str] = dataclasses.field(default_factory=list)
    """Extra tags for the orchestrator, on top of the derived lineage tags."""

    consumes: list[str] = dataclasses.field(default_factory=list)
    """Dotted paths of the records this job reads."""

    produces: list[str] = dataclasses.field(default_factory=list)
    """Dotted paths of the records this job writes."""

    repo_url: str | None = None
    """Git remote of the repository this job's code lives in."""

    script_path: str | None = None
    """Path to this job's source, relative to the repo root."""

    airflow: dict[str, dict[str, Any]] = dataclasses.field(default_factory=dict)
    """Airflow-specific config, under `dag`/`task` -- each merged as kwargs
    into the matching call in `into_airflow` (`pool`, `retries`, `owner`,
    `trigger_rule`, `max_active_runs`, ...). Generic on purpose: rekep does
    not maintain a list of which kwarg belongs to Airflow's `DAG` versus its
    `@task`, Airflow does."""

    env: dict[str, str] = dataclasses.field(default_factory=dict)
    """Environment variables this job's execution reads. Values may be Jinja
    (`{{ env.BUCKET }}`, `{{ git_branch_suffix }}`) -- the whole side file is
    rendered before parsing, so this dict arrives already resolved."""

    properties: dict[str, str] = dataclasses.field(default_factory=dict)
    """Generic extra properties: whatever a deployment needs to carry that
    is neither lineage nor Airflow config."""

    __fn: Any = None  # bound arrow_transform, from @arrow_task: state, not schema

    # -- identity -------------------------------------------------------

    def qualified_name(self) -> str:
        """`namespace` and `name` joined through `Namespace`, one identifier."""
        levels = [level for level in (self.namespace, self.name) if level]
        return Namespace.of(*levels).path() if levels else self.name

    def resource_uri(self) -> ResourceUri:
        """This job's identity: `job:/namespace/name`.

        A `ResourceUri`, the one place a job's and a dataset's identifiers
        are built and parsed -- so the two can never collide even when they
        share a namespace and a name, and every spelling resolves to one
        identity. A declared `uri` wins; otherwise it is built from
        `namespace` and `name`, which is what an orchestrator uses anyway.
        """
        if self.uri:
            return ResourceUri.parse(self.uri, service="jobs")
        return ResourceUri.of("jobs", *(self.namespace or "").split("/"), self.name)

    def source_code_location_facet(self) -> dict[str, Any]:
        """OpenLineage `SourceCodeLocationJobFacet`: where this job's code lives.

        `repo_url`/`script_path` are this job's own declaration; `version`
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
        """Every static facet this job carries; `sourceCodeLocation` when declared."""
        facets: dict[str, Any] = {}
        if self.repo_url or self.script_path:
            facets["sourceCodeLocation"] = self.source_code_location_facet()
        return facets

    # -- the program ----------------------------------------------------

    def bind(
        self, fn: Callable[[Iterator[pyarrow.RecordBatch]], Iterator[pyarrow.RecordBatch]]
    ) -> Job:
        """This job with `fn` attached as its `arrow_transform`; config stays authoritative."""
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

    def with_lineage(self, client: Any) -> Job:
        """Bind a lineage client, and return this job so the call chains.

        A call rather than a field: a client is a runtime handle, and a side
        file that declares a job has no business carrying one. Until one is
        bound, `run_tracked` is `run` -- see `rekep.lineage`.
        """
        self.__dict__["_Job__client"] = client
        return self

    def lineage_client(self) -> Any | None:
        """The bound client, or None when nothing is listening."""
        return self.__dict__.get("_Job__client")

    def lineage(self) -> Any | None:
        """The boundary for one run of this job, or None when lineage is off.

        Inputs and outputs are the datasets `consumes`/`produces` name,
        identified the same way a `Dataset` identifies itself, and resolved
        *before* the run starts: a bad dotted path should fail as a
        configuration error, not as a run that started and then died.
        """
        from rekep.lineage import Lineage
        from rekep.run import InputDataset, OutputDataset

        client = self.lineage_client()
        if client is None:
            return None
        namespace = self.namespace or "default"
        return Lineage(
            client=client,
            job=self,
            inputs=[
                InputDataset(namespace=namespace, name=cls.doris_table_name())
                for cls in self.consumed_records()
            ],
            outputs=[
                OutputDataset(namespace=namespace, name=cls.doris_table_name())
                for cls in self.produced_records()
            ],
        )

    def run_tracked(self) -> Any:
        """`run()`, wrapped in a run: `START` before, `COMPLETE`/`FAIL` after.

        The same boundary `Dataset`'s protocol writers wrap their own I/O
        in, but around this job's whole extract -> transform -> load. With
        no lineage client bound it *is* `run()` -- no run, no events, no
        cost -- so a job is tracked by binding one, never by choosing a
        different method to call.
        """
        run = self.lineage()
        if run is None:
            return self.run()

        run.start()
        try:
            result = self.run()
        except Exception as error:
            run.fail(error)
            raise
        run.complete()
        return result

    def __call__(self) -> Any:
        """Run this job, lineage tracked -- what an `@arrow_task` call does."""
        return self.run_tracked()

    # -- interop ----------------------------------------------------------

    def into_airflow(self) -> Any:
        """This job as a single-task Airflow DAG, lineage tagged and documented.

        **A job is the task.** There is no decorator to wrap a function in
        and no DAG subclass to inherit from: a `Job` already declares what a
        task needs -- what it reads, what it writes, when it runs, how to run
        it -- so this hands those to Airflow's own `DAG` and `@task` and gets
        out of the way. Anything Airflow accepts, `airflow["dag"]` and
        `airflow["task"]` pass straight through, because rekep keeps no list
        of which kwarg belongs to which; Airflow has one already.

        What is derived rather than passed is the lineage
        (`rekep.airflow.lineage`): tags and a Consumes/Produces table for the
        DAG, inlets and outlets for the task, from `consumes`/`produces`.
        """
        from rekep.airflow import lineage, sdk

        consumes, produces = self.consumed_records(), self.produced_records()
        with sdk.DAG(
            self.name,
            **lineage.dag_arguments(
                consumes,
                produces,
                description=self.description,
                schedule=self.schedule,
                tags=list(self.tags),
                catchup=False,
                **self.airflow.get("dag", {}),
            ),
        ) as built:
            sdk.task(
                **lineage.task_arguments(
                    consumes, produces, task_id=self.name, **self.airflow.get("task", {})
                )
            )(self.run_tracked)()
        return built

    # -- lineage ------------------------------------------------------------

    def consumed_records(self) -> list[type[Record]]:
        """The record classes behind `consumes`."""
        return [_record_class(path) for path in self.consumes]

    def produced_records(self) -> list[type[Record]]:
        """The record classes behind `produces`."""
        return [_record_class(path) for path in self.produces]


@record
class Passthrough(Job):
    """Copy log batches through unchanged; the wiring reference.

    Exists so a deployment can smoke-test its side files, sources and DAG
    plumbing with a job whose transform provably does nothing.
    """

    def arrow_transform(
        self, batches: Iterator[pyarrow.RecordBatch]
    ) -> Iterator[pyarrow.RecordBatch]:
        yield from batches


def arrow_task(
    fn: Callable[[Iterator[pyarrow.RecordBatch]], Iterator[pyarrow.RecordBatch]] | None = None,
    *,
    config: Job | None = None,
    name: str | None = None,
    namespace: str | None = None,
    consumes: Sequence[type[Record] | str] = (),
    produces: Sequence[type[Record] | str] = (),
    **job_kwargs: Any,
) -> Any:
    """Bind a batches-in/batches-out function as a `Job`'s `arrow_transform`.

    `@arrow_task` (bare) or `@arrow_task(name=..., consumes=[Log])` (configured)
    turns a plain function into a fully-declared, lineage-tracked `Job` --
    calling the result runs `run_tracked()`, so every call opens a `Run`
    before extract -> transform -> load and closes it `COMPLETE`/`FAIL` after,
    covering the read and the write both, not only a `Dataset`'s own writes.

    `config=` takes an already-built `Job` (typically one loaded from a side
    file) and binds `fn` onto it, config staying authoritative; everything
    else builds a fresh one. Lineage takes record classes or dotted paths
    interchangeably, like `Job.consumes`/`produces` do.
    """

    def wrap(target: Callable[..., Any]) -> Job:
        job = config or Job(
            name=name or target.__name__,
            namespace=namespace,
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
    """Build the job a side file declares.

    The file names its class under the `job` key and configures it with the
    rest; it may use Jinja (`{{ env.BUCKET }}`), rendered with `context` and
    the environment before parsing. Which job classes exist is Python's
    business -- the side file only picks one and fills its fields in.
    """
    path = pathlib.Path(path)
    mapping = _parse(render(path.read_text(encoding="utf-8"), **context), path.suffix)
    dotted = mapping.pop("job", None)
    if not dotted:
        raise ValueError(f"{path} declares no `job:` class")
    cls = _job_class(str(dotted))
    return cls.from_dict(mapping)


def load_all(root: str | os.PathLike[str] | None = None, **context: Any) -> list[Job]:
    """Every job declared under `root`, in name order, and registered.

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


def _parse(text: str, suffix: str) -> dict[str, Any]:
    if suffix in (".yaml", ".yml"):
        return require("yaml", "yaml").safe_load(text) or {}
    if suffix == ".toml":
        return tomllib.loads(text)
    if suffix == ".json":
        return dict(json.loads(text))
    raise ValueError(f"no parser for {suffix!r} job files")


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
