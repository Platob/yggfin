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
(`Job.events()`) rather than emitted anywhere.

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
from rekep.namespace import Namespace, unique_uri
from rekep.records import Record, record
from rekep.render import render
from rekep.require import require

#: Where job side files live, relative to the deployment root. Overridable per
#: call and by environment, so a jobs folder can point anywhere.
JOBS_ROOT = pathlib.Path(os.environ.get("REKEP_JOBS_ROOT", "stacks/jobs"))

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

    source: str | None = None
    """URL of the log the default `extract` reads, None when overridden."""

    tags: list[str] = dataclasses.field(default_factory=list)
    """Extra tags for the orchestrator, on top of the derived lineage tags."""

    consumes: list[str] = dataclasses.field(default_factory=list)
    """Dotted paths of the records this job reads."""

    produces: list[str] = dataclasses.field(default_factory=list)
    """Dotted paths of the records this job writes."""

    __fn: Any = None  # bound arrow_transform, from @arrow_task: state, not schema

    # -- identity -------------------------------------------------------

    def qualified_name(self) -> str:
        """`namespace` and `name` joined through `Namespace`, one identifier."""
        levels = [level for level in (self.namespace, self.name) if level]
        return Namespace.of(*levels).path() if levels else self.name

    def uri(self) -> str:
        """This job's globally unique id: `job://namespace/name`.

        Built by `rekep.namespace.unique_uri`, the one place a job and a
        dataset's identifiers come from -- so the two can never collide even
        when they share a namespace and a name.
        """
        return unique_uri("job", self.namespace, self.name)

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
        with LogFile.from_url(self.source) as log:
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

    def run_tracked(self) -> Any:
        """`run()`, wrapped in a `Run`: `START` before, `COMPLETE`/`FAIL` after.

        The same boundary `Dataset`'s protocol writers wrap their own writes
        in, but around this job's whole extract -> transform -> load: inputs
        and outputs are the datasets `consumes`/`produces` name, identified
        the same way a `Dataset` identifies itself. Events land in
        `self.events()`, internal bookkeeping, not an emission.
        """
        from rekep.run import InputDataset, OutputDataset, Run, RunEvent, RunState
        from rekep.run import now as _now

        run = Run()
        namespace = self.namespace or "default"
        inputs = [
            InputDataset(namespace=namespace, name=cls.doris_table_name())
            for cls in self.consumed_records()
        ]
        outputs = [
            OutputDataset(namespace=namespace, name=cls.doris_table_name())
            for cls in self.produced_records()
        ]
        self._emit(RunEvent(RunState.START, _now(), run, self, inputs, outputs))
        try:
            result = self.run()
        except Exception:
            self._emit(RunEvent(RunState.FAIL, _now(), run, self, inputs, outputs))
            raise
        self._emit(RunEvent(RunState.COMPLETE, _now(), run, self, inputs, outputs))
        return result

    def events(self) -> list[Any]:
        """This job's own lineage log: every tracked run, `run_tracked` kept."""
        return list(self.__dict__.get("_Job__events", ()))

    def _emit(self, event: Any) -> Any:
        self.__dict__.setdefault("_Job__events", []).append(event)
        return event

    def __call__(self) -> Any:
        """Run this job, lineage tracked -- what an `@arrow_task` call does."""
        return self.run_tracked()

    # -- interop ----------------------------------------------------------

    def into_airflow(self) -> Any:
        """This job as a single-task Airflow DAG, lineage tagged and documented."""
        from rekep.airflow.decorators import DAG
        from rekep.airflow.decorators import task as airflow_task

        consumes, produces = self.consumed_records(), self.produced_records()
        with DAG(
            self.name,
            description=self.description,
            schedule=self.schedule,
            tags=list(self.tags),
            consumes=consumes,
            produces=produces,
            catchup=False,
        ) as built:
            airflow_task(task_id=self.name, consumes=consumes, produces=produces)(
                self.run_tracked
            )()
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


def load_all(root: str | os.PathLike[str] = JOBS_ROOT, **context: Any) -> list[Job]:
    """Every job declared under `root`, in name order."""
    return [
        load(path, **context)
        for path in sorted(pathlib.Path(root).glob("*"))
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
