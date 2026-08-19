"""Flows: data movements declared as records, transformed in Arrow."""

from __future__ import annotations

import abc
import dataclasses
import json
import os
import pathlib
import tomllib
from collections.abc import Iterator
from typing import Any

import pyarrow

from rekep.imports import locate
from rekep.records import Record, record
from rekep.render import render
from rekep.require import require

#: Where flow side files live, relative to the deployment root. Overridable per
#: call and by environment, so a DAG folder can point anywhere.
FLOWS_ROOT = pathlib.Path(os.environ.get("REKEP_FLOWS_ROOT", "stacks/flows"))

#: Config extensions a flows directory is scanned for.
EXTENSIONS = (".yaml", ".yml", ".toml", ".json")


@record
class Flow(Record, abc.ABC):
    """One data movement: what it reads, what it writes, how it transforms.

    A flow is both a record and a program. The record half is deployment
    configuration -- name, schedule, lineage -- loaded from a side file under
    `stacks/flows` and round-trippable like any other record. The program half
    is `arrow_transform`, the one abstract method: batches in, batches out,
    all processing in Arrow.

    `run` chains the three stages -- `extract` yields source batches,
    `arrow_transform` reshapes them, `load` disposes of them -- and each stage
    can be overridden alone.
    """

    name: str
    """Flow identifier; becomes the DAG id."""

    schedule: str | None = None
    """Cron expression or Airflow schedule alias, None for manual runs."""

    description: str | None = None
    """One line on what this movement is for."""

    source: str | None = None
    """URL of the log the default `extract` reads, None when overridden."""

    tags: list[str] = dataclasses.field(default_factory=list)
    """Extra tags for the orchestrator, on top of the derived lineage tags."""

    consumes: list[str] = dataclasses.field(default_factory=list)
    """Dotted paths of the records this flow reads."""

    produces: list[str] = dataclasses.field(default_factory=list)
    """Dotted paths of the records this flow writes."""

    # -- the program --------------------------------------------------------

    @abc.abstractmethod
    def arrow_transform(
        self, batches: Iterator[pyarrow.RecordBatch]
    ) -> Iterator[pyarrow.RecordBatch]:
        """Reshape a stream of record batches, one batch at a time.

        The contract mirrors the rest of the package: never materialise the
        stream, lean on `pyarrow.compute` for the work, and let schema changes
        show in each batch's schema.
        """

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

    # -- interop ------------------------------------------------------------

    def into_task(self) -> Any:
        """This flow as one bound Task: `run` is the callable, lineage kept."""
        from rekep.flows.task import Task

        return Task(
            name=self.name,
            callable=f"{type(self).__module__}.{type(self).__qualname__}",
            consumes=list(self.consumes),
            produces=list(self.produces),
        ).bind(lambda **_: self.run())

    def into_dag(self) -> Any:
        """This flow as a single-task Dag, side-file config carried over."""
        from rekep.flows.dag import Dag

        return Dag(
            name=self.name,
            schedule=self.schedule,
            description=self.description,
            tags=list(self.tags),
            tasks=[self.into_task()],
        )

    # -- lineage ------------------------------------------------------------

    def consumed_records(self) -> list[type[Record]]:
        """The record classes behind `consumes`."""
        return [_record_class(path) for path in self.consumes]

    def produced_records(self) -> list[type[Record]]:
        """The record classes behind `produces`."""
        return [_record_class(path) for path in self.produces]


def load(path: str | os.PathLike[str], **context: Any) -> Flow:
    """Build the flow a side file declares.

    The file names its class under the `flow` key and configures it with the
    rest; it may use Jinja (`{{ env.BUCKET }}`), rendered with `context` and
    the environment before parsing. Which flow classes exist is Python's
    business -- the side file only picks one and fills its fields in.
    """
    path = pathlib.Path(path)
    mapping = _parse(render(path.read_text(encoding="utf-8"), **context), path.suffix)
    dotted = mapping.pop("flow", None)
    if not dotted:
        raise ValueError(f"{path} declares no `flow:` class")
    cls = _flow_class(str(dotted))
    return cls.from_dict(mapping)


def load_all(root: str | os.PathLike[str] = FLOWS_ROOT, **context: Any) -> list[Flow]:
    """Every flow declared under `root`, in name order."""
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
    raise ValueError(f"no parser for {suffix!r} flow files")


def _flow_class(dotted: str) -> type[Flow]:
    cls = locate(dotted)
    if not (isinstance(cls, type) and issubclass(cls, Flow)):
        raise TypeError(f"{dotted} is not a Flow subclass")
    if abc.ABC in cls.__bases__ or getattr(cls, "__abstractmethods__", None):
        raise TypeError(f"{dotted} is abstract and cannot be configured directly")
    return cls


def _record_class(dotted: str) -> type[Record]:
    cls = locate(dotted)
    if not (isinstance(cls, type) and issubclass(cls, Record)):
        raise TypeError(f"{dotted} is not a Record")
    return cls
