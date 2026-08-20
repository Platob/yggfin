"""Run: one execution instance of a job, OpenLineage's `RunEvent` shape
(https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md).

A `RunEvent` ties a `Run` -- an execution instance, identified by `run_id` --
to the `Job` that ran and the datasets it read and wrote, at one moment named
by `event_type`: `START` before work begins, `COMPLETE`/`FAIL`/`ABORT` after.
`InputDataset`/`OutputDataset` carry only identity and facets, deliberately
lighter than the full `Dataset` resource -- an event references what it
moved, it does not restate the whole schema.

Nothing here calls out anywhere: these are the shapes, and `rekep.lineage`
is what decides who is told. A `Dataset` or `Job` with no client bound never
builds one of these at all.
"""

from __future__ import annotations

import dataclasses
import datetime
import enum
import uuid
from typing import Any

from rekep.job import Job
from rekep.records import Record, record

#: URI identifying rekep as the producer of every event it builds.
PRODUCER = "https://github.com/Platob/yggfin"

#: OpenLineage schema version this package's events claim to follow.
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json"


def now() -> datetime.datetime:
    """The current time, timezone-aware -- every `RunEvent.event_time`'s source."""
    return datetime.datetime.now(datetime.UTC)


class RunState(enum.Enum):
    """`RunEvent.event_type`: where a run stands, in the order it is seen."""

    START = "START"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    ABORT = "ABORT"
    FAIL = "FAIL"
    OTHER = "OTHER"


@record
class Run(Record):
    """One execution instance of a job, identified by `run_id`."""

    run_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    """RFC 4122 UUID identifying this run, stable across its events."""

    facets: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Run facets: `nominalTime`, `parent`, `errorMessage`, and the rest."""


@record
class InputDataset(Record):
    """A dataset a run read, identified and faceted -- not the full resource."""

    namespace: str
    """Namespace of the dataset read."""

    name: str
    """Name of the dataset read, unique within `namespace`."""

    facets: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Dataset facets as of this run: `schema`, `dataSource`, and the rest."""

    input_facets: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Facets only meaningful on an input: `dataQualityMetrics`, `inputStatistics`."""


@record
class OutputDataset(Record):
    """A dataset a run wrote, identified and faceted -- not the full resource."""

    namespace: str
    """Namespace of the dataset written."""

    name: str
    """Name of the dataset written, unique within `namespace`."""

    facets: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Dataset facets as of this run: `schema`, `dataSource`, and the rest."""

    output_facets: dict[str, Any] = dataclasses.field(default_factory=dict)
    """Facets only meaningful on an output: `outputStatistics` (rows, bytes)."""


@record
class RunEvent(Record):
    """One moment in a run's life, in OpenLineage's own `RunEvent` shape."""

    event_type: RunState
    """Where the run stands at `event_time`."""

    event_time: datetime.datetime
    """When this event was recorded, timezone-aware."""

    run: Run
    """The run this event belongs to."""

    job: Job
    """The job whose run this is."""

    inputs: list[InputDataset] = dataclasses.field(default_factory=list)
    """Datasets read as of this event."""

    outputs: list[OutputDataset] = dataclasses.field(default_factory=list)
    """Datasets written as of this event."""

    producer: str = PRODUCER
    """URI identifying the system that generated this event."""

    schema_url: str = SCHEMA_URL
    """Pointer to the OpenLineage schema version this event follows."""
