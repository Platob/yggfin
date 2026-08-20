"""FilesToLogs: parse raw trading-log files into `Log` records.

The reference first stage of the log-processing pipeline. `Job.extract`
already does the parsing (`LogFile.from_url(self.source)`), so
`arrow_transform` only has to pass batches through unchanged -- structurally
the same shape as `rekep.job.Passthrough`, but purpose-named, and with
`produces` defaulted so a side file naming no lineage still gets it right.
Configure it under `stacks/jobs/` (see the shipped `files_to_logs.yaml`).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator

import pyarrow

from rekep.job import Job
from rekep.records import record


@record
class FilesToLogs(Job):
    """Parse raw log files at `source` into `Log` records, unchanged."""

    produces: list[str] = dataclasses.field(default_factory=lambda: ["rekep:///records/log"])
    """Defaults to `Log` -- `extract` parses nothing else."""

    def arrow_transform(
        self, batches: Iterator[pyarrow.RecordBatch]
    ) -> Iterator[pyarrow.RecordBatch]:
        yield from batches
