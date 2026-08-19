"""The identity flow: batches through unchanged."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow

from rekep.flows.flow import Flow
from rekep.records import record


@record
class Passthrough(Flow):
    """Copy log batches through unchanged; the wiring reference.

    Exists so a deployment can smoke-test its side files, sources and DAG
    plumbing with a flow whose transform provably does nothing.
    """

    def arrow_transform(
        self, batches: Iterator[pyarrow.RecordBatch]
    ) -> Iterator[pyarrow.RecordBatch]:
        yield from batches
