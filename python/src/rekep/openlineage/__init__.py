"""OpenLineage run events, emitted to a local file -- no Marquez/HTTP backend.

`OpenLineage.from_path` opens the file transport; `start_run` wraps one job
execution in START/COMPLETE/FAIL `RunEvent`s, with datasets and facets
derived straight from the records a run consumes and produces -- the same
records already drive Arrow, Iceberg, DDL and the Airflow asset graph (house
rule #4: a record is the whole product, lineage included).

Requires the `openlineage` extra; nothing below imports `openlineage` at
module scope, so this package itself is always importable -- only calling
`OpenLineage.from_path` or `dataset_of` needs the dependency installed.
"""

from __future__ import annotations

from rekep.openlineage.client import OpenLineage, Run
from rekep.openlineage.datasets import dataset_of, namespace_of

__all__ = ["OpenLineage", "Run", "dataset_of", "namespace_of"]
