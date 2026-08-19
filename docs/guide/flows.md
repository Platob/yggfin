# Flows

A `Flow` is one data movement: what it reads, what it writes, and how it
transforms — declared as a record, transformed in Arrow.

## Declaring

```python
from collections.abc import Iterator
import pyarrow
import pyarrow.compute as pc
from rekep import record
from rekep.flows import Flow

@record
class ErrorsOnly(Flow):
    """Keep the rows whose message carries a stack trace."""

    def arrow_transform(
        self, batches: Iterator[pyarrow.RecordBatch]
    ) -> Iterator[pyarrow.RecordBatch]:
        for batch in batches:
            mask = pc.match_substring(batch.column("message"), "Exception")
            yield batch.filter(mask)
```

`arrow_transform` is the one abstract method: batches in, batches out, nothing
materialised. `run()` chains `extract → arrow_transform → load`; the default
`extract` parses the log at `self.source`, the default `load` drains and
counts, and each stage overrides independently.

## Side files

Deployment configuration lives in one file per flow under `stacks/flows`,
schema'd by the `Flow` record itself and rendered with Jinja before parsing:

```yaml
# stacks/flows/passthrough.yaml
flow: rekep.flows.Passthrough
name: passthrough
schedule: "@daily"
source: "{{ env.get('REKEP_SOURCE_URL', '') }}"
consumes: [rekep.models.Log]
produces: [rekep.models.Log]
```

```python
from rekep.flows import load, load_all

flow = load("stacks/flows/passthrough.yaml")
flows = load_all()          # every side file, name-sorted
```

## Airflow

An Airflow DAG folder needs one line:

```python
from rekep.airflow.flows import dags

globals().update(dags())
```

Each side file becomes a DAG whose tags, docs, inlets and outlets derive from
the `consumes`/`produces` record lists — the lineage graph writes itself.
The `rekep.airflow.dag` / `task` decorators accept the same `consumes=` /
`produces=` for hand-written DAGs.
