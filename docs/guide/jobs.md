# Jobs

A `Job` is OpenLineage's resource for a process that consumes and produces
datasets — what it reads, what it writes, and how it transforms — declared as
a record, transformed in Arrow.

## Declaring

```python
from collections.abc import Iterator
import pyarrow
import pyarrow.compute as pc
from rekep import record
from rekep.job import Job

@record
class ErrorsOnly(Job):
    """Keep the rows whose message carries a stack trace."""

    def arrow_transform(
        self, batches: Iterator[pyarrow.RecordBatch]
    ) -> Iterator[pyarrow.RecordBatch]:
        for batch in batches:
            mask = pc.match_substring(batch.column("message"), "Exception")
            yield batch.filter(mask)
```

`arrow_transform` is the one method a real job overrides: batches in, batches
out, nothing materialised. It is not enforced abstract — a bare `Job` still
declares and describes its lineage — but calling `run()` without overriding
it raises, naming the class. `run()` chains
`extract → arrow_transform → load`; the default `extract` parses the log at
`self.source`, the default `load` drains and counts, and each stage overrides
independently.

## Side files

Deployment configuration lives in one file per job under `stacks/jobs`,
schema'd by the `Job` record itself and rendered with Jinja before parsing:

```yaml
# stacks/jobs/passthrough.yaml
job: rekep.job.Passthrough
name: passthrough
schedule: "@daily"
source: "{{ env.get('REKEP_SOURCE_URL', '') }}"
consumes: [rekep.models.Log]
produces: [rekep.models.Log]
```

```python
from rekep.job import load, load_all

job = load("stacks/jobs/passthrough.yaml")
jobs = load_all()          # every side file, name-sorted
```

## Namespace and identity

`namespace` and `name` give a job its OpenLineage identity;
`qualified_name()` joins them through `Namespace`, the same recursive
path-builder a `Dataset`'s location uses. `uri()` goes one step further —
`rekep.namespace.unique_uri` scopes it to the `job://` scheme, so a job and a
`Dataset` sharing a namespace and a name never collide:

```python
from rekep.job import Job

Job(name="task", namespace="dag").qualified_name()   # "dag.task"
Job(name="task", namespace="dag").uri()               # "job://dag/task"
```

## `@arrow_task`: a function as a lineage-tracked job

For a one-off transform, `@arrow_task` skips the `@record class ... (Job)`
declaration — it binds a plain batches-in/batches-out function as a `Job`'s
`arrow_transform`, and calling the result runs it through `run_tracked()`:
extract → transform → load, wrapped in a `Run` that opens `START` before and
closes `COMPLETE`/`FAIL` after, kept on the job itself (`job.events()`):

```python
from rekep.job import arrow_task
from rekep.models import Log

@arrow_task(name="errors_only", consumes=[Log], produces=[Log])
def errors_only(batches):
    for batch in batches:
        yield batch.filter(...)

errors_only()               # runs it, START/COMPLETE tracked
errors_only.events()        # the RunEvents this call just produced
```

`config=` takes an already-built `Job` (typically loaded from a side file)
and binds the function onto it instead of building a fresh one.

## Airflow

An Airflow DAG folder needs one line:

```python
from rekep.airflow.jobs import dags

globals().update(dags())
```

Each side file becomes one DAG with one task, whose tags, docs, inlets and
outlets derive from the `consumes`/`produces` record lists — the lineage
graph writes itself. The `rekep.airflow.dag` / `task` decorators accept the
same `consumes=` / `produces=` for hand-written DAGs.
