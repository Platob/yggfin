# Jobs (tasks)

A `Job` is OpenLineage's resource for a process that consumes and produces
datasets — what it reads, what it writes, and how it transforms — declared as
a record, transformed in Arrow.

**A job is one task.** OpenLineage names a job hierarchically, `dag_id` then
`task_id`, and that is exactly what a `Job` is here: one node of a graph. The
graph itself is a [`Dag`](dags.md).

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

`arrow_transform` is the one method a real task overrides: batches in, batches
out, nothing materialised. It is not enforced abstract — a bare `Job` still
declares and describes its lineage — but calling `run()` without overriding
it raises, naming the class. `run()` chains
`extract → arrow_transform → load`; the default `extract` parses the log at
`self.source`, the default `load` drains and counts, and each stage overrides
independently.

## Side files

Deployment configuration lives in one file per task under `stacks/jobs`,
schema'd by the `Job` record itself and rendered with Jinja before parsing:

```yaml
# stacks/jobs/passthrough.yaml
job: passthrough                    # the class, by name -- never an import path
uri: rekep:///jobs/passthrough      # this task's identity
schedule: "@daily"
source: "{{ env.get('REKEP_SOURCE_URL', '') }}"
consumes: [rekep:///records/log]    # the records it reads, by URI
produces: [rekep:///records/log]
```

Nothing here points at a module. `job:` names a **declared class** — writing
`@record class Passthrough(Job)` is the declaration, and `rekep.classes` is
where a name is looked back up — and `consumes`/`produces` name **records by
URI**, the same way a dataset does. A file move renames nothing, and the
identity in a config stops being an import statement in disguise. A
deployment's own modules are declared to rekep once, in `$REKEP_MODULES`,
rather than in every reference.

```python
from rekep.job import find, load, load_all

job = load("stacks/jobs/passthrough.yaml")   # one file, whatever it declares
jobs = load_all()                            # every side file, name-sorted
job = find("rekep:///jobs/passthrough")         # one identity, from the registry
```

`load` takes a path and builds whatever it declares; `find` takes an identity
and returns the object that already answers to it, reading the folder only if
nobody does.

## Identity is one URI

A task is named by a `uri`, the same path spelling a dataset and a dag use —
one identity rather than a name beside a namespace, because a resource that
can spell itself two ways eventually spells itself two different ways.
`rekep.namespace.ResourceUri` scopes it to the `jobs` service, so a task and a
`Dataset` sharing a namespace and a name never collide:

```python
from rekep.job import Job

job = Job(uri="rekep:///jobs/dag/task")
job.task_id()          # "task"       -- what Airflow calls a task_id
job.task_namespace()   # "dag"
job.task_name()        # "dag.task"   -- every level joined
str(job.resource_uri())  # "rekep:///jobs/dag/task"
```

`task_name()` is the whole hierarchy, joined the way OpenLineage names a job.
A dag qualifies it one step further — `Dag.task_name(job)` is
`<dag_id>.<task_id>` — because a task named inside a dag is *that dag's* task,
not a second job that happens to share a name.

## Configuration: source, tags, Airflow, environment

Beyond lineage, a `Job` carries what a real deployment needs, all of it
Jinja-capable since the whole side file renders before parsing:

```yaml
repo_url: https://github.com/Platob/yggfin
script_path: python/src/rekep/jobs/files_to_logs.py
tags:
  domain: pipeline
  stage: ingestion
env:
  LOG_LEVEL: INFO
properties:
  team: trading-platform
airflow:
  task:
    pool: default_pool
    retries: 2
```

- **`repo_url`/`script_path`** feed `source_code_location_facet()` -- an
  OpenLineage `SourceCodeLocationJobFacet` with `version`/`branch` read
  fresh from `rekep.render.git_context()` each call, not baked in at deploy
  time. `facets()` includes it automatically once either is set.
- **`tags`** is a **mapping**, not a list: the key names the dimension and
  the value answers it, so `stage: ingestion` says what makes it a tag while
  a bare `ingestion` never could — and two declarations of one key are one
  decision to resolve rather than two entries to carry. Airflow's own tags
  are opaque strings, so the mapping is flattened to `key=value` at that
  boundary (`rekep.airflow.lineage.airflow_tags`) and nowhere else.
- **`env`/`properties`** are plain `dict[str, str]`: environment variables
  and whatever else a deployment needs to carry that is neither lineage nor
  orchestrator config.
- **`airflow["dag"]`/`airflow["task"]`** merge straight into the Airflow
  `DAG(...)`/`@task(...)` calls `Dag.into_airflow()` makes -- any real
  Airflow kwarg (`pool`, `retries`, `trigger_rule`, `max_active_runs`, ...),
  since rekep does not maintain its own list of which belongs where; Airflow
  does.

## `@arrow_task`: a function as a task

For a one-off transform, `@arrow_task` skips the `@record class ... (Job)`
declaration — it binds a plain batches-in/batches-out function as a `Job`'s
`arrow_transform`, and calling the result runs `extract → transform → load`:

```python
from rekep.job import arrow_task
from rekep.models import Log

@arrow_task(uri="rekep:///jobs/trading/errors_only", consumes=[Log], produces=[Log])
def errors_only(batches):
    for batch in batches:
        yield batch.filter(...)

errors_only()          # runs it
```

Undeclared, the identity is `rekep:///jobs/<function name>` — a decorator that
made you name the thing twice would be a worse decorator. `config=` takes an
already-built `Job` (typically loaded from a side file) and binds the
function onto it instead of building a fresh one.

## Lineage: represented, never emitted

rekep says what a run *is* and stops there. `into_run_event(state)` builds
OpenLineage's own `RunEvent` for a task, inputs and outputs resolved from
`consumes`/`produces`:

```python
from rekep.run import RunState

start = job.into_run_event(RunState.START)
done = job.into_run_event(RunState.COMPLETE, start.run)   # one run, two moments
start.into_json()                                         # what leaves the process
```

There is **no client here and no transport**. A collector's job is to
collect; a client rekep does not ship is a client rekep cannot get wrong, and
nothing in a read or a write pays for tracking that may never be read. See
[Datasets](datasets.md) for the same split on a dataset's own I/O.

## Orchestration: a job is the task, a dag is the graph

One task alone is not a pipeline. `stacks/dags/` declares which tasks belong
together and in what order, and `Dag.into_airflow()` projects that onto
Airflow — see the [Dags guide](dags.md).

```python
from rekep.dag import Dag

Dag.from_job(job).into_airflow()   # the one-task pipeline, no side file needed
```

## The shipped pipeline: `files_to_logs` → `logs_to_records`

`rekep.jobs` (a package, mirroring `models/`) holds the concrete tasks this
package ships, one module each — `job.py` is the machinery, `jobs/` the
tasks built on it, declared under `stacks/jobs/` and wired together by
`stacks/dags/trading_logs.yaml`:

- **`FilesToLogs`** parses raw log files at `source` into `Log` records --
  `arrow_transform` is the identity, since `extract` already does the
  parsing. `stacks/jobs/files_to_logs.yaml` keeps a stable identity across
  branches: there is one canonical ingestion task.
- **`LogsToRecords`** structures `Log.message` into `ParsedMessage`:
  `|`-delimited `key=value` pairs, a leading `#` stripped from the key
  (`rekep.jobs.parse_fields`), and the `8=` tag (FIX's BeginString) pulled
  out as `protocol` when the message opens with one. Not FIX-specific --
  any pipe-separated `key=value` run decodes the same way, FIX is just the
  common case. `stacks/jobs/logs_to_records.yaml` picks up
  `{{ git_branch_suffix }}` in its `uri`: each branch iterates in its own
  working copy, unlike `files_to_logs`'s stable one -- the same Jinja + git
  context machinery every side file has, just used differently per asset.
  `stacks/datasets/parsed_messages.yaml` makes the same choice one layer
  down, at storage: an Iceberg branch instead of a namespace (see the
  [Datasets guide](datasets.md)).

```python
from rekep.jobs import FilesToLogs, LogsToRecords

f2l = FilesToLogs(uri="rekep:///jobs/f2l", source="app.txt")
logs = f2l.arrow_transform(f2l.extract())

l2r = LogsToRecords(uri="rekep:///jobs/l2r")
records = l2r.arrow_transform(logs)   # ParsedMessage-shaped batches
```

## Branch-conditional naming

Every side file already renders through `rekep.render.render` before it is
parsed, and `git_context()`'s `git_branch_suffix`/`git_branch_slug`/
`git_branch_prefix` are always in scope — Jinja or not. So **whether an
asset follows the git branch is a per-file choice, not a mode**: nothing has
to be turned on, and nothing has to be turned off for the files that should
stay put.

| File | Choice | Why |
| --- | --- | --- |
| `stacks/jobs/files_to_logs.yaml` | stable | one canonical ingestion task |
| `stacks/datasets/log.yaml` | stable | the shared raw table |
| `stacks/jobs/logs_to_records.yaml` | `{{ git_branch_suffix }}` in the `uri` | the parser under development |
| `stacks/dags/trading_logs.yaml` | `{{ git_branch_suffix }}` in the `uri` | the graph that runs it |
| `stacks/datasets/parsed_messages.yaml` | an Iceberg `branch` | its output, isolated per branch |

The working assets make the choice one layer apart: the task gets its own
identity, the dataset gets its own Iceberg branch of the *same* table (see
the [Datasets guide](datasets.md#branches-write-audit-publish)).
