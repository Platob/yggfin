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
`rekep.namespace.ResourceUri` scopes it to the `job:` scheme, so a job and a
`Dataset` sharing a namespace and a name never collide:

```python
from rekep.job import Job

Job(name="task", namespace="dag").qualified_name()   # "dag.task"
Job(name="task", namespace="dag").uri()               # "job:/dag/task"
```

## Configuration: source, Airflow, environment

Beyond lineage, a `Job` carries what a real deployment needs, all of it
Jinja-capable since the whole side file renders before parsing:

```yaml
repo_url: https://github.com/Platob/yggfin
script_path: python/src/rekep/jobs/files_to_logs.py
env:
  LOG_LEVEL: INFO
properties:
  team: trading-platform
airflow:
  dag:
    max_active_runs: 1
  task:
    pool: default_pool
    retries: 2
```

- **`repo_url`/`script_path`** feed `source_code_location_facet()` -- an
  OpenLineage `SourceCodeLocationJobFacet` with `version`/`branch` read
  fresh from `rekep.render.git_context()` each call, not baked in at deploy
  time. `facets()` includes it automatically once either is set.
- **`env`/`properties`** are plain `dict[str, str]`: environment variables
  and whatever else a deployment needs to carry that is neither lineage nor
  Airflow config.
- **`airflow["dag"]`/`airflow["task"]`** merge straight into `into_airflow`'s
  `DAG(...)`/`@task(...)` calls -- any real Airflow kwarg (`pool`,
  `retries`, `trigger_rule`, `max_active_runs`, ...), since rekep does not
  maintain its own list of which belongs where; Airflow does.

## `@arrow_task`: a function as a lineage-tracked job

For a one-off transform, `@arrow_task` skips the `@record class ... (Job)`
declaration — it binds a plain batches-in/batches-out function as a `Job`'s
`arrow_transform`, and calling the result runs it through `run_tracked()`:
extract → transform → load, wrapped in a run that opens `START` before and
closes `COMPLETE`/`FAIL` after — **when a lineage client is bound**, and as
plain `run()` when none is:

```python
from rekep.job import arrow_task
from rekep.lineage import Collector
from rekep.models import Log

@arrow_task(name="errors_only", consumes=[Log], produces=[Log])
def errors_only(batches):
    for batch in batches:
        yield batch.filter(...)

errors_only()                              # just runs; nothing is tracked

collector = Collector()
errors_only.with_lineage(collector)()      # runs, START/COMPLETE emitted
collector.events                           # what this call produced
```

Inputs and outputs come from `consumes`/`produces`, resolved *before* the run
starts — a bad dotted path is a configuration error, not a run that began and
then died. See [Datasets](datasets.md#lineage-opt-in-or-pay-nothing) for the
same boundary around a dataset's own I/O.

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
graph writes itself, and `airflow["dag"]`/`airflow["task"]` (above) carry
whatever else the DAG or task needs. The `rekep.airflow.dag` / `task`
decorators accept the same `consumes=` / `produces=` for hand-written DAGs.

## The shipped pipeline: `files_to_logs` → `logs_to_records`

`rekep.jobs` (a package, mirroring `models/`) holds the concrete jobs this
package ships, one module each — `job.py` is the machinery, `jobs/` the
jobs built on it, declared under `stacks/jobs/`:

- **`FilesToLogs`** parses raw log files at `source` into `Log` records --
  `arrow_transform` is the identity, since `extract` already does the
  parsing. `stacks/jobs/files_to_logs.yaml` keeps a stable namespace across
  branches: there is one canonical ingestion job.
- **`LogsToRecords`** structures `Log.message` into `ParsedMessage`:
  `|`-delimited `key=value` pairs, a leading `#` stripped from the key
  (`rekep.jobs.parse_fields`), and the `8=` tag (FIX's BeginString) pulled
  out as `protocol` when the message opens with one. Not FIX-specific --
  any pipe-separated `key=value` run decodes the same way, FIX is just the
  common case. `stacks/jobs/logs_to_records.yaml` picks up
  `{{ git_branch_suffix }}` in its namespace and name: each branch iterates
  in its own working copy, unlike `files_to_logs`'s stable one -- the same
  Jinja + git-context machinery every side file has, just used differently
  per asset. `stacks/datasets/parsed_messages.yaml` makes the same choice
  one layer down, at storage: an Iceberg branch instead of a namespace (see
  the [Datasets guide](datasets.md)).

```python
from rekep.jobs import FilesToLogs, LogsToRecords

f2l = FilesToLogs(name="f2l", source="app.txt")
logs = f2l.arrow_transform(f2l.extract())

l2r = LogsToRecords(name="l2r")
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
| `stacks/jobs/files_to_logs.yaml` | stable | one canonical ingestion job |
| `stacks/datasets/log.yaml` | stable | the shared raw table |
| `stacks/jobs/logs_to_records.yaml` | `{{ git_branch_suffix }}` in name and namespace | the parser under development |
| `stacks/datasets/parsed_messages.yaml` | an Iceberg `branch` | its output, isolated per branch |

The last two are the working assets, and they make the choice one layer
apart: the job gets its own namespace, the dataset gets its own Iceberg
branch of the *same* table (see the [Datasets guide](datasets.md#branches-write-audit-publish)).
