# Dags

A `Dag` is the graph a set of [tasks](jobs.md) form: which ones belong
together, what has to finish before what, and when the whole thing runs.

**It is rekep's own implementation, not a description of someone else's.**
The graph is resolved and validated here, ordered here, and executed here.
Airflow is one *projection* of it — the same relationship a record has with
Iceberg — so a dag can be declared, loaded, ordered, shown and run with no
orchestrator installed at all.

## Declaring

```yaml
# stacks/dags/trading_logs.yaml
uri: rekep:/dags/pipeline/trading_logs
description: Parse raw trading logs, then structure their messages.
schedule: "@daily"
tags:
  domain: pipeline
  owner: data-eng
tasks:
  - rekep:/jobs/pipeline/files_to_logs
  - rekep:/jobs/pipeline/logs_to_records
dependencies:
  logs_to_records:
    - files_to_logs
airflow:
  dag:
    max_active_runs: 1
```

- **`uri`** is the identity, the same path spelling everything else uses:
  `rekep:/dags/pipeline/trading_logs` and `rekep:/jobs/pipeline/trading_logs`
  are two resources, not one name used twice — the service is what tells them
  apart, and it is a path part like any other.
- **`tasks`** are **references**, by task URI, never copies. The task's own
  side file under `stacks/jobs/` stays its only declaration; a graph that
  restated a task's config would be a second place for it to disagree with
  itself. Declaration order is the tie-break the ordering falls back on.
- **`dependencies`** reads downstream-first, because that is the question
  being asked ("what does this one need?"), and names tasks by **id** — the
  last level of the URI — because that is the name a reader of the graph
  already has in front of them. A task named nowhere here starts immediately.
- **`tags`** is a mapping, like a task's.
- **`airflow.dag` / `airflow.task`** pass straight through to Airflow; a
  task's own `airflow["task"]` wins over the dag's default.

A `dag:` key naming a subclass is optional — a graph of references has
nothing to subclass for, so `Dag` itself is the default rather than a
required ceremony.

## The graph

```python
from rekep.dag import find, load, load_all

dag = find("rekep:/dags/pipeline/trading_logs")

dag.tasks_by_id()    # {"files_to_logs": Job(...), "logs_to_records": Job(...)}
dag.upstreams()      # {"files_to_logs": [], "logs_to_records": ["files_to_logs"]}
dag.downstreams()    # the same graph read the other way
dag.order()          # ["files_to_logs", "logs_to_records"]
dag.roots()          # where a run starts
dag.leaves()         # where it ends
dag.task_name("logs_to_records")   # "trading_logs.logs_to_records"
```

`order()` is Kahn's algorithm with **declaration order as the tie-break**:
among the tasks that are ready, the one written first goes first, so the same
dag always produces the same sequence. An order that depended on set
iteration would make a failure reproduce only sometimes, which is the worst
kind of failure to debug.

Three things are refused by name rather than half-done:

| Refused | Why |
| --- | --- |
| two tasks with the same id | `dependencies` names a task by its id, so a duplicate makes the graph ambiguous |
| an edge naming an undeclared task | caught at load, where the file can be fixed, instead of silently ordering nothing |
| a cycle | there is no ready task and no way to pick one; the error names exactly what is still waiting |

## Running

```python
dag.run()      # {"files_to_logs": 24, "logs_to_records": 24}
```

Sequential and in-process: the executor a laptop, a test and a cron line
want, not a replacement for a scheduler. A failing task stops the run — the
tasks after it declared they needed it — and the exception propagates, since
a dag that swallowed one would report success for a pipeline that did not
happen.

From the CLI:

```console
$ rekep dag list
rekep:/dags/passthrough  schedule=@daily  passthrough
rekep:/dags/pipeline/trading_logs  schedule=@daily  files_to_logs -> logs_to_records

$ rekep dag show --uri rekep:/dags/pipeline/trading_logs
rekep:/dags/pipeline/trading_logs  schedule=@daily
  trading_logs.files_to_logs  uri=rekep:/jobs/pipeline/files_to_logs  after=-
  trading_logs.logs_to_records  uri=rekep:/jobs/pipeline/logs_to_records  after=files_to_logs

$ rekep dag run --uri rekep:/dags/pipeline/trading_logs
trading_logs.files_to_logs: 24
trading_logs.logs_to_records: 24
```

## One task, one dag

A pipeline that is genuinely one step still needs a dag to be scheduled, and
should not have to write a second file to say so:

```python
from rekep.dag import Dag

Dag.from_job(job)     # rekep:/dags/<the job's own path>, one task, no edges
```

Schedule, description and tags come from the task, since with one task there
is nobody else for them to belong to.

## Airflow

`into_airflow()` builds a real Airflow DAG: one task per job, edges wired
from `dependencies`, and **nothing of Airflow's authoring API wrapped** — no
`@dag` of ours, no `DAG` subclass. Anything Airflow accepts reaches it
untouched through `airflow["dag"]`/`airflow["task"]`, because rekep keeps no
list of which kwarg belongs to which; Airflow has one already.

```python
from rekep.airflow.dags import dags

globals().update(dags())     # an Airflow DAG folder needs one line
```

What *is* derived is the part Airflow cannot derive: tags and a
Consumes/Produces table for the dag, inlets and outlets for each task, all
from the records those tasks declare. The lineage graph writes itself.

`rekep airflow deploy --dags-folder <path>` converges one generated module
per side file into an Airflow dags folder — the side file stays the single
source, loaded and built fresh at parse time:

```console
$ rekep airflow deploy --config stacks/dags --dags-folder /opt/airflow/dags
converged dags: passthrough.py, trading_logs.py
```
