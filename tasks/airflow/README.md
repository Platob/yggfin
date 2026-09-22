# Airflow

`pipeline.py` declares the daily `rekep_ingestion` DAG:

```text
parse_messages -> parse_fix_raw -> parse_fix_refined
      |                 |                  |
      v                 v                  v
logs.messages        fix.raw          fix.refined
```

One Params mapping covers the three task documents, so a name two of them
share -- `start`, `end`, `catalog`, `registry` -- means one thing on every
node, and the table each stage reads is named for what it reads, `messages`
and `raw`, so a run's conf cannot hand one stage the other's source.

`products.py` declares `rekep_products`, which is one node, `build_dbt`:

```text
fix.refined -> build_dbt -> orders.events, orders.current, executions.fills
```

Its schedule is the `fix.refined` Asset the first DAG publishes last, so a build
starts when `parse_fix_refined` writes and neither DAG names the other's tasks.
dbt is in the `runner` group, so the same locked environment runs it.

Each node is a `MarimoOperator`. The operator enters the repository's locked
`runner` environment offline and starts `marimo_runner.py` directly; it never
calls `rekep task run`:

```text
uv run --project <repository>/python --group runner --no-sync --offline \
  --no-progress --no-env-file -- python \
  <repository>/tasks/airflow/marimo_runner.py <task.json> \
  --parameters-file <attempt>/parameters.json --result-file <attempt>/result.json
```

Install that locked environment and Airflow on the worker before enabling the
DAG. `--no-sync --offline` prevents a scheduled run from resolving or changing
dependencies.

The operator takes:

| argument | required | what it does |
| --- | :---: | --- |
| `document` | yes | task JSON, relative to `repository`; one outside it is refused |
| `repository` | yes | checkout root holding `python/` and `tasks/`, and the child's working directory |
| `parameters` | no | per-task overrides; a name the document does not declare fails the task |
| `environment` | no | variables for the child, over the worker's own |
| `cache_dir` | no | sets `UV_CACHE_DIR`, pointing uv at the worker's shared cache |
| `outlets` | no | the Assets this task publishes |

`environment` is how one task gets a credential-bearing variable without it
passing through Params or task JSON. The counts of a finished run are attached
to each outlet's asset event, and the whole result is returned into XCom.

The runner loads the adjacent JSON task document, replaces the Marimo
`parameters` cell, runs the application, validates its small result mapping,
and publishes that JSON atomically. Parameters and results live in a private
directory unique to one Airflow attempt and are removed on success or failure.
`on_kill` terminates the runner's process group.

The ingestion DAG runs daily, and each run covers its data interval: the
operator hands it to all three tasks as `start` and `end`, so a day's run reads
the day's lines under `filesystem` and replaces them in all three tables. A
manual trigger covers the last complete day unless its conf names `start` and
`end`, which win over the interval. Configure
`tasks/parse_messages/parse_messages.json`,
`tasks/parse_fix_raw/parse_fix_raw.json` and
`tasks/parse_fix_refined/parse_fix_refined.json`, then let it run or trigger
it. The local SQLite catalog is for one-host smoke
tests; production catalog and S3 settings belong in those task documents.

`build_dbt` takes its catalog from `tasks/build_dbt/build_dbt.json`, where
`null` means the dbt profile's own. dbt writes `target/` and `logs/` under
`data/dbt` unless `DBT_TARGET_PATH` and `DBT_LOG_PATH` say otherwise -- and a
model is staged under the target path, so setting those two through
`environment` is what a worker with a read-only checkout needs.
