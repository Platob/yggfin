# Airflow

`pipeline.py` declares the manually triggered `rekep_ingestion` DAG:

```text
parse_messages -> parse_fix
     |                |
     v                v
logs.messages     fix.messages
```

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
dependencies; `cache_dir` may point uv at the worker's shared cache.

The runner loads the adjacent JSON task document, replaces the Marimo
`parameters` cell, runs the application, validates its small result mapping,
and publishes that JSON atomically. Parameters and results live in a private
directory unique to one Airflow attempt and are removed on success or failure.
`on_kill` terminates the runner's process group.

The DAG is intentionally unscheduled: its `filesystem` must name an immutable
capture or prefix for each run. Configure `tasks/parse_messages/parse_messages.json`
and `tasks/parse_fix/parse_fix.json`, then trigger the DAG. The local SQLite
catalog is for one-host smoke tests; production catalog and S3 settings belong
in those task documents.
