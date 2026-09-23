# Airflow

`pipeline.py` declares the daily `rekep_ingestion` DAG:

```text
parse_messages -> parse_fix_raw -> parse_fix_refined -> parse_books
                                                           |
                                                           v
                                                      market.books
                                                           |
                              +----------------------------+---------------------+
                              v                            v                     v
                         parse_orders                 parse_quotes        parse_executions
                              |                            |                     |
                              v                            v                     v
                         market.orders                market.quotes        market.executions
```

Seven adjacent task documents own defaults. A shared Params mapping names
sources by role (`messages`, `raw`, `refined`, `books`) and supplies the
catalog, window and codec settings. The book task's target is `market.books`.

The three event tasks run independently after books commit. Each declares
`upstream_task_id="parse_books"`. The operator validates that parent's Stage
result from XCom, then pins the actual `start`, `end`, `snapshot_id` and
matching source-table names after ordinary parameter resolution. Generic
Params cannot redirect a child to another book snapshot or window. A missing
or malformed result fails before launching a subprocess. Zero means an empty
source without a committed head and never follows a later head.

`products.py` retains the optional `rekep_products` DAG:

```text
fix.refined -> build_dbt -> orders.events, orders.current, executions.fills
```

Its schedule is the `fix.refined` Asset; it can start independently of market
processing. Configure its own catalog to match ingestion because an Asset
trigger does not inherit the ingestion run's conf.

Each node is a `MarimoOperator`. It starts the standalone runner in the
repository's locked environment, without calling the CLI:

```text
uv run --project <repository>/python --group runner --no-sync --offline \
  --no-progress --no-env-file -- python \
  <repository>/tasks/airflow/marimo_runner.py <task.json> \
  --parameters-file <attempt>/parameters.json --result-file <attempt>/result.json
```

Install the locked environment, including Yggdryl 0.1.11, before enabling the
DAG. `--no-sync --offline` prevents dependency
resolution during a scheduled run. The child works from the repository root.

| argument | required | meaning |
| --- | :---: | --- |
| `document` | yes | task JSON beneath `repository` |
| `repository` | yes | checkout containing `python/` and `tasks/` |
| `parameters` | no | overrides for declared task parameters |
| `environment` | no | child environment, including credential-bearing variables |
| `cache_dir` | no | worker's shared `UV_CACHE_DIR` |
| `outlets` | no | Assets published after success |
| `upstream_task_id` | no | Stage result that pins the child's window, snapshot and source |

Parameters resolve from document defaults, operator overrides, DAG Params,
then the run interval, except explicit conf bounds win. A configured upstream
handoff applies last. Daily runs use their data interval; a manual trigger
covers the last complete day unless conf supplies its bounds.

Results contain counts, locations, window and snapshot IDs, never data rows.
The runner publishes validated JSON atomically; the operator attaches the
result to outlet events and XCom. Attempt files live in a private directory
and are removed on success or failure. `on_kill` terminates the process group.

Use the [Airflow guide](../../docs/pipeline/airflow.md) for worker setup and
catalog deployment. SQLite is appropriate for one-host smoke runs; production
settings belong in task documents or worker configuration. Optional dbt writes
under `data/dbt` unless `DBT_TARGET_PATH` and `DBT_LOG_PATH` override it.
