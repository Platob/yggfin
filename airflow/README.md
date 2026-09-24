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

Each node runs the bundled task of its own name. The task's shipped
`python/src/rekep/tasks/<name>.json` owns its defaults, and the DAG reads those
files without importing `rekep`. A shared Params mapping names sources by role
(`messages`, `raw`, `refined`, `books`) and supplies the catalog, window and
codec settings. The book task's target is `market.books`.

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
processing. An Asset trigger does not inherit the ingestion run's conf, so give
`build_dbt` the ingestion catalog as `REKEP_DBT_CATALOG` in the worker's
environment.

`dispatch.py` builds every node and decides where it runs. With
`REKEP_EKS_CONFIG` unset, each node is a `RekepOperator` (`rekep_operator.py`),
which runs `rekep tasks <task_name> run` on the worker, in the repository's
locked environment:

```text
uv run --project <repository>/python --group runner --no-sync --offline \
  --no-progress --no-env-file -- rekep tasks <task_name> run \
  --parameters-file <attempt>/parameters.json --result-file <attempt>/result.json
```

Install the locked environment, including Yggdryl 0.1.11, before enabling the
DAG. `--no-sync --offline` prevents dependency resolution during a scheduled
run. The child works from the repository root.

| argument | required | meaning |
| --- | :---: | --- |
| `task_name` | yes | bundled task, as `rekep tasks list` names it |
| `repository` | yes | checkout containing `python/pyproject.toml` |
| `parameters` | no | overrides for declared task parameters |
| `environment` | no | child environment, including credential-bearing variables |
| `cache_dir` | no | worker's shared `UV_CACHE_DIR` |
| `outlets` | no | Assets published after success |
| `upstream_task_id` | no | Stage result that pins the child's window, snapshot and source |

Parameters resolve from shipped defaults, operator overrides, DAG Params,
then the run interval, except explicit conf bounds win. A configured upstream
handoff applies last. Daily runs use their data interval; a manual trigger
covers the last complete day unless conf supplies its bounds. An unknown
`task_name` or an undeclared parameter fails before a process starts.

Results contain counts, locations, window and snapshot IDs, never data rows.
The CLI publishes validated JSON atomically; the operator attaches the result
to outlet events and XCom. Attempt files live in a private directory and are
removed on success or failure. `on_kill` terminates the process group.

With `REKEP_EKS_CONFIG` naming a JSON document of `EksPodOperator` keywords,
each node is an `EksRekepOperator` (`eks_rekep_operator.py`), which runs the
same task in a pod on an EKS cluster, from the image the repository's
`Dockerfile` builds:

```json
{
  "cluster_name": "market-data",
  "image": "123456789012.dkr.ecr.eu-west-1.amazonaws.com/rekep:abc1234",
  "namespace": "rekep",
  "service_account_name": "rekep",
  "region": "eu-west-1",
  "tasks": {"build_dbt": {"container_resources": {"requests": {"memory": "16Gi"}}}}
}
```

Keywords at the top apply to every task; `tasks.<name>` replaces them whole
for one. The operator resolves parameters on the worker exactly as
`RekepOperator` does, hands the pod `rekep tasks <name> run` with each
parameter as `--parameter NAME=<json>` and `--result-file
/airflow/xcom/return.json`, and validates the result the XCom sidecar hands
back. Unknown tasks, undeclared parameters and a malformed result fail as
they do on the worker, and before any pod exists when they can.

Use the [Airflow guide](../docs/pipeline/airflow.md) for worker setup,
catalog deployment and [EKS dispatch](../docs/pipeline/airflow.md#dispatch-on-eks). SQLite is appropriate for one-host smoke runs; production
settings belong in DAG Params or worker configuration. Optional dbt writes
under `data/dbt` unless `DBT_TARGET_PATH` and `DBT_LOG_PATH` override it.
