# Airflow

The canonical [deployment and operations guide](../../docs/pipeline/operations/airflow.md) covers
installation, configuration, local validation, scheduling, historical
backfills, Iceberg branches, distributed deployment, monitoring, retries, and
troubleshooting.

`market_pipeline.py` declares both DAGs, `rekep_market_pipeline` and
`rekep_iceberg_maintenance`. `marimo_operator.py` declares
`MarimoOperator(BaseOperator)`, the one operator every task runs under. Airflow
puts this directory on `sys.path`, which is what makes
`from marimo_operator import MarimoOperator` resolve, so deploy both files
together:

```bash
export AIRFLOW__CORE__DAGS_FOLDER=/path/to/rekep/tasks/airflow
```

The DAG names its own checkout as `Path(__file__).resolve().parents[2]` and
hands it to every operator as `repository`, a template field for a deployment
that keeps the DAG outside the checkout.

The scheduler and DAG processor need `apache-airflow>=3.0,<4`,
`apache-airflow-providers-standard>=1.0,<2` and `rekep[yaml]`. Every task
worker also needs `uv` and the `runner` environment, made once from the lock:

```bash
uv sync --project /path/to/rekep/python --locked --group runner
```

A task is then one command, run with the repository as its working directory:

```text
uv run --project <repository>/python --group runner --no-sync --offline --no-progress --no-env-file -- \
  rekep task run <repository>/tasks/<name>/<name>.yml \
  --parameters-file <attempt>/parameters.json --result-file <attempt>/result.json
```

`--no-sync --offline` means a scheduled task resolves no dependency, reaches no
index, and writes to no environment. `<attempt>` is a 0700 directory per task
attempt, deleted whether the task landed or raised.

Validate one interval:

```bash
airflow dags test \
  --dagfile-path /path/to/rekep/tasks/airflow/market_pipeline.py \
  --conf '{"branch":"root","books":true}' \
  rekep_market_pipeline \
  2026-08-21T10:00:00Z
```

Before running, point `tasks/parse_messages/parse_messages.yml` at an existing
capture. The default `data/capture` directory is not included. The active
SQLite catalog and local warehouse are suitable only for a single-host test.
`root`, `main`, and `master` select the same Iceberg root ref; every other
branch must already exist on every relevant table.

```text
parse_messages -> route_messages -> parse_fix -> route_fix
                                                +-> parse_instruments
                                                `-> parse_market -> route_market
                                                                    +-> flatten_orders
                                                                    `-> flatten_executions
```

Every task returns the same result mapping, which the operator pushes to XCom
under `return_value`. Each route reads one named count out of it —
`parse_messages.read`, `parse_fix.routed.market`, `parse_market.flatten.orders`
and `parse_market.flatten.executions` — and skips the consumers whose count is
zero. Routes read attempted counts rather than new writes, so retries and
intentional replays still reach their consumers. Empty intervals are skipped.
In direct mode `parse_market` writes orders and executions itself and returns
zero `flatten` counts, so both flatteners are skipped. In book mode each
flattener is routed from its own nested row count; an interval with only orders
does not run the execution flattener, or vice versa.

Set the DAG's boolean `books` parameter to `false` for direct mode without
editing the deployed YAML. A DAG Param replaces a same-name parameter the task
document already declares and keeps its native type, so `books` arrives as the
boolean.

Five tasks declare one Airflow `Asset` outlet for the table they write on every
run — `logs.messages`, `fix.market`, `market.instruments`, `market.orders` and
`market.executions` — and the operator attaches that run's `task`, `read`,
`written` and `skipped` counts to the asset event. Nothing is scheduled on
them.

`rekep_iceberg_maintenance` runs daily at 02:30 UTC with catch-up disabled. It
runs `tasks/optimize_iceberg/optimize_iceberg.yml`, keeping at least 24
snapshots and seven days of history before sweeping files that have been
unreachable for three days. Review that policy before unpausing the DAG.

`rekep_market_pipeline` is hourly, starts at 2026-01-01 UTC, and has catch-up
enabled. Review the intended history before unpausing it; otherwise Airflow
creates every completed hourly interval since that date.
