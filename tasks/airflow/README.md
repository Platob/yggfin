# Airflow

The canonical [deployment and operations guide](../../docs/airflow.md) covers
installation, configuration, local validation, scheduling, historical
backfills, Iceberg branches, distributed deployment, monitoring, retries, and
troubleshooting.

The DAG needs Airflow 3.x and
`apache-airflow-providers-papermill>=3.13,<4`. Deploy the complete repository
revision because parsing loads the adjacent YAML files and workers execute the
notebooks.

```bash
export REKEP_ROOT=/path/to/rekep
export REKEP_NOTEBOOK_OUTPUT=/tmp/rekep-notebooks
mkdir -p "$REKEP_NOTEBOOK_OUTPUT"

airflow dags test \
  --dagfile-path "$REKEP_ROOT/tasks/airflow/market_pipeline.py" \
  --conf '{"branch":"root"}' \
  rekep_market_pipeline \
  2026-08-21T10:00:00Z
```

Before running, point `tasks/parse_messages/parse_messages.yml` at an existing
capture. The default `data/capture` directory is not included. The active
SQLite catalog and local warehouse are suitable only for a single-host test.

`REKEP_NOTEBOOK_OUTPUT` must exist and be visible to every task worker because
route tasks read the executed notebooks. `root`, `main`, and `master` select
the same Iceberg root ref; every other branch must already exist on every
relevant table.

```text
parse_messages -> route_messages -> parse_fix -> route_fix
                                                +-> flatten_instruments
                                                `-> parse_market -> route_market
                                                                    +-> flatten_orders
                                                                    `-> flatten_executions
```

Every notebook returns counts through Scrapbook. Routes use attempted/read
counts rather than new writes, so retries and intentional replays still reach
their consumers. Empty intervals are skipped. In direct mode `parse_market`
writes orders and executions itself and returns zero `flatten` counts, so both
flattening tasks are skipped at runtime. In book mode each flattener is routed
from its own nested row count; an interval with only orders does not run the
execution flattener, or vice versa.

The DAG is hourly, starts at 2026-01-01 UTC, and has catch-up enabled. Review
the intended history before unpausing it; otherwise Airflow creates every
completed hourly interval since that date.
