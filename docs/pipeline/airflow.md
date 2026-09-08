# Airflow deployment

`tasks/airflow/pipeline.py` declares the ready-to-run `rekep_ingestion` DAG:

```text
parse_messages -> parse_fix
     |                |
     v                v
logs.messages     fix.messages
```

The DAG exposes the union of both adjacent task documents as Params. A manual
run can therefore replace `filesystem`, `catalog`, `registry`, `branch`,
`version`, or `dedup` without creating another DAG.

## How a task runs

Each node is `MarimoOperator`. It starts the standalone child runner from the
same checkout and locked environment:

```text
uv run --project <repo>/python --group runner --no-sync --offline \
  --no-progress --no-env-file -- python \
  <repo>/tasks/airflow/marimo_runner.py <task.json> \
  --parameters-file <attempt>/parameters.json \
  --result-file <attempt>/result.json
```

`--no-sync --offline` makes a scheduled run deterministic: deployment must
resolve and install dependencies before the DAG is enabled. The child runs the
same Marimo application as `rekep task run`, validates its Stage result, and
publishes JSON atomically. `on_kill` terminates the child process group.

## Install a worker checkout

```bash
export REKEP_ROOT=/opt/rekep
git clone https://github.com/Platob/yggfin.git "$REKEP_ROOT"
uv sync --project "$REKEP_ROOT/python" --locked --group runner --group airflow
```

The `uv` executable must be on the scheduler and worker PATH. Every worker must
see the checkout at the same absolute path, or use a deployment mechanism that
sets the operator's `repository` to its local checkout.

Point Airflow at the DAG folder:

```bash
export AIRFLOW_HOME=/var/lib/airflow
export AIRFLOW__CORE__DAGS_FOLDER="$REKEP_ROOT/tasks/airflow"
uv run --project "$REKEP_ROOT/python" --group airflow airflow db migrate
uv run --project "$REKEP_ROOT/python" --group airflow airflow dags list
```

For a local evaluation, `airflow standalone` starts all components:

```bash
uv run --project "$REKEP_ROOT/python" --group airflow airflow standalone
```

## Local-files trigger

First deploy the tables, then trigger with an absolute source path visible to
the worker:

```bash
uv run --project "$REKEP_ROOT/python" rekep iceberg deploy \
  "$REKEP_ROOT/tasks/parse_messages/parse_messages.json"

uv run --project "$REKEP_ROOT/python" --group airflow airflow dags trigger \
  rekep_ingestion \
  --conf '{"filesystem":"file:///srv/capture/2026-08-14"}'
```

The default SQLite catalog is suitable only when scheduler and task execution
share one durable host filesystem.

## S3 capture with SQL catalog

Deploy the warehouse as described in [S3 deployment](operations/deploy.md#s3-with-a-sql-catalog),
then pass both source and catalog Params:

```bash
uv run --project "$REKEP_ROOT/python" --group airflow airflow dags trigger \
  rekep_ingestion \
  --conf '{
    "filesystem":"s3://market-capture/ulbridge/2026/08/14?region=eu-west-1",
    "catalog":{
      "name":"rekep",
      "properties":{
        "type":"sql",
        "uri":"sqlite:////var/lib/rekep/catalog.db",
        "warehouse":"s3://market-warehouse/rekep",
        "s3.region":"eu-west-1"
      }
    }
  }'
```

Use a shared SQL service instead of SQLite for distributed workers.

## AWS Glue and S3

Give scheduler/workers an IAM role and set their region. Deploy both tables
once with the same role and settings:

```bash
export AWS_REGION=eu-west-1
uv run --project "$REKEP_ROOT/python" rekep iceberg deploy \
  --catalog rekep \
  --property type=glue \
  --property warehouse=s3://market-warehouse/rekep \
  --property glue.region=eu-west-1 \
  --property s3.region=eu-west-1
```

Trigger:

```bash
uv run --project "$REKEP_ROOT/python" --group airflow airflow dags trigger \
  rekep_ingestion \
  --conf '{
    "filesystem":"s3://market-capture/ulbridge/2026/08/14?region=eu-west-1",
    "catalog":{
      "name":"rekep",
      "properties":{
        "type":"glue",
        "warehouse":"s3://market-warehouse/rekep",
        "glue.region":"eu-west-1",
        "s3.region":"eu-west-1"
      }
    }
  }'
```

Do not pass AWS keys in `--conf`. Use an EC2 instance profile, ECS task role,
EKS web identity, or the worker's standard AWS credential chain.

## Production checklist

1. Pin and deploy one repository revision to every worker.
2. Run `uv sync --locked` while network access is allowed.
3. Deploy both tables and rerun deploy to see `present`.
4. Confirm the worker can list/read capture objects and read/write the
   warehouse prefix.
5. Trigger one immutable capture manually and compare stage counts.
6. Replay it and require zero writes and zero new snapshots.
7. Inspect `nounmappedfixentries` before enabling a recurring schedule.
8. Keep `max_active_runs=1` unless catalog and source-window ownership are
   designed for concurrent commits.
