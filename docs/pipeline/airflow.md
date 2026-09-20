# Airflow deployment

`tasks/airflow/pipeline.py` declares the ready-to-run `rekep_ingestion` DAG:

```text
parse_messages -> parse_fix_bronze -> parse_fix_silver
     |                  |                   |
     v                  v                   v
logs.messages       fix.bronze          fix.silver
```

It runs daily, and each run covers its own data interval: the operator hands
the interval to all three tasks as their `start` and `end`, so a day's run
reads the day's lines under `filesystem` and replaces them in all three
tables. The DAG exposes the union of the three adjacent task documents as
Params. A manual run can therefore replace `filesystem`, `rowheader`, `start`,
`end`, `catalog`, `messages`, `registry` or `bronze` without creating another
DAG, and a bound the run's conf names wins over the interval. `messages` and
`bronze` are named for the table each FIX stage reads, so one Params mapping
over three documents cannot hand one stage the other's source. There is no
`version` Param: what a message was read at is what its own `beginstring`
said.

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
publishes JSON atomically. `on_kill` terminates the child process group. The
child's working directory is the checkout, so a relative `catalog` setting
resolves there and not in the scheduler's own directory.

### Operator arguments

| argument | required | what it does |
| --- | :---: | --- |
| `document` | yes | task JSON, relative to `repository`; one outside it is refused |
| `repository` | yes | checkout root holding `python/` and `tasks/`; also the child's working directory |
| `parameters` | no | per-task overrides; a name the task document does not declare fails the task |
| `environment` | no | variables for the child, over the worker's own |
| `cache_dir` | no | sets `UV_CACHE_DIR`, to point `uv` at a shared worker cache |
| `outlets` | no | the Assets this task publishes |

`document`, `repository`, `parameters` and `environment` are template fields.
`environment` is the supported way to give one task a credential-bearing
variable without putting it in Params or in task JSON.

Parameters merge in one order, later winning: task document defaults, then the
operator's `parameters`, then the DAG run's Params, then the data interval --
and only for a name the task document already declares, so a task that does
not take `start` is never handed the scheduler's. Two exceptions keep a
person's intent: an interval with no width, which Airflow infers for a manual
run of an unscheduled DAG, fills nothing, and a bound the run's own conf names
is left as named.

### Assets and what a run returns

Each node declares one `Asset` outlet named exactly for the table it writes,
`logs.messages`, `fix.bronze` and `fix.silver`, so a downstream DAG can be
scheduled on any of them. When a run finishes, the operator attaches `task`,
`read`, `written` and `skipped` to the event of every outlet its result names
as a target -- a table this run did not write claims nothing.

`execute` returns the validated Stage mapping, so the same counts land in XCom
under `return_value`. Its fields are listed in
[Logs and task results](operations/logs.md#result-schema); it is a summary, never rows.

## The products DAG

`tasks/airflow/products.py` declares `rekep_products`, which is one node,
[`build_dbt`](tasks/build-dbt.md):

```text
fix.silver -> build_dbt -> orders.events, orders.current, executions.fills
```

Its schedule is the `fix.silver` Asset the ingestion DAG publishes last, so a
build starts when `parse_fix_silver` writes and the two DAGs are one route
without either naming the other's tasks. A product reads the walked rows and
never `fix.bronze`, so no build waits on the parse alone. The node declares
one outlet per table the dbt models commit, so a third DAG can be scheduled on
a product the same way.

Its Params are the `build_dbt` document's own: `project`, `profiles`,
`target`, `select`, `catalog` and `log_level`. A worker runs dbt out of the
`runner` group, the same locked environment every other task runs in, and dbt
writes `target/` and `logs/` under the project directory unless
`DBT_TARGET_PATH` and `DBT_LOG_PATH` say otherwise -- which is what the
operator's `environment` argument is for.

### Retries

The DAG sets no `retries`, so every task is `retries=0` and one transient S3
or catalog error fails the run. Raising it is safe and is the recommended
configuration: each attempt writes into its own private directory keyed on the
try number, that directory is removed whether the attempt lands or raises, and
all three writers replace on their field-declared key -- `currhashcode` for
`logs.messages`, `curruuid` for `fix.bronze` and for `fix.silver` -- so a
retry re-reads the same window and lands the same rows over whatever the
failed attempt left.

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
cd "$REKEP_ROOT"
uv run --project "$REKEP_ROOT/python" rekep iceberg deploy \
  "$REKEP_ROOT/tasks/parse_messages/parse_messages.json"

uv run --project "$REKEP_ROOT/python" --group airflow airflow dags trigger \
  rekep_ingestion \
  --conf '{"filesystem":"file:///srv/capture/2026-08-14","start":"2026-08-14","end":"2026-08-14"}'
```

A manual trigger of the daily DAG covers the last complete day unless its
conf names the window, which is what the `start` and `end` above do for a
dated capture. The `cd` matters: the checked-in document's `uri` and `warehouse` are both
relative, and the operator runs its child with the checkout as the working
directory. Deploying from anywhere else creates a second catalog next to
wherever the command was typed, and the DAG then writes to an empty one. Pass
absolute settings instead if the deploy cannot run from the checkout:

```bash
uv run --project "$REKEP_ROOT/python" rekep iceberg deploy \
  --property type=sql \
  --property uri=sqlite:////var/lib/rekep/catalog.db \
  --property warehouse=/var/lib/rekep/warehouse
```

and name the same catalog in `--conf`, as the S3 example below does.

The default SQLite catalog is suitable only when scheduler and task execution
share one durable host filesystem.

## A run on this checkout

The check the integration suite makes, by hand: a private `AIRFLOW_HOME`, the
bundled fixture as `filesystem`, and the catalog of your choice as `CATALOG`.

```bash
cd "$REKEP_ROOT"
export AIRFLOW_HOME="$(mktemp -d)"
export AIRFLOW__CORE__DAGS_FOLDER="$REKEP_ROOT/tasks/airflow"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
CATALOG='{"name":"rekep","properties":{"type":"sql","uri":"sqlite:////var/lib/rekep/catalog.db","warehouse":"/var/lib/rekep/warehouse"}}'
uv run --project "$REKEP_ROOT/python" --group airflow airflow db migrate
uv run --project "$REKEP_ROOT/python" --group airflow airflow dags test \
  rekep_ingestion \
  --conf '{"filesystem":"file://'"$REKEP_ROOT"'/python/tests/data/ulbridge.log",
           "start":"2026-08-14","end":"2026-08-14","catalog":'"$CATALOG"'}'
uv run --project "$REKEP_ROOT/python" --group airflow airflow dags test \
  rekep_products --conf '{"catalog":'"$CATALOG"'}'
```

Airflow 3.3.1 printed these lines among its own:

```text
INFO rekep.logs parse_messages finished: 144 read, 141 written, 3 skipped → messages=logs.messages in 0.8s
INFO rekep.logs parse_fix_bronze finished: 141 read, 53 written, 26 skipped → bronze=fix.bronze in 1.3s
INFO rekep.logs parse_fix_silver finished: 53 read, 53 written, 0 skipped → silver=fix.silver in 1.4s
DagRun Finished: dag_id=rekep_ingestion, ... state=success
INFO rekep.logs build_dbt 29 nodes ran: 4 models, 25 tests, 0 warned
INFO rekep.logs build_dbt finished: 29 read, 66 written, 0 skipped → executions_fills=executions.fills, orders_events=orders.events, orders_current=orders.current in 3.2s
DagRun Finished: dag_id=rekep_products, ... state=success
```

`dags test` proves that the DAG parses under Airflow's own loading, that the
run's `--conf` reaches every node as its Params, and that each node ran the
locked runner into the catalog the conf named. It runs one DAG directly and
fires no Asset-triggered run, which is why the products DAG has its own command
above; a scheduler fires it on the `fix.silver` event. `airflow assets list`
then names the six Assets: `logs.messages`, `fix.bronze`, `fix.silver`,
`orders.events`, `orders.current` and `executions.fills`. The integration test
`test_a_real_dag_run_publishes_both_tables_from_its_conf` in
`python/tests/test_marimo_operator.py` runs the ingestion DAG this way.

### Under a scheduler

What `dags test` cannot show, a scheduler does: `airflow standalone` in a
private home of the same shape, both DAGs unpaused, and the local-files
trigger above issued with no `catalog` in its conf. The scheduler recorded a
manual run of `rekep_ingestion` that took 22 seconds and, before that run was
marked finished, a run of `rekep_products` it created itself off the event
`parse_fix_silver` had just published:

```text
Created asset-triggered DagRun for 'rekep_products': ... consumed 1 asset events
```

Its `run_id` begins `asset_triggered__`, it finished ten seconds later, and
its `build_dbt` logged the same two lines as above. Both DAGs wrote the
checkout's default catalog, `data/catalog.db`, because the conf named none.
That is the rule the run shows: an asset-triggered run has no conf at all, so
`rekep_products` reads the catalog its own document names, and the two DAGs
name the same catalog through their documents or not at all. A first attempt
that had pointed the ingestion trigger at a catalog of its own failed in
`build_dbt` with `Table does not exist: fix.silver` for exactly that reason.
The warehouse then held the six tables at the counts every other route lands
-- 141, 53, 53, 49, 9 and 8 rows -- and
`tools/pipeline_samples.py --catalog … --check` against it answered
`4 samples match`.

## S3 capture with SQL catalog

Deploy the warehouse as described in [S3 deployment](operations/deploy.md#s3-with-a-sql-catalog),
then pass both source and catalog Params:

```bash
uv run --project "$REKEP_ROOT/python" --group airflow airflow dags trigger \
  rekep_ingestion \
  --conf '{
    "filesystem":"s3://market-capture/ulbridge/2026/08/14?region=eu-west-1",
    "start":"2026-08-14",
    "end":"2026-08-14",
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

Give scheduler/workers an IAM role and set their region. Deploy the three
tables once with the same role and settings:

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
    "start":"2026-08-14",
    "end":"2026-08-14",
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
3. Deploy the three tables and rerun deploy to see `present`.
4. Confirm the worker can list/read capture objects and read/write the
   warehouse prefix.
5. Trigger one immutable capture manually, naming its day, and compare stage
   counts.
6. Replay it and require the same counts and unchanged table row counts: a
   replay lands the same rows once.
7. Inspect `fixentries` for entries of tag 0 before enabling a recurring
   schedule: those are the pairs no dictionary explained, and `nofixentries`
   is how many the message carried.
8. Keep `max_active_runs=1` unless catalog and source-window ownership are
   designed for concurrent commits.
9. Set `retries` and `retry_delay`; the default is no retry, and a retried
   attempt is idempotent.
