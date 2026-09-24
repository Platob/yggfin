# Airflow deployment

`airflow/pipeline.py` declares the ready-to-run `rekep_ingestion` DAG:

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

The daily DAG runs seven bundled tasks over its data interval, each node the
task of its own name. A task's shipped `python/src/rekep/tasks/<name>.json`
owns its defaults, and the DAG files read those documents without importing
`rekep`, so parsing a DAG stays light. Shared Params include the catalog,
source tables, registry, codec options and `snapshot_millis`. Explicit
`start`/`end` in run conf win over the interval for book creation and its
predecessors.

The three event tasks wait for `parse_books`, then run independently. Their
`upstream_task_id="parse_books"` handoff validates the parent Stage result
and sets `books`, `start`, `end` and `snapshot_id` from its actual target,
window and committed snapshot after generic parameter merging. This prevents
one child from following a later head or a conflicting window. `snapshot_id`
is not a DAG Param. Missing or malformed handoff data fails before the child
process starts; zero is the explicit empty-source snapshot sentinel.

`rekep_products` remains the optional `build_dbt` DAG triggered by the
`fix.refined` Asset. Its SQL products are separate from `market.*`, and it can
start independently of native book processing.

## How a task runs

`airflow/dispatch.py` builds every node. By default each is `RekepOperator`,
which runs the task on the worker; with `REKEP_EKS_CONFIG` set each is
`EksRekepOperator`, which runs it in a pod ([Dispatch on EKS](#dispatch-on-eks)).
Both resolve parameters and validate the result the same way.
`RekepOperator` runs the task through the `rekep` CLI, from the same checkout
and locked environment:

```text
uv run --project <repo>/python --group runner --no-sync --offline \
  --no-progress --no-env-file -- rekep tasks <task_name> run \
  --parameters-file <attempt>/parameters.json \
  --result-file <attempt>/result.json
```

`--no-sync --offline` makes a scheduled run deterministic: deployment must
resolve and install dependencies before the DAG is enabled. The child is the
command a person runs by hand: it validates its Stage result and publishes it
to `--result-file` atomically, with logs and any traceback on the task log.
`on_kill` terminates the child process group. The child's working directory is
the checkout, so a relative `catalog` setting resolves there and not in the
scheduler's own directory.

### Operator arguments

| argument | required | what it does |
| --- | :---: | --- |
| `task_name` | yes | the bundled task to run, as `rekep tasks list` names it; any other name fails before a process starts |
| `repository` | yes | checkout root holding `python/pyproject.toml`; also the child's working directory |
| `parameters` | no | per-task overrides; a name the task does not declare fails the task |
| `environment` | no | variables for the child, over the worker's own |
| `cache_dir` | no | sets `UV_CACHE_DIR`, to point `uv` at a shared worker cache |
| `outlets` | no | the Assets this task publishes |
| `upstream_task_id` | no | validated parent result that pins a child's window, snapshot and source table |

`task_name`, `repository`, `parameters`, `environment` and `upstream_task_id`
are template fields.

`EksRekepOperator` takes the same `task_name`, `repository` (only the source of
defaults there), `parameters`, `outlets` and `upstream_task_id`, plus
`EksPodOperator`'s keywords: `cluster_name` and `image` are required, and
`cmds`, `arguments` and `do_xcom_push` are the operator's own and refused.
`environment` is the supported way to give one task a credential-bearing
variable without putting it in Params.

Parameters merge in one order, later winning: the task's shipped defaults,
then the operator's `parameters`, then the DAG run's Params, then the data
interval -- and only for a name the task already declares, so a task that does
not take `start` is never handed the scheduler's. Two exceptions keep a
person's intent: an interval with no width, which Airflow infers for a manual
run of an unscheduled DAG, fills nothing, and a bound the run's own conf names
is left as named.

### Assets and what a run returns

Each node declares one `Asset` outlet named exactly for each table its task
writes -- `logs.messages`, `fix.raw`, `fix.refined`, `market.books` and the
three `market.*` event tables -- so a downstream DAG can be scheduled on any
of them. When a run finishes, the operator attaches `task`, `read`, `written`
and `skipped` to the event of every outlet its result names as a target -- a
table this run did not write claims nothing.

`execute` returns the validated Stage mapping, so the same counts land in XCom
under `return_value`. Its fields are listed in
[Logs and task results](operations/logs.md#result-schema); it is a summary, never rows.

### Reproduce a node by hand

A node is one command, so a failed attempt reruns from a shell with the same
parameters. `show` prints what a run would take; `run` prints its result:

```bash
cd "$REKEP_ROOT"
uv run --project python --group runner rekep tasks parse_messages show \
  --parameter start=2026-08-14 --parameter end=2026-08-14
uv run --project python --group runner rekep tasks parse_messages run \
  --parameter filesystem=file:///srv/capture/2026-08-14 \
  --parameter start=2026-08-14 --parameter end=2026-08-14
```

## The products DAG

`airflow/products.py` declares `rekep_products`, which is one node,
[`build_dbt`](tasks/build-dbt.md):

```text
fix.refined -> build_dbt -> orders.events, orders.current, executions.fills
```

Its schedule is the `fix.refined` Asset the ingestion DAG publishes last, so a
build starts when `parse_fix_refined` writes and the two DAGs are one route
without either naming the other's tasks. A product reads the walked rows and
never `fix.raw`, so no build waits on the parse alone. The node declares
one outlet per table the dbt models commit, so a third DAG can be scheduled on
a product the same way.

Its Params are the `build_dbt` task's own: `project`, `profiles`, `target`,
`select`, `catalog` and `log_level`. A worker runs dbt out of the `runner`
group, the same locked environment every other task runs in, and dbt writes
`target/` and `logs/` under the project directory unless `DBT_TARGET_PATH` and
`DBT_LOG_PATH` say otherwise -- which is what the operator's `environment`
argument is for.

### Retries

The DAG sets no `retries`, so every task is `retries=0` and one transient S3
or catalog error fails the run. Raising it is safe and is the recommended
configuration: each attempt writes into its own private directory keyed on the
try number, that directory is removed whether the attempt lands or raises, and
the three original ingestion writers merge on their declared `curruuid`
keys. Market writers atomically replace the exact window, leaving the prior
snapshot visible on failure. Every event-task retry retains its parent book
snapshot and window rather than following a newer head.

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
export AIRFLOW__CORE__DAGS_FOLDER="$REKEP_ROOT/airflow"
uv run --project "$REKEP_ROOT/python" --group airflow airflow db migrate
uv run --project "$REKEP_ROOT/python" --group airflow airflow dags list
```

For a local evaluation, `airflow standalone` starts all components:

```bash
uv run --project "$REKEP_ROOT/python" --group airflow airflow standalone
```

## Deploy the tables

Each ingestion task creates the tables it writes with
`rekep tasks <name> deploy`, so the seven tables are one deploy per task. A
deploy reports `created` or `present` per table; rerun it to check one.

```bash
cd "$REKEP_ROOT"
for task in parse_messages parse_fix_raw parse_fix_refined parse_books \
  parse_orders parse_quotes parse_executions; do
  uv run --project python rekep tasks "$task" deploy
done
```

The `cd` matters: the shipped default `uri` and `warehouse` are both relative,
and the operator runs its child with the checkout as the working directory.
Deploying from anywhere else creates a second catalog next to wherever the
command was typed, and the DAG then writes to an empty one. Otherwise name the
catalog in a parameters file and hand the same file to every deploy:

```bash
cat > /var/lib/rekep/catalog.json <<'JSON'
{
  "catalog": {
    "name": "rekep",
    "properties": {
      "type": "sql",
      "uri": "sqlite:////var/lib/rekep/catalog.db",
      "warehouse": "/var/lib/rekep/warehouse"
    }
  }
}
JSON
for task in parse_messages parse_fix_raw parse_fix_refined parse_books \
  parse_orders parse_quotes parse_executions; do
  uv run --project "$REKEP_ROOT/python" rekep tasks "$task" deploy \
    --parameters-file /var/lib/rekep/catalog.json
done
```

and name the same catalog in each run's `--conf`, as the examples below do.
The sections below change only the file's `catalog`.

## Local-files trigger

After the deploy, trigger with an absolute source path visible to the worker:

```bash
uv run --project "$REKEP_ROOT/python" --group airflow airflow dags trigger \
  rekep_ingestion \
  --conf '{"filesystem":"file:///srv/capture/2026-08-14","start":"2026-08-14","end":"2026-08-14"}'
```

A manual trigger of the daily DAG covers the last complete day unless its
conf names the window, which is what the `start` and `end` above do for a
dated capture.

The default SQLite catalog is suitable only when scheduler and task execution
share one durable host filesystem.

## Verify the graph

`airflow dags test rekep_ingestion --conf ...` executes the seven-stage graph
in one run. Use a capture whose admitted market messages satisfy the native
contract, including explicit sides for AE trade occurrences. Compare the
book task's `snapshot_id` with each event task's `source_snapshot_id`, and
require the same window on all four results. The three source IDs must agree
even if the book table head changes after the parent finishes.

`airflow assets list` names the seven ingestion/market Assets and, when the
optional products DAG is loaded, its three SQL product Assets. A scheduler
can trigger `rekep_products` from the refined Asset; `dags test` does not
simulate that separate scheduler-triggered run. An Asset-triggered run has no
ingestion run conf to inherit, so give `build_dbt` the ingestion catalog in
the worker's environment: its `catalog` parameter is null by default, and then
`REKEP_DBT_CATALOG`, the same mapping as JSON, is what the build commits
through (on EKS, set it under `tasks.build_dbt.env_vars`):

```bash
export REKEP_DBT_CATALOG='{"name":"rekep","properties":{"type":"sql","uri":"sqlite:////var/lib/rekep/catalog.db","warehouse":"/var/lib/rekep/warehouse"}}'
```

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

Give scheduler/workers an IAM role and set their region. Deploy the seven
tables once with the same role, the deploy loop above and this catalog file:

```bash
export AWS_REGION=eu-west-1
cat > /var/lib/rekep/catalog.json <<'JSON'
{
  "catalog": {
    "name": "rekep",
    "properties": {
      "type": "glue",
      "warehouse": "s3://market-warehouse/rekep",
      "glue.region": "eu-west-1",
      "s3.region": "eu-west-1"
    }
  }
}
JSON
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

## AWS S3 Tables

A table bucket is named by the endpoint that serves it -- its ARN at the S3
Tables endpoint, `<account>:s3tablescatalog/<name>` at the Glue one -- and the
`runner` group carries the extra that signs those REST calls. Deploy it as
described in [AWS S3 Tables](operations/deploy.md#aws-s3-tables), then trigger
with that catalog:

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
        "type":"s3tables",
        "warehouse":"arn:aws:s3tables:eu-west-1:123456789012:bucket/market-tables"
      }
    }
  }'
```

Behind the Glue endpoint the conf is
`"warehouse":"123456789012:s3tablescatalog/market-tables"` with
`"rest.signing-region":"eu-west-1"`, and the worker's role needs its Lake
Formation grants. `rekep_products` takes its catalog from `REKEP_DBT_CATALOG`
in the worker's environment, so set it to this same table bucket.

## Dispatch on EKS

Set `REKEP_EKS_CONFIG` to a JSON document in the environment of the DAG
processor and the workers, and every node becomes an `EksRekepOperator`: an
[`EksPodOperator`](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/eks.html)
that runs its task in a pod on an EKS cluster. The document is the operator's
keywords -- anything `EksPodOperator` and `KubernetesPodOperator` take -- for
every task, and under `tasks` the ones a single task replaces whole:

```json
{
  "cluster_name": "market-data",
  "image": "123456789012.dkr.ecr.eu-west-1.amazonaws.com/rekep:abc1234",
  "namespace": "rekep",
  "service_account_name": "rekep",
  "region": "eu-west-1",
  "aws_conn_id": "aws_default",
  "env_vars": {"AWS_REGION": "eu-west-1"},
  "container_resources": {"requests": {"cpu": "2", "memory": "8Gi"}},
  "tasks": {
    "build_dbt": {
      "container_resources": {"requests": {"memory": "16Gi"}},
      "env_vars": {"AWS_REGION": "eu-west-1", "REKEP_DBT_CATALOG": "{\"name\": \"rekep\", \"properties\": {\"type\": \"glue\", \"warehouse\": \"s3://market-warehouse/rekep\"}}"}
    }
  }
}
```

`container_resources` is spelled as the Kubernetes object's `requests` and
`limits`; nested pod settings the operator takes no keyword for go in
`pod_template_dict`. The DAG processor reads the document when it parses the
DAGs, so a change to it is a change to the DAGs, and a task name under `tasks`
that ships no defaults fails the parse.

The worker resolves each run's parameters exactly as it does for
`RekepOperator` -- the checkout's defaults, the operator's, the DAG's Params,
the interval, the upstream snapshot -- and the pod's container runs

```text
rekep tasks <name> run --parameter NAME=<json> ... --result-file /airflow/xcom/return.json
```

one `--parameter` per resolved parameter, each value spelled as its JSON, so
the pod runs exactly what the worker resolved. The pod's XCom sidecar hands
the result back; the operator validates it and attaches its counts to the
outlet Assets, and the three event pods read the book pod's snapshot from it.
A pod that fails fails the task; `deferrable=True` waits for it in the
triggerer instead of a worker slot.

**The image.** `Dockerfile` at the repository root builds the image a pod
runs: `rekep` on the PATH with the locked `runner` group, and the dbt project
at the working directory its defaults name, as a non-root user. Build it from
the commit the DAGs are deployed from and push it where the cluster pulls:

```bash
docker build -t 123456789012.dkr.ecr.eu-west-1.amazonaws.com/rekep:$(git rev-parse --short HEAD) .
docker push 123456789012.dkr.ecr.eu-west-1.amazonaws.com/rekep:$(git rev-parse --short HEAD)
```

The pod is handed every parameter the checkout declares, so an image from
another revision refuses a parameter it does not take, and the run fails
rather than running something else. Every stage can be run in a built image
exactly as its pod runs it:

```bash
REKEP_IMAGE=rekep:dev uv run --project python pytest -q -m integration \
  python/tests/test_eks_rekep_operator.py -k image
```

**Access.** The operator's `aws_conn_id` (or the worker's AWS environment)
reaches the EKS API to create the pod; the pod reaches S3, Glue and S3 Tables
as its `service_account_name`'s IAM role (IRSA), so no credential is a
parameter. The XCom sidecar image is `alpine` from Docker Hub unless the
`kubernetes_default` connection's extras name `xcom_sidecar_container_image`,
which a private cluster sets to a mirror it can pull.

## Production checklist

1. Pin and deploy one repository revision to every worker.
2. Run `uv sync --locked` while network access is allowed.
3. Deploy the seven tables and rerun deploy to see `present`.
4. Confirm the worker can list/read capture objects and read/write the
   warehouse prefix.
5. Trigger one immutable capture manually, naming its day, and compare stage
   counts.
6. Replay the window and compare rows. Market tables replace exactly the
   window, including empty output; ingestion retains its keyed replay rules.
7. Inspect `fixentries` for entries of tag 0 before enabling a recurring
   schedule: those are the pairs no dictionary explained, and `nofixentries`
   is how many the message carried.
8. Keep `max_active_runs=1` unless catalog and source-window ownership are
   designed for concurrent commits.
9. Set `retries` and `retry_delay`; the default is no retry, and a retried
   attempt is idempotent.
10. On EKS, tag the image with the commit the DAGs ship from, and run one day
    through a pod of each task before enabling the schedule.
