# CLI

One service class per capability, and the service *is* the command word:
`rekep <service> <command>` — `rekep dataset deploy`, `rekep ddl dump`. A new
capability is a new service class, not a flag on an old command.

```console
$ rekep --help
    ddl                 DDL from record declarations
    product             record declarations as files
    docs                generated documentation pages
    records             deploy a record class to the stacks
    dataset             datasets deployed into iceberg or doris
    dag                 declared dags: list, show, run
    iceberg             iceberg deployment stack
    doris               doris deployment stack
    airflow             deployable Airflow DAG modules
    install             stand doris or airflow up from nothing
    tutorial            a guided tour, zero to local lakehouse
```

## DDL

Emit `CREATE TABLE` from a record declaration:

```bash
rekep ddl dump \
    --namespace rekep.models.Log \
    --table-name log_records \
    --property "write.format.default=parquet" \
    --out stacks/ddl/iceberg
```

writes `stacks/ddl/iceberg/log_records.sql`:

```sql
CREATE TABLE IF NOT EXISTS log_records (
    url STRING NOT NULL COMMENT 'Path of the log the line came from, ...',
    unix BIGINT NOT NULL COMMENT 'Timestamp as whole nanoseconds since the epoch, naive UTC.',
    ...
)
USING iceberg
COMMENT 'One parsed line of a trading log.'
TBLPROPERTIES (
    'write.format.default' = 'parquet'
);
```

Column comments come from the field docstrings; `NOT NULL` from the type
hints; default partition columns from `Arrow(iceberg={"partition": "true"})`
field metadata, overridable with `--partition-by`.

## Jinja everywhere a deploy value goes

Any string option may be a template, rendered with the other arguments,
`--var` pairs, and the process environment:

```bash
DATA_BUCKET=s3://lake rekep ddl dump \
    --namespace rekep.models.Log \
    --location "{{ env.DATA_BUCKET }}/logs" \
    --var zone=eu \
    --property "comment={{ zone }}:{{ namespace }}" \
    --out -
```

Undefined variables fail loudly — a half-rendered config is worse than a loud
one. Templating needs the `jinja` extra; untemplated values never touch it.

## Deploy one record

The stacks converge whole folders (`catalogs/`, `namespaces/`); `records
deploy` converges a single record class into one or more of them, stack
defaults filling in namespace and properties:

```bash
rekep records deploy --pyclass rekep.models.Log --target iceberg
rekep records deploy --pyclass rekep.models.Log     --target iceberg --target doris --dry-run
```

Iceberg converges live (catalog checked, namespace and table
`get_or_create`/`create_or_update`, both via `Iceberg.deploy_one`); Doris
emits its ordered statements. `--dry-run` and `--verbose` behave as
everywhere else.

## Deploy datasets

A `Dataset` carries its own namespace and per-protocol properties, so
`stacks/datasets/*.yaml` needs no matching `tables/` side file — `dataset
deploy` converges every declared dataset autonomously:

```bash
rekep dataset deploy --config stacks/datasets --target iceberg
rekep dataset list --config stacks/datasets
```

`--stack-config` points at the Iceberg/Doris deployment the dataset's
namespace must resolve against (default `stacks/<target>`); `into_iceberg_table()`/
`into_doris_table()` build the ad hoc table, `--dry-run` plans without
converging.

## Optimize datasets

Compaction and snapshot retention, idempotent like every other verb — a
table already laid out well reports nothing rewritten and nothing expired:

```bash
rekep dataset optimize --dry-run
rekep dataset optimize --branch dev
```

It takes no policy arguments on purpose: `protocols.iceberg.compact_min_files`
and `protocols.iceberg.retain` in each dataset's own side file are the whole
policy, so a scheduler runs the bare command. See
[Datasets](datasets.md#maintenance-compact-cleanup-optimize).

```console
$ rekep dataset optimize --dry-run
ds:/default/log: would rewrite 0 files in 0 partitions, 0 snapshots expired, 0 files freed
ds:/default/parsed_messages: would rewrite 6 files in 1 partitions, 0 snapshots expired, 0 files freed
```

## Dags: list, show, run

A `Dag` is rekep's own graph, so listing it, reading its order and running it
need no orchestrator installed:

```console
$ rekep dag list
dag:/passthrough  schedule=@daily  passthrough
dag:/pipeline/trading_logs  schedule=@daily  files_to_logs -> logs_to_records

$ rekep dag show --uri dag:/pipeline/trading_logs
dag:/pipeline/trading_logs  schedule=@daily
  trading_logs.files_to_logs  uri=job:/pipeline/files_to_logs  after=-
  trading_logs.logs_to_records  uri=job:/pipeline/logs_to_records  after=files_to_logs

$ rekep dag run --uri dag:/pipeline/trading_logs
trading_logs.files_to_logs: 24
trading_logs.logs_to_records: 24
```

`--config` points at the dags folder and `--jobs-config` at the jobs folder
the tasks are declared in (defaults: `stacks/dags`, `stacks/jobs`). `run`
walks the dependency order in this process — the executor a laptop, a test
and a cron line want, not a replacement for a scheduler.

## Airflow: dag modules as a deployable resource

```console
$ rekep airflow deploy --config stacks/dags --dags-folder /opt/airflow/dags
converged dags: passthrough.py, trading_logs.py

$ rekep airflow dags list
passthrough.py  dag:/passthrough  1 task(s)
trading_logs.py  dag:/pipeline/trading_logs  2 task(s)
```

One generated module per side file, idempotent like every other verb: the
side file stays the single source, loaded and built fresh at parse time, and
identical content is a logged no-op. `--dry-run` reports without writing. See
the [Dags guide](dags.md#airflow).
