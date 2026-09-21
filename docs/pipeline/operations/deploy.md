# Deploy Iceberg tables

`rekep iceberg deploy` creates the three tables ingestion writes --
`logs.messages`, `fix.bronze` and `fix.silver` -- with the same runtime fields
their tasks use: `Message` for the raw product and `fix_message_field` for
both FIX tables, which answers all 123 native columns from the dictionary alone
without consuming a capture row. Deployment is idempotent: an
existing table is reported as `present` and is not rewritten.

The products the dbt project derives are not deployed here: a model declares
its own shape, and the dataset creates the table on the model's first commit.
`rekep iceberg deploy` is for a catalog the runner may not write to, which is
where ingestion lands.

## Local SQLite and files

The checked task document already names a SQLite catalog and local warehouse:

```bash
uv run --project python rekep iceberg deploy \
  tasks/parse_messages/parse_messages.json
```

Expected result shape:

```json
{
  "catalog": {
    "name": "rekep",
    "properties": {
      "type": "sql",
      "uri": "sqlite:///data/catalog.db",
      "warehouse": "file:///srv/rekep/data/warehouse"
    }
  },
  "tables": {
    "logs.messages": "created",
    "fix.bronze": "created",
    "fix.silver": "created"
  }
}
```

The printed `warehouse` is the resolved one: building the catalog handle
rewrites a scheme-less path into an absolute `file://` URL against the current
working directory. The `uri` is left as written, so a relative SQLite path
stays relative and also resolves against the working directory. Run deploy
from the checkout root, or give both settings absolute values, so the tables
land where the tasks later look for them.

Preview without creating anything:

```bash
uv run --project python rekep iceberg deploy \
  tasks/parse_messages/parse_messages.json --dry-run
```

Create one product only:

```bash
uv run --project python rekep iceberg deploy \
  tasks/parse_messages/parse_messages.json --table fix.bronze
```

## S3 with a SQL catalog

This mode keeps catalog metadata in a durable SQL database while data and
metadata files live on S3. The example uses SQLite for a single worker; use a
shared SQL service when multiple workers need the catalog.

```bash
export AWS_REGION=eu-west-1
uv run --project python rekep iceberg deploy \
  --catalog rekep \
  --property type=sql \
  --property uri=sqlite:////var/lib/rekep/catalog.db \
  --property warehouse=s3://market-warehouse/rekep \
  --property s3.region=eu-west-1
```

For an S3-compatible service, also set `s3.endpoint` and, when required,
`s3.force-virtual-addressing=false`. Capture-reader endpoint options belong on
the `filesystem` URI and are documented on
[`parse_messages`](../tasks/parse-messages.md#source-uri-forms).

## AWS Glue and S3

Install the Glue extra (the locked `runner` group already includes it), then
authenticate with an instance/task role, web identity, AWS profile, or standard
AWS environment variables.

```bash
uv sync --project python --locked --group runner
export AWS_REGION=eu-west-1
aws sts get-caller-identity
```

Create the three tables:

```bash
uv run --project python rekep iceberg deploy \
  --catalog rekep \
  --property type=glue \
  --property warehouse=s3://market-warehouse/rekep \
  --property glue.region=eu-west-1 \
  --property s3.region=eu-west-1
```

The worker needs Glue database/table permissions and S3 list/read/write/delete
permissions for the warehouse prefix. The capture bucket additionally needs
list/read permissions. Prefer an IAM role; do not place access keys in task
JSON, CLI arguments, or Airflow Params.

Use this parameters file for the three ingestion tasks:

```json
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
```

```bash
uv run --project python rekep task run \
  tasks/parse_messages/parse_messages.json \
  --parameters-file /run/rekep/aws.json \
  --parameter 'filesystem="s3://market-capture/ulbridge/2026/08/14?region=eu-west-1"'
uv run --project python rekep task run \
  tasks/parse_fix_bronze/parse_fix_bronze.json \
  --parameters-file /run/rekep/aws.json
uv run --project python rekep task run \
  tasks/parse_fix_silver/parse_fix_silver.json \
  --parameters-file /run/rekep/aws.json
```

## Table properties and branches

```bash
uv run --project python rekep iceberg deploy \
  --catalog rekep \
  --property type=glue \
  --property warehouse=s3://market-warehouse/rekep \
  --table-property write.format.default=parquet \
  --table-property write.parquet.compression-codec=zstd \
  --branch production
```

Properties are applied only when a table is created. Deployment deliberately
does not mutate an existing table; use maintenance or a reviewed migration for
that.

A newly deployed FIX table has exactly the 123 native FixMsg columns. Because
schema merge is additive, an existing table that still has `sourceurl`,
`rownum`, `timestamp`, `timepartition`, `threadId`, `pluginid`, or `level` --
the names that table holds them under -- must delete those columns through a
reviewed PyIceberg `update_schema()` transaction before its windows are
replayed. `sourceurl`, `rownum`, `msgthreadid` and `loglevel` are the raw
facts and remain in `logs.messages`, the last two under those spellings. The
other three are retired outright: a line's own instant is `currunix` and the
table is laid out by the hour of it rather than by a column beside it, and the
plugin a line names is `msgpluginid`, a native FixMsg column the parse fills.

## Migrating a warehouse that holds the retired FIX table

There is no compatibility shim for the one table the two FIX tables replaced.
Run `rekep iceberg deploy` once: it creates `fix.bronze` and `fix.silver`, and
it reports whatever `logs.messages` it finds as `present` -- deployment reads
the catalog and not a table's shape.

A `logs.messages` of the previous shape is not this one. It is the generic
event layout now: `timestamp`, `timepartition` and `pluginid` are gone,
`currunix` is required and is what the table is laid out by, `curruuid` is
required and is the second field, and every field id is renumbered. Three
columns removed, a required column added and the ids restated are not an
additive widening, and Iceberg adds no required column to rows that never
held it -- so drop the raw table before the replay rather than looking for a
migration of it. The capture is what it was read from, and the capture is
still there.

Then replay each window through `parse_messages`, `parse_fix_bronze` and
`parse_fix_silver`, in that order, and drop the retired FIX table. The first
run creates `logs.messages` in the shape above, so the raw table is created
and not evolved. The capture is read again, so every line states the instant,
the identity and the content code the read settles over its bytes, and the
parse reads that stored identity back rather than recomputing one -- which is
why `srcuuids` joins the line that landed, and why the replay is the migration
and not the fallback. A retired table the previous core wrote cannot be walked
in place: its rows are not the pinned core's 123-column native row, and
`parse_fix_silver` reads a table named as its `bronze` only in that shape. The
products are rebuilt by [`build_dbt`](../tasks/build-dbt.md) afterwards; drop
`orders.events`, `orders.current` and `executions.fills` first, because the
products' `lastpx` and `avgpx` moved from double to decimal with the
dictionary, and a column an existing table already holds is not retyped in
place.

## Python API

```python
from rekep.deploy import deploy
from rekep.iceberg import IcebergCatalog

catalog = IcebergCatalog(
    name="rekep",
    properties={
        "type": "glue",
        "warehouse": "s3://market-warehouse/rekep",
        "glue.region": "eu-west-1",
        "s3.region": "eu-west-1",
    },
)
try:
    result = deploy(catalog, dry_run=False)
finally:
    catalog.close()

assert set(result) == {"logs.messages", "fix.bronze", "fix.silver"}
```

## Verification

Run deploy again and require every value to be `present`. Then execute an empty
or small immutable capture and inspect table schemas, partitions, snapshots,
and the stage counts before enabling a schedule.
