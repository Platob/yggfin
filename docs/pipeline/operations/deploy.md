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

## AWS S3 Tables

A table bucket is served by an Iceberg REST catalog AWS hosts and maintains,
and AWS serves it at two endpoints. `type: s3tables` is one type for both,
because the choice is not a second setting: each endpoint names the bucket
differently, so the `warehouse` is what says which door a run knocks at.

| door | `warehouse` | endpoint | signed for | governs access |
| --- | --- | --- | --- | --- |
| S3 Tables | `arn:aws:s3tables:<region>:<account>:bucket/<name>` | `https://s3tables.<region>.amazonaws.com/iceberg` | `s3tables` | IAM alone |
| AWS Glue | `<account>:s3tablescatalog/<name>` | `https://glue.<region>.amazonaws.com/iceberg` | `glue` | IAM and Lake Formation |

Take the Glue door when the bucket is already integrated with the AWS
analytics services -- Lake Formation grants, cross-account access, and the
same tables in Athena, Redshift, EMR and QuickSight. Take the S3 Tables door
for a worker that owns its bucket outright and reaches it with IAM and
nothing else. Both land the same rows in the same tables.

Everything else follows from that one value: the endpoint, the SigV4 signing
name, the signing region, and the region the table files are in. Install the
extra -- pyiceberg signs those REST calls through boto3, which the Iceberg
extra does not pull -- and authenticate the way every other AWS mode here
does:

```bash
uv sync --project python --extra s3tables
export AWS_REGION=eu-west-1
aws sts get-caller-identity
```

### The S3 Tables endpoint

The ARN states the region, so the ARN is the whole configuration:

```bash
uv run --project python rekep iceberg deploy \
  --catalog rekep \
  --property type=s3tables \
  --property warehouse=arn:aws:s3tables:eu-west-1:123456789012:bucket/market-tables
```

```json
{
  "catalog": {
    "name": "rekep",
    "properties": {
      "type": "s3tables",
      "warehouse": "arn:aws:s3tables:eu-west-1:123456789012:bucket/market-tables"
    }
  }
}
```

The worker needs the S3 Tables namespace and table actions on that bucket,
plus `s3tables:GetTableData` and `s3tables:PutTableData` for the rows
themselves; the endpoint vends the credentials each table's files are read and
written with.

### The AWS Glue endpoint

Integrate the table bucket with the AWS analytics services first: that mounts
it in the Glue Data Catalog as the federated catalog `s3tablescatalog/<name>`,
which is the name this door takes, and it puts Lake Formation in front of
every table. Grant the worker's role what it has to do there -- creating the
namespaces and tables `rekep iceberg deploy` creates, and reading and writing
the ones ingestion fills -- and enable full table access for external engines,
which is what lets Lake Formation vend credentials to a client like this one.

```json
{
  "catalog": {
    "name": "rekep",
    "properties": {
      "type": "s3tables",
      "warehouse": "123456789012:s3tablescatalog/market-tables",
      "rest.signing-region": "eu-west-1"
    }
  }
}
```

This name carries no region, so one is stated: `rest.signing-region` here, or
`AWS_REGION` in the worker's environment, or the profile or instance the
worker runs under. Nothing guesses one -- the wrong region signs for a catalog
that is not this one -- and a run that finds none says so and names where to
put it.

The role needs the Glue Iceberg REST actions on that catalog and
`lakeformation:GetDataAccess` for the credentials Lake Formation vends,
beside the S3 Tables data actions above.

### Either door

The catalog goes in a parameters file, for the three ingestion tasks and for
`build_dbt`:

```bash
uv run --project python rekep task run \
  tasks/parse_messages/parse_messages.json \
  --parameters-file /run/rekep/aws.json \
  --parameter 'filesystem="s3://market-capture/ulbridge/2026/08/14?region=eu-west-1"'
```

The capture bucket still needs its own list and read permissions. Prefer an
IAM role; do not place access keys in task JSON, CLI arguments, or Airflow
Params.

What the warehouse does not decide stays the operator's, under the standard
property names, and is kept exactly as written -- a VPC endpoint or a FIPS one
as `uri`, another `rest.signing-region`, explicit `s3.*` credentials:

```json
{
  "catalog": {
    "name": "rekep",
    "properties": {
      "type": "s3tables",
      "warehouse": "arn:aws:s3tables:eu-west-1:123456789012:bucket/market-tables",
      "uri": "https://vpce-0abc123.s3tables.eu-west-1.vpce.amazonaws.com/iceberg"
    }
  }
}
```

A table bucket's namespaces are one level deep and spelled in lowercase
letters, digits and underscores, which `logs`, `fix`, `orders` and
`executions` already are. Deployment, ingestion, the dbt build and their
replays are otherwise exactly what they are on any other catalog.

Two things differ, and both because the service owns the files:

- Maintenance sweeps nothing. S3 Tables compacts these tables and expires
  their snapshots on a schedule of its own, and the bucket behind a table is
  not one this account lists, so a sweep can settle no file's ownership:
  [`optimize_iceberg`](../../storage/iceberg.md#maintenance) deletes nothing
  here, reports `deleted: 0` and records which bucket keeps its files, while
  the compaction and snapshot expiry it asks for still commit through the
  catalog. Set `remove_orphans` to `false` to say so in the document as well,
  and consider leaving the pass itself to the service.
- A drop takes the data with it. What a drop may ask for is the table's to
  decide and not the door's: an S3 Tables table answers a drop that would keep
  its files with a 400, so `drop_table` purges on a table bucket whether or
  not it was asked to.

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

The same recreate-and-replay rule applies to the immediately preceding shape
whose columns already match but whose identifier field is `currhashcode`.
Iceberg schema merge does not replace identifier fields: the current table has
`curruuid` as its sole identifier, while `currhashcode` is ordinary content
metadata.

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
