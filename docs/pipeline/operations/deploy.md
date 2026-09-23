# Deploy Iceberg tables

`rekep iceberg deploy` creates seven tables from four runtime field factories:
Message for `logs.messages`, FixMsg for `fix.raw` and `fix.refined`, Book for
`market.books`, and MarketEvent for `market.orders`, `market.quotes` and
`market.executions`. Shapes derive without consuming input rows. Deployment
is idempotent: existing tables are reported as `present` and left unchanged.

The optional dbt models declare their own shapes and create their tables on
first commit. Install the pinned Yggdryl 0.1.11 dependency before deploy.

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
    "fix.raw": "created",
    "fix.refined": "created",
    "market.books": "created",
    "market.orders": "created",
    "market.quotes": "created",
    "market.executions": "created"
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
  tasks/parse_messages/parse_messages.json --table fix.raw
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

Create the seven tables:

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

Use this parameters file for the ingestion and market tasks:

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
  tasks/parse_fix_raw/parse_fix_raw.json \
  --parameters-file /run/rekep/aws.json
uv run --project python rekep task run \
  tasks/parse_fix_refined/parse_fix_refined.json \
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
| S3 Tables, located | `s3tables://<name>?region=<region>&account=<account>` | the same, or the endpoint the locator states | `s3tables` | IAM alone |
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

### Where the endpoint is

An ARN names the bucket and says nothing about where the catalog answers, so
the regional endpoint is what it resolves to unless the worker's environment
names another, below. The bucket's locator -- the `s3tables:` URL the ARN
redirects to, `yggdryl.Arn.locator()` -- is the same bucket spelled as a
location, and a location can say where its endpoint is
the way every `s3:` URL here does: in its host, with a port and a `scheme`
where an emulator of the service answers on one, or under `endpoint_override`
in its query. The two fields a location does not carry, the region and the
account, go in the query too, because the endpoint takes the bucket under its
ARN and the ARN is spelled back from them:

```json
{
  "catalog": {
    "name": "rekep",
    "properties": {
      "type": "s3tables",
      "warehouse": "s3tables://market-tables?region=eu-west-1&account=123456789012&endpoint_override=vpce-0abc123.s3tables.eu-west-1.vpce.amazonaws.com"
    }
  }
}
```

`s3tables://localhost:9001/market-tables?scheme=http&region=eu-west-1&account=123456789012`
reaches an emulator on a developer's machine the same way, and a bucket in
another partition names it: `partition=aws-cn`. A locator that states no
region is read like the Glue name, from `rest.signing-region` or the
environment.

What the warehouse does not decide stays the operator's, under the standard
property names, and is kept exactly as written -- a `uri` stated outright,
another `rest.signing-region`, explicit `s3.*` credentials:

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

The worker's environment is the third way to say where the endpoint is, and
the one that states it for every S3 Tables catalog the worker runs rather
than in one document. It is read from the endpoint variables the AWS CLI
reads -- the variables alone, not a profile's `endpoint_url` or `services`
section -- after a `uri` stated outright and the locator's endpoint, and
before the regional one: `AWS_ENDPOINT_URL_S3TABLES` for an ARN or a locator,
`AWS_ENDPOINT_URL_GLUE` for the Glue name, each with `/iceberg` added unless
its path already ends in it. The table files follow `AWS_ENDPOINT_URL_S3`
into `s3.endpoint` where the catalog states none:

```bash
export AWS_ENDPOINT_URL_S3TABLES=http://localhost:4566
export AWS_ENDPOINT_URL_S3=http://localhost:4566
```

The generic `AWS_ENDPOINT_URL` stands in for `AWS_ENDPOINT_URL_S3` and
nothing else. It names one endpoint for every service at once, which suits
the files, read through S3 alone, but not the catalog: the two doors cannot
both be at one value, so a stray export of it never moves the `uri`. It
still moves the files: where an emulator is set up with `AWS_ENDPOINT_URL`
alone, name its catalog too, with `AWS_ENDPOINT_URL_S3TABLES` or
`AWS_ENDPOINT_URL_GLUE`, or the catalog and its files are on two different
services. `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true` in the environment turns
every one of these off.

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

A newly deployed FIX table has exactly the 128 native FixMsg columns, and a
newly deployed `logs.messages` the 12 columns of the `Message` contract. A
table of an older shape is not evolved into either; it is dropped and
replayed from capture, as the next section says.

## Migrating a warehouse written under an earlier yggdryl

An existing table with an obsolete native schema or identity contract must
be rebuilt from its source under the pinned release. Deployment reports an
existing table as `present`; it does not validate or rewrite its schema.
There is no second identity spelling or translation layer in Yggfin.

Recreate affected source tables and dependent products together, deploy their
current declarations, and replay capture through `parse_messages`,
`parse_fix_raw` and `parse_fix_refined` in order. Then run `parse_books` and
its three event projections from the newly committed snapshot. Rebuild the
optional dbt products when they are deployed too. The seven native table
shapes come from four runtime contracts; the SQL products retain their own
model declarations.

Replay captures from their original locations. A line without its own clock
uses the source object's modification time; copying the object can therefore
change the native identity. Preserve the explicit `srcuuids` provenance join
rather than recomputing it from stored timestamps or content codes.

For market tables already using the current shape, exact window replacement
removes changed or disappeared keys within its predicate, including an empty
rerun. It does not repair incompatible schemas or older source tables. Keyed
FIX ingestion likewise cannot remove an old key merely because a new native
revision produces a different one.

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

assert set(result) == {"logs.messages", "fix.raw", "fix.refined"}
```

## Verification

Run deploy again and require every value to be `present`. Then execute an empty
or small immutable capture and inspect table schemas, partitions, snapshots,
and the stage counts before enabling a schedule.
