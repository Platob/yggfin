# Deploy Iceberg tables

`rekep iceberg deploy` creates both current products—`logs.messages` and
`fix.messages`—with the same runtime fields their tasks use. Deployment is
idempotent: an existing table is reported as `present` and is not rewritten.

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
    "fix.messages": "created"
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
  tasks/parse_messages/parse_messages.json --table fix.messages
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

Create both tables:

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

Use this parameters file for both ingestion tasks:

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
  tasks/parse_fix/parse_fix.json \
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

assert set(result) == {"logs.messages", "fix.messages"}
```

## Verification

Run deploy again and require every value to be `present`. Then execute an empty
or small immutable capture and inspect table schemas, partitions, snapshots,
and the stage counts before enabling a schedule.
