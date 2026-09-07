# Iceberg

rekep keeps Iceberg at the PyIceberg/PyArrow boundary. Nothing below it owns
catalogs, snapshots, scan planning, or commits.

```python
from rekep import Message
from rekep.iceberg import IcebergCatalog

catalog = IcebergCatalog(
    name="local",
    properties={
        "type": "sql",
        "uri": "sqlite:///catalog.db",
        "warehouse": "warehouse",
    },
)
messages = catalog.dataset("logs.messages", field=Message.field())
```

## Stream writes

```python
written = messages.append_arrow_reader(
    reader,
    Message.field(),
    merge_by=True,
)
```

`merge_by=True` uses the primary key declared on the native Field. Existing
keys are skipped; a missing table is created. `commit_batch_num` and the
optional `commit_row_size` bound each storage commit independently from input
batch size.

`overwrite_arrow_reader` replaces rows matching the declared key and inserts
the remainder. Both APIs require a schema-bearing `RecordBatchReader` and
consume one batch at a time. The batch and table helpers build that reader.

Set `merge_schema=True` on a dataset, or on an `append_arrow_*` or
`overwrite_arrow_*` write, to add columns from its authoritative write Field
before the first batch is consumed:

```python
from yggdryl import Field

fix_field = Field.from_arrow_schema(reader.schema, name="FixMessage")
fixes = catalog.dataset(
    "fix.messages",
    field=fix_field,
    merge_schema=True,
)
fixes.append_arrow_reader(reader, fix_field)
```

Pass the current Field explicitly when its reader may be newer than the stored
table. This mode is enabled by `parse_fix`; the raw `Message` contract remains
fixed. It is additive only: existing types, nullability, comments, field IDs,
identifier fields, partition specs, and sort orders do not change. Iceberg
assigns IDs to additions. A column added to an existing table must be nullable
because older rows have no value for it; the first write creates a missing
table directly from its Field. Schema updates are table-wide even when rows are
written to a branch. A write with no new column makes no schema commit.

Before either write, the native `Field` applies its declarations in dependency
order: **cast → derived partition columns → digest holders**.

Three declarations look similar and are not:

| declaration | key | what it does |
| --- | --- | --- |
| `partition_key()` | `field:partition` | physical layout marker; rekep maps it to an identity Iceberg spec |
| `partition_key("hour")` | `iceberg:partition_key` | a non-identity Iceberg transform |
| `derived_from(...)` | `partition:sources` | an executable Arrow derivation, computing a real column |

A derived transform such as `day` may also appear under `partition:transform`;
it computes a separate Arrow column and does not define the table spec. The
identity and transformed Iceberg markers are mutually exclusive -- a field
carrying both is rejected at declaration and at spec conversion. Iceberg itself
may declare several transforms over one source column, so schema projection
omits that ambiguous marker and leaves the table's `PartitionSpec`
authoritative for storage planning.

## Stream reads

```python
reader = messages.read_arrow_reader(
    columns=("url", "rownum", "body"),
    row_filter="rownum >= 1000",
    limit=100,
)
try:
    for batch in reader:
        consume(batch)
finally:
    reader.close()
```

Filters, projections, limits, and ordering are pushed into scan planning where
PyIceberg supports them. Every read accepts `snapshot_id`; every read and write
accepts `branch`. `root`, `main`, and `master` address the physical main ref.

## Filesystem boundary

Capture sources are bound with `IOBase`. Iceberg locations use the table's
configured PyIceberg `FileIO` and PyArrow streams. `IcebergFileIO` only tracks
transaction outputs so failed commits can clean up their own files; it is not
a general filesystem abstraction.

Configure warehouse S3 behavior with standard catalog properties:

```json
{
  "catalog": {
    "name": "production",
    "properties": {
      "type": "glue",
      "warehouse": "s3://warehouse-bucket/rekep",
      "glue.region": "eu-west-1",
      "s3.region": "eu-west-1",
      "s3.endpoint": "https://s3.example.net"
    }
  }
}
```

Credentials belong in the provider chain or secret-backed `s3.*` properties,
never in committed task documents.

## Message schema replacement

The current table contract uses `url`, `rownum`, `branch`, and a derived
`timepartition` with an Iceberg `hour` transform. Recreate an older messages
table from `Message.field()` and reingest its source captures; rekep carries no
legacy name, timestamp-type, or partition-layout compatibility path.

## Maintenance

```python
import datetime

messages.compact(branch="root")
messages.cleanup(retain=24, older_than=datetime.timedelta(days=7))
messages.optimize(
    branch="root",
    retain=24,
    older_than=datetime.timedelta(days=7),
)
```

Compaction rewrites small files. Cleanup expires old snapshots and removes only
files unreachable from every retained ref after the configured grace period.
The checked maintenance job exposes the same controls:

```bash
rekep task run tasks/optimize_iceberg/optimize_iceberg.json
```

| parameter | meaning |
| --- | --- |
| `namespace` | `null` visits every namespace recursively |
| `min_files` | compaction threshold |
| `retain`, `snapshot_age_days` | how much time travel is preserved |
| `orphan_age_days` | protects files from active or recently failed writers |
| `remove_orphans`, `metadata` | enable the orphan sweep, and include the metadata directory in it |
| `branch` | `root`, `main` and `master` all select the Iceberg root branch |
| `log_level` | `DEBUG` for file and plan details |

Each table reports under its full identifier -- `logs.messages`,
`fix.messages` -- so equal table names in different namespaces cannot collide.

Run long transaction checks explicitly with `pytest -m integration`.
