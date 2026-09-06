# Iceberg

Yggfin keeps Iceberg at the PyIceberg/PyArrow boundary. Yggdryl does not own
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
    batches,
    Message.field(),
    merge_by=True,
)
```

`merge_by=True` uses the primary key declared on the native Field. Existing
keys are skipped; a missing table is created. `commit_batch_num` and the
optional `commit_row_size` bound each storage commit independently from input
batch size.

`overwrite_arrow_reader` replaces rows matching the declared key and inserts
the remainder. Both APIs consume one batch at a time.

Before either write, the native Field applies its declarations in dependency
order: cast, derived partition columns, then digest holders. Yggdryl
`partition:sources` describes an executable Arrow derivation. It is distinct
from the Iceberg partition spec: `field:partition` is Yggdryl's physical-layout
marker, which yggfin maps to an identity Iceberg spec. Non-identity transforms
passed to `partition_key(...)` remain under `iceberg:partition_key`. A native
derived transform such as `day` may also appear under `partition:transform`;
that declaration computes a separate Arrow column and does not define the
table spec.

The identity and transformed Iceberg markers are mutually exclusive;
declaration and Iceberg spec conversion reject a field carrying both. A native
`derived_from(...)` declaration is independent and may be composed with the
chosen Iceberg marker.

Iceberg itself may declare several transforms over one source column. A Field
member has only one physical marker, so schema projection deliberately omits
that ambiguous marker and leaves the table's native `PartitionSpec`
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

Capture sources use Yggdryl `IOBase`. Iceberg locations use the table's
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

## Message schema migration

If a table already stores `timestamp` as `timestamptz` and only uses the former
source names, rename those columns in place before ingestion resumes. Their
field and identifier IDs remain stable:

```python
table = catalog.load_table("logs.messages")
with table.update_schema() as update:
    update.rename_column("sourceurl", "url")
    update.rename_column("sourcerownum", "rownum")
```

Iceberg cannot evolve the legacy `string` timestamp column to `timestamptz`.
Such a table needs a replacement created from `Message.field()`. Prefer
reingesting the original captures; otherwise stream the old rows through an
explicit UTC timestamp conversion into the replacement. Validate row counts
and `(url, rownum)` keys before switching consumers. Do not update the timestamp
type in place.

This is an operator migration, not a runtime compatibility path. Fresh tables
use `url`, `rownum`, and the nullable microsecond `timestamptz` directly.

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

A null `namespace` visits every namespace recursively. `min_files` is the
compaction threshold; `retain` and `snapshot_age_days` preserve recent time
travel; `orphan_age_days` protects files from active or recently failed
writers. `root`, `main`, and `master` select the same Iceberg root branch.
`remove_orphans` enables the sweep; `metadata` includes the metadata directory
in it. Set `log_level` to `DEBUG` for file and plan details.

Run long transaction checks explicitly with `pytest -m integration`.
