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

## Stream reads

```python
reader = messages.read_arrow_reader(
    columns=("sourceurl", "sourcerownum", "body"),
    row_filter="sourcerownum >= 1000",
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

```yaml
catalog:
  name: production
  properties:
    type: glue
    warehouse: s3://warehouse-bucket/rekep
    glue.region: eu-west-1
    s3.region: eu-west-1
    s3.endpoint: https://s3.example.net
```

Credentials belong in the provider chain or secret-backed `s3.*` properties,
never in committed task documents.

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
Run long transaction checks explicitly with `pytest -m integration`.
