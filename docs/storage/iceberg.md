# Iceberg

`IcebergDataset` exposes a table as an Arrow stream. PyIceberg owns planning,
schema ids, snapshots, and commits; rekep supplies recursive casting, commit
grouping, and maintenance policy. Yggdryl owns general resources outside this
table boundary.

```python
from rekep import FixMsg
from rekep.iceberg import IcebergCatalog

catalog = IcebergCatalog(
    name="local",
    properties={
        "type": "sql",
        "uri": "sqlite:///catalog.db",
        "warehouse": "warehouse",
    },
)
logs = catalog.dataset(
    "market",
    namespace="fix",
    field=FixMsg.into_field(),
    branch="root",
)
```

The catalog is shared. The dataset keeps `name="market"` and
`namespace="fix"` as explicit table coordinates.

## Read

```python
reader = logs.read_arrow_reader(
    row_filter="msgtype = 'D'",
    columns=["unix", "hash", "msgseqnum", "msgtype"],
    order_by=["unix", "msgseqnum", "hash"],
    snapshot_id=None,
    branch="root",
)
print(reader.schema.names)


def rows():
    """One fresh reader per call: a stream is consumed by whoever reads it."""
    return logs.read_arrow_reader(columns=["unix", "hash", "msgseqnum", "msgtype"])
```

```text
['unix', 'hash', 'msgseqnum', 'msgtype']
```

Every `order_by` column must be projected — sorting on one the reader will not
hand back is refused rather than dropped, so `MsgSeqNum` is in `columns` here.

Filters and projections reach scan planning. Ordered reads sort planned
partition paths deterministically and stream one partition at a time. A file
whose recorded sort order starts with the requested keys joins the direct
merge; any other file is externally sorted through bounded local IPC runs and
a multi-pass merge. This preserves event order without materializing the
table. Unordered reads remain the store's native stream.

A table that was never written reads as no rows under the shape asked for,
rather than raising: on the first interval of a fresh catalog every stage
reads an upstream its own upstream has not created yet. With neither a schema
nor a declared shape there is nothing to answer with, and the read raises.

## Write

```python
# A reader is consumed by the write it is handed to, so each takes its own.
logs.overwrite_arrow_reader(rows(), merge_by=True, commit_batch_num=8)
logs.append_arrow_reader(
    rows(), merge_by=True, commit_batch_num=8, commit_row_size=250_000
)
```

`overwrite_*` replaces the rows whose keys match and inserts the rest. A false
`merge_by` on a table partitioned by `identity`, `day`, `hour`, `month`,
`year`, `bucket`, or `truncate` makes the streamed rows the complete
replacement for their partitions; it remains invalid on an unpartitioned
table.

`append_*` inserts, skipping stored keys when `merge_by` names them and
inserting everything when it does not.

Appending to a missing table creates it. `merge_by=True` skips keys already
stored; write/upsert replaces them. Input batches accumulate to the requested
commit size. Schema additions are nullable and additive.

Keyed writes scope primary and explicit merge keys by the table's transformed
partition identity. Primary-key values are therefore unique within a partition;
the same value in another partition is another row. The raw partition source is
not row equality: two timestamps in the same `day(timestamp)` partition still
name one key.

An initial keyed chunk on an empty table lands every transformed partition in
one PyIceberg transaction. Later merge or insert chunks plan, read, and commit
one touched partition at a time. Deletes use
the matched stored row's source values, so their exact predicate cannot reach
the same key in another partition.

The default physical sort order is exactly the declared sort keys. Partition
transforms decide file placement independently and add no implicit sort
column.

A complete partition replacement applies the table's transforms with Arrow,
stages bounded local Parquet files, then copies them through the table's
configured filesystem to final Iceberg data locations.

One transaction drops only data files in the exact transformed partitions and
registers each new file from its footer; order-preserving transforms use
PyIceberg `add_files`, while bucket partitions carry the already-computed hash
into the same snapshot. Parquet bytes are never collected into an Arrow table
for the commit.

Local stages are removed after each upload, and cleanup attempts every
uploaded path even if one removal fails. A failure whose catalog
acknowledgement is ambiguous leaves the files for orphan maintenance rather
than risk deleting committed data.

Blind appends and complete-partition replacements retry transient catalog and
object-store failures four times by default. Each retry refreshes the branch,
checks the stable operation id, and uses exponential full jitter. A planned
keyed rewrite still settles a lost acknowledgement by operation id, but returns
a concurrent conflict so its caller can replay against current files and keys.
An acknowledgement lost after a successful commit is therefore not written a
second time. `commit_retries`, `retry_backoff`, and `retry_max_backoff` tune
that policy per dataset.

The source must keep each partition contiguous, and a recurrence after another
partition is rejected. Writes are incremental: complete groups committed
before a later source or ordering error remain committed.

Writes commit after eight source batches by default. `commit_batch_num` and the
optional `commit_row_size` are simultaneous bounds: the first one reached
commits. Complete partition replacements spill to staged Parquet files, so one
partition may cross either boundary without having to fit in memory.

Renaming, retyping or narrowing a column is not an additive Iceberg
evolution and no merge migrates one: recreate or rewrite the table, on every
retained branch, before appending. Dataset writes do not guess missing lineage
or keep dropped columns alive.

Every data verb accepts `branch`; reads also accept `snapshot_id`. `root`,
`main`, and `master` are aliases for Iceberg's physical `main` ref, so task
configuration does not depend on an organization's default-branch spelling.
A producer that does not provide Iceberg ids is numbered at creation, while a
contract that already carries ids keeps them.

## Delete

```python
from pyiceberg.expressions import EqualTo

removed = logs.delete_where("msgtype = '0'", commit_file_count=16)
removed += logs.delete(EqualTo("msgtype", "1"))

# No filter removes every row. A missing table is a zero-row no-op.
removed += logs.delete(branch="root")
```

Strings use PyIceberg's SQL predicate grammar; PyIceberg BooleanExpression
objects pass through unchanged. Planning prunes partitions and files first.
Each commit rewrites at most `commit_file_count` candidate files, and a partial
file is filtered one RecordBatch at a time. The return value is the number of
rows removed, so a replay that finds no match reports zero without committing.

### Arrow widths on read

A read hands back `string` and `binary`, not `large_string` and `large_binary`:

```python
from rekep import FixMsg
from rekep.iceberg import IcebergCatalog

catalog = IcebergCatalog(
    name="local",
    properties={"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "warehouse"},
)
logs = catalog.dataset("fix.market", field=FixMsg.into_field())
print(logs.read_arrow_reader().schema.field("msgtype").type)
```

```text
string
```

PyIceberg's configuration page documents `pyarrow.use-large-types-on-read`,
defaulting to true. **0.11.1 reads it nowhere** — no constant, no `Config()`
lookup, and setting `PYICEBERG_PYARROW__USE_LARGE_TYPES_ON_READ` moves
nothing. `schema_to_pyarrow` returns `pa.large_string()` outright, so a table
written from `string` columns read back wider than it was written and every
join against a value this package built itself had two widths to reconcile.

So the width is decided here instead, at the seam where the scan's shape is
built. That also keeps the reading stable across a PyIceberg that changes its
mind, since the dependency names no upper bound. Measured over 400,000 rows,
interleaved in one process, the cast does not show above host noise.

Nothing downstream depends on it either way: the transcription brings a batch
onto the raw declaration before its kernels see it, so a wide batch and a
narrow one meet the same code.

### Table properties

The tasks declare three, and every write task declares the same three:

```yaml
table_properties:
  history.expire.max-snapshot-age-ms: "604800000"   # a week of time travel
  write.metadata.previous-versions-max: "20"
  write.metadata.delete-after-commit.enabled: "true"
```

The last two bound the metadata directory. A commit writes a new
`*.metadata.json` and keeps every earlier one, so a pipeline making bounded
stream commits grows that directory for as long as it runs. Over 40
appends to one table:

| declared | `*.metadata.json` | `*.avro` |
| --- | ---: | ---: |
| neither | 41 | 80 |
| both | 21 | 80 |

`optimize` retrofits the same two values onto a table that lacks them, so this
is not a second mechanism -- it is the same one, applied from the commit that
creates the table rather than from the first maintenance pass to reach it. A
table that already carries them costs `optimize` no commit to set.

Four more are worth knowing about and are deliberately **not** declared:

| property | why not |
| --- | --- |
| `commit.manifest-merge.enabled` | measured *worse* at write time -- 84 manifests against 80 -- because a merge writes new ones and the originals stay until expiry. `optimize` sets it for the pass that also compacts and expires, which is where it pays. |
| `write.parquet.compression-codec` | PyIceberg already defaults to `zstd`. |
| `downcast-ns-timestamp-to-us-on-write` | every temporal column in these contracts is already `timestamp[us, tz=UTC]`, so nothing is downcast. |
| `pyarrow.use-large-types-on-read` | 0.11.1 reads it nowhere, so neither value changes anything. The width is decided in this package instead -- see [Arrow widths on read](#arrow-widths-on-read). |

`write.object-storage.enabled` inserts a hashed prefix before each data file
(`data/0010/0011/1010/10110100/…`), which spreads writes across S3 prefixes.
The tables partition hourly and commit far below S3's per-prefix request
ceiling, so it is off; turning it on changes every future data path and does
not move existing files.

## Filesystems

General resources are yggdryl `IOBase` handles. `rekep.resources.resource`
binds a local path, a URI resolved by PyArrow, or a raw path on an injected
`pyarrow.fs.FileSystem`. `IOBase` then owns traversal, codecs, decompression,
byte streams, text media, and Arrow record batches.

Iceberg keeps a narrower boundary. PyIceberg's `FileIO` owns table locations
and supplies its native input and output files over PyArrow streams. Yggfin's
`IcebergFileIO` adds transaction output tracking; it is not a second general
filesystem, URL, spill, or content-cache layer. A configured third-party
`py-io-impl` remains the table boundary and is wrapped only for the same output
ownership.

### What an `s3://` netloc names

An Iceberg warehouse URI names only its bucket and key:
`s3://example-bucket/rekep/warehouse`.

#### Settings a location carries

The location carries no endpoint or credentials. Keep it portable and
configure the store with PyIceberg's standard `s3.*` catalog properties:

```yaml
catalog:
  name: rekep-production
  properties:
    type: glue
    warehouse: s3://example-bucket/rekep/warehouse
    glue.region: eu-west-1
    s3.region: eu-west-1
    s3.endpoint: https://s3.example.net
    s3.access-key-id: "<from-secret-store>"
    s3.secret-access-key: "<from-secret-store>"
    s3.session-token: "<from-secret-store>"
```

`glue.region` configures the catalog service; `s3.region` configures the
warehouse. `s3.endpoint` selects an S3-compatible store, and the standard
credential properties or provider chain authenticate it. Do not put an
endpoint or credentials in the warehouse URI. These table properties do not
configure an independent capture source; inject that source's configured Arrow
filesystem when it needs different settings.

PyIceberg's native PyArrow FileIO does not consume `s3.profile-name` or
`s3.signer.*`; use the provider chain, the credential properties above, or a
configured `s3.role-arn`.

### Maintenance filesystem boundary

Orphan discovery asks the loaded table's `FileIO` for the exact configured
filesystem and path. It never rebuilds a store from the location string. Live
locations and listed paths are compared only after that resolution, so a MinIO,
Ceph, or other compatible warehouse is not accidentally scanned on AWS.

Yggdryl `0.1.1` does not yet expose the complete bound Arrow filesystem
contract or modification time. Until that parity lands, maintenance performs
its narrow directory listing through the table's PyArrow filesystem and uses
Arrow `FileInfo.mtime` for `orphan_age`. A missing mtime cannot prove an orphan
old enough, so the file is retained. Deletion is still bound to the same
filesystem and raw path through `IOBase.from_fs`.

### Encryption at rest

Turn it on at the **bucket**. A bucket whose default encryption is SSE-S3 or
SSE-KMS encrypts every object this package writes -- Parquet data, manifests,
manifest lists and metadata JSON alike -- and decrypts every one it reads, with
no configuration here and no change to a read or a write.

```bash
aws s3api put-bucket-encryption --bucket rekep-warehouse \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":
    {"SSEAlgorithm":"aws:kms","KMSMasterKeyID":"arn:aws:kms:eu-west-1:111122223333:key/…"}}]}'
```

`KMSMasterKeyID` is the custom-key setting. Yggfin does not translate Iceberg
`s3.sse.*` properties into per-request PyArrow headers. Where bucket-default
encryption is insufficient, configure a PyIceberg `py-io-impl` that implements
the required request headers; yggfin wraps it only to retain transaction output
ownership.

## Maintenance

```python
import datetime

logs.compact(branch="root")
logs.cleanup(retain=24, older_than=datetime.timedelta(days=7))
logs.optimize(
    branch="root",
    retain=24,
    older_than=datetime.timedelta(days=7),
    orphan_age=datetime.timedelta(days=3),
)
```

After a successful outermost append, insert, merge, or overwrite, the writer
expires old snapshots once. `snapshot_expiry` accepts a `timedelta` relative to
now, a `datetime`, or a parseable instant string. When omitted, the dataset
reads `history.expire.max-snapshot-age-ms` and then Iceberg's default.

Writers do not compact or sweep files. Expiration removes history from the
current metadata JSON -- it permanently removes time travel and rollback to an
expired state, and PyIceberg 0.11 deletes no physical files during it.

`cleanup` follows that commit with rekep's reachability sweep. Only files
unreferenced by every retained snapshot, branch, tag, statistics entry and
current metadata version are eligible: obsolete Parquet, manifest-list Avro,
manifest Avro and untracked metadata JSON. A live file whose recorded spelling
the sweep cannot reduce against the table's directories is kept, never deleted.
`orphan_age` is a grace period for files an in-flight writer produced but never
committed.

`optimize` compacts, then cleans, and returns counts for each. It also
retrofits absent manifest-merge and metadata-retention properties; an explicit
table property still wins.

The shipped schedule and policy live in the
[Airflow deployment](../pipeline/operations/airflow.md#run-iceberg-maintenance)
guide. Run it when ordinary writers are quiet: commit conflicts fail safely and
retry, but avoiding overlap prevents wasted rewrites.

## Testing and benchmark

Long transaction suites are marked `integration`; normal pull-request tests
cover planning and pure helpers. `python/benchmarks/bench_iceberg.py --quick`
measures focused commits, scans, merges, and maintenance—not a full pipeline.
