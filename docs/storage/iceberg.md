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
messages = catalog.dataset("logs.messages", field=Message.into_field())
```

## Stream writes

```python
written = messages.overwrite_arrow_reader(
    reader,
    Message.into_field(),
    merge_by=True,
)
```

`merge_by=True` uses the primary key declared on the native Field:
`(sourceurl, rownum)` for `logs.messages`, and `(sourceurl, rownum, msghash)`
for `fix.messages`, where a parse answers one row per message and a source URL
and row number alone therefore name no row. A missing table is created.
`commit_batch_num` and the optional `commit_row_size` bound each storage
commit independently from input batch size, however many partitions the
bounded chunk spans: its parts are staged one at a time and committed together.

Two verbs, and each returns the rows it wrote:

- `append_arrow_reader` is blind: every row lands, whatever the table holds.
- `overwrite_arrow_reader` replaces. Each bounded chunk is staged locally, the
  stored rows it replaces are taken out, and the staged files are appended in
  the same commit. Under `merge_by` those rows are the ones carrying the
  chunk's keys in the same transformed partition -- the same key on two days
  is two rows, and a null partition value is a partition of its own; with
  `merge_by=False` on a partitioned table they are every row of the
  partitions the chunk touches, emptied once per write and only added to
  after that.

Both APIs require a schema-bearing `RecordBatchReader` and consume one batch
at a time. The batch and table helpers build that reader.

A replay of a window is therefore a commit that lands the same rows: the table
holds each key once however often the window runs, and `written` reports what
the run carried rather than what it changed. A key that recurs within a chunk
keeps its first row; one that recurs in a later chunk replaces the row the
earlier chunk landed. A null or NaN key is refused, because no join finds the
row it would replace.

### What a commit holds

Every verb writes the same way: a bounded chunk is split into its transformed
partitions, and each partition is taken out of the chunk, written to a local
Parquet file, uploaded through the table's configured `FileIO`, and committed
by path. What the write holds past the chunk it was handed is one partition
rather than every partition's rows.

A keyed replace plans the stored files to read from the chunk's key bounds --
between each key column's least and greatest value, and between the partition
source's, widened to the hours or days a time transform partitions by -- keeps
the ones in the partitions the chunk carries, and reads each one a batch at a
time, writing it back through the same stager without the rows an Arrow
anti-join on the keys drops. A file every row of which survives stands
as it was; one that loses rows is replaced by its rewrite; one that loses
every row is deleted unread the next time round. One commit per chunk carries
the deletions, the rewrites and the chunk together.

Measured on pyiceberg 0.12 over 20,000 rows of 200-byte payload in eight
batches, as the Arrow high-water mark over one commit divided by the chunk:

| verb | 1 partition | 2 partitions | 4 partitions | 16 partitions |
| --- | ---: | ---: | ---: | ---: |
| `append_arrow_*` | 1.55 | 1.06 | 0.80 | 0.55 |
| `overwrite_arrow_*`, `merge_by`, replaying every row | 1.55 | 1.11 | 1.11 | 1.11 |

Under 2.25 chunks in every case, which is what
`test_a_bounded_write_holds_a_bounded_multiple_of_its_chunk` pins. A chunk
that overlaps what is stored reads the keys of each stored file it plans
first, so a file it replaces whole is never decoded past them; one it keeps
part of is then streamed one batch at a time, so what it holds beside the
chunk is one batch of that file rather than the file.

Staged files record the order they were written in, which is what lets
`order_by` read them back without sorting each one again. A table whose
recorded order the shape cannot hold -- a transformed sort field, a
nulls-first one, a nested column -- is written unsorted and says so. A file that does not record an order
is sorted on every read, through Arrow IPC runs on local disk; over four files
of that same 524,288-row table, dropping that pass took a warm ordered read
from 85 ms to 62 ms and wrote no temporary file at all.

The local stage is a bounded file and not a copy of the commit: one file
exists at a time and it is deleted as soon as it is uploaded, so a commit
needs up to `write.target-file-size-bytes` of free space wherever Python puts
temporary files -- `TMPDIR` on POSIX, which on a container is often a tmpfs
and therefore memory. A warehouse on that same local disk writes those bytes
twice, once to the stage and once to the table.

A commit's files are encoded and uploaded one after another, on the thread
that called the write. It is less work than splitting a chunk and handing the
parts to a pool -- measured, less processor time -- but none of it overlaps,
so a chunk spanning many partitions finishes later on a machine with cores to
spare, and a warehouse on an object store pays one round trip per partition
rather than overlapping them. On the shapes this pipeline writes -- a chunk
spanning an hour or two -- that is one or two of each; a table partitioned
into many parts per commit, a bucket spec among them, pays per part.

A refused commit deletes what it uploaded, and a commit whose acknowledgement
is lost leaves its files for the orphan sweep to settle rather than deleting
rows that may be live.

Set `merge_schema=True` on a dataset, or on an `append_arrow_*` or
`overwrite_arrow_*` write, to add columns from its authoritative write Field
before the first batch is consumed:

```python
from rekep import Field

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
order: **cast → derived partition columns → digest holders**. Both published
tables lay out on `timepartition` alone — the hour transform over the capture
`timestamp` — and neither materializes a second layout column beside it.

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
    columns=("sourceurl", "rownum", "body"),
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

The current raw contract uses `sourceurl`, `rownum`, the ULBridge header
fields, `bodyhash`, and a derived `timepartition` with an Iceberg `hour`
transform. Recreate an older messages table from `Message.into_field()` and
reingest its source captures; rekep carries no legacy name, timestamp-type,
digest-name, or partition-layout compatibility path.

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
