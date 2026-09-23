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

`merge_by=True` uses the primary key declared on the native Field. It is
`curruuid` alone for `logs.messages`, `fix.raw`, and `fix.refined`; a
`logs.messages` UUID identifies a line and a FIX UUID identifies a settled
event. A parse answers one row per message; the object a line was read from
stays on `logs.messages` and identifies no event row. A missing table is
created.
Without `row_filter`, `commit_batch_num` and the optional `commit_row_size`
bound each storage commit independently from input batch size, however many partitions the
bounded chunk spans: its parts are staged one at a time and committed together.

Two verbs, and each returns the rows it wrote:

- `append_arrow_reader` is blind: every row lands, whatever the table holds.
- `overwrite_arrow_reader` replaces. Without `row_filter`, each bounded chunk is written to the
  store one partition at a time, the stored rows it replaces are taken out,
  and the written files are appended in the same commit. Under `merge_by`
  those rows are the ones carrying the chunk's keys in the same transformed
  partition -- the same key on two days is two rows, and a null partition
  value is a partition of its own; with `merge_by=False` on a partitioned
  table they are every row of the partitions the chunk touches, emptied once
  per write and only added to after that.

Both APIs require a schema-bearing `RecordBatchReader` and consume one batch
at a time. The batch and table helpers build that reader.

A keyed replay lands the same rows: the table
holds each key once however often the window runs, and `written` reports what
the run carried rather than what it changed. A key that recurs within a chunk
keeps its first row; one that recurs in a later chunk replaces the row the
earlier chunk landed. A null or NaN key is refused, because no join finds the
row it would replace.

The keyed replace path is also the optimized append path. It prunes manifests and
files with the incoming partition and `curruuid` bounds. If none contains a
matching key, the commit is an Iceberg append; only an actual match becomes an
overwrite that rewrites the affected file. This keeps first-seen windows on
the cheap append operation without making retries blind. Iceberg identifier
fields describe identity but do not enforce uniqueness, so calling blind
`append_arrow_reader` for an idempotent pipeline would duplicate a replay.

### Atomic predicate replacement

Pass a SQL or PyIceberg `row_filter` to replace exactly its matching rows in
one atomic snapshot. The four market tasks use this mode with strict
`start <= currunix < end`, without an epoch or null exception:

```python
from rekep.market import book_field, market_window_filter
from rekep.times import window_of

window = window_of("2026-09-21T10:00:00Z", "2026-09-21T10:00:10Z")
books = catalog.dataset("market.books", field=book_field())
written = books.overwrite_arrow_reader(
    book_reader,
    book_field(),
    row_filter=market_window_filter(window),
)
```

Here `book_reader` carries the native books selected for that window. Every
incoming row must satisfy the predicate; an outside row or source failure
publishes nothing. An empty source still removes existing matching rows.
Rows outside the predicate survive, including those in the same hour
partition. Concurrent changes inside the selected predicate cause a commit
conflict rather than a partial replacement.

This mode ignores `merge_by` and retains source duplicates. It replaces the
selected row set, not individual incoming keys. `commit_batch_num` and
`commit_row_size` bound staging chunks rather than the number of commits:
completed chunks reside in the table's `FileIO`, and only file metadata is
retained until the single removal-and-addition commit. Whole-hour keyless
replacement and ordinary keyed replay do not provide these partial-window
semantics.

### What a commit holds

Every verb stages a bounded chunk by splitting it into its transformed
partitions, and each partition is taken out of the chunk, streamed through
PyIceberg's Parquet writer into the table's configured `FileIO`, then published
by path. Ordinary writes commit each chunk; predicate replacement commits
the complete staged selection atomically. What the write holds past the chunk it was handed is one partition
rather than every partition's rows, and nothing touches local disk on the
way: the writer opens the store's own output stream, and the statistics the
commit records are what it answers on closing the file rather than what a
second read of the file would find.

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

One file is open at a time, and it is the table's own: a warehouse on local
disk writes each byte once, and a write needs no temporary space for its data
files. The one local stage left is the external sort above, which an ordered
read makes of a file that does not record its order.

A commit's files are encoded one after another, on the thread that called the
write, each streamed to the store as its row groups close -- so on an object
store the upload of one row group overlaps the encoding of the next, through
Arrow's own output stream. It is less work than splitting a chunk and handing
the parts to a pool, because it never copies a partition out of the chunk,
but no two files overlap, so a chunk spanning many partitions finishes later
on a machine with cores to spare. On the shapes this pipeline writes -- a
chunk spanning an hour or two -- that is one or two files; a table
partitioned into many parts per commit, a bucket spec among them, pays per
part.

A refused commit deletes what it wrote, and a commit whose acknowledgement is
lost leaves its files for the orphan sweep to settle rather than deleting rows
that may be live.

A commit another writer beats is retried by PyIceberg against the refreshed
head, and the retry is validated against what landed in between. An
overwrite declares the rows it takes out -- the key bounds a keyed replace
planned by, the partitions a keyless one empties, the predicate a delete
names -- and a commit since the plan that added or deleted rows under that
predicate is a conflict: the write raises `CommitFailedException` for a fresh
plan, with nothing of its own left behind. A commit anywhere else in the
table is not, and the retry lands; two windows replayed side by side land
whichever order their commits arrive in. A blind append conflicts with
nothing.

Set `merge_schema=True` on a dataset, or on an `append_arrow_*` or
`overwrite_arrow_*` write, to add columns from its authoritative write Field
before the first batch is consumed:

```python
from rekep import Field

fix_field = Field.from_arrow_schema(reader.schema, name="FixMsg")
fixes = catalog.dataset(
    "fix.refined",
    field=fix_field,
    merge_schema=True,
)
fixes.append_arrow_reader(reader, fix_field)
```

Pass the current Field explicitly when its reader may be newer than the stored
table. This mode is enabled by `parse_fix_raw` and `parse_fix_refined`; the
`Message` contract remains fixed. It is additive only: existing types,
nullability, comments, field IDs, identifier fields, partition specs, and sort
orders do not change. Iceberg assigns IDs to additions. A column added to an
existing table must be nullable because older rows have no value for it; the
first write creates a missing table directly from its Field. Schema updates
are table-wide even when rows are written to a branch. A write with no new
column makes no schema commit.

Seven pipeline tables share four runtime-derived schemas: Message for
`logs.messages`; FixMsg for `fix.raw` and `fix.refined`; Book for
`market.books`; and MarketEvent for `market.orders`, `market.quotes` and
`market.executions`. Book derives from the native empty book reader, while
MarketEvent derives from its execution child. Their reviewed
[contracts](../contracts/index.md) are generated output, not alternate schemas.

`merge_schema=True` cannot retire or rename columns or translate native
identities. An obsolete schema or identity contract requires rebuilding
affected source tables and dependent products under the pinned release;
see [deployment](../pipeline/operations/deploy.md). Exact market-window
replacement removes stale rows inside its predicate but does not repair an
incompatible table schema.

Before either write, the native `Field` applies its declarations in dependency
order: **cast → derived partition columns → digest holders**. All seven ingestion tables
lay out on `currunix` alone, the hour transform over the event's own instant --
what the message stated on a FIX row, what the read settled over the line on a
text row -- and none materializes a second layout column beside it. The
`Message` contract derives nothing at all: every column of it is one the read
already states.

Three declarations look similar and are not:

| declaration | key | what it does |
| --- | --- | --- |
| `partition_key()` | `field:partition` | physical layout marker; rekep maps it to an identity Iceberg spec |
| `partition_key("hour")` | `ICEBERG:partition_key` | a non-identity Iceberg transform |
| `derived_from(...)` | `PARTITION:sources` | an executable Arrow derivation, computing a real column |

A derived transform such as `day` may also appear under `PARTITION:transform`;
it computes a separate Arrow column and does not define the table spec. The
identity and transformed Iceberg markers are mutually exclusive -- a field
carrying both is rejected at declaration and at spec conversion. Iceberg itself
may declare several transforms over one source column, so schema projection
omits that ambiguous marker and leaves the table's `PartitionSpec`
authoritative for storage planning.

## Stream reads

```python
reader = messages.read_arrow_reader(
    columns=("crosscode", "seqnum", "body"),
    row_filter="seqnum >= 1000",
    order_by=("seqnum",),
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

An ordered read streams already-disjoint file ranges directly. Overlapping
ranges are externally sorted and merged through Arrow IPC scratch, with at
most 16 file streams in one merge step; closing the reader also closes sources
and removes scratch files. This bounds open-file fan-in without collecting the
whole scan into a table before its first output batch.

Identifier behavior does not depend on a column spelling or value type. UUID,
integer, and string identifiers all follow the declared Iceberg field:
`append_arrow_reader` is blind, while overwrite-by-key replaces matches only
inside the affected partition. FIX happens to declare `curruuid`; the storage
layer has no FIX-specific key or merge branch.

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

AWS S3 Tables is the one type rekep resolves itself, because a table bucket is
served by an Iceberg REST catalog AWS hosts -- at two endpoints, which the
`warehouse` chooses between, since each names the bucket its own way:

| `warehouse` | endpoint | signed for |
| --- | --- | --- |
| `arn:aws:s3tables:<region>:<account>:bucket/<name>` | `https://s3tables.<region>.amazonaws.com/iceberg` | `s3tables` |
| `s3tables://<name>?region=<region>&account=<account>` | the same, or the one the locator states | `s3tables` |
| `<account>:s3tablescatalog/<name>` | `https://glue.<region>.amazonaws.com/iceberg` | `glue` |

```json
{
  "catalog": {
    "name": "production",
    "properties": {
      "type": "s3tables",
      "warehouse": "arn:aws:s3tables:eu-west-1:123456789012:bucket/market-tables"
    }
  }
}
```

The warehouse is read as the `yggdryl.Uri` it is, with no regular expression
of rekep's own. An ARN redirects through `Arn.locator()` to the `s3tables:`
URL its bucket spells, and that locator is the second spelling: the same
bucket with the two fields a location does not carry, `region` and `account`,
in its query -- and `partition` where the bucket is outside `aws`. Both
resolve through one reading of the locator, which refuses anything under the
bucket, because a table's locator names a table and not a catalog, and spells
the ARN back for the endpoint, which takes the bucket under that name and no
other. What the locator can say that the ARN cannot is where the endpoint is:
its host, with a port and a `scheme` where an emulator answers on one, or
`endpoint_override` in its query, spelled exactly as an `s3:` URL spells a
MinIO endpoint here. The ARN states its region; a locator states one under
`region` or takes it, as the Glue name does, from `rest.signing-region` or the
worker's AWS environment. Anything else the warehouse does not decide -- a
`uri` stated outright, another signing region, explicit `s3.*` settings -- is
kept exactly as stated.
`IcebergCatalog.table_bucket` answers the warehouse for such a catalog and
`None` for every other, which is how maintenance knows whose files it is
looking at and why a drop there purges. The extra is `rekep[s3tables]`:
pyiceberg signs those REST calls through boto3. Which door to take, and what
Lake Formation asks for behind the Glue one, is in
[AWS S3 Tables](../pipeline/operations/deploy.md#aws-s3-tables).

The worker's environment is the third way to state a table bucket's endpoint,
and a default for every S3 Tables catalog the worker runs rather than a
setting of one: a `uri` stated outright wins, then the locator's endpoint,
then the environment, then the partition's regional endpoint. It is read from
the variables the AWS CLI reads -- the variables alone, not a profile's
`endpoint_url` or `services` section:

| variable | states |
| --- | --- |
| `AWS_ENDPOINT_URL_S3TABLES` | the `uri` for an ARN or a locator, with `/iceberg` added unless its path ends in it |
| `AWS_ENDPOINT_URL_GLUE` | the `uri` for a `<account>:s3tablescatalog/<name>`, the same way |
| `AWS_ENDPOINT_URL_S3` | `s3.endpoint`, where the table files are read and written |
| `AWS_ENDPOINT_URL` | `s3.endpoint` only, where `AWS_ENDPOINT_URL_S3` is not set |
| `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true` | that none of the above is read |

The generic `AWS_ENDPOINT_URL` names one endpoint for every service at once.
That suits the files, which are read through S3 alone, but not the catalog: a
table bucket has two doors, S3 Tables and Glue, and one value cannot be right
for both, so it is never read for the `uri`. Any other catalog type takes
`s3.endpoint` only as a property: pyiceberg builds Arrow's S3 filesystem
itself, and that reads neither variable.

## Message schema replacement

The `Message` contract is the native event layout: `currunix`, `curruuid`,
`currhashcode`, `crosscode` and `seqnum` as the read states them, the line
past its header, and the ULBridge header captures beside them. It derives
nothing, is keyed only by `curruuid`, and is laid out by the hour of
`currunix` alone. A `logs.messages` written under yggdryl 0.1.9 or earlier
holds other identities under other field ids and is not evolved into this
shape: the table is dropped, recreated from `Message.into_field()` by
`rekep iceberg deploy`, and its captures replayed -- together with both FIX
tables and dependent market or optional SQL products, whose provenance and keys join to it. rekep
carries no legacy name, timestamp-type, digest-name, or partition-layout
compatibility path.

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
Under an S3 Tables table bucket it removes nothing: the service writes and
deletes those files as it compacts, and the bucket behind a table is not one
the account lists, so no listing here can settle a file's ownership. The sweep
reports `deleted: 0` and records which bucket keeps its files.
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
`fix.refined` -- so equal table names in different namespaces cannot collide.

Run long transaction checks explicitly with `pytest -m integration`.
