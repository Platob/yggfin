# Iceberg datasets

rekep keeps Iceberg at the PyIceberg/PyArrow boundary. Nothing below it owns
catalogs, snapshots, scan planning, or commits. A task reaches a table as an
`IcebergDataset`, through `Storages.dataset` or one layer's
`IcebergCatalog.dataset`; the examples below are its verbs.

```python
from rekep.iceberg import IcebergCatalog
from rekep.text import log_message_field

catalog = IcebergCatalog(
    name="bronze",
    properties={
        "type": "sql",
        "uri": "sqlite:///bronze.db",
        "warehouse": "bronze",
    },
)
messages = catalog.dataset("record_keeping.log_messages", field=log_message_field())
```

## Stream writes

```python
written = messages.merge_arrow_reader(reader, log_message_field())
```

Three verbs write a stream. Each creates a missing table and returns the rows
it wrote:

- `append_arrow_reader` appends the differences: under a key, only the rows
  whose key their transformed partition does not hold. It never rewrites or
  deletes a stored file, so a stored row holding other values stays as it
  is. It returns the rows added.
- `merge_arrow_reader` upserts the differences under a key: a row whose key
  is absent is inserted, one whose stored row holds other values replaces it,
  and one stored as it is is left alone. Only a file holding a replaced row
  is rewritten, and a key stored twice is collapsed into one row. Values are
  compared as a staged file would hold them: PyIceberg stores a null list of structs as
  an empty one, so the two are the same row. It returns the rows inserted or
  replaced.
- `overwrite_arrow_reader` replaces and takes no key: given a `row_filter`,
  exactly the rows it selects, in one commit
  ([below](#atomic-predicate-replacement)); without one, every row of the
  partitions the stream touches, each emptied once per write and only added
  to after that. An unpartitioned table needs a `row_filter`.

`merge_by` names the key. None is the primary key declared on the native
Field, and an append to a field declaring none is blind; True requires that
key, a list of columns names another, and `False` makes an append blind. A
merge requires a key. The primary key is `curruuid` alone on every keyed
table of the graph: on bronze `log_messages` it identifies a line, on both
`fix_messages` tables a settled event. A key is scoped to its transformed
partition -- the same key on two days is two rows, and a null partition value
is a partition of its own. A key that recurs within the stream keeps its
first row in the table's sort order -- across commits too, because a merge
carries the keys it settled in the partition it is writing to that
partition's next chunk -- and a null or NaN key is refused,
because no join matches it. So a replay through a keyed append or a merge
writes nothing, commits nothing and returns 0, and the table holds each key
once however often a window runs. Iceberg identifier fields describe identity
but do not enforce uniqueness: a blind append of a replay duplicates it.

Every verb takes a schema-bearing `RecordBatchReader` and consumes it one
batch at a time; the `*_arrow_batch` and `*_arrow_table` helpers build that
reader.

A commit holds `commit_row_size` rows or the rows of `commit_batch_num` input
batches, whichever bound is reached first, however many partitions they
span. Each defaults to the dataset's own -- eight batches and no row bound
unless configured -- either may be None, and with neither the whole stream is
one commit. The tasks cut by `commit_row_size` alone
([commits](../tasks/index.md#commits)). Under a `row_filter` the bounds size
staging chunks instead, as the next section says.

### Atomic predicate replacement

Pass a SQL or PyIceberg `row_filter` to replace exactly its matching rows in
one atomic snapshot. `parse_books`, `parse_orders`, `parse_quotes` and
`parse_executions` use this mode with strict `start <= currunix < end`,
without an epoch or null exception:

```python
from rekep.market import book_field, market_window_filter
from rekep.times import window_of

window = window_of("2026-09-21T10:00:00Z", "2026-09-21T10:00:10Z")
books = catalog.dataset("record_keeping.books", field=book_field())
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

This mode retains source duplicates. It replaces the selected row set, not
individual incoming keys. `commit_batch_num` and `commit_row_size` bound
staging chunks rather than the number of commits: completed chunks reside in
the table's `FileIO`, and only file metadata is retained until the single
removal-and-addition commit. Whole-hour replacement and keyed writes do not
provide these partial-window semantics.

### What a commit holds

Every write lays its rows out in the table's order before it commits any of
them. The stream is spilled to a local Arrow IPC folder under the dataset's
`spill_directory` -- None is the system's temporary directory, which
`TMPDIR` moves -- as one sorted run per bounded chunk and transformed
partition, in one folder per partition. Each partition is then merged back,
at most 16 runs open at once, in partition order, and cut again into commits
of `commit_row_size` rows, or of the largest chunk spilled when only
`commit_batch_num` bounds them. So:

- each partition's files hold disjoint ranges in the table's sort order,
  which an ordered read concatenates rather than merges and a filter on a
  sort column prunes by row group;
- memory holds one chunk and one merge step of at most half a chunk, never
  the stream: each step sorts a window of every run, so overlapping runs do
  not add up -- 300 overlapping runs of one partition held 1.8 chunks;
- the stream is consumed before the first commit, so a producer that fails
  commits nothing.

A table with neither partitions nor a sort order has nothing to lay out: its
stream goes straight to its commits.

The merge takes a block at a time: from every run's current batch, the rows
that sort no later than the least of those batches' last rows, sorted once, so
the work in Python is per batch whatever the input order. Measured on 300,000
rows of 200-byte payload over 48 hour partitions, a keyed first write took
1.1 s from sorted input and 1.0 s from shuffled input, against 1.0 s and 1.7 s
for the previous direct chunked write, and an ordered read of the shuffled
landing took 0.9 s against 15.9 s, because its files no longer overlap.

Each commit stages its chunk one transformed partition at a time, streamed
through PyIceberg's Parquet writer into the table's configured `FileIO`, then
published by path. What the write holds past the chunk is one partition
rather than every partition's rows, and no data file touches local disk: the
writer opens the store's own output stream, and the statistics the commit
records are what it answers on closing the file rather than what a second
read of the file would find.

A keyed write never scans the table. Each chunk plans once, over one range
per transformed partition it carries -- the partition source's, widened to
the hours or days a time transform partitions by, and that partition's least
and greatest key -- so it opens no partition between two it carries and,
within each, only the files whose key bounds overlap that partition's keys.
Of each planned file it reads the key columns first, a batch at a time: an
append drops the rows whose key the file holds and stops once no row is left
to decide. A merge reads a file whole only where it holds a key the chunk
carries, to compare those rows, and rewrites it only where it holds a row the
chunk replaces: streamed back through the same stager without those keys, a
batch at a time, and published in the chunk's commit. A file the chunk
replaces nothing in stands as it was.

Measured on pyiceberg 0.12 and pyarrow 25 over eight batches of 20,000 rows
of 200-byte payload, written as one commit, as the Arrow high-water mark over
the write divided by the chunk:

| write | 1 partition | 2 partitions | 4 partitions | 16 partitions |
| --- | ---: | ---: | ---: | ---: |
| `append_arrow_*`, `merge_arrow_*` or `overwrite_arrow_*` of new keys | 1.05 | 2.01 | 1.52 | 1.14 |
| `merge_arrow_*` replaying every row | 2.23 | 2.01 | 1.52 | 1.14 |

Every write holds under 2.25 chunks, worst where a chunk splits into two
interleaved halves, which is what
`test_a_bounded_write_holds_a_bounded_multiple_of_its_chunk` pins. A merge
replaying every row also holds a batch of the stored rows its keys match, and
compares them `COMPARE_ROW_SIZE` (16,384) pairs at a time: compared all at
once, one stored partition holding the whole chunk measured 3.66 chunks.

Staged files record the order they were written in, which is what lets
`order_by` read them back without sorting each one again. A table whose
recorded order the shape cannot hold -- a transformed sort field, one ordering
its nulls against its direction's default, a nested column -- is written
unsorted and says so. A file that does not record an order
is sorted on every read, through Arrow IPC runs on local disk; over four files
of a 524,288-row table, dropping that pass took a warm ordered read
from 85 ms to 62 ms and wrote no temporary file at all.

One data file is open at a time, and it is the table's own: a warehouse on
local disk writes each data byte once. The local stages are Arrow IPC, never
Parquet: a write's spill, which holds the stream's rows once, and the
external sort above, which an ordered read makes of files that do not record
their order.

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
head, and the retry is validated against what landed in between. Every write
but a blind append declares a predicate: a keyed write the partition and key
ranges it planned by, an overwrite the rows it takes out -- the partitions it
empties, or its `row_filter`. A commit since the plan that added or removed
rows under that predicate is a conflict: the write raises
`CommitFailedException` for a fresh plan, with nothing of its own left
behind, so no key lands twice. Added rows are judged under the table's
`write.delete.isolation-level`: `serializable`, Iceberg's default, refuses
them; `snapshot` admits them, and with them a concurrent writer's row of the
same key. A keyed chunk that finds nothing to commit still checks the head it
read, and raises the same way where it moved under it. A commit anywhere else
in the table is no conflict, and the retry lands; two windows run side by
side land whichever order their commits arrive in. A blind append declares
nothing and conflicts with nothing.

Set `merge_schema=True` on a dataset, or on an `append_arrow_*`,
`merge_arrow_*` or `overwrite_arrow_*` write, to add columns from its
authoritative write Field before the first batch is consumed:

```python
from rekep import Field

fix_field = Field.from_arrow_schema(reader.schema, name="FixMsg")
fixes = catalog.dataset(
    "record_keeping.fix_messages",
    field=fix_field,
    merge_schema=True,
)
fixes.append_arrow_reader(reader, fix_field)
```

Pass the current Field explicitly when its reader may be newer than the stored
table. Every task but `parse_log_messages` enables this mode; the
`log_messages` row is the text read's own and stays fixed. It is additive
only: existing types, nullability, comments, field IDs, identifier fields,
partition specs, and sort orders do not change. Iceberg assigns IDs to
additions. A column added to an existing table must be nullable because older
rows have no value for it; the first write creates a missing table directly
from its Field. Schema updates are table-wide even when rows are written to a
branch. A write with no new column makes no schema commit.

The seven tables share four fields: the text read's row for bronze
`log_messages`, the dictionary's fixed FIX row for both `fix_messages`, the
book fold's row for `books`, and its execution child for `orders`, `quotes`
and `executions`. [Tables](../tables/index.md) lists them; their contracts
under `schemas/` are generated output, not alternate schemas.

`merge_schema=True` cannot retire or rename columns or translate native
identities. An obsolete schema or identity contract requires rebuilding the
affected tables and every table after them, as
[a table's shape changed](index.md#a-tables-shape-changed) says. Exact
window replacement removes stale rows inside its predicate but does not
repair an incompatible table schema.

Before either write, the native `Field` applies its declarations in dependency
order: **cast → derived partition columns → digest holders**. All seven ingestion tables
lay out on `currunix` alone, the hour transform over the event's own instant --
what the message stated on a FIX row, what the read settled over the line on a
text row -- and none materializes a second layout column beside it. The
`log_messages` row derives nothing at all: every column of it is one the read
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

Every order here takes a null as the least value: first ascending and last
descending, the null order Iceberg records for each direction by default, and
the one a created table's sort order records. A NaN follows every number
either way. An event's `seqnum` is null at place zero among the events of its
instant, so the first of them reads first.

An ordered read streams already-disjoint file ranges directly. Overlapping
ranges are externally sorted and merged through Arrow IPC scratch, with at
most 16 file streams in one merge step; closing the reader also closes sources
and removes scratch files. This bounds open-file fan-in without collecting the
whole scan into a table before its first output batch.

Identifier behavior does not depend on a column spelling or value type. UUID,
integer, and string identifiers all follow the declared Iceberg field: a
keyed append skips, and a merge replaces where it differs, a match only
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
  "name": "production",
  "properties": {
    "type": "glue",
    "warehouse": "s3://warehouse-bucket/rekep",
    "glue.region": "eu-west-1",
    "s3.region": "eu-west-1",
    "s3.endpoint": "https://s3.example.net"
  }
}
```

Credentials belong in the provider chain or secret-backed `s3.*` properties,
never in a catalog mapping committed beside the code.

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
  "name": "production",
  "properties": {
    "type": "s3tables",
    "warehouse": "arn:aws:s3tables:eu-west-1:123456789012:bucket/market-tables"
  }
}
```

The warehouse is read as the `rekep.Uri` it is, with no regular expression
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
[AWS S3 Tables](catalogs.md#aws-s3-tables).

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

A catalog-wide pass is `optimize` over `IcebergCatalog.datasets`, which yields
one dataset per table of a namespace, or of every namespace recursively when
it names none:

```python
import datetime
import tempfile
from pathlib import Path

from rekep import Storages
from rekep.pipeline import parse_log_messages
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
storages = Storages.from_dict(
    {
        layer: {
            "name": layer,
            "properties": {
                "type": "sql",
                "uri": f"sqlite:///{root / layer}.db",
                "warehouse": str(root / layer),
            },
        }
        for layer in ("bronze", "silver", "gold")
    }
)
with storages:
    day = window_of("2026-08-14", "2026-08-14")
    parse_log_messages("file:data/capture/ulbridge.log", storages, day)
    bronze = storages.catalog("bronze")
    reports = {}
    for dataset in bronze.datasets():
        try:
            reports[dataset.identifier] = dataset.optimize(
                branch="root",
                retain=24,
                older_than=datetime.timedelta(days=7),
                orphan_age=datetime.timedelta(days=3),
                remove_orphans=bronze.table_bucket is None,
            )
        finally:
            dataset.close()
    assert set(reports) == {"record_keeping.log_messages"}
    report = reports["record_keeping.log_messages"]
    assert set(report) >= {"rewritten", "expired", "deleted", "bytes"}
```

| keyword | meaning |
| --- | --- |
| `min_files` | compaction threshold, 2 by default |
| `retain`, `older_than` | how much time travel is preserved: the last `retain` snapshots and every one newer than `older_than` survive, beside every ref's own |
| `orphan_age` | protects files from active or recently failed writers, three days by default |
| `remove_orphans`, `metadata` | enable the orphan sweep, and include the metadata directory in it |
| `branch` | `root`, `main` and `master` all select the Iceberg root branch |

`retain=0` keeps no snapshot but the refs' own heads. Each report is keyed by
the table's full identifier inside its catalog -- `record_keeping.log_messages`
-- so equal table names in different namespaces cannot collide; a sweep over
all three layers is one pass per `storages.catalog(layer)`.

Run long transaction checks explicitly with `pytest -m integration`.
