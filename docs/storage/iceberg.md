# Iceberg

`IcebergDataset` exposes a table as an Arrow stream. Pyiceberg owns planning,
schema ids, snapshots, and commits; rekep supplies recursive casting,
filesystem normalization, commit grouping, and maintenance policy.

```python
from rekep import FixMsg
from rekep.iceberg import IcebergDataset

logs = IcebergDataset(
    field=FixMsg.into_field("fix.market"),
    catalog="local",
    properties={
        "type": "sql",
        "uri": "sqlite:///catalog.db",
        "warehouse": "file:///warehouse",
    },
    branch="root",
)
```

The field name is the full table identifier; `logs.namespace` is `"fix"`.

## Read

```python
reader = logs.read_arrow_reader(
    row_filter=filter,
    columns=["unix", "hash", "Symbol"],
    order_by=["unix", "MsgSeqNum", "hash"],
    snapshot_id=None,
    branch="root",
)
```

Filters and projections reach scan planning. Ordered reads sort planned
partition paths deterministically, stream one partition at a time, and rely on
the declared in-file sort order. This bounds merge state and preserves event
order without materializing the table. Unordered reads remain the store's
native stream.

A table that was never written reads as no rows under the shape asked for,
rather than raising: on the first interval of a fresh catalog every stage
reads an upstream its own upstream has not created yet. With neither a schema
nor a declared shape there is nothing to answer with, and the read raises.

## Write

```python
logs.overwrite_arrow_reader(reader, merge_by=True, commit_row_size=250_000)
logs.append_arrow_reader(reader, merge_by=True, commit_row_size=250_000)
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

Each bounded input chunk is split by transformed partition, and merge or
insert plans, reads, and commits one touched partition at a time. Deletes use
the matched stored row's source values, so their exact predicate cannot reach
the same key in another partition.

The default physical sort order starts with partition source columns before the
declared sort keys. A transformed partition already decides file placement;
ordering its source within that file keeps useful timestamp, truncated-value,
or bucket-source locality instead of recording a constant transformed value.

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

The source must keep each partition contiguous, and a recurrence after another
partition is rejected. Writes are incremental: complete groups committed
before a later source or ordering error remain committed.

Replacements accumulate toward `commit_row_size`; the default is 1,000,000
rows and `0` groups the whole ordered stream into one transaction. The same
setting caps each staged Parquet file, so an individual partition may exceed
it without having to fit in memory.

The market-contract cutover is not an additive Iceberg evolution. It renames
Book payloads, types `linked_events`, requires collections, removes event
fields, requires the generic `Message.entries` and nested argument values,
renames the FixMsg sequence, and renames `unix_hour` to `unix_partition` while
rescaling its epoch-nanosecond `long` values to epoch-second `int`.

All of that needs an explicit table migration or recreation. Recreate or
rewrite every table using one of the six pipeline contracts, on every retained
branch, before appending: an ordinary merge cannot migrate the renamed,
rescaled, narrowed partition field. Dataset writes do not guess missing
lineage or keep retired columns alive.

Rebuild `logs.messages` as part of that cutover: its `hash` now identifies
only the exact message payload, so it cannot be mixed with rows written by the
previous provenance-framed identity.

Every data verb accepts `branch`; reads also accept `snapshot_id`. `root`,
`main`, and `master` are aliases for Iceberg's physical `main` ref, so task
configuration does not depend on an organization's default-branch spelling.
A producer that does not provide Iceberg ids is numbered at creation, while a
contract that already carries ids keeps them.

## Filesystems

Local and object-store locations resolve through `pyarrow.fs`. Credentials,
endpoint, bucket, and path are parsed once by `Url`; explicit catalog
properties win. Hadoop-style `s3a://` and legacy `s3n://` locations use the
same Arrow S3 filesystem as `s3://`. Recorded and listed paths are compared
only after the same resolver normalizes them.

### What an `s3://` netloc names

`s3://bucket/key` is a bucket. `s3://store/bucket/key` is an S3-compatible
store addressed path-style. A netloc is the store when it carries a port, when
it is an IP address, when it is one of Amazon's own hostnames, or when its last
label is a public suffix -- a registered name somebody pointed at a machine.
Nothing else is: one label, a numeric last label, and the private-use suffixes
(`.internal`, `.local`, `.lan`, `.corp`, ...) stay bucket names, which a bucket
with dots in it needs.

```python
from rekep.urls import Url

for text in (
    "s3://bucket/logs/a.txt",                    # a bucket
    "s3://minio:9000/bucket/logs/a.txt",         # a store, by its port
    "s3://s3.eu.cloud.ovh.net/bucket/logs/a.txt",  # a store, by its name
    "s3://bucket.s3.eu-west-1.amazonaws.com/logs/a.txt",  # AWS, virtual-hosted
    "s3://my.logs.2026/logs/a.txt",              # a bucket, dots and all
):
    url = Url.from_string(text)
    print(f"{url.bucket:<14} {url.key:<12} {url.endpoint}")
```

```text
bucket         logs/a.txt   None
bucket         logs/a.txt   minio:9000
bucket         logs/a.txt   s3.eu.cloud.ovh.net
bucket         logs/a.txt   s3.eu-west-1.amazonaws.com
my.logs.2026   logs/a.txt   None
```

Two spellings the shape cannot decide say so with `?endpoint_override=`, which
is a decision and beats a shape:

- a bucket really named for a domain -- the S3 static-website pattern,
  `s3://www.example.com/index.html?endpoint_override=s3.amazonaws.com`;
- a bucket addressed virtual-hosted style on a store that is not Amazon's,
  `s3://bucket/key?endpoint_override=s3.example.net&force_virtual_addressing=true`,
  because only AWS publishes which of its leading labels is a bucket -- and an
  overridden endpoint is addressed path-style unless the location says
  otherwise.

Everything derived from the location follows that reading: the Arrow
`S3FileSystem`, the `s3.endpoint`, `s3.access-key-id`, `s3.secret-access-key`
and `s3.region` a catalog is configured with, the FileIO cache identity, and
the spill identity.

### Settings a location carries

| query key | Arrow argument | catalog property |
| --- | --- | --- |
| `region` | `region` | `s3.region` |
| `scheme` | `scheme` | part of `s3.endpoint` |
| `endpoint_override` | `endpoint_override` | `s3.endpoint` |
| `force_virtual_addressing` | `force_virtual_addressing` | `s3.force-virtual-addressing` |
| `anonymous` | `anonymous` | `s3.anonymous` |
| `allow_bucket_creation` | `allow_bucket_creation` | -- |

```python
from rekep.urls import Url, properties_of

warehouse = "s3://key:secret@minio.example.net/rekep/warehouse?force_virtual_addressing=true"
print(dict(properties_of(Url.from_string(warehouse))))
```

```text
{'s3.endpoint': 'https://minio.example.net', 's3.access-key-id': 'key',
 's3.secret-access-key': 'secret', 's3.force-virtual-addressing': 'true'}
```

A flag is on only where it is spelled `true`, so a typo reads as off, on the
catalog path as well as Arrow's. The two that decide whether a store answers at
all are the addressing style -- Arrow addresses an overridden endpoint
path-style, which is what MinIO and Ceph want and what a store serving only
`bucket.endpoint` refuses -- and `anonymous`, so a public bucket is read as
nobody instead of as whatever the credential chain found.

`endpoint_override` may carry its own transport (`http://minio:9000`) or not
(`minio:9000`); either way it reaches Arrow as a connect string and the catalog
as one URL. Without a transport, a hostname with no dot -- a container or a
laptop -- is read as plaintext and anything else as TLS; `scheme` says it
outright.

Name `region` wherever the store signs with one. Without it PyIceberg asks AWS
which region hosts a bucket of that name, which a bucket on another store is
not, and falls back to the SDK default.

PyIceberg is configured with the package-level
`rekep.arrow_file_io.ArrowFileIO` implementation.

Partition staging uses that same instance to copy local Parquet files to the
final path produced by Iceberg's location provider. Local warehouses and
`s3`/`s3a` warehouses therefore share one bounded copy path and the catalog's
endpoint, region, and credential configuration.

`ArrowFileIO.spill(local=None, temporary=False)` returns a local `ArrowFileIO`;
an already-local bound input returns itself. Persistent spills
use a deterministic name, are pulled again when the remote byte size changes,
and never serve stale bytes after the remote disappears. Temporary spills are
uniquely owned and deleted on `close`, after their open stream is closed.

`TextFile` holds that owner directly. A compressed remote log is copied as raw
compressed bytes in 4 MiB chunks, decoded incrementally by Arrow, and purged
when the reader finishes. Its disk use is the compressed object size and its
memory use stays bounded by the copy/read chunk and one parsed record batch;
the expanded capture is never materialized as a file or Arrow table.

Exhausting the reader closes that owner. A partial consumer closes the
surrounding `TextFile` context to release the decoder and temporary spill.

S3-compatible stores can be configured once per process. `S3_ENDPOINT_URL`,
`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_SESSION_TOKEN`, and
`S3_REGION` become Iceberg S3 defaults and also configure direct Arrow access.

An explicit catalog property wins over a value in a location URL, which wins
over the environment. For the endpoint only, `AWS_ENDPOINT_URL_S3` and then
`AWS_ENDPOINT_URL` are lower-priority fallbacks. When the portable variables
are absent, Arrow still uses the standard AWS profile, workload-role, and
credential environment chain.

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

The checked-in project tasks set that table property to seven days. Writers do
not compact or sweep files; call those operations explicitly, or run the
scheduled maintenance described below.

Snapshot expiration itself removes history from the current metadata JSON. It
does not remove rows visible in a retained snapshot, but it permanently removes
time travel and rollback to an expired state. PyIceberg 0.11 does not delete
physical files during that operation.

`cleanup` follows the metadata commit with rekep's reachability sweep: only
files unreferenced by every retained snapshot, branch, tag, statistics entry,
and current metadata version are eligible -- obsolete Parquet data,
manifest-list Avro, manifest Avro, and untracked metadata JSON. The orphan age
is a grace period for files an in-flight writer produced but never committed.

`optimize` compacts, then cleans, returning counts for each action. It also
supplies absent manifest merging and metadata JSON retention properties to
existing tables; an explicit table property still wins.

The Airflow deployment runs `optimize_iceberg` daily at 02:30 UTC. Its
checked-in policy keeps at least 24 snapshots and seven days, waits three days
before deleting unreachable files, and visits nested namespaces, which gives
idle tables another sweep after their grace period has elapsed.

Run it when ordinary writers are quiet; catalog commit conflicts fail safely
and are retried, but avoiding overlap prevents wasted rewrites.

## Testing and benchmark

Long transaction suites are marked `integration`; normal pull-request tests
cover planning and pure helpers. `python/benchmarks/bench_iceberg.py --quick`
measures focused commits, scans, merges, and maintenance—not a full pipeline.
