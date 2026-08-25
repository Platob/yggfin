# Iceberg

`IcebergDataset` exposes a table as an Arrow stream. Pyiceberg owns planning,
schema ids, snapshots, and commits; rekep supplies recursive casting,
filesystem normalization, commit grouping, and maintenance policy.

```python
from rekep import FixMsg
from rekep.iceberg import IcebergDataset

logs = IcebergDataset(
    name="fix.market",
    catalog="local",
    properties={
        "type": "sql",
        "uri": "sqlite:///catalog.db",
        "warehouse": "file:///warehouse",
    },
    field=FixMsg.into_field(),
    branch="root",
)
```

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

`overwrite_*` replaces the rows whose keys match and inserts the rest, and has
no keyless mode: replacing rows means knowing which rows. `append_*` inserts,
skipping the keys already stored when `merge_by` names them and inserting
everything when it does not.

Appending to a missing table creates it. `merge_by=True` skips keys already
stored; write/upsert replaces them. Input batches accumulate to the requested
commit size. Schema additions are nullable and additive.

Keyed writes push safe key ranges, including declared derived partition
columns, into Iceberg planning. Stored candidates are then consumed one
partition and record batch at a time. Overwrite retains only compact positions
into the current commit chunk, while insert progressively removes stored keys
and stops once none remain; neither path collects the planned table rows.

The supported stable PyIceberg commit API still requires an Arrow `Table`, and
its newer reader write path does not support partitioned tables. Consequently,
one `commit_row_size` chunk is the only intentional write materialization;
the default is 1,000,000 rows. Passing `0` or `None` explicitly requests one
whole-stream commit and therefore requires that stream to fit in memory.

The current market-contract cutover is not an additive Iceberg evolution:
renamed Book payloads, typed `linked_events`, required collections, removed
event fields, the required generic `Message.kwargs`, required nested argument
values, the FixMsg sequence rename, and renaming `unix_hour` to
`unix_partition` while changing its values from epoch-nanosecond `long` to
epoch-second `int` need an explicit table migration or recreation. Recreate or
rewrite every table using one of the six pipeline contracts, on every retained
branch, before appending: an ordinary merge cannot migrate the renamed,
rescaled, narrowed partition field. Dataset writes do not guess missing lineage
or keep retired columns alive.

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

S3-compatible stores can be configured once per process. `S3_ENDPOINT_URL`,
`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_SESSION_TOKEN`, and `S3_REGION`
become Iceberg S3 defaults and also configure direct Arrow access. An explicit
catalog property wins over a value in a location URL, which wins over the
environment. For the endpoint only, `AWS_ENDPOINT_URL_S3` and then
`AWS_ENDPOINT_URL` are lower-priority fallbacks. When the portable variables
are absent, Arrow still uses the standard AWS profile, workload-role, and
credential environment chain.

## Maintenance

```python
logs.compact(branch="root")
logs.cleanup()
logs.optimize(branch="root")
```

Write streams automatically ask for compaction once at least 16 files are
compactable. The pass settles once a partition has no newer data and keeps all
snapshots; set `auto_compact=False` when another service owns file rewriting.
Cleanup expires snapshots and removes only old files unreferenced by any live
snapshot. `optimize` compacts, then cleans, returning counts for each action;
`auto_optimize=True` explicitly permits a writer to run that full policy.

## Testing and benchmark

Long transaction suites are marked `integration`; normal pull-request tests
cover planning and pure helpers. `python/benchmarks/bench_iceberg.py --quick`
measures focused commits, scans, merges, and maintenance—not a full pipeline.
