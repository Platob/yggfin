# Iceberg

`IcebergDataset` exposes a table as an Arrow stream. Pyiceberg owns planning,
schema ids, snapshots, and commits; rekep supplies recursive casting,
filesystem normalization, commit grouping, and maintenance policy.

```python
from rekep.iceberg import IcebergDataset

logs = IcebergDataset(
    name="market.logs",
    catalog="local",
    properties={
        "type": "sql",
        "uri": "sqlite:///catalog.db",
        "warehouse": "file:///warehouse",
    },
    field=FixMessage.into_field(),
    branch="root",
)
```

## Read

```python
reader = logs.read_arrow_reader(
    row_filter=filter,
    columns=["unix", "hash", "symbol"],
    order_by=["unix", "msg_seq_num", "hash"],
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

The current market-contract cutover is not an additive Iceberg evolution:
renamed Book payloads, typed `linked_events`, required collections, removed
event fields, the FixMessage sequence rename, and renaming `unix_hour` to
`unix_partition` while changing its values from epoch-nanosecond `long` to
epoch-second `int` need an explicit table migration or recreation. Recreate or
rewrite every table using one of the five market contracts, on every retained
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
properties win. Recorded and listed paths are compared only after the same
resolver normalizes them.

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
