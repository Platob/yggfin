# Datasets

A `Dataset` is OpenLineage's resource for one namespace-qualified data
product: schema, physical location across platforms, and the readers and
writers that move data in and out of it — each internally lineage-tracked.

## Declaring

`record` names the schema helper — any `Record` subclass, whose Arrow
projection *is* the dataset's schema facet:

```python
from rekep.dataset import Dataset

dataset = Dataset(record="rekep.models.Log", name="logs", namespace="trading")
dataset.schema_facet()   # OpenLineage SchemaDatasetFacet: the record's fields
dataset.uri()            # "dataset://trading/logs" -- globally unique
```

## Location: shared, direct, protocol-specific

Location is layered the same way `Arrow` field metadata already is — shared
keys and protocol-prefixed overrides:

```python
dataset = Dataset(
    record="rekep.models.Log",
    direct="s3://lake/log",                              # every protocol's fallback
    properties={"format": "parquet"},                     # shared by every protocol
    protocols={"iceberg": {"location": "s3://lake/iceberg/log"}},  # iceberg's own
)
dataset.location("iceberg")               # "s3://lake/iceberg/log"
dataset.location("doris")                 # "s3://lake/log" -- falls back to direct
dataset.protocol_properties("iceberg")    # properties merged with iceberg's own
```

## The whole side file, in one place

Everything below is configuration, not code: a `stacks/datasets/*.yaml` file
declares it once and every verb — deploy, write, read, maintain — reads it
from there. This is `stacks/datasets/parsed_messages.yaml`, the shipped
example — the working, iterating dataset, as opposed to `log.yaml`'s stable
one, which declares nothing but its record, name and namespace:

```yaml
record: rekep.models.ParsedMessage
name: parsed_messages
namespace: default
protocols:
  iceberg:
    branch: "{{ 'main' if git_branch_slug in ('main', 'master') else git_branch_slug }}"
    merge_by: "true"
    compact_min_files: "4"
    retain: 7d
```

| `protocols.iceberg` key | Means | Read by |
| --- | --- | --- |
| `location` | Where the table lives | `into_iceberg_table()` |
| `branch` | Which Iceberg branch writes and reads target | `iceberg_branch()` |
| `merge_by` | `true` (merge on the primary key), `false`, or `a,b` | `merge_columns()` |
| `merge_schema` | `true` to add columns the stream has and the table does not | `merge_schema()` |
| `compact_min_files` | Files in a partition before it is worth rewriting | `iceberg_compact_min_files()` |
| `retain` | Snapshot retention window: `7d`, `12h`, `90m`, `2w` | `iceberg_retention()` |

Every other key under `protocols.iceberg` (and every key in `properties`) is
a **table property**, persisted on the table itself — the six above route a
write or a maintenance pass instead, so `table_properties()` filters them out
rather than writing them to disk as if they described the data.

The two layers apply to the policy keys: `properties` is what every protocol
sees and `protocols.<name>` is the exception layer over it, so a dataset that
wants one policy everywhere declares it once at the top and only the
differences go underneath. `location` is the exception to the exception — it
has `direct` as its own shared spelling, so `properties: {location: ...}`
means nothing; use `direct:`.

Side files render through Jinja before they are parsed, with `git_context()`
always in scope, so `branch` above resolves per git branch with nothing extra
to wire in. See [Jobs](jobs.md#branch-conditional-naming) for why that is a
per-file choice rather than a mode.

## Deploying: autonomous, no `tables/` side file

`stacks/iceberg/` and `stacks/doris/` declare only `catalogs/` and
`namespaces/` — a table needs no matching side file of its own. A `Dataset`
under `stacks/datasets/` carries everything a table declaration used to
(record, name, namespace, protocol properties) and converges itself:

```python
from rekep.dataset import Dataset
from rekep.iceberg import Iceberg

stack = Iceberg.load("stacks/iceberg")      # catalogs + namespaces only
for dataset in Dataset.load_all("stacks/datasets"):
    dataset.deploy_iceberg(stack)           # or dataset.deploy("iceberg", stack)
```

`into_iceberg_table()`/`into_doris_table()` build the ad hoc `IcebergTable`/
`DorisTable` `deploy_iceberg`/`deploy_doris` hand to `Iceberg.deploy_one`/
`Doris.deploy_one` — the same catalog-check, namespace-`get_or_create`,
table-`create_or_update` sequence `rekep records deploy` uses for a
bare record, just resolved from the dataset's own fields instead of stack
defaults. The CLI shape is `rekep dataset deploy --target iceberg`
(see the [CLI guide](cli.md)).

## Writing

`write_arrow_reader` dispatches by `format` to a private
`_{format}_write_arrow_reader`, which opens a `Run`, calls the matching
*public* `{format}_write_arrow_reader` hook, and closes the run on the way
out — `START` before, `COMPLETE`/`FAIL` after:

```python
dataset.write_arrow_reader(reader, format="iceberg", table=live_table)
dataset.write_arrow_reader(reader, format="file")   # uses direct/protocols["file"]["location"]
```

The public hooks are the customisation points a deployment overrides
(resolve a table from a catalog, build a filesystem from credentials); the
private ones exist only to be the lineage boundary. A new protocol
implements the public hook and gets the tracking for free.

### `merge_by`: one argument picks append or upsert

```python
dataset.write_arrow_reader(reader, "iceberg", table=t)                      # append
dataset.write_arrow_reader(reader, "iceberg", table=t, merge_by=True)       # upsert on the primary key
dataset.write_arrow_reader(reader, "iceberg", table=t, merge_by=["url", "unix"])
dataset.write_arrow_reader(reader, "iceberg", table=t, overwrite=True)      # replace everything
dataset.write_arrow_reader(reader, "iceberg", table=t, overwrite="date = '2026-08-14'")
```

- **`merge_by=True`** merges on the record's own primary key — the
  `Arrow(key=True)` fields that already became the table's Iceberg
  identifier fields. `ParsedMessage.hash64` needs no extra wiring, and
  re-parsing the same log corrects rows in place instead of duplicating
  them. A record declaring no primary key refuses `True` by name rather
  than guessing a join key.
- **`merge_by=[...]`** merges on exactly those columns.
- **falsy** (the default, unless the side file says otherwise) appends.
- **A table with no snapshot yet has nothing to merge against**, so the
  merge is skipped and the first write simply appends — logged, not silent.
- `overwrite` and `merge_by` are two different writes and refuse to combine.

### `merge_schema`: when the source grows a column

`merge_by` decides what happens to a *row* that is already there;
`merge_schema` decides what happens to a **column** that is not:

```python
dataset.write_arrow_reader(reader, "iceberg", table=t, merge_schema=True)
```

- Columns the record and the stream **both** have are cast to the record's
  declared types, exactly as always. A source spelling a column `int64`
  does not get to widen a table that declared `int32` — that is losing the
  declaration, not evolving the schema.
- Columns **only the stream** has are added to the table
  (pyiceberg's own `union_by_name`) and then written. They are added
  nullable, which is not a preference: rows already written have nothing to
  put in them, and Iceberg refuses a required addition outright.
- Off by default, and off unless declared: a table growing a column should
  be a decision, made once, in the file that describes the dataset.

Only the genuinely new fields are handed to `union_by_name`, never the whole
union. Restating a column the table already has would re-assert its
nullability too, and Iceberg maps Arrow's nullable flag onto `required`
verbatim — a NOT NULL column would silently become optional. Nothing is
restated, so nothing can be relaxed.

**Column identity comes back from the table, never from the record.**
`union_by_name` does not keep the field ids an Arrow schema arrives with; it
assigns its own, counting on from the table's `last-column-id`. Since
Iceberg matches columns by id, a write that kept the record's numbering
would be right only by luck — and wrong the moment two widening writes carry
*different* extra columns, filing the second one's data under the first
one's column with no error anywhere. Every write therefore takes its ids
back from the table after evolving it.

One thing looks odd in the snapshot log and is deliberate. **A snapshot
records the schema it was written under, and a scan projects that schema,
not the table's current one** — so after any schema change, reading a branch
back still yields the old column set, and a merge fails comparing the two
against the data it was handed. Every Iceberg write therefore checks the ref
it is about to write to, and moves a stale one forward with an empty append:
no rows, so no data file, one metadata-only commit. It is the ref that is
checked, not the table — a branch forked before an evolution is stale even
when the table itself is perfectly up to date, and a `deploy` that added a
column leaves every existing branch in exactly that state.

The file writer takes the same argument and means the same thing, with no
evolution step — a file layout has no schema to migrate, so the widened
columns simply land in the parquet.

### `chunk_rows`: why a batch is not a unit of work

Both shapes accumulate `chunk_rows` rows (default 100,000) per call rather
than writing batch by batch. In Iceberg every call commits a snapshot and
lands at least one data file per partition it touches, so appending a reader
of ten thousand small batches leaves ten thousand snapshots and as many tiny
files for every later scan to open. Accumulating first keeps memory bounded
by the *parameter* rather than the input, and it is worth roughly an order of
magnitude — 20,000 rows arriving in 500-row batches:

| `chunk_rows` | append rows/s | merge rows/s | files left |
| ---: | ---: | ---: | ---: |
| 500 | 10,259 | 879 | 40 |
| 5,000 | 95,565 | 7,418 | 4 |
| 20,000 | 90,114 | 13,286 | 1 |

See [Benchmarks](../benchmarks.md#writing-iceberg-bench_iceberg_upsertpy) for
how much of that is solid (the file count and the merge column) and how much
is measurement noise (the append column above 5,000).

### Reshaping onto the record's schema

Every public write hook starts by casting what it was handed onto the
record's Arrow schema (`Record.cast_arrow_reader`, unsafe): a plain iterator
of batches becomes a reader, columns are cast, missing *nullable* ones are
filled with nulls, extras are dropped and the order is fixed. So a job's
output pipes straight in:

```python
dataset.write_arrow_reader(job.arrow_transform(job.extract()), "iceberg", table=t)
```

A missing **non-nullable** column is refused by name instead — filling a NOT
NULL column with nulls only fails later, at the write, where the cause is
much harder to see. See [Records](records.md#casting-onto-a-records-schema).

### Files: partitioned by the record's own declaration

`file_write_arrow_reader` maps a URI to a `(filesystem, path)` pair through
`rekep.filesystems.resolve`, cached per URL, and streams the reader into
`pyarrow.dataset.write_dataset` — never materialised. Its `partitioning`
defaults to `hive_partitioning()`, built from the same `Arrow(partition=...)`
declaration Iceberg's partition spec comes from:

```python
dataset = Dataset(record="rekep.models.Log", direct="file:///lake/log")
dataset.write_arrow_reader(reader, "file")        # -> /lake/log/date=2026-08-14/part-....parquet
dataset.write_arrow_reader(reader, "file", partitioning=False)   # flat
```

Only `identity` transforms become directories; a `day` or `bucket[16]`
partition is a value Iceberg *computes* at write time, and inventing that
column here would mean inventing data the record never declared, so those
are skipped (logged) rather than guessed at.

On the Iceberg side those same computed transforms take Iceberg's own
`<column>_<transform>` partition-field name (`at_day`, `account_bucket_16`),
because a partition field may not shadow a schema column while holding a
value that is not that column's. Writing them also needs pyiceberg's optional
native extra (`pip install "pyiceberg[pyiceberg-core]"`), which is pyiceberg's
requirement, not this package's — an `identity` partition needs nothing.

Each write also gets its own basename prefix and
`existing_data_behavior="overwrite_or_ignore"`, so writing twice into one
location appends instead of colliding on `part-0.parquet`. Both are ordinary
options: pass `existing_data_behavior="delete_matching"` for
replace-the-directory semantics.

## Reading, with filter pushdown

`read_arrow_reader` is the mirror image, and returns a **lazy**
`pyarrow.RecordBatchReader` — nothing is read until it is iterated:

```python
reader = dataset.read_arrow_reader(
    "iceberg", table=live_table, row_filter="date >= '2026-08-01'", columns=["hash64", "protocol"]
)
```

`row_filter` and `columns` are not applied to the result — they are handed to
the *scan planner*, which is the whole point. Iceberg prunes partitions from
the filter, then files from their column statistics, then row groups inside
the files that survive, and only then reads. Both spellings pyiceberg accepts
work: a string (`"date >= '2026-08-01'"`) or a built `pyiceberg.expressions`
tree.

Which snapshot is read resolves the same way the write side resolves where to
write: an explicit `snapshot_id` wins, then `branch` (or the declared
`protocols.iceberg.branch`), then the table's current state. A declared branch
that does not exist yet reads `main` rather than failing.

The file reader is the same shape through Arrow's own scanner, and defaults
its `partitioning` to `hive_partitioning()` — so a dataset written by
`file_write_arrow_reader` reads back with its partition columns intact,
without being told the layout twice:

```python
import pyarrow.compute

dataset.read_arrow_reader("file", row_filter=pyarrow.compute.field("hash64") > 0)
```

## Branches: write-audit-publish

```python
dev = Dataset(record="rekep.models.ParsedMessage", protocols={"iceberg": {"branch": "dev"}})
dev.write_arrow_reader(reader, "iceberg", table=t)   # main untouched
dev.read_arrow_reader("iceberg", table=t)            # reads the branch back
dev.iceberg_publish(table=t)                         # fast-forward main onto it
```

A branch that does not exist yet is created as a *fork of `main`'s current
snapshot* first, not through pyiceberg's own auto-creation (which gives a
branch no parent at all — an independent, empty lineage that merely shares
the table). That fork is what makes a branch useful: iterate against real
data, `main` untouched, until the output is good enough to promote.

Publishing is its own explicit call and never something a write does on its
own — the branch write leaving `main` alone is exactly what made it safe.

A table with **no snapshot at all** has nothing to fork from and Iceberg
allows only `main` there, so the very first write ever lands on `main`
regardless of `branch`, logged. That is why the shipped config maps git's
`main`/`master` onto Iceberg's literal `main` rather than through
`git_branch_slug`'s own spelling of it.

## Maintenance: compaction and retention

Streaming writes make this necessary across *runs*, which no single write can
batch away: every run commits at least one file per partition it touched, so
a table written once a minute has a thousand files a day and a scan pays a
thousand file opens.

```python
dataset.iceberg_compact(table=t, min_input_files=8, dry_run=True)
dataset.iceberg_expire_snapshots(table=t, older_than=datetime.timedelta(days=7))
dataset.iceberg_maintain(table=t)      # both, from the side file's own policy
```

pyiceberg has no `rewrite_data_files` procedure, so compaction is built from
what it does have: `inspect.data_files()` says which partition each file
belongs to (metadata, so choosing costs no scan), a scan filtered to the
crowded partitions reads exactly those rows, and `dynamic_partition_overwrite`
replaces exactly the partitions written back. One commit, no other partition
touched.

Only `identity` partitions can be targeted that way — only they have a
partition value that is also a column value to filter on. A table partitioned
by a computed transform is refused by name, with `row_filter=` as the way to
say what to rewrite instead. An unpartitioned table is the simple case: too
many files means rewrite all of them.

Compaction runs before expiry on purpose: rewriting files frees nothing while
the snapshots referencing the old ones are still there. The CLI needs no
arguments at all, because the side file already carries the policy:

```console
$ rekep dataset maintain --dry-run
dataset://default/log: would rewrite 0 files in 0 partitions, 0 snapshots expired
dataset://default/parsed_messages: would rewrite 6 files in 1 partitions, 0 snapshots expired
```

A dataset declaring no `retain` keeps all its history, which is the safe
default for something nobody has thought about yet.

## Lineage: opt in, or pay nothing

Lineage happens when a client is listening, and not otherwise:

```python
from rekep.lineage import Collector

collector = Collector()
dataset.with_lineage(collector).write_arrow_reader(reader, "iceberg", table=live_table)
collector.events   # [RunEvent(START, ...), RunEvent(COMPLETE, outputStatistics={"rowCount": ...})]
```

A client is anything with `emit(event)` — `Collector` keeps them in a list,
which is what tests and notebooks want. Binding returns the dataset, so it
chains, and it changes nothing about the *declaration*: a client is a runtime
handle, and `dataset.into_json()` is identical either way.

**With no client bound, none of it happens.** Not tracked-and-discarded: no
run is created, no timestamp taken, no schema facet composed — and a read is
handed back the protocol's own reader instead of one wrapped in a
row-counting generator, which is a per-batch cost on the hot path.

A `FAIL` carries the exception as an `errorMessage` run facet, because a
failure that does not say what went wrong is worth less than the traceback
the caller is about to see anyway.

A read is lazy, so its run cannot close where the call returns: `START` is
emitted when the scan is planned — the moment it commits to a snapshot and a
filter — and `COMPLETE` when the last batch comes out, carrying the row count
nothing could have known before then. A reader abandoned half-way leaves its
run open, which is the honest record of what happened.

The events are OpenLineage's own `RunEvent` shape. A real
`openlineage-python` client type-checks its own classes, so handing one these
unconverted will not work — the protocol here is deliberately a duck, so a
test double is three lines and an adapter is the only thing a real backend
needs. `Job.run_tracked()` (and `@arrow_task`) wrap a whole
`extract -> transform -> load` the same way, and are plain `run()` until a
client is bound.
