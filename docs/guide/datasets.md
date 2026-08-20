# Datasets

A `Dataset` is OpenLineage's resource for one namespace-qualified data
product: schema, physical location across platforms, and the readers and
writers that move data in and out of it.

## Declaring: a schema and a URI

Two fields carry the identity. `schema` names the `Record` class whose Arrow
projection *is* this dataset's schema; `uri` is everything else — catalog,
namespace, name, and the branch — as one path:

```python
from rekep.dataset import Dataset

dataset = Dataset(schema="rekep:///records/log", uri="rekep:///datasets/warehouse/trading/logs")
dataset.arrow_schema()          # pyarrow.Schema -- what everything downstream uses
dataset.schema_facet()          # OpenLineage SchemaDatasetFacet: the same fields
dataset.resource_uri()          # rekep:///datasets/warehouse/trading/logs
dataset.dataset_name()          # "logs"
dataset.dataset_namespace()     # "trading"
```

A **path** rather than dots, because a catalog contains namespaces and a
namespace contains tables — `a.b.c` cannot say whether that is three levels or
one name with dots in it, and Iceberg namespaces are legitimately multi-level.
Levels read right to left, so a shorter URI is a *less qualified* name rather
than a different shape: `rekep:///datasets/logs` is `logs` in `default`.

**One spelling, not a family of them.** `rekep:` is the only scheme and the
service is the first path part, so a new kind of resource is a new path part
rather than a new scheme to teach every parser — and a URI in a log line, a
side file and the registry key is the same string rather than three that have
to be normalised before they can be compared.

**Three slashes**, the shape `file:///var/log` has: `//` opens the slot a URI
keeps for a host and the third `/` begins the path. Nothing hosts a rekep
identity today, so the slot stays empty — but a deployment that one day needs
to say *whose* datasets these are writes
`rekep://lake.internal/datasets/...` without one committed URI changing.
Anything else with this scheme is refused by name: too few slashes (which
would spend the host's slot on a name the path already has room for), or a
filled authority (which nothing reads yet, and dropping it silently would be
worse than refusing it).

The branch rides along as the fragment, because a branch is not a different
dataset: a dataset declaring `protocols.iceberg.branch: dev` has the URI
`rekep:///datasets/trading/logs#dev`.

`schema` is a reference rather than an inline field list on purpose. A
declaration has to survive a round trip through a file, and only a name can:
the class *is* the schema, so pointing at it keeps one definition instead of
two that can disagree. And a **URI** rather than an import path, because the
record is a resource like everything else here -- named by what it is called,
not by which module happens to hold it, so moving the file renames nothing.

Undeclared, the URI is built from the record's own snake_case name in
`default` — so the smallest useful dataset is one line:

```python
Dataset(schema="rekep:///records/log")   # rekep:///datasets/log
```

## Where declarations live

`Dataset.load_all()` and `Job.load_all()` read the checkout's
`stacks/datasets`/`stacks/jobs` when it has them, and the user's
`~/.config/rekep/...` when it does not — a repository that declares its own
pipelines is never quietly overridden by a home directory, and a bare
`pip install rekep` still has somewhere to keep things.

```python
Dataset.load_all()                       # stacks/datasets, else ~/.config/rekep/datasets
Dataset.load_all("/etc/rekep/datasets")  # or wherever you say
Dataset(schema="rekep:///records/log", uri="rekep:///datasets/trading/logs").dump()  # writes logs.yaml, schema included
Dataset.load("rekep:///datasets/trading/logs")         # from the registry, or by loading the folder
```

Everything loaded lands in a process-wide registry keyed by URI
(`rekep.config.REGISTRY`), so resolving a reference does not mean re-reading a
directory and two modules asking for the same dataset get the same object.
`REKEP_CONFIG_HOME`, `REKEP_STACKS_HOME`, `REKEP_DATASETS_ROOT` and
`REKEP_JOBS_ROOT` override the defaults.

## Location: shared, direct, protocol-specific

Location is layered the same way `Arrow` field metadata already is — shared
keys and protocol-prefixed overrides:

```python
dataset = Dataset(
    schema="rekep:///records/log",
    direct="s3://lake/log",                              # every protocol's fallback
    properties={"format": "parquet"},                     # shared by every protocol
    protocols={"iceberg": {"location": "s3://lake/iceberg/log"}},  # iceberg's own
)
dataset.location("iceberg")               # "s3://lake/iceberg/log"
dataset.location("doris")                 # "s3://lake/log" -- falls back to direct
dataset.protocol_properties("iceberg")    # properties merged with iceberg's own
```

## The whole data product, in one file

**One file per product and nothing beside it.** A `stacks/datasets/*.yaml`
file carries the declaration — `schema`, `uri`, `protocols` — *and* the
schema that declaration resolves to, written out as `description`/`fields`.
Every verb (deploy, write, read, maintain) reads the first half; a reviewer
reads the second without opening Python:

```yaml
# stacks/datasets/log.yaml, after `rekep dataset sync`
schema: rekep:///records/log
uri: rekep:///datasets/default/log
description: One parsed line of a trading log.
fields:
- name: url
  type: string
  description: Path of the log the line came from, as its filesystem addresses it.
  iceberg:
    field_id: 1
- name: unix
  ...
```

That block is **generated, never hand-maintained**: `rekep dataset sync`
writes it from the record, `verify()` refuses a file that drifted the moment
the dataset is projected onto a protocol, and CI fails when the two disagree
— the same contract `IcebergTable`'s own `fields` block has, one layer up.
There is no second folder of schema dumps to keep in step, because there is
no second file.

The other half is configuration, not code. This is
`stacks/datasets/parsed_messages.yaml`, the shipped example — the working,
iterating dataset, as opposed to `log.yaml`'s stable one:

```yaml
schema: rekep:///records/parsed_message
uri: rekep:///datasets/default/parsed_messages
protocols:
  iceberg:
    branch: "{{ 'main' if git_branch_slug in ('main', 'master') else git_branch_slug }}"
    merge_by: "true"
    commit_row_size: "50000"
    compact_min_files: "4"
    retain: 7d
```

| `protocols.iceberg` key | Means | Read by |
| --- | --- | --- |
| `location` | Where the table lives | `into_iceberg_table()` |
| `branch` | Which Iceberg branch writes and reads target | `iceberg_branch()` |
| `merge_by` | `true` (merge on the primary key), `false`, or `a,b` | `merge_columns()` |
| `merge_schema` | `true` to add columns the stream has and the table does not | `merge_schema()` |
| `retain` | Snapshot retention window: `7d`, `12h`, `90m`, `2w` | `iceberg_retention()` |
| `compact_min_files` | Files in a partition before it is worth rewriting | `iceberg_compact_min_files()` |
| `commit_row_size` | Rows a write accumulates before it commits | `commit_row_size()` |

Every other key under `protocols.iceberg` (and every key in `properties`) is
a **table property**, persisted on the table itself — the seven above route a
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
per-file choice rather than a mode. It is also why this file carries no
generated `fields` block: `sync` leaves a templated file alone rather than
resolving its template against whichever machine ran the command, so a file
that chooses the template chooses to describe its schema by pointing at the
record alone.

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

`write_arrow_reader` dispatches by `format` to the matching
`{format}_write_arrow_reader` hook:

```python
dataset.write_arrow_reader(reader, format="iceberg", table=live_table)
dataset.write_arrow_reader(reader, format="file")   # uses direct/protocols["file"]["location"]
```

Those hooks are the customisation points a deployment overrides (resolve a
table from a catalog, build a filesystem from credentials), and nothing wraps
them: a dataset moves data, and what a run of it *was* is
[`rekep.run`](jobs.md#lineage-represented-never-emitted)'s shape to describe.
A new protocol implements one hook and is done.

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

### `commit_row_size`: why a batch is not a unit of work

Both shapes accumulate `commit_row_size` rows (default 100,000) per commit
rather than writing batch by batch. In Iceberg every call commits a snapshot
and lands at least one data file per partition it touches, so appending a
reader of ten thousand small batches leaves ten thousand snapshots and as
many tiny files for every later scan to open. Accumulating first keeps memory
bounded by the *parameter* rather than the input, and it is worth roughly an
order of magnitude — 20,000 rows arriving in 500-row batches:

| `commit_row_size` | append rows/s | merge rows/s | files left |
| ---: | ---: | ---: | ---: |
| 500 | 10,259 | 879 | 40 |
| 5,000 | 95,565 | 7,418 | 4 |
| 20,000 | 90,114 | 13,286 | 1 |

See [Benchmarks](../benchmarks.md#writing-iceberg-bench_iceberg_upsertpy) for
how much of that is solid (the file count and the merge column) and how much
is measurement noise (the append column above 5,000).

How much a dataset commits at once is a property of the *data*, not of the
call site, so it is declared once in the side file
(`protocols.iceberg.commit_row_size`) and every write reads it from there; a
call-site argument still wins where one run genuinely differs. Undeclared, the
protocol answers for itself: Iceberg commits `COMMIT_ROW_SIZE` rows at a time
because every write commits *something* whether or not anyone chose a size,
and a file write leaves Arrow to size its own files.

```yaml
protocols:
  iceberg:
    commit_row_size: "50000"
```

The parameter means the same thing one layer down on the file side, where a
"commit" is a **file**: `commit_row_size` caps the rows per output file (and
the row group with it, since a group cannot exceed its file).

```python
dataset.write_arrow_reader(reader, "file", commit_row_size=50_000)
```

### Reshaping onto the record's schema

Every write hook starts by casting what it was handed onto the
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
dataset = Dataset(schema="rekep:///records/log", direct="file:///lake/log")
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
dev = Dataset(schema="rekep:///records/parsed_message", protocols={"iceberg": {"branch": "dev"}})
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

### Pruning a merge on the bounds Iceberg already has

Iceberg records the min and max of every column in every data file. Before
merging a chunk, its own key range is compared against those bounds: if no
existing file's range can overlap on even one join column, no row in the
chunk can match anything, and the merge is an anti-join guaranteed to find
nothing — so that chunk appends instead.

It costs one manifest read for the whole write and one `min_max` kernel per
key column per chunk. For the common shape — a stream of *new* data keyed on
something time-ordered — it prunes every chunk, and the difference is not
subtle: writing 1,000 fresh keys into a 1,000-row table took **0.058 s
pruned against 1.66 s merged**, same result either way.

That is why `unix` sits in the key beside `hash64`: the hash identifies a
line, but it is random, so a chunk of hashes always overlaps everything. A
key that leads with time gives the bounds something to prune on.

Bounds are only ever *widened* by Iceberg's own truncation of long strings,
so this can conclude "cannot match" but never wrongly conclude "does not
match".

## Maintenance: compact, cleanup, optimize

Streaming writes make this necessary across *runs*, which no single write can
batch away: every run commits at least one file per partition it touched, so
a table written once a minute has a thousand files a day and a scan pays a
thousand file opens.

```python
dataset.compact(table=t, dry_run=True)   # rewrite the fragmented partitions
dataset.cleanup(table=t)                 # reclaim what nothing references
dataset.optimize(table=t)                # whatever this table actually needs
```

### `compact()`

pyiceberg has no `rewrite_data_files` procedure, so compaction is built from
what it does have, and leans on it rather than reinventing it:
`inspect.data_files()` says how many files and how many bytes each partition
holds — metadata, so *choosing* costs no scan — a scan filtered to the
crowded partitions reads exactly those rows, and `dynamic_partition_overwrite`
replaces exactly the partitions written back. One commit, no other partition
touched.

The **output size is not decided here**: `write.target-file-size-bytes` is the
table's own property and pyiceberg's writer already bin-packs to it, so a
table that wants 128 MB files says so once, on the table. That same property
is half the test for what to rewrite — a partition needs
`compact_min_files` files *and* an average size under the target, because a
partition of eight full-sized files is not fragmented, it is just big.

Only `identity` partitions can be targeted — only they have a partition value
that is also a column value to filter on. A table partitioned by a computed
transform is refused by name, with `row_filter=` as the way to say what to
rewrite instead.

### `cleanup()`

Three steps, and the third is the one nothing else does:

1. **Metadata files** are pyiceberg's own job once the table says so.
   `write.metadata.delete-after-commit.enabled` and
   `write.metadata.previous-versions-max` make every commit prune the
   `metadata.json` trail behind it — retroactively, on the first commit after
   they are set. So `cleanup` sets them rather than deleting anything itself.
2. **Snapshots** past `protocols.iceberg.retain` are expired.
3. **Orphans.** Expiring a snapshot in pyiceberg drops metadata and *nothing
   else*: every data file only that snapshot referenced stays on disk,
   unreachable and unaccounted for. Expiry is a garbage *producer*. So the
   reachable set is computed from `inspect.all_files()`/`all_manifests()`
   across every surviving snapshot, the warehouse is listed, and the
   difference is deleted.

   `orphan_grace` (three days by default) is what makes that safe: a write in
   flight has files on disk that no committed snapshot references yet, and
   reachability alone cannot tell those from garbage. Age can.

### `optimize()`

Compact, then clean up — and enable manifest merging before either. The order
is not a preference:

- Compaction **creates** garbage: the files it replaced become unreachable
  the moment the new ones commit, so cleaning first would only have to be
  redone.
- `commit.manifest-merge.enabled` is **off** in pyiceberg, so a streaming
  table accumulates one manifest per commit forever. Turning it on costs
  nothing per commit and means the next thousand writes never need this pass.

`compact_min_files` and `retain` are the whole policy, so the CLI needs no
arguments at all:

```console
$ rekep dataset optimize --dry-run
rekep:///datasets/default/log: would rewrite 0 files in 0 partitions, 0 snapshots expired, 0 files freed
rekep:///datasets/default/parsed_messages: would rewrite 6 files in 1 partitions, 0 snapshots expired, 0 files freed
```

A dataset declaring no `retain` keeps all its history, which is the safe
default for something nobody has thought about yet.

## Lineage: represented, never emitted

A dataset describes what it *is* to a run, and stops there:

```python
dataset.facets()      # {"schema": {...}, "dataSource": {"uri": "rekep:///datasets/trading/logs"}}
dataset.as_input()    # InputDataset(namespace="trading", name="logs", facets=...)
dataset.as_output(outputStatistics={"rowCount": 2})
```

These are OpenLineage's own shapes (`rekep.run`), ready to hang off a
`RunEvent` a task builds with
[`into_run_event`](jobs.md#lineage-represented-never-emitted). There is no
client, no `emit`, and no wrapper around a read or a write — so a read is
handed back the protocol's own reader rather than one wrapped in a
row-counting generator, which was a per-batch cost on the hot path for
tracking nobody may ever have read.
