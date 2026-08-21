# Iceberg

`IcebergDataset` is an Iceberg table you read and write as Arrow. pyiceberg does
the Iceberg — planning scans, writing files, committing snapshots — and this
adds the two ends: the **shape** the data is cast onto, and the **streaming**
that keeps a commit from happening once per batch. Then it adds the part nobody
enjoys writing: the maintenance.

```bash
pip install "rekep[iceberg]"
```

That pulls in pyiceberg, a SQLite catalog so everything below runs locally, and
Iceberg's Rust core -- which is what computes a `day` or `bucket[16]` partition
value when a write lands on a table partitioned by a transform.

## A catalog

=== "Local (no services)"

    ```python
    from rekep.iceberg import IcebergCatalog

    catalog = IcebergCatalog(
        name="local",
        properties={
            "type": "sql",
            "uri": "sqlite:///catalog.db",
            "warehouse": "file:///data/warehouse",
        },
    )
    ```

=== "REST / Glue / anything"

    ```python
    catalog = IcebergCatalog(name="prod", properties={"type": "rest", "uri": "https://..."})
    ```

    Whatever pyiceberg loads, loads. The one default added here is
    `py-io-impl` → this package's `ArrowFileIO`, so Iceberg reads, writes and
    maintenance all go through the same `pyarrow.fs` handles as everything else
    — one credential chain, one set of URI rules — plus the two things it adds:
    Windows drive letters parse, and immutable metadata is
    [fetched once](#what-the-store-is-asked). Naming another wins.

=== "Namespaces"

    ```python
    catalog.namespaces()                          # ['trading'] -- one level
    catalog.namespaces(recursive=True)            # ['trading', 'trading.eu']
    space = catalog.create_namespace("trading", {"owner": "desk"})
    space.properties                              # {'owner': 'desk'}
    space.update_properties({"owner": "risk"})
    space.tables()                                # ['trading.quotes']
    catalog.drop_namespace("trading")             # missing is not an error
    ```

=== "Tables"

    ```python
    catalog.tables()                              # every namespace, nested ones too
    catalog.tables("trading")                     # one of them
    catalog.table_exists("trading.quotes")
    catalog.rename_table("trading.quotes", "trading.ticks")
    catalog.drop_table("trading.ticks", purge=True)   # purge deletes the files too
    ```

=== "Datasets"

    ```python
    quotes = catalog.dataset("trading.quotes", struct=Quote.FIELD)
    space = catalog.namespace("trading")
    quotes = space.dataset("quotes", struct=Quote.FIELD)

    for dataset in catalog.datasets("trading"):   # a sweep over a namespace
        dataset.optimize()
    ```

## A dataset

```python
from rekep.iceberg import IcebergDataset

quotes = IcebergDataset(
    name="trading.quotes",
    catalog="local",
    properties={"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "file:///wh"},
    struct=Quote.FIELD,        # optional: the shape to create the table from
    commit_row_size=1_000_000, # rows per commit when a write does not say
    optimize_commits=True,     # commit properties tuned for a stream
    plan_merges=True,          # plan a merge's scan instead of handing it over
)
```

`struct` is your [declaration](types.md). With one, the table is created from it
— schema, column comments, identifier fields and partition spec. Without one,
the table's own schema is the shape, read back as a `StructField` with its docs,
keys and partitions intact.

=== "Create"

    ```python
    quotes.create_with()                       # from the declared shape
    quotes.create_with(Quote)                  # from a @field class
    quotes.create_with_arrow_schema(schema)    # from a plain Arrow schema
    quotes.create_with_arrow_field(field)
    quotes.exists                              # True
    ```

    Creating what is already there is not an error. A plain Arrow schema is
    numbered for you — Iceberg identifies columns by id, and one that carries
    ids (from a parquet footer, or another Iceberg table) keeps them.

=== "Write"

    ```python
    quotes.write_arrow(batch)                  # a batch, a table, a reader, a list
    quotes.write_arrow(reader, merge_by=True, commit_row_size=1_000_000)
    quotes.write_arrow(table, merge_by=["symbol", "day"])
    quotes.write_arrow(table, branch="dev")
    quotes.write_arrow(table, commit_row_size=0)   # one commit, whatever the size
    quotes.append_arrow(reader, merge_by=True)     # insert-only: never rewrites
    ```

    Writes **append by default and create what is not there yet**. The stream is
    cast onto the shape first, so a nearly-right batch lands instead of failing
    pyiceberg's schema check.

    Iceberg lands a file and a snapshot per commit, so `commit_row_size` is the
    knob that decides what a later scan has to plan. It is a *lower bound*: a
    chunk closes at the first batch boundary at or beyond it, so a commit can
    never be smaller than the reader's batch.

=== "Read"

    ```python
    quotes.read_arrow_table()
    quotes.read_arrow_reader()                             # streamed
    quotes.read_arrow(pyarrow.RecordBatchReader)           # same, by type
    quotes.read_arrow_table(
        row_filter="day = '2026-08-14' and size > 10",     # pushed to the planner
        columns=["symbol", "size"],                        # so is the projection
        snapshot_id=...,                                   # or an older state
        branch="dev",                                      # or another line of it
    )
    quotes.read_arrow_table(Narrow.FIELD)   # the projection follows the shape
    ```

    Asking for a narrow shape reads narrow columns: the projection is taken from
    the target field rather than reading everything and dropping the rest.
    Naming `columns` overrides it.

!!! note "`limit` prunes files, not just rows"

    pyiceberg applies `limit` to the rows, *after* submitting every planned
    file for reading. The plan itself is cut here instead: files are taken in
    plan order until their record counts alone satisfy the limit, and the rest
    are never opened — measured, `limit=100` on an eight-file table opened
    **one** file instead of eight.

    A record count is trusted exactly where pyiceberg's own `count()` trusts
    one: the file's `residual` is `AlwaysTrue`, meaning its partition already
    satisfies the whole filter, and no delete file has removed rows from it.
    So a limit under a **partition** filter is trimmed too — `limit=2` under
    `day = '2026-08-14'` opens one of the day's three files — while a filter
    the files themselves have to answer hands the whole plan back. The row cap
    is pyiceberg's either way.

!!! note "A reader is a stream, and holds the pool, not the table"

    `ArrowScan` submits every planned file to its thread pool at once, and each
    finished one holds a whole file's decoded batches until the consumer
    reaches it — which makes a reader over a big table a `read_arrow_table`
    that takes longer. Measured on 24 files and 99 MiB, one batch of 20,000
    rows opened all 24 and left Arrow holding 97 of those MiB.

    The plan goes over a group at a time instead, the group being the pool's
    own width: past that there is a queue, not parallelism. On the same
    fixture: 8 files open, 32.5 MiB held. Draining the whole reader took
    54.6–56.5 ms against 59.6–72.4 ms before, so the bound costs nothing. A
    plan carrying delete files still goes over whole — `_read_all_delete_files`
    runs per scan, and a shared delete file would be read once per group.

=== "Delete"

    ```python
    quotes.delete("day < '2026-01-01'")
    ```

!!! note "Casting is opt-in on the way out"

    With no `schema`, a read hands back pyiceberg's own reader untouched —
    widths included. That is the fastest path, and it is the honest one: a
    conversion nobody asked for is paid per row. Pass a shape
    (`read_arrow_table(Quote.FIELD)`) when you want your own.

## Merging

`merge_by` is one argument, and it decides the whole write:

| value | what happens |
| --- | --- |
| `None` / `False` / `[]` | append |
| `True` | merge on the primary key the shape declares |
| `["symbol", "day"]` | merge on those columns |

```python
quotes.write_arrow(table, merge_by=True)
updated, inserted = quotes.merge_arrow_table(chunk, True)   # one chunk, reported
```

Declaring the key once — `Annotated[str, Field.primary_key()]` — is what makes
`merge_by=True` mean something.

### Appending without rewriting

`append_arrow` takes the same arguments as `write_arrow`, and `merge_by` means
something cheaper there: rows whose key is already stored are **dropped**, the
rest are inserted, and nothing stored is ever rewritten — the half of an upsert
a stream of immutable rows needs.

```python
quotes.append_arrow(reader, merge_by=True, commit_row_size=1_000_000)
inserted = quotes.insert_arrow_table(chunk, True)   # one chunk, reported
```

The scan that finds what is already stored is pruned to the chunk's key ranges
exactly as a merge's is, and it also projects to the **key columns alone**: an
append never compares non-key columns, so it never reads them. A chunk of new
keys plans to zero files and costs a plain append; a full replay reads keys,
matches everything, and commits nothing. Duplicate keys inside the stream
collapse to their first row; a null or NaN key is refused, because no predicate
can ever find it again.

### How a merge is planned

The algorithm is pyiceberg's: find the stored rows a chunk matches, overwrite
the ones whose non-key columns changed, append the rest. What this package
changes is how the matching rows are *found*, because that is where the time
goes.

=== "The problem"

    `Table.upsert` builds its scan filter with **one equality term per incoming
    row** — for a composite key, `Or(And(k1 = …, k2 = …), …)` — and then binds
    that same expression to Arrow once per matched batch to work out what to
    insert. Both are quadratic in the chunk. Measured on a two-column key:

    | chunk rows | pyiceberg upsert |
    | --- | --- |
    | 500 | 700 rows/s |
    | 1,000 | 730 rows/s |
    | 2,000 | 590 rows/s |
    | 4,000 | 440 rows/s |

    Worse, pyiceberg's evaluators give up on an `In` of more than 200 literals,
    so past that a single-column upsert stops pruning altogether and reads every
    file in the table.

=== "The fix"

    The scan is filtered by the chunk's **key values or ranges** — a handful of
    terms per key column, whatever the chunk's size — which every matching row
    satisfies, so the scan returns a superset and nothing can be missed. Rows to
    insert then come from one Arrow anti-join, and rows to update from a
    vectorised comparison instead of pyiceberg's per-row Python loop.

    Past `MERGE_IN_LIMIT` distinct values a key column cannot be named one
    value at a time, and one min/max range there prunes nothing on keys that
    sit in a **few bands of a wide span** — a backfill, or a replay of two days
    into a month. So the column is described by up to `MERGE_RANGE_BANDS`
    ranges instead, found by placing every value in one of 64 equal slices of
    `[min, max]` and merging the occupied ones back: a slice reports the exact
    min and max of what landed in it, so the union covers every value however
    the arithmetic rounded. Measured on 20 files of clustered keys, replaying
    two distant ones planned **18 files before and 2 after**; a replay of one
    contiguous band plans 1 either way and pays 0.02 s against 0.04 for the
    banding pass.

    | scenario (4,000-row chunk, 20k-row table) | planned | pyiceberg |
    | --- | --- | --- |
    | every key new | 0.09 s | 2.47 s (28×) |
    | every key stored, values unchanged | 0.20 s | 9.83 s (48×) |
    | half new, half unchanged | 0.20 s | 8.12 s (42×) |

    A chunk of entirely new keys prunes to **zero files** and becomes a plain
    append, which is what a log ingest hits every time — the scan is planned
    once, sees nothing to read, and the chunk goes straight to a commit with
    no reader built and no data file opened.

=== "Updating"

    When most rows genuinely *change*, finding them stops being the cost and
    the filter that names them for deletion becomes it. That filter stays
    exact — a range there would delete rows the chunk never touched — but it
    does not have to repeat itself. `create_match_filter` spells a composite
    key as one `And(EqualTo, EqualTo)` per row, and pyiceberg binds that whole
    tree **once per manifest it plans**: profiled on a 5,000-row update of an
    eight-file table, 15.8 of the 18.1 seconds went on nineteen metrics
    evaluators at 770 ms each, against 0.4 ms for the ranges beside it.

    So whatever a key column repeats is said once. The rows are grouped on the
    key column with the fewest distinct values and each group becomes one term:
    a `(symbol, day)` key over one day collapses 500 terms into one
    `And(EqualTo(day, …), In(symbol, […]))`. Measured on 10,000 stored rows
    over four days, both legs run twice:

    | rows updated | terms | seconds | rows/s |
    | --- | --- | --- | --- |
    | 500 | 1,000 → **2** | 0.95 → **0.22** | 527 → 2,245 |
    | 2,000 | 4,000 → **2** | 4.55 → **0.23** | 440 → 8,935 |
    | 5,000 | 10,000 → **4** | 21.16 → **0.43** | 236 → 11,589 |

    Two shapes keep the library's per-row form whole: a float key column
    holding a zero, because the inner half becomes an `In` and `pc.is_in`
    hashes `-0.0` apart from the `0.0` it equals; and a key that repeats
    nothing, where one group per row is the tree it already builds. The second
    is measured too — `(at, hash64)` at 0.51–11.94 s for the same 500 to 5,000
    rows — because an exact filter over *n* arbitrary key pairs is *n* terms
    and there is no smaller way to say it.

=== "Coherence"

    Same rows, same values, same snapshots: `tests/iceberg/test_coherence.py`
    runs every scenario twice on identical tables — once through this package,
    once through `Table.upsert` — and compares the contents, including the
    edges Arrow and Iceberg disagree about (a `-0.0` key against a stored
    `0.0`, nulls on one side of a comparison, a map column no join may carry).
    Set `plan_merges=False` to use the library's own path.

    ```python
    quotes.plan_merges = False      # hand the whole chunk to Table.upsert
    ```

    Four refusals are deliberately **stricter** than the library's, because
    every alternative corrupts the table:

    - a stored table with duplicate merge keys is refused wherever the copies
      are (pyiceberg checks one record batch at a time, so copies in two files
      slip past it and it writes a third);
    - a null merge key, and a NaN one — no predicate can name either, so the
      stored row is never found and a second one is inserted, again on every
      later merge;
    - a chunk that does not carry every column the table has: Iceberg's own
      schema check allows a missing **optional** column, and a merge that took
      it would write nulls over whatever is stored there.

    And two are deliberately more forgiving, because refusing costs data and
    accepting cannot: a `-0.0` key matches the `0.0` it equals (Arrow hashes
    them apart, so the library inserts a duplicate key), and a chunk whose
    columns arrive in another order is merged rather than rejected. A column
    renamed since the branch's last commit is matched by **field id**, not by
    name — the library reads the head under the old schema and cannot cast at
    all.

!!! tip "One key column merges faster than two"

    With a single join column pyiceberg uses `In(...)` rather than a per-row
    `Or`, which is 10–20× faster on its own path and prunes better on ours. If a
    hash or a surrogate id identifies a row on its own, declare that as the key.

## Schema evolution

=== "Add columns"

    ```python
    wider = Quote.FIELD.merge_with(pyarrow.schema([("desk", pyarrow.string())]))

    quotes.add_fields(wider)              # ['desk'] -- and the declared shape follows
    quotes.add_fields(wider)              # [] -- nothing new, so no commit
    quotes.add_fields(wider, dry_run=True)

    quotes.add_fields(deeper)             # ['venue.country'] -- inside a struct, too
    ```

=== "What it does"

    Schema evolution as a merge, in Iceberg's own terms: matched by name, new
    columns added **optional** (rows already written have nothing to put in
    them), everything else left alone, at every level. What comes back is a
    **dotted path**, because that is what evolution can add: a member gained by
    a struct, a list's item or a map's value is a new column too. Nothing is
    ever dropped or retyped here — that is a migration, not an evolution, and
    it should be deliberate.

## Snapshots and branches

=== "Look"

    ```python
    quotes.snapshots()          # Iceberg's own metadata table
    quotes.refs()               # branches and tags
    quotes.data_files()         # every data file the current snapshot holds
    quotes.scan_plan("day = '2026-08-14'")
    # {'files': 2, 'rows': 20000, 'bytes': 190_000, 'total_files': 16, 'skipped': 14}
    ```

    `scan_plan` is metadata only, and it is the honest way to see whether a
    filter prunes: the rows a scan *returns* say nothing about the files it
    *opened*. A filter Iceberg cannot use — `!=`, a range over a bucketed
    column, a nested field, a column written without metrics — returns the right
    answer and reads the whole table. `skipped: 0` is that, visible.

=== "Branch"

    ```python
    quotes.create_branch("dev")
    quotes.write_arrow(batch, branch="dev")     # main is untouched
    quotes.read_arrow_table(branch="dev")
    quotes.remove_branch("dev")
    ```

=== "Go back"

    ```python
    quotes.read_arrow_table(snapshot_id=1234)   # read an older state
    quotes.rollback(1234)                       # or move the branch back to it
    ```

## Maintenance

A streaming job commits often, and an Iceberg table written that way accumulates
small files, then snapshots, then files nothing references any more. These four
calls are the whole routine.

=== "Sort what you filter on"

    ```python
    quotes = IcebergDataset(..., sort_by=["unix"])
    ```

    Off by default, because it costs a sort per commit. What it buys is inside
    the file: bounds are recorded per row group, and a filter skips a row group
    it cannot match without decoding it. Measured on one 600k-row commit, a
    top-5% filter took **214 ms unsorted and 22 ms sorted** — the same single
    file either way.

    It does *not* narrow file bounds for a stream that arrives shuffled: a
    chunk of shuffled rows spans the whole key range whatever order it is
    written in. File bounds come from chunks that are already roughly ordered,
    which is what a log is.

=== "Compact"

    ```python
    quotes.compact()                                   # everything fragmented
    quotes.compact(min_files=8)                        # only what is worth it
    quotes.compact(row_filter="day = '2026-08-14'")    # one partition at a time
    quotes.compact(target_file_size=512 * 1024 * 1024)
    ```

    Fragmented parts are read back and written out as Iceberg would have written
    them in one go. How big the output files are is
    `write.target-file-size-bytes` — Iceberg's own knob, never a size this code
    picks.

=== "Plan first"

    ```python
    quotes.compaction_plan(min_files=2)
    # [(EqualTo(day, 2026-08-14), 12), (EqualTo(day, 2026-08-15), 4)]

    quotes.compaction_marks()   # what has settled: {"main/day=…": [files, rows]}
    ```

    Partition by partition when every partition field is an identity of a
    column: then a partition *is* a predicate, and rewriting one touches nothing
    else. A derived transform (`day`, `bucket[16]`) hides which rows are where,
    and so does no partitioning at all, so the honest plan is the whole table at
    once — which means reading it whole, and `row_filter` is the way to compact
    a table that does not fit.

    Predicates are Iceberg **expressions**, not filter strings: a string has to
    be parsed back, and an apostrophe in a partition value or a timestamp
    partition made that parse fail. A null partition value is `IsNull` rather
    than a dropped term.

    A part is not planned again until something lands in it, which
    `compaction_marks()` records — in a table property, because expiring a
    snapshot erases what a snapshot summary would have said, and `optimize()`
    expires immediately after it compacts. `compact(branch=...)` plans that
    branch, and a filtered run marks nothing: what it rewrote is whatever the
    filter covered, which may be a fraction of a partition.

=== "Clean up"

    ```python
    quotes.cleanup(retain=3)                                   # keep 3 snapshots
    quotes.cleanup(older_than=datetime.timedelta(days=7))
    quotes.cleanup(dry_run=True)   # {'expired': 12, 'deleted': 40, 'bytes': 91234}
    ```

    Expiry in pyiceberg is metadata-only: it forgets snapshots, it does not
    remove what they were keeping alive. This does both — in **both**
    directories, because a stream fills `metadata/` faster than `data/`: after
    fifteen commits and a compaction, one measured table held 18 data files and
    58 metadata files, and the sweep took it to 2 and 22 with every retained
    snapshot still readable.

    The sweep is conservative on purpose: a file goes only when nothing live
    references it **and** it is older than `orphan_age` (three days by
    default), because a writer committing right now has files on disk that no
    snapshot mentions yet. The live set is read from the catalog at the moment
    of the sweep — a dataset object that has been open a while has not seen the
    other writers — but `orphan_age` is the only thing standing between the
    sweep and a writer committing *during* it. Lower it when nothing else is
    writing, and nowhere else. The live set for metadata is built from every
    direction at once — the current pointer, every entry in the metadata log,
    every retained snapshot's manifest list and every manifest reachable from
    it, the statistics the metadata registers, and a Hadoop catalog's
    `version-hint.text` — because deleting one of those does not lose a row, it
    loses the table. Pass `metadata=False` to sweep data only.

    `dry_run=True` reports what it would expire and what is *already* orphaned
    — not what expiring would strand, which is strictly more and cannot be
    known without committing the expiry.

=== "Everything"

    ```python
    quotes.optimize()
    # {'rewritten': 24, 'expired': 12, 'deleted': 53, 'bytes': 784345}
    ```

    Manifest merging on, then compact, then expire and sweep — in that order,
    because compacting makes the snapshots that cleanup then expires, and
    merging manifests first means those commits land in fewer of them.

    It **settles**, on every partition shape. A part is only planned again once
    something has landed in it since it was last rewritten — which is the only
    reliable signal, because pyiceberg sizes its output files from in-memory
    bytes, so a part that legitimately needs ten files still reports ten
    afterwards. A plan that only counted files would rewrite it on every run
    and double the table each time.

    A table the plan can only address *as a whole* — no partitioning, or
    transforms that hide which rows are where — settles as a whole, under a key
    of its own. Asking the per-partition question there recorded nothing a
    later run could match: measured on four commits over four days, a `day`
    partition rewrote 16 files, then 4, then 4, forever, with
    `compaction_marks()` empty throughout and the rows never changing. It is
    16, then 0, then 0.

    Everything `cleanup` takes is reachable here too. The sweep is the
    expensive half — a recursive listing of the whole store — so
    `optimize(remove_orphans=False)` is the compaction on its own:

    ```python
    quotes.optimize(remove_orphans=False)          # compact, expire, no listing
    quotes.optimize(retain=10, orphan_age=datetime.timedelta(days=1))
    ```

=== "Automatically"

    ```python
    quotes.maybe_optimize()                        # None, or optimize()'s report
    quotes = IcebergDataset(..., auto_optimize=True)
    quotes.write_arrow(reader, merge_by=True)      # ends by asking maybe_optimize
    ```

    `maybe_optimize` runs the routine only once cheap signals say the table
    needs it: snapshots past `AUTO_OPTIMIZE_SNAPSHOTS`, the branch head's
    manifests past `AUTO_OPTIMIZE_MANIFESTS`, or a compaction plan worth
    `AUTO_OPTIMIZE_FILES` — checked in that order, against metadata the write
    just paid for. Right after an optimize the signals are quiet again, so a
    stream that ends with it converges instead of compacting every time.

    The plan is the only expensive one, and it is bounded by a free one: a
    plan cannot rewrite more files than the branch holds, and the head snapshot
    already says how many that is (`total-data-files`). Below the threshold the
    planner is never asked — which is every call on a stream that has
    converged, and used to cost a walk of every manifest to learn nothing:
    measured, one partitions read and one manifest read at 5–6 ms, now none at
    all.

    `auto_optimize=True` asks at the end of every write stream. It is **off by
    default** for one reason: `optimize` expires snapshots, and whether
    yesterday's snapshots are still wanted is not something a writer can
    decide for its readers.

=== "Properties"

    ```python
    quotes.set_properties({"write.target-file-size-bytes": "268435456"})
    quotes.iceberg_table.properties
    ```

    `optimize_commits=True` (the default) creates a table with the properties a
    stream needs, measured over 40 commits against Iceberg's own defaults:

    | property | why |
    | --- | --- |
    | `commit.manifest-merge.enabled=true` | merge the manifests a stream produces |
    | `commit.manifest.min-count-to-merge=10` | **without this the merge is inert** — Iceberg waits for 100 manifests by default |
    | `write.metadata.previous-versions-max=20` | how many `metadata.json` versions to keep |
    | `write.metadata.delete-after-commit.enabled=true` | delete the ones past that, instead of leaking them |
    | `write.target-file-size-bytes` | how one commit's output is sliced |
    | `write.parquet.row-group-limit=131072` | a filter can only skip a row group, and Iceberg's default makes most files a single one |

    Result: manifests 40 → 4, `metadata.json` files 41 → 21, scan planning
    61 ms → 9 ms, at no commit-time cost.

    !!! note "`write.target-file-size-bytes` does less than it sounds"

        pyiceberg derives rows-per-file from the *in-memory* size of one commit
        and has no cross-commit state, so it can only split a large commit — it
        cannot fill a file across commits. `commit_row_size` and `compact` are
        the levers on file count.

## What the store is asked

Seconds on a local disk cannot show what a scan-per-chunk flow does to an
object store, so `bench_iceberg.py --only fs` counts **store calls** instead —
on the file handles themselves, below any cache, where one `open` is a GET and
one `create` a PUT. The waste has one shape: everything Iceberg writes below
the catalog pointer is immutable, yet pyiceberg re-reads the manifest list on
every scan plan and every manifest on every fetch. So `ArrowFileIO` keeps a
bounded, process-wide cache of those files' bytes — filled on first read *and
on write*, because the manifest list a commit just wrote is the one the next
chunk's scan plans from.

100,000 rows streamed in 8 commits, a local warehouse, measured twice:

| flow | GETs, cache off | GETs, cache on |
| --- | --- | --- |
| append stream | 13 | **0** |
| merge, every key new | 40 | **0** |
| merge, half stored | 33 | 5 — the data files that matched |
| insert-only, full replay | 24 | 10 — key columns only |
| read everything | 18 | 10 — the data files |
| read one partition | 5 | 2 |
| read `limit=100` | 9 | **1** |
| `scan_plan` one partition | 11 → **3** | **0** |
| optimize (compact + sweep) | 71 → **69** | 10 |

The two uncached counts that moved are not the cache's doing: `scan_plan` under
a filter stopped planning the table a second time to count what an unfiltered
scan would touch, and the sweep stopped walking the manifests once per half of
its live set.

With the cache on, the only GETs left are data files a read genuinely needs;
every manifest, manifest list and `metadata.json` fetch is served from memory
(214 hits, 0 misses over the sweep, ~500 KiB held). A merge-shaped ingest of
new keys now costs the store exactly what a blind append does.

```python
IcebergCatalog(name="prod", properties={
    ...,
    "rekep.io.cache-bytes": "134217728",   # resize the shared budget; "0" opts out
})
```

The cache holds **only** what Iceberg promises never to rewrite, and the UUID
is what the promise rests on: `.avro` manifests and manifest lists, and the
`00007-<uuid>.metadata.json` pyiceberg mints per attempt. The `v7.metadata.json`
a Hadoop-style catalog writes has no UUID, so two racing writers can both
produce it with different bytes — that one is read from the store every time.
Entries can go cold, never stale.

Data files are never cached: they are the bytes worth streaming, and one of
them would evict everything else. The budget is 64 MiB by default, LRU by
bytes, shared across the process the way pyiceberg shares its own
manifest-file cache. A file bigger than an eighth of the budget is never
stored — and stops being copied the moment it passes that, rather than being
accumulated whole and dropped on arrival. A write abandoned mid-file never
lands, and a deleted file is evicted **including by the sweep**, which deletes
through a `pyarrow.fs` handle and so has to say so itself: five swept manifests
went on answering `exists()` and handing over their bytes after one `cleanup`
before it did.

## The escape hatch

Everything pyiceberg can do that this does not wrap is one attribute away:

```python
quotes.iceberg_table          # pyiceberg Table
quotes.iceberg_catalog        # pyiceberg Catalog
quotes.refresh()              # drop what was loaded, see other writers' commits
```

And the shape is always a [`Field`](types.md), so the two views agree:

```python
quotes.into_struct_field()                    # StructField: docs, keys, partitions
quotes.into_arrow_schema()                    # the Arrow view of the same thing
quotes.into_struct_field().into_iceberg_schema()   # and back to Iceberg's
```
