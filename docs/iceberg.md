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

=== "On MinIO or S3"

    ```python
    catalog = IcebergCatalog(
        name="prod",
        properties={
            "type": "sql",
            "uri": "postgresql://...",
            "warehouse": "s3://AKIA:sec:ret@minio:9000/warehouse",
        },
    )
    ```

    The warehouse URL already says where the store is, which key reaches it
    and which secret, so it is not said again: `ArrowFileIO` reads
    `s3.endpoint`, `s3.access-key-id` and `s3.secret-access-key` out of it
    (through [`Url`](design.md#a-location-is-parsed-once-in-one-place), which is
    why a secret may contain a colon). Setting any of them explicitly wins —
    an explicit property is a decision, a URL is a default.

    The port is what says `minio:9000` is the *store* and `warehouse` the
    bucket. Without this, every parser in the stack reads `minio` as the
    bucket and drops the port — a legal bucket name, so nothing raises and the
    write lands nowhere anybody looks.

=== "REST / Glue / anything"

    ```python
    catalog = IcebergCatalog(name="prod", properties={"type": "rest", "uri": "https://..."})
    ```

    Whatever pyiceberg loads, loads. The one default added here is
    `py-io-impl` → this package's `ArrowFileIO`, so Iceberg reads, writes and
    maintenance all go through the same `pyarrow.fs` handles as everything else
    — one credential chain, one set of URI rules — plus the three things it
    adds: Windows drive letters parse, an S3 endpoint is told from a bucket,
    and immutable metadata is [fetched once](#what-the-store-is-asked). Naming
    another wins.

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
    sort_by=None,              # None: the shape's own sort keys. [] opts out
)
```

`struct` is your [declaration](types.md). With one, the table is created from it
— schema, column comments, identifier fields, partition spec and sort order.
Without one, the table's own schema is the shape, read back as a `StructField`
with its docs, keys and partitions intact.

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
    ids keeps them: a parquet footer's `PARQUET:field_id`, or our own
    `iceberg:field_id` when the shape came from a table or from a
    [contract](contracts.md) that pins them.

!!! note "The declared sort order reaches the data"

    A table records a sort order and every engine writing through it is meant
    to honour it — so this one does. `sort_by` defaults to the shape's own
    `Field.sort_key()` declarations, because a recorded order the writer
    ignores is a wish: a filter can only skip a **row group**, and a row group
    only helps if it covers a narrow slice of the column. On a shuffled 600k
    row commit, a top-5% filter decoded **one row group of five** sorted
    against five of five unsorted.

    A chunk that is *already* in that order is handed straight back. The
    question is far cheaper than the answer — on a million rows, 1.7 ms to
    check against 38.7 ms to sort — so a stream that arrives in time order,
    which is every capture and every log, pays almost nothing for the default.
    Pass `sort_by=[]` to opt out entirely, or name other columns to override.

!!! note "A wide shape says which columns must keep bounds"

    Iceberg infers min/max bounds for the first hundred **leaf** columns in
    declaration order and writes the rest with none, so a filter on a column
    past that budget reads every file — and still returns the right answer,
    which is why nothing notices. Position decides it, and a nested member
    added in front of a filter column pushes one over the edge: `Book` reached
    exactly a hundred leaves the day an instrument grew legs.

    So a table created from a shape declares the columns that shape is *read*
    by — its partition, sort and primary keys — as
    `write.metadata.metrics.column.<name>`, which takes them out of the budget
    entirely. Strings are bounded at sixteen characters, because a bound on a
    long string is the string, in every manifest entry that names the file.
    The budget itself is only raised when the shape is genuinely past it.

    ```python
    from rekep.iceberg import metrics_for

    metrics_for(Quote.FIELD)
    # {'write.metadata.metrics.column.symbol': 'truncate(16)',
    #  'write.metadata.metrics.column.day': 'full'}
    ```

    `table_properties` wins over all of it, so a column nothing filters on can
    be set to `none` and stop costing manifest bytes. pyiceberg collects every
    top-level primitive regardless of position, so today this changes nothing
    about what *this* writer records — it is written on the table for the
    engines that do honour it.

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

    A record count is trusted where pyiceberg's own `count()` trusts one: the
    file's `residual` is `AlwaysTrue`, meaning its partition already satisfies
    the whole filter, and no delete file has removed rows from it. So a limit
    under a **partition** filter is trimmed too — `limit=2` under
    `day = '2026-08-14'` opens one of the day's three files — while a filter
    the files themselves have to answer hands the whole plan back. The row cap
    is pyiceberg's either way.

    With one thing `count()` gets wrong. A residual is resolved against the
    partition value in *Python*, so a file in a **null** partition answers
    `venue != 'XPAR'` with `None != 'XPAR'` — True — and its residual comes
    back `AlwaysTrue`; Arrow then applies the same filter to the rows in
    three-valued logic, where `NULL != 'XPAR'` is NULL, and drops all of them.
    A filtered limit leaves those files to pyiceberg.

!!! note "A reader is a stream, and holds the pool, not the table"

    `ArrowScan` submits every planned file to its thread pool at once, and each
    finished one holds a whole file's decoded batches until the consumer
    reaches it — which makes a reader over a big table a `read_arrow_table`
    that takes longer. Measured on a 24-file, 99 MiB table, one batch of 20,000
    rows opened all 24 and left Arrow holding 97 of those MiB.

    The plan goes over a group at a time instead, the group being the pool's
    own width: past that there is a queue, not parallelism. On that same
    24-file fixture: 8 files open, 32.5 MiB held. Draining the whole reader is
    no slower for the bound — that leg, and a smaller table's counts, are
    [measured](#maintenance-and-what-a-reader-holds). A plan carrying delete
    files still goes over whole — `_read_all_delete_files` runs per scan, and a
    shared delete file would be read once per group.

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
`merge_by=True` mean something. For a parsed log it is the timestamp and
the hash of the raw line, so a replay of a rotated file merges onto itself.

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
    insert. Both are quadratic in the chunk, so a bigger chunk buys nothing:
    on a two-column key that is 700 rows/s at 500 rows and **440 at 4,000**
    ([measured](#appending-merging-and-what-pyicebergs-own-upsert-costs)).

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
    the arithmetic rounded — which it does, since a nanosecond timestamp does
    not fit a float64 mantissa and the cast is deliberately unchecked. Measured
    on 20 files of clustered keys, replaying two distant ones planned **18
    files before and 2 after**; a replay of one contiguous band plans 1 either
    way and pays the banding pass for nothing.

    Head to head on a 4,000-row chunk against a 20k-row table, with identical
    results either way, a replay whose keys are all stored and unchanged took
    **0.20 s here against pyiceberg's 9.83 s** — 48×
    ([measured](#appending-merging-and-what-pyicebergs-own-upsert-costs)).

    A chunk of entirely new keys prunes to **zero files** and becomes a plain
    append, which is what a log ingest hits every time — the scan is planned
    once, sees nothing to read, and the chunk goes straight to a commit with
    no reader built and no data file opened.

=== "A column the keys decide"

    A filter that names no partition column prunes nothing at the manifest
    list, and the market shapes are keyed on `(unix, hash)` while being
    partitioned on `unix_hour` — so the merge scaled with the **table**, not with
    the chunk being merged.

    But `unix_hour` *is* `unix`, truncated to the hour: two rows that agree on
    `unix` agree on it, so the chunk's own values are the values of every row
    that can match. That is a fact about the data rather than about any one
    write, so the field says it once:

    ```python
    unix_hour: Annotated[int, Field.partition_key(derived_from="unix")] = 0
    """`unix` truncated to the hour -- what the data is partitioned on."""
    ```

    A merge then names every column whose sources are all keys of *this*
    merge, and the scan prunes to the partitions the keys fall in. Replaying
    one hour, declared against not: **19 ms against 37** over 48 hourly
    partitions, **20 against 93** over 168, **27 against 164** over 336. The
    declared path barely moves; the other is linear in the table.

    The declaration may only ever widen the filter, never narrow it. A derived
    column holding a null or a NaN contributes no term at all, a derivation
    whose sources are not all keys here is ignored, and a shape read back from
    a table declares nothing — Iceberg records a partition spec, not *why* a
    column holds what it does, and saying nothing costs pruning rather than a
    row.

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
    over four days, a 5,000-row update goes from 10,000 terms and 21.16 s to
    **4 terms and 0.42–0.43 s** ([measured](#updating-what-is-stored)).

    Two shapes keep the library's per-row form whole: a float key column
    holding a zero, because the inner half becomes an `In` and `pc.is_in`
    hashes `-0.0` apart from the `0.0` it equals; and a key that repeats
    nothing, where one group per row is the tree it already builds. The second
    is measured too — `(at, hash)` at 0.51–11.94 s for the same 500 to 5,000
    rows — because an exact filter over *n* arbitrary key pairs is *n* terms
    and there is no smaller way to say it.

=== "Comparing what matched"

    Finding the rows a chunk matches is one half; deciding which of them
    actually *changed* is the other. pyiceberg joins on the keys and then
    compares the matched rows in Python — one `slice(i, 1)` and one `as_py()`
    per non-key column per row, about 50 µs a row. Here the pairs are gathered
    with `take` and compared column by column, so a million matched rows cost
    a handful of vectorised passes.

    Arrow has no equality kernel for a list, a struct or a map, and the
    fallback for those is **per column** rather than per comparison — which it
    was not, and that cost real time: `Log` carries twenty scalar columns and
    one `list<int64>`, and the list sent all twenty-one row by row through the
    library. A merge of 50,000 stored and unchanged rows spent **6.35 seconds
    of 7.2** inside `get_rows_to_update`; the same case now takes **0.39 s**
    against 6.19, sixteen times faster.

    The awkward column is still compared the library's way — both sides out of
    Arrow at once, then one `!=` per row — so the null and NaN semantics are
    unchanged: two nulls are equal, a null and a value are not, and a NaN
    equals nothing. `tests/iceberg/test_coherence.py` asserts that row for row
    against `get_rows_to_update`, for a list, a struct and a map.

=== "What the join hands back"

    An Arrow join emits its output a batch at a time and in whatever order
    the batches finish — measured on pyarrow 25, a 400k-row anti-join comes
    back in ten runs rather than one. The rows are right; their **layout** is
    not. A chunk is sorted on the way in so that each of a file's row groups
    covers a narrow slice of the sort key, and a scrambled take spreads every
    slice across all of them, which makes `sort_by` mean nothing for any chunk
    that had a row to drop — every partial replay, and only those.

    So the *positions* the join hands back are put back in order before the
    take. Sorting positions rather than any column is what keeps it honest: it
    restores the order the caller had, whatever that order was, and says
    nothing about what it should be.

    Measured on a 1.2M-row table, a top-5% filter after inserting 800k rows
    over 400k stored ones: **282,496 rows decoded in order against 400,000 to
    531,072 out of it**. The ordering itself did not show up against
    run-to-run variance on the insert.

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
    file either way ([measured](#reading-it-back)).

    It does *not* narrow file bounds for a stream that arrives shuffled: a
    chunk of shuffled rows spans the whole key range whatever order it is
    written in. File bounds come from chunks that are already roughly ordered,
    which is what a log is.

    `sort_by` is what *this* writer does to a batch before it commits it. A
    declaration's `Field.sort_key()` is a different thing: it writes Iceberg's
    own **sort order** onto the table, where every engine that writes through
    it — Spark, Doris, a Rust job — reads the same instruction. A shape that
    declares one gets it at `create_with` and needs no `sort_by`; `sort_by` is
    for a table whose schema you did not declare.

    ```python
    Order.FIELD.sort_keys()                    # {'unix': 'asc'}
    Order.FIELD.into_iceberg_sort_order()      # what lands on the table
    ```

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
    later run could match: measured on its own fixture of four commits over
    four days, a `day` partition rewrote 16 files, then 4, then 4, forever,
    with `compaction_marks()` empty throughout and the rows never changing. On
    that table it is now 16, then 0, then 0; the sweep's own table starts from
    13 files and lands the same way
    ([measured](#maintenance-and-what-a-reader-holds)).

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
    measured, one partitions read and one manifest read at 5–6 ms — the
    0.005–0.006 s row [below](#maintenance-and-what-a-reader-holds) — and now
    none at all.

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

Measured on 100,000 rows streamed in 8 commits, that takes an append stream
from 13 GETs to **0** and leaves only the data files a read genuinely needs —
every manifest, manifest list and `metadata.json` fetch is served from memory
([the counts](#store-calls)). A merge-shaped ingest of new keys now costs the
store exactly what a blind append does.

```python
IcebergCatalog(name="prod", properties={
    # whatever the catalog needs, plus:
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

The table's own shape carries its **column ids** — Iceberg identifies a column
by id and never by name, so `table_field` reads them back under
`iceberg:field_id`, a [contract](contracts.md) dumped from it publishes them,
and handing that contract back builds the same ids instead of a fresh
numbering:

```python
quotes.table_field.field("symbol").field_id            # 1
quotes.table_field.into_yaml("schemas/trading/quote.yaml")   # ids included
```

## Benchmarks

Every number below came out of `bench_iceberg.py` on one machine, measured
twice — a range where the two runs disagreed, one figure where they did not.
[Benchmarks](benchmarks.md) is how they are made and why they are quoted that
way.

```bash
cd python
uv run python benchmarks/bench_iceberg.py       # parse, stream into Iceberg, read back
uv run python benchmarks/bench_iceberg.py --only maintain   # the maintenance
uv run python benchmarks/bench_iceberg.py --only update     # the half that rewrites
uv run python benchmarks/bench_iceberg.py --only backfill   # replaying clustered keys
uv run python benchmarks/bench_iceberg.py --only fs         # store calls, not seconds
```

The default sweep streams 400,000 parsed rows over 8 days, partitioned by day,
written from a reader whose batches are 16,384 rows. The sections that use a
fixture of their own say so.

The Iceberg numbers use a local SQLite catalog and a file warehouse, so they are
storage-latency-free: they measure planning, commit and Arrow work, which is
what this package is responsible for. On an object store every commit also pays
a round trip — which makes the number of commits matter *more*, not less. What
those round trips add up to is counted rather than timed, in
[Store calls](#store-calls).

### How much a commit costs

| commit rows | seconds | rows/s | files | manifests | snapshots |
| --- | --- | --- | --- | --- | --- |
| 16,384 | 1.9–2.7 | 148k–213k | 32 | 7 | 25 |
| 65,536 | 0.50–0.64 | 623k–797k | 14 | 7 | 7 |
| 262,144 | 0.23–0.28 | 1.4M–1.7M | 9 | 2 | 2 |
| 1,000,000 | 0.26–0.32 | 1.3M–1.6M | 8 | 1 | 1 |
| the whole stream | 0.27–0.51 | 790k–1.5M | 8 | 1 | 1 |

Twenty-five commits leave **7** manifests rather than 25, because
`commit.manifest.min-count-to-merge` is set: Iceberg's own default waits for a
hundred manifests before merging any, which no stream of this size ever
reaches.

A commit is a file, a manifest and a snapshot, and every later scan pays for all
three — planning is linear in the number of files. That is why
`commit_row_size` defaults to a million rows rather than to the batch: below
about 250k rows the commits, not the data, are the work.

!!! note "A commit cannot be smaller than a batch"

    Chunks close at the first batch boundary at or beyond `commit_row_size`, so
    16,384 above is "one batch per commit". Asking for less changes nothing.

### Appending, merging, and what pyiceberg's own upsert costs

| case | commit rows | seconds | rows/s |
| --- | --- | --- | --- |
| append | 1,000,000 | 0.26–0.32 | 1.3M–1.6M |
| merge, every key new | 1,000,000 | 0.28–0.71 | 564k–1.4M |
| merge, half already stored | 1,000,000 | 0.76–0.89 | 449k–527k |
| **merge through `Table.upsert`** | one commit | 11.3–11.6 (for **4,000** rows) | **344–354** |

The last row is not a typo and not the same amount of work: pyiceberg's own
upsert was given a hundredth of the data and still took twenty times longer than
the full merge above it. Its scan filter carries one equality term per incoming
row, so the cost grows faster than the chunk:

| chunk rows | 1 join column | 2 join columns |
| --- | --- | --- |
| 500 | 6,200 rows/s | 700 rows/s |
| 1,000 | 9,100 rows/s | 730 rows/s |
| 2,000 | 7,100 rows/s | 590 rows/s |
| 4,000 | 10,700 rows/s | 440 rows/s |

Head to head on the same table (4,000-row chunk, 20,000-row table), with
identical results:

| scenario | planned merge | `Table.upsert` |
| --- | --- | --- |
| every key new | 0.09 s | 2.47 s (28×) |
| every key stored, values unchanged | 0.20 s | 9.83 s (48×) |
| half new, half unchanged | 0.20 s | 8.12 s (42×) |

Those are the streaming shapes: new data, and replays of data that has not
changed. A merge where most rows genuinely *change* is a different story —
about 2× — because the delete half still carries pyiceberg's exact per-row
filter, which it must: a range there would delete rows the chunk never touched.
Finding the rows is what got fast; rewriting them costs what it costs, and
[Updating what is stored](#updating-what-is-stored) is that half on its own.

[How a merge is planned](#how-a-merge-is-planned) explains why. The two paths
are compared row by row in `tests/iceberg/test_coherence.py`; set
`plan_merges=False` to use the library's own.

Building that scan filter is itself measured, because it runs once per commit
and it used to hash the whole key column to decide it could not name the values
in it. Probing a 201-row slice first answers the same question, on a 400k-row
chunk:

| merge key | before | after |
| --- | --- | --- |
| one high-cardinality integer | 27.0 ms | 0.4 ms |
| an integer and a string | 69.9 ms | 6.3 ms |
| one eight-value partition column | 1.3 ms | 1.5 ms |

The last row is the tax: where there really are few distinct values, the probe
is paid on top of the full pass — and that is the case where naming them one by
one prunes to exactly the right partitions, so it is worth paying.

### Partitioning and properties

| case | commit rows | rows/s | files |
| --- | --- | --- | --- |
| append, partitioned by day | 65,536 | 623k–797k | 14 |
| append, no partition | 50,000 | 1.01M–1.03M | 7 |
| append, Iceberg's default properties | 50,000 | 802k–852k | 14 |
| merge, no partition | 50,000 | 506k | 5 |

Partitioning costs about half the write throughput here, because eight days
means up to eight files per commit instead of one. It buys the read below.

### Reading it back

500,000 rows in 15 files, best of three, **warmed twice** before anything is
timed — once for the process and once per case. Without that the sweep was a
story about warm-up: three back-to-back runs put "everything" at 0.057, 0.031
and 0.027 s, a 2.1× spread that is nothing but an Acero join and a parquet
reader paying their setup in whichever case ran first.

`planned` is how many files the scan opened; `skipped` is what the filter saved.
The counts reproduce exactly; the seconds are a shared machine and move ±30%
between runs, so both runs are quoted.

| case | seconds | rows | planned | skipped |
| --- | --- | --- | --- | --- |
| everything | 0.080–0.111 | 500,000 | 15 | 0 |
| `date = '2026-08-14'` (partition) | 0.024–0.025 | 62,500 | 1 | 14 |
| partition + 3 of 8 columns | 0.016–0.019 | 62,500 | 1 | 14 |
| 3 of 8 columns, no filter | 0.055–0.061 | 500,000 | 15 | 0 |
| `unix < …` (correlates with the partition) | 0.042–0.062 | 125,000 | 3 | 12 |
| `driver_name = 'ULBridge'` (no useful statistics) | 0.093–0.105 | 125,000 | 15 | **0** |
| narrow shape, projection from the shape | 0.058–0.064 | 500,000 | 15 | 0 |
| narrow shape declared with the store's widths | 0.050–0.059 | 500,000 | 15 | 0 |

One more, measured separately because it is a write-side choice: sorting each
commit on the column a read filters. On a single 600k-row commit, a filter
matching the top 5% of `unix` values took **214 ms** when the rows
arrived shuffled and **22 ms** when the commit was sorted
(`sort_by=["unix"]`), with one file planned in both cases. That is
row-group skipping inside the file, and it only exists because
`write.parquet.row-group-limit` is set: Iceberg's default of a million rows per
group would make the whole file one group with nothing to skip.

Three things worth taking away:

- **A partition filter is worth 14 of 15 files.** A filter on a column that
  merely *correlates* with the partition still skips 12 — Iceberg prunes on
  per-file column bounds, not only on partitions.
- **A filter that cannot prune says nothing about it.** The `driver_name` filter
  returns exactly the right rows and reads every file. `scan_plan` is how you
  see that:

    ```python
    quotes.scan_plan("driver_name = 'ULBridge'")["skipped"]   # 0
    ```

- **Declaring narrower widths than the store costs a conversion per row —
  less than these docs used to claim.** Isolated, warm and best-of-nine, twice:
  three columns into `string` took 60.2–64.4 ms against 55.9–61.6 ms in the
  store's own `large_string`, with medians 68.6–72.8 against 62.0–67.8. That is
  7–9%, consistently in the same direction, and not the 25% an unwarmed sweep
  reported — the difference was warm-up charged to whichever case ran first.
  Declaring the store's widths (`dataset.table_field`) is still the right
  answer where a read is hot and the shape is only there to select columns; it
  is not the difference it looked like.

### Store calls

`bench_iceberg.py --only fs` counts store calls instead of seconds, for the
reason given under [what the store is asked](#what-the-store-is-asked): the
count is taken on the file handles themselves, below the cache, so one entry is
a call the store actually served — one `open` a GET, one `create` a PUT.
100,000 rows streamed in 8 commits, a local warehouse, measured twice with
identical counts:

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

Two of the uncached counts moved since, without the cache being involved at
all: `scan_plan` under a filter planned the table a second time purely to count
what an unfiltered scan would touch (11 → 3), and the sweep stopped walking the
manifests once per half of its live set (71 → 69).

Three separate changes produce the cached column, and they compose:

- **The content cache** ([what the store is asked](#what-the-store-is-asked)):
  manifests, manifest lists and `metadata.json` are immutable, so `ArrowFileIO`
  serves them from memory after the first fetch — and caches them *as they are
  written*, which is why a pure append stream makes zero GETs: every file the
  next chunk's plan wants is one this process just wrote. Over the sweep: 214
  hits, 0 misses, ~500 KiB held.
- **Plan-once merges**: a merge plans its scan once, and a plan of zero files
  commits the chunk as an append with no reader built — so `merge, all new`
  matches `append stream` exactly. (`Table.upsert` reads the table either way.)
- **Limit-aware planning**: with no filter, `limit` cuts the *plan*, not just
  the rows — one file opened where pyiceberg submits all eight.

The wall-clock is not the point on a local disk — the counts are exact and
reproduce to the call, the seconds are noisy — but it moves the same way: the
cached append stream landed in 0.29–0.32 s against 0.95–1.6 s uncached across
three runs. On an object store, where every one of those calls is a round
trip, the counts *are* the seconds.

### Maintenance, and what a reader holds

`bench_iceberg.py --only maintain`. Three things seconds on a local disk answer
badly and counts answer exactly. Same fixture, both legs run twice, every number
below reproduced to the digit.

**What a reader holds.** One batch of a 20-file, 21.1 MiB table, with a consumer
that is not instantaneous:

| | files opened | MiB held |
| --- | --- | --- |
| before | 20 | 18.9 |
| after | **8** | **7.3** |

`ArrowScan` submits every planned file to its thread pool at once and each
finished one holds a whole file's decoded batches until the consumer reaches it,
which makes `read_arrow_reader` a `read_arrow_table` that takes longer. The plan
now goes over a group at a time, the group being the pool's own width — so what
is held scales with the pool, not with the table. Draining the whole reader took
54.6–56.5 ms against 59.6–72.4 ms before: the bound costs nothing. The same
change is quoted under [A dataset](#a-dataset) on a bigger table — 24 files and
99 MiB, taken to 8 files and 32.5 MiB — which is a second fixture, not a second
reading of this one.

**What `maybe_optimize` walks to say no**, which is every call of a converged
`auto_optimize` stream:

| table | partition reads | manifest reads | seconds |
| --- | --- | --- | --- |
| quiet, before | 1 | 1 | 0.005–0.006 |
| quiet, after | **0** | **0** | 0.001–0.002 |
| settled, before | 1 | 4 | 0.009 |
| settled, after | **0** | **0** | 0.001 |

**Whether compaction converges**, in files rewritten per run:

| partitioning | run 1 | run 2 | run 3 |
| --- | --- | --- | --- |
| identity (`unix_hour`) | 13 | 0 | 0 |
| none | 10 | 0 | 0 |
| transform (`day`), before | 13 | **4** | **4** |
| transform (`day`), after | 13 | **0** | **0** |

The four-files-forever is what made every `optimize` on a `day`- or
`bucket[16]`-partitioned table a full read and a full rewrite, while the rows
never changed. The [Everything](#maintenance) tab tells the same story on its
own fixture of four commits over four days, where the counts are 16, then 4,
then 4.

### Backfilling

`bench_iceberg.py --only backfill`. A replay of keys that sit in a few bands of
a wide table — 20 files of 5,000 rows, the key clustered per file, the hash half
of it drawn per row so file bounds on it span everything and prune nothing.
`planned` is what the scan opens; the rows it returns say nothing about that.

| case | planned | seconds |
| --- | --- | --- |
| two distant bands | 18 → **2** | 0.14–0.19 → 0.05–0.09 |
| one contiguous band | 1 → 1 | 0.02 → 0.04–0.06 |
| half the table | 10 → 10 | 0.14–0.16 → 0.13–0.21 |

The planned counts are exact and reproduce to the file; the seconds are a
shared machine and move ±40% between rounds, so both rounds are quoted.

Past 200 distinct values a key column cannot be named one value at a time, and
the single min/max range it became spans everything between the bands. It is
described by up to eight ranges now, found by placing every value in one of 64
equal slices of `[min, max]` and merging the occupied ones back — no sort, and
a slice reports the exact min and max of what landed in it, so the union covers
every value however the index arithmetic rounded.

The last two rows are the cost, quoted because they are real: a chunk with no
gap to find pays the banding pass and prunes exactly what it did before. On a
400k-row integer column that is 7.8 ms, against 31–420 ms for the `unique` that
sorting would need.

### Updating what is stored

`bench_iceberg.py --only update`. A merge that *inserts* is measured everywhere
else here; this is the half that rewrites. The filter naming the rows to delete
is one term per row for a composite key, and pyiceberg binds that tree once per
manifest it plans. 10,000 stored rows, both legs run twice — and because both
runs of the sweep are quoted, the after column is a range.

**A key one half of which repeats** — `(symbol, day)` over four days:

| rows updated | terms | seconds | rows/s |
| --- | --- | --- | --- |
| 500 | 1,000 → **2** | 0.95 → **0.12–0.19** | 527 → 2,682–4,256 |
| 2,000 | 4,000 → **2** | 4.55 → **0.19–0.22** | 440 → 9,105–10,689 |
| 5,000 | 10,000 → **4** | 21.16 → **0.42–0.43** | 236 → 11,581–11,991 |

`terms` is the leaf count of the delete filter, and it is the whole story:
profiled at 5,000 rows, 15.8 of the 18.1 seconds went on nineteen
`_InclusiveMetricsEvaluator` constructions at 770 ms each, against 0.4 ms for
the key ranges beside it. Grouping the rows on the key column with the fewest
distinct values says the repeated half once. The filter stays exact, and
[the tests](https://github.com/Platob/rekep/blob/main/python/tests/iceberg/test_coherence.py)
compare it against pyiceberg's own row for row rather than against itself.

**A key that repeats nothing** — `(at, hash)`, where every value of both
halves is distinct — is left alone, because one group per row is the tree
`create_match_filter` already builds. Its numbers are what a merge of many
updates costs when nothing can be factored out of it, and they are here because
a sweep that left them out would read as a claim about merges rather than about
keys:

| rows updated | terms | seconds | rows/s |
| --- | --- | --- | --- |
| 500 | 1,000 | 0.51–0.54 | 921–988 |
| 2,000 | 4,000 | 3.08–3.10 | 645–649 |
| 5,000 | 10,000 | 11.76–11.94 | 419–425 |

That is roughly linear in the rows updated, and it is pyiceberg's cost: an
exact filter over *n* arbitrary key pairs is *n* terms, and there is no smaller
way to say it.
