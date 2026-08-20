# Iceberg

`IcebergDataset` is an Iceberg table you read and write as Arrow. pyiceberg does
the Iceberg — planning scans, writing files, committing snapshots — and this
adds the two ends: the **shape** the data is cast onto, and the **streaming**
that keeps a commit from happening once per batch. Then it adds the part nobody
enjoys writing: the maintenance.

```bash
pip install "rekep[iceberg]"
```

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
    `py-io-impl` → Arrow's `PyArrowFileIO`, so Iceberg reads, writes and
    maintenance all go through the same `pyarrow.fs` handles as everything else
    — one credential chain, one set of URI rules. Naming another wins.

=== "Namespaces"

    ```python
    catalog.namespaces()                          # ['trading']
    space = catalog.create_namespace("trading", {"owner": "desk"})
    space.properties                              # {'owner': 'desk'}
    space.update_properties({"owner": "risk"})
    space.tables()                                # ['trading.quotes']
    catalog.drop_namespace("trading")             # missing is not an error
    ```

=== "Tables"

    ```python
    catalog.tables()                              # every namespace
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
    struct=Quote.FIELD,     # optional: the shape to create the table from
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
    ```

    Writes **append by default and create what is not there yet**. The stream is
    cast onto the shape first, so a nearly-right batch lands instead of failing
    pyiceberg's schema check.

=== "Read"

    ```python
    quotes.read_arrow_table()
    quotes.read_arrow_reader()                             # streamed
    quotes.read_arrow(pyarrow.RecordBatchReader)           # same, by type
    quotes.read_arrow_table(
        row_filter="day = '2026-08-14' and size > 10",     # pushed to the planner
        columns=["symbol", "size"],                        # so is the projection
        limit=1_000,
        snapshot_id=...,                                   # or an older state
        branch="dev",                                      # or another line of it
    )
    ```

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
| `True` | upsert on the primary key the shape declares |
| `["symbol", "day"]` | upsert on those columns |

```python
quotes.write_arrow(table, merge_by=True)
```

An upsert is pyiceberg's own: it plans the matching rows itself, which is a job
for the engine that holds the statistics. Declaring the key once —
`Annotated[str, Field.primary_key()]` — is what makes `merge_by=True` mean
something.

## Schema evolution

=== "Add columns"

    ```python
    wider = Quote.FIELD.merge_with(pyarrow.schema([("desk", pyarrow.string())]))

    quotes.add_fields(wider)              # ['desk'] -- and the declared shape follows
    quotes.add_fields(wider)              # [] -- nothing new, so no commit
    quotes.add_fields(wider, dry_run=True)
    ```

=== "What it does"

    Schema evolution as a merge, in Iceberg's own terms: matched by name, new
    columns added **optional** (rows already written have nothing to put in
    them), everything else left alone, at every level. Nothing is ever dropped
    or retyped here — that is a migration, not an evolution, and it should be
    deliberate.

## Snapshots and branches

=== "Look"

    ```python
    quotes.snapshots()          # Iceberg's own metadata table
    quotes.refs()               # branches and tags
    quotes.data_files()         # every data file the current snapshot holds
    ```

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
    # [("day = '2026-08-14'", 12), ("day = '2026-08-15'", 4)]
    ```

    Partition by partition when every partition field is an identity of a
    column: then a partition *is* a predicate, and rewriting one touches nothing
    else. A derived transform (`day`, `bucket[16]`) hides which rows are where,
    so the honest plan is the whole table at once.

=== "Clean up"

    ```python
    quotes.cleanup(retain=3)                                   # keep 3 snapshots
    quotes.cleanup(older_than=datetime.timedelta(days=7))
    quotes.cleanup(dry_run=True)   # {'expired': 12, 'deleted': 40, 'bytes': 91234}
    ```

    Expiry in pyiceberg is metadata-only: it forgets snapshots, it does not
    remove what they were keeping alive. This does both — and the sweep is
    conservative on purpose: a file goes only when no live snapshot references
    it **and** it is older than `orphan_age` (three days by default), because a
    writer committing right now has files on disk that no snapshot mentions yet.
    Branch and tag heads are never expired.

=== "Everything"

    ```python
    quotes.optimize()
    # {'rewritten': 24, 'expired': 12, 'deleted': 24, 'bytes': 1048576}
    ```

    Manifest merging on, then compact, then expire and sweep — in that order,
    because compacting makes the snapshots that cleanup then expires, and
    merging manifests first means those commits land in fewer of them.

=== "Properties"

    ```python
    quotes.set_properties({"write.target-file-size-bytes": "268435456"})
    quotes.iceberg_table.properties
    ```

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
