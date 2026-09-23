# parse_executions

Read one pinned `market.books` snapshot and write `market.executions` for the
requested strict `[start, end)` window.

## Task document

These are the executable defaults:

```json
--8<-- "tasks/parse_executions/parse_executions.json"
```

`snapshot_id=null` resolves the current source snapshot once at task entry;
an explicit ID reads that snapshot. Zero is the empty-source sentinel and
never follows a later head. For a coordinated fan-out, pass the ID returned
by `parse_books` to all three event tasks. Airflow also copies the parent's
actual window, preventing generic task overrides from splitting the fan-out.

## Projection

Arrow flattens the book's root `executions` list. Native code has already
expanded admitted AE trades into explicitly sided execution leaves; this
task does not decompose them again or copy a trade root's aggregate quantity
over each child. Each event keeps its own native identity, clock, side,
price, quantity and provenance.

The projection uses Arrow list, struct and filter kernels over record
batches, without Python row dictionaries. `book_event_arrow_reader` accepts
only `orders`, `quotes` or `executions` as its requested kind; another name
is refused. The output schema derives from the native execution child and is
shared by the three flat market tables.

## Replay and commit

Source and output filtering use inclusive start and exclusive end, without
an epoch or null exception. The writer stages bounded files and atomically
replaces exactly that window, including an empty rerun. Outside rows are
preserved; a source or commit failure leaves the previous snapshot visible.
Native UUIDs remain keys, and the shared storage boundary preserves exact
decimals while narrowing timestamps to Iceberg v2 microseconds.

Use the [run guide](../operations/run.md#market-events-from-one-book-snapshot)
to launch all three projections against one snapshot. They are independent
and can execute concurrently after book creation succeeds.

## Application

```python
--8<-- "tasks/parse_executions/parse_executions.py"
```
