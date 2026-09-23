# parse_orders

Read one pinned `market.books` snapshot and write `market.orders` for the
requested strict `[start, end)` window.

## Task document

These are the executable defaults:

```json
--8<-- "tasks/parse_orders/parse_orders.json"
```

`snapshot_id=null` resolves the current source snapshot once at task entry;
an explicit ID reads that snapshot. Zero is the empty-source sentinel and
never follows a later head. For a coordinated fan-out, pass the ID returned
by `parse_books` to all three event tasks. Airflow also copies the parent's
actual window, preventing generic task overrides from splitting the fan-out.

## Projection

The Iceberg scan projects only `bid.deltas` and `ask.deltas`, excluding live
depth and side summaries. Arrow flattens those lists, selects native
`operationkind == "order"`, then projects the shared MarketEvent fields.
FIX `marketoperationid` retains the message category and cannot select this
child kind. The task does not flatten `live`: repeating resting depth would
manufacture events for unchanged entries.

Terminal deltas remain events. A full-snapshot membership replacement or
range deletion does not promise a synthetic cancellation for every removed
member. Each selected event keeps its own identity, clock, side, exact price
and quantity, identifiers and lineage.

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
--8<-- "tasks/parse_orders/parse_orders.py"
```
