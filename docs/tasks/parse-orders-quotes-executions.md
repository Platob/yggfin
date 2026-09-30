# parse_orders, parse_quotes, parse_executions

`parse_orders(storages, window, *, snapshot_id=None, commit_row_size=COMMIT_ROW_SIZE, source=BOOKS, target=ORDERS)`,
`parse_quotes(...)` with `target=QUOTES` and `parse_executions(...)` with
`target=EXECUTIONS` read one snapshot of `silver.record_keeping.books` and
replace the strict `[start, end)` window of `silver.record_keeping.orders`,
`.quotes` and `.executions`. `rekep.pipeline.FLATTENERS` maps each kind --
`orders`, `quotes`, `executions` -- to its task, and `EVENTS` to its table.

```python
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from rekep import Storages
from rekep.pipeline import (
    FLATTENERS,
    ORDERS,
    Landed,
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
    parse_orders,
)
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
storages = Storages.from_dict(
    {
        layer: {
            "name": layer,
            "properties": {
                "type": "sql",
                "uri": f"sqlite:///{root / layer}.db",
                "warehouse": str(root / layer),
            },
        }
        for layer in ("bronze", "silver", "gold")
    }
)
window = window_of("2026-08-14T00:00:00Z", "2026-08-14T16:30:00Z")
with storages:
    parse_log_messages("file:data/capture/ulbridge.log", storages, window)
    parse_fix_messages_raw(storages, window)
    parse_fix_messages_refined(storages, window)
    pinned = parse_books(storages, window).snapshot_id

    # The three kinds read one snapshot and write three tables: side by side.
    with ThreadPoolExecutor(max_workers=len(FLATTENERS)) as pool:
        running = {
            kind: pool.submit(task, storages, window, snapshot_id=pinned)
            for kind, task in FLATTENERS.items()
        }
        landed = {kind: future.result() for kind, future in running.items()}
    assert landed["orders"] == Landed(read=30, written=9, snapshot_id=pinned)
    assert landed["quotes"] == Landed(read=30, written=0, snapshot_id=pinned)
    assert landed["executions"] == Landed(read=30, written=9, snapshot_id=pinned)

    # Zero is a pinned absence: nothing is read, so the window is emptied.
    assert parse_orders(storages, window, snapshot_id=0) == Landed(
        read=0, written=0, snapshot_id=0
    )
    orders = storages.dataset(ORDERS)
    try:
        assert orders.read_arrow_table().num_rows == 0
    finally:
        orders.close()

    try:
        parse_orders(storages, window, snapshot_id=-1)
    except ValueError as refusal:
        assert "expected a nonnegative book snapshot_id or None" in str(refusal)
    else:
        raise AssertionError("a negative snapshot is refused")
```

## One book snapshot

`snapshot_id=None` pins the head the call finds, once, at its start. A
positive id reads that snapshot, and a positive id of a books table that does
not exist is refused before anything is written. Zero is what `None` pins
when the books table does not exist yet, or what a caller passes to read no
books: a pinned absence that reads nothing, never permission to follow a
newer head, so a rerun under it empties the window again. For a coordinated
run, pass all three the `snapshot_id` [`parse_books`](parse-books.md#write)
answered and the same window: they then read one book state even when
another writer moves the books table on, and they are independent of one
another, so they may run in threads, processes or separate scheduler tasks.
`Landed.snapshot_id` states the snapshot each read.

## Orders and quotes

The scan projects only the book's `deltas` (`rekep.pipeline.FLATTENED`),
the `alive` depth and the price levels excluded. Arrow flattens that list and
keeps the events whose `marketdatakind` is `ORDR`, or `QUOT`, as
`rekep.market.EVENT_KINDS` names them by `rekep.MarketDataKind` member. A
delta is an event -- a terminal one included, and every report of a fill,
beside the execution split out of it -- while `alive` depth is never
flattened: repeating resting depth would make events of entries that did not
change. A full-snapshot replacement or a range deletion does not promise a
cancellation for every member it removed.

## Executions

The scan projects only the book's `executions` list, which Arrow flattens.
The parse has already split each report of a fill into the execution it
reports, and each trade report into one execution per side it states, each
`FILLED`; the fold books each once, and this task decomposes nothing again and
copies no trade's aggregate quantity over its leaves.

## The row

The three tables share one shape, `rekep.market.market_event_field()`: the
book's execution child, so every event keeps its own identity, clock, side,
exact price and quantity, identifiers and lineage. `srcuuids` names the lines
the event was logged on, and an execution names the report it was split out
of beside them. [Orders](../tables/silver/orders.md),
[quotes](../tables/silver/quotes.md) and
[executions](../tables/silver/executions.md) list the columns;
[the samples](../samples/silver/executions.md) show every execution of the
shipped capture's window.

## Write

The output is filtered to `[start, end)`, staged `commit_row_size` rows at a
time, and replaces exactly that window in one snapshot, an empty rerun
included; rows outside it survive and a failure leaves the previous snapshot
visible. `read` counts the books of the pinned snapshot the window selected,
`written` the events stored, and `skipped` the events answered but not
written, normally zero. After a book replacement, run the three again against
the new snapshot.
