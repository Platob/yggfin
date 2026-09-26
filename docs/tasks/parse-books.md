# parse_books

`parse_books(storages, window, *, codec=None, snapshot_millis=0, source=FIX_MESSAGES, target=BOOKS)`
folds the silver FIX events of `[start, end)` into order books, replaces that
window of `silver.record_keeping.books` in one snapshot, and answers the
snapshot it committed for the [flattening tasks](parse-orders-quotes-executions.md) to read.

```python
import tempfile
from pathlib import Path

from rekep import Storages
from rekep.pipeline import (
    BOOKS,
    BOOKS_RUN,
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
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

    first = parse_books(storages, window)
    assert (first.read, first.written) == (14, 6)
    # A rerun replaces the same window with the same books, in a new snapshot.
    again = parse_books(storages, window)
    assert (again.read, again.written) == (14, 6)
    assert again.snapshot_id != first.snapshot_id

    books = storages.dataset(BOOKS)
    try:
        assert books.read_arrow_table().num_rows == 6
        runs = {
            snapshot.snapshot_id: snapshot.summary.get(BOOKS_RUN)
            for snapshot in books.iceberg_table.metadata.snapshots
        }
    finally:
        books.close()
    # Each fold's commit names the run that wrote it.
    assert None not in (runs[first.snapshot_id], runs[again.snapshot_id])
    assert runs[first.snapshot_id] != runs[again.snapshot_id]
```

## Fold

The scan reads the silver events of the strict window, `start <= currunix <
end` with no epoch or null exception, in `currunix, seqnum, curruuid` order.
`rekep.market.book_arrow_reader` reads each row back as its message and hands
them to the native book fold, which owns admission, operation kinds,
continuation, matching, expiration and book identity; the lifecycle is not
walked again.

The fold admits orders, quotes, executions, market-data updates and trade
reports; administration, requests, acknowledgements and reports that execute
nothing contribute nothing. An admitted message it cannot read is an error,
never a skipped row: the shipped capture's trade report at 14:52:55, whose
side states no `Side(54)`, is refused, and so is a cancel reject at 21:59:46
naming no symbol. The refusal leaves the table's previous snapshot visible.

```python
import tempfile
from pathlib import Path

from rekep import Storages
from rekep.pipeline import (
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
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
afternoon = window_of("2026-08-14T14:00:00Z", "2026-08-14T17:00:00Z")
with storages:
    parse_log_messages("file:data/capture/ulbridge.log", storages, afternoon)
    parse_fix_messages_raw(storages, afternoon)
    parse_fix_messages_refined(storages, afternoon)
    try:
        parse_books(storages, afternoon)
    except Exception as refusal:
        assert "expected a bid or ask side" in str(refusal)
    else:
        raise AssertionError("a trade report stating no side is refused")
```

A book starts with no depth before `start`: resting orders opened earlier are
not restored, so this is a window-local fold and not a checkpoint
reconstruction, and a partial update whose missing facts need an earlier
order can be refused. Operation times must not decrease. The native fold
keeps live depth as it streams bounded batches, so its memory grows with the
depth outstanding.

## The row

A book row is the fold's answer at an instant, per symbol: the event columns
every table opens with, the market facts of the operation that moved it,
`bidside` and `askside` -- each with its `live` depth, the `deltas` applied
since the book before and the `limits` `live` aggregates to -- and the
`executions` it traded. [The table page](../tables/silver/books.md) lists
every column and [its samples](../samples/silver/books.md) show a book with
depth. `snapshot_millis` above zero also emits a book snapshot on that
epoch-aligned millisecond grid.

The fold can emit an expiry past the last input, so the output is filtered
to the same strict window before it is written.

## Write

The write stages bounded chunks and replaces exactly `[start, end)` in one
Iceberg snapshot: rows outside the window survive, those sharing its hour
partition included, an empty rerun clears the window, and a failure leaves
the previous snapshot visible.

`snapshot_id` is the snapshot this call committed, found by the
`rekep.books-run-id` summary property (`BOOKS_RUN`) the write records with a
run identifier of its own -- never by the table's head, which a recovered
commit acknowledgement may refresh past. An empty window still commits a
snapshot, and `snapshot_id` names it. Hand it and the same window to
`parse_orders`, `parse_quotes` and `parse_executions`, so all three read one
book state even when another writer moves the table on.
