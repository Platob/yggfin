# Data samples

Every page in this section is a real landing: `tools/samples_dump.py` runs
every task over the shipped capture, `data/capture/ulbridge.log`, into three
temporary local catalogs over one window, `[2026-08-14 00:00, 2026-08-14 16:30)` UTC, and writes what
they landed. Nothing here is written by hand, so a page is what a run from
the repository root lands today.

## The window

It opens at midnight, so the trade of 01:03 is in it, and closes at 16:30:
after 12:46, the day's order flow, after 14:52, its one trade report, and
after 16:25, where the walk expires the day's open order. The bridge prints
a Central European summer clock, two hours ahead of the UTC its FIX frames
state, and every line is read in `Europe/Zurich`, the zone `parse_log_messages`
reads unless told another, so a line lands in the hour of the message it
carries: bronze `log_messages` and silver both hold hour 12.
[DAGs](../dags/index.md#late-events) says what a window must hold, and what
a capture read in another zone lands.

## What each task answered

| task | writes | read | written | skipped |
| --- | --- | :---: | :---: | :---: |
| [`parse_log_messages`](../tasks/parse-log-messages.md) | `bronze.record_keeping.log_messages` | 129 | 129 | 0 |
| [`parse_fix_messages_raw`](../tasks/parse-fix-messages-raw.md) | `bronze.record_keeping.fix_messages` | 129 | 126 | 0 |
| [`parse_fix_messages_refined`](../tasks/parse-fix-messages-refined.md) | `silver.record_keeping.fix_messages` | 126 | 51 | 0 |
| [`parse_books`](../tasks/parse-books.md) | `silver.record_keeping.books` | 51 | 30 | 0 |
| [`parse_orders`](../tasks/parse-orders-quotes-executions.md) | `silver.record_keeping.orders` | 30 | 9 | 0 |
| [`parse_quotes`](../tasks/parse-orders-quotes-executions.md) | `silver.record_keeping.quotes` | 30 | 0 | 0 |
| [`parse_executions`](../tasks/parse-orders-quotes-executions.md) | `silver.record_keeping.executions` | 30 | 9 | 0 |

## What each table holds

| table | rows | sample |
| --- | :---: | --- |
| [bronze.record_keeping.log_messages](../tables/bronze/log_messages.md) | 129 | [rows](bronze/log_messages.md) |
| [bronze.record_keeping.fix_messages](../tables/bronze/fix_messages.md) | 126 | [rows](bronze/fix_messages.md) |
| [silver.record_keeping.fix_messages](../tables/silver/fix_messages.md) | 51 | [rows](silver/fix_messages.md) |
| [silver.record_keeping.books](../tables/silver/books.md) | 30 | [rows](silver/books.md) |
| [silver.record_keeping.orders](../tables/silver/orders.md) | 9 | [rows](silver/orders.md) |
| [silver.record_keeping.quotes](../tables/silver/quotes.md) | 0 | [rows](silver/quotes.md) |
| [silver.record_keeping.executions](../tables/silver/executions.md) | 9 | [rows](silver/executions.md) |

## Reproduce it

From the repository root, into a scratch folder:

```python
import tempfile
from pathlib import Path

from rekep import Storages
from rekep.pipeline import (
    FLATTENERS,
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
    log = parse_log_messages("file:data/capture/ulbridge.log", storages, window)
    raw = parse_fix_messages_raw(storages, window)
    refined = parse_fix_messages_refined(storages, window)
    books = parse_books(storages, window)
    events = {
        kind: task(storages, window, snapshot_id=books.snapshot_id)
        for kind, task in FLATTENERS.items()
    }

assert (log.read, log.written, log.skipped) == (129, 129, 0)
assert (raw.read, raw.written, raw.skipped) == (129, 126, 0)
assert (refined.read, refined.written, refined.skipped) == (126, 51, 0)
assert (books.read, books.written, books.skipped) == (51, 30, 0)
assert {kind: landed.written for kind, landed in events.items()} == {
    "orders": 9,
    "quotes": 0,
    "executions": 9,
}
```
