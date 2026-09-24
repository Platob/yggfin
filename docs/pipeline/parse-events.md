# parse_events

`parse_events(kind, catalog, window, *, snapshot_id=None, source=BOOKS, target=None)`
reads one pinned `market.books` snapshot and replaces the strict `[start, end)`
window of `market.<kind>`: `market.orders`, `market.quotes` or
`market.executions`, which `EVENTS[kind]` names. Any other `kind` is refused.

```python
import tempfile
from pathlib import Path

from rekep.iceberg import IcebergCatalog
from rekep.pipeline import (
    Landed,
    parse_books,
    parse_events,
    parse_fix_raw,
    parse_fix_refined,
    parse_messages,
)
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
catalog = IcebergCatalog.from_dict(
    {
        "name": "rekep",
        "properties": {
            "type": "sql",
            "uri": f"sqlite:///{root}/catalog.db",
            "warehouse": str(root / "warehouse"),
        },
    }
)
day = window_of("2026-08-14", "2026-08-14")
midday = window_of("2026-08-14T12:00:00Z", "2026-08-14T13:00:00Z")
try:
    parse_messages("file:data/capture", catalog, day)
    parse_fix_raw(catalog, day)
    parse_fix_refined(catalog, day)
    books = parse_books(catalog, midday)

    pinned = books.snapshot_id
    assert parse_events("orders", catalog, midday, snapshot_id=pinned) == Landed(
        read=5, written=1, snapshot_id=pinned
    )
    assert parse_events("quotes", catalog, midday, snapshot_id=pinned).written == 0
    assert parse_events("executions", catalog, midday, snapshot_id=pinned).written == 7

    # Zero is a pinned absence: nothing is read, so the window is emptied.
    assert parse_events("orders", catalog, midday, snapshot_id=0) == Landed(
        read=0, written=0, snapshot_id=0
    )
    assert catalog.dataset("market.orders").read_arrow_table().num_rows == 0

    try:
        parse_events("trades", catalog, midday)
    except ValueError as refusal:
        assert "expected orders, quotes or executions" in str(refusal)
    else:
        raise AssertionError("a kind the book does not hold is refused")
finally:
    catalog.close()
```

## One book snapshot

`snapshot_id=None` pins the head this call finds, once, at its start. A
positive ID reads that snapshot, and a positive ID of a table that does not
exist is refused before anything is written. Zero is the snapshot a book write
with no head answers: it is a pinned absence and reads nothing, never
permission to follow a newer head, so a replay under it empties the window
again. For a coordinated fan-out, pass the `snapshot_id` returned by
[`parse_books`](parse-books.md#the-committed-snapshot) and the same window to
all three kinds: they then read one book state even when another writer
advances `market.books`, and they are independent of one another, so they may
run in separate processes. `Landed.snapshot_id` states the snapshot each one
read.

## Orders and quotes

The Iceberg scan projects only `bid.deltas` and `ask.deltas` (`FLATTENED`),
excluding live depth and side summaries. Arrow flattens those lists, selects
native `operationkind == "order"` or `"quote"`, then projects the shared
MarketEvent fields. FIX `marketoperationid` retains the message category and
cannot select this child kind. The stage does not flatten `live`: repeating
resting depth would manufacture events for unchanged entries.

Terminal deltas remain events. A full-snapshot membership replacement or
range deletion does not promise a synthetic cancellation for every removed
member. Each selected event keeps its own identity, clock, side, exact price
and quantity, identifiers and lineage.

## Executions

The scan projects only the book's root `executions` list, and Arrow flattens
it. Native code has already expanded admitted AE trades into explicitly sided
execution leaves; this stage does not decompose them again or copy a trade
root's aggregate quantity over each child. Each event keeps its own native
identity, clock, side, price, quantity and provenance.

## Shape

The projection uses Arrow list, struct and filter kernels over record
batches, without Python row dictionaries. `book_event_arrow_reader` accepts
only `orders`, `quotes` or `executions` as its requested kind. The output
schema, `market_event_field()`, derives from the native execution child and
is shared by the three flat market tables.

## Replay and commit

Source and output filtering use inclusive start and exclusive end, without
an epoch or null exception. The writer stages bounded files and atomically
replaces exactly that window, including an empty rerun. Outside rows are
preserved; a source or commit failure leaves the previous snapshot visible.
Native UUIDs remain keys, and the shared storage boundary preserves exact
decimals while narrowing timestamps to Iceberg v2 microseconds. After a book
replacement, run each kind again against the new book snapshot.
