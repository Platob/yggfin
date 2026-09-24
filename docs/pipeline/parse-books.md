# parse_books

`parse_books(catalog, window, *, codec=None, snapshot_millis=0, source=REFINED, target=BOOKS)`
reads the refined FIX events of `[start, end)`, folds them into native book
continuations, replaces that window of `market.books`, and answers the book
snapshot it committed for [`parse_events`](parse-events.md) to read.

```python
import tempfile
from pathlib import Path

from rekep.iceberg import IcebergCatalog
from rekep.pipeline import (
    BOOKS_RUN,
    parse_books,
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

    first = parse_books(catalog, midday)
    assert (first.read, first.written) == (12, 5)
    # A replay replaces the same window with the same books, in a new snapshot.
    again = parse_books(catalog, midday)
    assert (again.read, again.written) == (12, 5)
    assert again.snapshot_id != first.snapshot_id

    books = catalog.dataset("market.books")
    assert books.read_arrow_table().num_rows == 5
    runs = {
        snapshot.snapshot_id: snapshot.summary.get(BOOKS_RUN)
        for snapshot in books.iceberg_table.metadata.snapshots
    }
    # Each fold's commit names the run that wrote it.
    assert None not in (runs[first.snapshot_id], runs[again.snapshot_id])
    assert runs[first.snapshot_id] != runs[again.snapshot_id]
finally:
    catalog.close()
```

The native dependency is Yggdryl 0.1.11. Install the locked environment before
folding books or deploying the Book schema.

## Native fold

The scan requests `currunix, seqnum, curruuid` order and uses strict inclusive
start and exclusive end predicates. Epoch and null timestamps receive no
exception. `fix_row_messages` restores stored FIX rows to native types, then
`FixCodec.book_arrow_reader` owns operation conversion and book state.
Refined lifecycle enrichment is already settled and is not repeated.

The native reader admits orders, quotes, actual executions, W/X market-data
updates and AE trade reports. Administration, requests, acknowledgements and
non-executing reports contribute nothing. Source errors, malformed admitted
messages and unsupported AE corrections, cancels or status reports remain
errors. Effective operation times must be nondecreasing; an X entry clock can
differ from its parent FIX timestamp. Over the whole capture day the fold
raises on the AE trade report whose side states no `Side(54)`; the
[pipeline overview](index.md#run-the-graph) names both messages that keep the
market stages to the midday hour there.

Each book holds `bid.live`, `ask.live`, the deltas applied since its previous
emission, and execution leaves. A positive `snapshot_millis` enables the
native epoch-aligned snapshot grid; zero disables it. The stage keeps books
per symbol; it does not expose global consolidation. Native scheduled
expirations can extend past the input, so output is filtered to the same
strict window before writing.

Books start with empty depth at the requested window. Resting entries created
before `start` are not restored, and partial updates may fail if their missing
facts require such a predecessor. This is a window-local fold, not checkpoint
reconstruction. Native live state grows with outstanding depth even though
Arrow output batches and Iceberg staging remain bounded.

## Storage and replay

`book_field()` derives its schema from an empty native book reader. UUIDs,
lineage, exact decimals and identifiers remain native facts. The shared
storage boundary recursively represents uint64 codes as signed bit views,
UUIDs as fixed bytes and timestamp nanoseconds as Iceberg v2 microseconds.

The write stages bounded batches, then atomically replaces exactly
`[start, end)` in one snapshot. Rows outside the interval survive, including
those sharing its hour partition. An empty rerun clears prior window rows;
a failed source or commit leaves the prior snapshot visible.

## The committed snapshot

`snapshot_id` is the snapshot this call committed, found by the
`rekep.books-run-id` summary property (`BOOKS_RUN`) the write records with a
run identifier of its own -- never by the table's head, which a recovered
commit acknowledgement may refresh past. It is zero where the table has no
snapshot and the call wrote nothing. Hand it and the same window to every
[`parse_events`](parse-events.md) kind, so all three read one book state even
when another writer advances `market.books` in between.
