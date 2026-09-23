# parse_books

Read refined FIX events in `[start, end)` and write native book continuations
to `market.books`. The task returns the exact committed book `snapshot_id`
for the three downstream event tasks.

## Task document

These are the executable defaults:

```json
--8<-- "tasks/parse_books/parse_books.json"
```

The native dependency is Yggdryl 0.1.11. Install the locked environment before
running or deploying the Book schema.

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
differ from its parent FIX timestamp.

Each book holds `bid.live`, `ask.live`, the deltas applied since its previous
emission, and execution leaves. A positive `snapshot_millis` enables the
native epoch-aligned snapshot grid; zero disables it. This task keeps books
per symbol; it does not expose global consolidation as a task parameter.
Native scheduled expirations can extend past the input, so output is filtered
to the same strict window before writing.

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

## Run and fan out

This example assumes your deployed `fix.refined` contains valid market
records in the named window. It does not read the bundled August ingestion
fixture, whose incomplete AE side is correctly refused by native projection.

```bash
uv run --project python rekep task run tasks/parse_books/parse_books.json \
  --parameter 'start="2026-09-21T10:00:00Z"' --parameter 'end="2026-09-21T10:00:10Z"' \
  --result-file /tmp/rekep-books.json
```

Pass that result's `snapshot_id` and exact bounds to
[orders](parse-orders.md), [quotes](parse-quotes.md) and
[executions](parse-executions.md). They can run concurrently. Airflow injects
the validated parent result rather than independently following the book
head. Snapshot ID zero means no source head and must remain empty on replay.

## Application

```python
--8<-- "tasks/parse_books/parse_books.py"
```
