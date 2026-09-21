# parse_fix_silver

Read a bounded ordered history from `fix.bronze`, walk lifecycle state, and
write only this job's `fix.silver` events.

## Task document

```json
{
  "name": "parse_fix_silver",
  "application": "parse_fix_silver.py",
  "parameters": {
    "bronze": "fix.bronze",
    "registry": null,
    "codec_options": null,
    "start": null,
    "end": null,
    "catalog": {
      "name": "rekep",
      "properties": {
        "type": "sql",
        "uri": "sqlite:///data/catalog.db",
        "warehouse": "data/warehouse"
      }
    }
  }
}
```

## Walk the chains

`codec_options` has the same contract as bronze: `null` uses native defaults,
and an object is forwarded unchanged to `FixCodec`. Give both stages the same
semantic parse options; `snapshot_ns` affects lifecycle output only.

Lifecycle enrichment is stateful and ordered. It fills `prevuuid`, `seqnum`,
`parentuuids`, folded state, creation and expiry. Expiry is emitted at its
deadline; a replaced or cancelled generation cannot later emit a stale expiry.
With `snapshot_ns > 0`, the walk also emits owned views of every live event on
the requested nanosecond grid. Zero, the default, disables snapshots.

Parsing and local per-event enrichment may run in parallel, but prior-event
state belongs only to lifecycle. Native version 0.1.8 consumes a finite input
and stable-sorts it before walking. The Arrow scan streams batches into that
door, yet lifecycle still collects the selected rows. Undated rows are read
from the epoch partition on every job and that partition may grow, so this is
not a batch-memory-bounded path.

## Window and order

For a job window `[start, end)`, the task reads `[start - 1 hour, end)` with
`fix_window_filter` and requests `order_by=SORT_COLUMNS`, which is
`currunix, seqnum, curruuid`. Undated rows in the epoch partition are included
so lifecycle can date them from message facts. Native lifecycle stable-sorts
by effective event time only: equal-time messages retain the caller's order.
The stored reader supplies the deterministic tie order above; it does not
promise the same lineage as a differently ordered raw capture at an equal
timestamp.

Iceberg prunes chronological hour paths. Disjoint file ranges are concatenated
and overlapping ranges are externally merged with at most 16 input streams in
one merge step. The task does not build a Python `read_all` union before the
native walk.

The previous hour is context only. The output retains rows whose native
`currunix` is inside the job window, plus rows that remain unresolved at the
epoch floor. It excludes expiry events after the job end. This one-hour horizon
does not claim to reconstruct arbitrary chains whose relevant predecessor is
older.

## Read, order, widen, walk, narrow, write

`fix_lifecycle_arrow_reader` reads and emits the same 123-column **FixMsg**
field that bronze stored. `overwrite_arrow_reader(..., merge_by=True)` replaces matching `curruuid`
rows inside affected hourly partitions.

The identity field is `curruuid`. Iceberg's merge behavior is
generic: append is blind and overwrite is partition-scoped by whatever
identifier fields the dataset declares. No FIX-specific key rule lives in the
storage layer.

`eventtime` for downstream staging is the lifecycle's native `currunix`.
Reapplying `TransactTime` would incorrectly move a synthetic expiry back to an
older inherited transaction time.

## Row behavior

- Duplicate deliveries are removed without conflating distinct events.
- `crosscode` follows business-identifier priority: `OrderID`, `ClOrdID`,
  `OrigClOrdID`, `QuoteID`, `QuoteReqID`, `MDReqID`.
- Capture `session:context` is `identifiers["msgsectxid"]`, not a chain key.
- Lifted values are not duplicated in residual `fixentries`; unknown and
  unrepresentable content remains there.
- Reconstructing a row preserves canonical message semantics and its recorded
  event identity, not original wire ordering.

## Registry override

Set `registry` only to replay with an explicit dictionary. Bronze and silver
must use the same field and central `FIX:codeset` vocabularies. Review and
deploy the resulting schema change before writing it.

## Presentation

The interactive result reads row and partition counts from snapshot metadata.
Its visible chain is a bounded, ordered sample of at most 64 rows from the job
window; it never scans the full silver table merely to render the notebook.

## Sample rows

The checked sample is generated from the current task and owns its measured
row counts.

--8<-- "docs/pipeline/tasks/samples/parse-fix-silver.md"

## Migrating a warehouse that holds an older table

Deploy the reviewed `schemas/rekep/fixmsg.json` contract before replaying.
Schema replacement and data replay remain separate operations: replace the
declaration, then run bronze and silver for the intended windows.

## Run

```bash
uv run --project python rekep task run tasks/parse_fix_silver/parse_fix_silver.json \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
```
