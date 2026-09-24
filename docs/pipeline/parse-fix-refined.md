# parse_fix_refined

`parse_fix_refined(catalog, window, *, codec=None, source=RAW, target=REFINED)`
reads a bounded ordered history from `fix.raw`, walks lifecycle state, and
lands only the events it places in the window in `fix.refined`.

```python
import tempfile
from pathlib import Path

import pyarrow.compute

from rekep.iceberg import IcebergCatalog
from rekep.pipeline import Landed, parse_fix_raw, parse_fix_refined, parse_messages
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
try:
    parse_messages("file:data/capture", catalog, day)
    parse_fix_raw(catalog, day)
    # The walk merges the observations of one event and adds its expiry.
    assert parse_fix_refined(catalog, day) == Landed(read=49, written=19)

    refined = catalog.dataset("fix.refined").read_arrow_table()
    expired = pyarrow.compute.equal(refined.column("state"), "95EXPIRED")
    # The one row no line recorded is the expiry the walk generated.
    assert refined.filter(expired).column("recdunix").null_count == 1
    assert refined.column("recdunix").null_count == 1
finally:
    catalog.close()
```

## Walk the chains

`codec` has the same contract as the [raw stage](parse-fix-raw.md): `fix_codec()`
when None, and every pin is forwarded unchanged to `FixCodec`. Give both
stages the same codec; `snapshot_ns` affects lifecycle output only.

Lifecycle enrichment is stateful and ordered. It fills `prevuuid`, `seqnum`,
`parentuuids`, folded state, creation and expiry. Expiry is emitted at its
deadline; a replaced or cancelled generation cannot later emit a stale expiry.
With `snapshot_ns > 0`, the walk also emits owned views of every live event on
the requested nanosecond grid. Zero, the default, disables snapshots.

Parsing and local per-event enrichment may run in parallel, but prior-event
state belongs only to lifecycle. Native lifecycle processing still
collects and stable-sorts its finite scan result. The Arrow scan streams
batches into that door, yet lifecycle still collects the selected rows.
Undated rows are read
from the epoch partition on every run and that partition may grow, so this is
not a batch-memory-bounded path.

## Window and order

For a window `[start, end)`, the stage reads `[start - HISTORY, end)`, where
`rekep.pipeline.HISTORY` is one hour, with `fix_window_filter` and requests
`order_by=SORT_COLUMNS`, which is `currunix, seqnum, curruuid`, projected to
the field: the filter, the order and the projection are everything the scan
can take, and all three are pushed into it. The selection of the window after
the walk stays in Python because the walk needs its context rows. Undated rows
in the epoch partition are included so lifecycle can date them from message
facts. Native lifecycle stable-sorts by effective event time only: equal-time
messages retain the caller's order. The stored reader supplies the
deterministic tie order above; it does not promise the same lineage as a
differently ordered capture at an equal timestamp.

Iceberg prunes chronological hour paths. Disjoint file ranges are concatenated
and overlapping ranges are externally merged with at most 16 input streams in
one merge step. The stage does not build a Python `read_all` union before the
native walk.

The previous hour is context only. The output retains rows whose native
`currunix` is inside the window, plus rows that remain unresolved at the
epoch floor. It excludes expiry events after the window's end, which wait for
their own window. This one-hour horizon does not claim to reconstruct
arbitrary chains whose relevant predecessor is older.

## Read, order, widen, walk, narrow, write

`fix_lifecycle_arrow_reader` reads and emits the same 128-column **FixMsg**
field that `fix.raw` holds. The write replaces matching `curruuid` rows inside
affected hourly partitions.

The identity field is `curruuid`. Iceberg's merge behavior is
generic: append is blind and overwrite is partition-scoped by whatever
identifier fields the dataset declares. No FIX-specific key rule lives in the
storage layer.

`eventtime` for downstream staging is the lifecycle's native `currunix`.
Reapplying `TransactTime` would incorrectly move a synthetic expiry back to an
older inherited transaction time.

## Row behavior

- Duplicate deliveries are removed without conflating distinct events.
- `recdunix` is the earliest observation of the event and `refrecdunix` the
  clock of the observation the walk merged on; the one row no line recorded,
  the walk's expiry, states neither.
- `crosscode` follows business-identifier priority: `OrderID`, `ClOrdID`,
  `OrigClOrdID`, `QuoteID`, `QuoteReqID`, `MDReqID`.
- Capture session and context are `identifiers["msgsesseventid"]`, byte-length-prefixed with msgtype and `msgseqnum`, not a chain key.
- Lifted values are not duplicated in residual `fixentries`; unknown and
  unrepresentable content remains there.
- Reconstructing a row preserves canonical message semantics and its recorded
  event identity, not original wire ordering.

## One chain, through three tables

Business chain `00026877711XOEA0` is 27 lines of the capture, including the
partial fill and the fill that closed the order. The parse answers one
`fix.raw` row per message and the walk folds them into four events, each
naming every line it was logged on:

```python
import tempfile
from pathlib import Path

import pyarrow.compute

from rekep.iceberg import IcebergCatalog
from rekep.pipeline import parse_fix_raw, parse_fix_refined, parse_messages
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
chain = "crosscode == '00026877711XOEA0'"
try:
    parse_messages("file:data/capture", catalog, day)
    parse_fix_raw(catalog, day)
    parse_fix_refined(catalog, day)

    raw = catalog.dataset("fix.raw").read_arrow_table(row_filter=chain)
    walked = catalog.dataset("fix.refined").read_arrow_table(
        row_filter=chain, order_by=("currunix", "seqnum", "curruuid")
    )
    assert raw.num_rows == 27
    assert walked.column("state").to_pylist() == [
        "80FILLED",
        "40PARTFILL",
        "40PARTFILL",
        "80FILLED",
    ]
    state = walked.column("state")
    partial = walked.filter(pyarrow.compute.equal(state, "40PARTFILL"))
    filled = walked.filter(pyarrow.compute.equal(state, "80FILLED"))
    assert partial.column("seqnum").null_count == 2
    assert filled.column("seqnum").to_pylist() == [1, 1]
    assert sorted(filled.column("prevuuid").to_pylist()) == sorted(
        partial.column("curruuid").to_pylist()
    )

    # Provenance, not a recomputation: every line joins back by its identity.
    lines = pyarrow.table({"curruuid": pyarrow.compute.list_flatten(walked.column("srcuuids"))})
    messages = catalog.dataset("logs.messages").read_arrow_table(
        columns=("curruuid", "crosscode", "seqnum")
    )
    joined = lines.join(messages, keys="curruuid", join_type="inner")
    assert joined.num_rows == 27
finally:
    catalog.close()
```

The walked rows read in the table's own order, `currunix, seqnum, curruuid`.
Each `40PARTFILL` row starts a chain and carries no step; each `80FILLED` row
is step 1 of one, and names the `40PARTFILL` it follows in `prevuuid`.

## Registry override

Hand this stage the codec [`parse_fix_raw`](parse-fix-raw.md#registry-override)
parsed with. Both stages must use the same field and central `FIX:codeset`
vocabularies. Review the resulting schema change before writing it: the write
adds a column a newer dictionary declares, and changes nothing else.

## Migrating a warehouse that holds an older table

An older native schema or identity contract requires rebuilding affected
tables from capture under the pinned release; keyed replay does not remove
obsolete identities or repair field IDs. The steps are on
[Catalogs](../storage/catalogs.md#migrating-a-warehouse-written-under-an-earlier-yggdryl).
