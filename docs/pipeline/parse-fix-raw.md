# parse_fix_raw

`parse_fix_raw(catalog, window, *, codec=None, source=MESSAGES, target=RAW)`
parses the stored lines of one window into settled FIX events and lands them
in `fix.raw`. Nothing in this stage walks lifecycle chains.

```python
import tempfile
from pathlib import Path

import pyarrow.compute

from rekep.fix import UNDATED, fix_codec
from rekep.iceberg import IcebergCatalog
from rekep.pipeline import Landed, parse_fix_raw, parse_messages
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
codec = fix_codec(threads=2, batch_row_size=4_096)
try:
    parse_messages("file:data/capture", catalog, day)
    # 144 lines answer 79 messages, and the key folds the 30 that restate an
    # event another hop already logged: 49 events.
    assert parse_fix_raw(catalog, day, codec=codec) == Landed(read=144, written=49, skipped=30)

    raw = catalog.dataset("fix.raw").read_arrow_table(
        columns=("currunix", "seqnum", "prevuuid", "parentuuids", "srcuuids")
    )
    # Nothing has walked: no row has a place in a chain yet.
    for walked in ("seqnum", "prevuuid", "parentuuids"):
        assert raw.column(walked).null_count == raw.num_rows == 49
    # Each row names the one line it was parsed out of.
    assert pyarrow.compute.all(
        pyarrow.compute.equal(pyarrow.compute.list_value_length(raw.column("srcuuids")), 1)
    ).as_py()
    # A message that stated no clock waits at the pin for the walk to date it.
    undated = pyarrow.compute.equal(raw.column("currunix"), pyarrow.scalar(UNDATED))
    assert pyarrow.compute.sum(undated).as_py() == 33
finally:
    catalog.close()
```

## The parse, and nothing after it

The stage prunes `logs.messages` to `[start, end)` on `currunix`, the event the
read settled over each line, and reads the lines at the epoch pin beside
them, where a handle with no clock at all leaves a line. `currunix` is the
partition column itself and is never null, so the predicate names it and
Iceberg projects the bounds through the hour transform: the window's
partitions and the pin's own hour are opened, and nothing else. The scan is
projected to `rekep.fix.PARSE_COLUMNS`, the seven columns the parse consumes:
`currunix`, `curruuid`, `body`, `msgsessionid`, `msgctxid`, `msgseqnum` and
`msgpluginid` -- the line's clock, which becomes the message's `recdunix` and
`refrecdunix`; its identity, which becomes `srcuuids`; the body the frames
are read out of; and the four captures that fill a field by name.
`currhashcode`, `crosscode`, `seqnum`, `msgthreadid` and `loglevel` are not
read at all. `fix_parse_arrow_reader` is the batch form of the native
`FixCodec.parse_text_arrow_reader`: it selects the fixed row's 128 columns
off the parse's answer, which leads with the carried `body`. One line may
answer zero, one, or several messages. Parsing and local enrichment are
independent per event and may execute concurrently. `threads` defaults to
the available CPU count and zero becomes one. Output order remains input
order.

`codec` is the whole parse surface. `fix_codec(registry, **pins)` forwards
every pin unchanged to `FixCodec`, which validates each keyword natively;
Python keeps no whitelist or second interpretation. Common pins include
`batch_row_size`, `batch_byte_size`, `include_msgtypes`, `exclude_msgtypes`,
`threads`, `official_time_delay_ms`, and `snapshot_ns`.
Batching defaults to 32,768 rows and 128 MiB.
`snapshot_ns` is normally zero for the raw stage because snapshots belong to
a lifecycle walk.
`lstrip` belongs to the text read's `TextOptions`, changes the bytes a line
retains, and is neither a codec option nor enabled by this stage.

The default absence values are empty text, `null`, `<null>`, `none`, `n/a`,
and `[n/a]`, after trimming and case folding. `null_values` replaces that set.

## Read, parse, narrow, write

`fix_message_field(codec)` is the native 128-column row used directly by the
parse door and `fix.raw`. `msgthreadid`, `loglevel` and `body` exist only in
`logs.messages`, and `crosscode` and `seqnum` stand on both shapes meaning
the row they sit on -- here the message's chain identifier and its step in
the chain -- so no carried or unstored schema is constructed. The field is
declared from the dictionary alone rather than from the first batch, so an
empty window creates the same table a full one does. The reviewed **FixMsg**
contract is [`schemas/rekep/fixmsg.json`](../contracts/index.md).

The dataset is keyed by `curruuid`, partitioned by hour of `currunix`, and
sorted by `currunix, seqnum, curruuid`. The write replaces matching
identifiers inside affected partitions and adds columns a newer dictionary
declares (`merge_schema=True`).

## A row is an event, not a line

The line identity becomes provenance in `srcuuids`. The event key is
`curruuid`, so repeated captures of the same event meet while
distinct messages from one line remain distinct.

`crosscode` is the first non-empty business identifier in this order:
`OrderID`, `ClOrdID`, `OrigClOrdID`, `QuoteID`, `QuoteReqID`, `MDReqID`.
Capture session and context do not replace it. When both capture values exist,
`identifiers["msgsesseventid"]` records
`<msgtype-len>:<msgtype>|<session-len>:<session>|<context-len>:<context>|<msgseqnum>`
without changing event content identity.

## Schema and precision

The row includes `msgcat` (`MsgCat`), `exprtime`, and the normalized code columns
`isincode`, `cficode`, `cusipcode`, `sedolcode`, `bloombergcode`, `figicode`,
and `miccode`. Their native types include `FIGICode`. Code vocabularies are
stored once in the registry and referenced through `FIX:codeset`.

`fixentries` contains residual protocol data only. Lifted scalars and complete
groups are omitted once the output columns prove they represent them. Unknown,
ambiguous, failed-fit, and partial group content remains residual, and
`nofixentries` counts it. A row reconstructs canonical message semantics, not
the original pair order or framing bytes.

## Registry override

A venue dictionary is a codec over it:
`codec=fix_codec(fix_registry("file:///srv/fix"))`. The same codec must
declare the table field and parse its rows; a dictionary is never inferred
from a batch. Hand the same codec to
[`parse_fix_refined`](parse-fix-refined.md#registry-override), which reads each
row back with the dictionary that wrote it.

## Row behavior

- `seqnum`, `prevuuid`, and `parentuuids` remain empty because the raw stage
  has not walked a chain.
- `srcuuids` preserves capture provenance by joining to the line's
  `logs.messages.curruuid`; the FIX row carries no column of the line.
- `recdunix` and `refrecdunix` are both the line's own clock. `execunix` is
  what the bridge states, where it does.
- A message that stated no `SendingTime` sits at the codec's epoch pin until
  the walk dates it by its `TransactTime`: 33 of the capture's 49 rows.
- Unknown names remain residual tag-zero entries.
- A replay of one window overwrites the same partition-scoped `curruuid` rows.
