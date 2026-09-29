# parse_fix_messages_raw

`parse_fix_messages_raw(storages, window, *, codec=None, commit_row_size=COMMIT_ROW_SIZE, source=LOG_MESSAGES, target=FIX_MESSAGES_RAW)`
parses the stored lines of one window into FIX messages and lands them in
`bronze.record_keeping.fix_messages`. It parses and nothing more: no chain is
walked.

```python
import tempfile
from pathlib import Path

import pyarrow.compute

from rekep import FixCodec, Storages
from rekep.fix import UNDATED
from rekep.pipeline import FIX_MESSAGES_RAW, Landed, parse_fix_messages_raw, parse_log_messages
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
codec = FixCodec.from_env(default_sending_time=UNDATED, threads=2, batch_row_size=4_096)
with storages:
    parse_log_messages("file:data/capture/ulbridge.log", storages, window)
    # 129 lines carry 69 frames, and every report of a fill splits off the
    # execution it reports: 125 messages. The key folds the 53 that restate a
    # message another hop already logged: 72 rows.
    landed = parse_fix_messages_raw(storages, window, codec=codec)
    assert landed == Landed(read=129, written=72, skipped=53)

    raw = storages.dataset(FIX_MESSAGES_RAW)
    try:
        table = raw.read_arrow_table(columns=("currunix", "seqnum", "prevuuid", "srcuuids"))
    finally:
        raw.close()
    # Nothing has walked: no row has a place in a chain yet.
    assert table.column("seqnum").null_count == table.column("prevuuid").null_count == 72
    # Each row names the one line it was parsed from, and each of the 32
    # executions split out of a report names that report beside it.
    lengths = pyarrow.compute.list_value_length(table.column("srcuuids")).to_pylist()
    assert (lengths.count(1), lengths.count(2)) == (40, 32)
```

Over the capture's whole day the same task reads 144 lines, answers 135
messages -- 79 frames and the 56 executions their reports split off -- and
lands 77 rows, 58 of them folded.

## Read

The task scans the source for the window on `currunix` -- the instant each
line was printed at -- and the lines at the epoch beside it, where a handle
with no clock leaves a line, in the table's sort order,
`rekep.fix.SORT_COLUMNS` (`currunix`, `seqnum`, `curruuid`): the order the
lines were printed in, whatever files hold them. The parse is handed
`rekep.fix.PARSE_COLUMNS`, the seven columns it consumes: `currunix`,
`curruuid`, `body`, `msgsessionid`, `msgctxid`, `msgseqnum` and
`msgpluginid` -- the line's clock, its identity, the body the frames are read
out of, and the four captures that fill a FIX field by name. The scan opens
those and `seqnum`, which only the order reads, and no other column.

## Parse

`rekep.fix.fix_parse_arrow_reader(codec, lines)` reads every frame each line
carries: a line of prose answers none, a line carrying two frames answers two,
a report of a fill answers, beside itself, the execution it reports -- a trade
report one per side it states -- and a quote stating both sides answers one
quote per side: each a message of its own naming its source in `srcuuids`, an
execution `FILLED` whatever state its report reached.
Its rows are the dictionary's fixed row, `rekep.fix.fix_message_field(codec)`:
133 columns declared from the registry alone, so an empty window creates the
same table a full one does. Parsing and local enrichment are independent per
message and run on `threads` workers, the available CPU count by default,
while the output keeps the input's order.

`codec` is the whole parse surface: `FixCodec.from_env(**pins)` over the
process registry, or `FixCodec(registry, **pins)` over another, where the
native codec validates every pin. Useful pins are `batch_row_size` and
`batch_byte_size` (32,768 rows and 128 MiB by default), `include_msgtypes`,
`exclude_msgtypes`, `threads`, `official_time_delay_ms` (1,000 by default),
`null_values` and `default_sending_time`. None is
`FixCodec.from_env(default_sending_time=UNDATED)`.

## A row is a message

| column | on a bronze row |
| --- | --- |
| `currunix` | the transaction clock standing within `official_time_delay_ms` of the `SendingTime` the message states, else that `SendingTime`; a message stating none measures its transaction clock against the line it was read off the same way, and takes the line's instant where none stands that near |
| `curruuid` | the message's identity, a UUIDv7 over its instant and its content; the table's key |
| `crosscode` | the identifier every message of one lifecycle shares: `OrderID`, else `ClOrdID`, `OrigClOrdID`, `QuoteID`, `QuoteReqID` or `MDReqID`, the first stated, or `ExecID=` its `ExecID` for an execution split out of a report; prefixed `BUY:`, `SELL:` and so on with the side the message states |
| `srcuuids` | the `curruuid` of the one `log_messages` line the message was parsed from, and for an execution split out of a report that report's `curruuid` beside it |
| `state` | the first of `OrdStatus`, `ExecType`, `ExecAckStatus`, `TrdRptStatus`, `QuoteStatus`, `AllocStatus`, `ConfirmStatus`, `AffirmStatus`, `MassActionResponse` or `MassCancelResponse` the message states, else what its message type asks for, else `UNKNOWN`, as an `int32` [code](../tables/states.md) |
| `msgsesseventid` | `MsgType`, the session instance, the context and `MsgSeqNum` joined by `:`, where all four are stated |
| `seqnum`, `prevuuid`, `prevunix` | empty: nothing has walked |
| `fixentries` | what no lifted column represents, keyed `tag:name` |

`msgthreadid`, `loglevel` and `body` stay on the line: `srcuuids` joins a
message back to it. [The table page](../tables/bronze/fix_messages.md) lists
every column and [its samples](../samples/bronze/fix_messages.md) follow one
execution through the parse.

## Write

A bridge logs a message again at every hop it passes. A copy that restates
another exactly answers the same identity, so the table is keyed on
`curruuid` alone, within the hour of `currunix`, and the copies the key folded
into one row are `skipped`. Of the copies one run reads, the row kept is the
first printed, because the lines are read in the order they were printed in.
A copy stating no `SendingTime` takes the transaction clock it states only
where its line stands within `official_time_delay_ms` of it, which a line
read in its bridge's zone does,
so it folds with the copies that state one; read in another zone, it takes
its line's instant, hours away from them, and folds with none of them. A copy
a hop changed -- an enrichment plugin added a field -- is a row of its own
here, and the walk merges it into the event it is, in
[silver](parse-fix-messages-refined.md). The write merges the messages into
the table on that key, at most `commit_row_size` rows a commit
([commits](index.md#commits)) -- a key the hour lacks is inserted, one it
holds with other values replaced -- and adds any column a newer dictionary
declares (`merge_schema=True`). A rerun finds every message held as it is:
it writes none and counts all of them `skipped`.
