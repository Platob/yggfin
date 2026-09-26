# parse_fix_messages_refined

`parse_fix_messages_refined(storages, window, *, codec=None, source=FIX_MESSAGES_RAW, target=FIX_MESSAGES)`
reads the bronze FIX messages of the window and the hour before it, walks
them into the lifecycle chains they belong to, and lands the events the walk
places in the window in `silver.record_keeping.fix_messages`.

```python
import tempfile
from pathlib import Path

import pyarrow
import pyarrow.compute

from rekep import State, Storages
from rekep.pipeline import (
    FIX_MESSAGES,
    LOG_MESSAGES,
    Landed,
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
    # The walk merges the messages of one event and adds an expiry.
    assert parse_fix_messages_refined(storages, window) == Landed(read=41, written=14)

    refined = storages.dataset(FIX_MESSAGES)
    lines = storages.dataset(LOG_MESSAGES)
    try:
        walked = refined.read_arrow_table(row_filter="crosscode == '00079132557GLXC0'")
        logged = lines.read_arrow_table(columns=("curruuid", "seqnum"))
        expired = refined.read_arrow_table(row_filter=f"state == {int(State.EXPIRED)}")
    finally:
        refined.close()
        lines.close()

    # One order, filled in three steps plus a fill reported on its own.
    states = sorted(State(code).name for code in walked.column("state").to_pylist())
    assert states == ["FILLED", "FILLED", "PARTIALLY_FILLED", "PARTIALLY_FILLED"]
    assert sorted(step for step in walked.column("seqnum").to_pylist() if step) == [1, 2]
    heads = walked.filter(pyarrow.compute.is_null(walked.column("prevuuid")))
    assert heads.num_rows == 2

    # Provenance, not a recomputation: every line joins back by its identity.
    sources = pyarrow.table({"curruuid": pyarrow.compute.list_flatten(walked.column("srcuuids"))})
    assert sources.join(logged, keys="curruuid", join_type="inner").num_rows == 27

    # The one row no line recorded is the expiry the walk generated.
    assert expired.num_rows == 1
    assert expired.column("recdunix").null_count == 1
```

Over the capture's whole day the same task reads 48 bronze rows and lands 19
events.

## Read

For a window `[start, end)` the task reads `[start - HISTORY, end)` of its
source, where `rekep.pipeline.HISTORY` is one hour, through
`rekep.fix.fix_window_filter`, in `rekep.fix.SORT_COLUMNS` order --
`currunix, seqnum, curruuid` -- and every one of the three is pushed into the
scan. The filter also reads the rows at `UNDATED`, the pin of a message that
reached no clock at all, whose `TransactTime` falls in the window or that
state none. Iceberg plans the window's hours and the hour before; overlapping
files are merged at most 16 streams at a time, and no Python `read_all` union
is built before the walk.

## Walk

`rekep.fix.fix_lifecycle_arrow_reader(codec, rows)` reads each row back as
the message it was parsed as and walks the messages in their chains. The
walk is stateful and ordered: it dates a message by the `TransactTime` it
states where the parse could not, stably sorts by that instant, merges the
messages of one event, links each event to the one before it, and emits an
expiry at the deadline a chain stated -- never for a generation a replace or a
cancel retired. What it fills:

| column | on a silver row |
| --- | --- |
| `seqnum` | the event's step in its chain: how many came before it; empty on a chain's first event |
| `prevuuid`, `prevunix` | the event this one follows, and its instant |
| `srcuuids` | every line the event was logged on, the merged messages' lines together |
| `recdunix` | the earliest the event was recorded; empty on an expiry, which no line recorded |
| `creaunix`, `exprunix`, `state` | the creation, deadline and [state](../tables/states.md) the chain folded forward |
| `curruuid` | the event's identity, re-settled where the walk dated it: a silver key need not be its bronze twin's |

The walk collects and stably sorts its finite input, so its memory grows with
the window and the hour before it, not with a batch.
[The table page](../tables/silver/fix_messages.md) lists every column and
[its samples](../samples/silver/fix_messages.md) show two walked chains.

## Write

The walk's output is filtered to the events it placed in `[start, end)`: the
hour before warms the chains without replacing its own history with a
truncated replay, and an expiry the walk generates past `end` waits for its
own window. The write replaces the window's keys within their hour, adds any
column a newer dictionary declares, and a rerun lands the same rows again.
Silver is written from bronze and never in place, because a walked key is not
always the key of the bronze row it restates.

An event is written by the run whose window holds the instant the walk dated
it at, and read by the runs whose window, or the hour before it, holds its
bronze rows. A message stating no `SendingTime` sits in bronze at its line's
instant and is dated by the walk at its `TransactTime`; where the bridge's
clock runs ahead of the UTC its frames state, as the shipped capture's does
by two hours, only a window holding both lands it.
[DAGs](../dags/index.md#late-events) says how to schedule for that.

`codec` must be the one the bronze rows were parsed with,
`FixCodec.from_env(default_sending_time=UNDATED)` when None. Its
`snapshot_ns` pin, zero by default, emits a snapshot of every live event on
that nanosecond grid when positive.
