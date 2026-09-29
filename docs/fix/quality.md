# Quality and audit

Quality is represented in rows rather than hidden in parser control flow.

| signal | question |
| --- | --- |
| a line's `currhashcode` | did the line change: its object, its header, its row number or its body? |
| a FIX row's `currhashcode` | did the settled event change? |
| `curruuid` | which event is this, whichever hop logged it? |
| `srcuuids` | which lines was it logged on? |
| `fixentries` | which pairs or groups the dictionary names did no lifted column represent, keyed `tag:name`? |
| `metadata` | which pairs had no registry definition, under the key the message spelled? |
| a typed null beside a residual pair | which value failed translation or conversion? |
| `state` of `UNKNOWN` (0) | which message stated no status and asked for none by its type? |

## Distinct digests

A line's `currhashcode` digests the object it was read from, the header's
captures except the clock, its row number and then its body, so two lines of
identical bytes answer two codes: the shipped capture's 144 lines answer 144
codes and 144 identities. It is content metadata; bronze `log_messages` is
keyed on the line's `curruuid`.

A FIX row's `currhashcode` is the event's content code -- XXH3-64 over its
facts, its text, its metadata, the stated header cells and the entry tree,
never the row's storage -- so a message read back out of a row is the same
message, and two lines carrying the same frame answer one code while their
bytes differ: that is what folds a message logged at three hops into one
event. `curruuid` is the identity the codec derives; store it unchanged.

`crosshashcode` digests `crosscode` alone, the first business identifier
stated: `OrderID`, `ClOrdID`, `OrigClOrdID`, `QuoteID`, `QuoteReqID`,
`MDReqID`. The bridge's delivery is `msgsesseventid` -- the message type, the
session instance, the context and `MsgSeqNum` -- which never alters the
content identity.

## Stated claims a row disagrees with

A message that states its own `BodyLength(9)` or `CheckSum(10)` states a
claim about bytes the reader can check. The frame is read and the stated pair
lands in its column. An unrepresentable or conflicting pair remains residual;
a successfully lifted value is not duplicated in `fixentries`.

```python
from rekep import FixCodec

codec = FixCodec.from_env()
message = next(iter(codec.parse_line(b"8=FIX.4.4|9=999|35=D|55=AAPL|10=000|")))

# The stated length is nine hundred and ninety-nine; the frame is not.
assert message.by_name("bodylength").as_py() == 999
assert message.by_name("symbol").as_py() == "AAPL"
assert message.by_tag(35).as_py() == "D"
```

## Registry coverage

A pair no dictionary explains is an entry of `tag` 0 inside the residual
`fixentries`, under the key the wire spelled. Joining a FIX table's
`srcuuids` to the lines says where each came from:

```python
import pyarrow
import pyarrow.compute

# `events` is silver `fix_messages` and `lines` bronze `log_messages`, read as tables.
residual = events.select(("srcuuids", "fixentries"))
parents = pyarrow.compute.list_parent_indices(residual.column("srcuuids"))
residual = pyarrow.table(
    {
        "fixentries": pyarrow.compute.take(residual.column("fixentries"), parents),
        "lineuuid": pyarrow.compute.list_flatten(residual.column("srcuuids")),
    }
)
located = lines.select(("curruuid", "crosscode", "seqnum")).rename_columns(
    ("lineuuid", "crosscode", "seqnum")
)
provenance = residual.join(located, keys="lineuuid", join_type="left outer")
needs_dictionary_work = [
    (
        row["crosscode"],
        row["seqnum"],
        [entry["name"] for entry in row["fixentries"] or [] if entry["tag"] == 0],
    )
    for row in provenance.to_pylist()
]
```

Investigate those keys, add definitions to a dictionary of your own, replay
`parse_fix_messages_raw` and then `parse_fix_messages_refined` with one codec
over it ([another dictionary](index.md#another-dictionary)), and review the
contract diff `tools/schemas_dump.py` writes before publishing it.

## Replay guarantees

`srcuuids` joins a FIX row back to the stored lines it was read from -- the
one line a bronze row was parsed from, every line its event was logged on
once the walk merged them -- each by the line's own `curruuid`. On a line,
`crosscode` and `seqnum` are its object and row number; on a FIX row the same
two columns are the chain and the step. A replay of a window answers the same
bronze rows and, walked, the same silver rows, finds every one held as it is,
writes none and leaves no duplicate, because every identity derives from what
the bytes state and never from the instant a task ran.
