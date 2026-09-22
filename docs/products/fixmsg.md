# fix.raw and fix.refined

`fix.raw` and `fix.refined` store one **FixMsg** contract. `fix.raw` is the
settled parse of each event; `fix.refined` is that event after an ordered
lifecycle walk.

| property | contract |
| --- | --- |
| grain | one FIX event |
| stored columns | 128 |
| identifier field | `curruuid` |
| partition | `hour(currunix)` |
| sort order | `currunix`, `seqnum`, `curruuid` |
| reviewed snapshot | [`schemas/rekep/fixmsg.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fixmsg.json) |

The key is `curruuid`. Generic Iceberg writes remain generic:
append is blind, while an overwrite matches the configured identifier inside
the partition it is replacing.

## Row composition

The bundled registry produces 128 native columns. Parsing stored lines,
writing `fix.raw`, reconstructing messages, and walking `fix.refined` all use
this same shape. `msgthreadid`, `loglevel` and `body` are columns of
`logs.messages` alone; the header's other four -- `msgsessionid`,
`msgctxid`, `msgseqnum` and `msgpluginid` -- are columns of this shape too,
which is why a stored line fills them under the names it already holds.
`crosscode` and `seqnum` stand on both shapes and mean the row they sit on:
the object a line was read from and its row number there, the chain a
message belongs to and its step in the chain here.

The native row includes 32 crate fields and standard FIX fields chosen by the
registry. Its lifted vocabulary includes:

- `msgcat` (`MsgCat`), beside `msgtype` (`MsgType`);
- `exprtime`, the lifecycle expiry time;
- `isincode`, `cficode`, `cusipcode`, `sedolcode`, `bloombergcode`,
  `figicode`, and `miccode`.

These code columns use the native code datatypes, including `FIGICode`.
Versioned code vocabularies live centrally in the registry; a field references
one through `FIX:codeset`.

## Residual FIX entries

`fixentries` is the residual protocol tree and `nofixentries` counts it. A
scalar or complete group successfully represented in a lifted column is not
duplicated there. Unknown fields, failed conversions, ambiguous occurrences,
and groups that cannot be proved fully represented remain residual.

Reading a fixed row combines its lifted columns with those residual entries.
The result has the same canonical message semantics and preserves unknown and
unrepresentable content. It does not promise original pair spelling, arrival
order, body length, checksum, or byte-for-byte wire reproduction; encoding
builds a canonical FIX frame from the row.

The default absence spellings are empty text, `null`, `<null>`, `none`, `n/a`,
and `[n/a]`, after ASCII trimming and case folding. A codec's `null_values`
option replaces that set.

## Identity and capture context

`crosscode` is the first non-empty business identifier in this order:

1. `OrderID`
2. `ClOrdID`
3. `OrigClOrdID`
4. `QuoteID`
5. `QuoteReqID`
6. `MDReqID`

Capture session and context never replace that business identity. When both
are present, message type, session, context and sequence form one
byte-length-prefixed value, `identifiers["msgsesseventid"]` -- for example
`1:8|8:e7254b11|10:9f03166699|3088`; an incomplete set produces no synthetic key.
Changing capture context does not change the message content identity.

`fix.raw` has no lifecycle predecessor. `fix.refined` fills `seqnum`,
`prevuuid`, `parentuuids`, folded state, creation and expiry from the ordered
walk. Repeated deliveries are merged into one event, whose `srcuuids` names
every line it was logged on, without merging distinct events. On a `fix.raw`
row `recdunix` and `refrecdunix` are both the line's own clock; on a
`fix.refined` row `recdunix` is the earliest observation and `refrecdunix` the
clock of the reference the walk merged on, and the one row no line recorded,
the walk's expiry, states neither. `execunix` is what the bridge states, where
it does.

## Line provenance

`srcuuids` is the only reference to a stored line in FixMsg. Its values join
to `logs.messages.curruuid`, where the object the line was read from, its row
number, the instant the read settled over the line, the rest of its header and
its `body` remain available. The parse reads the stored identity back rather
than recomputing one, so the join names the line that landed. A stored FIX row
is self-contained for canonical message reconstruction.

## Inspect the product field

```python
from rekep.fix import fix_message_field

field = fix_message_field()
schema = field.into_arrow_schema()

assert len(schema) == 128
assert schema.field("msgtype").metadata[b"FIX:tag"] == b"35"
assert schema.field("curruuid").metadata[b"ICEBERG:primary_key"] == b"true"
assert schema.field("currunix").metadata[b"ICEBERG:partition_key"] == b"hour"
assert not {"msgthreadid", "loglevel", "body"} & set(schema.names)
assert {"crosscode", "seqnum", "srcuuids"} <= set(schema.names)
assert schema.names[-2:] == ["nofixentries", "fixentries"]
```
