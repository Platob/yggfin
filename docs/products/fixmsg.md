# fix.bronze and fix.silver

`fix.bronze` and `fix.silver` store one **FixMsg** contract. Bronze is the
settled parse of each event; silver is that event after an ordered lifecycle
walk.

| property | contract |
| --- | --- |
| grain | one FIX event |
| stored columns | 123 |
| identifier field | `curruuid` |
| partition | `hour(currunix)` |
| sort order | `currunix`, `seqnum`, `curruuid` |
| reviewed snapshot | [`schemas/rekep/fixmsg.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fixmsg.json) |

The key is `curruuid`. Generic Iceberg writes remain generic:
append is blind, while an overwrite matches the configured identifier inside
the partition it is replacing.

## Row composition

The bundled registry produces 123 native columns. Parsing stored capture rows,
writing bronze, reconstructing messages, and walking silver all use this same
shape. `sourceurl`, `rownum`, `msgthreadid`, `loglevel` and `body` are raw to
`logs.messages` and stay there; the header's other four -- `msgsessionid`,
`msgctxid`, `msgseqnum` and `msgpluginid` -- are columns of this shape too,
which is why a raw line fills them under the names it already stored.

The native row includes 29 crate fields and standard FIX fields chosen by the
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
are present, their `session:context` value is available as
`identifiers["msgsectxid"]`; either part alone produces no synthetic key.
Changing capture context does not change the message content identity.

`fix.bronze` has no lifecycle predecessor. `fix.silver` fills `seqnum`,
`prevuuid`, `parentuuids`, folded state, creation and expiry from the ordered
walk. Repeated deliveries are suppressed without suppressing distinct events.

## Raw-line provenance

`srcuuids` is the only raw-capture reference in FixMsg. Its values join to
`logs.messages.curruuid`, where `sourceurl`, `rownum`, the instant the read
settled over the line, the rest of its header and its whole `body` text remain
available. The parse reads the stored identity back rather than recomputing
one, so the join names the line that landed. A stored FIX row is
self-contained for canonical message reconstruction.

## Inspect the product field

```python
from rekep.fix import fix_message_field

field = fix_message_field()
schema = field.into_arrow_schema()

assert len(schema) == 123
assert schema.field("msgtype").metadata[b"FIX:tag"] == b"35"
assert schema.field("curruuid").metadata[b"ICEBERG:primary_key"] == b"true"
assert schema.field("currunix").metadata[b"ICEBERG:partition_key"] == b"hour"
assert not {
    "sourceurl", "rownum", "msgthreadid", "loglevel", "body",
} & set(schema.names)
assert schema.names[-2:] == ["nofixentries", "fixentries"]
```
