# Order-book products

The book is two products: immutable normalized mutations and derived
checkpoints. `book.updates` is the audit log; `book.snapshots` is a query
acceleration product that can always be rebuilt.

## book.updates

One market-data message may contain many depth entries. It emits one row per
entry, addressed by source position and entry index.

### Planned schema

| column | Arrow type | null | contract |
| --- | --- | :---: | --- |
| `updatekey` | `fixed_size_binary[16]` | no | `(url,rownum,entryindex)` digest; primary key |
| `url` | `string` | no | source object |
| `rownum` | `int64` | no | source line |
| `entryindex` | `int32` | no | occurrence index in arrival order |
| `msghash` | `fixed_size_binary[16]` | no | parsed source identity |
| `eventtime` | `timestamp[us, UTC]` | no | exchange time, then market timestamp |
| `timepartition` | `timestamp[us, UTC]` | no | Iceberg hour transform |
| `sequence` | `int64` | yes | market-data sequence |
| `bookkey` | `fixed_size_binary[16]` | no | venue/session/instrument/book identity |
| `symbolticker` | `string` | yes | normalized instrument |
| `securityid` | `string` | yes | source instrument id |
| `miccode` | `fixed_size_binary[4]` | yes | venue MIC |
| `side` | `fixed_size_binary[4]` | no | bid or offer |
| `action` | `string` | no | snapshot/new/change/delete/clear |
| `level` | `int32` | yes | source depth position |
| `entryid` | `string` | yes | source quote/order identity |
| `price` | `double` | yes | level price; null for clear actions |
| `size` | `double` | yes | aggregate quantity |
| `ordercount` | `int32` | yes | orders represented at level |
| `quotecondition` | `string` | yes | trading/quote condition |

### Apply rules

- Sequence and source position define deterministic order; clock alone never
  orders updates.
- Snapshot resets book state before its entries apply.
- New/change/delete require the venue's declared keying mode: entry id,
  position, or price level. No global guess is acceptable.
- A sequence gap marks the book stale until a new snapshot; it does not apply
  speculative fills.
- Unknown side/action values remain rejected updates linked to source rows.

## book.snapshots

One row is one complete book image at a checkpoint. A snapshot is built only
from a valid reset plus contiguous updates.

| column | Arrow type | contract |
| --- | --- | --- |
| `bookkey` | `fixed_size_binary[16]` | book identity; key member |
| `snapshotat` | `timestamp[us, UTC]` | checkpoint time; key member and partition source |
| `throughsequence` | `int64` | last applied sequence |
| `throughupdatekey` | `fixed_size_binary[16]` | exact final mutation lineage |
| `symbolticker` | `string` | normalized instrument |
| `miccode` | `fixed_size_binary[4]` | venue |
| `bids` | `list<struct<price,size,ordercount>>` | best-to-worst bid levels |
| `offers` | `list<struct<price,size,ordercount>>` | best-to-worst offer levels |
| `bestbid` / `bestoffer` | `double` | query accelerators derived from lists |
| `spread` | `double` | `bestoffer - bestbid` |
| `depth` | `int32` | stored level bound |
| `stale` | `bool` | sequence gap or invalid transition observed |

Acceptance replays snapshots plus incrementals, duplicate updates, gaps,
out-of-order arrival, deletes, clears, crossed books, and multiple instruments;
rebuilding from `book.updates` must reproduce byte-equivalent level arrays.
