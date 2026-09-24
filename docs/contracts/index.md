# Portable contracts

Checked-in contracts make the deployed Arrow and Iceberg shape reviewable
without creating a second schema owner.

| snapshot | columns | runtime constructor |
| --- | ---: | --- |
| [`message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.json) | 12 | `Message.into_field()` |
| [`fixmsg.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fixmsg.json) | 128 | `fix_message_field()` |
| [`book.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/book.json) | 53 | `book_field()` |
| [`marketevent.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/marketevent.json) | 50 | `market_event_field()` |

The second document is named **FixMsg** and declares both `fix.raw` and
`fix.refined`: one schema, one `curruuid` key, one hourly `currunix` partition,
and one `currunix, seqnum, curruuid` sort order.

## Market shapes

`rekep.market.book_field()` derives Book from the native empty book reader's
schema. Its required `bid` and `ask` structs retain `live` depth and `deltas`;
its required `executions` list contains already decomposed execution events.
`market_event_field()` derives the shared event shape from that execution
child. Order and quote deltas project onto the same shape after selection by
native `operationkind`; FIX `marketoperationid` is not the child-kind selector.

All market fields declare native `curruuid` as key, hour(`currunix`) as
partition, and `currunix, seqnum, curruuid` as sort order. The storage boundary
recursively narrows nanosecond timestamps to microseconds, UUIDs to fixed
bytes and semantic extensions to storage types. Unsigned codes are signed
views of the same bits. Exact decimal prices and quantities remain decimals.
The storage row therefore does not promise a nanosecond-exact native round trip.

## FIX row composition

The registry constructs one 128-column native row used by parse, storage, and
lifecycle. `msgthreadid`, `loglevel` and `body` remain solely in
`logs.messages`; a FIX row names the lines it was read from through
`srcuuids`. `crosscode` and `seqnum` stand on both rows and mean the row they
sit on: the object a line was read from and its row number there, the chain a
message belongs to and its step in it here. The bridge captures beside them --
`msgsessionid`, `msgctxid`, `msgseqnum` and `msgpluginid` -- are crate fields
of this row in their own right, which a stored line fills, so a stored row
goes on through the codec without one spelling being translated into another.
The native row's 32 crate fields include
`msgcat` (`MsgCat`), `exprtime`, and the lifted code columns `isincode`,
`cficode`, `cusipcode`, `sedolcode`, `bloombergcode`, `figicode`, and
`miccode`.

Code vocabularies live once in the registry and fields refer to them through
`FIX:codeset`. `fixentries` holds only residual pairs and groups that were not
fully represented in lifted columns; `nofixentries` counts that residual.
Reading a fixed row reconstructs canonical message semantics, including
groups and unknown fields, without claiming original wire order.

## Verify a snapshot

A snapshot is read back with `iceberg_contract_field`, under the name its file
spells -- a contract names no struct, because the catalog owns a table's name
-- and must build, declare its table, and match what the runtime constructor
records today:

```python
from pathlib import Path

from rekep import Message
from rekep.fix import fix_message_field
from rekep.iceberg import iceberg_contract, iceberg_contract_field, partition_keys, primary_keys
from rekep.market import book_field, market_event_field

CONTRACTS = {
    "message": (Message.into_field, 12),
    "fixmsg": (fix_message_field, 128),
    "book": (book_field, 53),
    "marketevent": (market_event_field, 50),
}
for stem, (factory, columns) in CONTRACTS.items():
    document = Path(f"schemas/rekep/{stem}.json").read_text(encoding="utf-8")
    field = iceberg_contract_field(document, stem)
    assert len(field.into_arrow_schema().names) == columns
    assert primary_keys(field) == ["curruuid"]
    assert partition_keys(field) == {"currunix": "hour"}
    assert document == f"{iceberg_contract(factory())}\n", f"{stem}.json has drifted"
```

## Regenerate

From the repository root, after a change to a runtime constructor:

```python
from pathlib import Path

from rekep import Message
from rekep.fix import fix_message_field
from rekep.iceberg import iceberg_contract
from rekep.market import book_field, market_event_field

FACTORIES = {
    "message": Message.into_field,
    "fixmsg": fix_message_field,
    "book": book_field,
    "marketevent": market_event_field,
}
for stem, factory in FACTORIES.items():
    Path(f"schemas/rekep/{stem}.json").write_text(
        f"{iceberg_contract(factory())}\n", encoding="utf-8", newline="\n"
    )
```

The runtime constructors answer their fields and `iceberg_contract` records
the Iceberg contract. The files are reviewed generated output, not alternate
implementations; `python/tests/test_schemas.py` fails on any drift.
