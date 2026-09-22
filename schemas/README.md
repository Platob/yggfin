# Table contract snapshots

The files in this directory are reviewed snapshots of the fields that declare
the published tables. Runtime constructors remain the source of truth.

| snapshot | runtime constructor | tables | stored columns |
| --- | --- | --- | ---: |
| `rekep/message.json` | `Message.into_field()` | `logs.messages` | 12 |
| `rekep/fixmsg.json` | `fix_message_field()` | `fix.raw`, `fix.refined` | 128 |

`fixmsg.json` is named **FixMsg**. It is generated from the live FIX registry,
then narrowed exactly as the Iceberg boundary narrows it. It is reviewed
output and is never edited as an alternate schema.

## Where the FIX row comes from

The native FIX row has 128 columns. Parse, storage, reconstruction, and
lifecycle all use that exact **FixMsg** shape. `msgthreadid`, `loglevel` and
`body` belong only to `logs.messages`, and FixMsg holds none of them.
`crosscode` and `seqnum` stand on both shapes and mean the row they sit on:
the object a line was read from and its row number there, a message's chain
and its step in it. The bridge's `msgsessionid`, `msgctxid`, `msgseqnum`, and
`msgpluginid` are not capture columns at all -- they are native FixMsg fields
a text line fills, so they stand on both shapes under one spelling and
nothing translates between them. A `fix.raw` row names its one line through
`srcuuids` and a `fix.refined` row every line its event was logged on, whose
values join to `logs.messages.curruuid`.

The native row contains 32 crate fields. These include `msgcat` (`MsgCat`),
the seven lifted identifier-code columns `isincode`, `cficode`, `cusipcode`,
`sedolcode`, `bloombergcode`, `figicode`, and `miccode`, and the lifecycle
clock `exprtime`. Registry code vocabularies are referenced by `FIX:codeset`
and owned centrally by the registry.

`fixentries` is the residual protocol tree. Values successfully represented
by lifted columns are omitted from it, and `nofixentries` counts what remains.
The fixed row can reconstruct the same canonical message semantics; it does
not promise the arrival byte order or a complete second copy of every lifted
pair.

## What a contract holds

Each snapshot is the structural `Field` document plus the table declarations
PyIceberg records: identifier fields, partition transforms and sort order. It
contains no data location, table UUID, snapshot history or catalog state.

These files are not alternate implementations. Runtime code builds the field,
`iceberg_contract` records its storage contract, and a review compares the
result with the checked-in snapshot.

## Regenerate either contract

```bash
uv run --project python rekep fields dump \
  --pyclass rekep.text:Message \
  --target schemas/rekep/message.json

uv run --project python rekep fields dump \
  --pyclass rekep.fix:fix_message_field \
  --target schemas/rekep/fixmsg.json
```

## Validate either document

```bash
uv run --project python rekep fields load --target schemas/rekep/message.json
uv run --project python rekep fields load --target schemas/rekep/fixmsg.json
```
