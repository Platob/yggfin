# Table contract snapshots

The files in this directory are reviewed snapshots of the fields that declare
the published tables. Runtime constructors remain the source of truth.

| snapshot | runtime constructor | tables | stored columns |
| --- | --- | --- | ---: |
| `rekep/message.json` | `Message.into_field()` | `logs.messages` | 13 |
| `rekep/fixmsg.json` | `fix_message_field()` | `fix.bronze`, `fix.silver` | 130 |

`fixmsg.json` is named **FixMsg**. It is generated from the live FIX registry,
then narrowed exactly as the Iceberg boundary narrows it. It is reviewed
output and is never edited as an alternate schema.

## Where the FIX row comes from

The native FIX row has 123 columns. Parsing a stored `Message` carries eight
non-overlapping source columns in front of that row, producing 131 columns at
the parse boundary. `body` is consumed by the codec and is not stored in the
FIX products, leaving the 130 columns in `fixmsg.json`.

The native row contains 29 crate fields. These include `msgcat` (`MsgCat`), the seven
lifted identifier-code columns `isincode`, `cficode`, `cusipcode`, `sedolcode`,
`bloombergcode`, `figicode`, and `miccode`, and the lifecycle clock
`exprtime`. Registry code vocabularies are referenced by `FIX:codeset` and
owned centrally by the registry.

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
  --name message \
  --target schemas/rekep/message.json

uv run --project python rekep fields dump \
  --name fixmsg \
  --target schemas/rekep/fixmsg.json
```

## Validate either document

```bash
uv run --project python rekep fields load --target schemas/rekep/message.json
uv run --project python rekep fields load --target schemas/rekep/fixmsg.json
```
