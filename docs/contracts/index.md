# Portable contracts

Checked-in contracts make the deployed Arrow and Iceberg shape reviewable
without creating a second schema owner.

| snapshot | columns | runtime constructor |
| --- | ---: | --- |
| [`message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.json) | 13 | `Message.into_field()` |
| [`fixmsg.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fixmsg.json) | 123 | `fix_message_field()` |

The second document is named **FixMsg** and declares both `fix.bronze` and
`fix.silver`: one schema, one `curruuid` key, one hourly `currunix` partition,
and one `currunix, seqnum, curruuid` sort order.

## FIX row composition

The registry constructs one 123-column native row used by parse, storage, and
lifecycle. Capture fields and `body` remain solely in `logs.messages`; a FIX
row names its raw source through `srcuuids`. The native row's 29
crate fields include `msgcat` (`MsgCat`), `exprtime`, and the lifted code columns
`isincode`, `cficode`, `cusipcode`, `sedolcode`, `bloombergcode`, `figicode`,
and `miccode`.

Code vocabularies live once in the registry and fields refer to them through
`FIX:codeset`. `fixentries` holds only residual pairs and groups that were not
fully represented in lifted columns; `nofixentries` counts that residual.
Reading a fixed row reconstructs canonical message semantics, including
groups and unknown fields, without claiming original wire order.

## Verify a snapshot

```bash
uv run --project python rekep fields load --target schemas/rekep/message.json
uv run --project python rekep fields load --target schemas/rekep/fixmsg.json
```

## Regenerate

```bash
uv run --project python rekep fields dump \
  --name message \
  --target schemas/rekep/message.json

uv run --project python rekep fields dump \
  --name fixmsg \
  --target schemas/rekep/fixmsg.json
```

The dump asks the runtime constructors for their fields and then records the
Iceberg contract. The files are reviewed generated output, not alternate
implementations.
