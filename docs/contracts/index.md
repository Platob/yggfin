# Schema contracts

![One declaration produces Arrow, a portable contract, and an Iceberg
table](../assets/compatibility-tree.svg)

`schemas/rekep/` publishes every persisted pipeline shape:

| Contract | Version | Rows |
| --- | ---: | --- |
| `message.yaml` | 3 | Source records with the standard header in columns of its own, a promoted message discriminator and residual arguments. |
| `fixmsg.yaml` | 2 | Parsed FIX records, including typed fields and lossless raw audit sidecars. |
| `instrument.yaml` | 2 | Versioned and hourly instrument state. |
| `book.yaml` | 2 | Book deltas, executions, and recovery state. |
| `order.yaml` | 2 | Flattened auditable order events. |
| `execution.yaml` | 2 | Flattened auditable executions. |

```python
from rekep import Field

message = Field.from_yaml("schemas/rekep/message.yaml")
message.into_arrow_schema()          # what a producer writes
message.cast_arrow(reader)           # what a consumer reads it back as
```

A contract preserves exact Arrow types, order, nullability, descriptions,
nested kinds, keys, partition transforms, field ids, and protocol metadata.
YAML and JSON use the same document model; the extension selects the codec.

## Metadata

- `iceberg: { ... }` stores keys, partitions, sort order, and assigned field ids.
- `fix: { ... }` stores canonical FIX name, tag, datatype, values, and version facts.
- `enum: { ... }` stores code key/value types and members.
- `metadata: { ... }` keeps protocol-neutral facts such as units and the schema namespace.

The document maps are restored to Arrow's collision-safe `iceberg:*`, `fix:*`,
and `enum:*` metadata keys when loaded.

Promoted FIX columns use the registry's spelling directly, for example
`OrigClOrdID` with `fix: { name: OrigClOrdID }`. Protocol-neutral and analytical
columns retain their own lower-case names.

## Evolution

Schema changes update declarations and generated contracts together while the
project is pre-release; there are no compatibility aliases in the contracts.

After compatibility is established, ordinary evolution is additive and
nullable. Dropping or retyping a field requires a new contract version.
Producers cast before writing; consumers load the same contract and may use
`merge_schema=True` to retain additive fields from a newer producer.

### Version 3: the standard header is columns

`Message` alone is at 3. Version 3 lifts `BeginString`, `BodyLength`,
`MsgType`, `MsgSeqNum`, `SenderCompID`, `TargetCompID` and `SendingTime` out of
`entries` into columns of their own. A table written under 2 must be rebuilt:
`parse_fix` refuses a source missing `MsgType`, `entries` or `protocol_code`
rather than reporting an empty successful run.

### Version 2: codes are mnemonics

Version 2 recodes every stable code. A code is now the ASCII mnemonic it
reads as, packed left-justified into its column, so `state` holds `41FILLED`
rather than `410`; `etype`, `state` and both `kind` flavours widened from
`int` to `long` to hold eight bytes.

There is no in-package migration. `from_int` reads version 2 values and
nothing else, so a store, a registry document or an Iceberg table written by
an earlier version must be rebuilt rather than read or appended to.

## Publishing

```bash
cd python
uv run rekep fields dump --pyclass rekep.text.message:Message \
  --target ../schemas/rekep/message.yaml
uv run rekep fields dump --pyclass rekep.text.fixmsg:FixMsg \
  --target ../schemas/rekep/fixmsg.yaml
uv run pytest tests/test_schemas.py
```

Schema tests parse, dump, and parse every file, then compare each package
contract with its owning declaration.
