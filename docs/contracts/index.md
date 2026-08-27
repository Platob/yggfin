# Schema contracts

![One declaration produces Arrow, a portable contract, and an Iceberg
table](../assets/compatibility-tree.svg)

`schemas/rekep/` publishes every persisted pipeline shape:

| Contract | Version | Rows |
| --- | ---: | --- |
| `message.yaml` | 2 | Source records with a promoted message discriminator and residual arguments. |
| `fixmsg.yaml` | 2 | Parsed FIX records, including typed fields and lossless raw audit sidecars. |
| `instrument.yaml` | 2 | Versioned and hourly instrument state. |
| `book.yaml` | 2 | Book deltas, executions, and recovery state. |
| `order.yaml` | 2 | Flattened auditable order events. |
| `execution.yaml` | 2 | Flattened auditable executions. |

```python
from rekep import Field

message = Field.from_yaml("schemas/rekep/message.yaml")
reader = message.cast_arrow(reader)
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

### Version 2: codes are mnemonics

Version 2 recodes every stable code. A code is now the ASCII mnemonic it
reads as, packed right-justified into its column, so `state` holds `FILLED`
rather than `410`; the columns whose codes outgrew four bytes -- `etype`,
`state` and both `kind` flavours -- widened from `int` to `long`.

Reading is covered: `from_stored` resolves an id from any generation this
package has written, so a warm registry cache, a stored `Field`'s metadata
and a row decoded through a declaration all keep naming the same members.

Writing into a version 1 table is not, and cannot be: Iceberg will not
promote a `long` into an `int` column. A table written before version 2
needs its `etype`, `state` and `kind` columns widened -- an
`ALTER TABLE ... ALTER COLUMN ... TYPE bigint` through an engine that
speaks Iceberg schema evolution -- before a version 2 producer can append
to it. Existing rows keep their old ids and read back correctly once the
column is wide enough to hold them.

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
