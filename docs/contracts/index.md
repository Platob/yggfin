# Schema contracts

![One declaration produces Arrow, a portable contract, and an Iceberg
table](../assets/compatibility-tree.svg#only-dark)
![One declaration produces Arrow, a portable contract, and an Iceberg
table](../assets/compatibility-tree-light.svg#only-light)

`schemas/rekep/` publishes every persisted pipeline shape:

| Contract | Version | Rows |
| --- | ---: | --- |
| `message.yaml` | 1 | Source records with the standard header in columns of its own, a promoted message discriminator and residual arguments. |
| `fixmsg.yaml` | 1 | Parsed FIX records, with reference facts in a nested `Instrument` component. |
| `instrument.yaml` | 1 | Immutable `InstrumentUpdate` events carrying that component. |
| `book.yaml` | 1 | Book deltas, executions, and recovery state. |
| `order.yaml` | 1 | Flattened auditable order events. |
| `execution.yaml` | 1 | Flattened auditable executions. |

```python
from rekep import Field

message = Field.from_yaml("schemas/rekep/message.yaml")
schema = message.into_arrow_schema()          # what a producer writes
print(len(schema), message.cast_arrow(schema.empty_table()).num_columns)
```

```text
58 58
```

A contract preserves exact Arrow types, order, nullability, descriptions,
nested kinds, keys, partition transforms, field ids, and protocol metadata.
YAML and JSON use the same document model; the extension selects the codec.
The reference nests — `FixMsg.instrument`, `InstrumentUpdate.instrument`, and
`Instrument.legs` — remain last in their owners so Iceberg's default column
bounds cover the flat leaves.

## Metadata

- `iceberg: { ... }` stores keys, partitions, sort order, and assigned field ids.
- `fix: { ... }` says which FIX field a column reads: its tag, its canonical
  name, its display and its FIX datatype. Not the rest of the record — the
  versions that declare it, the messages that carry it, the sources that
  answered and the values it enumerates stay in the registry, which is what
  keeps a contract a contract rather than a second copy of the dictionary.
- `enum: { ... }` stores code key/value types and members.
- `metadata: { ... }` keeps protocol-neutral facts such as units and the schema namespace.

The document maps are restored to Arrow's collision-safe `iceberg:*`, `fix:*`,
and `enum:*` metadata keys when loaded.

## Names

Every column in every contract is **folded**: lowercase, with everything that
is not a letter or a digit dropped. `OrigClOrdID` is `origclordid`,
`SourceURL` is `sourceurl`, `bid_levels` is `bidlevels`. One name serves as
the Arrow column, the Python attribute and the stored document's, so a grep
for a column reaches its declaration, its parser and its test.

The fold is also how a name is *matched*: a spelling is looked up by what it
folds to, which is what makes `MsgType`, `msgtype` and `MSGTYPE` one field
against the FIX registry rather than three.

What the fold throws away is kept, not lost. Every column carries
`fix: { display: ... }` — the name a reader is shown. A FIX column displays
the dictionary's own spelling (`OrigClOrdID`); every other column displays the
same shape, capitalised word by word and run together with acronyms preserved
(`sourceurl` → `SourceURL`, `altids` → `AltIDs`, `mic` → `MIC`). No display
carries a space, because no FIX field name does.

```python
from rekep import Field

order = Field.from_yaml("schemas/rekep/order.yaml")
for name in ("clordid", "price", "unixpartition"):
    print(f"{name:16} {order.field(name).fix.display}")
```

```text
clordid          ClOrdID
price            Price
unixpartition    UnixPartition
```

A column that reads a FIX field is named after that field, so a reader who
knows the dictionary knows the column: `ClOrdID <11>` is `clordid`,
`MinPriceIncrement <969>` is `minpriceincrement`. A `MarketEvent` uses the
flat summary slots `price` and `lastqty`: an Order holds limit price and
remaining live quantity, an Execution holds `LastPx <31>` and `LastQty <32>`,
and a Book holds midpoint and touch-size sum. A nested book `Level` keeps
compact `px` and `qty`; its nesting supplies the `MDEntryPx <270>` and
`MDEntrySize <271>` context. A nested protocol struct likewise drops the wire
prefix, so a leg's `LegCFICode <608>` is `cficode`, exactly as its
instrument's `CFICode <461>` is.

## Evolution

Schema changes update declarations and generated contracts together while the
project is pre-release; there are no compatibility aliases in the contracts.

After compatibility is established, ordinary evolution is additive and
nullable. Dropping or retyping a field requires a new contract version.
Producers cast before writing; consumers load the same contract and may use
`merge_schema=True` to retain additive fields from a newer producer.

**Every contract is at 1.** The numbers used to count the shapes each one had
been through, which was a history no reader could act on: nothing in the
package reads a stored version, nothing branches on one, and there is no
migration path -- a store, a registry document or an Iceberg table written by
an earlier release is rebuilt rather than read or appended to. So the counters
were reset to where they are useful, which is the first number a consumer of
*this* shape will see change.

The version is not part of a table's identity either: PyIceberg carries no
schema-level metadata, so it never survives the round trip and no write is
refused over it. What a reader actually depends on is the columns, and
`parse_fix` says so directly -- it refuses a source missing `msgtype`,
`entries` or `protocol` rather than reporting an empty successful run.

## Publishing

```bash
cd python
uv run rekep fields dump --pyclass rekep.text.message:Message \
  --target ../schemas/rekep/message.yaml
uv run rekep fields dump --pyclass rekep.text.fixmsg:FixMsg \
  --target ../schemas/rekep/fixmsg.yaml
uv run rekep fields dump --pyclass rekep.market.instrument:InstrumentUpdate \
  --target ../schemas/rekep/instrument.yaml
uv run pytest tests/test_schemas.py
```

Schema tests parse, dump, and parse every file, then compare each package
contract with its owning declaration.
