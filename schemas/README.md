# Contracts

`schemas/rekep/` contains the six persisted pipeline shapes:

```text
book.yaml
execution.yaml
fixmsg.yaml
instrument.yaml
message.yaml
order.yaml
```

Each file is one Arrow `Field` document with exact types, order, nullability,
keys, partitioning, descriptions, enum members, FIX metadata, and Iceberg ids.
`fixmsg.yaml` carries reference facts in its final `instrument` struct;
`instrument.yaml` is the flat `InstrumentUpdate` event envelope followed by
the same `Instrument` struct. Those two structs and `Instrument.legs` stay
last in their owners so Iceberg's default column bounds still cover the flat
leaves. `FixMsg.error` records row-local transcription degradation without
changing content identity. Every contract is version 1.

```python
from rekep import Field

shape = Field.from_yaml("schemas/rekep/message.yaml")
reader = shape.cast_arrow(reader)
```

Descriptions are short contract facts. Protocol identity uses top-level
`fix: { ... }`, `enum: { ... }`, and `iceberg: { ... }` maps; loading restores
their members as prefixed Arrow metadata. Repeated FIX data uses ordered lists
rather than maps.

Schema changes update declarations and generated contracts together. There
are no compatibility aliases in the contracts and no migration: a shape that
dropped or retyped a column is rebuilt, not read.

Regenerate a package contract from its declaration and run
`python/tests/test_schemas.py` before publishing it.
