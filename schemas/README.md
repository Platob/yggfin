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
Every contract is version 1.

```python
from rekep import Field

shape = Field.from_yaml("schemas/rekep/message.yaml")
reader = shape.cast_arrow(reader)
```

Descriptions are short contract facts. Protocol identity uses top-level
`fix: { ... }`, `enum: { ... }`, and `iceberg: { ... }` maps; loading restores
their members as prefixed Arrow metadata. Repeated FIX data uses ordered lists
rather than maps.

Schema changes update declarations and generated contracts together. Add
nullable fields; use a new version to drop or retype a field. There are no
compatibility aliases in the contracts.

Regenerate a package contract from its declaration and run
`python/tests/test_schemas.py` before publishing it.
