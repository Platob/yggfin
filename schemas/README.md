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
`message.yaml` is version 4 and `fixmsg.yaml` is version 6.

```python
from rekep import Field

shape = Field.from_yaml("schemas/rekep/message.yaml")
reader = shape.cast_arrow(reader)
```

Descriptions are short contract facts. Use metadata for protocol identity:
`fix:*`, `enum:*`, and `iceberg:*`. Repeated FIX data uses ordered lists rather
than maps.

Schema changes update declarations and generated contracts together while the
project is pre-release, with no legacy aliases to maintain. After compatibility
is established, add nullable fields and use a new version to drop or retype a
field.

Regenerate a package contract from its declaration and run
`python/tests/test_schemas.py` before publishing it.
