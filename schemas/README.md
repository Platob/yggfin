# Contracts

`schemas/rekep/` contains the five persisted pipeline shapes:

```text
book.yaml
execution.yaml
instrument.yaml
log.yaml
order.yaml
```

Each file is one Arrow `Field` document with exact types, order, nullability,
keys, partitioning, descriptions, enum members, FIX metadata, and Iceberg ids.

```python
from rekep import Field

shape = Field.from_yaml("schemas/rekep/log.yaml")
reader = shape.cast_arrow(reader)
```

Descriptions are short contract facts. Use metadata for protocol identity:
`fix:*`, `enum:*`, and `iceberg:*`. Repeated FIX data uses ordered lists rather
than maps.

All five contracts remain at version 1 while the project is pre-release. Schema
changes update declarations and generated contracts together, with no legacy
contract versions to migrate. After compatibility is established, add nullable
fields and use a new version to drop or retype a field.

Regenerate a package contract from its declaration and run
`python/tests/test_schemas.py` before publishing it.
