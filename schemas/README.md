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

Evolution is normally additive. Add nullable fields; use a new version to drop
or retype a field. The `linked_events` and Book-state refactor is an intentional
breaking cutover: migrate or recreate existing tables before adopting the new
contracts. No legacy aliases remain in the normalized shape.

Regenerate a package contract from its declaration and run
`python/tests/test_schemas.py` before publishing it.
