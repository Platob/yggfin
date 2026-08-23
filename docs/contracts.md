# Schema contracts

`schemas/rekep/` publishes only persisted pipeline products:

| Contract | Rows |
| --- | --- |
| `log.yaml` | Parsed source lines, including typed FIX fields. |
| `instrument.yaml` | Versioned and hourly instrument state. |
| `book.yaml` | Book deltas, executions, and recovery state. |
| `order.yaml` | Flattened auditable order events. |
| `execution.yaml` | Flattened auditable executions. |

```python
from rekep import Field

log = Field.from_yaml("schemas/rekep/log.yaml")
reader = log.cast_arrow(reader)
```

A contract preserves exact Arrow types, order, nullability, descriptions,
nested kinds, keys, partition transforms, field ids, and protocol metadata.
YAML and JSON use the same document model; the extension selects the codec.

## Metadata

- `iceberg:*` stores keys, partitions, sort order, and assigned field ids.
- `fix:*` stores canonical FIX name, tag, datatype, values, and version facts.
- `enum:*` stores code key/value types and members.
- `name`, `namespace`, `description`, and `version` identify the schema.

Code column names use snake case while metadata keeps protocol spelling, for
example `orig_cl_ord_id` with `fix:name: OrigClOrdID`.

## Evolution

Ordinary evolution is additive and nullable. Dropping or retyping a field is a
new contract version. Producers cast before writing; consumers load the same
contract and may use `merge_schema=True` to retain additive fields from a newer
producer.

The `linked_events` and Book-state refactor is an intentional breaking cutover:
it replaces stored names and types and makes collections and depths required.
Existing Iceberg tables must be explicitly migrated or recreated before using
these contracts; the core model does not keep parallel legacy columns. In
particular, an old lifecycle-only link has no event time from which to recover
`linked_events` losslessly.

## Publishing

```bash
cd python
uv run python -c "from rekep import Log; Log.into_field().into_yaml('../schemas/rekep/log.yaml')"
uv run pytest tests/test_schemas.py
```

Schema tests parse, dump, and parse every file, then compare each package
contract with its owning declaration.
