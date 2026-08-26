# Schema contracts

![One declaration produces Arrow, a portable contract, and an Iceberg
table](assets/compatibility-tree.svg)

`schemas/rekep/` publishes every persisted pipeline shape:

| Contract | Version | Rows |
| --- | ---: | --- |
| `message.yaml` | 4 | Source records with a promoted message discriminator and residual arguments. |
| `fixmsg.yaml` | 5 | Parsed FIX records, including typed canonical FIX fields. |
| `instrument.yaml` | 1 | Versioned and hourly instrument state. |
| `book.yaml` | 1 | Book deltas, executions, and recovery state. |
| `order.yaml` | 1 | Flattened auditable order events. |
| `execution.yaml` | 1 | Flattened auditable executions. |

```python
from rekep import Field

message = Field.from_yaml("schemas/rekep/message.yaml")
reader = message.cast_arrow(reader)
```

A contract preserves exact Arrow types, order, nullability, descriptions,
nested kinds, keys, partition transforms, field ids, and protocol metadata.
YAML and JSON use the same document model; the extension selects the codec.

## Metadata

- `iceberg:*` stores keys, partitions, sort order, and assigned field ids.
- `fix:*` stores canonical FIX name, tag, datatype, values, and version facts.
- `enum:*` stores code key/value types and members.
- `name`, `namespace`, `description`, and `version` identify the schema.

Promoted FIX columns use the registry's spelling directly, for example
`OrigClOrdID` with `fix:name: OrigClOrdID`. Protocol-neutral and analytical
columns retain their own lower-case names.

## Evolution

Schema changes update declarations and generated contracts together while the
project is pre-release; there are no compatibility aliases in the contracts.

After compatibility is established, ordinary evolution is additive and
nullable. Dropping or retyping a field requires a new contract version.
Producers cast before writing; consumers load the same contract and may use
`merge_schema=True` to retain additive fields from a newer producer.

## Publishing

```bash
cd python
uv run python -c "from rekep import Message; Message.into_field().into_yaml('../schemas/rekep/message.yaml')"
uv run python -c "from rekep import FixMsg; FixMsg.into_field().into_yaml('../schemas/rekep/fixmsg.yaml')"
uv run pytest tests/test_schemas.py
```

Schema tests parse, dump, and parse every file, then compare each package
contract with its owning declaration.
