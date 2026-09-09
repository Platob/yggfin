# Table contract snapshots

Both checked files are Iceberg table contracts, serialized by PyIceberg:

| file | runtime owner | product |
| --- | --- | --- |
| `rekep/message.json` | `rekep.Message.field()` | `logs.messages` |
| `rekep/fix-message.json` | `rekep.fix.fix_message_field()` | `fix.messages` |

They are review artifacts, not alternate implementations. Runtime fields
remain authoritative and tests require byte-for-byte agreement.
`fix-message.json` is therefore reviewed as generated output, never edited as
an independent schema definition.

## What a contract holds

Three keys, each named for the PyIceberg model whose own serialization it
holds:

| key | PyIceberg model | what it decides |
| --- | --- | --- |
| `schema` | `pyiceberg.schema.Schema` | column ids, names, types, nullability, docs, identifier fields |
| `partition-spec` | `pyiceberg.partitioning.PartitionSpec` | which columns lay the table out, under which transform |
| `sort-order` | `pyiceberg.table.sorting.SortOrder` | where a row sits inside its file; order 0 is unsorted |

A table has more than this — a location, a uuid, snapshots — but none of those
is a property of the shape, so none of them is here.

Column ids are part of the review: inserting a column mid-shape renumbers
every id after it, and the diff says so.

## What a contract does not hold

An Iceberg schema carries no Arrow field metadata, so these facts are read
from the runtime declaration instead:

| fact | where it is asserted |
| --- | --- |
| a digest's algorithm and sources | `Message.field()`, `python/tests/test_message.py` |
| a derived column's source columns | `Message.field()`, `python/tests/test_message.py` |
| a FIX tag, alias or display name | the registry under `python/src/rekep/_data/fix`, and the `tag` column of the [FIX product page](../docs/products/fix-message.md) |
| the struct's own description and `python.*` declaration keys | `Message.field()`, and the class docstring behind it |
| the struct's own name | the table above, and `rekep.deploy.TABLES` |

`python/tests/test_schemas.py` pins each loss as an assertion, so the cost of
the format is code rather than prose.

## Regenerate the raw contract

```bash
uv run --project python rekep fields dump \
  --pyclass rekep.text.message:Message \
  --target schemas/rekep/message.json
```

## Regenerate the registry-dependent FIX contract

```python
from pathlib import Path

from rekep.fix import fix_message_field
from rekep.iceberg import iceberg_contract

target = Path("schemas/rekep/fix-message.json")
target.write_text(f"{iceberg_contract(fix_message_field())}\n", encoding="utf-8")
```

## Validate either document

```bash
uv run --project python rekep fields load --target schemas/rekep/message.json
uv run --project python rekep fields load --target schemas/rekep/fix-message.json
```

`load` reads a contract back through `rekep.iceberg.iceberg_contract_field`
and names the shape after the file, since a contract names no struct.

The FIX snapshot uses the registry bundled at
`python/src/rekep/_data/fix`, includes the bridge vocabulary, carries the raw
`Message` schema, and narrows all nested and top-level nanosecond timestamps to
the microsecond precision Iceberg v2 stores.

Dataset documents are unaffected: `IcebergDataset` embeds a `Field` mapping
because `derived_columns()` reads the derived sources off it to prune a merge,
and a table contract cannot state those.
