# Table contract snapshots

Both checked files are Iceberg table contracts, serialized by PyIceberg:

| file | runtime owner | product | columns |
| --- | --- | --- | ---: |
| `rekep/message.json` | `rekep.Message.into_field()` | `logs.messages` | 12 |
| `rekep/fix-message.json` | `rekep.fix.fix_message_field()` | `fix.messages` | 130 |

They are review artifacts, not alternate implementations. Runtime fields
remain authoritative and tests require byte-for-byte agreement.
`fix-message.json` is therefore reviewed as generated output, never edited as
an independent schema definition.

## Where the FIX row comes from

`fix.messages` is the dictionary's own fixed row with the capture's own columns
in front of it, and yggfin defines neither:

```text
fix_schema_carrying(fix_carrier(Message.into_field()), fix_schema(registry, "fixmsg"))
```

123 columns come from the dictionary and 7 are the carrier's — `rownum`,
`timestamp`, `timepartition`, `threadId`, `level`, `bodyhash`, `body`. The
carrier's other five are named for the fields they fill, so `sourceurl`,
`msgsessionid`, `msgctxid`, `msgseqnum` and `pluginid` fold onto the row's own
columns instead of riding in front of it.

What yggfin adds is the three things a table is, and nothing else: the primary
key `curruuid`, the hour partition over `unix`, and the sort order
`unix, seqnum, curruuid`. `iceberg_fix_field` then narrows what a table
stores — see [the product page](../docs/products/fix-message.md).

The dictionary this was generated against is the one bundled under
`python/src/rekep/_data/fix`, taken from the core's own `config/fix` at
`f3a4bd0e27345c6e99226709d248c7b2174b011a`. Regenerating the contract against a
different dictionary is a failing `python/tests/test_schemas.py`, not a silent
schema evolution.

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
| a digest's algorithm and sources | `Message.into_field()`, `python/tests/test_message.py` |
| a derived column's source columns | `Message.into_field()`, `python/tests/test_message.py` |
| a FIX tag, alternate spelling or display name | the registry under `python/src/rekep/_data/fix`, and the `tag` column of the [FIX product page](../docs/products/fix-message.md) |
| the struct's own description and `python:*` declaration keys | `Message.into_field()`, and the class docstring behind it |
| the struct's own name | the table above, and `rekep.deploy.TABLES` |

`python/tests/test_schemas.py` pins each loss as an assertion, so the cost of
the format is code rather than prose.

## Regenerate either contract

```bash
uv run --project python rekep fields dump \
  --pyclass rekep.text.message:Message \
  --target schemas/rekep/message.json
uv run --project python rekep fields dump \
  --pyclass rekep.fix:fix_message_field \
  --target schemas/rekep/fix-message.json
```

`--pyclass` names a class, a field, or a function that builds one: no class
declares the FIX row, because it is the dictionary's row and not a Python
declaration.

## Validate either document

```bash
uv run --project python rekep fields load --target schemas/rekep/message.json
uv run --project python rekep fields load --target schemas/rekep/fix-message.json
```

`load` reads a contract back through `rekep.iceberg.iceberg_contract_field`
and names the shape after the file, since a contract names no struct.

Dataset documents are unaffected: `IcebergDataset` embeds a `Field` mapping
because `derived_columns()` reads the derived sources off it to prune a merge,
and a table contract cannot state those.
