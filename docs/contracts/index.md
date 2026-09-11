# Portable contracts

Two generated Iceberg table contracts make the current table shapes
reviewable. PyIceberg serializes them, so the document reads exactly as the
created table records itself:

| snapshot | columns | runtime constructor |
| --- | ---: | --- |
| [`message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.json) | 12 | `Message.into_field()` |
| [`fix-message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fix-message.json) | 114 | `fix_message_field()` |

Each document has three keys — `schema`, `partition-spec` and `sort-order` —
holding a `pyiceberg.schema.Schema`, a `pyiceberg.partitioning.PartitionSpec`
and a `pyiceberg.table.sorting.SortOrder`.

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "id": 1,
        "name": "url",
        "type": "string",
        "required": true,
        "doc": "Canonical URI of the source text object."
      }
    ],
    "schema-id": 0,
    "identifier-field-ids": [1, 2]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {
        "source-id": 4,
        "field-id": 1000,
        "transform": "hour",
        "name": "timepartition_hour"
      }
    ]
  },
  "sort-order": {"order-id": 0, "fields": []}
}
```

An Iceberg schema carries no Arrow field metadata, so digest sources, derived
partition sources and FIX tags are not in the document. The runtime
declaration owns them; [`schemas/README.md`](https://github.com/Platob/yggfin/blob/main/schemas/README.md)
says where each one is asserted.

## Verify a snapshot

```python
from pathlib import Path

from rekep import Message
from rekep.iceberg import iceberg_contract, iceberg_contract_field, partition_keys

document = Path("schemas/rekep/message.json").read_text(encoding="utf-8")
field = iceberg_contract_field(document, "Message")

assert document == f"{iceberg_contract(Message.into_field())}\n"
assert document == f"{iceberg_contract(field)}\n"
assert partition_keys(field) == {"timepartition": "hour"}
```

The second assertion is what makes the file trustworthy: a contract read back
and republished is the same bytes, so anything the reader drops shows up as a
diff instead of as a silent loss.

## Regenerate

```bash
uv run --project python rekep fields dump \
  --pyclass rekep.text.message:Message \
  --target schemas/rekep/message.json
uv run --project python rekep fields load \
  --target schemas/rekep/message.json
```

The FIX snapshot is built from an empty raw-message reader. This asks the live
registry and codec for their complete output schema without consuming a row,
then applies the Iceberg timestamp precision rule.

```python
from pathlib import Path

from rekep.fix import fix_message_field
from rekep.iceberg import iceberg_contract

Path("schemas/rekep/fix-message.json").write_text(
    f"{iceberg_contract(fix_message_field())}\n",
    encoding="utf-8",
)
```

The 114 columns are 9 carried capture columns, 82 selected specification
columns, 20 derived/runtime columns, `msgdirection`, and two arrival lists.
The runtime registry remains authoritative; a registry change must produce a
visible schema diff.
