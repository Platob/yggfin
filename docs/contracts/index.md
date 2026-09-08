# Portable schemas

Two generated Yggdryl `Field` documents make the table contracts reviewable:

| file | describes | authority |
| --- | --- | --- |
| [`schemas/rekep/message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.json) | fixed 12-column `Message` | `rekep.text.Message` |
| [`schemas/rekep/fix-message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fix-message.json) | 108-column checked-registry FIX result | runtime registry and codec |

The documents retain Arrow types, nullability, descriptions, FIX metadata,
digest declarations, primary keys, and Iceberg partition markers. rekep has no
parallel schema model.

```python
from pathlib import Path

from yggdryl import Field

document = Path("schemas/rekep/message.json").read_text(encoding="utf-8")
field = Field.from_json(document)

assert f"{field.into_json(indent=2)}\n" == document
assert field.into_arrow_schema().names[-2:] == ["bodyhash", "body"]
```

## Message generation

Change the Python declaration and regenerate its document together:

```bash
rekep fields dump --pyclass rekep.text.message:Message \
  --target schemas/rekep/message.json
rekep fields load --target schemas/rekep/message.json
```

## FixMessage generation

The FIX snapshot is derived, not hand-maintained:

1. create an empty reader with `Message.field()`;
2. load `config/fix`, whose registry already contains 16 Yggdryl fields;
3. add the ULBridge vocabulary with `with_ulbridge_fields()`;
4. call `parse_arrow_reader(..., branch="ulbridge")`;
5. recursively narrow nanosecond timestamps to Iceberg microseconds;
6. serialize `Field.from_arrow_schema`.

An empty reader is sufficient because the codec answers its output schema
before consuming a row. For the checked inputs the shape is:

| count | source |
| ---: | --- |
| 10 | carrier fields not claimed by folded fixed names |
| 80 | standard FIX projection |
| 16 | seeded Yggdryl fields, tags 65000–65015 |
| 2 | `nofixentries`, `nounmappedfixentries` |
| 108 | total |

The integration test also loads this JSON, constructs one schema-shaped batch,
writes it through the regular Iceberg path, and reads all 108 columns back.

## What stays outside the snapshot

The live registry remains authoritative. A registry can type a projected field
differently or add a nullable projection, and `parse_fix` derives that reader's
field at runtime. `merge_schema=True` may add those columns; it does not mutate
existing types, ids, keys, or partition rules.

`(url, rownum)` is the primary key in both snapshots. `timepartition` carries
the Iceberg hour transform. Every FIX timestamp is stored as
`timestamp[us, UTC]`, including clocks nested in repeating groups.
