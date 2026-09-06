# Portable schema

[`schemas/rekep/message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.json)
is the checked portable contract for `logs.messages`.
[`schemas/rekep/fix-message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fix-message.json)
is the full-registry `FixMsg` snapshot used for review and mock Iceberg writes.

```python
from pathlib import Path

from yggdryl import Field

document = Path("schemas/rekep/message.json").read_text(encoding="utf-8")
field = Field.from_json(document)
assert f"{field.into_json(indent=2)}\n" == document
print(field.into_arrow_schema())
```

The file is native Yggdryl `Field` JSON. It carries the Arrow types,
nullability, field metadata, and Iceberg key markers needed to reproduce the
table shape. Native partition and digest declarations, when present, use the
same validated metadata and round-trip through `Field.from_json` and
`Field.into_json`. The raw Message contract declares neither generated
protocol. Rekep has no parallel schema class or document implementation.

Regenerate it from the declaration:

```bash
rekep fields dump --pyclass rekep.text.message:Message \
  --target schemas/rekep/message.json
rekep fields load --target schemas/rekep/message.json
```

Schema changes update `Message` and this generated document together.

The FIX snapshot has 95 source-first columns at pinned Yggdryl `c9c84b24`.
Every timestamp in it is `timestamp[us, UTC]`; `(url, rownum)` remains the
primary key and `timepartition` retains its hourly Iceberg marker. It is a
derived fixture, not schema authority: `parse_fix` obtains the live schema from
the selected Yggdryl registry before streaming rows.

Generate it without parsing a message:

```python
from pathlib import Path

import pyarrow as pa
from yggdryl import Field, IOBase
from yggdryl.fix import FixRegistry, parse_arrow_reader

with IOBase.from_uri(Path("schemas/rekep/message.json").resolve().as_uri()) as source:
    message = Field.from_json(source.read_text())
registry = FixRegistry.from_handle(Path("../yggdryl/config/fix").resolve())
empty = pa.RecordBatchReader.from_batches(message.into_arrow_schema(), [])
reader = parse_arrow_reader(empty, registry, column="body")
fixed = Field.from_arrow_schema(reader.schema, name="FixMessage")
reader.close()
with IOBase.from_uri(Path("schemas/rekep/fix-message.json").resolve().as_uri()) as target:
    target.write_text(f"{fixed.into_json(indent=2)}\n")
```

The integration test loads that JSON with `Field.from_json`, constructs one
schema-shaped mock batch, streams it through the regular Iceberg writer, and
reads back all 95 columns. This isolates schema compatibility from parser
behavior.
