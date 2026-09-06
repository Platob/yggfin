# Portable schema

[`schemas/rekep/message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.json)
is the checked portable contract for `logs.messages`.

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
