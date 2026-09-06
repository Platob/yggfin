# Portable schema

[`schemas/rekep/message.yaml`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.yaml)
is the checked portable contract for `logs.messages`.

```python
from pathlib import Path

from yggdryl import Field

document = Path("schemas/rekep/message.yaml").read_text(encoding="utf-8")
field = Field.from_yaml(document)
print(field.into_arrow_schema())
```

The file is native Yggdryl `Field` YAML. It carries the Arrow types,
nullability, field metadata, and Iceberg key markers needed to reproduce the
table shape. Rekep has no parallel schema class or document implementation.

Regenerate it from the declaration:

```bash
rekep fields dump --pyclass rekep.text.message:Message \
  --target schemas/rekep/message.yaml
rekep fields load --target schemas/rekep/message.yaml
```

Schema changes update `Message` and this generated document together.
