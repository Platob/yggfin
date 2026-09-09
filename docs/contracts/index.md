# Portable schemas

Two generated `rekep.Field` documents make the current table contracts
reviewable:

| snapshot | columns | runtime constructor |
| --- | ---: | --- |
| [`message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/message.json) | 12 | `Message.field()` |
| [`fix-message.json`](https://github.com/Platob/yggfin/blob/main/schemas/rekep/fix-message.json) | 111 | `fix_message_field()` |

## Verify a snapshot

```python
from pathlib import Path

from rekep import Field, Message

document = Path("schemas/rekep/message.json").read_text(encoding="utf-8")
field = Field.from_json(document)

assert field == Message.field()
assert document == f"{field.into_json(indent=2)}\n"
```

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

field = fix_message_field()
Path("schemas/rekep/fix-message.json").write_text(
    f"{field.into_json(indent=2)}\n",
    encoding="utf-8",
)
```

The 111 columns are 10 carried source columns, 80 selected specification
columns, 19 derived/runtime columns, `msgdirection`, and two arrival lists.
The runtime registry remains authoritative; a registry change must produce a
visible schema diff.
