# Schema review snapshots

Both checked files are deterministic `rekep.Field` JSON:

| file | runtime owner | product |
| --- | --- | --- |
| `rekep/message.json` | `rekep.Message.field()` | `logs.messages` |
| `rekep/fix-message.json` | `rekep.fix.fix_message_field()` | `fix.messages` |

They are review artifacts, not alternate implementations. Runtime fields
remain authoritative and tests require byte-for-byte agreement.
`fix-message.json` is therefore reviewed as generated output, never edited as
an independent schema definition.

Regenerate the raw contract:

```bash
uv run --project python rekep fields dump \
  --pyclass rekep.text.message:Message \
  --target schemas/rekep/message.json
```

Regenerate the registry-dependent FIX contract:

```python
from pathlib import Path

from rekep.fix import fix_message_field

target = Path("schemas/rekep/fix-message.json")
target.write_text(f"{fix_message_field().into_json(indent=2)}\n", encoding="utf-8")
```

Validate either document:

```bash
uv run --project python rekep fields load --target schemas/rekep/message.json
uv run --project python rekep fields load --target schemas/rekep/fix-message.json
```

The FIX snapshot uses the registry bundled at
`python/src/rekep/_data/fix`, includes the bridge vocabulary, carries the raw
`Message` schema, and narrows all nested and top-level nanosecond timestamps to
the microsecond precision Iceberg v2 stores.
