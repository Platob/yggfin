# Contracts

Both files are native `Field` JSON:

- `message.json` is the generated contract for `logs.messages`.
- `fix-message.json` is a reproducible snapshot of the 108-column `FixMsg`
  schema this checkout's dictionary and ULBridge carrier produce.

```bash
rekep fields dump --pyclass rekep.text.message:Message \
  --target schemas/rekep/message.json
rekep fields load --target schemas/rekep/message.json
```

Each checked JSON file is `Field.into_json(indent=2)` followed by one
newline and loads with `Field.from_json(document)`. The schema and `Message`
declaration change together. Native protocol declarations, when present, are
validated metadata in the same document; source lists remain canonical compact
JSON strings inside that metadata.

The FIX snapshot is for schema review and Iceberg simulations. It is not a
second registry: production `parse_fix` always asks its selected runtime
registry for the schema before reading a batch. The dictionary it is generated
from is `config/fix`, and the regeneration procedure for `fix-message.json`
is on the [portable schema](../docs/contracts/index.md) page. Regenerate it
whenever that dictionary changes.
