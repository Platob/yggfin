# Contracts

`schemas/rekep/message.json` is the generated native Yggdryl `Field` document
for `logs.messages`.

```bash
rekep fields dump --pyclass rekep.text.message:Message \
  --target schemas/rekep/message.json
rekep fields load --target schemas/rekep/message.json
```

The checked JSON is `Message.field().into_json(indent=2)` followed by one
newline and loads with `Field.from_json(document)`. The schema and `Message`
declaration change together. Native protocol declarations, when present, are
validated metadata in the same document; source lists remain canonical compact
JSON strings inside that metadata. FIX and market contracts return only when
that layer is rebuilt directly on `yggdryl.fix`.
