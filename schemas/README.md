# Contracts

`schemas/rekep/message.yaml` is the generated native Yggdryl `Field` document
for `logs.messages`.

```bash
rekep fields dump --pyclass rekep.text.message:Message \
  --target schemas/rekep/message.yaml
rekep fields load --target schemas/rekep/message.yaml
```

The schema and `Message` declaration change together. FIX and market contracts
will return only when that layer is rebuilt directly on `yggdryl.fix`.
