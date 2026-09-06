# Field contract

Yggdryl `Field` is the public field type:

```python
from rekep import Field, Message
from yggdryl import Field as YggdrylField

assert Field is YggdrylField
assert Message.field().into_arrow_schema().names == [
    "sourceurl",
    "sourcerownum",
    "timestamp",
    "threadname",
    "plugin",
    "level",
    "body",
]
```

`@yggdryl.scalar` derives the cached field from the dataclass annotations.
`Annotated` options supply the two Iceberg primary-key markers. Native
`Field.into_arrow_schema`, `from_arrow_schema`, `into_json`, and `from_json`
own every conversion.

Rekep's `strict_cast_batch`, `strict_cast_table`, and `strict_cast_reader` are
thin boundary checks around native Yggdryl casts. They reject absent or null
required columns before Yggdryl's recursive Arrow cast reorders fields and
drops extras.
