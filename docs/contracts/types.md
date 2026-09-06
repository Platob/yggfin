# Field contract

Yggdryl `Field` is the public field type:

```python
import pyarrow

from rekep import Field, Message
from yggdryl import Field as YggdrylField

assert Field is YggdrylField
schema = Message.field().into_arrow_schema()
assert schema.names == [
    "url",
    "rownum",
    "timestamp",
    "threadname",
    "plugin",
    "level",
    "body",
]
timestamp = schema.field("timestamp")
assert timestamp.type == pyarrow.timestamp("us", tz="UTC")
assert timestamp.nullable
```

`@yggdryl.scalar` derives the cached field from the dataclass annotations.
`Annotated` options supply the two Iceberg primary-key markers. Native
`Field.into_arrow_schema`, `from_arrow_schema`, `into_json`, and `from_json`
own every conversion.

The Message timestamp is an aware UTC instant. Arrow stores it at microsecond
resolution, and Iceberg maps the column to `timestamptz`.

Rekep's `strict_cast_batch`, `strict_cast_table`, and `strict_cast_reader` add
one missing/null preflight to native Yggdryl application. Yggdryl then casts,
computes columns declared by `partition:sources` and optional
`partition:transform`, and fills `digest:role=holder` columns in that order.
Only a field identified as a native materialization target may bypass the
initial missing/null preflight. Yggdryl currently materializes direct and
Struct-nested fields, so yggfin rejects executable protocol metadata below list
or map values instead of leaving a canonical default uncomputed. Every
`partition:` or `digest:` declaration in a supported Struct scope reaches
native application, which rejects malformed or incomplete combinations. The
final batch must satisfy every required field.

Iceberg layout and executable derivation are separate declarations. Yggfin
maps `partition_key()` to Yggdryl's `field:partition` layout marker and an
identity Iceberg spec. Non-identity transforms passed to `partition_key(...)`
remain under `iceberg:partition_key`. Compose
`derived_from("event", "year")` with a field annotation when Yggdryl should
compute that field from `event`; its transform is the native expression
protocol, independent of the Iceberg spec.

The identity and transformed Iceberg markers are mutually exclusive;
declaration and Iceberg spec conversion reject a field carrying both.
