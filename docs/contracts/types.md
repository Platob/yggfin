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
    "timepartition",
    "threadname",
    "branch",
    "level",
    "body",
]
timestamp = schema.field("timestamp")
assert timestamp.type == pyarrow.timestamp("us", tz="UTC")
assert timestamp.nullable
partition = schema.field("timepartition")
assert partition.type == timestamp.type
assert partition.metadata[b"partition:sources"] == b'["timestamp"]'
assert partition.metadata[b"iceberg:partition_key"] == b"hour"
```

`@yggdryl.scalar` derives the cached field from the dataclass annotations.
`Annotated` options supply the two Iceberg primary-key markers. Native
`Field.into_arrow_schema`, `from_arrow_schema`, `into_json`, and `from_json`
own every conversion.

Iterate a `Field` for its immediate children, use `child.dtype.into_arrow()`
for one datatype, and use `partition_field_names` and `digest_field_names` for
the native protocol projections. Rekep adds no wrappers around those views;
its remaining field helpers declare Iceberg metadata or bridge it to PyIceberg.

The Message timestamp is an aware UTC instant. Arrow stores it at microsecond
resolution, and Iceberg maps it to `timestamptz`. Yggdryl derives the nullable
`timepartition` copy from `timestamp`; PyIceberg's `hour` transform makes that
column the UTC hourly partition without reducing its stored precision.

Every Rekep Arrow boundary delegates directly to native Yggdryl application with
`safe=False` and `nullability="strict"`. Message batches use
`Field.apply_arrow_batch`; Iceberg streams use `Field.apply_arrow_reader`, which
compiles once, then casts, computes `partition:sources` columns, and fills
`digest:role=holder` columns in that order. Missing and null required values,
protocol exemptions, nested array casts, final verification, and stream
ownership are Yggdryl contracts rather than Rekep implementations.

Iceberg layout and executable derivation are separate declarations. Yggfin
maps `partition_key()` to Yggdryl's `field:partition` layout marker and an
identity Iceberg spec. Non-identity transforms passed to `partition_key(...)`
remain under `iceberg:partition_key`. Compose
`derived_from("event", "year")` with a field annotation when Yggdryl should
compute that field from `event`; its transform is the native expression
protocol, independent of the Iceberg spec.

Remaining native apply gaps are scoped in the
[Yggdryl Arrow apply prompt](../prompts/yggdryl-arrow-apply.md).

The identity and transformed Iceberg markers are mutually exclusive;
declaration and Iceberg spec conversion reject a field carrying both.
