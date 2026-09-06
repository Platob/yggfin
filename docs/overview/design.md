# Design

## One field model

```python
from rekep import Field, Message
from yggdryl import Field as YggdrylField

assert Field is YggdrylField
schema = Message.field().into_arrow_schema()
```

Yggdryl `Field` is the schema authority. Rekep adds no wrapper class, protocol
metadata model, dataclass compiler, or alternate document codec.

## Stream by default

`IOBase.read_arrow_reader()` and Iceberg reads expose `RecordBatchReader`.
Row batching bounds retained rows, and write commit limits bound transaction
size. One exact body has no byte bound until Yggdryl exposes error-on-overflow.
Helpers returning an Arrow table are explicit choices for data known to fit in
memory.

## Keep ownership narrow

- Yggdryl owns filesystems, byte streams, compression, text media, and fields.
- Arrow owns columnar kernels and schema casts.
- PyIceberg owns tables, snapshots, planning, and commits.
- Rekep owns the `Message` contract and the small seam between those systems.

## Refuse ambiguity

Missing required columns, nulls in non-null fields, invalid merge keys, and
unresolved resources fail at their boundary. Rekep's strict preflight remains
small until Yggdryl exposes the same opt-in nullability policy natively.

## Keep orchestration outside the package

`tasks/parse_messages/` contains the Marimo application and its YAML input.
Package code contains reusable models and storage behavior only.
