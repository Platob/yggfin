# Design

## One field model

```python
from rekep import Field, Message
from yggdryl import Field as NativeField

assert Field is NativeField
schema = Message.field().into_arrow_schema()
```

The native `Field` is the schema authority. rekep adds no wrapper class,
protocol metadata model, dataclass compiler, or alternate document codec.

## Stream by default

`IOBase.read_arrow_reader()` and Iceberg reads expose `RecordBatchReader`.
Row batching bounds retained rows, and write commit limits bound transaction
size. One exact body has no byte bound until the core exposes error-on-overflow.
Helpers returning an Arrow table are explicit choices for data known to fit in
memory.

## Keep ownership narrow

- The native core owns filesystems, byte streams, compression, text media, FIX, and fields.
- Arrow owns columnar kernels and schema casts.
- PyIceberg owns tables, snapshots, planning, and commits.
- rekep owns the `Message` contract and the small seam between those systems.

## Refuse ambiguity

Missing required columns, nulls in non-null fields, invalid merge keys, and
unresolved resources fail at their boundary. The strict native apply policy
owns Arrow casts, declared derivations, nullability checks, and error paths.

## Keep orchestration outside the package

`tasks/parse_messages/` and `tasks/parse_fix/` contain the Marimo applications
and their JSON inputs. Package code contains reusable models and storage
behavior only.
