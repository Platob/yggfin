# Why Apache Arrow

Arrow is the one in-memory boundary between Yggdryl text media and Iceberg.

![Apache Arrow connects Iceberg tables, DataFrames, compute engines, and SQL databases.](../assets/arrow-hub.svg)

```python
import polars

from rekep import Message
from rekep.iceberg import iceberg_schema

field = Message.field()
arrow = field.into_arrow_schema()

print(arrow)
print(iceberg_schema(field))
print(polars.from_arrow(arrow.empty_table()))
```

Yggdryl emits `RecordBatch` objects. Rekep checks required values, then the
native `Field` casts and applies any declared partition and digest columns;
PyIceberg accepts the resulting Arrow stream. No row model or project
filesystem sits between those boundaries.

The remaining strict-preflight behavior belongs upstream; its exact target is
the [strict cast prompt](../prompts/yggdryl-strict-arrow-cast.md).

Arrow does not make every hand-off zero-copy. Compatible in-process buffers can
be shared, while Iceberg and encoded files necessarily read or write storage.
The schema remains the same either way.

Useful boundaries include [PyIceberg](https://py.iceberg.apache.org/api/),
[Polars](https://docs.pola.rs/user-guide/misc/arrow/),
[DataFusion](https://datafusion.apache.org/), and
[DuckDB](https://duckdb.org/docs/stable/guides/python/export_arrow.html).
