# Why Apache Arrow

Arrow is the in-memory contract every stage meets at. Not a database, not a
table format, not a compute engine — a columnar shape, and standard ways to
hand it over.

![Apache Arrow connects Iceberg tables, DataFrames, compute engines, and SQL databases; zero-copy sharing requires compatible buffers.](../assets/arrow-hub.svg#only-dark)
![Apache Arrow connects Iceberg tables, DataFrames, compute engines, and SQL databases; zero-copy sharing requires compatible buffers.](../assets/arrow-hub-light.svg#only-light)

One declaration becomes every boundary:

```python
import polars
from rekep import Execution, Field

shape = Field.from_class(Execution)
arrow = shape.into_arrow_schema()

print(len(arrow), len(shape.into_iceberg_schema().fields))
print(polars.from_arrow(arrow.empty_table()).width)
```

```text
49 49
49
```

## What the boundary buys

Each stage stays a stream of typed record batches, so parsing, casts, storage
and downstream compute agree on one schema — without an Iceberg table or a
DataFrame becoming the application's data model.

The [format spec](https://arrow.apache.org/docs/format/Intro.html) is
language-independent: compatible libraries in one process share buffers
through the C Data Interface or PyCapsule without copying, and IPC and Flight
move batches between processes. Zero-copy is a property of compatible types,
buffers and ownership — not of every arrow drawn on a diagram.

| boundary | what meets Arrow there |
| --- | --- |
| tables | [Iceberg](https://iceberg.apache.org/spec/) via [PyIceberg](https://py.iceberg.apache.org/api/), which reads to Arrow and accepts Arrow for writes |
| files | Arrow's own [Parquet](https://arrow.apache.org/docs/python/parquet.html) reader and writer; Avro and ORC through adapters that encode, not share |
| DataFrames | [Polars](https://docs.pola.rs/user-guide/misc/arrow/), [pandas](https://pandas.pydata.org/docs/user_guide/pyarrow.html), [cuDF](https://docs.rapids.ai/api/cudf/stable/cudf/10min/) (host↔device) |
| engines | [DataFusion](https://datafusion.apache.org/) natively, [DuckDB](https://duckdb.org/docs/stable/guides/python/export_arrow.html) in and out, [Spark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html) for JVM↔Python transfer |
| SQL | [ADBC](https://arrow.apache.org/adbc/) drivers and [Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html); [Doris](https://doris.apache.org/docs/4.x/connection-integration/arrow-flight-sql/) labels its own support experimental, as the diagram says |

Engines do not share internals; each can *meet* the pipeline at an Arrow
boundary instead of demanding a bespoke row model.

## Why this project chooses it

- a declaration becomes an Arrow schema once;
- readers stay streaming and batch-oriented;
- Iceberg, Parquet, DataFrames and database drivers meet the same records;
- language and engine choices change without redesigning persisted identities.

Fewer conversions owned by application code, one portable contract, and no
requirement that every consumer adopt the same storage or compute engine.
