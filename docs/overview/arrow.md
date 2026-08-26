# Why Apache Arrow

Apache Arrow is the shared in-memory contract in rekep. It is not a database,
table format, or compute engine. It gives those systems a common columnar
shape—schemas, arrays, and record batches—plus standard ways to exchange it.

![Apache Arrow connects Iceberg tables, DataFrames, compute engines, and SQL databases; zero-copy sharing requires compatible buffers.](../assets/arrow-hub.svg)

## The useful boundary

The [Arrow format specification](https://arrow.apache.org/docs/format/Intro.html)
is language-independent. Compatible libraries in one process can share buffers
through the C Data Interface or PyCapsule without copying; IPC and Flight move
Arrow batches across process or network boundaries. Zero-copy is a conditional
property of compatible types, buffers, and ownership, not a claim about every
arrow in the diagram.

That boundary is a good fit for rekep because each stage can remain a stream of
typed record batches. Parsing, recursive casts, storage, and downstream compute
agree on one schema without making an Iceberg table or a pandas DataFrame the
application's central data model.

## Files and table formats

[Apache Iceberg](https://iceberg.apache.org/spec/) is a table format: its
snapshots and metadata manage data files stored as Parquet, Avro, or ORC.
[PyIceberg](https://py.iceberg.apache.org/api/) reads to Arrow tables and batch
readers and accepts Arrow tables for writes. Arrow also has native
[Parquet readers and writers](https://arrow.apache.org/docs/python/parquet.html)
and implementation-specific adapters such as the
[Arrow Java Avro adapter](https://arrow.apache.org/cookbook/java/avro.html).
Those file boundaries encode or decode data; they are not presented as
zero-copy access to persistent bytes.

## DataFrames and compute

The same batches reach several styles of consumer:

- [Spark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html)
  uses Arrow for efficient JVM/Python transfers and pandas/PyArrow conversions.
- [pandas](https://pandas.pydata.org/docs/user_guide/pyarrow.html) supports
  Arrow-backed columns as well as conversion to and from Arrow.
- [Polars](https://docs.pola.rs/user-guide/misc/arrow/) follows Arrow's
  columnar layout and exchanges compatible data through the C Data Interface
  or PyCapsule.
- [DataFusion](https://datafusion.apache.org/) uses Arrow as its native
  in-memory format, while
  [DuckDB](https://duckdb.org/docs/stable/guides/python/export_arrow.html)
  can query Arrow objects and return Arrow tables or batch readers.
- [cuDF](https://docs.rapids.ai/api/cudf/stable/cudf/10min/) converts between
  GPU DataFrames and PyArrow tables, which crosses the host/device boundary.

This does not mean every engine has the same internals. It means each can meet
the pipeline at an Arrow boundary instead of requiring a bespoke row model.

## SQL databases

[ADBC](https://arrow.apache.org/adbc/) standardizes Arrow-native database
access, while [Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
defines a SQL protocol over Arrow Flight. The official
[ADBC driver list](https://arrow.apache.org/adbc/current/driver/index.html)
includes relational databases, warehouses, query engines, and Flight SQL.
Drivers may convert when the database is not Arrow-native.

Apache Doris also exposes
[Arrow Flight SQL](https://doris.apache.org/docs/4.x/connection-integration/arrow-flight-sql/),
but its current documentation labels the feature experimental and not
recommended for production. The diagram keeps that qualification visible.

## Why this project chooses it

Arrow gives rekep one authority at every scalable boundary:

- declarations become an Arrow schema once;
- readers stay streaming and batch-oriented;
- Iceberg, Parquet, DataFrames, and database adapters meet the same records;
- language and engine choices can change without redesigning persisted market
  identities or notebook interfaces.

That is the practical value of interoperability here: fewer conversions owned
by application code, one portable contract, and no requirement that every
consumer adopt the same storage or compute engine.
