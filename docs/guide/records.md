# Records

A record is a dataclass that *is* its data product: schema, serialisation,
Iceberg projection and DDL all derive from the one declaration.

## Declaring

```python
from typing import Annotated
import pyarrow
from rekep import Arrow, Record, record

@record
class Fill(Record):
    """One fill."""

    day: Annotated[str, Arrow(iceberg={"partition": "true"})]
    """Trading day."""

    qty: Annotated[int, Arrow(type=pyarrow.int32(), metadata={"unit": "lots"})]
    """Signed quantity."""

    note: str | None = None
    """Free-form remark."""
```

The rules, in order of importance:

- **Nullability is declared, not guessed** — `str` is `NOT NULL`, `str | None`
  is nullable, and that carries through Arrow, Iceberg and DDL.
- **Documentation lives under the field** — the string literal beneath each
  field becomes its Arrow `description`, its Iceberg `doc`, its SQL `COMMENT`.
  One line: it renders as a column comment everywhere.
- **Overrides ride on the annotation** — a narrower type, a unit, protocol
  properties (`Arrow(iceberg={...})` lands as `iceberg:*` metadata keys).
- **`__`-prefixed annotations are working state, not fields.**

## Projections

```python
Fill.into_arrow_schema()     # pyarrow.Schema, cached per class
Fill.into_iceberg_schema()   # pyiceberg Schema, fresh ids, docs carried
Fill.into_iceberg_ddl()      # CREATE TABLE fill (...) USING iceberg
Fill.into_yaml()             # the declaration, as reviewable YAML
```

Calling a serialiser **on the class** dumps the declaration; **on an
instance**, the values:

```python
Fill.into_yaml("fill.schema.yaml")            # the contract
Fill(day="2026-08-14", qty=5).into_json()     # one row
```

## Keys and partitions

`Arrow(key=True)` and `Arrow(partition=...)` are declared once on the field
and read back from the Arrow schema, which is the one authority every other
projection derives from:

```python
Fill.primary_keys()      # ["day", "order_id"] -- Iceberg identifier fields, Doris key columns
Fill.partition_keys()    # {"day": "day", "account": "bucket[16]", "venue": "identity"}
```

The same two lists are what a DDL `PRIMARY KEY` clause, an Iceberg
`PartitionSpec`, a Doris `DISTRIBUTED BY`, a `merge_by=True` upsert and a
hive-partitioned file write all resolve to — none of them re-walks the
declaration.

## Casting onto a record's schema

A record is also a *target* shape, for data that arrives nearly right:

```python
Fill.cast_arrow_batch(batch)              # cast, fill, drop, reorder
Fill.cast_arrow_reader(batches)           # the same, one batch at a time
Fill.cast_arrow_batch(batch, safe=True)   # Arrow's checking back on
```

Columns are cast to the declared types, missing **nullable** ones are filled
with nulls, extras are dropped and the order is fixed. It takes a plain
iterator of batches too, so `Job.arrow_transform`'s output becomes a reader
of the record's shape in one step — which is what
[`Dataset.write_arrow_reader`](datasets.md#reshaping-onto-the-records-schema)
does on every write.

Unsafe by default, deliberately: this is `pyarrow.compute.cast`'s unsafe
mode, the one that lets a value narrow or a timestamp lose precision instead
of raising. A cast *to a target schema* is a declaration that the target's
types are the authority, so the truncation is the intent.

A missing **non-nullable** column is refused by name instead of filled —
filling a NOT NULL column with nulls builds a batch that only fails later,
at the write, where the cause is much harder to see. `records.cast_batch`/
`records.cast_reader` are the same thing against any schema, for targets that
are not records.

## Files

Instances round-trip through YAML, TOML and JSON, symmetric with the dumps:

```python
fill = Fill.from_yaml("fill.yaml")
fill.into_toml("fill.toml")
payload = fill.into_json()        # no destination -> bytes back
```

`None` fields are omitted (TOML has no null; the dataclass default applies on
load), unknown keys are ignored, and nested records come back as records.

## From an external schema

The projection also runs in reverse — a schema from a parquet footer or an
Iceberg table becomes a record class, losslessly:

```python
Order = Record.from_arrow_schema(schema, name="Order")
Order.into_arrow_schema().equals(schema, check_metadata=True)  # True
```
