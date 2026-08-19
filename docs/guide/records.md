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
