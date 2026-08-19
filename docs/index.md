# yggfin / rekep

**rekep** parses trading application logs into [Apache Arrow](https://arrow.apache.org/)
and treats every data product as a single Python class: its schema, its files,
its DDL and its lineage all derive from one dataclass declaration.

```python
from rekep.logs import LogFile

with LogFile.from_url("s3://bucket/app-2026-08-14.txt.gz") as log:
    table = log.into_arrow_table()
```

## One declaration, every projection

A record is declared once, as a dataclass with one docstring per field:

```python
from rekep import Record, record

@record
class Log(Record):
    """One parsed line of a trading log."""

    url: str
    """Path of the log the line came from, as its filesystem addresses it."""

    unix: int
    """Timestamp as whole nanoseconds since the epoch, naive UTC."""
```

Everything else is derived, never hand-written beside it:

| You call | You get |
| --- | --- |
| `Log.into_arrow_schema()` | Arrow schema, descriptions as field metadata |
| `Log.into_iceberg_schema()` | Iceberg schema, fresh field ids, docs carried over |
| `Log.into_iceberg_ddl()` | `CREATE TABLE ... USING iceberg`, comments included |
| `Log.into_yaml()` | the declaration itself, reviewable and diffable |
| `row.into_json()` | one instance's values |
| `Record.from_arrow_schema(schema)` | a record class built back from an external schema |

## Where things live

- **`rekep.records`** — the machinery: `@record`, `Record`, the Arrow, Iceberg
  and DDL builders.
- **`rekep.models`** — the concrete records this package reads and writes.
- **`rekep.logs`** — `LogFile`: lazy, streaming, compression-transparent log
  access over any `pyarrow.fs` filesystem.
- **`rekep.flows`** — `Flow`: a data movement declared as a record, with one
  abstract `arrow_transform`.
- **`rekep.airflow`** — DAG authoring with lineage derived from records, and
  DAGs built from side files.
