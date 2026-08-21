# yggfin

`rekep`: trading logs as Arrow, a declaration that *is* a schema, and Iceberg
tables you can read and write without learning Iceberg first.

**[Documentation](https://platob.github.io/yggfin/)** —
[Types](https://platob.github.io/yggfin/types/) ·
[Logs](https://platob.github.io/yggfin/logs/) ·
[Iceberg](https://platob.github.io/yggfin/iceberg/)

```bash
pip install "rekep[all]"
```

```python
import datetime
from typing import Annotated

from rekep import Convertible, Field, Log, TextFile, field
from rekep.iceberg import IcebergDataset


@field
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, Field.primary_key()]
    """Instrument."""

    day: Annotated[datetime.date, Field.partition_key()]
    """Trading day."""

    size: int
    """Quantity."""


Quote.FIELD.into_arrow_schema()      # the declaration is the schema
Quote.FIELD.cast_arrow(batch)        # and the target shape for real data

logs = IcebergDataset(
    name="trading.logs",
    catalog="local",
    properties={"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "file:///wh"},
    struct=Log.FIELD,
)
with TextFile.from_path("app.txt.gz") as log:
    logs.append_arrow(log.read_arrow_reader(), merge_by=True, commit_row_size=1_000_000)

logs.read_arrow_table(row_filter="recorded_at_date = '2026-08-14'")
logs.optimize()                      # compact, expire, sweep
```

## What is in it

- **`fields`** — `Field` is one name, one Arrow type and metadata. `@field`
  turns a class into one; `StructField`, `ListField`, `MapField` and the list
  flavours make what is inside reachable as what it is; the casts take real data
  onto the shape, recursively, in Arrow kernels only.
- **`logs`** — `TextFile` parses a trading log into Arrow batches and writes
  batches back out as lines. It is a `Dataset`.
- **`dataset` / `iceberg`** — `Dataset` is a stream in and a stream out;
  `IcebergDataset` is that over an Iceberg table, with catalog and namespace
  CRUD and the maintenance (compact, expire, sweep) a streaming table needs.
  `write_arrow(merge_by=...)` upserts; `append_arrow(merge_by=...)` inserts
  only what is not stored yet and never rewrites a row.
- **`fix`** — `FixMessage` parses FIX log lines (SOH-, `|`- or `^A`-separated,
  wire tags or rendered `Name=Value` / `Group[i]=Member=Value` spellings),
  `parse_arrow_array` does whole columns in Arrow kernels, `tag_arrow_array`
  turns the maps' text keys into integer FIX tags, and `FixRegistry` scrapes
  every FIX version's fields from the OnixS dictionary into `~/.config/fix/` —
  name, datatype, comment, values — to work offline after.
- **`convert`** — `Convertible`: paired `from_*`/`into_*` methods that serialise
  any dataclass to dict, JSON, YAML or TOML and back.

## Development

```bash
cd python
uv sync
uv run pytest
uv run ruff check && uv run ruff format --check .
```

`AGENTS.md` carries the house style. Licensed under Apache-2.0.
