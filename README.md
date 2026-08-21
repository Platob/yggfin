# rekep

`rekep`: trading logs as Arrow, a declaration that *is* a schema, and Iceberg
tables you can read and write without learning Iceberg first.

**[Documentation](https://platob.github.io/rekep/)** —
[Design rules](https://platob.github.io/rekep/design/) ·
[Schema contracts](https://platob.github.io/rekep/contracts/) ·
[Types](https://platob.github.io/rekep/types/) ·
[Logs](https://platob.github.io/rekep/logs/) ·
[Iceberg](https://platob.github.io/rekep/iceberg/)

```bash
pip install "rekep[all]"
```

```python
import datetime
from typing import Annotated

from rekep import Convertible, Field, Log, TextFile, TextFiles, field
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

# or a whole capture: every log under the folder, in path order, one open at a time
capture = TextFiles.from_folder(
    "s3://bucket/logs/2026-08-14", pattern="*.txt*", static_values={"bridge": "bridge-1"}
)
logs.append_arrow(capture.read_arrow_reader(), merge_by=True, commit_row_size=1_000_000)

logs.read_arrow_table(row_filter="hunix = 1786665600000000000")
logs.optimize()                      # compact, expire, sweep
```

## What is in it

- **`fields`** — `Field` is one name, one Arrow type and metadata. `@field`
  turns a class into one; `StructField`, `ListField`, `MapField` and the list
  flavours make what is inside reachable as what it is; the casts take real data
  onto the shape, recursively, in Arrow kernels only.
- **`logs`** — `TextFile` parses a trading log into Arrow batches and writes
  batches back out as lines. `TextFiles` is the same over a folder of them: a
  lazy, `pyarrow.fs`-based walk in path order, one file open at a time, and a
  streamed byte flow (raw or compressed as it goes) for shipping a capture
  rather than parsing it. Both are a `Dataset`, and both take `static_values`
  for the constant columns a file never states itself.
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
  name, datatype, comment, values — to work offline after. That scrape is also
  committed here, as `data/fix.zip`, so there is nothing to wait for: a cache
  is a directory of JSON or a zip of the same, and the extension says which.
- **`market`** — `Order`, `Execution`, `BookSide` and `Book` as a *history*:
  every version of every thing is its own immutable row, keyed by a signed
  `int64` digest of its own content — the one column Iceberg, Spark and Doris
  all read alike — and linked to the version before it. A
  state, a side and a kind are banded `int32` codes, so "is it over" is one
  range predicate an engine can prune on rather than a set of literals it
  cannot; around forty columns carry the FIX field they came from, checked
  against `data/fix.zip` by CI. `Book.summarise_arrow` derives the mid, the
  spread, the microprice and the imbalance in kernels, once, so no reader has
  to reach into a nested list that no engine below prunes on — and
  `Book.from_events` folds one instrument's stream into the book it describes,
  one row per instant that moved it. `FixEvents` is the way in from a venue: a
  FIX message, or the pairs one was rendered as, read as the orders and
  executions it carries — with `unix` taken from `TransactTime <60>`, which is
  when the transaction happened, and not from `SendingTime <52>`, which is when
  the message was sent.
- **`tasks`** — a unit of work declared in a document rather than written as a
  script: `Task.from_yaml("tasks/parse_logs/parse_logs.yml").run()`. Two are
  shipped, and together they are the pipeline a capture needs. `ParseLogs` is
  one streaming pass over a folder of logs, every line classified by regular
  expression and landed in the Iceberg table for what it is about
  (`order_logs`, `execution_logs`, … `unknown_logs`). `ParseMarket` reads those
  lines as FIX and lands what they *mean* — `market.orders`,
  `market.executions`, and `market.books` folded from both, one row per instant
  that moved the book. Both append with a merge, so re-running one over a
  capture that grew by a day costs the day. Each has a commented `.yml` and a
  notebook that runs it end to end under `tasks/`.
- **`convert`** — `Convertible`: paired `from_*`/`into_*` methods that serialise
  any dataclass to dict, JSON, YAML or TOML and back.

## Schema contracts

`schemas/` at the repo root is what this repository publishes to whoever
exchanges data with it: one Arrow schema per file, YAML or JSON, nested types,
keys and column comments included.

```python
from rekep import Field

quote = Field.from_yaml("schemas/trading/quote.yaml")
quote.cast_arrow(batch)          # a contract is a target shape, not a comment
```

They are tested against the declarations they came from, so a column that
exists in code and not in the contract fails the build — and the same two
checks run from the command line, without writing Python:

```bash
rekep fields dump --pyclass rekep.logs.log:Log --target schemas/rekep/log.yaml
rekep fields load --target schemas/rekep/log.yaml     # does it still build?
```

## Published data

`data/` at the repo root is what this repository publishes that is the same for
everyone: `data/fix.zip` is the whole OnixS FIX dictionary, one JSON document
per version, scraped once and checked by the build.

```python
from rekep.fix import FixRegistry

registry = FixRegistry(cache_dir="data/fix.zip")   # a warm cache, nothing fetched
registry.field("Side").fix["values"]               # '{"1":"Buy","2":"Sell",...}'

FixRegistry(cache_dir="~/.config/fix").into_zip("fix.zip")   # a directory, published
```

## Development

```bash
cd python
uv sync
uv run pytest
uv run ruff check && uv run ruff format --check .
```

`AGENTS.md` carries the house style. Licensed under Apache-2.0.
