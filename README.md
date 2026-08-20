# yggfin

`rekep`: a trading log parser, and the small schema machinery it is built on.

Four pieces, nothing else:

- **`logs`** -- `LogFile`, streaming Arrow-native access to a trading log,
  local or on an object store, plain or compressed.
- **`fields`** -- `Field` and the `@field` decorator: a class *is* a field,
  with full Arrow interop in both directions, and the casts that make real
  data agree with one.
- **`dataset`** -- `Dataset`, the read and write ends of a stored product, and
  `iceberg.IcebergDataset`, its Iceberg implementation.
- **`convert`** -- `Convertible`: paired `from_*`/`into_*` methods that
  serialise any dataclass to dict, JSON, YAML or TOML and back.

## Install

```bash
pip install rekep            # pyarrow only
pip install rekep[iceberg]   # + the Iceberg dataset, SQLite catalog included
pip install rekep[all]       # + yaml, toml writing, and the faster line hash
```

## Reading a log

```python
from rekep import LogFile

with LogFile.from_path("app.txt.gz") as log:
    for batch in log.into_arrow_batches():   # nothing is materialised whole
        ...
    # or: log.into_arrow_table(), log.into_arrow_reader()
```

Rows come back shaped by `Log`: `url`, `unix`, `date`, `time`, `thread_name`,
`driver`, `message`, `hash64`. Compression is inferred from the extension,
timestamps are converted by Arrow compute rather than parsed row by row, and
a wrapped stack trace folds into the row above it.

## Declaring a field

`@field` turns a class into one `Field` -- a name, an Arrow type and metadata
-- built once, lazily, and reachable as `FIELD`:

```python
import pyarrow
from typing import Annotated
from rekep import Convertible, Field, field

@field
class Venue(Convertible):
    """A trading venue."""

    mic: Annotated[str, Field.primary_key()]
    """ISO 10383 market identifier."""

    size: Annotated[int, Field(arrow_type=pyarrow.int32(), metadata={"unit": "lots"})]
    day: Annotated[datetime.date, Field.partition_key()]
    timeout: float | None = None      # `| None` is what makes a column nullable

Venue.FIELD.name                      # 'Venue'
Venue.FIELD.into_arrow_schema()       # mic: string not null, size: int32 not null, ...
Venue.FIELD.field("mic").description  # 'ISO 10383 market identifier.'
Venue.FIELD.primary_keys()            # ['mic']
Venue.FIELD.into_json("venue.json")   # the declaration, as a document
```

The docstring under a member becomes the column comment; the class docstring
becomes the schema's. A bare `pyarrow.DataType`, `Mapping` or `str` inside
`Annotated` is shorthand for the type, the metadata or the description.

**The type picks the class.** A field whose type is a struct, a list or a map
comes back as a `StructField`, a `ListField` or a `MapField`, so what is
inside it is reachable as what it is:

```python
Venue.FIELD.field("legs").item.field("mic")   # a list's item
Venue.FIELD.field("limits").key               # a map's halves
Venue.FIELD.field("size").description = "Lots."  # a member is a view: setting
                                                 # this rebuilds the struct
```

Arrow converts back just as well, so a schema from a parquet footer or another
team's contract gets the same machinery:

```python
Field.from_arrow_schema(schema).into_dataclass()   # a @field class, losslessly
```

A field is also a **target shape**. `cast_arrow_array`, `cast_arrow_batch`,
`cast_arrow_table` and `cast_arrow_reader` cast columns onto it, fill missing
nullable ones, drop extras and reorder, so a nearly-right batch writes -- while
a missing NOT NULL column is refused by its path (`venue.mic`). The cast
recurses, so a struct that grew a member, a list of structs or a map with a
narrowed value are all handled where they are declared:

```python
Quote.FIELD.cast_arrow_batch(batch)                     # onto the declared shape
Quote.FIELD.cast_arrow_reader(batches, merge_schema=True)   # keep what it also has
```

`benchmarks/bench_cast.py` measures it: 1.1-2.4x faster than the equivalent
`Array.cast`, and columns the target does not declare cost nothing.

## Reading and writing a dataset

`Dataset` is a stream in and a stream out, and `IcebergDataset` is that over a
real Iceberg table -- pyiceberg plans the scans, writes the files and commits
the snapshots:

```python
from rekep import Log
from rekep.iceberg import IcebergDataset

logs = IcebergDataset(
    name="trading.logs",
    catalog="local",
    properties={"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "file:///data"},
    struct=Log.FIELD,          # the table is created from this the first time
)

with LogFile.from_path("app.txt.gz") as log:
    logs.write_arrow_reader(log.into_arrow_reader(), merge_by=True, commit_row_size=1_000_000)

logs.read_arrow_table(row_filter="date = '2026-08-14'", columns=["unix", "message"])
```

The declared shape carries everything the table needs: column comments become
Iceberg docs, `Field.primary_key()` becomes its identifier fields and what a
`merge_by=True` upsert joins on, and `Field.partition_key()` becomes its
partition spec. Reads push the filter, the columns and the limit down to the
scan planner, and pass the store's own batches through untouched unless a
schema to cast onto is asked for. Writes cast the stream onto the table's
shape and commit once per `commit_row_size` rows, because a batch is not a
unit of work for a store that lands a file per call.

## Serialising a dataclass

`Convertible` gives any dataclass paired builders and converters, with generic
forms that infer the format from an extension or a requested type:

```python
venue = Venue(mic="XPAR", size=1)
venue.into_yaml("venue.yaml")
Venue.from_("venue.yaml")            # -> from_yaml
venue.into_(dict)                    # -> into_dict
```

Fields that are None are omitted rather than written as null (TOML has no
null, and a missing key lets the default apply on the way back), and unknown
keys are ignored on load. Every method takes an open file, a path, a URI or
raw bytes -- pass nothing to be handed the encoded bytes.

## Development

```bash
cd python
uv sync
uv run pytest
uv run ruff check && uv run ruff format --check .
```

`AGENTS.md` carries the house style. Licensed under Apache-2.0.
