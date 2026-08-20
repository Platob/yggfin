# yggfin

`rekep`: a trading log parser, and the small schema machinery it is built on.

Three pieces, nothing else:

- **`logs`** -- `LogFile`, streaming Arrow-native access to a trading log,
  local or on an object store, plain or compressed.
- **`fields`** -- `Field` and the `@field` decorator: a class *is* a field,
  with full Arrow interop in both directions.
- **`convert`** -- `Convertible`: paired `from_*`/`into_*` methods that
  serialise any dataclass to dict, JSON, YAML or TOML and back.

## Install

```bash
pip install rekep          # pyarrow only
pip install rekep[all]     # + yaml, toml writing, and the faster line hash
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

    mic: str
    """ISO 10383 market identifier."""

    size: Annotated[int, Field(arrow_type=pyarrow.int32(), metadata={"unit": "lots"})]
    timeout: float | None = None      # `| None` is what makes a column nullable

Venue.FIELD.name                      # 'Venue'
Venue.FIELD.into_arrow_schema()       # mic: string not null, size: int32 not null, ...
Venue.FIELD.field("mic").description  # 'ISO 10383 market identifier.'
Venue.FIELD.into_json("venue.json")   # the declaration, as a document
```

The docstring under a member becomes the column comment; the class docstring
becomes the schema's. A bare `pyarrow.DataType`, `Mapping` or `str` inside
`Annotated` is shorthand for the type, the metadata or the description.

Arrow converts back just as well, so a schema from a parquet footer or another
team's contract gets the same machinery:

```python
Field.from_arrow_schema(schema).into_dataclass()   # a @field class, losslessly
```

A field is also a **target shape**: `cast_arrow_batch` and `cast_arrow_reader`
cast columns onto it, fill missing nullable ones, drop extras and reorder, so a
nearly-right batch writes -- while a missing NOT NULL column is refused by name.

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
