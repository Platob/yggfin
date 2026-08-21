# rekep

Trading logs as Arrow, a declaration that *is* a schema, and Iceberg tables you
can read and write without learning Iceberg first.

Three ideas, and everything else is built from them:

<div class="grid cards" markdown>

- :material-shape: **[Types](types.md)** — `Field` is one name, one Arrow type
  and metadata. `@field` turns a class into one, and the same object casts real
  data onto the shape it declares.

- :material-file-document-outline: **[Logs](logs.md)** — `TextFile` parses a
  trading log into Arrow batches, and writes them back out as lines. `LogFiles`
  does the same for a whole folder, in path order. Both are datasets, so
  pushing a capture into a table is one call.

- :material-table: **[Iceberg](iceberg.md)** — `IcebergDataset` reads and writes
  a table through pyiceberg, creates it from your declaration, and keeps it fast
  (compact, expire, sweep) without a maintenance job of your own.

</div>

And two pages about how they fit together, and how to build on them:

<div class="grid cards" markdown>

- :material-ruler-square: **[Design rules](design.md)** — Arrow is the hub, the
  shape is declared before the data, data is cast onto the declaration, and
  everything is a stream. The rules, and the process for exchanging Arrow data
  between two systems that do not share code.

- :material-file-sign: **[Schema contracts](contracts.md)** — the `schemas/`
  directory: one Arrow schema per file, in YAML or JSON, nested types included,
  pinned by CI so what is agreed and what is stored cannot drift apart.

</div>

## Install

=== "Just Arrow"

    ```bash
    pip install rekep
    ```

=== "With Iceberg"

    ```bash
    pip install "rekep[iceberg]"    # pyiceberg + a local SQLite catalog
    ```

=== "Everything"

    ```bash
    pip install "rekep[all]"        # + yaml, toml writing, faster line hashing
    ```

## In one screen

=== "Declare"

    ```python
    import datetime
    from typing import Annotated

    from rekep import Convertible, Field, field


    @field
    class Quote(Convertible):
        """One quote."""

        symbol: Annotated[str, Field.primary_key()]
        """Instrument."""

        day: Annotated[datetime.date, Field.partition_key()]
        """Trading day."""

        size: int
        """Quantity."""

        venue: str | None = None
        """Where it traded, when known."""
    ```

=== "Project"

    ```python
    Quote.FIELD.into_arrow_schema()      # symbol: string not null, day: date32 ...
    Quote.FIELD.into_iceberg_schema()    # ids, docs, identifier fields
    Quote.FIELD.primary_keys()           # ['symbol']
    Quote.FIELD.partition_keys()         # {'day': 'identity'}
    Quote.FIELD.into_yaml("schemas/trading/quote.yaml")   # the declaration, published
    ```

=== "Cast"

    ```python
    # A batch that is nearly right: wrong order, a narrow int, a missing column
    Quote.FIELD.cast_arrow(batch)
    Quote.FIELD.cast_arrow(table)
    Quote.FIELD.cast_arrow(batches, merge_schema=True)
    ```

=== "Capture"

    ```python
    from rekep import LogFiles, TextFile

    TextFile.from_path("app.txt.gz").read_arrow_table()      # one log

    files = LogFiles.from_folder("s3://bucket/logs", pattern="*.txt*")
    files.read_arrow_reader()          # every log, in path order, one open at a time
    files.into_byte_chunks(compression="gzip")   # or just ship the bytes
    ```

=== "Store"

    ```python
    from rekep.iceberg import IcebergDataset

    quotes = IcebergDataset(
        name="trading.quotes",
        catalog="local",
        properties={"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "file:///wh"},
        struct=Quote.FIELD,
    )
    quotes.write_arrow(batches, merge_by=True)     # creates the table if absent
    quotes.read_arrow_table(row_filter="day = '2026-08-14'")
    quotes.optimize()                              # compact, expire, sweep
    ```

## What holds it together

Arrow is the hub. A declaration projects onto an Arrow schema; every other view
— Iceberg, a contract file, documentation, a rebuilt Python class — comes from
that one projection rather than a second walk of the type system. Data is cast
onto the declaration, never the other way round, so a nearly-right batch lands
instead of failing a schema comparison.

Everything is a stream unless you say otherwise: a log is read batch by batch, a
folder of logs one file at a time, a write commits once per chunk of rows, and
nothing here needs a dataset to fit in memory.

Where two systems have to agree without sharing code, the declaration becomes a
[contract](contracts.md) — a file in the repository that reads back as the same
Arrow schema, keys, comments and nested types included.

Every page ends with the measurements behind its claims; how those are produced
is on [Benchmarks](benchmarks.md).
