# Logs

A trading log is a text file with a fixed header and a free-form payload:

```text
2026-08-14 00:05:01.167_520 [77-e72:9ef:72503] [ModuleFoo] (DEBUG) Found code
^timestamp                  ^thread_name       ^driver     ^level  ^message
```

`TextFile` reads that into Arrow batches, writes batches back out as lines, and
**is a dataset** — so a log and an Iceberg table are the same kind of thing to
whatever consumes them.

## Reading one

=== "Whole file"

    ```python
    from rekep import TextFile

    log = TextFile.from_path("app.txt.gz")     # .gz, .zst, plain: Arrow infers it
    table = log.read_arrow_table()
    table.num_rows
    ```

=== "Streaming"

    ```python
    with TextFile.from_path("app.txt") as log:
        for batch in log.read_arrow_reader():
            ...                                 # nothing is materialised whole
    ```

=== "On an object store"

    ```python
    log = TextFile.from_url("s3://bucket/logs/app.txt.zst")
    # or hand over a configured filesystem and treat url as a path on it
    log = TextFile.from_url("logs/app.txt", filesystem)
    ```

=== "Tuning"

    ```python
    log.read_arrow_reader(
        batch_row_size=65_536,      # rows per batch: memory is bounded by this
        read_byte_size=1 << 22,     # bytes per read: one ranged GET on a store
        fold_continuations=True,    # a wrapped stack trace stays one row
    )
    ```

## What comes out

`Log` is the shape, and like every other shape here it is a `@field` class:

| column | type | what it is |
| --- | --- | --- |
| `url` | `string` | the log the line came from |
| `unix` | `int64` | nanoseconds since the epoch — **primary key** with `hash64` |
| `date` | `date32` | the local calendar day — **partition** |
| `time` | `time64[us]` | the local time of day |
| `thread_name` | `string` | the first bracketed field |
| `driver` | `string` | the second bracketed field |
| `message` | `string` | payload, continuations folded in |
| `hash64` | `int64` | hash of the raw line — **primary key** with `unix` |

=== "Inspect it"

    ```python
    from rekep import Log

    Log.FIELD.names                       # the columns above
    Log.FIELD.field("unix").metadata      # {'unit': 'nanosecond', 'epoch': '1970-01-01', ...}
    Log.FIELD.primary_keys()              # ['unix', 'hash64']
    Log.FIELD.partition_keys()            # {'date': 'identity'}
    ```

=== "Local time"

    ```python
    log = TextFile.from_path("app.txt", timezone="Europe/Paris")
    ```

    A log writes a wall clock and says nothing about which one. Naming the zone
    turns the same characters into a real instant (`unix`); leaving it out keeps
    the older reading — the clock *is* UTC — rather than inventing a zone.

=== "Your own shape"

    ```python
    log.read_arrow_table(MyRow.FIELD)     # cast on the way out
    ```

## Writing one

Writes render the header back — in Arrow string kernels, not a loop — so a file
written here parses back into the same rows.

=== "Round trip"

    ```python
    rows = TextFile.from_path("app.txt").read_arrow_table()

    out = TextFile.from_path("copy.txt")
    out.write_arrow(rows)                  # creates the file, appends the lines
    TextFile.from_path("copy.txt").read_arrow_table()   # the same rows
    ```

=== "What a write needs"

    ```python
    # only the columns a line is made of; the rest is derived when it is read
    batch = pyarrow.RecordBatch.from_pydict(
        {"unix": [...], "thread_name": [...], "driver": [...], "message": [...]}
    )
    out.write_arrow(batch)
    ```

=== "In chunks"

    ```python
    out.write_arrow_reader(reader, commit_row_size=100_000)   # one write per chunk
    ```

!!! warning "A text file cannot merge"

    `merge_by=` is refused rather than quietly appending: there is nothing in a
    flat file to match a row against. Merge where merging means something — see
    [Iceberg](iceberg.md).

## Use case: a log, then a table

The whole point of a log being a dataset is that pushing it somewhere durable is
a read and a write, with nothing in between.

=== "1. Look at it locally"

    ```python
    from rekep import TextFile

    log = TextFile.from_path("app.txt.gz")
    table = log.read_arrow_table()

    import pyarrow.compute as pc
    errors = table.filter(pc.match_substring(table.column("message"), "Exception"))
    errors.num_rows
    ```

=== "2. Push it to Iceberg"

    ```python
    from rekep import Log, TextFile
    from rekep.iceberg import IcebergDataset

    logs = IcebergDataset(
        name="trading.logs",
        catalog="local",
        properties={"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "file:///wh"},
        struct=Log.FIELD,          # the table is created from this, once
    )

    with TextFile.from_path("app.txt.gz") as log:
        logs.write_arrow(
            log.read_arrow_reader(),   # streamed, never materialised
            merge_by=True,             # upsert on (unix, hash64): reruns are safe
            commit_row_size=1_000_000, # one snapshot per million rows
        )
    ```

=== "3. Query it back"

    ```python
    logs.read_arrow_table(
        row_filter="date = '2026-08-14'",       # pushed to the scan planner
        columns=["unix", "driver", "message"],  # so is the projection
    )
    ```

=== "4. Keep it fast"

    ```python
    logs.optimize()     # merge manifests, compact files, expire snapshots, sweep
    ```

!!! tip "Re-running the same file is free"

    `merge_by=True` upserts on `Log`'s declared primary key — the timestamp and
    the hash of the raw line — so re-ingesting a rotated log, or replaying a
    day, updates rows instead of duplicating them.

## Benchmarks

`python/benchmarks/bench_text_file.py` measures the parser end to end: rows/s,
MB/s and peak Arrow allocation, on plain and compressed inputs.

```bash
cd python
uv run python benchmarks/bench_text_file.py --quick
```

The hot loop is deliberately spartan — per row it is a regex match, an append
and a hash; everything columnar (timestamps, UTF-8 validation, the day and time
split) happens once per batch in Arrow. About 390k rows/s, and the two things
that move it are continuation density and whether `xxhash` is installed — see
[Benchmarks](benchmarks.md#parsing).
