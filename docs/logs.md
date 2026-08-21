# Logs

A trading log is a text file with a fixed header and a free-form payload:

```text
2026-08-14 00:05:01.167_520 [77-e72:9ef:72503] [ModuleFoo] (DEBUG) Found code
^timestamp                  ^thread_name       ^driver_name ^level ^message
```

`TextFile` reads that into Arrow batches, writes batches back out as lines, and
**is a dataset** — so a log and an Iceberg table are the same kind of thing to
whatever consumes them. `TextFiles` is the same for a whole folder of them,
because a capture is never one file.

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

    **Reading** works wherever `pyarrow.fs` reaches. **Writing** does not: a log
    is written by appending, and S3 and GCS have no append — only a whole-object
    put, and reading a log back to rewrite it is what a log is least able to
    afford. Write to a local path and upload it, or push the rows into an
    [Iceberg table](iceberg.md), which owns its own files.

=== "Tuning"

    ```python
    log.read_arrow_reader(
        batch_row_size=65_536,      # rows per batch: memory is bounded by this
        read_byte_size=1 << 22,     # bytes per read: one ranged GET on a store
        fold_continuations=True,    # a wrapped stack trace stays one row
    )
    ```

    The 4 MiB default read is a request size as much as a buffer: on an object
    store it is one ranged GET, so 64 KiB would mean a GET per 64 KiB. On a
    local disk the floor costs nothing measurable and only the ceiling shows —
    64 MiB spends more time waiting for a read than parsing it
    ([measured](#what-moves-the-parser)).

## Reading a folder of them

A capture is a live log, the rotations behind it, yesterday's gzipped in an
archive, and often a directory per host. `TextFiles` reads all of it as one
stream — in path order, one file open at a time, over any filesystem
`pyarrow.fs` reaches.

=== "A folder"

    ```python
    from rekep import TextFiles

    files = TextFiles.from_folder("/var/log/app", pattern="*.txt*")
    files.read_arrow_table()                       # every log, one table
    files.read_arrow_reader(batch_row_size=65_536) # or streamed, which is the point
    ```

    `from_folder` takes a local path or a URI, so an object store is the same
    call: `TextFiles.from_folder("s3://bucket/logs/2026-08-14")`. Every listing,
    every open and every read goes through `pyarrow.fs` — one credential chain,
    one set of URI rules, the same handles `TextFile` uses.

=== "Several, in order"

    ```python
    # the order given is kept: the archive is older, so it is read first
    files = TextFiles.from_folders(["/archive/app", "/var/log/app"], pattern="*.txt*")

    # a root may be a file: folders and files can be mixed, in one call
    files = TextFiles.from_folders(["s3://b/logs/app.1.txt.gz", "s3://b/logs/app.txt"])
    ```

    A stated order is a statement about time, so nothing re-sorts it. A root
    that is a **directory** is walked; a root that is a **file** is taken as it
    is, because naming a file *is* the selection.

=== "What order the walk is"

    ```python
    list(files.into_urls())
    # ['/var/log/app/app.1.txt.gz', '.../app.2.txt.gz', '.../app.10.txt.gz', '.../app.txt']

    TextFiles.from_folder("/var/log/app", reverse=True)       # the same order, backwards
    TextFiles.from_folder("/var/log/app", recursive=False)    # this folder only
    TextFiles.from_folder("/var/log/app", pattern="*.txt.gz") # base-name glob
    ```

    Order is decided here, not by the store: `pyarrow.fs` lists a directory in
    whatever order the filesystem answers in — inode order on Linux, arbitrary
    on an object store — so a set that did not sort would hand rows over in a
    different sequence on every machine. Digit runs compare as **numbers**, so
    `app.2.txt.gz` precedes `app.10.txt.gz` rather than following it.

    Which direction is *chronological* is the writer's convention, and a path
    cannot be asked: `app.txt` sorts after `app.1.txt.gz` while `app.log` sorts
    before `app.log.1.gz`. Where the order has to be exact, state it with
    `from_folders` — whose roots keep the order you gave them, `reverse` or
    not, because that order is your statement and the flag is about what the
    store decides.

=== "The paths themselves"

    ```python
    for url in files.into_urls():          # a generator: nothing is listed early
        print(url)

    for log in files.into_files():         # each one as a TextFile, unopened
        log.url

    for info in files.into_file_infos():   # pyarrow.fs.FileInfo: size, mtime, type
        info.size
    ```

    The walk goes one directory at a time, so the first path arrives without
    the whole tree being listed and a store is never asked to materialise every
    key under a prefix. A root that is **not there** is refused rather than
    skipped — a misspelt folder that quietly yields no rows is a pipeline that
    reports success and stores nothing.

=== "Bytes, not rows"

    ```python
    for chunk in files.into_byte_chunks():                        # decoded log text
        ship(chunk)

    for chunk in files.into_byte_chunks(compression="gzip"):      # one .gz member
        upload(chunk)

    files.read(1 << 20)          # it is also a binary stream: hand it to anything
    files.into_bytes()           # the whole capture, when it fits
    ```

    The other half of what a set is for: shipping a capture rather than parsing
    it. Each file is decoded by Arrow as it is read — `.gz`, `.zst`, plain, by
    extension — so what comes out is log text whatever the folder mixes, and
    what is held is one read. `compression=` re-encodes that stream through a
    codec Arrow can **stream** — `gzip`, `zstd`, `lz4`, `bz2`, `brotli`, but
    not `snappy` or `lz4_raw` — and it does so **as it goes**, because
    `Codec.compress` would need the whole capture in memory first
    ([measured](#shipping-the-bytes)).

    A file that does not end in a newline is separated from the next by one,
    here rather than in the parser: without it the last line of one log and the
    first of the next are glued into a single row.

=== "Into a table"

    ```python
    from rekep import Log, TextFiles
    from rekep.iceberg import IcebergDataset

    logs = IcebergDataset(
        name="trading.logs",
        catalog="local",
        properties={"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "file:///wh"},
        struct=Log.FIELD,
    )

    files = TextFiles.from_folder(
        "/var/log/app", pattern="*.txt*", static_values={"bridge": "bridge-1"}
    )
    logs.append_arrow(files.read_arrow_reader(), merge_by=True, commit_row_size=1_000_000)
    ```

    A set is a `Dataset`, so a whole capture goes into a table in one call, and
    `merge_by=True` makes re-running it free — the rotation that was already
    ingested matches on its key and is not written again.

!!! warning "A set of files cannot be written"

    `write_arrow` is refused rather than guessing: nothing says which file a
    row belongs in, and the answer — when it was captured — is not something a
    writer can decide afterwards. `append_arrow` is refused with it, and
    refused *first*: the generic append reads a key column before it writes,
    which here would parse the whole capture on its way to the same error.
    Write one file with `TextFile`, or a dataset that owns its own files
    ([Iceberg](iceberg.md)).

## What comes out

`Log` is the shape, and like every other shape here it is a `@field` class. It
is also published as a [contract](contracts.md) at `schemas/rekep/log.yaml`,
so a consumer that does not import this package still knows what a row is.

| column | type | what it is |
| --- | --- | --- |
| `url` | `string` | the log the line came from |
| `recorded_at_unix` | `int64` | nanoseconds since the epoch — **primary key** with `hash64` |
| `recorded_at_date` | `date32` | the local calendar day — **partition** |
| `recorded_at_time` | `time64[us]` | the local time of day |
| `thread_name` | `string` | the first bracketed field |
| `driver_name` | `string` | the second bracketed field |
| `category_id` | `int32` | categorisation placeholder — `0` until assigned, never null |
| `category_name` | `string` | categorisation placeholder — empty until assigned, never null |
| `message` | `string` | payload, continuations folded in |
| `hash64` | `int64` | xxh3-64 of the raw line — **primary key** with `recorded_at_unix` |

…and then whatever `static_values` declares, in the order it declares them.

=== "Inspect it"

    ```python
    from rekep import Log

    Log.FIELD.names                                   # the columns above
    Log.FIELD.field("recorded_at_unix").metadata      # {'unit': 'nanosecond', ...}
    Log.FIELD.primary_keys()                          # ['recorded_at_unix', 'hash64']
    Log.FIELD.partition_keys()                        # {'recorded_at_date': 'identity'}
    ```

=== "Local time"

    ```python
    log = TextFile.from_path("app.txt", timezone="Europe/Paris")
    files = TextFiles.from_folder("/var/log/app", timezone="Europe/Paris")
    ```

    A log writes a wall clock and says nothing about which one. Naming the zone
    turns the same characters into a real instant (`recorded_at_unix`); leaving it out keeps
    the older reading — the clock *is* UTC — rather than inventing a zone. A
    set passes whatever it is given to every file it opens.

=== "Your own shape"

    ```python
    log.read_arrow_table(MyRow.FIELD)     # cast on the way out
    ```

=== "Columns the file never says"

    ```python
    log = TextFile.from_path(
        "app.txt",
        static_values={
            "bridge": "bridge-1",                              # type inferred
            "desk": pyarrow.scalar("EU", pyarrow.large_string()),  # or stated
            "region": pyarrow.scalar(None, pyarrow.string()),      # a typed null
        },
    )
    log.read_arrow_table().schema.names[-3:]   # ['bridge', 'desk', 'region']
    ```

    A capture knows things the file does not: which bridge wrote it, which
    desk it belongs to, which environment it came from. `static_values` is
    where they go — **nothing here is hardcoded**, so a source names its own
    columns.

    Each entry becomes a constant column, appended **after** the data columns
    in the order given, so adding one never moves a column a reader is already
    selecting. A plain Python value has its Arrow type inferred; a
    `pyarrow.Scalar` states it, which is also the only way to say "null, of
    this type". A bare `None` is refused: Arrow's `null` type is a column no
    store can widen later.

## Writing one

Writes render the header back — in Arrow string kernels, not a loop — so a file
written here parses back into the same rows. Give the writer the same
`timezone` you gave the reader: `recorded_at_unix` is an instant and a line is a wall
clock, so the zone is what turns one back into the other.

=== "Round trip"

    ```python
    rows = TextFile.from_path("app.txt").read_arrow_table()

    out = TextFile.from_path("copy.txt")
    out.write_arrow(rows)                  # creates the file, appends the lines
    out.read_arrow_table()                 # the same rows, read again from the head
    ```

=== "What a write needs"

    ```python
    # only the columns a line is made of; the rest is derived when it is read
    batch = pyarrow.RecordBatch.from_pydict(
        {
            "recorded_at_unix": [...],
            "thread_name": [...],
            "driver_name": [...],
            "message": [...],
        }
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

    with TextFile.from_path("app.txt.gz", static_values={"bridge": "bridge-1"}) as log:
        logs.append_arrow(
            log.read_arrow_reader(),   # streamed, never materialised
            merge_by=True,             # insert only new (recorded_at_unix, hash64)
            commit_row_size=1_000_000, # one snapshot per million rows
        )
    ```

=== "3. Or push the whole capture"

    ```python
    from rekep import TextFiles

    files = TextFiles.from_folder(
        "s3://bucket/logs/2026-08-14", pattern="*.txt*", static_values={"bridge": "bridge-1"}
    )
    logs.append_arrow(files.read_arrow_reader(), merge_by=True, commit_row_size=1_000_000)
    ```

    The same call, over a folder instead of a file: one file open at a time,
    rows in path order, and the rotations that were ingested yesterday matched
    on their key and skipped.

=== "4. Query it back"

    ```python
    logs.read_arrow_table(
        row_filter="recorded_at_date = '2026-08-14'",             # pushed to the planner
        columns=["recorded_at_unix", "driver_name", "message"],   # so is the projection
    )
    ```

=== "5. Keep it fast"

    ```python
    logs.optimize()     # merge manifests, compact files, expire snapshots, sweep
    ```

!!! tip "Re-running the same file is free"

    `append_arrow(..., merge_by=True)` inserts only the rows whose primary key —
    the timestamp and the hash of the raw line — is not stored yet, and never
    rewrites what is: a log line is immutable, so re-ingesting a rotated log or
    replaying a day appends nothing, commits nothing, and touches no stored row.
    A `write_arrow` with `merge_by` is the upsert, for rows that do change.

## Benchmarks

`benchmarks/bench_text_file.py` measures the parser end to end — rows/s, MB/s
and peak Arrow allocation — on one file and on a folder of them. How these
numbers are produced, and how to read a range, is on
[Benchmarks](benchmarks.md).

```bash
cd python
uv run python benchmarks/bench_text_file.py                  # every sweep
uv run python benchmarks/bench_text_file.py --quick          # 200k rows, one config
uv run python benchmarks/bench_text_file.py --only variants  # what moves the parser
uv run python benchmarks/bench_text_file.py --only folders   # a capture of many files
```

The hot loop is deliberately spartan — per row it is a regex match, an append
and a hash; everything columnar (timestamps, UTF-8 validation, the day and time
split) happens once per batch in Arrow. That is why the numbers below move with
line shape and hashing, and barely with anything else.

### Reading one file

`bench_text_file.py --only sweep`. 400,000 rows, 54.4 MiB plain and 5.0 MiB
gzipped, best of three, both runs quoted. `peak` is Arrow's own allocator,
which is where the batches live.

| file | batch rows | read size | rows/s | peak |
| --- | --- | --- | --- | --- |
| plain | 16,384 | 64 KiB | 351k–366k | 2.8 MiB |
| plain | 65,536 | 64 KiB | 296k–300k | 11.2 MiB |
| plain | 65,536 | 1 MiB | 354k–360k | 11.2 MiB |
| plain | 65,536 | 4 MiB (the default) | 342k–357k | 11.2 MiB |
| plain | 65,536 | 8 MiB | 328k–334k | 11.2 MiB |
| plain | 262,144 | 4 MiB | 260k–265k | 44.8 MiB |
| gzip | 16,384 | 64 KiB | 351k–370k | 3.9 MiB |
| gzip | 65,536 | 64 KiB | 330k–337k | 12.5 MiB |
| gzip | 65,536 | 1 MiB | 331k–335k | 12.5 MiB |
| gzip | 65,536 | 4 MiB | 328k–331k | 12.5 MiB |
| gzip | 65,536 | 8 MiB | 318k–342k | 12.5 MiB |
| gzip | 262,144 | 4 MiB | 249k–253k | 46.6 MiB |

**Batch size is the memory knob, not a speed one.** Every configuration lands
between 249k and 370k rows/s, and the fastest rows are the *smallest* batches:
a 262,144-row batch costs 45 MiB to hold and parses no faster than a 16,384-row
one that costs 2.8. The 65,536 default sits where a batch is large enough to
amortise the per-batch Arrow work and small enough to stay under about 12 MiB.

**Compression is close to free in rows/s** — Arrow decodes in its C++ layer
while the row loop is the bottleneck — so a gzipped log parses at nearly the
rate of a plain one from a tenth of the bytes.

### What moves the parser

`bench_text_file.py --only variants`. A million rows, best of three, both runs
quoted, each case on its own file so the comparison is like for like. The
throughputs are per *parsed row*, not per byte: a log stuffed with stack traces
carries more bytes for the same number of rows.

| case | rows/s | vs the baseline |
| --- | --- | --- |
| a stack trace every 200 lines (the baseline) | 362k | — |
| no stack traces at all | 339k–369k | inside the spread |
| a trace every other line | 261k–291k | −20% to −28% |
| no continuation folding | 320k–348k | inside the spread |
| 64 KiB reads | 344k–364k | 0 to −5% |
| 64 MiB reads | 287k–321k | −11% to −21% |
| **blake2b line hash** (what xxh3 replaced) | **264k–268k** | **−26%** |
| gzip (12.5 MiB instead of 136 MiB) | 306k–323k | −11% to −15% |

Two things to take from that. **Folding is cheap until continuations
dominate**: a trace every 200 lines and no traces at all are the same number,
turning folding off does not reliably move it either, and only a trace every
*other* line costs anything — because folding is the only per-line work that is
not a regex match.

And **the hash is worth a quarter of the parser**. That row is the reason
`xxhash` is a hard dependency rather than an extra: `hash64` is half of the
primary key, so which hash produced it is data — under a fallback the same line
would be keyed differently in two environments — and the fallback would cost
26% of the parse as well.

Read size has a ceiling but, on a local disk, no floor worth naming: 64 MiB
spends more time waiting for a read than parsing it, while 64 KiB costs nothing
measurable here. It is not free on an object store, where the read size *is*
the request size and 64 KiB means a GET per 64 KiB — which is what the 4 MiB
default is chosen for.

### A folder of them

`bench_text_file.py --only folders`. The same million rows every time, cut into
a different number of files, so the column that moves is fragmentation and
nothing else. Best of three, both runs quoted.

!!! note "Its own machine"

    This sweep was run on a different machine from the two tables above, so its
    rows/s are comparable *within* the table and not against them.

| case | files | rows/s | batches | peak MiB |
| --- | --- | --- | --- | --- |
| one file | 1 | 416k–425k | 16 | 11.7 |
| 20 files | 20 | 416k–418k | 10 | 35.8 |
| 500 files | 500 | 411k–425k | 16 | 23.7 |
| 500 files, chained by hand | 500 | 421k–447k | **500** | 0.4 |
| 500 files, gzipped | 500 | 379k–390k | 16 | 25.5 |

**Fragmentation costs nothing measurable.** Five hundred files parse at the
rate one does, because the per-line regex is the bottleneck either way and an
open is paid once per two thousand rows here. On an object store it would be
one GET per file instead, which is why the walk lists lazily and reads one file
at a time.

**Combining short batches is the one thing a set does that a chain of readers
does not**, and the row above says what it costs: `chained by hand` is the same
files with every per-file batch handed straight downstream, and it is 500
batches instead of 16, at 421k–447k rows/s against 411k–425k and 0.4 MiB held
against 23.7. So the copy is worth a few per cent of the parse and one batch of
memory, and it buys a consumer thirty times fewer units of work — a store that
commits per call, or a writer that lands a row group per batch, pays that back
immediately.

The 20-file row holds the most (35.8 MiB) for a reason worth knowing:
`batch_row_size` is a **lower** bound, and a batch is never cut to hit it. Two
50,000-row files combine into one 100,000-row batch when 65,536 was asked for.

**Gzip costs about 8%** of the rows/s (379k–390k against 411k–425k) and is
decoded in Arrow's C++ layer while the row loop is the bottleneck — the same
result the single-file sweep gets from the other direction.

### Shipping the bytes

The byte flow, on the same 500-file capture: 133.5 MiB of log text, best of
three, both runs quoted. `held` is Python's own peak allocation
(`tracemalloc`), because these chunks are `bytes` and never reach Arrow's
allocator.

| flow | MB/s | out | held |
| --- | --- | --- | --- |
| `into_byte_chunks()` | 2,346–2,655 | 133.5 MiB | **4.4 MiB** |
| `into_byte_chunks(compression="gzip")` | 77.3–78.6 | 10.9 MiB | **4.4 MiB** |
| `into_byte_chunks(compression="zstd")` | 1,526–1,564 | 0.02 MiB | **4.4 MiB** |
| `into_bytes()` (materialised) | 714–716 | 133.5 MiB | **267.1 MiB** |

The last row is the configuration expected to be bad, and it is here to be
quoted: the same stream with nothing bounding it holds 267 MiB for a 133.5 MiB
capture — twice the payload, because joining copies — against 4.4 MiB for the
generator. That is the whole reason the flow is a generator, and why
`compression=` encodes as it goes instead of calling `Codec.compress` on the
lot.

Compressing is the codec's cost, not this package's: gzip runs at 77–79 MB/s
and zstd at 1.5 GB/s on the same bytes. (The synthetic log compresses
absurdly well — 0.02 MiB out of 133.5 — so read the *rate*, not the ratio.)

Walking the 500 paths costs **3.5–4.0 ms** on a local disk, one listing per
directory.
