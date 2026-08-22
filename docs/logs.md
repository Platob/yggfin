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

**A parsed line is an [`Event`](market.md)**, which is what lets a capture be
read beside the orders and books it describes rather than beside nothing. It
carries the same envelope every other event does, and adds four columns of its
own:

| column | type | what it is |
| --- | --- | --- |
| `unix` | `int64` | nanoseconds since the epoch — **primary key** with `hash` |
| `hunix` | `int64` | `unix` floored to the hour — **partition** |
| `etype` | `int32` | which kind of event the line is, decided by `LogRules` |
| `hash` | `int64` | xxh3-64 of the raw line — **primary key** with `unix` |
| `xhash` | `int64` | the same digest: a line is its own lifecycle |
| `url` | `string` | the log the line came from |
| `thread_name` | `string` | the first bracketed field |
| `driver_name` | `string` | the second bracketed field |
| `message` | `string` | payload, continuations folded in |
| `category_id` | `int32` | which kind of *message* the line carries; 0 is none |
| `category_name` | `string` | what that category is called |
| `fix_tags` | `map<int32,string>` | the message's fields under their FIX tags, in wire order |
| `keyval` | `map<string,string>` | the fields no tag answers for, as the log spelled them |

…the rest of the envelope (`cunix`, `runix`, `version`, `state`, the
previous-version columns), and then whatever `static_values` declares, in the
order it declares them. The envelope's unused half is constant down a whole
capture, where run-length and dictionary encoding collapse it to nothing.

**The digest is a signed `int64`, and the key is `(unix, hash)`.** Sixty-four
bits alone would be thin at a capture's scale — a few billion lines is a week of
a busy feed, which is where the birthday argument starts to bite — but two
digests only meet **in the table** if they also fall on the same nanosecond, so
the bound is per instant rather than per capture. What that buys is the column
itself: an `int64` is a join key, a sort key and a bucket source in every engine
below Arrow, where a `fixed_size_binary[16]` is a different thing in each of
them ([why](market.md#through-iceberg-spark-and-doris)).

**The two map columns are maps, and null is not empty.** An Arrow map is a
list of `(key, value)` structs underneath, and it is the one nested type that
keeps **duplicate keys in the order they arrived** — which is the whole reason
the parser returns one, because a repeating group *is* tags repeating. Both are
nullable, and the two absences are different facts: `null` is a line that
carries no message at all, `[]` is a message that carried nothing left to
store. A store that spelled those the same way could not tell a bridge that
sent an empty payload from a stack trace.

**A write is never asked for either of them.** The columns a line is made of —
the timestamp, the two bracketed fields and the message — are what
`write_arrow` demands, and everything else is derived when the line is read
back. The category and its pairs are functions of the message, so they are
read, not written.

**The partition is the hour, and it is derived from the instant.** That is the
reverse of the day-and-time columns it replaces, and deliberately: a partition
has to be a function of the column a reader filters on, and that column is
`unix`. Two logs written in different zones at the same moment then land in the
same partition, which is the only reading under which partitioning prunes
anything. An hour rather than a day because a day of ticks is one partition,
which prunes nothing inside a session; the same `int64` as `unix` because a
partition filter and a time filter are then one comparison, with no cast
between a date and an instant in the middle of it.

=== "Inspect it"

    ```python
    from rekep import Log

    Log.FIELD.names                                   # the columns above
    Log.FIELD.field("unix").metadata      # {'unit': 'nanosecond', ...}
    Log.FIELD.primary_keys()                          # ['unix', 'hash']
    Log.FIELD.partition_keys()                        # {'hunix': 'identity'}
    ```

=== "Local time"

    ```python
    log = TextFile.from_path("app.txt", timezone="Europe/Paris")
    files = TextFiles.from_folder("/var/log/app", timezone="Europe/Paris")
    ```

    A log writes a wall clock and says nothing about which one. Naming the zone
    turns the same characters into a real instant (`unix`); leaving it out keeps
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

## What each line is about

`etype` is what the line is *about* — an order, a fill, a book update — and
`LogRules` decides it. The rules are nothing but data: an ordered list of
regular expressions, matched against the message.

=== "The defaults"

    ```python
    from rekep.logs import TextFiles

    files = TextFiles.from_folder("/var/log/app")     # the default rules
    table = files.read_arrow_table()
    table.column("etype").to_pylist()                 # 210, 110, 0, 310, ...
    ```

    They read a FIX trading log by the two spellings every one of them uses:
    the wire `35=` message type, and the name a rendered log prints. So
    `35=8|` and `sent ExecutionReport for cl-1` are both an `EXECUTION`.

=== "Your own"

    ```python
    from rekep.logs.log import LogRule, LogRules
    from rekep.market import EventType

    rules = LogRules(rules=[
        LogRule(r"\bTIMEOUT\b", EventType.UNKNOWN, "a stall"),
        LogRule(r"OrderAck", EventType.ORDER, "our own gateway's spelling"),
    ])
    TextFiles.from_folder("/var/log/app", rules=rules)
    ```

    A desk with its own log format writes its own rules rather than patching
    this package — and because they are data, they travel in a
    [task document](tasks.md) with the rest of the job. `LogRules.from_yaml`
    reads a set on its own.

=== "What it costs"

    ```python
    LogRules().etype_arrow(batch.column("message"))    # one kernel per rule
    ```

    One Arrow kernel per rule over the whole message column, applied in
    reverse so the earliest rule is the one that survives. A handful of passes
    per batch, nothing per row.

**The first match wins, and no match is `UNKNOWN`.** Both halves matter. An
ordered list is what lets a specific rule sit in front of a general one without
either having to know about the other — an execution report quoting the order
it fills names both, and it is a fill. And a line nothing matches is still a
line: it is parsed, keyed and partitioned like every other, under a type that
says plainly that nobody has classified it. Dropping it, or guessing, is how a
log stops being a record of what happened.

## What each line carries

`etype` says what a line is *about*. `category_id` says what kind of **message**
it holds, and the two map columns are that message read: `fix_tags` for every
field a FIX tag answers for, `keyval` for every field none does.

=== "The defaults"

    ```python
    from rekep import TextFile

    with TextFile.from_path("app.txt") as log:
        table = log.read_arrow_table()

    table.column("category_name").to_pylist()   # ['OTHER', 'FIX', 'UL', ...]
    table.column("fix_tags")[1].as_py()         # [(8,'FIX.4.2'), (35,'D'), (55,'TTF'), ...]
    table.column("keyval")[2].as_py()           # [('ISINCODE','XX0000084733'), ...]
    ```

    Three categories out of the box: a **BeginString** anywhere is FIX, two or
    more **`#NAME=`** tokens are a UL bridge message, and everything else is
    OTHER — which is most of a capture, parses to nothing, and leaves both maps
    null. What each is and how to read it is a [rule set](fix.md#what-kind-of-message-a-line-carries).

=== "Point it at a dictionary"

    ```python
    from rekep.fix import FixCodec, FixRegistry

    codec = FixCodec(
        registry=FixRegistry(cache_dir="data/fix.zip"),   # read, never scraped
        fix_version="4.4",       # what a name resolves against with no BeginString
    )
    TextFile.from_path("app.txt", codec=codec)
    ```

    Wire messages need no dictionary — their keys are already tags. A **bridge**
    message's keys are names, and a codec with no dictionary resolves none of
    them: they land in `keyval` and the capture is stored anyway. A cold cache
    costs the tags, never the log.

=== "Values that mean nothing"

    ```python
    FixCodec(null_values=frozenset({"", "null", "<null>", "n/a"}))   # the default
    FixCodec(null_values=frozenset())                               # keep every pair
    ```

    `ACCOUNT=<null>` is an absent account. The pair is dropped before either
    map sees it, so nothing downstream has to re-implement the check
    ([why](fix.md#names-to-tags)).

=== "A folder, and a job"

    ```python
    TextFiles.from_folder("/var/log/app", codec=codec)   # every file gets this one
    ```

    ```yaml
    # or in a task document, where each of these is one flat key
    categories: {rules: [...]}
    fix_version: "4.4"
    fix_dictionary: ../data/fix.zip
    null_values: ["", "null", "<null>", "n/a"]
    ```

**A batch is cut per category before it is parsed, and put back afterwards.**
`parse_arrow_array` samples a column once and reads every row of it that way —
which is right, and which is why a capture cannot be handed to it whole: a
trading log mixes wire messages, bridge messages and prose line by line, and
one sample would read two of the three wrong. So the batch is filtered into one
slice per category, each parsed under its own rule, and the slices are
scattered back into the row order they came in — two kernels and no Python,
because the positions of every slice concatenated are a permutation of the
batch and sorting a permutation is the same thing as inverting it.

The codec cuts again inside a slice, on the same principle: one FIX session
writes `|` and the next writes SOH, and a capture holds both.

## A second codec

`TextFile` holds a **codec** and never learns which protocol it is reading.
That is the seam, and it is four verbs:

| verb | what it answers |
| --- | --- |
| `categorise(messages, drivers)` | one `(category_id, category_name)` per row |
| `into_pairs(messages, category_id)` | one `map<string,string>` per row — null for a category that reads nothing |
| `into_fix_pairs(pairs, version)` | those pairs split into the keys the protocol names and the keys it does not |
| `version_of(message, category_id)` | which protocol version to read under, and where that answer came from |

`rekep.fix.FixCodec` is the one this package ships. A second codec — market
data, an internal binary envelope, a venue's own text format — implements the
same four and nothing above it changes: not `Log`, not `TextFile`, not
`TextFiles`, not `ParseLogs`, not the contract.

Three things a second codec has to hold to, and they are the whole contract:

- **Whole columns, never a row.** Every verb takes and returns Arrow arrays of
  the batch's length. A seam that handed over one row at a time would put a
  Python loop in the middle of the hot path, which is the one thing this
  pipeline is built not to have.
- **Row order is preserved, and so are nulls.** What comes back lines up with
  what went in, position for position. A null row stays null; an unparseable
  row comes back as an empty map, never as an exception — a capture is a record
  of what happened, and a line nobody can read is still a line that was
  written.
- **Category 0 is "no message".** `into_pairs` returns a column of *nulls* for
  it rather than empty maps, because "this line is not a message" and "this
  message said nothing" are different facts and the schema keeps them apart.

A protocol with no versions answers `version_of` with `(None, "none")`, which
is an answer. A protocol whose keys are not FIX tags still fills `fix_tags`
with whatever integer identity it has, and puts the rest in `keyval`.

## Writing one

Writes render the header back — in Arrow string kernels, not a loop — so a file
written here parses back into the same rows. Give the writer the same
`timezone` you gave the reader: `unix` is an instant and a line is a wall
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
            "unix": [...],
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
            merge_by=True,             # insert only new (unix, hash)
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
        row_filter="hunix = 1786665600000000000",             # pushed to the planner
        columns=["unix", "driver_name", "message"],   # so is the projection
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
uv run python benchmarks/bench_text_file.py --only messages  # the message layer
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
`xxhash` is a hard dependency rather than an extra: `hash` is half of the
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
nothing else. Every file holds its *own* rows — 500 copies of one file would
tell a compressor something about the fixture rather than about the data. Best
of three, both runs quoted.

!!! note "Its own machine"

    This sweep was run on a different machine from the two tables above, so its
    rows/s are comparable *within* the table and not against them.

| case | files | rows/s | batches | peak MiB |
| --- | --- | --- | --- | --- |
| one file | 1 | 387k–398k | 16 | 11.5 |
| 20 files | 20 | 404k–413k | 10 | 35.2 |
| 500 files | 500 | 400k–408k | 16 | 23.5 |
| 500 files, chained by hand | 500 | 387k–416k | **500** | 0.4 |
| 500 files, gzipped | 500 | 366k–385k | 16 | 25.3 |

**Fragmentation costs nothing measurable.** Five hundred files parse at the
rate one does, because the per-line regex is the bottleneck either way and an
open is paid once per two thousand rows here. On an object store it would be
one GET per file instead, which is why the walk lists lazily and reads one file
at a time.

**Combining short batches is the one thing a set does that a chain of readers
does not**, and the row above is what it costs: `chained by hand` is the same
files with every per-file batch handed straight downstream. Its 387k–416k and
the combined 400k–408k overlap — the copy is inside the noise of this machine —
and what it buys is **16 batches instead of 500**, which is thirty times fewer
units of work for a store that commits per call or a writer that lands a row
group per batch. What it costs is memory: one combined batch instead of one
short one, 23.5 MiB against 0.4.

The 20-file row holds the most (35.2 MiB) for a reason worth knowing:
`batch_row_size` is a **lower** bound, and a batch is never cut to hit it. Two
50,000-row files combine into one 100,000-row batch when 65,536 was asked for.

**Gzip costs about 8%** of the rows/s (366k–385k against 400k–408k) and is
decoded in Arrow's C++ layer while the row loop is the bottleneck.

### Shipping the bytes

The byte flow, on the same 500-file capture: 136.3 MiB of log text, best of
three, both runs quoted. `held` is Python's own peak allocation
(`tracemalloc`), because these chunks are `bytes` and never reach Arrow's
allocator. Every flow is decoded and compared against the raw stream before it
is timed — a codec stream that lost its trailer would otherwise print as a
*better* ratio rather than as a failure.

| flow | MB/s | out | held |
| --- | --- | --- | --- |
| `into_byte_chunks()` | 2,596–2,835 | 136.3 MiB | **4.3 MiB** |
| `into_byte_chunks(compression="gzip")` | 76.6–77.7 | 11.0 MiB | **4.4 MiB** |
| `into_byte_chunks(compression="zstd")` | 737–773 | 2.3 MiB | **4.4 MiB** |
| `into_bytes()` (materialised) | 701–752 | 136.3 MiB | **272.6 MiB** |

The last row is the configuration expected to be bad, and it is here to be
quoted: the same stream with nothing bounding it holds 272.6 MiB for a 136.3
MiB capture — twice the payload, because joining copies — against 4.3 MiB for
the generator. That is the whole reason the flow is a generator, and why
`compression=` encodes as it goes instead of calling `Codec.compress` on the
lot.

Compressing is the codec's cost, not this package's: gzip runs at 77 MB/s and
zstd at 737–773 MB/s on the same bytes, for 11.0 MiB and 2.3 MiB out of 136.3.

Walking the 500 paths costs **2.6–2.8 ms** on a local disk, one listing per
directory, keyed on base names rather than whole paths.

### The message layer

`bench_text_file.py --only messages`. A different question from the four tables
above: not *how fast is the parser* but **what should each stage of it be made
of**, and what does the whole layer cost. One million lines of a mixed capture
— 60% prose, 25% wire FIX, 15% bridge messages — streamed at
`DEFAULT_BATCH_ROW_SIZE`, with every stage timed as four implementations over
the same rows. Best of three, both runs quoted.

Two things about the method, because they are what makes the table worth
reading. Every implementation is asserted to give **the scalar parser's own
pairs**, pair for pair, before it is timed — so a row here is a row that gave
the right answer. And memory is **process RSS**, sampled, not Arrow's
allocator: two of the four candidates allocate where Arrow cannot see them, and
a column that could only report one of the three allocators would decide the
question by leaving out the answer. It is read from `/proc`, so it is a Linux
figure and it is sampled, which is why it is quoted as a range rather than to
the megabyte.

!!! note "Its own machine"

    This sweep was run on a different machine from the tables above, and on a
    longer-lined fixture: 40% of these rows carry a whole message. Its rows/s
    are comparable *within* the sweep and not against them — which is the only
    comparison it is for.

**What the layer costs, which is the number to have.**

| a whole capture, streamed | rows/s | MB/s | added RSS |
| --- | --- | --- | --- |
| the message layer, on | 94.8k–98.3k | 19.6–20.3 | 217 MiB |
| `Rules(rules=[])` — no rules at all | 222k–223k | 45.9–46.0 | 41–45 MiB |

**Reading every message costs 2.3× the parse**, and it is opt-out: a rule set
with nothing in it categorises every line OTHER, parses none of them, and
leaves both map columns null. That row is also the honest comparison against
the parser before any of this existed — which measured 233k–240k rows/s on the
same fixture, so the four columns *existing* costs about 5% and the other 2.2×
is the messages actually being read.

Where the 2.3× goes is the next table, and it is not evenly spread: the bridge
category is 15% of the capture and about two thirds of the added time, because
a bridge line is the one that needs its message start found, its `#` markers
shed and its nested group entries expanded.

**Stage two, message → pairs.** `n/a` is not a failure, it is the answer: the
named path builds its keys with `extract_regex` and has no `list_element` for
offsets arithmetic to replace, and the polars chain raced here expresses the
cut but not the ragged fan-out a nested group entry needs.

| category | implementation | rows/s | pairs/s | added RSS |
| --- | --- | --- | --- | --- |
| OTHER | `FixMessage.from_text` | 198k–202k | 0 | ~0 |
| OTHER | **`parse_arrow_array`** | **2.35M–2.36M** | 0 | 1.6–2.4 MiB |
| OTHER | numpy over the buffers | n/a | — | — |
| OTHER | polars | 2.86M–3.12M | 0 | 5.0–6.4 MiB |
| FIX | `FixMessage.from_text` | 93.7k–96.6k | 1.41M–1.45M | 135–145 MiB |
| FIX | **`parse_arrow_array`** | **227k–231k** | **3.41M–3.47M** | 4.5–11.5 MiB |
| FIX | numpy over the buffers | 201k–203k | 3.01M–3.04M | 165–184 MiB |
| FIX | polars | 149k–157k | 2.23M–2.35M | 237–243 MiB |
| UL | `FixMessage.from_text` | 27.1k–28.8k | 407k–432k | 127–138 MiB |
| UL | **`parse_arrow_array`** | **56.3k–56.7k** | **845k–851k** | 38 MiB |
| UL | numpy over the buffers | n/a | — | — |
| UL | polars | n/a | — | — |

**The Arrow kernels win the stage, and the two candidates lose differently.**
The numpy cut — the same `parse_arrow_array` with `split_pattern` + two
`list_element` calls replaced by `searchsorted` and a ragged gather over the
flattened token buffer — is **12% slower** and holds **fifteen times** the
memory, because a gather materialises both halves where Arrow's cut re-uses the
buffer it already has. That is the whole result of the experiment: offsets
arithmetic is not automatically cheaper than a kernel that is already offsets
arithmetic.

Polars is the interesting loss, and it lost twice. It is the fastest thing here
on prose, and it pays for that with a quarter of a gigabyte on the wire column
where it is also 30% *slower*, because `split` → `explode` → `group_by`
materialises one row per token and then regroups. And on the bridge column it
does not run at all: a nested group entry is a fan-out of one token into
several pairs, and expressing that would have meant a second within-group
position and a second regex dialect to keep in step with the scalar parser. A
win on one category out of three, at 4–40× the memory, is not an argument for a
runtime dependency — so polars stays in the `bench` group, which is what that
group is for.

**And the OTHER row is why the categories exist at all.** Sixty per cent of a
capture parses to nothing whichever implementation reads it. The scalar parser
spends 198k rows/s discovering that; the vectorised one spends 2.35M. Deciding
what a line *is* before parsing it is worth an order of magnitude on the
majority of every capture, and it is the reason a rule set runs first.

**Stage three, name → tag.** The keys of the bridge column (983,040 of them),
resolved against the real published dictionary (`data/fix.zip`, 1,566 names).
The decoration is stripped outside the clock — `NOPARTYIDS[0].PARTYID` says
where a field sits, not what it is — so what is raced is the resolution alone.
87% resolve; the rest are the venue's own names, which is the realistic case.

| implementation | keys/s | added RSS |
| --- | --- | --- |
| `_tag_numbers` — the path this replaced | 3.32M–3.33M | 0–1.9 MiB |
| Python dict over `to_pylist()` | 8.78M–8.89M | ~0 |
| **`pyarrow.compute.index_in`** | **75.4M–75.6M** | ~0 |
| polars join | 49.2M–51.3M | 5.8–6.6 MiB |

**`index_in` is 23× the path it replaced and 8× a Python dict**, and it wins
while building its own value set inside the clock — 1,566 strings per call,
which is exactly the work a cached index removes. So a name→tag index is an
Arrow value set, built once per FIX version and probed with one kernel: never a
dictionary rebuilt per call, and never a probe per row.
