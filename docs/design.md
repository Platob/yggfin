# Design rules

Every piece of this package follows the same handful of rules, and they are
written down here because they are also what a service exchanging Arrow data
*with* it is expected to follow. They are not style: each one is the answer to
a way real data went wrong — a schema that drifted, a batch that did not fit,
a job that ran out of memory at the ninetieth percentile of its input.

The short version: **Arrow is the hub, the shape is declared before the data,
data is cast onto the declaration, and everything is a stream.**

## Arrow is the hub

One authority says what the data *is*, and every other view is derived from it
rather than maintained beside it. That authority is the Arrow schema.

```text
                      a declaration (@field class)
                      a contract file (schemas/*.yaml)
                                   │
                                   ▼
                            ARROW SCHEMA  ── the one authority
                          ╱        │        ╲
                         ▼         ▼         ▼
                Iceberg schema   parquet   a rebuilt
                ids, docs,       footer    Python class
                partitions
```

```python
Quote.FIELD.into_arrow_schema()        # what the data is
Quote.FIELD.into_iceberg_schema()      # the same thing, in Iceberg's terms
Quote.FIELD.into_dataclass()           # and back to a Python class
```

A second walk of the type system is a second place to be wrong. The Iceberg
projection is pyiceberg's own conversion plus the identity Arrow cannot carry;
the documentation of a column is the column's own metadata; the contract file
is the schema written down.

**And the schema says where it came from.** `into_arrow_schema()` puts the
declaration's name and namespace in the schema's metadata, so a schema that
travels still names the shape that produced it, and `Field.from_arrow_schema`
reads that identity back.

## The shape is declared before there is data

A declaration is not documentation of what a producer happens to emit today.
It is a statement of what the data must be, written before any of it exists,
and it carries everything a downstream needs to store the data properly.

```python
@field
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, Field.primary_key()]
    """Instrument."""

    day: Annotated[datetime.date, Field.partition_key()]
    """Trading day."""

    venue: str | None = None
    """Where it traded, when known."""
```

- **Nullability is declared, never guessed.** `str` is NOT NULL, `str | None`
  is nullable. A column nobody decided about is a column that becomes nullable
  the first time a producer forgets it.
- **A doc is the column comment**, and it travels: Arrow metadata, an Iceberg
  `doc`, a parquet footer, a contract file.
- **Keys and partitions are part of the shape**, not of the job that writes it:
  `Field.primary_key()` is what makes `merge_by=True` mean something, wherever
  the data lands.

The same statement lives in a file when the two sides of an exchange do not
share code — see [Schema contracts](contracts.md).

## Data is cast onto the declaration, never the other way round

A batch that is nearly right is the normal case: a column narrower than
declared, columns in another order, one the producer has not started sending
yet. Comparing schemas and refusing is easy and useless; casting onto the
declaration is what lets the data land.

```python
Quote.FIELD.cast_arrow(batch)                      # order, widths, missing nullables
Quote.FIELD.cast_arrow(batch, merge_schema=True)   # keep what the data has and we do not
```

What the cast will and will not do is itself a rule:

| the data has | the cast does |
| --- | --- |
| a wider or narrower type | casts it (unsafe by default: the declaration is the authority) |
| columns in another order | reorders them |
| a column the declaration does not have | drops it — or appends it with `merge_schema=True` |
| no value for a **nullable** column | fills it with nulls |
| no value for a **NOT NULL** column | **refuses, naming the path** (`'Quote.day' is missing and not nullable`) |

The refusal is the point of the table. Filling a NOT NULL column with nulls
builds data that fails later, at the write, where nothing remembers which
producer sent it.

The cast **recurses** — a struct member by member, a list its item, a map both
halves — so a nested member that is missing, narrowed or in another order is
handled where it is declared, and never by a Python loop over rows.

## Stream; never materialise

Anything that scales with the input is an iterator, and memory is bounded by a
parameter that names its unit and its dimension.

```python
log.read_arrow_reader(batch_row_size=65_536, read_byte_size=1 << 22)
logs.append_arrow(reader, merge_by=True, commit_row_size=1_000_000)
```

- `batch_row_size`, `read_byte_size`, `commit_row_size` — unit and dimension in
  the name, so nobody has to guess whether a number is rows or bytes.
- A dataset is the one thing here bigger than memory, so nothing in its
  interface may need all of it: `read_arrow_table` is for when the caller says
  it fits.
- One file open at a time, one batch held, whatever the input is: five hundred
  rotated logs parse at the rate one does
  ([measured](logs.md#a-folder-of-them)).
- **A batch is not a unit of work downstream.** A store that commits per call
  accumulates rows first; a set of small files combines its short batches. Both
  are the same rule: the size of the thing arriving is not the size of the
  thing to do.

## Let the library own what it already knows

Arrow owns codec detection, URI resolution, decompression and every
shape-changing kernel; pyiceberg owns type conversion, id assignment, scan
planning and snapshots. Delegating is not laziness — it is how the edge cases
stay correct.

```python
TextFile.from_path("app.txt.zst")     # Arrow picks the codec off the extension
logs.read_arrow_table(row_filter="unix_hour = 1786665600000000000")   # Iceberg plans
```

But **probe real behaviour before designing around an assumption.** Several
APIs surprise: a library default that is inert until a second one is set, a
`limit` that is not pushed down, an expiry that forgets snapshots without
deleting what they kept alive. Each of those cost a rewrite here, and each one
is now a measured number on the page that owns it.

## A location is parsed once, in one place

A path is not a string. `file:///C:/warehouse`, `C:\warehouse`,
`s3://bucket/key`, `s3://key:secret@minio:9000/bucket/key` and `logs/app.txt`
all name a file, and each is read differently — so everything that compares two
locations, or hands one to a filesystem, reduces both through the same parser.
That parser is `Url`.

```python
from rekep import Url

url = Url.from_string("s3://AKIA:sec:ret@minio:9000/logs/2026-08-14/app.txt")
url.endpoint      # 'minio:9000'  -- a port means a store, not a bucket
url.bucket        # 'logs'        -- so the bucket is the first path segment
url.password      # 'sec:ret'     -- userinfo splits on the *first* colon
url.into_filesystem()             # (S3FileSystem, 'logs/2026-08-14/app.txt')
repr(url)         # Url('s3://AKIA:***@minio:9000/logs/2026-08-14/app.txt')
```

Most S3 locations carry **no port at all**, because the store answers on 443 —
so a rule that only reads a port loses the bucket into the key on every one of
them. What is read is the netloc's shape:

```python
Url.from_string("s3://logs/app.txt").bucket                        # 'logs'
Url.from_string("s3://my.logs.2026/app.txt").bucket                # 'my.logs.2026'
Url.from_string("s3://s3.eu-west-1.amazonaws.com/logs/app.txt").bucket   # 'logs'
Url.from_string("s3://logs.s3.eu-west-1.amazonaws.com/app.txt").bucket   # 'logs'
Url.from_string("s3://minio.corp.com/logs/app.txt").bucket         # 'logs'
```

A bucket may carry dots — `my.logs.2026` is a legal name — so a dot decides
nothing. The **last label** decides: a name ending in `.com` is a hostname
somebody registered and pointed at a store, and a name that does not is a
bucket. Amazon's own hostnames are read further, because AWS publishes which
labels are the service: the bucket in front of one is taken off it
(`logs.s3.eu-west-1.amazonaws.com` → the bucket `logs`), and the region in it
is kept, because SigV4 signs with a region and a location signed for the wrong
one is refused rather than redirected.

The one location whose bucket really *is* a hostname is the S3 static-website
pattern, where AWS requires the bucket be named for the domain it serves. It
says so with `?endpoint_override=`, which is a decision stated in the location
and beats a shape inferred from it:

```python
Url.from_string("s3://www.example.com/index.html").bucket   # 'index.html' -- read as a store
Url.from_string(
    "s3://www.example.com/index.html?endpoint_override=s3.amazonaws.com"
).bucket                                                    # 'www.example.com'
```

A location is a value a job walks, so `Url` is a mutable dataclass and the walk
is in place; `copy()` is where a walk branches.

```python
root = Url.from_string("s3://key:secret@minio:9000/warehouse")
table = root.copy().join("trading", "quotes")   # returns the same object, moved
table.parent().path                             # 'warehouse/trading'
```

Three readings this fixes, each of which was a silent wrong answer before:

| spelling | read elsewhere as | read here as |
| --- | --- | --- |
| `s3://key:sec:ret@bucket/k` | a malformed URL, or the secret `sec` | the secret `sec:ret` |
| `s3://key:secret@minio:9000/wh` | a bucket named `minio`, port dropped | the endpoint `minio:9000`, bucket `wh` |
| `s3://wh.s3.eu-west-1.amazonaws.com/t` | a bucket named `wh.s3.eu-west-1.amazonaws.com` | the bucket `wh`, in `eu-west-1` |
| `C:/warehouse` | a URI with scheme `c` | a local path on drive `C:` |
| `C:\warehouse`, `file:///C:/warehouse` | two locations | one, spelled `C:/warehouse` |

The two middle rows are the dangerous ones: a bucket called `minio` is a legal
name and so is one called `wh.s3.eu-west-1.amazonaws.com`, so nothing raises —
the write simply lands where nobody looks.

A local path is spelled **POSIX**, always, on either host: `C:\warehouse`,
`C:/warehouse` and `file:///C:/warehouse` are one location, so one parser hands
back one string. A path that is already absolute stays where it is rather than
going back through `os.path.abspath`, which on Windows answers `/var/log` with
whichever drive the process happens to be on. Two paths only compare where both
were spelled the same way, and everything here that reduces one against another
— the orphan sweep, a folder of logs against its root — rests on that.

Because the parts are known, a location can also say what a *catalog* needs to
be told, rather than the caller repeating it as three settings:

```python
from rekep.urls import properties_of

properties_of(Url.from_string("s3://key:secret@minio:9000/wh"))
# {'s3.endpoint': 'http://minio:9000',
#  's3.access-key-id': 'key',
#  's3.secret-access-key': 'secret'}
```

That is what [`ArrowFileIO`](iceberg.md#a-catalog) fills in for a warehouse URL,
and what a caller sets explicitly always wins over it.

## Push the work to whoever holds the statistics

Filters, projections and limits go to the engine that can prune with them, and
the rows a scan *returns* say nothing about the files it *opened*.

```python
quotes.read_arrow_table(row_filter="day = '2026-08-14'", columns=["symbol", "size"])
quotes.scan_plan("driver_name = 'ULBridge'")["skipped"]   # 0 -- this filter prunes nothing
```

Reading rows in order to throw them away is the shape of every slow pipeline
here that was ever profiled.

**So nest nothing a filter needs.** No engine pushes a predicate into a map
and Iceberg writes no bounds under one, which is why the FIX tags a log is
queried on — who sent a message, when, what was traded, at what price — are
[lifted out of `fix_tags`](logs.md#the-message-flattened) into typed columns,
and stored in one place only. Only where the line carries the tag **once**,
though: lifting one occurrence out of a repeating group would answer "the
symbol" with whichever leg came first, so a row that repeats it keeps the lot
in the map and the column is null.

## Refuse rather than guess

Where a guess would corrupt data or lose it silently, the answer is an error
that names the way out. The refusals in this package are all of that kind:

| refused | because |
| --- | --- |
| a nullable primary key | an identifier that may be missing identifies nothing |
| a null or NaN merge key | no predicate can find that row again, so a replay inserts it twice |
| `merge_by` on a text file | there is nothing in a flat file to match a row against |
| writing a set of log files | nothing says which file a row belongs in |
| a missing root folder | a set that skipped it reads a capture short and reports success |
| a recursive or unknown declaration | naming the member and the way out beats inventing a type |

## `from_` builds, `into_` converts

Every conversion is a named method, so it can be called directly, read in a
traceback and overridden. The generic forms infer which one is meant.

```python
TextFiles.from_folder("/var/log/app")    # build
Quote.FIELD.into_json("quote.json")     # convert
log.into_(pyarrow.Table)                # a type is the result asked for
Field.from_("quote.yaml")               # a value is a source
```

A type argument is the requested result and is consumed by the dispatch; a
value is a source or a destination and is passed through. There is never a
`format=` argument and never a module-level factory beside a class.

## Exchanging Arrow data between two systems

The rules above are what one process does. The same rules are what makes two
processes agree, and the process for that is deliberately small.

```text
   producer                     the agreement                    consumer
   ────────                     ─────────────                    ────────
   declare the shape   ──▶   schemas/trading/quote.yaml   ◀──   load the contract
   cast on the way out            (in the repo,                 cast on the way in
          │                     reviewed, versioned)                    │
          ▼                            │                               ▼
   Arrow IPC · parquet · an Iceberg table · a log file  ─────────▶  batches that fit
              (the schema travels with the data)
```

1. **Declare the shape**, once, in whichever side owns the data.
2. **Publish it as a contract** under [`schemas/`](contracts.md) — one file per
   shape, YAML or JSON, in the repository, reviewed like code.
3. **The producer casts onto the contract before it sends.** Not after, and
   never "the consumer will cope": a producer that ships whatever it happens to
   hold makes every consumer a parser.
4. **The transport carries the schema.** Arrow IPC, parquet and Iceberg all
   carry it; a log file does not, which is why the shape that reads one is
   declared in code and published beside it.
5. **The consumer loads the contract and casts on receipt**, with
   `merge_schema=True` when it wants to keep columns it does not know about
   yet.
6. **Evolution is additive.** A new column is added nullable, at any depth
   (`Field.merge_with`, `IcebergDataset.add_fields`). Retyping or dropping is a
   migration and gets a new version of the contract, announced.
7. **CI pins the agreement.** The contract files are tested against the
   declarations they came from, so a column that exists in code and not in the
   contract fails the build rather than surprising a consumer.

What each side owes the other:

| the producer owes | the consumer owes |
| --- | --- |
| data cast onto the current contract | to read by name, never by column position |
| a new column added as nullable | to tolerate a column it does not know (`merge_schema=True`) |
| the contract file updated in the same change | to tolerate a nullable column being null |
| a version bump for anything not additive | to pin the contract version it was built against |

## How the pieces depend on each other

```text
   text/  ──┐                          fix/
   iceberg/ ─┴──▶ dataset ──▶ fields ──▶ convert ──▶ annotations
                                ▲                        ▲
                 the Arrow schema and every cast    type hints and
                 that lands data on it              the docstrings that
                                                    become descriptions
```

`text/` reaches sideways into `fix/` as well, and that edge is one **seam**
rather than a dependency on the protocol: `TextFile` holds a codec and calls
[five verbs](logs.md#a-second-codec) on it, so a second codec over another
protocol changes nothing above it.

Dependencies point one way. The one loop back is deliberate and lazy: a
`Field`'s `into_iceberg_*` imports the Iceberg projection at the point of use,
so the API stays on the class that owns the data without the core depending on
an optional extra.

## Where these rules are enforced

- **In the code**, by the refusals above.
- **In the tests**, which derive an expectation from the fixture and then pin
  it against a literal, so a broken regex cannot move both sides of an
  assertion together — and which cross every boundary the code branches on.
- **In the benchmarks**, which are re-run and quoted rather than remembered:
  every number on this site says which sweep produced it, and a claim that no
  longer matches its benchmark is a bug in the claim.
- **In `AGENTS.md`**, which is the same set of rules written for whoever
  changes this repository rather than for whoever uses it.

## Benchmarks

Nothing on this page has a runtime of its own — the rules are shapes, and what
they cost is measured on the page that owns each one:
[casting](types.md#benchmarks), [parsing a capture](logs.md#benchmarks),
[FIX](fix.md#benchmarks) and [Iceberg](iceberg.md#benchmarks). How those
numbers are produced, and how to read them, is on
[Benchmarks](benchmarks.md).
