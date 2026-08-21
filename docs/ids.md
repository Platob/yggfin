# Row ids

Every row this package produces carries one 64-bit integer that is both its
**identity** and its **order**: the millisecond it happened in the high bits,
a hash of what it says in the low bits, and the sign bit clear so every signed
consumer — Arrow, parquet, Iceberg, SQL — sorts it as the unsigned value it is.

```text
63       62                         21                        0
+--------+--------------------------+-------------------------+
| 0      | milliseconds since epoch | folded payload hash      |
| sign   | 42 bits                  | 21 bits                 |
+--------+--------------------------+-------------------------+
```

One integer, so it is the dedup key, the join key, the incremental-load
watermark and the sort column at the same time. Because time is in the high
bits, a range on it is a range on the clock, and Iceberg or parquet answer it
from per-file min/max statistics instead of reading rows.

## The bit budget

63 bits to spend. Every column is exact, and the starred row is the default.

| time unit | time bits | overflow (unix epoch) | overflow (epoch 2020-01-01) | hash bits | 50% collision in one tick |
| --- | --- | --- | --- | --- | --- |
| second | 32 | 2106-02-07 | 2156-02-07 | 31 | 54,562 rows |
| second | 33 | 2242-03-16 | 2292-03-15 | 30 | 38,581 rows |
| millisecond | 41 | 2039-09-07 | 2089-09-06 | 22 | 2,411 rows |
| **millisecond** ★ | **42** | **2109-05-15** | **2159-05-15** | **21** | **1,705 rows** |
| millisecond | 43 | 2248-09-26 | 2298-09-26 | 20 | 1,206 rows |
| millisecond | 45 | 3084-12-12 | 3134-12-13 | 18 | 603 rows |
| microsecond | 52 | 2112-09-17 | 2162-09-17 | 11 | 53 rows |
| microsecond | 53 | 2255-06-05 | 2305-06-05 | 10 | 38 rows |

The collision column is the birthday bound **within one tick** —
`1.1774 * sqrt(2**hash_bits)` — not per table. At the default 21 bits, 100 rows
in one millisecond expect 0.002 collisions, 1,000 expect 0.24 and 1,705 expect
0.69; it is [measured, not only quoted](#benchmarks). A source that bursts
harder than that wants microsecond time bits rather than more hash bits.

And a collision costs less than it looks: the ordering between two rows *in the
same millisecond*, and identity only where the payloads were equal anyway —
which is the case a dedup wants collapsed.

## Packing one

=== "From a row"

    ```python
    from rekep import ids

    ids.row_id(1_786_665_901_147, {"symbol": "XPAR", "size": 400, "venue": None})
    ids.row_id(datetime.datetime.now(tz=datetime.UTC), {"symbol": "XPAR"})
    ```

    A mapping is canonicalised, hashed and packed. A `datetime` is read as the
    instant it is (naive is UTC); milliseconds since the unix epoch work too.

=== "From bytes"

    ```python
    ids.row_id(1_786_665_901_147, b"2026-08-14 00:05:01.147_250 [t] [d] (INFO) hi")
    ```

    Bytes are hashed as they stand: a log line is already its own canonical
    form, and re-encoding it would only make it a different row.

=== "The halves"

    ```python
    packed = ids.pack(1_786_665_901_147, ids.hash_payload(b"a line"))
    ids.unpack(packed)              # (1786665901147, 137831) -- time, folded hash
    ids.pack(*ids.unpack(packed))   # the same id again
    ids.fold(ids.hash_payload(b"a line"))       # 21 bits of the whole 64
    ids.signed(ids.hash_payload(b"a line"))     # the digest an int64 column holds
    ```

=== "Another layout"

    ```python
    ids.pack(millis, digest, hash_bits=18)                    # more time, fewer rows per ms
    ids.pack(millis, digest, epoch_ms=ids.EPOCH_MS)           # count from 2020-01-01
    ids.unpack(packed, hash_bits=18, epoch_ms=ids.EPOCH_MS)   # read it back the same way
    ```

    Bits are spent from the epoch, so moving it forward buys years at the far
    end and refuses everything before it. Whatever a column was packed with,
    it has to be unpacked with — which is why `Log` writes the layout into the
    column's own metadata.

## Whole columns

```python
ids.pack_arrow(batch.column("recorded_at"), batch.column("hash64"))
ids.pack_arrow(nanos, digests, unit="ns")           # an int64 column names its unit
ids.unpack_arrow(batch.column("id"))                # (times, folded hashes)
```

Packing is `numpy.uint64` shifts over the whole column — no Python row loop,
and no float64 anywhere near it. Two details are the reason it is fast and
correct:

!!! note "A `timestamp("ms")` column reaches numpy without a copy"

    An Arrow timestamp is already an int64 of ticks, so viewing it as int64
    shares the buffer: nothing is converted per row, and the packing is one
    pass over memory the parser already produced. Any other unit is one
    integer division — floored, not truncated, so a pre-epoch tick lands
    inside its own millisecond rather than the one above it.

!!! warning "Every shift operand is `numpy.uint64`, deliberately"

    A shift whose other operand is a Python `int` promotes the array to
    float64 on numpy 1.x. float64 holds 53 bits, so the low bits of every id —
    the entire hash half — would be rounded away, and the ids would still look
    perfectly plausible.

## What makes an id reproducible

=== "The hash"

    ```python
    ids.hash_payload(b"a line")      # xxhash.xxh3_64_intdigest(payload, seed=ids.SEED)
    ids.SEED                         # 0x9E3779B185EBCA87, fixed in the module
    ```

    xxh3 is not an implementation detail that can be swapped: the digest *is*
    the low half of every id, so a different function — or the same one under
    a different seed — mints a different id for a row that is already stored,
    and a replay inserts it a second time. `xxhash` is therefore a hard
    dependency of this package, imported at module top, with no fallback and
    no optional guard. Python's own `hash()` is unusable here for the same
    reason: it is salted per process.

=== "The canonical bytes"

    ```python
    ids.canonical({"b": 1, "a": None})     # b'm2:s1:an0:s1:bi1:1'
    ids.hash_row({"a": None, "b": 1})      # the same row, whatever order it was built in
    ```

    Everything that is otherwise a choice is fixed, because two producers have
    to hash the same bytes:

    | | rule |
    | --- | --- |
    | field order | a mapping is written in **sorted key order**, never insertion order; a sequence keeps its own, because there position *is* the name |
    | encoding | text is UTF-8, integers are decimal ASCII (exact at any width), floats are 8 big-endian IEEE-754 bytes, times are microseconds since the unix epoch |
    | nulls | an explicit sentinel — `{"venue": None}` and `{"venue": ""}` are different rows and must not share an id |
    | framing | every value is `tag + length + ":" + body`, so `("ab", "c")` and `("a", "bc")` cannot produce the same bytes |
    | equal values | `-0.0` normalises to `0.0` and every NaN to one NaN, so rows that compare equal hash equal |

    Unicode is **not** normalised: composed and decomposed `"é"` are different
    rows. Normalise upstream if the source mixes them — doing it here would
    hide it.

=== "The refusals"

    ```python
    ids.pack(4_398_046_511_104, 0)
    # ValueError: 4398046511104 ms is outside the 42 time bits of a row id:
    #             1970-01-01 to 2109-05-15 (epoch_ms=0, hash_bits=21)
    ```

    A timestamp that does not fit is refused rather than wrapped, because a
    wrapped id is a *smaller* number and sorts before rows from years earlier.
    A null timestamp or a null hash is refused too: neither can be ordered or
    found again.

## In the pipeline

`Log.id` is this, packed per batch by the parser, and it is the only key
anything downstream needs.

=== "What the parser writes"

    ```python
    from rekep import TextFile, ids

    row = TextFile.from_path("app.txt").read_arrow_table().slice(0, 1).to_pylist()[0]
    row["id"] == ids.pack(row["recorded_at_unix"] // 1_000_000, row["hash64"])
    ```

    `hash64` stays beside it: the id holds 21 folded bits, which is enough to
    order and to dedup, and not enough to *prove* two lines from two captures
    are the same line.

=== "Dedup and merge"

    ```python
    logs.append_arrow(files.read_arrow_reader(), merge_by=True)   # merges on id
    logs.write_arrow(table, merge_by=True)                        # upserts on id
    ```

    `merge_by=True` reads the declared primary key, which is `["id"]`. The same
    line at the same millisecond has the same id in every capture that holds
    it, so replaying a rotated log inserts nothing.

=== "Watermark"

    ```python
    watermark = logs.read_arrow_table(columns=["id"]).column("id").to_pylist()
    latest = max(watermark)
    logs.read_arrow_table(row_filter=f"id > {latest}")   # only what arrived since
    ```

    One integer to carry between runs instead of a timestamp and a tiebreak,
    and the filter prunes: `scan_plan("id > …")` plans **zero files** when
    nothing is newer.

=== "Sort column"

    ```python
    logs = IcebergDataset(..., struct=Log.FIELD, sort_by=["id"])
    ```

    Sorting each commit on the id is sorting it on the clock, so per-file and
    per-row-group statistics bracket real time ranges and a time predicate
    skips what it cannot match — [what sorting buys](iceberg.md#reading-it-back).

=== "The layout travels with the column"

    ```python
    Log.FIELD.field("id").metadata
    # {'unit': 'millisecond', 'epoch': '1970-01-01', 'time_bits': '42',
    #  'hash_bits': '21', 'iceberg:primary_key': 'true'}
    ```

    A consumer that has the column can unpack it without reading this code,
    and the [contract](contracts.md) carries the same metadata into whatever
    reads it.

## Benchmarks

`benchmarks/bench_ids.py`. Packing is a column operation, hashing is per row by
construction, and canonicalising is the expensive half for anything that is not
already bytes — three different questions, so three sweeps. Every case asserts
its answer before it is timed. The method the whole site shares is on
[Benchmarks](benchmarks.md).

```bash
cd python
uv run python benchmarks/bench_ids.py            # every sweep
uv run python benchmarks/bench_ids.py --quick    # 100,000 rows, best of 3
```

### Packing a column

1,000,000 rows, best of five, both runs quoted.

| case | rows/s |
| --- | --- |
| `pack_arrow`, `timestamp[ms]` — the zero-copy path | 87.9M–91.9M |
| `pack_arrow`, `timestamp[ns]` — one integer divide | 67.4M–74.5M |
| `unpack_arrow` | 104M–140M |
| `fold_numpy` alone | 280M–363M |
| **`pack`, row by row in Python** | **1.42M–1.43M** |

The column path is about **sixty times** the row-by-row one, which is why the
parser packs per batch and not per line: at a million rows the ids cost 11–14
ms against 700 ms, on a parse that takes 2.7 seconds. Naming a unit other than
milliseconds costs the divide — 20% of a number that is already two orders of
magnitude above the parse — so an id is never the reason to convert a column.

### Hashing a row

1,000,000 log lines, 144.9 MiB, best of five.

| case | rows/s | MB/s |
| --- | --- | --- |
| **xxh3-64** (what an id uses) | 6.64M–6.91M | 962–1,000 |
| blake2b-64 (what it replaced) | 1.25M–1.31M | 180–189 |
| xxh3-64 + `signed()` | 3.89M–3.95M | 563–573 |

That ratio is the reason `xxhash` is a hard dependency rather than an extra:
the digest is the row's identity, so it cannot be swapped for a fallback, and
it is **five times** the hash it replaced. The `signed()` row is the honest
version of the same number — wrapping the digest into the int64 an Arrow column
holds costs about as much again as the hash itself, and the parser pays it once
per line.

### One row, end to end

200,000 rows, best of five.

| case | rows/s |
| --- | --- |
| `canonical(row)` alone | 81.0k–82.0k |
| `row_id(mapping)` — canonicalise, hash, pack | 75.1k–75.5k |
| **`row_id(bytes)` — a log line** | **1.05M–1.06M** |

Canonicalising is the whole cost: a six-field mapping is fourteen times slower
than the same id over bytes that are already canonical. That is why a log line
is hashed as it stands — it *is* its own canonical form — and why a producer
that mints ids per row should hand over bytes it already has rather than a
dict it builds for the purpose.

### Collisions

The birthday bound, measured rather than only quoted: every row in one
millisecond, and what is left after the fold.

| rows in one millisecond | distinct ids | collisions | expected |
| --- | --- | --- | --- |
| 100 | 100 | 0 | 0.00 |
| 1,000 | 999 | 1 | 0.24 |
| 1,705 | 1,704 | 1 | 0.69 |
| 10,000 | 9,967 | 33 | 23.84 |

Both runs produced these counts exactly. The measurement tracks the bound, and
the bound is what the [bit budget](#the-bit-budget) is chosen against: a source
that puts more than ~1,700 rows in a single millisecond is one that wants
microsecond time bits.
