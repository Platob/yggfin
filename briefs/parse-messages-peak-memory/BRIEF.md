# Task: cut peak memory of `rekep`'s log-parse → Iceberg-append path

> **Reconstruction note.** This file is transcribed from 16 screenshots of the
> original brief. Everything below is what was legible. Four things the text
> refers to were **not** on screen and are marked `[MISSING]` where they belong:
> hard constraints 1/2/3, the acceptance criteria, the report format, and G's
> carve-out sign-off. Re-attach them from the source document before starting —
> item G's whole gating depends on them.

Repo `/home/nfillot/work/tdl-data-record-keeping`, package `python/src/rekep`.
Target: the `parse_messages` task. Every number below is measured and
reproducible. Measure before and after each item; do not guess. One commit per
item.

**Prototype before you commit.** Several items below exist because a plausible
rule was written first and measured second, and the measurement contradicted it
— see the anchor post-mortem in item G. For any rule that inspects log content,
build the throwaway version, run it over the sample slices, print the counts,
and only then write the production code. A rule that has not been run over real
bytes is a hypothesis.

## Baseline (pinned — reproduce with `./bench.sh`, do not re-derive)

Three real gzipped FIX/application feeds, hourly Iceberg table `logs.messages`
(59 columns, `unixpartition` identity partition). `bench.sh` is the only place
capture filenames live (`LOGS` / `FEEDS` env). Nothing in this brief is coupled
to a feed, and **no rule you write may be either.**

| feed | shape | input | elapsed | read | written | skipped | peak RSS |
|---|---|---|---|---|---|---|---|
| **A** | FIX-dense, tag payloads | ~1.9 GB | 142.9 s | 1,087,654 | 1,087,616 | 38 | ~4.23 GiB |
| **B** | mixed | ~2.7 GB | 247.2 s | 1,884,311 | 1,792,697 | 91,614 | ~7.25 GiB |
| **C** | trace-dense, named payloads | ~34 GB | *excluded* | — | — | — | *would exceed 80 GiB* |

A and B are the benched pair. **Feed C is deliberately excluded from the
harness**: at the baseline's ~2.5x-input peak-RSS ratio an unbounded run peaks
past 80 GiB. Once you hit the acceptance criteria a successful feed-C run is the
real end-to-end proof — attempt it only then, watch RSS live, and stop it past
~60 GiB.

`skipped = read - written` by construction (`rekep/logs.py:156`) — merge-by-key
dedup count, not a filter. It is a correctness invariant.

**Headline: 3788 B/row in memory vs 123 B/row on disk — 31x inflation.** Parquet
already dictionary-encodes + ZSTDs every column (verified `RLE_DICTIONARY`,
`dict=True` on `body`, `entries.*`, `plugin`, `level`, `sourceurl`,
`threadname`). **The stored format is already efficient; the waste is entirely
in-memory.** So in-memory type changes should leave on-disk bytes ~unchanged —
that is both the point and the safety property the harness checks.

### Where the 3788 B/row goes (largest parquet file, 71,132 rows, 43.4 entries/row)

| item | B/row | % | fact |
|---|---:|---:|---|
| `entries` `list<struct<tag,key,value,comp>>` | 1923 | 50.8% | 39.5 entries/row table-wide |
| ↳ `value` `string` | 881 | 23.3% | 47,004 distinct of 113,835,016 entries |
| ↳ `key` `string` | 684 | 18.1% | **896 distinct, fully derivable from `tag`** |
| ↳ `comp` `string` | 179 | 4.7% | **non-null in 0 of 113,835,016 entries** |
| ↳ `tag` `int32` | 174 | 4.6% | 324 distinct |
| `body` `binary` | 1415 | 37.3% | raw message; `entries` is its parsed form |
| `sourceurl` `string` | 69 | 1.8% | **1 distinct value per file** |
| all-null columns | 111 | 2.9% | `prevhash` `fixed_size_binary[16]` = 16.1 B/row at 100% null |

Table-wide average is 39.5 entries/row, so whole-table `entries` cost is ~9%
below this column.

**Validated by rebuilding in memory:** dictionary-encoding the `entries`
children + low-cardinality top-level columns + `tag`→`uint16` gives
**3788 → 2495 B/row (34%)**; `entries` alone **1923 → 758 (2.5x)**. Narrowing
index widths should beat that.

## Two structural findings that matter more than per-row size

### 1. `batch_byte_size` means two different things in the same file

Config: `batch_byte_size: 67108864` (64 MiB), `batch_row_size: 65536`. At
3788 B/row a real batch is **237 MiB — 3.7x the configured budget**. One name,
two units:

- `text/text_file.py:657` `elif held_bytes > batch_byte_size and len(rows) > 1:`
  — `held_bytes` counts **raw input bytes** in the Python row list.
- `text/text_file.py:1315, 1321, 1328, 1336` — same parameter compared against
  Arrow `nbytes` in `_windowed_batches`.

They differ ~3.5x, so the cut happens on raw bytes and the batch is measured in
Arrow bytes. Consequences: the one knob an operator reaches for does not bound
the thing that costs memory; and `_windowed_batches` degenerates to a
**pass-through** (a batch cut at 64 MiB raw already exceeds 64 MiB Arrow on
arrival, so `held_bytes >= batch_byte_size` is true on the first run and
`_one()` / `concat_arrays` at `:1376` never coalesces). Harmless today, but it
starts copying the moment per-row cost drops from item A — re-check after A.

Fix: make the budget measure produced Arrow bytes, or split into two
honestly-named parameters.

### 2. Commit-time copy amplification: ~4x over the buffered-batch budget

`arrow_chunks` (`rekep/dataset.py:427`) accumulates `commit_batch_num: 8`
batches into one `pyarrow.Table` — 8 × 237 MiB ≈ **1.85 GiB per chunk**,
matching the observed sawtooth plateau. Its docstring concedes it: *"With both
bounds absent the whole stream is one chunk, which is the atomic write and the
one that costs the most memory."*

But peak RSS was 7.25-7.6 GiB and RSS jumped 4.3 → 7.6 GiB **in the last 6 s**
of the 247 s feed-B run — a copy at commit, not steady-state buffering. The
empty-table branch of `_insert_arrow_table` (`iceberg/dataset.py` ~1332-1345):

```python
runs = list(_partition_runs(_grouped_partition_chunk(chunk, partitions), partitions))
additions = [first_rows(normalised_keys(run, join), join) for run in runs]
fresh = pyarrow.concat_tables(additions, promote_options="none")
```

`chunk` + `additions` + `fresh` live simultaneously ≈ **3x chunk ≈ 5.5 GiB**,
plus the reader's in-flight batch. That is the peak. `_grouped_partition_chunk`
(`iceberg/dataset.py:4400`) also ends in `chunk.take(indices)` — another full
copy when partitions are not already sorted.

The writer produced 78 parquet files across 24 partitions with a **single 164 MB
row group each**, so per-partition writers each hold a full row group too.

## Order of work

Letters are stable report identifiers, **not** a schedule. Do them in this order:

1. **B5** — one-liner, independent of everything.
2. **H** — the shape everything else lands on. It **deletes B3 and B4**;
   implementing those first is wasted work. Read H before A.
3. **A** — biggest retained-footprint lever.
4. **B2**, **F**, then B, C, D, E, E2.
5. **I1** — immediately after H, while you still remember what H replaced. I1 is
   part of finishing H, not a follow-up; I2 is a 72-line deletion you can do any
   time.
6. **A2 only if A landed and there is budget left.** It buys ~49 B/row beyond A
   for a schema evolution and ~50 test updates, and it has a named blocker.
   Stopping after A is a legitimate outcome; half-landing A2 is not.
7. **G last.** It is the largest single lever but the only item that changes what
   is stored, so it must not contaminate measurements against the pinned
   baseline. Land it behind default-off flags. If out of time, G alone on feed C
   beats A–F combined — but it needs the sign-off in its carve-out and the others
   do not.

## Validation preamble

`./bench.sh baseline`, then `./bench.sh <label>` per item, then
`./compare.py baseline <label>`. It samples `/proc/<pid>/status` for peak RSS,
records the JSON task result, and reports per-column in-memory bytes/row from the
written parquet. See `README.md`.

Per-item checks below tell you *which* change won and catch semantic drift. Point
them at `/tmp/rekep_bench_<label>/warehouse`; run under
`uv run --project python --group runner python`.

**Work per file, never per warehouse.** The two-file run writes 78 parquet files
(275 MB on disk); loading them as one Arrow table materialises **~10.9 GiB** and
gets OOM-killed here — the very inflation you are fixing. Shared helpers used by
the snippets below:

```python
import glob, os, pyarrow, pyarrow.compute as pc, pyarrow.parquet as pq

def files_of(warehouse):
    """Parquet files the run wrote, largest first. Largest is representative."""
    return sorted(glob.glob(f"{warehouse}/**/*.parquet", recursive=True),
                  key=os.path.getsize, reverse=True)

def rss():
    with open("/proc/self/status") as handle:
        for line in handle:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    return 0
```

Sample slices for the content rules in item G live at `/tmp/<feed>_head.txt` and
`/tmp/<feed>_mid.txt` — 200k header-matched lines each, ~200-450 MB, seconds to
scan. Rebuild them with `head -n 200000` and `sed -n '4000000,4200000p'` over a
decompressed feed if they are gone.

---
## H. Split `parse_messages` into a vectorized phase and a row phase

**Read this first.** A–G shrink or skip what the current shape produces; H
changes the shape and makes the others small.

Today one vectorized pass does header extraction, entry tokenisation, protocol
classification, XML and referential re-parsing, and identity assignment, so every
stage sees a full-width column even when it applies to 0.28% of rows. B3's
redundant scan, B4's two unconditional `if_else` copies and E2's 6.95x transient
are three symptoms of one cause: per-row logic done with whole-column kernels.
The enrichment that matters is **sequence-dependent** (dedup against previous,
carry a field forward, number versions). No Arrow kernel expresses that.

**The shape already exists on the market side — port it, don't invent one:**

| anchor | role |
|---|---|
| `market/event.py:316-322` `from_arrow_reader(source) -> Iterator[Self]` | reader in, row objects out, lazily |
| `market/event.py:324-343` `into_arrow_reader(events, batch_row_size=65_536)` | rows in, bounded batches out |
| `market/event.py:433-455` `with_previous(previous) -> Self \| None` | returns `None` at `:449` when the version changes no stored fact — item G's "skip if same as previous", already written and tested |
| `market/fix.py:737` `FixEvents.__iter__`, `:1157-1163` `_finish` | the driver: one input row, zero or more output rows |

### Phase 1 — vectorized, returns a reader

`text/text_file.py` keeps the per-line `HEADER_PATTERN` match (a regex over a
variable-length line stream is the one Python loop that cannot be vectorized
away, and it is already the cheapest place to stand) and gains item G's strip
regexes. It emits **only** what the header says: `unix`, `threadname`, `plugin`,
`level`, `body`, `sourceurl`. No entries, no protocol, no XML re-parse, no
identity. Return an `OwnedRecordBatchReader` (`arrow_reader.py:53`) — the class
exists, `text_file.py:492-514` already builds one, and AGENTS.md requires
*"Primary APIs use `RecordBatchReader`"*.

Measured: those flat columns are **1,583 B/row of the batch's 3,916**. Everything
phase 1 does not produce is 2,333 B/row (60%) that never enters the row phase.

### Phase 2 — two methods on `Message`, exactly as named

```python
@classmethod
def enrich_messages(cls, rows: Iterable[Message]) -> Iterator[Message]:
    """Enrich already-sorted rows against the one before, dropping unchanged ones."""

@classmethod
def enrich_batches(
    cls,
    source: pyarrow.RecordBatchReader,
    batch_byte_size: int = 67_108_864,
) -> Iterator[pyarrow.RecordBatch]:
    """Drive `enrich_messages` over a reader, re-batching on accumulated bytes."""
```

`enrich_messages` holds **one** `previous` and nothing else: parse this row's
entries from its stripped body, classify protocol, carry forward what the line
omitted, compare against `previous`, yield or skip. One row's state, not one
batch's. `enrich_batches` walks the reader batch by batch, turns each into row
objects, feeds them through `enrich_messages`, accumulates output rows to the
byte bound, emits.

### The missing primitive: batch → iter\<scalar\>

`fields/rows.py` says *"rows in, columns out"* and only goes that way:
`struct_array(declared, rows)` at `:33`, reached from
`StructField.into_arrow_array` (`fields/field.py:1316`) and installed on every
`@scalar` class as `into_arrow_array` / `into_arrow_batch`
(`fields/field.py:1620-1621`). There is no inverse. Add it in `fields/rows.py`,
installed by `@scalar` the same way, named `from_arrow_batch` per the house
`from_*` builds / `into_*` converts rule.

**It must not go through `to_pylist()`.** `Event.from_arrow_reader` does
(`market/event.py:322`: `cls.from_dict(row) for row in batch.to_pylist()`) and
that is the single most expensive line in either direction. Largest baseline
file, 16,384 rows, one case per process:

| walk | held | µs/row |
|---|---:|---:|
| whole batch `to_pylist()` — what `from_arrow_reader` does now | **18,286 B/row** | ~28 |
| flat columns only `to_pylist()`, `entries` left in Arrow | 2,224 B/row | ~2.9 |
| per-row `as_py()` off combined columns, one row held | **46 B/row** | ~4.9 |

`to_pylist()` costs **4.7x the Arrow footprint** it reads (3,916 → 18,286 B/row)
= **1.14 GiB of Python dicts** for one 65,536-row batch. The per-row walk holds
398x less for 1.5x the CPU. Take the CPU. Two requirements follow:
`combine_chunks()` the columns once before indexing (indexing a `ChunkedArray`
per row is O(chunks)); and read only the members the enricher needs.

**Check — the row walk must not be a dict walk.** This decides
`from_arrow_batch`'s implementation. It is the one place where running cases in
one process gives the wrong answer: RSS after a large free stays high (item B5),
so an earlier case masks a later one. **One case per process:**

```python
import sys, time, pyarrow.parquet as pq

FLAT = ["unix", "vhash", "threadname", "plugin", "body", "code", "msgtype", "protocol"]
ROWS = 16384

largest = files_of("/tmp/rekep_full/warehouse")[0]
whole = pq.ParquetFile(largest).read().slice(0, ROWS).combine_chunks()
flat = [name for name in FLAT if name in whole.schema.names]
part = whole.select(flat).combine_chunks()

case, base, start = sys.argv[1], rss(), time.perf_counter()
if case == "whole":
    held = whole.to_pylist()
elif case == "flat":                              # what from_arrow_reader does
    held = part.to_pylist()                       # entries never leaves Arrow
elif case == "scan":                              # what from_arrow_batch must do
    columns = [part.column(name).combine_chunks() for name in flat]
    body = flat.index("body")
    held = 0
    for index in range(ROWS):                     # every member, one row held
        row = tuple(column[index].as_py() for column in columns)
        held += len(row[body] or b"")
print(f"{case:<6} {(rss() - base) / ROWS:8,.0f} B/row held"
      f"   {(time.perf_counter() - start) / ROWS * 1e6:6.1f} us/row")
```

Reproduce: `whole` 18,286 B/row @ ~28 µs, `flat` 2,224 @ ~2.9, `scan` 46 @ ~4.9.
Held bytes are stable to the byte; timings vary a few percent. `scan` must build
the **whole row tuple** — reading only `body` is 27 B/row at 1.0 µs and flatters
the design. Your `from_arrow_batch` must land near `scan`. Near `whole` means it
is building dicts — most likely `to_pylist`, `as_py()` on a whole column, or
`from_dict`.

### Re-batch on bytes, not rows — this is the resampling

`nbytes` appears in **exactly one place in the whole package**,
`text_file.py:1315-1336`. Every other bound counts rows or batches:
`into_arrow_reader`'s `batch_row_size` (`market/event.py:326`) and
`arrow_chunks`'s `row_size` / `batch_num` (`rekep/dataset.py:427`). A row count
cannot bound memory here:

- Across the 78 baseline files, bytes/row spans **540 to 4,318 — 8.0x** at equal
  row count. A 65,536-row batch is anywhere from **34 MiB to 270 MiB**.
- Within one file, consecutive 512-row windows span **840 KB to 2.67 MB — 3.2x**.

`enrich_batches` cannot build the array to decide whether to emit it, so
accumulate an estimate: `len(body) + 8 × len(entries)` predicts real Arrow bytes
with one calibration constant to within **±16.3% worst case** (mean ratio 2.17,
stdev 0.10 over 32 windows) against 8.0x for a row count. Running sum, emit when
it crosses `batch_byte_size`, reset. Name it `batch_byte_size` — AGENTS.md
requires size parameters to state their unit and `text_file.py` already uses that
name for this.

**Check — the estimator tracks real Arrow bytes.** Note `combine_chunks()` per
window: a bare `slice()` shares parent buffers and `nbytes` reports the parent.

```python
import statistics

predicted, actual = [], []
for start in range(0, whole.num_rows, 512):
    window = whole.slice(start, 512).combine_chunks()
    actual.append(window.nbytes)
    predicted.append(
        pc.sum(pc.binary_length(window.column("body").combine_chunks())).as_py()
        + 8 * pc.sum(pc.list_value_length(window.column("entries").combine_chunks())).as_py()
    )
ratios = [a / p for a, p in zip(actual, predicted)]
mean = statistics.mean(ratios)
print(f"actual/predicted  mean {mean:.2f}  stdev {statistics.stdev(ratios):.2f}"
      f"   worst error {max(max(ratios) / mean - 1, 1 - min(ratios) / mean):.1%}")
print(f"actual per 512-row window: {min(actual):,} .. {max(actual):,} B")
```

Baseline: mean 2.17, stdev 0.10, worst error 16.3%, windows
840,720 .. 2,669,892 B. Calibrate the constant on your own run; assert worst
error stays under 25%.

**While you are there, fix `arrow_chunks` (`rekep/dataset.py:427-470`):** it
slices input batches and holds the slices in a list until the chunk closes. An
Arrow slice is zero-copy and **keeps its parent's whole buffer alive**, so one
512-row slice of a 270 MiB batch retains 270 MiB. That is a large part of item F.

### Sortedness is a contract — assert it

`enrich_messages` is only correct on time-ordered input. Phase 1 emits in file
order, which for these logs is time order, and AGENTS.md promises *"File sets
open one naturally sorted path at a time"* — but a future multi-file or
partition-parallel read would interleave and `with_previous` would silently
compare unrelated rows, producing wrong `prevhash`, wrong version numbers, wrong
dedup. One kernel per batch, carrying the last value across the boundary:

```python
previous_last = None
for batch in reader:
    unix = batch.column("unix").combine_chunks()
    ordered = pc.all(pc.greater_equal(unix[1:], unix[:-1]), min_count=0).as_py()
    if previous_last is not None:
        ordered = ordered and unix[0].as_py() >= previous_last
    assert ordered, "enrich_messages needs time-ordered input"
    previous_last = unix[-1].as_py()
```

Run it over the full two-file baseline read before relying on it. A failure means
phase 1's ordering guarantee is weaker than AGENTS.md claims — that is a finding
to report, not a check to relax.

**Check — the restructure changed nothing observable.** With G's flags off, per
file, full row set:

```python
for name in sorted(set(base_table.schema.names) & set(new_table.schema.names)):
    assert base_table.column(name).equals(new_table.column(name)), f"{name} changed"
assert base_table.schema.names == new_table.schema.names, "column set or order changed"
```

`equals` on a ChunkedArray compares values and ignores chunking — verified:
`chunked_array([[1,2,3,4,5,6]]).equals(chunked_array([[1,2],[3,4,5],[6]]))` is
`True`, one differing value makes it `False`. So a different batch boundary
passes and a different value does not, exactly the distinction this item needs.
`base_table.equals(new_table)` holds across chunking too; the loop is here only
to name *which* column moved. Per file against `results/baseline/`, never the
whole warehouse (that load is the 10.9 GiB OOM).

### What H absorbs

Do A, B2, B5, C, D, G on top of this shape. **B3 and B4 disappear** — no
`null_values` full-column scan and no unconditional `if_else` merge once the
branch is a Python `if` on one row. Delete them from your plan, and delete them
from the source: **item I1 is the other half of this and is not optional.** E2's
6.95x transient becomes whatever one row costs. E and F stay; F gets easier
because phase 2 owns the batch boundary.

**House rule this brushes against.** AGENTS.md: *"Never use a Python row loop for
an Arrow shape conversion. Column comprehensions are fine."* That rule is about
**shape conversion** — casting, restructuring, projecting — and it still holds:
phase 1 is vectorized, `from_arrow_batch` reads member-by-member off declared
types with nothing inferred per row, and `struct_array` already builds columns
with one list per member. Sequence-dependent enrichment is not a shape conversion
and has no kernel. If you agree after reading it, add one sentence to that
AGENTS.md bullet saying so, rather than leaving the next agent to guess whether
`enrich_messages` violates house style.

## A. Narrow the `entries` element types — biggest retained lever (~1200 B/row)

`ENTRIES` at `rekep/entries.py:311`:

```python
ENTRIES: pyarrow.DataType = pyarrow.list_(
    pyarrow.field("item", Entry.into_field().dtype, nullable=False)
)
```

`Entry` scalar at `rekep/entries.py:36-54` (`tag: int` via
`TAG = pyarrow.int32()` at line 15, `key: str`, `value: str`,
`comp: str | None`). Use the **narrowest dictionary index that fits**:

| field | now | proposed | B/row |
|---|---|---|---:|
| `tag` | `int32` | `uint16` | 174 → 87 |
| `key` | `string`, 896 distinct | `dictionary(int16, string)` | 684 → ~87 |
| `value` | `string`, 47k distinct | `dictionary(int32, string)` | 881 → 454 |
| `comp` | `string`, 100% null | `dictionary(int8, string)` — **or delete it, see A2** | 179 → ~49 |

Exploit rather than fight: the parse stage **already folds keys to a dictionary
and throws it away**. See `text/message.py:133-138` — *"a key column is read
through its distinct spellings, so one `take` off the folded dictionary gives
every entry the code of the field it spells"*. Storing the folded form is the
cheap path, not an extra one.

Caveats to handle, not ignore:

- `comp` is populated only by the referential/XML paths
  (`text/entries.py:435-470`, `:616-624`); always null for FIX logs. An all-null
  `string` still pays ~4.1 B/element, `int8`-indexed ~1.1 B. Confirm `int8` fits
  the referential paths' distinct-`comp` count, else `int16`. Deleting it
  outright is worth another ~49 B/row — **item A2, and read it before you try.**
- `uint16` for `tag`: standard FIX tags fit but custom tags can exceed 65535. The
  regex at `rekep/entries.py:16` is `^[0-9]{1,9}$` — up to 9 digits. Do **not**
  silently truncate: widen to `uint32` (saves nothing vs int32) or reject
  out-of-range tags with a clear error. **Prefer correctness over the 87 B/row.**
- `ENTRY_PARTS` (`rekep/entries.py:314`) derives from the type, and consumers use
  `compute.struct_field(..., "comp")` (`text/fixmsg_arrow.py:45,166`;
  `text/entries.py:306`). Dictionary-typed struct children change what kernels
  accept — expect decode/encode at kernel boundaries and check every
  `struct_field` consumer.

**Check (also covers C, D) — the representation actually narrowed.**

```python
def entries_footprint(path):
    tbl = pq.ParquetFile(path).read(columns=["entries"])
    col = tbl.column("entries").combine_chunks()
    flat = pc.list_flatten(col)
    for child in ("tag", "key", "value", "comp"):
        arr = pc.struct_field(flat, child)
        print(f"{child:6} {str(arr.type):28} {arr.nbytes / tbl.num_rows:8.1f} B/row")
    print(f"entries total {col.nbytes / tbl.num_rows:.1f} B/row over {tbl.num_rows} rows")
```

Baseline prints, on the largest file: `tag int32 173.7`, `key string 684.3`,
`value string 881.5`, `comp string 179.2`, total `1922.6 B/row over 71132 rows`.
These must move toward the table above and nothing else should. **If a child's
type changed but its bytes/row did not, the dictionary is being decoded before it
reaches the writer** — find that site; it is the whole failure mode for item A.

**Check — no value was mangled.** Dictionary encoding must be a pure
representation change. Per file, only on a `BENCH_LIMIT`-bounded run (this
materialises Python lists of every entry):

```python
def entries_digest(path):
    flat = pc.list_flatten(pq.ParquetFile(path).read(columns=["entries"])
                           .column("entries").combine_chunks())
    return {c: pc.struct_field(flat, c)
             .cast(pyarrow.int64() if c == "tag" else pyarrow.string()).to_pylist()
            for c in ("tag", "key", "value", "comp")}

base, new = entries_digest(BASE_FILE), entries_digest(NEW_FILE)
for c in base:
    if base[c] != new[c]:
        i = next(i for i, (a, b) in enumerate(zip(base[c], new[c])) if a != b)
        raise AssertionError(f"{c} differs at {i}: {base[c][i]!r} != {new[c][i]!r}")
```

The `.cast()` makes it representation-independent: `dictionary(int16, string)`
and plain `string` holding the same values compare equal. This asserts *content*;
`entries_footprint` asserts *layout*. Both must pass.

## A2. Remove `comp` from the `Entry` struct — worth 179 B/row, with one real blocker

**The measurement.** `comp` is `string`, nullable, field index 3 of the entry
struct (`entries.py:54`). On these feeds it is **100% null: 3,089,398 of
3,089,398 entries, zero distinct non-null values**, and it still costs
**179.2 B/row — 9.3% of `entries`, 4.7% of the whole row**, because an all-null
Arrow string array still pays its offsets buffer. Deleting it takes `entries`
from 1922.6 to 1743.5 B/row.

**Read this before you start: the incremental win over item A is small and the
blast radius is not.** Item A's `dictionary(int8, string)` already recovers ~73%
of those bytes at near-zero risk. Full removal buys **~49 B/row more** (~2.5% of
`entries`) and costs a schema evolution, two regenerated contract YAMLs, and ~50
test functions. **Do item A first, measure, and treat A2 as a separate change.**
If the elapsed or risk budget is tight, stopping at the dictionary is a
legitimate outcome — say so in the report rather than half-landing this.

### The premise as usually stated is wrong — the registry cannot supply `comp`

The natural theory is "downstream FIX parsing can rebuild component context from
the field registry plus key parsing." Audited, and it does not hold:

- The registry (`fix/registry.py` — `component()`, `components()`,
  `group_count_tags()`, `repeating_groups()`) declares *which* fields sit in which
  component and which tags open a group. It holds **no occurrence index**. `[0]`
  vs `[1]` is per-message runtime data with no registry representation, and
  nothing in `registry.py` is keyed by component path.
- Worse, the registry is never *asked* about a component path today:
  `fix/access.py:196-201` and `_KEY_TAIL` (`access.py:496-501`) resolve on the
  terminal name with lead and index **stripped**.
- And the writers **move** the prefix out of `key` rather than copying it
  (`structure_arrow` `entries.py:275-308`, `_key_parts` `:317-322`). Measured:

| input spelling | stored `key` | stored `comp` |
|---|---|---|
| `NoPartyIDs[0].PartyID` | `PartyID` | `NoPartyIDs[0]` |
| `Instrument.NoSecurityAltID[0].SecurityAltID` | `SecurityAltID` | `Instrument.NoSecurityAltID[0]` |
| `Strategies[0].NoLegs[0].600` | `600` | `Strategies[0].NoLegs[0]` |
| `Instrument.Symbol` | `Instrument.Symbol` | *null* |

So dropping the column as-is **destroys information that exists nowhere else**:
the group name, its occurrence index, and the full ancestor chain. That is the
occurrence identity `market/event.py:1121` groups on, the
`event[i].action[j].order[k]` tree `fix/oms.py:19-32` reconstructs, the
`TickRules[i]` ladder order `market/instrument.py:1211` rebuilds, and the
scoped-vs-root disambiguation at `fix/components.py:343-347`. **Do not delete the
column and leave the writers alone.**

### The version that does work: keep the whole spelling in `key`, derive `comp` on read

Store the full spelling — `NoPartyIDs[0].PartyID`, not `PartyID` — and derive the
split at the read boundary. This is **fully derivable by regex, no registry
involved**, because `_GROUPED_KEY` (`entries.py:18`) is a pure syntactic split:

```
(?s)^(?:(?P<comp>.*\[[0-9]+\])\.(?P<key>[^.]+)|(?P<plain>.*))$
```

Feed it `Entry.spelling` (`entries.py:149-158`) and it reproduces `comp` exactly
for every `...[N]`-suffixed lead. Tag derivation survives too, because
`_key_parts` rpartitions *before* calling `_terminal_tag`
(`Strategies[0].NoLegs[0].600` → tag 600). The read view is already
comp-agnostic: `_view()` (`:164`) concatenates the two halves back together
before matching, so `name`, `index`, `lead`, `entry_lead`, `folded` all keep
working off the merged spelling. `fix/components.py:230` (`_INDEXED_COMPONENT`)
is the second-stage splitter and is unchanged.

Note the byte accounting shifts rather than vanishing: `key` gets longer. Under
item A's `dictionary(int16, string)` that is nearly free — the distinct spellings
are the dictionary, and 896 distinct keys becoming a few thousand still fits
`int16`. **Measure `key`'s bytes/row after, not just `comp`'s.** If `key` grows
by more than the 179 B you removed, A2 is a loss; report that.

### The blocker: `comp="Referential"` has no index and does not survive the round trip

`_REFERENTIAL_COMP = "Referential"` (`text/entries.py:88`) is written at seven
sites (`text/entries.py:435,437,441,459,467,470,556`) with **no `[N]`**.
`ENTRY_LEAD` (`entries.py:27`) is `\[[0-9]+\]$`, so:

```
Entry(key="InstrumentKey", value="X", comp="Referential").spelling == "Referential.InstrumentKey"
_key_parts("Referential.InstrumentKey") == (0, "Referential.InstrumentKey", None)   # comp lost
```

A merged `Referential.InstrumentKey` is **indistinguishable from a genuine dotted
proprietary key** such as `TECH.CLIENTID`. That is not cosmetic:
`fix/transcribe.py:781-783` documents in-line that `TECH.CLIENTID` must not
resolve as `CLIENTID`, so collapsing the two forms silently changes registry
resolution for every referential entry.

Resolve it explicitly before touching anything else. Options, in order of
preference:

1. Give the referential prefix an index — write `Referential[0]` at those seven
   sites. One-line change each, makes it syntactically indistinguishable from any
   other indexed lead, and `_GROUPED_KEY` recovers it. Costs a stored-form change
   for referential rows, so it needs its own before/after comparison of the
   referential test fixtures.
2. Keep a marker the split can recognise that a proprietary key cannot produce.
3. Decide referential rows may lose the prefix — **only** with sign-off from
   whoever consumes them, and only after checking
   `text/fixmsg.py:939,1315,1329` and `market/transacted.py:520-528`, which gate
   behaviour on `comp` being non-null.

Do not pick (3) by default because it is the least code.

### The four struct literals that break outright

Positional 4-element `from_arrays` calls — these fail the moment the type is 3
members wide:

- `text/entries.py:301-309`
- `text/fixmsg_arrow.py:160-168`
- `fix/transcribe.py:1176-1179`
- `text/entries.py:913-915` (implicitly 4-wide)

These adapt on their own, because they are name- or arity-driven:
`fix/transcribe.py:2137-2143` (`zip(ENTRY_PARTS, parts, strict=True)` — note
`strict=True` couples `structure_arrow`'s arity to `ENTRY_PARTS`' length, so they
must change together), `fix/components.py:995-1002`, `fix/oms.py:571-575`.
`ENTRY_PARTS` (`entries.py:314`) is derived from `ENTRIES.value_type`, not
hand-written, so it shrinks to a 3-tuple by itself — which is why `strict=True`
will catch a half-done change for you.

### Two failures that are silent — check these by hand

Everything else raises. These two do not:

- `market/instrument.py:1137` — `{"key","value","comp"}.issubset(source.type.value_type.names)`
  is a structural duck-type probe. Remove `comp` and it returns `False` for every
  entries column, and the function **silently yields nothing**.
- `text/fixmsg.py:3352-3354` — `entry.get("comp")` on a `Mapping`, so it returns
  `None` instead of raising. Same shape in the test helper at
  `tests/text/test_messages.py:1188`, which feeds `_pairs` and `_keys` and
  therefore any test using them.

### Scope, so you can decide before starting

- **~40 read sites** across `fix/transcribe.py`, `fix/components.py`,
  `fix/oms.py`, `fix/message.py`, `text/fixmsg.py`, `text/fixmsg_arrow.py`,
  `market/event.py`, `market/fix_arrow.py`, `market/instrument.py`,
  `market/transacted.py`.
- **~50 test functions.** Six pin the struct shape directly and must be updated
  first, as they tell you whether the change is coherent:
  `tests/fix/test_transcribe.py:924` (`ENTRY_PARTS == (...)`),
  `tests/text/test_fixmsg.py:378-379` (nullability and dtype),
  `tests/text/test_message.py:100` (field-name list), `tests/test_cli.py:207`
  (pinned printed schema string), `tests/test_schemas.py:51`,
  `tests/test_docs.py:443`.
- **Two committed contract YAMLs must be regenerated** in the same change:
  `schemas/rekep/message.yaml:302-305`, `schemas/rekep/fixmsg.yaml:300-303`
  **and `:329-332`** — comp appears twice in fixmsg. They are generated
  (`rekep fields dump --pyclass rekep.text.message:Message`,
  `schemas/README.md:75-76`) and `tests/test_schemas.py:51` enforces agreement.
- **Executable docs**: `docs/fix/index.md:95` and `docs/products/message.md:71`
  print `entry.comp`, and `tests/test_docs.py:443` runs every python fence under
  `docs/` and diffs stdout against the following fence. Also update the prose at
  `docs/products/message.md:80-84` and the member contract table at
  `docs/fix/fixmsg.md:129-136`.
- **Committed registry data**: `data/fix/fields/000030.json:326` and the same
  member inside `fix/registry.zip → fields/000030.json` — the `Unmap`
  pseudo-field, tag 30021 (`fix/rekep.py:220`, `:248`).
- **Iceberg field ids renumber.** No `field_id` is pinned in `schemas/`
  (`grep -c field_id` is 0 for all six files, despite `docs/contracts/index.md:40`
  claiming ids are stored); they are assigned at runtime by fresh numbering
  (`iceberg/fields.py:37-63`). So removing member 3 renumbers **every field id
  after it** in any table carrying `ENTRIES`. There is no migration code and every
  contract is version 1 with no migration path (`docs/contracts/index.md:97-107`).
  Against an existing table this is a real schema evolution — do it in a scratch
  catalog, never against a shared warehouse.

**Check — the derivation is lossless before you delete anything.** Run this
*first*, on the current schema, over real rows. It is the whole decision:

```python
from rekep.entries import Entry, _key_parts

losses = 0
for entry in entries:                        # Entry instances from a parsed batch
    if entry.comp is None:
        continue
    tag, key, comp = _key_parts(entry.spelling)
    if comp != entry.comp or key != entry.key:
        losses += 1
        if losses < 10:
            print(f"{entry.spelling!r}: comp {entry.comp!r} -> {comp!r}, key {entry.key!r} -> {key!r}")
print(f"{losses} entries whose comp cannot be recovered from their spelling")
```

Zero is the only acceptable result, and you will not get zero until the
referential prefix carries an index. **Every non-zero row is data the removal
would destroy.** Run it across FIX, XML and referential fixtures, not just a FIX
capture — on a FIX capture `comp` is 100% null and this check passes vacuously,
which is exactly the trap.

## B. `sourceurl` — 69 B/row for one distinct value per file

Built at `text/text_file.py:781` via `_constant_column(count, self.url)` (helper
at `text/text_file.py:1635`), declared plain `str` at `text/message.py:171`.
Ideal dictionary case: one entry, N indices → 69 → 4 B/row. Read
`_constant_column`'s docstring first — it documents a deliberate `take`-vs-`repeat`
tradeoff with measured timings and notes *"these bytes are written to a store"*.
Keep that reasoning intact.

## B2. The UTF-8 fallback cliff — cheap fix, confirmed on real data

`text/message.py:550-560`:

```python
try:
    return pyarrow.compute.fill_null(binary.cast(pyarrow.string()), "")
except pyarrow.ArrowInvalid:
    return pyarrow.array(
        ["" if value is None else value.decode("utf-8", "replace")
         for value in binary.to_pylist()],
        pyarrow.string(),
    )
```

**A single invalid-UTF-8 body anywhere in a batch sends the *entire batch*
through `to_pylist()`** — a full Python `bytes` plus a full `str` per row, plus
list overhead, none of it visible to the Arrow pool.

Measured: **537 rows of 2,880,313 have invalid UTF-8 bodies (0.0186%)**,
poisoning **9 of 78 batches (11.5%)** through the Iceberg scan reader or
**10 of 101 (9.9%)** through parquet `iter_batches` — the same 537 rows either
way, ~10-12% of batches wherever the boundaries fall. `measure.py` reports
`UTF8_FALLBACK_BATCHES` / `UTF8_BAD_ROWS`. At ~1415 B/row of `body` an affected
batch materialises well over 100 MiB of transient Python heap. Very likely the
mechanism behind the RSS floor ratcheting between commits — CPython's arena
allocator does not reliably return memory after millions of small objects.

Fix: identify the invalid rows vectorised and repair only those, keeping the fast
`cast` for the rest. Do **not** change output values — `"replace"` semantics for
the genuinely-invalid rows must be preserved exactly, since those 537 bodies are
already in the stored table.

Also: `body` is `large_binary` (64-bit offsets, 8 B/row) rather than `binary`
(32-bit, 4 B/row). Check whether any single body can exceed 2 GB; if not,
`binary` halves the offset buffer.

**Check — the fallback stopped firing batch-wide.** Bad-row count must stay
**537**; you may not fix, drop, or re-encode those rows, and their stored bodies
must be byte-identical to baseline.

```python
def invalid_utf8_rows(warehouse):
    bad = 0
    for f in files_of(warehouse):
        for b in pq.ParquetFile(f).read(columns=["body"]).column("body").to_pylist():
            if b is None:
                continue
            try:
                b.decode("utf-8")
            except UnicodeDecodeError:
                bad += 1
    return bad

assert invalid_utf8_rows(WAREHOUSE) == 537   # baseline prints 537
```

## B3. The `null_values` scan reads every entry value — *deleted by H*

> **Item H removes this.** Once the branch is a Python `if` on one row there is no
> full-column scan to optimise. Kept for the finding and in case H is deferred.

`tasks/parse_messages/parse_messages.yml:43` is
`null_values: ["", "null", "<null>", "n/a", "none"]` — **non-empty**, so the scan
in `normalized_arrow` (`text/entries.py:216-241`) runs on every call. It is
called **3x per batch** (`text/message.py:449`, `:460`, `:464`) but only the
first sees a full entries child (FIXXML is 3.47% of rows at 3.2 entries/row =
0.28% of entries), so treat it as **one expensive scan per batch, not three**:

```python
absent = frozenset(str(value).strip().lower() for value in null_values or ())
if not absent or not len(stored):
    return stored
entries = compute.list_flatten(stored)          # 0.00x, zero-copy
if not len(entries):
    return stored
parents = compute.list_parent_indices(stored).cast(pyarrow.int64())
values = compute.struct_field(entries, "value")  # 0.00x, zero-copy
missing = compute.is_in(
    compute.utf8_lower(compute.utf8_trim_whitespace(values)),   # <-- 2 full copies
    value_set=pyarrow.array(sorted(absent), pyarrow.string()),
)
keep = compute.and_(compute.is_valid(values), compute.invert(compute.fill_null(missing, False)))
if compute.all(keep, min_count=0).as_py():      # early exit AFTER the copies
    return stored
```

`utf8_trim_whitespace` then `utf8_lower` each materialise a full copy of `value`
— the **single largest thing in the schema at 881 B/row**, 46% of `entries`, 23%
of the row. ~1762 B/row of transient allocation, **~110 MiB per 65,536-row
batch**, to answer a yes/no question that almost always returns "keep
everything". The `compute.all(...)` early exit happens *after* both copies.

Fix: **run the predicate over the dictionary, not the entries.** 47,004 distinct
`value` spellings among 113,835,016 entries = ~2400x work reduction.
`dictionary_encode()` the child (or take A's already-encoded dictionary),
evaluate `utf8_lower(utf8_trim_whitespace(...))` + `is_in` on the ~47k
dictionary, map the boolean back through the indices with `take`. This is the
trick the codebase already praises at `rekep/entries.py:280-284`
(`keys.dictionary_encode()` as a transient per-batch fold), applied to the other
child.

**Synergy: item A makes this scan nearly free automatically** — once `value` is
stored dictionary-encoded the dictionary *is* the ~47k array. Do A first, then
make this site dictionary-aware; do not write a bespoke encode path A will
obsolete.

Semantics that must not change: an entry is dropped iff its `value` is null, or
its `value` after `utf8_trim_whitespace` + `utf8_lower` is in the set. Keep the
`min_count=0` on `compute.all` and the `fill_null(missing, False)` — both matter
for empty/all-null inputs.

**Check — same entries survive.** Compare entries-per-row, not a total: a total
can match while the distribution shifts.

```python
def entry_lengths(path):
    return pc.list_value_length(pq.ParquetFile(path).read(columns=["entries"])
                                .column("entries").combine_chunks())

base, new = entry_lengths(BASE_FILE), entry_lengths(NEW_FILE)
assert len(base) == len(new) and pc.all(pc.equal(base, new)).as_py(), "entries-per-row changed"
```

Then exercise the predicate against the old implementation on the cases that
distinguish dictionary-side from entries-side evaluation — empty strings,
all-null, mixed case, trim-then-match, and an all-kept batch (the `compute.all`
early-exit path):

```python
CASES = [[], [""], ["null"], [" NULL "], ["<null>"], ["N/A"], [None],
         ["real"], ["real", " none ", "x"], ["a"] * 1000 + [""]]
for case in CASES:
    assert old_normalized(make_entries(case)) == new_normalized(make_entries(case)), case
```

The last case matters most: a value repeated 1000 times is one dictionary entry,
so it is where the two implementations do most-different amounts of work while
being required to agree.

## B4. Two unconditional full copies of `entries` per batch — *deleted by H*

> **Item H removes this**, same reason as B3.

`text/message.py:446-469`, verbatim:

```python
text = _body_text_arrow(bodies)
raw_entries, token_errors = Entry.payload_arrow_with_diagnostics(text)
entries = Entry.normalized_arrow(raw_entries, plugins, plugin_keys, null_values)
xml = compute.equal(families, Protocol.XML.into_stored())
referential = compute.equal(families, Protocol.REFERENTIAL.into_stored())
xml_entries, parse_errors = xml_payload_arrow(bodies, xml)
xml_entries = Entry.normalized_arrow(xml_entries, plugins, plugin_keys, null_values)
entries = compute.if_else(xml, xml_entries, entries)                   # :461  full copy
referential_entries, referential_errors = referential_payload_arrow(bodies, referential)
referential_entries = Entry.normalized_arrow(referential_entries, plugins, plugin_keys, null_values)
entries = compute.if_else(referential, referential_entries, entries)   # :466  full copy
```

`compute.if_else` on a nested type has **no all-false short-circuit** — it
materialises a complete new `list<struct<...>>` either way. Measured **0.99x**
(one full copy) on a real ENTRIES array, and **+46.3 MB on a 22 MB-body batch
whose `xml` mask was entirely `False`**. Two back to back on the largest column:
**~3846 B/row transient, ~240 MiB per 65,536-row batch**, to overwrite rows that
are 3.47% (XML) and near-zero (referential) of the batch.

Worse, `xml_payload_arrow` (`text/entries.py:313`) and
`referential_payload_arrow` (`:346`) are called unconditionally and early-exit
only on `rows == 0` (`:322-324`), not on an empty *selection*, so on a pure-FIX
batch each still walks `_selected_payloads` and builds a full-length ENTRIES
array of empty lists via `_restored_payloads`.

Fix: guard all three sites on `compute.any(xml, min_count=0).as_py()` — a cheap
boolean reduction. When false, skip the payload parse, skip `normalized_arrow`,
and leave `entries` alone rather than `if_else`-ing it onto itself. Keep the
error columns correct: `parse_errors` / `referential_errors` must still come out
full-length and all-null in the skipped case, since `_merge_error_columns` at
`:467-468` consumes them. Cheapest large win in the list — a guard, not a
redesign — and independent of A.

## C. Low-cardinality top-level columns (~3.3%, 8.4 MiB/file measured)

Dictionary-encode: `level` (2-4 distinct, 8.6 B/row), `plugin`
(`fixed_size_binary[16]`, 15-28 distinct, 16 B/row), `protocol`
(`fixed_size_binary[16]`, 6 distinct, 16 B/row), `msgtype` (14-44 distinct,
10.8 B/row), `threadname` (8,866 distinct/file, 327,986 across the day,
31.5 B/row).

`threadname` is the careful one: 327,986 distinct across the day means a per-file
dictionary is fine but a **process-lifetime dictionary or intern cache keyed on
`threadname` would itself be an unbounded growth term. Do not add one.**

## D. All-null columns paying full width (111 B/row, 2.9%)

`prevhash` `fixed_size_binary[16]` costs the full 16.1 B/row at 100% null —
fixed-size-binary allocates its values buffer regardless of validity.
`expunix`/`snapunix`/`prevunix` `int64` cost 8.1 B/row each at 100% null. Lowest
priority; only worth it if it falls out of other changes.

## E. `batch_byte_size` accounting (finding 1)

Make the byte budget bound produced Arrow bytes. This is what makes the whole
thing *operable*: after A-D the per-row cost drops, and an honest byte budget
converts that into a predictable ceiling instead of a bigger batch.

## E2. Transient copies inside FIX payload parsing (~6x body, per batch)

Everything above is *retained* footprint; this is **transient**, additive to it.
Parsing one batch allocates roughly **6 full copies of `body`** before the writer
sees it.

Synthetic 65,536-row SOH FIX batch (34.18 MiB body, 2.36M child entries), PyArrow
pool plus 0.5 ms RSS sampler, one subprocess per case:
`payload_arrow_with_diagnostics` peaks at **6.95x body in Arrow, 8.33x in RSS,
retaining 1.95x**. Per-step retained multipliers:

| line | code | xbody |
|---|---|---:|
| `text/entries.py:890` | `tokens = split_payload_arrow(body, separator)` | 1.20 |
| `text/entries.py:891` | `parsed = compute.extract_regex(tokens.values, _TOKEN)` | 1.41 |
| `text/entries.py:905` | `compute.filter(struct_field(parsed, "value"), matched)` | 0.95 |
| `text/entries.py:904` | `compute.utf8_trim_whitespace(...)` | 0.94 ← **2nd full copy of every value** |
| `text/entries.py:914` | `Entry.structure_arrow(keys, values)` | 0.99 |
| `text/entries.py:895` | `keys = compute.filter(keys, matched)` | 0.45 |

All live simultaneously at 8.06x. `build_list` and `scattered`
(`fields/arrays.py:60-61`), `StructArray.from_arrays` (`:913`) and
`ListArray.from_arrays` (`:916`) are genuinely zero-copy — leave them alone. The
unmatched-token diagnostics cost only +0.08x (6.87x → 6.95x), so they are **not**
the problem; do not remove them for memory.

Real batches carry ~88 MiB of `body` at 65,536 rows → roughly **600-700 MiB
transient per batch**, concurrent with the ~1.85 GiB of buffered chunks. The
filter-then-trim pair at `:904-905` looks fusable or reorderable to drop one full
copy of every value.

Caveats: those multipliers are from **synthetic** bodies, so protocol mix and
entries/row are guesses — re-measure on a real slice. Scripts in `/tmp/mema/`
(`one.py`, `steps.py`, `scale.py`) if they survive. Corroborated by a different
harness on real code: `parse_arrow` peaks at **8.9x raw body bytes and retains
3.3x**, stage-by-stage live footprint on a 22 MB-body batch going
22 → 69.6 → 116.0 → 162.5 → 211.5 MB (Arrow peak 228.9 MB). But that run
attributes the last three steps to `message.py:461`, `:466`, `:469` — two of the
three are **item B4/H**, not this item. Do those first and re-measure, or you
optimise against a number they have already moved. The same run put the emitted
batch at **3.46x raw input bytes** (`entries` alone 2.10x) — the constant that
turns 64 MiB of input into a 237 MiB batch, the other half of finding 1.

**Check (also covers F) — the transient peak actually dropped.** Whole-run peak
RSS mixes retained and transient; to attribute a change, sample the pool's
`max_memory` around a single batch:

```python
pool = pyarrow.default_memory_pool()
before = pool.max_memory()
out = payload_arrow(body, separator)          # or arrow_chunks(...) for F
print(f"peak delta {(pool.max_memory() - before) / body.nbytes:.2f}x body")
```

`max_memory` is monotonic — read deltas, never absolutes — and run one case per
subprocess or earlier cases mask later ones. Beat **6.95x body** for the parse
path (E2) and **~3x the buffered chunk** held live at commit (F).

## F. Commit-time copies (finding 2) — the largest RSS lever

Avoid materialising `chunk` + `additions` + `fresh` simultaneously in the
empty-table branch of `_insert_arrow_table`. Append per partition run rather than
`concat_tables`-ing every partition into one table first, or stream the runs. The
existing comment explains the concat exists so *"PyIceberg lands every partition
in one append transaction"* — this is a **transactionality-vs-memory tradeoff,
not dead weight.** If you break the single transaction, say so explicitly and
justify it; if you preserve it, find a way to hand PyIceberg the runs without a
full concat. `_grouped_partition_chunk`'s `chunk.take(indices)`
(`iceberg/dataset.py:4400`) is skipped when already sorted
(`_reader_in_sort_order`, `:4394`). Log lines are chronological and
`unixpartition` derives from `unix`, so this likely already short-circuits —
**verify before optimising**, don't assume it's hot.

## B5. mimalloc keeps freed pages; ask for them back at each commit

Verified here: `pyarrow.default_memory_pool().backend_name` is **`mimalloc`** and
`release_unused()` exists on it. After a full read stream
`pyarrow.total_allocated_bytes()` was **0 MB** — nothing leaked — while RSS was
still **315 MB above baseline**, and one `release_unused()` dropped RSS from
**466 MB to 179 MB**. So ~90% of the residual resident set is freed-but-unreturned
pages, and mimalloc never returns them on its own. This is why the profile
*ratchets* between commits instead of falling to a flat floor.

Fix: call `release_unused()` on the default pool once per commit in
`_append_arrow_reader` (`iceberg/dataset.py:1253`), right after the chunk is
released — where the run's largest allocation has just died.

Two caveats, so you measure rather than assume:

- This attacks the *floor*, not the *peak*. Acceptance is on peak RSS, so alone it
  may move the number less than A-B4 do. It should still help on a long run: a
  lower floor at commit *N* is what every later peak is measured on top of.
- `release_unused()` costs time. Once per commit is negligible; per batch may not
  be. Do not put it in the batch loop without measuring against the elapsed
  guard.

## I. Delete what you replace — no dual paths, no dead helpers

Two halves. The second is small; the first is the one that decides whether this
work is an improvement or an accretion.

### I1. When H supersedes a path, delete the path

H replaces the batch-wide row walk. B3 and B4 exist *inside* what it replaces.
The failure mode is landing H beside the old code and leaving the old code
reachable — behind a flag, behind an `if`, or just imported and unused. Then peak
RSS does not move, because the pessimistic path is still what runs in some
configuration, and the next person cannot tell which one is the contract.

So, on H landing:

- **`null_values` batch scan (B3) — delete the scan, not just its call.** Once
  enrichment sees rows, the null test happens per row on the value already in
  hand. Remove the vectorized scan and every helper that existed only to feed it.
  Do not leave it as a "fast path".
- **The two unconditional `entries` copies (B4) — delete both.** Not "skip when
  unnecessary". If the row phase owns entries, nothing upstream should be
  materialising a second copy to be safe.
- **One code path per stage.** If you need the old behaviour to compare against,
  compare against `git stash`/a branch, not against a runtime flag. The only flags
  this brief sanctions are item G's, and those exist because G changes stored
  output.
- **Delete, do not deprecate.** No `_legacy_*` alias, no `warnings.warn` shim, no
  commented-out block. This is a private path inside `text/`; there is no external
  caller to keep compatible.
- Grep for orphans afterwards: every import, module-level constant, and helper
  that had exactly one call site in the deleted code is now dead too. Deleting the
  caller and keeping the helper is the most common way this item gets half-done.

**Check.** After H, the deleted names must not resolve anywhere:

```python
import subprocess
for name in DELETED:                      # the exact identifiers you removed
    hits = subprocess.run(["git", "grep", "-n", "--", name],
                          capture_output=True, text=True).stdout
    assert not hits, f"{name} still referenced:\n{hits}"
```

### I2. Three genuinely dead definitions — audited, delete outright

A reachability sweep of `text/message.py`, `text/entries.py`, `text/text_file.py`
and the `entries.py` surface they use found exactly three definitions with **zero
call sites**, no `__all__` entry, no test, no doc reference, and no indirect
dispatch:

| definition | anchor | lines |
|---|---|---:|
| `pop_arrow` (module fn) | `text/entries.py:697-750` | 54 |
| `Entry.pop_arrow` (its only caller) | `entries.py:262-273` | 12 |
| `Entry.looks_structured_arrow` (wrapper) | `entries.py:221-226` | 6 |

`pop_arrow` is a self-contained two-layer pair: the classmethod's only caller is
nothing, and the module function's only caller is the classmethod. It is not
`Rule.pop` (`fix/rules.py:180,229,237`), which is live and consumed by an
independent `_popped_pairs` (`fix/transcribe.py:398,2295`).
`Entry.looks_structured_arrow` is a wrapper — **delete only the classmethod**;
the module-level `looks_structured_arrow` (`text/entries.py:105-117`) is live
from `_payload_arrow` (`:149`).

Two things to know before deleting:

- `Entry` is published in `rekep.__all__`, so removing two of its public
  classmethods is a **nominal API break** even though nothing in this repo, its
  tests, benchmarks, tasks, or docs calls them. Note it in the report; do not let
  it stop the deletion.
- After removing `pop_arrow`, no import in `text/entries.py` is orphaned —
  `column_names` (`:287`), `build_list` (`:236,310`), `dense_counts` (`:238`) and
  `null_mask` (`:240,310`) all have other callers. Re-check anyway.

**Do not delete these, even though they look dead.** They were checked and are
reachable:

- `_timezone_transitions` / `_windows_utc_micros` / `_windows_local_micros` /
  `_datetime_micros` (`text/text_file.py:1532-1632`, ~95 lines) — live behind
  `os.name == "nt"` guards (`text_file.py:978,1523`) and unreachable on POSIX.
  **Windows code, not dead code.** It is also untested here (the
  `windows`/`posix` fixtures at `tests/test_arrow_file_io.py:34,39` patch
  `arrow_file_io._WINDOWS`, not `os.name`) — that makes it *risky to touch*, not
  removable.
- `_windowed_batches` (`text/text_file.py:1280-1340`) — called from
  `into_arrow_batches` (`:569`) and `text_files.py:524`, tested.
- `_plugin_keys` (`text/entries.py:244-272`) and `_renamed_keys` (`:275-310`) —
  both live from the `plugin_keys` branch of `normalized_arrow`
  (`text/entries.py:196-215`), which is a documented config surface
  (`text_file.py:204`, `tasks/parse_messages/parse_messages.py:51`).
- `unix_of` — two distinct functions, **neither in `text/**`**: `times.py:274-279`
  (no cache, exported, used by five tasks) and `fix/fields.py:401-462`
  (`lru_cache(maxsize=8192)`, load-bearing because a capture re-reads the same
  stamp text per field). The parsing path uses `_local_micros` / `_unix_nanos` /
  `TimestampField.into_unix_arrow` instead.
- `Protocol.REFERENTIAL` **is** set, in `fix/rules.py:395` (the column write for
  any body matching `REFERENTIAL_PAYLOAD_PATTERN`, `rules.py:128`), via
  `rules.py:233-238` and `text/message.py:321`, and settable from YAML through
  `rules.py:191`. Proven end-to-end by `tests/text/test_message.py:909`.
- `TextFile.read1` / `readinto1` (`text/text_file.py:912-920`) —
  `io.BufferedIOBase` overrides (`text_file.py:145`). No in-repo caller by design;
  the protocol is the caller.

The point of that list is not the individual entries. It is that **six of the
nine things that looked dead were reachable through a guard, a config branch, a
protocol, or a getattr registry.** Before deleting anything not in the I2 table,
check for those four mechanisms specifically, and report what you deleted with
its call-site evidence.

## G. Strip structurally, keep direction, drop repeated payloads — up to 48% of feed C

Everything above shrinks the *representation*. This removes bytes and rows the
capture duplicates. Biggest win available, and the only item that **deliberately
violates hard constraints 2 and 3** — read the carve-out before starting.
`[MISSING: constraints 1/2/3 and G's carve-out were not in the screenshots.]`

**The pattern.** These feeds interleave a per-stage enrichment trace with the
messages: the same payload re-emitted after every stage, each time behind
different prose, microseconds apart. Anonymised, this is the shape — `<ts>`
matches the header regex, `KEY_n`/`v_n` stand for whatever the feed's fields are
called:

```
<ts> [<thread>] [<plugin>] (DEBUG) -> Stage one <FIELD> : #KEY_A=v1|#KEY_B=v2|...|#KEY_N=vN|
<ts> [<thread>] [<plugin>] (DEBUG) -> Stage two <FIELD> : #KEY_A=v1|#KEY_B=v2|...|#KEY_N=vN|
<ts> [<thread>] [<plugin>] (DEBUG) Emitting : #KEY_A=v1|#KEY_B=v2|...|#KEY_N=vN|
<ts> [<thread>] [<plugin>] (INFO)  Inbound : 8=FIX.4.4|9=<n>|35=D|...|10=<n>|
<ts> [<thread>] [<plugin>] (DEBUG) Wrapped (#KEY_A=v1|#KEY_B=v2|)
<ts> [<thread>] [<plugin>] (DEBUG) Inbound Emitting : #KEY_A=v1|#KEY_A=v1|#KEY_B=v2|
```

Up to fifteen lines, ~4.5 KB of payload each, one logical message.
`HEADER_PATTERN` (`text/text_file.py:60-68`) captures `timestamp`, `threadname`,
`plugin`, `level`, then `(?P<body>.*)$` — so **all of that prose is inside
`body`**.

**Why the existing dedup cannot see them.** `text/message.py:530` is
`columns["vhash"] = hash_bytes_arrow(columns["body"])` — vhash is over `body`
alone, which is right. Two problems stack:

1. The prose is *in* `body`, so two lines carrying an identical payload hash
   differently.
2. `hash` is `txhash.couple128_arrow(cls._clock_micros(...), vhash)`
   (`text/message.py:531-533`) and `_clock_micros` (`market/event.py:395-405`)
   floors to whole **microseconds**. The trace lines are 7-14 µs apart, so even
   with equal vhash they get distinct `hash` and `merge_by: true` cannot collapse
   them.

So **stripping is not primarily a byte saving — it is the enabler.** Strip, and
vhash becomes a true content identity.

### Rule 1 — find the payload structurally. Never ship a prefix list.

The prose is **not a closed set.** Measured per 200k-line slice: **3,084-5,846
distinct prefixes**; collapsing every digit run to a placeholder only takes
3,419 → 2,743, and the top 12 cover 42-95% depending on feed. Prefix shapes seen
include `word :`, `-> word`, `word ->`, `-> word :`, `&ident =`, a bare
identifier with no separator at all, and composites that embed payload fragments
in the prose (line 6 of the sample above). **Any literal list is wrong on
arrival.**

Anchor on the payload instead. The payload starts at the first **token run**: a
`key=value<delim>` pair followed by at least one more `=`. Find it with a linear
scan, not a regex:

```python
KEY_BYTES = frozenset(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-")

def payload_start(body: bytes) -> int:
    """Offset of the payload's first token, or -1 when the line carries none."""
    for delimiter in (0x7C, 0x01):                    # '|' then SOH
        first = body.find(bytes([delimiter]))
        if first < 0:
            continue
        equals = body.rfind(b"=", 0, first)
        if equals < 0:
            continue
        start = equals
        while start > 0 and body[start - 1] in KEY_BYTES:
            start -= 1
        if start > 0 and body[start - 1] == 0x23:      # keep a leading '#'
            start -= 1
        if start == equals:                            # empty key, not a token
            continue
        if body.find(b"=", first + 1) < 0:             # need a second token
            continue
        return start
    return -1
```

**Post-mortem, and the reason for the "prototype first" rule at the top.** An
earlier revision of this brief shipped a regex anchor,
`#[A-Z0-9_]+=|(?:^|[|\x01 ])\d{1,4}=`. Running it produced three separate
defects, each measured over 200k-line slices:

| defect | feed A | feed C | cause |
|---|---:|---:|---|
| missed a payload the scan finds | 12,094 | 8,028 | key charset excluded `.` and `-` |
| claimed payload in prose with no delimiter | 1,523 | 1,718 | bare `\d{1,4}=` matches prose |
| put the boundary *later* than the scan — **ate payload** | 66 | 2,873 | anchored on a later token |

It also over-stripped **+1.79% of body bytes** on one slice while *under*-stripping
on another. And a stricter regex form of the same rule (requiring two complete
tokens) backtracks catastrophically on 4.5 KB payloads — it did not finish a
200k-line slice in 10 minutes, where the scan above does it in **0.71 µs/line**.
Requiring a real delimiter matters: **27,920-48,930 lines per slice contain `=`
but no delimiter** and are prose, not payload.

The invariant that catches all three defects at once: **after stripping, the head
must contain no token run.** `payload_start(head) < 0`. Verified 0 failures over
~800k lines across four slices.

### Rule 2 — strip the trailing suffix too

Prose also wraps payloads (`Wrapped (#KEY_A=v1|...|)`). Content after the
payload's last delimiter is a suffix, not a field: **4.28-7.29% of payload
lines** carry one, over **1-24 distinct families**, dominated by a single `)`. It
is worth almost nothing in bytes (**0.0015-0.0044% of body**) — take it for
dedup, not for size: some suffixes carry variable data (an id, a name) that
otherwise breaks byte-identity between two copies of the same payload. Stripping
it moves dedup **+0.16 to +1.23 points**. Strip symmetrically with rule 1: the
payload is `body[start : last_delimiter + 1]`.

### Rule 3 — extract direction, and put it in vhash. This one is correctness, not savings.

**Direction-blind dedup silently collapses an inbound message with its outbound
copy.** The prose that must be stripped is also the only place the direction is
recorded, so it has to be read out before it is discarded. Measured consecutive
pairs with an identical payload but *opposite* direction, per 200k-line slice:
**feed C 13,431 / 17,241, feed A 10,164, feed B 6,516.** Those are distinct
business events and must not merge.

Classify the stripped head with two configurable keyword families, **leftmost
match wins** — the verb of the log statement, not a noun inside it. Composite
prose matching both families is real (327-948 lines per slice, over 3-434
distinct prefixes), which is exactly why position decides:

```python
INBOUND = re.compile(rb"(?i)receiv|inbound|\bin\b|from|sroute|\bread\b")
OUTBOUND = re.compile(rb"(?i)send|outbound|\bout\b|push|writ|emit|publish")

def direction_of(head: bytes) -> str:
    into, out = INBOUND.search(head), OUTBOUND.search(head)
    if into and out:
        return "in" if into.start() < out.start() else "out"
    return "in" if into else "out" if out else "none"
```

Store direction as its own low-cardinality column (3 values — dictionary-encode
it per item C, ~1 B/row) **and mix it into `vhash`**, so identity is
`(direction, payload)` rather than payload alone:

```python
# text/message.py:530, replacing hash_bytes_arrow(columns["body"])
identity = pyarrow.compute.binary_join_element_wise(
    direction_code, payload, pyarrow.scalar(b"", pyarrow.binary())
)
columns["vhash"] = hash_bytes_arrow(identity)
```

`couple128_arrow` (`txhash.py:88-101`) already composes an identity out of
`binary_join_element_wise` with an empty separator — follow that precedent rather
than inventing a framing. Use a fixed-width code so the join stays unambiguous; a
variable-length direction string would let `("in", "x")` and `("i", "nx")`
collide.

**Do not drop `body`.** Keep the raw line's body stored as it is today; the
stripped payload and the direction are *derived* columns. The prose is the only
evidence of which enrichment stage emitted a line, and constraint 1 treats `body`
as contract.

### Rule 4 — drop the consecutive duplicate

Compare each row's `(direction, payload)` to the previous one and skip when
equal. Behind an explicit flag, default *off*.

**One previous payload — do not build a per-thread cache.** Both were measured:
per-thread buys a few points on feed C and ~0.02 on A and B. Against that it
needs a dict holding one payload per live thread, and there are **~21,625
distinct threadnames in 200k feed-A lines** (327,986 across a day). At ~4.5 KB
per payload that is an unbounded growth term of tens to hundreds of MB, in a
brief whose entire point is peak RSS — and it contradicts item C's warning
against threadname-keyed structures. If you want the per-thread points anyway,
the only acceptable shape is a bounded LRU (N = 64-256) keyed on the payload's
16-byte hash rather than the payload, so retained state is O(N × 16 B). Measure
it; do not assume it wins.

### Measured, with all four rules applied

| feed / slice | prefix bytes | dup, direction-blind | **dup, direction-aware** | crossed pairs |
|---|---:|---:|---:|---:|
| C head 200k | 0.69% | 47.74% | **35.86%** | 13,431 |
| C mid 200k | 0.70% | 46.51% | **31.12%** | 17,241 |
| A head 200k | 0.90% | 12.81% | **3.93%** | 10,164 |
| B head 200k | 2.21% | 11.35% | **5.71%** | 6,516 |

Percentages are of total `body` bytes. **Direction-aware is the number to
implement against**; the direction-blind column is there only to show the ~9-15
points that a naive strip would wrongly collapse. Feed C head and mid agree to
1.2 points, so ~1/3 of its body bytes really are redundant.

---

## `[MISSING]` — not captured in the screenshots

The brief refers to these; re-attach them from the source before starting:

1. **Hard constraints 1, 2, 3.** Constraint 1 is quoted as treating `body` as
   contract; G "deliberately violates hard constraints 2 and 3". The text of all
   three is absent.
2. **G's carve-out and its sign-off.** "It needs the sign-off in its carve-out and
   the others do not."
3. **The acceptance criteria.** Referenced twice — the feed-C proof run is gated
   on hitting them, and "acceptance is on peak RSS".
4. **The report format.** Items say "say so in the report", "note it in the
   report", "report that" — the expected shape is not on screen.
