# All task prompts — `parse_messages` peak-memory work

Thirteen self-contained prompts. Dispatch each to its own agent as-is.
Source brief: `briefs/parse-messages-peak-memory/BRIEF.md`. Waves, ownership
matrix and merge order: `PLAN.md`.

| prompt | wave | items | may start |
|---|---|---|---|
| `W0-1-baseline-gate.md` | 0 | — | now |
| `W0-2-commit-path.md` | 0 | B5, F | now |
| `W0-3-dead-code.md` | 0 | I2 | now |
| `W0-4-row-primitive.md` | 0 | H dependency | now |
| `W0-5-g-prototype.md` | 0 | G dependency (no `src/` edits) | now |
| `W1-trunk-H-I1.md` | 1 | H, I1, deletes B3 + B4 | after W0·4, W0·3 merge |
| `W2-A-entries-types.md` | 2 | A | after W1 merges |
| `W2-B-column-widths.md` | 2 | B, C, D | after W1 merges |
| `W2-C-body-and-parse.md` | 2 | B2, E, E2 | after W1 merges (E after W2·A) |
| `W3-A2-comp-removal.md` | 3 | A2 | gated: A landed + explicit go |
| `W3-G-strip-direction.md` | 3 | G | after W0·5 + W1; needs carve-out sign-off |
| `MERGE-final.md` | — | integration, proof, report | after the above |

`00-STANDING-RULES.md` is inlined in every prompt; it is here as the single place
to edit if a rule changes.

**Five agents can start immediately** (all of wave 0). The trunk is W1 — nothing


---

<!-- file: prompts/00-STANDING-RULES.md -->

# Standing rules (inlined into every task prompt)

Repo `tdl-data-record-keeping`, package `python/src/rekep`. Target: the
`parse_messages` task. Full context: `briefs/parse-messages-peak-memory/BRIEF.md`.
Read `AGENTS.md` before writing code.

1. **Prototype before you commit.** Any rule that inspects log content gets built
   throwaway, run over the sample slices, counts printed — then written for
   production. A rule that has not been run over real bytes is a hypothesis.
2. **One commit per item**, prefixed with the brief's letter. Branch
   `mem/<letter>-<slug>`.
3. **Measure before and after.** `./bench.sh <label>`, then
   `./compare.py baseline <label>`. Never claim an improvement you did not
   measure. Run under `uv run --project python --group runner python`.
4. **Work per file, never per warehouse** — the 78 files as one table is ~10.9 GiB
   and OOM-kills. Use `files_of(warehouse)[0]`, the largest, as representative.
5. **No rule may be coupled to a feed.** Capture filenames live only in `bench.sh`.
6. **Delete what you replace.** No dual paths, no `_legacy_*` alias, no
   `warnings.warn` shim, no commented-out block, no runtime flag preserving old
   behaviour. Only item G ships flags.
7. **In-memory changes must leave on-disk parquet bytes ~unchanged.** Parquet
   already dictionary-encodes and ZSTDs everything; the waste is entirely
   in-memory. This is the safety property.
8. **`skipped = read - written`** (`rekep/logs.py:156`) is a merge-by-key dedup
   count and a correctness invariant. It may not move (except under G's flags).
9. **Stay inside your ownership list.** Needing to edit a symbol you do not own is
   a stop-and-escalate, not a judgement call. See `PLAN.md`.

Shared helpers for every check snippet:

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

---

<!-- file: prompts/W0-1-baseline-gate.md -->

# W0·1 — Pin the baseline (gate for every measurement claim)

**Runs:** immediately, in parallel with all other W0 tasks.
**Blocks:** every `compare.py` claim any other task makes.
**Owns:** `bench.sh`, `compare.py`, `measure.py`, `results/baseline/`. **No `src/` edits.**

Read `briefs/parse-messages-peak-memory/BRIEF.md` §Baseline and §Validation
preamble, and `00-STANDING-RULES.md`. Read `README.md` for harness usage.

## Job

Reproduce the pinned baseline so that every later `./compare.py baseline <label>`
means something, and confirm the harness measures what the brief says it does.

1. `./bench.sh baseline` over feeds A and B. **Do not attempt feed C** — at the
   baseline's ~2.5x-input peak-RSS ratio it peaks past 80 GiB. It is the
   end-to-end proof *after* acceptance, not a baseline input.
2. Confirm against the pinned table. Deviation beyond a few percent is a finding
   to report, not a number to overwrite:

   | feed | input | elapsed | read | written | skipped | peak RSS |
   |---|---|---|---|---|---|---|
   | A | ~1.9 GB | 142.9 s | 1,087,654 | 1,087,616 | 38 | ~4.23 GiB |
   | B | ~2.7 GB | 247.2 s | 1,884,311 | 1,792,697 | 91,614 | ~7.25 GiB |

3. Verify `skipped == read - written` holds on both feeds.
4. Record the per-column bytes/row baseline from the **largest** written parquet
   file, so later tasks can diff layout. Expected on that file (71,132 rows):
   `tag int32 173.7`, `key string 684.3`, `value string 881.5`,
   `comp string 179.2`, `entries` total `1922.6 B/row`; whole row 3788 B/row in
   memory vs 123 B/row on disk.
5. Confirm the two counters other tasks depend on:
   - `measure.py` reports `UTF8_FALLBACK_BATCHES` / `UTF8_BAD_ROWS`; the invalid
     UTF-8 body count is **537** of 2,880,313 rows. W2·C asserts this exact
     number.
   - `pyarrow.default_memory_pool().backend_name` is `mimalloc` and
     `release_unused()` exists on it. W0·2 depends on this.
6. Keep `results/baseline/` intact and per-file — later checks diff **per file**
   against it, never the whole warehouse.

## Deliverable

`results/baseline/` plus a short note: reproduced numbers vs pinned numbers, any
deviation, and confirmation of items 3–5. If the harness cannot reproduce the
baseline, say so loudly — every other task's claims rest on it.

---

<!-- file: prompts/W0-2-commit-path.md -->

# W0·2 — Commit-path memory: B5 + F + `arrow_chunks` slice retention

**Runs:** immediately, parallel with all W0 tasks. Independent of the `text/` trunk.
**Owns:** `rekep/dataset.py` → `arrow_chunks`; `rekep/iceberg/dataset.py` →
`_append_arrow_reader`, `_insert_arrow_table`, `_grouped_partition_chunk`.
**Touches nothing under `rekep/text/`.**

Read `BRIEF.md` §B5, §F, §"Two structural findings" #2, and the `arrow_chunks`
paragraph inside §H. Read `00-STANDING-RULES.md`.

## Three commits, in this order

### 1. `B5: release unused pool pages at each commit`

Verified on this machine: after a full read stream
`pyarrow.total_allocated_bytes()` was **0 MB** — nothing leaked — while RSS was
still **315 MB above baseline**, and one `release_unused()` dropped RSS from
**466 MB to 179 MB**. ~90% of the residual resident set is freed-but-unreturned
mimalloc pages, which never come back on their own. This is why the profile
*ratchets* between commits instead of falling to a flat floor.

Call `release_unused()` on the default pool once per commit in
`_append_arrow_reader` (`iceberg/dataset.py:1253`), right after the chunk is
released — where the run's largest allocation has just died.

Two caveats, so you measure rather than assume:
- This attacks the **floor**, not the peak. Acceptance is on peak RSS, so alone it
  may move the number less than the `text/` items do. It should still help on a
  long run: a lower floor at commit *N* is what every later peak is measured on
  top of.
- `release_unused()` costs time. Once per commit is negligible; per batch may not
  be. **Do not put it in the batch loop** without measuring against the elapsed
  guard.

### 2. `F: stop materialising chunk + additions + fresh at commit`

The empty-table branch of `_insert_arrow_table` (`iceberg/dataset.py` ~1332-1345):

```python
runs = list(_partition_runs(_grouped_partition_chunk(chunk, partitions), partitions))
additions = [first_rows(normalised_keys(run, join), join) for run in runs]
fresh = pyarrow.concat_tables(additions, promote_options="none")
```

`chunk` + `additions` + `fresh` live simultaneously ≈ **3x chunk ≈ 5.5 GiB**,
plus the reader's in-flight batch. That is the peak: RSS jumped 4.3 → 7.6 GiB in
the **last 6 s** of the 247 s feed-B run — a copy at commit, not steady-state
buffering.

Append per partition run rather than `concat_tables`-ing every partition into one
table first, or stream the runs. **The concat is a transactionality-vs-memory
tradeoff, not dead weight** — the existing comment explains it exists so
"PyIceberg lands every partition in one append transaction". If you break the
single transaction, say so explicitly and justify it. If you preserve it, find a
way to hand PyIceberg the runs without a full concat.

`_grouped_partition_chunk`'s `chunk.take(indices)` (`:4400`) is another full copy,
but it is skipped when already sorted (`_reader_in_sort_order`, `:4394`). Log
lines are chronological and `unixpartition` derives from `unix`, so this likely
already short-circuits — **verify before optimising; do not assume it is hot.**

Note the writer produced 78 parquet files across 24 partitions with a **single
164 MB row group each**, so per-partition writers each hold a full row group too.

### 3. `F: stop retaining parent buffers in arrow_chunks`

`arrow_chunks` (`rekep/dataset.py:427-470`) slices input batches and holds the
slices in a list until the chunk closes. **An Arrow slice is zero-copy and keeps
its parent's whole buffer alive**, so one 512-row slice of a 270 MiB batch
retains 270 MiB. Its own docstring already concedes the shape: *"With both bounds
absent the whole stream is one chunk, which is the atomic write and the one that
costs the most memory."*

At `commit_batch_num: 8` and 237 MiB real batches a chunk is ~1.85 GiB, matching
the observed sawtooth plateau.

## Check — attribute the change, don't infer it

Whole-run peak RSS mixes retained and transient. Sample the pool's `max_memory`
around a single commit:

```python
pool = pyarrow.default_memory_pool()
before = pool.max_memory()
out = arrow_chunks(...)
print(f"peak delta {(pool.max_memory() - before) / body.nbytes:.2f}x body")
```

`max_memory` is **monotonic** — read deltas, never absolutes — and run one case
per subprocess, or earlier cases mask later ones. Target: beat **~3x the buffered
chunk** held live at commit.

Then `./bench.sh <label>` + `./compare.py baseline <label>` per commit, and
confirm `read` / `written` / `skipped` are unchanged.

## Out of scope

Anything under `rekep/text/`. If the fix seems to need a change there, stop and
escalate — the trunk task (W1) owns those files.

---

<!-- file: prompts/W0-3-dead-code.md -->

# W0·3 — I2: delete three audited-dead definitions

**Runs:** immediately. **Merges first**, before W1 and W2·A rewrite these files.
**Owns:** `rekep/text/entries.py` → `pop_arrow` only; `rekep/entries.py` →
`Entry.pop_arrow` and `Entry.looks_structured_arrow` only.

Read `BRIEF.md` §I2 and `00-STANDING-RULES.md`. This is a 72-line deletion, but
the audit around it is the point — read the "do not delete these" list before
touching anything.

## Delete exactly these three

| definition | anchor | lines |
|---|---|---:|
| `pop_arrow` (module fn) | `text/entries.py:697-750` | 54 |
| `Entry.pop_arrow` (its only caller) | `entries.py:262-273` | 12 |
| `Entry.looks_structured_arrow` (wrapper) | `entries.py:221-226` | 6 |

A reachability sweep found these have **zero call sites**, no `__all__` entry, no
test, no doc reference, and no indirect dispatch. `pop_arrow` is a self-contained
two-layer pair: the classmethod's only caller is nothing, and the module
function's only caller is the classmethod.

Two traps:
- It is **not** `Rule.pop` (`fix/rules.py:180,229,237`), which is live and
  consumed by an independent `_popped_pairs` (`fix/transcribe.py:398,2295`).
- `Entry.looks_structured_arrow` is a wrapper — **delete only the classmethod**.
  The module-level `looks_structured_arrow` (`text/entries.py:105-117`) is live
  from `_payload_arrow` (`:149`).

Two things to know before deleting:
- `Entry` is published in `rekep.__all__`, so removing two of its public
  classmethods is a **nominal API break** even though nothing in this repo, its
  tests, benchmarks, tasks, or docs calls them. Note it in the report; do not let
  it stop the deletion.
- After removing `pop_arrow`, no import in `text/entries.py` is orphaned —
  `column_names` (`:287`), `build_list` (`:236,310`), `dense_counts` (`:238`) and
  `null_mask` (`:240,310`) all have other callers. **Re-check anyway.**

## Do NOT delete these — they were checked and are reachable

- `_timezone_transitions` / `_windows_utc_micros` / `_windows_local_micros` /
  `_datetime_micros` (`text/text_file.py:1532-1632`, ~95 lines) — live behind
  `os.name == "nt"` guards (`:978`, `:1523`), unreachable on POSIX. **Windows
  code, not dead code**, and untested here (the `windows`/`posix` fixtures at
  `tests/test_arrow_file_io.py:34,39` patch `arrow_file_io._WINDOWS`, not
  `os.name`) — risky to touch, not removable.
- `_windowed_batches` (`text/text_file.py:1280-1340`) — called from
  `into_arrow_batches` (`:569`) and `text_files.py:524`, tested.
- `_plugin_keys` (`text/entries.py:244-272`) and `_renamed_keys` (`:275-310`) —
  live from the `plugin_keys` branch of `normalized_arrow` (`:196-215`), a
  documented config surface.
- `unix_of` — two distinct functions, neither in `text/**`: `times.py:274-279`
  and `fix/fields.py:401-462` (`lru_cache(maxsize=8192)`, load-bearing).
- `Protocol.REFERENTIAL` **is** set, in `fix/rules.py:395` via `:233-238` and
  `text/message.py:321`. Proven by `tests/text/test_message.py:909`.
- `TextFile.read1` / `readinto1` (`text/text_file.py:912-920`) —
  `io.BufferedIOBase` overrides; the protocol is the caller.

**Six of the nine things that looked dead were reachable** through a guard, a
config branch, a protocol, or a getattr registry. Before deleting anything not in
the table above, check for those four mechanisms specifically.

## Check

```python
import subprocess
for name in DELETED:                      # the exact identifiers you removed
    hits = subprocess.run(["git", "grep", "-n", "--", name],
                          capture_output=True, text=True).stdout
    assert not hits, f"{name} still referenced:\n{hits}"
```

Plus: full test suite green. No bench run needed — this frees no measurable
memory and should claim none.

## Deliverable

One commit, `I2: delete three dead definitions`, reporting each deletion **with
its call-site evidence** and noting the nominal `rekep.__all__` API break.

---

<!-- file: prompts/W0-4-row-primitive.md -->

# W0·4 — The missing primitive: `from_arrow_batch` (batch → iter\<scalar\>)

**Runs:** immediately. **Must merge before W1 (the H trunk) starts** — H consumes
this.
**Owns:** `rekep/fields/rows.py`; `rekep/fields/field.py` → the `@scalar`
installation lines only (`:1620-1621` area).

Read `BRIEF.md` §H → "The missing primitive" and `00-STANDING-RULES.md`. Read
`AGENTS.md` on naming before you pick the name.

## Why this exists as its own task

`fields/rows.py` says *"rows in, columns out"* and only goes that way:
`struct_array(declared, rows)` at `:33`, reached from
`StructField.into_arrow_array` (`fields/field.py:1316`) and installed on every
`@scalar` class as `into_arrow_array` / `into_arrow_batch`
(`fields/field.py:1620-1621`). **There is no inverse.** Item H's row phase cannot
exist without one, and inventing it inside H would bury the one measurement that
decides whether H is worth doing at all.

Add it in `fields/rows.py`, installed by `@scalar` the same way, named
`from_arrow_batch` per the house `from_*` builds / `into_*` converts rule.

## The hard requirement: it must not go through `to_pylist()`

`Event.from_arrow_reader` does (`market/event.py:322`:
`cls.from_dict(row) for row in batch.to_pylist()`) and **that is the single most
expensive line in either direction.** Largest baseline file, 16,384 rows, one case
per process:

| walk | held | µs/row |
|---|---:|---:|
| whole batch `to_pylist()` — what `from_arrow_reader` does now | **18,286 B/row** | ~28 |
| flat columns only `to_pylist()`, `entries` left in Arrow | 2,224 B/row | ~2.9 |
| per-row `as_py()` off combined columns, one row held | **46 B/row** | ~4.9 |

`to_pylist()` costs **4.7x the Arrow footprint it reads** (3,916 → 18,286 B/row)
= **1.14 GiB of Python dicts** for one 65,536-row batch. The per-row walk holds
**398x less for 1.5x the CPU. Take the CPU.**

Two requirements follow:
- `combine_chunks()` the columns **once** before indexing — indexing a
  `ChunkedArray` per row is O(chunks).
- Read only the members the consumer needs.

## Check — the row walk must not be a dict walk

This decides the implementation. It is the one place where running cases in one
process gives the wrong answer: RSS after a large free stays high (mimalloc keeps
freed pages, item B5), so an earlier case masks a later one. **One case per
process:**

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
Held bytes are stable to the byte; timings vary a few percent.

- `scan` must build the **whole row tuple** — reading only `body` is 27 B/row at
  1.0 µs and flatters the design.
- **Your `from_arrow_batch` must land near `scan` (~46 B/row).** Near `whole`
  means it is building dicts — most likely `to_pylist`, `as_py()` on a whole
  column, or `from_dict`. That is a failed implementation, not a slow one.

## House rule to check yourself against

AGENTS.md: *"Never use a Python row loop for an Arrow shape conversion. Column
comprehensions are fine."* That rule is about **shape conversion** — casting,
restructuring, projecting. `from_arrow_batch` reads member-by-member off
**declared** types with nothing inferred per row, which keeps it on the right side
of the rule. Do not let it start inferring.

## Deliverable

One commit, `H: add from_arrow_batch, the inverse of struct_array`, with the
three-case held-bytes numbers from **your** machine in the commit body, plus a
unit test round-tripping a `@scalar` class through
`into_arrow_batch` → `from_arrow_batch`.

---

<!-- file: prompts/W0-5-g-prototype.md -->

# W0·5 — G prototype: validate the content rules against real bytes (NO production code)

**Runs:** immediately, parallel with everything. **Unblocks W3·G at the end.**
**Owns:** `briefs/parse-messages-peak-memory/g-prototype/` only.
**Edits nothing under `python/src/`.** That restriction is the point of the task.

Read `BRIEF.md` §G in full and `00-STANDING-RULES.md`.

## Why this is a separate, early, code-free task

The brief's opening rule — *"Prototype before you commit... a rule that has not
been run over real bytes is a hypothesis"* — exists **because of item G**. An
earlier revision of the brief shipped a regex anchor,
`#[A-Z0-9_]+=|(?:^|[|\x01 ])\d{1,4}=`, and running it produced three separate
defects:

| defect | feed A | feed C | cause |
|---|---:|---:|---|
| missed a payload the scan finds | 12,094 | 8,028 | key charset excluded `.` and `-` |
| claimed payload in prose with no delimiter | 1,523 | 1,718 | bare `\d{1,4}=` matches prose |
| put the boundary *later* than the scan — **ate payload** | 66 | 2,873 | anchored on a later token |

It also over-stripped **+1.79% of body bytes** on one slice while *under*-stripping
on another, and a stricter regex form backtracks catastrophically on 4.5 KB
payloads — it did not finish a 200k-line slice in 10 minutes, where the linear
scan does it in **0.71 µs/line**.

So: measure first, ship later. G lands last (it changes stored output); this task
front-loads everything about it that can be validated without touching `src/`.

## Inputs

Sample slices at `/tmp/<feed>_head.txt` and `/tmp/<feed>_mid.txt` — 200k
header-matched lines each, ~200-450 MB, seconds to scan. Rebuild with
`head -n 200000` and `sed -n '4000000,4200000p'` over a decompressed feed if
gone. **Never hard-code a feed name** — take paths as arguments.

## Validate all four rules

### Rule 1 — structural payload anchor. Never ship a prefix list.

The prose is **not a closed set**: **3,084-5,846 distinct prefixes** per 200k
slice; collapsing digit runs only takes 3,419 → 2,743, and the top 12 cover
42-95% depending on feed. Shapes seen: `word :`, `-> word`, `word ->`,
`-> word :`, `&ident =`, a bare identifier with no separator, and composites that
embed payload fragments in the prose. **Any literal list is wrong on arrival.**

Anchor on the payload: it starts at the first **token run** — a
`key=value<delim>` pair followed by at least one more `=`. Linear scan, not a
regex:

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

Requiring a real delimiter matters: **27,920-48,930 lines per slice contain `=`
but no delimiter** and are prose, not payload.

**The invariant that catches all three regex defects at once: after stripping,
the head must contain no token run** — `payload_start(head) < 0`. Reproduce the
brief's result: **0 failures over ~800k lines across four slices.** Also
reproduce the **0.71 µs/line** throughput.

### Rule 2 — strip the trailing suffix

Content after the payload's last delimiter is a suffix, not a field:
**4.28-7.29% of payload lines** carry one, over **1-24 distinct families**,
dominated by a single `)`. Worth almost nothing in bytes
(**0.0015-0.0044% of body**) — take it for **dedup, not size**: some suffixes
carry variable data (an id, a name) that breaks byte-identity between two copies
of the same payload. It moves dedup **+0.16 to +1.23 points**. Payload is
`body[start : last_delimiter + 1]`.

### Rule 3 — direction. This one is correctness, not savings.

**Direction-blind dedup silently collapses an inbound message with its outbound
copy.** Consecutive pairs with identical payload but *opposite* direction, per
200k slice: **feed C 13,431 / 17,241, feed A 10,164, feed B 6,516.** Distinct
business events; they must not merge.

Two configurable keyword families, **leftmost match wins** — the verb of the log
statement, not a noun inside it. Composite prose matching both families is real
(327-948 lines per slice, over 3-434 distinct prefixes), which is exactly why
position decides:

```python
INBOUND = re.compile(rb"(?i)receiv|inbound|\bin\b|from|sroute|\bread\b")
OUTBOUND = re.compile(rb"(?i)send|outbound|\bout\b|push|writ|emit|publish")

def direction_of(head: bytes) -> str:
    into, out = INBOUND.search(head), OUTBOUND.search(head)
    if into and out:
        return "in" if into.start() < out.start() else "out"
    return "in" if into else "out" if out else "none"
```

### Rule 4 — consecutive-duplicate drop, and the cache that must not be built

Compare each row's `(direction, payload)` to the previous and skip when equal.
**One previous payload — do not build a per-thread cache.** Measured: per-thread
buys a few points on feed C and ~0.02 on A and B, against a dict holding one
payload per live thread with **~21,625 distinct threadnames in 200k feed-A lines**
(327,986 across a day). At ~4.5 KB per payload that is an unbounded growth term
of tens to hundreds of MB — in a brief whose entire point is peak RSS. If you
want the per-thread points anyway, the only acceptable shape is a bounded LRU
(N = 64-256) keyed on the payload's **16-byte hash**, not the payload, so
retained state is O(N × 16 B). Measure it; do not assume it wins.

## Reproduce this table

| feed / slice | prefix bytes | dup, direction-blind | **dup, direction-aware** | crossed pairs |
|---|---:|---:|---:|---:|
| C head 200k | 0.69% | 47.74% | **35.86%** | 13,431 |
| C mid 200k | 0.70% | 46.51% | **31.12%** | 17,241 |
| A head 200k | 0.90% | 12.81% | **3.93%** | 10,164 |
| B head 200k | 2.21% | 11.35% | **5.71%** | 6,516 |

Percentages are of total `body` bytes. **Direction-aware is the number W3·G
implements against**; the direction-blind column exists only to show the ~9-15
points a naive strip would wrongly collapse.

## Deliverable

A scratch script plus a measurement report under `g-prototype/`: the table above
reproduced on your slices, the `payload_start(head) < 0` failure count, the
µs/line throughput, and a go/no-go on each rule. **No `src/` edits, no flags, no
schema change** — W3·G does that, using your numbers.

---

<!-- file: prompts/W1-trunk-H-I1.md -->

# W1 — TRUNK: item H (vectorized phase + row phase) and item I1 (delete what it replaces)

**Runs:** after W0·4 (`from_arrow_batch`) and W0·3 (dead-code deletion) merge.
**Blocks:** W2·A, W2·B, W2·C, W3·G. Nothing else may touch `rekep/text/` while
this is in flight.
**Owns:** `rekep/text/text_file.py`, `rekep/text/message.py`,
`rekep/text/entries.py`, `tasks/parse_messages/*`, plus one sentence in
`AGENTS.md`.

Read `BRIEF.md` §H, §I1, §B3 and §B4 in full, then `00-STANDING-RULES.md`. **Read
§H before §A** — the brief is explicit that implementing B3/B4 before H is wasted
work, because H deletes them.

## The cause you are fixing

Today one vectorized pass does header extraction, entry tokenisation, protocol
classification, XML and referential re-parsing, and identity assignment, so
**every stage sees a full-width column even when it applies to 0.28% of rows**.
B3's redundant scan, B4's two unconditional `if_else` copies and E2's 6.95x
transient are three symptoms of one cause: **per-row logic done with whole-column
kernels.** The enrichment that matters is **sequence-dependent** (dedup against
previous, carry a field forward, number versions). No Arrow kernel expresses that.

## The shape already exists on the market side — port it, don't invent one

| anchor | role |
|---|---|
| `market/event.py:316-322` `from_arrow_reader(source) -> Iterator[Self]` | reader in, row objects out, lazily |
| `market/event.py:324-343` `into_arrow_reader(events, batch_row_size=65_536)` | rows in, bounded batches out |
| `market/event.py:433-455` `with_previous(previous) -> Self \| None` | returns `None` at `:449` when the version changes no stored fact — G's "skip if same as previous", already written and tested |
| `market/fix.py:737` `FixEvents.__iter__`, `:1157-1163` `_finish` | the driver: one input row, zero or more output rows |

## Phase 1 — vectorized, returns a reader

`text/text_file.py` keeps the per-line `HEADER_PATTERN` match (a regex over a
variable-length line stream is the one Python loop that cannot be vectorized
away, and it is already the cheapest place to stand) and later gains item G's
strip regexes. It emits **only what the header says**: `unix`, `threadname`,
`plugin`, `level`, `body`, `sourceurl`. **No entries, no protocol, no XML
re-parse, no identity.**

Return an `OwnedRecordBatchReader` (`arrow_reader.py:53`) — the class exists,
`text_file.py:492-514` already builds one, and AGENTS.md requires *"Primary APIs
use `RecordBatchReader`"*.

Measured: those flat columns are **1,583 B/row of the batch's 3,916**. Everything
phase 1 does not produce is **2,333 B/row (60%) that never enters the row phase.**

## Phase 2 — two methods on `Message`, exactly as named

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
omitted, compare against `previous`, yield or skip. **One row's state, not one
batch's.** `enrich_batches` walks the reader batch by batch, turns each into row
objects (via `from_arrow_batch` from W0·4), feeds them through
`enrich_messages`, accumulates output rows to the byte bound, emits.

## Re-batch on bytes, not rows — this is the resampling

`nbytes` appears in **exactly one place in the whole package**,
`text_file.py:1315-1336`. Every other bound counts rows or batches. A row count
cannot bound memory here:

- Across the 78 baseline files, bytes/row spans **540 to 4,318 — 8.0x** at equal
  row count. A 65,536-row batch is anywhere from **34 MiB to 270 MiB**.
- Within one file, consecutive 512-row windows span **840 KB to 2.67 MB — 3.2x**.

`enrich_batches` cannot build the array to decide whether to emit it, so
accumulate an estimate: **`len(body) + 8 × len(entries)`** predicts real Arrow
bytes with one calibration constant to within **±16.3% worst case** (mean ratio
2.17, stdev 0.10 over 32 windows) against 8.0x for a row count. Running sum, emit
when it crosses `batch_byte_size`, reset. Name it `batch_byte_size` — AGENTS.md
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

Baseline: mean 2.17, stdev 0.10, worst error 16.3%, windows 840,720..2,669,892 B.
Calibrate the constant on your own run; **assert worst error stays under 25%**.

## Sortedness is a contract — assert it

`enrich_messages` is only correct on time-ordered input. Phase 1 emits in file
order, which for these logs is time order, and AGENTS.md promises *"File sets open
one naturally sorted path at a time"* — but a future multi-file or
partition-parallel read would interleave and `with_previous` would silently
compare unrelated rows, producing **wrong `prevhash`, wrong version numbers,
wrong dedup**. One kernel per batch, carrying the last value across the boundary:

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

Run it over the full two-file baseline read before relying on it. **A failure
means phase 1's ordering guarantee is weaker than AGENTS.md claims — that is a
finding to report, not a check to relax.**

## I1 — delete what you replace. Not optional; part of finishing H.

The failure mode is landing H beside the old code and leaving the old code
reachable — behind a flag, behind an `if`, or just imported and unused. **Then
peak RSS does not move**, because the pessimistic path is still what runs in some
configuration, and the next person cannot tell which one is the contract.

- **B3, the `null_values` batch scan — delete the scan, not just its call.**
  `normalized_arrow` (`text/entries.py:216-241`) materialises two full copies of
  `value` (`utf8_trim_whitespace` then `utf8_lower`) — the single largest thing in
  the schema at 881 B/row — to answer a yes/no that almost always returns "keep
  everything", with the `compute.all` early exit happening *after* both copies:
  ~1762 B/row transient, **~110 MiB per 65,536-row batch**. Once enrichment sees
  rows, the null test happens per row on the value already in hand. Remove the
  scan and every helper that existed only to feed it. **Do not leave it as a
  "fast path".**
- **B4, the two unconditional `entries` copies — delete both.**
  `text/message.py:461` and `:466` are `compute.if_else` on a nested type, which
  has **no all-false short-circuit**: measured 0.99x (one full copy) on a real
  ENTRIES array and **+46.3 MB on a 22 MB-body batch whose `xml` mask was entirely
  `False`** — ~3846 B/row transient, ~240 MiB per batch, to overwrite rows that
  are 3.47% (XML) and near-zero (referential) of the batch. Also
  `xml_payload_arrow` (`text/entries.py:313`) and `referential_payload_arrow`
  (`:346`) early-exit only on `rows == 0` (`:322-324`), not on an empty
  *selection*. **Not "skip when unnecessary" — delete.** If the row phase owns
  entries, nothing upstream should be materialising a second copy to be safe.
- **One code path per stage.** Need the old behaviour to compare against? Compare
  against `git stash`/a branch, not a runtime flag.
- **Delete, do not deprecate.** No `_legacy_*`, no `warnings.warn`, no
  commented-out block. Private path inside `text/`; no external caller.
- **Grep for orphans afterwards.** Every import, module-level constant and helper
  with exactly one call site in the deleted code is now dead too. **Deleting the
  caller and keeping the helper is the most common way this item gets half-done.**

Semantics that must survive the deletion: an entry is dropped iff its `value` is
null, or its `value` after `utf8_trim_whitespace` + `utf8_lower` is in the
`null_values` set. Error columns must stay correct — `parse_errors` /
`referential_errors` full-length and all-null in a skipped case, since
`_merge_error_columns` (`:467-468`) consumes them.

## Checks

**1. Deleted names resolve nowhere:**

```python
import subprocess
for name in DELETED:
    hits = subprocess.run(["git", "grep", "-n", "--", name],
                          capture_output=True, text=True).stdout
    assert not hits, f"{name} still referenced:\n{hits}"
```

**2. The restructure changed nothing observable.** Per file, full row set, against
`results/baseline/` — never the whole warehouse (that load is the 10.9 GiB OOM):

```python
for name in sorted(set(base_table.schema.names) & set(new_table.schema.names)):
    assert base_table.column(name).equals(new_table.column(name)), f"{name} changed"
assert base_table.schema.names == new_table.schema.names, "column set or order changed"
```

`equals` on a ChunkedArray compares values and **ignores chunking** — verified:
`chunked_array([[1,2,3,4,5,6]]).equals(chunked_array([[1,2],[3,4,5],[6]]))` is
`True`, one differing value makes it `False`. So a different batch boundary passes
and a different value does not — exactly the distinction this item needs.

**3. Entries-per-row unchanged** (a total can match while the distribution
shifts):

```python
base, new = entry_lengths(BASE_FILE), entry_lengths(NEW_FILE)
assert len(base) == len(new) and pc.all(pc.equal(base, new)).as_py(), "entries-per-row changed"
```

**4.** `./bench.sh h` + `./compare.py baseline h`; `read`/`written`/`skipped`
unchanged; on-disk parquet bytes ~unchanged.

## AGENTS.md — settle the house rule rather than leaving it ambiguous

AGENTS.md: *"Never use a Python row loop for an Arrow shape conversion. Column
comprehensions are fine."* That rule is about **shape conversion** — casting,
restructuring, projecting — and it still holds here: phase 1 is vectorized,
`from_arrow_batch` reads member-by-member off declared types with nothing inferred
per row, and `struct_array` already builds columns with one list per member.
Sequence-dependent enrichment is not a shape conversion and has no kernel. If you
agree after reading it, **add one sentence to that bullet saying so**, rather than
leaving the next agent to guess whether `enrich_messages` violates house style.

## Deliverable

Commits for phase 1, phase 2, the sortedness assertion, and the I1 deletions —
separately, in that order. Report which of the 2,333 B/row actually stopped
entering the row phase.

---

<!-- file: prompts/W2-A-entries-types.md -->

# W2·A — Item A: narrow the `entries` element types (biggest retained lever, ~1200 B/row)

**Runs:** after W1 (H) merges. Parallel with W2·B and W2·C.
**Owns:** `rekep/entries.py`; the `struct_field` consumers in
`rekep/text/fixmsg_arrow.py` and `rekep/fix/*`.
**Do not touch** `rekep/text/message.py` column assembly (W2·B) or
`rekep/text/entries.py` payload internals (W2·C).

Read `BRIEF.md` §A and §A2's opening (so you know where to stop), plus
`00-STANDING-RULES.md`. **Read §H first if you have not** — this lands on the
post-H shape.

## Target

`ENTRIES` at `rekep/entries.py:311`; `Entry` scalar at `:36-54` (`tag: int` via
`TAG = pyarrow.int32()` at line 15, `key: str`, `value: str`, `comp: str | None`).
Use the **narrowest dictionary index that fits**:

| field | now | proposed | B/row |
|---|---|---|---:|
| `tag` | `int32` | `uint16` | 174 → 87 |
| `key` | `string`, 896 distinct | `dictionary(int16, string)` | 684 → ~87 |
| `value` | `string`, 47k distinct | `dictionary(int32, string)` | 881 → 454 |
| `comp` | `string`, 100% null | `dictionary(int8, string)` — **not deletion; that is A2** | 179 → ~49 |

`entries` is 1923 B/row, **50.8% of the whole row**. Rebuilt in memory, this plus
the low-cardinality top-level columns takes the row 3788 → 2495 B/row (34%), and
`entries` alone 1923 → 758 (2.5x). **Narrowing index widths should beat that.**

## Exploit rather than fight

The parse stage **already folds keys to a dictionary and throws it away** —
`text/message.py:133-138`: *"a key column is read through its distinct spellings,
so one `take` off the folded dictionary gives every entry the code of the field it
spells"*. **Storing the folded form is the cheap path, not an extra one.**

## Caveats to handle, not ignore

- **`comp`**: populated only by the referential/XML paths
  (`text/entries.py:435-470`, `:616-624`); always null for FIX logs. An all-null
  `string` still pays ~4.1 B/element, `int8`-indexed ~1.1 B. **Confirm `int8` fits
  the referential paths' distinct-`comp` count, else `int16`.** Deleting the
  member outright is item A2 — out of scope here, and it has a real blocker.
- **`uint16` for `tag`**: standard FIX tags fit but custom tags can exceed 65535.
  The regex at `rekep/entries.py:16` is `^[0-9]{1,9}$` — **up to 9 digits**. Do
  **not** silently truncate: widen to `uint32` (saves nothing vs int32) or reject
  out-of-range tags with a clear error. **Prefer correctness over the 87 B/row.**
- **`ENTRY_PARTS`** (`:314`) derives from the type, and consumers use
  `compute.struct_field(..., "comp")` (`text/fixmsg_arrow.py:45,166`;
  `text/entries.py:306`). **Dictionary-typed struct children change what kernels
  accept** — expect decode/encode at kernel boundaries and check every
  `struct_field` consumer.

## Check 1 — the representation actually narrowed (also covers C, D)

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

Baseline on the largest file: `tag int32 173.7`, `key string 684.3`,
`value string 881.5`, `comp string 179.2`, total `1922.6 B/row over 71132 rows`.
These must move toward the table above **and nothing else should**.

> **If a child's type changed but its bytes/row did not, the dictionary is being
> decoded before it reaches the writer. Find that site — it is the whole failure
> mode for item A.**

## Check 2 — no value was mangled

Dictionary encoding must be a **pure representation change**. Per file, only on a
`BENCH_LIMIT`-bounded run (this materialises Python lists of every entry):

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

The `.cast()` makes it representation-independent: `dictionary(int16, string)` and
plain `string` holding the same values compare equal. `entries_digest` asserts
**content**; `entries_footprint` asserts **layout**. **Both must pass.**

## Check 3 — on-disk bytes did not move

Parquet already dictionary-encodes and ZSTDs these columns. An in-memory type
change should leave written bytes ~unchanged. A large on-disk delta means you
changed what is stored, not how it is held.

## Then re-check finding 1

`_windowed_batches` currently degenerates to a pass-through because a batch cut at
64 MiB raw already exceeds 64 MiB Arrow on arrival. **It starts copying the moment
per-row cost drops from this item.** Re-run the batching path after A lands and
hand the result to W2·C, who owns the budget fix (item E).

## Deliverable

One commit, `A: narrow entries element types`, with before/after
`entries_footprint` output and the `./compare.py baseline a` result. State
explicitly whether you stopped at the dictionary for `comp` — **that is a
legitimate outcome**, and A2 is a separate, gated decision.

---

<!-- file: prompts/W2-B-column-widths.md -->

# W2·B — Items B, C, D: column widths outside `entries`

**Runs:** after W1 (H) merges. Parallel with W2·A and W2·C.
**Owns:** `rekep/text/message.py` → field declarations and `columns[...]`
assembly; `rekep/text/text_file.py` → `_constant_column` (`:1635`) and its call
site (`:781`).
**Shares two files with W2·C at different symbols** — W2·C owns
`_body_text_arrow` and the `held_bytes` budget. Stay off those.

Read `BRIEF.md` §B, §C, §D and `00-STANDING-RULES.md`.

## B — `sourceurl`: 69 B/row for one distinct value per file

Built at `text/text_file.py:781` via `_constant_column(count, self.url)` (helper at
`:1635`), declared plain `str` at `text/message.py:171`. **One distinct value per
file** — the ideal dictionary case: one entry, N indices → **69 → 4 B/row**.

**Read `_constant_column`'s docstring first.** It documents a deliberate
`take`-vs-`repeat` tradeoff with measured timings and notes *"these bytes are
written to a store"*. **Keep that reasoning intact** — if your change invalidates
it, update the docstring in the same commit rather than leaving a stale
justification.

## C — Low-cardinality top-level columns (~3.3%, 8.4 MiB/file measured)

Dictionary-encode:

| column | type now | distinct | B/row |
|---|---|---:|---:|
| `level` | string | 2-4 | 8.6 |
| `plugin` | `fixed_size_binary[16]` | 15-28 | 16 |
| `protocol` | `fixed_size_binary[16]` | 6 | 16 |
| `msgtype` | string | 14-44 | 10.8 |
| `threadname` | string | 8,866/file | 31.5 |

> **`threadname` is the careful one.** 327,986 distinct across the day means a
> per-file dictionary is fine, but a **process-lifetime dictionary or intern cache
> keyed on `threadname` would itself be an unbounded growth term. Do not add
> one.** (Item G's rule 4 hits the same trap from the other side: ~21,625 distinct
> threadnames in 200k feed-A lines.)

## D — All-null columns paying full width (111 B/row, 2.9%)

`prevhash` `fixed_size_binary[16]` costs the full **16.1 B/row at 100% null** —
fixed-size-binary allocates its values buffer regardless of validity.
`expunix` / `snapunix` / `prevunix` `int64` cost 8.1 B/row each at 100% null.

**Lowest priority in the whole brief; only worth it if it falls out of other
changes.** Do not spend the schema risk here for 111 B/row — if it is not nearly
free, say so and skip it.

## Checks

1. **Per-column bytes/row moved, and only where intended** — `compare.py` reports
   this directly from the written parquet. Every column you did not name must be
   unchanged.
2. **On-disk parquet bytes ~unchanged.** These columns are already
   `RLE_DICTIONARY`-encoded on disk (`plugin`, `level`, `sourceurl`, `threadname`
   verified `dict=True`). **The waste is entirely in-memory** — a large on-disk
   delta means you changed what is stored.
3. **Values identical to baseline**, per file, against `results/baseline/`:

```python
for name in sorted(set(base_table.schema.names) & set(new_table.schema.names)):
    assert base_table.column(name).equals(new_table.column(name)), f"{name} changed"
assert base_table.schema.names == new_table.schema.names, "column set or order changed"
```

`equals` compares values and ignores chunking, so a different batch boundary
passes and a different value does not.

4. `./bench.sh <label>` + `./compare.py baseline <label>` per item;
   `read`/`written`/`skipped` unchanged.

## Deliverable

Up to three commits — `B:`, `C:`, `D:` — each with its own measurement. If D is
not nearly free, one line in the report saying it was skipped and why beats a
risky commit for 2.9%.

---

<!-- file: prompts/W2-C-body-and-parse.md -->

# W2·C — Items B2, E, E2: the body cast, the byte budget, and the parse transients

**Runs:** after W1 (H) merges. Parallel with W2·A and W2·B.
**Owns:** `rekep/text/message.py` → `_body_text_arrow` (~`:546-560`);
`rekep/text/entries.py` → `payload_arrow*` internals (~`:880-920`);
`rekep/text/text_file.py` → the `held_bytes` budget (`:657`) and
`_windowed_batches` (`:1280-1340`).
**Shares two files with W2·B at different symbols** — W2·B owns the column
declarations, `columns[...]` assembly and `_constant_column`. Stay off those.

Read `BRIEF.md` §B2, §E, §E2 and the "two structural findings" #1, plus
`00-STANDING-RULES.md`.

## B2 — the UTF-8 fallback cliff (cheap fix, confirmed on real data)

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

**A single invalid-UTF-8 body anywhere in a batch sends the *entire batch* through
`to_pylist()`** — a full Python `bytes` plus a full `str` per row, plus list
overhead, **none of it visible to the Arrow pool**.

Measured: **537 rows of 2,880,313 have invalid UTF-8 bodies (0.0186%)**, poisoning
**9 of 78 batches (11.5%)** through the Iceberg scan reader or **10 of 101 (9.9%)**
through parquet `iter_batches` — the same 537 rows either way. At ~1415 B/row of
`body` an affected batch materialises **well over 100 MiB of transient Python
heap**. Very likely the mechanism behind the RSS floor ratcheting between commits
— CPython's arena allocator does not reliably return memory after millions of
small objects.

**Fix:** identify the invalid rows vectorised and repair only those, keeping the
fast `cast` for the rest. **Do not change output values** — `"replace"` semantics
for the genuinely-invalid rows must be preserved exactly, since those 537 bodies
are already in the stored table.

Also: `body` is `large_binary` (64-bit offsets, 8 B/row) rather than `binary`
(32-bit, 4 B/row). **Check whether any single body can exceed 2 GB**; if not,
`binary` halves the offset buffer.

**Check — the fallback stopped firing batch-wide.** The bad-row count must stay
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

`measure.py` reports `UTF8_FALLBACK_BATCHES` / `UTF8_BAD_ROWS` — both must fall to
zero fallback batches with the bad-row count intact.

## E — `batch_byte_size` means two different things in the same file

Config: `batch_byte_size: 67108864` (64 MiB), `batch_row_size: 65536`. At
3788 B/row a real batch is **237 MiB — 3.7x the configured budget.** One name, two
units:

- `text/text_file.py:657` — `elif held_bytes > batch_byte_size and len(rows) > 1:`
  counts **raw input bytes** in the Python row list.
- `text/text_file.py:1315, 1321, 1328, 1336` — the same parameter compared against
  Arrow `nbytes` in `_windowed_batches`.

They differ ~3.5x. Consequences: **the one knob an operator reaches for does not
bound the thing that costs memory**; and `_windowed_batches` degenerates to a
**pass-through** — a batch cut at 64 MiB raw already exceeds 64 MiB Arrow on
arrival, so `held_bytes >= batch_byte_size` is true on the first run and `_one()` /
`concat_arrays` at `:1376` never coalesces.

**Fix:** make the budget measure produced Arrow bytes, or split into two
honestly-named parameters. AGENTS.md requires size parameters to state their unit.

> **Re-check after W2·A lands.** The pass-through is harmless today but **starts
> copying the moment per-row cost drops from item A.** Coordinate: A's owner is
> re-running the batching path and handing you the result.

This is what makes the whole effort *operable*: after A–D the per-row cost drops,
and an honest byte budget converts that into a **predictable ceiling instead of a
bigger batch**.

## E2 — transient copies inside FIX payload parsing (~6x body, per batch)

Everything else is *retained* footprint; this is **transient, additive to it.**
Parsing one batch allocates roughly **6 full copies of `body`** before the writer
sees it. `payload_arrow_with_diagnostics` peaks at **6.95x body in Arrow, 8.33x in
RSS, retaining 1.95x.** Per-step retained multipliers:

| line | code | xbody |
|---|---|---:|
| `text/entries.py:890` | `tokens = split_payload_arrow(body, separator)` | 1.20 |
| `text/entries.py:891` | `parsed = compute.extract_regex(tokens.values, _TOKEN)` | 1.41 |
| `text/entries.py:905` | `compute.filter(struct_field(parsed, "value"), matched)` | 0.95 |
| `text/entries.py:904` | `compute.utf8_trim_whitespace(...)` | 0.94 ← **2nd full copy of every value** |
| `text/entries.py:914` | `Entry.structure_arrow(keys, values)` | 0.99 |
| `text/entries.py:895` | `keys = compute.filter(keys, matched)` | 0.45 |

All live simultaneously at 8.06x. **The filter-then-trim pair at `:904-905` looks
fusable or reorderable to drop one full copy of every value** — that is the lever.

**Leave these alone:** `build_list` and `scattered` (`fields/arrays.py:60-61`),
`StructArray.from_arrays` (`:913`) and `ListArray.from_arrays` (`:916`) are
genuinely zero-copy. The unmatched-token **diagnostics cost only +0.08x**
(6.87x → 6.95x) — they are **not** the problem; do not remove them for memory.

**Two caveats that decide your order of work:**
- Those multipliers are from **synthetic** bodies (65,536-row SOH FIX batch,
  34.18 MiB body, 2.36M child entries), so protocol mix and entries/row are
  guesses. **Re-measure on a real slice.** Scripts in `/tmp/mema/` (`one.py`,
  `steps.py`, `scale.py`) if they survive.
- A corroborating harness on real code attributes its last three steps to
  `message.py:461`, `:466`, `:469` — **two of those three are item B4/H, already
  deleted by W1, not this item.** Re-measure post-H or you optimise against a
  number someone else has already moved.

**Check — the transient peak actually dropped.** Whole-run peak RSS mixes retained
and transient; to attribute a change, sample the pool around a single batch:

```python
pool = pyarrow.default_memory_pool()
before = pool.max_memory()
out = payload_arrow(body, separator)
print(f"peak delta {(pool.max_memory() - before) / body.nbytes:.2f}x body")
```

`max_memory` is **monotonic — read deltas, never absolutes** — and run one case per
subprocess or earlier cases mask later ones. **Beat 6.95x body.**

## Deliverable

Three commits — `B2:`, `E:`, `E2:` — each measured separately. For E2, report the
**real-slice** multipliers, not the synthetic ones.

---

<!-- file: prompts/W3-A2-comp-removal.md -->

# W3·A2 — GATED: remove `comp` from the `Entry` struct (179 B/row, one real blocker)

**Gate 1:** W2·A (item A) must have landed and been measured.
**Gate 2:** someone must decide the schema evolution is worth **~49 B/row beyond
what A already recovered**. **"We stopped after A" is a legitimate outcome and
must be written in the report. Half-landing this is not.**
**Owns:** `rekep/entries.py`, ~40 read sites, ~50 test functions,
`schemas/rekep/*.yaml`, `docs/`, `data/fix/fields/000030.json`,
`fix/registry.zip`.

Read `BRIEF.md` §A2 **in full** before writing anything, plus
`00-STANDING-RULES.md`.

## The measurement, and the honest cost

`comp` is `string`, nullable, field index 3 of the entry struct (`entries.py:54`).
On these feeds it is **100% null: 3,089,398 of 3,089,398 entries, zero distinct
non-null values**, and it still costs **179.2 B/row — 9.3% of `entries`, 4.7% of
the whole row**, because an all-null Arrow string array still pays its offsets
buffer. Deleting it takes `entries` from 1922.6 to 1743.5 B/row.

**But item A's `dictionary(int8, string)` already recovers ~73% of those bytes at
near-zero risk.** Full removal buys **~49 B/row more** (~2.5% of `entries`) and
costs a schema evolution, two regenerated contract YAMLs, and ~50 test functions.
**The incremental win is small and the blast radius is not.**

## Do this check FIRST — it is the whole decision

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

**Zero is the only acceptable result, and you will not get zero until the
referential prefix carries an index.** Every non-zero row is data the removal
would destroy.

> **Run it across FIX, XML *and* referential fixtures.** On a FIX capture `comp` is
> 100% null and this check passes **vacuously** — that is exactly the trap.

## The premise most people start from is wrong

"Downstream FIX parsing can rebuild component context from the field registry plus
key parsing." **Audited; it does not hold:**

- `fix/registry.py` (`component()`, `components()`, `group_count_tags()`,
  `repeating_groups()`) declares *which* fields sit in which component and which
  tags open a group. It holds **no occurrence index**. `[0]` vs `[1]` is
  per-message runtime data with no registry representation.
- The registry is never *asked* about a component path today: `fix/access.py:196-201`
  and `_KEY_TAIL` (`:496-501`) resolve on the terminal name with lead and index
  **stripped**.
- The writers **move** the prefix out of `key` rather than copying it
  (`structure_arrow` `entries.py:275-308`, `_key_parts` `:317-322`):

| input spelling | stored `key` | stored `comp` |
|---|---|---|
| `NoPartyIDs[0].PartyID` | `PartyID` | `NoPartyIDs[0]` |
| `Instrument.NoSecurityAltID[0].SecurityAltID` | `SecurityAltID` | `Instrument.NoSecurityAltID[0]` |
| `Strategies[0].NoLegs[0].600` | `600` | `Strategies[0].NoLegs[0]` |
| `Instrument.Symbol` | `Instrument.Symbol` | *null* |

**Dropping the column as-is destroys information that exists nowhere else**: the
group name, its occurrence index, and the full ancestor chain — the occurrence
identity `market/event.py:1121` groups on, the `event[i].action[j].order[k]` tree
`fix/oms.py:19-32` reconstructs, the `TickRules[i]` ladder order
`market/instrument.py:1211` rebuilds, and the scoped-vs-root disambiguation at
`fix/components.py:343-347`. **Do not delete the column and leave the writers
alone.**

## The version that works: keep the whole spelling in `key`, derive `comp` on read

Store `NoPartyIDs[0].PartyID`, not `PartyID`, and derive the split at the read
boundary. **Fully derivable by regex, no registry involved**, because
`_GROUPED_KEY` (`entries.py:18`) is a pure syntactic split:

```
(?s)^(?:(?P<comp>.*\[[0-9]+\])\.(?P<key>[^.]+)|(?P<plain>.*))$
```

Tag derivation survives (`_key_parts` rpartitions *before* `_terminal_tag`:
`Strategies[0].NoLegs[0].600` → tag 600). The read view is already comp-agnostic:
`_view()` (`:164`) concatenates the halves before matching, so `name`, `index`,
`lead`, `entry_lead`, `folded` keep working. `fix/components.py:230`
(`_INDEXED_COMPONENT`) is the second-stage splitter and is unchanged.

**The byte accounting shifts rather than vanishing: `key` gets longer.** Under item
A's `dictionary(int16, string)` that is nearly free — the distinct spellings are
the dictionary, and 896 distinct keys becoming a few thousand still fits `int16`.
**Measure `key`'s bytes/row after, not just `comp`'s. If `key` grows by more than
the 179 B you removed, A2 is a loss — report that.**

## The blocker: `comp="Referential"` has no index

`_REFERENTIAL_COMP = "Referential"` (`text/entries.py:88`) is written at **seven
sites** (`:435,437,441,459,467,470,556`) with **no `[N]`**. `ENTRY_LEAD`
(`entries.py:27`) is `\[[0-9]+\]$`, so:

```
Entry(key="InstrumentKey", value="X", comp="Referential").spelling == "Referential.InstrumentKey"
_key_parts("Referential.InstrumentKey") == (0, "Referential.InstrumentKey", None)   # comp lost
```

A merged `Referential.InstrumentKey` is **indistinguishable from a genuine dotted
proprietary key** such as `TECH.CLIENTID`. Not cosmetic:
`fix/transcribe.py:781-783` documents in-line that `TECH.CLIENTID` must not
resolve as `CLIENTID`, so collapsing the forms **silently changes registry
resolution for every referential entry.**

Resolve this **explicitly, before touching anything else**. In order of preference:

1. **Give the referential prefix an index** — write `Referential[0]` at those seven
   sites. One-line change each, syntactically indistinguishable from any other
   indexed lead, and `_GROUPED_KEY` recovers it. Costs a stored-form change for
   referential rows → needs its own before/after comparison of the referential
   test fixtures.
2. Keep a marker the split can recognise that a proprietary key cannot produce.
3. Decide referential rows may lose the prefix — **only with sign-off from whoever
   consumes them**, and only after checking `text/fixmsg.py:939,1315,1329` and
   `market/transacted.py:520-528`, which gate behaviour on `comp` being non-null.

> **Do not pick (3) by default because it is the least code.**

## What breaks loudly (fine) and what breaks silently (dangerous)

Positional 4-element `from_arrays` calls fail the moment the type is 3 wide —
`text/entries.py:301-309`, `text/fixmsg_arrow.py:160-168`,
`fix/transcribe.py:1176-1179`, `text/entries.py:913-915` (implicitly 4-wide).

These adapt on their own because they are name- or arity-driven:
`fix/transcribe.py:2137-2143` (`zip(ENTRY_PARTS, parts, strict=True)` — `strict=True`
couples `structure_arrow`'s arity to `ENTRY_PARTS`' length, so **they must change
together, and it will catch a half-done change for you**),
`fix/components.py:995-1002`, `fix/oms.py:571-575`. `ENTRY_PARTS` (`entries.py:314`)
derives from `ENTRIES.value_type`, so it shrinks to a 3-tuple by itself.

**Two failures are silent. Check these by hand:**
- `market/instrument.py:1137` — `{"key","value","comp"}.issubset(source.type.value_type.names)`
  is a structural duck-type probe. Remove `comp` and it returns `False` for every
  entries column, and the function **silently yields nothing**.
- `text/fixmsg.py:3352-3354` — `entry.get("comp")` on a `Mapping` returns `None`
  instead of raising. Same shape in the test helper at
  `tests/text/test_messages.py:1188`, which feeds `_pairs` and `_keys` and
  therefore any test using them.

## Full scope, so you can decide before starting

- **~40 read sites**: `fix/transcribe.py`, `fix/components.py`, `fix/oms.py`,
  `fix/message.py`, `text/fixmsg.py`, `text/fixmsg_arrow.py`, `market/event.py`,
  `market/fix_arrow.py`, `market/instrument.py`, `market/transacted.py`.
- **~50 test functions.** Six pin the struct shape directly and **must be updated
  first** — they tell you whether the change is coherent:
  `tests/fix/test_transcribe.py:924`, `tests/text/test_fixmsg.py:378-379`,
  `tests/text/test_message.py:100`, `tests/test_cli.py:207`,
  `tests/test_schemas.py:51`, `tests/test_docs.py:443`.
- **Two committed contract YAMLs regenerated in the same change**:
  `schemas/rekep/message.yaml:302-305`, `schemas/rekep/fixmsg.yaml:300-303` **and
  `:329-332`** — comp appears twice in fixmsg. Generated via
  `rekep fields dump --pyclass rekep.text.message:Message`; `tests/test_schemas.py:51`
  enforces agreement.
- **Executable docs**: `docs/fix/index.md:95` and `docs/products/message.md:71`
  print `entry.comp`, and `tests/test_docs.py:443` runs every python fence under
  `docs/` and diffs stdout against the following fence. Also update the prose at
  `docs/products/message.md:80-84` and the member contract table at
  `docs/fix/fixmsg.md:129-136`.
- **Committed registry data**: `data/fix/fields/000030.json:326` and the same
  member inside `fix/registry.zip → fields/000030.json` — the `Unmap`
  pseudo-field, tag 30021 (`fix/rekep.py:220`, `:248`).
- **Iceberg field ids renumber.** No `field_id` is pinned in `schemas/` (`grep -c
  field_id` is 0 for all six files, despite `docs/contracts/index.md:40` claiming
  ids are stored); they are assigned at runtime by fresh numbering
  (`iceberg/fields.py:37-63`). Removing member 3 renumbers **every field id after
  it** in any table carrying `ENTRIES`. There is no migration code and every
  contract is version 1 with no migration path (`docs/contracts/index.md:97-107`).
  **Do it in a scratch catalog, never against a shared warehouse.**

## Deliverable

Either a complete A2 — blocker resolved, lossless check at zero, all scope items
updated, `key` bytes/row measured — **or** a report saying you stopped at item A's
dictionary and why. Nothing in between.

---

<!-- file: prompts/W3-G-strip-direction.md -->

# W3·G — Item G: strip structurally, keep direction, drop repeated payloads (up to 48% of feed C)

**Runs last.** Requires W0·5's prototype numbers and W1 (H) merged. **Land it
behind default-off flags.**
**Owns:** `rekep/text/text_file.py` → the phase-1 strip; `rekep/text/message.py`
→ `vhash` identity and the direction column;
`tasks/parse_messages/parse_messages.yml` → the flags.

Read `BRIEF.md` §G in full, W0·5's prototype report, and `00-STANDING-RULES.md`.

## Why last, and what it needs that no other item needs

Everything else shrinks the **representation**. G removes **bytes and rows the
capture duplicates**. It is the biggest win available — and the only item that
**changes what is stored**, so it must not contaminate measurements against the
pinned baseline.

`[MISSING]` — the brief says G "deliberately violates hard constraints 2 and 3"
and **"needs the sign-off in its carve-out"**. The constraints and the carve-out
were not in the source screenshots. **Get them re-attached and signed off before
landing this.** If out of time overall: G alone on feed C beats A–F combined, but
only with that sign-off; the other items do not need it.

## The pattern

These feeds interleave a per-stage enrichment trace with the messages: the same
payload re-emitted after every stage, each behind different prose, microseconds
apart. Up to **fifteen lines, ~4.5 KB of payload each, one logical message.**
`HEADER_PATTERN` (`text/text_file.py:60-68`) captures `timestamp`, `threadname`,
`plugin`, `level`, then `(?P<body>.*)$` — **so all of that prose is inside
`body`.**

**Why the existing dedup cannot see them.** `text/message.py:530` is
`columns["vhash"] = hash_bytes_arrow(columns["body"])` — vhash over `body` alone,
which is right. Two problems stack:

1. The prose is *in* `body`, so two lines carrying an identical payload hash
   differently.
2. `hash` is `txhash.couple128_arrow(cls._clock_micros(...), vhash)`
   (`:531-533`) and `_clock_micros` (`market/event.py:395-405`) floors to whole
   **microseconds**. The trace lines are **7-14 µs apart**, so even with equal
   vhash they get distinct `hash` and `merge_by: true` cannot collapse them.

**So stripping is not primarily a byte saving — it is the enabler.** Strip, and
vhash becomes a true content identity.

## Implement the four rules W0·5 validated

Take `payload_start`, the suffix rule, `direction_of` and the consecutive-duplicate
rule **from the prototype report**, with its measured counts. Do not re-derive
them, and **never ship a prefix list** — the prose is not a closed set
(3,084-5,846 distinct prefixes per 200k slice) and any literal list is wrong on
arrival. The prototype's post-mortem is why: an earlier regex anchor missed
12,094 payloads on feed A, claimed 1,523 prose lines as payload, and **ate
payload** on 2,873 feed-C lines.

Carry the invariant into production tests: **after stripping, the head must
contain no token run** — `payload_start(head) < 0`, 0 failures over ~800k lines.

### Direction is correctness, not savings

**Direction-blind dedup silently collapses an inbound message with its outbound
copy** — measured consecutive pairs with identical payload but opposite direction:
**feed C 13,431 / 17,241, feed A 10,164, feed B 6,516.** Distinct business events.

Store direction as its own low-cardinality column (3 values — dictionary-encode
per item C, ~1 B/row) **and mix it into `vhash`**, so identity is
`(direction, payload)` rather than payload alone:

```python
# text/message.py:530, replacing hash_bytes_arrow(columns["body"])
identity = pyarrow.compute.binary_join_element_wise(
    direction_code, payload, pyarrow.scalar(b"", pyarrow.binary())
)
columns["vhash"] = hash_bytes_arrow(identity)
```

`couple128_arrow` (`txhash.py:88-101`) already composes an identity out of
`binary_join_element_wise` with an empty separator — **follow that precedent
rather than inventing a framing.** Use a **fixed-width code** so the join stays
unambiguous; a variable-length direction string would let `("in", "x")` and
`("i", "nx")` collide.

Keep the two keyword families **configurable**, leftmost-match-wins — the verb of
the log statement, not a noun inside it.

### Two hard "do nots"

- **Do not drop `body`.** Keep the raw line's body stored as it is today; the
  stripped payload and the direction are **derived** columns. The prose is the only
  evidence of which enrichment stage emitted a line, and constraint 1 treats `body`
  as contract.
- **Do not build a per-thread cache** for rule 4. One previous payload. Per-thread
  buys a few points on feed C and ~0.02 on A and B, against a dict holding one
  payload per live thread with **~21,625 distinct threadnames in 200k feed-A
  lines** (327,986 across a day) at ~4.5 KB each — an unbounded growth term of tens
  to hundreds of MB, **in a brief whose entire point is peak RSS**, and it
  contradicts item C's warning against threadname-keyed structures. If you want
  those points anyway, the only acceptable shape is a **bounded LRU (N = 64-256)
  keyed on the payload's 16-byte hash**, not the payload, so retained state is
  O(N × 16 B). Measure it; do not assume it wins.

## Flags

Rule 4 (the duplicate drop) goes behind an explicit flag, **default off**. These
are **the only runtime flags this whole brief sanctions**, and they exist solely
because G changes stored output. Everything else in the plan deletes what it
replaces.

With the flags off, the item must be a no-op: same rows, same `vhash`, same
`skipped`, byte-identical stored `body`.

## Target numbers

| feed / slice | prefix bytes | dup, direction-blind | **dup, direction-aware** | crossed pairs |
|---|---:|---:|---:|---:|
| C head 200k | 0.69% | 47.74% | **35.86%** | 13,431 |
| C mid 200k | 0.70% | 46.51% | **31.12%** | 17,241 |
| A head 200k | 0.90% | 12.81% | **3.93%** | 10,164 |
| B head 200k | 2.21% | 11.35% | **5.71%** | 6,516 |

**Implement against the direction-aware column.** The direction-blind column
exists only to show the ~9-15 points a naive strip would wrongly collapse.

## Checks

1. **Flags off = no observable change**, per file against `results/baseline/`
   (column-by-column `equals`, `skipped` unchanged).
2. **Flags on:** `skipped` rises by the direction-aware duplicate count and
   **nothing else moves** — every surviving row byte-identical, `body` untouched.
3. `payload_start(head) < 0` over the full slices, 0 failures.
4. No inbound/outbound pair merged — assert the crossed-pair counts above are
   preserved as **distinct** rows.

## Deliverable

Commits for the strip, the direction column + vhash identity, and the flagged
duplicate drop — separately. Report the direction-aware dedup achieved per feed,
and state the sign-off you obtained for the carve-out.

---

<!-- file: prompts/MERGE-final.md -->

# MERGE — integrate every branch, re-measure, prove it end to end, write the report

**Runs last**, after the wave tasks have pushed their branches. This is the only
task allowed to touch files across ownership boundaries, and only to resolve
conflicts.

Read `BRIEF.md` and `PLAN.md` in full, plus every wave task's report. Read
`00-STANDING-RULES.md` — they still apply, especially "never claim an improvement
you did not measure".

## 1. Merge in this order, re-running the harness between steps

`W0·3` → `W0·4` → `W0·2` → `W1` → `W2·A` → `W2·B` → `W2·C` → `W3·A2`? → `W3·G`

- **W0·3 (dead code) first**: a pure deletion in two files that W1 and W2·A later
  rewrite. Landing it first turns a three-way conflict into a no-op.
- **W0·4 before W1**: H consumes `from_arrow_batch`.
- **W0·2 (commit path) is independent** of the `text/` trunk — merge it whenever
  it is ready, but before measuring the combined peak.
- **W2·A before W2·C**: item A drops per-row cost, which is what makes
  `_windowed_batches` start copying (finding 1). W2·C's byte-budget fix must be
  measured against the post-A per-row cost, not the baseline's.
- **W3·G last**, flags off for every combined measurement.

The only expected textual conflicts are W2·B vs W2·C in `text/message.py` and
`text/text_file.py` — different symbols by design. If a conflict spans symbols
neither task owned, someone went outside their lane: find out what else they
touched before resolving.

## 2. The four things that must still be true

1. **`skipped == read - written`** on both feeds (`rekep/logs.py:156`). This is a
   merge-by-key dedup count and a correctness invariant, not a statistic. With
   G's flags off it must match the baseline exactly: A 38, B 91,614.
2. **On-disk parquet bytes ~unchanged.** Parquet already dictionary-encodes and
   ZSTDs every column; the waste was entirely in-memory. A large on-disk delta
   means someone changed what is stored. G's flags-off path included.
3. **Values identical, per file, against `results/baseline/`** — never the whole
   warehouse, that load is the ~10.9 GiB OOM:

   ```python
   for name in sorted(set(base_table.schema.names) & set(new_table.schema.names)):
       assert base_table.column(name).equals(new_table.column(name)), f"{name} changed"
   assert base_table.schema.names == new_table.schema.names, "column set or order changed"
   ```

4. **537 invalid-UTF-8 bodies still present and byte-identical.** They may not be
   fixed, dropped, or re-encoded.

## 3. Re-run every per-item check on the merged tree

Individually-passing checks can fail in combination — that is the point of running
them again here:

| check | from | catches |
|---|---|---|
| `entries_footprint` layout + `entries_digest` content | A | a dictionary decoded before the writer; a mangled value |
| entries-per-row equality | B3/H | a shifted drop distribution |
| `invalid_utf8_rows(WAREHOUSE) == 537` | B2 | the fallback silently re-firing |
| `payload_start(head) < 0` | G | over/under-stripping |
| sortedness assertion over the full two-file read | H | phase-1 ordering weaker than AGENTS.md claims |
| `git grep` for every deleted identifier | I1/I2 | a dual path left reachable |
| pool `max_memory` deltas, one case per subprocess | E2/F | a transient peak that did not actually drop |

**`max_memory` is monotonic — read deltas, never absolutes.** RSS after a large
free stays high (mimalloc keeps freed pages), so **one case per process** or an
earlier case masks a later one.

## 4. Confirm no dual paths survived the merge

I1 is the item most likely to be half-done across a merge: each task deletes its
own replaced code, but a merge can resurrect a helper. Grep the union of every
task's `DELETED` list on the merged tree. **Peak RSS not moving as much as the sum
of the parts is the symptom of a pessimistic path still running in some
configuration.**

## 5. Feed C — the real end-to-end proof

Only **after** the acceptance criteria are met. Feed C is ~34 GB, trace-dense; at
the baseline's ~2.5x-input peak-RSS ratio an unbounded run peaks **past 80 GiB**,
which is why it was excluded from the harness.

**Watch RSS live and stop the run past ~60 GiB.** A stopped run is a data point,
not a failure — report where it was and what was resident.

`[MISSING]` — the acceptance criteria were not in the source screenshots. Working
definition until re-attached: peak RSS on A and B falls materially against the
pinned baseline (A ~4.23 GiB, B ~7.25 GiB), the four invariants above hold, and
every per-item check passes.

## 6. The report

One document. Per item, using the brief's **stable letters** (A, A2, B, B2, B3,
B4, B5, C, D, E, E2, F, G, H, I1, I2):

- **What landed, what did not, and why.** "Stopped after A, did not do A2" is a
  legitimate outcome and must be stated as a decision, not omitted.
- **Measured before/after**, with the check that attributes the win to *that*
  item. Whole-run peak RSS mixes retained and transient — say which you moved.
- **Anything the brief predicted that the measurement contradicted.** The brief was
  built by prototyping first precisely because that happened repeatedly; a
  contradiction is a finding, not an embarrassment.
- **Findings to escalate**, specifically:
  - the nominal `rekep.__all__` API break from I2 (`Entry.pop_arrow`,
    `Entry.looks_structured_arrow`);
  - any sortedness assertion failure — that means phase 1's ordering guarantee is
    weaker than AGENTS.md claims;
  - whether the `_grouped_partition_chunk` `take` was actually hot or already
    short-circuiting;
  - if F broke PyIceberg's single append transaction, the explicit justification;
  - the four `[MISSING]` sections of the brief (constraints 1/2/3, G's carve-out
    and sign-off, acceptance criteria, report format) and how you resolved them.
- **The AGENTS.md sentence** W1 added about sequence-dependent enrichment vs the
  "no Python row loop for an Arrow shape conversion" rule — flag it for review;
  it is a house-style change, not a code change.

## Deliverable

A merged branch that passes the full test suite, a `./compare.py baseline final`
run for feeds A and B, the feed-C attempt result, and the report above.
