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
