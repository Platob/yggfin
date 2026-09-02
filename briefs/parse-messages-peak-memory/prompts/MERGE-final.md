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
