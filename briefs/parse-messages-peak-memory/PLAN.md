# Execution plan: parallel waves, ownership, merge order

Source of truth for scope: [`BRIEF.md`](BRIEF.md). This file only says **who
runs when, on which files, and how the results come back together**.

The brief's letters (A, A2, B, B2, B3, B4, B5, C, D, E, E2, F, G, H, I1, I2) are
**stable report identifiers, not a schedule**. The tasks below repackage them
into units that can run concurrently without touching the same code.

## The one dependency that shapes everything

**H changes the shape; A–G shrink or skip what the shape produces.** H also
*deletes* B3 and B4 outright, so anyone implementing B3/B4 in parallel with H is
writing code that gets deleted on merge. That forces a trunk:

```
              ┌─ W0·1 baseline gate ────────────────────────────────┐
              ├─ W0·2 commit path      (B5, F, arrow_chunks)  ──────┤
  start ──────┼─ W0·3 dead code        (I2)                   ──────┤
              ├─ W0·4 row primitive    (from_arrow_batch)  ─┐       │
              └─ W0·5 G prototype      (measure only)  ─────┼───┐   │
                                                            ▼   │   │
                             W1 ── H + I1  (deletes B3, B4) ─┐  │   │
                                                             ▼  │   │
              ┌─ W2·A entries types    (A)             ──────┤  │   │
              ├─ W2·B column widths    (B, C, D)       ──────┤  │   │
              └─ W2·C body + parse     (B2, E, E2)     ──────┤  │   │
                                                             ▼  ▼   ▼
                             W3·A2 comp removal (gated) ──┐  │      │
                             W3·G  strip/direction (flags)─┴──┴──┐  │
                                                                 ▼  ▼
                                                            MERGE + report
```

- **W0·4 must merge before W1 starts.** H consumes `from_arrow_batch`; splitting
  the primitive out keeps the trunk task from also inventing it.
- **W0·5 produces no production code.** It validates G's content rules against
  real bytes, which is the brief's "prototype before you commit" rule made into a
  separate deliverable. Its output unblocks W3·G at the end.
- **W2·A/B/C fan out only after W1 merges.** They read the post-H shape.
- **W3·A2 is gated twice**: A must have landed *and* someone must decide the
  schema evolution is worth ~49 B/row. "Stopped after A" is a valid outcome.
- **W3·G lands last** because it is the only item that changes stored output; it
  must not contaminate measurements against the pinned baseline.

## Ownership matrix — the rule that keeps merges clean

A task may edit **only** the (file, symbol) pairs it owns. Needing anything else
is a stop-and-escalate, not a judgement call.

| task | items | owns (file → symbols) |
|---|---|---|
| **W0·1** baseline | — | `bench.sh`, `compare.py`, `measure.py`, `results/baseline/`. No `src/` edits. |
| **W0·2** commit path | B5, F | `rekep/dataset.py` → `arrow_chunks`; `rekep/iceberg/dataset.py` → `_append_arrow_reader`, `_insert_arrow_table`, `_grouped_partition_chunk` |
| **W0·3** dead code | I2 | `rekep/text/entries.py` → `pop_arrow` only; `rekep/entries.py` → `Entry.pop_arrow`, `Entry.looks_structured_arrow` only |
| **W0·4** row primitive | (H dep) | `rekep/fields/rows.py`; `rekep/fields/field.py` → `@scalar` installation lines only |
| **W0·5** G prototype | (G dep) | `briefs/parse-messages-peak-memory/g-prototype/` only. **No `src/` edits.** |
| **W1** trunk | H, I1, ×B3, ×B4 | `rekep/text/text_file.py`, `rekep/text/message.py`, `rekep/text/entries.py`, `tasks/parse_messages/*`, `AGENTS.md` (one sentence) |
| **W2·A** entries types | A | `rekep/entries.py`; `struct_field` consumers in `rekep/text/fixmsg_arrow.py`, `rekep/fix/*` |
| **W2·B** column widths | B, C, D | `rekep/text/message.py` → field declarations + `columns[...]` assembly; `rekep/text/text_file.py` → `_constant_column` and its call site |
| **W2·C** body + parse | B2, E, E2 | `rekep/text/message.py` → `_body_text_arrow`; `rekep/text/entries.py` → `payload_arrow*` internals; `rekep/text/text_file.py` → `held_bytes` budget + `_windowed_batches` |
| **W3·A2** comp removal | A2 | `rekep/entries.py`, ~40 read sites, ~50 tests, `schemas/rekep/*.yaml`, `docs/`, `data/fix/fields/000030.json`, `fix/registry.zip` |
| **W3·G** strip/direction | G | `rekep/text/text_file.py` → phase-1 strip; `rekep/text/message.py` → `vhash` identity + direction column; `tasks/parse_messages/parse_messages.yml` → flags |

**W2·B and W2·C share two files at different symbols.** That is deliberate and
mergeable, but it is the only overlap in the plan — if either finds itself
editing outside its symbol list, it stops.

## Standing rules, repeated in every prompt

1. **Prototype before you commit.** Any rule that inspects log content gets built
   throwaway, run over the sample slices, counts printed — *then* written for
   production. A rule that has not been run over real bytes is a hypothesis.
2. **One commit per item**, message prefixed with the brief's letter
   (`A: narrow entries element types`). Branch `mem/<letter>-<slug>`.
3. **Measure before and after each item.** `./bench.sh <label>` then
   `./compare.py baseline <label>`. Never assert an improvement you did not
   measure.
4. **Work per file, never per warehouse.** Loading the 78 parquet files as one
   table materialises ~10.9 GiB and gets OOM-killed.
5. **No rule may be coupled to a feed.** Capture filenames live only in
   `bench.sh`.
6. **Delete what you replace.** No dual paths, no `_legacy_*`, no runtime flag to
   keep the old behaviour. The only sanctioned flags are G's.
7. **In-memory changes must leave on-disk bytes ~unchanged.** That is the safety
   property, and the harness checks it.
8. **`skipped = read - written` is a correctness invariant**, not a statistic.

## Ordering inside the merge

Merge in this order; each step re-runs the harness before the next starts:

`W0·3` → `W0·4` → `W0·2` → `W1` → `W2·A` → `W2·B` → `W2·C` → `W3·A2`? → `W3·G`

W0·3 goes first because it is a pure deletion in two files that W1 and W2·A later
rewrite — landing it first turns a three-way conflict into a no-op. W0·2 is
independent of the `text/` trunk and can merge whenever it is ready.

## What "done" looks like

`[MISSING]` — the acceptance criteria were not in the source screenshots. Until
they are re-attached, the working definition is: **peak RSS on feeds A and B
falls materially against the pinned baseline, `read`/`written`/`skipped` are
unchanged, every per-item check in the brief passes, and on-disk parquet bytes
are ~unchanged (G excepted, whose flags default off).** The feed-C run is the
end-to-end proof and is attempted only after that — watch RSS live, stop past
~60 GiB.
