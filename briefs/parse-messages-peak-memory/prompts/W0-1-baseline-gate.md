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
