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
