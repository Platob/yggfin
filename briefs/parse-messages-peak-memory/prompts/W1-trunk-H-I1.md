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
