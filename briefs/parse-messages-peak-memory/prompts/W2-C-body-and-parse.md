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
