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
