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
