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
