# Benchmarks

Benchmarks measure reusable internal paths, not notebooks or orchestration.
Each script verifies its result before timing it; subject pages retain the
fixture and current measurements.

| page | implementation | command |
| --- | --- | --- |
| [Types](types.md) | Recursive Arrow casts | `bench_cast.py` |
| [Logs](logs.md) | Text files, folders, and parsing stages | `bench_text_file.py` |
| [FIX](fix.md) | Parsing and registry lookup | `bench_fix.py`, `bench_fix_registry.py` |
| [Market](market.md) | Identities, conversion, and book folding | `bench_market.py` |
| [Iceberg](iceberg.md) | Reads, writes, merges, and maintenance | `bench_iceberg.py` |

```bash
cd python
uv run python benchmarks/bench_cast.py --quick
uv run python benchmarks/bench_text_file.py --quick
uv run python benchmarks/bench_fix.py --quick
uv run python benchmarks/bench_market.py --quick
uv run python benchmarks/bench_iceberg.py --quick
```

Use warm repeated runs, retain adverse configurations, and report the counts a
reader pays for—planned files, manifests, requests, and rows—beside elapsed
time. Raw workflow warehouses are temporary; the compact measured fixture is
recorded on [End-to-end run](workflow-run.md).

The market benchmark keeps its quick replay matrix small. Pass `--matrix` for
the selected 10K/100K/1M event cross-section over 10 to 10K live levels and 1,
10, or 100 orders per level. Its representative fold also reports identity
calls, copies, standing probes, heap operations, sorting, snapshot
materializations, skipped expiry scans, emitted payloads, and peak traced
memory.

## Latest quick run

Measured 2026-08-23 on Windows 11, Python 3.12.13, and PyArrow 25.0.1. These
figures are directional; the scripts assert their outputs before timing them.

| path | fixture | result |
| --- | ---: | ---: |
| Text log, plain | 50,000 rows | 89,098 rows/s |
| Text log, gzip | 50,000 rows | 100,677 rows/s |
| FIX wire parser | 10,000 rows | 92,628–198,324 rows/s |
| Normalized Instrument Log decode | 500 rows | 912 → 17,544 rows/s; 19.2x |
| Recursive Arrow reshape | 50,000 rows | 109.8M rows/s |
| Book summary, 10 levels/side | 200 books | 99,640 books/s |
| Stateful book fold | 10,000 events | 1.958 s; 5,105 books/s |
| Replay shape matrix | 1,000-2,000 events | 3,950-5,297 events/s |
| Large replay, 100 levels x 10 orders | 10K / 100K / 1M events | 4,815 / 4,330 / 4,504 events/s |
| Snapshot / recovery | 2,000 orders, 100 levels | 54.2 ms / 15.6 ms |
| Iceberg append | 5,000 rows, 4 partitions | 15,097–18,490 rows/s |
| Iceberg merge, all new | 5,000 rows | 12,301–13,387 rows/s |
| Iceberg merge, half stored | 5,000 rows | 410–412 rows/s |

The complete text parser clears the 50k rows/s first-layer target. Exact
half-stored Iceberg upserts remain the clearest scale-up target; append and
monotonic insert should be preferred when their semantics fit.

Package-authored instrument rows now decode their promoted fields and ordered
pair buffer directly. External and legacy rows still use the FIX registry;
the measured normalized path fell from 1,095.9 to 57.0 µs/row.

On the same 10,000-event fold, replay improved from 9.762 s (1,024 books/s) to
1.958 s (5,105 books/s), about 5.0x. A changed bid now rebuilds only bid levels
and bid summaries; ask values are carried from the preceding Book before
cross-side prices are derived. The same optimization applies in the other
direction. Book identity now additionally includes every ordered live Order
hash, so dense-book throughput also measures that required linear identity
input; duplicate-event shortcuts avoid the walk when no Book is emitted.

The [notebook smoke run](workflow-run.md) exercises all five stages, all three
log routes, registry-backed instrument enrichment, book recovery rows,
auditable rejection, and a zero-write replay. The bounded market benchmark
separately rejected 10 of 200 malformed events and emitted 70 auditable
`INTERNAL_EXPIRED` deltas while enforcing `max_side_alive=10`.
