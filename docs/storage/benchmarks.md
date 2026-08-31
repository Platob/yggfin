# Benchmarks

Reusable internal paths, not notebooks or orchestration. Each script asserts
its result before timing it.

```bash
cd python
uv run python benchmarks/bench_cast.py --quick
uv run python benchmarks/bench_text_file.py --quick
uv run python benchmarks/bench_fix.py --quick
uv run python benchmarks/bench_fix_registry.py --quick
uv run python benchmarks/bench_market.py --quick
uv run python benchmarks/bench_iceberg.py --quick
```

All six finish in about four minutes together, which is what makes running
them a normal part of a change rather than an occasion.

| page | path | script |
| --- | --- | --- |
| [Types](../contracts/types.md) | recursive Arrow casts | `bench_cast.py` |
| [FixMsg](../fix/fixmsg.md) | text files, `Message` rows, FIX parsing | `bench_text_file.py` |
| [FIX](../fix/index.md) | parsing and registry lookup | `bench_fix.py`, `bench_fix_registry.py` |
| [Market](../market/index.md) | identities, conversion, book folding | `bench_market.py` |
| [Iceberg](iceberg.md) | reads, writes, merges, maintenance | `bench_iceberg.py` |

Report the counts a reader pays for — planned files, manifests, requests, rows
— beside elapsed time, from warm repeated runs, keeping adverse
configurations.

## Latest quick run

2026-08-30, Linux 6.18, Python 3.12.3, PyArrow 25.0.1, `--quick` throughout.
Directional, not comparable across machines.

| path | fixture | result |
| --- | ---: | ---: |
| Line to header columns | 50,000 lines | 745,392 lines/s |
| Wire parse, vectorised | 10,000 rows | 175,927 rows/s |
| Rendered parse, vectorised | 10,000 rows | 53,314 rows/s |
| Key column to tags, named keys | 112,500 keys, 6,133 names | 52.4M keys/s |
| Recursive Arrow reshape | 50,000 rows | 314.4M rows/s |
| Book summary, 10 levels/side | 200 books | 304,538 books/s |
| Stateful book fold | 200 events | 10,623 books/s |
| Replay shape matrix | 1,000–2,000 events | 6,422–6,993 events/s |
| Snapshot / recovery | 2,000 orders, 100 levels | 16.9 ms / 19.3 ms |
| Iceberg append | 5,000 rows, 4 partitions | 45,623–47,007 rows/s |
| Iceberg merge, all new | 5,000 rows | 12,029–15,022 rows/s |
| Iceberg merge, half stored | 5,000 rows | 15,131–20,631 rows/s |

Append remains the fastest write by a wide margin; prefer it and monotonic
insert where their semantics fit.

### Empty-table keyed writes

2026-08-31, Windows 11, Python 3.12.13, PyArrow 25.0.1, local SQLite catalog
and filesystem warehouse. The before and after runs used the same generated
log and benchmark command.

| path | rows / partitions | before | after |
| --- | ---: | ---: | ---: |
| Fresh keyed merge, quick | 5,000 / 4 | 6,567–8,156 rows/s; 4 snapshots | 20,571–21,289 rows/s; 1 snapshot |
| Fresh keyed merge, one commit | 100,000 / 8 | 48,095 rows/s; 8 snapshots | 204,535 rows/s; 1 snapshot |
| Monotonic insert, six bounded commits | 100,000 | — | 226,128 rows/s |
| Read one partition, three columns | 1,250 of 5,000 | — | 42,765 rows/s; 1 of 4 files planned |
| Delete one partition | 1,250 of 5,000 | — | 11,745 removed rows/s; 2 files planned |
| Delete part of one file | 625 of 5,000 | — | 5,915 removed rows/s; 1 file planned |
| Delete with no match | 5,000 | — | 23 ms; 0 files planned; 0 snapshots |
| Cached store calls, one-partition read | 1,250 of 5,000 | 6 GETs without cache | 2 GETs with cache; data files only |

The changed path removes partition-recursive commits only while the selected
branch has no snapshot. Once rows exist, exact per-partition matching and
bounded rewrites remain unchanged.

### Complete message layers

2026-09-01, Windows 11, Python 3.12.13, PyArrow 25.0.1. The mixed 50,000-row
capture is 60% OTHER, 25% FIX and 15% FIXML.

| command | body to `Message` | `Message` to `FixMsg` |
| --- | ---: | ---: |
| `bench_text_file.py --quick --only messages` | 31,662 rows/s | 5,921 rows/s |
| `bench_text_file.py --rows 50000 --repeat 3 --only messages` | 26,248 rows/s | 6,316 rows/s |

Vector tokenization reaches 68,262–370,264 rows/s and tag resolution reaches
29.5M keys/s on the repeated run. The complete `Message` and `FixMsg` shapes
remain below 50,000 rows/s.

## Where the parse stages spend their time

2026-08-27, same machine, over `bench_text_file.py`'s mixed capture — 100,000
rows at 60% OTHER, 25% FIX, 15% FIXML, batches of 65,536, warm:

| stage | rows/s |
| --- | ---: |
| text | ~68,000 |
| FIX | ~50,000 |
| both | ~29,000 |

Recorded with their profiles, because three optimization proposals were parked
pending exactly this measurement:

| proposal | what the profile said |
| --- | --- |
| collapse the classification probe scans | worth about a tenth; `Entry.parse_arrow` is ~¾ of `Message.parse_arrow` and the probes ~⅐. RE2 cannot express the before-checksum guard in one pass — no lookahead, no per-row slice — so value and position stay two scans. Parked. |
| cut per-call kernel dispatch | ~85% of a warm batch runs inside Arrow kernels, over a millisecond per call across ~2,000 calls, so wrapper overhead is under a tenth. Group-by fragmentation grows with distinct protocol/version groups, which this fixture keeps small. |
| the two seconds at the front | one-time: a fresh codec materializes the merged field table and per-version declarations, then caches them. Dominates a short profile, vanishes over a long run. |
| a bridge fast path | the reference path costs ~0.5 s/batch against the flat FIX path's 0.2 s, and per *field* a named read is already on par with a wire one — the row-rate gap is message size. Worth doing against a real FIXML-heavy capture, not this fixture. |

Reproduce with `bench_text_file.capture` and `cProfile` over
`Message.parse_arrow` and `FixMsg.from_message_batch` separately, warm.

## What moved, and what only looked like it

Collapsing each rule's pattern list into one alternation nearly doubled
classification: **1.9x** on `Rules.into_arrow_protocol_array` over the same
65,536-row batch (571,000 → 1,076,000 rows/s), interleaved against the
pre-change module in one process, protocol answers asserted identical first.
Direction resolution was unchanged. That beats the 1.53x a position-based
combined pass measured on real captures, and it kept
first-configured-rule-wins.

A same-day rerun of every parsing benchmark's quick mode read 10–30% below the
table above *across the board* — including paths no change has touched, such
as the per-line header loop and `_tag_numbers`. That is what host variance
looks like against a regression: the controlled interleaved A/B on the changed
path, in one process, moved the other way, and every benchmark's own
vector-against-scalar assertion held.

## Why the parser is fast where it is

A key column is read through its **distinct spellings**, not its rows. A
message keys its fields from a bounded vocabulary, so a batch of a hundred
thousand entries carries a few dozen spellings; every scan of them —
`FixCodec.structure`, `TagIndex.resolve_with_match`, `_tag_numbers` — runs
over the column's dictionary and is taken back across the entries. On a
captured batch: **10x** structuring a wire message, **18x** a bridge one,
**25x** resolving a bridge one's names. `_tag_numbers` now beats all four
implementations `bench_text_file.py` races it against, including a bare
`pyarrow.compute.index_in` over the same keys.

The one row-at-a-time loop left — the per-line header match — is raced there
too and wins: **798,965 lines/s against 419,462** for one RE2 pass with
continuations numbered by cumulative sum and joined by group-by. RE2 walks an
alternation of three timestamp shapes over every byte; the loop stops at the
first character of a line that is not a header.

Everything a translation needs from a dictionary — a name's wire tag, the tags
the shapes store, the ones kept for audit — resolves once per
`(registry, version)` and is read by every message (`MarketTags`,
`FieldAccess.tag_text`). Rebuilding those per message was a third of what
converting one cost.

Neither instrument path is the faster one on this fixture: a package-authored
row decodes ~30 promoted fields by name against one row's entries, and the
registry path builds the message once and translates it.

A changed bid rebuilds only bid levels and bid summaries; ask values carry
from the preceding Book before cross-side prices derive, and vice versa. Book
value identity includes every ordered live Order `vhash`, so dense-book
throughput also measures that linear input; duplicate-event shortcuts skip the
walk when no Book is emitted.

The [notebook smoke run](../pipeline/operations/run.md) exercises all six
jobs, all three log routes, registry-backed instrument enrichment, book
recovery rows, auditable rejection, and a zero-write replay. The bounded
market benchmark separately rejected 10 of 200 malformed events and emitted 70
auditable `INTERNAL_EXPIRED` deltas while enforcing `max_side_alive=10`.
