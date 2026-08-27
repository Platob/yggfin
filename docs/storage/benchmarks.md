# Benchmarks

Benchmarks measure reusable internal paths, not notebooks or orchestration.
Each script verifies its result before timing it; subject pages retain the
fixture and current measurements.

| page | implementation | command |
| --- | --- | --- |
| [Types](../contracts/types.md) | Recursive Arrow casts | `bench_cast.py` |
| [FixMsg](../fix/fixmsg.md) | Text files, Message rows, and FIX parsing | `bench_text_file.py` |
| [FIX](../fix/index.md) | Parsing and registry lookup | `bench_fix.py`, `bench_fix_registry.py` |
| [Market](../market/index.md) | Identities, conversion, and book folding | `bench_market.py` |
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
recorded on [End-to-end run](../pipeline/operations/run.md).

The market benchmark replays three book shapes -- deep, wide, and one order a
level -- at `--rows` events each. Its representative fold also reports identity
calls, copies, standing probes, heap operations, sorting, snapshot
materializations, skipped expiry scans, emitted payloads, and peak traced
memory.

## Latest quick run

Measured 2026-08-25 on Linux 6.18, Python 3.12.3, and PyArrow 25.0.1, with
`--quick` on every script. These figures are directional and not comparable
across machines; the scripts assert their outputs before timing them.

| path | fixture | result |
| --- | ---: | ---: |
| Line to header columns | 50,000 lines | 798,965 lines/s |
| Wire parse, vectorised | 10,000 rows | 230,364–511,920 rows/s |
| Rendered parse, vectorised | 10,000 rows | 120,751–214,143 rows/s |
| Key column to tags, named keys | 112,500 keys, 6,071 names | 51.3M keys/s |
| Recursive Arrow reshape | 50,000 rows | 315.7M rows/s |
| Wire line to market events | 100 messages of each shape | 8,179 / 5,525 / 2,281 rows/s |
| Normalized Instrument FixMsg decode | 500 rows | 389.7 µs/row |
| Generic Instrument reconstruction | 500 rows | 271.8 µs/row |
| Book summary, 10 levels/side | 200 books | 404,040 books/s |
| Stateful book fold | 200 events | 17,832 books/s |
| Replay shape matrix | 1,000–2,000 events | 5,941–11,803 events/s |
| Snapshot / recovery | 2,000 orders, 100 levels | 16.7 ms / 4.4 ms |
| Iceberg append | 5,000 rows, 4 partitions | 37,332–37,819 rows/s |
| Iceberg merge, all new | 5,000 rows | 21,936–22,186 rows/s |
| Iceberg merge, half stored | 5,000 rows | 984–992 rows/s |

Exact half-stored Iceberg upserts remain the clearest scale-up target; append
and monotonic insert should be preferred when their semantics fit.

## The parse stages, remeasured

Measured 2026-08-27 on the same machine and versions as above, over
`bench_text_file.py`'s mixed capture -- 100,000 rows at 60% OTHER, 25% FIX,
15% UL, in batches of 65,536 -- with everything warm: the text stage reads
about 68,000 rows/s, the FIX stage about 50,000 rows/s, and the two together
about 29,000 rows/s. Directional figures, like everything on this page, and
recorded with their profiles because three optimization proposals were parked
pending exactly this measurement:

- **The text stage is its tokenizer.** `Kwarg.parse_arrow` is roughly three
  quarters of `Message.parse_arrow`; the classification probes
  (`_msg_type_probe`, `looks_structured_arrow`) are about a seventh.
  Collapsing the probe scans into one combined extraction -- proposed as a
  speed and coherence fix both -- would buy about a tenth here, in the
  hottest correctness-critical code, and RE2 cannot express the
  before-checksum guard in a single pass anyway: no lookahead, no per-row
  slice, so the discriminator's value and its position stay two scans.
  Parked until a real-capture profile shows the probes dominating somewhere.
- **The FIX stage is kernel-bound, not dispatch-bound.** About 85% of a warm
  batch runs inside Arrow kernels, averaging over a millisecond per call
  across roughly two thousand calls, which puts the fixed per-call wrapper
  overhead under a tenth. Group-by fragmentation grows with the number of
  distinct protocol and version groups, which this fixture keeps small;
  remeasure on a capture that carries more before acting on it.
- **What looked like the win was one-time.** The first batch through a fresh
  codec pays about two seconds of registry materialization -- the merged
  field table, the per-version declarations -- cached on the codec
  thereafter. It dominates any short profile and vanishes over a long run.
- **A bridge fast path is bounded by its own share.** The reference path
  costs this fixture about 0.5s per batch against the flat FIX path's 0.2s,
  and per field the named read is already on par with the wire one -- the
  row-rate gap between the protocols is message size. A vectorized named
  fast path is real work worth doing against a real UL-heavy capture, and
  not against this fixture.

Reproduce with `bench_text_file.capture` and `cProfile` over
`Message.parse_arrow` and `FixMsg.from_message_arrow_batch` separately, warm.

Collapsing each rule's pattern list into one alternation nearly doubled
classification on its own: 1.9x on `Rules.into_arrow_protocol_array` over the
same 65,536-row mixed batch (571,000 to 1,076,000 rows/s), measured
interleaved against the pre-change module on one machine in one process, with
the protocol answers asserted identical first. Direction resolution was
unchanged. That is more than the 1.53x a position-based combined pass
measured on real captures, and it kept first-configured-rule-wins.

A same-day rerun of every parsing benchmark's quick mode read 10-30% below
the table above across the board -- including paths no change has touched
since, such as the per-line header loop and `_tag_numbers` -- which is what
host variance looks like against what a regression looks like: a controlled
interleaved A/B on the changed path, in one process, moved the other way.
Every benchmark's own vector-against-scalar assertion held.

A key column is read through its **distinct** spellings, not its rows. A
message keys its fields out of a bounded vocabulary, so a batch of a hundred
thousand entries carries a few dozen spellings, and every scan of them --
`FixCodec.structure`, `TagIndex.resolve_with_match`, `_tag_numbers` -- runs
over the column's dictionary and is taken back across the entries. On a
captured batch that is 10x for the structuration of a wire message, 18x for a
bridge one, and 25x for resolving a bridge one's names. `_tag_numbers` is now
the fastest of the four implementations `bench_text_file.py` races it against,
ahead of a bare `pyarrow.compute.index_in` over the same keys.

The one row-at-a-time loop the parser has left -- the per-line header match --
is raced there too, and wins: 798,965 lines/s against 419,462 for one RE2 pass
with the continuations numbered by a cumulative sum and joined by a group-by.
RE2 walks an alternation of three timestamp shapes over every byte of the
capture where the loop stops at the first character of a line that is not a
header.

Everything a translation needs from a dictionary -- a name's wire tag, the tags
the shapes already store, the ones kept for audit -- is resolved once per
`(registry, version)` and read by every message (`MarketTags`,
`FieldAccess.tag_text`). Rebuilding those a message at a time was a third of
what converting one cost.

Package-authored instrument rows decode their promoted fields and ordered pair
buffer directly, and external or legacy rows still use the FIX registry.
Neither path is the faster one on this fixture: the direct decode reads about
thirty fields by name against one row's entries, and the registry path builds
the message once and translates it.

A changed bid rebuilds only bid levels and bid summaries; ask values are
carried from the preceding Book before cross-side prices are derived, and the
same applies in the other direction. Book identity includes every ordered live
Order hash, so dense-book throughput also measures that required linear
identity input; duplicate-event shortcuts avoid the walk when no Book is
emitted.

The [notebook smoke run](../pipeline/operations/run.md) exercises all six jobs, all three
log routes, registry-backed instrument enrichment, book recovery rows,
auditable rejection, and a zero-write replay. The bounded market benchmark
separately rejected 10 of 200 malformed events and emitted 70 auditable
`INTERNAL_EXPIRED` deltas while enforcing `max_side_alive=10`.
