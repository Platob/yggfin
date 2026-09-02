# Benchmarks

Reusable internal paths, not task applications or orchestration. Each script
asserts its result before timing it.

```bash
cd python
uv run python benchmarks/bench_cast.py --quick
uv run python benchmarks/bench_text_file.py --quick
uv run python benchmarks/bench_fix.py --quick
uv run python benchmarks/bench_fix_registry.py --quick
uv run python benchmarks/bench_fixmsg.py --quick
uv run python benchmarks/bench_market.py --quick
uv run python benchmarks/bench_iceberg.py --quick
```

All seven finish in about five minutes together, which is what makes running
them a normal part of a change rather than an occasion.

| page | path | script |
| --- | --- | --- |
| [Types](../contracts/types.md) | recursive Arrow casts | `bench_cast.py` |
| [FixMsg](../fix/fixmsg.md) | text files, `Message` rows, the boundary between them | `bench_text_file.py`, `bench_fixmsg.py` |
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
29.5M keys/s on the repeated run. That is one number for the whole FIX half;
`bench_fixmsg.py` takes it apart, and what it costs now is below.

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

Reproduce with `bench_fixmsg.py --only stages` and `--only kernels`, which
account one batch by transcription stage and by Arrow kernel and call site.
`cProfile` cannot: nine tenths of this boundary runs inside kernels, and it
reports one `pyarrow.compute` wrapper for all of them.

### What the boundary cost, and what it costs now

2026-09-01, Linux 6.18, Python 3.12, PyArrow 25.0.1, four cores, over
`bench_fixmsg.py`'s 20,000-row captures. Every change below was asserted
byte-identical first — 77 shapes across the four mixes at seven sizes, whole
and with `body` projected away, widened to `large_string`, plus hand-built
batches spanning four FIX versions, a misplaced `CheckSum <10>` that forces the
recursive best-effort split, and one batch per protocol family — before any of
it was timed.

| mix | before | after | median |
| --- | ---: | ---: | ---: |
| mixed 60/25/15 | 15,507–19,797 rows/s | 21,520–26,308 rows/s | 1.36x |
| wire FIX only | 25,099–32,707 rows/s | 29,133–38,118 rows/s | 1.24x |
| bridge FIXML only | 9,157–12,371 rows/s | 13,439–16,145 rows/s | 1.38x |
| unparsed text only | 27,328–35,022 rows/s | 39,505–49,881 rows/s | 1.44x |

Alternating runs of the two trees, `--only mix --rows 20000`, ranges as well as
the ratio because this host's spread is wider than some of the wins. On three
of the four mixes the ranges do not overlap at all — the slowest run after
beats the fastest run before. Wire FIX overlaps, and takes the flat
specialization rather than the registry path, so its 1.24x is the weakest
number here.

Where the milliseconds went, over one 20,000-row mixed batch, `--only stages`
(1,285 ms before, 915 ms after):

| stage | before | after | what changed |
| --- | ---: | ---: | --- |
| resolve per version | 371.9 ms | 268.6 ms | the split is inverted once per batch, not once per column |
| classify protocol | 248.9 ms | 258.7 ms | unchanged; see the parked row above |
| identify | 240.2 ms | 177.6 ms | a digest part that frames alike in every row is one constant |
| lift component groups | 164.6 ms | 46.7 ms | a component path is read once per distinct spelling |
| transcription errors | 85.7 ms | 34.8 ms | an empty diagnostic side is not joined row by row |
| classify direction | 31.8 ms | 19.8 ms | an anchor scans only the rows carrying its protocol |

The three call sites that led the profile are the three that moved: the
identity framing join went 100.5 ms to 63.5 ms, and `fix/components.py`'s
91.2 ms path scan and `fields/arrays.py`'s 34.9 ms of repeated
`array_sort_indices` left the top fifteen entirely. Read a single `--only
kernels` figure as indicative: two post-change runs of that sweep accounted
766 ms and 968 ms of the same batch.

What leads now is `text/entries.py`'s `_parse_style` at 67 ms over two calls,
tokenizing the bridge and text payloads — the same scan
`Rules.into_arrow_protocol_array` is parked on above, which is why classify is
the one stage that did not move. A batch still costs about 80 ms before any of
its rows do, so 256 rows run at 2,666 rows/s against 26,261 for 20,000: hand
this large batches.

Three more were priced and left alone:

| proposal | what the measurement said |
| --- | --- |
| skip the per-group `take` of wholly-null columns | 5.1 ms of the 8.2 ms those takes cost per 8,000-row mixed batch, but a dropped key changes which columns `_resolved_columns` sees, and the two groups' key sets must match or the scatter raises. 1% of the boundary for a behaviour change. |
| stop re-inferring versions in `_resolved_batch_columns` | 17.5 ms per mixed 8,000-row batch, and not a duplicate: `_versions_arrow` is passed the newly *versioned* protocols and a `_begin_strings_arrow` rebuilt from them, and the two readings can legitimately disagree. |
| a direction split on a nearly-homogeneous batch | the per-protocol split reads 0.93x where one anchored protocol takes 90% of the rows and the whole-column shortcut does not fire. Direction is 2% of the boundary, so this is ~0.2% there, and a per-category task batch is exactly that shape. Kept, because the mixed capture it is measured on is the one the pipeline reads. |

### And the text layer in front of it

2026-09-02, same host, over `bench_text_file.py`'s 40,000-row captures.
Asserted byte-identical first over 72 read shapes: the four mixes at three
batch sizes, each also unfolded, at a small read size and under a bounded row
size; an empty file, one line, gzip, CRLF, invalid UTF-8, and a line past the
row bound; and twelve reads with msgtype, regex, plugin, window, static-value
and null-value bounds set, including a *null nested* static value, which is the
one shape where taking a constant column and repeating it differ in bytes.

| mix | before | after |
| --- | ---: | ---: |
| mixed 60/25/15 | 39,552–50,153 rows/s | 57,131–73,215 rows/s |
| wire FIX only | 33,348–38,968 rows/s | 59,804–65,935 rows/s |
| bridge FIXML only | 28,994–31,335 rows/s | 43,321–54,386 rows/s |
| unparsed text only | 75,888–80,070 rows/s | 86,465–111,637 rows/s |

Medians **1.35x** mixed, 1.68x wire, 1.73x bridge, 1.33x text. No mix's two
ranges overlap — on the mixed capture that is nine readings a side, the slowest
after (704 ms) still ahead of the fastest before (798 ms) — so unlike the FIX
boundary above, this one is resolvable against the host's spread.

Stage by stage, medians of five alternating pairs over one mixed 40,000-row
capture, 936 ms to 683 ms:

| stage | before | after | what changed |
| --- | ---: | ---: | --- |
| tokenize the payload | 414.1 ms | 340.2 ms | a greedy value group the trim already right-strips; the `#` marker read per distinct spelling; each message-start vector reading only the rows the ones in front left null |
| lift the session columns | 97.7 ms | 44.6 ms | one code per distinct key spelling, once, instead of a pass over every entry per declared field |
| parse the payloads | 79.3 ms | 50.2 ms | the discriminator probe reads the rows that lifted none |
| classify protocol | 68.2 ms | 35.0 ms | rules tried in order over a shrinking column |
| version the protocols | 59.4 ms | 31.9 ms | one scan of `entries` for all three version fields |
| assemble one batch | 58.2 ms | 25.6 ms | a constant column is taken from one row, not built per row |
| probe the message types | 50.1 ms | — | a read declaring no msgtype no longer probes one |
| classify direction | 37.9 ms | 36.2 ms | unchanged |

RE2 is still what this layer is: `extract_regex` 483.9 ms, `find_substring_regex`
226.6 ms and `match_substring_regex` 106.9 ms are half of every kernel
millisecond, and `_parse_style` alone is 251 ms of them. Tokenizing is now
*half* the read rather than two fifths, because everything around it got
cheaper and it did not.

Two of these live in `fix/rules.py` and `fix/transcribe.py`, which the FIX
stage reads too, so the boundary above moved with them without being touched:
**1.28x** on an all-wire capture and 1.05x on a mixed one, over the same
alternating runs.

Four more were priced and left alone:

| proposal | what the measurement said |
| --- | --- |
| narrow `_parse_generic`'s separator candidates the same way | 1.3–2.0 ms of its 86 ms (mixed) and 123 ms (prose). Only the trailing candidate is ever answered away, because prose settles on the whitespace candidate, which is the last expensive one. |
| skip `_common_separators`' marker probes where no row is marked | 10.6 ms on wire and 4.4 ms on prose, and exactly 0 on the mixed capture, where 6,000 rows in 40,000 are marked. Recovering those needs a filter and scatter through a comment-dense correctness rule. |
| the line loop | `_iter_lines` is 26.9 ms of a 2,205 ms read for decompression and splitting, 136.1 ms with the Python header match — which is the whole of the unaccounted remainder, and the loop is already raced against a kernel pass under `--only messages`. |
| gate `_merge_reasons` on a null count | 3.4 ms per batch over a column that is all-null on every fixture row: 0.25% of the read for a second path. |

## What moved, and what only looked like it

Collapsing each rule's pattern list into one alternation nearly doubled
classification: **1.9x** on `Rules.into_arrow_protocol_array` over the same
65,536-row batch (571,000 → 1,076,000 rows/s), interleaved against the
pre-change module in one process, protocol answers asserted identical first.
Direction resolution was unchanged by it, and moved on its own later — the
section above. That beats the 1.53x a position-based
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

The [task smoke run](../pipeline/operations/run.md) exercises all six
jobs, all three log routes, registry-backed instrument enrichment, book
recovery rows, auditable rejection, and a zero-write replay. The bounded
market benchmark separately rejected 10 of 200 malformed events and emitted 70
auditable `INTERNAL_EXPIRED` deltas while enforcing `max_side_alive=10`.
