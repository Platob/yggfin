# Benchmarks

Every number on this site was measured, and it lives on the page it is about:
casting is on [Types](types.md#benchmarks), parsing on
[Logs](logs.md#benchmarks), FIX on [FIX](fix.md#benchmarks), identifiers and
books on [Market](market.md#benchmarks), whole jobs on
[Tasks](tasks.md#benchmarks), and everything about tables on
[Iceberg](iceberg.md#benchmarks). This page is the method —
what the scripts do, and how to read what they print.

The scripts are in `python/benchmarks/`, they ship with the package, and they
build their own fixtures, so any of them runs on a clean checkout.

## Running them

```bash
cd python
uv run python benchmarks/bench_text_file.py                  # parsing a log
uv run python benchmarks/bench_text_file.py --only variants  # what moves the parser
uv run python benchmarks/bench_text_file.py --only folders   # a capture of many files
uv run python benchmarks/bench_cast.py                       # casting data onto a shape
uv run python benchmarks/bench_fix.py                        # FIX, scalar and vectorised
uv run python benchmarks/bench_market.py                     # identifiers, and a book's prices
uv run python benchmarks/bench_tasks.py                      # parse, fan out, append with a merge
uv run python benchmarks/bench_iceberg.py                    # parse, stream in, read back
uv run python benchmarks/bench_iceberg.py --only maintain    # the maintenance
uv run python benchmarks/bench_iceberg.py --only update      # the half that rewrites
uv run python benchmarks/bench_iceberg.py --only backfill    # replaying clustered keys
uv run python benchmarks/bench_iceberg.py --only fs          # what the store is asked
```

`--quick` runs a small fixture and one configuration, which is what to use when
you are changing a benchmark rather than reading one.

## Where the results are

| page | what it measures | script |
| --- | --- | --- |
| [Types](types.md#benchmarks) | casting a batch, a nested column, a stream onto a shape — against `Array.cast` on the same data | `bench_cast.py` |
| [Logs](logs.md#benchmarks) | parsing one log; parsing a folder of them; shipping the bytes | `bench_text_file.py` |
| [FIX](fix.md#benchmarks) | the scalar parser, the vectorised one, and turning keys into tags | `bench_fix.py` |
| [Market](market.md#benchmarks) | building identifier columns, and deriving a book's flat prices | `bench_market.py` |
| [Tasks](tasks.md#benchmarks) | parsing a capture, fanning it out, and what a replay costs | `bench_tasks.py` |
| [Iceberg](iceberg.md#benchmarks) | commits, merges, reads, maintenance, backfills, and store calls | `bench_iceberg.py` |

## How to read a number

Five rules produce the figures on this site, and they are the same five that
decide whether a number is worth writing down at all.

**Verify the answer, then time it.** A benchmark that measures the wrong answer
measures nothing. `bench_cast.py` asserts its result equals pyarrow's own cast
before it starts a clock; `bench_fix.py` asserts the vectorised parse *is* the
scalar one; `bench_market.py` asserts the vectorised identifiers are the scalar
ones and that a book actually derived; the Iceberg sweeps assert the row counts they wrote.

**Measure twice.** Every number quoted is a number that reproduced. Where two
runs agree, one figure is quoted; where they do not, **the range is quoted** —
`0.99–1.12`, `411k–425k` — and the spread is the point rather than an
inconvenience. A single run is never quoted as if it were a specification.

**Re-measure when the code under a number changes.** Making the line hash xxh3
changed what the parser does per line, so every parser table was run again
rather than carried over: a number that describes an older version of the code
is worse than no number, because it reads as current.

**Measure warm, and in isolation.** An Acero join costs its own initialisation
on the first call in a process, so a sequence of timed stages charges the whole
of it to whichever ran first: one reordering looked 5× faster that way and was
worth 1.7 ms once both sides were warmed and run best-of-five. Three
back-to-back runs of the same read once gave 0.057, 0.031 and 0.027 s — a 2.1×
spread that was nothing but warm-up.

**Sweep what is expected to be bad.** A sweep that only tries the configuration
the code is good at is an advertisement. `into_bytes()` is in the byte-flow
table precisely because it holds the whole capture; `chained by hand` is in the
folder table because it is what a set costs without its one optimisation;
`blake2b line hash` is in the parser table because it is the hash `xxhash`
replaced; `Table.upsert` is in the merge table because it is the alternative.

**Prefer a count to a second.** Seconds on a local disk say very little about a
job on an object store, and they move ±30–40% on a shared machine. Counts do
not move at all: files planned, files opened, manifests read, GETs served,
rows rewritten, terms in a filter. Where a count answers the question, it is
the number quoted, and the seconds beside it are labelled as noisy.

!!! note "Local disk is not an object store"

    The Iceberg sweeps use a SQLite catalog and a file warehouse, so they are
    storage-latency-free: they measure planning, commit and Arrow work, which
    is what this package is responsible for. On an object store every commit
    and every plan also pays a round trip — which makes the *number of calls*
    matter more, not less. That is measured separately, by counting calls on
    the file handles themselves: [what the store is
    asked](iceberg.md#what-the-store-is-asked).

## Quoting them

A number in a docstring or on this site is part of the code's contract with
whoever reads it, so it carries its fixture — how many rows, how many files,
best of how many — and it is re-measured when the code under it changes.
Rounding a slow row away, or dropping the case that did not improve, is how a
claim stops matching the benchmark under it.

Where a faster path here replaces a library's own, the two are compared row by
row in the tests rather than only in a benchmark
([`tests/iceberg/test_coherence.py`](https://github.com/Platob/rekep/blob/main/python/tests/iceberg/test_coherence.py)),
and a flag switches back to the library. A benchmark says which is faster; only
a test says they agree.
