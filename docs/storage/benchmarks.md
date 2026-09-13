# Benchmarks

Reference host, fastest of two warmed runs. Not portable service-level
guarantees: they include local PyIceberg transaction work and fresh
Python/Marimo startup.

## Message parsing

70,000 header-plus-body records, schema and first/last row verified before
timing -- past the 65,536-row native batch boundary.

```bash
cd python
uv run python benchmarks/bench_message.py
```

| source | native rows/s | `Message` rows/s | decoded MiB/s | first batch ms |
| --- | ---: | ---: | ---: | ---: |
| URI local plain | 107,322 | 96,420 | 13.5 | 714 |
| URI local gzip | 98,351 | 89,443 | 12.5 | 689 |

*native* drains header-framed text batches without an output field. *`Message`*
installs `Message.field()` on that same native reader, adding conversion to
`timestamp[us, UTC]`, derivation of `timepartition`, the `bodyhash` digest, and
strict final verification. The contract path measured 9–10% below framing
alone on this run; there is no Python row pass between them.

The remaining product work is [on the roadmap](../roadmap/index.md). Staging a
remote object locally does not help ingestion: an earlier diagnostic that
copied gzip to a local resource reached 83,868 rows/s against 97,813 direct in
the same run. `IOBase.buffered()` is a positional-read cache the sequential
record reader bypasses, so a remote source stays streamed directly.

## Message to Iceberg

Three 100,000-row gzip objects in consecutive UTC hours, each parsed by a fresh
task process into one local table: 300,000 rows, three hourly data files, three
snapshots.

| interval | stage rows/s | fresh-process wall rows/s |
| --- | ---: | ---: |
| 1, including table creation | 25,497 | 8,974 |
| 2 | 27,586 | 7,039 |
| 3 | 30,048 | 14,229 |
| replay of interval 1 | 27,824 | 9,578 |

```text
replay      100,000 read, 0 written, 100,000 skipped
            no data file, no snapshot
partition   timepartition_hour: hour → epoch hours 496296-496298
pruning     a one-hour filter read 100,000 rows from one file, pruned two
compression 1.82 MiB gzip → 12.58 MiB decoded, three batches
```

Fresh-process rates are dominated by Python/Marimo startup rather than by the
pipeline.

## Iceberg internals

```bash
cd python
uv run python benchmarks/bench_iceberg.py           # 20,000 rows over four days
uv run python benchmarks/bench_iceberg.py --quick   # 2,500 rows over two days
```

Bounded writes, scans, keyed insertion, maintenance and deletion over synthetic
rows. The test suite smokes the quick write path only; exhaustive timing is
opt-in.

The write sweep reports `peak MiB`: the Arrow high-water mark over one write,
measured through a proxy memory pool installed for that write alone, because
a pool never lowers its high-water mark and a process that has already
allocated would otherwise answer about the process. It is where a writer that
collects a chunk instead of staging it shows up; the wall clock is much the
same either way.

## Staged writes

Three verbs over one 70 MiB chunk of 524,288 rows, against the same chunk
handed whole to PyIceberg's writer. Peak is the Arrow high-water mark over the
commit, in chunks; seconds is the whole streamed write.

| verb | partitions | peak, whole | peak, staged | seconds, whole | seconds, staged |
| --- | ---: | ---: | ---: | ---: | ---: |
| append | 1 | 2.01 | 1.07 | 0.87 | 0.24 |
| append | 4 | 1.78 | 1.52 | 0.86 | 0.29 |
| append | 24 | 1.21 | 1.15 | 0.59 | 0.33 |
| keyed append | 1 | 3.12 | 1.23 | 1.49 | 0.50 |
| keyed append | 4 | 4.95 | 1.52 | 1.85 | 0.33 |
| keyed append | 24 | 4.95 | 1.12 | 1.84 | 0.67 |
| merge | 1 | 2.01 | 1.17 | 0.70 | 0.30 |
| merge | 4 | 3.94 | 1.52 | 1.31 | 0.91 |
| merge | 24 | 3.94 | 1.12 | 1.87 | 0.49 |

Splitting a chunk by partition inside PyIceberg copies each partition twice and
holds every copy at once, because it submits all of them to its pool before
the first file is written; a keyed write then held its partitions' rows again
until the commit. Staging takes one partition out of the chunk at a time.
