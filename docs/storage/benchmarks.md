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
installs `Message.into_field()` on that same native reader, adding conversion to
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
