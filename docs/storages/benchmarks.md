# Benchmarks

Three focused benchmarks live under `python/benchmarks/`. Each verifies what
it measures against a reference -- the schema and the first and last row, or a
Python row-by-row reference -- before it times anything, and each takes
`--quick` for a smoke run and `--repeat` for the number of warmed runs it
keeps the fastest of.

| script | measures | reference it checks first |
| --- | --- | --- |
| `bench_message.py` | plain and gzip text objects read into stored `log_messages` batches: the native text read, then the storage boundary `parse_log_messages` pays | the schema and the first and last row of every source |
| `bench_market.py` | flattening book deltas and executions into event rows with Arrow kernels, as `parse_orders`, `parse_quotes` and `parse_executions` do | a Python row-by-row flattening of the same books |
| `bench_iceberg.py` | Iceberg commits, streamed and ordered scans, keyed and window replacements, maintenance, deletes and a backfill over synthetic hourly rows | the rows written read back |

```bash
cd python
uv run python benchmarks/bench_message.py
uv run python benchmarks/bench_market.py
uv run python benchmarks/bench_iceberg.py --only write
```

`bench_iceberg.py --only` narrows it to one of `write`, `stream`, `read`,
`maintain`, `update`, `delete` or `backfill`, and `--rows` and `--days` size
its corpus. Figures depend on the host, the Python and Arrow builds and the
catalog, so a figure is only stated beside the run that produced it: rerun
the script on the host that matters rather than reading one from here.

Two findings are properties of the design rather than of a host, and hold on
any of them:

- Staging a remote capture locally does not make a read faster: the text read
  streams one object at a time with bounded read-ahead, and
  `IOBase.buffered()` is a positional-read cache the sequential record reader
  bypasses. The production path never stages a remote file.
- A write holds a bounded multiple of the chunk it was handed, whatever the
  number of partitions the chunk spans, because it stages one partition at a
  time: [Iceberg datasets](iceberg.md#what-a-commit-holds) has the measured
  bound and the test that pins it.
