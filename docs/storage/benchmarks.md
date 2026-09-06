# Benchmarks

## Message parsing

The focused parser benchmark generates 70,000 realistic header-plus-body
records and verifies the schema, row count, first and last rows before timing.
That crosses Yggdryl's 65,536-row batch boundary. The fastest of two warmed runs
on the reference Windows host is reported below.

| source | Yggdryl rows/s | Message rows/s | decoded MiB/s | first batch ms |
| --- | ---: | ---: | ---: | ---: |
| URI local plain | 126,920 | 90,296 | 11.4 | 739 |
| URI local gzip | 108,307 | 87,195 | 11.0 | 725 |

```bash
cd python
uv run python benchmarks/bench_message.py
```

The Yggdryl column drains native text batches. The Message column includes the
native schema application used by `parse_messages`. These values use the
release-built Yggdryl 0.1.1 at `4cdbf5b9`, PyArrow 25.0.1, and the direct native
`url`/`rownum` columns. The Message measurement includes vectorized conversion
to nullable Arrow `timestamp[us, UTC]`, native derivation of `timepartition`,
and strict final verification.

The preceding `083da992` contract, which did not derive a partition column,
measured 110,949 plain and 107,440 gzip Message rows/s. The current complete
contract is 18.6% and 18.8% lower respectively. Native plain control was flat;
native gzip varied from 125,896 to 108,307 rows/s on this host. The applied
partition plan is therefore visible in the parser cost, and its no-protocol and
dependency-projection optimizations are scoped in the
[Yggdryl Arrow apply prompt](../prompts/yggdryl-arrow-apply.md).

A separate earlier diagnostic at Yggdryl `bfadd39f` included copying gzip to a
local staging resource. It reached 83,868 Message rows/s and a 733 ms first
batch, against 97,813 rows/s and 653 ms direct in the same run.
`IOBase.buffered()` is a positional-read cache that the sequential record
reader bypasses. Staging and that cache therefore add no useful layer to this
one-pass scan; a remote source stays streamed directly. Whole-byte and
record-write bottlenecks are isolated in the
[Yggdryl IOBase throughput prompt](../prompts/yggdryl-iobase-throughput.md).

## Message-to-Iceberg run

Three separate 100,000-row gzip objects in consecutive UTC hours were parsed by
fresh task processes and written into one local Iceberg table. The final table
held 300,000 rows in three hourly data files and three snapshots.

| interval | stage rows/s | fresh-process wall rows/s |
| --- | ---: | ---: |
| 1, including table creation | 25,497 | 8,974 |
| 2 | 27,586 | 7,039 |
| 3 | 30,048 | 14,229 |

Replaying one complete interval read 100,000 rows, wrote zero, skipped 100,000,
and created neither a data file nor a snapshot. Its stage rate was 27,824 rows/s
and fresh-process wall rate was 9,578 rows/s.

Across the three inserts, aggregate in-task throughput was 27,586 rows/s;
aggregate fresh-process throughput was 9,266 rows/s. Fresh-process rates remain
dominated by Python/Marimo startup and local PyIceberg transaction variance.

Each object decoded to 12.58 MiB from 1.82 MiB of gzip. A streamed read returned
three batches. The physical spec was `timepartition_hour: hour`, with epoch-hour
values 496296 through 496298. A one-hour filter read 100,000 rows from one file
and pruned the other two. These measurements used release-built Yggdryl 0.1.1
at `4cdbf5b9`, PyArrow 25.0.1, and PyIceberg 0.11.1.
They include local PyIceberg transaction work and fresh Python/Marimo startup;
they are not portable service-level guarantees.

## Iceberg internals

```bash
cd python
uv run python benchmarks/bench_iceberg.py
uv run python benchmarks/bench_iceberg.py --quick
```

The default sweep uses 20,000 rows over four days and at most two timing runs;
`--quick` uses 2,500 rows over two days with one timing run. The benchmark
covers bounded writes, scans, keyed insertion, maintenance, and deletion over
synthetic rows. Its test smoke executes only the representative quick write
path; exhaustive timing is opt-in. It does not duplicate the application
pipeline.
