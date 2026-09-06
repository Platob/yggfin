# Benchmarks

## Message parsing

The focused parser benchmark generates 200,000 realistic header-plus-body
records and verifies the schema, row count, first and last rows before timing.
The table reports the fastest of five warmed runs on the reference Windows
host.

| source | Yggdryl rows/s | Message rows/s | decoded MiB/s | first batch ms |
| --- | ---: | ---: | ---: | ---: |
| URI local plain | 102,180 | 87,932 | 11.1 | 725 |
| URI local gzip | 102,225 | 88,330 | 11.1 | 722 |

```bash
cd python
uv run python benchmarks/bench_message.py --rows 200000 --repeat 5
```

The Yggdryl column drains native text batches. The Message column includes the
strict schema application used by `parse_messages`. These values use the
release-built Yggdryl 0.1.1 at `ac093526`, PyArrow 25.0.1, and the direct native
`url`/`rownum` columns. The Message measurement includes vectorized conversion
to the nullable Arrow `timestamp[us, UTC]` contract.

A separate earlier diagnostic at Yggdryl `bfadd39f` included copying gzip to a
local staging resource. It reached 83,868 Message rows/s and a 733 ms first
batch, against 97,813 rows/s and 653 ms direct in the same run.
`IOBase.buffered()` is a positional-read cache that the sequential record
reader bypasses. Staging and that cache therefore add no useful layer to this
one-pass scan; a remote source stays streamed directly. Whole-byte and
record-write bottlenecks are isolated in the
[Yggdryl IOBase throughput prompt](../prompts/yggdryl-iobase-throughput.md).

## Message-to-Iceberg run

Three separate 100,000-row gzip objects were parsed by fresh task processes
and written into one local Iceberg table. The final table held 300,000 rows in
three data files and three snapshots.

| interval | stage rows/s | fresh-process wall rows/s |
| --- | ---: | ---: |
| 1, including table creation | 28,571 | 13,912 |
| 2 | 28,321 | 13,880 |
| 3 | 30,479 | 14,658 |

Replaying one complete interval read 100,000 rows, wrote zero, skipped 100,000,
and created neither a data file nor a snapshot. Its stage rate was 31,066 rows/s
and fresh-process wall rate was 14,375 rows/s.

Each object decoded to 12.58 MiB from 1.82 MiB of gzip. The stored timestamp was
verified as `timestamp[us, UTC]`; its first value was the expected aware UTC
instant. These measurements used release-built Yggdryl 0.1.1 at `ac093526`,
PyArrow 25.0.1, and PyIceberg 0.11.1.
They include local PyIceberg transaction work and fresh Python/Marimo startup;
they are not portable service-level guarantees.

## Iceberg internals

```bash
cd python
uv run python benchmarks/bench_iceberg.py --quick
```

That benchmark covers bounded writes, scans, keyed insertion, maintenance, and
deletion over synthetic rows. It does not duplicate the application pipeline.
