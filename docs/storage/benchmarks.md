# Benchmarks

## Message parsing

The focused parser benchmark generates 200,000 realistic header-plus-body
records and verifies the schema, row count, first and last rows before timing.
The table reports the fastest of three warmed runs on the reference Windows
host.

| source | Yggdryl rows/s | Message rows/s | decoded MiB/s | first batch ms |
| --- | ---: | ---: | ---: | ---: |
| URI local plain | 85,660 | 96,848 | 12.2 | 684 |
| URI local gzip | 99,359 | 97,813 | 12.3 | 653 |

```bash
cd python
uv run python benchmarks/bench_message.py --rows 200000 --repeat 3
```

The Yggdryl column drains native text batches. The Message column includes the
source-column rename and strict schema cast used by `parse_messages`.

A separate gzip diagnostic that included copying to a local staging resource
reached 83,868 Message rows/s and a 733 ms first batch, against 97,813 rows/s
and 653 ms direct. `IOBase.buffered()` is a positional-read cache that the
sequential record reader bypasses. Staging and that cache therefore add no
useful layer to this one-pass scan; a remote source stays streamed directly.

## Message-to-Iceberg run

Three separate 100,000-row gzip objects were parsed by fresh task processes
and written into one local Iceberg table. The final table held 300,000 rows in
three data files and three snapshots.

| interval | stage rows/s | fresh-process wall rows/s |
| --- | ---: | ---: |
| 1, including table creation | 7,207 | 3,107 |
| 2 | 15,347 | 7,656 |
| 3 | 15,131 | 6,290 |

Replaying one complete interval read 100,000 rows, wrote zero, skipped 100,000,
and created neither a data file nor a snapshot. Its stage rate was 16,841 rows/s
and fresh-process wall rate was 8,825 rows/s.

These measurements used release-built Yggdryl 0.1.1 at `bfadd39f`, PyArrow
25.0.1, and PyIceberg 0.11.1. They include local PyIceberg transaction work and
fresh Python/Marimo startup; they are not portable service-level guarantees.

## Iceberg internals

```bash
cd python
uv run python benchmarks/bench_iceberg.py --quick
```

That benchmark covers bounded writes, scans, keyed insertion, maintenance, and
deletion over synthetic rows. It does not duplicate the application pipeline.
