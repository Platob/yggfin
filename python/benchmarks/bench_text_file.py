"""Benchmark TextFile streaming: rows/s, MB/s, and peak Arrow memory.

Run from `python/`::

    uv run python benchmarks/bench_text_file.py            # full sweep
    uv run python benchmarks/bench_text_file.py --quick    # one config, small file

The generated log matches the parser's target layout, with a stack trace folded
in every ~200 lines so continuation handling is part of what is measured.
Memory is reported from Arrow's own allocator (`pyarrow.total_allocated_bytes`),
which is where the batches actually live -- `tracemalloc` cannot see them.
"""

from __future__ import annotations

import argparse
import gzip
import pathlib
import sys
import tempfile
import time

import pyarrow

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep.logs import TextFile  # noqa: E402

DRIVERS = [b"OMSSales_Enrichment", b"ULBridge", b"ModuleMarketDataManager", b"ObjkeyTagWrapper"]
LEVELS = [b"(DEBUG) ", b"(INFO) ", b"(WARNING) ", b""]
TRACE = b"java.lang.IllegalStateException: synthetic\n\tat com.example.A.b(A.java:1)\n"


def generate(path: pathlib.Path, rows: int) -> int:
    """Write a synthetic log of `rows` records; returns its size in bytes."""
    with path.open("wb") as out:
        for i in range(rows):
            second, micro = divmod(i, 1_000_000)
            out.write(
                b"2026-08-14 %02d:%02d:%02d.%03d_%03d [250-e7256476:9effef3e6a:%05d] [%s] %s"
                % (
                    second // 3600 % 24,
                    second // 60 % 60,
                    second % 60,
                    micro // 1000,
                    micro % 1000,
                    72500 + i % 8,
                    DRIVERS[i % len(DRIVERS)],
                    LEVELS[i % len(LEVELS)],
                )
            )
            out.write(
                b"payload %d: ACCOUNT=ACCT-%06d routed XPAR qty=%d\n" % (i, i % 500, i % 10_000)
            )
            if i % 200 == 199:
                out.write(TRACE)
    return path.stat().st_size


def run(path: pathlib.Path, batch_row_size: int, read_byte_size: int) -> dict[str, float]:
    """One parse of the whole file; wall time and peak Arrow allocation."""
    base = pyarrow.total_allocated_bytes()
    peak = 0
    rows = 0
    started = time.perf_counter()
    with TextFile.from_path(path) as log:
        reader = log.into_arrow_reader(batch_row_size=batch_row_size, read_byte_size=read_byte_size)
        for batch in reader:
            rows += batch.num_rows
            peak = max(peak, pyarrow.total_allocated_bytes() - base)
    elapsed = time.perf_counter() - started
    return {"rows": rows, "seconds": elapsed, "peak_arrow_mb": peak / 2**20}


def sweep(rows: int, quick: bool) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        plain = pathlib.Path(tmp) / "bench.txt"
        nbytes = generate(plain, rows)
        gz = pathlib.Path(tmp) / "bench.txt.gz"
        gz.write_bytes(gzip.compress(plain.read_bytes(), compresslevel=1))
        gz_mib = gz.stat().st_size / 2**20
        print(f"{rows:,} rows, {nbytes / 2**20:.1f} MiB plain, {gz_mib:.1f} MiB gz")
        columns = ("file", "batch_rows", "read_KiB", "seconds", "rows/s", "MB/s", "peakMiB")
        widths = (6, 10, 9, 8, 10, 7, 8)
        print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))

        configs = (
            [(65_536, 1 << 22)]
            if quick
            else [
                (16_384, 1 << 16),
                (65_536, 1 << 16),
                (65_536, 1 << 20),
                (65_536, 1 << 22),
                (65_536, 1 << 23),
                (262_144, 1 << 22),
            ]
        )
        for name, path in (("plain", plain), ("gz", gz)):
            for batch_row_size, read_byte_size in configs:
                result = run(path, batch_row_size, read_byte_size)
                print(
                    f"{name:6} {batch_row_size:>10,} {read_byte_size >> 10:>9,} "
                    f"{result['seconds']:>8.2f} {result['rows'] / result['seconds']:>10,.0f} "
                    f"{nbytes / 2**20 / result['seconds']:>7.1f} {result['peak_arrow_mb']:>8.1f}"
                )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--quick", action="store_true")
    arguments = parser.parse_args()
    sweep(arguments.rows if not arguments.quick else 200_000, arguments.quick)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
