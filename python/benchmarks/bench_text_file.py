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
import contextlib
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


def generate(path: pathlib.Path, rows: int, trace_every: int = 200) -> int:
    """Write a synthetic log of `rows` records; returns its size in bytes.

    `trace_every` is how often a multi-line stack trace appears. Folding those
    into the row above is the one piece of per-line work that is not a regex
    match, so a log full of them is the parser's bad case.
    """
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
            if trace_every and i % trace_every == trace_every - 1:
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


def variants(rows: int, repeat: int) -> None:
    """The axes that are not batch sizes: continuations, hashing, decoding.

    Each is measured on its own file so the comparison is like for like, and
    the throughputs are per *parsed row*, not per byte -- a log stuffed with
    stack traces carries more bytes for the same number of rows.
    """
    print(f"\n{rows:,} rows, best of {repeat}")
    columns = ("case", "MiB", "seconds", "rows/s", "MB/s")
    widths = (28, 7, 9, 12, 8)
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))

    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        cases: list[tuple[str, pathlib.Path, dict, bool]] = []
        densities = (("no stack traces", 0), ("a trace every 200", 200), ("half traces", 2))
        for label, every in densities:
            path = root / f"traces-{every}.txt"
            generate(path, rows, every)
            cases.append((label, path, {}, False))
        plain = root / "traces-200.txt"
        cases.append(("no folding", plain, {"fold_continuations": False}, False))
        cases.append(("64 KiB reads", plain, {"read_byte_size": 1 << 16}, False))
        cases.append(("64 MiB reads", plain, {"read_byte_size": 1 << 26}, False))
        cases.append(("blake2b line hash", plain, {}, True))

        gz = root / "traces-200.txt.gz"
        gz.write_bytes(gzip.compress(plain.read_bytes(), compresslevel=1))
        cases.append(("gzip", gz, {}, False))

        for label, path, options, blake in cases:
            nbytes = path.stat().st_size
            fastest = float("inf")
            for _ in range(repeat):
                with _hashing(blake):
                    started = time.perf_counter()
                    TextFile.from_path(path).read_arrow_reader(**options).read_all()
                    fastest = min(fastest, time.perf_counter() - started)
            print(
                f"{label:>28} {nbytes / 2**20:>7.1f} {fastest:>9.3f} "
                f"{rows / fastest:>12,.0f} {nbytes / 2**20 / fastest:>8.1f}"
            )


@contextlib.contextmanager
def _hashing(blake: bool):
    """Run the body with the fallback line hash, or with whatever is installed.

    `xxhash` is picked at import when it is there; the two hashes are not
    interchangeable (a `hash64` is stable within an environment, not across
    them), so this measures the cost of the choice, not a switch to flip.
    """
    import hashlib

    from rekep.logs import text_file

    if not blake:
        yield
        return

    def fallback(raw: bytes) -> int:
        digest = hashlib.blake2b(raw, digest_size=8).digest()
        return int.from_bytes(digest, "little", signed=True)

    original = text_file._hash64
    text_file._hash64 = fallback
    try:
        yield
    finally:
        text_file._hash64 = original


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--repeat", type=int, default=3)
    arguments = parser.parse_args()
    rows = 200_000 if arguments.quick else arguments.rows
    sweep(rows, arguments.quick)
    variants(rows, 1 if arguments.quick else arguments.repeat)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
