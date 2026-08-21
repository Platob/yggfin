"""Benchmark TextFile streaming: rows/s, MB/s, and peak Arrow memory.

Run from `python/`::

    uv run python benchmarks/bench_text_file.py            # every sweep
    uv run python benchmarks/bench_text_file.py --quick    # one config, small file
    uv run python benchmarks/bench_text_file.py --only folders   # a capture of many files

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
import tracemalloc

import pyarrow

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep.logs import TextFile, TextFiles  # noqa: E402

DRIVERS = [b"OMSSales_Enrichment", b"ULBridge", b"ModuleMarketDataManager", b"ObjkeyTagWrapper"]
LEVELS = [b"(DEBUG) ", b"(INFO) ", b"(WARNING) ", b""]
TRACE = b"java.lang.IllegalStateException: synthetic\n\tat com.example.A.b(A.java:1)\n"


def generate(path: pathlib.Path, rows: int, trace_every: int = 200, start: int = 0) -> int:
    """Write a synthetic log of `rows` records; returns its size in bytes.

    `trace_every` is how often a multi-line stack trace appears. Folding those
    into the row above is the one piece of per-line work that is not a regex
    match, so a log full of them is the parser's bad case.

    `start` offsets the row numbers, which is what makes a *folder* of these
    files a capture rather than 500 copies of one file: without it every file
    holds the same bytes, and a compressor with a long-range matcher reports a
    ratio that says more about the fixture than about the data.
    """
    with path.open("wb") as out:
        for index in range(rows):
            i = start + index
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
            if trace_every and index % trace_every == trace_every - 1:
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
        cases.append(("blake2b line hash", plain, {}, True))  # what xxh3 replaced

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


def folders(rows: int, repeat: int) -> None:
    """A capture is a folder, not a file: rotations, an archive, some gzipped.

    The same rows every time, cut into a different number of files, so the
    column that moves is fragmentation and nothing else. `chained by hand` is
    the configuration expected to be bad: the per-file readers concatenated
    with no batch combining, which is what a set of small files costs if the
    short batches are handed downstream as they come.
    """
    print(f"\n{rows:,} rows through a folder, best of {repeat}")
    columns = ("case", "files", "seconds", "rows/s", "batches", "peakMiB")
    widths = (26, 6, 9, 12, 9, 8)
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))

    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        shapes = (("one", 1), ("few", 20), ("many", 500))
        for name, count in shapes:
            folder = root / name
            folder.mkdir()
            per = rows // count
            for index in range(count):
                # Each file holds its own rows, not a copy of the first file's.
                generate(folder / f"app.{index}.txt", per, start=index * per)
        gzipped = root / "many-gz"
        gzipped.mkdir()
        for source in sorted((root / "many").iterdir()):
            (gzipped / f"{source.name}.gz").write_bytes(
                gzip.compress(source.read_bytes(), compresslevel=1)
            )

        def parse(folder: pathlib.Path, chained: bool = False) -> tuple[int, int, int]:
            """Rows, batches and peak Arrow bytes out of one pass.

            Peak is sampled *inside* the loop: by the time a pass is over the
            batches are released, and a reading taken then says nothing about
            what a consumer had to hold.
            """
            files = TextFiles.from_folder(folder)
            if chained:
                batches = (batch for log in files.into_files() for batch in _drained(log))
            else:
                batches = files.into_arrow_batches()
            base = pyarrow.total_allocated_bytes()
            counted = 0
            seen = 0
            peak = 0
            for batch in batches:
                counted += batch.num_rows
                seen += 1
                peak = max(peak, pyarrow.total_allocated_bytes() - base)
            return counted, seen, peak

        cases = (
            ("one file", root / "one", 1, False),
            ("20 files", root / "few", 20, False),
            ("500 files", root / "many", 500, False),
            ("500 files, chained by hand", root / "many", 500, True),
            ("500 files, gzipped", gzipped, 500, False),
        )
        for label, folder, count, chained in cases:
            parse(folder, chained)  # warm: the first pass in a process pays for the rest
            fastest, batches, peak = float("inf"), 0, 0
            for _ in range(repeat):
                started = time.perf_counter()
                counted, batches, held = parse(folder, chained)
                elapsed = time.perf_counter() - started
                peak = max(peak, held)
                assert counted == rows, f"{label} parsed {counted:,} of {rows:,}"
                fastest = min(fastest, elapsed)
            print(
                f"{label:>26} {count:>6,} {fastest:>9.3f} {rows / fastest:>12,.0f} "
                f"{batches:>9,} {peak / 2**20:>8.1f}"
            )

        _byte_flows(root / "many", repeat)
        _listing(root / "many", repeat)


def _drained(log: TextFile) -> list[pyarrow.RecordBatch]:
    """One file's batches, read and closed, with nothing combined across files."""
    with log:
        return list(log.into_arrow_batches())


def _byte_flows(folder: pathlib.Path, repeat: int) -> None:
    """The other half of a set: shipping the bytes rather than parsing them.

    Peak is Python's own allocation (`tracemalloc`), because these chunks are
    `bytes` and never reach Arrow's allocator. `into_bytes()` is here to be
    bad: it is the same stream with nothing bounding it.
    """
    nbytes = sum(path.stat().st_size for path in folder.iterdir())
    print(f"\n{nbytes / 2**20:.1f} MiB of log bytes, best of {repeat}")
    columns = ("flow", "seconds", "MB/s", "outMiB", "peakMiB")
    widths = (26, 9, 8, 8, 9)
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))

    flows = (
        ("streamed raw", None, lambda files: _consume(files.into_byte_chunks())),
        ("streamed gzip", "gzip", lambda f: _consume(f.into_byte_chunks(compression="gzip"))),
        ("streamed zstd", "zstd", lambda f: _consume(f.into_byte_chunks(compression="zstd"))),
        ("into_bytes (materialised)", None, lambda files: (len(files.into_bytes()), 0)),
    )
    raw = TextFiles.from_folder(folder).into_bytes()
    for label, codec, flow in flows:
        _verify(folder, codec, raw)  # a truncated stream is a better ratio, not a failure
        flow(TextFiles.from_folder(folder))  # warm
        fastest, produced, peak = float("inf"), 0, 0
        for _ in range(repeat):
            tracemalloc.start()
            started = time.perf_counter()
            produced, _ = flow(TextFiles.from_folder(folder))
            elapsed = time.perf_counter() - started
            peak = max(peak, tracemalloc.get_traced_memory()[1])
            tracemalloc.stop()
            fastest = min(fastest, elapsed)
        print(
            f"{label:>26} {fastest:>9.3f} {nbytes / 2**20 / fastest:>8.1f} "
            f"{produced / 2**20:>8.2f} {peak / 2**20:>9.1f}"
        )


def _verify(folder: pathlib.Path, codec: str | None, raw: bytes) -> None:
    """Check a flow's answer before it is timed, outside the timed region.

    A codec stream that lost its trailer decodes short -- and would print as a
    *better* ratio rather than as a failure, which is the one way a benchmark
    can flatter the code it measures.
    """
    files = TextFiles.from_folder(folder)
    if codec is None:
        assert files.into_bytes() == raw, "the raw flow changed the bytes"
        return
    blob = files.into_bytes(compression=codec)
    with pyarrow.CompressedInputStream(pyarrow.BufferReader(blob), codec) as stream:
        assert stream.read() == raw, f"the {codec} flow does not decode back"


def _consume(chunks) -> tuple[int, int]:
    """Bytes and chunks out of a flow, holding neither."""
    produced = 0
    seen = 0
    for chunk in chunks:
        produced += len(chunk)
        seen += 1
    return produced, seen


def _listing(folder: pathlib.Path, repeat: int) -> None:
    """What the walk itself costs, since a scan of a store is not free."""
    files = TextFiles.from_folder(folder)
    fastest = float("inf")
    counted = 0
    for _ in range(repeat):
        started = time.perf_counter()
        counted = sum(1 for _ in files.into_urls())
        fastest = min(fastest, time.perf_counter() - started)
    print(f"\nwalking {counted:,} paths: {fastest * 1000:.1f} ms")


@contextlib.contextmanager
def _hashing(blake: bool):
    """Run the body with blake2b as the line hash instead of xxh3.

    Not a switch anyone can flip: the digest is the low half of every row id,
    so `xxhash` is a hard dependency and the hash is part of the data's
    identity. This is here to price that decision -- what the parser would cost
    if the id's hash were the one this package used before it had ids.
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
    parser.add_argument("--only", choices=("sweep", "variants", "folders"), default=None)
    arguments = parser.parse_args()
    rows = 200_000 if arguments.quick else arguments.rows
    repeat = 1 if arguments.quick else arguments.repeat
    if arguments.only in (None, "sweep"):
        sweep(rows, arguments.quick)
    if arguments.only in (None, "variants"):
        variants(rows, repeat)
    if arguments.only in (None, "folders"):
        folders(rows, repeat)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
