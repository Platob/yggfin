"""Benchmark LogsToRecords: rows/s parsing pipe-delimited FIX-shaped messages.

Run from `python/`::

    uv run python benchmarks/bench_message_parser.py            # full sweep
    uv run python benchmarks/bench_message_parser.py --quick    # one size

Generates synthetic `Log` batches whose `message` is FIX-shaped
(`8=FIX.4.4|9=112|35=D|...`) directly in Arrow -- no file round trip, since
this measures `LogsToRecords.arrow_transform` alone, not `LogFile` (see
`bench_log_file.py` for that).
"""

from __future__ import annotations

import argparse
import datetime
import pathlib
import sys
import time

import pyarrow

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep.jobs import LogsToRecords  # noqa: E402
from rekep.models import Log  # noqa: E402

TAGS = ("8", "9", "35", "49", "56", "11", "55", "54", "38", "44", "17", "150", "39")
DAY = datetime.date(2026, 8, 14)
NOON = datetime.time(12, 0)


def generate_batch(rows: int, fields_per_row: int, offset: int) -> pyarrow.RecordBatch:
    """One `Log`-shaped batch whose `message` is a FIX-like pipe string."""
    messages = []
    for i in range(rows):
        n = offset + i
        segments = [f"{tag}=v{n % 997}" for tag in TAGS[:fields_per_row]]
        messages.append("|".join(segments) + "|")
    return pyarrow.RecordBatch.from_pydict(
        {
            "url": ["bench"] * rows,
            "unix": list(range(offset, offset + rows)),
            "date": [DAY] * rows,
            "time": [NOON] * rows,
            "thread_name": ["t"] * rows,
            "driver": ["d"] * rows,
            "message": messages,
            "hash64": list(range(offset, offset + rows)),
        },
        schema=Log.into_arrow_schema(),
    )


def run(rows: int, batch_row_size: int, fields_per_row: int) -> dict[str, float]:
    batches = [
        generate_batch(min(batch_row_size, rows - offset), fields_per_row, offset)
        for offset in range(0, rows, batch_row_size)
    ]
    job = LogsToRecords(name="bench")
    started = time.perf_counter()
    parsed = sum(out.num_rows for out in job.arrow_transform(iter(batches)))
    elapsed = time.perf_counter() - started
    return {"rows": parsed, "seconds": elapsed}


def sweep(rows: int, quick: bool) -> None:
    print(f"{rows:,} rows of FIX-shaped messages")
    columns = ("batch_rows", "fields", "seconds", "rows/s")
    widths = (10, 6, 8, 12)
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))

    configs = [(65_536, 10)] if quick else [(16_384, 5), (65_536, 5), (65_536, 10), (262_144, 10)]
    for batch_row_size, fields_per_row in configs:
        result = run(rows, batch_row_size, fields_per_row)
        print(
            f"{batch_row_size:>10,} {fields_per_row:>6} {result['seconds']:>8.2f} "
            f"{result['rows'] / result['seconds']:>12,.0f}"
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
