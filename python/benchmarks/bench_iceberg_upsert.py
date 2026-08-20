"""Benchmark Dataset.write_arrow_reader against Iceberg: append vs. upsert.

Run from `python/`::

    uv run python benchmarks/bench_iceberg_upsert.py            # full sweep
    uv run python benchmarks/bench_iceberg_upsert.py --quick    # one size

A fully local SQLite catalog + file warehouse, gitignored under a scratch
temp dir (see `rekep tutorial` for the same setup interactively). Each round
writes `rows` fresh `ParsedMessage` rows, then upserts a second batch that is
half overlapping keys (updates) and half new ones (inserts) -- the mixed
workload `upsert` is actually for, not a pure-insert best case. `chunk_rows`
sweeps to show why chunking matters: too small and `upsert` pays its
per-call planning cost too many times, too large and it stops streaming.
"""

from __future__ import annotations

import argparse
import datetime
import pathlib
import sys
import tempfile
import time

import pyarrow

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep.dataset import Dataset  # noqa: E402
from rekep.iceberg import Iceberg  # noqa: E402
from rekep.models import ParsedMessage  # noqa: E402
from rekep.records.iceberg import IcebergCatalog, IcebergDeployment  # noqa: E402

DAY = datetime.date(2026, 8, 14)


def generate_reader(rows: int, offset: int) -> pyarrow.RecordBatchReader:
    schema = ParsedMessage.into_arrow_schema()
    batch = pyarrow.RecordBatch.from_pydict(
        {
            "url": ["bench"] * rows,
            "unix": list(range(offset, offset + rows)),
            "date": [DAY] * rows,
            "hash64": list(range(offset, offset + rows)),
            "protocol": ["FIX.4.4"] * rows,
            "fields": [{"35": "D"}] * rows,
        },
        schema=schema,
    )
    return pyarrow.RecordBatchReader.from_batches(schema, [batch])


def bench_one(rows: int, chunk_rows: int) -> dict[str, float]:
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp).as_posix()
        stack = Iceberg(
            IcebergDeployment(
                catalogs=[
                    IcebergCatalog(uri=f"sqlite:///{root}/cat.db", warehouse=f"file://{root}/wh")
                ]
            )
        )
        dataset = Dataset(record="rekep.models.ParsedMessage", name="messages")
        dataset.deploy_iceberg(stack)
        table = stack.tables.get(dataset.into_iceberg_table())

        started = time.perf_counter()
        dataset.write_arrow_reader(generate_reader(rows, 0), format="iceberg", table=table)
        append_seconds = time.perf_counter() - started

        # half updates (same keys), half inserts (fresh keys) -- upsert's mixed case.
        started = time.perf_counter()
        dataset.write_arrow_reader(
            generate_reader(rows, rows // 2),
            format="iceberg",
            table=table,
            mode="upsert",
            chunk_rows=chunk_rows,
        )
        upsert_seconds = time.perf_counter() - started

        return {
            "append_rows_s": rows / append_seconds,
            "upsert_rows_s": rows / upsert_seconds,
        }


def sweep(rows: int, quick: bool) -> None:
    print(f"{rows:,} rows per round, mixed 50% update / 50% insert for upsert")
    columns = ("chunk_rows", "append rows/s", "upsert rows/s")
    widths = (12, 15, 15)
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))

    chunk_sizes = [rows] if quick else [max(1, rows // 20), max(1, rows // 4), rows]
    for chunk_rows in chunk_sizes:
        result = bench_one(rows, chunk_rows)
        print(
            f"{chunk_rows:>12,} {result['append_rows_s']:>15,.0f} {result['upsert_rows_s']:>15,.0f}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--quick", action="store_true")
    arguments = parser.parse_args()
    sweep(arguments.rows if not arguments.quick else 2_000, arguments.quick)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
