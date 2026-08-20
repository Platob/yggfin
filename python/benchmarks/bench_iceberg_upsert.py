"""Benchmark Dataset.write_arrow_reader against Iceberg: append vs. upsert.

Run from `python/`::

    uv run python benchmarks/bench_iceberg_upsert.py            # full sweep
    uv run python benchmarks/bench_iceberg_upsert.py --quick    # one size

A fully local SQLite catalog + file warehouse, gitignored under a scratch
temp dir (see `rekep tutorial` for the same setup interactively). Each round
writes `rows` fresh `ParsedMessage` rows arriving as many small batches --
the shape a streaming transform actually produces -- then merges a second
reader that is half overlapping keys (updates) and half new ones (inserts),
the mixed workload `merge_by` is actually for rather than a pure-insert best
case.

`commit_row_size` is what the sweep is really about, and it costs twice over.
The **throughput** column is the obvious half: too small and every call pays
its planning cost again, too large and the write stops streaming. The
**files** column is the half that keeps costing after the write returns:
every call commits a snapshot and lands at least one data file per
partition, so a small `commit_row_size` leaves a table that every later scan pays
to open. `iceberg_compact` can repair that afterwards -- not paying for it
in the first place is cheaper.
"""

from __future__ import annotations

import argparse
import datetime
import pathlib
import sys
import tempfile
import time
from collections.abc import Iterator

import pyarrow

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep.dataset import Dataset  # noqa: E402
from rekep.iceberg import Iceberg  # noqa: E402
from rekep.models import ParsedMessage  # noqa: E402
from rekep.records.iceberg import IcebergCatalog, IcebergDeployment  # noqa: E402

DAY = datetime.date(2026, 8, 14)


#: Rows per incoming batch. Small on purpose: a transform yields batches,
#: not tables, and how the writer groups them is the thing being measured.
BATCH_ROWS = 500


def generate_reader(rows: int, offset: int) -> pyarrow.RecordBatchReader:
    schema = ParsedMessage.into_arrow_schema()

    def batches() -> Iterator[pyarrow.RecordBatch]:
        for start in range(offset, offset + rows, BATCH_ROWS):
            size = min(BATCH_ROWS, offset + rows - start)
            yield pyarrow.RecordBatch.from_pydict(
                {
                    "url": ["bench"] * size,
                    "unix": list(range(start, start + size)),
                    "date": [DAY] * size,
                    "hash64": list(range(start, start + size)),
                    "protocol": ["FIX.4.4"] * size,
                    "fields": [{"35": "D"}] * size,
                },
                schema=schema,
            )

    return pyarrow.RecordBatchReader.from_batches(schema, batches())


def bench_one(rows: int, commit_row_size: int) -> dict[str, float]:
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp).as_posix()
        stack = Iceberg(
            IcebergDeployment(
                catalogs=[
                    IcebergCatalog(
                        endpoint=f"sqlite:///{root}/cat.db", warehouse=f"file://{root}/wh"
                    )
                ]
            )
        )
        dataset = Dataset(schema="rekep.models.ParsedMessage", uri="rekep:/datasets/messages")
        dataset.deploy_iceberg(stack)
        table = stack.tables.get(dataset.into_iceberg_table())

        started = time.perf_counter()
        dataset.write_arrow_reader(
            generate_reader(rows, 0), format="iceberg", table=table, commit_row_size=commit_row_size
        )
        append_seconds = time.perf_counter() - started
        table.refresh()
        append_files = table.inspect.data_files().num_rows

        # half updates (same keys), half inserts (fresh keys) -- upsert's mixed case.
        started = time.perf_counter()
        dataset.write_arrow_reader(
            generate_reader(rows, rows // 2),
            format="iceberg",
            table=table,
            merge_by=True,
            commit_row_size=commit_row_size,
        )
        upsert_seconds = time.perf_counter() - started

        return {
            "append_rows_s": rows / append_seconds,
            "upsert_rows_s": rows / upsert_seconds,
            "append_files": float(append_files),
        }


def sweep(rows: int, quick: bool) -> None:
    print(f"{rows:,} rows per round in {BATCH_ROWS}-row batches, merge is 50% update / 50% insert")
    columns = ("commit_row_size", "append rows/s", "merge rows/s", "files left")
    widths = (16, 15, 15, 12)
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))

    commit_sizes = [rows] if quick else [BATCH_ROWS, max(1, rows // 4), rows]
    for commit_row_size in commit_sizes:
        result = bench_one(rows, commit_row_size)
        print(
            f"{commit_row_size:>16,} {result['append_rows_s']:>15,.0f} "
            f"{result['upsert_rows_s']:>15,.0f} {result['append_files']:>12,.0f}"
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
