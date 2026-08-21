"""Benchmark the two hot paths of the market module: identifiers, and book prices.

Run from `python/`::

    uv run python benchmarks/bench_market.py            # full sweep
    uv run python benchmarks/bench_market.py --quick    # fewer rows, fewer repeats

Three questions, answered on columns shaped like a real feed:

1. **What does `h128_arrow` buy over `h128_of` per row?** Both build the same
   identifiers, and the vectorised result is asserted equal to the scalar one
   *before* anything is timed -- a benchmark that measures the wrong answer
   measures nothing.
2. **What does the length prefix cost?** It is what makes the encoding
   injective (`identity.py`), and it is two extra kernels per part, so the
   plain join is timed beside it rather than assumed cheap.
3. **What does deriving a book's columns cost, against reading them back?**
   The whole argument for storing `px`, `spread` and `micro_px` is that a
   reader should never recompute them -- which is only worth saying if the
   computation is worth avoiding. So the derivation is timed against the
   column it fills, and, separately, against reading a price out of a side
   that is *already* derived: the nested access is not the cost, the walk over
   the levels is, and the two lines say so.

Every case is warmed once and reported as the best of `--repeat` runs; run the
script twice before quoting a number anywhere.
"""

from __future__ import annotations

import argparse
import datetime
import pathlib
import sys
import time
from collections.abc import Callable

import pyarrow
import pyarrow.compute

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep.market import Book, BookSide  # noqa: E402
from rekep.market.identity import ABSENT, SEPARATOR, h128_arrow, h128_of, uuids_of  # noqa: E402

DAY = datetime.date(2024, 3, 14)
UNIX = 1710374400_000000000


def timed(work: Callable[[], object], repeat: int) -> tuple[float, object]:
    """Best of `repeat`, warmed once -- a first call charges its own set-up."""
    result = work()
    best = float("inf")
    for _ in range(repeat):
        start = time.perf_counter()
        work()
        best = min(best, time.perf_counter() - start)
    return best, result


def report(label: str, seconds: float, rows: int, against: float | None = None) -> None:
    per_row = seconds / rows * 1e9
    ratio = f"  {against / seconds:6.1f}x" if against else ""
    print(f"  {label:<44} {seconds * 1000:8.2f} ms  {per_row:7.1f} ns/row{ratio}")


# -- 1 and 2: identifiers ---------------------------------------------------


def identifier_columns(rows: int) -> tuple[pyarrow.Array, ...]:
    """The parts a real order identifier is built from."""
    return (
        pyarrow.array([f"S{index % 5000}" for index in range(rows)]),
        pyarrow.array(["XNAS", "XLON", "XPAR"][index % 3] for index in range(rows)),
        pyarrow.array([f"cl-{index}" for index in range(rows)]),
    )


def plain_join(*columns: pyarrow.Array) -> pyarrow.Array:
    """The join without the length prefix, for the cost of injectivity alone."""
    return pyarrow.compute.binary_join_element_wise(
        *[column.cast(pyarrow.binary(), safe=False) for column in columns],
        pyarrow.scalar(SEPARATOR, type=pyarrow.binary()),
        null_handling="replace",
        null_replacement=ABSENT,
    )


def bench_identifiers(rows: int, repeat: int) -> None:
    print(f"\nIdentifiers -- {rows:,} rows")
    columns = identifier_columns(rows)
    scalar_rows = min(rows, 200_000)
    parts = [column.to_pylist()[:scalar_rows] for column in columns]

    vector, built = timed(lambda: h128_arrow("Order", *columns), repeat)
    scalar, one_at_a_time = timed(
        lambda: [h128_of("Order", *values) for values in zip(*parts, strict=True)], repeat
    )
    # Verified before it is timed: the two must be the same identifiers.
    assert uuids_of(built)[:scalar_rows] == one_at_a_time, "the two builders disagree"

    joined, _ = timed(lambda: plain_join(*columns), repeat)
    prefixed, _ = timed(
        lambda: pyarrow.compute.binary_join_element_wise(
            *[
                value
                for column in columns
                for value in (
                    pyarrow.compute.binary_length(column)
                    .cast(pyarrow.string())
                    .cast(pyarrow.binary()),
                    column.cast(pyarrow.binary(), safe=False),
                )
            ],
            pyarrow.scalar(SEPARATOR, type=pyarrow.binary()),
            null_handling="replace",
            null_replacement=ABSENT,
        ),
        repeat,
    )

    report("h128_of, one row at a time", scalar, scalar_rows)
    report("h128_arrow, whole column", vector, rows, against=scalar / scalar_rows * rows)
    report("  of which: the join, no length prefix", joined, rows)
    report("  of which: the join, length prefixed", prefixed, rows)


# -- 3: the book ------------------------------------------------------------


def side_rows(rows: int, depth: int, base: float) -> list[dict[str, object]]:
    """One book side per row, `depth` levels deep, filled as the shape requires."""
    return [
        {
            "unix": UNIX,
            "date": DAY,
            "cunix": UNIX,
            "runix": UNIX,
            "h128": (index + 1).to_bytes(16, "big"),
            "xh128": (index + 1).to_bytes(16, "big"),
            "version": 1,
            "state": 210,
            "symbol": f"S{index % 5000}",
            "prev_state": 0,
            "side": 100,
            "px_unit": "USD",
            "qty_unit": "SHARES",
            "instrument": {"xh128": bytes(16), "symbol": "S", "kind": 110, "option_kind": 0},
            "alive": [
                {"px": base + step * 0.01, "qty": 100.0 + step, "orders": 3}
                for step in range(depth)
            ],
        }
        for index in range(rows)
    ]


def books(rows: int, depth: int) -> pyarrow.RecordBatch:
    given = {
        "unix": [UNIX] * rows,
        "date": [DAY] * rows,
        "cunix": [UNIX] * rows,
        "runix": [UNIX] * rows,
        "h128": [(index + 1).to_bytes(16, "big") for index in range(rows)],
        "xh128": [(index + 1).to_bytes(16, "big") for index in range(rows)],
        "version": [1] * rows,
        "state": [210] * rows,
        "symbol": [f"S{index % 5000}" for index in range(rows)],
        "prev_state": [0] * rows,
        "side": [0] * rows,
        "px_unit": ["USD"] * rows,
        "qty_unit": ["SHARES"] * rows,
        "instrument": [
            {"xh128": bytes(16), "symbol": "S", "kind": 110, "option_kind": 0} for _ in range(rows)
        ],
        "bid": side_rows(rows, depth, 100.0),
        "ask": side_rows(rows, depth, 100.5),
    }
    return Book.FIELD.cast_arrow_batch(pyarrow.RecordBatch.from_pydict(given))


def bench_book(rows: int, depth: int, repeat: int) -> None:
    print(f"\nBook -- {rows:,} rows, {depth} levels a side")
    batch = books(rows, depth)
    sides = BookSide.FIELD.cast_arrow_batch(
        pyarrow.RecordBatch.from_pydict(
            {
                name: [row[name] for row in side_rows(rows, depth, 100.0)]
                for name in side_rows(1, 1, 100.0)[0]
            }
        )
    )

    summarised, filled = timed(lambda: Book.summarise_arrow_batch(batch), repeat)
    side_time, side_filled = timed(lambda: BookSide.summarise_arrow_batch(sides), repeat)

    # What a reader that trusts the stored columns pays: one flat column read.
    flat, _ = timed(lambda: pyarrow.compute.mean(filled.column("micro_px")), repeat)

    # And the cheapest thing a reader could do with sides that are *already*
    # derived, which is here to say that the nested access is not the cost:
    # the walk over the levels is, and it is what `summarised` measures.
    def nested() -> object:
        bid = pyarrow.compute.struct_field(filled.column("bid"), "px")
        ask = pyarrow.compute.struct_field(filled.column("ask"), "px")
        return pyarrow.compute.mean(
            pyarrow.compute.divide(pyarrow.compute.add(bid, ask), pyarrow.scalar(2.0))
        )

    nested_time, _ = timed(nested, repeat)

    assert filled.column("spread")[0].as_py() is not None, "nothing was derived"
    assert filled.column("bid")[0].as_py()["depth"] == depth, "the sides were not derived"
    assert side_filled.column("depth")[0].as_py() == depth, "the depth is wrong"

    report("BookSide.summarise: best/depth/total, from the levels", side_time, rows)
    report("Book.summarise: both sides, then mid/spread/micro", summarised, rows)
    report("read the stored micro_px column", flat, rows, against=summarised)
    report("read bid.px/ask.px from the derived sides", nested_time, rows, against=summarised)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--repeat", type=int, default=5)
    parsed = parser.parse_args()
    rows = 100_000 if parsed.quick else 1_000_000
    repeat = 2 if parsed.quick else parsed.repeat

    bench_identifiers(rows, repeat)
    for depth in (1, 10) if parsed.quick else (1, 10, 50):
        bench_book(rows // 10, depth, repeat)


if __name__ == "__main__":
    main()
