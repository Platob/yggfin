"""Benchmark the two hot paths of the market module: identifiers, and book prices.

Run from `python/`::

    uv run python benchmarks/bench_market.py            # full sweep
    uv run python benchmarks/bench_market.py --quick    # fewer rows, fewer repeats

Three questions, answered on columns shaped like a real feed:

1. **What does `hash_arrow` buy over `hash_of` per row?** Both build the same
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
import pathlib
import sys
import time
from collections.abc import Callable

import pyarrow
import pyarrow.compute

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep.market import Book, BookSide  # noqa: E402
from rekep.market.fields import dictionary_arrow  # noqa: E402
from rekep.market.identity import (  # noqa: E402
    ABSENT,
    SEPARATOR,
    _binary,
    _length,
    hash_arrow,
    hash_of,
    uuids_of,
)

#: On an hour boundary, so `unix` and `hunix` agree without the fixture
#: having to derive one from the other.
UNIX = 1710374400_000000000

#: The states a day of orders actually visits, which is what makes the column
#: worth encoding: a handful of distinct values repeated a million times.
STATES = [210, 310, 410, 510, 610, 240]


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
    # A float and an integer among them on purpose: the two builders diverged
    # on exactly those, and a guard fed only text agreed with itself.
    return (
        pyarrow.array([f"S{index % 5000}" for index in range(rows)]),
        pyarrow.array(["XNAS", "XLON", "XPAR"][index % 3] for index in range(rows)),
        pyarrow.array([f"cl-{index}" for index in range(rows)]),
        pyarrow.array([10.0 + index % 97 * 0.01 for index in range(rows)], pyarrow.float64()),
        pyarrow.array([index % 1000 for index in range(rows)], pyarrow.int64()),
    )


def plain_join(*columns: pyarrow.Array) -> pyarrow.Array:
    """The join without the length prefix, for the cost of injectivity alone.

    Through the module's own `_binary`, not a cast written here: a benchmark
    that re-implements the step it is measuring drifts away from it, and this
    one did -- a direct `cast(binary)` works on the text columns it used to be
    fed and raises on the float one added since.
    """
    return pyarrow.compute.binary_join_element_wise(
        *[_binary(column) for column in columns],
        pyarrow.scalar(SEPARATOR, type=pyarrow.binary()),
        null_handling="replace",
        null_replacement=ABSENT,
    )


def prefixed_join(*columns: pyarrow.Array) -> pyarrow.Array:
    """The join `hash_arrow` actually does: each part behind its own length."""
    parts = []
    for column in columns:
        binary = _binary(column)
        parts += [_length(binary), binary]
    return pyarrow.compute.binary_join_element_wise(
        *parts,
        pyarrow.scalar(SEPARATOR, type=pyarrow.binary()),
        null_handling="replace",
        null_replacement=ABSENT,
    )


def bench_identifiers(rows: int, repeat: int) -> None:
    print(f"\nIdentifiers -- {rows:,} rows")
    columns = identifier_columns(rows)
    scalar_rows = min(rows, 200_000)
    parts = [column.to_pylist()[:scalar_rows] for column in columns]

    vector, built = timed(lambda: hash_arrow("Order", *columns), repeat)
    scalar, one_at_a_time = timed(
        lambda: [hash_of("Order", *values) for values in zip(*parts, strict=True)], repeat
    )
    # Verified before it is timed: the two must be the same identifiers.
    assert uuids_of(built)[:scalar_rows] == one_at_a_time, "the two builders disagree"

    joined, _ = timed(lambda: plain_join(*columns), repeat)
    prefixed, _ = timed(lambda: prefixed_join(*columns), repeat)

    report("hash_of, one row at a time", scalar, scalar_rows)
    report("hash_arrow, whole column", vector, rows, against=scalar / scalar_rows * rows)
    report("  of which: the join, no length prefix", joined, rows)
    report("  of which: the join, length prefixed", prefixed, rows)


# -- 3: the book ------------------------------------------------------------


def levels(depth: int, base: float) -> list[dict[str, object]]:
    """One side's live levels, `depth` of them, best first."""
    return [{"px": base + step * 0.01, "qty": 100.0 + step, "orders": 3} for step in range(depth)]


def envelope(rows: int) -> dict[str, object]:
    """The NOT NULL half of any market event, one column per row."""
    return {
        "unix": [UNIX] * rows,
        "hunix": [UNIX] * rows,
        "etype": [0] * rows,
        "cunix": [UNIX] * rows,
        "runix": [UNIX] * rows,
        "hash": [(index + 1).to_bytes(16, "big") for index in range(rows)],
        "xhash": [(index + 1).to_bytes(16, "big") for index in range(rows)],
        "version": [1] * rows,
        "state": [210] * rows,
        "symbol": [f"S{index % 5000}" for index in range(rows)],
        "prev_state": [0] * rows,
        "side": [0] * rows,
        "px_unit": ["USD"] * rows,
        "qty_unit": ["SHARES"] * rows,
        "instrument": [
            {"xhash": bytes(16), "symbol": "S", "kind": 110, "option_kind": 0} for _ in range(rows)
        ],
    }


def sides(rows: int, depth: int) -> pyarrow.RecordBatch:
    """A batch of book sides, each carrying its own levels and nothing derived."""
    given = envelope(rows) | {"alive": [levels(depth, 100.0)] * rows}
    return BookSide.FIELD.cast_arrow_batch(pyarrow.RecordBatch.from_pydict(given))


def books(rows: int, depth: int) -> pyarrow.RecordBatch:
    """A batch of books, both sides flat and only their levels filled in."""
    given = envelope(rows) | {
        "bid_alive": [levels(depth, 100.0)] * rows,
        "ask_alive": [levels(depth, 100.5)] * rows,
    }
    return Book.FIELD.cast_arrow_batch(pyarrow.RecordBatch.from_pydict(given))


def bench_book(rows: int, depth: int, repeat: int) -> None:
    print(f"\nBook -- {rows:,} rows, {depth} levels a side")
    batch, side_batch = books(rows, depth), sides(rows, depth)

    summarised, filled = timed(lambda: Book.summarise_arrow_batch(batch), repeat)
    side_time, side_filled = timed(lambda: BookSide.summarise_arrow_batch(side_batch), repeat)

    # What a reader that trusts the stored columns pays: one flat column read.
    flat, _ = timed(lambda: pyarrow.compute.mean(filled.column("micro_px")), repeat)

    # And the cheapest thing a reader could do with sides that are *already*
    # derived, which is here to say that the flat access is not the cost: the
    # walk over the levels is, and it is what `summarised` measures.
    def crossed() -> object:
        return pyarrow.compute.mean(
            pyarrow.compute.divide(
                pyarrow.compute.add(filled.column("bid_px"), filled.column("ask_px")),
                pyarrow.scalar(2.0),
            )
        )

    crossed_time, _ = timed(crossed, repeat)

    assert filled.column("spread")[0].as_py() is not None, "nothing was derived"
    assert filled.column("bid_depth")[0].as_py() == depth, "the sides were not derived"
    assert side_filled.column("depth")[0].as_py() == depth, "the depth is wrong"

    report("BookSide.summarise: best/depth/total, from the levels", side_time, rows)
    report("Book.summarise: both sides, then mid/spread/micro", summarised, rows)
    report("read the stored micro_px column", flat, rows, against=summarised)
    report("recompute a mid from the stored bid_px/ask_px", crossed_time, rows, against=summarised)


# -- 4: enum storage ---------------------------------------------------------


def bench_codes(rows: int, repeat: int) -> None:
    """What dictionary encoding buys a column whose whole point is that it repeats."""
    print(f"\nRanged codes -- {rows:,} rows, {len(STATES)} distinct")
    plain = pyarrow.array([STATES[index % len(STATES)] for index in range(rows)], pyarrow.int32())
    target = pyarrow.dictionary(pyarrow.int8(), pyarrow.int32())

    encode, encoded = timed(lambda: dictionary_arrow(plain, target), repeat)
    decode, decoded = timed(lambda: dictionary_arrow(encoded, pyarrow.int32()), repeat)
    indices = encoded.indices
    reindex, _ = timed(lambda: dictionary_arrow(indices, target), repeat)
    assert decoded.equals(plain), "the round trip lost values"

    report("dictionary_arrow: values -> encoded (case 1)", encode, rows)
    report("dictionary_arrow: indices -> encoded (case 2)", reindex, rows, against=encode)
    report("dictionary_arrow: encoded -> values", decode, rows)
    print(
        f"  {'bytes in memory':<44} "
        f"{plain.nbytes / 1e6:8.2f} MB -> {encoded.nbytes / 1e6:7.2f} MB"
        f"  {plain.nbytes / max(encoded.nbytes, 1):6.1f}x"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--repeat", type=int, default=5)
    parsed = parser.parse_args()
    rows = 100_000 if parsed.quick else 1_000_000
    repeat = 2 if parsed.quick else parsed.repeat

    # Acero costs its own initialisation on the first grouped aggregate in a
    # process, and `_list_sums` is one. Left unwarmed it lands on whichever
    # sweep runs first and makes the shallowest book look like the slowest --
    # which it did, at 1.6x the cost of a book ten times deeper.
    Book.summarise_arrow_batch(books(16, 2))
    BookSide.summarise_arrow_batch(sides(16, 2))

    bench_identifiers(rows, repeat)
    for depth in (1, 10) if parsed.quick else (1, 10, 50):
        bench_book(rows // 10, depth, repeat)
    bench_codes(rows, repeat)


if __name__ == "__main__":
    main()
