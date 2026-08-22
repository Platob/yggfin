"""Benchmark the two hot paths of the market module: identifiers, and book prices.

Run from `python/`::

    uv run python benchmarks/bench_market.py            # full sweep
    uv run python benchmarks/bench_market.py --quick    # fewer rows, fewer repeats

Five questions, answered on columns shaped like a real feed:

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
4. **What does folding a stream into books cost?** `Book.from_events` keeps
   every live order, so its cost is not the number of books but the number of
   events and how deep the book gets -- timed on a stream shaped like a feed,
   a quarter of whose events replace an order already resting.
5. **What does reading a venue's FIX cost per message?** `FixEvents` is the
   way in, so its throughput is the ceiling on everything downstream. Timed
   on the three shapes a feed is actually made of -- an order request, a
   filled execution report, and a market-data refresh whose entries fan out
   to several events each -- because they cost very different amounts and one
   averaged number would hide that.

Every case is warmed once and reported as the best of `--repeat` runs; run the
script twice before quoting a number anywhere.
"""

from __future__ import annotations

import argparse
import pathlib
import random
import sys
import time
from collections.abc import Callable

import pyarrow
import pyarrow.compute

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep.fix import FixMessage  # noqa: E402
from rekep.market import (  # noqa: E402
    Book,
    BookSide,
    FixEvents,
    identity,  # noqa: E402
)
from rekep.market.fields import dictionary_arrow  # noqa: E402
from rekep.market.identity import (  # noqa: E402
    _binary,
    _length,
    hash_arrow,
    hash_of,
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
    """The join with no framing at all, for the cost of injectivity alone.

    Through the module's own `_binary`, not a cast written here: a benchmark
    that re-implements the step it is measuring drifts away from it, and this
    one did -- a direct `cast(binary)` works on the text columns it used to be
    fed and raises on the float one added since.
    """
    return pyarrow.compute.binary_join_element_wise(
        *[_binary(column) for column in columns],
        pyarrow.scalar(b"", type=pyarrow.binary()),
        null_handling="replace",
        null_replacement=b"",
    )


def framed_join(*columns: pyarrow.Array) -> pyarrow.Array:
    """The join `hash_arrow` actually does: each part behind its own length."""
    parts = []
    for column in columns:
        binary = _binary(column)
        parts += [_length(binary), binary]
    return pyarrow.compute.binary_join_element_wise(
        *parts,
        pyarrow.scalar(b"", type=pyarrow.binary()),
        null_handling="replace",
        null_replacement=b"",
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
    assert built.to_pylist()[:scalar_rows] == one_at_a_time, "the two builders disagree"

    joined, _ = timed(lambda: plain_join(*columns), repeat)
    framed, _ = timed(lambda: framed_join(*columns), repeat)

    report("hash_of, one row at a time", scalar, scalar_rows)
    report("hash_arrow, whole column", vector, rows, against=scalar / scalar_rows * rows)
    report("  of which: the join, unframed", joined, rows)
    report("  of which: the join, length framed", framed, rows)


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
        "hash": [index + 1 for index in range(rows)],
        "xhash": [index + 1 for index in range(rows)],
        "version": [1] * rows,
        "state": [210] * rows,
        "symbol": [f"S{index % 5000}" for index in range(rows)],
        "prev_state": [0] * rows,
        "instrument_hash": [index % 5000 + 1 for index in range(rows)],
        "side": [0] * rows,
        "px_unit": ["USD"] * rows,
        "qty_unit": ["SHARES"] * rows,
        "instrument": [
            {"xhash": 0, "symbol": "S", "kind": 110, "option_kind": 0} for _ in range(rows)
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


#: The three message shapes a feed is made of, in the spelling a log prints.
FEED = {
    "NewOrderSingle <D>": (
        "8=FIX.4.4|9=140|35=D|49=BRK|56=CLI|34=7|52=20260814-00:05:01.147|"
        "11=CL-7|55=BTC-USD|207=XCME|15=USD|54=1|38=100|44=100.5|40=2|59=1|"
        "60=20260814-00:05:01.140|10=001"
    ),
    "ExecutionReport <8>, filled": (
        "8=FIX.4.4|9=212|35=8|49=BRK|56=CLI|34=8|52=20260814-00:05:01.148|"
        "37=ORD-9|11=CL-7|17=EX-3|150=F|39=1|55=BTC-USD|207=XCME|15=USD|54=1|"
        "38=100|44=100.5|32=40|31=100.25|14=40|151=60|6=100.25|"
        "60=20260814-00:05:01.141|1057=Y|10=002"
    ),
    "MarketData <X>, 5 entries": (
        "8=FIX.4.4|9=260|35=X|49=XCME|52=20260814-00:05:01.149|55=BTC-USD|"
        "207=XCME|15=USD|268=5|"
        "279=0|269=0|270=100.0|271=5|272=20260814|273=00:05:01.100|"
        "279=1|269=1|270=100.5|271=7|273=00:05:01.110|"
        "279=0|269=0|270=99.5|271=3|273=00:05:01.120|"
        "279=2|269=1|270=101.0|271=1|273=00:05:01.130|"
        "279=0|269=2|270=100.2|271=1|273=00:05:01.140|10=003"
    ),
}


def bench_fix(rows: int, repeat: int) -> None:
    """What one message costs, from the wire line to identified market events."""
    print(f"\nFixEvents -- {rows:,} messages of each shape")
    for label, line in FEED.items():
        lines = [line] * rows
        produced = len(list(FixEvents.from_text(line)))
        assert produced, label
        assert all(one.unix and one.xhash for one in FixEvents.from_text(line)), (
            f"{label} produced an event with no time or no identity"
        )
        # Parsing and translating, together, because that is what a task does:
        # a line off a log becomes rows in a table, and splitting the two here
        # would report a number no caller can have.
        seconds, _ = timed(
            lambda lines=lines: [one for line in lines for one in FixEvents.from_text(line)],
            repeat,
        )
        report(f"{label} -> {produced} event(s)", seconds, rows)
        print(f"  {'events/s':<44} {rows * produced / seconds:>12,.0f}")


def stream(events: int) -> list[object]:
    """One instrument's order flow: a fresh order, a restatement, a cancel, a print.

    Shaped like a feed rather than like a best case -- a quarter of the events
    replace an order already resting, which is the case the fold exists for and
    the only one that has to find what it replaces.
    """
    from rekep.market import ExecKind, Execution, Instrument, Order, Side, State

    instrument = Instrument(symbol="BTC-USD", exchange="XCME")
    generate = random.Random(5)
    built: list[object] = []
    for index in range(events):
        unix = 1_787_308_200_000_000_000 + index * 1_000_000
        side = Side.BID if index % 2 else Side.ASK
        named = f"O{index % 200}"
        shape = index % 4
        if shape == 3:
            built.append(
                Execution(
                    unix=unix,
                    instrument=instrument,
                    symbol="BTC-USD",
                    px=100.0 + generate.randrange(-20, 20) * 0.01,
                    qty=1.0,
                    kind=ExecKind.TRADED,
                    exec_id=f"E{index}",
                ).with_previous(None)
            )
            continue
        built.append(
            Order(
                unix=unix,
                instrument=instrument,
                symbol="BTC-USD",
                side=side,
                px=100.0 + side.sign * -1 * generate.randrange(1, 20) * 0.01,
                qty=float(generate.randrange(1, 50)),
                order_id=named,
                state=State.CANCELLED if shape == 2 else State.NEW,
            ).with_previous(None)
        )
    return built


def bench_ceiling(rows: int, repeat: int) -> None:
    """What a compiled extension could take off a message, at most.

    Not a proposal and not a plan: a **ceiling**. Each leg makes one candidate
    for a C or Cython extension cost literally nothing -- the line is already
    tokenised, the identity frame is a constant, the digest is a constant --
    so what is still on the clock at the end is what no extension removes,
    which is Python building `Order`s, `Execution`s and their enums.

    Reported because "rewrite the hot loop in C++" is a decision that should
    be made against a number rather than against a profile: Amdahl's law is
    the whole answer here, and it is cheaper to measure than to discover.
    """
    print(f"\nThe ceiling on compiling it -- {rows:,} refreshes")
    line = FEED["MarketData <X>, 5 entries"]
    lines = [line] * rows
    parsed = [FixMessage.from_text(line) for line in lines]

    def whole() -> int:
        return sum(1 for one in lines for _ in FixEvents.from_text(one))

    def translated() -> int:
        return sum(1 for one in parsed for _ in FixEvents(message=one))

    assert whole() == translated(), "the two legs must produce the same events"
    base, _ = timed(whole, repeat)
    report("as it is", base, rows)

    for label, patch in (
        ("tokenising free", {}),
        ("+ framing free", {"frame": lambda parts: b"x"}),
        ("+ hashing free", {"frame": lambda parts: b"x", "hash_bytes": lambda raw: 1}),
    ):
        held = {name: getattr(identity, name) for name in patch}
        for name, stub in patch.items():
            setattr(identity, name, stub)
        try:
            seconds, _ = timed(translated, repeat)
        finally:
            for name, real in held.items():
                setattr(identity, name, real)
        print(f"  {label:<44} {seconds * 1000:>8.1f} ms   {base / seconds:>6.2f}x")
    print(f"  {'what no extension removes':<44} {seconds / base * 100:>8.0f}% of the run")


def bench_fold(events: int, repeat: int) -> None:
    """What folding one instrument's stream into books costs, per event and per book."""
    print(f"\nBook.from_events -- {events:,} events, one instrument")
    given = stream(events)
    produced = len(list(Book.from_events(given)))
    assert produced, "the fold produced no books at all"
    seconds, _ = timed(lambda: list(Book.from_events(given)), repeat)
    report("fold", seconds, events)
    print(f"  {'books/s':<44} {produced / seconds:>12,.0f}   ({produced:,} books)")


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
    bench_fix(rows // 20, repeat)
    bench_ceiling(rows // 20, repeat)
    bench_fold(rows // 10, repeat)


if __name__ == "__main__":
    main()
