"""Benchmark market identities, translation, folding, and Arrow conversion."""

from __future__ import annotations

import argparse
import copy
import pathlib
import random
import sys
import time
from collections.abc import Callable

import pyarrow
import pyarrow.compute

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep.fix import FixMessage, parse_arrow_array  # noqa: E402
from rekep.market import (  # noqa: E402
    MIC,
    Book,
    FixEvents,
    Instrument,
    identity,  # noqa: E402
)
from rekep.market.book import _Side  # noqa: E402
from rekep.market.fields import dictionary_arrow  # noqa: E402
from rekep.market.identity import (  # noqa: E402
    _binary,
    _length,
    hash_arrow,
    hash_of,
)

#: On an hour boundary, so `unix` and `unix_hour` agree without the fixture
#: having to derive one from the other.
UNIX = 1710374400_000000000

#: The states a day of orders actually visits, which is what makes the column
#: worth encoding: a handful of distinct values repeated through a feed.
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


def bench_instruments(rows: int, repeat: int) -> None:
    """Price the repeated identity spellings a feed sends on every message."""

    def build(distinct: int) -> list[Instrument]:
        return [Instrument(symbol=f"S{index % distinct}", exchange="XPAR") for index in range(rows)]

    print(f"\nInstrument identity cache -- {rows:,} rows")
    unique, uncached = timed(lambda: build(rows), repeat)
    repeated, cached = timed(lambda: build(100), repeat)
    assert cached[0].xhash == cached[100].xhash
    assert uncached[0].xhash != uncached[-1].xhash
    report("all spellings unique", unique, rows)
    report("100 repeated spellings", repeated, rows, against=unique)


def bench_instrument_logs(rows: int, repeat: int) -> None:
    """Decode package-authored instrument rows directly and through generic FIX."""
    from rekep.enums import AssetKind, Currency, Side
    from rekep.market import Leg

    sample = min(rows, 500)
    instrument = Instrument(
        unix=UNIX,
        symbol="CAL-27",
        kind=AssetKind.MULTILEG,
        security_id="FR0000000001",
        security_id_source="4",
        alt_ids={"RIC": "CAL.N"},
        security_type="MLEG",
        exchange="XPAR",
        currency=Currency.EUR,
        multiplier=10.0,
        tick=0.01,
        lot=1.0,
        legs=[
            Leg(
                symbol="JUN-27",
                side=Side.BUY,
                kind=AssetKind.FUTURE,
                security_type="FUT",
            )
        ],
    ).with_previous(None)
    assert instrument is not None
    log = instrument.into_log()

    def through_registry() -> list[Instrument]:
        built = []
        for _ in range(sample):
            parsed = next(log.into_fix_events().into_instruments())
            built.append(log._instrument_version(parsed))
        return built

    def direct() -> list[Instrument]:
        return [log.into_instrument() for _ in range(sample)]

    registry, generic = timed(through_registry, repeat)
    normalized, decoded = timed(direct, repeat)
    assert generic[0].into_dict() == instrument.into_dict()
    assert decoded[0].into_dict() == instrument.into_dict()

    print(f"\nInstrument <-> normalized Log -- {sample:,} rows")
    report("generic FIX/registry reconstruction", registry, sample)
    report("direct normalized-row decode", normalized, sample, against=registry)


def bench_mics(rows: int, repeat: int) -> None:
    """Decode the low-cardinality venue column a feed repeats per event."""
    values = [("XPAR", "XNAS", "XCME")[index % 3] for index in range(rows)]
    parse = MIC._from_text.__wrapped__
    uncached, reference = timed(lambda: [parse(MIC, value) for value in values], repeat)
    seconds, decoded = timed(lambda: [MIC.from_str(value) for value in values], repeat)
    assert decoded == reference
    assert {one.code for one in decoded} == {"XPAR", "XNAS", "XCME"}
    print(f"\nMIC decoding cache -- {rows:,} rows")
    report("normalise and decode every row", uncached, rows)
    report("three repeated venue spellings", seconds, rows, against=uncached)


# -- 3: the book ------------------------------------------------------------


def levels(depth: int, base: float) -> list[dict[str, object]]:
    """One side's live levels, `depth` of them, best first."""
    return [
        {
            "px": base + step * 0.01,
            "qty": 100.0 + step,
            "order_xhash": [step * 3 + offset for offset in range(3)],
            "exec_xhash": [],
        }
        for step in range(depth)
    ]


def envelope(rows: int) -> dict[str, object]:
    """The NOT NULL half of any market event, one column per row."""
    return {
        "unix": [UNIX] * rows,
        "unix_hour": [UNIX] * rows,
        "etype": [0] * rows,
        "cunix": [UNIX] * rows,
        "runix": [UNIX] * rows,
        "hash": [index + 1 for index in range(rows)],
        "xhash": [index + 1 for index in range(rows)],
        "version": [1] * rows,
        "state": [210] * rows,
        "code": [f"S{index % 5000}" for index in range(rows)],
        "xcode": [f"S{index % 5000}" for index in range(rows)],
        "prev_state": [0] * rows,
        "instrument_xhash": [index % 5000 + 1 for index in range(rows)],
        "kind": [0] * rows,
        "side": [0] * rows,
        "px_unit": ["USD"] * rows,
        "qty_unit": ["SHARES"] * rows,
    }


def books(rows: int, depth: int) -> pyarrow.RecordBatch:
    """A batch of books, both sides flat and only their levels filled in."""
    given = envelope(rows) | {
        "bid_levels": [levels(depth, 100.0)] * rows,
        "ask_levels": [levels(depth, 100.5)] * rows,
    }
    return Book.into_field().cast_arrow_batch(pyarrow.RecordBatch.from_pydict(given))


def bench_book(rows: int, depth: int, repeat: int) -> None:
    print(f"\nBook -- {rows:,} rows, {depth} levels a side")
    batch = books(rows, depth)

    summarised, filled = timed(lambda: Book.summarise_arrow_batch(batch), repeat)

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


def bench_fix_parser(rows: int, repeat: int) -> None:
    """Batch tokenisation gate before Python event materialisation."""
    print(f"\nFIX batch parser -- {rows:,} messages of each shape")
    for label, line in FEED.items():
        column = pyarrow.array([line] * rows)
        expected = FixMessage.from_text(line).pairs
        assert parse_arrow_array(column.slice(0, 2)).to_pylist() == [expected, expected]
        seconds, parsed = timed(lambda column=column: len(parse_arrow_array(column)), repeat)
        assert parsed == rows
        rate = rows / seconds
        print(f"  {label:<44} {rate:>12,.0f} rows/s")
        assert rate >= 50_000, f"{label} parsed at only {rate:,.0f} rows/s"


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
    """One instrument's orders, replacements, cancels, fills and rejections.

    Shaped like a feed rather than like a best case -- a quarter of the events
    replace an order already resting, which is the case the fold exists for and
    the only one that has to find what it replaces; one in twenty is malformed.
    """
    from rekep.market import Execution, Instrument, MarketKind, Order, Side, State

    instrument = Instrument(symbol="BTC-USD", exchange="XCME")

    def ready(event):
        built = event.attach_instrument(instrument).with_previous(None)
        if built is None:
            raise AssertionError("a first event cannot be unchanged")
        return built

    generate = random.Random(5)
    built: list[object] = []
    for index in range(events):
        unix = 1_787_308_200_000_000_000 + index * 1_000_000
        side = Side.BID if index % 2 else Side.ASK
        named = f"O{index % 200}"
        shape = index % 4
        if shape == 3:
            built.append(
                ready(
                    Execution(
                        unix=unix,
                        code="BTC-USD",
                        px=100.0 + generate.randrange(-20, 20) * 0.01,
                        qty=1.0,
                        state=State.FILLED,
                        exec_id=f"E{index}",
                    )
                )
            )
            continue
        quoted = 100.0 + side.sign * -1 * generate.randrange(1, 20) * 0.01
        built.append(
            ready(
                Order(
                    unix=unix,
                    code="BTC-USD",
                    side=side,
                    px=float("nan") if index % 20 == 0 else quoted,
                    qty=float(generate.randrange(1, 50)),
                    order_id=named,
                    state=State.CANCELLED if shape == 2 else State.NEW,
                    kind=MarketKind.LIMIT_ORDER if index % 20 == 0 else MarketKind.UNKNOWN,
                )
            )
        )
    return built


def bench_lifecycle(rows: int, repeat: int) -> None:
    """Cached state comparison and indexed explicit-expiry lookup."""
    import heapq

    from rekep.market import Order, Side, State

    previous = next(one for one in stream(4) if isinstance(one, Order) and one.px == one.px)
    current = copy.copy(previous)
    pictured = copy.copy(previous)
    pictured.sunix = previous.unix
    assert current.same_as(previous) and pictured.same_as(pictured)

    print(f"\nLifecycle hot paths -- {rows:,} comparisons")
    for label, one, other in (
        ("same_as, ordinary event", current, previous),
        ("same_as, snapshot", pictured, pictured),
    ):
        seconds, equal = timed(
            lambda one=one, other=other: sum(one.same_as(other) for _ in range(rows)), repeat
        )
        assert equal == rows
        report(label, seconds, rows)
        print(f"  {'states/s':<44} {rows / seconds:>12,.0f}")

    live = min(max(rows // 10, 1_000), 10_000)
    probes = max(rows // 1_000, 100)
    side = _Side(Side.BID)
    expiry = UNIX + 10**15
    clocks = list(range(live))
    random.Random(9).shuffle(clocks)
    for index, clock in enumerate(clocks):
        side.apply(
            Order(
                unix=UNIX + clock,
                cunix=UNIX + clock,
                xhash=index + 1,
                hash=index + 1,
                code="BTC-USD",
                xcode=f"O{index}",
                order_id=f"O{index}",
                side=Side.BID,
                px=100.0,
                qty=1.0,
                state=State.NEW,
                eunix=expiry if index % 100 == 0 else None,
            )
        )
    indexed = sum(len(bucket) for bucket in side._expiring.values())
    assert len(side.orders) == live and indexed == (live + 99) // 100

    def probe_expiry() -> int:
        return sum(len(side.expire(UNIX + live)) for _ in range(probes))

    seconds, expired = timed(probe_expiry, repeat)
    assert expired == 0 and len(side.orders) == live
    print(f"\nExplicit expiry index -- {live:,} live orders, {indexed:,} indexed")
    report("expire, no due order", seconds, probes)
    print(f"  {'probes/s':<44} {probes / seconds:>12,.0f}")

    def sorted_tie() -> list[Order]:
        return sorted(
            side.orders.values(),
            key=lambda order: (order.cunix or order.unix, order.xhash),
            reverse=True,
        )[:1]

    sorted_seconds, sorted_expected = timed(sorted_tie, repeat)
    max_seconds, max_actual = timed(lambda: side._evictions(1), repeat)
    assert [one.xhash for one in max_actual] == [one.xhash for one in sorted_expected]
    print(f"\nCapacity tie -- {live:,} orders at one price, one eviction")
    report("sorted whole price level", sorted_seconds, live)
    report("max within price level", max_seconds, live, against=sorted_seconds)

    capped = _Side(Side.BID)
    for index in range(live):
        capped.apply(
            Order(
                unix=UNIX + index,
                cunix=UNIX + index,
                xhash=index + 1,
                hash=index + 1,
                code="BTC-USD",
                xcode=f"O{index}",
                order_id=f"O{index}",
                side=Side.BID,
                px=float(live - index),
                qty=1.0,
                state=State.NEW,
            )
        )

    def global_scan() -> list[Order]:
        return heapq.nlargest(
            1,
            capped.orders.values(),
            key=lambda order: (-(order.px or 0.0), order.cunix or order.unix, order.xhash),
        )

    scanned, expected = timed(global_scan, repeat)
    indexed, actual = timed(lambda: capped._evictions(1), repeat)
    assert [one.xhash for one in actual] == [one.xhash for one in expected]
    print(f"\nCapacity eviction -- {live:,} live price levels, one eviction")
    report("global live-order scan", scanned, live)
    report("indexed worst-level traversal", indexed, live, against=scanned)


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
    from rekep.market import BookIterator, State

    print(f"\nBook.from_events -- {events:,} events, one instrument")
    given = stream(events)

    def fold(max_side_alive: int | None = None) -> tuple[list[Book], BookIterator]:
        folding = BookIterator.from_events(given, snapshot_every=0, max_side_alive=max_side_alive)
        return list(folding.books), folding

    books, _ = fold()
    produced = len(books)
    assert produced, "the fold produced no books at all"
    rejected = sum(
        one.state is State.INTERNAL_REJECTED for book in books for one in book.order_events or ()
    )
    assert rejected == (events + 19) // 20, "the benchmark lost its malformed-order leg"
    seconds, _ = timed(fold, repeat)
    report("fold", seconds, events)
    print(f"  {'books/s':<44} {produced / seconds:>12,.0f}   ({produced:,} books)")
    print(f"  {'rejected orders':<44} {rejected:>12,}")

    guarded, (same, unchanged) = timed(lambda: fold(1_000), repeat)
    assert [one.hash for one in same] == [one.hash for one in books]
    assert all(
        len(state.bid.orders) <= 1_000 and len(state.ask.orders) <= 1_000
        for state in unchanged.folding.values()
    )
    report("fold, inactive max_side_alive", guarded, events)

    bounded, (limited, folding) = timed(lambda: fold(10), repeat)
    expired = sum(
        event.state is State.INTERNAL_EXPIRED
        for book in limited
        for event in book.order_events or ()
    )
    assert expired, "the bound emitted no auditable expiry"
    assert all(
        len(state.bid.orders) <= 10 and len(state.ask.orders) <= 10
        for state in folding.folding.values()
    )
    report("fold, max_side_alive=10", bounded, events)
    print(f"  {'synthetic expiries':<44} {expired:>12,}")

    sample = books[:12_000]
    schema = Book.into_field().into_arrow_schema()

    def document_projection() -> pyarrow.Table:
        batch = pyarrow.RecordBatch.from_pylist(
            [book.into_dict() for book in sample], schema=schema
        )
        return pyarrow.Table.from_batches([batch], schema=schema)

    generic, expected = timed(document_projection, repeat)
    assert expected.num_rows == len(sample)
    print(f"\nBook to Arrow -- {len(sample):,} rows")
    report("generic document projection", generic, len(sample))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--repeat", type=int, default=5)
    parsed = parser.parse_args()
    rows = 2_000 if parsed.quick else parsed.rows
    repeat = 1 if parsed.quick else parsed.repeat

    # Acero costs its own initialisation on the first grouped aggregate in a
    # process, and `_list_sums` is one. Left unwarmed it lands on whichever
    # sweep runs first and makes the shallowest book look like the slowest --
    # which it did, at 1.6x the cost of a book ten times deeper.
    Book.summarise_arrow_batch(books(16, 2))

    bench_identifiers(rows, repeat)
    bench_instruments(rows, repeat)
    bench_instrument_logs(rows, repeat)
    bench_mics(rows, repeat)
    for depth in (1, 10) if parsed.quick else (1, 10, 50):
        bench_book(rows // 10, depth, repeat)
    bench_codes(rows, repeat)
    bench_lifecycle(rows, repeat)
    bench_fix_parser(rows // 20, repeat)
    bench_fix(rows // 20, repeat)
    bench_ceiling(rows // 20, repeat)
    bench_fold(rows // 10, repeat)


if __name__ == "__main__":
    main()
