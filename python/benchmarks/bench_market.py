"""Benchmark market identities, translation, folding, and Arrow conversion."""

from __future__ import annotations

import builtins
import contextlib
import copy
import dataclasses
import datetime
import heapq
import os
import pathlib
import random
import sys
import threading
import time
import tracemalloc
from collections import Counter
from collections.abc import Callable, Iterator

import pyarrow
import pyarrow.compute

if os.name == "nt":
    import ctypes

    class _ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [
            ("cb", ctypes.c_ulong),
            ("page_fault_count", ctypes.c_ulong),
            ("peak_working_set_size", ctypes.c_size_t),
            ("working_set_size", ctypes.c_size_t),
            ("quota_peak_paged_pool_usage", ctypes.c_size_t),
            ("quota_paged_pool_usage", ctypes.c_size_t),
            ("quota_peak_non_paged_pool_usage", ctypes.c_size_t),
            ("quota_non_paged_pool_usage", ctypes.c_size_t),
            ("pagefile_usage", ctypes.c_size_t),
            ("peak_pagefile_usage", ctypes.c_size_t),
        ]

    _get_current_process = ctypes.windll.kernel32.GetCurrentProcess  # type: ignore[attr-defined]
    _get_current_process.restype = ctypes.c_void_p
    _get_process_memory_info = ctypes.windll.psapi.GetProcessMemoryInfo  # type: ignore[attr-defined]
    _get_process_memory_info.argtypes = (
        ctypes.c_void_p,
        ctypes.POINTER(_ProcessMemoryCounters),
        ctypes.c_ulong,
    )


# `src` for the package under measurement, and this folder for `_bench`,
# so a benchmark imports the same whether it is run or imported.
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from _bench import best_of, identical, parser, report  # noqa: E402

from rekep import FixMsg  # noqa: E402
from rekep.enums import State  # noqa: E402
from rekep.fix import parse_arrow_array  # noqa: E402
from rekep.fix.message import parse_pairs  # noqa: E402
from rekep.market import (  # noqa: E402
    MIC,
    Book,
    FixEvents,
    Instrument,
    identity,  # noqa: E402
)
from rekep.market.book import _Side  # noqa: E402
from rekep.market.event import SECOND  # noqa: E402
from rekep.market.fields import dictionary_arrow  # noqa: E402
from rekep.market.identity import (  # noqa: E402
    _binary,
    _length,
    hash_arrow,
    hash_bytes_of,
    hash_of,
)

#: One hour boundary in the nanosecond event clock and second partition clock.
UNIX = 1710374400_000000000
UNIX_PARTITION = UNIX // SECOND

#: The states a day of orders actually visits, which is what makes the column
#: worth encoding: a handful of distinct values repeated through a feed.
STATES = [
    int(State.NEW),
    int(State.PARTIALLY_FILLED),
    int(State.FILLED),
    int(State.CANCELLED),
    int(State.REJECTED),
    int(State.PENDING_CANCEL),
]

#: The width `State` declares for its own column, which is what a stored
#: lifecycle column is: a packed ASCII mnemonic, not a small ordinal.
STATE_WIDTH = State.into_arrow_type().index_type
FIX_DICTIONARY = pathlib.Path(__file__).resolve().parents[2] / "data" / "fix"


def timed(work: Callable[[], object], repeat: int) -> tuple[float, object]:
    """`best_of`, keeping what the work returned -- the callers here read it."""
    return best_of(work, repeat), work()


@contextlib.contextmanager
def peak_rss() -> Iterator[Callable[[], int]]:
    """Sample the process resident set while one streaming path runs."""
    peak = [_rss_bytes()]
    stop = threading.Event()

    def watch() -> None:
        while not stop.wait(0.002):
            peak[0] = max(peak[0], _rss_bytes())

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    try:
        yield lambda: peak[0]
    finally:
        stop.set()
        watcher.join()
        peak[0] = max(peak[0], _rss_bytes())


def _rss_bytes() -> int:
    """Resident bytes right now, or zero where the platform does not expose them."""
    if os.name == "nt":
        counters = _ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        if _get_process_memory_info(_get_current_process(), ctypes.byref(counters), counters.cb):
            return int(counters.working_set_size)
        return 0
    try:
        with open("/proc/self/statm") as stream:
            return int(stream.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError, AttributeError):
        return 0


@contextlib.contextmanager
def counted_operations() -> Iterator[Counter[str]]:
    """Count expensive fold operations without adding hooks to production code."""
    import rekep.market.book as book_module
    import rekep.market.event as event_module
    import rekep.market.identity as identity_module
    from rekep.market import Book as BookRow
    from rekep.market import Event, Level, Order

    counts: Counter[str] = Counter()
    restored: list[tuple[object, str, object]] = []
    absent = object()

    def patch(owner: object, name: str, replacement: object) -> None:
        previous = getattr(owner, name, absent)
        restored.append((owner, name, previous))
        setattr(owner, name, replacement)

    def calls(name: str, function: Callable[..., object]) -> Callable[..., object]:
        def counted(*args: object, **kwargs: object) -> object:
            counts[name] += 1
            return function(*args, **kwargs)

        return counted

    patch(event_module, "hash_of", calls("hash_of", event_module.hash_of))
    patch(identity_module, "frame", calls("frame", identity_module.frame))
    patch(identity_module, "hash_bytes", calls("hash_bytes", identity_module.hash_bytes))
    patch(Event, "life_hash", calls("life_hash", Event.life_hash))

    patch(BookRow, "__init__", calls("book_objects", BookRow.__init__))
    patch(Level, "__init__", calls("level_objects", Level.__init__))

    original_copy = copy.copy

    def counted_copy(value: object) -> object:
        if isinstance(value, Order):
            counts["order_copies"] += 1
        elif isinstance(value, BookRow):
            counts["book_copies"] += 1
        return original_copy(value)

    patch(copy, "copy", counted_copy)
    original_replace = dataclasses.replace

    def counted_replace(value: object, /, **changes: object) -> object:
        counts["dataclasses_replace"] += 1
        return original_replace(value, **changes)

    patch(dataclasses, "replace", counted_replace)

    for name in ("heappush", "heappop", "heapreplace", "nlargest", "nsmallest"):
        function = getattr(heapq, name)
        patch(heapq, name, calls(f"heap_{name}", function))
    for name in ("bisect_left", "bisect_right", "insort_left", "insort_right"):
        function = getattr(book_module.bisect, name)
        patch(book_module.bisect, name, calls(f"bisect_{name}", function))

    side = getattr(book_module, "_Side", None)
    if side is not None and hasattr(side, "standing"):
        patch(side, "standing", calls("standing_probes", side.standing))
        patch(side, "expire", calls("expiry_scans", side.expire))
        patch(side, "_join", calls("level_joins", side._join))
        patch(side, "_leave", calls("level_leaves", side._leave))
        original_sorted_orders = side.__dict__["sorted_orders"]

        def counted_sorted_orders(instance: object) -> object:
            counts["full_order_scan_calls"] += 1
            counts["full_orders_scanned"] += len(instance.orders)
            return original_sorted_orders.__get__(instance, side)

        patch(side, "sorted_orders", property(counted_sorted_orders))

    original_snapshot = Event.make_snapshot

    def counted_snapshot(event: Event, *args: object, **kwargs: object) -> object:
        taken = original_snapshot(event, *args, **kwargs)
        if isinstance(event, BookRow) and taken is not None:
            counts["snapshot_materializations"] += 1
        return taken

    patch(Event, "make_snapshot", counted_snapshot)
    patch(book_module, "sorted", calls("sort_calls", builtins.sorted))

    try:
        yield counts
    finally:
        for owner, name, previous in reversed(restored):
            if previous is absent:
                delattr(owner, name)
            else:
                setattr(owner, name, previous)


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
    # Verified before it is timed: the two must be the same identifiers. One
    # builds a column and answers in the sixteen bytes a column stores; the
    # other builds one identity and answers in the integer a reader works in.
    assert built.to_pylist()[:scalar_rows] == [hash_bytes_of(one) for one in one_at_a_time], (
        "the two builders disagree"
    )

    joined, _ = timed(lambda: plain_join(*columns), repeat)
    framed, _ = timed(lambda: framed_join(*columns), repeat)

    report("hash_of, one row at a time", scalar, scalar_rows)
    report("hash_arrow, whole column", vector, rows, against=scalar / scalar_rows * rows)
    report("  of which: the join, unframed", joined, rows)
    report("  of which: the join, length framed", framed, rows)


def bench_instruments(rows: int, repeat: int) -> None:
    """Price the repeated identity spellings a feed sends on every message."""

    def build(distinct: int) -> list[Instrument]:
        return [
            Instrument(symbol=f"S{index % distinct}", securityexchange="XPAR")
            for index in range(rows)
        ]

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
    from rekep.text import FixMsg

    sample = min(rows, 500)
    instrument = Instrument(
        unix=UNIX,
        symbol="CAL-27",
        kind=AssetKind.MULTILEG,
        securityid="FR0000000001",
        securityidsource="4",
        altids={"RIC": "CAL.N"},
        securitytype="MLEG",
        securityexchange="XPAR",
        currency=Currency.EUR,
        contractmultiplier=10.0,
        minpriceincrement=0.01,
        roundlot=1.0,
        legs=[
            Leg(
                symbol="JUN-27",
                side=Side.BUY,
                kind=AssetKind.FUTURE,
                securitytype="FUT",
            )
        ],
    ).with_previous(None)
    assert instrument is not None
    # The registry leg is deliberately FIX.4.4; protocol reads never infer a
    # version when neither BeginString nor FIXT application-version tags exist.
    log = instrument.into_fixmsg(beginstring="FIX.4.4")
    source = next(
        iter(FixMsg.into_arrow_reader((log for _ in range(sample)), batch_row_size=sample))
    )

    def through_registry() -> list[Instrument]:
        built = []
        for _ in range(sample):
            parsed = next(log.into_fix_events().into_instruments())
            built.append(log._instrument_version(parsed))
        return built

    def direct() -> pyarrow.RecordBatch:
        return FixMsg.into_instrument_arrow_batch(source)

    registry, generic = timed(through_registry, repeat)
    normalized, decoded = timed(direct, repeat)
    assert generic[0].into_dict() == instrument.into_dict()
    assert (
        Instrument.from_dict(decoded.slice(0, 1).to_pylist()[0]).into_dict()
        == instrument.into_dict()
    )

    print(f"\nInstrument <-> normalized FixMsg -- {sample:,} rows")
    report("generic FIX/registry reconstruction", registry, sample)
    report("batch normalized-row projection", normalized, sample, against=registry)


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
        }
        for step in range(depth)
    ]


def envelope(rows: int) -> dict[str, object]:
    """The NOT NULL half of any market event, one column per row."""
    return {
        "unix": [UNIX] * rows,
        "unixpartition": [UNIX_PARTITION] * rows,
        "eventtype": [0] * rows,
        "creaunix": [UNIX] * rows,
        "recunix": [UNIX] * rows,
        "hash": [index + 1 for index in range(rows)],
        "xhash": [index + 1 for index in range(rows)],
        "linkedhashes": [[] for _ in range(rows)],
        "version": [1] * rows,
        "state": [210] * rows,
        "code": [f"S{index % 5000}" for index in range(rows)],
        "altids": [{"symbol": f"S{index % 5000}"} for index in range(rows)],
        "instrumentxhash": [index % 5000 + 1 for index in range(rows)],
        "instrumentcode": [f"S{index % 5000}" for index in range(rows)],
        "kind": [0] * rows,
        "side": [0] * rows,
        "pxunit": ["USD"] * rows,
        "qtyunit": ["SHARES"] * rows,
    }


def books(rows: int, depth: int) -> pyarrow.RecordBatch:
    """A batch of snapshots, both sides flat and only their levels filled in."""
    given = envelope(rows) | {
        "snapunix": [UNIX] * rows,
        "biddepth": [depth] * rows,
        "askdepth": [depth] * rows,
        "bidlevels": [levels(depth, 100.0)] * rows,
        "asklevels": [levels(depth, 100.5)] * rows,
        "deltas": [[] for _ in range(rows)],
        "executions": [[] for _ in range(rows)],
        "bidalive": [[] for _ in range(rows)],
        "askalive": [[] for _ in range(rows)],
    }
    return Book.into_field().cast_arrow_batch(pyarrow.RecordBatch.from_pydict(given))


def bench_book(rows: int, depth: int, repeat: int) -> None:
    print(f"\nBook -- {rows:,} rows, {depth} levels a side")
    batch = books(rows, depth)

    summarised, filled = timed(lambda: Book.summarise_arrow_batch(batch), repeat)

    # What a reader that trusts the stored columns pays: one flat column read.
    flat, _ = timed(lambda: pyarrow.compute.mean(filled.column("vwap")), repeat)

    # And the cheapest thing a reader could do with sides that are *already*
    # derived, which is here to say that the flat access is not the cost: the
    # walk over the levels is, and it is what `summarised` measures.
    def crossed() -> object:
        return pyarrow.compute.mean(
            pyarrow.compute.divide(
                pyarrow.compute.add(filled.column("bidpx"), filled.column("askpx")),
                pyarrow.scalar(2.0),
            )
        )

    crossed_time, _ = timed(crossed, repeat)

    assert filled.column("spread")[0].as_py() is not None, "nothing was derived"
    assert filled.column("biddepth")[0].as_py() == depth, "the sides were not derived"

    report("Book.summarise: both sides, then mid/spread/vwap", summarised, rows)
    report("read the stored vwap column", flat, rows, against=summarised)
    report("recompute a mid from the stored bidpx/askpx", crossed_time, rows, against=summarised)


# -- 4: enum storage ---------------------------------------------------------


def bench_codes(rows: int, repeat: int) -> None:
    """What dictionary encoding buys a column whose whole point is that it repeats."""
    print(f"\nASCII codes -- {rows:,} rows, {len(STATES)} distinct")
    plain = pyarrow.array([STATES[index % len(STATES)] for index in range(rows)], STATE_WIDTH)
    target = pyarrow.dictionary(pyarrow.int8(), STATE_WIDTH)

    encode, encoded = timed(lambda: dictionary_arrow(plain, target), repeat)
    decode, decoded = timed(lambda: dictionary_arrow(encoded, STATE_WIDTH), repeat)
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
        expected = parse_pairs(line)
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
        assert all(one.unix and one.hash for one in FixEvents.from_text(line)), (
            f"{label} produced an event with no time or no version identity"
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

    instrument = Instrument(symbol="BTC-USD", securityexchange="XCME")

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
                        px=100.0 + generate.randrange(-20, 20) * 0.01,
                        qty=1.0,
                        state=State.FILLED,
                        execid=f"E{index}",
                    )
                )
            )
            continue
        quoted = 100.0 + side.sign * -1 * generate.randrange(1, 20) * 0.01
        built.append(
            ready(
                Order(
                    unix=unix,
                    side=side,
                    px=float("nan") if index % 20 == 0 else quoted,
                    qty=float(generate.randrange(1, 50)),
                    orderid=named,
                    state=State.CANCELLED if shape == 2 else State.NEW,
                    kind=MarketKind.LIMIT_ORDER if index % 20 == 0 else MarketKind.UNKNOWN,
                )
            )
        )
    return built


def shaped_stream(events: int, live_levels: int, orders_per_level: int) -> Iterator[object]:
    """A streaming steady-state order book with a controlled live shape."""
    from rekep.market import Instrument, Order, Side, State

    if events < 1 or live_levels < 1 or orders_per_level < 1:
        raise ValueError("events, live_levels and orders_per_level must be positive")
    capacity = live_levels * orders_per_level
    instrument = Instrument(symbol="MATRIX", securityexchange="XCME")
    for index in range(events):
        slot = index % capacity
        cycle = index // capacity
        level = slot // orders_per_level
        unix = UNIX + index * 1_000_000
        event = Order(
            unix=unix,
            creaunix=unix,
            side=Side.BID,
            px=100.0 - level * 0.01,
            qty=1.0 + cycle % 2,
            orderid=f"M{slot}",
            state=State.NEW,
        )
        built = event.attach_instrument(instrument).with_previous(None)
        if built is None:
            raise AssertionError("a first normalized order cannot be unchanged")
        yield built


def fold_shape(events: int, live_levels: int, orders_per_level: int) -> tuple[Counter[str], object]:
    """Stream one replay shape and retain only output counts and final state."""
    from rekep.market import BookIterator

    folding = BookIterator.from_events(
        shaped_stream(events, live_levels, orders_per_level),
        snapshot_every=0,
        max_order_age_ns=None,
    )
    counts: Counter[str] = Counter()
    for book in folding.books:
        counts["books"] += 1
        counts["deltas"] += len(book.deltas)
        counts["executions"] += len(book.executions)
        counts["materialized_levels"] += len(book.bidlevels) + len(book.asklevels)
        counts["snapshots"] += book.snapunix is not None
    state = next(iter(folding.folding.values()))
    counts["live_orders"] = len(state.bid.orders) + len(state.ask.orders)
    counts["live_levels"] = state.bid.depth + state.ask.depth
    return counts, folding


def bench_standing(rows: int, repeat: int) -> None:
    """Lifecycle and namespaced-code lookup at shallow and deep book sizes."""
    from rekep.market import Execution, Order, Side, State

    probes = max(rows, 1_000)
    print(f"\nStanding lookup -- {probes:,} probes")
    for live in (100, min(max(rows, 1_000), 10_000)):
        side = _Side(Side.BID)
        for index in range(live):
            side.apply(
                Order(
                    unix=UNIX + index,
                    creaunix=UNIX + index,
                    xhash=index + 1,
                    hash=index + 1,
                    code=f"O{index}",
                    altids={
                        "secondary_order_id": f"V{index}",
                        "secondary_cl_ord_id": f"C{index}",
                    },
                    orderid=f"O{index}",
                    clordid=f"CL{index}",
                    side=Side.BID,
                    px=100.0,
                    qty=1.0,
                    state=State.NEW,
                )
            )
        target = side.orders[live // 2 + 1]
        cases = (
            ("exact xhash", Order(xhash=target.xhash)),
            ("linked xhash", Execution(linkedhashes=[target.xhash])),
            ("venue code", Order(altids={"secondary_order_id": f"V{live // 2}"})),
            ("client code", Order(altids={"secondary_cl_ord_id": f"C{live // 2}"})),
            ("code miss", Order(altids={"secondary_order_id": "missing"})),
        )

        def linear(event: Order | Execution, side: _Side = side) -> Order | None:
            identities = ([event.xhash] if event.is_order() and event.xhash else []) + [
                identity for identity in event.linkedhashes
            ]
            if identities:
                return next(
                    (order for order in side.orders.values() if order.xhash in identities), None
                )
            aliases = set(Order.lookup_altids_of(event))
            return next(
                (
                    order
                    for order in side.orders.values()
                    if aliases.intersection(Order.lookup_altids_of(order))
                ),
                None,
            )

        for label, probe in cases:
            expected = linear(probe)
            assert side.standing(probe) is expected
            seconds, found = timed(
                lambda probe=probe, expected=expected, side=side: sum(
                    side.standing(probe) is expected for _ in range(probes)
                ),
                repeat,
            )
            assert found == probes
            report(f"{label}, {live:,} live orders", seconds, probes)


def bench_operation_counts(rows: int) -> None:
    """One representative replay with allocations and hot operations counted."""
    import rekep.market.event as event_module

    events = min(max(rows, 2_000), 10_000)
    event_module._life_hash.cache_clear()
    tracemalloc.start()
    try:
        with counted_operations() as operations:
            output, folding = fold_shape(events, 100, 10)
            state = next(iter(folding.folding.values()))
            snapshot_unix = UNIX + events * 1_000_000 + 1
            if not folding._snapshot_book(state, snapshot_unix):
                raise AssertionError("the representative live book did not snapshot")
            snapshot = folding._books.pop()
            output["snapshots"] += 1
            output["snapshot_alive"] += len(snapshot.bidalive) + len(snapshot.askalive)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    operations.update(output)
    operations["peak_kib"] = round(peak / 1024)
    print(f"\nFold operations -- {events:,} events, 100 levels, 10 orders/level")
    for name in (
        "books",
        "snapshots",
        "snapshot_alive",
        "deltas",
        "executions",
        "live_orders",
        "live_levels",
        "materialized_levels",
        "hash_of",
        "hash_bytes",
        "frame",
        "life_hash",
        "standing_probes",
        "expiry_scans",
        "level_joins",
        "level_leaves",
        "full_order_scan_calls",
        "full_orders_scanned",
        "book_objects",
        "level_objects",
        "order_copies",
        "book_copies",
        "dataclasses_replace",
        "sort_calls",
        "heap_heappush",
        "heap_heappop",
        "bisect_bisect_left",
        "bisect_bisect_right",
        "bisect_insort_left",
        "bisect_insort_right",
        "snapshot_materializations",
        "peak_kib",
    ):
        print(f"  {name:<44} {operations[name]:>12,}")


def bench_snapshot(rows: int, repeat: int) -> None:
    """Materialize and restore a full recovery row outside the delta hot path."""
    from rekep.market import BookIterator

    live = min(max(rows, 1_000), 10_000)
    live_levels = min(live, 100)
    orders_per_level = (live + live_levels - 1) // live_levels
    _, folding = fold_shape(live, live_levels, orders_per_level)
    state = next(iter(folding.folding.values()))
    boundary = [UNIX + live * 1_000_000]

    def materialize() -> Book:
        boundary[0] += 1
        if not folding._snapshot_book(state, boundary[0]):
            raise AssertionError("the live book did not produce a recovery snapshot")
        return folding._books.popleft()

    snapshot_seconds, snapshot = timed(materialize, repeat)
    assert not snapshot.deltas and not snapshot.executions
    assert len(snapshot.bidalive) + len(snapshot.askalive) == live

    def recover() -> BookIterator:
        return BookIterator(snapshots=(snapshot,), snapshot_every=0, max_order_age_ns=None)

    recovery_seconds, recovered = timed(recover, repeat)
    restored = next(iter(recovered.folding.values()))
    assert len(restored.bid.orders) + len(restored.ask.orders) == live
    print(f"\nSnapshot/recovery -- {live:,} live orders, {live_levels:,} levels")
    report("snapshot materialization", snapshot_seconds, live)
    report("snapshot recovery", recovery_seconds, live)


def bench_replay_matrix(rows: int, repeat: int, *, quick: bool) -> None:
    """Replay throughput across selected event counts and live-book shapes."""
    if quick:
        configurations = ((1_000, 10, 1), (2_000, 100, 1), (2_000, 100, 10))
        matrix_repeat = 1
    else:
        events = max(rows, 10_000)
        configurations = ((events, 10, 100), (events, 100, 10), (events, 1_000, 1))
        matrix_repeat = min(repeat, 2)

    print("\nReplay matrix -- events; live levels; orders/level")
    for events, levels, per_level in configurations:
        seconds, result = timed(
            lambda events=events, levels=levels, per_level=per_level: fold_shape(
                events, levels, per_level
            ),
            matrix_repeat,
        )
        counts, _ = result
        expected_orders = min(events, levels * per_level)
        expected_levels = min(levels, (expected_orders + per_level - 1) // per_level)
        assert counts["live_orders"] == expected_orders
        assert counts["live_levels"] == expected_levels
        print(
            f"  {events:>9,}; {levels:>6,}; {per_level:>3,}  "
            f"{events / seconds:>11,.0f} events/s  "
            f"{counts['books'] / seconds:>11,.0f} books/s  "
            f"{seconds / events * 1e9:>9,.0f} ns/event"
        )


def bench_lifecycle(rows: int, repeat: int) -> None:
    """Cached state comparison and indexed explicit-expiry lookup."""
    from rekep.market import Order, Side, State

    previous = next(one for one in stream(4) if isinstance(one, Order) and one.px == one.px)
    current = copy.copy(previous)
    pictured = copy.copy(previous)
    pictured.snapunix = previous.unix
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
                creaunix=UNIX + clock,
                xhash=index + 1,
                hash=index + 1,
                code=f"O{index}",
                altids={"symbol": "BTC-USD"},
                orderid=f"O{index}",
                side=Side.BID,
                px=100.0,
                qty=1.0,
                state=State.NEW,
                expunix=expiry if index % 100 == 0 else None,
            )
        )
    indexed = len(side._deadlines)
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
            key=lambda order: (order.creaunix or order.unix, order.xhash),
            reverse=True,
        )[:1]

    sorted_seconds, sorted_expected = timed(sorted_tie, repeat)
    max_seconds, max_actual = timed(lambda: side._evictions(1), repeat)
    assert [one.xhash for one in max_actual] == [one.xhash for one in sorted_expected]
    print(f"\nCapacity tie -- {live:,} orders at one price, one eviction")
    report("sorted whole price level", sorted_seconds, live)
    report("max within price level", max_seconds, live, against=sorted_seconds)

    started = time.perf_counter()
    due = side.expire(expiry)
    due_seconds = time.perf_counter() - started
    assert len(due) == indexed and len(side.orders) == live - indexed
    print(f"\nExplicit expiry due set -- {indexed:,} of {live:,} live orders")
    report("expire, small due set", due_seconds, indexed)

    capped = _Side(Side.BID)
    for index in range(live):
        capped.apply(
            Order(
                unix=UNIX + index,
                creaunix=UNIX + index,
                xhash=index + 1,
                hash=index + 1,
                code=f"O{index}",
                altids={"symbol": "BTC-USD"},
                orderid=f"O{index}",
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
            key=lambda order: (-(order.px or 0.0), order.creaunix or order.unix, order.xhash),
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
    parsed = [FixMsg.from_text(line) for line in lines]

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


def log_stream(rows: int) -> list[object]:
    """One instrument's feed as the parsed log rows `Book.from_fixmsgs` reads.

    Built as `FixMsg` rows carrying wire tags rather than through a text file, so
    what is measured is the two halves of the generator -- translating a parsed
    row back into market events, and folding those into books -- and not the
    tokenizer in front of them, which `bench_fix_parser` prices on its own.
    """
    from rekep import FixMsg, Message
    from rekep.fix import FixCodec, FixRegistry

    base = 1_786_665_901_000_000_000
    # The order and the fill, and not the market-data shape beside them: its
    # entries carry their own `MDEntryTime <273>`, which is what orders them
    # and not the message clock this walks forward.
    shapes = [
        parse_pairs(line) for label, line in FEED.items() if not label.startswith("MarketData")
    ]
    built: list[Message] = []
    for index in range(rows):
        stamp = _fix_stamp(base + index * 1_000_000)
        renamed = {"52": stamp, "60": stamp, "11": f"CL-{index}", "17": f"EX-{index}"}
        built.append(
            Message(
                unix=base + index * 1_000_000,
                message="|".join(
                    f"{tag}={renamed.get(tag, value)}" for tag, value in shapes[index % len(shapes)]
                ),
            )
        )
    if not built:
        return []
    raw = next(iter(Message.into_arrow_reader(built, batch_row_size=rows)))
    parsed = FixMsg.from_message_batch(raw, FixCodec(registry=FixRegistry.from_builtin()))
    return list(FixMsg.from_arrow_reader([parsed]))


def _fix_stamp(unix: int) -> str:
    """One nanosecond instant as the `UTCTimestamp` a message spells."""
    moment = datetime.datetime.fromtimestamp(unix / 1e9, tz=datetime.UTC)
    return moment.strftime("%Y%m%d-%H:%M:%S.") + f"{moment.microsecond // 1000:03d}"


def _pipeline_message_batches(
    rows: int,
    batch_row_size: int = 65_536,
    *,
    alternating_technical: bool = False,
) -> tuple[pyarrow.RecordBatch, ...]:
    """Reusable columnar Message input matching the parse-fix task boundary."""
    from rekep import Message
    from rekep.enums import EventType
    from rekep.market.event import unix_partition_arrow

    base = 1_786_665_901_000_000_000
    shapes = [
        (EventType.ORDER, parse_pairs(FEED["NewOrderSingle <D>"])),
        (EventType.EXECUTION, parse_pairs(FEED["ExecutionReport <8>, filled"])),
    ]
    schema = Message.into_field().into_arrow_schema()
    batches = []
    for start in range(0, rows, batch_row_size):
        stop = min(start + batch_row_size, rows)
        indices = range(start, stop)
        unix = pyarrow.array([base + index * 1_000_000 for index in indices], pyarrow.int64())
        kinds, protocols, msg_types, entries = [], [], [], []
        for index in indices:
            if alternating_technical and index % 2:
                kinds.append(int(EventType.MISC))
                protocols.append("OTHER")
                msg_types.append(None)
                entries.append(None)
                continue
            eventtype, pairs = shapes[index % len(shapes)]
            stamp = _fix_stamp(base + index * 1_000_000)
            renamed = {
                "52": stamp,
                "60": stamp,
                "11": f"CL-{index // 2}",
                "17": f"EX-{index}",
            }
            kinds.append(int(eventtype))
            protocols.append("FIX")
            msg_types.append(next(value for tag, value in pairs if tag == "35"))
            entries.append(
                [
                    {
                        "tag": int(tag),
                        "key": str(tag),
                        "value": renamed.get(tag, value),
                        "namespace": None,
                        "comp": None,
                    }
                    for tag, value in pairs
                    if tag != "35"
                ]
            )
        count = stop - start
        columns: dict[str, pyarrow.Array] = {
            "unix": unix,
            "unixpartition": unix_partition_arrow(unix),
            "eventtype": pyarrow.array(kinds, EventType.into_arrow_type().index_type),
            "creaunix": unix,
            "recunix": unix,
            "sourceurl": pyarrow.repeat(pyarrow.scalar("pipeline-benchmark.log"), count),
            "sourcerownum": pyarrow.array(range(start + 1, stop + 1), pyarrow.int64()),
            "message": pyarrow.repeat(pyarrow.scalar(""), count),
            "protocolcode": pyarrow.array(protocols),
            "msgtype": pyarrow.array(msg_types),
            "entries": pyarrow.array(entries, schema.field("entries").type),
        }
        arrays = []
        for field in schema:
            column = columns.get(field.name)
            if column is None:
                if field.nullable:
                    column = pyarrow.nulls(count, field.type)
                elif pyarrow.types.is_list(field.type) or pyarrow.types.is_map(field.type):
                    column = pyarrow.array([[]] * count, field.type)
                elif pyarrow.types.is_string(field.type):
                    column = pyarrow.repeat(pyarrow.scalar(""), count)
                elif pyarrow.types.is_boolean(field.type):
                    column = pyarrow.repeat(pyarrow.scalar(False), count)
                elif pyarrow.types.is_fixed_size_binary(field.type):
                    # An identity column is its width in bytes, and zero is
                    # that many of them -- `scalar(0, ...)` is not a number
                    # here, it is a buffer Arrow refuses to guess the size of.
                    column = pyarrow.repeat(
                        pyarrow.scalar(b"\x00" * field.type.byte_width, field.type), count
                    )
                else:
                    column = pyarrow.repeat(pyarrow.scalar(0, field.type), count)
            arrays.append(column)
        batches.append(pyarrow.RecordBatch.from_arrays(arrays, schema=schema))
    return tuple(batches)


def _pipeline_batches(
    messages: tuple[pyarrow.RecordBatch, ...], codec: object
) -> Iterator[pyarrow.RecordBatch]:
    """Parse-fix batches with the raw payload projected out before conversion."""
    for batch in messages:
        yield FixMsg.from_message_batch(batch.drop_columns(["message"]), codec)


def bench_pipeline(rows: int) -> None:
    """Measure homogeneous and alternating columnar books-off stages."""
    from rekep.fix import FixCodec, FixRegistry

    registry = FixRegistry(cache_dir=FIX_DICTIONARY, offline=True)
    codec = FixCodec(registry=registry)
    messages = _pipeline_message_batches(rows)
    mixed_messages = _pipeline_message_batches(rows, alternating_technical=True)
    if messages:
        # Registry declarations and lookup arrays are process setup, not work
        # repeated for each production batch.
        tuple(_pipeline_batches((messages[0].slice(0, 2),), codec))
    print(f"\nDirect market pipeline -- {rows:,} parsed messages")

    def measure(label: str, source: tuple[pyarrow.RecordBatch, ...]) -> None:
        with peak_rss() as sampled:
            started = time.perf_counter()
            parsed = tuple(_pipeline_batches(source, codec))
            parse_seconds = time.perf_counter() - started
        parse_peak = sampled()
        if parsed:
            list(
                FixMsg.into_market_arrow_batches(
                    parsed[0].slice(0, 2), batch_row_size=2, registry=registry
                )
            )

        def direct() -> int:
            return sum(
                batch.num_rows
                for _, batch in FixMsg.into_market_arrow_batches(
                    parsed, batch_row_size=65_536, registry=registry
                )
            )

        with peak_rss() as sampled:
            started = time.perf_counter()
            produced = direct()
            direct_seconds = time.perf_counter() - started
        direct_peak = sampled()
        assert produced, "direct translation produced no rows"
        assert not direct_peak or direct_peak < 4 * 2**30, (
            f"direct translation peaked at {direct_peak / 2**30:.2f} GiB RSS"
        )
        parse_memory = "n/a" if not parse_peak else f"{parse_peak / 2**20:,.1f} MiB"
        direct_memory = "n/a" if not direct_peak else f"{direct_peak / 2**20:,.1f} MiB"
        print(f"  {label}")
        print(
            f"    {'Message -> FixMsg':<42} {rows / parse_seconds:>12,.0f} records/s  "
            f"{parse_memory} peak RSS"
        )
        print(
            f"    {'books off':<42} {rows / direct_seconds:>12,.0f} records/s  "
            f"{produced:,} output rows  {direct_memory} peak RSS  "
            f"{rows / (parse_seconds + direct_seconds):,.0f}/s with parse"
        )

    measure("homogeneous standard FIX", messages)
    measure("alternating FIX / technical", mixed_messages)


def bench_from_logs(rows: int, repeat: int) -> None:
    """The whole generator: parsed log rows in, books out."""
    from rekep.market import BookIterator

    print(f"\nBook.from_fixmsgs -- {rows:,} parsed rows, one instrument")
    logs = log_stream(rows)

    def translate() -> int:
        return sum(1 for log in logs for _ in log.into_market_events())

    def fold() -> int:
        return sum(1 for _ in BookIterator(logs=logs, snapshot_every=0).books)

    events = translate()
    assert events, "the log stream translated to nothing at all"
    read, _ = timed(translate, repeat)
    report("parsed row -> market events", read, rows)
    whole, produced = timed(fold, repeat)
    assert produced, "the fold produced no books at all"
    report("parsed row -> books", whole, rows)
    print(f"  {'logs/s':<44} {rows / whole:>12,.0f}   ({produced:,} books, {events:,} events)")
    print(f"  {'of it spent translating':<44} {read / whole * 100:>11.0f}%")


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
    rejected = sum(one.state is State.INTERNAL_REJECTED for book in books for one in book.deltas)
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
        event.state is State.INTERNAL_EXPIRED for book in limited for event in book.deltas
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
        batch = pyarrow.RecordBatch.from_pylist([book.into_row() for book in sample], schema=schema)
        return pyarrow.Table.from_batches([batch], schema=schema)

    def column_projection() -> pyarrow.Table:
        batch = Book.into_arrow_batch(sample)
        return pyarrow.Table.from_batches([batch], schema=schema)

    generic, expected = timed(document_projection, repeat)
    columnar, built = timed(column_projection, repeat)
    assert expected.num_rows == len(sample)
    assert identical(built, expected), "the two projections must be the same table"
    print(f"\nBook to Arrow -- {len(sample):,} rows")
    report("row by row, through a document", generic, len(sample))
    report("member by member, off the objects", columnar, len(sample), against=generic)


def main() -> None:
    parsed = parser(__doc__, rows=20_000).parse_args()
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
    bench_standing(rows, repeat)
    # Tiny Arrow arrays benchmark call overhead instead of parser throughput.
    bench_fix_parser(max(rows // 20, 10_000), repeat)
    bench_fix(rows // 20, repeat)
    bench_ceiling(rows // 20, repeat)
    bench_fold(rows // 10, repeat)
    bench_from_logs(rows // 20, repeat)
    # Scalar market translation is deliberately included, so keep this sweep
    # bounded while `--rows` still scales it for a dedicated benchmark run.
    bench_pipeline(max(rows // 4, 500))
    bench_operation_counts(rows)
    bench_snapshot(rows, repeat)
    bench_replay_matrix(rows, repeat, quick=parsed.quick)


if __name__ == "__main__":
    main()
