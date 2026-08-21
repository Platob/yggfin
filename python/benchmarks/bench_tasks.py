"""Benchmark the job a scheduler actually runs: parse a capture, fan it out, append.

Run from `python/`::

    uv run python benchmarks/bench_tasks.py            # full sweep
    uv run python benchmarks/bench_tasks.py --quick    # a tenth of the capture

Four questions, answered on a synthetic capture whose mix of event kinds
matches the tests' fixture:

1. **What does the first run cost?** Everything lands, so this is the parse
   plus the write, and it is the number every other one is read against.
2. **What does a replay cost?** The point of appending with `merge_by` is that
   re-running over a capture that did not grow writes nothing -- so what is
   left is the read, and how much cheaper that is than the write is the whole
   argument for it.
3. **What does the growth cost?** The case a scheduled job is in every time but
   the first: a capture that gained a day, where only the day should be paid
   for.
4. **What does the fan-out cost?** One pass writing N tables, against the same
   rows going to one -- because a job that split its output by reparsing the
   capture once per kind is the alternative, and it is N passes.

Every case asserts what landed before it is timed, and the whole capture is
parsed once into memory first where the point is to measure the write rather
than the parser. Run it twice before quoting a number.
"""

from __future__ import annotations

import argparse
import pathlib
import shutil
import sys
import tempfile
import time
from collections.abc import Callable

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep.market import EventType  # noqa: E402
from rekep.tasks import ParseLogs  # noqa: E402

#: One line per kind, so a capture is an even mix of the things a log holds.
KINDS = {
    EventType.EXECUTION: "8=FIX.4.4\x0135=8\x0117=e{i}\x01",
    EventType.ORDER: "sent NewOrderSingle AAPL {i}@10.0",
    EventType.BOOK_SIDE: "8=FIX.4.4\x0135=X\x01268={i}\x01",
    EventType.BOOK: "8=FIX.4.4\x0135=W\x01268={i}\x01",
    EventType.QUOTE: "8=FIX.4.4\x0135=S\x01117=q{i}\x01",
    EventType.UNKNOWN: "heartbeat {i}",
}


def write_capture(folder: pathlib.Path, rows: int, name: str, day: str) -> int:
    """A log of about `rows` lines, spread over a day's hours and every kind."""
    folder.mkdir(parents=True, exist_ok=True)
    templates = list(KINDS.values())
    lines = []
    for index in range(rows):
        hour, minute, second = (index // 3600) % 24, (index // 60) % 60, index % 60
        template = templates[index % len(templates)]
        lines.append(
            f"{day} {hour:02d}:{minute:02d}:{second:02d}.167_520 [t-1] [Bridge] "
            + template.format(i=index)
        )
    (folder / name).write_text("\n".join(lines) + "\n")
    return len(lines)


def timed(work: Callable[[], object]) -> tuple[float, object]:
    """One run, wall clock. Not best-of: a write is not idempotent in cost."""
    start = time.perf_counter()
    result = work()
    return time.perf_counter() - start, result


def report(label: str, seconds: float, rows: int, against: float | None = None) -> None:
    rate = rows / seconds if seconds else float("inf")
    ratio = f"  {against / seconds:6.1f}x" if against and seconds else ""
    print(f"  {label:<44} {seconds * 1000:9.1f} ms  {rate:12,.0f} rows/s{ratio}")


def catalog(work: pathlib.Path, name: str) -> dict[str, str]:
    warehouse = work / f"warehouse-{name}"
    warehouse.mkdir(parents=True, exist_ok=True)
    return {
        "type": "sql",
        "uri": f"sqlite:///{(work / f'{name}.db').as_posix()}",
        "warehouse": warehouse.as_uri(),
    }


def task_for(source: pathlib.Path, work: pathlib.Path, name: str, **declared: object) -> ParseLogs:
    return ParseLogs(
        source=str(source),
        catalog="bench",
        namespace="logs",
        properties=catalog(work, name),
        **declared,
    )


# -- 1, 2 and 3: the write, the replay, and the growth -----------------------


def bench_append(source: pathlib.Path, work: pathlib.Path, rows: int) -> None:
    print(f"\nParse and append -- {rows:,} rows, {len(KINDS)} kinds")
    task = task_for(source, work, "merge")

    first, landed = timed(task.run)
    assert landed.landed == rows, f"the first run landed {landed.landed}, not {rows}"
    report("first run: parse, fan out, write", first, rows)

    replay, again = timed(task.run)
    assert again.landed == 0 and again.skipped == rows, "a replay wrote something"
    report("replay: everything already stored", replay, rows, against=first)

    grown = write_capture(source, rows, "grown.log", "2026-08-15")
    growth, third = timed(task.run)
    assert third.landed == grown, f"the growth landed {third.landed}, not {grown}"
    report("a capture that gained a day", growth, rows + grown, against=first)

    plain = task_for(source, work, "plain", merge_by=False)
    without, _ = timed(plain.run)
    report("merge_by=False, over the same rows", without, rows + grown)


# -- 4: the fan-out ----------------------------------------------------------


def bench_fanout(source: pathlib.Path, work: pathlib.Path, rows: int) -> None:
    print(f"\nFanning out -- {rows:,} rows")
    spread = task_for(source, work, "spread")
    many, report_many = timed(spread.run)
    assert len(report_many.written) == len(KINDS), "not every kind landed"
    report(f"one pass -> {len(KINDS)} tables", many, rows)

    #: The same rows into one table, which is what the fan-out is read against.
    one = task_for(source, work, "one", table="all_logs")
    single, report_one = timed(one.run)
    assert len(report_one.written) == 1
    report("one pass -> 1 table", single, rows, against=many)

    # Splitting is usually the *cheaper* of the two, which is not obvious and
    # is worth saying: an append's merge anti-joins each chunk against what its
    # own target already holds, so N targets each hold a fraction of the rows
    # and each anti-join is against a fraction of the keys. The fan-out buys a
    # column-partitioned read and shrinks its own merge while it is at it.
    (name, quick), (_, slow) = sorted(
        (("split", many), ("single", single)), key=lambda pair: pair[1]
    )
    print(
        f"  {'so ' + name + ' is cheaper by':<44} "
        f"{(slow - quick) * 1000:9.1f} ms  ({slow / quick:.2f}x) over {len(KINDS)} kinds"
    )


# -- the commit size ---------------------------------------------------------


def bench_commits(source: pathlib.Path, work: pathlib.Path, rows: int) -> None:
    """What the memory bound costs, and that it does not change the answer."""
    print(f"\nCommit size -- {rows:,} rows")
    landed = None
    for size in (10_000, 100_000, 1_000_000):
        task = task_for(source, work, f"commit{size}", commit_row_size=size)
        seconds, done = timed(task.run)
        assert landed is None or done.landed == landed, "a commit size changed the result"
        landed = done.landed
        commits = -(-rows // size)
        report(f"commit_row_size={size:,} (~{commits} commits)", seconds, rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--rows", type=int, default=None)
    parsed = parser.parse_args()
    rows = parsed.rows or (20_000 if parsed.quick else 200_000)

    work = pathlib.Path(tempfile.mkdtemp(prefix="rekep-bench-tasks-"))
    try:
        source = work / "capture"
        written = write_capture(source, rows, "a.log", "2026-08-14")
        bench_append(source, work, written)

        fresh = work / "fanout"
        write_capture(fresh, rows, "a.log", "2026-08-14")
        bench_fanout(fresh, work, written)
        bench_commits(fresh, work, written)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
