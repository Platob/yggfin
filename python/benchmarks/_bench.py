"""What every benchmark here does the same way: time a call, print a line.

Five of the six had their own "fastest of N runs" under four names, and the
number a benchmark prints is only comparable to the number beside it if both
were measured the same way -- so there is one timer, and it warms up.
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from typing import Any

#: One column width for every label printed here, so a run reads as a table.
LABEL = 44


def best_of(call: Callable[[], Any], repeat: int = 5, *, warm: bool = True) -> float:
    """Seconds of the fastest of `repeat` calls, after one untimed warm-up.

    Fastest and not mean: the slow runs measure whatever else the machine was
    doing. The warm-up is charged nothing, because a first call pays for
    imports, caches and page faults the ones after it do not.
    """
    if warm:
        call()
    fastest = float("inf")
    for _ in range(repeat):
        started = time.perf_counter()
        call()
        fastest = min(fastest, time.perf_counter() - started)
    return fastest


def timed(call: Callable[[], Any]) -> tuple[float, Any]:
    """One untimed-warm-up-free call: `(seconds, whatever it returned)`.

    For work that cannot be repeated -- a write that lands rows, a compaction
    that rewrites files -- where the result is the point.
    """
    started = time.perf_counter()
    result = call()
    return time.perf_counter() - started, result


def report(
    label: str,
    seconds: float,
    rows: int | None = None,
    against: float | None = None,
) -> None:
    """One measurement, one line: milliseconds, and per-row when rows are known."""
    per_row = f"  {seconds / rows * 1e9:7.1f} ns/row" if rows else ""
    ratio = f"  {against / seconds:6.1f}x" if against else ""
    print(f"  {label:<{LABEL}} {seconds * 1000:8.2f} ms{per_row}{ratio}")


def parser(
    description: str,
    *,
    rows: int | None = None,
    repeat: int = 5,
) -> argparse.ArgumentParser:
    """`--rows`, `--repeat` and `--quick`, which every benchmark here takes.

    `rows=None` for a benchmark whose size is not a row count; it then takes
    no `--rows` at all rather than one nothing reads.
    """
    parsed = argparse.ArgumentParser(description=description)
    if rows is not None:
        parsed.add_argument("--rows", type=int, default=rows)
    parsed.add_argument("--repeat", type=int, default=repeat)
    parsed.add_argument("--quick", action="store_true")
    return parsed


def identical(left: Any, right: Any) -> bool:
    """Whether two batches or tables hold the same bytes, NaN included.

    `RecordBatch.equals` compares values, and a NaN price is not equal to
    itself -- so two projections of the same rows read as different where a
    feed sent one. Serialising both settles it exactly: same schema, same
    buffers, same bytes.
    """
    import pyarrow
    import pyarrow.ipc

    def written(batch: Any) -> bytes:
        sink = pyarrow.BufferOutputStream()
        with pyarrow.ipc.new_stream(sink, batch.schema) as writer:
            writer.write(batch)
        return sink.getvalue().to_pybytes()

    return written(left) == written(right)
