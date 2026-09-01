"""What every benchmark here does the same way: time a call, print a line.

Five of the six had their own "fastest of N runs" under four names, and the
number a benchmark prints is only comparable to the number beside it if both
were measured the same way -- so there is one timer, and it warms up.

`Accounted` and the two blocks under it are the same argument one level down:
a kernel-bound path is a table of stages and kernels, and two benchmarks
reporting that table have to build it the same way to be read side by side.
"""

from __future__ import annotations

import argparse
import collections
import contextlib
import os
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from typing import Any

#: One column width for every label printed here, so a run reads as a table.
LABEL = 44

#: How far back a kernel's caller can be before naming it is not worth the
#: walk: a package frame is one or two frames up in every case measured here.
_SITE_DEPTH = 6

#: `pyarrow.compute`'s own dispatcher. Its callers are the kernels the report
#: already names, so accounting it apart only charges them their wrapper.
_PLUMBING = frozenset({"call_function"})


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


class Accounted:
    """Wall time attributed to named calls, inclusive and exclusive of nesting.

    Exclusive is what a stage costs once the accounted stages inside it are
    charged to themselves, so the column adds up to the run rather than
    counting the same milliseconds twice.
    """

    def __init__(self) -> None:
        self.inclusive: dict[str, float] = collections.defaultdict(float)
        self.exclusive: dict[str, float] = collections.defaultdict(float)
        self.calls: dict[str, int] = collections.Counter()
        self.stack: list[float] = []

    @property
    def depth(self) -> int:
        return len(self.stack)

    def wrap(self, label: str, call: Callable[..., Any]) -> Callable[..., Any]:
        """`call`, charging its own time to `label` and its nested time to itself."""

        def accounted(*args: Any, **kwargs: Any) -> Any:
            self.stack.append(0.0)
            started = time.perf_counter()
            try:
                return call(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - started
                nested = self.stack.pop()
                if self.stack:
                    self.stack[-1] += elapsed
                self.inclusive[label] += elapsed
                self.exclusive[label] += elapsed - nested
                self.calls[label] += 1

        return accounted

    def report(self, header: str, wall: float, width: int = 30, top: int | None = None) -> None:
        """One line per label, widest own time first, against the measured `wall`."""
        order = sorted(self.exclusive, key=lambda label: -self.exclusive[label])
        print(f"\n  {header:<{width}} {'own ms':>8} {'total ms':>9} {'calls':>7} {'own':>6}")
        for label in order[:top]:
            own = self.exclusive[label]
            print(
                f"  {label:<{width}} {own * 1000:>8.1f} {self.inclusive[label] * 1000:>9.1f} "
                f"{self.calls[label]:>7} {own / wall:>5.1%}"
            )
        counted = sum(self.exclusive.values())
        print(
            f"  {'accounted':<{width}} {counted * 1000:>8.1f} "
            f"{'':>9} {'':>7} {counted / wall:>5.1%}"
        )


def _owner(path: str) -> Any:
    """The module or class a stage is wrapped on, from `module[:Class]`."""
    module, _, name = path.partition(":")
    __import__(module)
    owner = sys.modules[module]
    return getattr(owner, name) if name else owner


@contextlib.contextmanager
def accounted_stages(stages: Sequence[tuple[str, str, str]]) -> Iterator[Accounted]:
    """Time each `(label, module[:Class], attribute)` for the length of the block.

    Wrapped on the owner the call goes *through* -- a class attribute, or the
    module the consumer looks the name up in -- because a helper imported by
    name into its caller is a different object from the one its own module
    holds, and only one of them is what runs.
    """
    account = Accounted()
    restore: list[Callable[[], None]] = []
    for label, path, name in stages:
        owner = _owner(path)
        if not hasattr(owner, name):
            continue
        declared = owner.__dict__.get(name, getattr(owner, name))
        if isinstance(declared, classmethod | staticmethod):
            kind = type(declared)
            setattr(owner, name, kind(account.wrap(label, declared.__func__)))
        else:
            setattr(owner, name, account.wrap(label, getattr(owner, name)))
        restore.append(
            lambda owner=owner, name=name, declared=declared: setattr(owner, name, declared)
        )
    try:
        yield account
    finally:
        for undo in restore:
            undo()


def _site() -> str:
    """The package frame that asked for a kernel, as `file:line`.

    Frames rather than `traceback.extract_stack`, which reads source lines off
    disk per call and costs more than several of the kernels it is naming.
    """
    marker = f"rekep{os.sep}"
    frame: Any = sys._getframe(2)
    for _ in range(_SITE_DEPTH):
        if frame is None:
            break
        name = frame.f_code.co_filename
        if marker in name:
            return f"{name.split(marker)[-1]}:{frame.f_lineno}"
        frame = frame.f_back
    return "?"


@contextlib.contextmanager
def accounted_kernels() -> Iterator[tuple[Accounted, Accounted]]:
    """Time every `pyarrow.compute` call in the block, by kernel and by call site.

    Wrapping costs about a fifth of a kernel-bound run, spread over the calls,
    so read the shares against the block's own elapsed time and not against a
    clean one measured beside it.
    """
    import pyarrow.compute

    compute = pyarrow.compute
    by_kernel, by_site = Accounted(), Accounted()
    original = {
        name: value
        for name in dir(compute)
        if not name.startswith("_")
        and name not in _PLUMBING
        and callable(value := getattr(compute, name))
        and not isinstance(value, type)
    }

    def measured(name: str, kernel: Callable[..., Any]) -> Callable[..., Any]:
        counted = by_kernel.wrap(name, kernel)

        def timed(*args: Any, **kwargs: Any) -> Any:
            if by_kernel.depth:
                return counted(*args, **kwargs)
            return by_site.wrap(f"{_site()} {name}", counted)(*args, **kwargs)

        return timed

    for name, value in original.items():
        setattr(compute, name, measured(name, value))
    try:
        yield by_kernel, by_site
    finally:
        for name, value in original.items():
            setattr(compute, name, value)
