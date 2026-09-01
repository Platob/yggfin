"""Benchmark the `Message` -> `FixMsg` boundary: the mix, the stages, the kernels.

`bench_text_file.py` prices this boundary as one number beside the text layer
it follows. This script is that number taken apart -- which protocol mix pays
what, which stage of `from_message_batch` the milliseconds are in, and which
Arrow kernel each call site spends them on -- because the boundary is entirely
kernel-bound and a proposal against it is otherwise a guess.
"""

from __future__ import annotations

import collections
import os
import pathlib
import random
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from typing import Any

import pyarrow
import pyarrow.compute

# `src` for the package under measurement, and this folder for `_bench`,
# so a benchmark imports the same whether it is run or imported.
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from _bench import best_of, parser  # noqa: E402

# The capture is `bench_text_file`'s, payload for payload: one fixture for the
# text layer and for the FIX layer that reads it, so the two scripts' rows/s
# are the same rows. Only the *mix* is this script's, because a mix is what
# the first sweep varies.
from bench_text_file import PLUGINS, _capture_line  # noqa: E402

from rekep.fix.registry import FixRegistry  # noqa: E402
from rekep.fix.transcribe import FixCodec  # noqa: E402
from rekep.text import FixMsg, TextFile  # noqa: E402

#: The mixes worth pricing apart. `mixed` is the captured share; the three
#: after it are the same payloads with one protocol left, because the boundary
#: takes a different path for each and an average hides which one moved.
MIXES: tuple[tuple[str, tuple[tuple[str, int], ...]], ...] = (
    ("mixed 60/25/15", (("OTHER", 60), ("FIX", 25), ("FIXML", 15))),
    ("wire FIX only", (("FIX", 1),)),
    ("bridge FIXML only", (("FIXML", 1),)),
    ("unparsed text only", (("OTHER", 1),)),
)

#: What `--only stages` accounts, in the order a batch reaches them, as
#: `(label, module[:Class], attribute)`. Every one of them is reached through
#: its owner -- a class attribute, or a module imported inside the call -- so
#: wrapping it there is what the transcription actually invokes. A helper the
#: consumers import *by name* cannot be, and is left to the call-site report.
STAGES: tuple[tuple[str, str, str], ...] = (
    ("classify protocol", "rekep.fix.rules:Rules", "into_arrow_protocol_array"),
    ("classify direction", "rekep.fix.rules:Rules", "into_arrow_direction_array"),
    ("flat FIX specialization", "rekep.text.fixmsg_arrow", "into_flat_fixmsg_batch"),
    ("registry reference path", "rekep.text.fixmsg:FixMsg", "_from_message_batch_reference"),
    ("entries -> pairs", "rekep.fix.transcribe:FixCodec", "into_pairs_from_entries"),
    ("complete the pairs", "rekep.fix.transcribe:FixCodec", "complete_pairs"),
    ("pairs -> entries", "rekep.fix.transcribe:FixCodec", "into_message_entries"),
    ("version the protocols", "rekep.fix.transcribe:FixCodec", "into_versioned_protocols"),
    ("resolve per version", "rekep.text.fixmsg:FixMsg", "_resolved_batch_columns"),
    (
        "lift component groups",
        "rekep.fix.components:ComponentGroup",
        "into_arrow_arrays_with_errors",
    ),
    ("identify", "rekep.text.fixmsg:FixMsg", "identified"),
    ("transcription errors", "rekep.text.fixmsg:FixMsg", "_with_transcription_errors"),
)


def staged(path: pathlib.Path, rows: int, shares: Sequence[tuple[str, int]], seed: int = 5) -> None:
    """Write a capture of `rows` lines dealt out in `shares`.

    A seeded shuffle of one slot per share point, repeated, so the mix is exact
    and two runs read the same file -- `bench_text_file.capture`'s rule, over a
    mix this script chooses.
    """
    generate = random.Random(seed)
    slots = [name for name, share in shares for _ in range(share)]
    generate.shuffle(slots)
    with path.open("wb") as out:
        for i in range(rows):
            second, micro = divmod(i, 1_000_000)
            out.write(
                b"2026-08-14 %02d:%02d:%02d.%03d_%03d [250-e7256476:9effef3e6a:%05d] [%s] "
                % (
                    second // 3600 % 24,
                    second // 60 % 60,
                    second % 60,
                    micro // 1000,
                    micro % 1000,
                    72500 + i % 8,
                    PLUGINS[i % len(PLUGINS)],
                )
            )
            out.write(_capture_line(slots[i % len(slots)], i, generate).encode() + b"\n")


def raw_batch(rows: int, shares: Sequence[tuple[str, int]]) -> pyarrow.RecordBatch:
    """One raw `Message` batch, read back through the text layer that writes it."""
    with tempfile.TemporaryDirectory() as tmp:
        path = pathlib.Path(tmp) / "capture.txt"
        staged(path, rows, shares)
        with TextFile.from_path(path) as log:
            return next(log.into_arrow_batches(batch_row_size=rows))


def checked(batch: pyarrow.RecordBatch, codec: FixCodec, sample: int = 8) -> pyarrow.RecordBatch:
    """The batch answer, asserted row by row against the one-row answer.

    A row's transcription is its own: the batch splits by protocol and by
    version and scatters the parts back, so a slice of the whole result has to
    equal the result of that slice. Over `sample` rows and not all of them
    because a one-row conversion costs nearly what the batch costs.
    """
    parsed = FixMsg.from_message_batch(batch, codec)
    assert parsed.num_rows == batch.num_rows
    for row in range(min(sample, batch.num_rows)):
        one = FixMsg.from_message_batch(batch.slice(row, 1), codec)
        assert one.equals(parsed.slice(row, 1)), f"row {row} reads differently alone"
    return parsed


def sweep_mix(rows: int, repeat: int) -> None:
    """What the boundary costs per protocol mix, per row and per entry."""
    compute = pyarrow.compute
    codec = FixCodec(registry=FixRegistry())
    print(f"\nMessage -> FixMsg, {rows:,} rows, best of {repeat}")
    print(f"  {'mix':<22} {'rows/s':>10} {'us/row':>9} {'MiB/s':>9} {'entries/s':>12}")
    for label, shares in MIXES:
        batch = raw_batch(rows, shares)
        parsed = checked(batch, codec, sample=24)
        payload = compute.sum(compute.binary_length(batch.column("body")), min_count=0).as_py()
        entries = compute.sum(
            compute.list_value_length(parsed.column("entries")), min_count=0
        ).as_py()
        seconds = best_of(lambda batch=batch: FixMsg.from_message_batch(batch, codec), repeat)
        print(
            f"  {label:<22} {rows / seconds:>10,.0f} {seconds / rows * 1e6:>9.1f} "
            f"{payload / seconds / 2**20:>9.1f} {(entries or 0) / seconds:>12,.0f}"
        )


def sweep_batch(rows: int, repeat: int) -> None:
    """The same rows at several batch sizes: what a batch costs before its rows.

    Every split the transcription makes -- by protocol, by version, by
    separator -- is a fixed number of kernel calls, and a kernel call on ten
    rows costs nearly what one on ten thousand does. A rate still climbing with
    the batch is that constant being amortized, and where it stops climbing is
    the batch size a reader should hand this.
    """
    codec = FixCodec(registry=FixRegistry())
    batch = raw_batch(rows, MIXES[0][1])
    checked(batch, codec)
    print(f"\nbatch size, mixed 60/25/15, best of {repeat}")
    print(f"  {'rows in the batch':>18} {'rows/s':>10} {'ms/batch':>10}")
    sizes = [size for size in (256, 2_048, 16_384, 131_072) if size < rows] + [rows]
    for size in sizes:
        sliced = batch.slice(0, size)
        seconds = best_of(lambda sliced=sliced: FixMsg.from_message_batch(sliced, codec), repeat)
        print(f"  {size:>18,} {size / seconds:>10,.0f} {seconds * 1000:>10.1f}")


class Accounted:
    """Wall time attributed to named calls, inclusive and exclusive of nesting.

    Exclusive is what a stage costs after the accounted stages inside it are
    charged to themselves, so the column adds up to the batch rather than
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


def _rebind(owner: Any, name: str, wrap: Callable[[Callable[..., Any]], Any]) -> Callable[[], None]:
    """Wrap `owner.name` in place, keeping whatever descriptor declared it."""
    declared = owner.__dict__.get(name, getattr(owner, name))
    if isinstance(declared, classmethod):
        setattr(owner, name, classmethod(wrap(declared.__func__)))
    elif isinstance(declared, staticmethod):
        setattr(owner, name, staticmethod(wrap(declared.__func__)))
    else:
        setattr(owner, name, wrap(getattr(owner, name)))
    return lambda: setattr(owner, name, declared)


def sweep_stages(rows: int, repeat: int) -> None:
    """Where one mixed batch's milliseconds are, stage by stage."""
    codec = FixCodec(registry=FixRegistry())
    batch = raw_batch(rows, MIXES[0][1])
    checked(batch, codec)
    clean = best_of(lambda: FixMsg.from_message_batch(batch, codec), repeat)
    account = Accounted()
    restore = [
        _rebind(_owner(path), name, lambda call, label=label: account.wrap(label, call))
        for label, path, name in STAGES
        if hasattr(_owner(path), name)
    ]
    try:
        started = time.perf_counter()
        FixMsg.from_message_batch(batch, codec)
        wall = time.perf_counter() - started
    finally:
        for undo in restore:
            undo()
    print(
        f"\nstages of one mixed batch, {rows:,} rows, "
        f"{clean * 1000:.1f} ms clean, {wall * 1000:.1f} ms instrumented"
    )
    account.report("stage", wall)


#: How far back a kernel's caller can be before it is not worth naming: a
#: package frame is one or two frames up in every case here, and walking
#: further is time charged to the measurement rather than to the parser.
_SITE_DEPTH = 6

#: `pyarrow.compute`'s own dispatcher. Its callers are the kernels above it in
#: this report, so accounting it separately only charges them the wrapper.
_PLUMBING = frozenset({"call_function"})


def _site() -> str:
    """The package frame that asked for a kernel, as `file:line`.

    Frames rather than `traceback.extract_stack`, which reads source lines from
    disk per call and costs more than several of the kernels it is naming.
    """
    marker = f"rekep{os.sep}"
    frame = sys._getframe(2)
    for _ in range(_SITE_DEPTH):
        if frame is None:
            break
        name = frame.f_code.co_filename
        if marker in name:
            return f"{name.split(marker)[-1]}:{frame.f_lineno}"
        frame = frame.f_back
    return "?"


def _accounted_kernels(call: Callable[[], Any]) -> tuple[Accounted, Accounted, float]:
    """One call with every Arrow kernel accounted, by kernel and by call site."""
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
        started = time.perf_counter()
        call()
        wall = time.perf_counter() - started
    finally:
        for name, value in original.items():
            setattr(compute, name, value)
    return by_kernel, by_site, wall


def sweep_kernels(rows: int, top: int) -> None:
    """Which Arrow kernels the boundary is, and which call site each is under.

    Wrapping every kernel costs about a fifth of the run, spread over the
    calls, so the shares are read against the *instrumented* wall time -- not
    against the clean one printed beside it.
    """
    codec = FixCodec(registry=FixRegistry())
    batch = raw_batch(rows, MIXES[0][1])
    checked(batch, codec)
    clean = best_of(lambda: FixMsg.from_message_batch(batch, codec), 1)
    by_kernel, by_site, wall = _accounted_kernels(lambda: FixMsg.from_message_batch(batch, codec))
    print(
        f"\nkernels of one mixed batch, {rows:,} rows, "
        f"{clean * 1000:.1f} ms clean, {wall * 1000:.1f} ms instrumented"
    )
    by_kernel.report("kernel", wall, top=top)
    by_site.report("call site", wall, width=44, top=top)


def main() -> int:
    options = parser(__doc__, rows=20_000, repeat=3)
    options.add_argument("--only", choices=("mix", "batch", "stages", "kernels"), default=None)
    options.add_argument("--top", type=int, default=15)
    arguments = options.parse_args()
    rows = 2_000 if arguments.quick else arguments.rows
    repeat = 1 if arguments.quick else arguments.repeat
    if arguments.only in (None, "mix"):
        sweep_mix(rows, repeat)
    if arguments.only in (None, "batch"):
        sweep_batch(rows, repeat)
    if arguments.only in (None, "stages"):
        sweep_stages(rows, repeat)
    if arguments.only in (None, "kernels"):
        sweep_kernels(rows, arguments.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
