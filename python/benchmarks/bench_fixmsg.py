"""Benchmark raw `Message` to parsed `FixMsg` protocol mixes and kernels."""

from __future__ import annotations

import pathlib
import random
import sys
import time
from collections.abc import Sequence

import pyarrow
import pyarrow.compute

# `src` for the package under measurement, and this folder for `_bench`,
# so a benchmark imports the same whether it is run or imported.
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from _bench import accounted_kernels, accounted_stages, best_of, parser  # noqa: E402

from rekep.fix import SOH  # noqa: E402
from rekep.fix.registry import FixRegistry  # noqa: E402
from rekep.fix.transcribe import FixCodec  # noqa: E402
from rekep.text import FixMsg, Message  # noqa: E402

CATEGORY_SHARES: tuple[tuple[str, int], ...] = (("OTHER", 60), ("FIX", 25), ("FIXML", 15))
MIXES: tuple[tuple[str, tuple[tuple[str, int], ...]], ...] = (
    ("mixed 60/25/15", CATEGORY_SHARES),
    ("wire FIX only", (("FIX", 1),)),
    ("bridge FIXML only", (("FIXML", 1),)),
    ("unparsed text only", (("OTHER", 1),)),
)
PLUGINS = ("OMSSales_Enrichment", "ULBridge", "ModuleMarketDataManager", "ObjkeyTagWrapper")
LEVELS = ("DEBUG", "INFO", "WARNING", None)

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


def raw_batch(rows: int, shares: Sequence[tuple[str, int]]) -> pyarrow.RecordBatch:
    """One raw batch built without timing or depending on a text reader."""
    generate = random.Random(5)
    slots = [name for name, share in shares for _ in range(share)]
    generate.shuffle(slots)
    messages = []
    for index in range(rows):
        second, micro = divmod(index, 1_000_000)
        messages.append(
            Message(
                sourceurl="memory:///bench-fixmsg.log",
                sourcerownum=index + 1,
                timestamp=(
                    f"2026-08-14 {second // 3600 % 24:02d}:{second // 60 % 60:02d}:"
                    f"{second % 60:02d}.{micro // 1000:03d}_{micro % 1000:03d}"
                ),
                threadname=f"250-e7256476:9effef3e6a:{72500 + index % 8:05d}",
                plugin=PLUGINS[index % len(PLUGINS)],
                level=LEVELS[index % len(LEVELS)],
                body=_body(slots[index % len(slots)], index, generate),
            )
        )
    return Message.into_arrow_batch(messages)


def _body(protocol: str, index: int, generate: random.Random) -> bytes:
    """One representative payload in the source spelling for `protocol`."""
    if protocol == "FIX":
        fields = (
            "8=FIX.4.2",
            "9=176",
            "35=D",
            f"34={1000 + index}",
            "49=BUYSIDE",
            "56=XPAR",
            "52=20260814-00:05:01.147",
            f"11=ORD-{index:010d}",
            f"55=S{index % 512}",
            "54=1",
            f"38={generate.randint(1, 10_000)}",
            "40=2",
            f"44={generate.random() * 100:.4f}",
            "59=0",
            "10=203",
        )
        return f"sending >> {'|'.join(fields)}| << queued seq={1000 + index}".encode()
    if protocol == "FIXML":
        parties = SOH.join(("PARTYID=BUYSIDE", "PARTYIDSOURCE=D", "PARTYROLE=1"))
        venue = SOH.join(("PARTYID=XPAR", "PARTYIDSOURCE=G", "PARTYROLE=17"))
        fields = (
            "#ISINCODE=XX0000084733",
            "#CFICODE=FXXXSX",
            f"#SYMBOL=S{index % 512}",
            "#SIDE=1",
            f"#ORDERQTY={generate.randint(1, 10_000)}",
            f"#PRICE={generate.random() * 100:.4f}",
            "#NOPARTYIDS=2",
            f"#NOPARTYIDS[0]={parties}",
            f"#NOPARTYIDS[1]={venue}",
            "#TRANSACTTIME=20260814-00:05:01.148",
            "#UNKNOWNVENUEFIELD=Z9",
        )
        return ("toBridge " + "|".join(fields)).encode()
    return (
        f"After Enrichment -> ACCOUNT=ACCT-{index % 500:06d} "
        f"CLIENTID=MCFP2 VENUE=XPAR qty={index % 997}"
    ).encode()


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


def sweep_stages(rows: int, repeat: int) -> None:
    """Where one mixed batch's milliseconds are, stage by stage."""
    codec = FixCodec(registry=FixRegistry())
    batch = raw_batch(rows, MIXES[0][1])
    checked(batch, codec)
    clean = best_of(lambda: FixMsg.from_message_batch(batch, codec), repeat)
    with accounted_stages(STAGES) as account:
        started = time.perf_counter()
        FixMsg.from_message_batch(batch, codec)
        wall = time.perf_counter() - started
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


def sweep_kernels(rows: int, top: int) -> None:
    """Which Arrow kernels the boundary is, and which call site each is under.

    The shares are read against the *instrumented* wall time printed beside
    the clean one, because wrapping the kernels costs about a fifth of the run.
    """
    codec = FixCodec(registry=FixRegistry())
    batch = raw_batch(rows, MIXES[0][1])
    checked(batch, codec)
    clean = best_of(lambda: FixMsg.from_message_batch(batch, codec), 1)
    with accounted_kernels() as (by_kernel, by_site):
        started = time.perf_counter()
        FixMsg.from_message_batch(batch, codec)
        wall = time.perf_counter() - started
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
