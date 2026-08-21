"""Benchmark the row id: packing, folding, hashing, and canonicalising a row.

Run from `python/`::

    uv run python benchmarks/bench_ids.py            # every sweep
    uv run python benchmarks/bench_ids.py --quick    # 100,000 rows, best of 3

Three questions, and they have different answers. **Packing** is uint64 shifts
over a whole column, so it is measured per column and the number is enormous --
what matters there is that the Arrow path costs nothing beside the parse it
rides on, and that a `timestamp("ms")` column reaches numpy without a copy.
**Hashing** is per row by construction: xxh3 has no vectorised form, and the
digest is the row's identity. **Canonicalising** is the expensive half of an id
for anything that is not already bytes, which is why a log line -- which is --
is hashed as it stands.

Every case asserts its answer before it is timed: a benchmark that measures the
wrong id measures nothing.
"""

from __future__ import annotations

import argparse
import hashlib
import pathlib
import sys
import time

import numpy
import pyarrow

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from rekep import ids  # noqa: E402

#: A moment inside the time bits, and the shape of a real log line.
MOMENT = 1_786_665_901_147
LINE = (
    b"2026-08-14 00:05:01.147_250 [250-e7256476:9effef3e6a:72505] [OMSSales_Enrichment] "
    b"(DEBUG) payload 12345: ACCOUNT=ACCT-000123 routed XPAR qty=400"
)


def rows(count: int) -> tuple[pyarrow.Array, pyarrow.Array, list[bytes]]:
    """One column of timestamps, one of digests, and the lines they came from."""
    lines = [LINE + b" #%d" % index for index in range(count)]
    digests = [ids.signed(ids.hash_payload(line)) for line in lines]
    millis = [MOMENT + index // 8 for index in range(count)]  # eight rows per millisecond
    return (
        pyarrow.array(millis, pyarrow.timestamp("ms")),
        pyarrow.array(digests, pyarrow.int64()),
        lines,
    )


def best(repeat: int, call) -> float:  # noqa: ANN001 - a thunk
    """Seconds of the fastest of `repeat` runs, warmed once first."""
    call()
    fastest = float("inf")
    for _ in range(repeat):
        started = time.perf_counter()
        call()
        fastest = min(fastest, time.perf_counter() - started)
    return fastest


def sweep_packing(count: int, repeat: int) -> None:
    """The column path against the scalar one, and the units beside each other."""
    millis, digests, _ = rows(count)
    nanos = pyarrow.array(
        [value * 1_000_000 for value in millis.view(pyarrow.int64()).to_pylist()],
        pyarrow.timestamp("ns"),
    )
    expected = ids.pack_arrow(millis, digests)
    assert expected.to_pylist()[:3] == [
        ids.pack(moment, digest)
        for moment, digest in zip(
            millis.view(pyarrow.int64()).to_pylist()[:3], digests.to_pylist()[:3], strict=True
        )
    ], "the column path and the scalar one disagree"
    assert ids.pack_arrow(nanos, digests).equals(expected), "the unit changed the answer"

    print(f"\npacking {count:,} rows, best of {repeat}")
    print(f"{'case':>34} {'seconds':>10} {'rows/s':>16}")
    cases = (
        ("pack_arrow, timestamp[ms] (no copy)", lambda: ids.pack_arrow(millis, digests)),
        ("pack_arrow, timestamp[ns] (divide)", lambda: ids.pack_arrow(nanos, digests)),
        ("unpack_arrow", lambda: ids.unpack_arrow(expected)),
        ("fold_numpy alone", lambda: ids.fold_numpy(digests)),
        (
            "pack, row by row in Python",
            lambda: [
                ids.pack(moment, digest)
                for moment, digest in zip(
                    millis.view(pyarrow.int64()).to_pylist(), digests.to_pylist(), strict=True
                )
            ],
        ),
    )
    for label, call in cases:
        seconds = best(repeat, call)
        print(f"{label:>34} {seconds:>10.4f} {count / seconds:>16,.0f}")


def sweep_hashing(count: int, repeat: int) -> None:
    """What the identity costs per row, against the hash it replaced.

    blake2b is here because it is what this package used before the id made the
    digest an identity, and because the number is the reason `xxhash` is a
    dependency rather than an extra.
    """
    _, _, lines = rows(count)
    nbytes = sum(len(line) for line in lines)
    assert ids.hash_payload(lines[0]) != ids.hash_payload(lines[1])

    print(f"\nhashing {count:,} lines, {nbytes / 2**20:.1f} MiB, best of {repeat}")
    print(f"{'case':>34} {'seconds':>10} {'rows/s':>16} {'MB/s':>9}")
    cases = (
        ("xxh3-64 (the id's hash)", lambda: [ids.hash_payload(line) for line in lines]),
        (
            "blake2b-64 (what it replaced)",
            lambda: [hashlib.blake2b(line, digest_size=8).digest() for line in lines],
        ),
        ("xxh3-64 + signed()", lambda: [ids.signed(ids.hash_payload(line)) for line in lines]),
    )
    for label, call in cases:
        seconds = best(repeat, call)
        print(
            f"{label:>34} {seconds:>10.4f} {count / seconds:>16,.0f} "
            f"{nbytes / 2**20 / seconds:>9.1f}"
        )


def sweep_rows(count: int, repeat: int) -> None:
    """A logical row, end to end: canonicalise, hash, pack.

    The comparison that matters is against a payload that is *already* bytes:
    a log line is its own canonical form, and re-encoding it would only make it
    a different row.
    """
    row = {
        "symbol": "XPAR",
        "size": 400,
        "venue": None,
        "account": "ACCT-000123",
        "price": 12.5,
        "tags": ["a", "b"],
    }
    lines = [LINE + b" #%d" % index for index in range(count)]
    assert ids.row_id(MOMENT, row) == ids.pack(MOMENT, ids.hash_row(row))

    print(f"\none row, end to end, {count:,} times, best of {repeat}")
    print(f"{'case':>34} {'seconds':>10} {'rows/s':>16}")
    cases = (
        ("canonical(row) alone", lambda: [ids.canonical(row) for _ in range(count)]),
        ("row_id(mapping)", lambda: [ids.row_id(MOMENT, row) for _ in range(count)]),
        ("row_id(bytes) -- a log line", lambda: [ids.row_id(MOMENT, line) for line in lines]),
    )
    for label, call in cases:
        seconds = best(repeat, call)
        print(f"{label:>34} {seconds:>10.4f} {count / seconds:>16,.0f}")


def sweep_collisions(count: int) -> None:
    """The birthday bound, measured rather than only quoted.

    Ids collide *within one millisecond*, so the sweep puts every row in one
    and counts what is left. The expected number is `k*(k-1)/2 / 2**HASH_BITS`.
    """
    print(f"\ncollisions in one millisecond ({ids.HASH_BITS} hash bits)")
    columns = ("rows in the millisecond", "distinct ids", "collisions", "expected")
    widths = (34, 14, 12, 10)
    print(" ".join(f"{name:>{width}}" for name, width in zip(columns, widths, strict=True)))
    for burst in (100, 1_000, 1_705, min(count, 10_000)):
        lines = [LINE + b" #%d" % index for index in range(burst)]
        packed = {ids.row_id(MOMENT, line) for line in lines}
        expected = burst * (burst - 1) / 2 / 2**ids.HASH_BITS
        print(f"{burst:>34,} {len(packed):>14,} {burst - len(packed):>12,} {expected:>10.2f}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--quick", action="store_true")
    arguments = parser.parse_args()
    count = 100_000 if arguments.quick else arguments.rows
    repeat = 3 if arguments.quick else arguments.repeat
    print(f"numpy {numpy.__version__}, pyarrow {pyarrow.__version__}")
    sweep_packing(count, repeat)
    sweep_hashing(count, repeat)
    sweep_rows(min(count, 200_000), repeat)
    sweep_collisions(count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
