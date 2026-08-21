"""Benchmark FIX parsing: lines to maps, maps to tag-numbered maps.

Run from `python/`::

    uv run python benchmarks/bench_fix.py            # full sweep
    uv run python benchmarks/bench_fix.py --quick    # smaller column, fewer repeats

Three questions, answered on synthetic columns whose shape matches the tests'
fixtures (wire messages with and without log noise and repeating groups, and
rendered `Name=Value` / `Group[i]=Member=Value` lines):

1. What does the vectorised parser buy over the scalar one? Both are timed on
   the same rows, and the vectorised result is asserted equal to the scalar
   one *before* anything is timed -- a benchmark that measures the wrong
   answer measures nothing.
2. Which kernels should the hot paths be made of? The tag/value cut is raced
   (`split_pattern` + `list_element` against one `extract_regex`, trimming
   and not), because that choice is baked into `parse_arrow_array` and the
   loser looked entirely plausible.
3. What does `tag_arrow_array` cost? The all-numeric cast fast path, and the
   dictionary-encoded name resolution that rendered keys pay for.

Every case is warmed once and reported as the best of `--repeat` runs; run
the script twice before quoting a number anywhere.
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

from rekep.fix import FixMessage, parse_arrow_array, tag_arrow_array  # noqa: E402

NOISE = "2026-08-14 00:05:01.147_250 [250-e7256476:9effef3e6a:72505] [ULBridge] (INFO) sent "

#: The `names` mapping a rendered column resolves through -- what
#: `FixRegistry.tags()` would return, inlined so the benchmark needs no cache.
NAMES = {
    "Side": 54,
    "Price": 44,
    "OrderQty": 38,
    "Symbol": 55,
    "Account": 1,
    "PartyID": 448,
    "PartyRole": 452,
    "NoPartyIDs": 453,
    "TransactTime": 60,
}


def wire_lines(rows: int, separator: str, *, noise: bool, groups: bool) -> list[str]:
    """Wire messages: `8=FIX...` with optional log noise and repeating groups."""
    generate = random.Random(7)
    lines = []
    for i in range(rows):
        fields = [
            "8=FIX.4.2",
            "9=178",
            "35=8",
            "49=BRK",
            "56=CLI",
            f"34={i}",
            "52=20260814-00:05:01.147",
            f"55=S{i % 512}",
            "54=1",
            f"38={generate.randint(1, 10_000)}",
            f"44={generate.random() * 100:.4f}",
            "58=fill on XPAR",
        ]
        if groups:
            fields.append("453=2")
            for entry in range(2):
                fields += [f"448=P{entry}", "447=D", f"452={entry + 1}"]
        fields.append("10=045")
        line = separator.join(fields)
        if noise:
            line = f"{NOISE}{line}{separator} latency={i % 90}us"
        lines.append(line)
    return lines


def rendered_lines(rows: int, *, groups: bool = True) -> list[str]:
    """Rendered messages: `Name=Value` pairs, indexed group entries optional.

    The two cases bracket the named path: with no group entries the inner
    `member=` regex is skipped entirely, with them a third of the tokens go
    through it.
    """
    generate = random.Random(11)
    entries = (
        [
            "NoPartyIDs[0]=PartyID=BRK",
            "NoPartyIDs[0]=PartyRole=1",
            "NoPartyIDs[1]=PartyID=CLI",
        ]
        if groups
        else []
    )
    return [
        " | ".join(
            [
                f"Account=ACCT-{i % 500:06d}",
                f"Symbol=S{i % 512}",
                "Side=1",
                f"OrderQty={generate.randint(1, 10_000)}",
                f"Price={generate.random() * 100:.4f}",
                *entries,
                f"took={i % 90}us",
            ]
        )
        for i in range(rows)
    ]


def best_of(function: Callable[[], object], repeat: int) -> float:
    """Fastest of `repeat` timed calls, after one untimed warm-up."""
    function()
    fastest = float("inf")
    for _ in range(repeat):
        started = time.perf_counter()
        function()
        fastest = min(fastest, time.perf_counter() - started)
    return fastest


def check(column: pyarrow.Array, **kwargs: object) -> None:
    """The vectorised answer *is* the scalar answer, asserted before timing."""
    maps = parse_arrow_array(column, **kwargs)
    for line, row in zip(column.to_pylist(), maps.to_pylist(), strict=True):
        expected = FixMessage.from_text(line, kwargs.get("separator")).pairs
        assert row == expected, (line, row, expected)


def sweep_parsing(rows: int, repeat: int) -> None:
    print(f"\nparsing, {rows:,} rows, best of {repeat}")
    columns = ("case", "rows/s scalar", "rows/s vector", "speedup", "fields/s vector")
    widths = (26, 14, 14, 8, 16)
    print(" ".join(f"{c:>{w}}" for c, w in zip(columns, widths, strict=True)))

    cases = [
        ("wire, SOH", wire_lines(rows, "\x01", noise=False, groups=False), {}),
        ("wire, pipe", wire_lines(rows, "|", noise=False, groups=False), {}),
        ("wire, pipe + noise", wire_lines(rows, "|", noise=True, groups=False), {}),
        ("wire, pipe + groups", wire_lines(rows, "|", noise=False, groups=True), {}),
        ("rendered names + [i]", rendered_lines(rows), {}),
        ("rendered, no groups", rendered_lines(rows, groups=False), {}),
    ]
    for label, lines, kwargs in cases:
        column = pyarrow.array(lines)
        check(column.slice(0, min(len(column), 512)), **kwargs)
        fields = int(
            pyarrow.compute.sum(
                pyarrow.compute.list_value_length(
                    parse_arrow_array(column, **kwargs).cast(
                        pyarrow.list_(
                            pyarrow.struct([("k", pyarrow.string()), ("v", pyarrow.string())])
                        )
                    )
                )
            ).as_py()
        )
        scalar = best_of(lambda lines=lines: [FixMessage.from_text(line) for line in lines], repeat)
        vector = best_of(
            lambda column=column, kwargs=kwargs: parse_arrow_array(column, **kwargs), repeat
        )
        print(
            f"{label:>26} {rows / scalar:>14,.0f} {rows / vector:>14,.0f} "
            f"{scalar / vector:>7.1f}x {fields / vector:>16,.0f}"
        )


def sweep_cut(rows: int, repeat: int) -> None:
    """The race `parse_arrow_array`'s tag/value cut was decided by."""
    print(f"\ntag/value cut, {rows:,} tokens, best of {repeat}")
    generate = random.Random(3)
    tokens = pyarrow.array(
        [
            f"{generate.choice([8, 9, 35, 49, 54, 58, 268, 269])}={'x' * generate.randint(1, 12)}"
            for _ in range(rows)
        ]
    )
    compute = pyarrow.compute

    def by_split() -> tuple:
        halves = compute.split_pattern(tokens, "=", max_splits=1)
        return (
            compute.utf8_trim_whitespace(compute.list_element(halves, 0)),
            compute.utf8_trim_whitespace(compute.list_element(halves, 1)),
        )

    def by_extract_trimming() -> tuple:
        found = compute.extract_regex(tokens, r"^\s*(?P<t>\d+)\s*=\s*(?P<v>.*?)\s*$")
        return compute.struct_field(found, "t"), compute.struct_field(found, "v")

    def by_extract_greedy() -> tuple:
        found = compute.extract_regex(tokens, r"^(?P<t>\d+)=(?P<v>.*)$")
        return compute.struct_field(found, "t"), compute.struct_field(found, "v")

    reference = by_split()
    for candidate in (by_extract_trimming, by_extract_greedy):
        tags, values = candidate()
        assert tags.equals(reference[0]) and values.equals(reference[1])

    for label, function in (
        ("split + list_element", by_split),
        ("extract_regex, trimming", by_extract_trimming),
        ("extract_regex, greedy", by_extract_greedy),
    ):
        seconds = best_of(function, repeat)
        print(f"{label:>26} {rows / seconds:>14,.0f} tokens/s")


def sweep_tags(rows: int, repeat: int) -> None:
    print(f"\ntag_arrow_array, {rows:,} rows, best of {repeat}")
    wire = parse_arrow_array(pyarrow.array(wire_lines(rows, "|", noise=False, groups=True)))
    rendered = parse_arrow_array(pyarrow.array(rendered_lines(rows)))
    entries = {
        "numeric keys -> int32 cast": (wire, {}),
        "numeric keys -> int64 cast": (wire, {"key_type": pyarrow.int64()}),
        "rendered keys via names": (rendered, {"names": NAMES, "drop_unknown": True}),
    }
    for label, (maps, kwargs) in entries.items():
        keys = int(
            pyarrow.compute.sum(
                pyarrow.compute.list_value_length(
                    maps.cast(
                        pyarrow.list_(
                            pyarrow.struct([("k", pyarrow.string()), ("v", pyarrow.string())])
                        )
                    )
                )
            ).as_py()
        )
        seconds = best_of(lambda maps=maps, kwargs=kwargs: tag_arrow_array(maps, **kwargs), repeat)
        print(f"{label:>28} {rows / seconds:>14,.0f} rows/s {keys / seconds:>14,.0f} keys/s")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--quick", action="store_true")
    arguments = parser.parse_args()
    rows = 10_000 if arguments.quick else arguments.rows
    repeat = 3 if arguments.quick else arguments.repeat
    sweep_parsing(rows, repeat)
    sweep_cut(rows * 10, repeat)
    sweep_tags(rows, repeat)


if __name__ == "__main__":
    main()
