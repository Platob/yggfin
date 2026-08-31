"""Benchmark FIX parsing: lines to maps, maps to tag-numbered maps."""

from __future__ import annotations

import pathlib
import random
import re
import sys

import pyarrow
import pyarrow.compute

# `src` for the package under measurement, and this folder for `_bench`,
# so a benchmark imports the same whether it is run or imported.
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from _bench import best_of, parser  # noqa: E402

from rekep import FixMsg  # noqa: E402
from rekep.fix import (  # noqa: E402
    parse_arrow_array,
    tag_arrow_array,
)
from rekep.fix.message import _Names, _resolved_key, parse_pairs  # noqa: E402

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


def check(column: pyarrow.Array, **kwargs: object) -> None:
    """The vectorised answer *is* the scalar answer, asserted before timing."""
    maps = parse_arrow_array(column, **kwargs)
    for line, row in zip(column.to_pylist(), maps.to_pylist(), strict=True):
        expected = parse_pairs(line, kwargs.get("separator"))
        assert row == expected, (line, row, expected)


def sweep_parsing(rows: int, repeat: int, scalar_rows: int) -> None:
    """The scalar reference against the kernel, per row.

    Both legs are *rates*, so the scalar one is measured over `scalar_rows` and
    the kernel over all of them: the scalar path runs at some hundreds of rows
    a second against the kernel's hundreds of thousands, and pricing it at the
    same width is the whole runtime of this script.
    """
    print(f"\nparsing, {rows:,} rows ({scalar_rows:,} scalar), best of {repeat}")
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
        sampled = lines[:scalar_rows]
        scalar = best_of(lambda lines=sampled: [FixMsg.from_text(line) for line in lines], repeat)
        vector = best_of(
            lambda column=column, kwargs=kwargs: parse_arrow_array(column, **kwargs), repeat
        )
        scalar_rate, vector_rate = len(sampled) / scalar, rows / vector
        print(
            f"{label:>26} {scalar_rate:>14,.0f} {vector_rate:>14,.0f} "
            f"{vector_rate / scalar_rate:>7.1f}x {fields / vector:>16,.0f}"
        )


def sweep_tags(rows: int, repeat: int) -> None:
    print(f"\ntag_arrow_array, {rows:,} rows, best of {repeat}")
    wire = parse_arrow_array(pyarrow.array(wire_lines(rows, "|", noise=False, groups=True)))
    rendered = parse_arrow_array(pyarrow.array(rendered_lines(rows)))
    entries = {
        "numeric keys -> int32 cast": (wire, {}),
        "numeric keys -> int64 cast": (wire, {"key_type": pyarrow.int64()}),
        "rendered keys via names": (rendered, {"names": NAMES, "drop_unknown": True}),
    }
    # The two widths are one reading of one column, so they answer alike before
    # either is timed; the rendered mode reads different keys and is checked
    # against the names it was given.
    narrow, wide = (
        tag_arrow_array(wire, **kwargs) for kwargs in ({}, {"key_type": pyarrow.int64()})
    )
    assert narrow.cast(wide.type).equals(wide)
    named = tag_arrow_array(rendered, names=NAMES, drop_unknown=True)
    tagged = {tag for row in named.to_pylist() for tag, _ in row}
    assert tagged and tagged <= set(NAMES.values()), tagged
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


def named_pairs(rows: int) -> list[list[tuple[str, object]]]:
    """One message per row as `(name, value)` pairs, spelled every way a bridge does.

    A fifth of the keys are numeric, a fifth are already canonical, and the
    rest are cased, separator-ed or decorated -- which is the mix that decides
    whether the fold is paying for itself or for nothing.
    """
    generate = random.Random(11)
    spellings = ("Side", "side", "SIDE", "s_i_d_e".replace("_", ""), "Instrument.Side")
    built = []
    for i in range(rows):
        built.append(
            [
                ("MsgType", "8"),
                ("34", i),
                ("TransactTime", "20260814-00:05:01.147"),
                ("Symbol", f"S{i % 512}"),
                (generate.choice(spellings), 1),
                ("order_qty", float(generate.randint(1, 10_000))),
                ("Price", generate.random() * 100),
                ("PartyID[0]", "BRK"),
                ("VenueOwnField", "kept"),
            ]
        )
    return built


def sweep_pairs(rows: int, repeat: int, scalar_rows: int) -> None:
    """`from_pairs`, and the three ways its keys could have been resolved.

    `from_pairs` is scalar, so it is priced over `scalar_rows` for the reason
    `sweep_parsing` prices its own scalar leg over a sample; the key race below
    is in kernels and gets every key.
    """
    print(f"\nfrom_pairs, {scalar_rows:,} rows, best of {repeat}")
    pairs = named_pairs(scalar_rows)
    fields = sum(len(one) for one in pairs)
    built = [FixMsg.from_pairs(one, NAMES) for one in pairs]
    assert built[0].get(54).raw == "1"
    assert built[0].get("VenueOwnField").raw == "kept"
    seconds = best_of(lambda: [FixMsg.from_pairs(one, NAMES) for one in pairs], repeat)
    print(
        f"{'from_pairs':>28} {scalar_rows / seconds:>14,.0f} rows/s "
        f"{fields / seconds:>14,.0f} fields/s"
    )

    # The key-resolution race, on the keys alone. `from_pairs` above pays for
    # value rendering and object construction too, which would drown the
    # difference the three readings actually make.
    #
    # Raced at two dictionary sizes on purpose. An alternation's cost scales
    # with how many names are in it and a hash probe's does not, so a race at
    # nine names says nothing about a FIX dictionary at fifteen hundred -- and
    # nine names is exactly where "just use a regex" looks right.
    keys = [key for one in named_pairs(rows) for key, _ in one]
    print(f"\n  resolving {len(keys):,} keys, best of {repeat}")
    for size in (len(NAMES), 1_500):
        _race_keys(keys, _sized_names(size), repeat)


def _sized_names(size: int) -> dict[str, int]:
    """`NAMES`, padded with plausible field names up to `size` entries.

    Padded rather than scraped so the benchmark runs with no cache and no
    network; what matters to the race is how many alternatives a regex has to
    get past, and a synthetic name is as many as a real one.
    """
    built = dict(NAMES)
    generate = random.Random(3)
    parts = ("Order", "Exec", "Leg", "Party", "Settl", "Alloc", "Quote", "Trade", "Md")
    tails = ("ID", "Qty", "Px", "Type", "Date", "Time", "Ref", "Source", "Status", "Code")
    tag = 5_000
    while len(built) < size:
        built.setdefault(
            f"{generate.choice(parts)}{generate.choice(tails)}{len(built)}", tag := tag + 1
        )
    return built


def _race_keys(keys: list[str], names: dict[str, int], repeat: int) -> None:
    """Three readings of the same keys, each resolving to a **tag**.

    All three resolve, because an alternation that only answers "is this a
    known name" is half a lookup: the tag still has to be found afterwards, and
    racing the half against the whole is how a regex wins a benchmark it would
    lose in production.
    """
    folded = _Names.of(names).keys
    lowered = {name.lower(): str(tag) for name, tag in names.items()}
    alternation = re.compile(
        r"^(?:" + "|".join(sorted(map(re.escape, names), key=len, reverse=True)) + r")$",
        re.IGNORECASE,
    )

    def by_resolved_key() -> object:
        return [_resolved_key(key, folded) for key in keys]

    def by_alternation() -> object:
        found = []
        for key in keys:
            match = alternation.match(key)
            found.append(lowered.get(match[0].lower()) if match else key)
        return found

    def by_probe() -> object:
        return [lowered.get(key.lower(), key) for key in keys]

    # Not equal outputs: the shipped path also resolves a *rendered* key --
    # `PartyID[0]`, `Instrument.Side` -- which neither of the other two can
    # see, and that is the difference being priced. What has to hold is that
    # where the plain probe answers at all, all three answer the same.
    shipped, plain = by_resolved_key(), by_probe()
    for key, one, other in zip(keys, shipped, plain, strict=True):
        if other != key:
            assert one == other, (key, one, other)
    print(f"    {len(names):,} names in the dictionary")
    for label, reading in (
        ("_resolved_key (shipped)", by_resolved_key),
        ("one case-insensitive alternation", by_alternation),
        ("lower, then probe", by_probe),
    ):
        seconds = best_of(reading, repeat)
        print(f"{label:>40} {len(keys) / seconds:>14,.0f} keys/s")


def main() -> None:
    arguments = parser(__doc__, rows=100_000).parse_args()
    # `--quick` is the CI smoke leg: one measured vector pass and a bounded
    # scalar reference are enough to exercise every assertion. Statistical
    # timing remains the default benchmark's job.
    rows = 1_000 if arguments.quick else arguments.rows
    repeat = 1 if arguments.quick else arguments.repeat
    sweep_parsing(rows, repeat, 5 if arguments.quick else 2_000)
    sweep_tags(rows, repeat)
    sweep_pairs(rows, repeat, 10 if arguments.quick else 10_000)


if __name__ == "__main__":
    main()
