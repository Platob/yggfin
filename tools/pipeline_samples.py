"""Publish one order of the bundled capture as every stage lands it.

The pipeline pages under `docs/pipeline/tasks/` show real rows rather than
invented ones: one business chain of `python/tests/data/ulbridge.log` -- a
partial fill and the fill that closed the order after it -- as `parse_messages` stores its lines,
`parse_fix_raw` parses them, `parse_fix_refined` walks them and `build_dbt`
derives the products. Each page includes its own Markdown file from
`docs/pipeline/tasks/samples/` through `pymdownx.snippets`, so what a page
shows is what a run lands, and `--check` regenerates into memory and fails on
any difference, which is what the test suite asks.

Run from the repository root whenever a stage or the fixture changes:

    uv run --project python --group runner python tools/pipeline_samples.py
    uv run --project python --group runner python tools/pipeline_samples.py --check

Each of the four tasks runs as `rekep tasks <name> run` runs it, into a
private catalog; `--catalog` reads an existing warehouse -- the one an Airflow
run wrote, say -- instead.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime
import decimal
import io
import json
import os
import pathlib
import sys
import tempfile
import uuid
from typing import Any

import pyarrow
import pyarrow.compute

ROOT = pathlib.Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "docs" / "pipeline" / "tasks" / "samples"
FIXTURE = ROOT / "python" / "tests" / "data" / "ulbridge.log"

#: The day the fixture was captured on; `end` names the exclusive end of it.
WINDOW = {"start": "2026-08-14", "end": "2026-08-14"}

#: The business identifier the pages follow.
CHAIN = "00026877711XOEA0"

#: Every task, in dependency order, with the parameters of its own.
TASKS = (
    ("parse_messages", {"filesystem": "file:python/tests/data/ulbridge.log", **WINDOW}),
    ("parse_fix_raw", dict(WINDOW)),
    ("parse_fix_refined", dict(WINDOW)),
    ("build_dbt", {}),
)

TABLES = (
    "logs.messages",
    "fix.raw",
    "fix.refined",
    "orders.events",
    "orders.current",
    "executions.fills",
)

#: One sample file per task page.
PAGES = ("parse-messages", "parse-fix-raw", "parse-fix-refined", "build-dbt")


# -- landing the fixture ------------------------------------------------------


def catalog_in(root: pathlib.Path) -> dict[str, Any]:
    """A SQLite catalog and file warehouse under `root`."""
    return {
        "name": "rekep",
        "properties": {
            "type": "sql",
            "uri": f"sqlite:///{root / 'catalog.db'}",
            "warehouse": (root / "warehouse").as_uri(),
        },
    }


def landed(root: pathlib.Path) -> dict[str, Any]:
    """Run the four tasks over the fixture into a catalog under `root`."""
    from rekep import cli

    catalog = catalog_in(root)
    os.environ.setdefault("DBT_TARGET_PATH", str(root / "dbt-target"))
    os.environ.setdefault("DBT_LOG_PATH", str(root / "dbt-logs"))
    for name, held in TASKS:
        argv = [
            "tasks",
            name,
            "run",
            "--parameter",
            f"catalog={json.dumps(catalog)}",
        ]
        for parameter, value in held.items():
            argv += ["--parameter", f"{parameter}={json.dumps(value)}"]
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            code = cli.main(argv)
        if code != 0:
            raise SystemExit(f"{name} exited with {code}")
        result = json.loads(printed.getvalue())
        counts = ", ".join(f"{result[count]} {count}" for count in ("read", "written", "skipped"))
        print(f"  {name}: {counts}")
    return catalog


def tables(catalog: dict[str, Any]) -> dict[str, pyarrow.Table]:
    """Every table the pages read, whole."""
    from rekep.iceberg import IcebergCatalog

    store = IcebergCatalog.from_dict(catalog)
    try:
        return {name: store.dataset(name).read_arrow_table() for name in TABLES}
    finally:
        store.close()


# -- one chain across the tables ---------------------------------------------


def _uuids(values: list[bytes]) -> pyarrow.Array:
    return pyarrow.array(values, type=pyarrow.binary(16))


def raw_seqnums(lines: pyarrow.Table) -> dict[bytes, int]:
    """Each line's row number, keyed by the identity FIX rows name in `srcuuids`."""
    return dict(
        zip(
            lines.column("curruuid").to_pylist(),
            lines.column("seqnum").to_pylist(),
            strict=True,
        )
    )


def source_seqnum(row: dict[str, Any], positions: dict[bytes, int]) -> int | None:
    """The row number of the line a FIX row was parsed from, where it names one."""
    sources = row.get("srcuuids") or ()
    return positions.get(sources[0]) if sources else None


def walked(chain: pyarrow.Table) -> pyarrow.Table:
    """The chain in the refined table's own order.

    The declared sort order, `currunix, seqnum, curruuid`, with a null step
    after the numbered ones, which is where Arrow and the stored table both
    put it: a head follows the steps that follow it.
    """
    return chain.sort_by(
        [("currunix", "ascending"), ("seqnum", "ascending"), ("curruuid", "ascending")]
    )


def selected(held: dict[str, pyarrow.Table]) -> dict[str, pyarrow.Table]:
    """The chain's rows in every table, each in the order its page reads them."""
    equal, is_in = pyarrow.compute.equal, pyarrow.compute.is_in
    refined = held["fix.refined"]
    chain = refined.filter(equal(refined.column("crosscode"), CHAIN))
    if not chain.num_rows:
        raise SystemExit(f"{CHAIN} is not a chain of fix.refined")
    sources = _uuids(
        sorted({line for named in chain.column("srcuuids").to_pylist() for line in named})
    )
    orders = _uuids(sorted(set(chain.column("crossuuid").to_pylist())))

    lines = held["logs.messages"]
    raw = held["fix.raw"]
    positions = raw_seqnums(lines)
    named = pyarrow.compute.list_element(raw.column("srcuuids"), 0)
    raw_chain = raw.filter(is_in(named, value_set=sources))
    events, current, fills = (held[name] for name in TABLES[3:])

    def by_order(table: pyarrow.Table) -> pyarrow.Table:
        return table.filter(is_in(table.column("orderkey"), value_set=orders))

    return {
        "logs.messages": lines.filter(is_in(lines.column("curruuid"), value_set=sources)).sort_by(
            "seqnum"
        ),
        "fix.raw": raw_chain.take(
            pyarrow.array(
                sorted(
                    range(raw_chain.num_rows),
                    key=lambda index: (
                        source_seqnum(
                            raw_chain.slice(index, 1).to_pylist()[0],
                            positions,
                        )
                        or -1
                    ),
                )
            )
        ),
        "fix.refined": walked(chain),
        "orders.events": by_order(events).sort_by(
            [("eventtime", "ascending"), ("eventkey", "ascending")]
        ),
        "orders.current": by_order(current),
        "executions.fills": by_order(fills).sort_by("executionkey"),
    }


# -- rendering ----------------------------------------------------------------


def instant(value: datetime.datetime) -> str:
    """UTC, to the millisecond, and to the microsecond only where one is set."""
    text = value.strftime("%Y-%m-%d %H:%M:%S.%f")
    return text if value.microsecond % 1000 else text[:-3]


def identity(value: bytes) -> str:
    """The last eight hex digits of a sixteen-byte identity, marked as a tail."""
    return f"…{uuid.UUID(bytes=value).hex[-8:]}"


def cell(value: Any) -> str:
    """One value as a table cell; null is an empty cell."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, bytes):
        return identity(value) if len(value) == 16 else value.decode("utf-8", "replace")
    if isinstance(value, datetime.datetime):
        return instant(value)
    if isinstance(value, decimal.Decimal):
        text = format(value.normalize(), "f")
        return text if text else "0"
    if isinstance(value, list):
        return str(len(value))
    return str(value).replace("|", "\\|")


def prose(body: str, width: int = 64) -> str:
    """The body -- what the bridge printed after its header -- cut to `width`."""
    return cell(body[:width] + ("…" if len(body) > width else ""))


def table(
    columns: list[str], rows: list[dict[str, Any]], shown: dict[str, Any] | None = None
) -> str:
    """A Markdown table over `columns`, rendering each cell with `shown` or `cell`."""
    shown = shown or {}
    head = "| " + " | ".join(f"`{name}`" for name in columns) + " |"
    rule = "|" + "|".join(" --- " for _ in columns) + "|"
    lines = [head, rule]
    for row in rows:
        lines.append("| " + " | ".join(shown.get(name, cell)(row[name]) for name in columns) + " |")
    return "\n".join(lines)


def keyed(columns: list[str], row: dict[str, Any]) -> str:
    """One row as a two-column table, a column a line."""
    lines = ["| column | value |", "| --- | --- |"]
    lines += [f"| `{name}` | {cell(row[name])} |" for name in columns]
    return "\n".join(lines)


def rows(held: pyarrow.Table, columns: list[str]) -> list[dict[str, Any]]:
    return held.select(columns).to_pylist()


def messages_page(lines: pyarrow.Table) -> str:
    captures = [
        "seqnum",
        "currunix",
        "msgthreadid",
        "msgsessionid",
        "msgctxid",
        "msgseqnum",
        "msgpluginid",
        "loglevel",
    ]
    identities = ["seqnum", "currhashcode", "curruuid", "body"]
    return "\n\n".join(
        [
            f"**The event each of the {lines.num_rows} lines settled on, "
            "and what its header stated**",
            table(captures, rows(lines, captures)),
            "**What each line is: its code, its identity, and its body past the header**",
            table(identities, rows(lines, identities), {"body": prose}),
        ]
    )


def raw_page(raw: pyarrow.Table) -> str:
    read = [
        "msgtype",
        "msgdirection",
        "execid",
        "ordstatus",
        "lastqty",
        "cumqty",
        "leavesqty",
        "sendingtime",
        "transacttime",
    ]
    event = [
        "currunix",
        "curruuid",
        "crosscode",
        "seqnum",
        "prevuuid",
        "prevunix",
        "parentuuids",
        "srcuuids",
    ]
    return "\n\n".join(
        [
            f"**What the parse read off the {raw.num_rows} messages**",
            table(read, rows(raw, read)),
            "**The event columns a `fix.raw` row carries, and the four it leaves empty**",
            table(
                event,
                rows(raw, event),
                {"srcuuids": lambda value: identity(value[0])},
            ),
        ]
    )


def refined_page(refined: pyarrow.Table, raw: pyarrow.Table) -> str:
    walked = [
        "currunix",
        "curruuid",
        "prevuuid",
        "seqnum",
        "parentuuids",
        "msgdirection",
        "state",
        "creaunix",
        "exprtime",
    ]
    # A `fix.raw` row is one frame off one line, so the line it names is what a
    # walked row is read back through. A walked row names every line its
    # event was logged on, so the join is one row per line, not per event.
    before = {
        row["srcuuids"][0]: row
        for row in rows(raw, ["srcuuids", "currunix", "curruuid"])
        if row["srcuuids"]
    }

    def read_at(line: bytes, held: str) -> Any:
        if line not in before:
            raise SystemExit(
                f"a walked row names a line no `fix.raw` row of the chain read: {line!r}"
            )
        return before[line][held]

    moved = [
        {
            "srcuuid": line,
            "fix.raw currunix": read_at(line, "currunix"),
            "fix.raw curruuid": read_at(line, "curruuid"),
            "fix.refined currunix": row["currunix"],
            "fix.refined curruuid": row["curruuid"],
        }
        for row in rows(refined, ["srcuuids", "currunix", "curruuid"])
        for line in (row["srcuuids"] or ())
    ]
    return "\n\n".join(
        [
            f"**The {refined.num_rows} walked rows of chain `{CHAIN}`, in the table's own order**",
            table(walked, rows(refined, walked)),
            "**What the walk did to each line it folded: its row in both tables**",
            table(list(moved[0]), moved),
        ]
    )


def products_page(events: pyarrow.Table, current: pyarrow.Table, fills: pyarrow.Table) -> str:
    event = [
        "eventkey",
        "prevuuid",
        "seqnum",
        "eventtime",
        "clordid",
        "cumqty",
        "leavesqty",
        "avgpx",
        "state",
        "exectype",
    ]
    fill = [
        "executionkey",
        "eventkey",
        "executiontime",
        "execid",
        "isincode",
        "miccode",
        "lastqty",
        "lastpx",
        "currency",
        "state",
    ]
    held = current.to_pylist()
    if len(held) != 1:
        raise SystemExit(f"orders.current holds {len(held)} rows for the order, not one")
    return "\n\n".join(
        [
            f"**`orders.events`: the {events.num_rows} events of the order**",
            table(event, rows(events, event)),
            "**`orders.current`: the one row the events fold to**",
            keyed(current.column_names, held[0]),
            f"**`executions.fills`: the {fills.num_rows} executions the order states**",
            table(fill, rows(fills, fill)),
        ]
    )


def rendered(held: dict[str, pyarrow.Table]) -> dict[str, str]:
    """Every page's file, by page name, from the tables as landed."""
    chosen = selected(held)
    pages = {
        "parse-messages": messages_page(chosen["logs.messages"]),
        "parse-fix-raw": raw_page(chosen["fix.raw"]),
        "parse-fix-refined": refined_page(chosen["fix.refined"], chosen["fix.raw"]),
        "build-dbt": products_page(
            chosen["orders.events"],
            chosen["orders.current"],
            chosen["executions.fills"],
        ),
    }
    note = (
        f"<!-- Generated by tools/pipeline_samples.py from python/tests/data/ulbridge.log,"
        f" chain {CHAIN}. Do not edit: regenerate. -->\n\n"
    )
    return {name: note + pages[name] + "\n" for name in PAGES}


# -- entry point --------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="fail when a published sample differs")
    parser.add_argument("--catalog", help="JSON catalog to read instead of running the tasks")
    arguments = parser.parse_args(argv)

    os.chdir(ROOT)
    if arguments.catalog:
        held = tables(json.loads(arguments.catalog))
    else:
        with tempfile.TemporaryDirectory(prefix="rekep-samples-") as private:
            held = tables(landed(pathlib.Path(private)))
    pages = rendered(held)

    if arguments.check:
        stale = [
            name
            for name, text in pages.items()
            if not (SAMPLES / f"{name}.md").is_file()
            or (SAMPLES / f"{name}.md").read_text(encoding="utf-8") != text
        ]
        if stale:
            print(
                f"stale: {', '.join(stale)} -- run tools/pipeline_samples.py",
                file=sys.stderr,
            )
            return 1
        print(f"{len(pages)} samples match")
        return 0

    SAMPLES.mkdir(parents=True, exist_ok=True)
    for name, text in pages.items():
        target = SAMPLES / f"{name}.md"
        target.write_text(text, encoding="utf-8")
        print(f"  wrote {target.relative_to(ROOT)} ({len(text):,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
