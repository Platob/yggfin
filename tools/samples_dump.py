"""Write the data-sample pages: real rows the tasks land over the shipped capture.

Every task runs over `data/capture/ulbridge.log`, in production order and over
one window, into three temporary local catalogs, and this writes what they
landed as small tables a reader checks a column's meaning against:

- `docs/samples/index.md`, what each task answered and each table holds;
- `docs/samples/<layer>/<table>.md`, representative rows of one table.

The rows follow one story through the layers: the lines of one execution, the
frames the parse read off them -- each report of the fill with the execution
it splits off, every copy a row of its own -- the event the walk folded them
into, and the execution leaf the book fold answered for it.

The capture is read as `file:data/capture/ulbridge.log` from the repository
root, so every identity printed is the one a reader's own run from there lands.
No page holds a snapshot id, which is new on every commit. Run from the
repository root whenever a task, a table's shape or the capture changes:

    uv run --project python python tools/samples_dump.py

`python/tests/test_docs.py` fails, under `-m integration`, on any drift between
a page and this.
"""

from __future__ import annotations

import collections
import contextlib
import decimal
import os
import pathlib
import tempfile
import uuid
import zoneinfo
from collections.abc import Iterator, Sequence
from typing import Any

from rekep import MarketDataKind, Side, State, Storages
from rekep.dataset import sorted_rows
from rekep.fix import (
    EVENT_CLOCK,
    PARSE_COLUMNS,
    SORT_COLUMNS,
    UNDATED,
    FixCodec,
    fix_parse_arrow_reader,
)
from rekep.iceberg import window_filter
from rekep.pipeline import (
    BOOKS,
    EVENTS,
    EXECUTIONS,
    FIX_MESSAGES,
    FIX_MESSAGES_RAW,
    FLATTENERS,
    LOG_MESSAGES,
    ORDERS,
    QUOTES,
    Landed,
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
)
from rekep.storages import LAYERS
from rekep.text import log_message_field
from rekep.times import TIMEZONE, window_of

ROOT = pathlib.Path(__file__).resolve().parents[1]
PAGES = ROOT / "docs" / "samples"

#: The capture, as a reader's run from the repository root addresses it.
CAPTURE = "file:data/capture/ulbridge.log"

#: The one window every task runs over. The capture is read in its bridge's
#: zone, so a line lands in the hour of the message it carries. The window
#: opens at midnight, so the early trade of 01:03 is in it, and closes at
#: 16:30: past 12:46, the day's order flow, past 14:52, its one trade report,
#: and past 16:25, where the walk expires the day's open order.
WINDOW = window_of("2026-08-14T00:00:00Z", "2026-08-14T16:30:00Z")

#: The execution the pages follow through every layer: the last fill of order
#: `00084776691VFRM7`, logged at every hop it passed.
EXECID = "00030561317VOJO7"

#: The chains the silver page shows walked, as their side-prefixed cross
#: codes: an order filled at one instant, whose fills the walk takes in the
#: table's order -- the place each copy took there, then `curruuid` -- rather
#: than in the order they happened, so its last fill stands as a head of its
#: own; and one left open that the walk restates on every hour and expires at
#: its deadline.
CHAINS = ("BUYS:00084776691VFRM7", "BUYS:00037497066VFRM7")

#: The lines the log page shows: the first ones the capture holds.
FIRST_LINES = 8

#: How much of a line's body a cell shows.
EXCERPT = 72


@contextlib.contextmanager
def landing() -> Iterator[Storages]:
    """Three empty local catalogs under a temporary folder, from the repository root."""
    held = pathlib.Path.cwd()
    with tempfile.TemporaryDirectory() as scratch:
        root = pathlib.Path(scratch)
        mapping = {
            layer: {
                "name": layer,
                "properties": {
                    "type": "sql",
                    "uri": f"sqlite:///{root / layer}.db",
                    "warehouse": str(root / layer),
                },
            }
            for layer in LAYERS
        }
        os.chdir(ROOT)
        try:
            with Storages.from_dict(mapping) as storages:
                yield storages
        finally:
            os.chdir(held)


def run(storages: Storages) -> dict[str, Landed]:
    """Every task over `WINDOW`, in production order, by task name."""
    landed = {
        "parse_log_messages": parse_log_messages(CAPTURE, storages, WINDOW),
        "parse_fix_messages_raw": parse_fix_messages_raw(storages, WINDOW),
        "parse_fix_messages_refined": parse_fix_messages_refined(storages, WINDOW),
        "parse_books": parse_books(storages, WINDOW),
    }
    books = landed["parse_books"].snapshot_id
    for task in FLATTENERS.values():
        landed[task.__name__] = task(storages, WINDOW, snapshot_id=books)
    return landed


def read(storages: Storages, table: str) -> list[dict[str, Any]]:
    """One stored table, whole, as rows in its sort order."""
    dataset = storages.dataset(table)
    try:
        held = dataset.read_arrow_table()
    finally:
        dataset.close()
    return sorted_rows(held, SORT_COLUMNS).to_pylist()


def parsed(storages: Storages) -> list[dict[str, Any]]:
    """Every message the parse answers over the window's stored lines, before the key folds them."""
    carrier = log_message_field()
    lines = storages.dataset(LOG_MESSAGES, field=carrier)
    try:
        scanned = lines.read_arrow_reader(
            carrier,
            row_filter=window_filter(EVENT_CLOCK, WINDOW),
            columns=PARSE_COLUMNS,
        )
        codec = FixCodec.from_env(default_sending_time=UNDATED)
        answered = fix_parse_arrow_reader(codec, scanned)
        try:
            return answered.read_all().select(["curruuid", "srcuuids"]).to_pylist()
        finally:
            answered.close()
    finally:
        lines.close()


# -- cells --------------------------------------------------------------------


def line_of(by_line: dict[uuid.UUID | None, dict[str, Any]], row: dict[str, Any]) -> dict[str, Any]:
    """The one line a parsed message names, whatever report it names beside it."""
    (line,) = [
        by_line[identity(source)] for source in row["srcuuids"] if identity(source) in by_line
    ]
    return line


def identity(value: Any) -> uuid.UUID | None:
    """An identity as a table holds it (sixteen bytes) or as a read states it."""
    if value is None or isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(bytes=bytes(value))


def short(value: Any) -> str:
    """An identity by the last six hex digits of it, which is what tells two apart here."""
    held = identity(value)
    return "" if held is None else f"`…{held.hex[-6:]}`"


def instant(value: Any) -> str:
    """An instant in UTC, to the microsecond it is stored at."""
    if value is None:
        return ""
    text = value.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
    return f"`{text}`"


def state(code: int | None) -> str:
    """A `state` code with the name of the member it stores."""
    return "" if code is None else f"`{State(code).name}` ({code})"


def side(code: int | None) -> str:
    """A `side` code with the name of the member it stores."""
    return "" if code is None else f"`{Side(code).name}` ({code})"


def marketdatakind(code: int | None) -> str:
    """A `marketdatakind` code with the name of the member it stores."""
    return "" if code is None else f"`{MarketDataKind(code).name}` ({code})"


def number(value: Any) -> str:
    """A decimal as its digits, without the scale's trailing zeros."""
    if value is None:
        return ""
    if isinstance(value, decimal.Decimal):
        value = value.normalize()
        return f"{value:f}" if value == value.to_integral() else str(value)
    return str(value)


def code(value: Any) -> str:
    """Text as a code span, printable and one line."""
    if value is None or value == "":
        return ""
    text = "".join(
        ch if ch.isprintable() else f"\\x{ord(ch):02x}" for ch in str(value).replace("\r\n", "\n")
    )
    fence = "``" if "`" in text else "`"
    pad = " " if fence == "``" else ""
    return f"{fence}{pad}{text}{pad}{fence}"


def excerpt(text: str | None, width: int = EXCERPT) -> str:
    """The start of a body, as a code span."""
    if text is None:
        return ""
    held = text if len(text) <= width else text[: width - 1] + "…"
    return code(held)


def table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    """A Markdown table, numbers right-aligned by their header's trailing colon."""
    aligned = [":---:" if head.endswith(":") else "---" for head in headers]
    names = [head.rstrip(":") for head in headers]
    lines = [f"| {' | '.join(names)} |", f"| {' | '.join(aligned)} |"]
    lines += [f"| {' | '.join(row)} |" for row in rows]
    return lines


def rows_of(count: int) -> str:
    return f"{count} row" if count == 1 else f"{count} rows"


def window_text() -> str:
    lower, upper = WINDOW
    return f"`[{lower:%Y-%m-%d %H:%M}, {upper:%Y-%m-%d %H:%M})` UTC"


def page(title: str, lead: Sequence[str], *sections: Sequence[str]) -> str:
    """A page: its title, its lead paragraph and its sections, one blank line apart."""
    blocks = []
    for part in ([f"# {title}"], list(lead), *(list(section) for section in sections)):
        while part and part[-1] == "":
            part.pop()
        if part:
            blocks.append("\n".join(part))
    return "\n\n".join(blocks) + "\n"


def table_link(table: str, up: int) -> str:
    """A link from a sample page to the table's column reference."""
    layer, _, name = table.partition(".")
    return f"[{table}]({'../' * up}tables/{layer}/{name.split('.', 1)[1]}.md)"


# -- pages --------------------------------------------------------------------

WRITES = {
    "parse_log_messages": LOG_MESSAGES,
    "parse_fix_messages_raw": FIX_MESSAGES_RAW,
    "parse_fix_messages_refined": FIX_MESSAGES,
    "parse_books": BOOKS,
    "parse_orders": ORDERS,
    "parse_quotes": QUOTES,
    "parse_executions": EXECUTIONS,
}


def index_page(landed: dict[str, Landed], stored: dict[str, list[dict[str, Any]]]) -> str:
    answered = table(
        ["task", "writes", "read:", "written:", "skipped:"],
        [
            [
                f"[`{task}`](../tasks/{task_page(task)})",
                f"`{WRITES[task]}`",
                str(held.read),
                str(held.written),
                str(held.skipped),
            ]
            for task, held in landed.items()
        ],
    )
    held = table(
        ["table", "rows:", "sample"],
        [
            [
                table_link(name, 1),
                str(len(stored[name])),
                f"[rows]({name.split('.')[0]}/{name.split('.')[2]}.md)",
            ]
            for name in WRITES.values()
        ],
    )
    lower, upper = WINDOW
    reproduce = [
        "```python",
        "import tempfile",
        "from pathlib import Path",
        "",
        "from rekep import Storages",
        "from rekep.pipeline import (",
        "    FLATTENERS,",
        "    parse_books,",
        "    parse_fix_messages_raw,",
        "    parse_fix_messages_refined,",
        "    parse_log_messages,",
        ")",
        "from rekep.times import window_of",
        "",
        "root = Path(tempfile.mkdtemp())",
        "storages = Storages.from_dict(",
        "    {",
        "        layer: {",
        '            "name": layer,',
        '            "properties": {',
        '                "type": "sql",',
        '                "uri": f"sqlite:///{root / layer}.db",',
        '                "warehouse": str(root / layer),',
        "            },",
        "        }",
        '        for layer in ("bronze", "silver", "gold")',
        "    }",
        ")",
        f'window = window_of("{lower:%Y-%m-%dT%H:%M:%SZ}", "{upper:%Y-%m-%dT%H:%M:%SZ}")',
        "with storages:",
        f'    log = parse_log_messages("{CAPTURE}", storages, window)',
        "    raw = parse_fix_messages_raw(storages, window)",
        "    refined = parse_fix_messages_refined(storages, window)",
        "    books = parse_books(storages, window)",
        "    events = {",
        "        kind: task(storages, window, snapshot_id=books.snapshot_id)",
        "        for kind, task in FLATTENERS.items()",
        "    }",
        "",
        *[
            f"assert ({name}.read, {name}.written, {name}.skipped) == "
            f"({held.read}, {held.written}, {held.skipped})"
            for name, held in (
                ("log", landed["parse_log_messages"]),
                ("raw", landed["parse_fix_messages_raw"]),
                ("refined", landed["parse_fix_messages_refined"]),
                ("books", landed["parse_books"]),
            )
        ],
        "assert {kind: landed.written for kind, landed in events.items()} == {",
        *[f'    "{kind}": {landed[task.__name__].written},' for kind, task in FLATTENERS.items()],
        "}",
        "```",
    ]
    return page(
        "Data samples",
        [
            "Every page in this section is a real landing: `tools/samples_dump.py` runs",
            "every task over the shipped capture, `data/capture/ulbridge.log`, into three",
            f"temporary local catalogs over one window, {window_text()}, and writes what",
            "they landed. Nothing here is written by hand, so a page is what a run from",
            "the repository root lands today.",
        ],
        [
            "## The window",
            "",
            "It opens at midnight, so the trade of 01:03 is in it, and closes at 16:30:",
            "after 12:46, the day's order flow, after 14:52, its one trade report, and",
            "after 16:25, where the walk expires the day's open order. The bridge prints",
            "a Central European summer clock, two hours ahead of the UTC its FIX frames",
            f"state, and every line is read in `{TIMEZONE}`, the zone `parse_log_messages`",
            "reads unless told another, so a line lands in the hour of the message it",
            "carries: bronze `log_messages` and silver both hold hour 12.",
            "[DAGs](../dags/index.md#late-events) says what a window must hold, and what",
            "a capture read in another zone lands.",
        ],
        ["## What each task answered", "", *answered],
        ["## What each table holds", "", *held],
        [
            "## Reproduce it",
            "",
            "From the repository root, into a scratch folder:",
            "",
            *reproduce,
        ],
    )


def task_page(task: str) -> str:
    if task in ("parse_orders", "parse_quotes", "parse_executions"):
        return "parse-orders-quotes-executions.md"
    return task.replace("_", "-") + ".md"


def log_page(lines: list[dict[str, Any]]) -> str:
    bridge = zoneinfo.ZoneInfo(TIMEZONE)
    hours = collections.Counter(
        (line["currunix"].strftime("%H:00"), line["currunix"].astimezone(bridge).strftime("%H:00"))
        for line in lines
    )
    first = sorted(lines, key=lambda line: line["seqnum"])[:FIRST_LINES]
    shared = first[0]
    return page(
        "bronze.record_keeping.log_messages",
        [
            f"{len(lines)} lines of the capture fall in {window_text()}; each is one row.",
            f"Columns: {table_link(LOG_MESSAGES, 2)}.",
        ],
        [
            "## Lines per hour",
            "",
            "A line is dated by its row header, whose clock states no offset and is read",
            f"in the zone the bridge prints in, `{TIMEZONE}`, so the table is laid out by",
            "the UTC hour each line was printed at: the hour of the messages the lines",
            "carry, two hours before the one the bridge's own clock spells.",
            "",
            *table(
                ["hour of `currunix`", "on the bridge's clock", "lines:"],
                [
                    [f"`{hour}`", f"`{printed}`", str(count)]
                    for (hour, printed), count in sorted(hours.items())
                ],
            ),
        ],
        [
            f"## The first {FIRST_LINES} lines",
            "",
            "`seqnum` is the row number in the object the line was read from, and `body`",
            "is the line past its row header; the other columns are the header's captures.",
            "",
            *table(
                [
                    "seqnum:",
                    "currunix",
                    "curruuid",
                    "msgpluginid",
                    "loglevel",
                    "msgseqnum:",
                    "body",
                ],
                [
                    [
                        str(line["seqnum"]),
                        instant(line["currunix"]),
                        short(line["curruuid"]),
                        code(line["msgpluginid"]),
                        code(line["loglevel"]),
                        number(line["msgseqnum"]),
                        excerpt(line["body"]),
                    ]
                    for line in first
                ],
            ),
        ],
        [
            "## What every line states",
            "",
            "A line is an event the read settled, so it carries the event columns every",
            "table opens with; a line is not a lifecycle, so most of them are empty.",
            "",
            *table(
                ["column", "on every line"],
                [
                    ["`crosscode`", code(shared["crosscode"])],
                    ["`state`", state(shared["state"])],
                    [
                        "`creaunix`",
                        "the earliest instant the read has dated a line of the capture by",
                    ],
                    ["`prevunix`", "the instant the read dated the line before by"],
                    ["`recdunix`, `exprunix`, `snapunix`", "empty"],
                    ["`prevuuid`, `srcuuids`", "empty"],
                ],
            ),
        ],
    )


def raw_page(
    lines: list[dict[str, Any]],
    raw: list[dict[str, Any]],
    answered: list[dict[str, Any]],
) -> str:
    by_line = {identity(line["curruuid"]): line for line in lines}
    execution = [row for row in raw if row["execid"] == EXECID]
    if not execution:
        raise ValueError(f"the capture carries no bronze row of execution {EXECID}")
    # What the parse answered off each line: an execution split out of a
    # report names the report beside the line.
    frames: dict[int, list[uuid.UUID | None]] = collections.defaultdict(list)
    for message in answered:
        for source in message["srcuuids"]:
            if identity(source) in by_line:
                frames[by_line[identity(source)]["seqnum"]].append(identity(message["curruuid"]))
    held = {identity(row["curruuid"]) for row in execution}
    carrying = [seqnum for seqnum, messages in frames.items() if held & set(messages)]
    first, last = min(carrying), max(carrying)
    around = [line for line in sorted(lines, key=lambda line: line["seqnum"])]
    around = [line for line in around if first <= line["seqnum"] <= last]
    walked = []
    for line in around:
        messages = frames.get(line["seqnum"], [])
        landed = "; ".join(f"row {short(message)}" for message in messages) or "no frame"
        walked.append(
            [
                str(line["seqnum"]),
                code(line["msgpluginid"]),
                excerpt(line["body"], 48),
                landed,
            ]
        )
    counts = collections.Counter((row["msgtype"], row["state"]) for row in raw)
    return page(
        "bronze.record_keeping.fix_messages",
        [
            f"{len(raw)} rows: one per FIX message the parse read off the stored lines of",
            f"{window_text()}, keyed on the message's identity. Nothing has walked, so",
            "`prevuuid` is empty on every row; `seqnum` is the message's place in its",
            "run -- the messages the parse handed over at one instant, one after",
            "another -- null at place zero.",
            f"Columns: {table_link(FIX_MESSAGES_RAW, 2)}.",
        ],
        [
            "## One execution, logged at every hop",
            "",
            f"Execution `{EXECID}` is the last fill of order `{CHAINS[0]}`. The bridge",
            f"logged it on lines {first} to {last}, once per plugin it passed, and wrote",
            "prose between.",
            "The parse answers a message per frame a line carries, and a report of a fill",
            "answers the execution it splits off beside it. Every copy is a row of its",
            "own: its place in its run reaches its identity, so bronze keeps each hop's",
            "copy and the walk folds them into one event.",
            "",
            *table(["line:", "msgpluginid", "body", "the parse"], walked),
        ],
        [
            "## Its rows",
            "",
            "A message stating its own `SendingTime` is dated by the transaction clock",
            "standing within `official_time_delay_ms` of it, here its `TransactTime`; one",
            "stating none is measured against the line it was read off, which, read in",
            "the bridge's zone, stands within that delay of its `TransactTime`, so it is",
            "dated by that clock too, at the precision its frame spells. Each row names",
            "the one line it was parsed from, and the execution a report splits off",
            "names that report beside it, `FILLED`: one fill, complete in itself,",
            "whatever the report's own state. The report itself is filed under its",
            "order's category, `ORDR`.",
            "",
            *table(
                [
                    "currunix",
                    "seqnum:",
                    "msgcat",
                    "curruuid",
                    "state",
                    "sendingtime",
                    "transacttime",
                    "line:",
                ],
                [
                    [
                        instant(row["currunix"]),
                        number(row["seqnum"]),
                        marketdatakind(row["msgcat"]),
                        short(row["curruuid"]),
                        state(row["state"]),
                        instant(row["sendingtime"]),
                        instant(row["transacttime"]),
                        str(line_of(by_line, row)["seqnum"]),
                    ]
                    for row in execution
                ],
            ),
        ],
        [
            "## Every row, by message type and state",
            "",
            *table(
                ["msgtype", "state", "rows:"],
                [
                    [code(msgtype), state(held), str(count)]
                    for (msgtype, held), count in sorted(
                        counts.items(), key=lambda kv: (str(kv[0][0]), kv[0][1] or 0)
                    )
                ],
            ),
        ],
    )


def refined_page(lines: list[dict[str, Any]], refined: list[dict[str, Any]]) -> str:
    by_line = {identity(line["curruuid"]): line for line in lines}
    events = [row for row in refined if row["snapunix"] is None]
    views = [row for row in refined if row["snapunix"] is not None]

    def restated(view: dict[str, Any]) -> dict[str, Any]:
        """The live event a view restates: its chain's step, as it stood."""
        (event,) = [
            row
            for row in events
            if row["crossuuid"] == view["crossuuid"]
            and row["seqnum"] == view["seqnum"]
            and row["prevuuid"] == view["prevuuid"]
            and row["state"] == view["state"]
        ]
        return event

    def named(row: dict[str, Any]) -> list[int]:
        return sorted(
            by_line[identity(source)]["seqnum"]
            for source in row["srcuuids"] or []
            if identity(source) in by_line
        )

    chains = []
    for chain in CHAINS:
        rows = walked([row for row in events if row["crosscode"] == chain])
        if not rows:
            raise ValueError(f"the walk answers no chain {chain}")
        chains += [
            f"### `{chain}`",
            "",
            *table(
                [
                    "seqnum:",
                    "currunix",
                    "state",
                    "curruuid",
                    "prevuuid",
                    "prevunix",
                    "lines",
                ],
                [
                    [
                        number(row["seqnum"]),
                        instant(row["currunix"]),
                        state(row["state"]),
                        short(row["curruuid"]),
                        short(row["prevuuid"]),
                        instant(row["prevunix"]),
                        ", ".join(map(str, named(row))) or "none",
                    ]
                    for row in rows
                ],
            ),
            "",
        ]
    (event,) = [
        row
        for row in events
        if row["execid"] == EXECID and row["msgcat"] == int(MarketDataKind.EXEC)
    ]
    sources = [
        by_line[identity(source)] for source in event["srcuuids"] if identity(source) in by_line
    ]
    states = collections.Counter(row["state"] for row in refined)
    return page(
        "silver.record_keeping.fix_messages",
        [
            f"{len(refined)} rows: the bronze messages of {window_text()} walked into",
            f"the {len(events)} events they are, the copies of each folded into one row",
            "placed in its chain and naming the lines its session event was logged on,",
            f"and {len(views)} hourly views of the chains alive.",
            f"Columns: {table_link(FIX_MESSAGES, 2)}.",
        ],
        [
            "## Two chains",
            "",
            "`crosscode` is the business identifier a chain shares, `seqnum` an event's",
            "place among the events of its instant and `prevuuid` the event it follows.",
            "An expiry is an event the walk generates at the deadline the chain stated:",
            "no line recorded it.",
            "",
            f"The fills of `{CHAINS[0]}` are dated at one instant, and the walk takes",
            "the rows of one instant in the order the table holds them -- by the place",
            "each copy took in its run of the parse, then by `curruuid` -- rather than",
            f"in the order they happened: here the last fill, execution `{EXECID}`, is",
            "taken first and stands as a head of its own, while the first two chain on",
            "to the bridge's later `FILLED` restatement, so [the books](books.md) hold",
            "the order resting between the two.",
            "",
            *chains,
        ],
        [
            "## Every chain alive on the hour",
            "",
            f"{len(views)} rows are views: at every whole hour the walk crosses, each chain",
            "still alive is restated as it stands, dated at the tick -- `currunix` and",
            "`snapunix` both -- under the identity that instant derives, which opens",
            "with the tick's millisecond. It repeats the event's place, state and",
            "lines and moves no chain on.",
            "",
            *table(
                ["snapunix", "crosscode", "state", "curruuid", "restates"],
                [
                    [
                        instant(row["snapunix"]),
                        code(row["crosscode"]),
                        state(row["state"]),
                        f"`{identity(row['curruuid'])}`",
                        f"`{identity(restated(row)['curruuid'])}`",
                    ]
                    for row in views
                ],
            ),
        ],
        [
            "## One event, every line it was logged on",
            "",
            f"Execution `{EXECID}` is one event, {short(event['curruuid'])}, dated",
            f"{instant(event['currunix'])} by its `TransactTime` and recorded first at",
            f"{instant(event['recdunix'])}; `srcuuids` names the {len(sources)} lines",
            "whose frames the walk merged into it, each joining to a",
            "`bronze.record_keeping.log_messages.curruuid`, beside the reports it was",
            "split out of:",
            "",
            *table(
                ["line:", "curruuid", "msgpluginid", "body"],
                [
                    [
                        str(line["seqnum"]),
                        short(line["curruuid"]),
                        code(line["msgpluginid"]),
                        excerpt(line["body"], 56),
                    ]
                    for line in sorted(sources, key=lambda line: line["seqnum"])
                ],
            ),
        ],
        [
            "## States",
            "",
            "`state` is stored as the `int32` code of a lifecycle-sorted enum;",
            "[States](../../tables/states.md) lists every member.",
            "",
            *table(
                ["state", "rows:"],
                [[state(held), str(count)] for held, count in sorted(states.items())],
            ),
        ],
    )


def walked(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One chain's rows in the order it was walked: each head, then what follows it."""
    following = collections.defaultdict(list)
    for row in rows:
        following[identity(row["prevuuid"])].append(row)
    ordered: list[dict[str, Any]] = []
    stack = sorted(following[None], key=lambda row: (row["currunix"], row["curruuid"]))[::-1]
    while stack:
        row = stack.pop()
        ordered.append(row)
        after = following[identity(row["curruuid"])]
        stack += sorted(after, key=lambda row: (row["currunix"], row["curruuid"]))[::-1]
    return ordered


def books_page(lines: list[dict[str, Any]], books: list[dict[str, Any]]) -> str:
    def count(row: dict[str, Any], part: str) -> str:
        return str(len(row[part] or []))

    deep = [row for row in books if row["alive"]]
    detail: list[str] = []
    if deep:
        book = deep[0]
        levels = [(column, level) for column in LIMITS for level in book[column] or []]
        detail = [
            "## One book with depth",
            "",
            f"The first book standing any depth is {code(book['crosscode'])} at",
            f"{instant(book['currunix'])}. `alive` is what stands on either side, `deltas`",
            "what changed since the book before, `executions` what traded, and",
            "`bidlimits` and `asklimits` the price levels `alive` aggregates to, best",
            "first; `bidpx` and `askpx` are the best tradable level of each.",
            "",
            *table(
                [
                    "marketdatakind",
                    "side",
                    "crosscode",
                    "state",
                    "price:",
                    "quantity:",
                    "curruuid",
                ],
                [
                    [
                        marketdatakind(entry["marketdatakind"]),
                        side(entry["side"]),
                        code(entry["crosscode"]),
                        state(entry["state"]),
                        number(entry["price"]),
                        number(entry["quantity"]),
                        short(entry["curruuid"]),
                    ]
                    for entry in book["alive"]
                ],
            ),
            "",
            *table(
                ["levels", "price:", "quantity:", "orders:", "tradable"],
                [
                    [
                        f"`{column}`",
                        number(level["price"]),
                        number(level["quantity"]),
                        str(len(level["uuids"] or [])),
                        code(str(level["tradable"]).lower()),
                    ]
                    for column, level in levels
                ],
            ),
        ]
    return page(
        "silver.record_keeping.books",
        [
            f"{len(books)} rows: one per book the fold answered over the silver events of",
            f"{window_text()}, read from the hour before its start,",
            "and every book again on each whole hour between the fold's first and last",
            "operation, `snapunix` set and no event restated: the views silver holds",
            "there are the book's membership, never a delta. A book is one `MIC:CFI`",
            "category per instant, so `crosscode` is the category and no book states a",
            f"ticker. Columns: {table_link(BOOKS, 2)}.",
        ],
        [
            "## Every book",
            "",
            *table(
                [
                    "currunix",
                    "crosscode",
                    "curruuid",
                    "alive:",
                    "deltas:",
                    "executions:",
                    "bidlimits:",
                    "asklimits:",
                    "bidpx:",
                    "askpx:",
                ],
                [
                    [
                        instant(row["currunix"]),
                        code(row["crosscode"]),
                        short(row["curruuid"]),
                        count(row, "alive"),
                        count(row, "deltas"),
                        count(row, "executions"),
                        count(row, "bidlimits"),
                        count(row, "asklimits"),
                        number(row["bidpx"]),
                        number(row["askpx"]),
                    ]
                    for row in books
                ],
            ),
        ],
        detail,
    )


#: A book's price levels, as the book row names them: its bids, then its asks.
LIMITS = ("bidlimits", "asklimits")


def events_page(
    table_name: str,
    lines: list[dict[str, Any]],
    events: list[dict[str, Any]],
    told: str,
) -> str:
    by_line = {identity(line["curruuid"]): line for line in lines}
    kind = next(kind for kind, name in EVENTS.items() if name == table_name)
    if events:
        rows = table(
            [
                "currunix",
                "marketdatakind",
                "isincode",
                "crosscode",
                "side",
                "state",
                "price:",
                "quantity:",
                "lastpx:",
                "lastqty:",
                "lines",
            ],
            [
                [
                    instant(row["currunix"]),
                    marketdatakind(row["marketdatakind"]),
                    code(row["isincode"]),
                    code(row["crosscode"]),
                    side(row["side"]),
                    state(row["state"]),
                    number(row["price"]),
                    number(row["quantity"]),
                    number(row["lastpx"]),
                    number(row["lastqty"]),
                    ", ".join(
                        str(seqnum)
                        for seqnum in sorted(
                            by_line[identity(source)]["seqnum"]
                            for source in row["srcuuids"] or []
                            if identity(source) in by_line
                        )
                    ),
                ]
                for row in events
            ],
        )
    else:
        rows = [f"The window lands no {kind[:-1]} event: the capture carries none in it."]
    return page(
        table_name,
        [
            f"{rows_of(len(events))}: {told} of the one book snapshot `parse_books` committed",
            f"over {window_text()}. Columns: {table_link(table_name, 2)}.",
        ],
        ["## Every row", "", *rows],
    )


def published() -> Iterator[tuple[pathlib.Path, str]]:
    """Every page this writes, with the text it holds."""
    with landing() as storages:
        landed = run(storages)
        stored = {table: read(storages, table) for table in WRITES.values()}
        answered = parsed(storages)
    lines = stored[LOG_MESSAGES]
    yield PAGES / "index.md", index_page(landed, stored)
    yield PAGES / "bronze" / "log_messages.md", log_page(lines)
    yield (
        PAGES / "bronze" / "fix_messages.md",
        raw_page(lines, stored[FIX_MESSAGES_RAW], answered),
    )
    yield (
        PAGES / "silver" / "fix_messages.md",
        refined_page(lines, stored[FIX_MESSAGES]),
    )
    yield PAGES / "silver" / "books.md", books_page(lines, stored[BOOKS])
    told = {
        ORDERS: "the order deltas",
        QUOTES: "the quote deltas",
        EXECUTIONS: "the execution leaves",
    }
    for table_name in (ORDERS, QUOTES, EXECUTIONS):
        yield (
            PAGES / "silver" / f"{table_name.rsplit('.', 1)[1]}.md",
            events_page(table_name, lines, stored[table_name], told[table_name]),
        )


def main() -> None:
    for path, text in published():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        print(path.relative_to(ROOT))


if __name__ == "__main__":
    main()
