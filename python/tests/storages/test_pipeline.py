"""The tasks over the shipped capture, in production order across the three layers.

`landing` lands the capture once: its whole day into bronze `log_messages`,
then every later task over `EARLY` and over the main window `WINDOW`. Pinned
here: what each task answered, which data files it opened, what the walk
settled in silver, that a rerun leaves every table holding what it held, and
that `commit_row_size` bounds a commit and never what lands. The samples
print for a reader running with `-s`.
"""

from __future__ import annotations

import collections
import dataclasses
import datetime
import json
import uuid
from pathlib import Path
from typing import Annotated, Any

import pyarrow
import pytest
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, IsNull, LessThan, Or
from pyiceberg.expressions.visitors import bind
from pyiceberg.io.pyarrow import expression_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, TimestamptzType

from rekep import MarketDataKind, Side, State, Storages, scalar
from rekep.arrow_reader import OwnedRecordBatchReader
from rekep.fields import partition_key
from rekep.fix import (
    EVENT_CLOCK,
    PARSE_COLUMNS,
    UNDATED,
    FixCodec,
    fix_parse_arrow_reader,
    fix_window_filter,
)
from rekep.iceberg import window_filter
from rekep.market import market_window_filter
from rekep.pipeline import (
    BOOKS,
    COMMIT_ROW_SIZE,
    EVENTS,
    FIX_MESSAGES,
    FIX_MESSAGES_RAW,
    FLATTENED,
    FLATTENERS,
    HISTORY,
    LOG_MESSAGES,
    Landed,
    _Count,
    parse_log_messages,
)
from rekep.text import log_message_field
from rekep.times import EPOCH, within

from .conftest import (
    CAPTURE,
    DAY,
    EARLY,
    END,
    START,
    WINDOW,
    Landing,
    graph,
    hours,
    planned,
    read,
    rows,
    snapshots,
)

pytestmark = pytest.mark.integration

UTC = datetime.timezone.utc

#: What `parse_log_messages` answers over the capture's day: every line, once.
LOG = Landed(read=144, written=144)

#: What every later task answers over `EARLY`: the sixteen lines of 01:00
#: carry one fill report logged at three hops -- its receipt and two
#: restatements, each dated 01:03:17 by the transaction clock it states --
#: and each copy splits off the execution it reports. Bronze keeps every
#: copy, each placed apart at its instant. The walk merges the observations
#: into the report's event and the execution's, folded into one book holding
#: one order delta and one execution.
EARLY_LANDED = {
    "parse_fix_messages_raw": Landed(read=16, written=6),
    "parse_fix_messages_refined": Landed(read=6, written=2),
    "parse_books": Landed(read=2, written=1),
    "parse_orders": Landed(read=1, written=1),
    "parse_quotes": Landed(read=1, written=0),
    "parse_executions": Landed(read=1, written=1),
}

#: What every later task answers over `WINDOW`. Its 113 lines are the 112 of
#: 12:46, carrying 65 frames, and the trade report's of 14:52; every fill
#: report splits off the execution it reports, and the trade report one per
#: side it states -- its one side states no `Side(54)`, so an `UNKN` one --
#: so the parse answers 120 messages. A restatement states no `SendingTime`,
#: and its line stands within `official_time_delay_ms` of the `TransactTime`
#: it states, so it is dated by that clock, in the hour of the message it
#: restates; every copy is placed apart at its instant, so bronze keeps all
#: 120. The walk reads those 120 rows -- the hour before `START` holds none --
#: and merges the observations of one event into 19 rows, the trade report
#: and its execution among them -- the fill report of 12:46:39.743 the bridge
#: forwarded again under its own `MsgSeqNum`, and the execution that copy
#: splits off, folded into the two they repeat -- plus the expiry it emits at
#: 16:25, and restates the three chains still alive after 12:46 -- the logon,
#: the new order and the acknowledged order that expires -- at 13:00, 14:00,
#: 15:00 and 16:00: 20 events and 12 views, each a row of its own, so the key
#: folds nothing. The books fold those 32 into 6 books of events and the two
#: categories' books on each of the four hours, 14; their deltas and
#: executions are seven orders -- the new order and six fill reports -- no
#: quote and seven executions, one split out of each fill report and the
#: trade's, the hourly books adding none.
LANDED = {
    "parse_fix_messages_raw": Landed(read=113, written=120),
    "parse_fix_messages_refined": Landed(read=120, written=32),
    "parse_books": Landed(read=32, written=14),
    "parse_orders": Landed(read=14, written=7),
    "parse_quotes": Landed(read=14, written=0),
    "parse_executions": Landed(read=14, written=7),
}

#: Rows each table holds after both windows. The gold catalog holds no table.
STORED = {
    LOG_MESSAGES: 144,
    FIX_MESSAGES_RAW: 126,
    FIX_MESSAGES: 34,
    BOOKS: 15,
    EVENTS["orders"]: 8,
    EVENTS["quotes"]: 0,
    EVENTS["executions"]: 8,
}

#: The table each task scans, the hours of it that scan plans, and every hour
#: the table holds -- `EARLY`'s among them. The window's hours are 12 to 16,
#: and the walk's scan reaches `HISTORY` before them, to 11. A line is dated
#: in the hour of the message it carries, so `log_messages` and bronze
#: `fix_messages` hold the same hours in the window: 12, the events of 12:46,
#: and 14, the trade report of 14:52. Silver and the books hold every hour
#: from 12 to 16, the hourly views and books among them.
READS = {
    "parse_fix_messages_raw": (LOG_MESSAGES, ["12", "14"], ["01", "12", "14", "21"]),
    "parse_fix_messages_refined": (FIX_MESSAGES_RAW, ["12", "14"], ["01", "12", "14"]),
    "parse_books": (
        FIX_MESSAGES,
        ["12", "13", "14", "15", "16"],
        ["01", "12", "13", "14", "15", "16"],
    ),
    **{
        task: (BOOKS, ["12", "13", "14", "15", "16"], ["01", "12", "13", "14", "15", "16"])
        for task in ("parse_orders", "parse_quotes", "parse_executions")
    },
}

#: The tasks keyed on `curruuid`, by the table each appends to: a run appends
#: only the keys its table does not hold.
KEYED = {
    "parse_log_messages": LOG_MESSAGES,
    "parse_fix_messages_raw": FIX_MESSAGES_RAW,
    "parse_fix_messages_refined": FIX_MESSAGES,
}

#: Fewer rows than the log over `DAY`, the parse or the walk over `WINDOW`
#: appends, so each commits several times where `COMMIT_ROW_SIZE` commits once.
SMALL_COMMIT_ROW_SIZE = 7

#: The identifiers `crosscode` takes the first non-empty one of, in order.
CROSS_IDENTIFIERS = ("orderid", "clordid", "origclordid", "quoteid", "quotereqid", "mdreqid")

#: The status fields a message's state is read off, first stated first, by
#: the columns the row states them in; a message stating none takes what its
#: message type asks for.
STATUS_FIELDS = ((39, "ordstatus"), (150, "exectype"), (297, "quotestatus"))


def counted(landed: dict[str, Landed]) -> dict[str, Landed]:
    """What each task read, wrote and skipped, without the snapshot it pinned."""
    return {name: dataclasses.replace(held, snapshot_id=None) for name, held in landed.items()}


def replayed(landed: Landed) -> Landed:
    """What a keyed task answers over a window it already landed: the rows it
    read, every row it answered a key its table holds, and none written."""
    return dataclasses.replace(landed, written=0, skipped=landed.written + landed.skipped)


def rerun(landed: dict[str, Landed]) -> dict[str, Landed]:
    """What every task answers over a window it already landed: a keyed task
    appends nothing, and the books and the event tables write what they
    wrote, since they replace their window."""
    return {task: replayed(held) if task in KEYED else held for task, held in landed.items()}


def added(storages: Storages, table: str) -> list[tuple[str, int]]:
    """Each snapshot of one table, oldest first: its operation and the rows it added."""
    layer, _, name = table.partition(".")
    held = storages.catalog(layer).load_table(name).metadata.snapshots
    return [
        (snapshot.summary.operation.value, int(snapshot.summary.get("added-records") or 0))
        for snapshot in held
    ]


def short(identity: bytes | None) -> str:
    """An identity as its last six hex digits: a UUIDv7 opens with its millisecond."""
    return "-" if identity is None else uuid.UUID(bytes=identity).hex[-6:]


def in_window(table: pyarrow.Table, window: tuple[datetime.datetime, datetime.datetime]) -> list:
    """The rows of `table` whose `currunix` a window covers, as dictionaries."""
    return table.filter(within(table.column(EVENT_CLOCK), window)).to_pylist()


def clock(instant: datetime.datetime | None) -> str:
    """An instant of the capture's day as its clock, to the microsecond."""
    return "-" if instant is None else f"{instant:%H:%M:%S.%f}"


def events(table: pyarrow.Table) -> list[dict[str, Any]]:
    """The rows of a silver table that are events, not a grid view of one."""
    return [row for row in table.to_pylist() if row["snapunix"] is None]


def settled(state: State) -> bool:
    """Whether a state ends its chain: done, cancelled or failed."""
    return state.is_done() or state.is_cancelled() or state.is_failed()


def stated(row: dict[str, Any]) -> State:
    """The state a parsed message implies about itself, by the rule the parse reads."""
    for tag, column in STATUS_FIELDS:
        if row[column] is not None:
            found = State.from_fix_status(tag, row[column])
            if found is not None:
                return found
    return State.from_fix_msgtype(row["msgtype"] or "") or State.UNKNOWN


#: The kinds whose cross code is stored under the side the message states.
SIDED = {MarketDataKind.ORDR, MarketDataKind.QUOT, MarketDataKind.EXEC}


def cross_code(row: dict[str, Any], split: bool) -> str | None:
    """The identifier `crosscode` takes, per the rule it is read by: an
    execution split out of a fill report chains on its `ExecID`, one split
    out of a trade on its side of the trade -- `{len}:{own}|{tag}:{len}:{id}`,
    `own` the side's first `OrderID`, `ClOrdID` or `OrigClOrdID` and `id` its
    first `SideExecID`, `SideTradeID`, `SideTradeReportID`, `OrderID` or
    `ClOrdID` -- and an order's, a quote's or an execution's is prefixed with
    the four-letter code of the side it states; any other kind, and a message
    stating no side, keeps the bare code."""
    if split and row["msgtype"] == "AE":
        side = trade_side(row)
        own = next(side[tag] for tag in (37, 11, 41) if tag in side)
        tag = next(tag for tag in (1427, 1506, 1005, 37, 11) if tag in side)
        code = f"{len(own)}:{own}|{tag}:{len(side[tag])}:{side[tag]}"
    elif split:
        code = row["execid"]
    else:
        code = next((row[name] for name in CROSS_IDENTIFIERS if row[name]), None)
    side = Side(row["side"] or Side.UNKN)
    if code is None or side is Side.UNKN or row["msgcat"] not in SIDED:
        return code
    return f"{side.name}:{code}"


def trade_side(row: dict[str, Any]) -> dict[int, str]:
    """The one side of its trade an execution a trade split off holds, by
    tag: its `NoSides(552)` group, which the row keeps in `fixentries`."""
    (group,) = [held for key, held in row["fixentries"] if key.startswith("552:")]
    (side,) = json.loads(group)
    return {int(key.partition(":")[0]): held for key, held in side.items()}


def split_off(rows: list[dict[str, Any]], reports: set[bytes]) -> list[bool]:
    """Whether each row is an execution split out of a report: it names the
    report among its sources, beside the lines."""
    return [bool(reports.intersection(row["srcuuids"])) for row in rows]


def line_of(row: dict[str, Any], reports: set[bytes]) -> bytes:
    """The one line a parsed message names, whatever report it names beside it."""
    (line,) = [source for source in row["srcuuids"] if source not in reports]
    return line


# -- what each task answered ---------------------------------------------------


def test_every_task_lands_its_window_and_the_gold_catalog_stays_empty(landing: Landing) -> None:
    print(f"\n{'parse_log_messages':<28} DAY    {landing.log}")
    for label, landed in (("EARLY", landing.early), ("WINDOW", landing.landed)):
        for task, held in landed.items():
            print(f"{task:<28} {label:<6} {held}")
    stored = rows(landing.storages)
    for table, count in stored.items():
        print(f"stored {table:<36} {count} rows")

    assert landing.log == LOG
    assert counted(landing.early) == EARLY_LANDED
    assert counted(landing.landed) == LANDED
    for landed in (landing.early, landing.landed):
        books = landed["parse_books"].snapshot_id
        assert books is not None and books > 0
        for task in FLATTENERS.values():
            assert landed[task.__name__].snapshot_id == books, "every kind reads the one commit"
    assert stored == STORED
    assert landing.storages.gold.tables() == []
    assert landing.storages.gold.namespaces() == []


def test_each_task_opens_only_the_hours_its_window_covers(landing: Landing) -> None:
    """The window is a predicate the scan hands Iceberg, so the plan holds only
    the hour partitions it covers -- and those are all the task opened."""
    storages = landing.storages
    books = landing.landed["parse_books"].snapshot_id
    predicates = {
        "parse_fix_messages_raw": window_filter(EVENT_CLOCK, WINDOW),
        "parse_fix_messages_refined": fix_window_filter((START - HISTORY, END)),
        "parse_books": market_window_filter((START - HISTORY, END)),
        **{task.__name__: market_window_filter(WINDOW) for task in FLATTENERS.values()},
    }
    covered = {f"{hour:02d}" for hour in range(START.hour - 1, END.hour + 1)}
    print()
    for task, (table, expected, held) in READS.items():
        snapshot = books if table == BOOKS else None
        every = planned(storages, table, snapshot_id=snapshot)
        scanned = planned(storages, table, predicates[task], snapshot_id=snapshot)
        opened = hours(landing.opened[task], table)
        print(
            f"{task:<28} scans {table:<36} plans hours {hours(scanned, table)} "
            f"of {hours(every, table)} ({len(scanned)} of {len(every)} files), opened {opened}"
        )
        assert hours(every, table) == held
        assert hours(scanned, table) == expected
        assert set(expected) <= covered, "only the window's hours, and the walk's hour before it"
        assert set(expected) < set(held), "the table holds hours the window leaves unread"
        assert opened == expected, "a task opens what its scan planned and nothing else"
        assert len(scanned) < len(every), "the plan leaves the other hours' files unread"
        assert landing.opened[task] == set(scanned), "the very files the plan names"


def test_a_file_of_the_windows_last_hour_past_its_end_is_left_unplanned(
    landing: Landing,
) -> None:
    """A partition is planned by the bounds of its files, not by its hour: a
    window ending at 14:30 covers hour 14, which holds only the trade
    report's line of 14:52, so its scan plans hour 12 alone and reads the 112
    lines of 12:46."""
    cut = (START, datetime.datetime(2026, 8, 14, 14, 30, tzinfo=UTC))
    predicate = window_filter(EVENT_CLOCK, cut)
    storages = landing.storages
    assert hours(planned(storages, LOG_MESSAGES), LOG_MESSAGES) == ["01", "12", "14", "21"]
    assert hours(planned(storages, LOG_MESSAGES, predicate), LOG_MESSAGES) == ["12"]
    assert read(storages, LOG_MESSAGES, row_filter=predicate).num_rows == 112


# -- what the walk settled -----------------------------------------------------


def test_the_walk_links_each_chain_and_expires_the_order_left_open(landing: Landing) -> None:
    """Every step follows a row this table holds, at or after its instant --
    placed after it where the two share one; a step never moves its chain
    back; creation is carried forward; and the one order still open at its
    expiry gets the row that ends it, at that instant, recorded by no line.
    The hourly views are the walk's restatements and follow no step, so a
    chain is its events."""
    silver = events(landing.table(FIX_MESSAGES))
    held = {row["curruuid"]: row for row in silver}
    followed = collections.defaultdict(list)
    for row in silver:
        if row["prevuuid"] is not None:
            followed[row["prevuuid"]].append(row)

    links = [row for row in silver if row["prevuuid"] is not None]
    assert links, "the walk links steps into chains"
    for row in links:
        before = held[row["prevuuid"]]
        assert row["prevunix"] == before[EVENT_CLOCK] <= row[EVENT_CLOCK]
        if row[EVENT_CLOCK] == before[EVENT_CLOCK]:
            assert (row["seqnum"] or 0) > (before["seqnum"] or 0), "a later place there"
        assert (row["crossuuid"], row["crosscode"]) == (before["crossuuid"], before["crosscode"])
        assert State(row["state"]).rank >= State(before["state"]).rank, "the furthest state wins"
        assert row["creaunix"] <= before["creaunix"], "the earliest creation is carried forward"
    heads = [row for row in silver if row["prevuuid"] is None]
    assert all(row["prevunix"] is None for row in heads)

    def chain(head: dict[str, Any]) -> list[dict[str, Any]]:
        steps = [head]
        while followed[steps[-1]["curruuid"]]:
            (step,) = followed[steps[-1]["curruuid"]]
            steps.append(step)
        return steps

    chains = sorted((chain(head) for head in heads), key=len, reverse=True)
    for steps in chains[:2]:
        print(f"\nchain {steps[0]['crosscode']}:")
        for step in steps:
            print(
                f"  seqnum={step['seqnum']} {clock(step[EVENT_CLOCK])} "
                f"{State(step['state']).name:<16} msgtype={step['msgtype']} "
                f"curruuid=..{short(step['curruuid'])} prevuuid=..{short(step['prevuuid'])} "
                f"prevunix={clock(step['prevunix'])} "
                f"lines={len(step['srcuuids'])}"
            )

    expired = [row for row in silver if row["state"] == State.EXPIRED]
    assert len(expired) == 1
    for row in expired:
        order = held[row["prevuuid"]]
        assert (
            row[EVENT_CLOCK]
            == order["exprunix"]
            == datetime.datetime(2026, 8, 14, 16, 25, tzinfo=UTC)
        )
        assert State(order["state"]).is_live()
        assert not followed[row["curruuid"]], "an expiry ends its chain"
        assert row["recdunix"] is None, "no line recorded it: the walk emitted it"
        assert row["srcuuids"] == order["srcuuids"], "it names the lines of the order it ends"
    # And only the order left open expires: every other chain that reached its
    # expiry inside the window had already settled.
    for steps in chains:
        last = steps[-1]
        if last["exprunix"] is not None and START <= last["exprunix"] < END:
            assert settled(State(last["state"])), last["crosscode"]


def test_a_message_logged_at_every_hop_is_one_silver_row(landing: Landing) -> None:
    """Every line of the window that carries a message is accounted for once.

    The parse answers one row per message, and bronze keeps one per key -- an
    identity within its hour. Every copy of a message is placed apart at its
    instant, so each is a key of its own and bronze keeps them all. The walk
    then merges the observations of one event -- the hops that restated it
    under one session event, `msgsesseventid` -- into one silver row naming
    each of their lines, and folds a copy of it another session event
    delivered at its instant into that row without naming the copy's line.
    """
    storages = landing.storages
    lines = {row["curruuid"]: row for row in landing.table(LOG_MESSAGES).to_pylist()}
    bronze = in_window(landing.table(FIX_MESSAGES_RAW), (START - HISTORY, END))
    silver = in_window(landing.table(FIX_MESSAGES), WINDOW)
    # A view names the lines of the event it restates, so only events count.
    recorded = [row for row in silver if row["recdunix"] and row["snapunix"] is None]
    reports = {row["curruuid"] for row in landing.table(FIX_MESSAGES_RAW).to_pylist()}
    # A parsed message names one line, and an execution split out of a
    # report names the report beside it.
    kept = {line_of(row, reports) for row in bronze}

    # The window's lines parsed again, as the raw task parsed them.
    codec = FixCodec.from_env(default_sending_time=UNDATED)
    dataset = storages.dataset(LOG_MESSAGES)
    try:
        scan = dataset.read_arrow_reader(
            log_message_field(),
            row_filter=window_filter(EVENT_CLOCK, WINDOW),
            columns=PARSE_COLUMNS,
        )
        parsed = fix_parse_arrow_reader(codec, scan).read_all()
    finally:
        dataset.close()
    identities = {held.bytes for held in parsed.column("curruuid").to_pylist()}
    keys = collections.defaultdict(list)
    for identity, instant, sources in zip(
        parsed.column("curruuid").to_pylist(),
        parsed.column(EVENT_CLOCK).to_pylist(),
        parsed.column("srcuuids").to_pylist(),
        strict=True,
    ):
        hour = instant.replace(minute=0, second=0, microsecond=0)
        (line,) = [held.bytes for held in sources if held.bytes not in identities]
        keys[(identity.bytes, hour)].append(line)
    raw = landing.landed["parse_fix_messages_raw"]
    assert parsed.num_rows == raw.written + raw.skipped
    assert len(keys) == raw.written
    for sources in keys.values():
        assert len(kept.intersection(sources)) == 1, "bronze keeps one line per key"
    assert sum(len(sources) - 1 for sources in keys.values()) == raw.skipped
    folded = {line for sources in keys.values() for line in sources} - kept

    # A line is named by the one event it reports, and an execution split
    # out of that report names the same lines beside the report.
    split = split_off(recorded, reports)
    named = collections.Counter(
        line
        for row, held in zip(recorded, split, strict=True)
        if not held
        for line in row["srcuuids"]
    )
    assert set(named.values()) == {1}, "a line is named by one event"
    assert set(named) <= kept, "no line bronze folded is named"
    # The one line no event names carries the fill report of 12:46:39.743
    # the bridge forwarded under its own `MsgSeqNum`: another session event
    # than the copies it repeats, at their instant and with their content, so
    # the walk folds it into their event and only its bronze row names it.
    (forwarded,) = kept - set(named)
    for row in bronze:
        if line_of(row, reports) != forwarded or reports.intersection(row["srcuuids"]):
            continue
        repeated = [
            held
            for held in bronze
            if (held["currhashcode"], held[EVENT_CLOCK]) == (row["currhashcode"], row[EVENT_CLOCK])
            and line_of(held, reports) in named
        ]
        assert repeated, "it repeats a message whose lines an event names"
        assert row["msgsesseventid"] not in {held["msgsesseventid"] for held in repeated}
    executed = {
        line
        for row, held in zip(recorded, split, strict=True)
        if held
        for line in row["srcuuids"]
        if line not in reports
    }
    assert executed and executed <= kept

    widest = max(
        (row for row, held in zip(recorded, split, strict=True) if not held),
        key=lambda row: len(row["srcuuids"]),
    )
    hops = sorted((lines[line] for line in widest["srcuuids"]), key=lambda line: line["seqnum"])
    restated = sorted(
        lines[line]["seqnum"]
        for sources in keys.values()
        if kept.intersection(sources) <= set(widest["srcuuids"])
        for line in sources
        if line in folded
    )
    print(
        f"\nraw: {parsed.num_rows} messages off {raw.read} lines, {len(keys)} keys, "
        f"{raw.skipped} folded"
        f"\n{widest['crosscode']} execid={widest['execid']} "
        f"{State(widest['state']).name}: one silver row naming {len(hops)} lines"
    )
    for line in hops:
        print(f"  line {line['seqnum']:>3} {line['msgpluginid']:<36} {line['body'][:60]}")
    print(f"  and folded in bronze: lines {restated}")
    assert len(hops) > 2
    assert len({line["msgpluginid"] for line in hops}) > 2, "logged at several hops"
    observed = [row for row in bronze if line_of(row, reports) in set(widest["srcuuids"])]
    assert {row["execid"] for row in observed} == {widest["execid"]}, "one message, restated"


def test_the_walk_folds_state_creation_and_recording(landing: Landing) -> None:
    """`state` is an `int32` code of the lifecycle-sorted enum: a parse reads it
    off the first status field a message states, else off its type -- a new
    order is `PENDING_NEW` -- and the walk keeps the furthest rank its
    observations reached, saying `UPDATED` of a `NEW` stated over a live new
    one. `creaunix` folds to the earliest creation,
    `recdunix` to the earliest line, and `crosscode` is the first identifier
    stated."""
    lines = {row["curruuid"]: row for row in landing.table(LOG_MESSAGES).to_pylist()}
    bronze = landing.table(FIX_MESSAGES_RAW)
    silver = landing.table(FIX_MESSAGES)
    for table, held in ((FIX_MESSAGES_RAW, bronze), (FIX_MESSAGES, silver)):
        assert held.schema.field("state").type == pyarrow.int32()
        layer, _, name = table.partition(".")
        stored = landing.storages.catalog(layer).load_table(name).schema().find_field("state")
        assert stored.field_type == IntegerType()
        codes = collections.Counter(State(code).name for code in held.column("state").to_pylist())
        print(f"\n{table} states: {dict(sorted(codes.items()))}")

    parsed = bronze.to_pylist()
    reports = {row["curruuid"] for row in parsed}
    observations = collections.defaultdict(list)
    for row, split in zip(parsed, split_off(parsed, reports), strict=True):
        # An execution split out of a report is one fill, complete in itself,
        # whatever state the report it was split out of reached.
        expected = State.FILLED if split else stated(row)
        assert row["state"] == expected, (row["msgtype"], row["ordstatus"], row["exectype"])
        assert row["crosscode"] == cross_code(row, split)
        line = line_of(row, reports)
        assert row["recdunix"] == lines[line][EVENT_CLOCK], "the line's own clock"
        observations[(line, split)].append(row)
    orders = [row for row in parsed if row["msgtype"] == "D"]
    assert orders and {State(row["state"]) for row in orders} == {State.PENDING_NEW}

    walked = silver.to_pylist()
    for row, split in zip(walked, split_off(walked, reports), strict=True):
        assert row["crosscode"] == cross_code(row, split)
        assert row["creaunix"] is not None and row["creaunix"] <= row[EVENT_CLOCK]
        if row["recdunix"] is None:
            continue
        seen = [held for line in row["srcuuids"] for held in observations[(line, split)]]
        reached = max(State(held["state"]).rank for held in seen)
        if row["state"] == State.UPDATED:
            assert reached == State.NEW.rank, "the order stated anew and carrying on"
        else:
            assert State(row["state"]).rank == reached
        assert row["recdunix"] == min(held["recdunix"] for held in seen)
        assert row["creaunix"] <= min(held["creaunix"] for held in seen)
        if row["msgtype"] == "D":
            assert row["state"] == State.PENDING_NEW
            print(
                f"new order {row['crosscode']}: {State(row['state']).name}, created "
                f"{row['creaunix']:%H:%M:%S.%f} off {len(seen)} observations"
            )


def test_no_parsed_message_sits_at_the_pin(landing: Landing) -> None:
    """A message stating no sending clock is dated by the transaction clock
    standing within `official_time_delay_ms` of its line, else by its line,
    so a text read leaves nothing at `UNDATED` for the walk to place by
    transaction time."""
    bronze = landing.table(FIX_MESSAGES_RAW).to_pylist()
    assert not [row for row in bronze if row[EVENT_CLOCK] == UNDATED]
    silver = landing.table(FIX_MESSAGES).column(EVENT_CLOCK).to_pylist()
    assert UNDATED not in silver


# -- a rerun -------------------------------------------------------------------


def test_a_rerun_of_every_task_over_its_window_lands_identical_rows(landing: Landing) -> None:
    """Idempotent: the log over its day and every later task over the window
    answer what they answered and leave every table holding what it held. A
    keyed task finds every key it answers held, so it appends none and
    commits nothing; the books and the event tables replace their window
    with the same rows in one commit each; and `EARLY`'s rows are left as
    they were."""
    storages = landing.storages
    before = {table: read(storages, table) for table in storages.tables()}
    commits = {table: snapshots(storages, table) for table in before}

    log = parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    again = graph(storages, WINDOW)

    assert log == replayed(LOG) == Landed(read=144, written=0, skipped=144)
    assert counted(again) == rerun(LANDED)
    books = again["parse_books"].snapshot_id
    assert books != landing.landed["parse_books"].snapshot_id
    assert all(again[task.__name__].snapshot_id == books for task in FLATTENERS.values())
    for table, held in before.items():
        assert read(storages, table).equals(held), f"{table} landed other rows"
    after = {table: snapshots(storages, table) for table in before}
    print(f"\nsnapshots before the rerun {commits}\nand after it {after}")
    keyed = set(KEYED.values())
    assert after == {
        table: count if table in keyed else count + 1 for table, count in commits.items()
    }, "a keyed table commits nothing, and a market table one replacement"


# -- the commit size -----------------------------------------------------------


def test_the_commit_row_size_bounds_a_commit_and_never_what_lands(
    landing: Landing, storages: Storages
) -> None:
    """`commit_row_size` is how many rows one commit of a task holds, never
    which rows land: the capture through every task at seven rows a commit
    answers what the landing at `COMMIT_ROW_SIZE` answered and stores the
    very rows it stores. A keyed table takes a commit per seven rows it
    appends, each one an append, where the landing took one per run; the
    books and the event tables still replace a window in one commit, whatever
    it holds. A replay at seven rows a commit appends nothing and commits
    nothing to a keyed table."""
    small = {"commit_row_size": SMALL_COMMIT_ROW_SIZE}
    assert SMALL_COMMIT_ROW_SIZE < COMMIT_ROW_SIZE

    log = parse_log_messages(CAPTURE.as_uri(), storages, DAY, **small)
    early = graph(storages, EARLY, **small)
    landed = graph(storages, WINDOW, **small)

    assert log == landing.log
    assert counted(early) == counted(landing.early)
    assert counted(landed) == counted(landing.landed)
    tables = sorted(landing.storages.tables())
    assert sorted(storages.tables()) == tables
    for table in tables:
        assert read(storages, table).equals(landing.table(table)), f"{table} landed other rows"

    commits = {table: added(storages, table) for table in tables}
    for table, held in commits.items():
        print(f"\n{table:<36} {len(held)} commits adding {[rows for _, rows in held]}")
    for table in KEYED.values():
        held = commits[table]
        assert {operation for operation, _ in held} == {"append"}, "a keyed table only appends"
        assert max(rows for _, rows in held) <= SMALL_COMMIT_ROW_SIZE
        assert sum(rows for _, rows in held) == read(storages, table).num_rows
        assert len(held) > snapshots(landing.storages, table), "more commits than the default"
    # The log's 144 lines, seven to a commit.
    assert len(commits[LOG_MESSAGES]) == -(-LOG.written // SMALL_COMMIT_ROW_SIZE)
    for table in {BOOKS, *EVENTS.values()}:
        assert len(commits[table]) == len((EARLY, WINDOW)), "one replacement per window"
    assert max(rows for _, rows in commits[BOOKS]) > SMALL_COMMIT_ROW_SIZE, (
        "a window's books are one commit however many rows they hold"
    )

    before = {table: read(storages, table) for table in tables}
    assert parse_log_messages(CAPTURE.as_uri(), storages, DAY, **small) == replayed(landing.log)
    again = graph(storages, WINDOW, **small)

    assert counted(again) == rerun(counted(landing.landed))
    for table, held in before.items():
        assert read(storages, table).equals(held), f"{table} landed other rows"
    for table in tables:
        replaced = table not in KEYED.values()
        assert len(added(storages, table)) == len(commits[table]) + replaced, table


def test_a_rerun_replaces_exactly_the_rows_whose_content_changed(storages: Storages) -> None:
    """A keyed task merges: a stored row the rerun states otherwise under the
    same identity is replaced, and every other row -- and every file holding
    none of those -- is left as it was."""
    parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    landed = read(storages, LOG_MESSAGES)
    field = log_message_field()
    lines = storages.dataset(LOG_MESSAGES, field=field)
    try:
        hours = pyarrow.compute.floor_temporal(landed.column("currunix"), unit="hour")
        stale = landed.filter(pyarrow.compute.equal(hours, hours[0])).slice(0, 3)
        level = stale.schema.get_field_index("loglevel")
        stale = stale.set_column(
            level,
            stale.schema.field(level),
            pyarrow.array(["STALE"] * stale.num_rows, stale.schema.field(level).type),
        )
        assert lines.merge_arrow_table(stale, field) == 3, "the stale rows replace the stored"
        before = {task.file.file_path for task in lines.refresh().iceberg_table.scan().plan_files()}
    finally:
        lines.close()

    rerun = parse_log_messages(CAPTURE.as_uri(), storages, DAY)

    assert rerun == Landed(read=144, written=3, skipped=141)
    assert read(storages, LOG_MESSAGES).equals(landed), "the read's own rows are back"
    after = set(planned(storages, LOG_MESSAGES))
    assert len(before - after) == 1, "only the file holding the stale rows is rewritten"


# -- the task surface ----------------------------------------------------------


def test_landed_counts_only_what_a_task_states() -> None:
    landed = Landed(read=3, written=2)

    assert landed == Landed(read=3, written=2, skipped=0, snapshot_id=None)
    with pytest.raises(dataclasses.FrozenInstanceError):
        landed.written = 3  # type: ignore[misc]


def test_a_counted_reader_closes_the_reader_it_counts() -> None:
    """A task registers only the counted reader, so unwinding it -- on an
    error too -- must release the scan underneath."""
    released: list[str] = []
    batch = pyarrow.record_batch({"n": [1, 2, 3]})
    source = OwnedRecordBatchReader(batch.schema, iter([batch]), lambda: released.append("scan"))
    count = _Count()
    counted_reader = count(source)

    assert counted_reader.read_all().num_rows == count.rows == 3
    counted_reader.close()
    assert released == ["scan"]


def test_every_kind_a_book_flattens_lands_in_a_table_of_its_own() -> None:
    assert set(EVENTS) == set(FLATTENED) == set(FLATTENERS)
    assert len(set(EVENTS.values())) == len(EVENTS)


@pytest.mark.parametrize("kind", sorted(FLATTENERS))
@pytest.mark.parametrize("snapshot_id", [True, False, -1, 1.0, "1"])
def test_a_flattener_refuses_a_snapshot_id_that_is_not_a_nonnegative_int(
    storages: Any, kind: str, snapshot_id: Any
) -> None:
    """A bool is an int to Python and never a snapshot here: `False` is not
    the pinned absence `0` is."""
    with pytest.raises(ValueError, match="nonnegative book snapshot_id or None"):
        FLATTENERS[kind](storages, WINDOW, snapshot_id=snapshot_id)
    assert list(storages.tables()) == []


def test_window_filter_is_the_arrow_window_as_a_scan_predicate() -> None:
    """`window_filter` states over a stored column what `within` answers over
    an Arrow one: `[start, end)`, the `EPOCH` pin and a null."""
    lower = datetime.datetime(2026, 8, 14, tzinfo=UTC)
    upper = datetime.datetime(2026, 8, 15, tzinfo=UTC)
    tick = datetime.timedelta(microseconds=1)

    predicate = window_filter(EVENT_CLOCK, (lower, upper))

    assert predicate == Or(
        And(GreaterThanOrEqual(EVENT_CLOCK, lower), LessThan(EVENT_CLOCK, upper)),
        Or(EqualTo(EVENT_CLOCK, EPOCH), IsNull(EVENT_CLOCK)),
    )
    instants = [lower - tick, lower, lower + (upper - lower) / 2, upper - tick, upper, EPOCH, None]
    column = pyarrow.array(instants, pyarrow.timestamp("us", tz="UTC"))
    stamped = pyarrow.table({EVENT_CLOCK: column})
    schema = Schema(NestedField(1, EVENT_CLOCK, TimestamptzType(), required=False))
    scanned = stamped.filter(expression_to_pyarrow(bind(schema, predicate, case_sensitive=True)))
    assert scanned.column(EVENT_CLOCK).to_pylist() == instants[1:4] + [EPOCH, None]
    assert scanned.equals(stamped.filter(within(column, (lower, upper))))


@scalar
class Stamped:
    """A row laid out by the hour of its instant."""

    name: str
    currunix: Annotated[datetime.datetime, partition_key("hour")]


def test_window_filter_opens_the_hours_a_window_touches_and_the_pins(
    storages: Any, tmp_path: Path
) -> None:
    hour = datetime.datetime(2026, 8, 14, 10, tzinfo=UTC)
    minutes = datetime.timedelta(minutes=1)
    instants = {
        "before": hour - 30 * minutes,
        "early": hour + 15 * minutes,
        "late": hour + 45 * minutes,
        "end": hour + 60 * minutes,
        "pinned": EPOCH,
    }
    field = Stamped.into_field()
    stamped = pyarrow.Table.from_pydict(
        {"name": list(instants), EVENT_CLOCK: list(instants.values())},
        schema=field.into_arrow_schema(),
    )
    dataset = storages.dataset("bronze.logs.stamped", field=field)
    try:
        assert dataset.append_arrow_reader(stamped.to_reader(), field) == len(instants)
        predicate = window_filter(EVENT_CLOCK, (hour, hour + 60 * minutes))

        plan = dataset.scan_plan(predicate)
        held = dataset.read_arrow_table(row_filter=predicate)
    finally:
        dataset.close()

    assert (plan["files"], plan["skipped"]) == (2, 2), "the window's hour and the pin's"
    assert sorted(held.column("name").to_pylist()) == ["early", "late", "pinned"]
