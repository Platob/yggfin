"""The book fold and the three flatteners, over the capture's landing and over frames.

The landing's books are traced back to the silver events they fold and
forward to the event tables they flatten into, and the flatteners are run
side by side against the one books snapshot. `FRAMES` -- a heartbeat, a
snapshot, an incremental update, an order and a two-sided trade report --
are written into silver directly to pin replacement and snapshot pinning.
"""

from __future__ import annotations

import collections
import concurrent.futures
import datetime
import uuid
from pathlib import Path
from typing import Any

import pyarrow
import pytest

from rekep import Side, State, Storages, pipeline
from rekep.fields import stored_arrow_reader
from rekep.fix import EVENT_CLOCK, FixCodec, fix_message_field, fix_parse_field
from rekep.iceberg import IcebergDataset
from rekep.market import EVENT_KINDS, book_event_arrow_reader, market_window_filter
from rekep.pipeline import (
    BOOKS,
    EVENTS,
    FIX_MESSAGES,
    FIX_MESSAGES_RAW,
    FLATTENED,
    FLATTENERS,
    HISTORY,
    LOG_MESSAGES,
    SNAPSHOT_MILLIS,
    Landed,
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
)
from rekep.times import window_of

from .conftest import CAPTURE, DAY, WINDOW, Landing, read, rows, snapshots

pytestmark = pytest.mark.integration

UTC = datetime.timezone.utc

#: A valid market exchange: administration, a snapshot, an incremental update
#: carrying a trade, an order and a two-sided trade report.
FRAMES = (
    b"8=FIX.4.4|35=0|52=20260921-10:00:00|10=0|",
    b"8=FIX.4.4|35=W|52=20260921-10:00:01|55=AAPL|268=2|"
    b"269=0|278=B1|270=100|271=10|269=1|278=A1|270=102|271=12|10=0|",
    b"8=FIX.4.4|35=X|52=20260921-10:00:02|55=AAPL|268=2|"
    b"279=1|269=0|278=B1|270=101|271=11|"
    b"279=0|269=2|278=T1|270=101|271=2|10=0|",
    b"8=FIX.4.4|35=D|52=20260921-10:00:03|11=O1|55=AAPL|54=1|38=5|44=99|10=0|",
    b"8=FIX.4.4|35=AE|52=20260921-10:00:04|571=T2|150=F|55=AAPL|"
    b"32=10|31=101.25|60=20260921-10:00:04|552=2|"
    b"54=1|1427=BUY-EXEC|1009=4|37=BUY-ORDER|"
    b"54=2|1427=SELL-EXEC|1009=6|37=SELL-ORDER|10=0|",
)

#: The ten seconds `FRAMES` are dated in.
FRAMES_WINDOW = window_of("2026-09-21T10:00:00Z", "2026-09-21T10:00:10Z")

#: The events each kind flattens out of the four books `FRAMES` fold into.
COUNTS = {"orders": 1, "quotes": 3, "executions": 3}


def short(identity: bytes | None) -> str:
    """An identity as its last six hex digits: a UUIDv7 opens with its millisecond."""
    return "-" if identity is None else uuid.UUID(bytes=identity).hex[-6:]


def refined(storages: Storages, frames: tuple[bytes, ...]) -> None:
    """Replace `FRAMES_WINDOW` of silver `fix_messages` with `frames`, parsed and stored."""
    codec = FixCodec.from_env(batch_row_size=2)
    field = fix_message_field(codec)
    # The line door splits a trade report into the executions it states.
    messages = [message for frame in frames for message in codec.parse_line(frame)]
    native = codec.arrow_reader(fix_parse_field(codec), messages)
    reader = stored_arrow_reader(native, field)
    try:
        dataset = storages.dataset(FIX_MESSAGES, field=field)
        try:
            dataset.overwrite_arrow_reader(
                reader, field, row_filter=market_window_filter(FRAMES_WINDOW)
            )
        finally:
            dataset.close()
    finally:
        reader.close()


def fanout(storages: Storages, books: Landed) -> dict[str, pyarrow.Table]:
    """Every event kind off the snapshot `books` committed, and what each stored."""
    results = {}
    for kind, expected in COUNTS.items():
        landed = FLATTENERS[kind](storages, FRAMES_WINDOW, snapshot_id=books.snapshot_id)
        assert landed == Landed(read=4, written=expected, snapshot_id=books.snapshot_id)
        results[kind] = read(storages, EVENTS[kind])
    return results


# -- the capture's books ---------------------------------------------------------


def test_every_flattened_event_is_a_book_delta_folded_from_one_silver_event(
    landing: Landing,
) -> None:
    """An order or a quote is a delta of one book, an execution one of a
    book's executions; each names the lines of the silver event it was folded
    from, and carries that event's instrument: its ISIN the one the event
    states, and its category the key of the book it stands in, since every
    book is a `MIC:CFI` category and no event states a ticker. A side the
    silver row leaves unstated is `UNKN` in the book, whose side is never
    null."""
    books = landing.table(BOOKS).to_pylist()
    assert books, "the window's events fold into books"
    carried: dict[str, dict[bytes, dict[str, Any]]] = {kind: {} for kind in EVENTS}
    for book in books:
        for delta in book["deltas"] or ():
            kind = next(
                kind for kind, code in EVENT_KINDS.items() if code == delta["marketdatakind"]
            )
            carried[kind][delta["curruuid"]] = book
        for execution in book["executions"] or ():
            carried["executions"][execution["curruuid"]] = book
    # A grid view names the lines of the event it restates: only events fold.
    silver = [
        row
        for row in landing.table(FIX_MESSAGES).to_pylist()
        if row["recdunix"] and row["snapunix"] is None
    ]
    folded = {tuple(sorted(row["srcuuids"])): row for row in silver}

    print()
    for kind, table in EVENTS.items():
        events = landing.table(table).to_pylist()
        assert {event["curruuid"] for event in events} == set(carried[kind]), table
        for event in events:
            book = carried[kind][event["curruuid"]]
            source = folded[tuple(sorted(event["srcuuids"]))]
            securities = dict(event["securityids"] or ())
            category = f"{event['miccode'] or 'XXXX'}:{event['cficode'] or 'XXXXXX'}"
            print(
                f"{table:<34} ..{short(event['curruuid'])} {event['marketdatakind']:<3} "
                f"{category:<12} {securities.get('ISIN')} "
                f"in book ..{short(book['curruuid'])} "
                f"folded from silver ..{short(source['curruuid'])} {source['crosscode']}"
            )
            assert event["marketdatakind"] == EVENT_KINDS[kind]
            assert event[EVENT_CLOCK] == source[EVENT_CLOCK]
            assert event["crosscode"] == source["crosscode"]
            assert event["state"] == source["state"]
            assert event["side"] == (source["side"] or int(Side.UNKN))
            assert event["ticker"] is None and book["ticker"] is None
            assert book["crosscode"] == category
            assert event["isincode"] == securities.get("ISIN") == source["isincode"]


def test_the_flatteners_side_by_side_land_what_they_land_in_turn(landing: Landing) -> None:
    """The three kinds read one books snapshot and write three tables, so run
    at once against the snapshot the landing's books committed they answer
    and store exactly what the landing's run, one after another, did."""
    snapshot = landing.landed["parse_books"].snapshot_id
    in_turn = {kind: landing.landed[task.__name__] for kind, task in FLATTENERS.items()}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(FLATTENERS)) as pool:
        running = {
            kind: pool.submit(task, landing.storages, WINDOW, snapshot_id=snapshot)
            for kind, task in FLATTENERS.items()
        }
        side_by_side = {kind: future.result() for kind, future in running.items()}

    print(f"\nin turn      {in_turn}\nside by side {side_by_side}")
    assert side_by_side == in_turn
    for table, held in landing.flattened.items():
        assert landing.table(table).equals(held), table


#: The order the capture's cancel reject answers, under the side the walk gives it.
CANCELLED = "SELL:816179183-1983-98963_912"


def test_books_fold_a_cancel_reject_under_the_side_of_its_order(storages: Storages) -> None:
    """The trade report of 14:52:55, whose side group states no `Side(54)`,
    splits off one execution of side `UNKN`, which the fold books under
    `XXXX:XXXXXX` beside the order the events of 12:46 left alive there: the
    books from 14:00 are that order's hourly restatements and the book of
    the execution. The capture's last order message is a cancel reject at
    21:59:46 that states no `Side(54)` either; the walk joins it to the one
    live side of its order, the sell, and writes that side into its silver
    row, so the fold books it beside the cancel request it answers and the
    books outside its window stay."""
    parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    walked = (
        datetime.datetime(2026, 8, 14, 12, tzinfo=UTC),
        datetime.datetime(2026, 8, 14, 17, tzinfo=UTC),
    )
    parse_fix_messages_raw(storages, walked)
    parse_fix_messages_refined(storages, walked)
    assert "AE" in read(storages, FIX_MESSAGES).column("msgtype").to_pylist()
    traded = (datetime.datetime(2026, 8, 14, 14, tzinfo=UTC), walked[1])
    assert parse_books(storages, traded).written == 4
    booked = read(storages, BOOKS).to_pylist()
    assert [f"{book[EVENT_CLOCK]:%H:%M:%S}" for book in booked] == [
        "14:00:00",
        "14:52:55",
        "15:00:00",
        "16:00:00",
    ]
    for book in booked:
        assert (book["crosscode"], len(book["alive"])) == ("XXXX:XXXXXX", 1)
        assert not book["deltas"]
    grid = [book for book in booked if book["snapunix"] is not None]
    assert all(book["snapunix"] == book[EVENT_CLOCK] and not book["executions"] for book in grid)
    (execution,) = booked[1]["executions"]
    assert (execution["side"], State(execution["state"]).name) == (Side.UNKN, "FILLED")

    window = (
        datetime.datetime(2026, 8, 14, 21, tzinfo=UTC),
        datetime.datetime(2026, 8, 14, 22, tzinfo=UTC),
    )
    parse_fix_messages_raw(storages, DAY)
    walked = parse_fix_messages_refined(storages, window)
    held = read(storages, FIX_MESSAGES).select((EVENT_CLOCK, "msgtype", "side", "crosscode"))
    print(f"\nrefined over [21:00, 22:00): {walked}\n{held.to_pylist()}")
    rejected = datetime.datetime(2026, 8, 14, 21, 59, 46, tzinfo=UTC)
    assert [row for row in held.to_pylist() if row["msgtype"] == "9"] == [
        {EVENT_CLOCK: rejected, "msgtype": "9", "side": Side.SELL, "crosscode": CANCELLED}
    ]

    # The window's three silver rows: the reject, the cancel request it
    # answers, and the bridge's restatement of the reject, which keeps the
    # bare code and books nothing -- the copy of it the bridge forwarded under
    # its own `MsgSeqNum` folded into it.
    landed = parse_books(storages, window)
    print(f"books over [21:00, 22:00): {landed}")
    assert (landed.read, landed.written) == (3, 1)
    stored = read(storages, BOOKS).to_pylist()
    assert stored[:-1] == booked, "the books outside the window stay"
    book = stored[-1]
    assert (book[EVENT_CLOCK], book["snapunix"], book["crosscode"]) == (
        rejected,
        None,
        "XXXX:XXXXXX",
    )
    # One instant's steps of the chain fold in the chain's order whatever
    # order silver reads them back in: the cancel request, then the reject,
    # which states the order rejected, so the order leaves the book.
    assert [
        (delta["crosscode"], delta["side"], State(delta["state"]).name) for delta in book["deltas"]
    ] == [(CANCELLED, Side.SELL, "PENDING_CANCEL"), (CANCELLED, Side.SELL, "REJECTED")]
    assert not book["alive"]
    assert not book["executions"]


#: The capture's day up to the window's end: the report of 01:03, the events
#: of 12:46, in two categories, and the trade report of 14:52:55, whose one
#: execution books under `XXXX:XXXXXX`.
MORNING = (
    datetime.datetime(2026, 8, 14, tzinfo=UTC),
    datetime.datetime(2026, 8, 14, 16, 30, tzinfo=UTC),
)

#: The whole hour after the 01:03 report: its window opens on the book that
#: report left, which only the hour before `start` holds.
OPENED = (datetime.datetime(2026, 8, 14, 2, tzinfo=UTC), MORNING[1])


def stored_books(storages: Storages, table: str, since: datetime.datetime) -> list:
    """The books of `table` dated at or after `since`, as rows."""
    held = read(storages, table)
    return [row for row in held.to_pylist() if row[EVENT_CLOCK] >= since]


def test_a_window_opens_on_the_book_its_hour_before_left(
    storages: Storages, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The fold reads `HISTORY` before `start` and writes only the window, and
    its hourly grid restates each book at every whole hour: so the window
    opening at 02:00 opens on the book the 01:03 report left, and every book
    it lands is the one the fold over the whole morning lands at that
    instant. Silver holds the trade the report left alive restated on every
    hour, so even without the hour before the window opens on that book."""
    assert SNAPSHOT_MILLIS == HISTORY / datetime.timedelta(milliseconds=1) == 3_600_000
    parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    parse_fix_messages_raw(storages, MORNING)
    parse_fix_messages_refined(storages, MORNING)
    whole = parse_books(storages, MORNING)
    opened = parse_books(storages, OPENED, target="silver.record_keeping.opened_books")

    expected = stored_books(storages, BOOKS, OPENED[0])
    found = stored_books(storages, "silver.record_keeping.opened_books", OPENED[0])
    print(f"\nwhole morning {whole}\nfrom 02:00    {opened}")
    for book in found:
        print(f"{book[EVENT_CLOCK]:%H:%M:%S.%f} {book['snapunix']} {book['crosscode']}")
    assert opened.read == whole.read == 49, "the hour before 02:00 holds the 01:03 report"
    assert found == expected
    first = found[0]
    assert first[EVENT_CLOCK] == first["snapunix"] == OPENED[0]
    assert first["crosscode"] == "JOVM:XXXXXX"
    assert [entry["state"] for entry in first["alive"]] == [4002]
    assert not first["deltas"] and not first["executions"], "a grid book restates no event"

    monkeypatch.setattr(pipeline, "HISTORY", datetime.timedelta(0))
    cold = parse_books(storages, OPENED, target="silver.record_keeping.cold_books")
    held = stored_books(storages, "silver.record_keeping.cold_books", OPENED[0])
    assert cold.read == whole.read - 2, "the 01:03 report and the execution split out of it"
    assert held == expected, "the view of 02:00 carries the book the report left"


def test_books_at_a_window_start_are_the_books_the_whole_history_holds(
    storages: Storages,
) -> None:
    """Every event the capture's afternoon books is dated 12:46 but the one
    execution the trade report of 14:52:55 splits off -- so a window opening
    at 13:00 or 14:00 lands none of the others, and one opening at 14:00
    reads none of them either: what its books hold at `start` comes from the
    views the walk restated on the hour, which silver holds. Each book it
    lands is the one the fold over the whole morning lands at that instant --
    but a book the morning's fills left empty, which the whole fold keeps
    restating with nothing in it, names no live chain a view could carry, so
    a window whose hour before opens after it emptied does not know it."""
    parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    parse_fix_messages_raw(storages, MORNING)
    parse_fix_messages_refined(storages, MORNING)
    parse_books(storages, MORNING)
    for hour, read_rows, emptied in ((13, 37, 0), (14, 19, 3)):
        start = datetime.datetime(2026, 8, 14, hour, tzinfo=UTC)
        target = f"silver.record_keeping.books_from_{hour}"
        landed = parse_books(storages, (start, MORNING[1]), target=target)
        found = stored_books(storages, target, start)
        expected = stored_books(storages, BOOKS, start)
        print(f"\nfrom {hour}:00 {landed}: {len(found)} of the whole fold's {len(expected)}")
        assert landed.read == read_rows
        assert found == [book for book in expected if book in found]
        missing = [book for book in expected if book not in found]
        assert len(missing) == emptied
        for book in missing:
            assert book["snapunix"] == book[EVENT_CLOCK]
            assert (book["crosscode"], book["alive"], book["deltas"], book["executions"]) == (
                "XSWX:ESVTFR",
                [],
                [],
                [],
            )
            assert not book["bidlimits"] and not book["asklimits"]
        standing = [book for book in found if book[EVENT_CLOCK] == start and book["alive"]]
        assert sorted((book["crosscode"], len(book["alive"])) for book in standing) == [
            ("JOVM:XXXXXX", 1),
            ("XXXX:XXXXXX", 1),
        ]


#: The keys every event kind is matched to the silver event it flattens by.
MATCHED = (EVENT_CLOCK, "crosscode", "state")


def test_the_book_grid_flattens_every_event_once(storages: Storages) -> None:
    """Neither grid adds an event: a lifecycle view is folded as its book's
    membership at its instant and a grid book restates what its book holds,
    each answering no delta or execution, so the event tables flattened from
    the hourly silver and books are exactly those flattened with no grid at
    all. Every order and every execution is flattened off one silver event
    under a key of its own: the lines each hop logged one report on fold
    into one event, so no report lands twice."""
    parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    parse_fix_messages_raw(storages, MORNING)
    walked = parse_fix_messages_refined(storages, MORNING)
    events = parse_fix_messages_refined(
        storages, MORNING, snapshot_millis=0, target="silver.record_keeping.plain_fix_messages"
    )
    gridded = parse_books(storages, MORNING)
    plain = parse_books(
        storages,
        MORNING,
        snapshot_millis=0,
        source="silver.record_keeping.plain_fix_messages",
        target="silver.record_keeping.plain_books",
    )
    grid = [row for row in read(storages, BOOKS).to_pylist() if row["snapunix"] is not None]
    print(f"\nhourly {walked} {gridded}\nnone   {events} {plain}")
    assert (walked.written, events.written) == (49, 22)
    assert gridded.written == plain.written + len(grid) == 30
    assert all(not row["deltas"] and not row["executions"] for row in grid)
    for kind, task in FLATTENERS.items():
        hourly = task(storages, MORNING, snapshot_id=gridded.snapshot_id)
        flat = task(
            storages,
            MORNING,
            snapshot_id=plain.snapshot_id,
            source="silver.record_keeping.plain_books",
            target=f"silver.record_keeping.plain_{kind}",
        )
        assert hourly.written == flat.written, kind
        assert (
            read(storages, EVENTS[kind]).to_pylist()
            == read(storages, f"silver.record_keeping.plain_{kind}").to_pylist()
        ), kind

    silver = [row for row in read(storages, FIX_MESSAGES).to_pylist() if row["snapunix"] is None]
    matched = collections.Counter(tuple(row[key] for key in MATCHED) for row in silver)
    flattened = set()
    for kind, landed in {"orders": 8, "executions": 8}.items():
        held = read(storages, EVENTS[kind]).to_pylist()
        found = collections.Counter(tuple(row[key] for key in MATCHED) for row in held)
        identities = {row["curruuid"] for row in held}
        print(f"{kind}: {len(held)} rows, {len(identities)} keys, off {sum(found.values())} events")
        assert len(held) == len(identities) == landed, kind
        assert all(row["snapunix"] is None for row in held), "no view is an event"
        for key, count in found.items():
            # One delta or one execution per event: the two fills of one
            # order reported at one instant are two events and two orders.
            assert count == matched[key], (kind, key)
        flattened |= set(found)
    left = collections.Counter(
        (row["msgtype"], State(row["state"]).name)
        for row in silver
        if tuple(row[key] for key in MATCHED) not in flattened
    )
    print(f"not a book input: {dict(left)}")
    # Administration, the one message the bridge sent as XML, the trade
    # report -- whose execution is flattened on its own -- and the chain of
    # an order only ever acknowledged -- its acknowledgement, stated anew and
    # so updated, and the expiry that ends it -- execute nothing, so the fold
    # admits none of them; every other event is flattened, once.
    assert left == {
        ("A", "UNKNOWN"): 1,
        ("n", "FILLED"): 1,
        ("AE", "FILLED"): 1,
        ("8", "NEW"): 1,
        ("8", "UPDATED"): 1,
        ("8", "EXPIRED"): 1,
    }


# -- books over frames -----------------------------------------------------------


def test_books_replace_their_window_and_a_pinned_fanout_reads_its_snapshot(
    storages: Storages,
) -> None:
    refined(storages, FRAMES)
    first = parse_books(storages, FRAMES_WINDOW)
    assert (first.read, first.written) == (6, 4)
    original = fanout(storages, first)
    executions = original["executions"].to_pylist()
    # The update's trade entry states its quantity; the report was split into
    # one execution per side when it was parsed, each at the quantity that
    # side filled.
    assert [row["quantity"] for row in executions if row["lastqty"] is None] == [2]
    assert {(row["side"], row["lastqty"]) for row in executions if row["lastqty"]} == {
        (int(Side.BUYS), 4),
        (int(Side.SELL), 6),
    }

    changed = tuple(frame.replace(b"44=99|", b"44=98|") for frame in FRAMES)
    refined(storages, changed)
    second = parse_books(storages, FRAMES_WINDOW)
    assert second.snapshot_id != first.snapshot_id
    assert read(storages, BOOKS).num_rows == 4
    pinned = fanout(storages, first)
    for kind in COUNTS:
        assert pinned[kind].equals(original[kind])
    current = fanout(storages, second)
    assert current["orders"].column("price").to_pylist() == [98]
    for kind, expected in COUNTS.items():
        assert read(storages, EVENTS[kind]).num_rows == expected

    refined(storages, ())
    empty = parse_books(storages, FRAMES_WINDOW)
    assert (empty.read, empty.written) == (0, 0)
    assert read(storages, BOOKS).num_rows == 0
    for kind, task in FLATTENERS.items():
        landed = task(storages, FRAMES_WINDOW, snapshot_id=empty.snapshot_id)
        assert (landed.read, landed.written) == (0, 0)
        assert read(storages, EVENTS[kind]).num_rows == 0


def test_an_order_stating_no_side_is_left_out_of_the_books(storages: Storages) -> None:
    """A new order that states no `Side(54)`, with no chain before it to take
    one from, stands on neither side of a book: the fold leaves it out, with
    a warning through `logging`, and never fails on what a message states,
    so the books land what the other messages fold."""
    refined(storages, FRAMES)
    parse_books(storages, FRAMES_WINDOW)
    booked = read(storages, BOOKS).to_pylist()
    sideless = b"8=FIX.4.4|35=D|52=20260921-10:00:05|11=O2|55=AAPL|38=5|44=99|10=0|"
    refined(storages, (*FRAMES, sideless))

    landed = parse_books(storages, FRAMES_WINDOW)

    print(f"\nwith a side-less order: {landed}")
    assert (landed.read, landed.written) == (7, 4)
    assert read(storages, BOOKS).to_pylist() == booked, "the order books nowhere"


def test_an_absent_snapshot_does_not_follow_newly_created_books(storages: Storages) -> None:
    """Zero is a pinned absence: it reads nothing even once a newer head
    exists. None pins the head the call finds, and a missing table has none."""
    for task in FLATTENERS.values():
        assert task(storages, FRAMES_WINDOW) == Landed(read=0, written=0, snapshot_id=0)
    absent = parse_books(storages, FRAMES_WINDOW)
    assert absent.written == 0
    refined(storages, FRAMES)
    current = parse_books(storages, FRAMES_WINDOW)
    assert current.written == 4
    for task in FLATTENERS.values():
        assert task(storages, FRAMES_WINDOW, snapshot_id=0) == Landed(
            read=0, written=0, snapshot_id=0
        )
    for kind, expected in COUNTS.items():
        assert FLATTENERS[kind](storages, FRAMES_WINDOW) == Landed(
            read=4, written=expected, snapshot_id=current.snapshot_id
        )


def test_books_answer_their_own_commit_even_when_the_cached_head_advances(
    storages: Storages, monkeypatch: pytest.MonkeyPatch
) -> None:
    refined(storages, FRAMES)
    overwrite = IcebergDataset.overwrite_arrow_reader
    committed = []

    def advanced(dataset: IcebergDataset, *args: Any, **options: Any) -> int:
        count = overwrite(dataset, *args, **options)
        if (dataset.catalog_name, dataset.identifier) == ("silver", "record_keeping.books"):
            committed.append(dataset.iceberg_table.current_snapshot().snapshot_id)
            # A retry after a lost acknowledgement can return a refreshed
            # table whose head includes a later writer. The books keep their
            # own commit's marker.
            dataset.append_arrow_reader(dataset.read_arrow_reader(), merge_by=False)
            dataset.refresh()
            assert dataset.iceberg_table.current_snapshot().snapshot_id != committed[-1]
        return count

    monkeypatch.setattr(IcebergDataset, "overwrite_arrow_reader", advanced)
    books = parse_books(storages, FRAMES_WINDOW)
    assert books.snapshot_id == committed[0]
    assert read(storages, BOOKS).num_rows == 8
    fanout(storages, books)


def test_a_missing_pinned_book_table_is_refused_before_the_events_are_cleared(
    storages: Storages,
) -> None:
    refined(storages, FRAMES)
    books = parse_books(storages, FRAMES_WINDOW)
    original = fanout(storages, books)
    for kind, task in FLATTENERS.items():
        with pytest.raises(ValueError, match=r"silver\.record_keeping\.missing has no snapshot"):
            task(
                storages,
                FRAMES_WINDOW,
                snapshot_id=books.snapshot_id,
                source="silver.record_keeping.missing",
            )
        assert read(storages, EVENTS[kind]).equals(original[kind])


def test_order_and_quote_tasks_read_only_the_nested_deltas(
    storages: Storages, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The deltas and never the live depth, which repeats every resting entry
    on every book, and off the one snapshot the task is pinned to."""
    refined(storages, FRAMES)
    committed = parse_books(storages, FRAMES_WINDOW)
    books = read(storages, BOOKS)
    depth = books.select(("alive", "deltas"))
    columns = ("deltas",)
    assert FLATTENED["orders"] == FLATTENED["quotes"] == columns
    dataset = storages.dataset(BOOKS)
    try:
        with dataset.read_arrow_reader(
            columns=columns, snapshot_id=committed.snapshot_id
        ) as reader:
            projected = reader.read_all()
    finally:
        dataset.close()
    assert projected.num_rows == books.num_rows == 4
    assert projected.nbytes < depth.nbytes, "the scan must not materialize repeated live depth"
    assert projected.schema.names == list(columns)

    scans = []
    scan = IcebergDataset.read_arrow_reader

    def observed(dataset: IcebergDataset, *args: Any, **options: Any) -> Any:
        source = scan(dataset, *args, **options)
        if (dataset.catalog_name, dataset.identifier) == ("silver", "record_keeping.books"):
            scans.append((options.get("columns"), options.get("snapshot_id"), source.schema))
        return source

    monkeypatch.setattr(IcebergDataset, "read_arrow_reader", observed)
    for kind in ("orders", "quotes"):
        with book_event_arrow_reader(depth.to_reader(), kind) as reader:
            reference = reader.read_all().sort_by("curruuid")
        landed = FLATTENERS[kind](storages, FRAMES_WINDOW, snapshot_id=committed.snapshot_id)
        assert landed.written == COUNTS[kind]
        assert landed.snapshot_id == committed.snapshot_id
        stored = read(storages, EVENTS[kind]).sort_by("curruuid")
        assert stored.equals(reference, check_metadata=False)

    assert len(scans) == 2
    for selected, snapshot_id, schema in scans:
        assert selected == columns
        assert snapshot_id == committed.snapshot_id
        assert schema.names == list(columns)


# -- the whole graph over a market capture -----------------------------------------

#: A valid market capture: administration, quotes, a partial update and trade,
#: an order and a two-sided trade report, one frame per line.
CAPTURED = (
    "8=FIX.4.4|35=0|34=1|52=20260814-10:00:00|10=0|",
    "8=FIX.4.4|35=W|34=2|52=20260814-10:00:01|55=AAPL|268=2|"
    "269=0|278=B1|270=100|271=10|269=1|278=A1|270=102|271=12|10=0|",
    "8=FIX.4.4|35=X|34=3|52=20260814-10:00:02|55=AAPL|268=2|"
    "279=1|269=0|278=B1|270=101|271=11|279=0|269=2|278=T1|270=101|271=2|10=0|",
    "8=FIX.4.4|35=D|34=4|52=20260814-10:00:03|11=O1|55=AAPL|54=1|38=5|44=99|59=1|10=0|",
    "8=FIX.4.4|35=AE|34=5|52=20260814-10:00:04|571=T2|150=F|55=AAPL|"
    "32=10|31=101.25|60=20260814-10:00:04|552=2|"
    "54=1|1427=BUY-EXEC|1009=4|37=BUY-ORDER|54=2|1427=SELL-EXEC|1009=6|37=SELL-ORDER|10=0|",
)

#: Every task over `CAPTURED`'s day: the log lands every line; the parse
#: leaves the heartbeat out and splits the trade report into the two
#: executions it states, and the six messages walk, fold into four books and
#: flatten into one order, three quotes and three executions.
LANDED = {
    "parse_log_messages": Landed(read=5, written=5),
    "parse_fix_messages_raw": Landed(read=5, written=6),
    "parse_fix_messages_refined": Landed(read=6, written=6),
    "parse_books": Landed(read=6, written=4),
    "parse_orders": Landed(read=4, written=1),
    "parse_quotes": Landed(read=4, written=3),
    "parse_executions": Landed(read=4, written=3),
}

#: Every task replaying that day: the three keyed tasks answer what they
#: answered, each row a key their table already holds, and append none; the
#: books and the event tables replace their window with what they wrote.
REPLAYED = {
    **LANDED,
    "parse_log_messages": Landed(read=5, written=0, skipped=5),
    "parse_fix_messages_raw": Landed(read=5, written=0, skipped=6),
    "parse_fix_messages_refined": Landed(read=6, written=0, skipped=6),
}

#: The tables a replay commits nothing to: those keyed on `curruuid`.
KEYED = (LOG_MESSAGES, FIX_MESSAGES_RAW, FIX_MESSAGES)


def graph_over(storages: Storages, capture: Path) -> dict[str, Landed]:
    """Every task over `DAY` in production order, the flatteners pinned to the books."""
    landed = {
        "parse_log_messages": parse_log_messages(capture.as_uri(), storages, DAY),
        "parse_fix_messages_raw": parse_fix_messages_raw(storages, DAY),
        "parse_fix_messages_refined": parse_fix_messages_refined(storages, DAY),
        "parse_books": parse_books(storages, DAY),
    }
    books = landed["parse_books"].snapshot_id
    for task in FLATTENERS.values():
        landed[task.__name__] = task(storages, DAY, snapshot_id=books)
    return landed


def counted(landed: dict[str, Landed]) -> dict[str, Landed]:
    """What each task read, wrote and skipped, without the snapshot it pinned."""
    return {name: Landed(held.read, held.written, held.skipped) for name, held in landed.items()}


def test_the_graph_lands_a_market_capture_and_a_replay_leaves_it_as_landed(
    storages: Storages, tmp_path: Path
) -> None:
    """Each kind reads the books once from the same immutable snapshot, and
    replaying the day leaves every table holding what it held: the keyed
    tables append nothing and commit nothing, and the books and the event
    tables replace their window in one new snapshot each."""
    capture = tmp_path / "market.log"
    capture.write_text(
        "".join(
            f"2026-08-14 10:00:0{index}.000 [250-e7256476:9effef3e6a:72504] "
            f"[ULBridge] (INFO) Sending : {frame}\n"
            for index, frame in enumerate(CAPTURED)
        ),
        encoding="utf-8",
    )

    landed = graph_over(storages, capture)

    print(f"\n{landed}")
    assert counted(landed) == LANDED
    books = landed["parse_books"].snapshot_id
    assert books is not None and books > 0
    for task in FLATTENERS.values():
        assert landed[task.__name__].snapshot_id == books
    stored = rows(storages)

    replayed = graph_over(storages, capture)

    assert counted(replayed) == REPLAYED
    assert replayed["parse_books"].snapshot_id != books
    assert rows(storages) == stored
    assert {table: snapshots(storages, table) for table in stored} == {
        table: 1 if table in KEYED else 2 for table in stored
    }, "a replay appends no key and replaces a market window in one commit"
