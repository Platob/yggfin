"""The book fold and the three flatteners, over the capture's landing and over frames.

The landing's books are traced back to the silver events they fold and
forward to the event tables they flatten into, and the flatteners are run
side by side against the one books snapshot. `FRAMES` -- a heartbeat, a
snapshot, an incremental update, an order and a two-sided trade report --
are written into silver directly to pin replacement and snapshot pinning.
"""

from __future__ import annotations

import concurrent.futures
import datetime
import uuid
from pathlib import Path
from typing import Any

import pyarrow
import pytest

from rekep import Storages
from rekep.fields import stored_arrow_reader
from rekep.fix import EVENT_CLOCK, FixCodec, fix_message_field, fix_parse_field
from rekep.iceberg import IcebergDataset
from rekep.market import EVENT_KINDS, SIDES, book_event_arrow_reader, market_window_filter
from rekep.pipeline import (
    BOOKS,
    EVENTS,
    FIX_MESSAGES,
    FLATTENED,
    FLATTENERS,
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
    native = codec.arrow_reader(fix_parse_field(codec), map(codec.parse_fix_line, frames))
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
    """An order or a quote is a delta of one book side, an execution one of a
    book's executions; each names the lines of the silver event it was folded
    from, and carries that event's instrument: its ticker is the symbol and
    its ISIN the one the event states."""
    books = landing.table(BOOKS).to_pylist()
    assert books, "the window's events fold into books"
    carried: dict[str, dict[bytes, dict[str, Any]]] = {kind: {} for kind in EVENTS}
    for book in books:
        for side in SIDES:
            for delta in (book[side] or {}).get("deltas") or ():
                kind = next(kind for kind, leaf in EVENT_KINDS.items() if leaf == delta["kind"])
                carried[kind][delta["curruuid"]] = book
        for execution in book["executions"] or ():
            carried["executions"][execution["curruuid"]] = book
    silver = [row for row in landing.table(FIX_MESSAGES).to_pylist() if row["recdunix"]]
    folded = {tuple(sorted(row["srcuuids"])): row for row in silver}

    print()
    for kind, table in EVENTS.items():
        events = landing.table(table).to_pylist()
        assert {event["curruuid"] for event in events} == set(carried[kind]), table
        for event in events:
            book = carried[kind][event["curruuid"]]
            source = folded[tuple(sorted(event["srcuuids"]))]
            securities = dict(event["securityids"] or ())
            print(
                f"{table:<34} ..{short(event['curruuid'])} {event['kind']:<15} "
                f"{event['ticker']:<12} {securities.get('ISIN')} "
                f"in book ..{short(book['curruuid'])} "
                f"folded from silver ..{short(source['curruuid'])} {source['crosscode']}"
            )
            assert event["kind"] == EVENT_KINDS[kind]
            assert event[EVENT_CLOCK] == source[EVENT_CLOCK]
            assert event["crosscode"] == source["crosscode"]
            assert event["state"] == source["state"]
            assert event["ticker"] == source["symbol"] == book["ticker"]
            assert securities.get("ISIN") == source["isincode"]


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


def test_books_refuse_a_trade_report_whose_side_states_no_side(storages: Storages) -> None:
    """The capture's 16:52 line carries a trade report dated 14:52:55 whose
    side group states no `Side(54)`. The fold admits it and refuses it by
    path, so a window holding it lands no book: an invalid admitted message
    stays an error, and the books keep what they held."""
    window = (
        datetime.datetime(2026, 8, 14, 14, tzinfo=UTC),
        datetime.datetime(2026, 8, 14, 17, tzinfo=UTC),
    )
    parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    parse_fix_messages_raw(storages, window)
    walked = parse_fix_messages_refined(storages, window)
    held = read(storages, FIX_MESSAGES).select((EVENT_CLOCK, "msgtype", "state", "crosscode"))
    print(f"\nrefined over [14:00, 17:00): {walked}\n{held.to_pylist()}")
    assert "AE" in held.column("msgtype").to_pylist()

    with pytest.raises(pyarrow.ArrowInvalid, match=r"NoSides\(552\)\[0\]\.Side\(54\)"):
        parse_books(storages, window)

    assert BOOKS not in storages.tables() or read(storages, BOOKS).num_rows == 0


# -- books over frames -----------------------------------------------------------


def test_books_replace_their_window_and_a_pinned_fanout_reads_its_snapshot(
    storages: Storages,
) -> None:
    refined(storages, FRAMES)
    first = parse_books(storages, FRAMES_WINDOW)
    assert (first.read, first.written) == (5, 4)
    original = fanout(storages, first)
    executions = original["executions"].to_pylist()
    # The update's trade entry states its quantity; the report decomposes into
    # one execution per side, each at the quantity that side filled.
    assert [row["quantity"] for row in executions if row["lastqty"] is None] == [2]
    assert {(row["side"], row["lastqty"]) for row in executions if row["lastqty"]} == {
        ("BUY", 4),
        ("SELL", 6),
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
            dataset.append_arrow_reader(dataset.read_arrow_reader())
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
    sides = books.select(SIDES)
    columns = tuple(f"{side}.deltas" for side in SIDES)
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
    assert projected.nbytes < sides.nbytes, "the scan must not materialize repeated live depth"
    for side in projected.schema:
        assert [member.name for member in side.type] == ["deltas"]

    scans = []
    scan = IcebergDataset.read_arrow_reader

    def observed(dataset: IcebergDataset, *args: Any, **options: Any) -> Any:
        source = scan(dataset, *args, **options)
        if (dataset.catalog_name, dataset.identifier) == ("silver", "record_keeping.books"):
            scans.append((options.get("columns"), options.get("snapshot_id"), source.schema))
        return source

    monkeypatch.setattr(IcebergDataset, "read_arrow_reader", observed)
    for kind in ("orders", "quotes"):
        with book_event_arrow_reader(sides.to_reader(), kind) as reader:
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
        assert schema.names == list(SIDES)
        for side in schema:
            assert [member.name for member in side.type] == ["deltas"]


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
#: leaves the heartbeat out, and the four market frames walk, fold into four
#: books and flatten into one order, three quotes and three executions.
LANDED = {
    "parse_log_messages": Landed(read=5, written=5),
    "parse_fix_messages_raw": Landed(read=5, written=4),
    "parse_fix_messages_refined": Landed(read=4, written=4),
    "parse_books": Landed(read=4, written=4),
    "parse_orders": Landed(read=4, written=1),
    "parse_quotes": Landed(read=4, written=3),
    "parse_executions": Landed(read=4, written=3),
}


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


def test_the_graph_lands_a_market_capture_and_a_replay_lands_it_again(
    storages: Storages, tmp_path: Path
) -> None:
    """Each kind reads the books once from the same immutable snapshot, and
    replaying the day replaces every output in one new snapshot per table."""
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

    assert counted(replayed) == LANDED
    assert replayed["parse_books"].snapshot_id != books
    assert rows(storages) == stored
    assert {table: snapshots(storages, table) for table in stored} == dict.fromkeys(stored, 2), (
        "a replay replaces its window in one commit per table"
    )
