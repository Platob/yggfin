"""The market stages over real Iceberg snapshots, including replacement, and the
whole graph over one ULBridge-framed market capture."""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.fields import stored_arrow_reader
from rekep.fix import fix_codec, fix_message_field, fix_parse_field
from rekep.iceberg import IcebergDataset
from rekep.market import market_window_filter
from rekep.pipeline import (
    EVENTS,
    Landed,
    parse_books,
    parse_events,
    parse_fix_raw,
    parse_fix_refined,
    parse_messages,
)
from rekep.times import window_of

from .test_market import FRAMES
from .test_pipeline import Warehouse

pytestmark = pytest.mark.integration

#: The ten seconds `test_market.FRAMES` are dated in.
WINDOW = window_of("2026-09-21T10:00:00Z", "2026-09-21T10:00:10Z")

#: The events each kind flattens out of the four books `FRAMES` fold into.
COUNTS = {"orders": 1, "quotes": 3, "executions": 3}


def refined(warehouse: Warehouse, frames: tuple[bytes, ...]) -> None:
    """Replace `WINDOW` of `fix.refined` with `frames`, parsed and stored."""
    codec = fix_codec(batch_row_size=2)
    field = fix_message_field(codec)
    native = codec.arrow_reader(fix_parse_field(codec), map(codec.parse_fix_line, frames))
    reader = stored_arrow_reader(native, field)
    try:
        with warehouse.opened() as store:
            dataset = store.dataset("fix.refined", field=field)
            try:
                dataset.overwrite_arrow_reader(
                    reader, field, row_filter=market_window_filter(WINDOW)
                )
            finally:
                dataset.close()
    finally:
        reader.close()


def fanout(warehouse: Warehouse, books: Landed) -> dict[str, pyarrow.Table]:
    """Every event kind off the snapshot `books` committed, and what each stored."""
    results = {}
    for kind, expected in COUNTS.items():
        landed = warehouse.run(parse_events, kind, snapshot_id=books.snapshot_id)
        assert landed == Landed(read=4, written=expected, snapshot_id=books.snapshot_id)
        results[kind] = warehouse.table(f"market.{kind}")
    return results


def test_book_pipeline_replaces_windows_and_pins_fanout(tmp_path: Path) -> None:
    warehouse = Warehouse(tmp_path, WINDOW)
    refined(warehouse, FRAMES)
    first = warehouse.run(parse_books)
    assert (first.read, first.written) == (5, 4)
    original = fanout(warehouse, first)
    assert {row["quantity"] for row in original["executions"].to_pylist()} == {2, 4, 6}

    changed = tuple(frame.replace(b"44=99|", b"44=98|") for frame in FRAMES)
    refined(warehouse, changed)
    second = warehouse.run(parse_books)
    assert second.snapshot_id != first.snapshot_id
    assert warehouse.table("market.books").num_rows == 4
    pinned = fanout(warehouse, first)
    for kind in COUNTS:
        assert pinned[kind].equals(original[kind])
    current = fanout(warehouse, second)
    assert current["orders"].column("price").to_pylist() == [98]
    for kind, expected in COUNTS.items():
        assert warehouse.table(f"market.{kind}").num_rows == expected

    refined(warehouse, ())
    empty = warehouse.run(parse_books)
    assert (empty.read, empty.written) == (0, 0)
    assert warehouse.table("market.books").num_rows == 0
    for kind in COUNTS:
        landed = warehouse.run(parse_events, kind, snapshot_id=empty.snapshot_id)
        assert (landed.read, landed.written) == (0, 0)
        assert warehouse.table(f"market.{kind}").num_rows == 0


def test_absent_snapshot_does_not_follow_newly_created_books(tmp_path: Path) -> None:
    """Zero is a pinned absence: it reads nothing even once a newer head
    exists. None pins the head the call finds, and a missing table has none."""
    warehouse = Warehouse(tmp_path, WINDOW)
    for kind in COUNTS:
        assert warehouse.run(parse_events, kind) == Landed(read=0, written=0, snapshot_id=0)
    absent = warehouse.run(parse_books)
    assert absent.written == 0
    refined(warehouse, FRAMES)
    current = warehouse.run(parse_books)
    assert current.written == 4
    for kind in COUNTS:
        assert warehouse.run(parse_events, kind, snapshot_id=0) == Landed(
            read=0, written=0, snapshot_id=0
        )
    for kind, expected in COUNTS.items():
        assert warehouse.run(parse_events, kind) == Landed(
            read=4, written=expected, snapshot_id=current.snapshot_id
        )


def test_books_publish_their_commit_even_when_the_cached_head_advances(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    warehouse = Warehouse(tmp_path, WINDOW)
    refined(warehouse, FRAMES)
    overwrite = IcebergDataset.overwrite_arrow_reader
    committed = []

    def advanced(dataset, *args, **options):
        count = overwrite(dataset, *args, **options)
        if dataset.identifier == "market.books":
            committed.append(dataset.iceberg_table.current_snapshot().snapshot_id)
            # A retry after lost acknowledgement can return a refreshed table
            # whose head includes a later writer. Retain our own commit marker.
            dataset.append_arrow_reader(dataset.read_arrow_reader())
            dataset.refresh()
            assert dataset.iceberg_table.current_snapshot().snapshot_id != committed[-1]
        return count

    monkeypatch.setattr(IcebergDataset, "overwrite_arrow_reader", advanced)
    books = warehouse.run(parse_books)
    assert books.snapshot_id == committed[0]
    assert warehouse.table("market.books").num_rows == 8
    fanout(warehouse, books)


def test_missing_pinned_book_table_refuses_before_clearing_events(tmp_path: Path) -> None:
    warehouse = Warehouse(tmp_path, WINDOW)
    refined(warehouse, FRAMES)
    books = warehouse.run(parse_books)
    original = fanout(warehouse, books)
    for kind in COUNTS:
        with pytest.raises(ValueError, match="market.missing has no snapshot .*: table is missing"):
            warehouse.run(
                parse_events, kind, snapshot_id=books.snapshot_id, source="market.missing"
            )
        assert warehouse.table(f"market.{kind}").equals(original[kind])


# -- the whole graph ---------------------------------------------------------

#: A valid market capture: administration, quotes, a partial update and trade,
#: an order and a two-sided trade report. The historical bridge fixture stays
#: covered by `test_workflow.py`; its incomplete trade sides are refused by books.
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

#: The day `CAPTURED` is dated on.
DAY = window_of("2026-08-14", "2026-08-14")

#: The raw stage omits the heartbeat; all four market frames survive the walk
#: and publish events in the three projected tables. A replay replaces them.
LANDED = {
    "logs.messages": Landed(read=5, written=5),
    "fix.raw": Landed(read=5, written=4),
    "fix.refined": Landed(read=4, written=4),
    "market.books": Landed(read=4, written=4),
    "market.orders": Landed(read=4, written=1),
    "market.quotes": Landed(read=4, written=3),
    "market.executions": Landed(read=4, written=3),
}
STORED = {name: landed.written for name, landed in LANDED.items()}


def graph(warehouse: Warehouse, capture: Path) -> dict[str, Landed]:
    """Every stage in production order, the kinds pinned to the books' commit."""
    landed = {
        "logs.messages": warehouse.run(parse_messages, capture.as_uri()),
        "fix.raw": warehouse.run(parse_fix_raw),
        "fix.refined": warehouse.run(parse_fix_refined),
        "market.books": warehouse.run(parse_books),
    }
    books = landed["market.books"].snapshot_id
    for kind, table in EVENTS.items():
        landed[table] = warehouse.run(parse_events, kind, snapshot_id=books)
    return landed


def counted(landed: dict[str, Landed]) -> dict[str, Landed]:
    """What each stage read and wrote, without the snapshot it committed or read."""
    return {name: Landed(held.read, held.written, held.skipped) for name, held in landed.items()}


def test_the_graph_publishes_the_market_capture_and_replays_it(tmp_path: Path) -> None:
    """A run reads each book once per kind from the same immutable snapshot,
    and replaying the window replaces every output in one new snapshot."""
    capture = tmp_path / "market.log"
    capture.write_text(
        "".join(
            f"2026-08-14 10:00:0{index}.000 [250-e7256476:9effef3e6a:72504] "
            f"[ULBridge] (INFO) Sending : {frame}\n"
            for index, frame in enumerate(CAPTURED)
        ),
        encoding="utf-8",
    )
    warehouse = Warehouse(tmp_path, DAY)

    landed = graph(warehouse, capture)

    assert counted(landed) == LANDED
    books = landed["market.books"].snapshot_id
    assert books is not None and books > 0
    for table in EVENTS.values():
        assert landed[table].snapshot_id == books
    assert warehouse.rows() == STORED

    replayed = graph(warehouse, capture)

    assert counted(replayed) == LANDED
    assert replayed["market.books"].snapshot_id != books
    assert warehouse.rows() == STORED
    assert warehouse.snapshots() == {name: 2 for name in STORED}, (
        "a replay replaces its window in one commit per table"
    )
