"""Reading market logs as the orders, executions and books they carry.

The pipeline end to end: FIX lines in a folder, three Iceberg tables out, and a
second run that lands nothing because the first one already did.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.market import Book, EventType, Execution, Order, Reference
from rekep.tasks import ParseMarket, Task

pytest.importorskip("pyiceberg")

#: Where every message below is stamped, so a test can name an instant.
DAY = "20260821"

#: One instrument, because a book is one instrument's -- which is the whole
#: reason the events are partitioned the way they are.
ABOUT = "55=BTC-USD|207=XCME|15=USD"


def header(second: int) -> str:
    """A log line's own prefix, which is what `runix` is read from."""
    return f"2026-08-21 10:30:{second:02d}.000_000 [t-1] [Bridge] "


def entry(kind: str, action: str, px: float, qty: float, second: int, named: str = "") -> str:
    """One `NoMDEntries <268>` entry, with its own `MDEntryTime <273>`."""
    named = f"|278={named}" if named else ""
    return f"279={action}|269={kind}|270={px}|271={qty}{named}|272={DAY}|273=10:30:{second:02d}.000"


def refresh(second: int, *entries: str) -> str:
    """One MarketDataIncrementalRefresh <X>."""
    inside = "|".join(entries)
    return (
        f"{header(second)}8=FIX.4.4|35=X|49=XCME|52={DAY}-10:30:{second:02d}.500|"
        f"{ABOUT}|268={len(entries)}|{inside}|10=001"
    )


#: A capture whose every number below is derived from: a two-a-side snapshot,
#: a size change, a trade at the offer, and a pull of the best offer.
LINES = [
    refresh(
        0,
        entry("0", "0", 100.0, 5, 0, "B1"),
        entry("0", "0", 99.5, 3, 0, "B2"),
        entry("1", "0", 100.5, 7, 0, "A1"),
    ),
    refresh(1, entry("0", "1", 100.0, 9, 1, "B1")),
    refresh(2, entry("2", "0", 100.5, 3, 2)),
    refresh(3, entry("1", "2", 100.5, 0, 3, "A1")),
]

#: What the capture holds, counted from `LINES` rather than typed twice: three
#: bid/offer entries in the snapshot, one change, one pull -- and one trade.
ORDERS = 5
EXECUTIONS = 1
INSTANTS = 4
#: One instrument, learnt once: every message names it the same way.
INSTRUMENTS = 1


@pytest.fixture
def capture(tmp_path: Path) -> Path:
    folder = tmp_path / "capture"
    folder.mkdir()
    (folder / "bridge.log").write_text("\n".join(LINES) + "\n")
    return folder


def parse(capture: Path, catalog: dict[str, str], **declared: object) -> ParseMarket:
    return ParseMarket(
        source=str(capture),
        pattern="*.log",
        venue="XCME",
        catalog="test",
        namespace="market",
        properties=dict(catalog),
        **declared,
    )


# -- what one pass lands -----------------------------------------------------


def test_one_pass_lands_the_events_and_the_book_they_fold_into(
    capture: Path, catalog: dict
) -> None:
    report = parse(capture, catalog).run()
    assert report.rows == len(LINES) == 4, "derived from the fixture, pinned here"
    assert report.written == {
        "market.orders": ORDERS,
        "market.executions": EXECUTIONS,
        "market.books": INSTANTS,
        "market.instruments": INSTRUMENTS,
    }


def test_each_table_holds_the_shape_it_is_named_for(capture: Path, catalog: dict) -> None:
    """The columns, and a lossless cast back onto the declaration.

    Not schema equality: a table read back through Iceberg spells a column
    comment `doc`, carries the field ids Iceberg assigned, and hands a `string`
    back as a `large_string` -- all of which are the protocol's representation
    and none of which is a difference in the shape. What has to hold is that
    the declaration still reads it, which is what the cast says.
    """
    task = parse(capture, catalog)
    task.run()
    for shape, declared in (("orders", Order), ("executions", Execution), ("books", Book)):
        stored = task.target(shape).read_arrow_table()
        assert stored.schema.names == declared.FIELD.names, shape
        onto = declared.FIELD.cast_arrow_table(stored)
        assert onto.schema.equals(declared.FIELD.into_arrow_schema()), shape
        assert onto.num_rows == stored.num_rows, shape


def test_the_books_are_one_per_instant_that_moved_them(capture: Path, catalog: dict) -> None:
    """Not one per message: several entries at one instant are one state."""
    task = parse(capture, catalog)
    task.run()
    books = task.target("books").read_arrow_table().sort_by("unix")
    assert books.num_rows == INSTANTS
    assert len(set(books.column("unix").to_pylist())) == INSTANTS


def test_the_book_says_what_the_capture_did_to_it(capture: Path, catalog: dict) -> None:
    """Read down the columns and the capture is legible without opening it."""
    task = parse(capture, catalog)
    task.run()
    books = task.target("books").read_arrow_table().sort_by("unix")
    assert books.column("bid_px").to_pylist() == [100.0] * 4
    assert books.column("bid_qty").to_pylist() == [5.0, 9.0, 9.0, 9.0], "the size change"
    assert books.column("ask_qty").to_pylist() == [7.0, 7.0, 4.0, None], "the trade, then the pull"
    assert books.column("spread").to_pylist()[-1] is None, "one side left, so no spread at all"


def test_a_trade_lands_as_an_execution_and_on_the_side_it_hit(capture: Path, catalog: dict) -> None:
    task = parse(capture, catalog)
    task.run()
    (fill,) = task.target("executions").read_arrow_table().to_pylist()
    assert (fill["px"], fill["qty"]) == (100.5, 3.0)
    books = task.target("books").read_arrow_table().sort_by("unix")
    printed = books.column("ask_executions").to_pylist()[2]
    assert [(one["px"], one["qty"]) for one in printed] == [(100.5, 3.0)]


def test_every_row_is_stamped_with_the_entry_s_own_time_not_the_message_s(
    capture: Path, catalog: dict
) -> None:
    """`MDEntryTime <273>` is the transaction; `SendingTime <52>` is transmission,
    and every message here sends half a second after its entries happened."""
    task = parse(capture, catalog)
    task.run()
    orders = task.target("orders").read_arrow_table()
    for row in orders.to_pylist():
        assert row["unix"] % 1_000_000_000 == 0, "on the second, as the entries are"


def test_every_row_lands_in_the_partition_its_instrument_belongs_to(
    capture: Path, catalog: dict
) -> None:
    task = parse(capture, catalog)
    task.run()
    for shape in ("orders", "executions", "books"):
        stored = task.target(shape).read_arrow_table()
        hashes = set(stored.column("instrument_hash").to_pylist())
        assert len(hashes) == 1 and 0 not in hashes, shape


# -- running it again --------------------------------------------------------


def test_a_replay_lands_nothing(capture: Path, catalog: dict) -> None:
    """The point of appending with `merge_by`: nothing stored is ever rewritten."""
    task = parse(capture, catalog)
    task.run()
    again = task.run()
    assert again.landed == 0
    assert again.skipped == ORDERS + EXECUTIONS + INSTANTS + INSTRUMENTS


def test_a_capture_that_grew_costs_the_growth(capture: Path, catalog: dict) -> None:
    task = parse(capture, catalog)
    task.run()
    (capture / "bridge.log").write_text(
        "\n".join([*LINES, refresh(4, entry("1", "0", 100.2, 6, 4, "A3"))]) + "\n"
    )
    after = task.run()
    assert after.rows == len(LINES) + 1
    assert after.written == {
        "market.orders": 1,
        "market.executions": 0,
        "market.books": 1,
        "market.instruments": 0,
    }


def test_appending_everything_is_what_false_means(capture: Path, catalog: dict) -> None:
    task = parse(capture, catalog, merge_by=False)
    task.run()
    again = task.run()
    assert again.written["market.orders"] == ORDERS, "duplicates and all"


# -- the pieces on their own -------------------------------------------------


def test_the_declaration_is_the_schema_and_never_inferred_from_the_rows(
    capture: Path, catalog: dict
) -> None:
    """`into_dict` leaves out a member that is None, so a schema inferred from the
    first row's keys loses every column the first book happens not to have -- and
    every row after it is cast onto that and comes back null."""
    task = parse(capture, catalog)
    events = [one for batch in task.into_arrow_batches() for one in task.into_events(batch)]
    books = list(Book.from_events(events))

    # Derived, not typed out: whichever columns the first book happens not to
    # have are exactly the ones an inferred schema would drop, and which those
    # are is a property of the fixture rather than something to pin.
    first = books[0].into_dict()
    lost = [
        name
        for name in Book.FIELD.names
        if name not in first and any(name in one.into_dict() for one in books[1:])
    ]
    assert lost, "the fixture has to exercise this, or the test cannot fail"

    table = task.into_arrow_table("books", books)
    assert table.schema.names == Book.FIELD.names
    for name in lost:
        assert table.column(name).null_count < len(books), f"{name} came back all null"


def test_an_empty_run_of_events_is_still_the_declared_schema(capture: Path, catalog: dict) -> None:
    empty = parse(capture, catalog).into_arrow_table("books", [])
    assert empty.num_rows == 0
    assert empty.schema.equals(Book.FIELD.into_arrow_schema())


def test_a_source_that_is_a_dataset_document_is_read_as_that_store(
    capture: Path, catalog: dict
) -> None:
    """Which is how this chains onto `parse_logs` without re-reading the capture."""
    from rekep.iceberg import IcebergDataset

    task = parse(capture, catalog)
    task.run()
    task.source = {
        "kind": "iceberg",
        "name": "market.orders",
        "catalog": "test",
        "properties": dict(catalog),
    }
    reading = task.source_dataset()
    assert isinstance(reading, IcebergDataset) and reading.name == "market.orders"


def test_a_source_that_does_not_exist_says_so_rather_than_reading_nothing(
    tmp_path: Path, catalog: dict
) -> None:
    """Getting it wrong reads zero rows and reports success."""
    with pytest.raises(FileNotFoundError):
        parse(tmp_path / "nowhere", catalog).source_dataset()


def test_skipping_the_fold_lands_only_the_events(capture: Path, catalog: dict) -> None:
    report = parse(capture, catalog, books=False).run()
    assert "market.books" not in report.written
    assert report.written == {"market.orders": ORDERS, "market.executions": EXECUTIONS}


def test_a_limit_stops_reading_rather_than_reading_on(capture: Path, catalog: dict) -> None:
    report = parse(capture, catalog, limit=2).run()
    assert report.rows == 2


def test_the_task_reads_from_its_own_document() -> None:
    """The kind dispatches, exactly as a schema contract's does."""
    built = Task.from_dict({"kind": "parse_market", "source": "x", "namespace": "m"})
    assert isinstance(built, ParseMarket) and built.namespace == "m"


def test_the_document_and_the_object_agree_on_the_kind() -> None:
    with pytest.raises(ValueError, match="parse_market"):
        ParseMarket.from_dict({"kind": "parse_logs", "source": "x"})


def test_a_message_that_is_not_market_data_produces_nothing(catalog: dict) -> None:
    """A feed is mostly heartbeats, and an empty iterator is the right answer."""
    task = ParseMarket(source="", properties=dict(catalog))
    batch = pyarrow.RecordBatch.from_pydict({"message": ["8=FIX.4.4|35=0|10=1", None, ""]})
    assert list(task.into_events(batch)) == []


# -- the instrument table ----------------------------------------------------


def test_the_instruments_the_capture_taught_it_are_a_table_of_their_own(
    capture: Path, catalog: dict
) -> None:
    task = parse(capture, catalog)
    task.run()
    stored = task.target("instruments").read_arrow_table()
    assert stored.num_rows == INSTRUMENTS
    assert stored.column("symbol").to_pylist() == ["BTC-USD"]
    assert stored.schema.names == Reference.FIELD.names
    assert stored.column("etype").to_pylist() == [int(EventType.INSTRUMENT)]
    assert stored.column("unix").to_pylist()[0] > 0, "stamped with when it was learnt"


def test_an_instrument_is_landed_once_however_often_the_feed_repeats_it(
    capture: Path, catalog: dict
) -> None:
    """A row per message would be the feed again rather than the reference data."""
    report = parse(capture, catalog).run()
    assert report.written["market.instruments"] == 1
    assert report.rows == len(LINES), "though every one of them named it"


def test_a_message_that_knows_more_lands_another_version(tmp_path: Path, catalog: dict) -> None:
    folder = tmp_path / "richer"
    folder.mkdir()
    (folder / "bridge.log").write_text(
        "\n".join(
            [
                LINES[0],
                # the same instrument, with reference data attached
                LINES[1].replace(ABOUT, f"{ABOUT}|167=FUT|461=FFICSX|48=US1234567890|22=4"),
            ]
        )
        + "\n"
    )
    task = parse(folder, catalog)
    task.run()
    stored = task.target("instruments").read_arrow_table().sort_by("version")
    rows = stored.to_pylist()
    assert len(rows) == 2, "one bare, one enriched -- two versions of what is known"
    assert {one["xhash"] for one in rows} == {rows[0]["xhash"]}, "and one identity"
    assert [one["instrument"]["isin_code"] for one in rows] == [None, "US1234567890"]
    assert [one["version"] for one in rows] == [0, 1]
    assert rows[1]["prev_hash"] == rows[0]["hash"], "chained, like every other row here"


# -- the hourly grid, through the whole job ----------------------------------


def test_the_books_carry_the_hourly_grid_the_fold_produced(tmp_path: Path, catalog: dict) -> None:
    """A gap of hours with no messages still has a row per hour in the table."""
    from rekep.market.event import HOUR

    folder = tmp_path / "gapped"
    folder.mkdir()
    later = (
        refresh(0, entry("0", "1", 100.0, 9, 0, "B1"))
        .replace(f"{DAY}-10:30:00.500", f"{DAY}-13:30:00.500")
        .replace("273=10:30:00.000", "273=13:30:00.000")
    )
    (folder / "bridge.log").write_text("\n".join([LINES[0], later]) + "\n")

    task = parse(folder, catalog)
    task.run()
    books = task.target("books").read_arrow_table().sort_by("unix")
    hours = sorted({one // HOUR for one in books.column("unix").to_pylist()})
    assert len(hours) == 4, f"10:00 through 13:00 inclusive, got {hours}"
    taken = books.column("sunix").to_pylist()
    assert sum(one is not None for one in taken) == 3, "three of them are pictures"


def test_turning_the_grid_off_leaves_only_the_instants_that_moved(
    capture: Path, catalog: dict
) -> None:
    task = parse(capture, catalog, snapshot_every=0)
    report = task.run()
    assert report.written["market.books"] == INSTANTS
    stored = task.target("books").read_arrow_table()
    assert all(one is None for one in stored.column("sunix").to_pylist())


# -- fanning the translation out ---------------------------------------------


def test_workers_change_how_it_runs_and_not_what_it_lands(
    capture: Path, catalog: dict, tmp_path: Path
) -> None:
    """The one property a fan-out has to have. `executor.map` hands results back in
    submission order, which a fold depends on and a shard would otherwise destroy."""
    alone = parse(capture, catalog).run()
    second = {
        "type": "sql",
        "uri": f"sqlite:///{(tmp_path / 'second.db').as_posix()}",
        "warehouse": (tmp_path / "second").as_uri(),
    }
    (tmp_path / "second").mkdir()
    together = parse(capture, second, workers=2).run()
    assert together.written == alone.written
    assert together.rows == alone.rows


def test_the_events_come_back_in_the_order_the_lines_were_read(
    capture: Path, catalog: dict
) -> None:
    task = parse(capture, catalog, workers=2)
    serial = [one for batch in task.into_arrow_batches() for one in task.into_events(batch)]
    fanned = list(task.into_parallel_events())
    assert [one.hash for one in fanned] == [one.hash for one in serial]


def test_a_shard_carries_the_lines_and_their_clocks_and_nothing_else(
    capture: Path, catalog: dict
) -> None:
    """A bound method would drag the task -- its catalog properties, its source,
    its buffers -- through the pickle to every worker."""
    task = parse(capture, catalog, workers=2, shard_row_size=2)
    shards = list(task._shards())
    assert len(shards) > 1, "the fixture has to actually shard"
    for column, venue, count, lines in shards:
        assert column == task.column and venue == "XCME"
        assert count == len(lines) <= 2
        assert all(isinstance(text, str) and isinstance(clock, int) for text, clock in lines)


def test_one_worker_stays_in_this_process(capture: Path, catalog: dict) -> None:
    """Which is the default, and what a small capture wants: a pool costs more to
    start than the work it would take away."""
    report = parse(capture, catalog, workers=1).run()
    assert report.landed
