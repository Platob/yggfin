"""`parse_log_messages`: the capture's lines into bronze `log_messages`, by the read's own field.

A line is an event the native text read settles: dated by the row header's
`mtime` capture, else by the modification time of the object it was read
from, keyed on its own identity and laid out by the hour of that instant. The
window is the read's own `where`, so the task lands the lines it covers and
leaves every other line where it was.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Any

import pyarrow
import pyarrow.compute
import pytest
from pyiceberg.expressions import EqualTo

from rekep import Field, IOBase, Storages
from rekep.fix import EVENT_CLOCK
from rekep.iceberg import IcebergDataset
from rekep.pipeline import (
    FIX_MESSAGES,
    FIX_MESSAGES_RAW,
    LOG_MESSAGES,
    Landed,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
)
from rekep.text import log_message_field
from rekep.times import EPOCH, window_of

from .conftest import CAPTURE, DAY, Landing, layout, read, rows

pytestmark = pytest.mark.integration

UTC = datetime.timezone.utc

#: How many of the capture's 144 lines the bridge printed in each hour.
HOURS = {3: 16, 14: 112, 16: 1, 23: 15}

#: One dated ULBridge line, on the capture's day.
LINE = b"2026-08-14 12:46:39.769 [1] [ULBridge] (INFO)   --> 8=FIX.4.4|35=0|10=000|\n"


@pytest.fixture
def closed(monkeypatch: pytest.MonkeyPatch) -> list[IOBase]:
    """Every `IOBase` closed from Python while the test runs."""
    handles: list[IOBase] = []
    close = IOBase.close

    def recorded(handle: IOBase) -> None:
        handles.append(handle)
        close(handle)

    monkeypatch.setattr(IOBase, "close", recorded)
    return handles


def test_the_log_holds_every_line_once_dated_by_its_header(landing: Landing) -> None:
    """The native read's own row, keyed on each line's identity: the shipped
    header dates all 144 lines -- the fifteen that group their micros too --
    so none takes the file's modification time and none sits at the pin, and
    three lines repeating another's bytes are three rows, because the content
    code digests the line's row number and object beside its bytes."""
    lines = landing.table(LOG_MESSAGES)

    assert lines.schema.equals(log_message_field().into_arrow_schema(), check_metadata=False)
    assert lines.schema.field(EVENT_CLOCK).type == pyarrow.timestamp("us", tz="UTC")
    instants = {row["seqnum"]: row[EVENT_CLOCK] for row in lines.to_pylist()}
    assert set(instants) == set(range(1, 145)), "the row number the read counts from 1"
    assert instants[1] == datetime.datetime(2026, 8, 14, 14, 46, 39, 769000, tzinfo=UTC)
    assert EPOCH not in instants.values()
    by_hour = {hour: sum(1 for at in instants.values() if at.hour == hour) for hour in HOURS}
    print(f"\nlines by the hour the bridge printed them: {by_hour}")
    assert by_hour == HOURS

    identities = lines.column("curruuid").to_pylist()
    assert len(set(identities)) == lines.num_rows
    assert bytes(16) not in identities
    assert lines.schema.field("currhashcode").type == pyarrow.int64()
    assert len(set(lines.column("currhashcode").to_pylist())) == 144
    bodies = lines.column("body").to_pylist()
    assert len(set(bodies)) < len(bodies), "some lines repeat another's bytes exactly"
    assert layout(landing.storages, LOG_MESSAGES) == {
        "key": {"curruuid"},
        "spec": [(EVENT_CLOCK, "hour")],
        "sort": [(EVENT_CLOCK, "identity"), ("seqnum", "identity"), ("curruuid", "identity")],
    }


def test_a_window_the_capture_falls_outside_reads_nothing_and_writes_none(
    storages: Storages, tmp_path: Path
) -> None:
    """The last day up to now does not cover the capture. The window is the
    read's own `where`, so the read answers no line at all, and each task
    creates its table empty."""
    recent = window_of()
    nothing = Landed(read=0, written=0)

    assert parse_log_messages(CAPTURE.as_uri(), storages, recent) == nothing
    assert rows(storages) == {LOG_MESSAGES: 0}
    assert parse_fix_messages_raw(storages, recent) == nothing
    assert parse_fix_messages_refined(storages, recent) == nothing
    assert rows(storages) == {LOG_MESSAGES: 0, FIX_MESSAGES_RAW: 0, FIX_MESSAGES: 0}
    # A line the header cannot date is dated by its object's own modification
    # time, so it is in the window that covers that instant. The window ends
    # past it rather than at `now`: before 3.13, Windows reads `now` off a
    # clock that ticks every ~16 ms, behind the file's mtime.
    unframed = tmp_path / "unframed.log"
    unframed.write_bytes(b"one physical line\n")
    mtime = datetime.datetime.fromtimestamp(unframed.stat().st_mtime, UTC)
    covering = window_of(end=mtime + datetime.timedelta(minutes=1))
    assert parse_log_messages(unframed.as_uri(), storages, covering) == Landed(read=1, written=1)
    held = read(storages, LOG_MESSAGES)
    assert held.column("msgpluginid").to_pylist() == [None]
    assert held.column(EVENT_CLOCK).to_pylist() != [EPOCH]


def test_a_bridge_printing_local_time_is_read_in_its_zone(storages: Storages) -> None:
    """The capture's bridge prints its clock two hours ahead of the UTC its
    FIX frames state: read in its zone, a line lands in the hour of the
    message it carries, so hourly silver windows place every event whose
    chain their history holds, and two observations of one delivery fold."""
    parse_log_messages(f"file:{CAPTURE}", storages, DAY, timezone="Europe/Zurich")
    lines = read(storages, LOG_MESSAGES)
    hours = pyarrow.compute.hour(lines.column("currunix")).to_pylist()
    assert {hour: hours.count(hour) for hour in set(hours)} == {1: 16, 12: 112, 14: 1, 21: 15}

    parse_fix_messages_raw(storages, DAY)
    hourly = [
        parse_fix_messages_refined(
            storages,
            (DAY[0] + datetime.timedelta(hours=hour), DAY[0] + datetime.timedelta(hours=hour + 1)),
        ).written
        for hour in range(24)
    ]
    daily = parse_fix_messages_refined(storages, DAY).written
    # Every event lands hour by hour but the one expiry whose order began
    # more than `HISTORY` before it: a walk's history is bounded.
    assert (sum(hourly), daily) == (15, 16)


def test_a_window_replaces_only_the_lines_it_covers(storages: Storages) -> None:
    """A run over the whole day and then one over its first part leave every
    line once: the second run replaces the lines its window covers and no
    other. The capture's lines straddle one second, which is where it cuts."""
    parse_log_messages(CAPTURE.as_uri(), storages, DAY)
    stored = read(storages, LOG_MESSAGES)
    cut = datetime.datetime(2026, 8, 14, 14, 46, 40, tzinfo=UTC)
    later = stored.filter(pyarrow.compute.greater_equal(stored.column(EVENT_CLOCK), cut)).num_rows
    assert 0 < later < stored.num_rows, "the capture straddles this cut"

    first = parse_log_messages(CAPTURE.as_uri(), storages, (DAY[0], cut))

    earlier = stored.num_rows - later
    assert first == Landed(read=earlier, written=earlier)
    assert read(storages, LOG_MESSAGES).equals(stored), "the later lines were not the run's"


def test_an_empty_capture_is_read_and_produces_nothing(storages: Storages, tmp_path: Path) -> None:
    """Zero rows is a run, not a failure: the table is created and holds none."""
    empty = tmp_path / "empty"
    empty.mkdir()
    (empty / "quiet.log").write_text("", encoding="utf-8")

    assert parse_log_messages(empty.as_uri(), storages, DAY) == Landed(read=0, written=0)
    assert rows(storages) == {LOG_MESSAGES: 0}


def test_several_files_are_one_capture(storages: Storages, tmp_path: Path) -> None:
    """A capture is a directory, opened one naturally sorted path at a time."""
    capture = tmp_path / "capture"
    capture.mkdir()
    lines = CAPTURE.read_bytes().split(b"\n")
    middle = len(lines) // 2
    (capture / "a.log").write_bytes(b"\n".join(lines[:middle]) + b"\n")
    (capture / "b.log").write_bytes(b"\n".join(lines[middle:]))

    assert parse_log_messages(capture.as_uri(), storages, DAY) == Landed(read=144, written=144)
    assert rows(storages) == {LOG_MESSAGES: 144}


def test_a_missing_capture_is_refused_and_only_a_bound_uri_is_closed(
    storages: Storages, tmp_path: Path, closed: list[IOBase]
) -> None:
    """The native read of an absent path answers no rows, so the task refuses
    it before it opens a table. A URI is bound for the call and closed with
    it; a handle is the caller's and stays open."""
    absent = tmp_path / "absent.log"
    handle = IOBase.from_uri(absent.as_uri())
    try:
        with pytest.raises(FileNotFoundError, match="absent.log"):
            parse_log_messages(absent.as_uri(), storages, DAY)
        assert [bound.masked_uri for bound in closed] == [absent.as_uri()]

        with pytest.raises(FileNotFoundError, match="absent.log"):
            parse_log_messages(handle, storages, DAY)
        assert len(closed) == 1, "the caller's handle is not closed"
        assert list(storages.tables()) == []
    finally:
        handle.close()


def test_a_uri_is_bound_for_the_task_and_a_handle_stays_the_callers(
    storages: Storages, tmp_path: Path, closed: list[IOBase]
) -> None:
    capture = tmp_path / "capture.log"
    capture.write_bytes(LINE)
    handle = IOBase.from_uri(capture.as_uri())
    try:
        assert parse_log_messages(handle, storages, DAY) == Landed(read=1, written=1)
        assert all(bound is not handle for bound in closed)
        assert handle.exists(), "and the caller may read it again"

        before = len(closed)
        assert parse_log_messages(capture.as_uri(), storages, DAY) == Landed(read=1, written=1)
        assert [bound.masked_uri for bound in closed[before:]] == [capture.as_uri()]
    finally:
        handle.close()
    assert rows(storages) == {LOG_MESSAGES: 1}, "the second run replaced the first"


def test_lines_stream_through_hour_partitions(
    storages: Storages, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The task hands Iceberg a reader under the read's own field, and a read
    of one instant plans the one hour holding it."""
    capture = tmp_path / "hourly"
    capture.mkdir()
    line = CAPTURE.read_bytes().split(b"\n", 1)[0] + b"\n"
    (capture / "14.log").write_bytes(line)
    (capture / "15.log").write_bytes(line.replace(b"2026-08-14 14:", b"2026-08-14 15:", 1))

    handed_to_iceberg: list[pyarrow.Schema] = []
    replace = IcebergDataset.overwrite_arrow_reader

    def observed(dataset: IcebergDataset, source: Any, *args: Any, **kwargs: Any) -> int:
        assert isinstance(source, pyarrow.RecordBatchReader)
        handed_to_iceberg.append(source.schema)
        return replace(dataset, source, *args, **kwargs)

    monkeypatch.setattr(IcebergDataset, "overwrite_arrow_reader", observed)

    assert parse_log_messages(capture.as_uri(), storages, DAY) == Landed(read=2, written=2)
    assert handed_to_iceberg == [log_message_field().into_arrow_schema()]

    first = datetime.datetime(2026, 8, 14, 14, 46, 39, 769000, tzinfo=UTC)
    second = datetime.datetime(2026, 8, 14, 15, 46, 39, 769000, tzinfo=UTC)
    lines = storages.dataset(LOG_MESSAGES, field=log_message_field())
    try:
        spec = lines.iceberg_table.spec()
        assert [(field.name, str(field.transform)) for field in spec.fields] == [
            ("currunix_hour", "hour")
        ]
        assert {row["partition"]["currunix_hour"] for row in lines.data_files().to_pylist()} == {
            int(first.timestamp() // 3600),
            int(second.timestamp() // 3600),
        }
        held = []
        for instant in (first, second):
            assert lines.scan_plan(EqualTo(EVENT_CLOCK, instant))["skipped"] == 1
            reader = lines.read_arrow_reader(
                log_message_field(), row_filter=EqualTo(EVENT_CLOCK, instant)
            )
            try:
                assert isinstance(reader, pyarrow.RecordBatchReader)
                held.extend(row for batch in reader for row in batch.to_pylist())
            finally:
                reader.close()
    finally:
        lines.close()

    assert {row[EVENT_CLOCK] for row in held} == {first, second}
    assert {row["msgpluginid"] for row in held} == {"ULBridge"}
    assert len({row["curruuid"] for row in held}) == 2, "the same bytes at two clocks: two lines"


def test_a_text_table_of_the_previous_shape_is_a_table_of_its_own(storages: Storages) -> None:
    """The event the read settles is stated on every row, so a table that
    never held it is not this one: Iceberg adds no required column to rows
    that never had it, and the dataset refuses rather than landing a table
    half of whose rows state no instant, no identity and no code."""
    declared = log_message_field().into_arrow_schema()
    # The sort order the table declares names both columns, so the previous
    # shape declares none beside what its own columns still mark.
    previous = Field.from_arrow_schema(
        pyarrow.schema(
            [member for member in declared if member.name not in {EVENT_CLOCK, "curruuid"}]
        ),
        name="log_messages",
    )
    lines = storages.dataset(LOG_MESSAGES, field=previous)
    lines.create_with_field(previous)
    lines.close()
    lines = storages.dataset(LOG_MESSAGES, field=log_message_field())
    try:
        # Named, so the refusal is the one this test means and not another
        # required column reached first.
        with pytest.raises(ValueError, match=f"cannot add required column: {EVENT_CLOCK}"):
            lines.add_fields(log_message_field())
    finally:
        lines.close()
