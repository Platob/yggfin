"""Parsing a capture into one table per kind, and what a second run costs.

The append half is what these are mostly about: a job that is re-run over a
capture that grew has to land the growth and nothing else, and a job that is
re-run over one that did not has to land nothing at all.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow
import pytest

from rekep.market import EventType
from rekep.tasks import ParseLogs, Task

from .conftest import EXPECTED_ROWS, HOURS, KINDS, PER_HOUR, write_capture

pytest.importorskip("pyiceberg")


def parse(capture: Path, catalog: dict[str, str], **declared: object) -> ParseLogs:
    return ParseLogs(
        source=str(capture),
        catalog="test",
        namespace="logs",
        properties=dict(catalog),
        **declared,
    )


# -- the fan-out -------------------------------------------------------------


def test_one_pass_lands_every_kind_in_its_own_table(capture: Path, catalog: dict) -> None:
    task = parse(capture, catalog)
    report = task.run()

    assert report.rows == EXPECTED_ROWS == 48, "derived from the fixture, pinned here"
    assert report.landed == EXPECTED_ROWS and report.skipped == 0
    assert set(report.written) == {f"logs.{kind.name.lower()}_logs" for kind in KINDS}
    for kind in KINDS:
        assert report.written[f"logs.{kind.name.lower()}_logs"] == PER_HOUR * HOURS


def test_a_table_holds_only_the_kind_it_is_named_for(capture: Path, catalog: dict) -> None:
    task = parse(capture, catalog)
    task.run()
    for kind in KINDS:
        rows = task.target(int(kind)).read_arrow_table()
        assert set(rows.column("etype").to_pylist()) == {int(kind)}, kind.name
        assert rows.num_rows == PER_HOUR * HOURS


def test_a_line_nothing_classifies_still_lands(capture: Path, catalog: dict) -> None:
    """Dropping it would make the job lossy exactly when a log format changes."""
    task = parse(capture, catalog)
    task.run()
    unknown = task.target(int(EventType.UNKNOWN)).read_arrow_table()
    assert unknown.num_rows == PER_HOUR * HOURS
    assert all("heartbeat" in message for message in unknown.column("message").to_pylist())


def test_the_rows_land_in_the_hour_they_happened_in(capture: Path, catalog: dict) -> None:
    task = parse(capture, catalog)
    task.run()
    rows = task.target(int(EventType.ORDER)).read_arrow_table()
    hours = sorted(set(rows.column("unix_hour").to_pylist()))
    assert len(hours) == HOURS
    assert hours[1] - hours[0] == 3_600_000_000_000, "an hour apart, in nanoseconds"


def test_the_target_names_are_the_event_types(capture: Path, catalog: dict) -> None:
    task = parse(capture, catalog)
    assert task.target_name(int(EventType.ORDER)) == "logs.order_logs"
    assert task.target_name(int(EventType.BOOK_SIDE)) == "logs.book_side_logs"
    assert task.target_name(int(EventType.UNKNOWN)) == "logs.unknown_logs"
    flat = ParseLogs(source=str(capture), namespace="")
    assert flat.target_name(0) == "unknown_logs", "no namespace, no prefix"
    named = ParseLogs(source=str(capture), namespace="raw", table="{event_type}")
    assert named.target_name(int(EventType.ORDER)) == "raw.order"


# -- appending ---------------------------------------------------------------


def test_running_it_twice_lands_nothing_the_second_time(capture: Path, catalog: dict) -> None:
    """The whole point of an append's merge: a replay costs the read, not the write."""
    task = parse(capture, catalog)
    first = task.run()
    second = task.run()

    assert first.landed == EXPECTED_ROWS
    assert second.rows == EXPECTED_ROWS, "it still read the capture"
    assert second.landed == 0 and second.skipped == EXPECTED_ROWS
    for kind in KINDS:
        assert task.target(int(kind)).read_arrow_table().num_rows == PER_HOUR * HOURS


def test_a_capture_that_grew_costs_only_what_grew(capture: Path, catalog: dict) -> None:
    """The case a scheduled job is in every time but the first."""
    task = parse(capture, catalog)
    task.run()

    write_capture(capture, name="b.log", day="2026-08-15")
    again = task.run()

    assert again.rows == EXPECTED_ROWS * 2, "both files were read"
    assert again.landed == EXPECTED_ROWS, "and only the new day was written"
    assert again.skipped == EXPECTED_ROWS
    for kind in KINDS:
        assert task.target(int(kind)).read_arrow_table().num_rows == PER_HOUR * HOURS * 2


def test_the_same_line_twice_in_one_run_lands_once(capture: Path, catalog: dict) -> None:
    """A duplicate inside the stream collapses for the same reason as one across runs."""
    write_capture(capture, name="copy.log")  # byte-for-byte the same lines
    report = parse(capture, catalog).run()
    assert report.rows == EXPECTED_ROWS * 2
    assert report.landed == EXPECTED_ROWS and report.skipped == EXPECTED_ROWS


def test_two_different_lines_both_land(capture: Path, catalog: dict) -> None:
    """The other half: dedup must not swallow rows that only look alike."""
    task = parse(capture, catalog)
    task.run()
    before = task.target(int(EventType.ORDER)).read_arrow_table().num_rows

    # Same second, same thread, same driver, same kind -- a different payload.
    (capture / "c.log").write_text(
        "2026-08-14 00:00:00.167_520 [t-1] [Bridge] sent NewOrderSingle AAPL 999@10.0\n"
        "2026-08-14 00:00:00.167_520 [t-1] [Bridge] sent NewOrderSingle AAPL 998@10.0\n"
    )
    task.run()
    after = task.target(int(EventType.ORDER)).read_arrow_table()
    assert after.num_rows == before + 2, "two distinct lines, two rows"
    assert len(set(after.column("hash").to_pylist())) == after.num_rows


def test_appending_without_merging_keeps_every_copy(capture: Path, catalog: dict) -> None:
    """`merge_by=False` is a different job, and says so rather than deduplicating."""
    task = parse(capture, catalog, merge_by=False)
    task.run()
    task.run()
    assert task.target(int(EventType.ORDER)).read_arrow_table().num_rows == PER_HOUR * HOURS * 2


# -- the stream ---------------------------------------------------------------


def test_the_capture_is_read_as_a_stream(capture: Path, catalog: dict) -> None:
    """Batch by batch, so the job's memory is the buffers and not the capture."""
    task = parse(capture, catalog, batch_row_size=8)
    batches = list(task.into_arrow_batches())
    assert len(batches) > 1, "the fixture is bigger than one batch at this size"
    assert sum(batch.num_rows for batch in batches) == EXPECTED_ROWS


def test_a_limit_stops_without_reading_past_it(capture: Path, catalog: dict) -> None:
    task = parse(capture, catalog, batch_row_size=8, limit=10)
    assert sum(batch.num_rows for batch in task.into_arrow_batches()) == 10
    assert task.run().rows == 10


def test_a_batch_is_cut_into_one_part_per_kind_it_holds(capture: Path, catalog: dict) -> None:
    task = parse(capture, catalog)
    batch = next(iter(task.into_arrow_batches()))
    parts = dict(task.split(batch))
    assert set(parts) == {int(kind) for kind in KINDS}
    assert sum(part.num_rows for part in parts.values()) == batch.num_rows
    for code, part in parts.items():
        assert set(part.column("etype").to_pylist()) == {code}


def test_a_batch_of_one_kind_is_handed_through_whole(capture: Path, catalog: dict) -> None:
    """Filtering a batch against itself is a copy nobody asked for."""
    task = parse(capture, catalog)
    batch = next(iter(task.into_arrow_batches()))
    only = batch.filter(pyarrow.compute.equal(batch.column("etype"), int(EventType.ORDER)))
    parts = list(task.split(only))
    assert len(parts) == 1
    assert parts[0][1] is only


def test_a_commit_size_that_is_smaller_lands_the_same_rows(
    capture: Path, catalog: dict, tmp_path: Path
) -> None:
    """The buffering is a memory bound, not a filter: it must not change results."""
    counts = {}
    for size in (4, 10_000):
        catalogue = dict(catalog)
        catalogue["uri"] = f"sqlite:///{(tmp_path / f'c{size}.db').as_posix()}"
        task = parse(capture, catalogue, commit_row_size=size)
        task.run()
        counts[size] = {
            kind.name: task.target(int(kind)).read_arrow_table().num_rows for kind in KINDS
        }
    assert counts[4] == counts[10_000]


# -- reading one from a document ----------------------------------------------


def test_a_document_describes_a_whole_job(capture: Path, catalog: dict, tmp_path: Path) -> None:
    """Which is what a scheduler runs: a file, not a script."""
    document = tmp_path / "parse_logs.yml"
    parse(capture, catalog).into_yaml(str(document))

    task = Task.from_yaml(str(document))
    assert isinstance(task, ParseLogs)
    report = task.run()
    assert report.landed == EXPECTED_ROWS


def test_the_rules_travel_with_the_document(capture: Path, catalog: dict, tmp_path: Path) -> None:
    """A desk with its own log format writes its own rules, in the same file."""
    document = tmp_path / "parse_logs.yml"
    task = parse(capture, catalog)
    task.rules.rules = [rule for rule in task.rules.rules if rule.etype is EventType.ORDER]
    task.into_yaml(str(document))

    loaded = Task.from_yaml(str(document))
    assert [rule.etype for rule in loaded.rules.rules] == [EventType.ORDER]
    report = loaded.run()
    assert set(report.written) == {"logs.order_logs", "logs.unknown_logs"}
    assert report.written["logs.unknown_logs"] == EXPECTED_ROWS - PER_HOUR * HOURS


# -- what it refuses ----------------------------------------------------------


def test_a_source_that_is_not_there_is_refused_by_name(catalog: dict, tmp_path: Path) -> None:
    """A path that reads zero rows and reports success is the worst answer."""
    with pytest.raises(FileNotFoundError, match="does not exist"):
        parse(tmp_path / "nowhere", catalog).run()


def test_one_file_is_a_source_as_much_as_a_folder(capture: Path, catalog: dict) -> None:
    """Told apart by asking the filesystem, because a document cannot say."""
    report = parse(capture / "a.log", catalog).run()
    assert report.rows == EXPECTED_ROWS


# -- the commit bound ---------------------------------------------------------


def test_a_commit_holds_a_commit_size_worth_and_no_more(
    capture: Path, catalog: dict, tmp_path: Path
) -> None:
    """What `commit_row_size` actually promises, read back off the snapshots.

    The buffer is flushed when it *reaches* the size rather than when adding
    the next batch would pass it, so a commit holds between `commit_row_size`
    and one batch more -- and that upper bound is what the job's memory is.
    """
    for name in ("b.log", "c.log", "d.log", "e.log"):
        write_capture(capture, name=name, day=f"2026-08-{16 + ord(name[0]) - ord('b')}")
    task = parse(capture, catalog, batch_row_size=8, commit_row_size=25)
    report = task.run()

    assert report.rows == EXPECTED_ROWS * 5, "five days of the fixture"
    for kind in KINDS:
        dataset = task.target(int(kind))
        added = [
            int(dict(summary)["added-records"])
            for summary in dataset.snapshots().column("summary").to_pylist()
        ]
        assert sum(added) == PER_HOUR * HOURS * 5, kind.name
        assert len(added) > 1, "the fixture is bigger than one commit at this size"
        for count in added[:-1]:
            assert count >= task.commit_row_size, (kind.name, added)
            assert count < task.commit_row_size + task.batch_row_size, (kind.name, added)


def test_a_rotated_copy_of_a_file_is_skipped_rather_than_written(
    capture: Path, catalog: dict
) -> None:
    """A capture holds `app.log` and `app.log.1`, and the overlap is the norm."""
    (capture / "a.log.1").write_bytes((capture / "a.log").read_bytes())
    task = parse(capture, catalog)
    report = task.run()

    assert report.rows == EXPECTED_ROWS * 2, "both files were read"
    assert report.landed == EXPECTED_ROWS and report.skipped == EXPECTED_ROWS
    for kind in KINDS:
        assert task.target(int(kind)).read_arrow_table().num_rows == PER_HOUR * HOURS


# -- the message layer --------------------------------------------------------

#: The dictionary this repository publishes, so names resolve without a scrape.
DICTIONARY = Path(__file__).resolve().parents[3] / "data" / "fix.zip"

HEAD = "2026-08-14 00:00:0{slot}.167_520 [t-1] [Bridge] "

#: One line per spelling of a message, plus one carrying none.
MESSAGES = [
    "sending 8=FIX.4.2|35=D|49=BUYSIDE|11=ORD-1|55=TTF|54=1|38=1200|10=203|",
    "toBridge #ISINCODE=XX0000084733#SYMBOL=TTF#SIDE=1#ACCOUNT=<null>#UNKNOWNVENUEFIELD=Z9",
    "8=FIX.4.2|35=UL|34=2|#SYMBOL=TTF|#SIDE=1|10=044|",
    "heartbeat emitted seq=7",
]


@pytest.fixture
def messages(tmp_path: Path) -> Path:
    """A capture of four lines: FIX, a bridge, a wrapped bridge, and neither."""
    folder = tmp_path / "messages"
    folder.mkdir()
    (folder / "m.log").write_text(
        "".join(HEAD.format(slot=slot) + line + "\n" for slot, line in enumerate(MESSAGES))
    )
    return folder


def test_a_line_lands_with_the_message_it_carries(messages: Path, catalog: dict) -> None:
    """The whole layer, through the shipped job: a line, its tags, its rest."""
    task = parse(messages, catalog, fix_dictionary=str(DICTIONARY))
    task.run()

    rows = task.target(int(EventType.UNKNOWN)).read_arrow_table().sort_by("unix")
    assert rows.column("category_name").to_pylist() == ["UL", "UL", "OTHER"]
    assert rows.column("category_id").to_pylist() == [2, 2, 0]

    bridge = dict(rows.column("fix_tags")[0].as_py())
    assert bridge == {55: "TTF", 54: "1"}, "names the dictionary knows, as tags"
    assert 1 not in bridge, "`ACCOUNT=<null>` is an absent account, not the text"
    assert dict(rows.column("keyval")[0].as_py()) == {
        "ISINCODE": "XX0000084733",
        "UNKNOWNVENUEFIELD": "Z9",
    }, "and the names it does not, kept rather than dropped"

    wrapped = dict(rows.column("fix_tags")[1].as_py())
    assert wrapped[35] == "UL" and wrapped[55] == "TTF", "one message, read as one"


def test_a_line_carrying_no_message_says_so_rather_than_saying_nothing(
    messages: Path, catalog: dict
) -> None:
    """Null, not an empty map: "not a message" and "a message that said nothing"."""
    task = parse(messages, catalog, fix_dictionary=str(DICTIONARY))
    task.run()
    rows = task.target(int(EventType.UNKNOWN)).read_arrow_table().sort_by("unix")
    assert rows.column("category_name")[2].as_py() == "OTHER"
    assert rows.column("fix_tags")[2].as_py() is None
    assert rows.column("keyval")[2].as_py() is None


def test_the_wire_tags_land_for_a_line_that_is_about_something(
    messages: Path, catalog: dict
) -> None:
    """A FIX line lands in its own table, with the tags it was written with."""
    task = parse(messages, catalog, fix_dictionary=str(DICTIONARY))
    task.run()
    rows = task.target(int(EventType.ORDER)).read_arrow_table()
    assert rows.num_rows == 1
    assert rows.column("category_name").to_pylist() == ["FIX"]
    tags = dict(rows.column("fix_tags")[0].as_py())
    assert tags[35] == "D" and tags[11] == "ORD-1" and tags[38] == "1200"
