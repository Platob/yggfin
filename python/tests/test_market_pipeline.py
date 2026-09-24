"""Four standalone tasks over real Iceberg snapshots, including replacement."""

import json

import pytest

from rekep import cli
from rekep.fields import stored_arrow_reader
from rekep.fix import fix_codec, fix_message_field, fix_parse_field
from rekep.iceberg import IcebergCatalog, IcebergDataset
from rekep.market import market_window_filter
from rekep.times import window_of

from .test_market import FRAMES
from .test_workflow import Ran

pytestmark = pytest.mark.integration

WINDOW = {"start": "2026-09-21T10:00:00Z", "end": "2026-09-21T10:00:10Z"}
COUNTS = {"orders": 1, "quotes": 3, "executions": 3}


def refined(ran, frames):
    codec = fix_codec(batch_row_size=2)
    field = fix_message_field(codec)
    native = codec.arrow_reader(fix_parse_field(codec), map(codec.parse_fix_line, frames))
    reader = stored_arrow_reader(native, field)
    store = IcebergCatalog.from_dict(ran.catalog)
    try:
        dataset = store.dataset("fix.refined", field=field)
        try:
            dataset.overwrite_arrow_reader(
                reader, field, row_filter=market_window_filter(window_of(**WINDOW))
            )
        finally:
            dataset.close()
    finally:
        reader.close()
        store.close()


def fanout(ran, books):
    results = {}
    for kind, expected in COUNTS.items():
        result = ran.task(f"parse_{kind}", snapshot_id=books["snapshot_id"], **WINDOW)
        assert result["source_snapshot_id"] == books["snapshot_id"]
        assert result["window"] == books["window"]
        assert result["written"] == expected
        assert result["read"] == 4
        results[kind] = ran.table(f"market.{kind}")
    return results


def test_book_pipeline_replaces_windows_and_pins_fanout(tmp_path, capsys):
    ran = Ran(tmp_path, capsys)
    refined(ran, FRAMES)
    first = ran.task("parse_books", **WINDOW)
    assert (first["read"], first["written"]) == (5, 4)
    assert first["targets"] == {"books": "market.books"}
    original = fanout(ran, first)
    assert {row["quantity"] for row in original["executions"].to_pylist()} == {2, 4, 6}

    changed = tuple(frame.replace(b"44=99|", b"44=98|") for frame in FRAMES)
    refined(ran, changed)
    second = ran.task("parse_books", **WINDOW)
    assert second["snapshot_id"] != first["snapshot_id"]
    assert ran.table("market.books").num_rows == 4
    pinned = fanout(ran, first)
    for kind in COUNTS:
        assert pinned[kind].equals(original[kind])
    current = fanout(ran, second)
    assert current["orders"].column("price").to_pylist() == [98]
    for kind, expected in COUNTS.items():
        assert ran.table(f"market.{kind}").num_rows == expected

    refined(ran, ())
    empty = ran.task("parse_books", **WINDOW)
    assert (empty["read"], empty["written"]) == (0, 0)
    assert ran.table("market.books").num_rows == 0
    for kind in COUNTS:
        result = ran.task(f"parse_{kind}", snapshot_id=empty["snapshot_id"], **WINDOW)
        assert (result["read"], result["written"]) == (0, 0)
        assert ran.table(f"market.{kind}").num_rows == 0


def test_absent_snapshot_does_not_follow_newly_created_books(tmp_path, capsys):
    ran = Ran(tmp_path, capsys)
    absent = ran.task("parse_books", **WINDOW)
    assert absent["written"] == 0
    refined(ran, FRAMES)
    current = ran.task("parse_books", **WINDOW)
    assert current["written"] == 4
    for kind in COUNTS:
        result = ran.task(f"parse_{kind}", snapshot_id=0, **WINDOW)
        assert result["source_snapshot_id"] == 0
        assert (result["read"], result["written"]) == (0, 0)


def test_books_publish_their_commit_even_when_the_cached_head_advances(
    tmp_path, capsys, monkeypatch
):
    ran = Ran(tmp_path, capsys)
    refined(ran, FRAMES)
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
    books = ran.task("parse_books", **WINDOW)
    assert books["snapshot_id"] == committed[0]
    assert ran.table("market.books").num_rows == 8
    fanout(ran, books)


def test_missing_pinned_book_table_refuses_before_clearing_events(tmp_path, capsys):
    ran = Ran(tmp_path, capsys)
    refined(ran, FRAMES)
    books = ran.task("parse_books", **WINDOW)
    original = fanout(ran, books)
    for kind in COUNTS:
        parameters = {
            **WINDOW,
            "catalog": ran.catalog,
            "books": "market.missing",
            "snapshot_id": books["snapshot_id"],
        }
        argv = ["tasks", f"parse_{kind}", "run"]
        for name, value in parameters.items():
            argv.extend(["--parameter", f"{name}={json.dumps(value)}"])
        assert cli.main(argv) == 1
        capsys.readouterr()
        assert ran.table(f"market.{kind}").equals(original[kind])
