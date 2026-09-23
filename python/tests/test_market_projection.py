"""Market fanout projects deltas from Iceberg without reading live depth."""

import pytest

from rekep.iceberg import IcebergCatalog, IcebergDataset
from rekep.market import book_event_arrow_reader

from .test_market import FRAMES
from .test_market_pipeline import WINDOW, refined
from .test_workflow import Ran

pytestmark = pytest.mark.integration


def test_order_and_quote_tasks_read_only_nested_deltas(tmp_path, capsys, monkeypatch):
    ran = Ran(tmp_path, capsys)
    refined(ran, FRAMES)
    committed = ran.task("parse_books", **WINDOW)
    books = ran.table("market.books")
    sides = books.select(("bid", "ask"))
    columns = ("bid.deltas", "ask.deltas")
    store = IcebergCatalog.from_dict(ran.catalog)
    dataset = store.dataset("market.books")
    try:
        with dataset.read_arrow_reader(
            columns=columns, snapshot_id=committed["snapshot_id"]
        ) as reader:
            projected = reader.read_all()
    finally:
        dataset.close()
        store.close()
    assert projected.num_rows == books.num_rows == 4
    assert projected.nbytes < sides.nbytes, "the scan must not materialize repeated live depth"
    for side in projected.schema:
        assert [member.name for member in side.type] == ["deltas"]

    scans = []
    read = IcebergDataset.read_arrow_reader

    def observed(dataset, *args, **options):
        source = read(dataset, *args, **options)
        if dataset.identifier == "market.books":
            scans.append((options.get("columns"), source.schema))
        return source

    monkeypatch.setattr(IcebergDataset, "read_arrow_reader", observed)
    for kind, expected in (("orders", 1), ("quotes", 3)):
        with book_event_arrow_reader(sides.to_reader(), kind) as reader:
            reference = reader.read_all().sort_by("curruuid")
        result = ran.task(f"parse_{kind}", snapshot_id=committed["snapshot_id"], **WINDOW)
        assert result["written"] == expected
        assert result["source_snapshot_id"] == committed["snapshot_id"]
        stored = ran.table(f"market.{kind}").sort_by("curruuid")
        assert stored.equals(reference, check_metadata=False)

    assert len(scans) == 2
    for selected, schema in scans:
        assert selected == columns
        assert schema.names == ["bid", "ask"]
        for side in schema:
            assert [member.name for member in side.type] == ["deltas"]
