"""Market fanout projects deltas from Iceberg without reading live depth."""

import pytest

from rekep.iceberg import IcebergDataset
from rekep.market import book_event_arrow_reader
from rekep.pipeline import FLATTENED, parse_books, parse_events

from .test_market import FRAMES
from .test_market_pipeline import WINDOW, refined
from .test_pipeline import Warehouse

pytestmark = pytest.mark.integration


def test_order_and_quote_stages_read_only_nested_deltas(tmp_path, monkeypatch):
    warehouse = Warehouse(tmp_path, WINDOW)
    refined(warehouse, FRAMES)
    committed = warehouse.run(parse_books)
    books = warehouse.table("market.books")
    sides = books.select(("bid", "ask"))
    columns = ("bid.deltas", "ask.deltas")
    assert FLATTENED["orders"] == FLATTENED["quotes"] == columns
    with warehouse.opened() as store:
        dataset = store.dataset("market.books")
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
    read = IcebergDataset.read_arrow_reader

    def observed(dataset, *args, **options):
        source = read(dataset, *args, **options)
        if dataset.identifier == "market.books":
            scans.append((options.get("columns"), options.get("snapshot_id"), source.schema))
        return source

    monkeypatch.setattr(IcebergDataset, "read_arrow_reader", observed)
    for kind, expected in (("orders", 1), ("quotes", 3)):
        with book_event_arrow_reader(sides.to_reader(), kind) as reader:
            reference = reader.read_all().sort_by("curruuid")
        landed = warehouse.run(parse_events, kind, snapshot_id=committed.snapshot_id)
        assert landed.written == expected
        assert landed.snapshot_id == committed.snapshot_id
        stored = warehouse.table(f"market.{kind}").sort_by("curruuid")
        assert stored.equals(reference, check_metadata=False)

    assert len(scans) == 2
    for selected, snapshot_id, schema in scans:
        assert selected == columns
        assert snapshot_id == committed.snapshot_id
        assert schema.names == ["bid", "ask"]
        for side in schema:
            assert [member.name for member in side.type] == ["deltas"]
