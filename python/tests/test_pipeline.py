"""The stage functions: what each refuses before it writes, and what it owns.

`Warehouse` is the local catalog the pipeline tests run stages on; the
application contract over the shipped capture is `test_workflow.py`, and the
market stages are `test_market_pipeline.py`.
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Annotated, Any

import pyarrow
import pytest
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, IsNull, LessThan, Or
from pyiceberg.expressions.visitors import bind
from pyiceberg.io.pyarrow import expression_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, TimestamptzType

from rekep import IOBase, scalar
from rekep.arrow_reader import OwnedRecordBatchReader
from rekep.fields import partition_key
from rekep.iceberg import IcebergCatalog, window_filter
from rekep.pipeline import EVENTS, FLATTENED, Landed, _Count, parse_events, parse_messages
from rekep.times import EPOCH, window_of, within

from .conftest import catalog_properties

UTC = datetime.timezone.utc

#: What every stage takes as its window, as `rekep.times.window_of` answers it.
Window = tuple[datetime.datetime, datetime.datetime]

#: One dated ULBridge line, and the day it is dated on.
LINE = b"2026-08-14 12:46:39.769 [1] [ULBridge] (INFO)   --> 8=FIX.4.4|35=0|10=000|\n"
DAY = window_of("2026-08-14", "2026-08-14")


class Warehouse:
    """A local catalog, and the stages run on it over one window.

    Every stage and every read opens the catalog and closes it after, the way
    a caller running one stage at a time does, so no stage reads what another
    left in memory.
    """

    def __init__(self, root: Path, window: Window) -> None:
        self.catalog = {"name": "rekep", "properties": catalog_properties(root)}
        self.window = window

    @contextlib.contextmanager
    def opened(self) -> Iterator[IcebergCatalog]:
        """The catalog, closed when the block ends."""
        store = IcebergCatalog.from_dict(self.catalog)
        try:
            yield store
        finally:
            store.close()

    def run(
        self,
        stage: Callable[..., Landed],
        *leading: Any,
        window: Window | None = None,
        **options: Any,
    ) -> Landed:
        """`stage` over `window`, this warehouse's own unless the call names one.

        `leading` is what a stage takes before its catalog: the capture of
        `parse_messages`, the kind of `parse_events`.
        """
        with self.opened() as store:
            return stage(*leading, store, self.window if window is None else window, **options)

    def rows(self) -> dict[str, int]:
        """Every stored table's row count, by identifier."""
        with self.opened() as store:
            return {
                dataset.identifier: dataset.read_arrow_table().num_rows
                for dataset in store.datasets(None)
            }

    def table(self, name: str) -> pyarrow.Table:
        """One stored table, read whole."""
        with self.opened() as store:
            dataset = store.dataset(name)
            try:
                return dataset.read_arrow_table()
            finally:
                dataset.close()

    def layout(self, name: str) -> dict[str, Any]:
        """What Iceberg itself records about one table: key, spec and order."""
        with self.opened() as store:
            table = store.load_table(name)
            schema = table.schema()
            return {
                "key": {schema.find_column_name(held) for held in schema.identifier_field_ids},
                "spec": [
                    (schema.find_column_name(field.source_id), str(field.transform))
                    for field in table.spec().fields
                ],
                "sort": [
                    (schema.find_column_name(field.source_id), str(field.transform))
                    for field in table.sort_order().fields
                ],
            }

    def snapshots(self) -> dict[str, int]:
        """How many snapshots each stored table holds, by identifier."""
        with self.opened() as store:
            return {
                identifier: len(store.load_table(identifier).metadata.snapshots)
                for identifier in store.tables(None)
            }


@pytest.fixture()
def closed(monkeypatch: pytest.MonkeyPatch) -> list[IOBase]:
    """Every `IOBase` closed from Python while the test runs."""
    handles: list[IOBase] = []
    close = IOBase.close

    def recorded(handle: IOBase) -> None:
        handles.append(handle)
        close(handle)

    monkeypatch.setattr(IOBase, "close", recorded)
    return handles


def test_landed_counts_only_what_a_stage_states() -> None:
    landed = Landed(read=3, written=2)

    assert landed == Landed(read=3, written=2, skipped=0, snapshot_id=None)
    with pytest.raises(dataclasses.FrozenInstanceError):
        landed.written = 3  # type: ignore[misc]


def test_a_counted_reader_closes_the_reader_it_counts() -> None:
    """A stage registers only the counted reader, so unwinding it -- on an
    error too -- must release the scan underneath."""
    released: list[str] = []
    batch = pyarrow.record_batch({"n": [1, 2, 3]})
    source = OwnedRecordBatchReader(batch.schema, iter([batch]), lambda: released.append("scan"))
    count = _Count()
    counted = count(source)

    assert counted.read_all().num_rows == count.rows == 3
    counted.close()
    assert released == ["scan"]


@pytest.mark.parametrize("kind", ["books", "Orders", ""])
def test_parse_events_refuses_a_kind_it_does_not_flatten(tmp_path: Path, kind: str) -> None:
    assert set(EVENTS) == set(FLATTENED), "every kind a book flattens lands in a table"
    store = IcebergCatalog.from_dict({"name": "rekep", "properties": catalog_properties(tmp_path)})
    try:
        with pytest.raises(ValueError, match="expected orders, quotes or executions"):
            parse_events(kind, store, DAY)
        assert store.tables(None) == []
    finally:
        store.close()


@pytest.mark.parametrize("snapshot_id", [True, False, -1, 1.0, "1"])
def test_parse_events_refuses_a_snapshot_id_that_is_not_a_nonnegative_int(
    tmp_path: Path, snapshot_id: Any
) -> None:
    """A bool is an int to Python and never a snapshot here: `False` is not
    the pinned absence `0` is."""
    store = IcebergCatalog.from_dict({"name": "rekep", "properties": catalog_properties(tmp_path)})
    try:
        with pytest.raises(ValueError, match="nonnegative book snapshot_id or None"):
            parse_events("orders", store, DAY, snapshot_id=snapshot_id)
        assert store.tables(None) == []
    finally:
        store.close()


def test_a_missing_capture_is_refused_and_only_a_bound_uri_is_closed(
    tmp_path: Path, closed: list[IOBase]
) -> None:
    """The native read of an absent path answers no rows, so the stage refuses
    it before it opens a table. A URI is bound for the call and closed with
    it; a handle is the caller's and stays open."""
    absent = tmp_path / "absent.log"
    store = IcebergCatalog.from_dict({"name": "rekep", "properties": catalog_properties(tmp_path)})
    handle = IOBase.from_uri(absent.as_uri())
    try:
        with pytest.raises(FileNotFoundError, match="absent.log"):
            parse_messages(absent.as_uri(), store, DAY)
        assert [bound.masked_uri for bound in closed] == [absent.as_uri()]

        with pytest.raises(FileNotFoundError, match="absent.log"):
            parse_messages(handle, store, DAY)
        assert len(closed) == 1, "the caller's handle is not closed"
        assert store.tables(None) == []
    finally:
        handle.close()
        store.close()


@pytest.mark.integration
def test_a_uri_is_bound_for_the_stage_and_a_handle_stays_the_callers(
    tmp_path: Path, closed: list[IOBase]
) -> None:
    capture = tmp_path / "capture.log"
    capture.write_bytes(LINE)
    warehouse = Warehouse(tmp_path, DAY)
    handle = IOBase.from_uri(capture.as_uri())
    try:
        assert warehouse.run(parse_messages, handle) == Landed(read=1, written=1)
        assert all(bound is not handle for bound in closed)
        assert handle.exists(), "and the caller may read it again"

        before = len(closed)
        assert warehouse.run(parse_messages, capture.as_uri()) == Landed(read=1, written=1)
        assert [bound.masked_uri for bound in closed[before:]] == [capture.as_uri()]
    finally:
        handle.close()
    assert warehouse.rows() == {"logs.messages": 1}, "the second run replaced the first"


def test_window_filter_is_the_arrow_window_as_a_scan_predicate() -> None:
    """`window_filter` states over a stored column what `within` answers over
    an Arrow one: `[start, end)`, the `EPOCH` pin and a null."""
    lower = datetime.datetime(2026, 8, 14, tzinfo=UTC)
    upper = datetime.datetime(2026, 8, 15, tzinfo=UTC)
    tick = datetime.timedelta(microseconds=1)

    predicate = window_filter("currunix", (lower, upper))

    assert predicate == Or(
        And(GreaterThanOrEqual("currunix", lower), LessThan("currunix", upper)),
        Or(EqualTo("currunix", EPOCH), IsNull("currunix")),
    )
    instants = [lower - tick, lower, lower + (upper - lower) / 2, upper - tick, upper, EPOCH, None]
    column = pyarrow.array(instants, pyarrow.timestamp("us", tz="UTC"))
    rows = pyarrow.table({"currunix": column})
    schema = Schema(NestedField(1, "currunix", TimestamptzType(), required=False))
    scanned = rows.filter(expression_to_pyarrow(bind(schema, predicate, case_sensitive=True)))
    assert scanned.column("currunix").to_pylist() == instants[1:4] + [EPOCH, None]
    assert scanned.equals(rows.filter(within(column, (lower, upper))))


@scalar
class Stamped:
    """A row laid out by the hour of its instant."""

    name: str
    currunix: Annotated[datetime.datetime, partition_key("hour")]


@pytest.mark.integration
def test_window_filter_opens_the_hours_a_window_touches_and_the_pins(tmp_path: Path) -> None:
    hour = datetime.datetime(2026, 8, 14, 10, tzinfo=UTC)
    minutes = datetime.timedelta(minutes=1)
    stamped = {
        "before": hour - 30 * minutes,
        "early": hour + 15 * minutes,
        "late": hour + 45 * minutes,
        "end": hour + 60 * minutes,
        "pinned": EPOCH,
    }
    field = Stamped.into_field()
    rows = pyarrow.Table.from_pydict(
        {"name": list(stamped), "currunix": list(stamped.values())},
        schema=field.into_arrow_schema(),
    )
    store = IcebergCatalog.from_dict({"name": "rekep", "properties": catalog_properties(tmp_path)})
    dataset = store.dataset("logs.stamped", field=field)
    try:
        assert dataset.append_arrow_reader(rows.to_reader(), field) == len(stamped)
        predicate = window_filter("currunix", (hour, hour + 60 * minutes))

        plan = dataset.scan_plan(predicate)
        read = dataset.read_arrow_table(row_filter=predicate)
    finally:
        dataset.close()
        store.close()

    assert (plan["files"], plan["skipped"]) == (2, 2), "the window's hour and the pin's"
    assert sorted(read.column("name").to_pylist()) == ["early", "late", "pinned"]
