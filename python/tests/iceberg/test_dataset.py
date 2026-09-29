"""`IcebergDataset` against a real, fully local catalog: SQLite and a file warehouse."""

import dataclasses
import datetime
import math
import os
import subprocess
import sys
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated, Any

import pyarrow
import pyarrow.fs
import pyarrow.parquet
import pytest

if sys.version_info < (3, 11):  # pragma: no cover - the builtin is 3.11's
    from exceptiongroup import ExceptionGroup
from pyiceberg.conversions import from_bytes
from pyiceberg.expressions import EqualTo
from pyiceberg.transforms import BucketTransform, HourTransform, IdentityTransform
from pyiceberg.types import DateType, StringType, TimestamptzType

from rekep import (
    Field,
    IOBase,
    scalar,
)
from rekep.arrow_reader import OwnedRecordBatchReader
from rekep.fields import (
    derived_from,
    field_of,
    field_options,
    leaf_names,
    partition_key,
    primary_key,
    sort_key,
    stored_arrow_reader,
)
from rekep.iceberg import (
    IcebergCatalog,
    IcebergDataset,
    iceberg_schema,
    partition_keys,
    primary_keys,
    sort_keys,
)
from rekep.iceberg.dataset import (
    _applied_projection,
    _key_bounds,
    _partition_key_bounds,
    _PartitionColumn,
)
from rekep.iceberg.file_io import IcebergFileIO
from rekep.text import log_message_field, text_options
from rekep.times import EPOCH

from ..conftest import catalog_properties

#: The zone every instant here is spelled in.
UTC = datetime.timezone.utc

pytestmark = pytest.mark.integration


@scalar
class Quote:
    """One quote."""

    symbol: Annotated[str, primary_key()]
    """Instrument."""

    day: Annotated[datetime.date, partition_key()]
    """Trading day."""

    size: int
    """Quantity."""

    venue: str | None = None
    """Where it traded, when known."""


class CustomArrowFileIO(IcebergFileIO):
    """A distinct configured FileIO."""


def local(location: str) -> Path:
    """The directory behind a `file:` location, on any OS.

    Stripping `file://` by hand leaves `/C:/...` on Windows, which is not a
    path anything opens. `pyarrow.fs` owns the URI rules the store writes
    with, so it decides here too.
    """
    return Path(pyarrow.fs.FileSystem.from_uri(location)[1])


@pytest.fixture
def dataset(tmp_path: Path) -> IcebergDataset:
    return IcebergCatalog(name="test", properties=catalog_properties(tmp_path)).dataset(
        "trading.quotes", field=Quote.into_field()
    )


def quotes(count: int, message: str = "XPAR") -> pyarrow.Table:
    day = datetime.date(2026, 8, 14)
    return pyarrow.Table.from_pydict(
        {
            "symbol": [f"S{i}" for i in range(count)],
            "day": [day] * count,
            "size": list(range(count)),
            "venue": [message] * count,
        },
        schema=Quote.into_field().into_arrow_schema(),
    )


# -- creating -------------------------------------------------------------


def test_a_write_creates_the_table_from_the_declared_shape(dataset: IcebergDataset) -> None:
    assert not dataset.exists
    dataset.append_arrow_table(quotes(3))
    assert dataset.exists

    schema = dataset.iceberg_table.schema()
    assert [f.name for f in schema.fields] == [member.name for member in Quote.into_field()]
    assert schema.find_field("symbol").doc == "Instrument.", "the docs land as column comments"
    assert schema.identifier_field_ids == [schema.find_field("symbol").field_id]
    assert [f.name for f in dataset.iceberg_table.spec().fields] == ["day"]


def test_the_columns_a_reader_filters_on_are_declared_and_bounded(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(quotes(3))
    table = dataset.iceberg_table
    assert table.properties["write.metadata.metrics.column.symbol"] == "truncate(16)"
    assert table.properties["write.metadata.metrics.column.day"] == "full"

    keyed = table.schema().find_field("symbol").field_id
    written = [task.file for task in table.scan().plan_files()]
    assert written, "a write landed a file"
    assert all(keyed in one.lower_bounds for one in written), "the key is prunable"


def test_a_declared_property_wins_over_the_metrics_default(tmp_path: Path) -> None:
    dataset = IcebergDataset(
        name="quiet",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
        table_properties={"write.metadata.metrics.column.symbol": "none"},
    )
    dataset.get_or_create_table()
    assert dataset.iceberg_table.properties["write.metadata.metrics.column.symbol"] == "none"


def test_creating_is_idempotent(dataset: IcebergDataset) -> None:
    first = dataset.get_or_create_table()
    assert dataset.get_or_create_table().name() == first.name()


# -- what it holds --------------------------------------------------------


def test_the_declared_shape_wins(dataset: IcebergDataset) -> None:
    assert dataset.into_struct_field() == field_of(Quote, "quotes")
    assert dataset.name == "quotes"
    assert dataset.identifier == "trading.quotes"
    assert dataset.namespace == "trading"


def test_the_tables_own_shape_is_read_back(dataset: IcebergDataset, tmp_path: Path) -> None:
    dataset.append_arrow_table(quotes(1))
    found = IcebergCatalog(name="test", properties=catalog_properties(tmp_path)).dataset(
        dataset.identifier
    )
    shape = found.into_struct_field()
    assert shape.name == dataset.name
    assert [member.name for member in shape] == [member.name for member in Quote.into_field()]
    assert primary_keys(shape) == ["symbol"]
    assert partition_keys(shape) == {"day": "identity"}
    assert shape.field("symbol").metadata["description"] == "Instrument."
    assert not shape.field("size").nullable and shape.field("venue").nullable


# -- reading and writing --------------------------------------------------


def test_a_table_that_was_never_written_reads_as_no_rows(dataset: IcebergDataset) -> None:
    """The first interval of a pipeline reads upstreams that do not exist yet."""
    assert not dataset.exists
    assert dataset.read_arrow_table().num_rows == 0
    assert dataset.read_arrow_table().schema.names == leaf_names(Quote.into_field())
    filtered = dataset.read_arrow_reader(Quote.into_field(), row_filter=EqualTo("symbol", "X"))
    assert not list(filtered), "a filter on an absent table is answered, not refused"
    assert not dataset.exists, "reading does not create it"


def test_an_absent_table_reads_under_the_schema_it_was_asked_for(tmp_path: Path) -> None:
    bare = IcebergDataset(
        name="absent",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    reader = bare.read_arrow_reader(Quote.into_field(), columns=["symbol", "size"])
    assert reader.schema.names == ["symbol", "size"]
    assert reader.read_all().num_rows == 0
    assert bare.read_arrow_table().num_rows == 0


def test_rows_go_in_and_come_back(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(5))
    assert dataset.read_arrow_table().num_rows == 5


def test_a_read_without_a_schema_is_still_narrow(dataset: IcebergDataset) -> None:
    """`schema_to_pyarrow` answers `large_string` for every Iceberg string and
    takes no argument that says otherwise, so a table written from `string`
    columns read back wider than it was written. The reading is narrowed at
    that seam instead, which is what the configuration page's
    `pyarrow.use-large-types-on-read: false` would buy if 0.11.1 read it.

    Measured over 400,000 rows, interleaved against the unnarrowed reader in
    one process: best 14.6-15.7 ms against 14.4-18.5 ms, which is to say the
    cast does not show above this host's noise.
    """
    dataset.append_arrow_table(quotes(2))
    reader = dataset.read_arrow_reader()

    assert reader.schema.field("symbol").type == pyarrow.string()
    assert reader.read_all().schema.field("symbol").type == pyarrow.string(), (
        "and the batches agree with the reader that promised it"
    )


def test_a_read_casts_onto_the_schema_it_is_given(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    table = dataset.read_arrow_table(Quote.into_field())
    assert table.schema.equals(Quote.into_field().into_arrow_schema())


def test_a_filter_is_pushed_down_to_the_scan(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(5))
    assert dataset.read_arrow_table(row_filter="size >= 3").num_rows == 2


def test_columns_are_pushed_down_to_the_scan(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(3))
    assert dataset.read_arrow_table(columns=["symbol", "size"]).column_names == ["symbol", "size"]


def test_a_limit_is_pushed_down_to_the_scan(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(5))
    assert dataset.read_arrow_table(limit=2).num_rows == 2


@scalar
class Timed:
    """One row ordered by its event clock."""

    unix: Annotated[int, primary_key(), sort_key()]
    """Event time."""

    payload: str = "x"
    """Payload."""


@scalar
class DescendingTimed:
    """One row ordered newest first."""

    unix: Annotated[int, primary_key(), sort_key("desc")]


@scalar
class RepeatedTimed:
    """One event whose sort clock may be shared by several identities."""

    seq: Annotated[int, primary_key()]
    """Stable identity."""

    unix: Annotated[int, sort_key()]
    """Event time."""


@scalar
class PartitionedTimed:
    """One event in a dated, time-sorted stream."""

    day: Annotated[datetime.date, primary_key(), partition_key()]
    """Trading day."""

    unix: Annotated[int, primary_key(), sort_key()]
    """Event time."""

    payload: str
    """Observable source row."""


@scalar
class HourlyTimed:
    """One identified event partitioned and ordered by its UTC hour."""

    identity: Annotated[int, primary_key()]
    at: Annotated[datetime.datetime, partition_key("hour"), sort_key()]


def timed(*values: int) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {"unix": list(values), "payload": ["x"] * len(values)},
        schema=Timed.into_field().into_arrow_schema(),
    )


def descending_timed(*values: int) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {"unix": list(values)},
        schema=DescendingTimed.into_field().into_arrow_schema(),
    )


def repeated_timed(*values: int) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {"seq": list(range(len(values))), "unix": list(values)},
        schema=RepeatedTimed.into_field().into_arrow_schema(),
    )


def partitioned_timed(day: datetime.date, *values: int) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {
            "day": [day] * len(values),
            "unix": list(values),
            "payload": [f"{day}:{value}" for value in values],
        },
        schema=PartitionedTimed.into_field().into_arrow_schema(),
    )


def test_an_ordered_read_merges_overlapping_commits_before_applying_its_limit(
    tmp_path: Path,
) -> None:
    catalog = IcebergCatalog(name="ordered", properties=catalog_properties(tmp_path))
    ordered = catalog.dataset("trading.timed", field=Timed.into_field())
    commits = [(1, 4, 7), (2, 5, 8), (0, 3, 6, 9)]
    for values in commits:
        ordered.append_arrow_table(timed(*values), commit_row_size=1_000_000)

    field = ordered.iceberg_table.schema().find_field("unix")
    files = [task.file for task in ordered.iceberg_table.scan().plan_files()]
    lower = [from_bytes(field.field_type, one.lower_bounds[field.field_id]) for one in files]
    upper = [from_bytes(field.field_type, one.upper_bounds[field.field_id]) for one in files]
    expected = sorted(value for values in commits for value in values)

    found = ordered.read_arrow_reader(order_by="unix", limit=5).read_all()

    assert len(files) == len(commits) == 3, "one overlapping file per commit"
    assert max(lower) < min(upper), "the file ranges overlap, so a concatenation would fail"
    assert expected == list(range(10)), "the derived fixture still pins every instant"
    assert found.column("unix").to_pylist() == expected[:5] == [0, 1, 2, 3, 4]


def test_an_ordered_read_accepts_equal_adjacent_sort_values(tmp_path: Path) -> None:
    catalog = IcebergCatalog(name="repeated", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("trading.repeated_timed", field=RepeatedTimed.into_field())
    dataset.append_arrow_table(repeated_timed(7, 7), commit_row_size=1_000_000)

    found = dataset.read_arrow_reader(order_by="unix").read_all()

    assert found.column("seq").to_pylist() == [0, 1]


def test_closing_a_partial_ordered_limit_releases_the_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rekep.iceberg import dataset as module

    catalog = IcebergCatalog(name="partial", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("trading.partial_timed", field=Timed.into_field())
    dataset.append_arrow_table(timed(0, 1, 2, 3), commit_row_size=1_000_000)
    original = module._task_batches
    released = []

    def tracked(*args: Any, **kwargs: Any):
        try:
            yield from original(*args, **kwargs)
        finally:
            released.append(True)

    monkeypatch.setattr(module, "_task_batches", tracked)
    reader = dataset.read_arrow_reader(order_by="unix", limit=1)
    assert reader.read_next_batch().num_rows == 1
    reader.close()
    assert released == [True]


def test_a_read_finishes_each_sorted_partition_before_opening_the_next(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.io.pyarrow import PyArrowFile

    from rekep.iceberg import dataset as module

    catalog = IcebergCatalog(name="partition-order", properties=catalog_properties(tmp_path))
    ordered = catalog.dataset("trading.partitioned_timed", field=PartitionedTimed.into_field())
    first = datetime.date(2026, 8, 14)
    second = first + datetime.timedelta(days=1)
    for day, values in (
        (second, (0, 5)),
        (first, (1, 4)),
        (second, (1, 2)),
        (first, (2, 3)),
    ):
        ordered.append_arrow(partitioned_timed(day, *values), commit_row_size=1_000_000)

    scan = ordered.iceberg_table.scan()
    planned = list(scan.plan_files())
    paths = [path for path, _ in module._partition_tasks(scan, reversed(planned))]
    assert paths == ["day=2026-08-14", "day=2026-08-15"]

    opened: list[str] = []
    original = PyArrowFile.open

    def recorded(self: PyArrowFile, *args: object, **kwargs: object) -> object:
        if self.location.endswith(".parquet"):
            opened.append(self.location)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(PyArrowFile, "open", recorded)
    reader = ordered.read_arrow_reader(order_by="unix")
    head = reader.read_next_batch()
    assert opened and all("day=2026-08-14" in path for path in opened)
    found = pyarrow.Table.from_batches([head, *reader])
    assert list(
        zip(found.column("day").to_pylist(), found.column("unix").to_pylist(), strict=True)
    ) == [
        (first, 1),
        (first, 2),
        (first, 3),
        (first, 4),
        (second, 0),
        (second, 1),
        (second, 2),
        (second, 5),
    ]

    opened.clear()
    filtered = ordered.read_arrow_reader(
        row_filter="day = '2026-08-15'", order_by="unix"
    ).read_all()
    assert filtered.column("day").to_pylist() == [second] * 4
    assert opened and all("day=2026-08-15" in path for path in opened)


def test_hour_partition_paths_are_chronological_across_day_and_month(
    tmp_path: Path,
) -> None:
    from rekep.iceberg import dataset as module

    catalog = IcebergCatalog(name="hour-paths", properties=catalog_properties(tmp_path))
    ordered = catalog.dataset("trading.hourly_timed", field=HourlyTimed.into_field())
    instants = [
        datetime.datetime(2026, 1, 31, 23, 30, tzinfo=UTC),
        datetime.datetime(2026, 2, 1, 0, 15, tzinfo=UTC),
        datetime.datetime(2026, 2, 2, 0, 0, tzinfo=UTC),
    ]
    schema = HourlyTimed.into_field().into_arrow_schema()
    for identity in (2, 0, 1):
        ordered.append_arrow_table(
            pyarrow.Table.from_pydict(
                {"identity": [identity], "at": [instants[identity]]}, schema=schema
            ),
            commit_row_size=1_000_000,
        )

    scan = ordered.iceberg_table.scan()
    planned = list(scan.plan_files())
    paths = [path for path, _ in module._partition_tasks(scan, reversed(planned))]
    found = ordered.read_arrow_reader(order_by=("at", "identity")).read_all()

    assert paths == sorted(paths)
    assert ["2026-01-31-23", "2026-02-01-00", "2026-02-02-00"] == [
        path.rsplit("=", 1)[-1] for path in paths
    ]
    assert found.column("at").to_pylist() == instants


@scalar
class Chained:
    """One walked event, stored the way the lifecycle tables store theirs."""

    at: Annotated[datetime.datetime, partition_key("hour"), sort_key()]
    """Its instant, whose hour is its partition."""

    seq: Annotated[int, sort_key()]
    """Its place in its chain."""

    identity: Annotated[int, primary_key(), sort_key()]
    """Its own identity, the last tie breaker."""


def test_an_hourly_read_streams_one_hour_at_a_time_in_global_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The lifecycle reads: `(currunix, seqnum, curruuid)` over `hour(currunix)`.

    Each hour holds files whose ranges overlap, committed out of order, so
    every hour is a merge; the reader yields the first hour's rows before it
    opens a file of the second, and what it yields is the whole table in
    order -- nothing collects the scan to sort it.
    """
    from pyiceberg.io.pyarrow import PyArrowFile

    catalog = IcebergCatalog(name="hourly", properties=catalog_properties(tmp_path))
    chained = catalog.dataset("trading.chained", field=Chained.into_field())
    schema = Chained.into_field().into_arrow_schema()
    ten = datetime.datetime(2026, 8, 14, 10, tzinfo=UTC)
    eleven = ten + datetime.timedelta(hours=1)

    def at(hour: datetime.datetime, minute: int) -> datetime.datetime:
        return hour + datetime.timedelta(minutes=minute)

    commits = [
        [(0, at(eleven, 5), 0), (1, at(eleven, 30), 1)],
        [(2, at(ten, 10), 0), (3, at(ten, 40), 0)],
        [(4, at(eleven, 5), 1), (5, at(eleven, 50), 0)],
        [(6, at(ten, 10), 1), (7, at(ten, 20), 0), (8, at(ten, 55), 2)],
    ]
    for rows in commits:
        identity, instant, seq = zip(*rows, strict=True)
        chained.append_arrow_table(
            pyarrow.Table.from_pydict(
                {"identity": list(identity), "at": list(instant), "seq": list(seq)},
                schema=schema,
            ),
            commit_row_size=1_000_000,
        )
    expected = sorted(
        ((instant, seq, identity) for rows in commits for identity, instant, seq in rows),
    )

    events: list[tuple[str, int]] = []
    original = PyArrowFile.open

    def recorded(self: PyArrowFile, *args: object, **kwargs: object) -> object:
        if self.location.endswith(".parquet"):
            events.append(("open", 10 if "2026-08-14-10" in self.location else 11))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(PyArrowFile, "open", recorded)
    found = []
    with chained.read_arrow_reader(order_by=("at", "seq", "identity")) as reader:
        for batch in reader:
            rows = list(
                zip(
                    batch.column("at").to_pylist(),
                    batch.column("seq").to_pylist(),
                    batch.column("identity").to_pylist(),
                    strict=True,
                )
            )
            events.extend(("row", instant.hour) for instant, _, _ in rows)
            found.extend(rows)

    assert found == expected, "every row once, in `(at, seq, identity)` order across both hours"
    assert [hour for kind, hour in events if kind == "open"].count(10) == 2
    assert [hour for kind, hour in events if kind == "open"].count(11) == 2
    first_eleven = events.index(("open", 11))
    assert all(event != ("row", 11) for event in events[:first_eleven])
    assert ("row", 10) not in events[first_eleven:], "hour 10 is finished before 11 opens"
    assert ("open", 10) not in events[first_eleven:]


@scalar
class Sequenced:
    """One event ordered by clock and its source sequence."""

    unix: Annotated[int, primary_key(), sort_key()]
    """Event time."""

    seq: Annotated[int, primary_key(), sort_key()]
    """Source order among events at the same time."""

    payload: str
    """State transition used to make tie order observable."""


@scalar
class NullableSequence:
    """One event whose source may not provide a sequence."""

    unix: Annotated[int, primary_key()]
    """Event time."""

    seq: int | None
    """Optional source sequence."""

    hash: Annotated[int, primary_key()]
    """Stable fallback for an absent sequence."""

    payload: str
    """Observable value."""


def test_an_ordered_read_preserves_sequence_across_equal_time_commits(tmp_path: Path) -> None:
    catalog = IcebergCatalog(name="sequenced", properties=catalog_properties(tmp_path))
    ordered = catalog.dataset("trading.sequenced", field=Sequenced.into_field())
    commits = [
        [(10, 2, "cancel"), (11, 1, "next")],
        [(10, 1, "new"), (10, 3, "fill")],
    ]
    schema = Sequenced.into_field().into_arrow_schema()
    for rows in commits:
        ordered.append_arrow_table(
            pyarrow.Table.from_pylist(
                [dict(zip(("unix", "seq", "payload"), row, strict=True)) for row in rows],
                schema=schema,
            ),
            commit_row_size=1_000_000,
        )

    found = ordered.read_arrow_reader(order_by=("unix", "seq")).read_all()

    assert list(
        zip(
            found.column("unix").to_pylist(),
            found.column("seq").to_pylist(),
            strict=True,
        )
    ) == [
        (10, 1),
        (10, 2),
        (10, 3),
        (11, 1),
    ]
    assert found.column("payload").to_pylist() == ["new", "cancel", "fill", "next"]


def test_an_ordered_read_sorts_a_different_physical_layout_explicitly(
    tmp_path: Path,
) -> None:
    catalog = IcebergCatalog(name="nullable-sequence", properties=catalog_properties(tmp_path))
    ordered = catalog.dataset(
        "trading.nullable_sequence",
        field=NullableSequence.into_field(),
        sort_by=["hash"],
    )
    schema = NullableSequence.into_field().into_arrow_schema()
    for rows in (
        [(10, None, 2, "unknown-2"), (10, 2, 9, "second")],
        [(10, 1, 8, "first"), (10, None, 1, "unknown-1")],
    ):
        ordered.append_arrow_table(
            pyarrow.Table.from_pylist(
                [dict(zip(("unix", "seq", "hash", "payload"), row, strict=True)) for row in rows],
                schema=schema,
            ),
            commit_row_size=1_000_000,
        )

    found = ordered.read_arrow_reader(order_by=("unix", "seq", "hash")).read_all()

    assert ordered.sort_columns() == ["hash"]
    assert found.column("payload").to_pylist() == [
        "first",
        "second",
        "unknown-1",
        "unknown-2",
    ]


def test_an_external_order_uses_bounded_merge_fan_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each sort pass retains one batch from only its configured run fan-in."""
    from rekep.iceberg import dataset as module

    catalog = IcebergCatalog(name="bounded-sort", properties=catalog_properties(tmp_path))
    ordered = catalog.dataset(
        "trading.bounded_sort",
        field=NullableSequence.into_field(),
        sort_by=["hash"],
        table_properties={"write.parquet.row-group-limit": "1"},
    )
    schema = NullableSequence.into_field().into_arrow_schema()
    ordered.append_arrow_table(
        pyarrow.Table.from_pydict(
            {
                "unix": [8, 0, 7, 1, 6, 2, 5, 3, 4],
                "seq": [0] * 9,
                "hash": list(range(9)),
                "payload": [str(index) for index in range(9)],
            },
            schema=schema,
        ),
        commit_row_size=1_000_000,
    )
    merged: list[int] = []
    original = module._merge_batch_streams

    def bounded(streams: Sequence[Any], columns: Sequence[tuple[str, str]]) -> Any:
        merged.append(len(streams))
        return original(streams, columns)

    monkeypatch.setattr(module, "SORT_MERGE_FAN_IN", 2)
    monkeypatch.setattr(module, "_merge_batch_streams", bounded)

    found = ordered.read_arrow_reader(order_by=("unix", "seq", "hash")).read_all()

    assert found.column("unix").to_pylist() == list(range(9))
    assert len(merged) > 1 and max(merged) == 2


def test_overlapping_files_use_bounded_merge_fan_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An appended hour may have many overlapping files; its ordered read
    retains at most one batch from each member of the bounded merge fan-in."""
    from rekep.iceberg import dataset as module

    catalog = IcebergCatalog(name="bounded-files", properties=catalog_properties(tmp_path))
    ordered = catalog.dataset("trading.bounded_files", field=Timed.into_field())
    for index in range(5):
        ordered.append_arrow_table(timed(index, index + 10), commit_row_size=1_000_000)

    merged: list[int] = []
    original = module._merge_batch_streams

    def bounded(streams: Sequence[Any], columns: Sequence[tuple[str, str]]) -> Any:
        merged.append(len(streams))
        return original(streams, columns)

    monkeypatch.setattr(module, "SORT_MERGE_FAN_IN", 2)
    monkeypatch.setattr(module, "_merge_batch_streams", bounded)

    found = ordered.read_arrow_reader(order_by="unix").read_all()

    assert found.column("unix").to_pylist() == sorted([*range(5), *range(10, 15)])
    assert len(merged) > 1 and max(merged) == 2


def test_overlapping_file_spill_cleans_scratch_on_close_and_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rekep.iceberg import dataset as module

    catalog = IcebergCatalog(name="spill-cleanup", properties=catalog_properties(tmp_path))
    ordered = catalog.dataset("trading.spill_cleanup", field=Timed.into_field())
    for index in range(5):
        ordered.append_arrow_table(timed(index, index + 10), commit_row_size=1_000_000)

    original_temporary_directory = module.tempfile.TemporaryDirectory
    scratch: list[Path] = []

    def tracked_directory(*args: Any, **kwargs: Any) -> Any:
        held = original_temporary_directory(*args, **kwargs)
        scratch.append(Path(held.name))
        return held

    monkeypatch.setattr(module, "SORT_MERGE_FAN_IN", 2)
    monkeypatch.setattr(module.tempfile, "TemporaryDirectory", tracked_directory)

    reader = ordered.read_arrow_reader(order_by="unix")
    first = reader.read_next_batch()
    assert first.num_rows
    assert scratch and all(path.exists() for path in scratch)
    reader.close()
    assert all(not path.exists() for path in scratch)
    assert first.column("unix")[0].as_py() == 0

    scratch.clear()

    def refused(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("refused ordered merge")

    monkeypatch.setattr(module, "_row_key", refused)
    reader = ordered.read_arrow_reader(order_by="unix")
    with pytest.raises(RuntimeError, match="refused ordered merge"):
        reader.read_next_batch()
    assert scratch and all(not path.exists() for path in scratch)


def test_a_nearly_right_batch_is_cast_on_the_way_in(dataset: IcebergDataset) -> None:
    """Wrong order, a narrow integer, a column the source never produced."""
    assert dataset.merge_schema is False
    dataset.get_or_create_table()
    batch = pyarrow.RecordBatch.from_pydict(
        {
            "day": [datetime.date(2026, 8, 14)],
            "size": pyarrow.array([7], type=pyarrow.int16()),
            "symbol": ["A"],
            "noise": ["dropped"],
        }
    )
    dataset.append_arrow_reader(pyarrow.RecordBatchReader.from_batches(batch.schema, [batch]))
    stored = dataset.read_arrow_table(Quote.into_field())
    assert stored.column("size").to_pylist() == [7]
    assert stored.column("venue").to_pylist() == [None], "the missing nullable column was filled"
    assert "noise" not in dataset.iceberg_table.schema()


def test_commit_row_size_commits_one_snapshot_per_chunk(dataset: IcebergDataset) -> None:
    dataset.append_arrow_reader(quotes(6).to_reader(max_chunksize=1), commit_row_size=2)
    assert len(dataset.iceberg_table.history()) == 3
    assert dataset.read_arrow_table().num_rows == 6


def test_an_empty_stream_commits_nothing(dataset: IcebergDataset) -> None:
    dataset.get_or_create_table()
    schema = Quote.into_field().into_arrow_schema()
    dataset.append_arrow_reader(pyarrow.RecordBatchReader.from_batches(schema, []))
    assert dataset.iceberg_table.history() == []


class _ClosableBatches:
    """A batch source that records the writer releasing it."""

    def __init__(self, batches: Sequence[pyarrow.RecordBatch]) -> None:
        self.batches = iter(batches)
        self.closed = False
        self.close_calls = 0

    def __iter__(self) -> "_ClosableBatches":
        return self

    def __next__(self) -> pyarrow.RecordBatch:
        return next(self.batches)

    def close(self) -> None:
        self.close_calls += 1
        self.closed = True


def _owned_reader(source: Iterator[pyarrow.RecordBatch]) -> pyarrow.RecordBatchReader:
    """Expose a tracked Python iterator through one schema-bearing Arrow stream."""
    return OwnedRecordBatchReader(Quote.into_field().into_arrow_schema(), source, lambda: None)


def _written(dataset: IcebergDataset, verb: str, reader: pyarrow.RecordBatchReader) -> int:
    """One streamed write through the verb named, one row per commit."""
    if verb == "blind":
        return dataset.append_arrow_reader(reader, merge_by=False, commit_row_size=1)
    if verb == "append":
        return dataset.append_arrow_reader(reader, commit_row_size=1)
    if verb == "merge":
        return dataset.merge_arrow_reader(reader, commit_row_size=1)
    return dataset.overwrite_arrow_reader(reader, commit_row_size=1)


#: The method each verb of `_written` commits one chunk through.
CHUNK_METHODS = {
    "blind": "_append_chunk",
    "append": "_append_key_chunk",
    "merge": "_merge_chunk",
    "partitions": "_replace_chunk",
}


@pytest.mark.parametrize("verb", list(CHUNK_METHODS))
def test_a_completed_stream_write_closes_its_source_once(
    dataset: IcebergDataset, verb: str
) -> None:
    batches = _ClosableBatches(quotes(2).to_batches(max_chunksize=1))

    assert _written(dataset, verb, _owned_reader(batches)) == 2

    assert batches.close_calls == 1


@pytest.mark.parametrize(("verb", "chunk_method"), list(CHUNK_METHODS.items()))
def test_a_failed_stream_write_closes_its_source(
    dataset: IcebergDataset,
    monkeypatch: pytest.MonkeyPatch,
    verb: str,
    chunk_method: str,
) -> None:
    dataset.get_or_create_table()
    batches = _ClosableBatches(quotes(2).to_batches(max_chunksize=1))

    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("commit stopped")

    monkeypatch.setattr(dataset, chunk_method, fail)
    with pytest.raises(RuntimeError, match="commit stopped"):
        _written(dataset, verb, _owned_reader(batches))

    assert batches.closed


def _observed_writes(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> tuple[list[str], list[str]]:
    """Data files opened for writing in the store, and any of them opened again."""
    io = dataset.get_or_create_table().io
    original_output = io.new_output
    original_input = io.new_input
    written: list[str] = []
    reopened: list[str] = []

    def opened_for_writing(location: str) -> Any:
        if location.endswith(".parquet"):
            written.append(location)
        return original_output(location)

    def opened(location: str) -> Any:
        if location in written:
            reopened.append(location)
        return original_input(location)

    monkeypatch.setattr(io, "new_output", opened_for_writing)
    monkeypatch.setattr(io, "new_input", opened)
    return written, reopened


def _iceberg_artifacts(dataset: IcebergDataset) -> set[Path]:
    root = local(dataset.get_or_create_table().location())
    return {
        path.resolve()
        for path in root.rglob("*")
        if path.is_file()
        and (path.suffix in {".avro", ".parquet"} or path.name.endswith(".metadata.json"))
    }


def test_every_write_path_streams_one_file_per_partition_into_the_store(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One writer for every verb: a Parquet file per partition, streamed once
    through the table's FileIO and committed by path, with nothing on local
    disk and nothing read back to describe it."""
    with monkeypatch.context() as observed:
        written, reopened = _observed_writes(dataset, observed)
        assert dataset.append_arrow_table(quotes(2)) == 2
        assert dataset.merge_arrow_table(keyed("N", 2)) == 2

    assert len(written) == 2, "one file per write, one partition each"
    assert reopened == [], "the footer the writer closed supplied every DataFile metric"
    assert set(written) == set(dataset.data_files().column("file_path").to_pylist()), (
        "every file written is the one committed, under the location it was opened at"
    )
    assert {row["symbol"] for row in dataset.read_arrow_table().to_pylist()} == {
        "S0",
        "S1",
        "N0",
        "N1",
    }


def test_a_merge_writes_one_file_and_empties_the_file_it_replaces(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset.append_arrow_table(quotes(2))
    changed = quotes(3, "XETR")

    with monkeypatch.context() as observed:
        written, reopened = _observed_writes(dataset, observed)
        assert dataset.merge_arrow_table(changed, properties={"rekep.test": "staged-merge"}) == 3

    assert len(written) == 1, "one file carries the chunk; the emptied file is gone"
    assert reopened == []
    assert set(written) == set(dataset.data_files().column("file_path").to_pylist())
    assert stored_sizes(dataset) == {"S0": 0, "S1": 1, "S2": 2}
    assert set(dataset.read_arrow_table().column("venue").to_pylist()) == {"XETR"}


def test_a_failed_partitioned_append_removes_direct_writer_outputs(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table.update.snapshot import _FastAppendFiles

    dataset.get_or_create_table()
    before = _iceberg_artifacts(dataset)

    def refused(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("snapshot refused")

    monkeypatch.setattr(_FastAppendFiles, "append_data_file", refused)
    with pytest.raises(RuntimeError, match="snapshot refused"):
        dataset.append_arrow_table(quotes(2))

    assert _iceberg_artifacts(dataset) == before
    assert dataset.iceberg_table.history() == []


def test_a_retryable_direct_writer_failure_cleans_every_attempt(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table.update.snapshot import _FastAppendFiles

    dataset.get_or_create_table()
    before = _iceberg_artifacts(dataset)
    attempts = 0

    def refused(*_args: Any, **_kwargs: Any) -> None:
        nonlocal attempts
        attempts += 1
        raise OSError("direct writer stopped")

    monkeypatch.setattr(dataset, "retry_backoff", 0.0)
    monkeypatch.setattr(_FastAppendFiles, "append_data_file", refused)
    with pytest.raises(OSError, match="direct writer stopped"):
        dataset.append_arrow_table(quotes(2), merge_by=False)

    assert attempts == dataset.commit_retries + 1
    assert _iceberg_artifacts(dataset) == before
    assert dataset.iceberg_table.history() == []


def test_a_refused_catalog_commit_removes_unreferenced_outputs(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    dataset.get_or_create_table()
    before = _iceberg_artifacts(dataset)

    def refused(_transaction: Transaction) -> None:
        raise RuntimeError("catalog refused")

    monkeypatch.setattr(Transaction, "commit_transaction", refused)
    with pytest.raises(RuntimeError, match="catalog refused"):
        dataset.append_arrow_table(quotes(2))

    assert _iceberg_artifacts(dataset) == before
    assert dataset.iceberg_table.history() == []


def test_a_lost_commit_acknowledgement_keeps_files_the_snapshot_references(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    dataset.get_or_create_table()
    original = Transaction.commit_transaction

    def acknowledged(transaction: Transaction) -> None:
        original(transaction)
        raise RuntimeError("acknowledgement lost")

    monkeypatch.setattr(Transaction, "commit_transaction", acknowledged)
    with pytest.raises(RuntimeError, match="acknowledgement lost"):
        dataset.append_arrow_table(quotes(2))

    dataset.refresh()
    io = dataset.iceberg_table.io
    assert all(
        io.new_input(path).exists() for path in dataset.data_files()["file_path"].to_pylist()
    )
    assert dataset.read_arrow_table().num_rows == 2


def test_a_transient_commit_failure_retries_once(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.table import Transaction

    commit = Transaction.commit_transaction
    attempts = 0

    def transient(self: Transaction) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise CommitFailedException("concurrent commit")
        commit(self)

    monkeypatch.setattr(dataset, "retry_backoff", 0.0)
    monkeypatch.setattr(Transaction, "commit_transaction", transient)

    assert dataset.append_arrow_table(quotes(2), merge_by=False) == 2
    assert attempts == 2
    assert len(dataset.iceberg_table.snapshots()) == 1
    assert dataset.read_arrow_table().num_rows == 2


def test_a_lost_transient_commit_acknowledgement_is_not_replayed(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.exceptions import CommitStateUnknownException
    from pyiceberg.table import Transaction

    commit = Transaction.commit_transaction
    attempts = 0

    def committed(transaction: Transaction) -> None:
        nonlocal attempts
        attempts += 1
        commit(transaction)
        raise CommitStateUnknownException("commit acknowledgement lost")

    monkeypatch.setattr(dataset, "retry_backoff", 0.0)
    monkeypatch.setattr(Transaction, "commit_transaction", committed)

    assert dataset.append_arrow_table(quotes(2)) == 2
    assert attempts == 1, "the operation id found the commit before replaying it"
    assert len(dataset.iceberg_table.snapshots()) == 1
    assert dataset.read_arrow_table().num_rows == 2


def test_an_interrupt_after_commit_keeps_partitioned_files_live(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    original = Transaction.commit_transaction

    def committed_then_interrupted(transaction: Transaction) -> None:
        original(transaction)
        raise KeyboardInterrupt

    monkeypatch.setattr(Transaction, "commit_transaction", committed_then_interrupted)
    with pytest.raises(KeyboardInterrupt):
        dataset.append_arrow_table(quotes(2), commit_row_size=1_000_000)

    stored = dataset.refresh().read_arrow_table()
    assert stored.num_rows == 2
    io = dataset.iceberg_table.io
    assert all(
        io.new_input(path).exists() for path in dataset.data_files()["file_path"].to_pylist()
    )


def test_a_snapshot_construction_interrupt_removes_partition_stages(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table.update.snapshot import _FastAppendFiles

    before = _iceberg_artifacts(dataset)

    def interrupted(*_args: Any, **_kwargs: Any) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(_FastAppendFiles, "append_data_file", interrupted)
    with pytest.raises(KeyboardInterrupt):
        dataset.append_arrow_table(quotes(2), commit_row_size=1_000_000)

    assert _iceberg_artifacts(dataset) == before


def test_a_refused_partitioned_merge_removes_rewrites_and_avro(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    dataset.append_arrow_table(quotes(2), commit_row_size=1_000_000)
    before = _iceberg_artifacts(dataset)

    def refused(_transaction: Transaction) -> None:
        raise RuntimeError("catalog refused")

    monkeypatch.setattr(Transaction, "commit_transaction", refused)
    with pytest.raises(RuntimeError, match="catalog refused"):
        dataset.merge_arrow_table(quotes(1, "XETR"), commit_row_size=1_000_000)

    assert _iceberg_artifacts(dataset) == before
    assert set(dataset.refresh().read_arrow_table().column("venue").to_pylist()) == {"XPAR"}


def test_a_refused_unpartitioned_merge_removes_rewrites_and_avro(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    @scalar
    class FlatQuote:
        symbol: Annotated[str, primary_key()]
        size: int

    catalog = IcebergCatalog(name="flat-refusal", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.flat_refusal", field=FlatQuote.into_field())
    schema = FlatQuote.into_field().into_arrow_schema()
    dataset.append_arrow_table(
        pyarrow.Table.from_pydict({"symbol": ["S0", "S1"], "size": [0, 1]}, schema=schema),
        commit_row_size=1_000_000,
    )
    before = _iceberg_artifacts(dataset)

    def refused(_transaction: Transaction) -> None:
        raise RuntimeError("catalog refused")

    monkeypatch.setattr(Transaction, "commit_transaction", refused)
    with pytest.raises(RuntimeError, match="catalog refused"):
        dataset.merge_arrow_table(
            pyarrow.Table.from_pydict({"symbol": ["S0"], "size": [2]}, schema=schema),
            commit_row_size=1_000_000,
        )

    assert _iceberg_artifacts(dataset) == before
    assert dataset.refresh().read_arrow_table().column("size").to_pylist() == [0, 1]


def test_a_metadata_write_followed_by_catalog_refusal_leaves_no_artifact(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = dataset.catalog
    dataset.append_arrow_table(quotes(2), commit_row_size=1_000_000)
    before = _iceberg_artifacts(dataset)
    write = catalog._write_metadata

    def wrote_then_refused(*args: Any, **kwargs: Any) -> None:
        write(*args, **kwargs)
        raise RuntimeError("catalog refused after metadata")

    monkeypatch.setattr(catalog, "_write_metadata", wrote_then_refused)
    with pytest.raises(RuntimeError, match="refused after metadata"):
        dataset.merge_arrow_table(quotes(1, "XETR"), commit_row_size=1_000_000)

    assert _iceberg_artifacts(dataset) == before
    assert set(dataset.refresh().read_arrow_table().column("venue").to_pylist()) == {"XPAR"}


def test_a_custom_file_io_metadata_refusal_leaves_no_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    properties = {
        **catalog_properties(tmp_path),
        "py-io-impl": f"{__name__}.CustomArrowFileIO",
    }
    catalog = IcebergCatalog(name="custom-refusal", properties=properties)
    dataset = catalog.dataset("t.custom_refusal", field=Quote.into_field())
    dataset.append_arrow_table(quotes(2), commit_row_size=1_000_000)
    before = _iceberg_artifacts(dataset)
    write = catalog.catalog._write_metadata

    def wrote_then_refused(*args: Any, **kwargs: Any) -> None:
        write(*args, **kwargs)
        raise RuntimeError("custom catalog refused after metadata")

    monkeypatch.setattr(catalog.catalog, "_write_metadata", wrote_then_refused)
    with pytest.raises(RuntimeError, match="refused after metadata"):
        dataset.merge_arrow_table(quotes(1, "XETR"), commit_row_size=1_000_000)

    assert _iceberg_artifacts(dataset) == before
    assert set(dataset.refresh().read_arrow_table().column("venue").to_pylist()) == {"XPAR"}


def test_a_refused_manifest_merge_removes_superseded_avro(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    dataset.set_properties({"commit.manifest.min-count-to-merge": "2"})
    dataset.append_arrow_table(quotes(1), commit_row_size=1_000_000)
    before = _iceberg_artifacts(dataset)

    def refused(_transaction: Transaction) -> None:
        raise RuntimeError("catalog refused")

    monkeypatch.setattr(Transaction, "commit_transaction", refused)
    with pytest.raises(RuntimeError, match="catalog refused"):
        dataset.append_arrow_table(other_day(1), commit_row_size=1_000_000)

    assert _iceberg_artifacts(dataset) == before


def test_a_snapshot_construction_failure_removes_direct_writer_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table.update.snapshot import _FastAppendFiles

    @scalar
    class FlatQuote:
        symbol: Annotated[str, primary_key()]
        size: int

    catalog = IcebergCatalog(name="flat-build", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.flat_build", field=FlatQuote.into_field())
    schema = FlatQuote.into_field().into_arrow_schema()
    dataset.append_arrow_table(
        pyarrow.Table.from_pydict({"symbol": ["S0"], "size": [0]}, schema=schema),
        commit_row_size=1_000_000,
    )
    before = _iceberg_artifacts(dataset)

    def refused(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("snapshot construction refused")

    monkeypatch.setattr(_FastAppendFiles, "append_data_file", refused)
    with pytest.raises(RuntimeError, match="construction refused"):
        dataset.append_arrow_table(
            pyarrow.Table.from_pydict({"symbol": ["S2"], "size": [2]}, schema=schema),
            commit_row_size=1_000_000,
        )

    assert _iceberg_artifacts(dataset) == before


# -- merging --------------------------------------------------------------


def test_merge_by_true_replaces_on_the_declared_key(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(3, "XPAR"))
    assert dataset.merge_arrow_table(quotes(3, "XETR"), merge_by=True) == 3
    stored = dataset.read_arrow_table()
    assert stored.num_rows == 3, "the same keys came back, not three more rows"
    assert set(stored.column("venue").to_pylist()) == {"XETR"}


def test_merge_by_names_replaces_on_those(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2, "XPAR"))
    assert dataset.merge_arrow_table(quotes(2, "XETR"), merge_by=["symbol", "day"]) == 2
    assert dataset.read_arrow_table().num_rows == 2


def test_a_merge_lands_each_key_once_and_writes_only_what_changed(
    dataset: IcebergDataset,
) -> None:
    """A strict, non-contiguous subset changes; only that subset is written,
    and the stored keys stay unique."""
    dataset.append_arrow_table(quotes(5, "XPAR"))
    changed = quotes(5, "XPAR")
    venues = changed.column("venue").to_pylist()
    venues[1] = venues[3] = "XETR"
    changed = changed.set_column(
        changed.schema.get_field_index("venue"),
        changed.schema.field("venue"),
        pyarrow.array(venues, changed.schema.field("venue").type),
    )

    assert dataset.merge_arrow_table(changed) == 2, "the two rows that changed"

    stored = dataset.read_arrow_table().sort_by("symbol")
    assert stored.num_rows == 5, "a merge replaces a row, it never adds a copy"
    assert len(set(stored.column("symbol").to_pylist())) == 5, "no key is duplicated"
    assert stored.column("venue").to_pylist() == ["XPAR", "XETR", "XPAR", "XETR", "XPAR"]


def test_an_extension_typed_key_is_replaced_by_the_bytes_it_holds(tmp_path: Path) -> None:
    """An extension type carries no kernel of its own, so the key is joined
    and bounded as its storage: the sixteen bytes a UUID is."""
    import uuid as uuidlib

    schema = pyarrow.schema(
        [
            pyarrow.field(
                "id", pyarrow.uuid(), nullable=False, metadata={"ICEBERG:primary_key": "true"}
            ),
            pyarrow.field("size", pyarrow.int64()),
        ]
    )
    field = Field.from_arrow_schema(schema, name="Held")
    rows = IcebergCatalog(name="test", properties=catalog_properties(tmp_path)).dataset(
        "trading.held", field=field
    )
    keys = pyarrow.ExtensionArray.from_storage(
        pyarrow.uuid(),
        pyarrow.array([uuidlib.UUID(int=1).bytes, uuidlib.UUID(int=2).bytes], pyarrow.binary(16)),
    )

    def held(size: int) -> pyarrow.Table:
        return pyarrow.table(
            {"id": keys, "size": pyarrow.array([size, size], pyarrow.int64())}, schema=schema
        )

    assert rows.merge_arrow_table(held(1), field) == 2
    assert rows.merge_arrow_table(held(2), field) == 2, "a changed row replaces its key's"
    assert rows.merge_arrow_table(held(2), field) == 0, "and a replay writes nothing"
    stored = rows.refresh().read_arrow_table()
    assert stored.num_rows == 2
    assert set(stored.column("size").to_pylist()) == {2}


@pytest.mark.parametrize("kind", ["string", "int64", "uuid"])
def test_a_blind_append_and_a_partition_scoped_merge_use_any_declared_identifier(
    tmp_path: Path, kind: str
) -> None:
    """Iceberg keys are native Field declarations, not a FIX column name."""
    import uuid as uuidlib

    dtype = {
        "string": pyarrow.string(),
        "int64": pyarrow.int64(),
        "uuid": pyarrow.uuid(),
    }[kind]
    schema = pyarrow.schema(
        [
            pyarrow.field(
                "identity",
                dtype,
                nullable=False,
                metadata={**primary_key()["metadata"], **sort_key()["metadata"]},
            ),
            pyarrow.field("part", pyarrow.string(), metadata=partition_key()["metadata"]),
            pyarrow.field("value", pyarrow.int64()),
        ]
    )
    field = Field.from_arrow_schema(schema, name=f"Generic{kind}")
    dataset = IcebergCatalog(
        name=f"generic-{kind}", properties=catalog_properties(tmp_path)
    ).dataset(f"trading.generic_{kind}", field=field)
    identity = {"string": "A", "int64": 7, "uuid": uuidlib.UUID(int=7)}[kind]

    def rows(parts: Sequence[str], values: Sequence[int]) -> pyarrow.Table:
        identities = [identity] * len(parts)
        if kind == "uuid":
            keys = pyarrow.ExtensionArray.from_storage(
                pyarrow.uuid(),
                pyarrow.array([value.bytes for value in identities], pyarrow.binary(16)),
            )
        else:
            keys = pyarrow.array(identities, dtype)
        return pyarrow.Table.from_arrays(
            [keys, pyarrow.array(parts), pyarrow.array(values, pyarrow.int64())], schema=schema
        )

    dataset.append_arrow_table(rows(["old", "old", "kept"], [1, 2, 3]), merge_by=False)
    assert dataset.read_arrow_table(field).num_rows == 3, "append keeps repeated identifiers"

    assert dataset.merge_arrow_table(rows(["old"], [9]), field) == 1

    stored = dataset.read_arrow_table(field).to_pylist()
    assert sorted((row["part"], row["value"]) for row in stored) == [("kept", 3), ("old", 9)]


def test_an_overwrite_replaces_complete_partitions_from_a_stream(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(quotes(3))
    dataset.append_arrow_table(other_day(2))
    replacement = keyed("N", 5)

    dataset.overwrite_arrow_reader(replacement.to_reader(max_chunksize=1), commit_row_size=2)

    stored = dataset.read_arrow_table().to_pylist()
    today = [row for row in stored if row["day"] == datetime.date(2026, 8, 14)]
    tomorrow = [row for row in stored if row["day"] == datetime.date(2026, 8, 15)]
    assert {row["symbol"] for row in today} == {f"N{index}" for index in range(5)}
    assert {row["symbol"] for row in tomorrow} == {"D0", "D1"}
    today_files = [
        row
        for row in dataset.data_files().to_pylist()
        if row["partition"]["day"] == datetime.date(2026, 8, 14)
    ]
    assert sorted(row["record_count"] for row in today_files) == [1, 2, 2]


def test_partition_staging_streams_each_partition_into_the_store_once(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.append_arrow_table(other_day(2))
    source = pyarrow.Table.from_batches([*keyed("N", 3).to_batches(), *other_day(2).to_batches()])
    before = len(dataset.iceberg_table.history())

    with monkeypatch.context() as observed:
        written, reopened = _observed_writes(dataset, observed)
        observed.setattr(
            pyarrow,
            "concat_tables",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("collected")),
        )
        dataset.overwrite_arrow_reader(
            source.to_reader(max_chunksize=1),
            commit_row_size=2,
            properties={"rekep.test": "staged"},
        )

    assert reopened == [], "the footer the writer closed supplied every DataFile metric"
    assert len(dataset.iceberg_table.history()) - before == 3, "one snapshot per bounded commit"
    assert all(
        snapshot.summary["rekep.test"] == "staged"
        for snapshot in dataset.iceberg_table.snapshots()[-3:]
    )
    files = dataset.data_files().to_pylist()
    assert len(files) == 4, "a file per partition per commit, and nothing of what was stored"
    assert set(written) == {row["file_path"] for row in files}, (
        "every file written landed, and nothing else was written"
    )
    assert all(row["record_count"] <= 2 for row in files)
    assert all("day=" in row["file_path"] for row in files)
    stored = dataset.read_arrow_table().to_pylist()
    assert {row["symbol"] for row in stored if row["day"] == datetime.date(2026, 8, 14)} == {
        "N0",
        "N1",
        "N2",
    }


def test_a_failed_partition_commit_removes_unreferenced_stages(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table.update.snapshot import _OverwriteFiles

    dataset.append_arrow_table(quotes(2))
    before = {row["file_path"] for row in dataset.data_files().to_pylist()}
    io = dataset.iceberg_table.io

    def refused(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("catalog refused")

    written, _ = _observed_writes(dataset, monkeypatch)
    monkeypatch.setattr(_OverwriteFiles, "append_data_file", refused)
    with pytest.raises(RuntimeError, match="catalog refused"):
        dataset.overwrite_arrow_reader(quotes(3).to_reader(max_chunksize=1), commit_row_size=2)

    assert written
    assert all(not io.new_input(path).exists() for path in written)
    assert {row["file_path"] for row in dataset.refresh().data_files().to_pylist()} == before


def test_a_delete_drops_the_rewritten_copy_of_a_file_it_keeps(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A file whose bounds admit the filter but whose rows do not match is
    read, rewritten into a stage, and then kept as it was. The copy goes as
    soon as that is known: it was never committed, so leaving it for the way
    out would mean asking the catalog whether it is live -- a table load and
    a manifest walk, at the end of an operation that succeeded."""
    from rekep.iceberg.dataset import _PartitionStager

    day = datetime.date(2026, 8, 14)
    dataset.append_arrow_table(
        pyarrow.Table.from_pydict(
            {"symbol": ["A", "C"], "day": [day, day], "size": [1, 3], "venue": ["XPAR"] * 2},
            schema=Quote.into_field().into_arrow_schema(),
        ),
    )
    before = _iceberg_artifacts(dataset)

    discarded: list[tuple[str, ...]] = []
    discard = _PartitionStager.discard

    def recorded(self: _PartitionStager, partitions: Sequence[Any]) -> None:
        discarded.extend(partition.paths for partition in partitions)
        discard(self, partitions)

    monkeypatch.setattr(_PartitionStager, "discard", recorded)
    assert dataset.delete_where("symbol = 'B'") == 0, "between the file's bounds, matching nothing"

    assert [paths for paths in discarded if paths], "the copy it wrote was dropped"
    assert not any(
        Path(pyarrow.fs.FileSystem.from_uri(path)[1]).exists()
        for paths in discarded
        for path in paths
    )
    assert _iceberg_artifacts(dataset) == before
    assert {row["symbol"] for row in dataset.read_arrow_table().to_pylist()} == {"A", "C"}


def test_partition_cleanup_attempts_every_output_without_masking_the_source_error(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rekep.iceberg.dataset import _PartitionStager

    table = dataset.get_or_create_table()
    attempted: list[str] = []

    def refused(path: str) -> None:
        attempted.append(path)
        raise OSError(f"cannot remove {path}")

    monkeypatch.setattr(table.io, "delete", refused)
    with pytest.raises(RuntimeError, match="source failed"):
        with _PartitionStager(table, (), 1) as stager:
            stager.outputs.update({"first.parquet", "second.parquet"})
            raise RuntimeError("source failed")
    assert set(attempted) == {"first.parquet", "second.parquet"}

    attempted.clear()
    with pytest.raises(ExceptionGroup, match="partition staging cleanup failed") as caught:
        with _PartitionStager(table, (), 1) as stager:
            stager.outputs.update({"first.parquet", "second.parquet"})
    assert set(attempted) == {"first.parquet", "second.parquet"}
    assert len(caught.value.exceptions) == 2


def test_a_retried_staged_commit_still_has_the_files_it_committed(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A refusal the catalog is definite about deletes what the attempt made,
    but a retry commits the same staged files -- so those are the stager's to
    delete on the way out, not the refusal's."""
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.table import Transaction

    commit = Transaction.commit_transaction
    attempts = 0

    def contended(self: Transaction) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise CommitFailedException("another writer won")
        commit(self)

    monkeypatch.setattr(dataset, "retry_backoff", 0.0)
    monkeypatch.setattr(Transaction, "commit_transaction", contended)

    source = pyarrow.concat_tables([quotes(1), other_day(1)])
    dataset.append_arrow_reader(
        source.to_reader(max_chunksize=1), merge_by=False, commit_row_size=1_000_000
    )

    monkeypatch.undo()
    assert attempts == 2
    stored = dataset.refresh()
    io = stored.iceberg_table.io
    paths = stored.data_files().column("file_path").to_pylist()
    assert paths and all(io.new_input(path).exists() for path in paths)
    assert stored.read_arrow_table().num_rows == 2


def _replaced(dataset: IcebergDataset, verb: str, rows: pyarrow.Table, **options: Any) -> int:
    """One write that takes stored rows out: a `merge` by key, or an `overwrite`
    of the partitions `rows` touches."""
    if verb == "merge":
        return dataset.merge_arrow_table(rows, **options)
    return dataset.overwrite_arrow_table(rows, **options)


@pytest.mark.parametrize("verb", ["merge", "overwrite"])
def test_a_contended_replacement_is_handed_back_rather_than_rebuilt(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, verb: str
) -> None:
    """The files a replacement takes out were planned against the head that
    moved, so it raises for a fresh plan instead of committing a stale one --
    and leaves nothing it uploaded behind."""
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.table import Transaction

    dataset.append_arrow_table(quotes(2), commit_row_size=1_000_000)
    before = _iceberg_artifacts(dataset)

    def contended(_self: Transaction) -> None:
        raise CommitFailedException("another writer won")

    monkeypatch.setattr(dataset, "retry_backoff", 0.0)
    monkeypatch.setattr(Transaction, "commit_transaction", contended)
    with pytest.raises(CommitFailedException, match="another writer won"):
        _replaced(dataset, verb, quotes(2, "XETR"))

    monkeypatch.undo()
    assert _iceberg_artifacts(dataset) == before
    assert set(dataset.refresh().read_arrow_table().column("venue").to_pylist()) == {"XPAR"}


def _beaten_once(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, concurrent: Callable[[], None]
) -> Callable[[], int]:
    """Land `concurrent` the first time this dataset's catalog is asked to commit,
    and report that commit as beaten -- which PyIceberg retries on its own
    against the refreshed head, validating the retry against what landed in
    between. Returns how many commits the catalog was asked for."""
    from pyiceberg.exceptions import CommitFailedException

    catalog = dataset.catalog
    original = catalog.commit_table
    attempts = 0

    def beaten(*args: Any, **kwargs: Any) -> Any:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            concurrent()
            raise CommitFailedException("another writer won")
        return original(*args, **kwargs)

    monkeypatch.setattr(catalog, "commit_table", beaten)
    return lambda: attempts


def _another_writer(dataset: IcebergDataset) -> IcebergDataset:
    """A second handle on the same table, through a connection of its own.

    The same catalog *name*, because a SQL catalog keys its tables by it: a
    handle under another name would create a second table over the same
    files rather than race this one.
    """
    return IcebergCatalog(name=dataset.catalog_name, properties=dataset.catalog_properties).dataset(
        dataset.identifier, field=dataset.field
    )


@pytest.mark.parametrize("verb", ["merge", "overwrite"])
def test_a_replacement_beaten_by_an_unrelated_append_lands_on_the_retry(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, verb: str
) -> None:
    """A replacement declares the rows it takes out, so the retry PyIceberg
    makes past another writer is validated against those rows alone: a row
    landed in another day is no conflict, and the retry lands without a
    fresh plan."""
    dataset.append_arrow_table(quotes(2), commit_row_size=1_000_000)
    another = _another_writer(dataset)
    attempts = _beaten_once(dataset, monkeypatch, lambda: another.append_arrow_table(other_day(1)))

    assert _replaced(dataset, verb, quotes(2, "XETR")) == 2

    assert attempts() == 2, "PyIceberg's own retry landed it"
    stored = dataset.refresh().read_arrow_table().to_pylist()
    assert {row["venue"] for row in stored if row["day"] == datetime.date(2026, 8, 14)} == {"XETR"}
    assert [row["symbol"] for row in stored if row["day"] == datetime.date(2026, 8, 15)] == ["D0"]


@pytest.mark.parametrize("verb", ["merge", "overwrite"])
def test_a_replacement_beaten_by_a_conflicting_append_is_handed_back(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, verb: str
) -> None:
    """A row landed under the rows this replacement takes out -- one of its
    keys, or one of its partitions -- is a row the plan never saw, so the
    retry is refused and the write is handed back for a fresh plan, with
    nothing of its own left behind."""
    from pyiceberg.exceptions import CommitFailedException

    dataset.append_arrow_table(quotes(2), commit_row_size=1_000_000)
    another = _another_writer(dataset)
    _beaten_once(
        dataset,
        monkeypatch,
        lambda: another.append_arrow_table(quotes(1, "later"), merge_by=False),
    )

    with pytest.raises(CommitFailedException, match="changed since this write was planned"):
        _replaced(dataset, verb, quotes(2, "XETR"))

    stored = dataset.refresh().read_arrow_table().to_pylist()
    assert sorted(row["venue"] for row in stored) == ["XPAR", "XPAR", "later"]
    io = dataset.iceberg_table.io
    paths = dataset.data_files().column("file_path").to_pylist()
    assert len(paths) == 2 and all(io.new_input(path).exists() for path in paths)


@pytest.mark.parametrize("verb", ["merge", "partitions", "delete"])
def test_an_overwrite_declares_the_rows_it_takes_out(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, verb: str
) -> None:
    """What each verb hands PyIceberg as the rows it takes out: the key bounds
    a merge planned by, the partition sources an overwrite empties, and the
    predicate a delete names."""
    from pyiceberg.table.update.snapshot import _OverwriteFiles

    dataset.append_arrow_table(quotes(3))
    declared: list[Any] = []
    original = _OverwriteFiles.delete_by_predicate

    def recorded(self: Any, predicate: Any, case_sensitive: bool = True) -> None:
        declared.append(predicate)
        original(self, predicate, case_sensitive)

    monkeypatch.setattr(_OverwriteFiles, "delete_by_predicate", recorded)
    if verb == "merge":
        assert dataset.merge_arrow_table(quotes(2, "XETR")) == 2
    elif verb == "partitions":
        assert dataset.overwrite_arrow_table(quotes(2, "XETR")) == 2
    else:
        assert dataset.delete_where("size = 1") == 1

    assert len(declared) == 1, "one overwrite, one declaration"
    spelled = str(declared[0])
    if verb == "merge":
        assert "symbol" in spelled and "day" in spelled
    elif verb == "partitions":
        assert "day" in spelled and "symbol" not in spelled
    else:
        assert "size" in spelled


def test_an_interrupt_between_a_commit_and_its_handover_keeps_the_rows(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The stager deletes on the way out whatever it still owns, and a commit
    the catalog accepted hands its files over on the line after. An interrupt
    in between used to leave the stager deleting the data files of a live
    snapshot, so it asks whether they are live before deleting any."""
    from rekep.iceberg.dataset import _PartitionStager

    def interrupted(_self: _PartitionStager, _partitions: Any) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(_PartitionStager, "release", interrupted)
    with pytest.raises(KeyboardInterrupt):
        dataset.append_arrow_table(quotes(2))

    monkeypatch.undo()
    stored = dataset.refresh()
    io = stored.iceberg_table.io
    paths = stored.data_files().column("file_path").to_pylist()
    assert paths, "the commit landed"
    assert all(io.new_input(path).exists() for path in paths)
    assert stored.read_arrow_table().num_rows == 2


def test_partition_cleanup_survives_an_interrupt_closing_its_open_file(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An interrupt is not an ordinary error, and the files already written
    are still this object's to delete when one arrives."""
    from rekep.iceberg.dataset import _PartitionStager

    table = dataset.get_or_create_table()
    deleted: list[str] = []
    monkeypatch.setattr(table.io, "delete", lambda path: deleted.append(path))

    def interrupted(**_kwargs: object) -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        with _PartitionStager(table, (), 1) as stager:
            stager.outputs.update({"first.parquet", "second.parquet"})
            monkeypatch.setattr(stager, "_close_file", interrupted)

    assert set(deleted) == {"first.parquet", "second.parquet"}


def test_an_interleaved_partition_stream_empties_each_partition_once(
    dataset: IcebergDataset,
) -> None:
    """A partition split across chunks is emptied by the first chunk that
    touches it and only added to by the later ones, in whatever order the
    partitions arrive."""
    dataset.append_arrow_table(quotes(2))
    dataset.append_arrow_table(other_day(2))
    before = len(dataset.iceberg_table.history())
    day = datetime.date(2026, 8, 14)
    following = datetime.date(2026, 8, 15)
    replacement = pyarrow.Table.from_pydict(
        {
            "symbol": ["A", "B", "C"],
            "day": [day, following, day],
            "size": [1, 2, 3],
            "venue": ["XPAR", "XPAR", "XPAR"],
        },
        schema=Quote.into_field().into_arrow_schema(),
    )

    assert (
        dataset.overwrite_arrow_reader(replacement.to_reader(max_chunksize=1), commit_row_size=1)
        == 3
    )

    assert len(dataset.iceberg_table.history()) - before == 3, "one commit per chunk"
    stored = dataset.read_arrow_table().to_pylist()
    assert {row["symbol"] for row in stored if row["day"] == day} == {"A", "C"}
    assert {row["symbol"] for row in stored if row["day"] == following} == {"B"}


def test_complete_partition_runs_do_not_need_to_be_globally_sorted(
    dataset: IcebergDataset,
) -> None:
    first = datetime.date(2026, 8, 14)
    following = first + datetime.timedelta(days=1)
    rows = pyarrow.Table.from_pydict(
        {
            "symbol": ["B", "A"],
            "day": [following, first],
            "size": [2, 1],
            "venue": ["XPAR", "XPAR"],
        },
        schema=Quote.into_field().into_arrow_schema(),
    )

    dataset.overwrite_arrow_reader(rows.to_reader(max_chunksize=1), commit_batch_num=1)

    assert dataset.read_arrow_table().num_rows == 2


def test_a_failed_source_commits_nothing_of_its_stream(dataset: IcebergDataset) -> None:
    """A write spills its whole stream before its first commit, so a source
    that stops mid-stream leaves the table as it was, not the chunks before."""
    dataset.append_arrow_table(quotes(2))
    before = len(dataset.iceberg_table.history())

    def broken():
        yield from keyed("N", 1).to_batches()
        raise RuntimeError("source stopped")

    source = pyarrow.RecordBatchReader.from_batches(
        Quote.into_field().into_arrow_schema(), broken()
    )
    with pytest.raises(pyarrow.ArrowInvalid, match="source stopped"):
        dataset.overwrite_arrow_reader(source, commit_row_size=1)

    assert len(dataset.iceberg_table.history()) == before, "not even the first chunk landed"
    assert {row["symbol"] for row in dataset.read_arrow_table().to_pylist()} == {"S0", "S1"}


def test_what_names_no_stored_rows_to_replace_is_refused(tmp_path: Path) -> None:
    """An overwrite of an unpartitioned table without a `row_filter`, and a
    merge on no key, would each have to guess which stored rows they replace."""

    @scalar
    class Flat:
        symbol: Annotated[str, primary_key()]

    flat = IcebergDataset(
        name="flat_overwrite",
        namespace="trading",
        field=Flat.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    source = pyarrow.Table.from_pydict(
        {"symbol": ["A"]}, schema=Flat.into_field().into_arrow_schema()
    )
    flat.append_arrow_table(source)
    with pytest.raises(ValueError, match="the table is not partitioned"):
        flat.overwrite_arrow_table(source)
    for nothing in (False, []):
        with pytest.raises(ValueError, match="names nothing to match on"):
            flat.merge_arrow_table(source, merge_by=nothing)
    assert len(flat.iceberg_table.history()) == 1, "and neither committed"


def test_a_nan_identity_partition_is_refused_before_pyiceberg(tmp_path: Path) -> None:
    @scalar
    class FloatPartition:
        symbol: Annotated[str, primary_key()]
        partition: Annotated[float, partition_key()]

    values = IcebergDataset(
        name="float_partitions",
        namespace="trading",
        field=FloatPartition.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    source = pyarrow.Table.from_pydict(
        {"symbol": ["A"], "partition": [float("nan")]},
        schema=FloatPartition.into_field().into_arrow_schema(),
    )

    with pytest.raises(ValueError, match="partition column 'partition' contains NaN"):
        values.overwrite_arrow_table(source)

    assert values.iceberg_table.history() == []


def test_a_merge_touches_only_the_files_whose_bounds_admit_its_keys(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(quotes(3))
    dataset.append_arrow_table(other_day(2))
    before_files = {row["file_path"] for row in dataset.data_files().to_pylist()}
    incoming = pyarrow.concat_tables([quotes(1, "XETR"), keyed("N", 1)])
    assert dataset.merge_arrow_reader(incoming.to_reader(max_chunksize=1), commit_row_size=1) == 2

    stored = dataset.read_arrow_table().to_pylist()
    assert {row["symbol"] for row in stored} == {"S0", "S1", "S2", "N0", "D0", "D1"}
    assert next(row for row in stored if row["symbol"] == "S0")["venue"] == "XETR"
    after_files = {row["file_path"] for row in dataset.refresh().data_files().to_pylist()}
    assert len(before_files & after_files) == 1, "the other partition's file was untouched"

    history = len(dataset.iceberg_table.history())
    assert dataset.merge_arrow_table(incoming) == 0, "a replay writes nothing"
    assert len(dataset.iceberg_table.history()) == history, "and commits nothing"
    assert {row["symbol"] for row in dataset.read_arrow_table().to_pylist()} == {
        "S0",
        "S1",
        "S2",
        "N0",
        "D0",
        "D1",
    }, "and holds the same rows"


def test_a_key_in_another_partition_is_another_row(
    dataset: IcebergDataset,
) -> None:
    """A key is scoped to its partition: the same symbol on another day is a
    row of its own, and the stored one stays where it was."""
    dataset.append_arrow_table(quotes(1))
    moved = other_day(1).set_column(0, other_day(1).schema.field("symbol"), pyarrow.array(["S0"]))
    assert dataset.merge_arrow_table(moved) == 1

    stored = dataset.read_arrow_table().sort_by("day").to_pylist()
    assert [(row["symbol"], row["day"]) for row in stored] == [
        ("S0", datetime.date(2026, 8, 14)),
        ("S0", datetime.date(2026, 8, 15)),
    ]


def test_a_key_is_scoped_to_its_transformed_partition(
    tmp_path: Path,
) -> None:
    @scalar
    class Daily:
        symbol: Annotated[str, primary_key()]
        at: Annotated[datetime.datetime, partition_key("day")]
        value: int

    daily = IcebergDataset(
        name="daily_key",
        namespace="trading",
        field=Daily.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    schema = Daily.into_field().into_arrow_schema()
    first = datetime.datetime(2026, 8, 14, 1, tzinfo=UTC)
    next_day = first + datetime.timedelta(days=1)
    daily.append_arrow_table(
        pyarrow.Table.from_pydict(
            {"symbol": ["S", "S"], "at": [first, next_day], "value": [1, 9]},
            schema=schema,
        )
    )
    assert not daily.iceberg_table.sort_order().fields, (
        "a partition chooses the file and does not silently become its physical order"
    )

    daily.merge_arrow_table(
        pyarrow.Table.from_pydict(
            {"symbol": ["S"], "at": [first.replace(hour=2)], "value": [2]},
            schema=schema,
        ),
    )

    assert daily.read_arrow_table().sort_by("at").to_pylist() == [
        {"symbol": "S", "at": first.replace(hour=2), "value": 2},
        {"symbol": "S", "at": next_day, "value": 9},
    ], "the key's row of that day went, and its row of the next day is another row"

    duplicate = pyarrow.Table.from_pydict(
        {
            "symbol": ["N", "N"],
            "at": [first.replace(hour=3), first.replace(hour=4)],
            "value": [3, 4],
        },
        schema=schema,
    )
    assert daily.merge_arrow_table(duplicate) == 1, "the first of a key"
    assert {row["value"] for row in daily.read_arrow_table().to_pylist()} == {2, 9, 3}


def test_a_merge_on_a_bucketed_table_lands_its_rows(tmp_path: Path) -> None:
    @scalar
    class Bucketed:
        ident: Annotated[int, primary_key()]
        code: Annotated[str, partition_key("bucket[3]")]
        value: int

    bucketed = IcebergDataset(
        name="bucketed_key",
        namespace="trading",
        field=Bucketed.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    schema = Bucketed.into_field().into_arrow_schema()
    candidates = [f"code-{index:05d}" for index in range(2_000)]
    values = BucketTransform(3).pyarrow_transform(pyarrow.string())(pyarrow.array(candidates))
    by_bucket: dict[int, list[str]] = {index: [] for index in range(3)}
    for code, bucket in zip(candidates, values.to_pylist(), strict=True):
        by_bucket[bucket].append(code)
    old_codes = by_bucket[0][:201]
    new_codes = by_bucket[0][201:402]
    other_codes = [by_bucket[index][0] for index in range(1, 3)]
    bucketed.append_arrow_table(
        pyarrow.Table.from_pydict(
            {
                "ident": [*range(201), *([1_000, 1_001])],
                "code": [*old_codes, *other_codes],
                "value": [1] * 203,
            },
            schema=schema,
        )
    )
    before = {row["file_path"] for row in bucketed.data_files().to_pylist()}

    assert (
        bucketed.merge_arrow_table(
            pyarrow.Table.from_pydict(
                {"ident": range(201), "code": new_codes, "value": [2] * 201},
                schema=schema,
            ),
        )
        == 201
    )

    stored = bucketed.read_arrow_table().to_pylist()
    assert len(stored) == 203
    assert {row["code"] for row in stored if row["value"] == 2} == set(new_codes)
    assert {row["code"] for row in stored if row["value"] == 1} == set(other_codes)
    after = {row["file_path"] for row in bucketed.refresh().data_files().to_pylist()}
    assert len(before & after) == 2, "the other buckets' files stood: their key bounds excluded"


def test_a_partition_derived_from_the_primary_key_merges_and_overwrites_exactly(
    tmp_path: Path,
) -> None:
    @scalar
    class Tick:
        unix: Annotated[int, primary_key()]
        timepartition: Annotated[int, partition_key(), derived_from("unix")]
        venue: str

    ticks = IcebergDataset(
        name="dynamic_ticks",
        namespace="trading",
        field=Tick.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    ticks.append_arrow_table(pyarrow.table({"unix": [1, 2], "venue": ["XPAR", "XPAR"]}))
    assert ticks.merge_arrow_table(pyarrow.table({"unix": [1], "venue": ["XETR"]})) == 1
    assert ticks.overwrite_arrow_table(pyarrow.table({"unix": [2], "venue": ["XLON"]})) == 1

    assert {row["unix"]: row["venue"] for row in ticks.read_arrow_table().to_pylist()} == {
        1: "XETR",
        2: "XLON",
    }


def test_staged_partition_overwrite_honours_branch_and_properties(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.create_branch("work")
    replacement = keyed("W", 2)

    dataset.overwrite_arrow_table(
        replacement,
        branch="work",
        properties={"rekep.test": "partition-overwrite"},
    )

    assert {row["symbol"] for row in dataset.read_arrow_table().to_pylist()} == {"S0", "S1"}
    assert {row["symbol"] for row in dataset.read_arrow_table(branch="work").to_pylist()} == {
        "W0",
        "W1",
    }
    head = dataset.iceberg_table.refs()["work"]
    snapshot = dataset.iceberg_table.metadata.snapshot_by_id(head.snapshot_id)
    assert snapshot.summary["rekep.test"] == "partition-overwrite"


def test_dynamic_overwrite_requires_source_partition_columns(tmp_path: Path) -> None:
    @scalar
    class OptionalPartition:
        symbol: Annotated[str, primary_key()]
        venue: Annotated[str | None, partition_key()] = None

    partitioned = IcebergDataset(
        name="optional_partition",
        namespace="trading",
        field=OptionalPartition.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    partitioned.create_with()
    source = pyarrow.RecordBatchReader.from_batches(
        pyarrow.schema([pyarrow.field("symbol", pyarrow.string(), nullable=False)]),
        [pyarrow.record_batch([["A"]], names=["symbol"])],
    )

    with pytest.raises(ValueError, match="partition columns .* missing"):
        partitioned.overwrite_arrow_reader(source)


def test_a_null_partition_is_replaced_without_touching_the_others(tmp_path: Path) -> None:
    @scalar
    class OptionalPartition:
        symbol: Annotated[str, primary_key()]
        venue: Annotated[str | None, partition_key()] = None

    partitioned = IcebergDataset(
        name="null_partition",
        namespace="trading",
        field=OptionalPartition.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    schema = OptionalPartition.into_field().into_arrow_schema()
    partitioned.append_arrow_table(
        pyarrow.Table.from_pydict(
            {"symbol": ["old", "kept"], "venue": [None, "XPAR"]}, schema=schema
        )
    )
    replacement = pyarrow.Table.from_pydict(
        {"symbol": ["N0", "N1"], "venue": [None, None]}, schema=schema
    )

    partitioned.overwrite_arrow_reader(replacement.to_reader(max_chunksize=1), commit_row_size=1)

    assert sorted(partitioned.read_arrow_table().to_pylist(), key=lambda row: row["symbol"]) == [
        {"symbol": "N0", "venue": None},
        {"symbol": "N1", "venue": None},
        {"symbol": "kept", "venue": "XPAR"},
    ]


def test_a_day_partition_is_staged_and_replaced_as_one_unit(tmp_path: Path) -> None:
    @scalar
    class Daily:
        """One value partitioned by the day containing its timestamp."""

        code: str
        at: Annotated[datetime.datetime, partition_key("day")]

    daily = IcebergDataset(
        name="daily_partition_overwrite",
        namespace="trading",
        field=Daily.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    schema = Daily.into_field().into_arrow_schema()
    first = datetime.datetime(2026, 8, 14, 1, tzinfo=UTC)
    second = datetime.datetime(2026, 8, 15, 1, tzinfo=UTC)
    daily.append_arrow_table(
        pyarrow.Table.from_pydict(
            {"code": ["old-a", "old-b", "kept"], "at": [first, first.replace(hour=2), second]},
            schema=schema,
        )
    )
    before = {row["file_path"] for row in daily.data_files().to_pylist()}

    daily.overwrite_arrow_table(
        pyarrow.Table.from_pydict({"code": ["new"], "at": [first.replace(hour=12)]}, schema=schema),
    )

    assert {row["code"] for row in daily.read_arrow_table().to_pylist()} == {"new", "kept"}
    after = {row["file_path"] for row in daily.refresh().data_files().to_pylist()}
    assert len(before & after) == 1, "the untouched day keeps its data file"

    recurring = pyarrow.Table.from_pydict(
        {
            "code": ["first", "middle", "again"],
            "at": [first, second, first.replace(hour=20)],
        },
        schema=schema,
    )
    daily.overwrite_arrow_reader(recurring.to_reader(max_chunksize=1), commit_row_size=1_000_000)
    assert {row["code"] for row in daily.read_arrow_table().to_pylist()} == {
        "first",
        "middle",
        "again",
    }, "a day recurring inside one chunk is one partition, emptied once"


def test_partition_replacement_prunes_manifests_before_reading_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.manifest import ManifestFile

    from rekep.iceberg import dataset as module

    partitioned = IcebergDataset(
        name="manifest_pruning",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
        optimize_commits=False,
    )
    partitioned.append_arrow_table(quotes(1))
    partitioned.append_arrow_table(other_day(1))
    table = partitioned.iceberg_table
    manifests = table.current_snapshot().manifests(io=table.io)
    assert len(manifests) == 2

    visited: list[str] = []
    original = ManifestFile.fetch_manifest_entry

    def capture(self: object, *args: object, **kwargs: object) -> object:
        visited.append(str(self.manifest_path))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(ManifestFile, "fetch_manifest_entry", capture)
    replacement = module._StagedPartition({"day": datetime.date(2026, 8, 14)}, (), (), 0)

    found = module._partition_data_files(table, [replacement], "main")

    assert len(found) == 1
    assert len(set(visited)) == 1, "the other partition manifest was rejected by its summary"


def test_a_bucket_partition_is_staged_without_inverting_its_hash(tmp_path: Path) -> None:
    @scalar
    class Bucketed:
        """One value partitioned by its four-bucket Murmur3 hash."""

        code: Annotated[str, partition_key("bucket[4]")]
        size: int

    bucketed = IcebergDataset(
        name="bucket_partition_overwrite",
        namespace="trading",
        field=Bucketed.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    schema = Bucketed.into_field().into_arrow_schema()
    bucketed.append_arrow_table(
        pyarrow.Table.from_pydict({"code": ["aa", "ac", "ab"], "size": [1, 2, 3]}, schema=schema)
    )
    before = {row["file_path"] for row in bucketed.data_files().to_pylist()}

    bucketed.overwrite_arrow_table(
        pyarrow.Table.from_pydict({"code": ["ad"], "size": [4]}, schema=schema),
    )

    assert {row["code"] for row in bucketed.read_arrow_table().to_pylist()} == {"ad", "ab"}
    after = {row["file_path"] for row in bucketed.refresh().data_files().to_pylist()}
    assert len(before & after) == 1, "the other hash bucket keeps its data file"

    recurring = pyarrow.Table.from_pydict(
        {"code": ["aa", "ab", "ac"], "size": [5, 6, 7]}, schema=schema
    )
    bucketed.overwrite_arrow_reader(recurring.to_reader(max_chunksize=1), commit_row_size=1_000_000)
    assert {row["code"]: row["size"] for row in bucketed.read_arrow_table().to_pylist()} == {
        "aa": 5,
        "ab": 6,
        "ac": 7,
    }, "every bucket the chunk touched was emptied, once"


def test_a_truncated_partition_is_staged_and_replaced_as_one_unit(tmp_path: Path) -> None:
    @scalar
    class Truncated:
        """One value partitioned by its first two characters."""

        code: Annotated[str, partition_key("truncate[2]")]
        size: int

    truncated = IcebergDataset(
        name="truncate_partition_overwrite",
        namespace="trading",
        field=Truncated.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    schema = Truncated.into_field().into_arrow_schema()
    truncated.append_arrow_table(
        pyarrow.Table.from_pydict(
            {"code": ["aa-old-1", "aa-old-2", "bb-kept"], "size": [1, 2, 3]},
            schema=schema,
        )
    )
    before = {row["file_path"] for row in truncated.data_files().to_pylist()}

    truncated.overwrite_arrow_table(
        pyarrow.Table.from_pydict({"code": ["aa-new"], "size": [4]}, schema=schema),
    )

    assert {row["code"] for row in truncated.read_arrow_table().to_pylist()} == {
        "aa-new",
        "bb-kept",
    }
    after = {row["file_path"] for row in truncated.refresh().data_files().to_pylist()}
    assert len(before & after) == 1, "the other truncated prefix keeps its data file"

    recurring = pyarrow.Table.from_pydict(
        {"code": ["aa-one", "bb-one", "aa-two"], "size": [5, 6, 7]}, schema=schema
    )
    truncated.overwrite_arrow_reader(
        recurring.to_reader(max_chunksize=1), commit_row_size=1_000_000
    )
    assert {row["code"] for row in truncated.read_arrow_table().to_pylist()} == {
        "aa-one",
        "bb-one",
        "aa-two",
    }, "every prefix the chunk touched was emptied, once"


def test_an_empty_partition_overwrite_commits_nothing(dataset: IcebergDataset) -> None:
    dataset.create_with()
    empty = pyarrow.RecordBatchReader.from_batches(Quote.into_field().into_arrow_schema(), [])
    dataset.overwrite_arrow_reader(empty)
    assert dataset.iceberg_table.history() == []


def test_a_merge_on_a_partition_column_refuses_a_null_key(tmp_path: Path) -> None:
    @scalar
    class MaybeKeyed:
        day: Annotated[datetime.date, partition_key()]
        symbol: str | None = None

    target = IcebergDataset(
        name="maybe_keyed",
        namespace="trading",
        field=MaybeKeyed.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    source = pyarrow.Table.from_pydict(
        {"symbol": [None], "day": [datetime.date(2026, 8, 14)]},
        schema=MaybeKeyed.into_field().into_arrow_schema(),
    )

    with pytest.raises(ValueError, match="cannot be null"):
        target.merge_arrow_table(source, merge_by=["symbol", "day"])


def test_a_blind_append_adds_every_row_it_is_handed(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2), merge_by=False)
    dataset.append_arrow_table(quotes(2), merge_by=False)
    assert dataset.read_arrow_table().num_rows == 4, "blind: the same keys twice"


def test_merging_on_a_key_nothing_declares_is_refused_before_writing(tmp_path: Path) -> None:
    @scalar
    class Loose:
        symbol: str

    keyless = IcebergDataset(
        name="loose",
        namespace="trading",
        field=Loose.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    with pytest.raises(ValueError, match="no member declares one"):
        keyless.merge_columns(True)
    assert keyless.merge_columns(None) == [], "nothing declared, nothing to match on"
    rows = pyarrow.Table.from_pydict(
        {"symbol": ["A"]}, schema=Loose.into_field().into_arrow_schema()
    )
    with pytest.raises(ValueError, match="no member declares one"):
        keyless.merge_arrow_table(rows)
    assert keyless.iceberg_table.history() == []


def test_merge_columns_are_the_declared_key_or_the_names_given(
    dataset: IcebergDataset,
) -> None:
    assert dataset.merge_columns(True) == ["symbol"] == dataset.merge_columns(None)
    assert dataset.merge_columns(["symbol", "day"]) == ["symbol", "day"]
    assert dataset.merge_columns(False) == [] == dataset.merge_columns([])


def test_relative_snapshot_expiry_is_the_iceberg_property(
    tmp_path: Path,
) -> None:
    retained = IcebergDataset(
        name="retained_document",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
        snapshot_expiry=datetime.timedelta(days=7),
    )

    assert retained.snapshot_expiry is None
    assert retained.table_properties["history.expire.max-snapshot-age-ms"] == "604800000"
    assert retained.__dict__["_snapshot_expiry"] == datetime.timedelta(days=7)


def test_snapshot_expiry_rounds_up_to_icebergs_millisecond_precision(
    tmp_path: Path,
) -> None:
    retained = IcebergDataset(
        name="precise_retention",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
        snapshot_expiry=datetime.timedelta(microseconds=500),
    )

    assert retained.table_properties["history.expire.max-snapshot-age-ms"] == "1"
    assert retained.__dict__["_snapshot_expiry"] == datetime.timedelta(milliseconds=1)


# -- merging, chunk by chunk ------------------------------------------------


def stored_sizes(dataset: IcebergDataset) -> dict[str, int]:
    table = dataset.read_arrow_table()
    return dict(zip(*(table.column(name).to_pylist() for name in ("symbol", "size")), strict=True))


def test_a_merge_writes_every_changed_or_new_row(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    changed = quotes(3).set_column(2, "size", pyarrow.array([90, 91, 92], pyarrow.int64()))
    assert dataset.merge_arrow_table(changed) == 3
    assert stored_sizes(dataset) == {"S0": 90, "S1": 91, "S2": 92}, (
        "stored rows take the new values"
    )


def test_a_merge_replay_writes_nothing_and_commits_nothing(dataset: IcebergDataset) -> None:
    assert dataset.merge_arrow_table(quotes(3)) == 3
    table = dataset.iceberg_table
    snapshots = [one.snapshot_id for one in table.snapshots()]
    files = _data_paths(dataset)

    assert dataset.merge_arrow_table(quotes(3)) == 0, "every row is stored as it is"

    table = dataset.refresh().iceberg_table
    assert [one.snapshot_id for one in table.snapshots()] == snapshots, "no snapshot"
    assert _data_paths(dataset) == files, "and no file rewritten"
    assert dataset.read_arrow_table().num_rows == 3, "and held once"


def test_a_key_repeated_in_a_chunk_keeps_its_first_row(dataset: IcebergDataset) -> None:
    day = datetime.date(2026, 8, 14)
    chunk = pyarrow.Table.from_pydict(
        {
            "symbol": ["A", "A"],
            "day": [day, day],
            "size": [1, 9],
            "venue": ["XPAR", "XPAR"],
        },
        schema=Quote.into_field().into_arrow_schema(),
    )
    assert dataset.merge_arrow_table(chunk) == 1
    assert stored_sizes(dataset) == {"A": 1}


def test_a_key_repeated_in_a_later_chunk_keeps_the_row_the_earlier_one_landed(
    dataset: IcebergDataset,
) -> None:
    """Within one write a key keeps its first row, whichever chunk its
    recurrence lands in: the later chunk leaves what the earlier one settled."""
    day = datetime.date(2026, 8, 14)
    stream = pyarrow.Table.from_pydict(
        {
            "symbol": ["A", "B", "A"],
            "day": [day, day, day],
            "size": [1, 2, 9],
            "venue": ["XPAR"] * 3,
        },
        schema=Quote.into_field().into_arrow_schema(),
    )
    assert dataset.merge_arrow_reader(stream.to_reader(max_chunksize=1), commit_row_size=1) == 2
    assert stored_sizes(dataset) == {"A": 1, "B": 2}
    assert len(dataset.iceberg_table.snapshots()) == 2, "the recurrence committed nothing"


def test_an_initial_merge_lands_all_partitions_in_one_snapshot(
    dataset: IcebergDataset,
) -> None:
    today = quotes(3)
    tomorrow = today.set_column(
        today.schema.get_field_index("day"),
        today.schema.field("day"),
        pyarrow.array([datetime.date(2026, 8, 15)] * today.num_rows),
    )
    source = pyarrow.concat_tables([today, tomorrow])

    assert dataset.merge_arrow_table(source) == 6

    assert dataset.read_arrow_table().num_rows == 6
    assert len(dataset.iceberg_table.snapshots()) == 1
    assert dataset.data_files().num_rows == 2, "one file per partition"


def test_a_merge_over_stored_rows_lands_every_partition_in_one_snapshot(
    dataset: IcebergDataset,
) -> None:
    """Partitions are staged one at a time and committed together."""
    dataset.append_arrow_table(quotes(1))
    source = pyarrow.concat_tables([keyed("N", 2), other_day(2), third_day(2)])

    before = len(dataset.iceberg_table.snapshots())
    assert dataset.merge_arrow_table(source) == 6

    assert len(dataset.iceberg_table.snapshots()) == before + 1, "three partitions, one commit"
    assert {row["symbol"] for row in dataset.read_arrow_table().to_pylist()} == {
        "S0",
        "N0",
        "N1",
        "D0",
        "D1",
        "T0",
        "T1",
    }


def test_a_batched_merge_commit_stays_on_its_branch(dataset: IcebergDataset) -> None:
    """One commit for many partitions is still one commit on the named ref."""
    dataset.append_arrow_table(quotes(1))
    dataset.create_branch("work")
    source = pyarrow.concat_tables([keyed("N", 1), other_day(1), third_day(1)])

    before = len(dataset.iceberg_table.snapshots())
    assert dataset.merge_arrow_table(source, branch="work") == 3

    assert len(dataset.refresh().iceberg_table.snapshots()) == before + 1
    assert dataset.read_arrow_table().num_rows == 1, "main never saw the write"
    assert dataset.read_arrow_table(branch="work").num_rows == 4


def test_a_streamed_merge_commits_per_batch_bound_not_per_partition(
    dataset: IcebergDataset,
) -> None:
    """`commit_batch_num` bounds a partitioned stream's commits as it does a flat one."""
    dataset.append_arrow_table(quotes(1))
    source = pyarrow.concat_tables([keyed("N", 2), other_day(2), third_day(2)])

    before = len(dataset.iceberg_table.snapshots())
    assert dataset.merge_arrow_reader(source.to_reader(max_chunksize=1), commit_batch_num=6) == 6

    assert len(dataset.iceberg_table.snapshots()) == before + 1
    assert dataset.read_arrow_table().num_rows == 7


def test_a_chunk_that_empties_a_file_and_adds_partitions_is_one_commit(
    dataset: IcebergDataset,
) -> None:
    """What a chunk takes out and what it adds land together, whatever the
    partition count."""
    dataset.append_arrow_table(pyarrow.concat_tables([quotes(1), other_day(1)]))
    source = pyarrow.concat_tables([quotes(1, "XETR"), keyed("N", 1), third_day(1)])

    before = len(dataset.iceberg_table.snapshots())
    assert dataset.merge_arrow_table(source) == 3

    assert len(dataset.iceberg_table.snapshots()) - before == 1
    assert stored_sizes(dataset) == {"S0": 0, "D0": 0, "N0": 0, "T0": 0}
    assert (
        next(row for row in dataset.read_arrow_table().to_pylist() if row["symbol"] == "S0")[
            "venue"
        ]
        == "XETR"
    )


def test_a_refused_partition_leaves_the_whole_merged_chunk_uncommitted(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One commit per chunk means one outcome: a refused part strands no other."""
    from pyiceberg.table.update.snapshot import _FastAppendFiles

    dataset.append_arrow_table(quotes(1))
    before = len(dataset.iceberg_table.snapshots())

    def refused(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("snapshot refused")

    monkeypatch.setattr(_FastAppendFiles, "append_data_file", refused)
    with pytest.raises(RuntimeError, match="snapshot refused"):
        dataset.merge_arrow_table(
            pyarrow.concat_tables([keyed("N", 1), other_day(1), third_day(1)])
        )

    assert len(dataset.refresh().iceberg_table.snapshots()) == before
    assert {row["symbol"] for row in dataset.read_arrow_table().to_pylist()} == {"S0"}


def test_a_merge_refuses_a_null_or_nan_key(dataset: IcebergDataset) -> None:
    """No join finds the row a null or NaN key would replace, so the chunk is
    refused before anything of it is written."""
    dataset.append_arrow_table(quotes(1))
    before = _iceberg_artifacts(dataset)
    nulled = quotes(2).set_column(3, "venue", pyarrow.array(["XETR", None], pyarrow.string()))
    with pytest.raises(ValueError, match="cannot be null"):
        dataset.merge_arrow_table(nulled, merge_by=["venue"])
    assert _iceberg_artifacts(dataset) == before

    @scalar
    class Level:
        price: float
        size: int

    levels = dataset.store.dataset("trading.levels", field=Level.into_field())
    nan = pyarrow.Table.from_pydict(
        {"price": [float("nan")], "size": [1]}, schema=Level.into_field().into_arrow_schema()
    )
    with pytest.raises(ValueError, match="cannot be NaN"):
        levels.merge_arrow_table(nan, merge_by=["price"])
    assert levels.iceberg_table.history() == []


def test_append_adds_only_absent_keys_unless_told_to_be_blind(dataset: IcebergDataset) -> None:
    """The primary key the shape declares is what an append matches on by default."""
    assert dataset.append_arrow_table(quotes(2)) == 2
    assert dataset.append_arrow_table(quotes(2)) == 0, "every key is held"
    assert dataset.read_arrow_table().num_rows == 2
    assert dataset.append_arrow_table(quotes(2), merge_by=False) == 2
    assert dataset.read_arrow_table().num_rows == 4


def test_append_streams_one_commit_per_chunk(dataset: IcebergDataset) -> None:
    reader = quotes(6).to_reader(max_chunksize=1)
    dataset.append_arrow_reader(reader, commit_row_size=2)
    assert dataset.read_arrow_table().num_rows == 6
    assert len(dataset.iceberg_table.snapshots()) == 3, "two rows per commit"


def test_a_stale_plan_lands_on_the_retry_unless_the_moved_head_holds_its_rows(
    tmp_path: Path,
) -> None:
    """A commit planned against a head another writer moved is retried by
    PyIceberg against the new head, and what decides it is whether the rows
    landed in between are ones the plan takes out. A replacement is never
    rebuilt on a stale plan, so one that is refused is handed back for the
    caller to refresh and plan again, against what is stored now."""
    from pyiceberg.exceptions import CommitFailedException

    catalog = IcebergCatalog(name="concurrent", properties=catalog_properties(tmp_path))
    writer = catalog.dataset("trading.timed", field=Timed.into_field())
    assert writer.merge_arrow_table(timed(0, 1), commit_row_size=1_000_000) == 2
    other = catalog.dataset("trading.timed", field=Timed.into_field())
    other.append_arrow_table(timed(100), commit_row_size=1_000_000)

    assert writer.merge_arrow_table(timed(1, 2), commit_row_size=1_000_000) == 1, "key 1 is held"
    assert writer.read_arrow_table().sort_by("unix").column("unix").to_pylist() == [0, 1, 2, 100], (
        "nothing landed under keys 1 and 2, so the stale plan landed on the retry"
    )

    other.refresh().append_arrow_table(timed(3), commit_row_size=1_000_000)
    changed = timed(2, 3).set_column(1, "payload", pyarrow.array(["y", "y"]))
    with pytest.raises(CommitFailedException, match="branch main has changed"):
        writer.merge_arrow_table(changed, commit_row_size=1_000_000)

    assert writer.refresh().merge_arrow_table(changed) == 2
    stored = writer.read_arrow_table().sort_by("unix")
    assert list(
        zip(stored.column("unix").to_pylist(), stored.column("payload").to_pylist(), strict=True)
    ) == [(0, "x"), (1, "x"), (2, "y"), (3, "y"), (100, "x")], (
        "key 3 landed by the other writer was replaced by the fresh plan"
    )


def test_a_text_row_round_trips_through_iceberg(tmp_path: Path) -> None:
    """A line as the native read states it, past the storage boundary, is the
    row the table hands back: nothing is derived on the way in, and the
    table is laid out by the event the row already carries."""
    source = tmp_path / "capture.log"
    source.write_bytes(
        b"2026-08-14 09:30:00.123 [250-e7256476:9effef3e6a:72504] [ULBridge] (INFO) opaque\n"
    )
    field = log_message_field()
    handle = IOBase.from_uri(source.as_uri())
    try:
        stored = stored_arrow_reader(
            handle.read_arrow_reader(options=text_options()), field
        ).read_all()
    finally:
        handle.close()
    target = IcebergDataset(
        name="log_messages",
        namespace="record_keeping",
        field=field,
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )

    target.append_arrow_table(stored)

    reopened = IcebergCatalog(name="test", properties=catalog_properties(tmp_path)).dataset(
        target.identifier
    )
    held = reopened.read_arrow_table(field)
    assert held.equals(stored)
    row = held.to_pylist()[0]
    # The header's `09:30`, read in the zone the bridge prints in, two hours
    # ahead of UTC in the Central European summer.
    assert row["currunix"] == datetime.datetime(2026, 8, 14, 7, 30, 0, 123000, tzinfo=UTC)
    assert (row["body"], row["msgthreadid"], row["msgpluginid"]) == ("opaque", 250, "ULBridge")
    assert row["seqnum"] == 1 and row["state"] == 0
    projected = reopened.read_arrow_reader(field, columns=["currunix"])
    try:
        assert projected.schema.names == ["currunix"]
        assert projected.read_all().column("currunix").to_pylist() == [row["currunix"]]
    finally:
        projected.close()


def test_the_module_imports_without_pyiceberg() -> None:
    """pyiceberg is an extra: reaching for the dataset must not need it installed."""
    blocked = "import sys; sys.modules['pyiceberg'] = None; import rekep.iceberg; print('ok')"
    done = subprocess.run(  # noqa: S603
        [sys.executable, "-c", blocked], capture_output=True, text=True, check=False
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "ok"


def test_a_missing_extra_is_named_in_the_error(dataset: IcebergDataset) -> None:
    with pytest.MonkeyPatch.context() as patch:
        patch.setitem(sys.modules, "pyiceberg", None)
        with pytest.raises(ImportError, match=r"pip install rekep\[iceberg\]"):
            iceberg_schema(Quote.into_field())


# -- creating explicitly ----------------------------------------------------


def test_create_with_builds_the_table_before_any_write(dataset: IcebergDataset) -> None:
    dataset.create_with()
    assert dataset.exists
    assert dataset.read_arrow_table().num_rows == 0


def test_create_with_takes_a_shape_it_was_not_declared_with(tmp_path: Path) -> None:
    bare = IcebergDataset(
        name="bare",
        namespace="trading",
        field=field_of(pyarrow.schema([]), "bare"),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    schema = pyarrow.schema([pyarrow.field("symbol", pyarrow.string(), nullable=False)])
    bare.create_with(schema)
    assert [member.name for member in bare.into_struct_field()] == ["symbol"]
    assert bare.name == "bare"
    assert bare.identifier == "trading.bare"


def test_creating_twice_leaves_the_table_alone(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.create_with()
    assert dataset.read_arrow_table().num_rows == 2


# -- schema evolution -------------------------------------------------------


def test_add_fields_adds_what_the_table_lacks(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    wider = field_of(
        pyarrow.schema(
            [
                *Quote.into_field().into_arrow_schema(),
                ("desk", pyarrow.string()),
                ("pod", pyarrow.int32()),
            ]
        ),
        Quote.into_field().name,
    )
    assert dataset.add_fields(wider) == ["desk", "pod"]
    assert [member.name for member in dataset.table_field][-2:] == ["desk", "pod"]
    assert [member.name for member in dataset.into_struct_field()][-2:] == ["desk", "pod"], (
        "writes follow the table"
    )
    assert dataset.read_arrow_table().column("desk").to_pylist() == [None, None]


def test_add_fields_skips_when_there_is_nothing_new(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    before = len(dataset.iceberg_table.schemas())
    assert dataset.add_fields(Quote.into_field()) == []
    assert len(dataset.refresh().iceberg_table.schemas()) == before, "no commit was made"


def test_add_fields_can_report_without_touching_the_table(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    wider = field_of(
        pyarrow.schema([*Quote.into_field().into_arrow_schema(), ("desk", pyarrow.string())]),
        Quote.into_field().name,
    )
    assert dataset.add_fields(wider, dry_run=True) == ["desk"]
    assert "desk" not in [member.name for member in dataset.refresh().into_struct_field()]


def test_a_wider_batch_lands_after_the_columns_are_added(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    wider = field_of(
        pyarrow.schema([*Quote.into_field().into_arrow_schema(), ("desk", pyarrow.string())]),
        Quote.into_field().name,
    )
    dataset.add_fields(wider)
    batch = quotes(1).append_column("desk", pyarrow.array(["EQ"]))
    dataset.append_arrow(batch, merge_by=False)  # the declared shape moved with the table
    assert set(dataset.read_arrow_table().column("desk").to_pylist()) == {None, "EQ"}


def test_merge_schema_creates_a_missing_table_from_the_write_field(
    dataset: IcebergDataset,
) -> None:
    dataset.merge_schema = True
    wider = field_of(
        pyarrow.schema(
            [
                *Quote.into_field().into_arrow_schema(),
                pyarrow.field("desk", pyarrow.string(), nullable=False),
            ]
        ),
        Quote.into_field().name,
    )
    source = quotes(1).append_column("desk", pyarrow.array(["EQ"]))

    assert dataset.append_arrow_reader(source.to_reader(), wider) == 1

    table = dataset.iceberg_table
    assert len(table.schemas()) == 1, "creation needs no follow-up schema commit"
    assert table.schema().find_field("desk").required
    assert dataset.read_arrow_table().column("desk").to_pylist() == ["EQ"]


def test_merge_schema_adds_once_before_a_streamed_write(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    dataset.merge_schema = True
    before_schemas = len(dataset.iceberg_table.schemas())
    before_ids = {field.name: field.field_id for field in dataset.iceberg_table.schema().fields}
    before_spec = dataset.iceberg_table.spec()
    desk = pyarrow.field(
        "desk",
        pyarrow.string(),
        metadata={b"description": b"Execution desk.", b"FIX:tag": b"999"},
    )
    wider = field_of(
        pyarrow.schema([*Quote.into_field().into_arrow_schema(), desk]),
        Quote.into_field().name,
    )
    source = pyarrow.Table.from_pydict(
        {
            **quotes(2).to_pydict(),
            "desk": ["EQ", "FX"],
        },
        schema=wider.into_arrow_schema(),
    )

    written = dataset.append_arrow_reader(
        source.to_reader(max_chunksize=1),
        wider,
        commit_batch_num=1,
        merge_by=False,
    )

    table = dataset.iceberg_table
    after_ids = {field.name: field.field_id for field in table.schema().fields}
    assert written == 2
    assert len(table.schemas()) == before_schemas + 1, "one schema update, not one per batch"
    assert {name: after_ids[name] for name in before_ids} == before_ids
    assert after_ids["desk"] > max(before_ids.values()), "Iceberg assigned the new id"
    assert table.schema().find_field("desk").doc == "Execution desk."
    assert dataset.field["desk"].metadata["FIX:tag"] == "999"
    assert table.spec() == before_spec, "schema merging does not rewrite partition layout"
    assert set(dataset.read_arrow_table().column("desk").to_pylist()) == {None, "EQ", "FX"}

    reopened = dataset.store.dataset(
        dataset.identifier,
        field=Quote.into_field(),
        merge_schema=True,
    )
    reopened.append_arrow_reader(
        quotes(1, "reopened").append_column("desk", pyarrow.array(["OPS"])).to_reader(),
        wider,
        merge_by=False,
    )
    assert "OPS" in reopened.read_arrow_table().column("desk").to_pylist()


def test_merge_schema_refuses_a_required_addition_before_writing(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(quotes(1))
    before_schemas = len(dataset.iceberg_table.schemas())
    before_snapshots = len(dataset.iceberg_table.snapshots())
    wider = field_of(
        pyarrow.schema(
            [
                *Quote.into_field().into_arrow_schema(),
                pyarrow.field("desk", pyarrow.string(), nullable=False),
            ]
        ),
        Quote.into_field().name,
    )
    source = quotes(1).append_column("desk", pyarrow.array(["EQ"]))

    with pytest.raises(ValueError, match="cannot add required column: desk"):
        dataset.append_arrow_reader(source.to_reader(), wider, merge_schema=True)

    assert len(dataset.refresh().iceberg_table.schemas()) == before_schemas
    assert len(dataset.iceberg_table.snapshots()) == before_snapshots
    assert dataset.records == 1


def test_merge_schema_validates_a_branch_before_its_table_wide_update(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(quotes(1))
    dataset.create_branch("dev")
    wider = field_of(
        pyarrow.schema([*Quote.into_field().into_arrow_schema(), ("desk", pyarrow.string())]),
        Quote.into_field().name,
    )
    source = quotes(1, "dev").append_column("desk", pyarrow.array(["EQ"]))
    before_schemas = len(dataset.iceberg_table.schemas())

    with pytest.raises(ValueError, match="unknown ref=missing"):
        dataset.append_arrow_reader(
            source.to_reader(),
            wider,
            merge_schema=True,
            branch="missing",
        )
    assert len(dataset.refresh().iceberg_table.schemas()) == before_schemas

    dataset.append_arrow_reader(
        source.to_reader(),
        wider,
        merge_schema=True,
        branch="dev",
        merge_by=False,
    )
    assert dataset.read_arrow_table().column("desk").to_pylist() == [None]
    assert set(dataset.read_arrow_table(branch="dev").column("desk").to_pylist()) == {
        None,
        "EQ",
    }


# -- snapshots and branches -------------------------------------------------


def test_snapshots_are_listed(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    dataset.append_arrow_table(quotes(1), merge_by=False)
    assert dataset.snapshots().num_rows == 2


def test_a_read_can_go_back_to_an_older_snapshot(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    first = dataset.iceberg_table.current_snapshot().snapshot_id
    dataset.append_arrow_table(quotes(3), merge_by=False)
    assert dataset.refresh().read_arrow_table().num_rows == 5
    assert dataset.read_arrow_table(snapshot_id=first).num_rows == 2


def test_a_branch_is_written_and_read_on_its_own(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.create_branch("dev")
    dataset.append_arrow(quotes(3), branch="dev", merge_by=False)
    assert dataset.read_arrow_table(branch="dev").num_rows == 5
    assert dataset.read_arrow_table().num_rows == 2, "main is untouched"


@pytest.mark.parametrize("alias", ["root", "main", "master"])
def test_root_branch_aliases_override_a_named_default(dataset: IcebergDataset, alias: str) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.create_branch("dev")
    dataset.branch = "dev"
    dataset.append_arrow(quotes(1, alias), branch=alias, merge_by=False)
    assert dataset.read_arrow_table().num_rows == 2, "None still inherits dev"
    assert dataset.read_arrow_table(branch=alias).num_rows == 3
    assert dataset.scan_plan(branch=alias)["rows"] == 3
    assert set(dataset.refs()) == {"main", "dev"}, "aliases never become stored refs"


@pytest.mark.parametrize("alias", ["root", "main", "master"])
def test_root_branch_aliases_are_reserved(dataset: IcebergDataset, alias: str) -> None:
    dataset.append_arrow_table(quotes(1))
    with pytest.raises(ValueError, match="reserved"):
        dataset.create_branch(alias)
    with pytest.raises(ValueError, match="reserved"):
        dataset.remove_branch(alias)


@pytest.mark.parametrize("verb", ["merge", "overwrite"])
def test_a_missing_branch_is_refused_before_a_replace(dataset: IcebergDataset, verb: str) -> None:
    given = quotes(2)
    dataset.append_arrow_table(given)

    with pytest.raises(ValueError, match="unknown ref=missing"):
        _replaced(dataset, verb, given, branch="missing")

    assert set(dataset.refs()) == {"main"}
    assert dataset.read_arrow_table().num_rows == 2


@pytest.mark.parametrize("operation", ["append", "delete"])
def test_a_missing_branch_is_refused_by_blind_writes(
    dataset: IcebergDataset, operation: str
) -> None:
    dataset.append_arrow_table(quotes(2))

    with pytest.raises(ValueError, match="unknown ref=missing"):
        if operation == "append":
            dataset.append_arrow_table(quotes(1, "later"), branch="missing")
        else:
            dataset.delete("size >= 0", branch="missing")

    assert set(dataset.refs()) == {"main"}
    assert dataset.read_arrow_table().num_rows == 2


@pytest.mark.parametrize("operation", ["compaction_plan", "compact"])
def test_a_missing_branch_is_refused_before_maintenance_planning(
    dataset: IcebergDataset, operation: str
) -> None:
    dataset.append_arrow_table(quotes(2))
    with pytest.raises(ValueError, match="unknown ref=missing"):
        getattr(dataset, operation)(branch="missing")


def test_an_unwritten_root_still_plans_nothing_and_accepts_its_first_merge(
    dataset: IcebergDataset,
) -> None:
    dataset.create_with()
    assert dataset.refs() == {}
    assert dataset.compaction_plan(branch="root") == []

    dataset.overwrite_arrow_table(quotes(1), branch="root")
    assert dataset.read_arrow_table(branch="master").num_rows == 1


def test_a_branch_is_removed(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    dataset.create_branch("dev")
    assert "dev" in dataset.refs()
    dataset.remove_branch("dev")
    assert "dev" not in dataset.refs()


def test_branching_needs_something_to_branch_from(dataset: IcebergDataset) -> None:
    dataset.create_with()
    with pytest.raises(ValueError, match="no snapshot to branch from"):
        dataset.create_branch("dev")


def test_a_rollback_moves_the_table_back(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    first = dataset.iceberg_table.current_snapshot().snapshot_id
    dataset.append_arrow_table(quotes(3), merge_by=False)
    dataset.rollback(first)
    assert dataset.read_arrow_table().num_rows == 2


def test_rows_are_deleted_by_filter(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(5))
    dataset.delete("size >= 3")
    assert dataset.refresh().read_arrow_table().num_rows == 3


@pytest.mark.parametrize(
    "row_filter",
    ["size = 2", EqualTo("size", 2)],
    ids=["sql", "expression"],
)
def test_delete_where_accepts_sql_and_boolean_expressions(
    dataset: IcebergDataset, row_filter: object
) -> None:
    dataset.append_arrow_table(quotes(5))

    assert dataset.delete_where(row_filter) == 1
    assert dataset.refresh().read_arrow_table().column("size").to_pylist() == [0, 1, 3, 4]


def test_a_partial_file_delete_never_collects_its_scan(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.io.pyarrow import ArrowScan

    from rekep.iceberg import dataset as dataset_module

    dataset.append_arrow_table(quotes(5))
    task_batches = dataset_module._task_batches

    class StreamOnly:
        def __init__(self, batches: Iterator[pyarrow.RecordBatch]) -> None:
            self.batches = batches

        def __iter__(self) -> Iterator[pyarrow.RecordBatch]:
            return iter(self.batches)

        def read_all(self) -> None:
            pytest.fail("a partial-file delete must not collect its batches")

    def streamed(*args: object, **kwargs: object) -> StreamOnly:
        return StreamOnly(task_batches(*args, **kwargs))

    with monkeypatch.context() as guarded:
        guarded.setattr(
            ArrowScan,
            "to_table",
            lambda *_args, **_kwargs: pytest.fail(
                "a partial-file delete must not materialize its scan"
            ),
        )
        guarded.setattr(dataset_module, "_task_batches", streamed)
        assert dataset.delete_where(EqualTo("size", 2)) == 1

    assert dataset.refresh().read_arrow_table().column("size").to_pylist() == [0, 1, 3, 4]


def test_delete_rewrites_each_partial_file_and_commits_once(
    dataset: IcebergDataset,
) -> None:
    first = datetime.date(2026, 8, 14)
    second = first + datetime.timedelta(days=1)
    schema = Quote.into_field().into_arrow_schema()
    for write in range(2):
        dataset.append_arrow_table(
            pyarrow.Table.from_pydict(
                {
                    "symbol": [f"S{write}-{index}" for index in range(4)],
                    "day": [first, first, second, second],
                    "size": [0, 1, 0, 1],
                    "venue": ["XPAR"] * 4,
                },
                schema=schema,
            )
        )

    assert dataset.data_files().num_rows == 4, "two writes made one file per partition"
    before = len(dataset.iceberg_table.snapshots())

    assert dataset.delete_where("size = 1") == 4
    assert len(dataset.iceberg_table.snapshots()) - before == 1, (
        "every candidate file, rewritten one at a time, lands in one commit"
    )
    assert dataset.data_files().num_rows == 4, "each partial file was replaced in place"
    assert dataset.read_arrow_table().column("size").to_pylist() == [0, 0, 0, 0]


def test_delete_is_a_no_op_for_a_missing_table_or_no_matches(
    dataset: IcebergDataset,
) -> None:
    assert dataset.delete_where(EqualTo("size", 2)) == 0
    assert not dataset.exists, "a delete does not create its missing target"

    dataset.append_arrow_table(quotes(3))
    before = len(dataset.iceberg_table.snapshots())
    assert dataset.delete("size > 100") == 0
    assert len(dataset.iceberg_table.snapshots()) == before
    assert dataset.read_arrow_table().num_rows == 3


# -- maintenance ------------------------------------------------------------


def test_many_small_writes_leave_many_files(dataset: IcebergDataset) -> None:
    for _ in range(4):
        dataset.append_arrow_table(quotes(1), merge_by=False)
    assert dataset.data_files().num_rows >= 4


def test_compaction_rewrites_the_fragments(dataset: IcebergDataset) -> None:
    for index in range(4):
        dataset.append_arrow_table(quotes(2, f"venue{index}"), merge_by=False)
    before = dataset.data_files().num_rows
    rewritten = dataset.compact(min_files=2)
    assert rewritten == before
    assert dataset.data_files().num_rows < before, "the fragments became fewer files"
    assert dataset.read_arrow_table().num_rows == 8, "and every row survived"


def test_compaction_is_a_no_op_when_there_is_nothing_to_do(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    assert dataset.compact(min_files=5) == 0


def test_compaction_plans_one_partition_at_a_time(dataset: IcebergDataset) -> None:
    """A partition is a predicate when the transform is identity, so it can be
    rewritten without touching the rest of the table."""
    dataset.append_arrow_table(quotes(2))
    dataset.append_arrow_table(quotes(2), merge_by=False)
    plan = dataset.compaction_plan(min_files=2)
    assert len(plan) == 1
    assert plan[0][0] == EqualTo("day", datetime.date(2026, 8, 14)), (
        "an expression, not a string to parse back"
    )


def test_an_unpartitioned_table_compacts(tmp_path: Path) -> None:
    """The most ordinary table shape there is, and every verb raised on it."""

    @scalar
    class Flat:
        """A row with nothing to partition on."""

        symbol: str
        """Instrument."""

        size: int
        """Quantity."""

    catalog = IcebergCatalog(name="flat", properties=catalog_properties(tmp_path))
    flat = catalog.dataset("trading.flat", field=Flat.into_field())
    schema = Flat.into_field().into_arrow_schema()
    for index in range(4):
        flat.append_arrow(
            pyarrow.Table.from_pydict({"symbol": [f"S{index}"], "size": [index]}, schema=schema),
            commit_row_size=1_000_000,
        )
    before = flat.read_arrow_table().num_rows
    assert flat.compaction_plan(min_files=2) == [(None, 4)], "the whole table, as one part"
    assert flat.compact(min_files=2) == 4
    assert flat.refresh().data_files().num_rows == 1
    assert flat.read_arrow_table().num_rows == before
    assert flat.compact(min_files=2) == 0, "and it settles"


def test_a_transformed_partition_settles(tmp_path: Path) -> None:
    """The table this could only ever address as a whole, and so never settled.

    A `day` partition is not an identity, so the plan is the whole table under
    one key -- but the freshness test asked the *per-partition* question, which
    nothing on this branch ever records an answer to. Measured before the fix:
    16 files rewritten, then 4, then 4, forever, with `compaction_marks()`
    empty throughout, while the rows never changed. Every `optimize` on a
    `day`- or `bucket[16]`-partitioned table read it whole and wrote it whole.
    """

    @scalar
    class Event:
        """One event, partitioned by a transform of its timestamp."""

        symbol: str
        """Instrument."""

        at: Annotated[datetime.datetime, partition_key("day")]
        """When it happened."""

    catalog = IcebergCatalog(name="daily", properties=catalog_properties(tmp_path))
    daily = catalog.dataset("trading.daily", field=Event.into_field())
    schema = Event.into_field().into_arrow_schema()
    base = datetime.datetime(2026, 8, 14, tzinfo=UTC)
    for index in range(4):
        daily.append_arrow(
            pyarrow.Table.from_pydict(
                {
                    "symbol": [f"S{index}", f"T{index}"],
                    "at": [base, base + datetime.timedelta(days=1)],
                },
                schema=schema,
            ),
            commit_row_size=1_000_000,
        )
    assert str(daily.iceberg_table.spec().fields[0].transform) == "day", "not an identity"
    before = daily.read_arrow_table().num_rows

    first = daily.compact(min_files=2)
    assert first == 8, "every file, since a transform hides which rows are where"
    assert daily.compaction_marks(), "and what it rewrote was recorded"
    assert daily.compact(min_files=2) == 0, "so the second pass has nothing to do"
    assert daily.compact(min_files=2) == 0, "and so does the third"
    assert daily.refresh().read_arrow_table().num_rows == before

    daily.append_arrow(
        pyarrow.Table.from_pydict({"symbol": ["U"], "at": [base]}, schema=schema),
        commit_row_size=1_000_000,
    )
    assert daily.compact(min_files=2) > 0, "and a commit since unsettles it again"


@pytest.mark.parametrize("value", ["o'brien", "a b", None])
def test_a_partition_value_a_filter_string_cannot_hold(tmp_path: Path, value: str | None) -> None:
    """The predicate is an expression: an apostrophe has nothing to escape into.

    A null value is `IsNull` and not a dropped term -- dropping it left a
    predicate matching every other partition, so one stale partition rewrote
    the whole table and reported the count of one.
    """

    @scalar
    class Part:
        """A row partitioned by a string that may be awkward."""

        part: Annotated[str | None, partition_key()]
        """The partition."""

        size: int
        """Quantity."""

    catalog = IcebergCatalog(name="lit", properties=catalog_properties(tmp_path))
    parted = catalog.dataset("trading.parts", field=Part.into_field())
    schema = Part.into_field().into_arrow_schema()

    def rows(part: str | None, size: int) -> pyarrow.Table:
        return pyarrow.Table.from_pydict({"part": [part], "size": [size]}, schema=schema)

    for index in range(3):
        parted.append_arrow(rows(value, index), commit_row_size=1_000_000)
    parted.append_arrow(rows("untouched", 99), commit_row_size=1_000_000)
    before = sorted(parted.read_arrow_table().to_pylist(), key=lambda row: row["size"])
    others = {file["file_path"] for file in parted.refresh().data_files().to_pylist()}

    assert parted.compact(min_files=2) == 3, "the three files of that partition, and no more"
    after = parted.refresh()
    assert sorted(after.read_arrow_table().to_pylist(), key=lambda row: row["size"]) == before
    kept = others & {file["file_path"] for file in after.data_files().to_pylist()}
    assert len(kept) == 1, "the other partition's file was not rewritten"
    assert after.compact(min_files=2) == 0, "and it settles"


def test_compaction_settles_on_a_branch(dataset: IcebergDataset) -> None:
    """The plan came from main whatever branch the rewrite went to."""
    for _ in range(3):
        dataset.append_arrow(quotes(2), commit_row_size=1_000_000, merge_by=False)
    table = dataset.get_or_create_table()
    table.manage_snapshots().create_branch(table.current_snapshot().snapshot_id, "work").commit()
    dataset.refresh()
    for index in range(3):
        dataset.append_arrow(
            quotes(2, f"v{index}"), branch="work", commit_row_size=1_000_000, merge_by=False
        )
    assert dataset.compact(min_files=2, branch="work") > 0
    assert dataset.compact(min_files=2, branch="work") == 0, "it settles on the branch"
    assert dataset.compaction_plan(min_files=2, branch="work") == []
    assert dataset.compaction_plan(min_files=2) != [], "and main is still its own plan"


def test_a_filtered_compaction_marks_nothing(dataset: IcebergDataset) -> None:
    """A caller's filter may cover a fraction of a partition; the rest still needs it."""
    for _ in range(3):
        dataset.append_arrow(quotes(2), commit_row_size=1_000_000, merge_by=False)
    assert dataset.compact(row_filter="symbol = 'S0'") > 0
    assert dataset.compaction_marks() == {}
    assert dataset.compaction_plan(min_files=2) != [], "the partition is still planned"


def test_a_nested_addition_does_not_rewrite_its_parent(tmp_path: Path) -> None:
    nested = pyarrow.struct([pyarrow.field("a", pyarrow.string())])
    narrow = field_of(
        pyarrow.schema(
            [
                pyarrow.field(
                    "nested",
                    nested,
                    nullable=False,
                    metadata={b"description": b"Held parent."},
                )
            ]
        ),
        "NestedRows",
    )
    wider = field_of(
        pyarrow.schema(
            [
                pyarrow.field(
                    "nested",
                    pyarrow.struct([*nested, pyarrow.field("b", pyarrow.string())]),
                    metadata={b"description": b"Incoming parent."},
                )
            ]
        ),
        "NestedRows",
    )
    catalog = IcebergCatalog(name="parent", properties=catalog_properties(tmp_path))
    rows = catalog.dataset("trading.parent", field=narrow)
    rows.append_arrow(
        pyarrow.Table.from_pylist(
            [{"nested": {"a": "A"}}],
            schema=narrow.into_arrow_schema(),
        )
    )

    assert rows.add_fields(wider) == ["nested.b"]

    parent = rows.iceberg_table.schema().find_field("nested")
    assert parent.required
    assert parent.doc == "Held parent."


def test_a_member_added_inside_a_struct_is_added(tmp_path: Path) -> None:
    """The direct leaf update finds additions below an existing parent."""

    @scalar
    class Venue:
        """Where it traded."""

        mic: str | None = None
        """Market identifier."""

    @scalar
    class Narrow:
        """A quote whose venue knows only its mic."""

        symbol: str
        """Instrument."""

        venue: Venue | None = None
        """Where."""

    @scalar
    class Wide:
        """The same quote, whose venue has grown a country."""

        symbol: str
        """Instrument."""

        venue: Venue | None = None
        """Where."""

    wide = field_of(
        pyarrow.schema(
            [
                pyarrow.field("symbol", pyarrow.string(), nullable=False),
                pyarrow.field(
                    "venue",
                    pyarrow.struct(
                        [
                            pyarrow.field("mic", pyarrow.string()),
                            pyarrow.field("country", pyarrow.string()),
                        ]
                    ),
                ),
            ]
        ),
        Wide.into_field().name,
    )
    catalog = IcebergCatalog(name="nested", properties=catalog_properties(tmp_path))
    quotes_ = catalog.dataset("trading.nested", field=Narrow.into_field())
    narrow_schema = Narrow.into_field().into_arrow_schema()
    quotes_.append_arrow(
        pyarrow.Table.from_pydict(
            {"symbol": ["A"], "venue": [{"mic": "XPAR"}]}, schema=narrow_schema
        ),
        commit_row_size=1_000_000,
    )
    assert quotes_.add_fields(wide) == ["venue.country"]
    assert quotes_.add_fields(wide) == [], "nothing new, so no commit"
    quotes_.refresh()
    quotes_.append_arrow(
        pyarrow.Table.from_pydict(
            {"symbol": ["B"], "venue": [{"mic": "XLON", "country": "GB"}]},
            schema=wide.into_arrow_schema(),
        ),
        commit_row_size=1_000_000,
    )
    stored = sorted(quotes_.refresh().read_arrow_table().to_pylist(), key=lambda row: row["symbol"])
    assert stored[1]["venue"] == {"mic": "XLON", "country": "GB"}, "the value survived the write"


def test_a_member_added_inside_a_list_struct_is_added(tmp_path: Path) -> None:
    """FIX repeating groups evolve through Iceberg's list element wrapper."""
    narrow = field_of(
        pyarrow.schema(
            [
                pyarrow.field("symbol", pyarrow.string(), nullable=False),
                pyarrow.field(
                    "groups",
                    pyarrow.list_(pyarrow.struct([pyarrow.field("tag", pyarrow.int32())])),
                ),
            ]
        ),
        "ListRows",
    )
    wide = field_of(
        pyarrow.schema(
            [
                pyarrow.field("symbol", pyarrow.string(), nullable=False),
                pyarrow.field(
                    "groups",
                    pyarrow.list_(
                        pyarrow.struct(
                            [
                                pyarrow.field("tag", pyarrow.int32()),
                                pyarrow.field("value", pyarrow.string()),
                            ]
                        )
                    ),
                ),
            ]
        ),
        "ListRows",
    )
    catalog = IcebergCatalog(name="list_nested", properties=catalog_properties(tmp_path))
    rows = catalog.dataset("trading.list_nested", field=narrow)
    rows.append_arrow(
        pyarrow.Table.from_pylist(
            [{"symbol": "A", "groups": [{"tag": 35}]}],
            schema=narrow.into_arrow_schema(),
        )
    )

    assert rows.add_fields(wide) == ["groups.element.value"]
    assert rows.add_fields(wide) == []
    rows.append_arrow(
        pyarrow.Table.from_pylist(
            [{"symbol": "B", "groups": [{"tag": 35, "value": "D"}]}],
            schema=wide.into_arrow_schema(),
        )
    )

    stored = sorted(rows.refresh().read_arrow_table().to_pylist(), key=lambda row: row["symbol"])
    assert stored[0]["groups"] == [{"tag": 35, "value": None}]
    assert stored[1]["groups"] == [{"tag": 35, "value": "D"}]


def test_a_filter_compacts_only_that_part(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.append_arrow_table(quotes(2), merge_by=False)
    assert dataset.compact(row_filter="day = '2026-08-14'") > 0
    assert dataset.read_arrow_table().num_rows == 4


def test_cleanup_expires_old_snapshots(dataset: IcebergDataset) -> None:
    for _ in range(2):
        dataset.append_arrow_table(quotes(1), merge_by=False)
    report = dataset.cleanup(retain=1, remove_orphans=False)
    assert report["expired"] == 1
    assert dataset.refresh().snapshots().num_rows == 1
    assert dataset.read_arrow_table().num_rows == 2, "the data is still all there"


def test_a_stream_write_expires_once_after_all_of_its_commits(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    expired: list[object] = []
    original = dataset.expire_snapshots

    def capture(value: object = None) -> int:
        expired.append(value)
        return original(value)

    monkeypatch.setattr(dataset, "expire_snapshots", capture)
    retention = datetime.timedelta(0)
    dataset.append_arrow_reader(
        quotes(3).to_reader(max_chunksize=1),
        commit_row_size=1,
        snapshot_expiry=retention,
    )

    assert expired == [retention]
    assert len(dataset.iceberg_table.snapshots()) == 1
    assert dataset.read_arrow_table().num_rows == 3


def test_snapshot_expiry_accepts_an_absolute_string_on_a_merge(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(quotes(1))
    dataset.append_arrow_table(keyed("A", 1))
    assert len(dataset.iceberg_table.snapshots()) == 2

    assert dataset.merge_arrow_table(keyed("B", 1), snapshot_expiry="2100-01-01T00:00:00Z") == 1

    assert len(dataset.iceberg_table.snapshots()) == 1
    assert dataset.read_arrow_table().num_rows == 3


def test_snapshot_expiry_falls_back_to_the_configured_table_age(tmp_path: Path) -> None:
    properties = catalog_properties(tmp_path)
    existing = IcebergDataset(
        name="expiring",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=properties,
    )
    existing.append_arrow_table(quotes(1))
    existing.append_arrow_table(keyed("A", 1))
    assert len(existing.iceberg_table.snapshots()) == 2

    retained = IcebergDataset(
        name="expiring",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=properties,
        table_properties={"history.expire.max-snapshot-age-ms": "0"},
    )

    retained.append_arrow_reader(keyed("B", 3).to_reader(max_chunksize=1), commit_row_size=1)

    assert len(retained.iceberg_table.snapshots()) == 1
    assert retained.read_arrow_table().num_rows == 5


def test_an_invalid_snapshot_expiry_is_refused_before_a_write(dataset: IcebergDataset) -> None:
    with pytest.raises(ValueError, match="is not a datetime"):
        dataset.append_arrow_table(quotes(1), snapshot_expiry="next week-ish")

    assert dataset.iceberg_table.snapshots() == []


def test_cleanup_can_report_without_touching_anything(dataset: IcebergDataset) -> None:
    for _ in range(2):
        dataset.append_arrow_table(quotes(1), merge_by=False)
    report = dataset.cleanup(retain=1, dry_run=True)
    assert report["expired"] == 1
    assert dataset.refresh().snapshots().num_rows == 2


def test_cleanup_keeps_what_a_branch_still_references(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    dataset.create_branch("dev")
    dataset.append_arrow_table(quotes(1), merge_by=False)
    dataset.cleanup(retain=1, remove_orphans=False)
    assert dataset.refresh().snapshots().num_rows >= 2, "the branch head survived"


def test_cleanup_sweeps_the_files_expiry_stranded(dataset: IcebergDataset) -> None:
    """Expiry is metadata-only, so the sweep is the half that reclaims space."""
    for index in range(3):
        dataset.append_arrow_table(quotes(2, f"venue{index}"), merge_by=False)
    dataset.compact(min_files=2)
    assert dataset.orphan_files(older_than=datetime.timedelta(seconds=0)) == [], (
        "the files compaction replaced are still held by the snapshots before it"
    )
    report = dataset.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0))
    assert report["expired"] > 0
    assert report["deleted"] > 0, "expiring the old snapshots is what made them garbage"
    assert report["bytes"] > 0
    assert dataset.read_arrow_table().num_rows == 6, "only garbage went"


def test_the_live_set_walks_the_manifests_once(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Data and metadata are two readings of the same manifests. Two walks
    decoded every retained snapshot's manifest list twice -- 96 ms against 76
    on a 40-commit table, and a round trip per snapshot on a cold store."""
    from rekep.iceberg import dataset as module

    for index in range(3):
        dataset.append_arrow_table(quotes(2, f"venue{index}"), merge_by=False)
    table = dataset.iceberg_table
    walks = 0
    original = module._manifests

    def counted(target: object) -> object:
        nonlocal walks
        walks += 1
        return original(target)

    monkeypatch.setattr(module, "_manifests", counted)
    data, files = dataset._live(table)
    assert walks == 1
    assert data and files


def test_every_snapshots_manifest_list_is_live(dataset: IcebergDataset) -> None:
    """Collected from the snapshots and not from the manifest walk, which
    dedupes on the manifest path: a snapshot reaching only manifests another
    already reached would never have its own list named, and this is the set
    that may not be narrow."""
    for index in range(3):
        dataset.append_arrow_table(quotes(2, f"venue{index}"), merge_by=False)
    table = dataset.iceberg_table
    _, files = dataset._live(table)
    lists = {snapshot.manifest_list for snapshot in table.snapshots() if snapshot.manifest_list}
    assert lists and lists <= files


def test_a_recent_file_is_never_swept(dataset: IcebergDataset) -> None:
    """A writer committing right now has files no snapshot mentions yet."""
    dataset.append_arrow_table(quotes(2))
    dataset.append_arrow_table(quotes(2), merge_by=False)
    dataset.compact(min_files=2)
    assert dataset.orphan_files() == [], "nothing is old enough to be garbage"


def test_optimize_does_the_whole_routine(dataset: IcebergDataset) -> None:
    for index in range(4):
        dataset.append_arrow_table(quotes(2, f"venue{index}"), merge_by=False)
    report = dataset.optimize(min_files=2)
    assert report["rewritten"] > 0
    assert report["expired"] > 0
    assert dataset.iceberg_table.properties["commit.manifest-merge.enabled"] == "true"
    assert dataset.read_arrow_table().num_rows == 8


def test_optimize_retrofits_missing_metadata_maintenance_properties(tmp_path: Path) -> None:
    external = IcebergDataset(
        name="external",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
        optimize_commits=False,
    )
    external.append_arrow_table(quotes(2))
    external.optimize(remove_orphans=False)
    properties = external.iceberg_table.properties
    assert properties["commit.manifest-merge.enabled"] == "true"
    assert properties["commit.manifest.min-count-to-merge"] == "10"
    assert properties["write.metadata.previous-versions-max"] == "20"
    assert properties["write.metadata.delete-after-commit.enabled"] == "true"


def test_optimize_keeps_explicit_metadata_retention(tmp_path: Path) -> None:
    external = IcebergDataset(
        name="retained",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
        optimize_commits=False,
        table_properties={
            "write.metadata.previous-versions-max": "80",
            "write.metadata.delete-after-commit.enabled": "false",
        },
    )
    external.append_arrow_table(quotes(2))
    external.optimize(remove_orphans=False)
    properties = external.iceberg_table.properties
    assert properties["write.metadata.previous-versions-max"] == "80"
    assert properties["write.metadata.delete-after-commit.enabled"] == "false"


def test_properties_are_set_in_one_commit(dataset: IcebergDataset) -> None:
    dataset.create_with()
    dataset.set_properties({"write.target-file-size-bytes": "1048576"})
    assert dataset.iceberg_table.properties["write.target-file-size-bytes"] == "1048576"


def test_target_file_size_is_icebergs_knob_not_ours(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.append_arrow_table(quotes(2), merge_by=False)
    dataset.compact(min_files=2, target_file_size=8 * 1024 * 1024)
    assert dataset.iceberg_table.properties["write.target-file-size-bytes"] == str(8 * 1024 * 1024)


# -- field ids --------------------------------------------------------------


def test_a_schema_that_carries_ids_keeps_them(dataset: IcebergDataset) -> None:
    """Iceberg matches columns by id: taking the ids back is what keeps a
    round trip lossless instead of renumbering every column."""
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    dataset.append_arrow_table(quotes(1))
    declared = dataset.iceberg_table.schema()
    carried = field_of(schema_to_pyarrow(declared, include_field_ids=True))
    assert [f.field_id for f in iceberg_schema(carried).fields] == [
        f.field_id for f in declared.fields
    ]


def test_a_plain_arrow_schema_is_numbered_for_the_user(tmp_path: Path) -> None:
    schema = pyarrow.schema([pyarrow.field("a", pyarrow.int64(), nullable=False)])
    plain = IcebergDataset(
        name="plain",
        namespace="trading",
        field=field_of(schema, "plain"),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
    )
    plain.create_with()
    assert [f.field_id for f in plain.iceberg_table.schema().fields] == [1]
    assert plain.field.name == "plain"


# -- commits ----------------------------------------------------------------


def test_a_write_commits_in_chunks_of_the_datasets_own_size(dataset: IcebergDataset) -> None:
    dataset.commit_row_size = 2
    dataset.append_arrow_reader(quotes(6).to_reader(max_chunksize=1))
    assert len(dataset.iceberg_table.history()) == 3, "the dataset's size, with no call saying so"


def test_the_default_commit_boundary_is_eight_source_batches(dataset: IcebergDataset) -> None:
    assert dataset.commit_batch_num == 8
    assert dataset.commit_row_size is None
    dataset.append_arrow_reader(quotes(10).to_reader(max_chunksize=1))
    assert len(dataset.iceberg_table.history()) == 2


def test_a_write_can_override_the_commit_batch_count(dataset: IcebergDataset) -> None:
    dataset.append_arrow_reader(quotes(5).to_reader(max_chunksize=1), commit_batch_num=2)
    assert len(dataset.iceberg_table.history()) == 3


def test_commit_row_size_must_bound_the_stream(dataset: IcebergDataset) -> None:
    with pytest.raises(ValueError, match="commit_row_size must be positive"):
        dataset.append_arrow_reader(quotes(6).to_reader(max_chunksize=1), commit_row_size=0)


def test_commit_batch_num_must_bound_the_stream(dataset: IcebergDataset) -> None:
    with pytest.raises(ValueError, match="commit_batch_num must be positive"):
        dataset.append_arrow_reader(quotes(6).to_reader(max_chunksize=1), commit_batch_num=0)


@pytest.mark.parametrize("field", ["commit_batch_num", "commit_row_size"])
@pytest.mark.parametrize("value", [True, 1.5, "8"])
def test_commit_bounds_have_strict_constructor_types(
    dataset: IcebergDataset, field: str, value: Any
) -> None:
    with pytest.raises(TypeError, match=rf"{field} must be an integer"):
        dataclasses.replace(dataset, **{field: value})


@pytest.mark.parametrize("argument", ["commit_batch_num", "commit_row_size"])
@pytest.mark.parametrize("value", [False, 2.5, "2"])
def test_commit_bounds_have_strict_call_types(
    dataset: IcebergDataset, argument: str, value: Any
) -> None:
    with pytest.raises(TypeError, match=rf"{argument} must be an integer"):
        dataset.append_arrow_reader(
            quotes(2).to_reader(max_chunksize=1),
            **{argument: value},
        )


def test_one_input_batch_with_several_partition_runs_is_one_commit(
    dataset: IcebergDataset,
) -> None:
    today = quotes(2)
    tomorrow = today.set_column(
        today.schema.get_field_index("day"),
        today.schema.field("day"),
        pyarrow.array([datetime.date(2026, 8, 15)] * today.num_rows),
    )
    rows = pyarrow.concat_tables([today, tomorrow], promote_options="none").combine_chunks()
    (batch,) = rows.to_batches(max_chunksize=rows.num_rows)
    reader = pyarrow.RecordBatchReader.from_batches(rows.schema, [batch])

    dataset.overwrite_arrow_reader(reader, commit_batch_num=1)

    assert len(dataset.iceberg_table.history()) == 1
    assert dataset.read_arrow_table().num_rows == 4


def test_one_large_input_batch_respects_the_partition_commit_row_limit(
    dataset: IcebergDataset,
) -> None:
    first = datetime.date(2026, 8, 14)
    rows = pyarrow.Table.from_pydict(
        {
            "symbol": [f"S{index}" for index in range(6)],
            "day": [first + datetime.timedelta(days=index) for index in range(6)],
            "size": list(range(6)),
            "venue": ["XPAR"] * 6,
        },
        schema=Quote.into_field().into_arrow_schema(),
    )
    (batch,) = rows.to_batches(max_chunksize=rows.num_rows)
    reader = pyarrow.RecordBatchReader.from_batches(rows.schema, [batch])

    dataset.overwrite_arrow_reader(reader, commit_row_size=2)

    assert len(dataset.iceberg_table.history()) == 3
    assert dataset.read_arrow_table().num_rows == 6


def test_a_partition_split_across_chunks_is_emptied_once_and_then_added_to(
    dataset: IcebergDataset,
) -> None:
    """Six batches, each straddling two days, in three commits: a day the
    first chunk empties is only added to by the chunk after it."""
    first = datetime.date(2026, 8, 14)
    dataset.append_arrow_table(quotes(3))
    batches = []
    for index in range(6):
        rows = pyarrow.Table.from_pydict(
            {
                "symbol": [f"S{index:02d}a", f"S{index:02d}b"],
                "day": [
                    first + datetime.timedelta(days=index),
                    first + datetime.timedelta(days=index + 1),
                ],
                "size": [index * 2, index * 2 + 1],
                "venue": ["XPAR", "XPAR"],
            },
            schema=Quote.into_field().into_arrow_schema(),
        )
        batches.extend(rows.to_batches(max_chunksize=rows.num_rows))
    reader = pyarrow.RecordBatchReader.from_batches(batches[0].schema, batches)
    before = len(dataset.iceberg_table.history())

    assert dataset.overwrite_arrow_reader(reader, commit_batch_num=2) == 12

    assert len(dataset.iceberg_table.history()) - before == 3, "one commit per two batches"
    stored = dataset.read_arrow_table().to_pylist()
    assert len(stored) == 12, "the three stored rows of the first day went with it"
    by_day: dict[datetime.date, set[str]] = {}
    for row in stored:
        by_day.setdefault(row["day"], set()).add(row["symbol"])
    assert by_day[first] == {"S00a"}
    assert by_day[first + datetime.timedelta(days=1)] == {"S00b", "S01a"}, (
        "the day straddled by two batches of one chunk holds both halves"
    )
    assert by_day[first + datetime.timedelta(days=2)] == {"S01b", "S02a"}, (
        "and one straddled by two chunks holds both too: emptied once, then added to"
    )


class _SpillBoundedBatches:
    """A source that refuses to get one chunk ahead of the writer's spill."""

    def __init__(self, source: pyarrow.Table, rows: int) -> None:
        self.batches = iter(source.to_batches(max_chunksize=1))
        self.rows = rows
        self.consumed = 0
        self.spilled = 0
        self.max_held = 0
        self.drained = False

    def __iter__(self) -> "_SpillBoundedBatches":
        return self

    def __next__(self) -> pyarrow.RecordBatch:
        held = self.consumed - self.spilled
        if held >= self.rows:
            pytest.fail("the writer consumed beyond one chunk before spilling it")
        batch = next(self.batches, None)
        if batch is None:
            self.drained = True
            raise StopIteration
        self.consumed += batch.num_rows
        self.max_held = max(self.max_held, held + batch.num_rows)
        return batch

    def read_all(self) -> None:
        pytest.fail("a streamed Iceberg write must not collect its source")


def _spill_bounded(
    dataset: IcebergDataset,
    source: _SpillBoundedBatches,
    chunk_method: str,
    monkeypatch: pytest.MonkeyPatch,
) -> list[int]:
    """Credit `source` with the rows each first-generation run spills, and
    refuse a chunk `chunk_method` commits before the source ran dry. Returns
    the rows of each committed chunk, in commit order."""
    from rekep.iceberg import dataset as module

    write_run = module._write_ipc_batches
    committed: list[int] = []
    original = getattr(dataset, chunk_method)

    def spilled(path: str, schema: pyarrow.Schema, batches: Any) -> None:
        batches = list(batches)
        if os.path.basename(path).startswith("0-"):
            source.spilled += sum(batch.num_rows for batch in batches)
        write_run(path, schema, batches)

    def commit(table: Any, chunk: pyarrow.Table, *args: Any, **kwargs: Any) -> Any:
        assert source.drained, "the whole stream is spilled before the first commit"
        committed.append(chunk.num_rows)
        return original(table, chunk, *args, **kwargs)

    monkeypatch.setattr(module, "_write_ipc_batches", spilled)
    monkeypatch.setattr(dataset, chunk_method, commit)
    return committed


def _streaming_dataset(
    backend: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[IcebergDataset, str]:
    """The same SQL catalog with local or S3 table bytes."""
    properties = catalog_properties(tmp_path, backend)
    target = IcebergDataset(
        name="streamed",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name=f"streamed-{backend}",
        catalog_properties=properties,
    )
    if backend == "local":
        target.get_or_create_table()
        return target, "file:"

    remote = pyarrow.fs._MockFileSystem()
    remote.create_dir("bucket/metadata", recursive=True)
    remote.create_dir("bucket/data/day=2026-08-14", recursive=True)
    original = IcebergFileIO._initialize_fs

    def initialized(
        self: IcebergFileIO, scheme: str, netloc: str | None = None
    ) -> pyarrow.fs.FileSystem:
        return remote if scheme in {"s3", "s3a", "s3n"} else original(self, scheme, netloc)

    monkeypatch.setattr(IcebergFileIO, "_initialize_fs", initialized)
    target.create_with_field(
        target.field,
        location="s3://bucket/tables/streamed",
        properties={
            "write.data.path": "s3://bucket/data",
            "write.metadata.path": "s3://bucket/metadata",
        },
    )
    return target, "s3://bucket/data/"


@pytest.mark.parametrize("backend", ["local", "s3"])
def test_default_batch_commits_bound_consumption_on_local_and_remote_storage(
    backend: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset, _ = _streaming_dataset(backend, tmp_path, monkeypatch)
    source = _SpillBoundedBatches(quotes(10), rows=8)
    committed = _spill_bounded(dataset, source, "_append_key_chunk", monkeypatch)

    assert dataset.append_arrow_reader(_owned_reader(source)) == 10
    assert source.max_held == 8, "eight source batches held, then spilled"
    assert committed == [8, 2], "and read back in chunks of the largest one spilled"
    assert len(dataset.iceberg_table.history()) == 2


@pytest.mark.parametrize("backend", ["local", "s3"])
def test_partial_delete_streams_on_local_and_remote_storage(
    backend: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pyiceberg.io.pyarrow import ArrowScan

    dataset, _ = _streaming_dataset(backend, tmp_path, monkeypatch)
    dataset.append_arrow_table(quotes(4), commit_row_size=2)
    before = len(dataset.iceberg_table.snapshots())
    monkeypatch.setattr(
        ArrowScan,
        "to_table",
        lambda *_args, **_kwargs: pytest.fail("delete must stream remote files too"),
    )

    assert dataset.delete_where(EqualTo("size", 1)) == 1
    assert len(dataset.iceberg_table.snapshots()) == before + 1
    assert dataset.read_arrow_table().column("size").to_pylist() == [0, 2, 3]


@pytest.mark.parametrize("backend", ["local", "s3"])
@pytest.mark.parametrize("verb", list(CHUNK_METHODS))
def test_a_stream_write_is_spilled_whole_then_committed_chunk_by_chunk(
    backend: str,
    verb: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every verb takes its source one chunk at a time and spills each before
    it takes the next, so what it holds is one chunk; only a source run dry
    is committed, one snapshot per chunk read back."""
    dataset, data_prefix = _streaming_dataset(backend, tmp_path, monkeypatch)
    if verb in {"merge", "partitions"}:
        dataset.append_arrow_table(quotes(3, "before"), commit_row_size=1_000_000)
    source = _SpillBoundedBatches(quotes(3, "after"), rows=1)
    before = len(dataset.iceberg_table.history())
    committed = _spill_bounded(dataset, source, CHUNK_METHODS[verb], monkeypatch)

    assert _written(dataset, verb, _owned_reader(source)) == 3

    assert source.max_held == 1, "one chunk held at a time"
    assert committed == [1, 1, 1]
    assert len(dataset.iceberg_table.history()) == before + 3, "one snapshot per bounded chunk"
    assert set(dataset.read_arrow_table().column("venue").to_pylist()) == {"after"}
    assert all(
        path.startswith(data_prefix)
        for path in dataset.data_files().column("file_path").to_pylist()
    )


def test_a_created_table_carries_the_commit_properties(dataset: IcebergDataset) -> None:
    dataset.create_with()
    properties = dataset.iceberg_table.properties
    assert properties["commit.manifest-merge.enabled"] == "true"
    assert properties["write.target-file-size-bytes"] == str(256 * 1024 * 1024)
    assert properties["write.parquet.row-group-limit"] == str(128 * 1024)


def test_iceberg_defaults_can_be_kept(tmp_path: Path) -> None:
    bare = IcebergDataset(
        name="bare",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
        optimize_commits=False,
    )
    bare.create_with()
    assert "commit.manifest-merge.enabled" not in bare.iceberg_table.properties


def test_declared_table_properties_win_over_the_defaults(tmp_path: Path) -> None:
    tuned = IcebergDataset(
        name="tuned",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
        table_properties={"write.target-file-size-bytes": "1024"},
    )
    tuned.create_with()
    assert tuned.iceberg_table.properties["write.target-file-size-bytes"] == "1024"
    assert tuned.iceberg_table.properties["commit.manifest-merge.enabled"] == "true"


# -- planning ---------------------------------------------------------------


def test_a_merge_of_new_keys_writes_without_reading(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    """The pruning short circuit, through `merge_arrow`: keys no stored file's
    bounds admit plan to nothing, so the merge commits what it was given
    without opening a data file."""
    dataset.append_arrow_table(quotes(3))
    before = len(dataset.iceberg_table.history())
    opened.clear()
    assert dataset.merge_arrow(keyed("T", 3)) == 3  # keys nothing stored shares
    dataset.refresh()
    assert opened.get("data", 0) == 0, "nothing was read to arrive at the append"
    assert dataset.read_arrow_table().num_rows == 6, "the new keys landed beside the stored ones"
    assert len(dataset.iceberg_table.history()) > before


def test_a_snapshot_id_and_a_branch_together_are_refused(dataset: IcebergDataset) -> None:
    """Nothing checks the snapshot belongs to the branch, so one had to be ignored.

    pyiceberg refuses the same pair for the same reason. The dataset's own
    default branch is not the same thing -- an explicit snapshot id is how a
    caller reads past it.
    """
    dataset.append_arrow(quotes(2), commit_row_size=1_000_000)
    table = dataset.get_or_create_table()
    first = table.current_snapshot().snapshot_id
    table.manage_snapshots().create_branch(first, "dev").commit()
    dataset.refresh()
    dataset.append_arrow(quotes(1, "later"), commit_row_size=1_000_000, merge_by=False)

    with pytest.raises(ValueError, match="two different states"):
        dataset.read_arrow_table(snapshot_id=first, branch="dev")
    with pytest.raises(ValueError, match="two different states"):
        dataset.scan_plan(snapshot_id=first, branch="dev")
    for alias in ("root", "main", "master"):
        assert dataset.read_arrow_table(snapshot_id=first, branch=alias).num_rows == 2
        assert dataset.scan_plan(snapshot_id=first, branch=alias)["rows"] == 2
    dataset.branch = "dev"
    assert dataset.read_arrow_table(snapshot_id=first).num_rows == 2, "a default is not a conflict"


def test_scan_plan_does_not_plan_the_table_twice(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The second plan was only ever there for the *count* of files, and
    Iceberg records that per snapshot. Measured on 17 files: 15.6 ms for the
    pair against 3.7 ms for the filtered plan alone."""
    for index in range(4):
        dataset.append_arrow(quotes(2, f"v{index}"), commit_row_size=1_000_000, merge_by=False)
    dataset.append_arrow(other_day(2), commit_row_size=1_000_000)
    plans: list[object] = []
    original = IcebergDataset._planned
    monkeypatch.setattr(
        IcebergDataset,
        "_planned",
        lambda self, table, row_filter, columns, snapshot_id, branch: (
            plans.append(row_filter),
            original(self, table, row_filter, columns, snapshot_id, branch),
        )[1],
    )
    plan = dataset.scan_plan("day = '2026-08-14'")
    assert len(plans) == 1, "one plan, and the total came off the snapshot summary"
    assert plan["files"] == 4 and plan["total_files"] == 5 and plan["skipped"] == 1


def test_scan_plan_counts_the_state_it_was_asked_about(dataset: IcebergDataset) -> None:
    """A snapshot id and a branch each name a state of their own, and the total
    a filter is measured against has to be that state's, not the table's now."""
    dataset.append_arrow(quotes(2), commit_row_size=1_000_000)
    early = dataset.iceberg_table.current_snapshot().snapshot_id
    dataset.create_branch("dev")
    for index in range(3):
        dataset.append_arrow(quotes(2, f"v{index}"), commit_row_size=1_000_000, merge_by=False)
    assert dataset.scan_plan("day = '2026-08-14'")["total_files"] == 4
    assert dataset.scan_plan("day = '2026-08-14'", snapshot_id=early)["total_files"] == 1
    assert dataset.scan_plan("day = '2026-08-14'", branch="dev")["total_files"] == 1


def test_a_streamed_merge_loads_the_table_once(dataset: IcebergDataset) -> None:
    """A commit updates the table it was made on, so no chunk reloads it.

    The catalog round trip is free on SQLite and a network hop on REST or Glue;
    at one per commit a streaming merge would pay it per chunk, to learn what
    it had just done itself.
    """
    dataset.append_arrow_table(quotes(9))
    loaded = 0
    original = dataset.store.load_table

    def counted(name: str):
        nonlocal loaded
        loaded += 1
        return original(name)

    dataset.refresh()
    dataset.store.load_table = counted  # type: ignore[method-assign]
    try:
        rows = quotes(9, "XETR")
        assert dataset.merge_arrow(rows.to_reader(max_chunksize=3), commit_row_size=3) == 9
    finally:
        del dataset.store.load_table
    assert loaded == 1, "one load for the whole stream, not one per commit"
    assert set(dataset.read_arrow_table().column("venue").to_pylist()) == {"XETR"}, (
        "and what was committed is visible without a reload"
    )


# -- filesystem frugality ----------------------------------------------------


def other_day(count: int) -> pyarrow.Table:
    """`quotes`, in a partition of its own, so a partition filter has one to skip."""
    return pyarrow.Table.from_pydict(
        {
            "symbol": [f"D{i}" for i in range(count)],
            "day": [datetime.date(2026, 8, 15)] * count,
            "size": list(range(count)),
            "venue": ["XPAR"] * count,
        },
        schema=Quote.into_field().into_arrow_schema(),
    )


def third_day(count: int) -> pyarrow.Table:
    """`quotes` in a third partition, so a chunk can span more than two."""
    return pyarrow.Table.from_pydict(
        {
            "symbol": [f"T{i}" for i in range(count)],
            "day": [datetime.date(2026, 8, 16)] * count,
            "size": list(range(count)),
            "venue": ["XPAR"] * count,
        },
        schema=Quote.into_field().into_arrow_schema(),
    )


def other_day_keyed(prefix: str, count: int) -> pyarrow.Table:
    """`other_day`, under a chosen key prefix."""
    return pyarrow.Table.from_pydict(
        {
            "symbol": [f"{prefix}{i}" for i in range(count)],
            "day": [datetime.date(2026, 8, 15)] * count,
            "size": list(range(count)),
            "venue": ["XPAR"] * count,
        },
        schema=Quote.into_field().into_arrow_schema(),
    )


def keyed(prefix: str, count: int) -> pyarrow.Table:
    """`quotes`, under keys that share nothing with the `S...` ones."""
    day = datetime.date(2026, 8, 14)
    return pyarrow.Table.from_pydict(
        {
            "symbol": [f"{prefix}{i}" for i in range(count)],
            "day": [day] * count,
            "size": list(range(count)),
            "venue": ["XPAR"] * count,
        },
        schema=Quote.into_field().into_arrow_schema(),
    )


@pytest.fixture
def opened(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Native PyArrow input opens, grouped by file kind."""
    from pyiceberg.io.pyarrow import PyArrowFile

    counts: dict[str, int] = {}
    original = PyArrowFile.open

    def counted(self: PyArrowFile, *args: object, **kwargs: object) -> object:
        kind = "data" if self.location.endswith(".parquet") else "metadata"
        counts[kind] = counts.get(kind, 0) + 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(PyArrowFile, "open", counted)
    return counts


def test_a_merge_reads_a_stored_file_one_batch_at_a_time(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The file whose keys a chunk replaces is streamed through the stager,
    never collected: what the write holds past the chunk is one batch."""
    from pyiceberg.io.pyarrow import ArrowScan

    dataset.append_arrow_table(quotes(3))
    monkeypatch.setattr(
        ArrowScan,
        "to_table",
        lambda *_args, **_kwargs: pytest.fail("a merge must not materialize the file it reads"),
    )
    assert dataset.merge_arrow_table(quotes(3, "XETR")) == 3
    assert set(dataset.read_arrow_table().column("venue").to_pylist()) == {"XETR"}


def test_a_merge_of_disjoint_keys_opens_no_data_file(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    """Keys no stored file's bounds admit plan to nothing, and nothing is read."""
    dataset.append_arrow_table(quotes(3))
    opened.clear()
    assert dataset.merge_arrow_table(keyed("T", 3)) == 3
    assert opened.get("data", 0) == 0, "the merge was an append, arrived at by planning"
    assert dataset.read_arrow_table().num_rows == 6


@pytest.mark.parametrize("count", [20, 300])
def test_overlapping_bounds_with_no_matching_key_leave_the_stored_file_standing(
    dataset: IcebergDataset, count: int
) -> None:
    """The stored file's bounds admit the chunk's keys, so it is read -- and
    every row survives, so it stands and the chunk is one appended file."""
    day = datetime.date(2026, 8, 14)

    def spaced(offset: int) -> pyarrow.Table:
        keys = [offset + index * 2 for index in range(count)]
        return pyarrow.Table.from_pydict(
            {
                "symbol": [f"S{key:06d}" for key in keys],
                "day": [day] * count,
                "size": keys,
                "venue": ["XPAR"] * count,
            },
            schema=Quote.into_field().into_arrow_schema(),
        )

    dataset.append_arrow_table(spaced(0))
    before = {row["file_path"] for row in dataset.data_files().to_pylist()}
    snapshots = len(dataset.iceberg_table.snapshots())

    assert dataset.merge_arrow_table(spaced(1)) == count
    assert dataset.read_arrow_table().num_rows == 2 * count
    after = {row["file_path"] for row in dataset.refresh().data_files().to_pylist()}
    assert before < after and len(after) == 2, "the stored file stands beside the new one"
    assert len(dataset.iceberg_table.snapshots()) == snapshots + 1
    assert dataset.iceberg_table.snapshots()[-1].summary.operation.value == "append"


def test_a_merge_that_empties_a_file_deletes_it_unread_after_comparing(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    dataset.append_arrow_table(quotes(3))
    before = len(dataset.iceberg_table.snapshots())
    stored = _data_paths(dataset)
    opened.clear()
    assert dataset.merge_arrow_table(quotes(3, "XETR")) == 3
    assert opened.get("data", 0) == 2, (
        "one stored file: by its keys to find them held and whole to find them changed; "
        "its record count then says no row of it survives"
    )
    after = _data_paths(dataset)
    assert len(after) == 1 and not after & stored, "so it is deleted, never written back"
    assert len(dataset.iceberg_table.snapshots()) == before + 1
    assert dataset.iceberg_table.snapshots()[-1].summary.operation.value == "overwrite"


def test_a_merge_reaches_a_branch_cut_before_a_key_was_renamed(
    dataset: IcebergDataset,
) -> None:
    """A rename is metadata-only, so the branch head still carries the old
    name in its files; the key is named the way the table names it now, and
    read out of the files by id."""
    dataset.append_arrow(quotes(3), commit_row_size=1_000_000)
    dataset.create_branch("dev")
    with dataset.iceberg_table.update_schema() as update:
        update.rename_column("symbol", "ticker")
    dataset.refresh()
    dataset.field = dataset.table_field

    replayed = dataset.read_arrow_table(dataset.field, branch="dev")
    assert dataset.merge_arrow_table(replayed, merge_by=["ticker"], branch="dev") == 0, (
        "every key found under its new name, holding what it holds"
    )
    assert dataset.read_arrow_table(branch="dev").num_rows == 3, "matched, not doubled"
    changed = replayed.set_column(
        replayed.schema.get_field_index("venue"),
        replayed.schema.field("venue"),
        pyarrow.array(["XETR"] * 3, replayed.schema.field("venue").type),
    )
    assert dataset.merge_arrow_table(changed, merge_by=["ticker"], branch="dev") == 3
    assert set(dataset.read_arrow_table(branch="dev").column("venue").to_pylist()) == {"XETR"}
    fresh = replayed.slice(0, 1).set_column(
        replayed.schema.get_field_index("ticker"),
        replayed.schema.field("ticker"),
        pyarrow.array(["NEW"], replayed.schema.field("ticker").type),
    )
    assert dataset.merge_arrow_table(fresh, merge_by=["ticker"], branch="dev") == 1
    assert dataset.read_arrow_table(branch="dev").num_rows == 4
    assert dataset.read_arrow_table().num_rows == 3, "and main is untouched"


def test_a_merge_onto_a_branch_without_the_key_column_is_all_new(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow(quotes(3), commit_row_size=1_000_000)
    dataset.create_branch("dev")
    wider = field_of(
        pyarrow.schema([*Quote.into_field().into_arrow_schema(), ("desk", pyarrow.string())]),
        Quote.into_field().name,
    )
    dataset.add_fields(wider)
    dataset.field = dataset.table_field
    source = quotes(3).append_column("desk", pyarrow.array(["A", "B", "C"]))
    incoming = dataset.field.apply_arrow_reader(source.to_reader(), safe=False).read_all()

    assert dataset.merge_arrow_table(incoming, merge_by=["desk"], branch="dev") == 3
    assert dataset.read_arrow_table(branch="dev").num_rows == 6
    assert dataset.read_arrow_table().num_rows == 3


def test_a_bare_limit_opens_only_the_files_it_needs(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    """pyiceberg submits every planned file before its row cap bites; the plan
    is cut here instead, so a peek at a wide table stays a peek."""
    for _ in range(3):
        dataset.append_arrow_table(quotes(4), merge_by=False)  # three commits, three files
    opened.clear()
    assert dataset.read_arrow_reader(limit=2).read_all().num_rows == 2
    assert opened.get("data", 0) == 1, "one file already held the two rows"


def test_a_reader_opens_no_more_files_than_it_reads_ahead(
    dataset: IcebergDataset, opened: dict[str, int], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A group shares bounded delete state while data files decode on demand."""
    from rekep.iceberg import dataset as module

    monkeypatch.setattr(module, "_read_ahead", lambda: 2)
    for index in range(6):
        dataset.append_arrow(quotes(2, f"v{index}"), commit_row_size=1_000_000, merge_by=False)
    opened.clear()
    reader = dataset.read_arrow_reader()
    assert reader.read_next_batch().num_rows > 0
    assert opened.get("data", 0) == 1, "one decoded task, not the whole group"
    assert sum(batch.num_rows for batch in reader) + 2 == 12, "and the rest still comes"
    assert opened.get("data", 0) == 6


def test_a_limit_is_the_readers_and_not_each_groups(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Handing every group the whole limit would answer `limit=3` with three
    rows per group, which on four groups is twelve."""
    from rekep.iceberg import dataset as module

    monkeypatch.setattr(module, "_read_ahead", lambda: 1)
    for index in range(4):
        dataset.append_arrow(quotes(2, f"v{index}"), commit_row_size=1_000_000, merge_by=False)
    found = dataset.read_arrow_reader(row_filter="size >= 0", limit=3).read_all()
    assert found.num_rows == 3


def test_a_limit_under_a_partition_filter_opens_only_the_files_it_needs(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    """A filter the partition fully answers leaves an `AlwaysTrue` residual, so
    every row of a planned file matches and its record count is exact again."""
    for _ in range(3):
        dataset.append_arrow_table(quotes(4), merge_by=False)  # three files, all in one day
    dataset.append_arrow_table(other_day(4))
    opened.clear()
    found = dataset.read_arrow_reader(row_filter="day = '2026-08-14'", limit=2).read_all()
    assert found.num_rows == 2
    assert opened.get("data", 0) == 1, "one of the day's three files held both rows"


def test_a_limited_read_over_a_null_partition_returns_its_rows(tmp_path: Path) -> None:
    """`AlwaysTrue` is not always true. pyiceberg resolves a filter against the
    partition value in Python, so a null partition answers `venue != 'XPAR'`
    with `None != 'XPAR'` -- True -- and the residual says the file matches
    whole. Arrow then applies the same filter in three-valued logic, where
    `NULL != 'XPAR'` is NULL, and drops every row of it. Trimming there stopped
    at a file contributing nothing: measured, `limit=5` over a table with ten
    matching rows returned **zero**.
    """

    @scalar
    class Trade:
        """A trade whose venue may be unknown."""

        venue: Annotated[str | None, partition_key()]
        """Where it traded, when known -- so a null partition exists."""

        size: int
        """Quantity."""

    catalog = IcebergCatalog(name="nullpart", properties=catalog_properties(tmp_path))
    trades = catalog.dataset("trading.trades", field=Trade.into_field())
    schema = Trade.into_field().into_arrow_schema()
    trades.append_arrow(
        pyarrow.Table.from_pydict({"venue": ["XNYS"] * 10, "size": list(range(10))}, schema=schema),
        commit_row_size=1_000_000,
    )
    trades.append_arrow(
        pyarrow.Table.from_pydict({"venue": [None] * 10, "size": list(range(10))}, schema=schema),
        commit_row_size=1_000_000,
    )
    table = trades.refresh().iceberg_table
    row_filter = "venue != 'XPAR'"
    planned = list(table.scan(row_filter=row_filter).plan_files())
    assert any(task.file.partition[0] is None for task in planned), (
        "the fixture needs a file in the null partition to mean anything"
    )
    assert all(str(task.residual) == "AlwaysTrue()" for task in planned), (
        "and every residual claiming the file matches whole, which is the trap"
    )
    for limit in (1, 5, 10):
        theirs = table.scan(row_filter=row_filter, limit=limit).to_arrow_batch_reader()
        assert (
            trades.read_arrow_reader(row_filter=row_filter, limit=limit).read_all().num_rows
            == theirs.read_all().num_rows
        )
    assert trades.read_arrow_table(row_filter=row_filter).num_rows == 10


def test_a_limit_under_a_filter_opens_one_file_at_a_time_until_satisfied(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    """`size >= 3` is not a partition, so the residual survives planning: how
    many rows a file contributes is only known once it is read. A one-file
    group keeps the unread third file closed once two matches have arrived."""
    for _ in range(3):
        dataset.append_arrow_table(quotes(4), merge_by=False)
    opened.clear()
    found = dataset.read_arrow_reader(row_filter="size >= 3", limit=2).read_all()
    assert found.num_rows == 2, "the cap on the rows is still pyiceberg's"
    assert opened.get("data", 0) == 2


def test_a_limit_of_zero_opens_nothing(dataset: IcebergDataset, opened: dict[str, int]) -> None:
    dataset.append_arrow_table(quotes(4))
    opened.clear()
    assert dataset.read_arrow_reader(limit=0).read_all().num_rows == 0
    assert opened.get("data", 0) == 0


def _task(records: int, *, deletes: bool = False, residual: object = None) -> object:
    """One planned file, as `_limited_reader` reads one."""
    import types

    from pyiceberg.expressions import AlwaysTrue

    return types.SimpleNamespace(
        delete_files={"pos"} if deletes else set(),
        residual=AlwaysTrue() if residual is None else residual,
        file=types.SimpleNamespace(record_count=records),
    )


def _trimmed_to(monkeypatch: pytest.MonkeyPatch, tasks: list, limit: int) -> list:
    """The tasks `_limited_reader` would actually open, for that plan and limit."""
    import types

    from rekep.iceberg import dataset as module

    handed: dict[str, list] = {}

    def capture(scan: object, taken: list) -> str:
        handed["tasks"] = list(taken)
        return "reader"

    monkeypatch.setattr(module, "_planned_reader", capture)
    scan = types.SimpleNamespace(plan_files=lambda: tasks)
    assert module._limited_reader(scan, limit) == "reader"
    return handed["tasks"]


def test_a_limit_over_delete_files_reads_the_whole_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A file carrying deletes holds fewer live rows than it counts, so
    trimming by `record_count` could under-deliver; the plan goes back whole."""
    tasks = [_task(5, deletes=True), _task(5)]
    assert _trimmed_to(monkeypatch, tasks, 1) == tasks


def test_a_limit_over_a_surviving_residual_streams_the_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A residual keeps the plan lazy and reduces read-ahead to one file."""
    import types

    from pyiceberg.expressions import GreaterThan

    from rekep.iceberg import dataset as module

    tasks = [_task(5, residual=GreaterThan("size", 3)), _task(5)]
    handed: dict[str, Any] = {}

    def capture(scan: object, planned: object, *, group_size: int | None = None) -> str:
        handed["tasks"] = list(planned)
        handed["group_size"] = group_size
        return "reader"

    monkeypatch.setattr(module, "_unordered_reader", capture)
    scan = types.SimpleNamespace(plan_files=lambda: iter(tasks))
    assert module._limited_reader(scan, 1) == "reader"
    assert handed == {"tasks": tasks, "group_size": 1}


def test_a_bare_limit_cuts_the_plan_at_the_records_it_needs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tasks = [_task(5) for _ in range(4)]
    assert _trimmed_to(monkeypatch, tasks, 7) == tasks[:2], "five rows are not seven; ten are"


def test_a_bare_limit_does_not_consume_the_rest_of_a_lazy_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import types

    from rekep.iceberg import dataset as module

    tasks = [_task(5) for _ in range(4)]
    consumed: list[object] = []

    def planned():
        for task in tasks:
            consumed.append(task)
            yield task

    handed: list[object] = []
    monkeypatch.setattr(
        module, "_planned_reader", lambda scan, selected: (handed.extend(selected), "reader")[1]
    )
    scan = types.SimpleNamespace(plan_files=planned)
    assert module._limited_reader(scan, 7) == "reader"
    assert consumed == tasks[:2]
    assert handed == tasks[:2]


def test_an_unlimited_unordered_read_hands_the_plan_through_lazily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import types

    from rekep.iceberg import dataset as module

    tasks = [_task(5) for _ in range(3)]
    consumed: list[object] = []

    def planned():
        for task in tasks:
            consumed.append(task)
            yield task

    handed: dict[str, Any] = {}

    def capture(scan: object, selected: object) -> str:
        handed["tasks"] = selected
        return "reader"

    monkeypatch.setattr(module, "_unordered_reader", capture)
    scan = types.SimpleNamespace(plan_files=planned)
    assert module._limited_reader(scan, None) == "reader"
    assert consumed == []
    assert next(handed["tasks"]) is tasks[0]
    assert consumed == tasks[:1]


def test_delete_files_stay_in_bounded_read_groups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rekep.iceberg import dataset as module

    tasks = [_task(5, deletes=True) for _ in range(5)]
    monkeypatch.setattr(module, "_read_ahead", lambda: 2)
    monkeypatch.setattr(module, "_partition_tasks", lambda scan, planned: [("day=x", tasks)])
    monkeypatch.setattr(module, "_scan_reader", lambda scan, groups: list(groups))

    groups = module._planned_reader(object(), tasks)

    assert [len(group) for group in groups] == [2, 2, 1]


def test_a_task_group_decodes_one_file_before_its_first_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pyiceberg.io import pyarrow as iceberg_arrow

    from rekep.iceberg import dataset as module

    tasks = [object() for _ in range(4)]
    decoded = []
    delete_groups = []

    class Scan:
        def _record_batches_from_scan_tasks_and_deletes(self, planned, deletes):
            assert deletes == {}
            for task in planned:
                decoded.append(task)
                yield pyarrow.record_batch([[len(decoded)]], names=["value"])

    monkeypatch.setattr(
        iceberg_arrow,
        "_read_all_delete_files",
        lambda io, planned: (delete_groups.append(list(planned)), {})[1],
    )
    batches = module._task_batches(Scan(), object(), tasks)

    assert next(batches).column("value").to_pylist() == [1]
    assert decoded == tasks[:1]
    assert delete_groups == [tasks]


def test_a_limit_of_zero_takes_no_file_however_the_plan_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The walk asks whether the limit is met *before* it looks at a task, so a
    limit already satisfied opens nothing -- not even the file whose deletes
    would otherwise have handed the whole plan back."""
    assert _trimmed_to(monkeypatch, [_task(5, deletes=True), _task(5)], 0) == []


def test_a_cleanup_does_not_reload_the_table_it_just_expired(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Expiry commits on the table object this holds and updates it in place.
    `refresh()` is for seeing *other* writers, and on a REST or Glue catalog it
    is a network hop."""
    for _ in range(2):
        dataset.append_arrow_table(quotes(1), merge_by=False)
    loads: list[str] = []
    original = IcebergCatalog.load_table
    monkeypatch.setattr(
        IcebergCatalog,
        "load_table",
        lambda self, name: (loads.append(str(name)), original(self, name))[1],
    )
    report = dataset.cleanup(retain=1, remove_orphans=False)
    assert report["expired"] == 1
    assert dataset.snapshots().num_rows == 1, "the object already knows what it expired"
    assert len(loads) == 1, (
        "and that took one load: the refresh cleanup opens with, which is there "
        "because a live set built from a stale table deletes another writer's files"
    )


def test_optimize_can_skip_the_sweep(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sweep lists the whole store, which is the expensive half of a
    routine a stream may want to run often. It used to be unreachable: every
    keyword went to `compact`, which raised on it."""
    from rekep.iceberg import dataset as module

    listed: list[str] = []
    original = module._store_of
    monkeypatch.setattr(
        module,
        "_store_of",
        lambda table, directory: (listed.append(directory), original(table, directory))[1],
    )
    for index in range(4):
        dataset.append_arrow(quotes(2, f"v{index}"), commit_row_size=1_000_000, merge_by=False)
    report = dataset.optimize(min_files=2, remove_orphans=False)
    assert report["rewritten"] > 0 and report["deleted"] == 0
    assert listed == [], "not one directory resolved, so not one listed"
    assert dataset.refresh().read_arrow_table().num_rows == 8
    assert dataset.optimize(min_files=2, orphan_age=datetime.timedelta(seconds=0))["deleted"] > 0
    assert listed, "and asking for the sweep still sweeps"


def test_an_explicit_empty_batch_commits_nothing(dataset: IcebergDataset) -> None:
    schema = Quote.into_field().into_arrow_schema()
    empty = pyarrow.RecordBatch.from_arrays(
        [pyarrow.array([], field.type) for field in schema], schema=schema
    )

    reader = pyarrow.RecordBatchReader.from_batches(schema, [empty])
    assert dataset.append_arrow_reader(reader) == 0
    assert dataset.iceberg_table.current_snapshot() is None


def test_writes_leave_maintenance_to_explicit_calls(dataset: IcebergDataset) -> None:
    dataset.append_arrow(quotes(4).to_reader(max_chunksize=1), commit_row_size=1)

    assert dataset.data_files().num_rows == 4
    assert len(dataset.iceberg_table.snapshots()) == 4
    assert {"auto_compact", "auto_optimize"}.isdisjoint(IcebergDataset.__dataclass_fields__)
    assert not hasattr(dataset, "maybe_compact")
    assert not hasattr(dataset, "maybe_optimize")

    assert dataset.compact(min_files=2) == 4
    assert dataset.refresh().data_files().num_rows < 4


def test_compaction_aliases_settle_under_the_physical_root(dataset: IcebergDataset) -> None:
    for index in range(3):
        dataset.append_arrow(
            quotes(2, f"v{index}"), branch="master", commit_row_size=1_000_000, merge_by=False
        )
    assert dataset.compact(branch="master") > 0
    assert dataset.compact(branch="root") == 0
    assert dataset.compaction_marks()
    assert all(key.startswith("main/") for key in dataset.compaction_marks())


# -- maintenance that settles -----------------------------------------------


def test_compaction_stops_when_there_is_nothing_left_to_gain(dataset: IcebergDataset) -> None:
    """A part that legitimately needs several files must not be rewritten forever."""
    dataset.table_properties = {"write.target-file-size-bytes": str(16 * 1024)}
    for index in range(6):
        dataset.append_arrow(quotes(4, f"venue{index}"), commit_row_size=1_000_000, merge_by=False)
    assert dataset.compact(min_files=2) > 0
    files = dataset.refresh().data_files().num_rows
    assert dataset.compact(min_files=2) == 0, "the second pass has nothing to do"
    assert dataset.compaction_plan(min_files=2) == []
    assert dataset.refresh().data_files().num_rows == files, "and it did not grow the table"


def test_new_data_makes_a_compacted_partition_worth_planning_again(
    dataset: IcebergDataset,
) -> None:
    for _ in range(3):
        dataset.append_arrow(quotes(2), commit_row_size=1_000_000, merge_by=False)
    dataset.compact(min_files=2)
    assert dataset.compaction_plan(min_files=2) == []
    for _ in range(2):
        dataset.append_arrow(quotes(2, "XETR"), commit_row_size=1_000_000, merge_by=False)
    assert dataset.compaction_plan(min_files=2) != []


def test_the_compacted_parts_are_marked(dataset: IcebergDataset) -> None:
    """In a table property, which expiry cannot delete -- `optimize` expires."""
    for _ in range(2):
        dataset.append_arrow(quotes(2), commit_row_size=1_000_000, merge_by=False)
    dataset.compact(min_files=2)
    marks = dataset.compaction_marks()
    assert marks, "how a compacted part is recognised later"
    dataset.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0))
    assert dataset.refresh().compaction_marks() == marks, "and a sweep does not lose it"


# -- the file bounds a keyed write prunes by --------------------------------


def _covers(expression: object, table: pyarrow.Table, field: object) -> bool:
    """Whether every row of `table` satisfies an Iceberg expression, through Arrow.

    The bounds a keyed write plans by are a superset or they are a bug: a row
    they do not admit is a stored row the write never finds, so the chunk's
    copy lands beside it.
    """
    from pyiceberg.expressions.visitors import bind, rewrite_not
    from pyiceberg.io.pyarrow import expression_to_pyarrow

    bound = bind(iceberg_schema(field), rewrite_not(expression), case_sensitive=True)
    return table.filter(expression_to_pyarrow(bound)).num_rows == table.num_rows


@scalar
class Tick:
    """A row under a wide integer key."""

    at: Annotated[int, primary_key()]
    """A clustered timestamp."""

    payload: str
    """Payload."""


@pytest.mark.parametrize(
    "values",
    [
        list(range(300)) + list(range(10**7, 10**7 + 300)),
        list(range(600)),
        [(index * 7919) % 600_000 for index in range(600)],
        [5] * 300 + [9] * 300,
        [1_700_000_000_000_000_000 + i for i in range(300)],
    ],
)
def test_the_key_bounds_cover_every_value_the_chunk_carries(values: list[int]) -> None:
    """One range per key column, between its least and greatest value."""
    chunk = pyarrow.Table.from_pydict(
        {"at": values, "payload": ["x"] * len(values)},
        schema=Tick.into_field().into_arrow_schema(),
    )
    bounds = _key_bounds(chunk, ["at"])
    assert _covers(bounds, chunk, Tick.into_field()), "bounds that miss a key duplicate it"
    assert str(bounds) == str(_key_bounds(chunk.sort_by("at"), ["at"])), "whatever the order"


def test_the_key_bounds_span_every_key_column(dataset: IcebergDataset) -> None:
    from pyiceberg.expressions import And

    chunk = quotes(3)
    bounds = _key_bounds(chunk, ["symbol", "day"])
    assert isinstance(bounds, And)
    assert "symbol" in str(bounds.left) and "day" in str(bounds.right)
    assert _covers(bounds, chunk, Quote.into_field())


def test_a_column_no_bound_can_be_spelled_for_contributes_no_term() -> None:
    """A nanosecond instant is finer than an Iceberg literal, and a boolean
    has no range worth naming; each widens the bounds rather than narrowing
    them wrongly, and a chunk of only such columns plans everything."""
    from rekep.iceberg.dataset import _always_true

    nanos = pyarrow.chunked_array(
        [pyarrow.array([1_700_000_000_000_000_000 + i for i in range(6)], pyarrow.int64())]
    ).cast(pyarrow.timestamp("ns"))
    chunk = pyarrow.Table.from_arrays(
        [nanos, pyarrow.array([True, False] * 3), pyarrow.array(list(range(6)))],
        names=["at", "flag", "seq"],
    )
    assert _key_bounds(chunk, ["at"]) == _always_true(), "no term at all, not a wrong one"
    assert _key_bounds(chunk, ["flag"]) == _always_true()
    assert "seq" in str(_key_bounds(chunk, ["at", "flag", "seq"])), "and the one it can, it does"


def test_a_merge_of_many_keys_finds_every_stored_row(dataset: IcebergDataset) -> None:
    """The bounds are a superset, so what they plan must still hold every match."""
    count = 201
    rows = quotes(count)
    dataset.append_arrow(rows, commit_row_size=1_000_000)
    updated = rows.set_column(
        rows.schema.get_field_index("venue"),
        rows.schema.field("venue"),
        pyarrow.array(["XETR"] * rows.num_rows),
    )
    assert dataset.merge_arrow_table(updated) == count
    assert dataset.read_arrow_table().num_rows == count
    assert set(dataset.read_arrow_table().column("venue").to_pylist()) == {"XETR"}


# -- sweeping ---------------------------------------------------------------


def test_cleanup_sweeps_metadata_as_well_as_data(dataset: IcebergDataset) -> None:
    """A stream fills the metadata directory faster than the data one."""
    for index in range(3):
        dataset.append_arrow(quotes(2, f"venue{index}"), commit_row_size=1_000_000, merge_by=False)
    dataset.compact(min_files=2)
    location = local(dataset.iceberg_table.location())
    before = len(list((location / "metadata").rglob("*")))
    stored = dataset.read_arrow_table().num_rows
    report = dataset.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0))
    after = len(list((location / "metadata").rglob("*")))
    assert report["deleted"] > 0
    assert after < before, "the metadata directory shrank"
    assert dataset.refresh().read_arrow_table().num_rows == stored, "and the table still reads"


def test_cleanup_deletes_expired_snapshot_manifest_lists(dataset: IcebergDataset) -> None:
    for index in range(4):
        dataset.append_arrow(quotes(2, f"venue{index}"), commit_row_size=1_000_000, merge_by=False)
    expired = dataset.iceberg_table.snapshots()[:-1]
    manifest_lists = [local(snapshot.manifest_list) for snapshot in expired]
    assert manifest_lists and all(path.exists() for path in manifest_lists)

    dataset.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0))

    assert all(not path.exists() for path in manifest_lists)
    assert dataset.refresh().read_arrow_table().num_rows == 8


def test_every_retained_snapshot_still_reads_after_a_sweep(dataset: IcebergDataset) -> None:
    """The one thing a metadata sweep may never break."""
    for index in range(4):
        dataset.append_arrow(quotes(2, f"venue{index}"), commit_row_size=1_000_000, merge_by=False)
    dataset.cleanup(retain=3, orphan_age=datetime.timedelta(seconds=0))
    dataset.refresh()
    for snapshot in dataset.iceberg_table.snapshots():
        assert dataset.read_arrow_table(snapshot_id=snapshot.snapshot_id).num_rows > 0


def test_a_sweep_can_leave_metadata_alone(dataset: IcebergDataset) -> None:
    for index in range(4):
        dataset.append_arrow(quotes(2, f"venue{index}"), commit_row_size=1_000_000, merge_by=False)
    location = local(dataset.iceberg_table.location())
    before = {path for path in (location / "metadata").rglob("*")}
    dataset.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0), metadata=False)
    # Expiry writes a metadata version of its own, so the directory may grow --
    # what must not happen is a file disappearing from it.
    assert before <= {path for path in (location / "metadata").rglob("*")}


def test_a_sweep_finds_the_files_however_the_warehouse_is_spelled(tmp_path: Path) -> None:
    """`file:/x` is a valid URI that `pyarrow.fs` resolves to `/x`.

    Stripping the scheme by hand leaves `file:/x`, which matches nothing the
    listing returns -- so every live file looked orphaned and the sweep deleted
    the table. The same shape as `abfss://container@account.../x`, which cannot
    be exercised here. On Windows the odd spelling is a different one: `file:`
    plus a drive letter is a URI nothing resolves, and the trap is the bare
    `C:/x` path itself, which is not a URI at all. Every one of them reaches a
    table as the one absolute `file:///x` a reader resolves.
    """
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    posix = os.name != "nt"
    catalog = IcebergCatalog(
        name="single",
        properties={
            "type": "sql",
            "uri": f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
            # One slash, not three -- or on Windows, no scheme at all.
            "warehouse": f"file:{warehouse.as_posix()}" if posix else warehouse.as_posix(),
        },
    )
    quotes_ = catalog.dataset("trading.quotes", field=Quote.into_field())
    for _ in range(3):
        quotes_.append_arrow(quotes(2), commit_row_size=1_000_000, merge_by=False)
    stored = quotes_.read_arrow_table().num_rows
    location = quotes_.get_or_create_table().location()
    assert location == f"{warehouse.as_uri()}/trading/quotes", "one spelling reaches the table"
    quotes_.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0))
    assert quotes_.refresh().read_arrow_table().num_rows == stored, "the table still reads"


def test_a_sweep_follows_a_relocated_data_path(tmp_path: Path) -> None:
    """`write.data.path` moves the data; assuming `<location>/data` swept nothing."""
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    catalog = IcebergCatalog(name="relocated", properties=catalog_properties(tmp_path))
    quotes_ = catalog.dataset(
        "trading.quotes",
        field=Quote.into_field(),
        table_properties={"write.data.path": elsewhere.as_uri()},
    )
    for index in range(4):
        quotes_.append_arrow(quotes(2, f"v{index}"), commit_row_size=1_000_000, merge_by=False)
    quotes_.compact(min_files=2)
    stored = quotes_.refresh().read_arrow_table().num_rows
    written = len(list(elsewhere.rglob("*.parquet")))
    assert written > 0, "the data really did go elsewhere"

    quotes_.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0))
    live = {
        Path(path).name
        for path in quotes_.refresh()
        .iceberg_table.inspect.all_files()
        .column("file_path")
        .to_pylist()
    }
    remaining = {path.name for path in elsewhere.rglob("*.parquet")}
    assert remaining == live, "what the sweep left is exactly what is still referenced"
    assert quotes_.read_arrow_table().num_rows == stored, "and the table still reads"


def test_a_sweep_survives_a_data_path_that_contains_the_metadata(tmp_path: Path) -> None:
    """`write.data.path` is an arbitrary location, so the two directories the
    sweep walks need not be disjoint. Point it at the table root -- which is
    what a table written flat does -- and the data listing walks `metadata/`
    too. Guarding each directory with only its own half of the live set
    reported ten orphans, all ten of them the current pointer, the manifest
    lists and the manifests: `cleanup` deleted the table.
    """
    catalog = IcebergCatalog(name="flatpath", properties=catalog_properties(tmp_path))
    location = (tmp_path / "warehouse" / "trading" / "quotes").as_uri()
    quotes_ = catalog.dataset(
        "trading.quotes",
        field=Quote.into_field(),
        location=location,
        table_properties={"write.data.path": location},
    )
    for index in range(3):
        quotes_.append_arrow(quotes(2, f"v{index}"), commit_row_size=1_000_000, merge_by=False)
    table = quotes_.refresh().iceberg_table
    assert quotes_._metadata_path(table).startswith(quotes_._data_path(table)), (
        "the point of the fixture: one directory inside the other"
    )
    stored = quotes_.read_arrow_table().num_rows

    _, live = quotes_._live(table)
    doomed = {
        Path(path).name for path, _ in quotes_.orphan_files(datetime.timedelta(seconds=0))
    } & {Path(path).name for path in live}
    assert doomed == set(), "no live metadata file may be reported as an orphan"

    quotes_.cleanup(retain=10, orphan_age=datetime.timedelta(seconds=0))
    assert quotes_.refresh().read_arrow_table().num_rows == stored, "and the table still reads"


def test_a_sweep_finishes_when_an_orphan_is_already_gone(dataset: IcebergDataset) -> None:
    """Another sweeper between the listing and the delete. Raising there would
    abandon every orphan after it and throw away the report of the ones before."""
    for index in range(4):
        dataset.append_arrow(quotes(2, f"v{index}"), commit_row_size=1_000_000, merge_by=False)
    dataset.compact(min_files=2)
    dataset.cleanup(retain=1, remove_orphans=False)
    orphans = dataset._orphans(datetime.timedelta(seconds=0), metadata=True)
    assert len(orphans) > 1, "there has to be one after the vanished one"
    orphans[0][0].delete_file(orphans[0][1])  # the other sweeper got there first

    dataset._sweep(orphans)
    remaining = [path for _, path, _, _ in orphans if Path(path).exists()]
    assert remaining == [], "every one of them went, the vanished one included"
    assert dataset.refresh().read_arrow_table().num_rows == 8


def test_a_sweep_keeps_a_live_file_spelled_against_another_base(dataset: IcebergDataset) -> None:
    """`add_files` records the location it is handed, and `file:` with one
    slash is a location `file:///w/t/data` does not reduce. Answering the
    unreduced path made it match no listing, so the file looked orphaned:
    measured, `cleanup` deleted a data file the *current* snapshot referenced
    and the table stopped reading.
    """
    import pyarrow.parquet

    dataset.append_arrow_table(quotes(3))
    table = dataset.iceberg_table
    root = local(table.location())
    extra = root / "data" / f"day={datetime.date(2026, 8, 14)}" / "added-0000.parquet"
    pyarrow.parquet.write_table(quotes(4, "XETR"), extra)
    added = f"file:{extra.as_posix()}"  # one slash, not three
    table.add_files([added])
    dataset.refresh()
    assert dataset.read_arrow_table().num_rows == 7

    # Derived from what was added rather than from a `file:/` prefix: Windows
    # spells this one `file:C:/...`, which starts with neither, so the shape
    # test passed the fixture on one host and failed it on the other while the
    # sweep it guards behaved identically on both.
    assert added in dataset._live(dataset.iceberg_table)[0], (
        "the fixture only means something if the odd spelling really is recorded"
    )
    assert dataset.orphan_files(datetime.timedelta(seconds=0)) == []
    report = dataset.cleanup(retain=10, orphan_age=datetime.timedelta(seconds=0))
    assert report["deleted"] == 0
    assert extra.exists(), "the file a live snapshot references is still there"
    assert dataset.refresh().read_arrow_table().num_rows == 7, "and the table still reads"


def test_a_sweep_still_finds_an_orphan_beside_an_unreducible_live_file(
    dataset: IcebergDataset,
) -> None:
    """Holding those by base name is weaker than by path, and the weakness has
    to stop at names Iceberg minted -- a real orphan must still go."""
    import pyarrow.parquet

    dataset.append_arrow_table(quotes(3))
    table = dataset.iceberg_table
    root = local(table.location())
    partition = root / "data" / f"day={datetime.date(2026, 8, 14)}"
    live = partition / "added-0000.parquet"
    pyarrow.parquet.write_table(quotes(4, "XETR"), live)
    table.add_files([f"file:{live.as_posix()}"])
    dataset.refresh()
    junk = partition / "left-behind-0000.parquet"
    pyarrow.parquet.write_table(quotes(1), junk)

    swept = {Path(path).name for path, _ in dataset.orphan_files(datetime.timedelta(seconds=0))}
    assert swept == {"left-behind-0000.parquet"}
    assert dataset.cleanup(retain=10, orphan_age=datetime.timedelta(seconds=0))["deleted"] == 1
    assert not junk.exists() and live.exists()
    assert dataset.refresh().read_arrow_table().num_rows == 7


def test_a_sweep_asked_for_no_grace_period_takes_a_file_written_now(
    dataset: IcebergDataset,
) -> None:
    """`orphan_age=0` means what it says, whichever clock stamped the file.

    A file written a moment ago can carry an mtime a moment in the *future*:
    the filesystem stamps from its own clock and this process reads its own,
    and on a Windows runner the two disagreed often enough to spare a file the
    caller had just asked to have taken. The grace period is for a writer with
    uncommitted files on disk, and zero says there is not one.
    """
    import os

    import pyarrow.parquet

    dataset.append_arrow_table(quotes(3))
    root = local(dataset.iceberg_table.location())
    junk = root / "data" / f"day={datetime.date(2026, 8, 14)}" / "written-just-now.parquet"
    pyarrow.parquet.write_table(quotes(1), junk)
    # Stamped ahead on purpose, because that is the disagreement itself and
    # waiting for two real clocks to drift is a test that fails on one host in
    # ten. A minute is longer than any skew and shorter than the grace period
    # the first assertion asks for.
    ahead = junk.stat().st_mtime + 60
    os.utime(junk, (ahead, ahead))

    spared = dataset.orphan_files(datetime.timedelta(minutes=30))
    assert junk.name not in {Path(path).name for path, _ in spared}, "a grace period spares it"
    swept = {Path(path).name for path, _ in dataset.orphan_files(datetime.timedelta(0))}
    assert junk.name in swept, "and no grace period does not"


def test_a_sweep_does_not_delete_another_writers_files(tmp_path: Path) -> None:
    """A dataset that has been open a while has not seen the other writers.

    The live set is what decides a deletion, so building it from a stale table
    deletes whatever landed since -- measured before the fix: twelve files
    gone and the table unreadable.
    """
    properties = catalog_properties(tmp_path)
    catalog = IcebergCatalog(name="shared", properties=properties)
    catalog.dataset("trading.quotes", field=Quote.into_field()).append_arrow(
        quotes(2), commit_row_size=1_000_000
    )
    sweeper = IcebergCatalog(name="shared", properties=properties).dataset(
        "trading.quotes", field=Quote.into_field()
    )
    sweeper.get_or_create_table()  # loads the table, and caches it

    other = IcebergCatalog(name="shared", properties=properties).dataset(
        "trading.quotes", field=Quote.into_field()
    )
    for index in range(3):
        other.append_arrow(quotes(2, f"v{index}"), commit_row_size=1_000_000, merge_by=False)
    stored = other.read_arrow_table().num_rows

    report = sweeper.cleanup(retain=10, orphan_age=datetime.timedelta(seconds=0))
    assert report["deleted"] == 0, "nothing the other writer left is an orphan"
    assert other.refresh().read_arrow_table().num_rows == stored, "and the table still reads"


def test_a_sweep_keeps_the_files_only_the_metadata_names(dataset: IcebergDataset) -> None:
    """A Puffin statistics file and a Hadoop pointer are reachable no other way.

    Neither is named by a snapshot, a manifest or the metadata log, and a
    statistics file is exactly as old as the snapshot it describes -- so an age
    rule does not save it either. Sweeping one loses the table's statistics;
    sweeping the other loses the table.
    """
    from pyiceberg.table.statistics import StatisticsFile

    dataset.append_arrow(quotes(2), commit_row_size=1_000_000)
    table = dataset.get_or_create_table()
    metadata = local(table.location()) / "metadata"
    puffin = metadata / "stats.puffin"
    puffin.write_bytes(b"PFA1" + bytes(64))
    pointer = metadata / "version-hint.text"
    pointer.write_text("1")
    stray = metadata / "left-behind.avro"
    stray.write_bytes(b"nothing references this")
    with table.update_statistics() as update:
        update.set_statistics(
            StatisticsFile(
                snapshot_id=table.current_snapshot().snapshot_id,
                statistics_path=puffin.as_uri(),
                file_size_in_bytes=puffin.stat().st_size,
                file_footer_size_in_bytes=8,
                blob_metadata=[],
            )
        )
    dataset.refresh()
    swept = {Path(path).name for path, _ in dataset.orphan_files(datetime.timedelta(seconds=0))}
    assert "stats.puffin" not in swept, "the statistics the metadata registers"
    assert "version-hint.text" not in swept, "the pointer a Hadoop catalog reads"
    assert "left-behind.avro" in swept, "and a real orphan is still found"


def test_optimize_does_not_rewrite_properties_it_already_set(dataset: IcebergDataset) -> None:
    dataset.append_arrow(quotes(2), commit_row_size=1_000_000)
    dataset.optimize()
    versions = len(dataset.refresh().iceberg_table.metadata.metadata_log)
    dataset.optimize()
    assert len(dataset.refresh().iceberg_table.metadata.metadata_log) <= versions + 1


# -- sorting a commit -------------------------------------------------------


def test_sorting_a_commit_changes_the_order_not_the_rows(dataset: IcebergDataset) -> None:
    dataset.sort_by = ["size"]
    rows = quotes(6).sort_by([("size", "descending")])
    dataset.append_arrow(rows, commit_row_size=1_000_000)
    stored = dataset.read_arrow_table()
    assert stored.num_rows == 6
    assert sorted(stored.column("size").to_pylist()) == sorted(rows.column("size").to_pylist())


def test_sorting_is_off_unless_asked(dataset: IcebergDataset) -> None:
    rows = quotes(4)
    assert dataset.sorted(rows) is rows


def test_a_filtered_read_is_the_same_either_way(tmp_path: Path) -> None:
    """Sorting changes what a scan has to decode, never what it returns."""
    answers = []
    for index, sort_by in enumerate((None, ["size"])):
        target = IcebergDataset(
            name=f"sorted{index}",
            namespace="trading",
            field=Quote.into_field(),
            catalog_name="test",
            catalog_properties=catalog_properties(tmp_path),
            sort_by=sort_by,
        )
        target.append_arrow(quotes(50).sort_by([("size", "descending")]), commit_row_size=1_000_000)
        found = target.read_arrow_table(row_filter="size >= 40").to_pylist()
        answers.append(sorted(found, key=str))
    assert answers[0] == answers[1]


# -- a column derived from the keys -----------------------------------------


def test_a_digest_projection_keeps_its_native_input_dependencies() -> None:
    @scalar
    class Digested:
        venue: str
        payload: str
        digest: Annotated[
            int | None,
            field_options(metadata={"DIGEST:role": "holder", "DIGEST:sources": '["venue"]'}),
        ] = None

    assert _applied_projection(Digested.into_field(), ["digest"]) == Digested.into_field()
    projected = _applied_projection(Digested.into_field(), ["payload"])
    assert [member.name for member in projected] == ["payload"]


def test_a_partition_derived_from_a_digest_keeps_transitive_read_dependencies(
    tmp_path: Path,
) -> None:
    @scalar
    class PartitionedDigest:
        venue: str
        digest: Annotated[
            int | None,
            field_options(
                metadata={"DIGEST:role": "holder", "DIGEST:sources": '["venue"]'},
            ),
        ] = None
        part: Annotated[
            int | None,
            partition_key(),
            derived_from("digest"),
        ] = None

    field = PartitionedDigest.into_field()
    source = pyarrow.table({"venue": ["XPAR"]})
    complete = field.apply_arrow_reader(source.to_reader(), safe=False).read_all()
    complete = complete.set_column(
        complete.schema.get_field_index("part"),
        complete.schema.field("part"),
        complete.column("digest"),
    )
    catalog = IcebergCatalog(name="transitive", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("trading.transitive", field=field)
    dataset.append_arrow_table(complete)

    projected = dataset.read_arrow_reader(field, columns=["part"])
    try:
        assert projected.schema.names == ["part"]
        assert (
            projected.read_all().column("part").to_pylist() == complete.column("part").to_pylist()
        )
    finally:
        projected.close()
        dataset.close()
        catalog.close()


# -- the sort order the shape declares ---------------------------------------


@scalar
class Ticked:
    """A row the shape says is laid out in time order."""

    at: Annotated[int, primary_key(), sort_key()]
    """When."""

    seq: Annotated[int, sort_key()] = 0
    """Second key, so the order is lexicographic and not one column."""

    payload: str = "x"
    """Payload."""


@scalar
class DescendingTick:
    """A nullable clock physically ordered newest first."""

    day: Annotated[datetime.date, partition_key()]
    at: Annotated[int | None, sort_key("desc")] = None
    seq: Annotated[int, primary_key(), sort_key()] = 0


@scalar
class FloatingTick:
    """A nullable floating-point sort key."""

    value: Annotated[float | None, sort_key()]
    seq: int


def ticked(pairs: Sequence[tuple[int, int]]) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {
            "at": [at for at, _ in pairs],
            "seq": [seq for _, seq in pairs],
            "payload": ["x"] * len(pairs),
        },
        schema=Ticked.into_field().into_arrow_schema(),
    )


def descending_ticked(values: Sequence[int | None]) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {
            "day": [datetime.date(2026, 8, 14)] * len(values),
            "at": values,
            "seq": list(range(len(values))),
        },
        schema=DescendingTick.into_field().into_arrow_schema(),
    )


def floating_ticked(values: Sequence[float | None], start: int) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {"value": values, "seq": range(start, start + len(values))},
        schema=FloatingTick.into_field().into_arrow_schema(),
    )


def test_the_columns_sorted_by_are_the_ones_declared(tmp_path: Path) -> None:
    catalog = IcebergCatalog(name="sorted", properties=catalog_properties(tmp_path))
    assert catalog.dataset("t.a", field=Ticked.into_field()).sort_columns() == ["at", "seq"]
    assert catalog.dataset("t.b", field=Ticked.into_field(), sort_by=["seq"]).sort_columns() == [
        "seq"
    ]
    assert catalog.dataset("t.c", field=Ticked.into_field(), sort_by=[]).sort_columns() == []
    empty = field_of(pyarrow.schema([]), "t.d")
    assert catalog.dataset("t.d", field=empty).sort_columns() == []

    explicit = catalog.dataset("t.explicit", field=Ticked.into_field(), sort_by=["seq"])
    table = explicit.get_or_create_table()
    (sorting,) = table.sort_order().fields
    assert table.schema().find_column_name(sorting.source_id) == "seq"


def test_a_reopened_table_keeps_exact_sort_priority_and_null_policy(tmp_path: Path) -> None:
    from pyiceberg.table.sorting import NullOrder, SortDirection

    catalog = IcebergCatalog(name="sort-roundtrip", properties=catalog_properties(tmp_path))
    declared = catalog.dataset(
        "t.reordered",
        field=Ticked.into_field(),
        sort_by=["seq", "at"],
    )
    table = declared.get_or_create_table()

    assert [sorting.null_order for sorting in table.sort_order().fields] == [
        NullOrder.NULLS_LAST,
        NullOrder.NULLS_LAST,
    ]
    assert [sorting.direction for sorting in table.sort_order().fields] == [
        SortDirection.ASC,
        SortDirection.ASC,
    ]
    reopened = catalog.dataset("t.reordered")
    assert sort_keys(reopened.into_struct_field()) == {"seq": "asc", "at": "asc"}
    assert reopened.sort_fields() == [("seq", "ascending"), ("at", "ascending")]


def test_partition_staging_honours_descending_sort_and_nulls_last(tmp_path: Path) -> None:
    from pyiceberg.table.sorting import NullOrder, SortDirection

    catalog = IcebergCatalog(name="descending", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset(
        "t.descending",
        field=DescendingTick.into_field(),
    )
    dataset.append_arrow_table(descending_ticked([1, None, 3, 2]), commit_row_size=1_000_000)

    table = dataset.iceberg_table
    assert [(field.direction, field.null_order) for field in table.sort_order().fields] == [
        (SortDirection.DESC, NullOrder.NULLS_LAST),
        (SortDirection.ASC, NullOrder.NULLS_LAST),
    ]
    (path,) = dataset.data_files().column("file_path").to_pylist()
    physical = pyarrow.parquet.read_table(local(path))
    assert physical.column("at").to_pylist() == [3, 2, 1, None]


def test_a_descending_ordered_read_merges_commits_and_applies_its_limit(tmp_path: Path) -> None:
    catalog = IcebergCatalog(name="descending-read", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset(
        "t.descending_read",
        field=DescendingTick.into_field(),
    )
    dataset.append_arrow_table(descending_ticked([5, None, 1]), commit_row_size=1_000_000)
    dataset.append_arrow_table(
        descending_ticked([4, None, 2]), commit_row_size=1_000_000, merge_by=False
    )

    found = dataset.read_arrow_reader(order_by=("at", "descending"), limit=5).read_all()

    assert found.column("at").to_pylist() == [5, 4, 2, 1, None]


@pytest.mark.parametrize("special", [None, float("nan")])
def test_ordered_read_does_not_concatenate_file_local_special_tails(
    tmp_path: Path, special: float | None
) -> None:
    catalog = IcebergCatalog(name="special-read", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.special_read", field=FloatingTick.into_field())
    dataset.append_arrow_table(floating_ticked([1.0, special], 0), commit_row_size=1_000_000)
    dataset.append_arrow_table(floating_ticked([2.0, special], 2), commit_row_size=1_000_000)

    found = dataset.read_arrow_reader(order_by="value").read_all().column("value").to_pylist()

    assert found[:2] == [1.0, 2.0]
    if special is None:
        assert found[2:] == [None, None]
    else:
        assert all(math.isnan(value) for value in found[2:])


def test_a_snapshot_order_does_not_inherit_a_newer_table_direction(tmp_path: Path) -> None:
    catalog = IcebergCatalog(name="sort-evolution", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.sort_evolution", field=Ticked.into_field())
    dataset.append_arrow_table(ticked([(1, 0), (2, 0), (3, 0)]), commit_row_size=1_000_000)
    snapshot_id = dataset.iceberg_table.current_snapshot().snapshot_id
    with dataset.iceberg_table.update_sort_order() as update:
        update.desc("at", IdentityTransform())

    assert dataset.sort_fields() == [("at", "descending")]
    assert dataset.sorted(ticked([(1, 0), (3, 0), (2, 0)])).column("at").to_pylist() == [
        3,
        2,
        1,
    ]

    found = dataset.read_arrow_reader(snapshot_id=snapshot_id, order_by="at").read_all()

    assert found.column("at").to_pylist() == [1, 2, 3]


def test_an_interrupt_after_commit_keeps_unpartitioned_files_live(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    catalog = IcebergCatalog(name="flat-interrupt", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.flat_interrupt", field=Ticked.into_field())
    original = Transaction.commit_transaction

    def committed_then_interrupted(transaction: Transaction) -> None:
        original(transaction)
        raise KeyboardInterrupt

    monkeypatch.setattr(Transaction, "commit_transaction", committed_then_interrupted)
    with pytest.raises(KeyboardInterrupt):
        dataset.append_arrow_table(ticked([(1, 0), (2, 0)]), commit_row_size=1_000_000)

    assert dataset.refresh().read_arrow_table().column("at").to_pylist() == [1, 2]


def test_a_chunk_already_in_order_is_not_sorted_again(tmp_path: Path) -> None:
    """The common case on a capture, and the question is 20x cheaper than the
    answer -- so it is asked."""
    catalog = IcebergCatalog(name="sorted", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.a", field=Ticked.into_field())
    tidy = ticked([(1, 0), (1, 1), (2, 0), (3, 0)])
    assert dataset.sorted(tidy) is tidy, "handed straight back, not copied"
    assert dataset.sorted(ticked([(2, 0), (1, 0)])) is not tidy


def test_a_shuffled_write_lands_in_the_declared_order(tmp_path: Path) -> None:
    """The whole point: a sort order Iceberg records and the writer ignores is
    a wish. A filter can only skip a *row group*, so this is what it buys."""
    catalog = IcebergCatalog(name="layout", properties=catalog_properties(tmp_path))
    rows = 20_000
    shuffled = pyarrow.Table.from_pydict(
        {
            "at": [(index * 7919) % rows for index in range(rows)],
            "seq": [0] * rows,
            "payload": ["x"] * rows,
        },
        schema=Ticked.into_field().into_arrow_schema(),
    )

    def decoded(sort_by: Sequence[str] | None, name: str) -> tuple[int, int]:
        dataset = catalog.dataset(name, field=Ticked.into_field(), sort_by=sort_by)
        dataset.get_or_create_table().transaction().set_properties(
            **{"write.parquet.row-group-limit": "8192"}
        ).commit_transaction()
        dataset.refresh().append_arrow(shuffled, commit_row_size=1_000_000)
        groups = touched = 0
        floor = rows - rows // 10
        for task in dataset.refresh().iceberg_table.scan().plan_files():
            meta = pyarrow.parquet.ParquetFile(local(task.file.file_path)).metadata
            for index in range(meta.num_row_groups):
                groups += 1
                touched += meta.row_group(index).column(0).statistics.max >= floor
        return touched, groups

    declared, whole = decoded(None, "t.declared"), decoded([], "t.opted_out")
    assert whole[0] == whole[1], "unsorted, every row group holds the whole range"
    assert declared[0] < declared[1], "sorted, a filter skips most of them"
    assert declared[0] <= 2


@scalar
class Bounded:
    """A wide row under a clock key, spread over transformed partitions."""

    unix: Annotated[int, primary_key(), sort_key()]
    """Unique clock."""

    hour: Annotated[int, partition_key()]
    """Whole epoch hour used for identity partitioning."""

    body: bytes
    """Payload wide enough that the rows are what the memory is."""


def bounded_batch(index: int, rows: int, partitions: int) -> pyarrow.RecordBatch:
    unix = list(range(index * rows, (index + 1) * rows))
    return pyarrow.RecordBatch.from_pydict(
        {
            "unix": unix,
            # Interleaved on purpose: a chunk whose partitions arrive mixed is
            # the one a writer is tempted to sort or group as a whole.
            "hour": [(value * 7919) % partitions for value in unix],
            "body": [bytes(200)] * rows,
        },
        schema=Bounded.into_field().into_arrow_schema(),
    )


#: Proxy pools this measurement installed, kept for the run. A buffer
#: remembers the pool it came from and frees through it, so a proxy collected
#: while rows written under it are still alive takes the interpreter with it.
_MEASURED_POOLS: list[Any] = []


@contextmanager
def peak_arrow_memory() -> Iterator[Callable[[], int]]:
    """Bytes Arrow held at its highest inside the block, and only inside it.

    `max_memory` is a high-water mark a pool never lowers, so asking the
    default pool inside a suite that has already allocated answers about the
    suite. A proxy installed for the block starts at zero and counts what the
    block allocates, whoever allocates it.
    """
    parent = pyarrow.default_memory_pool()
    proxy = pyarrow.proxy_memory_pool(parent)
    _MEASURED_POOLS.append(proxy)
    pyarrow.set_memory_pool(proxy)
    try:
        yield lambda: int(proxy.max_memory())
    finally:
        pyarrow.set_memory_pool(parent)


@pytest.mark.parametrize("verb", ["append", "merge"])
@pytest.mark.parametrize("partitions", [1, 2, 4, 16])
def test_a_bounded_write_holds_a_bounded_multiple_of_its_chunk(
    verb: str, partitions: int, tmp_path: Path
) -> None:
    """The bound the streaming verbs exist for: a commit holds its chunk and
    a working copy, not a multiple of the chunk.

    Over these partition counts the staged writer measures 1.05 to 2.01
    chunks, worst where a chunk splits into two interleaved halves and each
    has to be gathered out of it; the writer that collected them instead --
    handing PyIceberg the whole chunk to split, and holding every part's rows
    until the commit -- measured 1.31 to 5.03 on these very chunks. A quarter
    of a chunk of headroom over the worst case leaves nothing in between that
    anyone wrote on purpose.
    """
    rows, batch_count = 20_000, 8
    catalog = IcebergCatalog(name="bounded", properties=catalog_properties(tmp_path))
    target = catalog.dataset("t.bounded", field=Bounded.into_field())
    # Something stored for a keyed write to plan against, with keys the
    # stream never brings, so every streamed row is genuinely new.
    target.append_arrow_table(
        pyarrow.Table.from_batches([bounded_batch(batch_count, rows, partitions).slice(0, 8)]),
    )
    chunk = bounded_batch(0, rows, partitions).nbytes * batch_count
    reader = pyarrow.RecordBatchReader.from_batches(
        Bounded.into_field().into_arrow_schema(),
        (bounded_batch(index, rows, partitions) for index in range(batch_count)),
    )

    with peak_arrow_memory() as peak:
        if verb == "merge":
            target.merge_arrow_reader(reader, commit_batch_num=batch_count)
        else:
            target.append_arrow_reader(reader, commit_batch_num=batch_count)
        held = peak()

    assert target.read_arrow_table().num_rows == rows * batch_count + 8
    assert held < 2.25 * chunk, f"{held / chunk:.2f} chunks held for one {verb}"


def test_a_merge_over_a_partly_stored_chunk_holds_a_bounded_multiple(
    tmp_path: Path,
) -> None:
    """A merge that changes part of a stored file: the file is streamed
    through the stager without the keys the chunk replaces, so what the write
    holds past the chunk is one batch of that file and the file's own
    replacement."""
    rows, batch_count, partitions = 20_000, 8, 1
    catalog = IcebergCatalog(name="overlap", properties=catalog_properties(tmp_path))
    target = catalog.dataset("t.overlap", field=Bounded.into_field())
    stale = bounded_batch(0, rows, partitions).slice(0, rows // 2)
    stale = stale.set_column(
        2, stale.schema.field("body"), pyarrow.array([bytes(100)] * stale.num_rows)
    )
    # Keys past every streamed one, which the stored file keeps.
    kept = bounded_batch(2 * batch_count, rows // 2, partitions)
    target.append_arrow_table(pyarrow.Table.from_batches([stale, kept]))
    stored = _data_paths(target)
    chunk = bounded_batch(0, rows, partitions).nbytes * batch_count
    reader = pyarrow.RecordBatchReader.from_batches(
        Bounded.into_field().into_arrow_schema(),
        (bounded_batch(index, rows, partitions) for index in range(batch_count)),
    )

    with peak_arrow_memory() as peak:
        written = target.merge_arrow_reader(reader, commit_batch_num=batch_count)
        held = peak()

    assert written == rows * batch_count, "every row the stream carried is changed or new"
    assert target.read_arrow_table().num_rows == rows * batch_count + rows // 2, (
        "the stale half went and the rest of its file stayed"
    )
    assert not _data_paths(target) & stored, "which was written back without it"
    assert held < 2.5 * chunk, f"{held / chunk:.2f} chunks held for a partial merge"


@pytest.mark.parametrize("batch_rows", [8, 700, 1000, 1500])
def test_a_staged_file_fills_its_row_groups_whatever_the_batches_are(
    batch_rows: int, tmp_path: Path
) -> None:
    """A row group per source batch is what writing each one as it arrives
    makes, and it costs both ways: too many groups when the batches are small
    -- 2,000 of 8 rows wrote 2,000 row groups and twice the bytes of the same
    rows in one -- and half-filled ones whenever the batch size is not a
    divisor of the limit. A row group is the table's declared size until the
    rows run out."""
    limit, batches = 1_000, 8
    catalog = IcebergCatalog(name="groups", properties=catalog_properties(tmp_path))
    target = catalog.dataset(
        "t.groups",
        field=Bounded.into_field(),
        table_properties={"write.parquet.row-group-limit": str(limit)},
    )
    reader = pyarrow.RecordBatchReader.from_batches(
        Bounded.into_field().into_arrow_schema(),
        (bounded_batch(index, batch_rows, 1) for index in range(batches)),
    )
    target.append_arrow_reader(reader, commit_batch_num=batches)

    files = target.data_files().to_pylist()
    assert len(files) == 1
    written = pyarrow.parquet.ParquetFile(local(files[0]["file_path"])).metadata
    rows = batch_rows * batches
    assert written.num_rows == rows
    assert written.num_row_groups == math.ceil(rows / limit)
    sizes = [written.row_group(index).num_rows for index in range(written.num_row_groups)]
    assert sizes[:-1] == [limit] * (written.num_row_groups - 1)


def test_a_file_claims_no_order_the_writer_could_not_lay_out(tmp_path: Path) -> None:
    """A reader takes a recorded sort order at its word. A shape cannot hold
    every order Iceberg can record -- a transformed sort field here -- and for
    those the writer has nothing to sort by, so its files must not claim one."""
    from pyiceberg.table.sorting import NullOrder
    from pyiceberg.transforms import IdentityTransform, TruncateTransform

    catalog = IcebergCatalog(name="claimed", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.claimed", field=Ticked.into_field())
    with dataset.get_or_create_table().update_sort_order() as update:
        update.asc("at", IdentityTransform(), NullOrder.NULLS_LAST)
        update.asc("payload", TruncateTransform(1), NullOrder.NULLS_LAST)
    dataset.refresh()

    assert dataset.iceberg_table.sort_order().order_id, "the table records an order"
    assert dataset.sort_fields() == [], "and the shape cannot hold it"

    dataset.append_arrow_table(ticked([(3, 0), (1, 0), (2, 0)]))

    assert dataset.data_files().column("sort_order_id").to_pylist() == [None]
    assert dataset.read_arrow_reader(order_by="at").read_all().column("at").to_pylist() == [1, 2, 3]


def test_no_row_is_lost_splitting_a_chunk_into_its_partitions(tmp_path: Path) -> None:
    """Every row of a chunk lands in exactly one partition, whatever order its
    partitions arrive in, including the batches that carry none and the rows
    whose partition column is null."""

    @scalar
    class Loose:
        ident: Annotated[int, primary_key()]
        part: Annotated[int | None, partition_key()] = None

    schema = Loose.into_field().into_arrow_schema()
    pattern = [0, 2, None, 1, 0, None, 2, 2, 1, 0]
    batches = [
        pyarrow.RecordBatch.from_pydict(
            {
                "ident": list(range(index * 10, index * 10 + size)),
                "part": [pattern[(index * 7 + row) % len(pattern)] for row in range(size)],
            },
            schema=schema,
        )
        for index, size in enumerate([10, 0, 7, 1, 0, 10])
    ]
    catalog = IcebergCatalog(name="split", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.split", field=Loose.into_field())
    dataset.append_arrow_reader(
        pyarrow.RecordBatchReader.from_batches(schema, iter(batches)),
        commit_batch_num=len(batches),
    )

    expected = {(row["ident"], row["part"]) for batch in batches for row in batch.to_pylist()}
    stored = dataset.read_arrow_table().to_pylist()
    assert {(row["ident"], row["part"]) for row in stored} == expected
    assert len(stored) == len(expected)
    assert all(row["record_count"] > 0 for row in dataset.data_files().to_pylist())


def test_a_partition_is_packed_into_files_by_its_own_row_width(tmp_path: Path) -> None:
    """`write.target-file-size-bytes` is a size in bytes, and rows are not all
    the same width. One bound taken from a whole chunk's average splits the
    partition of short rows into files a fraction of the target and packs the
    partition of wide rows past it."""

    @scalar
    class Wide:
        ident: Annotated[int, primary_key()]
        part: Annotated[int, partition_key()]
        body: str

    rows = 2_000
    catalog = IcebergCatalog(name="widths", properties=catalog_properties(tmp_path))
    target = catalog.dataset(
        "t.widths",
        field=Wide.into_field(),
        table_properties={"write.target-file-size-bytes": "100000"},
    )
    target.append_arrow_table(
        pyarrow.Table.from_pydict(
            {
                "ident": list(range(rows)),
                "part": [index % 2 for index in range(rows)],
                "body": ["x" * 8 if index % 2 == 0 else "y" * 1_000 for index in range(rows)],
            },
            schema=Wide.into_field().into_arrow_schema(),
        ),
    )

    files = target.data_files().to_pylist()
    narrow = [row for row in files if "part=0" in row["file_path"]]
    wide = [row for row in files if "part=1" in row["file_path"]]
    assert [row["record_count"] for row in narrow] == [rows // 2], "short rows fill one file"
    assert len(wide) > 1, "wide rows do not fit in one"
    assert all(row["file_size_in_bytes"] <= 100_000 for row in wide)


def test_every_verb_writes_a_table_partitioned_by_void(tmp_path: Path) -> None:
    """`void` is the last transform with an Arrow form, and the staged writer
    needs one for every field of the spec it is placing rows under."""
    from pyiceberg.transforms import VoidTransform

    @scalar
    class Loose:
        symbol: str | None = None
        size: int | None = None

    catalog = IcebergCatalog(name="void", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.void", field=Loose.into_field())
    with dataset.get_or_create_table().update_spec() as spec:
        spec.add_field("symbol", VoidTransform(), "symbol_void")
    dataset.refresh()
    schema = Loose.into_field().into_arrow_schema()

    def rows(symbols: Sequence[str], sizes: Sequence[int]) -> pyarrow.Table:
        return pyarrow.Table.from_pydict(
            {"symbol": list(symbols), "size": list(sizes)}, schema=schema
        )

    assert dataset.append_arrow_table(rows(["A", "B"], [1, 2])) == 2
    assert dataset.merge_arrow_table(rows(["B", "C"], [2, 3]), merge_by=["symbol"]) == 1, (
        "C; B is stored as it is"
    )
    assert dataset.merge_arrow_table(rows(["A"], [9]), merge_by=["symbol"]) == 1
    assert dataset.append_arrow_table(rows(["A", "D"], [0, 4]), merge_by=["symbol"]) == 1

    assert {row["symbol"]: row["size"] for row in dataset.read_arrow_table().to_pylist()} == {
        "A": 9,
        "B": 2,
        "C": 3,
        "D": 4,
    }
    assert dataset.overwrite_arrow_table(rows(["E"], [5])) == 1
    assert dataset.read_arrow_table().to_pylist() == [{"symbol": "E", "size": 5}], (
        "every row falls in the one partition void makes, and the overwrite replaced it"
    )
    assert [field.name for field in dataset.iceberg_table.spec().fields] == ["symbol_void"]


def test_a_written_file_records_the_order_it_was_written_in(tmp_path: Path) -> None:
    """A file that does not say it is sorted is sorted again on every ordered
    read, however carefully the writer laid it out -- so every verb that lays
    rows out in the table's order records that it did."""
    catalog = IcebergCatalog(name="recorded", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.recorded", field=Ticked.into_field())

    dataset.append_arrow(ticked([(1, 0), (2, 0)]))
    assert dataset.merge_arrow(ticked([(3, 0), (4, 0)])) == 2
    assert dataset.merge_arrow(ticked([(1, 1), (5, 0)])) == 2, "1 changed and 5 new"

    order_id = dataset.refresh().iceberg_table.sort_order().order_id
    assert order_id, "the declared shape orders this table"
    assert set(dataset.data_files().column("sort_order_id").to_pylist()) == {order_id}

    from rekep.iceberg import dataset as module

    def refuse(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("a file that records its order is not sorted again")

    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(module, "_externally_sorted_task_batches", refuse)
        read = dataset.read_arrow_reader(order_by="at").read_all()
    assert read.column("at").to_pylist() == [1, 2, 3, 4, 5]
    assert read.column("seq").to_pylist() == [1, 0, 0, 0, 0]


def test_a_scan_hands_back_the_narrow_arrow_types(dataset: IcebergDataset) -> None:
    """A scan is narrowed at this package's own seam, whatever width pyiceberg
    decodes a string to, and this is the check that says so when a release
    changes its mind about `pyarrow.use-large-types-on-read`.

    `pyiceberg>=0.12` has no upper bound, so "it is narrow today" is a fact
    about the resolved version rather than about the dependency.
    """
    dataset.append_arrow_table(quotes(4))

    scanned = dataset.read_arrow_reader().read_all()

    widths = {field.name: str(field.type) for field in scanned.schema}
    assert widths["symbol"] == "string", widths
    assert widths["venue"] == "string", widths
    assert not any("large" in one for one in widths.values()), widths


# -- appending what a partition does not hold --------------------------------


def _data_paths(dataset: IcebergDataset) -> set[str]:
    """The data files the current snapshot references."""
    return set(dataset.refresh().data_files().column("file_path").to_pylist())


def _operations(dataset: IcebergDataset) -> list[str]:
    """Each snapshot's operation, oldest first."""
    return [one.summary.operation.value for one in dataset.refresh().iceberg_table.snapshots()]


def _admitted(expression: object, table: pyarrow.Table, field: object) -> int:
    """How many rows of `table` an Iceberg expression admits, through Arrow."""
    from pyiceberg.expressions.visitors import bind, rewrite_not
    from pyiceberg.io.pyarrow import expression_to_pyarrow

    bound = bind(iceberg_schema(field), rewrite_not(expression), case_sensitive=True)
    return table.filter(expression_to_pyarrow(bound)).num_rows


def hourly_rows(*rows: tuple[int, datetime.datetime]) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {"identity": [identity for identity, _ in rows], "at": [at for _, at in rows]},
        schema=HourlyTimed.into_field().into_arrow_schema(),
    )


@scalar
class OptionalVenue:
    """A quote whose venue, its partition, may be unknown."""

    symbol: Annotated[str, primary_key()]
    """Instrument."""

    venue: Annotated[str | None, partition_key()] = None
    """Where it traded, when known."""


def optional_venue(*rows: tuple[str, str | None]) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {"symbol": [symbol for symbol, _ in rows], "venue": [venue for _, venue in rows]},
        schema=OptionalVenue.into_field().into_arrow_schema(),
    )


@pytest.fixture
def opened_files(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The data files native PyArrow opens for reading, in order."""
    from pyiceberg.io.pyarrow import PyArrowFile

    files: list[str] = []
    original = PyArrowFile.open

    def recorded(self: PyArrowFile, *args: object, **kwargs: object) -> object:
        if self.location.endswith(".parquet"):
            files.append(self.location)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(PyArrowFile, "open", recorded)
    return files


@pytest.mark.parametrize("merge_by", [False, []])
def test_a_merge_by_naming_nothing_keeps_the_append_blind(
    dataset: IcebergDataset, merge_by: bool | list[str]
) -> None:
    assert dataset.append_arrow_table(quotes(2), merge_by=merge_by) == 2
    assert dataset.append_arrow_table(quotes(2), merge_by=merge_by) == 2
    assert dataset.read_arrow_table().num_rows == 4


def test_a_keyed_append_adds_only_the_keys_its_partition_does_not_hold(
    dataset: IcebergDataset,
) -> None:
    """An append never replaces: the stored row of a key stands, every stored
    file stands, and what lands is one appended file per partition of the
    rows that were absent."""
    dataset.append_arrow_table(pyarrow.concat_tables([quotes(3), other_day(2)]))
    before = _data_paths(dataset)

    incoming = pyarrow.concat_tables([quotes(4, "XETR"), other_day_keyed("E", 1)])
    assert dataset.append_arrow_table(incoming, merge_by=True) == 2, "the rows it added"

    stored = dataset.read_arrow_table().to_pylist()
    assert {(row["symbol"], row["venue"]) for row in stored} == {
        ("S0", "XPAR"),
        ("S1", "XPAR"),
        ("S2", "XPAR"),
        ("S3", "XETR"),
        ("D0", "XPAR"),
        ("D1", "XPAR"),
        ("E0", "XPAR"),
    }
    after = _data_paths(dataset)
    assert before < after, "no stored file was rewritten or deleted"
    assert len(after - before) == 2, "one new file for each partition the chunk added to"
    assert _operations(dataset) == ["append", "append"]


def test_a_keyed_append_matches_on_the_columns_merge_by_names(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))

    assert dataset.append_arrow_table(keyed("N", 2), merge_by=["venue"]) == 0, (
        "the day already holds a row at XPAR"
    )
    moved = keyed("N", 2).set_column(3, "venue", pyarrow.array(["XETR"] * 2, pyarrow.string()))
    assert dataset.append_arrow_table(moved, merge_by=["venue"]) == 1, "XETR once: its first row"

    assert sorted(dataset.read_arrow_table().column("symbol").to_pylist()) == ["N0", "S0", "S1"]


def test_a_keyed_replay_appends_nothing_and_commits_nothing(dataset: IcebergDataset) -> None:
    source = pyarrow.concat_tables([quotes(3), other_day(2)])
    assert dataset.append_arrow_table(source, merge_by=True) == 5
    table = dataset.iceberg_table
    snapshots = [one.snapshot_id for one in table.snapshots()]
    metadata = table.metadata_location

    replayed = dataset.append_arrow_reader(
        source.to_reader(max_chunksize=1), merge_by=True, commit_row_size=2
    )

    assert replayed == 0
    table = dataset.refresh().iceberg_table
    assert [one.snapshot_id for one in table.snapshots()] == snapshots, "no snapshot"
    assert table.metadata_location == metadata, "and no metadata change of any kind"
    assert dataset.read_arrow_table().num_rows == 5


def test_an_empty_keyed_stream_appends_nothing(dataset: IcebergDataset) -> None:
    empty = pyarrow.RecordBatchReader.from_batches(Quote.into_field().into_arrow_schema(), [])
    assert dataset.append_arrow_reader(empty, merge_by=True) == 0
    assert dataset.exists and dataset.iceberg_table.history() == [], "created, never committed"


def test_a_key_recurring_in_a_chunk_keeps_its_first_row_in_each_partition(
    dataset: IcebergDataset,
) -> None:
    """Rows out of partition order in one batch are taken out by partition
    without losing which of a key's rows came first."""
    day, next_day = datetime.date(2026, 8, 14), datetime.date(2026, 8, 15)
    chunk = pyarrow.Table.from_pydict(
        {
            "symbol": ["A", "A", "B", "A"],
            "day": [day, next_day, day, day],
            "size": [1, 5, 2, 9],
            "venue": ["XPAR"] * 4,
        },
        schema=Quote.into_field().into_arrow_schema(),
    )

    assert dataset.append_arrow_table(chunk, merge_by=True) == 3

    stored = dataset.read_arrow_table().to_pylist()
    assert {(row["symbol"], row["day"]): row["size"] for row in stored} == {
        ("A", day): 1,
        ("A", next_day): 5,
        ("B", day): 2,
    }


def test_a_key_recurring_in_a_later_chunk_is_not_appended_again(
    dataset: IcebergDataset,
) -> None:
    """Each chunk commits before the next one plans, so the later chunk finds
    the key the earlier one appended, and keeps the stored row."""
    day = datetime.date(2026, 8, 14)
    stream = pyarrow.Table.from_pydict(
        {"symbol": ["A", "B", "A"], "day": [day] * 3, "size": [1, 2, 9], "venue": ["XPAR"] * 3},
        schema=Quote.into_field().into_arrow_schema(),
    )

    appended = dataset.append_arrow_reader(
        stream.to_reader(max_chunksize=1), merge_by=True, commit_row_size=1
    )

    assert appended == 2
    assert stored_sizes(dataset) == {"A": 1, "B": 2}
    assert len(dataset.iceberg_table.snapshots()) == 2, "the chunk it held whole committed nothing"


def test_a_keyed_append_scopes_a_key_to_its_transformed_partition(tmp_path: Path) -> None:
    """The same key within an hour is held, whatever its instant; from the
    first instant of the next hour it is another row."""
    catalog = IcebergCatalog(name="scoped", properties=catalog_properties(tmp_path))
    hourly = catalog.dataset("trading.hourly_timed", field=HourlyTimed.into_field())
    ten = datetime.datetime(2026, 8, 14, 10, 5, tzinfo=UTC)
    last = datetime.datetime(2026, 8, 14, 10, 59, 59, 999_999, tzinfo=UTC)
    eleven = datetime.datetime(2026, 8, 14, 11, tzinfo=UTC)
    hourly.append_arrow_table(hourly_rows((1, ten)))

    assert hourly.append_arrow_table(hourly_rows((1, last)), merge_by=True) == 0
    assert hourly.append_arrow_table(hourly_rows((1, eleven)), merge_by=True) == 1
    next_day = ten + datetime.timedelta(days=1)
    assert hourly.append_arrow_table(hourly_rows((1, next_day)), merge_by=True) == 1

    assert sorted(hourly.read_arrow_table().column("at").to_pylist()) == [ten, eleven, next_day]


def test_a_keyed_append_plans_and_reads_only_what_its_chunk_can_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, opened_files: list[str]
) -> None:
    """A chunk carrying the pinned epoch hour and an hour of 2026 plans those
    two hours and none of the fifty-six years between; within each, only the
    files whose key bounds admit its keys; and of those, only the key column."""
    from rekep.iceberg import dataset as module

    catalog = IcebergCatalog(name="pruned", properties=catalog_properties(tmp_path))
    hourly = catalog.dataset("trading.hourly_timed", field=HourlyTimed.into_field())
    minute, hour = datetime.timedelta(minutes=1), datetime.timedelta(hours=1)
    ten = datetime.datetime(2026, 8, 14, 10, tzinfo=UTC)
    hours = [EPOCH, datetime.datetime(2000, 1, 1, tzinfo=UTC), ten - hour, ten, ten + hour]
    hourly.append_arrow_table(
        hourly_rows(*((identity, at + minute) for at in hours for identity in (0, 10)))
    )
    low = _data_paths(hourly)
    hourly.append_arrow_table(
        hourly_rows(*((identity, at + minute) for at in hours for identity in (100, 110)))
    )
    expected = {path for path in low if "=1970-01-01-00/" in path or "=2026-08-14-10/" in path}
    chunk = hourly_rows(
        (5, EPOCH + 2 * minute), (10, EPOCH + 3 * minute), (0, ten + 2 * minute), (7, ten + minute)
    )
    single = hourly.iceberg_table.scan(
        row_filter=_key_bounds(chunk, ["at", "identity"], {"at": "hour"})
    ).plan_files()
    assert len(list(single)) == 4, "one range over the chunk would plan every hour between"

    planned: list[str] = []
    read: list[str] = []
    columns: set[str] = set()
    by_partition = module._tasks_by_partition
    task_batches = module._task_batches

    def planning(table: Any, tasks: Any) -> Any:
        tasks = list(tasks)
        planned.extend(task.file.file_path for task in tasks)
        return by_partition(table, tasks)

    def reading(scan: Any, io: Any, tasks: Any) -> Iterator[pyarrow.RecordBatch]:
        read.extend(task.file.file_path for task in tasks)
        for batch in task_batches(scan, io, tasks):
            columns.update(batch.schema.names)
            yield batch

    monkeypatch.setattr(module, "_tasks_by_partition", planning)
    monkeypatch.setattr(module, "_task_batches", reading)
    opened_files.clear()
    assert hourly.append_arrow_table(chunk, merge_by=True) == 2
    opened = set(opened_files)
    monkeypatch.undo()

    assert len(expected) == 2
    assert set(planned) == expected, "the carried hours' files whose keys overlap"
    assert set(read) == opened == expected, "and nothing else is opened"
    assert columns == {"identity"}, "a stored file is read by its key alone"
    stored = hourly.read_arrow_table()
    assert stored.num_rows == 22
    assert set(stored.column("identity").to_pylist()) == {0, 5, 7, 10, 100, 110}


def test_a_keyed_append_lays_each_partition_out_in_the_table_sort_order(tmp_path: Path) -> None:
    catalog = IcebergCatalog(name="laid-out", properties=catalog_properties(tmp_path))
    hourly = catalog.dataset("trading.hourly_timed", field=HourlyTimed.into_field())
    ten = datetime.datetime(2026, 8, 14, 10, tzinfo=UTC)
    minute = datetime.timedelta(minutes=1)
    hourly.append_arrow_table(hourly_rows((1, ten + 30 * minute)))
    before = _data_paths(hourly)
    shuffled = hourly_rows(
        (5, ten + 50 * minute),
        (1, ten + 40 * minute),
        (4, ten + 80 * minute),
        (3, ten + 5 * minute),
        (2, ten + 70 * minute),
    )

    assert hourly.append_arrow_table(shuffled, merge_by=True) == 4

    order_id = hourly.iceberg_table.sort_order().order_id
    added = [row for row in hourly.data_files().to_pylist() if row["file_path"] not in before]
    assert order_id, "the declared shape orders this table"
    assert [row["sort_order_id"] for row in added] == [order_id, order_id], "one per hour"
    laid_out = sorted(
        pyarrow.parquet.read_table(local(row["file_path"])).column("identity").to_pylist()
        for row in added
    )
    assert laid_out == [[2, 4], [3, 5]], "each hour's new rows in `at` order"


def test_a_keyed_append_refuses_a_null_or_nan_key_before_writing(
    dataset: IcebergDataset,
) -> None:
    """No key can match a null or a NaN, so either would land again on every
    replay: the chunk is refused, and nothing of it is written."""
    dataset.append_arrow_table(quotes(1))
    before = _iceberg_artifacts(dataset)
    nulled = quotes(2).set_column(3, "venue", pyarrow.array(["XPAR", None], pyarrow.string()))
    with pytest.raises(ValueError, match="cannot be null"):
        dataset.append_arrow_table(nulled, merge_by=["venue"])
    assert _iceberg_artifacts(dataset) == before

    @scalar
    class Level:
        price: float
        size: int

    levels = dataset.store.dataset("trading.levels", field=Level.into_field())
    nan = pyarrow.Table.from_pydict(
        {"price": [1.5, float("nan")], "size": [1, 2]},
        schema=Level.into_field().into_arrow_schema(),
    )
    with pytest.raises(ValueError, match="cannot be NaN"):
        levels.append_arrow_table(nan, merge_by=["price"])
    assert levels.iceberg_table.history() == []


def test_a_keyed_append_to_an_unpartitioned_table_prunes_by_its_keys(
    tmp_path: Path, opened_files: list[str]
) -> None:
    catalog = IcebergCatalog(name="flat", properties=catalog_properties(tmp_path))
    flat = catalog.dataset("trading.timed", field=Timed.into_field())
    flat.append_arrow_table(timed(1, 2, 3))

    assert flat.append_arrow_table(timed(3, 5, 2, 4), merge_by=True) == 2
    opened_files.clear()
    assert flat.append_arrow_table(timed(100, 101), merge_by=True) == 2
    assert opened_files == [], "keys no stored file's bounds admit open nothing"

    assert sorted(flat.read_arrow_table().column("unix").to_pylist()) == [1, 2, 3, 4, 5, 100, 101]


def test_a_keyed_append_holds_a_null_partition_apart(
    tmp_path: Path, opened_files: list[str]
) -> None:
    """A null partition is a partition: its keys are its own, and it is
    planned as the partition it is, not as every partition there is."""
    catalog = IcebergCatalog(name="optional", properties=catalog_properties(tmp_path))
    venues = catalog.dataset("trading.optional_venue", field=OptionalVenue.into_field())
    venues.append_arrow_table(optional_venue(("old", None), ("kept", "XPAR")))
    unknown = {
        row["file_path"]
        for row in venues.data_files().to_pylist()
        if row["partition"]["venue"] is None
    }

    opened_files.clear()
    incoming = optional_venue(("old", None), ("new", None), ("kept", None))
    assert venues.append_arrow_table(incoming, merge_by=True) == 2
    assert set(opened_files) == unknown, "the XPAR file holds `kept`, and is never opened"

    stored = venues.read_arrow_table().to_pylist()
    assert {(row["symbol"], row["venue"]) for row in stored} == {
        ("old", None),
        ("new", None),
        ("kept", None),
        ("kept", "XPAR"),
    }
    assert len(stored) == 4


#: An Arrow key type, and how a test integer is spelled as one of its values
#: -- an extension type's as its storage: a UUID's sixteen big-endian bytes.
KEY_KINDS: dict[str, tuple[Any, Callable[[int], Any]]] = {
    "string": (pyarrow.string(), lambda value: f"K{value:04d}"),
    "int32": (pyarrow.int32(), lambda value: value),
    "date": (pyarrow.date32(), lambda value: datetime.date(2026, 1, 1) + datetime.timedelta(value)),
    "decimal": (pyarrow.decimal128(12, 2), lambda value: value),
    "fixed": (pyarrow.binary(4), lambda value: value.to_bytes(4, "big")),
    "timestamp": (
        pyarrow.timestamp("us", tz="UTC"),
        lambda value: EPOCH + datetime.timedelta(seconds=value),
    ),
    "uuid": (pyarrow.uuid(), lambda value: value.to_bytes(16, "big")),
}


@pytest.mark.parametrize("kind", list(KEY_KINDS))
def test_a_keyed_append_matches_and_prunes_any_declared_key_type(
    tmp_path: Path, kind: str, opened_files: list[str]
) -> None:
    dtype, spell = KEY_KINDS[kind]
    schema = pyarrow.schema(
        [
            pyarrow.field(
                "identity",
                dtype,
                nullable=False,
                metadata={**primary_key()["metadata"], **sort_key()["metadata"]},
            ),
            pyarrow.field("part", pyarrow.string(), metadata=partition_key()["metadata"]),
            pyarrow.field("value", pyarrow.int64()),
        ]
    )
    field = Field.from_arrow_schema(schema, name=f"Keyed{kind}")
    keyed_by = IcebergCatalog(name=kind, properties=catalog_properties(tmp_path)).dataset(
        f"trading.keyed_{kind}", field=field
    )

    def rows(*pairs: tuple[int, str]) -> pyarrow.Table:
        values = [value for value, _ in pairs]
        spelled = [spell(value) for value in values]
        if isinstance(dtype, pyarrow.BaseExtensionType):
            identities = pyarrow.ExtensionArray.from_storage(
                dtype, pyarrow.array(spelled, dtype.storage_type)
            )
        else:
            identities = pyarrow.array(spelled, dtype)
        return pyarrow.Table.from_arrays(
            [identities, pyarrow.array([part for _, part in pairs]), pyarrow.array(values)],
            schema=schema,
        )

    assert keyed_by.append_arrow_table(rows((1, "a"), (2, "a")), field, merge_by=True) == 2
    incoming = rows((2, "a"), (3, "a"), (2, "b"))
    assert keyed_by.append_arrow_table(incoming, field, merge_by=True) == 2
    assert keyed_by.append_arrow_table(incoming, field, merge_by=True) == 0, "a replay"
    opened_files.clear()
    assert keyed_by.append_arrow_table(rows((200, "a"), (201, "a")), field, merge_by=True) == 2
    assert opened_files == [], "keys past every stored file's bounds open nothing"

    stored = keyed_by.refresh().read_arrow_table(field)
    placed = list(
        zip(*(stored.column(name).to_pylist() for name in ("part", "value")), strict=True)
    )
    assert sorted(placed) == [("a", 1), ("a", 2), ("a", 3), ("a", 200), ("a", 201), ("b", 2)]


def _quotes_dataset(tmp_path: Path, **options: Any) -> IcebergDataset:
    """The `dataset` fixture's table, under options of its own."""
    return IcebergDataset(
        name="quotes",
        namespace="trading",
        field=Quote.into_field(),
        catalog_name="test",
        catalog_properties=catalog_properties(tmp_path),
        **options,
    )


def _added_records(dataset: IcebergDataset) -> list[int]:
    """Rows each snapshot added, oldest first."""
    snapshots = dataset.refresh().iceberg_table.snapshots()
    return [int(one.summary["added-records"]) for one in snapshots]


def test_a_keyed_append_commits_each_chunk_that_adds_rows(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))

    streamed = dataset.append_arrow_reader(
        quotes(6).to_reader(max_chunksize=1), merge_by=True, commit_row_size=2
    )

    assert streamed == 4
    assert _added_records(dataset) == [2, 2, 2], "the chunk it held whole committed nothing"


@pytest.mark.parametrize("merge_by", [None, True])
def test_without_a_batch_bound_rows_alone_cut_commits(
    tmp_path: Path, merge_by: bool | None
) -> None:
    """Twenty one-row batches under ten rows a commit: two commits of ten,
    where the default eight-batch bound cuts three."""
    unbatched = _quotes_dataset(tmp_path, commit_batch_num=None, commit_row_size=10)
    assert unbatched.commit_batch_num is None
    assert unbatched._commit_limits(None, None) == (10, None)

    written = unbatched.append_arrow_reader(
        quotes(20).to_reader(max_chunksize=1), merge_by=merge_by
    )

    assert written == 20
    assert _added_records(unbatched) == [10, 10]


@pytest.mark.parametrize("merge_by", [None, True])
def test_with_no_bound_at_all_a_stream_is_one_commit(tmp_path: Path, merge_by: bool | None) -> None:
    unbounded = _quotes_dataset(tmp_path, commit_batch_num=None)
    assert unbounded._commit_limits(None, None) == (None, None)

    written = unbounded.append_arrow_reader(
        quotes(20).to_reader(max_chunksize=1), merge_by=merge_by
    )

    assert written == 20
    assert _added_records(unbounded) == [20]


@pytest.mark.parametrize("argument", ["commit_batch_num", "commit_row_size"])
def test_a_bound_that_bounds_nothing_is_still_refused(
    dataset: IcebergDataset, tmp_path: Path, argument: str
) -> None:
    with pytest.raises(ValueError, match=f"{argument} must be positive"):
        dataset.append_arrow_reader(
            quotes(2).to_reader(max_chunksize=1), merge_by=True, **{argument: 0}
        )
    assert dataset.iceberg_table.history() == []
    with pytest.raises(ValueError, match=f"{argument} must be positive"):
        _quotes_dataset(tmp_path, **{argument: 0})
    with pytest.raises(TypeError, match=f"{argument} must be an integer"):
        _quotes_dataset(tmp_path, **{argument: 1.5})


def test_a_keyed_append_reads_and_lands_on_its_own_branch(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    dataset.create_branch("work")
    dataset.append_arrow_table(quotes(2).slice(1))

    landed = dataset.append_arrow_table(
        quotes(2, "work"), merge_by=True, branch="work", properties={"rekep.test": "keyed"}
    )

    assert landed == 1, "the branch never held S1"
    assert dataset.append_arrow_table(quotes(2), merge_by=True) == 0, "main holds both"
    work = dataset.read_arrow_table(branch="work").to_pylist()
    assert {(row["symbol"], row["venue"]) for row in work} == {("S0", "XPAR"), ("S1", "work")}
    assert set(dataset.read_arrow_table().column("venue").to_pylist()) == {"XPAR"}
    head = dataset.iceberg_table.refs()["work"]
    snapshot = dataset.iceberg_table.metadata.snapshot_by_id(head.snapshot_id)
    assert snapshot.summary["rekep.test"] == "keyed"


def _landed_before_the_commit(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, concurrent: Callable[[], object]
) -> Callable[[], int]:
    """Land `concurrent` once `dataset` has planned and staged its next chunk
    and before it commits it, so the commit meets a head its plan never read;
    the catalog refuses it, and PyIceberg retries against the moved head,
    validating what landed in between. Returns how many commits this
    dataset's catalog was asked for."""
    original = dataset._commit_replacement
    catalog = dataset.catalog
    commit_table = catalog.commit_table
    attempts = 0

    def landed(*args: Any, **kwargs: Any) -> Any:
        monkeypatch.setattr(dataset, "_commit_replacement", original)
        concurrent()
        return original(*args, **kwargs)

    def counted(*args: Any, **kwargs: Any) -> Any:
        nonlocal attempts
        attempts += 1
        return commit_table(*args, **kwargs)

    monkeypatch.setattr(dataset, "_commit_replacement", landed)
    monkeypatch.setattr(catalog, "commit_table", counted)
    return lambda: attempts


@pytest.mark.parametrize("merged", [True, False])
def test_a_keyed_append_beaten_to_one_of_its_keys_is_handed_back_without_a_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, merged: bool
) -> None:
    """The append declares the partitions and key ranges its plan read. A key
    another writer landed under them since is one the plan never saw, so
    PyIceberg's retry refuses the commit -- a merged append as a fast one --
    and it is handed back, with nothing it staged left behind, for a fresh
    plan that finds the key held."""
    from pyiceberg.exceptions import CommitFailedException

    dataset = _quotes_dataset(
        tmp_path, table_properties={"commit.manifest-merge.enabled": str(merged).lower()}
    )
    dataset.append_arrow_table(quotes(2))
    another = _another_writer(dataset)
    attempts = _landed_before_the_commit(
        dataset, monkeypatch, lambda: another.append_arrow_table(quotes(3, "later").slice(2))
    )

    with monkeypatch.context() as observed:
        written, _ = _observed_writes(dataset, observed)
        with pytest.raises(CommitFailedException, match="changed since this write was planned"):
            dataset.append_arrow_table(quotes(3, "mine"), merge_by=True)

    assert attempts() == 1, "refused once; the retry was refused before it reached the catalog"
    assert written, "the chunk was staged before its commit met the other writer"
    assert not any(local(path).exists() for path in written), "and none of it is left"
    stored = dataset.refresh().read_arrow_table().to_pylist()
    assert sorted((row["symbol"], row["venue"]) for row in stored) == [
        ("S0", "XPAR"),
        ("S1", "XPAR"),
        ("S2", "later"),
    ]
    assert dataset.append_arrow_table(quotes(3, "mine"), merge_by=True) == 0


def test_a_keyed_append_beaten_to_the_removal_of_a_key_it_found_is_handed_back(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A key the plan found held, and so left out, is one another writer may
    have taken out since: rows removed under the declared ranges are a
    conflict too, or the key would be held nowhere."""
    from pyiceberg.exceptions import CommitFailedException

    dataset.append_arrow_table(quotes(2))
    another = _another_writer(dataset)
    _landed_before_the_commit(dataset, monkeypatch, lambda: another.delete_where("symbol = 'S1'"))

    with pytest.raises(CommitFailedException, match="changed since this write was planned"):
        dataset.append_arrow_table(quotes(3, "mine"), merge_by=True)

    assert dataset.refresh().read_arrow_table().column("symbol").to_pylist() == ["S0"]
    assert dataset.append_arrow_table(quotes(3, "mine"), merge_by=True) == 2
    stored = dataset.read_arrow_table().to_pylist()
    assert {(row["symbol"], row["venue"]) for row in stored} == {
        ("S0", "XPAR"),
        ("S1", "mine"),
        ("S2", "mine"),
    }


@pytest.mark.parametrize("elsewhere", ["another day", "another key range"])
def test_a_keyed_append_beaten_elsewhere_lands_on_the_retry(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, elsewhere: str
) -> None:
    """Rows landed outside the partitions and key ranges the plan read are no
    conflict: PyIceberg's retry lands the append beside them."""
    dataset.append_arrow_table(quotes(2))
    another = _another_writer(dataset)
    beside = other_day(1) if elsewhere == "another day" else keyed("Z", 1)
    attempts = _landed_before_the_commit(
        dataset, monkeypatch, lambda: another.append_arrow_table(beside)
    )

    assert dataset.append_arrow_table(quotes(3, "mine"), merge_by=True) == 1

    assert attempts() == 2, "refused against the moved head, landed on the retry"
    stored = dataset.refresh().read_arrow_table().to_pylist()
    assert sorted(row["symbol"] for row in stored) == sorted(
        ["S0", "S1", "S2", beside.column("symbol")[0].as_py()]
    )
    assert _operations(dataset) == ["append", "append", "append"]


def test_a_keyed_append_through_a_stale_handle_never_duplicates_a_key(tmp_path: Path) -> None:
    """A handle planning against a head another writer has moved plans on
    what it read, and its commit is checked against what landed since: a key
    landed under its ranges hands the write back, and anything else lands."""
    from pyiceberg.exceptions import CommitFailedException

    catalog = IcebergCatalog(name="stale", properties=catalog_properties(tmp_path))
    writer = catalog.dataset("trading.timed", field=Timed.into_field())
    assert writer.append_arrow_table(timed(0, 1), merge_by=True) == 2
    other = catalog.dataset("trading.timed", field=Timed.into_field())
    other.append_arrow_table(timed(100))

    assert writer.append_arrow_table(timed(1, 2), merge_by=True) == 1

    other.refresh().append_arrow_table(timed(3))
    with pytest.raises(CommitFailedException, match="branch main has changed"):
        writer.append_arrow_table(timed(3, 4), merge_by=True)
    assert writer.refresh().append_arrow_table(timed(3, 4), merge_by=True) == 1

    assert sorted(writer.read_arrow_table().column("unix").to_pylist()) == [0, 1, 2, 3, 4, 100]


def test_a_keyed_append_the_catalog_refuses_is_handed_back_rather_than_rebuilt(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A blind append rebuilds a refused commit on the new head; a keyed one
    decided what to add by reading the old one, so it is never committed
    again on that plan, and leaves nothing behind."""
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.table import Transaction

    dataset.append_arrow_table(quotes(1))
    before = _iceberg_artifacts(dataset)
    attempts = 0

    def contended(_self: Transaction) -> None:
        nonlocal attempts
        attempts += 1
        raise CommitFailedException("another writer won")

    monkeypatch.setattr(dataset, "retry_backoff", 0.0)
    monkeypatch.setattr(Transaction, "commit_transaction", contended)
    with pytest.raises(CommitFailedException, match="another writer won"):
        dataset.append_arrow_table(quotes(2), merge_by=True)
    monkeypatch.undo()

    assert attempts == 1
    assert _iceberg_artifacts(dataset) == before
    assert dataset.refresh().append_arrow_table(quotes(2), merge_by=True) == 1


def test_a_keyed_append_whose_acknowledgement_is_lost_is_found_rather_than_replayed(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.exceptions import CommitStateUnknownException
    from pyiceberg.table import Transaction

    dataset.append_arrow_table(quotes(1))
    commit = Transaction.commit_transaction
    attempts = 0

    def committed(transaction: Transaction) -> None:
        nonlocal attempts
        attempts += 1
        commit(transaction)
        raise CommitStateUnknownException("commit acknowledgement lost")

    monkeypatch.setattr(dataset, "retry_backoff", 0.0)
    monkeypatch.setattr(Transaction, "commit_transaction", committed)
    assert dataset.append_arrow_table(quotes(3), merge_by=True) == 2
    monkeypatch.undo()

    assert attempts == 1, "the operation id found the commit before replaying it"
    assert _added_records(dataset) == [1, 2]
    assert sorted(dataset.read_arrow_table().column("symbol").to_pylist()) == ["S0", "S1", "S2"]


@pytest.mark.parametrize("verb", ["merge", "overwrite"])
def test_a_replacement_that_takes_nothing_out_is_still_validated(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, verb: str
) -> None:
    """A merge or an overwrite into a partition holding none of its rows
    commits an append, and that append declares what it replaces as an
    overwrite does: a row another writer landed there since is refused, not
    landed beside."""
    from pyiceberg.exceptions import CommitFailedException

    dataset.append_arrow_table(quotes(1))
    another = _another_writer(dataset)
    _landed_before_the_commit(
        dataset, monkeypatch, lambda: another.append_arrow_table(other_day_keyed("N", 1))
    )

    with pytest.raises(CommitFailedException, match="changed since this write was planned"):
        _replaced(dataset, verb, other_day_keyed("N", 2))

    assert sorted(dataset.refresh().read_arrow_table().column("symbol").to_pylist()) == [
        "N0",
        "S0",
    ]


# -- merging what differs ------------------------------------------------------


def test_a_merge_inserts_absent_keys_and_rewrites_only_the_file_it_changes(
    dataset: IcebergDataset,
) -> None:
    """Of three stored files, the one holding a row that changed is written
    back without it; the one holding only rows the chunk carries unchanged,
    and the other day's, stand. What comes back is the rows inserted or
    replaced."""
    dataset.append_arrow_table(quotes(3))
    changed_file = _data_paths(dataset)
    dataset.append_arrow_table(keyed("K", 2))
    same_file = _data_paths(dataset) - changed_file
    dataset.append_arrow_table(other_day(2))
    other_file = _data_paths(dataset) - changed_file - same_file
    moved = quotes(3).set_column(
        3, quotes(3).schema.field("venue"), pyarrow.array(["XPAR", "XETR", "XPAR"])
    )
    incoming = pyarrow.concat_tables([moved, keyed("K", 2), keyed("N", 2), other_day(2)])

    assert dataset.merge_arrow_table(incoming) == 3, "S1 replaced, N0 and N1 inserted"

    after = _data_paths(dataset)
    assert same_file < after and other_file < after, "unchanged files stand"
    assert not changed_file & after, "the changed one is written back without S1"
    stored = dataset.read_arrow_table().to_pylist()
    assert len(stored) == 9
    assert {(row["symbol"], row["venue"]) for row in stored} == {
        ("S0", "XPAR"),
        ("S1", "XETR"),
        ("S2", "XPAR"),
        ("K0", "XPAR"),
        ("K1", "XPAR"),
        ("N0", "XPAR"),
        ("N1", "XPAR"),
        ("D0", "XPAR"),
        ("D1", "XPAR"),
    }
    assert _operations(dataset)[-1] == "overwrite"


def test_a_streamed_merge_replay_writes_nothing_and_commits_nothing(
    dataset: IcebergDataset,
) -> None:
    """Every chunk of a replay over several partitions finds its rows stored
    as they are: no file, no snapshot, and no metadata change of any kind."""
    source = pyarrow.concat_tables([quotes(3), other_day(2), third_day(2)])
    assert dataset.merge_arrow_table(source) == 7
    table = dataset.iceberg_table
    metadata, files = table.metadata_location, _data_paths(dataset)

    replayed = dataset.merge_arrow_reader(source.to_reader(max_chunksize=1), commit_row_size=2)

    assert replayed == 0
    assert dataset.refresh().iceberg_table.metadata_location == metadata
    assert _data_paths(dataset) == files
    assert dataset.read_arrow_table().num_rows == 7


def _entries_dataset(tmp_path: Path) -> tuple[IcebergDataset, Field]:
    """A table keyed on `symbol` whose `entries` is a nullable list of structs."""
    entry = pyarrow.struct([("tag", pyarrow.int32()), ("value", pyarrow.string())])
    schema = pyarrow.schema(
        [
            pyarrow.field(
                "symbol", pyarrow.string(), nullable=False, metadata=primary_key()["metadata"]
            ),
            pyarrow.field("entries", pyarrow.list_(pyarrow.field("element", entry))),
        ]
    )
    field = Field.from_arrow_schema(schema, name="Entries")
    catalog = IcebergCatalog(name="entries", properties=catalog_properties(tmp_path))
    return catalog.dataset("trading.entries", field=field), field


def test_a_null_list_stored_as_empty_is_the_same_row(tmp_path: Path) -> None:
    """PyIceberg writes a null list of structs as an empty one, so a row is
    compared as a file would hold it: replaying the null is no change, and a
    list that did change is."""
    entries, field = _entries_dataset(tmp_path)

    def rows(first: list[dict[str, Any]] | None) -> pyarrow.Table:
        return pyarrow.Table.from_pydict(
            {"symbol": ["A", "B"], "entries": [first, [{"tag": 1, "value": "x"}]]},
            schema=field.into_arrow_schema(),
        )

    assert entries.merge_arrow_table(rows(None), field) == 2
    assert entries.read_arrow_table(field).column("entries").to_pylist()[0] == [], (
        "the premise: the null was written as an empty list"
    )
    snapshots = len(entries.iceberg_table.snapshots())

    assert entries.merge_arrow_table(rows(None), field) == 0, "null against [] is no change"
    assert entries.merge_arrow_table(rows([]), field) == 0, "and neither is [] itself"
    assert len(entries.refresh().iceberg_table.snapshots()) == snapshots
    assert entries.merge_arrow_table(rows([{"tag": 2, "value": "y"}]), field) == 1
    assert entries.read_arrow_table(field).sort_by("symbol").to_pylist() == [
        {"symbol": "A", "entries": [{"tag": 2, "value": "y"}]},
        {"symbol": "B", "entries": [{"tag": 1, "value": "x"}]},
    ]


@pytest.mark.parametrize("stored", ["one file", "two files"])
def test_a_key_stored_twice_is_collapsed_to_the_incoming_row(
    dataset: IcebergDataset, stored: str
) -> None:
    """A blind append can hold a key twice. A merge carrying that key
    replaces it wherever it is, even where one copy holds the same values,
    and the partition holds it once after."""
    if stored == "one file":
        dataset.append_arrow_table(
            pyarrow.concat_tables([quotes(2), quotes(1, "again")]), merge_by=False
        )
    else:
        dataset.append_arrow_table(quotes(2))
        dataset.append_arrow_table(quotes(1, "again"), merge_by=False)
    assert sorted(dataset.read_arrow_table().column("symbol").to_pylist()) == ["S0", "S0", "S1"]

    assert dataset.merge_arrow_table(quotes(1)) == 1

    stored_rows = dataset.read_arrow_table().to_pylist()
    assert sorted((row["symbol"], row["venue"]) for row in stored_rows) == [
        ("S0", "XPAR"),
        ("S1", "XPAR"),
    ]
    assert dataset.merge_arrow_table(quotes(2)) == 0, "and once held, a replay is no change"


def test_a_merge_scopes_a_key_to_its_transformed_partition(tmp_path: Path) -> None:
    """Within an hour a key names one row, whatever its instant, and a row at
    another instant of that hour replaces it; from the next hour the key is
    another row."""
    catalog = IcebergCatalog(name="scoped", properties=catalog_properties(tmp_path))
    hourly = catalog.dataset("trading.hourly_timed", field=HourlyTimed.into_field())
    ten = datetime.datetime(2026, 8, 14, 10, 5, tzinfo=UTC)
    last = datetime.datetime(2026, 8, 14, 10, 59, 59, 999_999, tzinfo=UTC)
    eleven = datetime.datetime(2026, 8, 14, 11, tzinfo=UTC)
    assert hourly.merge_arrow_table(hourly_rows((1, ten))) == 1

    assert hourly.merge_arrow_table(hourly_rows((1, last))) == 1
    assert hourly.merge_arrow_table(hourly_rows((1, last))) == 0
    assert hourly.merge_arrow_table(hourly_rows((1, eleven))) == 1

    assert sorted(hourly.read_arrow_table().column("at").to_pylist()) == [last, eleven]


@pytest.mark.parametrize("verb", ["append", "merge"])
def test_a_key_recurring_across_chunks_keeps_its_first_row_in_the_sort_order(
    tmp_path: Path, verb: str
) -> None:
    """The stream is laid out in the table's sort order before it is cut into
    chunks, so a key's first row is its earliest `at`, not its first arrival,
    whichever chunk the others land in; they commit nothing."""
    catalog = IcebergCatalog(name="recurring", properties=catalog_properties(tmp_path))
    hourly = catalog.dataset("trading.hourly_timed", field=HourlyTimed.into_field())
    ten = datetime.datetime(2026, 8, 14, 10, tzinfo=UTC)
    minute = datetime.timedelta(minutes=1)
    stream = hourly_rows(
        (1, ten + 50 * minute),
        (2, ten + 20 * minute),
        (1, ten + 5 * minute),
        (1, ten + 30 * minute),
    )
    reader = stream.to_reader(max_chunksize=1)

    if verb == "append":
        written = hourly.append_arrow_reader(reader, commit_row_size=1)
    else:
        written = hourly.merge_arrow_reader(reader, commit_row_size=1)

    assert written == 2
    stored = hourly.read_arrow_table().to_pylist()
    assert {row["identity"]: row["at"] for row in stored} == {
        1: ten + 5 * minute,
        2: ten + 20 * minute,
    }
    assert len(hourly.iceberg_table.snapshots()) == 2, "one chunk per row, and two landed"


def test_a_merge_reads_and_lands_on_its_own_branch(dataset: IcebergDataset) -> None:
    """The branch holds S0 alone: its S0 is replaced and S1 inserted there,
    while main, holding both as they were, finds nothing to merge."""
    dataset.append_arrow_table(quotes(1))
    dataset.create_branch("work")
    dataset.append_arrow_table(quotes(2).slice(1))

    landed = dataset.merge_arrow_table(
        quotes(2, "work"), branch="work", properties={"rekep.test": "merged"}
    )

    assert landed == 2
    assert dataset.merge_arrow_table(quotes(2, "work"), branch="work") == 0, "a replay there"
    assert dataset.merge_arrow_table(quotes(2)) == 0, "main holds both as they are"
    work = dataset.read_arrow_table(branch="work").to_pylist()
    assert {(row["symbol"], row["venue"]) for row in work} == {("S0", "work"), ("S1", "work")}
    assert set(dataset.read_arrow_table().column("venue").to_pylist()) == {"XPAR"}
    head = dataset.iceberg_table.refs()["work"]
    snapshot = dataset.iceberg_table.metadata.snapshot_by_id(head.snapshot_id)
    assert snapshot.summary["rekep.test"] == "merged"


def test_a_merge_beaten_to_one_of_its_keys_is_handed_back_without_a_duplicate(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S2 was absent when the merge planned and another writer landed it
    before the commit: the retry is refused, nothing staged is left, and a
    fresh plan replaces the S2 it now finds."""
    from pyiceberg.exceptions import CommitFailedException

    dataset.append_arrow_table(quotes(2))
    another = _another_writer(dataset)
    attempts = _landed_before_the_commit(
        dataset, monkeypatch, lambda: another.append_arrow_table(quotes(3, "later").slice(2))
    )

    with monkeypatch.context() as observed:
        written, _ = _observed_writes(dataset, observed)
        with pytest.raises(CommitFailedException, match="changed since this write was planned"):
            dataset.merge_arrow_table(quotes(3, "mine"))

    assert attempts() == 1, "refused once; the retry was refused before it reached the catalog"
    assert written, "the chunk was staged before its commit met the other writer"
    assert not any(local(path).exists() for path in written), "and none of it is left"
    stored = dataset.refresh().read_arrow_table().to_pylist()
    assert sorted((row["symbol"], row["venue"]) for row in stored) == [
        ("S0", "XPAR"),
        ("S1", "XPAR"),
        ("S2", "later"),
    ]
    assert dataset.merge_arrow_table(quotes(3, "mine")) == 3
    stored = dataset.read_arrow_table().to_pylist()
    assert sorted((row["symbol"], row["venue"]) for row in stored) == [
        ("S0", "mine"),
        ("S1", "mine"),
        ("S2", "mine"),
    ]


@pytest.mark.parametrize("verb", ["append", "merge"])
def test_a_write_with_nothing_to_commit_is_handed_back_when_a_key_it_found_went(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, verb: str
) -> None:
    """Every row is found stored as it is, so nothing commits and no commit
    validates the plan -- but another writer deleted S1 after it was found,
    and success would lose it. The head the plan read is checked instead."""
    from pyiceberg.exceptions import CommitFailedException

    dataset.append_arrow_table(quotes(2))
    another = _another_writer(dataset)
    _landed_before_the_commit(dataset, monkeypatch, lambda: another.delete_where("symbol = 'S1'"))
    write = dataset.append_arrow_table if verb == "append" else dataset.merge_arrow_table

    with pytest.raises(CommitFailedException, match="changed since this write was planned"):
        write(quotes(2))

    assert len(dataset.refresh().iceberg_table.snapshots()) == 2, "the delete's commit alone"
    assert dataset.read_arrow_table().column("symbol").to_pylist() == ["S0"]
    assert write(quotes(2)) == 1, "a fresh plan finds S1 absent"


@pytest.mark.parametrize("verb", ["append", "merge"])
@pytest.mark.parametrize("elsewhere", ["another day", "another key range"])
def test_a_write_beaten_outside_what_it_read_lands_or_passes(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, verb: str, elsewhere: str
) -> None:
    """Rows another writer lands outside the partitions and key ranges a plan
    read are no conflict: a write with nothing to commit still succeeds, and
    one with rows to commit lands them on PyIceberg's retry."""
    dataset.append_arrow_table(quotes(2))
    another = _another_writer(dataset)
    beside = other_day(1) if elsewhere == "another day" else keyed("Z", 1)
    _landed_before_the_commit(dataset, monkeypatch, lambda: another.append_arrow_table(beside))
    write = dataset.append_arrow_table if verb == "append" else dataset.merge_arrow_table

    assert write(quotes(2)) == 0
    assert _operations(dataset) == ["append", "append"], "the other writer's commit alone"

    attempts = _landed_before_the_commit(
        dataset, monkeypatch, lambda: another.append_arrow_table(third_day(1))
    )
    changed = quotes(3, "mine")
    assert write(changed) == (1 if verb == "append" else 3)
    assert attempts() == 2, "refused against the moved head, landed on the retry"
    stored = dataset.refresh().read_arrow_table().column("symbol").to_pylist()
    assert sorted(stored) == sorted(["S0", "S1", "S2", "T0", beside.column("symbol")[0].as_py()])


def test_a_merge_plans_by_partition_and_reads_whole_only_the_files_holding_its_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, opened_files: list[str]
) -> None:
    """Never the table: the chunk's two hours are planned, and within them
    only the files whose key bounds admit its keys. Each planned file is read
    by its key alone; only the one holding a carried key is read whole."""
    from rekep.iceberg import dataset as module

    catalog = IcebergCatalog(name="merged", properties=catalog_properties(tmp_path))
    hourly = catalog.dataset("trading.hourly_timed", field=HourlyTimed.into_field())
    minute, hour = datetime.timedelta(minutes=1), datetime.timedelta(hours=1)
    ten = datetime.datetime(2026, 8, 14, 10, tzinfo=UTC)
    hours = [EPOCH, ten - hour, ten, ten + hour]
    hourly.append_arrow_table(
        hourly_rows(*((identity, at + minute) for at in hours for identity in (0, 10)))
    )
    low = _data_paths(hourly)
    hourly.append_arrow_table(
        hourly_rows(*((identity, at + minute) for at in hours for identity in (100, 110)))
    )
    (epoch_low,) = (path for path in low if "=1970-01-01-00/" in path)
    (ten_low,) = (path for path in low if "=2026-08-14-10/" in path)
    chunk = hourly_rows(
        (5, EPOCH + 2 * minute),  # new, inside the epoch file's key bounds
        (7, EPOCH + 3 * minute),  # new, likewise
        (0, ten + 2 * minute),  # held at 10:01, so changed
        (10, ten + minute),  # held as it is
    )

    planned: list[str] = []
    reads: list[tuple[str, frozenset[str]]] = []
    by_partition = module._tasks_by_partition
    task_batches = module._task_batches

    def planning(table: Any, tasks: Any) -> Any:
        tasks = list(tasks)
        planned.extend(task.file.file_path for task in tasks)
        return by_partition(table, tasks)

    def reading(scan: Any, io: Any, tasks: Any) -> Iterator[pyarrow.RecordBatch]:
        for batch in task_batches(scan, io, tasks):
            reads.extend((task.file.file_path, frozenset(batch.schema.names)) for task in tasks)
            yield batch

    monkeypatch.setattr(module, "_tasks_by_partition", planning)
    monkeypatch.setattr(module, "_task_batches", reading)
    opened_files.clear()
    assert hourly.merge_arrow_table(chunk) == 3
    opened = set(opened_files)
    monkeypatch.undo()

    assert set(planned) == {epoch_low, ten_low}, "the carried hours' files whose keys overlap"
    assert opened == {epoch_low, ten_low}, "and nothing else is opened"
    assert {path for path, names in reads if names == {"identity"}} == {epoch_low, ten_low}
    assert {path for path, names in reads if names != {"identity"}} == {ten_low}, (
        "whole rows only of the file holding a carried key"
    )
    stored = hourly.read_arrow_table()
    assert stored.num_rows == 18
    zero = stored.filter(pyarrow.compute.equal(stored.column("identity"), 0))
    assert sorted(zero.column("at").to_pylist()) == [
        EPOCH + minute,
        ten - hour + minute,
        ten + 2 * minute,
        ten + hour + minute,
    ], "the changed row replaced in its hour, and no other hour's touched"


# -- the sorted spill every write reads through ---------------------------------


def shuffled_hours(count: int) -> pyarrow.Table:
    """`count` rows over three hours, arriving in no order of hour or instant."""
    ten = datetime.datetime(2026, 8, 14, 10, tzinfo=UTC)
    return hourly_rows(
        *(
            (identity, ten + datetime.timedelta(minutes=(identity * 37) % 180))
            for identity in range(count)
        )
    )


def _files_by_hour(dataset: IcebergDataset) -> dict[Any, list[list[datetime.datetime]]]:
    """Each hour's data files, as the `at` values each holds, in stored order."""
    files: dict[Any, list[list[datetime.datetime]]] = {}
    for row in dataset.refresh().data_files().to_pylist():
        held = pyarrow.parquet.read_table(local(row["file_path"])).column("at").to_pylist()
        files.setdefault(row["partition"]["at_hour"], []).append(held)
    return files


#: Every verb, writing `shuffled_hours` into an empty `HourlyTimed` table.
SPILLED_VERBS: dict[str, Callable[[IcebergDataset, pyarrow.RecordBatchReader], int]] = {
    "blind": lambda target, reader: target.append_arrow_reader(
        reader, merge_by=False, commit_row_size=4
    ),
    "append": lambda target, reader: target.append_arrow_reader(reader, commit_row_size=4),
    "merge": lambda target, reader: target.merge_arrow_reader(reader, commit_row_size=4),
    "overwrite": lambda target, reader: target.overwrite_arrow_reader(reader, commit_row_size=4),
    "row_filter": lambda target, reader: target.overwrite_arrow_reader(
        reader, commit_row_size=4, row_filter="identity >= 0"
    ),
}


@pytest.mark.parametrize("verb", list(SPILLED_VERBS))
def test_every_verb_lays_each_partition_out_sorted_and_disjoint(tmp_path: Path, verb: str) -> None:
    """A shuffled stream over three hours: each hour lands in several files,
    each sorted on `at` and recording the table's order, their ranges
    disjoint, so the hour read file after file is in order."""
    catalog = IcebergCatalog(name="laid-out", properties=catalog_properties(tmp_path))
    hourly = catalog.dataset("trading.hourly_timed", field=HourlyTimed.into_field())
    source = shuffled_hours(30)

    assert SPILLED_VERBS[verb](hourly, source.to_reader(max_chunksize=1)) == 30

    order_id = hourly.iceberg_table.sort_order().order_id
    assert order_id, "the declared shape orders this table"
    assert set(hourly.data_files().column("sort_order_id").to_pylist()) == {order_id}
    files = _files_by_hour(hourly)
    assert len(files) == 3
    assert all(len(held) > 1 for held in files.values()), "each hour spans several files"
    for held in files.values():
        assert all(values == sorted(values) for values in held), "each file sorted"
        laid = [value for values in sorted(held) for value in values]
        assert laid == sorted(laid), "and the hour's files hold disjoint ranges"
    assert sorted(hourly.read_arrow_table().column("identity").to_pylist()) == list(range(30))


def _spill_runs(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The first-generation runs every write spills, recorded as they are written."""
    from rekep.iceberg import dataset as module

    write_run = module._write_ipc_batches
    runs: list[str] = []

    def spilled(path: str, schema: pyarrow.Schema, batches: Any) -> None:
        runs.append(path)
        write_run(path, schema, batches)

    monkeypatch.setattr(module, "_write_ipc_batches", spilled)
    return runs


def test_a_write_spills_under_its_spill_directory_and_leaves_it_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spill = tmp_path / "spill"
    spill.mkdir()
    catalog = IcebergCatalog(name="spilled", properties=catalog_properties(tmp_path))
    hourly = catalog.dataset(
        "trading.hourly_timed", field=HourlyTimed.into_field(), spill_directory=str(spill)
    )
    runs = _spill_runs(monkeypatch)

    assert hourly.merge_arrow_reader(shuffled_hours(12).to_reader(max_chunksize=1)) == 12

    assert len(runs) > 3, "a run per chunk and hour"
    assert all(Path(path).is_relative_to(spill) for path in runs)
    assert list(spill.iterdir()) == [], "and nothing of them is left"


@pytest.mark.parametrize("failure", ["source", "commit"])
def test_a_failed_write_leaves_its_spill_directory_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    """A source that stops mid-stream, or a commit that fails after the
    spill, takes the spilled runs with it and commits nothing."""
    spill = tmp_path / "spill"
    spill.mkdir()
    catalog = IcebergCatalog(name="spilled", properties=catalog_properties(tmp_path))
    hourly = catalog.dataset(
        "trading.hourly_timed", field=HourlyTimed.into_field(), spill_directory=str(spill)
    )
    hourly.get_or_create_table()
    runs = _spill_runs(monkeypatch)
    rows = shuffled_hours(12)

    def broken() -> Iterator[pyarrow.RecordBatch]:
        yield from rows.slice(0, 6).to_batches(max_chunksize=1)
        raise pyarrow.ArrowInvalid("source stopped")

    def refused(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("commit stopped")

    if failure == "source":
        source = pyarrow.RecordBatchReader.from_batches(rows.schema, broken())
        with pytest.raises(pyarrow.ArrowInvalid, match="source stopped"):
            hourly.merge_arrow_reader(source, commit_row_size=2)
    else:
        monkeypatch.setattr(hourly, "_merge_chunk", refused)
        with pytest.raises(RuntimeError, match="commit stopped"):
            hourly.merge_arrow_reader(rows.to_reader(max_chunksize=1), commit_row_size=2)

    assert runs, "something was spilled before the write failed"
    assert list(spill.iterdir()) == []
    assert hourly.refresh().iceberg_table.history() == []


def test_a_table_neither_partitioned_nor_sorted_streams_through_without_spilling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nothing to lay out, so nothing is spilled: each chunk commits before
    the next is taken. A sort order alone is something to lay out."""

    @scalar
    class Plain:
        symbol: Annotated[str, primary_key()]
        size: int

    catalog = IcebergCatalog(name="plain", properties=catalog_properties(tmp_path))
    plain = catalog.dataset("trading.plain", field=Plain.into_field())
    runs = _spill_runs(monkeypatch)
    rows = pyarrow.Table.from_pydict(
        {"symbol": ["C", "A", "B", "D"], "size": [3, 1, 2, 4]},
        schema=Plain.into_field().into_arrow_schema(),
    )
    taken = 0
    taken_at_commit: list[int] = []

    def counted() -> Iterator[pyarrow.RecordBatch]:
        nonlocal taken
        for batch in rows.to_batches(max_chunksize=1):
            taken += batch.num_rows
            yield batch

    original = plain._append_key_chunk

    def commit(table: Any, chunk: pyarrow.Table, *args: Any, **kwargs: Any) -> Any:
        taken_at_commit.append(taken)
        return original(table, chunk, *args, **kwargs)

    monkeypatch.setattr(plain, "_append_key_chunk", commit)
    source = pyarrow.RecordBatchReader.from_batches(rows.schema, counted())
    assert plain.append_arrow_reader(source, commit_row_size=2) == 4

    assert runs == [], "not one run spilled"
    assert taken_at_commit == [2, 4], "each chunk committed as soon as it was taken"
    assert plain.merge_arrow_table(rows) == 0
    assert plain.append_arrow_table(rows, merge_by=False) == 4
    assert runs == []

    sorted_flat = catalog.dataset("trading.timed", field=Timed.into_field())
    assert sorted_flat.append_arrow_table(timed(3, 1, 2)) == 3
    assert runs, "a table with a sort order lays its rows out through the spill"


@pytest.mark.parametrize(
    ("bound", "commits"),
    [({"commit_row_size": 4}, [4, 4, 2]), ({"commit_batch_num": 3}, [3, 3, 3, 1])],
)
def test_commits_are_cut_from_the_spilled_stream_in_partition_order(
    tmp_path: Path, bound: dict[str, int], commits: list[int]
) -> None:
    """Ten rows arriving eleven o'clock first: the spill hands them back ten
    o'clock first, each hour in `at` order, and the bound cuts that stream --
    by rows, or by the largest chunk it spilled -- whatever hour a cut falls in."""
    catalog = IcebergCatalog(name="cut", properties=catalog_properties(tmp_path))
    hourly = catalog.dataset("trading.hourly_timed", field=HourlyTimed.into_field())
    ten, minute = datetime.datetime(2026, 8, 14, 10, tzinfo=UTC), datetime.timedelta(minutes=1)
    arrivals = [60 + 50, 60 + 10, 60 + 30, 60 + 20, 60 + 40, 60 + 0, 30, 10, 20, 0]
    stream = hourly_rows(*((index, ten + at * minute) for index, at in enumerate(arrivals)))

    assert (
        hourly.append_arrow_reader(stream.to_reader(max_chunksize=1), merge_by=False, **bound) == 10
    )

    assert _added_records(hourly) == commits
    added: list[list[datetime.datetime]] = []
    held: set[int] = set()
    for snapshot in hourly.iceberg_table.snapshots():
        rows = hourly.read_arrow_table(snapshot_id=snapshot.snapshot_id).to_pylist()
        added.append(sorted(row["at"] for row in rows if row["identity"] not in held))
        held = {row["identity"] for row in rows}
    assert [at for commit in added for at in commit] == sorted(stream.column("at").to_pylist()), (
        "each commit took the laid-out stream up where the last one left it"
    )


@pytest.mark.parametrize("verb", ["blind", "append", "merge", "overwrite"])
@pytest.mark.parametrize("batch_count", [4, 16])
def test_a_spilled_write_holds_one_chunk_however_long_the_stream(
    tmp_path: Path, verb: str, batch_count: int
) -> None:
    """One source batch a chunk, arriving newest first over interleaved
    partitions, so every partition's runs have to be merged back: the write
    holds what one chunk does -- 2.04 chunks measured for every verb --
    whether the stream carries four chunks or sixteen."""
    rows, partitions = 20_000, 4
    catalog = IcebergCatalog(name="spilled", properties=catalog_properties(tmp_path))
    target = catalog.dataset("t.bounded", field=Bounded.into_field())
    target.append_arrow_table(
        pyarrow.Table.from_batches([bounded_batch(2 * batch_count, rows, partitions).slice(0, 8)])
    )
    chunk = bounded_batch(0, rows, partitions).nbytes
    reader = pyarrow.RecordBatchReader.from_batches(
        Bounded.into_field().into_arrow_schema(),
        (bounded_batch(index, rows, partitions) for index in reversed(range(batch_count))),
    )

    with peak_arrow_memory() as peak:
        if verb == "blind":
            target.append_arrow_reader(reader, merge_by=False, commit_batch_num=1)
        elif verb == "append":
            target.append_arrow_reader(reader, commit_batch_num=1)
        elif verb == "merge":
            target.merge_arrow_reader(reader, commit_batch_num=1)
        else:
            target.overwrite_arrow_reader(reader, commit_batch_num=1)
        held = peak()

    kept = 0 if verb == "overwrite" else 8
    assert target.read_arrow_table().num_rows == rows * batch_count + kept
    assert held < 2.5 * chunk, f"{held / chunk:.2f} chunks held for a {batch_count}-chunk {verb}"


# -- the partition key bounds a keyed append plans by ------------------------


def test_the_partition_key_bounds_bound_each_partition_by_its_own_keys() -> None:
    """One range per partition, and not one over the chunk: a key of one day
    inside another day's range, or any key of a day between, is not admitted,
    where one range over the chunk admits all three."""
    from pyiceberg.expressions import Or

    field = Quote.into_field()
    first, between, last = (datetime.date(2026, 8, day) for day in (14, 15, 16))

    def rows(*pairs: tuple[str, datetime.date]) -> pyarrow.Table:
        return pyarrow.Table.from_pydict(
            {
                "symbol": [symbol for symbol, _ in pairs],
                "day": [day for _, day in pairs],
                "size": [0] * len(pairs),
                "venue": ["XPAR"] * len(pairs),
            },
            schema=field.into_arrow_schema(),
        )

    chunk = rows(("A", first), ("C", first), ("X", last), ("Z", last))
    days = [_PartitionColumn("day", "day", IdentityTransform().pyarrow_transform(DateType()))]
    bounds = _partition_key_bounds(chunk, days, {"day": None}, ["symbol"])

    assert isinstance(bounds, Or)
    assert _covers(bounds, chunk, field), "bounds that miss a key duplicate it"
    outside = rows(("Y", first), ("B", last), ("B", between))
    assert _admitted(bounds, outside, field) == 0
    assert _admitted(_key_bounds(chunk, ["day", "symbol"]), outside, field) == 3


def test_the_partition_key_bounds_name_a_null_partition_as_null() -> None:
    from pyiceberg.expressions import IsNull

    field = OptionalVenue.into_field()
    chunk = optional_venue(("new", None), ("old", None), ("kept", "XPAR"))
    venues = [
        _PartitionColumn("venue", "venue", IdentityTransform().pyarrow_transform(StringType()))
    ]
    bounds = _partition_key_bounds(chunk, venues, {"venue": None}, ["symbol"])

    assert repr(IsNull("venue")) in str(bounds)
    assert _covers(bounds, chunk, field)
    assert _admitted(bounds, optional_venue(("kept", None), ("new", "XETR")), field) == 0


def test_the_partition_key_bounds_widen_a_time_partition_to_its_whole_unit() -> None:
    """Any instant of an hour the chunk carries is admitted, for a key in that
    hour's range; nothing of an hour it does not carry is."""
    field = HourlyTimed.into_field()
    ten = datetime.datetime(2026, 8, 14, 10, tzinfo=UTC)
    minute, hour = datetime.timedelta(minutes=1), datetime.timedelta(hours=1)
    chunk = hourly_rows((1, ten + 5 * minute), (2, ten + 10 * minute), (3, ten + 2 * hour))
    hours = [
        _PartitionColumn("at_hour", "at", HourTransform().pyarrow_transform(TimestamptzType()))
    ]
    bounds = _partition_key_bounds(chunk, hours, {"at": "hour"}, ["identity"])

    last = ten + hour - datetime.timedelta(microseconds=1)
    assert _covers(bounds, hourly_rows((1, ten), (2, last), (3, ten + 3 * hour - minute)), field)
    outside = hourly_rows(
        (2, ten + hour),
        (3, ten + hour + 30 * minute),
        (3, ten + 3 * hour),
        (1, ten - datetime.timedelta(microseconds=1)),
        (3, ten + 30 * minute),
    )
    assert _admitted(bounds, outside, field) == 0


def test_the_partition_key_bounds_bind_over_thousands_of_partitions() -> None:
    """One `Or` term per partition, as a balanced tree: two thousand hours
    bind without reaching the interpreter's recursion limit."""
    field = HourlyTimed.into_field()
    count = 2_000
    identities = pyarrow.array(range(count), pyarrow.int64())
    instants = pyarrow.compute.multiply(identities, 3_600_000_000).cast(
        pyarrow.timestamp("us", tz="UTC")
    )
    chunk = pyarrow.Table.from_arrays([identities, instants], schema=field.into_arrow_schema())
    hours = [
        _PartitionColumn("at_hour", "at", HourTransform().pyarrow_transform(TimestamptzType()))
    ]
    bounds = _partition_key_bounds(chunk, hours, {"at": "hour"}, ["identity"])

    assert _covers(bounds, chunk, field)
    assert str(bounds).count("GreaterThanOrEqual") == 2 * count, "a source and a key term each"
    shifted = chunk.set_column(0, "identity", pyarrow.compute.add(identities, 1))
    assert _admitted(bounds, shifted, field) == 0, "each hour admits its own key alone"


def test_a_column_no_bound_can_be_spelled_for_widens_its_partition_safely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A boolean or a nanosecond key contributes no term to its partition, a
    partition left with none plans everything, a chunk under no partition is
    bounded as a whole, and a kernel that refuses the grouping answers the
    chunk's own bounds: wider, never narrower."""
    from rekep.iceberg.dataset import _always_true

    nanos = pyarrow.chunked_array(
        [pyarrow.array([1_700_000_000_000_000_000 + i for i in range(6)], pyarrow.int64())]
    ).cast(pyarrow.timestamp("ns"))
    chunk = pyarrow.Table.from_arrays(
        [
            nanos,
            pyarrow.array([True, False] * 3),
            pyarrow.array(list(range(6))),
            pyarrow.array(["a", "a", "a", "b", "b", "b"]),
        ],
        names=["at", "flag", "seq", "part"],
    )
    parts = [_PartitionColumn("part", "part", IdentityTransform().pyarrow_transform(StringType()))]
    buckets = [
        _PartitionColumn("part_bucket", "part", BucketTransform(4).pyarrow_transform(StringType()))
    ]

    flagged = str(_partition_key_bounds(chunk, parts, {"part": None}, ["flag", "at"]))
    assert "part" in flagged and "flag" not in flagged and "'at'" not in flagged
    assert _partition_key_bounds(chunk, buckets, {}, ["flag", "at"]) == _always_true()
    assert _partition_key_bounds(chunk, None, {}, ["seq"]) == _key_bounds(chunk, ["seq"])

    def refused(*_args: Any, **_kwargs: Any) -> None:
        raise pyarrow.ArrowNotImplementedError("no grouped kernel")

    monkeypatch.setattr(pyarrow.TableGroupBy, "aggregate", refused)
    assert _partition_key_bounds(chunk, parts, {"part": None}, ["seq"]) == _key_bounds(
        chunk, ["part", "seq"]
    )
