"""`IcebergDataset` against a real, fully local catalog: SQLite and a file warehouse."""

import dataclasses
import datetime
import math
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Any

import pyarrow
import pyarrow.fs
import pyarrow.parquet
import pytest
from pyiceberg.conversions import from_bytes
from pyiceberg.expressions import EqualTo
from pyiceberg.transforms import IdentityTransform

from rekep import Convertible, Field, FixMsg, Kwarg, Message, StructField, scalar
from rekep.arrow_file_io import ArrowFileIO
from rekep.fix import Party
from rekep.iceberg import IcebergCatalog, IcebergDataset
from rekep.iceberg.dataset import MERGE_IN_LIMIT
from rekep.iceberg.fields import INFERRED_METRICS
from rekep.market import EventType

from ..conftest import catalog_properties

pytestmark = pytest.mark.integration


@scalar
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, Field.primary_key()]
    """Instrument."""

    day: Annotated[datetime.date, Field.partition_key()]
    """Trading day."""

    size: int
    """Quantity."""

    venue: str | None = None
    """Where it traded, when known."""


class CustomArrowFileIO(ArrowFileIO):
    """A distinct configured FileIO that keeps Windows URI handling."""


def local(location: str) -> Path:
    """The directory behind a `file:` location, on any OS.

    Stripping `file://` by hand leaves `/C:/...` on Windows, which is not a
    path anything opens. `pyarrow.fs` owns the URI rules the store writes
    with, so it decides here too.
    """
    return Path(pyarrow.fs.FileSystem.from_uri(location)[1])


@pytest.fixture
def dataset(tmp_path: Path) -> IcebergDataset:
    return IcebergDataset(
        field=Quote.into_field("trading.quotes"),
        catalog="test",
        properties=catalog_properties(tmp_path),
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


@pytest.fixture
def logs(tmp_path: Path) -> IcebergDataset:
    """The parser's own shape, which is the widest thing this package stores."""
    return IcebergDataset(
        field=FixMsg.into_field("trading.logs"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )


#: One wire FIX order as the parser lands it: the session layer and the fields
#: the message carries once flattened into columns of their own, and the party
#: repeating party group extracted as a list of structured entries.
FIX_LINE = FixMsg(
    source_url="a.txt",
    unix=1_786_665_901_167_520_000,
    hash=3,
    xhash=3,
    code="ORD-1",
    etype=EventType.ORDER,
    thread_name="t",
    plugin_code="d",
    message="sending 8=FIX.4.2|9=176|35=D|34=7|49=BUYSIDE|56=XPAR|11=ORD-1|55=TTF|10=203|",
    protocol_code="FIX",
    kwargs=[],
    Parties=[
        Party(PartyID="BUYSIDE", PartyIDSource="D", PartyRole=1),
        Party(PartyID="XPAR", PartyIDSource="G", PartyRole=17),
    ],
    BeginString="FIX.4.2",
    BodyLength=176,
    MsgType="D",
    MsgSeqNum=7,
    SenderCompID="BUYSIDE",
    TargetCompID="XPAR",
    SendingTime=datetime.datetime(2026, 8, 14, 9, 30, 0, 123000, tzinfo=datetime.UTC),
    PossDupFlag=True,
    Signature=b"\x00sealed",
    CheckSum="203",
    Symbol="TTF",
    ClOrdID="ORD-1",
    Side="1",
    OrderQty=1200.0,
    TransactTime=datetime.datetime(2026, 8, 14, 9, 30, tzinfo=datetime.UTC),
    Text="all good",
)


def log_table(*rows: FixMsg) -> pyarrow.Table:
    """Lines in the Python shape Arrow storage expects."""
    return pyarrow.Table.from_pylist(
        [dataclasses.asdict(row) for row in rows], FixMsg.into_field().into_arrow_schema()
    )


# -- creating -------------------------------------------------------------


def test_a_write_creates_the_table_from_the_declared_shape(dataset: IcebergDataset) -> None:
    assert not dataset.exists
    dataset.append_arrow_table(quotes(3))
    assert dataset.exists

    schema = dataset.iceberg_table.schema()
    assert [f.name for f in schema.fields] == Quote.into_field().names
    assert schema.find_field("symbol").doc == "Instrument.", "the docs land as column comments"
    assert schema.identifier_field_ids == [schema.find_field("symbol").field_id]
    assert [f.name for f in dataset.iceberg_table.spec().fields] == ["day"]


def test_a_streamed_polars_write_matches_arrow_and_keeps_partition_pruning(
    dataset: IcebergDataset,
) -> None:
    polars = pytest.importorskip("polars")
    first = datetime.date(2026, 8, 14)
    second = first + datetime.timedelta(days=1)
    expected = pyarrow.Table.from_pydict(
        {
            "symbol": ["A", "B", "C", "D"],
            "day": [first, first, second, second],
            "size": [1, 2, 3, 4],
            "venue": [None, "XPAR", None, "XAMS"],
        },
        schema=Quote.into_field().into_arrow_schema(),
    )
    source = polars.DataFrame(expected.to_pydict()).lazy()
    dataset.append_polars(source, batch_row_size=2, commit_row_size=2)

    stored = dataset.read_arrow_table(Quote.into_field()).sort_by("symbol")
    assert stored.schema.equals(Quote.into_field().into_arrow_schema())
    assert stored.equals(expected)
    plan = dataset.scan_plan("day = '2026-08-14'")
    assert plan["files"] == 1 and plan["total_files"] == 2 and plan["skipped"] == 1


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
        field=Quote.into_field("trading.quiet"),
        catalog="test",
        properties=catalog_properties(tmp_path),
        table_properties={"write.metadata.metrics.column.symbol": "none"},
    )
    dataset.get_or_create_table()
    assert dataset.iceberg_table.properties["write.metadata.metrics.column.symbol"] == "none"


def test_creating_is_idempotent(dataset: IcebergDataset) -> None:
    first = dataset.get_or_create_table()
    assert dataset.get_or_create_table().name() == first.name()


# -- what it holds --------------------------------------------------------


def test_the_declared_shape_wins(dataset: IcebergDataset) -> None:
    assert dataset.into_struct_field() is Quote.into_field("trading.quotes")
    assert dataset.name == "trading.quotes"
    assert dataset.namespace == "trading"


def test_the_tables_own_shape_is_read_back(dataset: IcebergDataset, tmp_path: Path) -> None:
    dataset.append_arrow_table(quotes(1))
    found = IcebergCatalog(name="test", properties=catalog_properties(tmp_path)).dataset(
        dataset.name
    )
    shape = found.into_struct_field()
    assert shape.name == dataset.name
    assert shape.names == Quote.into_field().names
    assert shape.primary_keys() == ["symbol"]
    assert shape.partition_keys() == {"day": "identity"}
    assert shape.field("symbol").description == "Instrument."
    assert not shape.field("size").nullable and shape.field("venue").nullable


# -- reading and writing --------------------------------------------------


def test_a_table_that_was_never_written_reads_as_no_rows(dataset: IcebergDataset) -> None:
    """The first interval of a pipeline reads upstreams that do not exist yet."""
    assert not dataset.exists
    assert dataset.read_arrow_table().num_rows == 0
    assert dataset.read_arrow_table().schema.names == Quote.into_field().leaf_names()
    filtered = dataset.read_arrow_reader(Quote.into_field(), row_filter=EqualTo("symbol", "X"))
    assert not list(filtered), "a filter on an absent table is answered, not refused"
    assert not dataset.exists, "reading does not create it"


def test_an_absent_table_reads_under_the_schema_it_was_asked_for(tmp_path: Path) -> None:
    bare = IcebergDataset(
        field=Quote.into_field("trading.absent"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    reader = bare.read_arrow_reader(Quote.into_field(), columns=["symbol", "size"])
    assert reader.schema.names == ["symbol", "size"]
    assert reader.read_all().num_rows == 0
    assert bare.read_arrow_table().num_rows == 0


def test_rows_go_in_and_come_back(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(5))
    assert dataset.read_arrow_table().num_rows == 5


def test_a_read_without_a_schema_is_the_stores_own(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    reader = dataset.read_arrow_reader()
    assert reader.schema.field("symbol").type == pyarrow.large_string(), "no conversion is paid"


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
class Timed(Convertible):
    """One row ordered by its event clock."""

    unix: Annotated[int, Field.primary_key(), Field.sort_key()]
    """Event time."""

    payload: str = "x"
    """Payload."""


@scalar
class DescendingTimed(Convertible):
    """One row ordered newest first."""

    unix: Annotated[int, Field.primary_key(), Field.sort_key("desc")]


@scalar
class RepeatedTimed(Convertible):
    """One event whose sort clock may be shared by several identities."""

    seq: Annotated[int, Field.primary_key()]
    """Stable identity."""

    unix: Annotated[int, Field.sort_key()]
    """Event time."""


@scalar
class PartitionedTimed(Convertible):
    """One event in a dated, time-sorted stream."""

    day: Annotated[datetime.date, Field.primary_key(), Field.partition_key()]
    """Trading day."""

    unix: Annotated[int, Field.primary_key(), Field.sort_key()]
    """Event time."""

    payload: str
    """Observable source row."""


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
        ordered.append_arrow_table(timed(*values), commit_row_size=0)

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
    dataset.append_arrow_table(repeated_timed(7, 7), commit_row_size=0)

    found = dataset.read_arrow_reader(order_by="unix").read_all()

    assert found.column("seq").to_pylist() == [0, 1]


def test_closing_a_partial_ordered_limit_releases_the_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rekep.iceberg import dataset as module

    catalog = IcebergCatalog(name="partial", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("trading.partial_timed", field=Timed.into_field())
    dataset.append_arrow_table(timed(0, 1, 2, 3), commit_row_size=0)
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
        ordered.append_arrow(partitioned_timed(day, *values), commit_row_size=0)

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


@scalar
class Sequenced(Convertible):
    """One event ordered by clock and its source sequence."""

    unix: Annotated[int, Field.primary_key(), Field.sort_key()]
    """Event time."""

    seq: Annotated[int, Field.primary_key(), Field.sort_key()]
    """Source order among events at the same time."""

    payload: str
    """State transition used to make tie order observable."""


@scalar
class NullableSequence(Convertible):
    """One event whose source may not provide a sequence."""

    unix: Annotated[int, Field.primary_key()]
    """Event time."""

    seq: int | None
    """Optional source sequence."""

    hash: Annotated[int, Field.primary_key()]
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
            commit_row_size=0,
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


def test_a_nullable_secondary_key_is_null_last_then_uses_the_stable_fallback(
    tmp_path: Path,
) -> None:
    catalog = IcebergCatalog(name="nullable-sequence", properties=catalog_properties(tmp_path))
    ordered = catalog.dataset(
        "trading.nullable_sequence",
        field=NullableSequence.into_field(),
        sort_by=["unix", "seq", "hash"],
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
            commit_row_size=0,
        )

    found = ordered.read_arrow_reader(order_by=("unix", "seq", "hash")).read_all()

    assert found.column("payload").to_pylist() == [
        "first",
        "second",
        "unknown-1",
        "unknown-2",
    ]


def test_a_nearly_right_batch_is_cast_on_the_way_in(dataset: IcebergDataset) -> None:
    """Wrong order, a narrow integer, a column the source never produced."""
    dataset.get_or_create_table()
    batch = pyarrow.RecordBatch.from_pydict(
        {
            "day": [datetime.date(2026, 8, 14)],
            "size": pyarrow.array([7], type=pyarrow.int16()),
            "symbol": ["A"],
            "noise": ["dropped"],
        }
    )
    dataset.append_arrow_reader(iter([batch]))
    stored = dataset.read_arrow_table(Quote.into_field())
    assert stored.column("size").to_pylist() == [7]
    assert stored.column("venue").to_pylist() == [None], "the missing nullable column was filled"


def test_commit_row_size_commits_one_snapshot_per_chunk(dataset: IcebergDataset) -> None:
    dataset.append_arrow_reader(quotes(6).to_reader(max_chunksize=1), commit_row_size=2)
    assert len(dataset.iceberg_table.history()) == 3
    assert dataset.read_arrow_table().num_rows == 6


def test_an_empty_stream_commits_nothing(dataset: IcebergDataset) -> None:
    dataset.get_or_create_table()
    dataset.append_arrow_reader(iter(()))
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


@pytest.mark.parametrize("verb", ["append", "overwrite"])
def test_a_completed_stream_write_closes_its_source(dataset: IcebergDataset, verb: str) -> None:
    source = _ClosableBatches(quotes(2).to_batches(max_chunksize=1))

    getattr(dataset, f"{verb}_arrow_reader")(source, merge_by=True, commit_row_size=1)

    assert source.closed


@pytest.mark.parametrize(
    ("verb", "chunk_method"),
    [("append", "insert_arrow_table"), ("overwrite", "merge_arrow_table")],
)
def test_a_failed_stream_write_closes_its_source(
    dataset: IcebergDataset,
    monkeypatch: pytest.MonkeyPatch,
    verb: str,
    chunk_method: str,
) -> None:
    dataset.get_or_create_table()
    source = _ClosableBatches(quotes(2).to_batches(max_chunksize=1))

    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("commit stopped")

    monkeypatch.setattr(dataset, chunk_method, fail)
    with pytest.raises(RuntimeError, match="commit stopped"):
        getattr(dataset, f"{verb}_arrow_reader")(source, merge_by=True, commit_row_size=1)

    assert source.closed


@pytest.mark.parametrize("merge_by", [False, True])
def test_a_direct_partition_write_closes_its_source_once(
    dataset: IcebergDataset, merge_by: bool
) -> None:
    source = _ClosableBatches(quotes(2).to_batches(max_chunksize=1))

    dataset.overwrite_partition_arrow_reader(source, merge_by=merge_by, commit_row_size=1)

    assert source.close_calls == 1


def _observed_staging(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> tuple[list[Path], set[str], list[str]]:
    """Local stages, uploaded targets, and target Parquet footer reopens."""
    io = dataset.get_or_create_table().io
    original_copy = io.copy_from_local
    original_input = io.new_input
    staged: list[Path] = []
    uploaded: set[str] = set()
    reopened: list[str] = []

    def copied(local_path: str, target: str) -> Any:
        path = Path(local_path)
        assert path.exists() and path.stat().st_size > 0
        staged.append(path)
        result = original_copy(local_path, target)
        uploaded.add(target)
        return result

    def opened(location: str) -> Any:
        if location in uploaded:
            reopened.append(location)
        return original_input(location)

    monkeypatch.setattr(io, "copy_from_local", copied)
    monkeypatch.setattr(io, "new_input", opened)
    return staged, uploaded, reopened


def _iceberg_artifacts(dataset: IcebergDataset) -> set[Path]:
    root = local(dataset.get_or_create_table().location())
    return {
        path.resolve()
        for path in root.rglob("*")
        if path.is_file()
        and (path.suffix in {".avro", ".parquet"} or path.name.endswith(".metadata.json"))
    }


def test_partitioned_insert_paths_stage_locally_without_reopening_targets(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    with monkeypatch.context() as observed:
        staged, uploaded, reopened = _observed_staging(dataset, observed)
        assert dataset.append_arrow_table(quotes(2)) == 2
        after_plain_append = len(staged)
        assert uploaded and reopened == []
        uploaded.clear()
        assert dataset.append_arrow_table(quotes(3), merge_by=True) == 1
        after_insert_merge = len(staged)
        assert uploaded and reopened == []
        uploaded.clear()
        dataset.overwrite_arrow_table(keyed("N", 2), merge_by=True)
        after_new_key_merge = len(staged)
        assert uploaded and reopened == []

    assert 0 < after_plain_append < after_insert_merge < after_new_key_merge
    assert not any(path.exists() for path in staged)
    assert {row["symbol"] for row in dataset.read_arrow_table().to_pylist()} == {
        "S0",
        "S1",
        "S2",
        "N0",
        "N1",
    }


def test_partitioned_merge_stages_both_updates_and_inserts(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset.append_arrow_table(quotes(2))
    changed = quotes(3, "XETR")

    with monkeypatch.context() as observed:
        staged, uploaded, reopened = _observed_staging(dataset, observed)
        dataset.overwrite_arrow_table(
            changed,
            merge_by=True,
            properties={"rekep.test": "staged-merge"},
        )

    assert len(staged) == len(uploaded) == 2, "updates and inserts are separate local stages"
    assert reopened == []
    assert not any(path.exists() for path in staged)
    assert stored_sizes(dataset) == {"S0": 0, "S1": 1, "S2": 2}
    assert set(dataset.read_arrow_table().column("venue").to_pylist()) == {"XETR"}


def test_a_failed_partitioned_append_removes_its_uploaded_stage(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table.update.snapshot import _FastAppendFiles

    io = dataset.get_or_create_table().io
    staged, uploaded, _ = _observed_staging(dataset, monkeypatch)

    def refused(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("snapshot refused")

    monkeypatch.setattr(_FastAppendFiles, "append_data_file", refused)
    with pytest.raises(RuntimeError, match="snapshot refused"):
        dataset.append_arrow_table(quotes(2))

    assert staged and not any(path.exists() for path in staged)
    assert uploaded and all(not io.new_input(path).exists() for path in uploaded)
    assert dataset.iceberg_table.history() == []


def test_a_lost_upload_acknowledgement_retries_target_cleanup(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    io = dataset.get_or_create_table().io
    copy = io.copy_from_local
    targets: set[str] = set()

    def copied_then_lost(local_path: str, target: str) -> None:
        copy(local_path, target)
        targets.add(target)
        raise OSError("upload acknowledgement lost")

    monkeypatch.setattr(io, "copy_from_local", copied_then_lost)
    with pytest.raises(OSError, match="acknowledgement lost"):
        dataset.append_arrow_table(quotes(2))

    assert targets and all(not io.new_input(path).exists() for path in targets)
    assert dataset.iceberg_table.history() == []


def test_a_refused_catalog_commit_removes_unreferenced_stages(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    io = dataset.get_or_create_table().io
    staged, uploaded, _ = _observed_staging(dataset, monkeypatch)

    def refused(_transaction: Transaction) -> None:
        raise RuntimeError("catalog refused")

    monkeypatch.setattr(Transaction, "commit_transaction", refused)
    with pytest.raises(RuntimeError, match="catalog refused"):
        dataset.append_arrow_table(quotes(2))

    assert staged and not any(path.exists() for path in staged)
    assert uploaded and all(not io.new_input(path).exists() for path in uploaded)
    assert dataset.iceberg_table.history() == []


def test_a_lost_commit_acknowledgement_keeps_files_the_snapshot_references(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    io = dataset.get_or_create_table().io
    staged, uploaded, _ = _observed_staging(dataset, monkeypatch)
    original = Transaction.commit_transaction

    def acknowledged(transaction: Transaction) -> None:
        original(transaction)
        raise RuntimeError("acknowledgement lost")

    monkeypatch.setattr(Transaction, "commit_transaction", acknowledged)
    with pytest.raises(RuntimeError, match="acknowledgement lost"):
        dataset.append_arrow_table(quotes(2))

    assert staged and not any(path.exists() for path in staged)
    assert uploaded and all(io.new_input(path).exists() for path in uploaded)
    assert dataset.refresh().read_arrow_table().num_rows == 2


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
        dataset.append_arrow_table(quotes(2), commit_row_size=0)

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
        dataset.append_arrow_table(quotes(2), commit_row_size=0)

    assert _iceberg_artifacts(dataset) == before


def test_a_refused_partitioned_merge_removes_rewrites_and_avro(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    dataset.append_arrow_table(quotes(2), commit_row_size=0)
    before = _iceberg_artifacts(dataset)

    def refused(_transaction: Transaction) -> None:
        raise RuntimeError("catalog refused")

    monkeypatch.setattr(Transaction, "commit_transaction", refused)
    with pytest.raises(RuntimeError, match="catalog refused"):
        dataset.overwrite_arrow_table(quotes(1, "XETR"), merge_by=True, commit_row_size=0)

    assert _iceberg_artifacts(dataset) == before
    assert set(dataset.refresh().read_arrow_table().column("venue").to_pylist()) == {"XPAR"}


def test_a_refused_unpartitioned_merge_removes_rewrites_and_avro(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    @scalar
    class FlatQuote(Convertible):
        symbol: Annotated[str, Field.primary_key()]
        size: int

    catalog = IcebergCatalog(name="flat-refusal", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.flat_refusal", field=FlatQuote.into_field())
    schema = FlatQuote.into_field().into_arrow_schema()
    dataset.append_arrow_table(
        pyarrow.Table.from_pydict({"symbol": ["S0", "S1"], "size": [0, 1]}, schema=schema),
        commit_row_size=0,
    )
    before = _iceberg_artifacts(dataset)

    def refused(_transaction: Transaction) -> None:
        raise RuntimeError("catalog refused")

    monkeypatch.setattr(Transaction, "commit_transaction", refused)
    with pytest.raises(RuntimeError, match="catalog refused"):
        dataset.overwrite_arrow_table(
            pyarrow.Table.from_pydict({"symbol": ["S0"], "size": [2]}, schema=schema),
            merge_by=True,
            commit_row_size=0,
        )

    assert _iceberg_artifacts(dataset) == before
    assert dataset.refresh().read_arrow_table().column("size").to_pylist() == [0, 1]


def test_a_metadata_write_followed_by_catalog_refusal_leaves_no_artifact(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = dataset.iceberg_catalog
    dataset.append_arrow_table(quotes(2), commit_row_size=0)
    before = _iceberg_artifacts(dataset)
    write = catalog._write_metadata

    def wrote_then_refused(*args: Any, **kwargs: Any) -> None:
        write(*args, **kwargs)
        raise RuntimeError("catalog refused after metadata")

    monkeypatch.setattr(catalog, "_write_metadata", wrote_then_refused)
    with pytest.raises(RuntimeError, match="refused after metadata"):
        dataset.overwrite_arrow_table(quotes(1, "XETR"), merge_by=True, commit_row_size=0)

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
    dataset.append_arrow_table(quotes(2), commit_row_size=0)
    before = _iceberg_artifacts(dataset)
    write = catalog.catalog._write_metadata

    def wrote_then_refused(*args: Any, **kwargs: Any) -> None:
        write(*args, **kwargs)
        raise RuntimeError("custom catalog refused after metadata")

    monkeypatch.setattr(catalog.catalog, "_write_metadata", wrote_then_refused)
    with pytest.raises(RuntimeError, match="refused after metadata"):
        dataset.overwrite_arrow_table(quotes(1, "XETR"), merge_by=True, commit_row_size=0)

    assert _iceberg_artifacts(dataset) == before
    assert set(dataset.refresh().read_arrow_table().column("venue").to_pylist()) == {"XPAR"}


def test_a_refused_manifest_merge_removes_superseded_avro(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    dataset.set_properties({"commit.manifest.min-count-to-merge": "2"})
    dataset.append_arrow_table(quotes(1), commit_row_size=0)
    before = _iceberg_artifacts(dataset)

    def refused(_transaction: Transaction) -> None:
        raise RuntimeError("catalog refused")

    monkeypatch.setattr(Transaction, "commit_transaction", refused)
    with pytest.raises(RuntimeError, match="catalog refused"):
        dataset.append_arrow_table(other_day(1), commit_row_size=0)

    assert _iceberg_artifacts(dataset) == before


def test_a_snapshot_construction_failure_removes_direct_writer_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table.update.snapshot import _FastAppendFiles

    @scalar
    class FlatQuote(Convertible):
        symbol: Annotated[str, Field.primary_key()]
        size: int

    catalog = IcebergCatalog(name="flat-build", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.flat_build", field=FlatQuote.into_field())
    schema = FlatQuote.into_field().into_arrow_schema()
    dataset.append_arrow_table(
        pyarrow.Table.from_pydict({"symbol": ["S0", "S1"], "size": [0, 1]}, schema=schema),
        commit_row_size=0,
    )
    before = _iceberg_artifacts(dataset)

    def refused(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("snapshot construction refused")

    monkeypatch.setattr(_FastAppendFiles, "append_data_file", refused)
    with pytest.raises(RuntimeError, match="construction refused"):
        dataset.overwrite_arrow_table(
            pyarrow.Table.from_pydict({"symbol": ["S0"], "size": [2]}, schema=schema),
            merge_by=True,
            commit_row_size=0,
        )

    assert _iceberg_artifacts(dataset) == before


# -- merging --------------------------------------------------------------


def test_merge_by_true_upserts_on_the_declared_key(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(3, "XPAR"))
    dataset.overwrite_arrow_table(quotes(3, "XETR"), merge_by=True)
    stored = dataset.read_arrow_table()
    assert stored.num_rows == 3, "the same keys came back, not three more rows"
    assert set(stored.column("venue").to_pylist()) == {"XETR"}


def test_merge_by_names_upserts_on_those(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2, "XPAR"))
    dataset.overwrite_arrow_table(quotes(2, "XETR"), merge_by=["symbol", "day"])
    assert dataset.read_arrow_table().num_rows == 2


def test_a_falsy_merge_by_replaces_complete_partitions_from_a_stream(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(quotes(3))
    dataset.append_arrow_table(other_day(2))
    replacement = keyed("N", 5)

    dataset.overwrite_arrow_reader(
        replacement.to_reader(max_chunksize=1), merge_by=False, commit_row_size=2
    )

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


def test_partition_staging_uses_local_disk_then_cleans_it(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.append_arrow_table(other_day(2))
    source = pyarrow.Table.from_batches([*keyed("N", 3).to_batches(), *other_day(2).to_batches()])
    staged: list[Path] = []
    uploaded: set[str] = set()
    reopened: list[str] = []
    io = dataset.iceberg_table.io
    original_copy = io.copy_from_local
    original_input = io.new_input
    before = len(dataset.iceberg_table.history())

    def copied(local_path: str, target: str) -> str:
        path = Path(local_path)
        assert path.exists() and path.stat().st_size > 0
        staged.append(path)
        result = original_copy(local_path, target)
        uploaded.add(target)
        return result

    def opened(location: str):
        if location in uploaded:
            reopened.append(location)
        return original_input(location)

    monkeypatch.setattr(io, "copy_from_local", copied)
    monkeypatch.setattr(io, "new_input", opened)
    with monkeypatch.context() as no_concat:
        no_concat.setattr(
            pyarrow,
            "concat_tables",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("collected")),
        )
        dataset.overwrite_arrow_reader(
            source.to_reader(max_chunksize=1),
            merge_by=False,
            commit_row_size=2,
            properties={"rekep.test": "staged"},
        )

    assert staged and not any(path.exists() for path in staged)
    assert reopened == [], "the local footer already supplied every DataFile metric"
    assert len(dataset.iceberg_table.history()) - before == 2, "one snapshot per bounded commit"
    assert all(
        snapshot.summary["rekep.test"] == "staged"
        for snapshot in dataset.iceberg_table.snapshots()[-2:]
    )
    monkeypatch.setattr(io, "new_input", original_input)
    files = dataset.data_files().to_pylist()
    assert len(files) == 3
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
    uploaded: list[str] = []
    io = dataset.iceberg_table.io
    original_copy = io.copy_from_local

    def copied(local_path: str, target: str) -> str:
        uploaded.append(target)
        return original_copy(local_path, target)

    def refused(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("catalog refused")

    monkeypatch.setattr(io, "copy_from_local", copied)
    monkeypatch.setattr(_OverwriteFiles, "append_data_file", refused)
    with pytest.raises(RuntimeError, match="catalog refused"):
        dataset.overwrite_arrow_reader(
            quotes(3).to_reader(max_chunksize=1), merge_by=False, commit_row_size=2
        )

    assert uploaded
    assert all(not io.new_input(path).exists() for path in uploaded)
    assert {row["file_path"] for row in dataset.refresh().data_files().to_pylist()} == before


def test_partition_cleanup_attempts_every_upload_without_masking_the_source_error(
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
            stager.uploaded.update({"first.parquet", "second.parquet"})
            raise RuntimeError("source failed")
    assert set(attempted) == {"first.parquet", "second.parquet"}

    attempted.clear()
    with pytest.raises(ExceptionGroup, match="partition staging cleanup failed") as caught:
        with _PartitionStager(table, (), 1) as stager:
            stager.uploaded.update({"first.parquet", "second.parquet"})
    assert set(attempted) == {"first.parquet", "second.parquet"}
    assert len(caught.value.exceptions) == 2


def test_an_interleaved_partition_stream_is_refused_before_its_pending_commit(
    dataset: IcebergDataset,
) -> None:
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

    with pytest.raises(ValueError, match="recurs after another partition"):
        dataset.overwrite_arrow_reader(
            replacement.to_reader(max_chunksize=1), merge_by=False, commit_row_size=0
        )

    assert len(dataset.iceberg_table.history()) == before
    assert {row["symbol"] for row in dataset.read_arrow_table().to_pylist()} == {
        "S0",
        "S1",
        "D0",
        "D1",
    }


def test_an_ordering_error_keeps_earlier_completed_partition_commits(
    dataset: IcebergDataset,
) -> None:
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

    with pytest.raises(ValueError, match="recurs after another partition"):
        dataset.overwrite_arrow_reader(
            replacement.to_reader(max_chunksize=1), merge_by=False, commit_row_size=1
        )

    assert len(dataset.iceberg_table.history()) > before
    stored = dataset.read_arrow_table().to_pylist()
    assert {row["symbol"] for row in stored if row["day"] == day} == {"A"}
    assert {row["symbol"] for row in stored if row["day"] == following} == {"D0", "D1"}


def test_a_failed_source_does_not_replace_its_partial_partition(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    before = len(dataset.iceberg_table.history())

    def broken():
        yield from keyed("N", 1).to_batches()
        raise RuntimeError("source stopped")

    with pytest.raises(RuntimeError, match="source stopped"):
        dataset.overwrite_arrow_reader(broken(), merge_by=False, commit_row_size=1)

    assert len(dataset.iceberg_table.history()) == before
    assert {row["symbol"] for row in dataset.read_arrow_table().to_pylist()} == {"S0", "S1"}


def test_a_falsy_merge_by_is_still_refused_without_partitions(tmp_path: Path) -> None:
    @scalar
    class Flat(Convertible):
        symbol: Annotated[str, Field.primary_key()]

    flat = IcebergDataset(
        field=Flat.into_field("trading.flat_overwrite"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    source = pyarrow.Table.from_pydict(
        {"symbol": ["A"]}, schema=Flat.into_field().into_arrow_schema()
    )
    flat.append_arrow_table(source)
    with pytest.raises(ValueError, match="names nothing to match on"):
        flat.overwrite_arrow_table(source, merge_by=False)


def test_a_nan_identity_partition_is_refused_before_pyiceberg(tmp_path: Path) -> None:
    @scalar
    class FloatPartition(Convertible):
        symbol: Annotated[str, Field.primary_key()]
        partition: Annotated[float, Field.partition_key()]

    values = IcebergDataset(
        field=FloatPartition.into_field("trading.float_partitions"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    source = pyarrow.Table.from_pydict(
        {"symbol": ["A"], "partition": [float("nan")]},
        schema=FloatPartition.into_field().into_arrow_schema(),
    )

    with pytest.raises(ValueError, match="partition column 'partition' contains NaN"):
        values.overwrite_arrow_table(source, merge_by=False)

    assert values.iceberg_table.history() == []


def test_a_partition_local_merge_uses_the_bounded_exact_path_and_skips_a_replay(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(quotes(3))
    dataset.append_arrow_table(other_day(2))
    before_files = {row["file_path"] for row in dataset.data_files().to_pylist()}
    incoming = pyarrow.concat_tables([quotes(1, "XETR"), keyed("N", 1)])
    dataset.overwrite_arrow_reader(
        incoming.to_reader(max_chunksize=1),
        merge_by=["symbol", "day"],
        commit_row_size=1,
    )

    stored = dataset.read_arrow_table().to_pylist()
    assert {row["symbol"] for row in stored} == {"S0", "S1", "S2", "N0", "D0", "D1"}
    assert next(row for row in stored if row["symbol"] == "S0")["venue"] == "XETR"
    after_files = {row["file_path"] for row in dataset.refresh().data_files().to_pylist()}
    assert len(before_files & after_files) == 1, "the other partition file was untouched"

    history = len(dataset.iceberg_table.history())
    dataset.overwrite_arrow_table(incoming, merge_by=["symbol", "day"])
    assert len(dataset.iceberg_table.history()) == history, "an identical replay committed nothing"


def test_a_key_that_can_move_partitions_keeps_the_exact_merge_path(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(quotes(1))
    moved = other_day(1).set_column(0, other_day(1).schema.field("symbol"), pyarrow.array(["S0"]))
    dataset.overwrite_arrow_table(moved, merge_by=True)

    stored = dataset.read_arrow_table().to_pylist()
    assert [(row["symbol"], row["day"]) for row in stored] == [("S0", datetime.date(2026, 8, 15))]


def test_a_partition_derived_from_the_primary_key_merges_exactly(
    tmp_path: Path,
) -> None:
    @scalar
    class Tick(Convertible):
        unix: Annotated[int, Field.primary_key()]
        unix_partition: Annotated[int, Field.partition_key(derived_from="unix")]
        venue: str

    ticks = IcebergDataset(
        field=Tick.into_field("trading.dynamic_ticks"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    schema = Tick.into_field().into_arrow_schema()
    ticks.append_arrow_table(
        pyarrow.Table.from_pydict(
            {"unix": [1, 2], "unix_partition": [0, 0], "venue": ["XPAR", "XPAR"]},
            schema=schema,
        )
    )
    ticks.overwrite_arrow_table(
        pyarrow.Table.from_pydict(
            {"unix": [1], "unix_partition": [0], "venue": ["XETR"]}, schema=schema
        ),
        merge_by=True,
    )

    assert {row["unix"]: row["venue"] for row in ticks.read_arrow_table().to_pylist()} == {
        1: "XETR",
        2: "XPAR",
    }


def test_staged_partition_overwrite_honours_branch_and_properties(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.create_branch("work")
    replacement = keyed("W", 2)

    dataset.overwrite_arrow_table(
        replacement,
        merge_by=False,
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
    class OptionalPartition(Convertible):
        symbol: Annotated[str, Field.primary_key()]
        venue: Annotated[str | None, Field.partition_key()] = None

    partitioned = IcebergDataset(
        field=OptionalPartition.into_field("trading.optional_partition"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    partitioned.create_with()
    source = pyarrow.RecordBatchReader.from_batches(
        pyarrow.schema([pyarrow.field("symbol", pyarrow.string(), nullable=False)]),
        [pyarrow.record_batch([["A"]], names=["symbol"])],
    )

    with pytest.raises(ValueError, match="partition columns .* missing"):
        partitioned.overwrite_arrow_reader(source, merge_by=False)


def test_a_null_partition_is_replaced_without_touching_the_others(tmp_path: Path) -> None:
    @scalar
    class OptionalPartition(Convertible):
        symbol: Annotated[str, Field.primary_key()]
        venue: Annotated[str | None, Field.partition_key()] = None

    partitioned = IcebergDataset(
        field=OptionalPartition.into_field("trading.null_partition"),
        catalog="test",
        properties=catalog_properties(tmp_path),
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

    partitioned.overwrite_arrow_reader(
        replacement.to_reader(max_chunksize=1), merge_by=False, commit_row_size=1
    )

    assert sorted(partitioned.read_arrow_table().to_pylist(), key=lambda row: row["symbol"]) == [
        {"symbol": "N0", "venue": None},
        {"symbol": "N1", "venue": None},
        {"symbol": "kept", "venue": "XPAR"},
    ]


def test_a_day_partition_is_staged_and_replaced_as_one_unit(tmp_path: Path) -> None:
    @scalar
    class Daily(Convertible):
        """One value partitioned by the day containing its timestamp."""

        code: str
        at: Annotated[datetime.datetime, Field.partition_key("day")]

    daily = IcebergDataset(
        field=Daily.into_field("trading.daily_partition_overwrite"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    schema = Daily.into_field().into_arrow_schema()
    first = datetime.datetime(2026, 8, 14, 1)
    second = datetime.datetime(2026, 8, 15, 1)
    daily.append_arrow_table(
        pyarrow.Table.from_pydict(
            {"code": ["old-a", "old-b", "kept"], "at": [first, first.replace(hour=2), second]},
            schema=schema,
        )
    )
    before = {row["file_path"] for row in daily.data_files().to_pylist()}

    daily.overwrite_arrow_table(
        pyarrow.Table.from_pydict({"code": ["new"], "at": [first.replace(hour=12)]}, schema=schema),
        merge_by=False,
    )

    assert {row["code"] for row in daily.read_arrow_table().to_pylist()} == {"new", "kept"}
    after = {row["file_path"] for row in daily.refresh().data_files().to_pylist()}
    assert len(before & after) == 1, "the untouched day keeps its data file"

    history = len(daily.iceberg_table.history())
    recurring = pyarrow.Table.from_pydict(
        {
            "code": ["first", "middle", "again"],
            "at": [first, second, first.replace(hour=20)],
        },
        schema=schema,
    )
    with pytest.raises(ValueError, match="recurs after another partition"):
        daily.overwrite_arrow_reader(
            recurring.to_reader(max_chunksize=1), merge_by=False, commit_row_size=0
        )
    assert len(daily.iceberg_table.history()) == history


def test_a_bucket_partition_is_staged_without_inverting_its_hash(tmp_path: Path) -> None:
    @scalar
    class Bucketed(Convertible):
        """One value partitioned by its four-bucket Murmur3 hash."""

        code: Annotated[str, Field.partition_key("bucket[4]")]
        size: int

    bucketed = IcebergDataset(
        field=Bucketed.into_field("trading.bucket_partition_overwrite"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    schema = Bucketed.into_field().into_arrow_schema()
    bucketed.append_arrow_table(
        pyarrow.Table.from_pydict({"code": ["aa", "ac", "ab"], "size": [1, 2, 3]}, schema=schema)
    )
    before = {row["file_path"] for row in bucketed.data_files().to_pylist()}

    bucketed.overwrite_arrow_table(
        pyarrow.Table.from_pydict({"code": ["ad"], "size": [4]}, schema=schema),
        merge_by=False,
    )

    assert {row["code"] for row in bucketed.read_arrow_table().to_pylist()} == {"ad", "ab"}
    after = {row["file_path"] for row in bucketed.refresh().data_files().to_pylist()}
    assert len(before & after) == 1, "the other hash bucket keeps its data file"

    history = len(bucketed.iceberg_table.history())
    recurring = pyarrow.Table.from_pydict(
        {"code": ["aa", "ab", "ac"], "size": [5, 6, 7]}, schema=schema
    )
    with pytest.raises(ValueError, match="recurs after another partition"):
        bucketed.overwrite_arrow_reader(
            recurring.to_reader(max_chunksize=1), merge_by=False, commit_row_size=0
        )
    assert len(bucketed.iceberg_table.history()) == history


def test_a_truncated_partition_is_staged_and_replaced_as_one_unit(tmp_path: Path) -> None:
    @scalar
    class Truncated(Convertible):
        """One value partitioned by its first two characters."""

        code: Annotated[str, Field.partition_key("truncate[2]")]
        size: int

    truncated = IcebergDataset(
        field=Truncated.into_field("trading.truncate_partition_overwrite"),
        catalog="test",
        properties=catalog_properties(tmp_path),
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
        merge_by=False,
    )

    assert {row["code"] for row in truncated.read_arrow_table().to_pylist()} == {
        "aa-new",
        "bb-kept",
    }
    after = {row["file_path"] for row in truncated.refresh().data_files().to_pylist()}
    assert len(before & after) == 1, "the other truncated prefix keeps its data file"

    history = len(truncated.iceberg_table.history())
    recurring = pyarrow.Table.from_pydict(
        {"code": ["aa-one", "bb-one", "aa-two"], "size": [5, 6, 7]}, schema=schema
    )
    with pytest.raises(ValueError, match="recurs after another partition"):
        truncated.overwrite_arrow_reader(
            recurring.to_reader(max_chunksize=1), merge_by=False, commit_row_size=0
        )
    assert len(truncated.iceberg_table.history()) == history


def test_an_empty_partition_overwrite_commits_nothing(dataset: IcebergDataset) -> None:
    dataset.create_with()
    empty = pyarrow.RecordBatchReader.from_batches(Quote.into_field().into_arrow_schema(), [])
    dataset.overwrite_arrow_reader(empty, merge_by=False)
    assert dataset.iceberg_table.history() == []


def test_a_dynamic_partition_merge_refuses_a_null_key(tmp_path: Path) -> None:
    @scalar
    class MaybeKeyed(Convertible):
        day: Annotated[datetime.date, Field.partition_key()]
        symbol: str | None = None

    target = IcebergDataset(
        field=MaybeKeyed.into_field("trading.maybe_keyed"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    source = pyarrow.Table.from_pydict(
        {"symbol": [None], "day": [datetime.date(2026, 8, 14)]},
        schema=MaybeKeyed.into_field().into_arrow_schema(),
    )

    with pytest.raises(ValueError, match="cannot be null"):
        target.overwrite_arrow_table(source, merge_by=["symbol", "day"])


def test_a_falsy_merge_by_appends(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.append_arrow_table(quotes(2), merge_by=False)
    assert dataset.read_arrow_table().num_rows == 4


def test_merging_on_a_key_nothing_declares_is_refused_before_writing(tmp_path: Path) -> None:
    @scalar
    class Loose(Convertible):
        symbol: str

    keyless = IcebergDataset(
        field=Loose.into_field("trading.loose"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    with pytest.raises(ValueError, match="no member declares one"):
        keyless.merge_columns(True)


# -- the dataset is also a document ---------------------------------------


def test_a_dataset_round_trips_through_yaml(dataset: IcebergDataset) -> None:
    """Its configuration -- the declared shape included -- is a file."""
    document = dataset.into_dict()
    assert "name" not in document
    assert document["field"]["name"] == "trading.quotes"
    rebuilt = IcebergDataset.from_yaml(dataset.into_yaml())
    assert (rebuilt.name, rebuilt.catalog, rebuilt.properties) == (
        dataset.name,
        dataset.catalog,
        dataset.properties,
    )
    assert isinstance(rebuilt.field, StructField)
    assert rebuilt.field == dataset.field
    assert rebuilt.namespace == "trading"


# -- appending (insert-only merges) -----------------------------------------


def stored_sizes(dataset: IcebergDataset) -> dict[str, int]:
    table = dataset.read_arrow_table()
    return dict(zip(*(table.column(name).to_pylist() for name in ("symbol", "size")), strict=True))


def test_append_merge_by_inserts_new_keys_and_never_rewrites(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    changed = quotes(3).set_column(2, "size", pyarrow.array([90, 91, 92], pyarrow.int64()))
    assert dataset.append_arrow_table(changed, merge_by=True) == 1
    assert stored_sizes(dataset) == {"S0": 0, "S1": 1, "S2": 92}, "stored rows keep their values"


def test_a_replay_commits_no_snapshot(dataset: IcebergDataset) -> None:
    assert dataset.append_arrow_table(quotes(3), merge_by=True) == 3
    before = len(dataset.iceberg_table.snapshots())
    assert dataset.append_arrow_table(quotes(3), merge_by=True) == 0
    assert len(dataset.iceberg_table.snapshots()) == before, "nothing new means no commit"
    assert dataset.read_arrow_table().num_rows == 3


def test_append_scans_keys_not_rows(dataset: IcebergDataset) -> None:
    """The insert's scan projects the key columns alone -- that is the point."""
    dataset.append_arrow_table(quotes(4))
    inserted = dataset.insert_arrow_table(quotes(6))
    assert inserted == 2
    assert stored_sizes(dataset) == {f"S{i}": i for i in range(6)}


def test_insert_collapses_duplicate_keys_to_the_first(dataset: IcebergDataset) -> None:
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
    assert dataset.insert_arrow_table(chunk) == 1
    assert stored_sizes(dataset) == {"A": 1}


def test_insert_refuses_a_null_or_nan_key(dataset: IcebergDataset) -> None:
    dataset.get_or_create_table()
    day = datetime.date(2026, 8, 14)
    schema = pyarrow.schema(
        [
            ("symbol", pyarrow.string()),
            ("day", pyarrow.date32()),
            ("size", pyarrow.int64()),
            ("venue", pyarrow.string()),
        ]
    )
    nulled = pyarrow.Table.from_pydict(
        {"symbol": [None], "day": [day], "size": [1], "venue": ["XPAR"]}, schema=schema
    )
    with pytest.raises(ValueError, match="cannot be null"):
        dataset.insert_arrow_table(nulled, ["symbol"])


def test_append_without_merge_by_is_a_plain_append(dataset: IcebergDataset) -> None:
    assert dataset.append_arrow_table(quotes(2)) == 2
    assert dataset.append_arrow_table(quotes(2)) == 2
    assert dataset.read_arrow_table().num_rows == 4


def test_append_streams_one_commit_per_chunk(dataset: IcebergDataset) -> None:
    reader = quotes(6).to_reader(max_chunksize=1)
    dataset.append_arrow_reader(reader, merge_by=True, commit_row_size=2)
    assert dataset.read_arrow_table().num_rows == 6
    assert len(dataset.iceberg_table.snapshots()) == 3, "two rows per commit"


def test_monotonic_inserts_plan_only_at_the_equal_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A strict new range is known fresh; equality still checks the stored key."""
    catalog = IcebergCatalog(name="monotonic", properties=catalog_properties(tmp_path))
    target = catalog.dataset("trading.timed", field=Timed.into_field())
    table = target.get_or_create_table()
    scans = 0
    original = table.scan

    def counted(*args: object, **kwargs: object) -> object:
        nonlocal scans
        scans += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(table, "scan", counted)
    target.append_arrow_reader(
        timed(0, 1, 2, 3, 4, 5).to_reader(max_chunksize=1),
        merge_by=True,
        commit_row_size=2,
    )
    assert scans == 0, "an empty table and two strictly later chunks need no plan"

    target.append_arrow_table(timed(5, 6), merge_by=True, commit_row_size=0)
    assert scans == 1, "equal to the upper bound may be a replay and must be checked"
    target.append_arrow_table(timed(7, 8), merge_by=True, commit_row_size=0)
    assert scans == 1, "the exact bound advances after the checked insert"

    monkeypatch.setattr(table, "scan", original)
    assert target.read_arrow_table().sort_by("unix").column("unix").to_pylist() == list(range(9))
    assert (
        target.data_files().num_rows,
        target.iceberg_table.inspect.manifests().num_rows,
        len(target.iceberg_table.snapshots()),
    ) == (5, 5, 5)


def test_descending_monotonic_inserts_advance_the_lower_frontier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = IcebergCatalog(name="descending-frontier", properties=catalog_properties(tmp_path))
    target = catalog.dataset("trading.descending_timed", field=DescendingTimed.into_field())
    table = target.get_or_create_table()
    scans = 0
    original = table.scan

    def counted(*args: object, **kwargs: object) -> object:
        nonlocal scans
        scans += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(table, "scan", counted)
    target.append_arrow_reader(
        descending_timed(8, 7, 6, 5, 4, 3).to_reader(max_chunksize=1),
        merge_by=True,
        commit_row_size=2,
    )
    assert scans == 0

    target.append_arrow_table(descending_timed(3, 2), merge_by=True, commit_row_size=0)
    assert scans == 1, "the equal boundary is still checked"
    target.append_arrow_table(descending_timed(1, 0), merge_by=True, commit_row_size=0)
    assert scans == 1, "the checked lower frontier advances"

    monkeypatch.setattr(table, "scan", original)
    assert target.read_arrow_table(order_by=[("unix", "descending")]).column(
        "unix"
    ).to_pylist() == list(range(8, -1, -1))


def test_a_direct_table_write_invalidates_the_insert_upper_bound(tmp_path: Path) -> None:
    """A same-handle write outside the wrapper must not make a later key look new."""
    catalog = IcebergCatalog(name="mixed", properties=catalog_properties(tmp_path))
    target = catalog.dataset("trading.timed", field=Timed.into_field())
    target.append_arrow_table(timed(0, 1), merge_by=True, commit_row_size=0)
    target.iceberg_table.append(timed(100))
    target.append_arrow_table(timed(50), merge_by=True, commit_row_size=0)
    snapshots = len(target.iceberg_table.snapshots())

    target.append_arrow_table(timed(100), merge_by=True, commit_row_size=0)

    assert len(target.iceberg_table.snapshots()) == snapshots, "the replay commits nothing"
    assert target.read_arrow_table().sort_by("unix").column("unix").to_pylist() == [0, 1, 50, 100]


def test_a_stale_monotonic_writer_cannot_bypass_an_external_commit(tmp_path: Path) -> None:
    """Iceberg rejects the stale append before a cached bound could duplicate a key."""
    from pyiceberg.exceptions import CommitFailedException

    catalog = IcebergCatalog(name="concurrent", properties=catalog_properties(tmp_path))
    writer = catalog.dataset("trading.timed", field=Timed.into_field())
    writer.append_arrow_table(timed(0), merge_by=True, commit_row_size=0)
    other = catalog.dataset("trading.timed", field=Timed.into_field())
    other.append_arrow_table(timed(100), commit_row_size=0)

    with pytest.raises(CommitFailedException, match="branch main has changed"):
        writer.append_arrow_table(timed(100), merge_by=True, commit_row_size=0)

    assert writer.refresh().read_arrow_table().sort_by("unix").column("unix").to_pylist() == [
        0,
        100,
    ]


def test_a_log_lands_in_a_table(logs: IcebergDataset) -> None:
    """The parser's own shape, end to end: declared, created, written, read.

    One line carrying every kind at once, because a column the catalog cannot
    hold fails at the write and nowhere earlier: the pair lists, a boolean, a
    double, a binary block, and a UTC microsecond timestamp.
    """
    assert len(FixMsg.into_field().names) == 109
    logs.overwrite_arrow_table(log_table(FIX_LINE), merge_by=True)
    logs.overwrite_arrow_table(log_table(FIX_LINE), merge_by=True)

    assert [one.name for one in logs.iceberg_table.schema().fields] == FixMsg.into_field().names
    stored = logs.read_arrow_table(FixMsg.into_field())
    assert stored.num_rows == 1, "the same line upserts onto itself"
    row = stored.to_pylist()[0]
    assert row["protocol_code"] == "FIX"
    assert row["kwargs"] == []
    assert [party["PartyID"] for party in row["Parties"]] == ["BUYSIDE", "XPAR"]
    assert row["MsgSeqNum"] == 7 and row["SenderCompID"] == "BUYSIDE"
    assert row["Symbol"] == "TTF" and row["ClOrdID"] == "ORD-1"
    assert row["OrderQty"] == 1200.0
    assert row["PossDupFlag"] is True
    assert row["Signature"] == b"\x00sealed", "binary keeps its leading zero byte"
    assert row["SendingTime"] == FIX_LINE.SendingTime
    assert row["CheckSum"] == "203", "text keeps checksum leading zeros"
    assert row["Price"] is None, "a field this message never carried"


def test_a_raw_message_argument_list_round_trips_through_iceberg(tmp_path: Path) -> None:
    target = IcebergDataset(
        field=Message.into_field("trading.messages"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    row = Message(
        unix=1,
        source_url="capture.log",
        source_rownum=7,
        message="opaque",
        kwargs=[
            Kwarg(key="Empty", value=""),
            Kwarg(key="55", value="TTF"),
        ],
    ).identify()

    target.append_arrow_table(Message.into_arrow_reader([row]).read_all())

    reopened = IcebergCatalog(name="test", properties=catalog_properties(tmp_path)).dataset(
        target.name
    )
    shape = reopened.into_struct_field().field("kwargs")
    stored = reopened.read_arrow_table(Message.into_field()).column("kwargs").to_pylist()
    assert shape.nullable is False and not shape.item.nullable
    assert shape.item.field("value").nullable is False
    assert stored == [[dataclasses.asdict(entry) for entry in row.kwargs]]


def test_pyiceberg_currently_collapses_absent_pair_lists_to_empty(
    logs: IcebergDataset,
) -> None:
    """Pin PyIceberg's loss of the outer `list<struct>` validity bitmap."""
    quiet = FixMsg(unix=1, hash=1, xhash=1, message="heartbeat emitted")
    bridged = FixMsg(unix=2, hash=2, xhash=2, message="toBridge #", protocol_code="UL", kwargs=[])
    logs.append_arrow_table(log_table(quiet, bridged, FIX_LINE))

    stored = logs.read_arrow_table(FixMsg.into_field()).sort_by("unix")
    assert stored.column("protocol_code").to_pylist() == ["OTHER", "UL", "FIX"]
    # PyIceberg's projection currently rebuilds list<struct> without its outer
    # validity bitmap, so an absent pair/component list reads as empty. The
    # parser-level contract still pins null versus empty before this boundary.
    assert stored.column("kwargs").to_pylist() == [[], [], []]
    assert stored.column("Parties").to_pylist()[0:2] == [[], []]


def test_the_stored_fields_keep_their_required_members(
    logs: IcebergDataset, tmp_path: Path
) -> None:
    """A stored field always says which field it is, through Iceberg too."""
    logs.append_arrow_table(log_table(FIX_LINE))
    found = IcebergCatalog(name="test", properties=catalog_properties(tmp_path)).dataset(logs.name)
    shape = found.into_struct_field().field("kwargs")
    assert shape.nullable and not shape.item.nullable
    required = {member.name for member in shape.item.fields if not member.nullable}
    assert required == {"tag", "key", "value"}


def test_the_flattened_columns_are_inside_the_bounds_budget(logs: IcebergDataset) -> None:
    """Iceberg bounds the first `write.metadata.metrics.max-inferred-column
    -defaults` leaves in pre-order, 100 by default, and flattening the message
    took this shape past that. So the budget stopped being one the table can
    inherit: `metrics_for` declares one wide enough for every leaf, because a
    column past the cutoff is written with no bounds at all -- and an unbounded
    column prunes nothing while looking exactly like a column that does.

    The count is pinned against the declaration rather than derived from it, so
    that a shape growing past the `MAX_INFERRED` ceiling is caught here, where
    the budget is reasoned about, and not as a tail of columns that quietly
    stopped pruning. `Text` is the last leaf this row fills, and a bound is
    recorded only for a column that carries a value.
    """
    logs.append_arrow_table(log_table(FIX_LINE))
    leaves = FixMsg.into_field().leaf_names()
    assert len(leaves) == 127
    assert int(logs.iceberg_table.properties[INFERRED_METRICS]) >= len(leaves)
    last = logs.iceberg_table.schema().find_field("Text").field_id
    written = [task.file for task in logs.iceberg_table.scan().plan_files()]
    assert written, "a write landed a file"
    assert all(last in one.lower_bounds for one in written), "the last column still prunes"


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
            Quote.into_field().into_iceberg_schema()


# -- creating explicitly ----------------------------------------------------


def test_create_with_builds_the_table_before_any_write(dataset: IcebergDataset) -> None:
    dataset.create_with()
    assert dataset.exists
    assert dataset.read_arrow_table().num_rows == 0


def test_create_with_takes_a_shape_it_was_not_declared_with(tmp_path: Path) -> None:
    bare = IcebergDataset(
        field=Field.from_(pyarrow.schema([]), "trading.bare"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    schema = pyarrow.schema([pyarrow.field("symbol", pyarrow.string(), nullable=False)])
    bare.create_with(schema)
    assert bare.into_struct_field().names == ["symbol"]
    assert bare.name == "trading.bare"


def test_creating_twice_leaves_the_table_alone(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.create_with()
    assert dataset.read_arrow_table().num_rows == 2


# -- schema evolution -------------------------------------------------------


def test_add_fields_adds_what_the_table_lacks(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    wider = Quote.into_field().merge_with(
        pyarrow.schema([("desk", pyarrow.string()), ("pod", pyarrow.int32())])
    )
    assert dataset.add_fields(wider) == ["desk", "pod"]
    assert dataset.table_field.names[-2:] == ["desk", "pod"]
    assert dataset.into_struct_field().names[-2:] == ["desk", "pod"], "writes follow the table"
    assert dataset.read_arrow_table().column("desk").to_pylist() == [None, None]


def test_add_fields_skips_when_there_is_nothing_new(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    before = len(dataset.iceberg_table.schemas())
    assert dataset.add_fields(Quote.into_field()) == []
    assert len(dataset.refresh().iceberg_table.schemas()) == before, "no commit was made"


def test_add_fields_can_report_without_touching_the_table(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    wider = Quote.into_field().merge_with(pyarrow.schema([("desk", pyarrow.string())]))
    assert dataset.add_fields(wider, dry_run=True) == ["desk"]
    assert "desk" not in dataset.refresh().into_struct_field().names


def test_a_wider_batch_lands_after_the_columns_are_added(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    wider = Quote.into_field().merge_with(pyarrow.schema([("desk", pyarrow.string())]))
    dataset.add_fields(wider)
    batch = quotes(1).append_column("desk", pyarrow.array(["EQ"]))
    dataset.append_arrow(batch)  # the declared shape moved with the table
    assert set(dataset.read_arrow_table().column("desk").to_pylist()) == {None, "EQ"}


# -- snapshots and branches -------------------------------------------------


def test_snapshots_are_listed(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    dataset.append_arrow_table(quotes(1))
    assert dataset.snapshots().num_rows == 2


def test_a_read_can_go_back_to_an_older_snapshot(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    first = dataset.iceberg_table.current_snapshot().snapshot_id
    dataset.append_arrow_table(quotes(3))
    assert dataset.refresh().read_arrow_table().num_rows == 5
    assert dataset.read_arrow_table(snapshot_id=first).num_rows == 2


def test_a_branch_is_written_and_read_on_its_own(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.create_branch("dev")
    dataset.append_arrow(quotes(3), branch="dev")
    assert dataset.read_arrow_table(branch="dev").num_rows == 5
    assert dataset.read_arrow_table().num_rows == 2, "main is untouched"


@pytest.mark.parametrize("alias", ["root", "main", "master"])
def test_root_branch_aliases_override_a_named_default(dataset: IcebergDataset, alias: str) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.create_branch("dev")
    dataset.branch = "dev"
    dataset.append_arrow(quotes(1, alias), branch=alias)
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


@pytest.mark.parametrize("plan_merges", [True, False])
def test_a_missing_branch_is_refused_before_a_merge_replay(
    dataset: IcebergDataset, plan_merges: bool
) -> None:
    given = quotes(2)
    dataset.append_arrow_table(given)
    dataset.plan_merges = plan_merges

    with pytest.raises(ValueError, match="unknown ref=missing"):
        dataset.overwrite_arrow_table(given, branch="missing")

    assert set(dataset.refs()) == {"main"}
    assert dataset.read_arrow_table().num_rows == 2


def test_a_missing_branch_is_refused_before_an_insert_replay(
    dataset: IcebergDataset,
) -> None:
    given = quotes(2)
    dataset.append_arrow_table(given)

    with pytest.raises(ValueError, match="unknown ref=missing"):
        dataset.append_arrow_table(given, merge_by=True, branch="missing")

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
    dataset.append_arrow_table(quotes(3))
    dataset.rollback(first)
    assert dataset.read_arrow_table().num_rows == 2


def test_rows_are_deleted_by_filter(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(5))
    dataset.delete("size >= 3")
    assert dataset.refresh().read_arrow_table().num_rows == 3


# -- maintenance ------------------------------------------------------------


def test_many_small_writes_leave_many_files(dataset: IcebergDataset) -> None:
    for _ in range(4):
        dataset.append_arrow_table(quotes(1))
    assert dataset.data_files().num_rows >= 4


def test_compaction_rewrites_the_fragments(dataset: IcebergDataset) -> None:
    for index in range(4):
        dataset.append_arrow_table(quotes(2, f"venue{index}"))
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
    dataset.append_arrow_table(quotes(2))
    plan = dataset.compaction_plan(min_files=2)
    assert len(plan) == 1
    assert plan[0][0] == EqualTo("day", datetime.date(2026, 8, 14)), (
        "an expression, not a string to parse back"
    )


def test_an_unpartitioned_table_compacts(tmp_path: Path) -> None:
    """The most ordinary table shape there is, and every verb raised on it."""

    @scalar
    class Flat(Convertible):
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
            commit_row_size=0,
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
    class Event(Convertible):
        """One event, partitioned by a transform of its timestamp."""

        symbol: str
        """Instrument."""

        at: Annotated[datetime.datetime, Field.partition_key("day")]
        """When it happened."""

    catalog = IcebergCatalog(name="daily", properties=catalog_properties(tmp_path))
    daily = catalog.dataset("trading.daily", field=Event.into_field())
    schema = Event.into_field().into_arrow_schema()
    base = datetime.datetime(2026, 8, 14)
    for index in range(4):
        daily.append_arrow(
            pyarrow.Table.from_pydict(
                {
                    "symbol": [f"S{index}", f"T{index}"],
                    "at": [base, base + datetime.timedelta(days=1)],
                },
                schema=schema,
            ),
            commit_row_size=0,
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
        commit_row_size=0,
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
    class Part(Convertible):
        """A row partitioned by a string that may be awkward."""

        part: Annotated[str | None, Field.partition_key()]
        """The partition."""

        size: int
        """Quantity."""

    catalog = IcebergCatalog(name="lit", properties=catalog_properties(tmp_path))
    parted = catalog.dataset("trading.parts", field=Part.into_field())
    schema = Part.into_field().into_arrow_schema()

    def rows(part: str | None, size: int) -> pyarrow.Table:
        return pyarrow.Table.from_pydict({"part": [part], "size": [size]}, schema=schema)

    for index in range(3):
        parted.append_arrow(rows(value, index), commit_row_size=0)
    parted.append_arrow(rows("untouched", 99), commit_row_size=0)
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
        dataset.append_arrow(quotes(2), commit_row_size=0)
    table = dataset.get_or_create_table()
    table.manage_snapshots().create_branch(table.current_snapshot().snapshot_id, "work").commit()
    dataset.refresh()
    for index in range(3):
        dataset.append_arrow(quotes(2, f"v{index}"), branch="work", commit_row_size=0)
    assert dataset.compact(min_files=2, branch="work") > 0
    assert dataset.compact(min_files=2, branch="work") == 0, "it settles on the branch"
    assert dataset.compaction_plan(min_files=2, branch="work") == []
    assert dataset.compaction_plan(min_files=2) != [], "and main is still its own plan"


def test_a_filtered_compaction_marks_nothing(dataset: IcebergDataset) -> None:
    """A caller's filter may cover a fraction of a partition; the rest still needs it."""
    for _ in range(3):
        dataset.append_arrow(quotes(2), commit_row_size=0)
    assert dataset.compact(row_filter="symbol = 'S0'") > 0
    assert dataset.compaction_marks() == {}
    assert dataset.compaction_plan(min_files=2) != [], "the partition is still planned"


def test_a_member_added_inside_a_struct_is_added(tmp_path: Path) -> None:
    """`union_by_name` adds it; comparing top-level names never asked for it."""

    @scalar
    class Venue(Convertible):
        """Where it traded."""

        mic: str | None = None
        """Market identifier."""

    @scalar
    class Narrow(Convertible):
        """A quote whose venue knows only its mic."""

        symbol: str
        """Instrument."""

        venue: Venue | None = None
        """Where."""

    @scalar
    class Wide(Convertible):
        """The same quote, whose venue has grown a country."""

        symbol: str
        """Instrument."""

        venue: Venue | None = None
        """Where."""

    wide = Wide.into_field().merge_with(
        pyarrow.struct(
            [
                pyarrow.field("symbol", pyarrow.string()),
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
        )
    )
    catalog = IcebergCatalog(name="nested", properties=catalog_properties(tmp_path))
    quotes_ = catalog.dataset("trading.nested", field=Narrow.into_field())
    narrow_schema = Narrow.into_field().into_arrow_schema()
    quotes_.append_arrow(
        pyarrow.Table.from_pydict(
            {"symbol": ["A"], "venue": [{"mic": "XPAR"}]}, schema=narrow_schema
        ),
        commit_row_size=0,
    )
    assert quotes_.add_fields(wide) == ["venue.country"]
    assert quotes_.add_fields(wide) == [], "nothing new, so no commit"
    quotes_.refresh()
    quotes_.append_arrow(
        pyarrow.Table.from_pydict(
            {"symbol": ["B"], "venue": [{"mic": "XLON", "country": "GB"}]},
            schema=wide.into_arrow_schema(),
        ),
        commit_row_size=0,
    )
    stored = sorted(quotes_.refresh().read_arrow_table().to_pylist(), key=lambda row: row["symbol"])
    assert stored[1]["venue"] == {"mic": "XLON", "country": "GB"}, "the value survived the write"


def test_a_filter_compacts_only_that_part(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(2))
    dataset.append_arrow_table(quotes(2))
    assert dataset.compact(row_filter="day = '2026-08-14'") > 0
    assert dataset.read_arrow_table().num_rows == 4


def test_cleanup_expires_old_snapshots(dataset: IcebergDataset) -> None:
    for _ in range(4):
        dataset.append_arrow_table(quotes(1))
    report = dataset.cleanup(retain=1, remove_orphans=False)
    assert report["expired"] == 3
    assert dataset.refresh().snapshots().num_rows == 1
    assert dataset.read_arrow_table().num_rows == 4, "the data is still all there"


def test_cleanup_can_report_without_touching_anything(dataset: IcebergDataset) -> None:
    for _ in range(3):
        dataset.append_arrow_table(quotes(1))
    report = dataset.cleanup(retain=1, dry_run=True)
    assert report["expired"] == 2
    assert dataset.refresh().snapshots().num_rows == 3


def test_cleanup_keeps_what_a_branch_still_references(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(quotes(1))
    dataset.create_branch("dev")
    for _ in range(3):
        dataset.append_arrow_table(quotes(1))
    dataset.cleanup(retain=1, remove_orphans=False)
    assert dataset.refresh().snapshots().num_rows >= 2, "the branch head survived"


def test_cleanup_sweeps_the_files_expiry_stranded(dataset: IcebergDataset) -> None:
    """Expiry is metadata-only, so the sweep is the half that reclaims space."""
    for index in range(3):
        dataset.append_arrow_table(quotes(2, f"venue{index}"))
    dataset.compact(min_files=2)
    assert dataset.orphan_files(older_than=datetime.timedelta(seconds=0)) == [], (
        "the files compaction replaced are still held by the snapshots before it"
    )
    report = dataset.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0))
    assert report["expired"] > 0
    assert report["deleted"] > 0, "expiring the old snapshots is what made them garbage"
    assert report["bytes"] > 0
    assert dataset.read_arrow_table().num_rows == 6, "only garbage went"


def test_a_sweep_forgets_the_bytes_of_what_it_deleted(dataset: IcebergDataset) -> None:
    """The cache is keyed by location and the sweep deletes through a
    `pyarrow.fs` handle, so nothing told it -- and a swept manifest went on
    answering `exists()` and handing over its bytes. Five of them, measured
    after one `cleanup`."""
    from rekep.arrow_file_io import CONTENT_CACHE

    for index in range(3):
        dataset.append_arrow_table(quotes(2, f"venue{index}"))
    dataset.compact(min_files=2)
    dataset.cleanup(retain=1, remove_orphans=False)  # expire, and strand what they held
    stranded = dataset._orphans(datetime.timedelta(seconds=0), metadata=True)
    held = [location for _, _, location, _ in stranded if CONTENT_CACHE.peek(location) is not None]
    assert held, "this process wrote those manifests, so it cached them"

    report = dataset.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0))
    assert report["deleted"] >= len(held)
    assert [location for location in held if CONTENT_CACHE.peek(location) is not None] == []
    assert dataset.read_arrow_table().num_rows == 6, "only garbage went"


def test_the_live_set_walks_the_manifests_once(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Data and metadata are two readings of the same manifests. Two walks
    decoded every retained snapshot's manifest list twice -- 96 ms against 76
    on a 40-commit table, and a round trip per snapshot on a cold store."""
    from rekep.iceberg import dataset as module

    for index in range(3):
        dataset.append_arrow_table(quotes(2, f"venue{index}"))
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
        dataset.append_arrow_table(quotes(2, f"venue{index}"))
    table = dataset.iceberg_table
    _, files = dataset._live(table)
    lists = {snapshot.manifest_list for snapshot in table.snapshots() if snapshot.manifest_list}
    assert lists and lists <= files


def test_a_recent_file_is_never_swept(dataset: IcebergDataset) -> None:
    """A writer committing right now has files no snapshot mentions yet."""
    dataset.append_arrow_table(quotes(2))
    dataset.append_arrow_table(quotes(2))
    dataset.compact(min_files=2)
    assert dataset.orphan_files() == [], "nothing is old enough to be garbage"


def test_optimize_does_the_whole_routine(dataset: IcebergDataset) -> None:
    for index in range(4):
        dataset.append_arrow_table(quotes(2, f"venue{index}"))
    report = dataset.optimize(min_files=2)
    assert report["rewritten"] > 0
    assert report["expired"] > 0
    assert dataset.iceberg_table.properties["commit.manifest-merge.enabled"] == "true"
    assert dataset.read_arrow_table().num_rows == 8


def test_optimize_retrofits_missing_metadata_maintenance_properties(tmp_path: Path) -> None:
    external = IcebergDataset(
        field=Quote.into_field("trading.external"),
        catalog="test",
        properties=catalog_properties(tmp_path),
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
        field=Quote.into_field("trading.retained"),
        catalog="test",
        properties=catalog_properties(tmp_path),
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
    dataset.append_arrow_table(quotes(2))
    dataset.compact(min_files=2, target_file_size=8 * 1024 * 1024)
    assert dataset.iceberg_table.properties["write.target-file-size-bytes"] == str(8 * 1024 * 1024)


# -- field ids --------------------------------------------------------------


def test_a_schema_that_carries_ids_keeps_them(dataset: IcebergDataset) -> None:
    """Iceberg matches columns by id: taking the ids back is what keeps a
    round trip lossless instead of renumbering every column."""
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    dataset.append_arrow_table(quotes(1))
    declared = dataset.iceberg_table.schema()
    carried = Field.from_arrow_schema(schema_to_pyarrow(declared, include_field_ids=True))
    assert [f.field_id for f in carried.into_iceberg_schema().fields] == [
        f.field_id for f in declared.fields
    ]


def test_a_plain_arrow_schema_is_numbered_for_the_user(tmp_path: Path) -> None:
    schema = pyarrow.schema([pyarrow.field("a", pyarrow.int64(), nullable=False)])
    plain = IcebergDataset(
        field=Field.from_(schema, "trading.plain"),
        catalog="test",
        properties=catalog_properties(tmp_path),
    )
    plain.create_with()
    assert [f.field_id for f in plain.iceberg_table.schema().fields] == [1]
    assert plain.field.name == "trading.plain"


# -- commits ----------------------------------------------------------------


def test_a_write_commits_in_chunks_of_the_datasets_own_size(dataset: IcebergDataset) -> None:
    dataset.commit_row_size = 2
    dataset.append_arrow_reader(quotes(6).to_reader(max_chunksize=1))
    assert len(dataset.iceberg_table.history()) == 3, "the dataset's size, with no call saying so"


def test_a_call_overrides_the_datasets_commit_size(dataset: IcebergDataset) -> None:
    dataset.commit_row_size = 2
    dataset.append_arrow_reader(quotes(6).to_reader(max_chunksize=1), commit_row_size=0)
    assert len(dataset.iceberg_table.history()) == 1, "0 means one commit for the stream"


def test_a_created_table_carries_the_commit_properties(dataset: IcebergDataset) -> None:
    dataset.create_with()
    properties = dataset.iceberg_table.properties
    assert properties["commit.manifest-merge.enabled"] == "true"
    assert properties["write.target-file-size-bytes"] == str(256 * 1024 * 1024)
    assert properties["write.parquet.row-group-limit"] == str(128 * 1024)


def test_iceberg_defaults_can_be_kept(tmp_path: Path) -> None:
    bare = IcebergDataset(
        field=Quote.into_field("trading.bare"),
        catalog="test",
        properties=catalog_properties(tmp_path),
        optimize_commits=False,
    )
    bare.create_with()
    assert "commit.manifest-merge.enabled" not in bare.iceberg_table.properties


def test_declared_table_properties_win_over_the_defaults(tmp_path: Path) -> None:
    tuned = IcebergDataset(
        field=Quote.into_field("trading.tuned"),
        catalog="test",
        properties=catalog_properties(tmp_path),
        table_properties={"write.target-file-size-bytes": "1024"},
    )
    tuned.create_with()
    assert tuned.iceberg_table.properties["write.target-file-size-bytes"] == "1024"
    assert tuned.iceberg_table.properties["commit.manifest-merge.enabled"] == "true"


# -- planning ---------------------------------------------------------------


def test_a_merge_of_new_keys_writes_without_reading(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    """The pruning short circuit, through `overwrite_arrow(merge_by=True)`: keys no
    stored file can hold plan to nothing, so the merge commits what it was
    given without opening a data file to compare against."""
    dataset.append_arrow_table(quotes(3))
    before = len(dataset.iceberg_table.history())
    opened.clear()
    dataset.overwrite_arrow(keyed("T", 3), merge_by=True)  # keys nothing stored shares
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
    dataset.append_arrow(quotes(2), commit_row_size=0)
    table = dataset.get_or_create_table()
    first = table.current_snapshot().snapshot_id
    table.manage_snapshots().create_branch(first, "dev").commit()
    dataset.refresh()
    dataset.append_arrow(quotes(1, "later"), commit_row_size=0)

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
        dataset.append_arrow(quotes(2, f"v{index}"), commit_row_size=0)
    dataset.append_arrow(other_day(2), commit_row_size=0)
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
    dataset.append_arrow(quotes(2), commit_row_size=0)
    early = dataset.iceberg_table.current_snapshot().snapshot_id
    dataset.create_branch("dev")
    for index in range(3):
        dataset.append_arrow(quotes(2, f"v{index}"), commit_row_size=0)
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
        dataset.overwrite_arrow(rows.to_reader(max_chunksize=3), merge_by=True, commit_row_size=3)
    finally:
        del dataset.store.load_table
    assert loaded == 1, "one load for the whole stream, not one per commit"
    assert set(dataset.read_arrow_table().column("venue").to_pylist()) == {"XETR"}, (
        "and what was committed is visible without a reload"
    )


def test_the_merge_path_can_be_handed_back_to_pyiceberg(dataset: IcebergDataset) -> None:
    dataset.plan_merges = False
    dataset.append_arrow_table(quotes(3))
    dataset.overwrite_arrow(quotes(3, "XETR"), merge_by=True)
    assert set(dataset.refresh().read_arrow_table().column("venue").to_pylist()) == {"XETR"}


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
    """Store opens by file kind, counted on `PyArrowFile` -- below any cache."""
    from pyiceberg.io.pyarrow import PyArrowFile

    counts: dict[str, int] = {}
    original = PyArrowFile.open

    def counted(self: PyArrowFile, *args: object, **kwargs: object) -> object:
        kind = "data" if self.location.endswith(".parquet") else "metadata"
        counts[kind] = counts.get(kind, 0) + 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(PyArrowFile, "open", counted)
    return counts


class _StreamOnly:
    """A planned reader whose batches are usable but whose collecting API is not."""

    def __init__(self, reader: pyarrow.RecordBatchReader) -> None:
        self.reader = reader
        self.schema = reader.schema

    def __iter__(self):
        return iter(self.reader)

    def __enter__(self) -> "_StreamOnly":
        return self

    def __exit__(self, *_args: object) -> None:
        self.reader.close()

    def read_all(self) -> None:
        pytest.fail("a planned Iceberg write scan must stay streamed")


def _forbid_planned_collect(monkeypatch: pytest.MonkeyPatch) -> None:
    from rekep.iceberg import dataset as module

    planned = module._planned_reader
    unordered = module._unordered_reader
    monkeypatch.setattr(
        module,
        "_planned_reader",
        lambda scan, tasks, **kwargs: _StreamOnly(planned(scan, tasks, **kwargs)),
    )
    monkeypatch.setattr(
        module,
        "_unordered_reader",
        lambda scan, tasks, **kwargs: _StreamOnly(unordered(scan, tasks, **kwargs)),
    )


def test_a_merge_consumes_its_planned_scan_without_collecting(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset.append_arrow_table(quotes(3))
    _forbid_planned_collect(monkeypatch)
    assert dataset.merge_arrow_table(quotes(3, "XETR")) == (3, 0)


def test_an_insert_consumes_its_key_scan_without_collecting(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset.append_arrow_table(quotes(3))
    _forbid_planned_collect(monkeypatch)
    assert dataset.insert_arrow_table(quotes(3, "XETR")) == 0


def test_a_replayed_insert_stops_planning_after_its_keys_are_exhausted(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A huge replay must not retain file tasks after the first exact match."""
    dataset.append_arrow_table(quotes(3), merge_by=False)
    dataset.append_arrow_table(quotes(3), merge_by=False)
    scan_type = type(dataset.iceberg_table.scan())
    original = scan_type.plan_files
    planned = 0

    def one_at_a_time(self: Any):
        nonlocal planned
        for task in original(self):
            planned += 1
            if planned > 1:
                pytest.fail("the replay planned a file after every source key matched")
            yield task

    monkeypatch.setattr(scan_type, "plan_files", one_at_a_time)

    assert dataset.insert_arrow_table(quotes(3)) == 0
    assert planned == 1


def test_a_streamed_merge_plans_only_the_partition_named_by_its_chunk(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    today = quotes(3)
    tomorrow = today.set_column(
        today.schema.get_field_index("day"),
        today.schema.field("day"),
        pyarrow.array([datetime.date(2026, 8, 15)] * today.num_rows),
    ).set_column(
        today.schema.get_field_index("venue"),
        today.schema.field("venue"),
        pyarrow.array(["tomorrow"] * today.num_rows),
    )
    dataset.append_arrow_table(today)
    dataset.append_arrow_table(tomorrow)

    from rekep.iceberg import dataset as module

    planned: list[Any] = []
    original = module._unordered_reader

    def capture(scan: Any, tasks: Any, **kwargs: Any) -> pyarrow.RecordBatchReader:
        tasks = list(tasks)
        planned.extend(tasks)
        return original(scan, tasks, **kwargs)

    monkeypatch.setattr(module, "_unordered_reader", capture)
    changed = today.set_column(
        today.schema.get_field_index("venue"),
        today.schema.field("venue"),
        pyarrow.array(["changed"] * today.num_rows),
    )
    assert dataset.merge_arrow_table(changed, ["symbol", "day"]) == (3, 0)
    assert len(planned) == 1, "the other identity partition never reaches the reader"
    held = dataset.read_arrow_table().to_pylist()
    assert {row["venue"] for row in held if row["day"] == datetime.date(2026, 8, 14)} == {"changed"}
    assert {row["venue"] for row in held if row["day"] == datetime.date(2026, 8, 15)} == {
        "tomorrow"
    }


def test_a_merge_of_disjoint_keys_opens_no_data_file(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    """Keys no stored file can hold plan to nothing, and nothing is read."""
    dataset.append_arrow_table(quotes(3))
    opened.clear()
    updated, inserted = dataset.merge_arrow_table(keyed("T", 3))
    assert (updated, inserted) == (0, 3)
    assert opened.get("data", 0) == 0, "the merge was an append, arrived at by planning"
    assert dataset.read_arrow_table().num_rows == 6


@pytest.mark.parametrize("count", [MERGE_IN_LIMIT, MERGE_IN_LIMIT + 1])
def test_a_merge_with_overlapping_bounds_and_no_exact_key_is_one_append(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, count: int
) -> None:
    """Cross the literal limit: neither exact nor ranged misses need an anti-join."""
    from rekep.iceberg import dataset as module

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
    before = (
        dataset.data_files().num_rows,
        dataset.iceberg_table.inspect.manifests().num_rows,
        len(dataset.iceberg_table.snapshots()),
    )

    def unnecessary(*args: object, **kwargs: object) -> None:
        pytest.fail("no exact match leaves nothing to compare")

    monkeypatch.setattr(module, "_changed", unnecessary)
    assert dataset.merge_arrow_table(spaced(1)) == (0, count)
    assert dataset.read_arrow_table().num_rows == 2 * count
    assert (
        dataset.data_files().num_rows,
        dataset.iceberg_table.inspect.manifests().num_rows,
        len(dataset.iceberg_table.snapshots()),
    ) == tuple(value + 1 for value in before)


def test_a_merge_that_matches_still_reads_and_updates(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    dataset.append_arrow_table(quotes(3))
    opened.clear()
    updated, inserted = dataset.merge_arrow_table(quotes(3, "XETR"))
    assert (updated, inserted) == (3, 0)
    assert opened.get("data", 0) > 0, "matching keys have to be read to be compared"


def test_a_replayed_insert_opens_data_once_and_commits_nothing(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    """Every key is stored, so the insert is nothing but the key scan: the one
    file holding them, read once, and no snapshot for the zero rows left."""
    dataset.append_arrow_table(quotes(3))
    before = len(dataset.iceberg_table.snapshots())
    opened.clear()
    assert dataset.insert_arrow_table(quotes(3, "XETR")) == 0
    assert opened.get("data", 0) == 1, "one stored file, opened once to read its keys"
    assert len(dataset.iceberg_table.snapshots()) == before, "nothing new, no commit"


def test_an_insert_of_disjoint_keys_appends_without_reading(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    dataset.append_arrow_table(quotes(3))
    opened.clear()
    assert dataset.insert_arrow_table(keyed("T", 2)) == 2
    assert opened.get("data", 0) == 0


def test_an_insert_reaches_a_branch_cut_before_a_key_was_renamed(
    dataset: IcebergDataset,
) -> None:
    """A rename is metadata-only, so the branch head still answers to the old
    name -- and this was the one verb that asked for the new one by name.
    `Could not find column: 'key'`, on a branch every other verb reads and
    writes happily."""
    dataset.append_arrow(quotes(3), commit_row_size=0)
    dataset.create_branch("dev")
    with dataset.iceberg_table.update_schema() as update:
        update.rename_column("symbol", "ticker")
    dataset.refresh()
    dataset.field = dataset.table_field

    # Under the *declared* shape, which is what names the column `ticker`:
    # a bare read of that branch hands back the branch's own name for it.
    replayed = dataset.read_arrow_table(dataset.field, branch="dev")
    assert dataset.insert_arrow_table(replayed, ["ticker"], branch="dev") == 0, (
        "every key is stored on that branch already, whatever it calls the column"
    )
    fresh = replayed.slice(0, 1).set_column(
        replayed.schema.get_field_index("ticker"),
        replayed.schema.field("ticker"),
        pyarrow.array(["NEW"], replayed.schema.field("ticker").type),
    )
    assert dataset.insert_arrow_table(fresh, ["ticker"], branch="dev") == 1
    assert dataset.read_arrow_table(branch="dev").num_rows == 4
    assert dataset.read_arrow_table().num_rows == 3, "and main is untouched"


def test_an_insert_onto_a_branch_without_the_key_column_is_all_new(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A key column added since the branch was cut: nothing on that branch can
    match one, so every row is new -- rather than a projection of whatever
    column the scan fell back to."""
    from rekep.iceberg import dataset as module

    dataset.append_arrow(quotes(3), commit_row_size=0)
    monkeypatch.setattr(module.IcebergDataset, "_selected", lambda self, target, scan: {})
    assert dataset.insert_arrow_table(quotes(3), ["symbol"]) == 3
    assert dataset.read_arrow_table().num_rows == 6


def test_a_bare_limit_opens_only_the_files_it_needs(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    """pyiceberg submits every planned file before its row cap bites; the plan
    is cut here instead, so a peek at a wide table stays a peek."""
    for _ in range(3):
        dataset.append_arrow_table(quotes(4))  # three commits, three files
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
        dataset.append_arrow(quotes(2, f"v{index}"), commit_row_size=0)
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
        dataset.append_arrow(quotes(2, f"v{index}"), commit_row_size=0)
    found = dataset.read_arrow_reader(row_filter="size >= 0", limit=3).read_all()
    assert found.num_rows == 3


def test_a_limit_under_a_partition_filter_opens_only_the_files_it_needs(
    dataset: IcebergDataset, opened: dict[str, int]
) -> None:
    """A filter the partition fully answers leaves an `AlwaysTrue` residual, so
    every row of a planned file matches and its record count is exact again."""
    for _ in range(3):
        dataset.append_arrow_table(quotes(4))  # three files, all in one day
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
    class Trade(Convertible):
        """A trade whose venue may be unknown."""

        venue: Annotated[str | None, Field.partition_key()]
        """Where it traded, when known -- so a null partition exists."""

        size: int
        """Quantity."""

    catalog = IcebergCatalog(name="nullpart", properties=catalog_properties(tmp_path))
    trades = catalog.dataset("trading.trades", field=Trade.into_field())
    schema = Trade.into_field().into_arrow_schema()
    trades.append_arrow(
        pyarrow.Table.from_pydict({"venue": ["XNYS"] * 10, "size": list(range(10))}, schema=schema),
        commit_row_size=0,
    )
    trades.append_arrow(
        pyarrow.Table.from_pydict({"venue": [None] * 10, "size": list(range(10))}, schema=schema),
        commit_row_size=0,
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
        dataset.append_arrow_table(quotes(4))
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
    for _ in range(4):
        dataset.append_arrow_table(quotes(1))
    loads: list[str] = []
    original = IcebergCatalog.load_table
    monkeypatch.setattr(
        IcebergCatalog,
        "load_table",
        lambda self, name: (loads.append(str(name)), original(self, name))[1],
    )
    report = dataset.cleanup(retain=1, remove_orphans=False)
    assert report["expired"] == 3
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
    original = module.resolve
    monkeypatch.setattr(module, "resolve", lambda url: (listed.append(url), original(url))[1])
    for index in range(4):
        dataset.append_arrow(quotes(2, f"v{index}"), commit_row_size=0)
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

    assert dataset.append_arrow_reader(iter([empty]), merge_by=False) == 0
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
        dataset.append_arrow(quotes(2, f"v{index}"), branch="master", commit_row_size=0)
    assert dataset.compact(branch="master") > 0
    assert dataset.compact(branch="root") == 0
    assert dataset.compaction_marks()
    assert all(key.startswith("main/") for key in dataset.compaction_marks())


# -- maintenance that settles -----------------------------------------------


def test_compaction_stops_when_there_is_nothing_left_to_gain(dataset: IcebergDataset) -> None:
    """A part that legitimately needs several files must not be rewritten forever."""
    dataset.table_properties = {"write.target-file-size-bytes": str(16 * 1024)}
    for index in range(6):
        dataset.append_arrow(quotes(4, f"venue{index}"), commit_row_size=0)
    assert dataset.compact(min_files=2) > 0
    files = dataset.refresh().data_files().num_rows
    assert dataset.compact(min_files=2) == 0, "the second pass has nothing to do"
    assert dataset.compaction_plan(min_files=2) == []
    assert dataset.refresh().data_files().num_rows == files, "and it did not grow the table"


def test_new_data_makes_a_compacted_partition_worth_planning_again(
    dataset: IcebergDataset,
) -> None:
    for _ in range(3):
        dataset.append_arrow(quotes(2), commit_row_size=0)
    dataset.compact(min_files=2)
    assert dataset.compaction_plan(min_files=2) == []
    for _ in range(2):
        dataset.append_arrow(quotes(2, "XETR"), commit_row_size=0)
    assert dataset.compaction_plan(min_files=2) != []


def test_the_compacted_parts_are_marked(dataset: IcebergDataset) -> None:
    """In a table property, which expiry cannot delete -- `optimize` expires."""
    for _ in range(2):
        dataset.append_arrow(quotes(2), commit_row_size=0)
    dataset.compact(min_files=2)
    marks = dataset.compaction_marks()
    assert marks, "how a compacted part is recognised later"
    dataset.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0))
    assert dataset.refresh().compaction_marks() == marks, "and a sweep does not lose it"


# -- the scan filter a merge builds -----------------------------------------


@pytest.mark.parametrize(
    ("values", "named"),
    [
        ([index % 8 for index in range(1000)], True),
        (list(range(200)), True),
        (list(range(201)), False),
        # The trap the slice probe could fall into: the head of the column has
        # few distinct values and the tail has thousands. Answering from the
        # probe alone would name 200 keys and miss every stored row past them.
        ([0] * 500 + list(range(5000)), False),
    ],
)
def test_a_key_column_is_named_only_when_it_can_be(values: list[int], named: bool) -> None:
    """`In` under the limit, a range past it -- however the values are ordered."""
    from rekep.iceberg.dataset import MERGE_IN_LIMIT, _distinct_under

    column = pyarrow.chunked_array([pyarrow.array(values, pyarrow.int64())])
    distinct = _distinct_under(column, MERGE_IN_LIMIT)
    assert (distinct is not None) is named
    if named:
        assert sorted(distinct.to_pylist()) == sorted(set(values))


def _covers(expression: object, table: pyarrow.Table, field: object) -> bool:
    """Whether every row of `table` satisfies an Iceberg expression, through Arrow.

    A merge's scan filter is a superset or it is a bug: a row it does not
    match is a stored row the merge never sees and inserts a second copy of.
    """
    from pyiceberg.expressions.visitors import bind, rewrite_not
    from pyiceberg.io.pyarrow import expression_to_pyarrow

    bound = bind(field.into_iceberg_schema(), rewrite_not(expression), case_sensitive=True)
    return table.filter(expression_to_pyarrow(bound)).num_rows == table.num_rows


@scalar
class Tick(Convertible):
    """A row under a wide integer key."""

    at: Annotated[int, Field.primary_key()]
    """A clustered timestamp."""

    payload: str
    """Payload."""


@pytest.mark.parametrize(
    ("values", "banded"),
    [
        # Two clusters a decade apart: the shape a backfill or a replay makes,
        # and the one a single min/max range cannot prune at all.
        (list(range(300)) + list(range(10**7, 10**7 + 300)), True),
        # Dense, and shuffled over the whole range: no gap to cut out, so the
        # bands would be a longer way of saying `min <= x <= max`.
        (list(range(600)), False),
        ([(index * 7919) % 600_000 for index in range(600)], False),
        # One value repeated past the naming limit, which has no range at all.
        ([5] * 300 + [9] * 300, False),
    ],
)
def test_a_key_range_names_the_bands_the_values_are_in(values: list[int], banded: bool) -> None:
    """Past the naming limit a key column was one min/max range, which prunes
    nothing on keys that sit in a few bands of a wide span. Whatever it
    becomes, every value has to still satisfy it."""
    from rekep.iceberg.dataset import _key_ranges

    chunk = pyarrow.Table.from_pydict(
        {"at": values, "payload": ["x"] * len(values)},
        schema=Tick.into_field().into_arrow_schema(),
    )
    ranges = _key_ranges(chunk, ["at"])
    assert _covers(ranges, chunk, Tick.into_field()), (
        "a scan filter that misses a key duplicates it"
    )
    assert (type(ranges).__name__ == "Or") is banded


@pytest.mark.parametrize(
    "kind",
    [pyarrow.date32(), pyarrow.date64(), pyarrow.timestamp("us"), pyarrow.timestamp("ms")],
)
def test_a_temporal_key_bands_whatever_width_it_stores(kind: pyarrow.DataType) -> None:
    """`date32` is what an Iceberg `date` column is, and Arrow has no
    `date32 -> int64` cast at all -- so asking for one made every date key fall
    out of the banding and silently keep the range it was meant to replace."""
    from rekep.iceberg.dataset import _banded

    start = datetime.date(2026, 1, 1)
    days = [start + datetime.timedelta(days=i) for i in range(110)]
    days += [start + datetime.timedelta(days=900 + i) for i in range(110)]
    values = (
        days
        if pyarrow.types.is_date32(kind)
        else [datetime.datetime(day.year, day.month, day.day) for day in days]
    )
    column = pyarrow.chunked_array([pyarrow.array(values, kind)])
    banded = _banded("at", column)
    assert type(banded).__name__ == "Or", "two clusters 900 days apart, and one range spans both"


@pytest.mark.parametrize(
    ("kind", "base"),
    [
        (pyarrow.int64(), 1_700_000_000_000_000_000),  # nanoseconds since the epoch
        (pyarrow.uint64(), 1 << 62),  # a line hash
    ],
)
def test_a_key_range_bands_values_past_a_double_mantissa(kind: pyarrow.DataType, base: int) -> None:
    """Arrow's default cast is range-checked, and every key this banding exists
    for -- a nanosecond timestamp, a 62-bit hash -- is past what a double holds
    exactly. The checked cast raised, the fallback swallowed it, and the single
    range came back: inert on precisely the columns it was written for.

    Rounding is what the placement is allowed to do. A slice reports the exact
    min and max of what landed in it, so two keys rounding together widen a
    band and cannot drop a value.
    """
    from pyiceberg.expressions.visitors import bind, rewrite_not
    from pyiceberg.io.pyarrow import expression_to_pyarrow
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    from rekep.iceberg.dataset import _key_ranges

    values = [base + i for i in range(300)] + [base + 10**17 + i for i in range(300)]
    chunk = pyarrow.table({"at": pyarrow.chunked_array([pyarrow.array(values, kind)])})
    ranges = _key_ranges(chunk, ["at"])
    assert type(ranges).__name__ == "Or", "two clusters, and one range spans both"

    schema = Schema(NestedField(1, "at", LongType(), required=True))
    bound = expression_to_pyarrow(bind(schema, rewrite_not(ranges), case_sensitive=True))
    assert chunk.filter(bound).num_rows == chunk.num_rows, "and it still covers every value"


def test_a_key_range_bands_a_type_that_cannot_be_subtracted() -> None:
    """Gaps are measured in the numbers a value was *placed* by, not in the
    values: `datetime.time` has no subtraction at all, and measuring a gap in
    the column's own values raised `TypeError` out of `_key_ranges` and out of
    every merge and insert on a table keyed by one. It takes more than
    `MERGE_RANGE_BANDS` occupied bands to reach the merging at all, which is
    why two clusters never found it.
    """
    from pyiceberg.expressions.visitors import bind, rewrite_not
    from pyiceberg.io.pyarrow import expression_to_pyarrow
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, TimeType

    from rekep.iceberg.dataset import MERGE_RANGE_BANDS, _key_ranges

    moments = [
        datetime.time(second // 3600 % 24, second // 60 % 60, second % 60)
        for cluster in range(MERGE_RANGE_BANDS * 3)
        for second in (cluster * 2_000 + offset for offset in range(15))
    ]
    chunk = pyarrow.table(
        {"at": pyarrow.chunked_array([pyarrow.array(moments, pyarrow.time64("us"))])}
    )
    ranges = _key_ranges(chunk, ["at"])
    assert type(ranges).__name__ == "Or"

    schema = Schema(NestedField(1, "at", TimeType(), required=True))
    bound = expression_to_pyarrow(bind(schema, rewrite_not(ranges), case_sensitive=True))
    assert chunk.filter(bound).num_rows == chunk.num_rows, "and it still covers every value"


def test_a_key_range_covers_a_column_it_cannot_band(dataset: IcebergDataset) -> None:
    """A string key has no arithmetic to find gaps with, and a nanosecond
    timestamp has finer bounds than an Iceberg literal can retain. One keeps
    its single range; the other contributes no term, which widens the filter --
    the only direction it may be wrong in."""
    from rekep.iceberg.dataset import _always_true, _banded, _key_ranges

    text = pyarrow.chunked_array([pyarrow.array([f"S{i:06d}" for i in range(600)])])
    assert type(_banded("symbol", text)).__name__ == "And", "one range, as before"

    nanos = pyarrow.chunked_array(
        [pyarrow.array([1_700_000_000_000_000_000 + i for i in range(600)], pyarrow.int64())]
    ).cast(pyarrow.timestamp("ns"))
    assert _banded("at", nanos) is None
    assert _banded("at", nanos.cast(pyarrow.time64("ns"))) is None
    chunk = pyarrow.Table.from_arrays([nanos], names=["at"])
    assert _key_ranges(chunk, ["at"]) == _always_true(), "no term at all, not a wrong one"


def test_a_backfill_plans_the_files_it_lands_in(tmp_path: Path) -> None:
    """The whole point: a replay of two distant bands of keys used to plan 26
    files of 30 to find the two that held them."""
    catalog = IcebergCatalog(name="bands", properties=catalog_properties(tmp_path))
    ticks = catalog.dataset("trading.ticks", field=Tick.into_field())
    schema = Tick.into_field().into_arrow_schema()
    commits = [
        pyarrow.Table.from_pydict(
            {"at": [band * 10**9 + i for i in range(400)], "payload": ["x"] * 400},
            schema=schema,
        )
        for band in range(10)
    ]
    for commit in commits:
        ticks.append_arrow(commit, commit_row_size=0)
    assert ticks.refresh().data_files().num_rows == 10

    replay = pyarrow.concat_tables([commits[1], commits[8]])
    from rekep.iceberg.dataset import _key_ranges

    plan = ticks.scan_plan(_key_ranges(replay, ["at"]))
    assert plan["files"] == 2, "the two bands, and not the eight between them"
    assert plan["skipped"] == 8
    assert ticks.insert_arrow_table(replay, True) == 0, "and every key is already stored"


def test_a_merge_past_the_limit_still_finds_every_stored_row(dataset: IcebergDataset) -> None:
    """A range is a superset, so what it plans must still hold every match."""
    rows = quotes(400)
    dataset.append_arrow(rows, commit_row_size=0)
    updated = rows.set_column(
        rows.schema.get_field_index("venue"),
        rows.schema.field("venue"),
        pyarrow.array(["XETR"] * rows.num_rows),
    )
    assert dataset.merge_arrow_table(updated, True) == (400, 0), "all updated, none inserted"
    assert dataset.read_arrow_table().num_rows == 400
    assert set(dataset.read_arrow_table().column("venue").to_pylist()) == {"XETR"}


# -- sweeping ---------------------------------------------------------------


def test_cleanup_sweeps_metadata_as_well_as_data(dataset: IcebergDataset) -> None:
    """A stream fills the metadata directory faster than the data one."""
    for index in range(8):
        dataset.append_arrow(quotes(2, f"venue{index}"), commit_row_size=0)
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
        dataset.append_arrow(quotes(2, f"venue{index}"), commit_row_size=0)
    expired = dataset.iceberg_table.snapshots()[:-1]
    manifest_lists = [local(snapshot.manifest_list) for snapshot in expired]
    assert manifest_lists and all(path.exists() for path in manifest_lists)

    dataset.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0))

    assert all(not path.exists() for path in manifest_lists)
    assert dataset.refresh().read_arrow_table().num_rows == 8


def test_every_retained_snapshot_still_reads_after_a_sweep(dataset: IcebergDataset) -> None:
    """The one thing a metadata sweep may never break."""
    for index in range(6):
        dataset.append_arrow(quotes(2, f"venue{index}"), commit_row_size=0)
    dataset.cleanup(retain=3, orphan_age=datetime.timedelta(seconds=0))
    dataset.refresh()
    for snapshot in dataset.iceberg_table.snapshots():
        assert dataset.read_arrow_table(snapshot_id=snapshot.snapshot_id).num_rows > 0


def test_a_sweep_can_leave_metadata_alone(dataset: IcebergDataset) -> None:
    for index in range(4):
        dataset.append_arrow(quotes(2, f"venue{index}"), commit_row_size=0)
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
    `C:/x` path itself, which is not a URI at all.
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
        quotes_.append_arrow(quotes(2), commit_row_size=0)
    stored = quotes_.read_arrow_table().num_rows
    location = quotes_.get_or_create_table().location()
    assert not location.startswith("file://"), "the odd spelling survived into the location"
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
        quotes_.append_arrow(quotes(2, f"v{index}"), commit_row_size=0)
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
        quotes_.append_arrow(quotes(2, f"v{index}"), commit_row_size=0)
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
        dataset.append_arrow(quotes(2, f"v{index}"), commit_row_size=0)
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
        quotes(2), commit_row_size=0
    )
    sweeper = IcebergCatalog(name="shared", properties=properties).dataset(
        "trading.quotes", field=Quote.into_field()
    )
    sweeper.get_or_create_table()  # loads the table, and caches it

    other = IcebergCatalog(name="shared", properties=properties).dataset(
        "trading.quotes", field=Quote.into_field()
    )
    for index in range(3):
        other.append_arrow(quotes(2, f"v{index}"), commit_row_size=0)
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

    dataset.append_arrow(quotes(2), commit_row_size=0)
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
    dataset.append_arrow(quotes(2), commit_row_size=0)
    dataset.optimize()
    versions = len(dataset.refresh().iceberg_table.metadata.metadata_log)
    dataset.optimize()
    assert len(dataset.refresh().iceberg_table.metadata.metadata_log) <= versions + 1


# -- sorting a commit -------------------------------------------------------


def test_sorting_a_commit_changes_the_order_not_the_rows(dataset: IcebergDataset) -> None:
    dataset.sort_by = ["size"]
    rows = quotes(6).sort_by([("size", "descending")])
    dataset.append_arrow(rows, commit_row_size=0)
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
            field=Quote.into_field().with_name(f"trading.sorted{index}"),
            catalog="test",
            properties=catalog_properties(tmp_path),
            sort_by=sort_by,
        )
        target.append_arrow(quotes(50).sort_by([("size", "descending")]), commit_row_size=0)
        found = target.read_arrow_table(row_filter="size >= 40").to_pylist()
        answers.append(sorted(found, key=str))
    assert answers[0] == answers[1]


# -- a column derived from the keys -----------------------------------------


@scalar
class Beat(Convertible):
    """A row keyed on a textual instant, partitioned on the hour it falls in."""

    at: Annotated[str, Field.primary_key()]
    """The instant, spelled out -- a key with no arithmetic to band."""

    hour: Annotated[str, Field.partition_key(derived_from="at")] = ""
    """`at` truncated to the hour -- what the data is partitioned on."""

    payload: str = "x"
    """Payload."""


def beats(hour: int, count: int = 200) -> pyarrow.Table:
    stamp = f"2026-08-14T{hour:02d}"
    return pyarrow.Table.from_pydict(
        {
            "at": [
                f"{stamp}:{index // 60:02d}:{index % 60:02d}.{index:04d}" for index in range(count)
            ],
            "hour": [stamp] * count,
            "payload": ["x"] * count,
        },
        schema=Beat.into_field().into_arrow_schema(),
    )


def test_a_derivation_is_usable_only_where_its_sources_are_keys() -> None:
    from rekep.iceberg.dataset import _derivable

    chunk = beats(0)
    declared = {"hour": ("at",), "day": ("at", "venue"), "absent": ("at",)}
    assert _derivable(chunk, ["at"], declared) == ["hour"], "day needs a key nothing joins on"
    assert _derivable(chunk, ["at", "hour"], declared) == [], "already a key, already named"
    assert _derivable(chunk, ["at"], None) == []


def test_a_derived_column_is_named_in_the_filter() -> None:
    from rekep.iceberg.dataset import _key_ranges

    chunk = beats(5)
    plain = _key_ranges(chunk, ["at"])
    named = _key_ranges(chunk, ["at"], Beat.into_field().derived_keys())
    assert "hour" not in str(plain), "the merge joins on `at` and knows nothing else"
    assert "hour" in str(named), "and `hour` is `at`, so it may say so"


def test_a_derived_column_with_nulls_contributes_no_term() -> None:
    """A term that cannot name every value would exclude a row it should match,
    and this filter is only ever allowed to be too wide."""
    from rekep.iceberg.dataset import _key_ranges

    chunk = beats(5)
    holed = chunk.set_column(
        chunk.schema.get_field_index("hour"),
        pyarrow.field("hour", pyarrow.string()),
        pyarrow.array([None] + ["2026-08-14T05"] * (chunk.num_rows - 1), pyarrow.string()),
    )
    assert "hour" not in str(_key_ranges(holed, ["at"], Beat.into_field().derived_keys()))


def test_a_replay_prunes_to_the_partitions_the_keys_fall_in(tmp_path: Path) -> None:
    """The whole point. A text key has no arithmetic to find gaps with, so two
    distant hours become one range covering every hour between them and the
    filter reads every hour between them -- while the partition column they are
    a function of names exactly the two."""
    catalog = IcebergCatalog(name="beats", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("trading.beats", field=Beat.into_field())
    for hour in range(12):
        dataset.insert_arrow_table(beats(hour), True)
    assert dataset.refresh().data_files().num_rows == 12

    from rekep.iceberg.dataset import _key_ranges

    replay = pyarrow.concat_tables([beats(2), beats(9)])
    unaware = dataset.scan_plan(_key_ranges(replay, ["at"]))
    aware = dataset.scan_plan(_key_ranges(replay, ["at"], dataset.derived_columns()))
    assert unaware["files"] == 8, "one text range, and every hour between the two"
    assert aware["files"] == 2, "the two hours, and not the six between them"
    assert dataset.insert_arrow_table(replay, True) == 0, "and every key is already stored"


def test_a_derivation_never_loses_a_row_the_merge_had_to_find(tmp_path: Path) -> None:
    """Pruning is only ever allowed to be a superset: the same merge, declared
    and not, has to update the same rows."""
    catalog = IcebergCatalog(name="same", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("trading.beats", field=Beat.into_field())
    for hour in range(6):
        dataset.insert_arrow_table(beats(hour), True)

    def repainted(colour: str) -> pyarrow.Table:
        rows = beats(3)
        return rows.set_column(
            rows.schema.get_field_index("payload"),
            rows.schema.field("payload"),
            pyarrow.array([colour] * rows.num_rows),
        )

    assert dataset.derived_columns() == {"hour": ("at",)}
    assert dataset.merge_arrow_table(repainted("y"), True) == (200, 0), "matched, none inserted"

    bare = catalog.dataset("trading.beats", field=_undeclared())
    assert bare.derived_columns() == {}
    assert bare.merge_arrow_table(repainted("z"), True) == (200, 0), "the same rows, the long way"
    held = dataset.refresh().read_arrow_table(row_filter="hour = '2026-08-14T03'")
    assert set(held.column("payload").to_pylist()) == {"z"}, "and the last write is what stands"


def _undeclared() -> StructField:
    """`Beat` with the derivation struck out, to merge the long way round."""
    plain = StructField.from_dict(Beat.into_field().into_dict())
    plain.field("hour").derived_from = None
    return plain


def test_a_table_read_back_declares_no_derivation(tmp_path: Path) -> None:
    """Iceberg records a partition spec, not why a column holds what it does."""
    catalog = IcebergCatalog(name="read", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("trading.beats", field=Beat.into_field())
    dataset.insert_arrow_table(beats(0), True)
    reread = catalog.dataset("trading.beats")
    assert reread.into_struct_field().partition_keys() == {"hour": "identity"}
    assert reread.derived_columns() == {}, "unsaid, which costs pruning and never a row"


# -- the sort order the shape declares ---------------------------------------


@scalar
class Ticked(Convertible):
    """A row the shape says is laid out in time order."""

    at: Annotated[int, Field.primary_key(), Field.sort_key()]
    """When."""

    seq: Annotated[int, Field.sort_key()] = 0
    """Second key, so the order is lexicographic and not one column."""

    payload: str = "x"
    """Payload."""


@scalar
class DescendingTick(Convertible):
    """A nullable clock physically ordered newest first."""

    day: Annotated[datetime.date, Field.partition_key()]
    at: Annotated[int | None, Field.sort_key("desc")] = None
    seq: Annotated[int, Field.primary_key(), Field.sort_key()] = 0


@scalar
class FloatingTick(Convertible):
    """A nullable floating-point sort key."""

    value: Annotated[float | None, Field.sort_key()]
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
    empty = Field.from_(pyarrow.schema([]), "t.d")
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
    assert reopened.into_struct_field().sort_keys() == {"seq": "asc", "at": "asc"}
    assert reopened.sort_fields() == [("seq", "ascending"), ("at", "ascending")]


def test_partition_staging_honours_descending_sort_and_nulls_last(tmp_path: Path) -> None:
    from pyiceberg.table.sorting import NullOrder, SortDirection

    catalog = IcebergCatalog(name="descending", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset(
        "t.descending",
        field=DescendingTick.into_field(),
    )
    dataset.append_arrow_table(descending_ticked([1, None, 3, 2]), commit_row_size=0)

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
    dataset.append_arrow_table(descending_ticked([5, None, 1]), commit_row_size=0)
    dataset.append_arrow_table(descending_ticked([4, None, 2]), commit_row_size=0)

    found = dataset.read_arrow_reader(order_by=("at", "descending"), limit=5).read_all()

    assert found.column("at").to_pylist() == [5, 4, 2, 1, None]


@pytest.mark.parametrize("special", [None, float("nan")])
def test_ordered_read_does_not_concatenate_file_local_special_tails(
    tmp_path: Path, special: float | None
) -> None:
    catalog = IcebergCatalog(name="special-read", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.special_read", field=FloatingTick.into_field())
    dataset.append_arrow_table(floating_ticked([1.0, special], 0), commit_row_size=0)
    dataset.append_arrow_table(floating_ticked([2.0, special], 2), commit_row_size=0)

    found = dataset.read_arrow_reader(order_by="value").read_all().column("value").to_pylist()

    assert found[:2] == [1.0, 2.0]
    if special is None:
        assert found[2:] == [None, None]
    else:
        assert all(math.isnan(value) for value in found[2:])


def test_a_snapshot_order_does_not_inherit_a_newer_table_direction(tmp_path: Path) -> None:
    catalog = IcebergCatalog(name="sort-evolution", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.sort_evolution", field=Ticked.into_field())
    dataset.append_arrow_table(ticked([(1, 0), (2, 0), (3, 0)]), commit_row_size=0)
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
        dataset.append_arrow_table(ticked([(1, 0), (2, 0)]), commit_row_size=0)

    assert dataset.refresh().read_arrow_table().column("at").to_pylist() == [1, 2]


def test_a_chunk_already_in_order_is_not_sorted_again(tmp_path: Path) -> None:
    """The common case on a capture, and the question is 20x cheaper than the
    answer -- so it is asked."""
    catalog = IcebergCatalog(name="sorted", properties=catalog_properties(tmp_path))
    dataset = catalog.dataset("t.a", field=Ticked.into_field())
    tidy = ticked([(1, 0), (1, 1), (2, 0), (3, 0)])
    assert dataset.sorted(tidy) is tidy, "handed straight back, not copied"
    assert dataset.sorted(ticked([(2, 0), (1, 0)])) is not tidy


@pytest.mark.parametrize(
    ("pairs", "ordered"),
    [
        ([(1, 0), (2, 0), (3, 0)], True),
        (
            [(1, 0), (1, 1), (1, 2)],
            True,
        ),
        ([(1, 1), (1, 0)], False),
        ([(2, 0), (1, 9)], False),
        ([(1, 0), (1, 0)], True),
        ([(1, 0)], True),
    ],
)
def test_sortedness_is_lexicographic_over_every_key(
    pairs: Sequence[tuple[int, int]], ordered: bool
) -> None:
    from rekep.iceberg.dataset import _in_sort_order

    assert _in_sort_order(ticked(pairs), ["at", "seq"]) is ordered


def test_sort_checks_apply_nulls_last_only_when_the_prefix_ties() -> None:
    from rekep.iceberg.dataset import _in_sort_order

    rows = ticked([(1, 0), (2, 0)])
    holed = rows.set_column(
        rows.schema.get_field_index("seq"),
        pyarrow.field("seq", pyarrow.int64()),
        pyarrow.array([None, 1], pyarrow.int64()),
    )
    assert _in_sort_order(holed, ["at", "seq"]) is True

    tied = holed.set_column(
        holed.schema.get_field_index("at"),
        holed.schema.field("at"),
        pyarrow.array([1, 1], pyarrow.int64()),
    )
    assert _in_sort_order(tied, ["at", "seq"]) is False
    assert _in_sort_order(tied.take(pyarrow.array([1, 0])), ["at", "seq"]) is True


def test_sort_checks_place_nan_after_numbers_and_before_null() -> None:
    from rekep.iceberg.dataset import _in_sort_order

    ascending = pyarrow.table({"value": [1.0, 2.0, float("nan"), None]})
    descending = pyarrow.table({"value": [2.0, 1.0, float("nan"), None]})

    assert _in_sort_order(ascending, [("value", "ascending")]) is True
    assert _in_sort_order(descending, [("value", "descending")]) is True
    assert _in_sort_order(ascending.take(pyarrow.array([2, 0, 1, 3])), ["value"]) is False


def test_an_unknown_sort_direction_is_refused() -> None:
    from rekep.iceberg.dataset import _sort_fields

    with pytest.raises(ValueError, match="unknown sort direction"):
        _sort_fields([("value", "sideways")])


def test_a_shuffled_write_lands_in_the_declared_order(tmp_path: Path) -> None:
    """The whole point: a sort order Iceberg records and the writer ignores is
    a wish. A filter can only skip a *row group*, so this is what it buys."""
    catalog = IcebergCatalog(name="layout", properties=catalog_properties(tmp_path))
    rows = 40_000
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
        dataset.refresh().append_arrow(shuffled, merge_by=True, commit_row_size=0)
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
