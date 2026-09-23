"""Atomic predicate replacement through a bounded reader and real local catalog."""

import datetime
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Annotated, Any

import pyarrow
import pyarrow.fs
import pytest
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

from rekep import Convertible, scalar
from rekep.fields import field_of, partition_key, primary_key, sort_key
from rekep.iceberg import IcebergCatalog, IcebergDataset

from ..conftest import catalog_properties

pytestmark = pytest.mark.integration
START = datetime.datetime(2026, 9, 23, 10, tzinfo=datetime.timezone.utc)


@scalar
class Event(Convertible):
    """An identified event in an hourly partition."""

    curruuid: Annotated[str, primary_key()]
    currunix: Annotated[datetime.datetime, partition_key("hour"), sort_key()]


@pytest.fixture
def dataset(tmp_path: Path) -> IcebergDataset:
    return IcebergCatalog(name="window", properties=catalog_properties(tmp_path)).dataset(
        "market.events", field=Event.into_field()
    )


def _rows(*values: tuple[int, int]) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {
            "curruuid": [f"00000000-0000-0000-0000-{identity:012}" for identity, _ in values],
            "currunix": [START + datetime.timedelta(minutes=minute) for _, minute in values],
        },
        schema=Event.into_field().into_arrow_schema(),
    )


def _window() -> Any:
    return And(
        GreaterThanOrEqual("currunix", START + datetime.timedelta(minutes=20)),
        LessThan("currunix", START + datetime.timedelta(minutes=40)),
    )


def _stored(dataset: IcebergDataset, branch: str | None = None) -> list[str]:
    return sorted(dataset.read_arrow_table(branch=branch).column("curruuid").to_pylist())


def _identities(*values: int) -> list[str]:
    return sorted(f"00000000-0000-0000-0000-{value:012}" for value in values)


def _artifacts(dataset: IcebergDataset) -> set[Path]:
    root = Path(pyarrow.fs.FileSystem.from_uri(dataset.iceberg_table.location())[1])
    return {path for path in root.rglob("*") if path.is_file()}


def test_replay_replaces_changed_identities_without_erasing_the_rest_of_the_hour(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(_rows((1, 19), (2, 20), (3, 39), (4, 40)))
    for _ in range(2):
        before = dataset.snapshots().num_rows
        assert (
            dataset.overwrite_arrow_reader(
                _rows((20, 20), (30, 39)).to_reader(), row_filter=_window()
            )
            == 2
        )
        assert dataset.snapshots().num_rows == before + 1
        assert _stored(dataset) == _identities(1, 20, 30, 4)


def test_empty_output_removes_the_window_and_preserves_its_neighbours(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(_rows((1, 19), (2, 20), (3, 39), (4, 40)))
    before = dataset.snapshots().num_rows
    assert dataset.overwrite_arrow_reader(_rows().to_reader(), row_filter=_window()) == 0
    assert dataset.snapshots().num_rows == before + 1
    assert _stored(dataset) == _identities(1, 4)


@pytest.mark.parametrize("empty", [False, True])
def test_a_missing_table_gets_one_predicate_snapshot(dataset: IcebergDataset, empty: bool) -> None:
    given = _rows() if empty else _rows((1, 20))
    assert dataset.overwrite_arrow_reader(given.to_reader(), row_filter=_window()) == given.num_rows
    assert dataset.snapshots().num_rows == 1
    assert _stored(dataset) == ([] if empty else _identities(1))


def test_a_new_table_stages_one_file_for_a_chunk_below_the_target_size(
    dataset: IcebergDataset,
) -> None:
    dataset.table_properties["write.target-file-size-bytes"] = "4096"
    given = _rows(*((identity, 20 + identity % 20) for identity in range(32)))
    assert (
        dataset.overwrite_arrow_reader(
            given.to_reader(max_chunksize=4),
            row_filter=_window(),
            commit_row_size=32,
            commit_batch_num=32,
        )
        == 32
    )
    assert dataset.data_files().column("record_count").to_pylist() == [32]


@pytest.mark.parametrize("minute", [19, 40])
def test_an_outside_row_in_a_later_chunk_aborts_the_whole_replacement(
    dataset: IcebergDataset, minute: int
) -> None:
    dataset.append_arrow_table(_rows((1, 25)))
    snapshot = dataset.iceberg_table.current_snapshot().snapshot_id
    before = _artifacts(dataset)
    with pytest.raises(ValueError, match="outside.*row_filter|row_filter.*outside"):
        dataset.overwrite_arrow_reader(
            _rows((2, 25), (3, minute)).to_reader(max_chunksize=1),
            row_filter=_window(),
            commit_batch_num=1,
        )
    assert dataset.refresh().iceberg_table.current_snapshot().snapshot_id == snapshot
    assert _stored(dataset) == _identities(1)
    assert _artifacts(dataset) == before


def test_a_source_failure_after_a_staged_chunk_publishes_nothing(dataset: IcebergDataset) -> None:
    dataset.append_arrow_table(_rows((1, 25)))
    snapshot = dataset.iceberg_table.current_snapshot().snapshot_id
    before = _artifacts(dataset)

    def failing() -> Iterator[pyarrow.RecordBatch]:
        yield _rows((2, 25)).to_batches()[0]
        raise RuntimeError("source failed after staging")

    source = pyarrow.RecordBatchReader.from_batches(_rows().schema, failing())
    with pytest.raises(Exception, match="source failed after staging"):
        dataset.overwrite_arrow_reader(source, row_filter=_window(), commit_batch_num=1)
    assert dataset.refresh().iceberg_table.current_snapshot().snapshot_id == snapshot
    assert _stored(dataset) == _identities(1)
    assert _artifacts(dataset) == before


def test_multiple_bounded_chunks_publish_one_snapshot_after_the_reader_finishes(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(_rows((1, 25)))
    snapshot = dataset.iceberg_table.current_snapshot().snapshot_id
    before = _artifacts(dataset)

    def source() -> Iterator[pyarrow.RecordBatch]:
        for identity, minute in [(2, 21), (3, 25), (4, 39)]:
            assert dataset.iceberg_table.current_snapshot().snapshot_id == snapshot
            if identity > 2:
                assert _artifacts(dataset) - before, "earlier chunks are staged, not held"
            yield _rows((identity, minute)).to_batches()[0]

    reader = pyarrow.RecordBatchReader.from_batches(_rows().schema, source())
    snapshots = dataset.snapshots().num_rows
    assert (
        dataset.overwrite_arrow_reader(
            reader, row_filter=_window(), commit_row_size=1, commit_batch_num=1
        )
        == 3
    )
    assert dataset.snapshots().num_rows == snapshots + 1
    assert _stored(dataset) == _identities(2, 3, 4)


def test_predicate_replacement_keeps_source_duplicates_and_ignores_merge_keys(
    dataset: IcebergDataset,
) -> None:
    given = _rows((1, 25), (1, 25))
    assert (
        dataset.overwrite_arrow_reader(
            given.to_reader(), row_filter=_window(), merge_by=["not_a_column"]
        )
        == 2
    )
    assert _stored(dataset) == _identities(1, 1)


def test_an_unpartitioned_nullable_clock_preserves_nulls_and_refuses_them_as_input(
    tmp_path: Path,
) -> None:
    schema = pyarrow.schema(
        [
            pyarrow.field("curruuid", pyarrow.string(), nullable=False),
            pyarrow.field("currunix", pyarrow.timestamp("us", "UTC")),
        ]
    )
    dataset = IcebergCatalog(name="nullable", properties=catalog_properties(tmp_path)).dataset(
        "market.events", field=field_of(schema)
    )
    undated = pyarrow.Table.from_pydict(
        {"curruuid": _identities(1), "currunix": [None]}, schema=schema
    )
    dataset.append_arrow_table(pyarrow.concat_tables([undated, _rows((2, 25)).cast(schema)]))
    assert (
        dataset.overwrite_arrow_reader(
            _rows((3, 30)).to_reader(), row_filter=_window(), merge_by=False
        )
        == 1
    )
    assert _stored(dataset) == _identities(1, 3)
    snapshot = dataset.iceberg_table.current_snapshot().snapshot_id
    with pytest.raises(ValueError, match="outside.*row_filter|row_filter.*outside"):
        dataset.overwrite_arrow_reader(undated.to_reader(), row_filter=_window())
    assert dataset.iceberg_table.current_snapshot().snapshot_id == snapshot


def test_predicate_replacement_and_properties_stay_on_the_selected_branch(
    dataset: IcebergDataset,
) -> None:
    dataset.append_arrow_table(_rows((1, 19), (2, 25), (3, 40)))
    dataset.create_branch("work")
    main = dataset.iceberg_table.current_snapshot().snapshot_id
    assert (
        dataset.overwrite_arrow_reader(
            _rows((4, 30)).to_reader(),
            row_filter=_window(),
            branch="work",
            properties={"window": "20:40"},
        )
        == 1
    )
    assert dataset.iceberg_table.current_snapshot().snapshot_id == main
    assert _stored(dataset) == _identities(1, 2, 3)
    assert _stored(dataset, "work") == _identities(1, 3, 4)
    head = dataset.iceberg_table.snapshot_by_name("work")
    assert head.summary["window"] == "20:40"


def _beaten_once(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, concurrent: Callable[[], None]
) -> None:
    original = dataset.catalog.commit_table
    beaten = False

    def commit(*args: Any, **kwargs: Any) -> Any:
        nonlocal beaten
        if not beaten:
            beaten = True
            concurrent()
            raise CommitFailedException("another writer won")
        return original(*args, **kwargs)

    monkeypatch.setattr(dataset.catalog, "commit_table", commit)


@pytest.mark.parametrize("empty", [False, True])
def test_a_concurrent_addition_in_an_initially_empty_window_conflicts(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch, empty: bool
) -> None:
    dataset.append_arrow_table(_rows((1, 10)))
    another = IcebergCatalog(
        name=dataset.catalog_name, properties=dataset.catalog_properties
    ).dataset(dataset.identifier, field=dataset.field)
    _beaten_once(dataset, monkeypatch, lambda: another.append_arrow_table(_rows((2, 25))))
    given = _rows() if empty else _rows((3, 30))
    with pytest.raises(CommitFailedException, match="changed since this write was planned"):
        dataset.overwrite_arrow_reader(given.to_reader(), row_filter=_window())
    assert _stored(dataset.refresh()) == _identities(1, 2)


def test_a_concurrent_addition_outside_the_window_survives_the_retry(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset.append_arrow_table(_rows((1, 25)))
    another = IcebergCatalog(
        name=dataset.catalog_name, properties=dataset.catalog_properties
    ).dataset(dataset.identifier, field=dataset.field)
    _beaten_once(dataset, monkeypatch, lambda: another.append_arrow_table(_rows((2, 40))))
    assert dataset.overwrite_arrow_reader(_rows((3, 30)).to_reader(), row_filter=_window()) == 1
    assert _stored(dataset.refresh()) == _identities(2, 3)


def test_a_lost_acknowledgement_keeps_the_exact_snapshot_addressable_by_its_properties(
    dataset: IcebergDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import Transaction

    dataset.append_arrow_table(_rows((1, 25)))
    another = IcebergCatalog(
        name=dataset.catalog_name, properties=dataset.catalog_properties
    ).dataset(dataset.identifier, field=dataset.field)
    commit = Transaction.commit_transaction
    committed: list[int] = []

    def interrupted(transaction: Transaction) -> Any:
        result = commit(transaction)
        if not committed:
            committed.append(dataset.iceberg_table.current_snapshot().snapshot_id)
            another.append_arrow_table(_rows((3, 50)))
            raise TimeoutError("acknowledgement lost after the next writer landed")
        return result

    monkeypatch.setattr(Transaction, "commit_transaction", interrupted)
    assert (
        dataset.overwrite_arrow_reader(
            _rows((2, 30)).to_reader(),
            row_filter=_window(),
            properties={"window.run": "this-replacement"},
        )
        == 1
    )
    table = dataset.iceberg_table
    exact = [
        snapshot.snapshot_id
        for snapshot in table.metadata.snapshots
        if snapshot.summary.get("window.run") == "this-replacement"
    ]
    assert exact == committed
    assert table.current_snapshot().snapshot_id != exact[0]
    historical = dataset.read_arrow_table(snapshot_id=exact[0])
    assert historical.column("curruuid").to_pylist() == _identities(2)
    assert _stored(dataset) == _identities(2, 3)
