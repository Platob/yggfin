import datetime
import pathlib
from typing import Any

import pyarrow
import pytest

from rekep.dataset import Dataset
from rekep.models import Log, ParsedMessage
from rekep.run import RunState


class FakeTable:
    """Stands in for a `pyiceberg.table.Table`: records what it was handed.

    `has_snapshot=True` (the default) simulates a table that already has
    data -- `current_snapshot()` truthy, so `iceberg_write_arrow_reader`
    proceeds straight to the declared branch; `refs`/`snapshot_by_name`
    still report "nothing yet" (no real snapshot store behind this), so
    `_ensure_iceberg_branch` no-ops rather than trying to fork one.
    `has_snapshot=False` simulates a brand new table -- the "bootstrap on
    main first" case.
    """

    def __init__(self, has_snapshot: bool = True) -> None:
        self.appended: list[pyarrow.Table] = []
        self.branches: list[str | None] = []
        self.has_snapshot = has_snapshot

    def append(self, table: pyarrow.Table, branch: str | None = None, **_: Any) -> None:
        self.appended.append(table)
        self.branches.append(branch)

    def refs(self) -> dict[str, Any]:
        return {}

    def snapshot_by_name(self, name: str) -> None:
        return None

    def current_snapshot(self) -> Any | None:
        return object() if self.has_snapshot else None


def reader_of(*rows: list[int]) -> pyarrow.RecordBatchReader:
    schema = pyarrow.schema([("a", pyarrow.int64())])
    batches = [pyarrow.RecordBatch.from_pydict({"a": row}, schema=schema) for row in rows]
    return pyarrow.RecordBatchReader.from_batches(schema, batches)


# -- identity / schema --------------------------------------------------


def test_dataset_name_defaults_to_the_record_snake_name() -> None:
    assert Dataset(record="rekep.models.Log").dataset_name() == "log"


def test_dataset_name_override_wins() -> None:
    assert Dataset(record="rekep.models.Log", name="logs").dataset_name() == "logs"


def test_record_class_resolves_the_dotted_path() -> None:
    assert Dataset(record="rekep.models.Log").record_class() is Log


def test_record_class_refuses_a_non_record() -> None:
    with pytest.raises(TypeError, match="not a Record"):
        Dataset(record="pathlib.Path").record_class()


def test_schema_facet_carries_the_records_fields() -> None:
    facet = Dataset(record="rekep.models.Log").schema_facet()
    assert {f["name"] for f in facet["fields"]} == {f.name for f in Log.into_arrow_schema()}


def test_facets_include_the_schema() -> None:
    assert "schema" in Dataset(record="rekep.models.Log").facets()


def test_uri_is_scoped_to_the_dataset_scheme() -> None:
    dataset = Dataset(record="rekep.models.Log", name="logs", namespace="trading")
    assert dataset.uri() == "dataset://trading/logs"


def test_facets_include_the_data_source_uri() -> None:
    dataset = Dataset(record="rekep.models.Log", name="logs", namespace="trading")
    assert dataset.facets()["dataSource"] == {"uri": "dataset://trading/logs"}


# -- location: shared, direct, protocol-specific -------------------------


def test_direct_location_is_the_fallback() -> None:
    dataset = Dataset(record="rekep.models.Log", direct="s3://lake/log")
    assert dataset.location() == "s3://lake/log"
    assert dataset.location("iceberg") == "s3://lake/log"


def test_protocol_location_overrides_direct() -> None:
    dataset = Dataset(
        record="rekep.models.Log",
        direct="s3://lake/log",
        protocols={"iceberg": {"location": "s3://lake/iceberg/log"}},
    )
    assert dataset.location("iceberg") == "s3://lake/iceberg/log"
    assert dataset.location("doris") == "s3://lake/log"


def test_protocol_properties_merge_shared_and_protocol_specific() -> None:
    dataset = Dataset(
        record="rekep.models.Log",
        properties={"a": "shared", "b": "shared"},
        protocols={"iceberg": {"b": "iceberg"}},
    )
    assert dataset.protocol_properties("iceberg") == {"a": "shared", "b": "iceberg"}
    assert dataset.protocol_properties("doris") == {"a": "shared", "b": "shared"}


# -- write_arrow_reader dispatch ------------------------------------------


def test_write_arrow_reader_wraps_a_plain_batch_iterator() -> None:
    """A job's `arrow_transform` output pipes straight in -- no wrapping needed."""
    schema = Log.into_arrow_schema()
    row = {
        "url": "a",
        "unix": 1,
        "date": datetime.date(2026, 8, 14),
        "time": datetime.time(0, 0),
        "thread_name": "t",
        "driver": "d",
        "message": "m",
        "hash64": 1,
    }
    batch = pyarrow.RecordBatch.from_pylist([row], schema=schema)

    def plain_iterator():
        yield batch

    dataset = Dataset(record="rekep.models.Log")
    table = FakeTable()
    written = dataset.write_arrow_reader(plain_iterator(), format="iceberg", table=table)
    assert written == 1
    assert table.appended[0].schema.equals(schema)


def test_write_arrow_reader_refuses_an_unknown_format() -> None:
    dataset = Dataset(record="rekep.models.Log")
    with pytest.raises(ValueError, match="no 'parquet' writer"):
        dataset.write_arrow_reader(reader_of([1]), format="parquet")


def test_iceberg_write_arrow_reader_needs_a_table() -> None:
    dataset = Dataset(record="rekep.models.Log")
    with pytest.raises(NotImplementedError, match="needs table="):
        dataset.write_arrow_reader(reader_of([1]), format="iceberg")


def test_write_arrow_reader_streams_one_batch_at_a_time() -> None:
    dataset = Dataset(record="rekep.models.Log", name="logs")
    table = FakeTable()
    reader = reader_of([1, 2, 3], [4, 5])
    written = dataset.write_arrow_reader(reader, format="iceberg", table=table)
    assert written == 5
    assert len(table.appended) == 2
    assert table.appended[0].num_rows == 3
    assert table.appended[1].num_rows == 2


def test_append_defaults_to_main_branch() -> None:
    dataset = Dataset(record="rekep.models.Log")
    table = FakeTable()
    dataset.write_arrow_reader(reader_of([1]), format="iceberg", table=table)
    assert table.branches == ["main"]


def test_append_uses_the_datasets_declared_branch() -> None:
    dataset = Dataset(record="rekep.models.Log", protocols={"iceberg": {"branch": "dev"}})
    table = FakeTable()
    dataset.write_arrow_reader(reader_of([1]), format="iceberg", table=table)
    assert table.branches == ["dev"]


def test_an_explicit_branch_argument_wins_over_the_declared_one() -> None:
    dataset = Dataset(record="rekep.models.Log", protocols={"iceberg": {"branch": "dev"}})
    table = FakeTable()
    dataset.write_arrow_reader(reader_of([1]), format="iceberg", table=table, branch="hotfix")
    assert table.branches == ["hotfix"]


def test_a_brand_new_table_bootstraps_on_main_regardless_of_the_declared_branch() -> None:
    """Iceberg allows only `main` before a table has any snapshot at all --
    the very first write always lands there, whatever `branch` says."""
    dataset = Dataset(record="rekep.models.Log", protocols={"iceberg": {"branch": "dev"}})
    table = FakeTable(has_snapshot=False)
    dataset.write_arrow_reader(reader_of([1]), format="iceberg", table=table, branch="dev")
    assert table.branches == ["main"]


def test_an_unknown_iceberg_mode_is_refused() -> None:
    dataset = Dataset(record="rekep.models.Log")
    with pytest.raises(ValueError, match="no iceberg 'delete' write mode"):
        dataset.write_arrow_reader(
            reader_of([1]), format="iceberg", table=FakeTable(), mode="delete"
        )


# -- internal lineage tracking between public and private -----------------


def test_a_successful_write_emits_start_then_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = Dataset(record="rekep.models.Log", name="logs", namespace="trading")
    dataset.write_arrow_reader(reader_of([1, 2]), format="iceberg", table=FakeTable())

    events = dataset.events()
    assert [e.event_type for e in events] == [RunState.START, RunState.COMPLETE]
    assert events[0].run.run_id == events[1].run.run_id, "same run, two moments"
    assert events[0].job.namespace == "trading"
    assert events[1].outputs[0].output_facets == {"outputStatistics": {"rowCount": 2}}


def test_a_failed_write_emits_start_then_fail_and_reraises() -> None:
    dataset = Dataset(record="rekep.models.Log")

    def boom(reader: Any, **_: Any) -> int:
        raise RuntimeError("catalog unreachable")

    dataset.iceberg_write_arrow_reader = boom  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="catalog unreachable"):
        dataset.write_arrow_reader(reader_of([1]), format="iceberg")

    assert [e.event_type for e in dataset.events()] == [RunState.START, RunState.FAIL]


def test_events_are_not_shared_between_instances() -> None:
    a = Dataset(record="rekep.models.Log", name="a")
    b = Dataset(record="rekep.models.Log", name="b")
    a.write_arrow_reader(reader_of([1]), format="iceberg", table=FakeTable())
    assert a.events()
    assert b.events() == []


# -- file_write_arrow_reader: generic uri + filesystem mapping -----------


def test_file_write_needs_a_location() -> None:
    dataset = Dataset(record="rekep.models.Log")
    with pytest.raises(NotImplementedError, match="needs a location"):
        dataset.write_arrow_reader(reader_of([1]), format="file")


def test_file_write_uses_direct_as_the_fallback_location(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(record="rekep.models.Log", name="logs", direct=(tmp_path / "out").as_uri())
    written = dataset.write_arrow_reader(reader_of([1, 2, 3], [4, 5]), format="file")
    assert written == 5
    assert list((tmp_path / "out").glob("*.parquet"))


def test_file_write_uri_override_wins_over_direct(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(record="rekep.models.Log", direct="s3://elsewhere/log")
    target = tmp_path / "override"
    written = dataset.write_arrow_reader(reader_of([1, 2]), format="file", uri=target.as_uri())
    assert written == 2
    assert list(target.glob("*.parquet"))


def test_file_write_tracks_lineage_like_iceberg_does(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(record="rekep.models.Log", name="logs", direct=(tmp_path / "out").as_uri())
    dataset.write_arrow_reader(reader_of([1, 2]), format="file")
    events = dataset.events()
    assert [e.event_type for e in events] == [RunState.START, RunState.COMPLETE]
    assert events[-1].outputs[0].output_facets == {"outputStatistics": {"rowCount": 2}}


# -- deploy: autonomous, no side file needed ------------------------------


def test_into_iceberg_table_carries_record_name_namespace_location() -> None:
    dataset = Dataset(
        record="rekep.models.Log",
        name="logs",
        namespace="trading",
        protocols={"iceberg": {"location": "s3://lake/iceberg/log"}},
    )
    table = dataset.into_iceberg_table()
    assert table.record == "rekep.models.Log"
    assert table.name == "logs"
    assert table.namespace == "trading"
    assert table.location == "s3://lake/iceberg/log"


def test_into_iceberg_table_does_not_leak_location_or_branch_into_properties() -> None:
    """`location`/`branch` route the write; they are not table properties."""
    dataset = Dataset(
        record="rekep.models.Log",
        properties={"format": "parquet"},
        protocols={"iceberg": {"location": "s3://lake/log", "branch": "dev"}},
    )
    table = dataset.into_iceberg_table()
    assert table.properties == {"format": "parquet"}


def test_into_doris_table_carries_record_name_namespace_properties() -> None:
    dataset = Dataset(
        record="rekep.models.Log",
        name="logs",
        namespace="trading",
        properties={"a": "shared"},
        protocols={"doris": {"b": "doris"}},
    )
    table = dataset.into_doris_table()
    assert table.record == "rekep.models.Log"
    assert table.name == "logs"
    assert table.namespace == "trading"
    assert table.properties == {"a": "shared", "b": "doris"}


def test_deploy_dispatches_to_iceberg_or_doris() -> None:
    class FakeStack:
        def __init__(self) -> None:
            self.deployed: list[Any] = []

        def deploy_one(self, table: Any, dry_run: bool = False) -> Any:
            self.deployed.append((table, dry_run))
            return table

    dataset = Dataset(record="rekep.models.Log", name="logs")
    stack = FakeStack()
    dataset.deploy("iceberg", stack, dry_run=True)
    (table, dry_run) = stack.deployed[0]
    assert type(table).__name__ == "IcebergTable"
    assert dry_run is True


def test_deploy_refuses_an_unknown_target() -> None:
    with pytest.raises(ValueError, match="no 'parquet' deploy target"):
        Dataset(record="rekep.models.Log").deploy("parquet", stack=None)


def test_deploy_iceberg_converges_a_real_local_catalog(tmp_path: pathlib.Path) -> None:
    """No `tables/` side file anywhere: the dataset's own config is enough."""
    from rekep.iceberg import Iceberg
    from rekep.records.iceberg import IcebergCatalog, IcebergDeployment

    root = tmp_path.as_posix()
    stack = Iceberg(
        IcebergDeployment(
            catalogs=[
                IcebergCatalog(uri=f"sqlite:///{root}/cat.db", warehouse=f"file://{root}/wh")
            ],
        )
    )
    dataset = Dataset(record="rekep.models.Log", name="logs")
    dataset.deploy_iceberg(stack)
    assert stack.catalogs.connect("iceberg").table_exists("default.logs")


def _local_iceberg_stack(tmp_path: pathlib.Path) -> Any:
    from rekep.iceberg import Iceberg
    from rekep.records.iceberg import IcebergCatalog, IcebergDeployment

    root = tmp_path.as_posix()
    return Iceberg(
        IcebergDeployment(
            catalogs=[IcebergCatalog(uri=f"sqlite:///{root}/cat.db", warehouse=f"file://{root}/wh")]
        )
    )


def _parsed_message_reader(*rows: dict[str, Any]) -> pyarrow.RecordBatchReader:
    schema = ParsedMessage.into_arrow_schema()
    defaults = {"date": datetime.date(2026, 8, 14), "protocol": None, "fields": {}}
    batch = pyarrow.RecordBatch.from_pylist([{**defaults, **row} for row in rows], schema=schema)
    return pyarrow.RecordBatchReader.from_batches(schema, [batch])


def test_upsert_against_a_real_local_catalog_uses_the_primary_key(
    tmp_path: pathlib.Path,
) -> None:
    """`join_cols=None` falls back to the record's own `Arrow(key=True)`
    field -- `ParsedMessage.hash64` -- with no extra wiring."""
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record="rekep.models.ParsedMessage", name="messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())

    reader = _parsed_message_reader(
        {"url": "a", "unix": 1, "hash64": 1}, {"url": "a", "unix": 2, "hash64": 2}
    )
    dataset.write_arrow_reader(reader, format="iceberg", table=table)

    updated = _parsed_message_reader(
        {"url": "a", "unix": 1, "hash64": 1, "protocol": "FIX.4.4"},
        {"url": "a", "unix": 3, "hash64": 3},
    )
    written = dataset.write_arrow_reader(
        reader=updated, format="iceberg", table=table, mode="upsert"
    )
    assert written == 2  # one updated, one inserted

    rows = {row["hash64"]: row for row in table.scan().to_arrow().to_pylist()}
    assert set(rows) == {1, 2, 3}
    assert rows[1]["protocol"] == "FIX.4.4", "hash64=1 was updated, not duplicated"


def test_branch_write_and_read_isolate_from_main(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record="rekep.models.Log", name="logs")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())

    main_dataset = Dataset(record="rekep.models.Log", name="logs")
    schema = table.schema().as_arrow()
    row = {
        "url": "a",
        "unix": 1,
        "date": datetime.date(2026, 8, 14),
        "time": datetime.time(0, 0),
        "thread_name": "t",
        "driver": "d",
        "message": "m",
        "hash64": 1,
    }
    reader = pyarrow.RecordBatchReader.from_batches(
        schema, [pyarrow.RecordBatch.from_pylist([row], schema=schema)]
    )
    main_dataset.write_arrow_reader(reader, format="iceberg", table=table)

    dev_dataset = Dataset(record="rekep.models.Log", protocols={"iceberg": {"branch": "dev"}})
    reader = pyarrow.RecordBatchReader.from_batches(
        schema, [pyarrow.RecordBatch.from_pylist([{**row, "hash64": 2}], schema=schema)]
    )
    dev_dataset.write_arrow_reader(reader, format="iceberg", table=table)

    assert len(table.scan().to_arrow()) == 1, "main untouched by the dev branch write"
    dev_snapshot = table.refs()["dev"].snapshot_id
    assert len(table.scan(snapshot_id=dev_snapshot).to_arrow()) == 2


# -- shipped stacks -----------------------------------------------------


def test_the_shipped_datasets_declare_real_records() -> None:
    """Every `stacks/datasets/*.yaml` must actually parse and name a real record."""
    repo_datasets = pathlib.Path(__file__).parents[2] / "stacks" / "datasets"
    datasets = Dataset.load_all(repo_datasets)
    assert {d.name for d in datasets} == {"log", "parsed_messages"}
    for dataset in datasets:
        assert dataset.record_class() is not None  # raises TypeError otherwise


def test_the_shipped_log_dataset_is_branch_agnostic() -> None:
    repo_datasets = pathlib.Path(__file__).parents[2] / "stacks" / "datasets"
    (log,) = [d for d in Dataset.load_all(repo_datasets) if d.name == "log"]
    assert log.record_class() is Log
    assert log.uri() == "dataset://default/log"
    assert log.iceberg_branch() is None


def test_the_shipped_datasets_resolve_against_the_shipped_namespaces() -> None:
    """Every shipped dataset must resolve against the shipped catalogs/namespaces."""
    from rekep.records.doris import DorisDeployment
    from rekep.records.iceberg import IcebergDeployment

    repo = pathlib.Path(__file__).parents[2] / "stacks"
    datasets = Dataset.load_all(repo / "datasets")

    iceberg_deployment = IcebergDeployment.load(repo / "iceberg")
    doris_deployment = DorisDeployment.load(repo / "doris")
    for dataset in datasets:
        iceberg_deployment.namespace(dataset.into_iceberg_table().namespace)  # does not raise
        doris_deployment.namespace(dataset.into_doris_table().namespace)  # does not raise


# -- round trip -------------------------------------------------------------


def test_dataset_round_trips_through_json() -> None:
    dataset = Dataset(
        record="rekep.models.Log",
        name="logs",
        namespace="trading",
        direct="s3://lake/log",
        properties={"format": "parquet"},
        protocols={"iceberg": {"location": "s3://lake/iceberg/log"}},
    )
    assert Dataset.from_json(dataset.into_json()) == dataset
