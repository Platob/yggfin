import pathlib
from typing import Any

import pyarrow
import pytest

from rekep.dataset import Dataset
from rekep.models import Log
from rekep.run import RunState


class FakeTable:
    """Stands in for a `pyiceberg.table.Table`: records what it was handed."""

    def __init__(self) -> None:
        self.appended: list[pyarrow.Table] = []

    def append(self, table: pyarrow.Table) -> None:
        self.appended.append(table)


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


# -- shipped stacks -----------------------------------------------------


def test_the_shipped_dataset_declares_the_shipped_record() -> None:
    """`stacks/datasets/log.yaml` must actually parse and name a real record."""
    repo_datasets = pathlib.Path(__file__).parents[2] / "stacks" / "datasets"
    (dataset,) = Dataset.load_all(repo_datasets)
    assert dataset.record_class() is Log
    assert dataset.uri() == "dataset://default/log"


def test_the_shipped_dataset_resolves_against_the_shipped_namespaces() -> None:
    """The shipped dataset must resolve against the shipped catalogs/namespaces."""
    from rekep.records.doris import DorisDeployment
    from rekep.records.iceberg import IcebergDeployment

    repo = pathlib.Path(__file__).parents[2] / "stacks"
    (dataset,) = Dataset.load_all(repo / "datasets")

    iceberg_deployment = IcebergDeployment.load(repo / "iceberg")
    iceberg_deployment.namespace(dataset.into_iceberg_table().namespace)  # does not raise

    doris_deployment = DorisDeployment.load(repo / "doris")
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
