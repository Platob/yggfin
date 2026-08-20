import dataclasses
import datetime
import pathlib
from typing import Annotated, Any

import pyarrow
import pytest

from rekep import Arrow, Record, record
from rekep.dataset import Dataset
from rekep.models import Log, ParsedMessage
from rekep.run import RunState


@dataclasses.dataclass
class FakeUpsertResult:
    """What `pyiceberg.table.Table.upsert` hands back: two counts."""

    rows_updated: int
    rows_inserted: int


class FakeTable:
    """Stands in for a `pyiceberg.table.Table`: records what it was handed.

    `has_snapshot=True` (the default) simulates a table that already has
    data -- `current_snapshot()` truthy, so `iceberg_write_arrow_reader`
    proceeds straight to the declared branch and a `merge_by` really
    merges; `refs`/`snapshot_by_name` still report "nothing yet" (no real
    snapshot store behind this), so `_ensure_iceberg_branch` no-ops rather
    than trying to fork one. `has_snapshot=False` simulates a brand new
    table -- both the "bootstrap on main first" and the "nothing to merge
    into, just append" cases.
    """

    def __init__(self, has_snapshot: bool = True) -> None:
        self.appended: list[pyarrow.Table] = []
        self.upserted: list[pyarrow.Table] = []
        self.overwritten: list[pyarrow.Table] = []
        self.branches: list[str | None] = []
        self.join_cols: list[list[str] | None] = []
        self.overwrite_filters: list[Any] = []
        self.has_snapshot = has_snapshot

    def append(self, table: pyarrow.Table, branch: str | None = None, **_: Any) -> None:
        self.appended.append(table)
        self.branches.append(branch)

    def upsert(
        self,
        table: pyarrow.Table,
        join_cols: list[str] | None = None,
        branch: str | None = None,
        **_: Any,
    ) -> FakeUpsertResult:
        self.upserted.append(table)
        self.branches.append(branch)
        self.join_cols.append(join_cols)
        return FakeUpsertResult(rows_updated=0, rows_inserted=table.num_rows)

    def overwrite(
        self,
        table: pyarrow.Table,
        overwrite_filter: Any = None,
        branch: str | None = None,
        **_: Any,
    ) -> None:
        self.overwritten.append(table)
        self.branches.append(branch)
        self.overwrite_filters.append(overwrite_filter)

    def refs(self) -> dict[str, Any]:
        return {}

    def snapshot_by_name(self, name: str) -> None:
        return None

    def current_snapshot(self) -> Any | None:
        return object() if self.has_snapshot else None


@record
class Trade(Record):
    """One trade, partitioned by a transform Iceberg computes rather than stores."""

    at: Annotated[datetime.datetime, Arrow(partition="day")]
    """When it traded."""

    size: int
    """How much."""


def log_rows(*hashes: int) -> list[dict[str, Any]]:
    """One valid `Log` row per hash -- the only field the tests vary."""
    return [
        {
            "url": "a",
            "unix": value,
            "date": datetime.date(2026, 8, 14),
            "time": datetime.time(0, 0),
            "thread_name": "t",
            "driver": "d",
            "message": "m",
            "hash64": value,
        }
        for value in hashes
    ]


def reader_of(*batches: list[int]) -> pyarrow.RecordBatchReader:
    """A `Log`-shaped reader, one batch per argument."""
    schema = Log.into_arrow_schema()
    return pyarrow.RecordBatchReader.from_batches(
        schema,
        [pyarrow.RecordBatch.from_pylist(log_rows(*values), schema=schema) for values in batches],
    )


def parsed_reader(
    *rows: dict[str, Any], batch_size: int | None = None
) -> pyarrow.RecordBatchReader:
    """A `ParsedMessage`-shaped reader; each row overrides a valid default.

    `batch_size` splits the rows across batches, which is how the chunking
    tests get a stream of many small batches to accumulate.
    """
    schema = ParsedMessage.into_arrow_schema()
    defaults = {
        "url": "a",
        "unix": 1,
        "date": datetime.date(2026, 8, 14),
        "protocol": None,
        "fields": {},
    }
    filled = [{**defaults, **row} for row in rows]
    size = batch_size or max(len(filled), 1)
    batches = [
        pyarrow.RecordBatch.from_pylist(filled[i : i + size], schema=schema)
        for i in range(0, len(filled), size)
    ]
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
    batch = pyarrow.RecordBatch.from_pylist(log_rows(1), schema=schema)

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


def test_an_append_batches_the_whole_reader_into_one_commit() -> None:
    """A batch is not a unit of work in Iceberg: every call is a snapshot
    and at least one file, so batches accumulate up to `chunk_rows`."""
    dataset = Dataset(record="rekep.models.Log", name="logs")
    reader = reader_of([1, 2, 3], [4, 5])
    table = FakeTable()
    written = dataset.write_arrow_reader(reader, format="iceberg", table=table)
    assert written == 5
    assert [chunk.num_rows for chunk in table.appended] == [5]


def test_an_append_is_still_bounded_by_chunk_rows() -> None:
    dataset = Dataset(record="rekep.models.Log", name="logs")
    reader = reader_of([1, 2, 3], [4, 5], [6])
    table = FakeTable()
    written = dataset.write_arrow_reader(reader, format="iceberg", table=table, chunk_rows=3)
    assert written == 6
    assert [chunk.num_rows for chunk in table.appended] == [3, 3]


# -- reshaping onto the record schema on the way in -----------------------


def test_a_write_casts_columns_to_the_records_types() -> None:
    """int32 in, int64 out: the record's declaration is the authority."""
    dataset = Dataset(record="rekep.models.Log")
    rows = log_rows(1)
    narrow = pyarrow.RecordBatch.from_pydict(
        {
            **{name: [rows[0][name]] for name in rows[0] if name != "unix"},
            "unix": pyarrow.array([1], type=pyarrow.int32()),
        }
    )
    table = FakeTable()
    dataset.write_arrow_reader(iter([narrow]), format="iceberg", table=table)
    assert table.appended[0].schema.equals(Log.into_arrow_schema())
    assert table.appended[0].column("unix").type == pyarrow.int64()


def test_a_write_fills_a_missing_nullable_column_with_nulls() -> None:
    dataset = Dataset(record="rekep.models.ParsedMessage")
    schema = ParsedMessage.into_arrow_schema()
    partial = pyarrow.RecordBatch.from_pydict(
        {
            "url": ["a"],
            "unix": [1],
            "date": [datetime.date(2026, 8, 14)],
            "hash64": [7],
            "fields": pyarrow.array([{}], type=pyarrow.map_(pyarrow.string(), pyarrow.string())),
        }
    )
    table = FakeTable()
    dataset.write_arrow_reader(iter([partial]), format="iceberg", table=table)
    written = table.appended[0]
    assert written.schema.equals(schema)
    assert written.column("protocol").to_pylist() == [None], "nullable, so filled"


def test_a_write_refuses_a_missing_non_nullable_column() -> None:
    dataset = Dataset(record="rekep.models.Log")
    partial = pyarrow.RecordBatch.from_pydict({"url": ["a"], "unix": [1]})
    with pytest.raises(ValueError, match="'date' is missing and not nullable"):
        dataset.write_arrow_reader(iter([partial]), format="iceberg", table=FakeTable())


def test_a_write_drops_a_column_the_record_does_not_declare() -> None:
    dataset = Dataset(record="rekep.models.Log")
    rows = log_rows(1)
    extra = pyarrow.RecordBatch.from_pydict(
        {**{name: [value] for name, value in rows[0].items()}, "extra": ["ignored"]}
    )
    table = FakeTable()
    dataset.write_arrow_reader(iter([extra]), format="iceberg", table=table)
    assert "extra" not in table.appended[0].schema.names


# -- merge_by: one argument picks append or upsert ------------------------


def test_no_merge_by_appends() -> None:
    dataset = Dataset(record="rekep.models.ParsedMessage")
    table = FakeTable()
    dataset.write_arrow_reader(parsed_reader({"hash64": 1}), format="iceberg", table=table)
    assert table.appended and not table.upserted


def test_merge_by_true_upserts_on_the_records_primary_key() -> None:
    dataset = Dataset(record="rekep.models.ParsedMessage")
    table = FakeTable()
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}), format="iceberg", table=table, merge_by=True
    )
    assert table.upserted and not table.appended
    assert table.join_cols == [["hash64"]], "the Arrow(key=True) field, unasked"


def test_merge_by_a_list_upserts_on_exactly_those_columns() -> None:
    dataset = Dataset(record="rekep.models.ParsedMessage")
    table = FakeTable()
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}), format="iceberg", table=table, merge_by=["url", "unix"]
    )
    assert table.join_cols == [["url", "unix"]]


def test_merge_by_true_is_refused_without_a_primary_key() -> None:
    """`Log` declares no `Arrow(key=True)`, and guessing a join key corrupts."""
    dataset = Dataset(record="rekep.models.Log")
    with pytest.raises(ValueError, match="merge_by=True needs a primary key"):
        dataset.write_arrow_reader(
            reader_of([1]), format="iceberg", table=FakeTable(), merge_by=True
        )


def test_merge_by_is_read_from_the_datasets_own_config() -> None:
    dataset = Dataset(
        record="rekep.models.ParsedMessage", protocols={"iceberg": {"merge_by": "true"}}
    )
    table = FakeTable()
    dataset.write_arrow_reader(parsed_reader({"hash64": 1}), format="iceberg", table=table)
    assert table.join_cols == [["hash64"]], "declared in the side file, not at the call site"


def test_a_configured_column_list_merges_on_those_columns() -> None:
    dataset = Dataset(
        record="rekep.models.ParsedMessage", protocols={"iceberg": {"merge_by": "url, unix"}}
    )
    assert dataset.merge_columns() == ["url", "unix"]


def test_a_configured_false_appends() -> None:
    dataset = Dataset(
        record="rekep.models.ParsedMessage", protocols={"iceberg": {"merge_by": "false"}}
    )
    assert dataset.merge_columns() is None


def test_merge_by_is_skipped_when_the_table_has_no_snapshot_to_merge_into() -> None:
    """Nothing to merge against: every row is an insert, so just append."""
    dataset = Dataset(record="rekep.models.ParsedMessage")
    table = FakeTable(has_snapshot=False)
    written = dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}), format="iceberg", table=table, merge_by=True
    )
    assert written == 1
    assert table.appended and not table.upserted


def test_merge_chunks_are_bounded_by_chunk_rows() -> None:
    dataset = Dataset(record="rekep.models.ParsedMessage")
    table = FakeTable()
    reader = parsed_reader(*({"hash64": i} for i in range(5)), batch_size=1)
    written = dataset.write_arrow_reader(
        reader, format="iceberg", table=table, merge_by=True, chunk_rows=2
    )
    assert written == 5
    assert [chunk.num_rows for chunk in table.upserted] == [2, 2, 1]


# -- overwrite ------------------------------------------------------------


def test_overwrite_true_replaces_the_whole_table() -> None:
    dataset = Dataset(record="rekep.models.Log")
    table = FakeTable()
    written = dataset.write_arrow_reader(
        reader_of([1, 2]), format="iceberg", table=table, overwrite=True
    )
    assert written == 2
    assert table.overwritten[0].num_rows == 2
    assert table.overwrite_filters == [None], "no filter: the whole table"


def test_overwrite_with_a_filter_passes_it_through() -> None:
    dataset = Dataset(record="rekep.models.Log")
    table = FakeTable()
    dataset.write_arrow_reader(
        reader_of([1]), format="iceberg", table=table, overwrite="date = '2026-08-14'"
    )
    assert table.overwrite_filters == ["date = '2026-08-14'"]


def test_overwrite_and_merge_by_together_are_refused() -> None:
    dataset = Dataset(record="rekep.models.ParsedMessage")
    with pytest.raises(ValueError, match="two different"):
        dataset.write_arrow_reader(
            parsed_reader({"hash64": 1}),
            format="iceberg",
            table=FakeTable(),
            overwrite=True,
            merge_by=True,
        )


# -- branches -------------------------------------------------------------


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
    assert list((tmp_path / "out").rglob("*.parquet"))


def test_file_write_uri_override_wins_over_direct(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(record="rekep.models.Log", direct="s3://elsewhere/log")
    target = tmp_path / "override"
    written = dataset.write_arrow_reader(reader_of([1, 2]), format="file", uri=target.as_uri())
    assert written == 2
    assert list(target.rglob("*.parquet"))


# -- file writes partition on the record's own declaration ----------------


def test_file_write_lays_out_hive_directories_for_the_records_partitions() -> None:
    """`Log.date` is `Arrow(partition=True)`, so the file writer uses it --
    the same declaration Iceberg's partition spec is built from."""
    dataset = Dataset(record="rekep.models.Log")
    assert dataset.partition_columns() == {"date": "identity"}


def test_file_write_partitions_by_date(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(record="rekep.models.Log", direct=(tmp_path / "out").as_uri())
    dataset.write_arrow_reader(reader_of([1, 2]), format="file")
    written = list((tmp_path / "out").rglob("*.parquet"))
    assert written, "something was written"
    assert all(path.parent.name == "date=2026-08-14" for path in written)


def test_file_write_can_be_asked_for_a_flat_layout(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(record="rekep.models.Log", direct=(tmp_path / "flat").as_uri())
    dataset.write_arrow_reader(reader_of([1]), format="file", partitioning=False)
    assert list((tmp_path / "flat").glob("*.parquet")), "no partition directory"


def test_two_file_writes_append_instead_of_colliding(tmp_path: pathlib.Path) -> None:
    """`write_dataset` defaults would raise on the second `part-0.parquet`."""
    dataset = Dataset(record="rekep.models.Log", direct=(tmp_path / "out").as_uri())
    dataset.write_arrow_reader(reader_of([1]), format="file")
    dataset.write_arrow_reader(reader_of([2]), format="file")
    assert len(list((tmp_path / "out").rglob("*.parquet"))) == 2


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


def test_upsert_against_a_real_local_catalog_uses_the_primary_key(
    tmp_path: pathlib.Path,
) -> None:
    """`join_cols=None` falls back to the record's own `Arrow(key=True)`
    field -- `ParsedMessage.hash64` -- with no extra wiring."""
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record="rekep.models.ParsedMessage", name="messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())

    reader = parsed_reader(
        {"url": "a", "unix": 1, "hash64": 1}, {"url": "a", "unix": 2, "hash64": 2}
    )
    dataset.write_arrow_reader(reader, format="iceberg", table=table)

    updated = parsed_reader(
        {"url": "a", "unix": 1, "hash64": 1, "protocol": "FIX.4.4"},
        {"url": "a", "unix": 3, "hash64": 3},
    )
    written = dataset.write_arrow_reader(
        reader=updated, format="iceberg", table=table, merge_by=True
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


# -- reading: dispatch, pushdown, lineage ---------------------------------


def test_read_arrow_reader_refuses_an_unknown_format() -> None:
    dataset = Dataset(record="rekep.models.Log")
    with pytest.raises(ValueError, match="no 'parquet' reader"):
        dataset.read_arrow_reader("parquet")


def test_iceberg_read_arrow_reader_needs_a_table() -> None:
    dataset = Dataset(record="rekep.models.Log")
    with pytest.raises(NotImplementedError, match="needs table="):
        dataset.read_arrow_reader("iceberg")


def test_file_read_needs_a_location() -> None:
    dataset = Dataset(record="rekep.models.Log")
    with pytest.raises(NotImplementedError, match="needs a location"):
        dataset.read_arrow_reader("file")


def test_a_file_round_trip_keeps_the_partition_column(tmp_path: pathlib.Path) -> None:
    """Written into `date=.../`, read back with `date` still a column --
    the record's own declaration lays it out and picks it up again."""
    dataset = Dataset(record="rekep.models.Log", direct=(tmp_path / "out").as_uri())
    dataset.write_arrow_reader(reader_of([1, 2]), format="file")
    read = dataset.read_arrow_reader("file").read_all()
    assert read.num_rows == 2
    assert read.column("date").to_pylist() == [datetime.date(2026, 8, 14)] * 2


def test_a_file_read_pushes_a_filter_down(tmp_path: pathlib.Path) -> None:
    import pyarrow.compute

    dataset = Dataset(record="rekep.models.Log", direct=(tmp_path / "out").as_uri())
    dataset.write_arrow_reader(reader_of([1, 2, 3]), format="file")
    matched = dataset.read_arrow_reader(
        "file", row_filter=pyarrow.compute.field("hash64") > 1, columns=["hash64"]
    ).read_all()
    assert matched.column_names == ["hash64"]
    assert sorted(matched.column("hash64").to_pylist()) == [2, 3]


def test_a_read_tracks_lineage_when_the_stream_ends(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(record="rekep.models.Log", name="logs", direct=(tmp_path / "o").as_uri())
    dataset.write_arrow_reader(reader_of([1, 2]), format="file")
    written = len(dataset.events())

    reader = dataset.read_arrow_reader("file")
    assert [e.event_type for e in dataset.events()[written:]] == [RunState.START], "planned only"

    reader.read_all()
    events = dataset.events()[written:]
    assert [e.event_type for e in events] == [RunState.START, RunState.COMPLETE]
    assert events[0].run.run_id == events[1].run.run_id, "same run, two moments"
    assert events[-1].inputs[0].input_facets == {"inputStatistics": {"rowCount": 2}}


def test_a_read_that_is_never_consumed_leaves_its_run_open(tmp_path: pathlib.Path) -> None:
    """A lazy read cannot claim to have finished; an abandoned one did not."""
    dataset = Dataset(record="rekep.models.Log", direct=(tmp_path / "o").as_uri())
    dataset.write_arrow_reader(reader_of([1]), format="file")
    opened = len(dataset.events())
    dataset.read_arrow_reader("file")
    reads = dataset.events()[opened:]
    assert [e.event_type for e in reads] == [RunState.START]


# -- reading an iceberg table: real catalog, real pushdown ----------------


def test_an_iceberg_read_filters_and_projects(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record="rekep.models.ParsedMessage", name="messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    dataset.write_arrow_reader(
        parsed_reader(
            {"hash64": 1, "date": datetime.date(2026, 8, 14)},
            {"hash64": 2, "date": datetime.date(2026, 8, 15)},
        ),
        format="iceberg",
        table=table,
    )
    table.refresh()

    matched = dataset.read_arrow_reader(
        "iceberg", table=table, row_filter="date = '2026-08-14'", columns=["hash64"]
    ).read_all()
    assert matched.column_names == ["hash64"]
    assert matched.column("hash64").to_pylist() == [1]


def test_an_iceberg_read_falls_back_to_main_for_a_branch_that_does_not_exist(
    tmp_path: pathlib.Path,
) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record="rekep.models.Log", name="logs")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    dataset.write_arrow_reader(reader_of([1]), format="iceberg", table=table)
    table.refresh()

    unborn = Dataset(
        record="rekep.models.Log", name="logs", protocols={"iceberg": {"branch": "nope"}}
    )
    assert unborn.read_arrow_reader("iceberg", table=table).read_all().num_rows == 1


# -- iceberg maintenance: compaction, expiry, publish ---------------------


def _crowd(dataset: Dataset, table: Any, days: list[int]) -> None:
    """One separate write per row, which is what leaves many small files."""
    for index, day in enumerate(days):
        dataset.write_arrow_reader(
            parsed_reader({"hash64": index, "date": datetime.date(2026, 8, day)}),
            format="iceberg",
            table=table,
        )
    table.refresh()


def test_compaction_rewrites_crowded_partitions_and_keeps_every_row(
    tmp_path: pathlib.Path,
) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record="rekep.models.ParsedMessage", name="messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    _crowd(dataset, table, [14, 15] * 3)

    assert table.inspect.data_files().num_rows == 6, "one file per write, two partitions"
    report = dataset.iceberg_compact(table=table, min_input_files=3)
    table.refresh()

    assert report["compacted"] is True
    assert report["files"] == 6
    assert report["rows"] == 6
    assert len(report["partitions"]) == 2
    assert table.inspect.data_files().num_rows == 2, "one file per partition now"
    assert table.scan().to_arrow().num_rows == 6, "same rows, new layout"


def test_compaction_leaves_partitions_below_the_threshold_alone(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record="rekep.models.ParsedMessage", name="messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    _crowd(dataset, table, [14, 15])

    report = dataset.iceberg_compact(table=table, min_input_files=3)
    assert report == {"partitions": [], "files": 0, "rows": 0, "compacted": False}
    assert table.inspect.data_files().num_rows == 2, "untouched"


def test_compaction_dry_run_reports_without_writing(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record="rekep.models.ParsedMessage", name="messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    _crowd(dataset, table, [14] * 4)

    report = dataset.iceberg_compact(table=table, min_input_files=3, dry_run=True)
    table.refresh()
    assert report["compacted"] is False
    assert report["files"] == 4
    assert table.inspect.data_files().num_rows == 4, "nothing rewritten"


def test_compaction_of_an_empty_table_does_nothing(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record="rekep.models.ParsedMessage", name="messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    assert dataset.iceberg_compact(table=table)["compacted"] is False


def test_expiring_snapshots_frees_history_but_not_data(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record="rekep.models.ParsedMessage", name="messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    _crowd(dataset, table, [14] * 4)

    everything = datetime.timedelta(seconds=-1)  # a cutoff in the future: every snapshot is old
    planned = dataset.iceberg_expire_snapshots(table=table, older_than=everything, dry_run=True)
    table.refresh()
    assert len(planned) == 3, "all but the one main still points at"
    assert len(table.snapshots()) == 4, "dry run wrote nothing"

    expired = dataset.iceberg_expire_snapshots(table=table, older_than=everything)
    table.refresh()
    assert expired == planned
    assert len(table.snapshots()) == 1
    assert table.scan().to_arrow().num_rows == 4, "the data is still all there"


def test_nothing_old_enough_expires_nothing(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record="rekep.models.ParsedMessage", name="messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    _crowd(dataset, table, [14] * 3)
    assert (
        dataset.iceberg_expire_snapshots(table=table, older_than=datetime.timedelta(days=7)) == []
    )


def test_publish_fast_forwards_main_onto_the_branch(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    trunk = Dataset(record="rekep.models.ParsedMessage", name="messages")
    trunk.deploy_iceberg(stack)
    table = stack.tables.get(trunk.into_iceberg_table())
    trunk.write_arrow_reader(parsed_reader({"hash64": 1}), format="iceberg", table=table)
    table.refresh()

    dev = Dataset(
        record="rekep.models.ParsedMessage",
        name="messages",
        protocols={"iceberg": {"branch": "dev"}},
    )
    dev.write_arrow_reader(parsed_reader({"hash64": 2}), format="iceberg", table=table)
    table.refresh()
    assert table.scan().to_arrow().num_rows == 1, "main untouched while the branch iterates"

    published = dev.iceberg_publish(table=table)
    table.refresh()
    assert published == table.refs()["dev"].snapshot_id
    assert table.scan().to_arrow().num_rows == 2, "main now carries the branch's work"


def test_publishing_main_onto_itself_is_refused(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(record="rekep.models.Log")
    with pytest.raises(ValueError, match="needs a branch other than 'main'"):
        dataset.iceberg_publish(table=FakeTable())


def test_publishing_a_branch_with_no_snapshot_yet_does_nothing() -> None:
    dataset = Dataset(record="rekep.models.Log", protocols={"iceberg": {"branch": "dev"}})
    assert dataset.iceberg_publish(table=FakeTable()) is None


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


def test_compaction_refuses_a_computed_partition_transform(tmp_path: pathlib.Path) -> None:
    """A `day` partition value is a day number, not a column value -- no
    predicate on the source column can name it, so it is refused not guessed.

    Built against a real deployed table's spec and schema; the write itself
    is not exercised, since applying a computed transform needs pyiceberg's
    optional `pyiceberg-core` extra.
    """
    from rekep.dataset import _partition_filter

    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record=f"{__name__}.Trade", name="trades")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    assert [field.name for field in table.spec().fields] == ["at_day"], "not shadowing `at`"

    with pytest.raises(ValueError, match="uses the day.* transform"):
        _partition_filter(table, [{"at_day": 20679}])


def test_a_partition_filter_matches_identity_partitions(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(record="rekep.models.Log", name="logs")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())

    from pyiceberg.expressions import EqualTo, Or

    from rekep.dataset import _partition_filter

    day = datetime.date(2026, 8, 14)
    assert _partition_filter(table, [{"date": day}]) == EqualTo("date", day)
    both = _partition_filter(table, [{"date": day}, {"date": datetime.date(2026, 8, 15)}])
    assert isinstance(both, Or)


def test_an_unpartitioned_table_filters_on_nothing() -> None:
    from rekep.dataset import _partition_filter

    assert _partition_filter(FakeTable(), [{}]) is None
