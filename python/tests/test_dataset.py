from __future__ import annotations

import dataclasses
import datetime
import itertools
import pathlib
import types
from typing import Annotated, Any

import pyarrow
import pytest

from rekep import Arrow, Record, record
from rekep.dataset import Dataset
from rekep.models import Log, ParsedMessage
from rekep.records.arrow import FIELD_ID_KEY
from rekep.records.arrow import _max_field_id as max_field_id


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

    def __init__(self, has_snapshot: bool = True, record: type[Record] = Log) -> None:
        #: This table's own Arrow schema: the record it was created from.
        #: `union_by_name` extends it the way a real table's does,
        #: **assigning its own field ids** rather than keeping the incoming
        #: ones, which is the behaviour the write path has to survive.
        self.arrow = record.into_arrow_schema()
        self.schema_version = 0
        self.unioned: list[pyarrow.Schema] = []
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

    def schema(self) -> Any:
        """Enough of a `pyiceberg.schema.Schema` for the write path to use."""
        arrow = self.arrow
        return types.SimpleNamespace(
            fields=[
                types.SimpleNamespace(name=field.name, required=not field.nullable)
                for field in arrow
            ],
            schema_id=self.schema_version,
            as_arrow=lambda: arrow,
        )

    def update_schema(self) -> Any:
        """A context manager whose `union_by_name` adds the names it is given.

        It renumbers the additions from this table's own highest field id,
        exactly as pyiceberg does -- the ids the caller stamped are dropped.
        """
        table = self

        class Update:
            def __enter__(self) -> Any:
                return self

            def __exit__(self, *_: Any) -> None:
                return None

            def union_by_name(self, schema: pyarrow.Schema) -> None:
                table.unioned.append(schema)
                counter = itertools.count(max_field_id(table.arrow) + 1)
                added = [
                    field.with_nullable(True).with_metadata(
                        {FIELD_ID_KEY: str(next(counter)).encode()}
                    )
                    for field in schema
                    if field.name not in table.arrow.names
                ]
                if added:
                    table.arrow = pyarrow.schema([*table.arrow, *added])
                    table.schema_version += 1

        return Update()

    def refresh(self) -> FakeTable:
        return self

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
    *rows: dict[str, Any],
    batch_size: int | None = None,
    extra: dict[str, Any] | None = None,
) -> pyarrow.RecordBatchReader:
    """A `ParsedMessage`-shaped reader; each row overrides a valid default.

    `batch_size` splits the rows across batches, which is how the chunking
    tests get a stream of many small batches to accumulate. `extra` adds
    columns the record does *not* declare, the same value on every row --
    what a source that grew a field looks like arriving.
    """
    schema = ParsedMessage.into_arrow_schema()
    defaults = {
        "url": "a",
        "unix": 1,
        "date": datetime.date(2026, 8, 14),
        "protocol": None,
        "fields": {},
    }
    filled = [{**defaults, **row, **(extra or {})} for row in rows]
    if extra:
        schema = pyarrow.schema(
            [
                *schema,
                *(pyarrow.field(name, pyarrow.scalar(value).type) for name, value in extra.items()),
            ]
        )
    size = batch_size or max(len(filled), 1)
    batches = [
        pyarrow.RecordBatch.from_pylist(filled[i : i + size], schema=schema)
        for i in range(0, len(filled), size)
    ]
    return pyarrow.RecordBatchReader.from_batches(schema, batches)


# -- identity / schema --------------------------------------------------


def test_dataset_name_defaults_to_the_record_snake_name() -> None:
    assert Dataset(schema="rekep:///records/log").dataset_name() == "log"


def test_dataset_name_override_wins() -> None:
    assert (
        Dataset(schema="rekep:///records/log", uri="rekep:///datasets/logs").dataset_name()
        == "logs"
    )


def test_record_class_resolves_the_record_uri() -> None:
    assert Dataset(schema="rekep:///records/log").record_class() is Log


def test_record_class_refuses_an_undeclared_record() -> None:
    with pytest.raises(KeyError, match="no record named 'nowhere'"):
        Dataset(schema="rekep:///records/nowhere").record_class()


def test_schema_facet_carries_the_records_fields() -> None:
    facet = Dataset(schema="rekep:///records/log").schema_facet()
    assert {f["name"] for f in facet["fields"]} == {f.name for f in Log.into_arrow_schema()}


def test_facets_include_the_schema() -> None:
    assert "schema" in Dataset(schema="rekep:///records/log").facets()


def test_uri_is_scoped_to_the_dataset_scheme() -> None:
    dataset = Dataset(schema="rekep:///records/log", uri="rekep:///datasets/trading/logs")
    assert str(dataset.resource_uri()) == "rekep:///datasets/trading/logs"


def test_facets_include_the_data_source_uri() -> None:
    dataset = Dataset(schema="rekep:///records/log", uri="rekep:///datasets/trading/logs")
    assert dataset.facets()["dataSource"] == {"uri": "rekep:///datasets/trading/logs"}


# -- location: shared, direct, protocol-specific -------------------------


def test_direct_location_is_the_fallback() -> None:
    dataset = Dataset(schema="rekep:///records/log", direct="s3://lake/log")
    assert dataset.location() == "s3://lake/log"
    assert dataset.location("iceberg") == "s3://lake/log"


def test_protocol_location_overrides_direct() -> None:
    dataset = Dataset(
        schema="rekep:///records/log",
        direct="s3://lake/log",
        protocols={"iceberg": {"location": "s3://lake/iceberg/log"}},
    )
    assert dataset.location("iceberg") == "s3://lake/iceberg/log"
    assert dataset.location("doris") == "s3://lake/log"


def test_protocol_properties_merge_shared_and_protocol_specific() -> None:
    dataset = Dataset(
        schema="rekep:///records/log",
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

    dataset = Dataset(schema="rekep:///records/log")
    table = FakeTable()
    written = dataset.write_arrow_reader(plain_iterator(), format="iceberg", table=table)
    assert written == 1
    assert table.appended[0].schema.equals(schema)


def test_write_arrow_reader_refuses_an_unknown_format() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    with pytest.raises(ValueError, match="no 'parquet' writer"):
        dataset.write_arrow_reader(reader_of([1]), format="parquet")


def test_iceberg_write_arrow_reader_needs_a_table() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    with pytest.raises(NotImplementedError, match="needs table="):
        dataset.write_arrow_reader(reader_of([1]), format="iceberg")


def test_an_append_batches_the_whole_reader_into_one_commit() -> None:
    """A batch is not a unit of work in Iceberg: every call is a snapshot
    and at least one file, so batches accumulate up to `commit_row_size`."""
    dataset = Dataset(schema="rekep:///records/log", uri="rekep:///datasets/logs")
    reader = reader_of([1, 2, 3], [4, 5])
    table = FakeTable()
    written = dataset.write_arrow_reader(reader, format="iceberg", table=table)
    assert written == 5
    assert [chunk.num_rows for chunk in table.appended] == [5]


def test_an_append_is_still_bounded_by_commit_row_size() -> None:
    dataset = Dataset(schema="rekep:///records/log", uri="rekep:///datasets/logs")
    reader = reader_of([1, 2, 3], [4, 5], [6])
    table = FakeTable()
    written = dataset.write_arrow_reader(reader, format="iceberg", table=table, commit_row_size=3)
    assert written == 6
    assert [chunk.num_rows for chunk in table.appended] == [3, 3]


def test_the_side_file_can_declare_the_commit_row_size() -> None:
    """How much a write commits at once is a property of the data, so the
    dataset declares it once instead of every call site remembering it."""
    dataset = Dataset(
        schema="rekep:///records/log",
        uri="rekep:///datasets/logs",
        protocols={"iceberg": {"commit_row_size": "2"}},
    )
    assert dataset.commit_row_size("iceberg") == 2
    table = FakeTable()
    dataset.write_arrow_reader(reader_of([1], [2], [3], [4], [5]), format="iceberg", table=table)
    assert [chunk.num_rows for chunk in table.appended] == [2, 2, 1]


def test_an_undeclared_commit_row_size_is_the_protocols_own_answer() -> None:
    """None, not a number: Iceberg commits whatever `COMMIT_ROW_SIZE` says,
    and a file write lets Arrow size its own files."""
    assert Dataset(schema="rekep:///records/log").commit_row_size("iceberg") is None


def test_a_call_site_still_wins_over_the_declaration() -> None:
    dataset = Dataset(
        schema="rekep:///records/log",
        uri="rekep:///datasets/logs",
        protocols={"iceberg": {"commit_row_size": "2"}},
    )
    table = FakeTable()
    dataset.write_arrow_reader(
        reader_of([1], [2], [3]), format="iceberg", table=table, commit_row_size=3
    )
    assert [chunk.num_rows for chunk in table.appended] == [3]


def test_commit_row_size_never_lands_on_the_table_as_a_property() -> None:
    """It routes a write; it does not describe the data."""
    dataset = Dataset(
        schema="rekep:///records/log",
        protocols={"iceberg": {"commit_row_size": "2", "owner": "eng"}},
    )
    assert dataset.table_properties("iceberg") == {"owner": "eng"}


# -- reshaping onto the record schema on the way in -----------------------


def test_a_write_casts_columns_to_the_records_types() -> None:
    """int32 in, int64 out: the record's declaration is the authority."""
    dataset = Dataset(schema="rekep:///records/log")
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
    dataset = Dataset(schema="rekep:///records/parsed_message")
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
    table = FakeTable(record=ParsedMessage)
    dataset.write_arrow_reader(iter([partial]), format="iceberg", table=table)
    written = table.appended[0]
    assert written.schema.equals(schema)
    assert written.column("protocol").to_pylist() == [None], "nullable, so filled"


def test_a_write_refuses_a_missing_non_nullable_column() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    partial = pyarrow.RecordBatch.from_pydict({"url": ["a"], "unix": [1]})
    with pytest.raises(ValueError, match="'date' is missing and not nullable"):
        dataset.write_arrow_reader(iter([partial]), format="iceberg", table=FakeTable())


def test_a_write_drops_a_column_the_record_does_not_declare() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    rows = log_rows(1)
    extra = pyarrow.RecordBatch.from_pydict(
        {**{name: [value] for name, value in rows[0].items()}, "extra": ["ignored"]}
    )
    table = FakeTable()
    dataset.write_arrow_reader(iter([extra]), format="iceberg", table=table)
    assert "extra" not in table.appended[0].schema.names


# -- merge_by: one argument picks append or upsert ------------------------


def test_no_merge_by_appends() -> None:
    dataset = Dataset(schema="rekep:///records/parsed_message")
    table = FakeTable(record=ParsedMessage)
    dataset.write_arrow_reader(parsed_reader({"hash64": 1}), format="iceberg", table=table)
    assert table.appended and not table.upserted


def test_merge_by_true_upserts_on_the_records_primary_key() -> None:
    dataset = Dataset(schema="rekep:///records/parsed_message")
    table = FakeTable(record=ParsedMessage)
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}), format="iceberg", table=table, merge_by=True
    )
    assert table.upserted and not table.appended
    assert table.join_cols == [["unix", "hash64"]], "both Arrow(key=True) fields, unasked"


def test_merge_by_a_list_upserts_on_exactly_those_columns() -> None:
    dataset = Dataset(schema="rekep:///records/parsed_message")
    table = FakeTable(record=ParsedMessage)
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}), format="iceberg", table=table, merge_by=["url", "unix"]
    )
    assert table.join_cols == [["url", "unix"]]


def test_merge_by_true_is_refused_without_a_primary_key() -> None:
    """Guessing a join key is how a merge silently corrupts a table."""

    @record
    class Keyless(Record):
        """No key to merge on."""

        value: int
        """A value."""

    dataset = Dataset(schema=str(Keyless.record_uri()))
    dataset.record_class = lambda: Keyless  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="merge_by=True needs a primary key"):
        dataset.merge_columns(True)


def test_merge_by_is_read_from_the_datasets_own_config() -> None:
    dataset = Dataset(
        schema="rekep:///records/parsed_message", protocols={"iceberg": {"merge_by": "true"}}
    )
    table = FakeTable(record=ParsedMessage)
    dataset.write_arrow_reader(parsed_reader({"hash64": 1}), format="iceberg", table=table)
    assert table.join_cols == [["unix", "hash64"]], (
        "declared in the side file, not at the call site"
    )


def test_a_configured_column_list_merges_on_those_columns() -> None:
    dataset = Dataset(
        schema="rekep:///records/parsed_message", protocols={"iceberg": {"merge_by": "url, unix"}}
    )
    assert dataset.merge_columns() == ["url", "unix"]


def test_a_configured_false_appends() -> None:
    dataset = Dataset(
        schema="rekep:///records/parsed_message", protocols={"iceberg": {"merge_by": "false"}}
    )
    assert dataset.merge_columns() is None


def test_merge_by_is_skipped_when_the_table_has_no_snapshot_to_merge_into() -> None:
    """Nothing to merge against: every row is an insert, so just append."""
    dataset = Dataset(schema="rekep:///records/parsed_message")
    table = FakeTable(has_snapshot=False, record=ParsedMessage)
    written = dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}), format="iceberg", table=table, merge_by=True
    )
    assert written == 1
    assert table.appended and not table.upserted


def test_merge_chunks_are_bounded_by_commit_row_size() -> None:
    dataset = Dataset(schema="rekep:///records/parsed_message")
    table = FakeTable(record=ParsedMessage)
    reader = parsed_reader(*({"hash64": i} for i in range(5)), batch_size=1)
    written = dataset.write_arrow_reader(
        reader, format="iceberg", table=table, merge_by=True, commit_row_size=2
    )
    assert written == 5
    assert [chunk.num_rows for chunk in table.upserted] == [2, 2, 1]


# -- overwrite ------------------------------------------------------------


def test_overwrite_true_replaces_the_whole_table() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    table = FakeTable()
    written = dataset.write_arrow_reader(
        reader_of([1, 2]), format="iceberg", table=table, overwrite=True
    )
    assert written == 2
    assert table.overwritten[0].num_rows == 2
    assert table.overwrite_filters == [None], "no filter: the whole table"


def test_overwrite_with_a_filter_passes_it_through() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    table = FakeTable()
    dataset.write_arrow_reader(
        reader_of([1]), format="iceberg", table=table, overwrite="date = '2026-08-14'"
    )
    assert table.overwrite_filters == ["date = '2026-08-14'"]


def test_overwrite_and_merge_by_together_are_refused() -> None:
    dataset = Dataset(schema="rekep:///records/parsed_message")
    with pytest.raises(ValueError, match="two different"):
        dataset.write_arrow_reader(
            parsed_reader({"hash64": 1}),
            format="iceberg",
            table=FakeTable(record=ParsedMessage),
            overwrite=True,
            merge_by=True,
        )


# -- branches -------------------------------------------------------------


def test_append_defaults_to_main_branch() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    table = FakeTable()
    dataset.write_arrow_reader(reader_of([1]), format="iceberg", table=table)
    assert table.branches == ["main"]


def test_append_uses_the_datasets_declared_branch() -> None:
    dataset = Dataset(schema="rekep:///records/log", protocols={"iceberg": {"branch": "dev"}})
    table = FakeTable()
    dataset.write_arrow_reader(reader_of([1]), format="iceberg", table=table)
    assert table.branches == ["dev"]


def test_an_explicit_branch_argument_wins_over_the_declared_one() -> None:
    dataset = Dataset(schema="rekep:///records/log", protocols={"iceberg": {"branch": "dev"}})
    table = FakeTable()
    dataset.write_arrow_reader(reader_of([1]), format="iceberg", table=table, branch="hotfix")
    assert table.branches == ["hotfix"]


def test_a_brand_new_table_bootstraps_on_main_regardless_of_the_declared_branch() -> None:
    """Iceberg allows only `main` before a table has any snapshot at all --
    the very first write always lands there, whatever `branch` says."""
    dataset = Dataset(schema="rekep:///records/log", protocols={"iceberg": {"branch": "dev"}})
    table = FakeTable(has_snapshot=False)
    dataset.write_arrow_reader(reader_of([1]), format="iceberg", table=table, branch="dev")
    assert table.branches == ["main"]


# -- file_write_arrow_reader: generic uri + filesystem mapping -----------


def test_file_write_needs_a_location() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    with pytest.raises(NotImplementedError, match="needs a location"):
        dataset.write_arrow_reader(reader_of([1]), format="file")


def test_file_write_uses_direct_as_the_fallback_location(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(
        schema="rekep:///records/log",
        uri="rekep:///datasets/logs",
        direct=(tmp_path / "out").as_uri(),
    )
    written = dataset.write_arrow_reader(reader_of([1, 2, 3], [4, 5]), format="file")
    assert written == 5
    assert list((tmp_path / "out").rglob("*.parquet"))


def test_file_write_uri_override_wins_over_direct(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(schema="rekep:///records/log", direct="s3://elsewhere/log")
    target = tmp_path / "override"
    written = dataset.write_arrow_reader(reader_of([1, 2]), format="file", uri=target.as_uri())
    assert written == 2
    assert list(target.rglob("*.parquet"))


# -- file writes partition on the record's own declaration ----------------


def test_file_write_lays_out_hive_directories_for_the_records_partitions() -> None:
    """`Log.date` is `Arrow(partition=True)`, so the file writer uses it --
    the same declaration Iceberg's partition spec is built from."""
    dataset = Dataset(schema="rekep:///records/log")
    assert dataset.partition_columns() == {"date": "identity"}


def test_file_write_partitions_by_date(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(schema="rekep:///records/log", direct=(tmp_path / "out").as_uri())
    dataset.write_arrow_reader(reader_of([1, 2]), format="file")
    written = list((tmp_path / "out").rglob("*.parquet"))
    assert written, "something was written"
    assert all(path.parent.name == "date=2026-08-14" for path in written)


def test_file_write_can_be_asked_for_a_flat_layout(tmp_path: pathlib.Path) -> None:
    dataset = Dataset(schema="rekep:///records/log", direct=(tmp_path / "flat").as_uri())
    dataset.write_arrow_reader(reader_of([1]), format="file", partitioning=False)
    assert list((tmp_path / "flat").glob("*.parquet")), "no partition directory"


def test_two_file_writes_append_instead_of_colliding(tmp_path: pathlib.Path) -> None:
    """`write_dataset` defaults would raise on the second `part-0.parquet`."""
    dataset = Dataset(schema="rekep:///records/log", direct=(tmp_path / "out").as_uri())
    dataset.write_arrow_reader(reader_of([1]), format="file")
    dataset.write_arrow_reader(reader_of([2]), format="file")
    assert len(list((tmp_path / "out").rglob("*.parquet"))) == 2


def test_a_file_write_caps_its_files_at_the_commit_row_size(tmp_path: pathlib.Path) -> None:
    """A file layout has no commit, so what a write lands per unit is a file."""
    dataset = Dataset(schema="rekep:///records/log", direct=(tmp_path / "out").as_uri())
    written = dataset.write_arrow_reader(
        reader_of([1, 2], [3, 4], [5]), format="file", commit_row_size=2
    )
    assert written == 5
    assert len(list((tmp_path / "out").rglob("*.parquet"))) == 3


def test_a_file_write_reads_the_commit_row_size_off_the_side_file(
    tmp_path: pathlib.Path,
) -> None:
    dataset = Dataset(
        schema="rekep:///records/log",
        direct=(tmp_path / "out").as_uri(),
        protocols={"file": {"commit_row_size": "2"}},
    )
    dataset.write_arrow_reader(reader_of([1, 2], [3, 4]), format="file")
    assert len(list((tmp_path / "out").rglob("*.parquet"))) == 2


# -- deploy: autonomous, no side file needed ------------------------------


def test_into_iceberg_table_carries_record_name_namespace_location() -> None:
    dataset = Dataset(
        schema="rekep:///records/log",
        uri="rekep:///datasets/trading/logs",
        protocols={"iceberg": {"location": "s3://lake/iceberg/log"}},
    )
    table = dataset.into_iceberg_table()
    assert table.record == "rekep:///records/log"
    assert table.name == "logs"
    assert table.namespace == "trading"
    assert table.location == "s3://lake/iceberg/log"


def test_into_iceberg_table_does_not_leak_location_or_branch_into_properties() -> None:
    """`location`/`branch` route the write; they are not table properties."""
    dataset = Dataset(
        schema="rekep:///records/log",
        properties={"format": "parquet"},
        protocols={"iceberg": {"location": "s3://lake/log", "branch": "dev"}},
    )
    table = dataset.into_iceberg_table()
    assert table.properties == {"format": "parquet"}


def test_into_doris_table_carries_record_name_namespace_properties() -> None:
    dataset = Dataset(
        schema="rekep:///records/log",
        uri="rekep:///datasets/trading/logs",
        properties={"a": "shared"},
        protocols={"doris": {"b": "doris"}},
    )
    table = dataset.into_doris_table()
    assert table.record == "rekep:///records/log"
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

    dataset = Dataset(schema="rekep:///records/log", uri="rekep:///datasets/logs")
    stack = FakeStack()
    dataset.deploy("iceberg", stack, dry_run=True)
    (table, dry_run) = stack.deployed[0]
    assert type(table).__name__ == "IcebergTable"
    assert dry_run is True


def test_deploy_refuses_an_unknown_target() -> None:
    with pytest.raises(ValueError, match="no 'parquet' deploy target"):
        Dataset(schema="rekep:///records/log").deploy("parquet", stack=None)


def test_deploy_iceberg_converges_a_real_local_catalog(tmp_path: pathlib.Path) -> None:
    """No `tables/` side file anywhere: the dataset's own config is enough."""
    from rekep.iceberg import Iceberg
    from rekep.records.iceberg import IcebergCatalog, IcebergDeployment

    root = tmp_path.as_posix()
    stack = Iceberg(
        IcebergDeployment(
            catalogs=[
                IcebergCatalog(endpoint=f"sqlite:///{root}/cat.db", warehouse=f"file://{root}/wh")
            ],
        )
    )
    dataset = Dataset(schema="rekep:///records/log", uri="rekep:///datasets/logs")
    dataset.deploy_iceberg(stack)
    assert stack.catalogs.connect("iceberg").table_exists("default.logs")


def _local_iceberg_stack(tmp_path: pathlib.Path) -> Any:
    from rekep.iceberg import Iceberg
    from rekep.records.iceberg import IcebergCatalog, IcebergDeployment

    root = tmp_path.as_posix()
    return Iceberg(
        IcebergDeployment(
            catalogs=[
                IcebergCatalog(endpoint=f"sqlite:///{root}/cat.db", warehouse=f"file://{root}/wh")
            ]
        )
    )


def test_upsert_against_a_real_local_catalog_uses_the_primary_key(
    tmp_path: pathlib.Path,
) -> None:
    """`join_cols=None` falls back to the record's own `Arrow(key=True)`
    field -- `ParsedMessage.hash64` -- with no extra wiring."""
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(schema="rekep:///records/parsed_message", uri="rekep:///datasets/messages")
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
    dataset = Dataset(schema="rekep:///records/log", uri="rekep:///datasets/logs")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())

    main_dataset = Dataset(schema="rekep:///records/log", uri="rekep:///datasets/logs")
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

    dev_dataset = Dataset(schema="rekep:///records/log", protocols={"iceberg": {"branch": "dev"}})
    reader = pyarrow.RecordBatchReader.from_batches(
        schema, [pyarrow.RecordBatch.from_pylist([{**row, "hash64": 2}], schema=schema)]
    )
    dev_dataset.write_arrow_reader(reader, format="iceberg", table=table)

    assert len(table.scan().to_arrow()) == 1, "main untouched by the dev branch write"
    dev_snapshot = table.refs()["dev"].snapshot_id
    assert len(table.scan(snapshot_id=dev_snapshot).to_arrow()) == 2


# -- reading: dispatch and pushdown ---------------------------------------


def test_read_arrow_reader_refuses_an_unknown_format() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    with pytest.raises(ValueError, match="no 'parquet' reader"):
        dataset.read_arrow_reader("parquet")


def test_iceberg_read_arrow_reader_needs_a_table() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    with pytest.raises(NotImplementedError, match="needs table="):
        dataset.read_arrow_reader("iceberg")


def test_file_read_needs_a_location() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    with pytest.raises(NotImplementedError, match="needs a location"):
        dataset.read_arrow_reader("file")


def test_a_file_round_trip_keeps_the_partition_column(tmp_path: pathlib.Path) -> None:
    """Written into `date=.../`, read back with `date` still a column --
    the record's own declaration lays it out and picks it up again."""
    dataset = Dataset(schema="rekep:///records/log", direct=(tmp_path / "out").as_uri())
    dataset.write_arrow_reader(reader_of([1, 2]), format="file")
    read = dataset.read_arrow_reader("file").read_all()
    assert read.num_rows == 2
    assert read.column("date").to_pylist() == [datetime.date(2026, 8, 14)] * 2


def test_a_file_read_pushes_a_filter_down(tmp_path: pathlib.Path) -> None:
    import pyarrow.compute

    dataset = Dataset(schema="rekep:///records/log", direct=(tmp_path / "out").as_uri())
    dataset.write_arrow_reader(reader_of([1, 2, 3]), format="file")
    matched = dataset.read_arrow_reader(
        "file", row_filter=pyarrow.compute.field("hash64") > 1, columns=["hash64"]
    ).read_all()
    assert matched.column_names == ["hash64"]
    assert sorted(matched.column("hash64").to_pylist()) == [2, 3]


def test_a_read_is_the_protocols_own_reader(tmp_path: pathlib.Path) -> None:
    """Nothing wraps it: no counting hop between the caller and the data."""
    dataset = Dataset(schema="rekep:///records/log", direct=(tmp_path / "o").as_uri())
    dataset.write_arrow_reader(reader_of([1, 2]), format="file")
    assert dataset.read_arrow_reader("file").read_all().num_rows == 2


# -- reading an iceberg table: real catalog, real pushdown ----------------


def test_an_iceberg_read_filters_and_projects(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(schema="rekep:///records/parsed_message", uri="rekep:///datasets/messages")
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
    dataset = Dataset(schema="rekep:///records/log", uri="rekep:///datasets/logs")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    dataset.write_arrow_reader(reader_of([1]), format="iceberg", table=table)
    table.refresh()

    unborn = Dataset(
        schema="rekep:///records/log",
        uri="rekep:///datasets/logs",
        protocols={"iceberg": {"branch": "nope"}},
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
    dataset = Dataset(schema="rekep:///records/parsed_message", uri="rekep:///datasets/messages")
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
    dataset = Dataset(schema="rekep:///records/parsed_message", uri="rekep:///datasets/messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    _crowd(dataset, table, [14, 15])

    report = dataset.iceberg_compact(table=table, min_input_files=3)
    assert report == {"partitions": [], "files": 0, "rows": 0, "compacted": False}
    assert table.inspect.data_files().num_rows == 2, "untouched"


def test_compaction_dry_run_reports_without_writing(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(schema="rekep:///records/parsed_message", uri="rekep:///datasets/messages")
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
    dataset = Dataset(schema="rekep:///records/parsed_message", uri="rekep:///datasets/messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    assert dataset.iceberg_compact(table=table)["compacted"] is False


def test_expiring_snapshots_frees_history_but_not_data(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(schema="rekep:///records/parsed_message", uri="rekep:///datasets/messages")
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
    dataset = Dataset(schema="rekep:///records/parsed_message", uri="rekep:///datasets/messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    _crowd(dataset, table, [14] * 3)
    assert (
        dataset.iceberg_expire_snapshots(table=table, older_than=datetime.timedelta(days=7)) == []
    )


def test_publish_fast_forwards_main_onto_the_branch(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    trunk = Dataset(schema="rekep:///records/parsed_message", uri="rekep:///datasets/messages")
    trunk.deploy_iceberg(stack)
    table = stack.tables.get(trunk.into_iceberg_table())
    trunk.write_arrow_reader(parsed_reader({"hash64": 1}), format="iceberg", table=table)
    table.refresh()

    dev = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
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
    dataset = Dataset(schema="rekep:///records/log")
    with pytest.raises(ValueError, match="needs a branch other than 'main'"):
        dataset.iceberg_publish(table=FakeTable())


def test_publishing_a_branch_with_no_snapshot_yet_does_nothing() -> None:
    dataset = Dataset(schema="rekep:///records/log", protocols={"iceberg": {"branch": "dev"}})
    assert dataset.iceberg_publish(table=FakeTable()) is None


# -- shipped stacks -----------------------------------------------------


def test_the_shipped_datasets_declare_real_records() -> None:
    """Every `stacks/datasets/*.yaml` must actually parse and name a real record."""
    repo_datasets = pathlib.Path(__file__).parents[2] / "stacks" / "datasets"
    datasets = Dataset.load_all(repo_datasets)
    assert {d.dataset_name() for d in datasets} == {"log", "parsed_messages"}
    for dataset in datasets:
        assert dataset.record_class() is not None  # raises TypeError otherwise


def test_the_shipped_log_dataset_is_branch_agnostic() -> None:
    repo_datasets = pathlib.Path(__file__).parents[2] / "stacks" / "datasets"
    (log,) = [d for d in Dataset.load_all(repo_datasets) if d.dataset_name() == "log"]
    assert log.record_class() is Log
    assert str(log.resource_uri()) == "rekep:///datasets/default/log"
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
        schema="rekep:///records/log",
        uri="rekep:///datasets/trading/logs",
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
    dataset = Dataset(schema=str(Trade.record_uri()), uri="rekep:///datasets/trades")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    assert [field.name for field in table.spec().fields] == ["at_day"], "not shadowing `at`"

    with pytest.raises(ValueError, match="uses the day.* transform"):
        _partition_filter(table, [{"at_day": 20679}])


def test_a_partition_filter_matches_identity_partitions(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(schema="rekep:///records/log", uri="rekep:///datasets/logs")
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


# -- merge_schema: cast the shared columns, add the new ones --------------


def test_merge_schemas_keeps_the_targets_spelling_for_shared_fields() -> None:
    """A source calling a column `int32` does not get to narrow the target."""
    from rekep.records import merge_schemas

    target = ParsedMessage.into_arrow_schema()
    incoming = pyarrow.schema([("hash64", pyarrow.int32()), ("venue", pyarrow.string())])
    merged = merge_schemas(incoming, target)
    assert merged.field("hash64").type == pyarrow.int64(), "the target's type wins"
    assert merged.names == [*target.names, "venue"], "new one appended, order kept"


def test_merge_schemas_forces_new_fields_nullable() -> None:
    """Rows already written predate the column and have nothing to put in it."""
    from rekep.records import merge_schemas

    target = ParsedMessage.into_arrow_schema()
    incoming = pyarrow.schema([pyarrow.field("venue", pyarrow.string(), nullable=False)])
    assert merge_schemas(incoming, target).field("venue").nullable


def test_merge_schemas_numbers_new_fields_after_the_highest_existing_id() -> None:
    """`fields` is a map, so it eats two ids of its own -- the new column
    must come after those, not after the top-level count."""
    from rekep.records.arrow import FIELD_ID_KEY, merge_schemas

    target = ParsedMessage.into_arrow_schema()
    merged = merge_schemas(pyarrow.schema([("venue", pyarrow.string())]), target)
    identifiers = {field.name: int((field.metadata or {})[FIELD_ID_KEY]) for field in merged}
    assert identifiers["venue"] == 9, "6 top-level fields + the map's key and value"
    assert len(set(identifiers.values())) == len(identifiers), "no id reused"


def test_merge_schemas_with_nothing_new_returns_the_target_itself() -> None:
    from rekep.records import merge_schemas

    target = ParsedMessage.into_arrow_schema()
    assert merge_schemas(target, target) is target


def test_a_write_keeps_an_unknown_column_when_merge_schema_is_on() -> None:
    dataset = Dataset(schema="rekep:///records/parsed_message")
    table = FakeTable(record=ParsedMessage)
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}, extra={"venue": "X"}),
        format="iceberg",
        table=table,
        merge_schema=True,
    )
    assert [schema.names for schema in table.unioned] == [["venue"]], "only the addition"
    (written,) = table.appended
    assert "venue" in written.schema.names
    assert written.column("venue").to_pylist() == ["X"]


def test_the_written_columns_carry_the_tables_field_ids_not_the_records() -> None:
    """Iceberg matches columns by id, and `union_by_name` renumbers what it
    adds -- so the ids the write carries have to come back from the table."""
    dataset = Dataset(schema="rekep:///records/parsed_message")
    table = FakeTable(record=ParsedMessage)
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}, extra={"venue": "X"}),
        format="iceberg",
        table=table,
        merge_schema=True,
    )
    (written,) = table.appended
    identifiers = {field.name: (field.metadata or {}).get(FIELD_ID_KEY) for field in written.schema}
    assert identifiers == {
        field.name: (field.metadata or {}).get(FIELD_ID_KEY) for field in table.arrow
    }, "every column identified the way the table identifies it"


def test_two_widening_writes_with_different_extras_stay_in_their_own_columns() -> None:
    """The corruption this guards against: the record's numbering does not
    move between writes, but the table's does, so the second write's column
    would otherwise be filed under the first write's id."""
    dataset = Dataset(schema="rekep:///records/parsed_message")
    table = FakeTable(record=ParsedMessage)
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}, extra={"region": "REGION"}),
        format="iceberg",
        table=table,
        merge_schema=True,
    )
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 2}, extra={"venue": "VENUE"}),
        format="iceberg",
        table=table,
        merge_schema=True,
    )
    live = {field.name: (field.metadata or {})[FIELD_ID_KEY] for field in table.arrow}
    assert live["region"] != live["venue"], "the table gave them different ids"
    second = table.appended[-1]
    assert (second.schema.field("venue").metadata or {})[FIELD_ID_KEY] == live["venue"]


def test_merge_schema_is_off_unless_declared() -> None:
    dataset = Dataset(schema="rekep:///records/parsed_message")
    table = FakeTable(record=ParsedMessage)
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}, extra={"venue": "X"}), format="iceberg", table=table
    )
    assert "venue" not in table.appended[0].schema.names, "dropped, as before"


def test_merge_schema_is_read_from_the_datasets_own_config() -> None:
    dataset = Dataset(
        schema="rekep:///records/parsed_message", protocols={"iceberg": {"merge_schema": "true"}}
    )
    assert dataset.merge_schema("iceberg") is True
    assert dataset.merge_schema("file") is False, "declared for iceberg only"


def test_a_shared_property_reaches_every_protocol() -> None:
    """`properties` is the shared layer; `protocols` is the exception layer."""
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        properties={"merge_schema": "true"},
        protocols={"file": {"merge_schema": "false"}},
    )
    assert dataset.merge_schema("iceberg") is True
    assert dataset.merge_schema("file") is False, "the protocol overrides the shared default"


def test_merge_schema_does_not_leak_into_table_properties() -> None:
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        protocols={"iceberg": {"merge_schema": "true", "team": "trading"}},
    )
    assert dataset.table_properties("iceberg") == {"team": "trading"}


def test_a_plain_iterator_is_peeked_not_consumed_for_its_schema() -> None:
    """merge_schema needs the incoming shape before the first write; a plain
    iterator only reveals it by producing a batch, so one is pulled and put
    straight back -- every batch still arrives, once, in order."""
    dataset = Dataset(schema="rekep:///records/parsed_message")
    table = FakeTable(record=ParsedMessage)
    reader = parsed_reader({"hash64": 1}, {"hash64": 2}, extra={"venue": "X"}, batch_size=1)
    written = dataset.write_arrow_reader(
        iter(reader), format="iceberg", table=table, merge_schema=True
    )
    assert written == 2, "both batches, neither lost to the peek"
    assert table.appended[-1].column("venue").to_pylist() == ["X", "X"]


def test_an_empty_stream_leaves_the_schema_alone() -> None:
    dataset = Dataset(schema="rekep:///records/parsed_message")
    table = FakeTable(record=ParsedMessage)
    written = dataset.write_arrow_reader(iter(()), format="iceberg", table=table, merge_schema=True)
    assert written == 0
    assert table.appended == [], "nothing written, nothing to widen"


# -- merge_schema against a real catalog ---------------------------------


def test_merge_schema_adds_the_column_to_a_real_table(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"merge_schema": "true"}},
    )
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    dataset.write_arrow_reader(parsed_reader({"hash64": 1}), format="iceberg", table=table)
    table.refresh()
    assert "venue" not in {field.name for field in table.schema().fields}

    dataset.write_arrow_reader(
        parsed_reader({"hash64": 2}, extra={"venue": "LSE"}), format="iceberg", table=table
    )
    table.refresh()
    added = {field.name: field for field in table.schema().fields}
    assert "venue" in added
    assert added["venue"].required is False, "rows already written have no value for it"

    rows = {row["hash64"]: row for row in table.scan().to_arrow().to_pylist()}
    assert rows[1]["venue"] is None, "backfilled null on the older row"
    assert rows[2]["venue"] == "LSE"


def test_merge_schema_leaves_an_existing_column_required(tmp_path: pathlib.Path) -> None:
    """union_by_name maps nullability verbatim, so restating a column would
    silently relax it -- only the additions are ever handed over."""
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"merge_schema": "true"}},
    )
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}, extra={"venue": "X"}), format="iceberg", table=table
    )
    table.refresh()
    assert {f.name for f in table.schema().fields if f.required} == {
        "url",
        "unix",
        "date",
        "hash64",
        "fields",
    }


def test_merging_rows_and_widening_columns_in_one_write(tmp_path: pathlib.Path) -> None:
    """The combination that needs the branch stamped: a snapshot records the
    schema it was written under, and a merge reads the branch back."""
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"merge_schema": "true", "merge_by": "true"}},
    )
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}, {"hash64": 2}), format="iceberg", table=table
    )
    table.refresh()

    written = dataset.write_arrow_reader(
        parsed_reader({"hash64": 2, "protocol": "FIX.4.4"}, {"hash64": 3}, extra={"venue": "X"}),
        format="iceberg",
        table=table,
    )
    table.refresh()
    assert written == 2, "one updated, one inserted"
    rows = {row["hash64"]: row for row in table.scan().to_arrow().to_pylist()}
    assert set(rows) == {1, 2, 3}, "merged on the key, not appended"
    assert rows[1]["venue"] is None
    assert rows[2] == {**rows[2], "protocol": "FIX.4.4", "venue": "X"}


def test_a_second_widening_write_costs_no_extra_schema_version(
    tmp_path: pathlib.Path,
) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"merge_schema": "true"}},
    )
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}, extra={"venue": "X"}), format="iceberg", table=table
    )
    table.refresh()
    versions = len(table.metadata.schemas)

    dataset.write_arrow_reader(
        parsed_reader({"hash64": 2}, extra={"venue": "Y"}), format="iceberg", table=table
    )
    table.refresh()
    assert len(table.metadata.schemas) == versions, "already caught up: no commit"


def test_a_file_write_carries_the_widened_column(tmp_path: pathlib.Path) -> None:
    import pyarrow.parquet

    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        direct=(tmp_path / "out").as_uri(),
        properties={"merge_schema": "true"},
    )
    dataset.write_arrow_reader(parsed_reader({"hash64": 1}, extra={"venue": "X"}), format="file")
    (written,) = list((tmp_path / "out").rglob("*.parquet"))
    assert "venue" in pyarrow.parquet.read_schema(written).names


def test_two_widening_writes_land_in_the_right_columns(tmp_path: pathlib.Path) -> None:
    """A source that grows one column, then a different one, is the whole
    point of merge_schema -- and the case where a stale field id would file
    the second write's data under the first write's column, silently."""
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"merge_schema": "true"}},
    )
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())

    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}, extra={"region": "REGION"}), format="iceberg", table=table
    )
    table.refresh()
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 2}, extra={"venue": "VENUE"}), format="iceberg", table=table
    )
    table.refresh()

    rows = {row["hash64"]: row for row in table.scan().to_arrow().to_pylist()}
    assert rows[1]["region"] == "REGION"
    assert rows[1]["venue"] is None
    assert rows[2]["venue"] == "VENUE", "not filed under `region`"
    assert rows[2]["region"] is None


def test_two_live_columns_declared_in_the_other_order_keep_their_values(
    tmp_path: pathlib.Path,
) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"merge_schema": "true"}},
    )
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())

    dataset.write_arrow_reader(
        parsed_reader({"hash64": 1}, extra={"region": "R1"}), format="iceberg", table=table
    )
    table.refresh()
    dataset.write_arrow_reader(
        parsed_reader({"hash64": 2}, extra={"venue": "VENUE", "region": "REGION"}),
        format="iceberg",
        table=table,
    )
    table.refresh()

    rows = {row["hash64"]: row for row in table.scan().to_arrow().to_pylist()}
    assert rows[2]["venue"] == "VENUE"
    assert rows[2]["region"] == "REGION", "declaration order does not move values"


def test_a_branch_forked_before_an_evolution_is_brought_forward(
    tmp_path: pathlib.Path,
) -> None:
    """The ref, not the table, is what a merge reads back -- so a branch
    that predates the schema change has to be moved forward even when the
    table itself has nothing left to add."""
    stack = _local_iceberg_stack(tmp_path)
    trunk = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"merge_schema": "true"}},
    )
    trunk.deploy_iceberg(stack)
    table = stack.tables.get(trunk.into_iceberg_table())
    trunk.write_arrow_reader(parsed_reader({"hash64": 1}), format="iceberg", table=table)
    table.refresh()

    # a branch forked from the pre-evolution snapshot
    table.manage_snapshots().create_branch(table.current_snapshot().snapshot_id, "dev").commit()
    table.refresh()

    # main widens; the table is now current but `dev`'s snapshot is not
    trunk.write_arrow_reader(
        parsed_reader({"hash64": 2}, extra={"venue": "X"}), format="iceberg", table=table
    )
    table.refresh()

    dev = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"branch": "dev", "merge_by": "true", "merge_schema": "true"}},
    )
    written = dev.write_arrow_reader(
        parsed_reader({"hash64": 1, "protocol": "FIX.4.4"}), format="iceberg", table=table
    )
    table.refresh()
    assert written == 1, "the merge ran rather than failing on a stale ref"
    rows = {r["hash64"]: r for r in table.scan().use_ref("dev").to_arrow().to_pylist()}
    assert rows[1]["protocol"] == "FIX.4.4"


def test_a_merge_after_a_deploy_widened_the_table(tmp_path: pathlib.Path) -> None:
    """Nothing about this write widens anything -- the *deploy* did. The ref
    is stale all the same, and the write has to cope."""
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"merge_by": "true"}},
    )
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    dataset.write_arrow_reader(parsed_reader({"hash64": 1}), format="iceberg", table=table)
    table.refresh()

    with table.update_schema() as update:
        update.union_by_name(pyarrow.schema([pyarrow.field("desk", pyarrow.string())]))
    table.refresh()

    written = dataset.write_arrow_reader(
        parsed_reader({"hash64": 1, "protocol": "FIX.4.4"}, {"hash64": 2}),
        format="iceberg",
        table=table,
    )
    table.refresh()
    assert written == 2, "one updated, one inserted"
    assert {r["hash64"] for r in table.scan().to_arrow().to_pylist()} == {1, 2}


# -- merges prune on the bounds Iceberg already records -------------------


def seeded(tmp_path: pathlib.Path, first: int, last: int) -> tuple[Any, Any, Dataset]:
    """A merging dataset whose table already holds keys `first`..`last`."""
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"merge_by": "true"}},
    )
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    dataset.write_arrow_reader(
        parsed_reader(*({"hash64": key, "unix": key} for key in range(first, last + 1))),
        format="iceberg",
        table=table,
    )
    table.refresh()
    return stack, table, dataset


def test_key_bounds_come_from_the_manifests(tmp_path: pathlib.Path) -> None:
    from rekep.dataset import _key_bounds

    _, table, _ = seeded(tmp_path, 0, 9)
    assert _key_bounds(table, "main", ["unix", "hash64"]) == {"unix": (0, 9), "hash64": (0, 9)}


def test_an_empty_branch_has_no_bounds_to_reason_with(tmp_path: pathlib.Path) -> None:
    from rekep.dataset import _key_bounds

    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(schema="rekep:///records/parsed_message", uri="rekep:///datasets/messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    assert _key_bounds(table, "main", ["hash64"]) is None


def test_a_disjoint_chunk_cannot_match_anything(tmp_path: pathlib.Path) -> None:
    from rekep.dataset import _key_bounds, _outside

    _, table, _ = seeded(tmp_path, 0, 9)
    bounds = _key_bounds(table, "main", ["unix", "hash64"])
    beyond = parsed_reader(*({"hash64": k, "unix": k} for k in range(100, 105))).read_all()
    assert _outside(beyond, ["unix", "hash64"], bounds) is True


def test_an_overlapping_chunk_is_not_pruned(tmp_path: pathlib.Path) -> None:
    from rekep.dataset import _key_bounds, _outside

    _, table, _ = seeded(tmp_path, 0, 9)
    bounds = _key_bounds(table, "main", ["unix", "hash64"])
    across = parsed_reader(*({"hash64": k, "unix": k} for k in range(5, 15))).read_all()
    assert _outside(across, ["unix", "hash64"], bounds) is False


def test_one_column_missing_the_range_is_enough(tmp_path: pathlib.Path) -> None:
    """A row matches only if it matches on every key column."""
    from rekep.dataset import _outside

    chunk = parsed_reader({"hash64": 3, "unix": 500}).read_all()
    bounds = {"unix": (0, 9), "hash64": (0, 9)}
    assert _outside(chunk, ["unix", "hash64"], bounds) is True, "unix is far outside"


def test_pruning_never_changes_what_the_table_ends_up_with(tmp_path: pathlib.Path) -> None:
    """Pruned or merged, the same rows: the whole optimization rests on
    'cannot match', which must never become 'did not match'."""
    _, table, dataset = seeded(tmp_path, 0, 9)

    dataset.write_arrow_reader(  # disjoint: pruned to an append
        parsed_reader(*({"hash64": k, "unix": k} for k in range(100, 105))),
        format="iceberg",
        table=table,
    )
    table.refresh()
    dataset.write_arrow_reader(  # overlapping: a real merge, correcting in place
        parsed_reader(*({"hash64": k, "unix": k, "protocol": "FIX.4.4"} for k in range(8, 13))),
        format="iceberg",
        table=table,
    )
    table.refresh()

    rows = {row["hash64"]: row for row in table.scan().to_arrow().to_pylist()}
    assert set(rows) == set(range(0, 13)) | set(range(100, 105))
    assert rows[8]["protocol"] == "FIX.4.4", "corrected, not duplicated"
    assert rows[0]["protocol"] is None, "untouched"
    assert len(rows) == len(table.scan().to_arrow()), "no duplicate keys anywhere"


# -- compact / cleanup / optimize -----------------------------------------


def test_a_partition_of_full_sized_files_is_not_fragmented(tmp_path: pathlib.Path) -> None:
    """Many files is not the test on its own -- a big partition is allowed
    to have many big files."""
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(schema="rekep:///records/parsed_message", uri="rekep:///datasets/messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    for index in range(6):
        dataset.write_arrow_reader(parsed_reader({"hash64": index}), format="iceberg", table=table)
    table.refresh()

    with table.transaction() as transaction:
        transaction.set_properties(**{"write.target-file-size-bytes": "1"})
    table.refresh()
    assert dataset.compact(table=table, min_input_files=3)["compacted"] is False


def test_cleanup_frees_the_files_expiry_strands(tmp_path: pathlib.Path) -> None:
    """pyiceberg's expire_snapshots is metadata-only: it *creates* garbage.
    Reclaiming it is the step nothing else does."""
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"retain": "0s", "compact_min_files": "3"}},
    )
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    for index in range(5):
        dataset.write_arrow_reader(parsed_reader({"hash64": index}), format="iceberg", table=table)
    table.refresh()
    dataset.compact(table=table)
    table.refresh()

    warehouse = tmp_path / "wh"
    before = len(list(warehouse.rglob("*.parquet")))
    report = dataset.cleanup(table=table, orphan_grace=datetime.timedelta(seconds=-1))
    table.refresh()

    assert report["properties"], "the metadata-retention properties were set"
    assert report["expired"], "snapshots past the retention window went"
    assert report["orphans"], "and the files they stranded were freed"
    assert len(list(warehouse.rglob("*.parquet"))) < before
    assert table.scan().to_arrow().num_rows == 5, "every row still readable"


def test_cleanup_spares_files_younger_than_the_grace_period(tmp_path: pathlib.Path) -> None:
    """A writer mid-commit has files no snapshot references yet."""
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"retain": "0s"}},
    )
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    for index in range(4):
        dataset.write_arrow_reader(parsed_reader({"hash64": index}), format="iceberg", table=table)
    table.refresh()
    dataset.compact(table=table, min_input_files=2)
    table.refresh()

    report = dataset.cleanup(table=table)  # default grace: three days
    assert report["orphans"] == [], "nothing on this table is old enough"


def test_optimize_enables_manifest_merging_once(tmp_path: pathlib.Path) -> None:
    """pyiceberg does not merge manifests by default, so a streaming table
    grows one per commit forever."""
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(schema="rekep:///records/parsed_message", uri="rekep:///datasets/messages")
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    dataset.write_arrow_reader(parsed_reader({"hash64": 1}), format="iceberg", table=table)
    table.refresh()

    assert dataset.optimize(table=table)["manifest_merge"] is True
    table.refresh()
    assert table.properties["commit.manifest-merge.enabled"] == "true"
    assert dataset.optimize(table=table)["manifest_merge"] is False, "already on"


def test_optimize_is_idempotent(tmp_path: pathlib.Path) -> None:
    stack = _local_iceberg_stack(tmp_path)
    dataset = Dataset(
        schema="rekep:///records/parsed_message",
        uri="rekep:///datasets/messages",
        protocols={"iceberg": {"compact_min_files": "3", "retain": "0s"}},
    )
    dataset.deploy_iceberg(stack)
    table = stack.tables.get(dataset.into_iceberg_table())
    for index in range(4):
        dataset.write_arrow_reader(parsed_reader({"hash64": index}), format="iceberg", table=table)
    table.refresh()

    assert dataset.optimize(table=table)["compaction"]["compacted"] is True
    table.refresh()
    settled = dataset.optimize(table=table)
    assert settled["compaction"]["compacted"] is False
    assert settled["cleanup"]["properties"] == []
    assert table.scan().to_arrow().num_rows == 4


def test_a_maintenance_verb_refuses_an_unknown_protocol() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    with pytest.raises(ValueError, match="no 'doris' compact"):
        dataset.compact("doris")


# -- identity: a schema and a URI, not a record and three fields ----------


def test_a_dataset_names_its_schema_and_projects_it() -> None:
    dataset = Dataset(schema="rekep:///records/log")
    assert dataset.record_class() is Log
    assert dataset.arrow_schema().equals(Log.into_arrow_schema())


def test_without_a_uri_the_record_names_the_dataset() -> None:
    dataset = Dataset(schema="rekep:///records/parsed_message")
    assert str(dataset.resource_uri()) == "rekep:///datasets/parsed_message"
    assert dataset.dataset_name() == "parsed_message"
    assert dataset.dataset_namespace() == "default"


def test_the_uri_carries_catalog_namespace_and_name() -> None:
    dataset = Dataset(
        schema="rekep:///records/log", uri="rekep:///datasets/warehouse/trading/orders"
    )
    assert dataset.dataset_name() == "orders"
    assert dataset.dataset_namespace() == "trading"
    assert dataset.resource_uri().catalog() == "warehouse"


def test_the_branch_rides_along_as_a_fragment() -> None:
    dataset = Dataset(
        schema="rekep:///records/log",
        uri="rekep:///datasets/trading/orders",
        protocols={"iceberg": {"branch": "dev"}},
    )
    assert str(dataset.resource_uri()) == "rekep:///datasets/trading/orders#dev"


def test_a_branch_written_into_the_uri_wins() -> None:
    """The identity is the identity; config fills a gap, it does not override."""
    dataset = Dataset(
        schema="rekep:///records/log",
        uri="rekep:///datasets/trading/orders#hotfix",
        protocols={"iceberg": {"branch": "dev"}},
    )
    assert dataset.resource_uri().branch == "hotfix"


def test_any_spelling_of_the_uri_declares_the_same_dataset() -> None:
    spellings = [
        "rekep:///datasets/warehouse/trading/orders",
        "rekep:///datasets//warehouse/trading/orders",
        "rekep:///datasets/warehouse/trading/orders",
    ]
    resolved = {
        str(Dataset(schema="rekep:///records/log", uri=text).resource_uri()) for text in spellings
    }
    assert resolved == {"rekep:///datasets/warehouse/trading/orders"}


def test_the_deployed_table_takes_its_name_from_the_uri() -> None:
    dataset = Dataset(
        schema="rekep:///records/log", uri="rekep:///datasets/warehouse/trading/orders"
    )
    table = dataset.into_iceberg_table()
    assert (table.name, table.namespace, table.record) == (
        "orders",
        "trading",
        "rekep:///records/log",
    )


def test_a_schema_naming_a_bare_record_name_resolves_too() -> None:
    """`log` is an incomplete reference, not a second spelling: it is what a
    call site types, and it resolves to the one record that answers to it."""
    assert Dataset(schema="log").record_class() is Log


def test_a_relative_file_uri_is_still_listable() -> None:
    """`file://stacks/wh` is a spelling catalogs accept and pyarrow refuses,
    because the first segment reads as a hostname."""
    from rekep.dataset import _listable

    assert _listable("file://stacks/iceberg/warehouse") == "stacks/iceberg/warehouse"
    assert _listable("file:///abs/warehouse") == "file:///abs/warehouse"
    assert _listable("s3://bucket/wh") == "s3://bucket/wh"


def test_an_unlistable_location_frees_nothing_rather_than_failing() -> None:
    """A maintenance pass must not die on a URI it cannot enumerate."""
    from rekep.dataset import _orphan_files

    class Unlistable(FakeTable):
        def location(self) -> str:
            return "weird://not/a/filesystem"

    assert _orphan_files(Unlistable(), datetime.timedelta(days=3)) == []
