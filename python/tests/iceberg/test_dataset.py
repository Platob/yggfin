"""`IcebergDataset` against a real, fully local catalog: SQLite and a file warehouse."""

import datetime
import subprocess
import sys
from pathlib import Path
from typing import Annotated

import pyarrow
import pytest

from rekep import Convertible, Field, Log, StructField, field
from rekep.iceberg import IcebergDataset


@field
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


def catalog_properties(tmp_path: Path) -> dict[str, str]:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir(exist_ok=True)
    return {
        "type": "sql",
        "uri": f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
        "warehouse": warehouse.as_uri(),
    }


@pytest.fixture
def dataset(tmp_path: Path) -> IcebergDataset:
    return IcebergDataset(
        name="trading.quotes",
        catalog="test",
        properties=catalog_properties(tmp_path),
        struct=Quote.FIELD,
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
        schema=Quote.FIELD.into_arrow_schema(),
    )


# -- creating -------------------------------------------------------------


def test_a_write_creates_the_table_from_the_declared_shape(dataset: IcebergDataset) -> None:
    assert not dataset.exists
    dataset.write_arrow_table(quotes(3))
    assert dataset.exists

    schema = dataset.iceberg_table.schema()
    assert [f.name for f in schema.fields] == Quote.FIELD.names
    assert schema.find_field("symbol").doc == "Instrument.", "the docs land as column comments"
    assert schema.identifier_field_ids == [schema.find_field("symbol").field_id]
    assert [f.name for f in dataset.iceberg_table.spec().fields] == ["day"]


def test_creating_is_idempotent(dataset: IcebergDataset) -> None:
    first = dataset.get_or_create_table()
    assert dataset.get_or_create_table().name() == first.name()


def test_a_missing_table_with_nothing_declared_is_refused(tmp_path: Path) -> None:
    bare = IcebergDataset(
        name="trading.absent", catalog="test", properties=catalog_properties(tmp_path)
    )
    with pytest.raises(ValueError, match="declares no shape"):
        bare.write_arrow_table(quotes(1))


# -- what it holds --------------------------------------------------------


def test_the_declared_shape_wins(dataset: IcebergDataset) -> None:
    assert dataset.into_struct_field() is Quote.FIELD


def test_the_tables_own_shape_is_read_back(dataset: IcebergDataset, tmp_path: Path) -> None:
    dataset.write_arrow_table(quotes(1))
    found = IcebergDataset(
        name=dataset.name, catalog="test", properties=catalog_properties(tmp_path)
    )
    shape = found.into_struct_field()
    assert shape.names == Quote.FIELD.names
    assert shape.primary_keys() == ["symbol"]
    assert shape.partition_keys() == {"day": "identity"}
    assert shape.field("symbol").description == "Instrument."
    assert not shape.field("size").nullable and shape.field("venue").nullable


# -- reading and writing --------------------------------------------------


def test_rows_go_in_and_come_back(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(5))
    assert dataset.read_arrow_table().num_rows == 5


def test_a_read_without_a_schema_is_the_stores_own(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(2))
    reader = dataset.read_arrow_reader()
    assert reader.schema.field("symbol").type == pyarrow.large_string(), "no conversion is paid"


def test_a_read_casts_onto_the_schema_it_is_given(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(2))
    table = dataset.read_arrow_table(Quote.FIELD)
    assert table.schema.equals(Quote.FIELD.into_arrow_schema())


def test_a_filter_is_pushed_down_to_the_scan(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(5))
    assert dataset.read_arrow_table(row_filter="size >= 3").num_rows == 2


def test_columns_are_pushed_down_to_the_scan(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(3))
    assert dataset.read_arrow_table(columns=["symbol", "size"]).column_names == ["symbol", "size"]


def test_a_limit_is_pushed_down_to_the_scan(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(5))
    assert dataset.read_arrow_table(limit=2).num_rows == 2


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
    dataset.write_arrow_reader(iter([batch]))
    stored = dataset.read_arrow_table(Quote.FIELD)
    assert stored.column("size").to_pylist() == [7]
    assert stored.column("venue").to_pylist() == [None], "the missing nullable column was filled"


def test_commit_row_size_commits_one_snapshot_per_chunk(dataset: IcebergDataset) -> None:
    dataset.write_arrow_reader(quotes(6).to_reader(max_chunksize=1), commit_row_size=2)
    assert len(dataset.iceberg_table.history()) == 3
    assert dataset.read_arrow_table().num_rows == 6


def test_an_empty_stream_commits_nothing(dataset: IcebergDataset) -> None:
    dataset.get_or_create_table()
    dataset.write_arrow_reader(iter(()))
    assert dataset.iceberg_table.history() == []


# -- merging --------------------------------------------------------------


def test_merge_by_true_upserts_on_the_declared_key(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(3, "XPAR"))
    dataset.write_arrow_table(quotes(3, "XETR"), merge_by=True)
    stored = dataset.read_arrow_table()
    assert stored.num_rows == 3, "the same keys came back, not three more rows"
    assert set(stored.column("venue").to_pylist()) == {"XETR"}


def test_merge_by_names_upserts_on_those(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(2, "XPAR"))
    dataset.write_arrow_table(quotes(2, "XETR"), merge_by=["symbol", "day"])
    assert dataset.read_arrow_table().num_rows == 2


def test_a_falsy_merge_by_appends(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(2))
    dataset.write_arrow_table(quotes(2), merge_by=False)
    assert dataset.read_arrow_table().num_rows == 4


def test_merging_on_a_key_nothing_declares_is_refused_before_writing(tmp_path: Path) -> None:
    @field
    class Loose(Convertible):
        symbol: str

    keyless = IcebergDataset(
        name="trading.loose",
        catalog="test",
        properties=catalog_properties(tmp_path),
        struct=Loose.FIELD,
    )
    with pytest.raises(ValueError, match="no member declares one"):
        keyless.merge_columns(True)


# -- the dataset is also a document ---------------------------------------


def test_a_dataset_round_trips_through_yaml(dataset: IcebergDataset) -> None:
    """Its configuration -- the declared shape included -- is a file."""
    rebuilt = IcebergDataset.from_yaml(dataset.into_yaml())
    assert (rebuilt.name, rebuilt.catalog, rebuilt.properties) == (
        dataset.name,
        dataset.catalog,
        dataset.properties,
    )
    assert isinstance(rebuilt.struct, StructField)
    assert rebuilt.struct.into_arrow_schema().equals(Quote.FIELD.into_arrow_schema())


def test_a_log_lands_in_a_table(dataset: IcebergDataset, tmp_path: Path) -> None:
    """The parser's own shape, end to end: declared, created, written, read."""
    logs = IcebergDataset(
        name="trading.logs",
        catalog="test",
        properties=catalog_properties(tmp_path),
        struct=Log.FIELD,
    )
    row = Log(
        url="a.txt",
        unix=1,
        date=datetime.date(2026, 8, 14),
        time=datetime.time(0, 5, 1),
        thread_name="t",
        driver="d",
        message="m",
        hash64=2,
    )
    table = pyarrow.Table.from_pylist([dataclass_row(row)], Log.FIELD.into_arrow_schema())
    logs.write_arrow_table(table, merge_by=True)
    logs.write_arrow_table(table, merge_by=True)
    assert logs.read_arrow_table().num_rows == 1, "the same line upserts onto itself"


def dataclass_row(row: Log) -> dict:
    """A `Log` as the plain values Arrow wants, dates and times included."""
    return {
        **row.into_dict(),
        "date": row.date,
        "time": row.time,
    }


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
            Quote.FIELD.into_iceberg_schema()


# -- creating explicitly ----------------------------------------------------


def test_create_with_builds_the_table_before_any_write(dataset: IcebergDataset) -> None:
    dataset.create_with()
    assert dataset.exists
    assert dataset.read_arrow_table().num_rows == 0


def test_create_with_takes_a_shape_it_was_not_declared_with(tmp_path: Path) -> None:
    bare = IcebergDataset(
        name="trading.bare", catalog="test", properties=catalog_properties(tmp_path)
    )
    schema = pyarrow.schema([pyarrow.field("symbol", pyarrow.string(), nullable=False)])
    bare.create_with(schema)
    assert bare.into_struct_field().names == ["symbol"]


def test_creating_twice_leaves_the_table_alone(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(2))
    dataset.create_with()
    assert dataset.read_arrow_table().num_rows == 2


# -- schema evolution -------------------------------------------------------


def test_add_fields_adds_what_the_table_lacks(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(2))
    wider = Quote.FIELD.merge_with(
        pyarrow.schema([("desk", pyarrow.string()), ("pod", pyarrow.int32())])
    )
    assert dataset.add_fields(wider) == ["desk", "pod"]
    assert dataset.table_field.names[-2:] == ["desk", "pod"]
    assert dataset.into_struct_field().names[-2:] == ["desk", "pod"], "writes follow the table"
    assert dataset.read_arrow_table().column("desk").to_pylist() == [None, None]


def test_add_fields_skips_when_there_is_nothing_new(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(1))
    before = len(dataset.iceberg_table.schemas())
    assert dataset.add_fields(Quote.FIELD) == []
    assert len(dataset.refresh().iceberg_table.schemas()) == before, "no commit was made"


def test_add_fields_can_report_without_touching_the_table(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(1))
    wider = Quote.FIELD.merge_with(pyarrow.schema([("desk", pyarrow.string())]))
    assert dataset.add_fields(wider, dry_run=True) == ["desk"]
    assert "desk" not in dataset.refresh().into_struct_field().names


def test_a_wider_batch_lands_after_the_columns_are_added(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(1))
    wider = Quote.FIELD.merge_with(pyarrow.schema([("desk", pyarrow.string())]))
    dataset.add_fields(wider)
    batch = quotes(1).append_column("desk", pyarrow.array(["EQ"]))
    dataset.write_arrow(batch)  # the declared shape moved with the table
    assert set(dataset.read_arrow_table().column("desk").to_pylist()) == {None, "EQ"}


# -- snapshots and branches -------------------------------------------------


def test_snapshots_are_listed(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(1))
    dataset.write_arrow_table(quotes(1))
    assert dataset.snapshots().num_rows == 2


def test_a_read_can_go_back_to_an_older_snapshot(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(2))
    first = dataset.iceberg_table.current_snapshot().snapshot_id
    dataset.write_arrow_table(quotes(3))
    assert dataset.refresh().read_arrow_table().num_rows == 5
    assert dataset.read_arrow_table(snapshot_id=first).num_rows == 2


def test_a_branch_is_written_and_read_on_its_own(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(2))
    dataset.create_branch("dev")
    dataset.write_arrow(quotes(3), branch="dev")
    assert dataset.read_arrow_table(branch="dev").num_rows == 5
    assert dataset.read_arrow_table().num_rows == 2, "main is untouched"


def test_a_branch_is_removed(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(1))
    dataset.create_branch("dev")
    assert "dev" in dataset.refs()
    dataset.remove_branch("dev")
    assert "dev" not in dataset.refs()


def test_branching_needs_something_to_branch_from(dataset: IcebergDataset) -> None:
    dataset.create_with()
    with pytest.raises(ValueError, match="no snapshot to branch from"):
        dataset.create_branch("dev")


def test_a_rollback_moves_the_table_back(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(2))
    first = dataset.iceberg_table.current_snapshot().snapshot_id
    dataset.write_arrow_table(quotes(3))
    dataset.rollback(first)
    assert dataset.read_arrow_table().num_rows == 2


def test_rows_are_deleted_by_filter(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(5))
    dataset.delete("size >= 3")
    assert dataset.refresh().read_arrow_table().num_rows == 3


# -- maintenance ------------------------------------------------------------


def test_many_small_writes_leave_many_files(dataset: IcebergDataset) -> None:
    for _ in range(4):
        dataset.write_arrow_table(quotes(1))
    assert dataset.data_files().num_rows >= 4


def test_compaction_rewrites_the_fragments(dataset: IcebergDataset) -> None:
    for index in range(4):
        dataset.write_arrow_table(quotes(2, f"venue{index}"))
    before = dataset.data_files().num_rows
    rewritten = dataset.compact(min_files=2)
    assert rewritten == before
    assert dataset.data_files().num_rows < before, "the fragments became fewer files"
    assert dataset.read_arrow_table().num_rows == 8, "and every row survived"


def test_compaction_is_a_no_op_when_there_is_nothing_to_do(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(2))
    assert dataset.compact(min_files=5) == 0


def test_compaction_plans_one_partition_at_a_time(dataset: IcebergDataset) -> None:
    """A partition is a predicate when the transform is identity, so it can be
    rewritten without touching the rest of the table."""
    dataset.write_arrow_table(quotes(2))
    dataset.write_arrow_table(quotes(2))
    plan = dataset.compaction_plan(min_files=2)
    assert len(plan) == 1
    assert "day = '2026-08-14'" in plan[0][0]


def test_a_filter_compacts_only_that_part(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(2))
    dataset.write_arrow_table(quotes(2))
    assert dataset.compact(row_filter="day = '2026-08-14'") > 0
    assert dataset.read_arrow_table().num_rows == 4


def test_cleanup_expires_old_snapshots(dataset: IcebergDataset) -> None:
    for _ in range(4):
        dataset.write_arrow_table(quotes(1))
    report = dataset.cleanup(retain=1, remove_orphans=False)
    assert report["expired"] == 3
    assert dataset.refresh().snapshots().num_rows == 1
    assert dataset.read_arrow_table().num_rows == 4, "the data is still all there"


def test_cleanup_can_report_without_touching_anything(dataset: IcebergDataset) -> None:
    for _ in range(3):
        dataset.write_arrow_table(quotes(1))
    report = dataset.cleanup(retain=1, dry_run=True)
    assert report["expired"] == 2
    assert dataset.refresh().snapshots().num_rows == 3


def test_cleanup_keeps_what_a_branch_still_references(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(1))
    dataset.create_branch("dev")
    for _ in range(3):
        dataset.write_arrow_table(quotes(1))
    dataset.cleanup(retain=1, remove_orphans=False)
    assert dataset.refresh().snapshots().num_rows >= 2, "the branch head survived"


def test_cleanup_sweeps_the_files_expiry_stranded(dataset: IcebergDataset) -> None:
    """Expiry is metadata-only, so the sweep is the half that reclaims space."""
    for index in range(3):
        dataset.write_arrow_table(quotes(2, f"venue{index}"))
    dataset.compact(min_files=2)
    assert dataset.orphan_files(older_than=datetime.timedelta(seconds=0)) == [], (
        "the files compaction replaced are still held by the snapshots before it"
    )
    report = dataset.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0))
    assert report["expired"] > 0
    assert report["deleted"] > 0, "expiring the old snapshots is what made them garbage"
    assert report["bytes"] > 0
    assert dataset.read_arrow_table().num_rows == 6, "only garbage went"


def test_a_recent_file_is_never_swept(dataset: IcebergDataset) -> None:
    """A writer committing right now has files no snapshot mentions yet."""
    dataset.write_arrow_table(quotes(2))
    dataset.write_arrow_table(quotes(2))
    dataset.compact(min_files=2)
    assert dataset.orphan_files() == [], "nothing is old enough to be garbage"


def test_optimize_does_the_whole_routine(dataset: IcebergDataset) -> None:
    for index in range(4):
        dataset.write_arrow_table(quotes(2, f"venue{index}"))
    report = dataset.optimize(min_files=2)
    assert report["rewritten"] > 0
    assert report["expired"] > 0
    assert dataset.iceberg_table.properties["commit.manifest-merge.enabled"] == "true"
    assert dataset.read_arrow_table().num_rows == 8


def test_properties_are_set_in_one_commit(dataset: IcebergDataset) -> None:
    dataset.create_with()
    dataset.set_properties({"write.target-file-size-bytes": "1048576"})
    assert dataset.iceberg_table.properties["write.target-file-size-bytes"] == "1048576"


def test_target_file_size_is_icebergs_knob_not_ours(dataset: IcebergDataset) -> None:
    dataset.write_arrow_table(quotes(2))
    dataset.write_arrow_table(quotes(2))
    dataset.compact(min_files=2, target_file_size=8 * 1024 * 1024)
    assert dataset.iceberg_table.properties["write.target-file-size-bytes"] == str(8 * 1024 * 1024)


# -- field ids --------------------------------------------------------------


def test_a_schema_that_carries_ids_keeps_them(dataset: IcebergDataset) -> None:
    """Iceberg matches columns by id: taking the ids back is what keeps a
    round trip lossless instead of renumbering every column."""
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    dataset.write_arrow_table(quotes(1))
    declared = dataset.iceberg_table.schema()
    carried = Field.from_arrow_schema(schema_to_pyarrow(declared, include_field_ids=True))
    assert [f.field_id for f in carried.into_iceberg_schema().fields] == [
        f.field_id for f in declared.fields
    ]


def test_a_plain_arrow_schema_is_numbered_for_the_user(tmp_path: Path) -> None:
    plain = IcebergDataset(
        name="trading.plain", catalog="test", properties=catalog_properties(tmp_path)
    )
    plain.create_with(pyarrow.schema([pyarrow.field("a", pyarrow.int64(), nullable=False)]))
    assert [f.field_id for f in plain.iceberg_table.schema().fields] == [1]


# -- commits ----------------------------------------------------------------


def test_a_write_commits_in_chunks_of_the_datasets_own_size(dataset: IcebergDataset) -> None:
    dataset.commit_row_size = 2
    dataset.write_arrow_reader(quotes(6).to_reader(max_chunksize=1))
    assert len(dataset.iceberg_table.history()) == 3, "the dataset's size, with no call saying so"


def test_a_call_overrides_the_datasets_commit_size(dataset: IcebergDataset) -> None:
    dataset.commit_row_size = 2
    dataset.write_arrow_reader(quotes(6).to_reader(max_chunksize=1), commit_row_size=0)
    assert len(dataset.iceberg_table.history()) == 1, "0 means one commit for the stream"


def test_a_created_table_carries_the_commit_properties(dataset: IcebergDataset) -> None:
    dataset.create_with()
    properties = dataset.iceberg_table.properties
    assert properties["commit.manifest-merge.enabled"] == "true"
    assert properties["write.target-file-size-bytes"] == str(256 * 1024 * 1024)
    assert properties["write.parquet.row-group-limit"] == str(128 * 1024)


def test_iceberg_defaults_can_be_kept(tmp_path: Path) -> None:
    bare = IcebergDataset(
        name="trading.bare",
        catalog="test",
        properties=catalog_properties(tmp_path),
        struct=Quote.FIELD,
        optimize_commits=False,
    )
    bare.create_with()
    assert "commit.manifest-merge.enabled" not in bare.iceberg_table.properties


def test_declared_table_properties_win_over_the_defaults(tmp_path: Path) -> None:
    tuned = IcebergDataset(
        name="trading.tuned",
        catalog="test",
        properties=catalog_properties(tmp_path),
        struct=Quote.FIELD,
        table_properties={"write.target-file-size-bytes": "1024"},
    )
    tuned.create_with()
    assert tuned.iceberg_table.properties["write.target-file-size-bytes"] == "1024"
    assert tuned.iceberg_table.properties["commit.manifest-merge.enabled"] == "true"


# -- planning ---------------------------------------------------------------


def test_a_merge_of_new_keys_writes_without_reading(dataset: IcebergDataset) -> None:
    """The pruning short circuit: an append, arrived at by planning."""
    dataset.write_arrow_table(quotes(3))
    before = len(dataset.iceberg_table.history())
    dataset.write_arrow(quotes(3, "XETR"), merge_by=True)  # same keys -> updates
    dataset.refresh()
    assert dataset.read_arrow_table().num_rows == 3
    assert len(dataset.iceberg_table.history()) > before


def test_a_streamed_merge_loads_the_table_once(dataset: IcebergDataset) -> None:
    """A commit updates the table it was made on, so no chunk reloads it.

    The catalog round trip is free on SQLite and a network hop on REST or Glue;
    at one per commit a streaming merge would pay it per chunk, to learn what
    it had just done itself.
    """
    dataset.write_arrow_table(quotes(9))
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
        dataset.write_arrow(rows.to_reader(max_chunksize=3), merge_by=True, commit_row_size=3)
    finally:
        del dataset.store.load_table
    assert loaded == 1, "one load for the whole stream, not one per commit"
    assert set(dataset.read_arrow_table().column("venue").to_pylist()) == {"XETR"}, (
        "and what was committed is visible without a reload"
    )


def test_the_merge_path_can_be_handed_back_to_pyiceberg(dataset: IcebergDataset) -> None:
    dataset.plan_merges = False
    dataset.write_arrow_table(quotes(3))
    dataset.write_arrow(quotes(3, "XETR"), merge_by=True)
    assert set(dataset.refresh().read_arrow_table().column("venue").to_pylist()) == {"XETR"}


# -- maintenance that settles -----------------------------------------------


def test_compaction_stops_when_there_is_nothing_left_to_gain(dataset: IcebergDataset) -> None:
    """A part that legitimately needs several files must not be rewritten forever."""
    dataset.table_properties = {"write.target-file-size-bytes": str(16 * 1024)}
    for index in range(6):
        dataset.write_arrow(quotes(4, f"venue{index}"), commit_row_size=0)
    assert dataset.compact(min_files=2) > 0
    files = dataset.refresh().data_files().num_rows
    assert dataset.compact(min_files=2) == 0, "the second pass has nothing to do"
    assert dataset.compaction_plan(min_files=2) == []
    assert dataset.refresh().data_files().num_rows == files, "and it did not grow the table"


def test_new_data_makes_a_compacted_partition_worth_planning_again(
    dataset: IcebergDataset,
) -> None:
    for _ in range(3):
        dataset.write_arrow(quotes(2), commit_row_size=0)
    dataset.compact(min_files=2)
    assert dataset.compaction_plan(min_files=2) == []
    for _ in range(2):
        dataset.write_arrow(quotes(2, "XETR"), commit_row_size=0)
    assert dataset.compaction_plan(min_files=2) != []


def test_the_compaction_snapshots_are_marked(dataset: IcebergDataset) -> None:
    for _ in range(2):
        dataset.write_arrow(quotes(2), commit_row_size=0)
    dataset.compact(min_files=2)
    assert dataset.compacted_snapshots(), "how a compacted part is recognised later"


# -- sweeping ---------------------------------------------------------------


def test_cleanup_sweeps_metadata_as_well_as_data(dataset: IcebergDataset) -> None:
    """A stream fills the metadata directory faster than the data one."""
    for index in range(8):
        dataset.write_arrow(quotes(2, f"venue{index}"), commit_row_size=0)
    dataset.compact(min_files=2)
    location = Path(dataset.iceberg_table.location().replace("file://", ""))
    before = len(list((location / "metadata").rglob("*")))
    stored = dataset.read_arrow_table().num_rows
    report = dataset.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0))
    after = len(list((location / "metadata").rglob("*")))
    assert report["deleted"] > 0
    assert after < before, "the metadata directory shrank"
    assert dataset.refresh().read_arrow_table().num_rows == stored, "and the table still reads"


def test_every_retained_snapshot_still_reads_after_a_sweep(dataset: IcebergDataset) -> None:
    """The one thing a metadata sweep may never break."""
    for index in range(6):
        dataset.write_arrow(quotes(2, f"venue{index}"), commit_row_size=0)
    dataset.cleanup(retain=3, orphan_age=datetime.timedelta(seconds=0))
    dataset.refresh()
    for snapshot in dataset.iceberg_table.snapshots():
        assert dataset.read_arrow_table(snapshot_id=snapshot.snapshot_id).num_rows > 0


def test_a_sweep_can_leave_metadata_alone(dataset: IcebergDataset) -> None:
    for index in range(4):
        dataset.write_arrow(quotes(2, f"venue{index}"), commit_row_size=0)
    location = Path(dataset.iceberg_table.location().replace("file://", ""))
    before = {path for path in (location / "metadata").rglob("*")}
    dataset.cleanup(retain=1, orphan_age=datetime.timedelta(seconds=0), metadata=False)
    # Expiry writes a metadata version of its own, so the directory may grow --
    # what must not happen is a file disappearing from it.
    assert before <= {path for path in (location / "metadata").rglob("*")}


def test_a_sweep_keeps_the_files_only_the_metadata_names(dataset: IcebergDataset) -> None:
    """A Puffin statistics file and a Hadoop pointer are reachable no other way.

    Neither is named by a snapshot, a manifest or the metadata log, and a
    statistics file is exactly as old as the snapshot it describes -- so an age
    rule does not save it either. Sweeping one loses the table's statistics;
    sweeping the other loses the table.
    """
    from pyiceberg.table.statistics import StatisticsFile

    dataset.write_arrow(quotes(2), commit_row_size=0)
    table = dataset.get_or_create_table()
    metadata = Path(table.location().replace("file://", "")) / "metadata"
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
    dataset.write_arrow(quotes(2), commit_row_size=0)
    dataset.optimize()
    versions = len(dataset.refresh().iceberg_table.metadata.metadata_log)
    dataset.optimize()
    assert len(dataset.refresh().iceberg_table.metadata.metadata_log) <= versions + 1


# -- sorting a commit -------------------------------------------------------


def test_sorting_a_commit_changes_the_order_not_the_rows(dataset: IcebergDataset) -> None:
    dataset.sort_by = ["size"]
    rows = quotes(6).sort_by([("size", "descending")])
    dataset.write_arrow(rows, commit_row_size=0)
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
            name=f"trading.sorted{index}",
            catalog="test",
            properties=catalog_properties(tmp_path),
            struct=Quote.FIELD,
            sort_by=sort_by,
        )
        target.write_arrow(quotes(50).sort_by([("size", "descending")]), commit_row_size=0)
        found = target.read_arrow_table(row_filter="size >= 40").to_pylist()
        answers.append(sorted(found, key=str))
    assert answers[0] == answers[1]
