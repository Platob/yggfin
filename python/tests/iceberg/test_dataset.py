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
