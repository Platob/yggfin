"""The `Dataset` contract, exercised through the smallest implementation of it."""

import dataclasses
import datetime
from collections.abc import Iterator, Sequence
from typing import Annotated, Any

import pyarrow
import pytest

from rekep import Convertible, Dataset, Field, StructField, field
from rekep.dataset import arrow_chunks


@field
class Quote(Convertible):
    """One quote."""

    symbol: str
    """Instrument."""

    day: datetime.date
    """Trading day."""

    size: int | None = None
    """Quantity, when the venue printed one."""


@dataclasses.dataclass(eq=False)
class MemoryDataset(Dataset):
    """A dataset that keeps its commits in a list -- everything, nothing more."""

    struct: StructField
    commits: list[pyarrow.Table] = dataclasses.field(default_factory=list)
    created: bool = False

    def into_struct_field(self) -> StructField:
        return self.struct

    @property
    def exists(self) -> bool:
        return self.created

    def create_with_field(self, field: StructField, **kwargs: Any) -> "MemoryDataset":
        self.struct = field
        self.created = True
        return self

    def read_arrow_reader(self, schema: Any = None, **kwargs: Any) -> pyarrow.RecordBatchReader:
        batches: list[pyarrow.RecordBatch] = []
        for commit in self.commits:
            batches.extend(commit.to_batches())
        reader = pyarrow.RecordBatchReader.from_batches(
            self.struct.into_arrow_schema(), iter(batches)
        )
        return reader if schema is None else self.target_field(schema).cast_arrow_reader(reader)

    def write_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader | Iterator[pyarrow.RecordBatch],
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = None,
        commit_row_size: int | None = None,
    ) -> None:
        self.merge_columns(merge_by)  # refuses an impossible merge before writing anything
        self.get_or_create()  # a write appends, and appending to nothing is a create
        reader = self.target_field(schema).cast_arrow_reader(source)
        self.commits.extend(arrow_chunks(reader, commit_row_size))


@pytest.fixture
def dataset() -> MemoryDataset:
    return MemoryDataset(struct=Quote.FIELD)


def batch_of(**columns: list) -> pyarrow.RecordBatch:
    return pyarrow.RecordBatch.from_pydict(columns)


def rows(count: int) -> pyarrow.RecordBatch:
    day = datetime.date(2026, 8, 14)
    return batch_of(
        symbol=[f"S{i}" for i in range(count)], day=[day] * count, size=list(range(count))
    )


# -- the shape --------------------------------------------------------------


def test_a_dataset_says_what_it_holds(dataset: MemoryDataset) -> None:
    assert dataset.into_struct_field() is Quote.FIELD
    assert dataset.into_arrow_schema().equals(Quote.FIELD.into_arrow_schema())


def test_the_target_of_a_cast_is_the_dataset_unless_one_is_given(dataset: MemoryDataset) -> None:
    assert dataset.target_field() is Quote.FIELD
    other = Field.from_arrow_schema(pyarrow.schema([("symbol", pyarrow.string())]))
    assert dataset.target_field(other) is other, "a field is taken as it is"
    assert dataset.target_field(other.into_arrow_schema()) == other, "a schema becomes one"


def test_an_incomplete_implementation_cannot_be_built() -> None:
    class Half(Dataset):
        def into_struct_field(self) -> StructField:
            return Quote.FIELD

    with pytest.raises(TypeError, match="abstract"):
        Half()


# -- merging ----------------------------------------------------------------


def test_merge_by_true_means_the_declared_primary_key() -> None:
    @field
    class Keyed(Convertible):
        symbol: Annotated[str, Field.primary_key()]
        """Instrument."""

        size: int
        """Quantity."""

    assert MemoryDataset(struct=Keyed.FIELD).merge_columns(True) == ["symbol"]


def test_merge_by_a_list_means_those_columns(dataset: MemoryDataset) -> None:
    assert dataset.merge_columns(["symbol", "day"]) == ["symbol", "day"]


@pytest.mark.parametrize("merge_by", [None, False, []])
def test_a_falsy_merge_by_means_append(dataset: MemoryDataset, merge_by: object) -> None:
    assert dataset.merge_columns(merge_by) == []


def test_merging_on_a_key_nothing_declares_is_refused(dataset: MemoryDataset) -> None:
    with pytest.raises(ValueError, match="no member declares one"):
        dataset.merge_columns(True)


# -- reading and writing ----------------------------------------------------


def test_a_write_casts_onto_the_datasets_shape(dataset: MemoryDataset) -> None:
    """The incoming stream is nearly right: wrong order, one column missing."""
    batch = batch_of(day=[datetime.date(2026, 8, 14)], symbol=["A"], noise=[1])
    dataset.write_arrow_reader(iter([batch]))
    stored = dataset.commits[0]
    assert stored.schema.equals(Quote.FIELD.into_arrow_schema())
    assert stored.column("size").to_pylist() == [None]


def test_a_write_can_be_cast_onto_another_shape(dataset: MemoryDataset) -> None:
    narrow = Field.from_arrow_schema(pyarrow.schema([("symbol", pyarrow.string())]))
    dataset.write_arrow_reader(iter([rows(2)]), schema=narrow)
    assert dataset.commits[0].column_names == ["symbol"]


def test_commit_row_size_bounds_what_one_commit_carries(dataset: MemoryDataset) -> None:
    dataset.write_arrow_reader(iter([rows(1) for _ in range(5)]), commit_row_size=2)
    assert [commit.num_rows for commit in dataset.commits] == [2, 2, 1]


def test_no_commit_row_size_writes_the_stream_as_one(dataset: MemoryDataset) -> None:
    dataset.write_arrow_reader(iter([rows(1) for _ in range(5)]))
    assert [commit.num_rows for commit in dataset.commits] == [5]


def test_an_empty_stream_commits_nothing(dataset: MemoryDataset) -> None:
    dataset.write_arrow_reader(iter(()))
    assert dataset.commits == []


def test_a_table_goes_in_and_comes_back(dataset: MemoryDataset) -> None:
    table = pyarrow.Table.from_batches([rows(3)])
    dataset.write_arrow_table(table)
    assert dataset.read_arrow_table().num_rows == 3


def test_a_read_casts_only_when_asked(dataset: MemoryDataset) -> None:
    dataset.write_arrow_table(pyarrow.Table.from_batches([rows(1)]))
    assert dataset.read_arrow_reader().schema.equals(Quote.FIELD.into_arrow_schema())
    narrow = pyarrow.schema([("symbol", pyarrow.large_string())])
    assert dataset.read_arrow_reader(narrow).schema.field("symbol").type == pyarrow.large_string()


# -- appending --------------------------------------------------------------


@field
class Keyed(Convertible):
    """One keyed row."""

    symbol: Annotated[str, Field.primary_key()]
    """Instrument."""

    size: int
    """Quantity."""


@pytest.fixture
def keyed() -> MemoryDataset:
    return MemoryDataset(struct=Keyed.FIELD)


def keyed_batch(symbols: list[str], sizes: list[int]) -> pyarrow.RecordBatch:
    return batch_of(symbol=symbols, size=sizes)


def stored_rows(dataset: MemoryDataset) -> dict[str, int]:
    table = dataset.read_arrow_table()
    return dict(zip(*(table.column(name).to_pylist() for name in ("symbol", "size")), strict=True))


def test_append_without_merge_by_is_a_plain_write(keyed: MemoryDataset) -> None:
    keyed.append_arrow(keyed_batch(["A"], [1]))
    keyed.append_arrow(keyed_batch(["A"], [2]))
    assert keyed.read_arrow_table().num_rows == 2, "falsy merge_by appends, same as a write"


def test_append_merge_by_skips_stored_keys_and_never_rewrites(keyed: MemoryDataset) -> None:
    keyed.write_arrow(keyed_batch(["A", "B"], [1, 2]))
    keyed.append_arrow(keyed_batch(["B", "C"], [20, 3]), merge_by=True)
    assert stored_rows(keyed) == {"A": 1, "B": 2, "C": 3}, "B keeps its stored value"


def test_replaying_a_stream_appends_nothing(keyed: MemoryDataset) -> None:
    batch = keyed_batch(["A", "B"], [1, 2])
    keyed.append_arrow(batch, merge_by=True)
    commits = len(keyed.commits)
    keyed.append_arrow(batch, merge_by=True)
    assert stored_rows(keyed) == {"A": 1, "B": 2}
    assert len(keyed.commits) == commits, "a replay is not even a commit"


def test_duplicate_keys_inside_the_stream_collapse_to_the_first(keyed: MemoryDataset) -> None:
    keyed.append_arrow_reader(
        iter([keyed_batch(["A", "A"], [1, 9]), keyed_batch(["A"], [8])]), merge_by=True
    )
    assert stored_rows(keyed) == {"A": 1}


def test_append_merge_by_a_list_names_the_columns(keyed: MemoryDataset) -> None:
    keyed.append_arrow(keyed_batch(["A"], [1]), merge_by=["symbol"])
    keyed.append_arrow(keyed_batch(["A"], [9]), merge_by=["symbol"])
    assert stored_rows(keyed) == {"A": 1}


def test_a_null_merge_key_is_refused(dataset: MemoryDataset) -> None:
    batch = batch_of(symbol=["A"], day=[datetime.date(2026, 8, 14)], size=[None])
    with pytest.raises(ValueError, match="merge key and cannot be null"):
        dataset.append_arrow(batch, merge_by=["size"])


def test_append_creates_what_is_not_there(keyed: MemoryDataset) -> None:
    assert not keyed.exists
    keyed.append_arrow(keyed_batch(["A"], [1]), merge_by=True)
    assert keyed.exists and stored_rows(keyed) == {"A": 1}


def test_append_arrow_picks_the_method_by_what_it_is(keyed: MemoryDataset) -> None:
    batch = keyed_batch(["A"], [1])
    keyed.append_arrow(batch, merge_by=True)
    keyed.append_arrow(pyarrow.Table.from_batches([batch]), merge_by=True)
    keyed.append_arrow(iter([batch]), merge_by=True)
    assert stored_rows(keyed) == {"A": 1}


# -- chunking ---------------------------------------------------------------


def test_chunks_group_a_stream_by_row_count() -> None:
    chunks = list(arrow_chunks(iter([rows(3), rows(3), rows(3)]), 4))
    assert [chunk.num_rows for chunk in chunks] == [6, 3], "a chunk closes once it is big enough"


def test_chunks_take_the_schema_from_a_reader() -> None:
    reader = pyarrow.RecordBatchReader.from_batches(rows(1).schema, iter([rows(1)]))
    (chunk,) = arrow_chunks(reader, None)
    assert chunk.schema.equals(reader.schema)


# -- creating ---------------------------------------------------------------


def test_a_write_creates_what_is_not_there(dataset: MemoryDataset) -> None:
    assert not dataset.exists
    dataset.write_arrow(rows(1))
    assert dataset.exists, "a write appends, and appending to nothing is a create"


def test_create_with_takes_whatever_names_a_shape(dataset: MemoryDataset) -> None:
    schema = pyarrow.schema([("symbol", pyarrow.string())])
    assert dataset.create_with(schema).into_arrow_schema().names == ["symbol"]
    assert dataset.create_with_arrow_schema(schema).exists
    assert dataset.create_with_arrow_field(
        pyarrow.field("q", pyarrow.struct([("a", pyarrow.int64())]))
    )
    assert dataset.create_with(Quote).into_struct_field().names == Quote.FIELD.names


def test_create_with_nothing_uses_the_declared_shape(dataset: MemoryDataset) -> None:
    assert dataset.create_with().into_struct_field() is Quote.FIELD


def test_get_or_create_is_idempotent(dataset: MemoryDataset) -> None:
    dataset.create_with()
    dataset.commits.append(pyarrow.Table.from_batches([rows(1)]))
    dataset.get_or_create()
    assert len(dataset.commits) == 1, "an existing dataset is left alone"


# -- generic redirects ------------------------------------------------------


def test_write_arrow_picks_the_method_by_what_it_is(dataset: MemoryDataset) -> None:
    batch = rows(1)
    dataset.write_arrow(batch)
    dataset.write_arrow(pyarrow.Table.from_batches([batch]))
    dataset.write_arrow(iter([batch]))
    dataset.write_arrow([batch])
    assert [commit.num_rows for commit in dataset.commits] == [1, 1, 1, 1]


def test_read_arrow_picks_the_method_by_the_type_asked_for(dataset: MemoryDataset) -> None:
    dataset.write_arrow(rows(2))
    assert isinstance(dataset.read_arrow(), pyarrow.Table)
    assert isinstance(dataset.read_arrow(pyarrow.Table), pyarrow.Table)
    assert isinstance(dataset.read_arrow(pyarrow.RecordBatchReader), pyarrow.RecordBatchReader)


def test_a_write_of_something_unwritable_is_refused(dataset: MemoryDataset) -> None:
    with pytest.raises(TypeError, match="cannot infer"):
        dataset.write_arrow("not arrow data")
