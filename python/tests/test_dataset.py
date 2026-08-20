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

    def into_struct_field(self) -> StructField:
        return self.struct

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
        size: int

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


# -- chunking ---------------------------------------------------------------


def test_chunks_group_a_stream_by_row_count() -> None:
    chunks = list(arrow_chunks(iter([rows(3), rows(3), rows(3)]), 4))
    assert [chunk.num_rows for chunk in chunks] == [6, 3], "a chunk closes once it is big enough"


def test_chunks_take_the_schema_from_a_reader() -> None:
    reader = pyarrow.RecordBatchReader.from_batches(rows(1).schema, iter([rows(1)]))
    (chunk,) = arrow_chunks(reader, None)
    assert chunk.schema.equals(reader.schema)
