"""The `Dataset` contract, exercised through the smallest implementation of it."""

import dataclasses
import datetime
from collections.abc import Iterator, Sequence
from typing import Annotated, Any

import pyarrow
import pytest

from rekep import Convertible, Dataset, Field, StructField, scalar
from rekep.dataset import _needs_compatible_polars_arrow, _polars_table, arrow_chunks


@scalar
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

    field: StructField
    commits: list[pyarrow.Table] = dataclasses.field(default_factory=list)
    created: bool = False

    def into_struct_field(self) -> StructField:
        return self.field

    @property
    def exists(self) -> bool:
        return self.created

    def create_with_field(self, field: StructField, **kwargs: Any) -> "MemoryDataset":
        self.field = field
        self.created = True
        return self

    def read_arrow_reader(self, schema: Any = None, **kwargs: Any) -> pyarrow.RecordBatchReader:
        batches: list[pyarrow.RecordBatch] = []
        for commit in self.commits:
            batches.extend(commit.to_batches())
        reader = pyarrow.RecordBatchReader.from_batches(
            self.field.into_arrow_schema(), iter(batches)
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
    return MemoryDataset(field=Quote.into_field())


def batch_of(**columns: list) -> pyarrow.RecordBatch:
    return pyarrow.RecordBatch.from_pydict(columns)


def rows(count: int) -> pyarrow.RecordBatch:
    day = datetime.date(2026, 8, 14)
    return batch_of(
        symbol=[f"S{i}" for i in range(count)], day=[day] * count, size=list(range(count))
    )


# -- the shape --------------------------------------------------------------


def test_a_dataset_says_what_it_holds(dataset: MemoryDataset) -> None:
    assert dataset.into_struct_field() is Quote.into_field()
    assert dataset.into_arrow_schema().equals(Quote.into_field().into_arrow_schema())


def test_the_target_of_a_cast_is_the_dataset_unless_one_is_given(dataset: MemoryDataset) -> None:
    assert dataset.target_field() is Quote.into_field()
    other = Field.from_arrow_schema(pyarrow.schema([("symbol", pyarrow.string())]))
    assert dataset.target_field(other) is other, "a field is taken as it is"
    assert dataset.target_field(other.into_arrow_schema()) == other, "a schema becomes one"


def test_an_incomplete_implementation_cannot_be_built() -> None:
    class Half(Dataset):
        def into_struct_field(self) -> StructField:
            return Quote.into_field()

    with pytest.raises(TypeError, match="abstract"):
        Half()


# -- merging ----------------------------------------------------------------


def test_merge_by_true_means_the_declared_primary_key() -> None:
    @scalar
    class Keyed(Convertible):
        symbol: Annotated[str, Field.primary_key()]
        """Instrument."""

        size: int
        """Quantity."""

    assert MemoryDataset(field=Keyed.into_field()).merge_columns(True) == ["symbol"]


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
    assert stored.schema.equals(Quote.into_field().into_arrow_schema())
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
    assert dataset.read_arrow_reader().schema.equals(Quote.into_field().into_arrow_schema())
    narrow = pyarrow.schema([("symbol", pyarrow.large_string())])
    assert dataset.read_arrow_reader(narrow).schema.field("symbol").type == pyarrow.large_string()


def test_polars_batches_stream_from_the_arrow_reader(
    dataset: MemoryDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    polars = pytest.importorskip("polars")
    dataset.write_arrow_reader(iter([rows(1), rows(1)]), commit_row_size=1)
    monkeypatch.setattr(
        MemoryDataset,
        "read_arrow_table",
        lambda *_args, **_kwargs: pytest.fail("read_polars_batches materialised the dataset"),
    )
    frames = list(dataset.read_polars_batches())
    assert [frame.height for frame in frames] == [1, 1]
    assert polars.concat(frames).columns == Quote.into_field().names


def test_read_polars_is_the_explicit_in_memory_form(dataset: MemoryDataset) -> None:
    pytest.importorskip("polars")
    dataset.write_arrow_table(pyarrow.Table.from_batches([rows(3)]))
    frame = dataset.read_polars()
    assert frame.shape == (3, 3)
    assert frame["size"].to_list() == [0, 1, 2]


def test_a_polars_frame_is_cast_onto_the_datasets_shape(dataset: MemoryDataset) -> None:
    polars = pytest.importorskip("polars")
    source = polars.DataFrame(
        {
            "day": [datetime.date(2026, 8, 14)],
            "noise": [9],
            "symbol": ["A"],
        }
    )
    dataset.write_polars(source)
    stored = dataset.commits[0]
    assert stored.schema.equals(Quote.into_field().into_arrow_schema())
    assert stored.to_pydict() == {
        "symbol": ["A"],
        "day": [datetime.date(2026, 8, 14)],
        "size": [None],
    }


def test_a_lazy_polars_frame_stays_streamed(
    dataset: MemoryDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    polars = pytest.importorskip("polars")
    source = polars.DataFrame(
        {
            "symbol": [f"S{i}" for i in range(5)],
            "day": [datetime.date(2026, 8, 14)] * 5,
            "size": list(range(5)),
        }
    ).lazy()
    monkeypatch.setattr(
        polars.LazyFrame,
        "collect",
        lambda *_args, **_kwargs: pytest.fail("write_polars materialised the LazyFrame"),
    )
    dataset.write_polars(source, batch_row_size=2, commit_row_size=2)
    assert [commit.num_rows for commit in dataset.commits] == [2, 2, 1]
    assert dataset.read_arrow_table().column("size").to_pylist() == list(range(5))


def test_polars_export_preserves_a_compatible_text_layout() -> None:
    polars = pytest.importorskip("polars")

    class Frame:
        options: dict[str, object]

        def to_arrow(self, **options: object) -> pyarrow.Table:
            self.options = options
            return pyarrow.table({"symbol": ["A"]})

    target = Field.from_arrow_schema(pyarrow.schema([("symbol", pyarrow.string())]))
    frame = Frame()
    stored = _polars_table(frame, target, polars)
    assert frame.options == {}, "newest would turn text into a view that storage casts back"
    assert stored.schema.equals(target.into_arrow_schema())


def test_polars_export_uses_the_newest_layout_when_the_contract_keeps_it() -> None:
    polars = pytest.importorskip("polars")

    class Frame:
        options: dict[str, object]

        def to_arrow(self, **options: object) -> pyarrow.Table:
            self.options = options
            return pyarrow.table({"symbol": pyarrow.array(["A"], type=pyarrow.string_view())})

    target = Field.from_arrow_schema(pyarrow.schema([("symbol", pyarrow.string_view())]))
    frame = Frame()
    stored = _polars_table(frame, target, polars)
    assert frame.options == {"compat_level": polars.CompatLevel.newest()}
    assert stored.schema.equals(target.into_arrow_schema())


@pytest.mark.parametrize(
    ("data_type", "compatible"),
    [
        (pyarrow.int64(), False),
        (pyarrow.list_(pyarrow.int64()), False),
        (pyarrow.string_view(), False),
        (pyarrow.string(), True),
        (pyarrow.list_(pyarrow.large_binary()), True),
        (pyarrow.struct([("nested", pyarrow.string())]), True),
        (pyarrow.dictionary(pyarrow.int32(), pyarrow.string()), True),
    ],
)
def test_polars_compatibility_follows_nested_arrow_types(
    data_type: pyarrow.DataType, compatible: bool
) -> None:
    assert _needs_compatible_polars_arrow(data_type) is compatible


def test_polars_cannot_fill_a_missing_required_column(dataset: MemoryDataset) -> None:
    polars = pytest.importorskip("polars")
    with pytest.raises(ValueError, match="symbol"):
        dataset.write_polars(polars.DataFrame({"day": [datetime.date(2026, 8, 14)]}))
    assert dataset.commits == []


# -- appending --------------------------------------------------------------


@scalar
class Keyed(Convertible):
    """One keyed row."""

    symbol: Annotated[str, Field.primary_key()]
    """Instrument."""

    size: int
    """Quantity."""


@pytest.fixture
def keyed() -> MemoryDataset:
    return MemoryDataset(field=Keyed.into_field())


def keyed_batch(symbols: list[str], sizes: list[int]) -> pyarrow.RecordBatch:
    return batch_of(symbol=symbols, size=sizes)


def stored_rows(dataset: MemoryDataset) -> dict[str, int]:
    table = dataset.read_arrow_table()
    return dict(zip(*(table.column(name).to_pylist() for name in ("symbol", "size")), strict=True))


def test_append_without_merge_by_is_a_plain_write(keyed: MemoryDataset) -> None:
    assert keyed.append_arrow(keyed_batch(["A"], [1])) == 1
    assert keyed.append_arrow(keyed_batch(["A"], [2])) == 1
    assert keyed.read_arrow_table().num_rows == 2, "falsy merge_by appends, same as a write"


def test_append_merge_by_skips_stored_keys_and_never_rewrites(keyed: MemoryDataset) -> None:
    keyed.write_arrow(keyed_batch(["A", "B"], [1, 2]))
    assert keyed.append_arrow(keyed_batch(["B", "C"], [20, 3]), merge_by=True) == 1
    assert stored_rows(keyed) == {"A": 1, "B": 2, "C": 3}, "B keeps its stored value"


def test_append_polars_skips_a_stored_key(keyed: MemoryDataset) -> None:
    polars = pytest.importorskip("polars")
    keyed.write_arrow(keyed_batch(["A"], [1]))
    assert (
        keyed.append_polars(polars.DataFrame({"symbol": ["A", "B"], "size": [9, 2]}), merge_by=True)
        == 1
    )
    assert stored_rows(keyed) == {"A": 1, "B": 2}


def test_replaying_a_stream_appends_nothing(keyed: MemoryDataset) -> None:
    batch = keyed_batch(["A", "B"], [1, 2])
    assert keyed.append_arrow(batch, merge_by=True) == 2
    commits = len(keyed.commits)
    assert keyed.append_arrow(batch, merge_by=True) == 0
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
    assert dataset.create_with(Quote).into_struct_field().names == Quote.into_field().names


def test_create_with_nothing_uses_the_declared_shape(dataset: MemoryDataset) -> None:
    assert dataset.create_with().into_struct_field() is Quote.into_field()


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


# -- reading one out of a document -------------------------------------------


def test_a_document_says_which_store_it_names() -> None:
    """The same dispatch a task's `kind` gets: one lookup, keyed by what each
    implementation declares, so a scheduler reads a document for a class it has
    never imported by name."""
    built = Dataset.from_dict({"kind": "text_file", "url": "a.log"})
    assert type(built).into_kind() == "text_file"
    assert built.url.endswith("a.log")


def test_an_implementation_behind_an_optional_dependency_is_imported_by_the_document() -> None:
    """And by nothing else, which is what keeps the dependency optional."""
    pytest.importorskip("pyiceberg")
    from rekep.iceberg import IcebergDataset

    built = Dataset.from_dict({"kind": "iceberg", "name": "a.b", "catalog": "c"})
    assert isinstance(built, IcebergDataset) and built.name == "a.b"


def test_a_document_with_no_kind_says_what_it_could_have_said() -> None:
    with pytest.raises(ValueError, match="add a `kind`"):
        Dataset.from_dict({"url": "a.log"})


def test_a_kind_nothing_implements_lists_what_does() -> None:
    with pytest.raises(ValueError, match="no dataset of kind 'parquet'"):
        Dataset.from_dict({"kind": "parquet"})


def test_a_concrete_class_still_reads_its_own_document_with_no_kind_in_it() -> None:
    """Which is what keeps `IcebergDataset.from_yaml(...)` working unchanged."""
    from rekep.text import TextFile

    assert TextFile.from_dict({"url": "a.log"}).url.endswith("a.log")


def test_a_concrete_class_refuses_a_document_naming_a_different_store() -> None:
    """Rather than quietly building the wrong store from the right fields."""
    from rekep.text import TextFile

    with pytest.raises(ValueError, match="text_file"):
        TextFile.from_dict({"kind": "text_files", "url": "a.log"})


def test_every_shipped_kind_is_reachable_from_a_document() -> None:
    """Every lazy module registers the kind its document names."""
    for kind in ("iceberg", "text_file", "text_files"):
        if kind == "iceberg":
            pytest.importorskip("pyiceberg")
        built = Dataset._imported(kind)
        assert built is not None and built.into_kind() == kind


# -- what a join hands back --------------------------------------------------


def joinable(keys: Sequence[int]) -> pyarrow.Table:
    return pyarrow.table(
        {
            "at": pyarrow.array(keys, pyarrow.int64()),
            "payload": pyarrow.array([f"row-{key}" for key in keys]),
        }
    )


def descents(table: pyarrow.Table, column: str) -> int:
    """How many times `column` goes backwards -- zero on a table still in order."""
    values = table.column(column).combine_chunks()
    return pyarrow.compute.sum(pyarrow.compute.less(values[1:], values[:-1])).as_py() or 0


def test_an_anti_join_hands_the_rows_back_in_the_order_they_came() -> None:
    """Arrow emits a join a batch at a time, in whatever order they finish. The
    rows are right and their layout is not: a chunk is sorted before it is
    written so each row group covers a narrow slice of the sort key, and a
    scrambled take spreads every slice over all of them."""
    from rekep.dataset import anti_join

    chunk = joinable(range(200_000))
    stored = joinable(range(0, 200_000, 3))
    fresh = anti_join(chunk, stored, ["at"])
    assert fresh.num_rows == 200_000 - len(range(0, 200_000, 3))
    assert descents(fresh, "at") == 0, "the chunk's own order, not the join's"
    assert fresh.column("payload")[0].as_py() == "row-1", "and the right rows in it"


def test_a_semi_join_hands_the_rows_back_in_the_order_they_came() -> None:
    from rekep.dataset import semi_join

    stored = joinable(range(200_000))
    chunk = joinable(range(0, 200_000, 3))
    kept = semi_join(stored, chunk, ["at"])
    assert kept.num_rows == len(range(0, 200_000, 3))
    assert descents(kept, "at") == 0


def test_a_join_that_drops_nothing_is_the_table_itself() -> None:
    """The common case on a stream of new keys, and the one that must not pay
    for an ordering it already has."""
    from rekep.dataset import anti_join

    chunk = joinable(range(10))
    assert anti_join(chunk, joinable([]), ["at"]) is chunk
