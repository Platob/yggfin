"""The `Dataset` contract, exercised through the smallest implementation of it."""

import dataclasses
import datetime
from collections.abc import Sequence
from typing import Annotated, Any

import pyarrow
import pytest

from rekep import Convertible, Dataset, Field, scalar
from rekep.dataset import (
    anti_join,
    arrow_chunks,
    first_rows,
    in_sort_order,
    normalised_keys,
    sort_order_fields,
)
from rekep.fields import field_of, primary_key, replace_field


@scalar
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, primary_key()]
    """Instrument."""

    day: datetime.date
    """Trading day."""

    size: int | None = None
    """Quantity, when the venue printed one."""


@scalar
class ArrayRow(Convertible):
    """One nested Arrow cast at the dataset seam."""

    values: list[int]


@dataclasses.dataclass(eq=False)
class MemoryDataset(Dataset):
    """A dataset that keeps its commits in a list -- everything, nothing more."""

    field: Field
    commits: list[pyarrow.Table] = dataclasses.field(default_factory=list)
    created: bool = False

    def into_struct_field(self) -> Field:
        return self.field

    @property
    def exists(self) -> bool:
        return self.created

    def create_with_field(self, field: Field, **kwargs: Any) -> "MemoryDataset":
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
        return (
            reader
            if schema is None
            else self.target_field(schema).apply_arrow_reader(
                reader,
                safe=False,
                nullability="strict",
            )
        )

    def overwrite_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader,
        schema: Any = None,
        merge_by: bool | Sequence[str] | None = True,
        commit_row_size: int | None = None,
    ) -> int:
        join = self.merge_columns(merge_by)
        if not join:
            raise ValueError(f"merge_by={merge_by!r} names nothing to match on")
        target = self.target_field(schema)
        reader = target.apply_arrow_reader(source, safe=False, nullability="strict")
        self.get_or_create()
        written = 0
        for chunk in arrow_chunks(reader, commit_row_size):
            for name in join:
                if chunk.column(name).null_count:
                    raise ValueError(f"column {name!r} is a merge key and cannot be null")
            chunk = first_rows(normalised_keys(chunk, join), join)
            # What the chunk carries replaces what the commits held under the
            # same key: each stored commit loses those rows, and the chunk
            # lands whole as a commit of its own.
            self.commits = [
                kept
                for kept in (anti_join(commit, chunk, join) for commit in self.commits)
                if kept.num_rows
            ]
            self.commits.append(chunk)
            written += chunk.num_rows
        return written

    def append_arrow_reader(
        self,
        source: pyarrow.RecordBatchReader,
        schema: Any = None,
        commit_row_size: int | None = None,
        **kwargs: Any,
    ) -> int:
        return self._commit(source, schema, commit_row_size)

    def _commit(
        self,
        source: pyarrow.RecordBatchReader,
        schema: Any = None,
        commit_row_size: int | None = None,
    ) -> int:
        self.get_or_create()  # a write appends, and appending to nothing is a create
        reader = self.target_field(schema).apply_arrow_reader(
            source,
            safe=False,
            nullability="strict",
        )
        inserted = 0
        for chunk in arrow_chunks(reader, commit_row_size):
            self.commits.append(chunk)
            inserted += chunk.num_rows
        return inserted


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


def reader_of(
    *batches: pyarrow.RecordBatch,
    schema: pyarrow.Schema | None = None,
) -> pyarrow.RecordBatchReader:
    """One schema-bearing stream for the native apply boundary."""
    source_schema = (
        schema
        if schema is not None
        else batches[0].schema
        if batches
        else Quote.into_field().into_arrow_schema()
    )
    return pyarrow.RecordBatchReader.from_batches(source_schema, batches)


# -- the shape --------------------------------------------------------------


def test_a_dataset_says_what_it_holds(dataset: MemoryDataset) -> None:
    assert dataset.into_struct_field() is Quote.into_field()
    assert dataset.into_arrow_schema().equals(Quote.into_field().into_arrow_schema())


def test_the_target_of_a_cast_is_the_dataset_unless_one_is_given(dataset: MemoryDataset) -> None:
    assert dataset.target_field() is Quote.into_field()
    other = field_of(pyarrow.schema([("symbol", pyarrow.string())]))
    assert dataset.target_field(other) is other, "a field is taken as it is"
    assert dataset.target_field(other.into_arrow_schema()) == other, "a schema becomes one"


def test_an_incomplete_implementation_cannot_be_built() -> None:
    class Half(Dataset):
        def into_struct_field(self) -> Field:
            return Quote.into_field()

    with pytest.raises(TypeError, match="abstract"):
        Half()


def test_only_public_reader_methods_are_required_for_writes() -> None:
    required = {
        name
        for name in Dataset.__abstractmethods__
        if name.startswith("append_") or name.startswith("overwrite_")
    }
    assert required == {"append_arrow_reader", "overwrite_arrow_reader"}


# -- merging ----------------------------------------------------------------


def test_merge_by_true_means_the_declared_primary_key() -> None:
    @scalar
    class Keyed(Convertible):
        symbol: Annotated[str, primary_key()]
        """Instrument."""

        size: int
        """Quantity."""

    assert MemoryDataset(field=Keyed.into_field()).merge_columns(True) == ["symbol"]


def test_merge_by_a_list_means_those_columns(dataset: MemoryDataset) -> None:
    assert dataset.merge_columns(["symbol", "day"]) == ["symbol", "day"]


@pytest.mark.parametrize("merge_by", [None, False, []])
def test_a_falsy_merge_by_names_nothing_to_match_on(
    dataset: MemoryDataset, merge_by: object
) -> None:
    """Which the append family reads as "insert every row", and an overwrite refuses."""
    assert dataset.merge_columns(merge_by) == []
    with pytest.raises(ValueError, match="names nothing to match on"):
        dataset.overwrite_arrow(rows(1), merge_by=merge_by)


def test_merging_on_a_key_nothing_declares_is_refused() -> None:
    @scalar
    class Loose(Convertible):
        symbol: str
        """Instrument, and nothing says it identifies one."""

    with pytest.raises(ValueError, match="no member declares one"):
        MemoryDataset(field=Loose.into_field()).merge_columns(True)


# -- reading and writing ----------------------------------------------------


def test_a_write_casts_onto_the_datasets_shape(dataset: MemoryDataset) -> None:
    """The incoming stream is nearly right: wrong order, one column missing."""
    batch = batch_of(day=[datetime.date(2026, 8, 14)], symbol=["A"], noise=[1])
    dataset.overwrite_arrow_reader(reader_of(batch))
    stored = dataset.commits[0]
    assert stored.schema.equals(Quote.into_field().into_arrow_schema())
    assert stored.column("size").to_pylist() == [None]


def test_a_write_can_be_cast_onto_another_shape(dataset: MemoryDataset) -> None:
    narrow = field_of(pyarrow.schema([("symbol", pyarrow.string())]))
    dataset.overwrite_arrow_reader(reader_of(rows(2)), schema=narrow)
    assert dataset.commits[0].column_names == ["symbol"]


def test_a_write_uses_yggdryls_native_array_cast() -> None:
    dataset = MemoryDataset(field=ArrayRow.into_field())
    batch = pyarrow.record_batch(
        [pyarrow.array([[1, 2], []], pyarrow.list_(pyarrow.int32()))],
        names=["values"],
    )

    assert dataset.append_arrow_reader(reader_of(batch)) == 2
    stored = dataset.commits[0]
    assert stored.schema == ArrayRow.into_field().into_arrow_schema()
    assert stored.column("values").to_pylist() == [[1, 2], []]


def test_commit_row_size_bounds_what_one_commit_carries(dataset: MemoryDataset) -> None:
    dataset.append_arrow_reader(reader_of(*(rows(1) for _ in range(5))), commit_row_size=2)
    assert [commit.num_rows for commit in dataset.commits] == [2, 2, 1]


def test_no_commit_row_size_writes_the_stream_as_one(dataset: MemoryDataset) -> None:
    dataset.append_arrow_reader(reader_of(*(rows(1) for _ in range(5))))
    assert [commit.num_rows for commit in dataset.commits] == [5]


def test_an_empty_stream_commits_nothing(dataset: MemoryDataset) -> None:
    dataset.overwrite_arrow_reader(reader_of())
    assert dataset.commits == []


def test_a_table_goes_in_and_comes_back(dataset: MemoryDataset) -> None:
    table = pyarrow.Table.from_batches([rows(3)])
    dataset.overwrite_arrow_table(table)
    assert dataset.read_arrow_table().num_rows == 3


def test_a_read_casts_only_when_asked(dataset: MemoryDataset) -> None:
    dataset.overwrite_arrow_table(pyarrow.Table.from_batches([rows(1)]))
    assert dataset.read_arrow_reader().schema.equals(Quote.into_field().into_arrow_schema())
    narrow = pyarrow.schema([("symbol", pyarrow.large_string())])
    assert dataset.read_arrow_reader(narrow).schema.field("symbol").type == pyarrow.large_string()


# -- appending --------------------------------------------------------------


@scalar
class Keyed(Convertible):
    """One keyed row."""

    symbol: Annotated[str, primary_key()]
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


def test_append_is_a_plain_write(keyed: MemoryDataset) -> None:
    assert keyed.append_arrow(keyed_batch(["A"], [1])) == 1
    assert keyed.append_arrow(keyed_batch(["A"], [2])) == 1
    assert keyed.read_arrow_table().num_rows == 2, "an append adds every row, keyed or not"


def test_overwrite_replaces_stored_keys_and_adds_the_rest(keyed: MemoryDataset) -> None:
    keyed.overwrite_arrow(keyed_batch(["A", "B"], [1, 2]))
    assert keyed.overwrite_arrow(keyed_batch(["B", "C"], [20, 3]), merge_by=True) == 2
    assert stored_rows(keyed) == {"A": 1, "B": 20, "C": 3}, "B carries the value it was handed"


def test_replaying_a_stream_leaves_the_same_rows(keyed: MemoryDataset) -> None:
    batch = keyed_batch(["A", "B"], [1, 2])
    assert keyed.overwrite_arrow(batch, merge_by=True) == 2
    assert keyed.overwrite_arrow(batch, merge_by=True) == 2, "carried again, and said so"
    assert stored_rows(keyed) == {"A": 1, "B": 2}
    assert keyed.read_arrow_table().num_rows == 2, "a replay holds each key once"


def test_duplicate_keys_inside_a_chunk_collapse_to_the_first(keyed: MemoryDataset) -> None:
    keyed.overwrite_arrow_reader(
        reader_of(keyed_batch(["A", "A"], [1, 9]), keyed_batch(["A"], [8])),
        merge_by=True,
    )
    assert stored_rows(keyed) == {"A": 1}


def test_a_key_repeated_in_a_later_chunk_replaces_the_earlier(keyed: MemoryDataset) -> None:
    keyed.overwrite_arrow_reader(
        reader_of(keyed_batch(["A"], [1]), keyed_batch(["A"], [8])),
        merge_by=True,
        commit_row_size=1,
    )
    assert stored_rows(keyed) == {"A": 8}


def test_overwrite_by_a_list_names_the_columns(keyed: MemoryDataset) -> None:
    keyed.overwrite_arrow(keyed_batch(["A"], [1]), merge_by=["symbol"])
    keyed.overwrite_arrow(keyed_batch(["A"], [9]), merge_by=["symbol"])
    assert stored_rows(keyed) == {"A": 9}


def test_a_null_merge_key_is_refused(dataset: MemoryDataset) -> None:
    batch = batch_of(symbol=["A"], day=[datetime.date(2026, 8, 14)], size=[None])
    with pytest.raises(ValueError, match="merge key and cannot be null"):
        dataset.overwrite_arrow(batch, merge_by=["size"])


def test_overwrite_creates_what_is_not_there(keyed: MemoryDataset) -> None:
    assert not keyed.exists
    keyed.overwrite_arrow(keyed_batch(["A"], [1]), merge_by=True)
    assert keyed.exists and stored_rows(keyed) == {"A": 1}


def test_append_arrow_picks_the_method_by_what_it_is(keyed: MemoryDataset) -> None:
    batch = keyed_batch(["A"], [1])
    keyed.append_arrow(batch)
    keyed.append_arrow(pyarrow.Table.from_batches([batch]))
    keyed.append_arrow(reader_of(batch))
    assert keyed.read_arrow_table().num_rows == 3


# -- chunking ---------------------------------------------------------------


def test_chunks_group_a_stream_by_row_count() -> None:
    chunks = list(arrow_chunks(iter([rows(3), rows(3), rows(3)]), 4))
    assert [chunk.num_rows for chunk in chunks] == [4, 4, 1]


def test_chunks_flush_at_the_first_row_or_batch_bound() -> None:
    chunks = list(arrow_chunks(iter([rows(3), rows(3), rows(3)]), 5, 2))
    assert [chunk.num_rows for chunk in chunks] == [5, 4]


def test_chunks_group_a_stream_by_batch_count() -> None:
    chunks = list(arrow_chunks(iter([rows(1) for _ in range(10)]), None, 8))
    assert [chunk.num_rows for chunk in chunks] == [8, 2]


def test_chunks_refuse_an_unbounded_row_size() -> None:
    with pytest.raises(ValueError, match="row_size must be positive"):
        list(arrow_chunks(iter([rows(1)]), 0))


def test_chunks_refuse_an_unbounded_batch_count() -> None:
    with pytest.raises(ValueError, match="batch_num must be positive"):
        list(arrow_chunks(iter([rows(1)]), None, 0))


@pytest.mark.parametrize("name", ["row_size", "batch_num"])
@pytest.mark.parametrize("value", [True, 1.5, "2"])
def test_chunk_bounds_have_strict_integer_types(name: str, value: Any) -> None:
    arguments = {"row_size": None, "batch_num": None, name: value}
    with pytest.raises(TypeError, match=rf"{name} must be an integer"):
        list(arrow_chunks(iter([rows(1)]), **arguments))


def test_chunks_take_the_schema_from_a_reader() -> None:
    reader = pyarrow.RecordBatchReader.from_batches(rows(1).schema, iter([rows(1)]))
    (chunk,) = arrow_chunks(reader, None)
    assert chunk.schema.equals(reader.schema)


def test_chunks_ignore_explicit_zero_row_batches() -> None:
    assert list(arrow_chunks(iter([rows(0)]), None)) == []
    (chunk,) = arrow_chunks(iter([rows(0), rows(2), rows(0)]), None)
    assert chunk.num_rows == 2


# -- creating ---------------------------------------------------------------


def test_a_write_creates_what_is_not_there(dataset: MemoryDataset) -> None:
    assert not dataset.exists
    dataset.overwrite_arrow(rows(1))
    assert dataset.exists, "a write appends, and appending to nothing is a create"


def test_create_with_takes_whatever_names_a_shape(dataset: MemoryDataset) -> None:
    schema = pyarrow.schema([("symbol", pyarrow.string())])
    assert dataset.create_with(schema).into_arrow_schema().names == ["symbol"]
    assert dataset.create_with_arrow_schema(schema).exists
    assert dataset.create_with_arrow_field(
        pyarrow.field("q", pyarrow.struct([("a", pyarrow.int64())]))
    )
    assert [member.name for member in dataset.create_with(Quote).into_struct_field()] == [
        member.name for member in Quote.into_field()
    ]


def test_create_with_nothing_uses_the_declared_shape(dataset: MemoryDataset) -> None:
    assert dataset.create_with().into_struct_field() is Quote.into_field()


def test_get_or_create_is_idempotent(dataset: MemoryDataset) -> None:
    dataset.create_with()
    dataset.commits.append(pyarrow.Table.from_batches([rows(1)]))
    dataset.get_or_create()
    assert len(dataset.commits) == 1, "an existing dataset is left alone"


# -- generic redirects ------------------------------------------------------


def test_overwrite_arrow_picks_the_method_by_what_it_is(dataset: MemoryDataset) -> None:
    batch = rows(1)
    assert dataset.overwrite_arrow(batch) == 1
    assert dataset.overwrite_arrow(pyarrow.Table.from_batches([batch])) == 1
    assert dataset.overwrite_arrow(reader_of(batch)) == 1
    assert [commit.num_rows for commit in dataset.commits] == [1], "one key, replaced twice"


def test_read_arrow_picks_the_method_by_the_type_asked_for(dataset: MemoryDataset) -> None:
    dataset.overwrite_arrow(rows(2))
    assert isinstance(dataset.read_arrow(), pyarrow.Table)
    assert isinstance(dataset.read_arrow(pyarrow.Table), pyarrow.Table)
    assert isinstance(dataset.read_arrow(pyarrow.RecordBatchReader), pyarrow.RecordBatchReader)


def test_an_overwrite_of_something_unwritable_is_refused(dataset: MemoryDataset) -> None:
    with pytest.raises(TypeError, match="cannot infer"):
        dataset.overwrite_arrow("not arrow data")


# -- reading one out of a document -------------------------------------------


def test_an_implementation_behind_an_optional_dependency_is_imported_by_the_document() -> None:
    """And by nothing else, which is what keeps the dependency optional."""
    pytest.importorskip("pyiceberg")
    from rekep.iceberg import IcebergDataset

    field = Quote.into_field()
    built = Dataset.from_dict(
        {
            "kind": "iceberg",
            "name": "b",
            "namespace": "a",
            "field": field.into_dict(),
            "catalog_name": "c",
        }
    )
    assert isinstance(built, IcebergDataset)
    assert (built.name, built.namespace, built.field, built.catalog_name) == (
        "b",
        "a",
        replace_field(field, name="b"),
        "c",
    )


def test_a_document_with_no_kind_says_what_it_could_have_said() -> None:
    with pytest.raises(ValueError, match="add a `kind`"):
        Dataset.from_dict({"url": "a.log"})


def test_a_kind_nothing_implements_lists_what_does() -> None:
    with pytest.raises(ValueError, match="no dataset of kind 'parquet'"):
        Dataset.from_dict({"kind": "parquet"})


def test_every_shipped_kind_is_reachable_from_a_document() -> None:
    """Every lazy module registers the kind its document names."""
    pytest.importorskip("pyiceberg")
    built = Dataset._imported("iceberg")
    assert built is not None and built.into_kind() == "iceberg"


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

    count = 70_000
    chunk = joinable(range(count))
    stored = joinable(range(0, count, 100))
    fresh = anti_join(chunk, stored, ["at"])
    assert fresh.num_rows == count - len(range(0, count, 100))
    assert descents(fresh, "at") == 0, "the chunk's own order, not the join's"
    assert fresh.column("payload")[0].as_py() == "row-1", "and the right rows in it"


def test_an_anti_join_brings_the_keys_it_is_handed_onto_the_rows_types() -> None:
    """A scan hands a stored `string` back as `large_string`, and Acero
    refuses to join the two; the keys are cast, the rows are not."""
    stored = pyarrow.table(
        {"at": pyarrow.array(["a", "b", "c"], pyarrow.large_string()), "payload": [1, 2, 3]}
    )
    chunk = pyarrow.table({"at": pyarrow.array(["b"], pyarrow.string())})

    kept = anti_join(stored, chunk, ["at"])

    assert kept.column("at").to_pylist() == ["a", "c"]
    assert kept.schema.field("at").type == pyarrow.large_string(), "the rows keep their types"


def test_a_join_that_drops_nothing_is_the_table_itself() -> None:
    """The common case on a stream of new keys, and the one that must not pay
    for an ordering it already has."""
    from rekep.dataset import anti_join

    chunk = joinable(range(10))
    assert anti_join(chunk, joinable([]), ["at"]) is chunk


# -- ordering ----------------------------------------------------------------


def ordered_pairs(pairs: Sequence[tuple[int, int]]) -> pyarrow.Table:
    return pyarrow.table(
        {
            "at": pyarrow.array([at for at, _ in pairs], pyarrow.int64()),
            "seq": pyarrow.array([seq for _, seq in pairs], pyarrow.int64()),
            "payload": pyarrow.array(["x"] * len(pairs)),
        }
    )


@pytest.mark.parametrize(
    ("pairs", "ordered"),
    [
        ([(1, 0), (2, 0), (3, 0)], True),
        (
            [(1, 0), (1, 1), (1, 2)],
            True,
        ),
        ([(1, 1), (1, 0)], False),
        ([(2, 0), (1, 9)], False),
        ([(1, 0), (1, 0)], True),
        ([(1, 0)], True),
    ],
)
def test_sortedness_is_lexicographic_over_every_key(
    pairs: Sequence[tuple[int, int]], ordered: bool
) -> None:
    assert in_sort_order(ordered_pairs(pairs), ["at", "seq"]) is ordered


def test_sort_checks_apply_nulls_last_only_when_the_prefix_ties() -> None:
    rows = ordered_pairs([(1, 0), (2, 0)])
    holed = rows.set_column(
        rows.schema.get_field_index("seq"),
        pyarrow.field("seq", pyarrow.int64()),
        pyarrow.array([None, 1], pyarrow.int64()),
    )
    assert in_sort_order(holed, ["at", "seq"]) is True

    tied = holed.set_column(
        holed.schema.get_field_index("at"),
        holed.schema.field("at"),
        pyarrow.array([1, 1], pyarrow.int64()),
    )
    assert in_sort_order(tied, ["at", "seq"]) is False
    assert in_sort_order(tied.take(pyarrow.array([1, 0])), ["at", "seq"]) is True


def test_sort_checks_place_nan_after_numbers_and_before_null() -> None:
    ascending = pyarrow.table({"value": [1.0, 2.0, float("nan"), None]})
    descending = pyarrow.table({"value": [2.0, 1.0, float("nan"), None]})

    assert in_sort_order(ascending, [("value", "ascending")]) is True
    assert in_sort_order(descending, [("value", "descending")]) is True
    assert in_sort_order(ascending.take(pyarrow.array([2, 0, 1, 3])), ["value"]) is False


def test_an_unknown_sort_direction_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown sort direction"):
        sort_order_fields([("value", "sideways")])


# -- one row per key ---------------------------------------------------------


@pytest.mark.parametrize(
    ("keys", "kept"),
    [
        ([1, 2, 3], [1, 2, 3]),
        ([1, 1, 2, 2, 2, 3], [1, 2, 3]),
        ([3, 1, 2], [3, 1, 2]),
        ([3, 1, 3, 2, 1], [3, 1, 2]),
        ([1, 1], [1]),
        ([1], [1]),
        ([], []),
    ],
)
def test_one_row_per_key_keeps_the_first_in_the_tables_own_order(
    keys: Sequence[int], kept: Sequence[int]
) -> None:
    """Ordered or not, the answer is the same one: the first row of each key."""
    table = pyarrow.table(
        {
            "at": pyarrow.array(keys, pyarrow.int64()),
            "payload": pyarrow.array([f"row-{index}" for index in range(len(keys))]),
        }
    )
    first = first_rows(table, ["at"])
    assert first.column("at").to_pylist() == list(kept)
    assert first.column("payload").to_pylist() == [f"row-{list(keys).index(key)}" for key in kept]


def test_one_row_per_key_never_builds_a_key_table_for_an_ordered_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shape a stream arrives in: sorted on the key it is keyed on. The
    group that would answer it costs a hash table over every key in the
    chunk, and comparing neighbours answers the same question in a pass."""
    import rekep.dataset

    table = pyarrow.table(
        {
            "at": pyarrow.array([1, 1, 2, 3, 3, 3], pyarrow.int64()),
            "payload": pyarrow.array([f"row-{index}" for index in range(6)]),
        }
    )
    monkeypatch.setattr(
        rekep.dataset,
        "keys_of",
        lambda *_args, **_kwargs: pytest.fail("an ordered chunk needs no key table"),
    )

    first = first_rows(table, ["at"])
    assert first.column("at").to_pylist() == [1, 2, 3]
    assert first.column("payload").to_pylist() == ["row-0", "row-2", "row-3"]


def test_one_row_per_key_groups_nulls_and_nans_the_way_a_group_by_does() -> None:
    """Two nulls are one key and two NaNs are one key, ordered or not."""
    nan = float("nan")
    ordered = pyarrow.table({"at": pyarrow.array([1.0, 2.0, 2.0, nan, nan, None, None])})
    assert first_rows(ordered, ["at"]).num_rows == 4
    shuffled = pyarrow.table({"at": pyarrow.array([None, 2.0, nan, 2.0, None, nan, 1.0])})
    assert first_rows(shuffled, ["at"]).num_rows == 4
