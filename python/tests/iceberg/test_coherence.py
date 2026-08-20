"""Where we plan the work ourselves, the answer must be pyiceberg's own.

Every test here runs the same scenario twice -- once through this package, once
through the official library on an identical table -- and compares the rows that
come out. A faster path that returns something else is not an optimisation.
"""

import datetime
from pathlib import Path
from typing import Annotated

import pyarrow
import pyarrow.compute
import pytest

from rekep import Convertible, Field, field
from rekep.iceberg import IcebergCatalog, IcebergDataset


@field
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, Field.primary_key()]
    """Instrument."""

    day: Annotated[datetime.date, Field.partition_key()]
    """Trading day, and the partition."""

    seq: Annotated[int, Field.primary_key()]
    """Sequence within the day -- the second half of a composite key."""

    size: int
    """Quantity."""

    venue: str | None = None
    """Where it traded, when known."""


DAY = datetime.date(2026, 8, 14)


def properties(tmp_path: Path, name: str) -> dict[str, str]:
    warehouse = tmp_path / name
    warehouse.mkdir(parents=True, exist_ok=True)
    return {
        "type": "sql",
        "uri": f"sqlite:///{(tmp_path / f'{name}.db').as_posix()}",
        "warehouse": warehouse.as_uri(),
    }


def quotes(start: int, count: int, venue: str = "XPAR", *, days: int = 1) -> pyarrow.Table:
    """`count` rows from `start`, spread over `days` partitions."""
    return pyarrow.Table.from_pydict(
        {
            "symbol": [f"S{(start + i) % 7}" for i in range(count)],
            "day": [DAY + datetime.timedelta(days=(start + i) % days) for i in range(count)],
            "seq": [start + i for i in range(count)],
            "size": [(start + i) * 10 for i in range(count)],
            "venue": [venue] * count,
        },
        schema=Quote.FIELD.into_arrow_schema(),
    )


def sorted_rows(table: pyarrow.Table) -> list[dict]:
    """Rows in a stable order, so two tables compare by content alone."""
    keys = [(name, "ascending") for name in ("seq", "symbol") if name in table.column_names]
    return table.sort_by(keys).to_pylist()


@pytest.fixture
def pair(tmp_path: Path) -> tuple[IcebergDataset, IcebergDataset]:
    """Two identical tables: one this package writes, one pyiceberg does."""
    built = []
    for name in ("ours", "theirs"):
        catalog = IcebergCatalog(name=name, properties=properties(tmp_path, name))
        dataset = catalog.dataset("trading.quotes", struct=Quote.FIELD)
        dataset.create_with()
        built.append(dataset)
    ours, theirs = built
    assert ours.plan_merges, "the point of the comparison"
    theirs.plan_merges = False
    return ours, theirs


def merge(pair: tuple[IcebergDataset, IcebergDataset], chunk: pyarrow.Table, **kwargs) -> None:
    for dataset in pair:
        dataset.write_arrow(chunk, merge_by=True, commit_row_size=0, **kwargs)


# -- the merge --------------------------------------------------------------


def test_a_merge_into_an_empty_table_agrees(pair) -> None:
    merge(pair, quotes(0, 50))
    ours, theirs = pair
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


def test_a_merge_of_entirely_new_keys_agrees(pair) -> None:
    """The case the planning exists for: nothing can match, so nothing is read."""
    for dataset in pair:
        dataset.write_arrow(quotes(0, 100), commit_row_size=0)
    merge(pair, quotes(100, 100))
    ours, theirs = pair
    assert ours.read_arrow_table().num_rows == 200
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


def test_a_merge_of_unchanged_rows_agrees(pair) -> None:
    """Re-ingesting the same lines must not duplicate them -- or rewrite them."""
    for dataset in pair:
        dataset.write_arrow(quotes(0, 60), commit_row_size=0)
    merge(pair, quotes(0, 60))
    ours, theirs = pair
    assert ours.read_arrow_table().num_rows == 60
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


def test_a_merge_that_updates_values_agrees(pair) -> None:
    for dataset in pair:
        dataset.write_arrow(quotes(0, 60, "XPAR"), commit_row_size=0)
    merge(pair, quotes(0, 60, "XETR"))
    ours, theirs = pair
    stored = ours.read_arrow_table()
    assert set(stored.column("venue").to_pylist()) == {"XETR"}
    assert sorted_rows(stored) == sorted_rows(theirs.read_arrow_table())


def test_a_half_matching_merge_agrees(pair) -> None:
    """The interesting one: some rows update, some insert, in one chunk."""
    for dataset in pair:
        dataset.write_arrow(quotes(0, 80, "XPAR"), commit_row_size=0)
    merge(pair, quotes(40, 80, "XETR"))
    ours, theirs = pair
    stored = ours.read_arrow_table()
    assert stored.num_rows == 120
    assert sorted_rows(stored) == sorted_rows(theirs.read_arrow_table())


def test_a_merge_across_partitions_agrees(pair) -> None:
    for dataset in pair:
        dataset.write_arrow(quotes(0, 90, "XPAR", days=3), commit_row_size=0)
    merge(pair, quotes(45, 90, "XETR", days=3))
    ours, theirs = pair
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


def test_a_streamed_merge_agrees_with_a_single_one(pair) -> None:
    """Chunking changes how many commits happen, never what is stored."""
    ours, theirs = pair
    ours.write_arrow(quotes(0, 120, "XPAR"), commit_row_size=0)
    theirs.write_arrow(quotes(0, 120, "XPAR"), commit_row_size=0)
    ours.write_arrow_reader(
        iter(quotes(60, 120, "XETR").to_batches(max_chunksize=25)),
        merge_by=True,
        commit_row_size=25,
    )
    theirs.write_arrow(quotes(60, 120, "XETR"), merge_by=True, commit_row_size=0)
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


def test_a_merge_on_named_columns_agrees(pair) -> None:
    for dataset in pair:
        dataset.write_arrow(quotes(0, 40, "XPAR"), commit_row_size=0)
    for dataset in pair:
        dataset.write_arrow(quotes(20, 40, "XETR"), merge_by=["seq"], commit_row_size=0)
    ours, theirs = pair
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


def test_a_merge_on_a_branch_agrees(pair) -> None:
    for dataset in pair:
        dataset.write_arrow(quotes(0, 40), commit_row_size=0)
        dataset.create_branch("dev")
    merge(pair, quotes(20, 40, "XETR"), branch="dev")
    ours, theirs = pair
    assert sorted_rows(ours.read_arrow_table(branch="dev")) == sorted_rows(
        theirs.read_arrow_table(branch="dev")
    )
    assert ours.read_arrow_table().num_rows == 40, "main is untouched, as pyiceberg leaves it"


def test_duplicate_source_keys_are_refused_by_both(pair) -> None:
    doubled = pyarrow.concat_tables([quotes(0, 5), quotes(0, 5)])
    ours, theirs = pair
    for dataset in (ours, theirs):
        with pytest.raises(ValueError, match="[Dd]uplicate"):
            dataset.write_arrow(doubled, merge_by=True, commit_row_size=0)


def test_the_report_says_what_moved(pair) -> None:
    ours, _ = pair
    ours.write_arrow(quotes(0, 40, "XPAR"), commit_row_size=0)
    assert ours.merge_arrow_table(quotes(20, 40, "XETR"), True) == (20, 20)
    assert ours.merge_arrow_table(quotes(20, 40, "XETR"), True) == (0, 0), "nothing changed"


# -- reading ----------------------------------------------------------------


@pytest.fixture
def stored(tmp_path: Path) -> IcebergDataset:
    catalog = IcebergCatalog(name="read", properties=properties(tmp_path, "read"))
    dataset = catalog.dataset("trading.quotes", struct=Quote.FIELD)
    dataset.write_arrow(quotes(0, 300, days=5), commit_row_size=100)
    return dataset


def test_a_filtered_read_returns_what_filtering_afterwards_would(stored) -> None:
    whole = stored.read_arrow_table()
    expected = whole.filter(pyarrow.compute.equal(whole.column("day"), DAY))
    pushed = stored.read_arrow_table(row_filter=f"day = '{DAY}'")
    assert sorted_rows(pushed) == sorted_rows(expected)


def test_a_filtered_read_plans_fewer_files_than_a_whole_one(stored) -> None:
    """Pruning is the point; a fast scan that read everything got lucky."""
    table = stored.iceberg_table
    everything = len(list(table.scan().plan_files()))
    one_day = len(list(table.scan(row_filter=f"day = '{DAY}'").plan_files()))
    assert 0 < one_day < everything


def test_a_projection_returns_what_selecting_afterwards_would(stored) -> None:
    narrow = Field.from_arrow_schema(
        pyarrow.schema([Quote.FIELD.into_arrow_schema().field(name) for name in ("seq", "size")]),
        "Narrow",
    )
    pushed = stored.read_arrow_table(narrow)
    assert pushed.column_names == ["seq", "size"]
    assert sorted_rows(pushed) == sorted_rows(stored.read_arrow_table().select(["seq", "size"]))


def test_a_projection_does_not_read_the_columns_it_drops(stored) -> None:
    narrow = Field.from_arrow_schema(
        pyarrow.schema([Quote.FIELD.into_arrow_schema().field("seq")]), "Narrow"
    )
    assert stored._selected(None, narrow) == ("seq",), "the scan is told, not the cast"
    assert stored._selected(["size"], narrow) == ("size",), "an explicit list wins"
    assert stored._selected(None, None) == ("*",)


def test_a_shape_the_table_does_not_have_still_reads(stored) -> None:
    """A column the target declares and the store lacks is filled, not refused."""
    wider = Quote.FIELD.merge_with(pyarrow.schema([("desk", pyarrow.string())]))
    table = stored.read_arrow_table(wider)
    assert table.column("desk").null_count == table.num_rows


# -- what the store itself says ---------------------------------------------


def test_the_official_library_reads_what_we_wrote(stored) -> None:
    """No wrapper on the read side: pyiceberg's own scan, on its own terms."""
    ours = stored.read_arrow_table()
    theirs = stored.iceberg_catalog.load_table("trading.quotes").scan().to_arrow()
    assert theirs.num_rows == ours.num_rows
    assert sorted_rows(theirs.select(ours.column_names)) == sorted_rows(ours)


def test_the_schema_we_declare_is_the_schema_it_stores(stored) -> None:
    declared = Quote.FIELD.into_iceberg_schema()
    stored_schema = stored.iceberg_table.schema()
    assert [(f.name, str(f.field_type), f.required, f.doc) for f in stored_schema.fields] == [
        (f.name, str(f.field_type), f.required, f.doc) for f in declared.fields
    ]
    assert stored_schema.identifier_field_ids == declared.identifier_field_ids


def test_the_partition_spec_we_declare_is_the_one_it_partitions_by(stored) -> None:
    assert [f.name for f in stored.iceberg_table.spec().fields] == ["day"]
    assert stored.table_field.partition_keys() == {"day": "identity"}


# -- the vectorised comparison ----------------------------------------------


def compare_pair(rows: int = 400) -> tuple[pyarrow.Table, pyarrow.Table]:
    """A source and a stored table that differ in every way that matters."""
    schema = pyarrow.schema(
        [
            ("k", pyarrow.int64()),
            ("text", pyarrow.string()),
            ("value", pyarrow.float64()),
            ("count", pyarrow.int32()),
        ]
    )
    source = pyarrow.Table.from_pydict(
        {
            "k": list(range(rows)),
            "text": [f"v{i % 13}" if i % 11 else None for i in range(rows)],
            "value": [float(i) for i in range(rows)],
            "count": [i % 7 for i in range(rows)],
        },
        schema=schema,
    )
    stored = pyarrow.Table.from_pydict(
        {
            "k": list(range(rows)),
            "text": [f"v{i % 13}" if i % 11 else None for i in range(rows)],
            "value": [float(i) + (1 if i % 5 == 0 else 0) for i in range(rows)],
            "count": [i % 7 if i % 13 else None for i in range(rows)],
        },
        schema=schema,
    )
    return source, stored


def rows_of(table: pyarrow.Table) -> list[dict]:
    return table.sort_by("k").to_pylist()


@pytest.mark.parametrize("cut", [0, 1, 200, 400], ids=["empty", "one", "half", "whole"])
def test_the_vectorised_comparison_finds_what_pyiceberg_finds(cut: int) -> None:
    """Nulls on one side, values on the other, and every size of overlap."""
    from pyiceberg.table import upsert_util

    from rekep.iceberg.dataset import _changed

    source, stored = compare_pair()
    stored = stored.slice(0, cut)
    assert rows_of(_changed(source, stored, ["k"])) == rows_of(
        upsert_util.get_rows_to_update(source, stored, ["k"])
    )


def test_unchanged_rows_are_not_rewritten() -> None:
    from rekep.iceberg.dataset import _changed

    source, _ = compare_pair()
    assert len(_changed(source, source, ["k"])) == 0


def test_a_column_arrow_cannot_compare_falls_back_to_pyiceberg() -> None:
    """A struct has no equality kernel, so the slow path is the correct path."""
    from pyiceberg.table import upsert_util

    from rekep.iceberg.dataset import _changed

    schema = pyarrow.schema(
        [("k", pyarrow.int64()), ("book", pyarrow.struct([("bid", pyarrow.float64())]))]
    )
    source = pyarrow.Table.from_pylist([{"k": 1, "book": {"bid": 1.0}}], schema=schema)
    stored = pyarrow.Table.from_pylist([{"k": 1, "book": {"bid": 2.0}}], schema=schema)
    assert (
        _changed(source, stored, ["k"]).to_pylist()
        == upsert_util.get_rows_to_update(source, stored, ["k"]).to_pylist()
    )


def test_a_stored_table_with_duplicate_keys_is_refused() -> None:
    from rekep.iceberg.dataset import _changed

    source, stored = compare_pair(20)
    doubled = pyarrow.concat_tables([stored, stored])
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        _changed(source, doubled, ["k"])


# -- what a filter really touches -------------------------------------------


def test_the_plan_says_what_a_filter_skips(stored) -> None:
    whole = stored.scan_plan()
    assert whole["skipped"] == 0
    assert whole["files"] == whole["total_files"] > 1
    assert whole["rows"] == stored.read_arrow_table().num_rows

    one_day = stored.scan_plan(f"day = '{DAY}'")
    assert one_day["files"] < whole["files"]
    assert one_day["skipped"] == whole["files"] - one_day["files"]
    assert 0 < one_day["bytes"] < whole["bytes"]


def test_a_filter_that_cannot_prune_says_so(stored) -> None:
    """The point of the plan: a correct answer that read everything."""
    assert stored.scan_plan("venue != 'XPAR'")["skipped"] == 0


def test_a_merge_of_new_keys_plans_nothing(stored) -> None:
    """What turns a merge into an append -- the reason it is worth planning."""
    from rekep.iceberg.dataset import _key_ranges

    fresh = quotes(10_000, 20)
    assert stored.scan_plan(_key_ranges(fresh, ["symbol", "seq"]))["files"] == 0


# -- the scan filter is a superset, and must be treated as one --------------


def test_a_duplicate_outside_the_chunks_keys_does_not_abort_the_merge(pair) -> None:
    """The range filter reads rows the chunk never mentions; they are not its business.

    A stored duplicate anywhere inside the chunk's key range would otherwise
    abort a merge that has nothing to do with it -- while pyiceberg's own upsert
    completes, because its filter names the keys one by one.
    """
    ours, theirs = pair
    for dataset in pair:
        dataset.write_arrow(quotes(0, 300, days=3), commit_row_size=0)
        # A key the chunk below never touches, stored twice.
        stray = quotes(900, 1)
        dataset.write_arrow(stray, commit_row_size=0)
        dataset.write_arrow(stray, commit_row_size=0)
    chunk = quotes(0, 300, "XETR", days=3)
    assert len(chunk) > 200, "past MERGE_IN_LIMIT, so the filter is a range"
    merge(pair, chunk)
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


def test_a_duplicate_the_chunk_does_match_is_still_refused(pair) -> None:
    """Being lenient about the rest does not make this one safe.

    Deliberately stricter than the library here: pyiceberg checks the stored
    rows for duplicate keys one record batch at a time, so two copies of a key
    in two different files slip past it and the upsert writes a third. Refusing
    is the honest answer -- a table whose identifier fields do not identify a
    row cannot be merged into.
    """
    ours, _ = pair
    ours.write_arrow(quotes(0, 5), commit_row_size=0)
    ours.write_arrow(quotes(0, 5), commit_row_size=0)  # every key now stored twice
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        ours.write_arrow(quotes(0, 5, "XETR"), merge_by=True, commit_row_size=0)


def test_an_update_past_the_in_limit_still_prunes(tmp_path: Path) -> None:
    """The delete side of a merge names its rows exactly, and past 200 of them
    Iceberg stops using that list to prune -- unless the ranges are ANDed on.

    Single-column keys only: pyiceberg's ceiling is on `In` literals, and a
    composite key becomes an `Or` of equalities, which keeps pruning (slowly).
    """
    from pyiceberg.expressions import And
    from pyiceberg.table.upsert_util import create_match_filter

    from rekep.iceberg.dataset import _key_ranges

    catalog = IcebergCatalog(name="wide", properties=properties(tmp_path, "wide"))
    dataset = catalog.dataset("trading.quotes", struct=Quote.FIELD)
    # One commit per key range, so the files carry disjoint bounds -- which is
    # what makes a range predicate able to skip any of them at all.
    for start in range(0, 1_000, 200):
        dataset.write_arrow(quotes(start, 200), commit_row_size=0)

    updates = quotes(0, 250, "XETR")  # 250 keys: past the 200-literal ceiling
    exact = create_match_filter(updates, ["seq"])
    assert dataset.scan_plan(exact)["skipped"] == 0, "the exact filter alone cannot prune"
    narrowed = And(exact, _key_ranges(updates, ["seq"]))
    assert dataset.scan_plan(narrowed)["skipped"] > 0, "the ranges are what prune it"


# -- what Arrow and Iceberg disagree about ----------------------------------


@field
class Nested(Convertible):
    """A row with a column Arrow cannot compare."""

    key: Annotated[str, Field.primary_key()]
    """Identity."""

    size: int
    """Quantity."""

    book: dict[str, int] | None = None
    """A map: no equality kernel, and no join may carry it."""


def nested_rows(keys: range, size: int) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {
            "key": [f"K{i}" for i in keys],
            "size": [size] * len(keys),
            "book": [[("bid", 1)] for _ in keys],
        },
        schema=Nested.FIELD.into_arrow_schema(),
    )


def nested_pair(tmp_path: Path) -> tuple[IcebergDataset, IcebergDataset]:
    built = []
    for name in ("nested-ours", "nested-theirs"):
        catalog = IcebergCatalog(name=name, properties=properties(tmp_path, name))
        dataset = catalog.dataset("trading.nested", struct=Nested.FIELD)
        dataset.create_with()
        built.append(dataset)
    built[1].plan_merges = False
    return built[0], built[1]


def test_a_nested_column_does_not_stop_a_merge(tmp_path: Path) -> None:
    """Arrow refuses a map as join payload, so no join may carry one."""
    ours, theirs = nested_pair(tmp_path)
    for dataset in (ours, theirs):
        dataset.write_arrow(nested_rows(range(4), 1), commit_row_size=0)
        dataset.write_arrow(nested_rows(range(2, 6), 9), merge_by=True, commit_row_size=0)
    assert ours.read_arrow_table().num_rows == 6
    assert sorted(ours.read_arrow_table().column("size").to_pylist()) == sorted(
        theirs.read_arrow_table().column("size").to_pylist()
    )


def test_a_signed_zero_key_matches_the_zero_it_equals(tmp_path: Path) -> None:
    """`-0.0 == 0.0` in Python and in Iceberg; they hash apart in Arrow."""

    @field
    class Level(Convertible):
        """A price level."""

        price: float
        """The key, deliberately a float."""

        size: int
        """Quantity."""

    stored = pyarrow.Table.from_pydict(
        {"price": [0.0], "size": [1]}, schema=Level.FIELD.into_arrow_schema()
    )
    incoming = pyarrow.Table.from_pydict(
        {"price": [-0.0], "size": [2]}, schema=Level.FIELD.into_arrow_schema()
    )
    catalog = IcebergCatalog(name="zero", properties=properties(tmp_path, "zero"))
    dataset = catalog.dataset("trading.levels", struct=Level.FIELD)
    dataset.write_arrow(stored, commit_row_size=0)
    dataset.write_arrow(incoming, merge_by=["price"], commit_row_size=0)
    assert dataset.refresh().read_arrow_table().num_rows == 1, "one price, not two"


def test_a_null_merge_key_is_refused(stored) -> None:
    """No predicate finds the row it would match, so merging it duplicates it."""
    rows = pyarrow.Table.from_pydict(
        {
            "symbol": pyarrow.array([None], pyarrow.string()),
            "day": [DAY],
            "seq": [1],
            "size": [1],
            "venue": ["XPAR"],
        },
        schema=Quote.FIELD.into_arrow_schema(),
    )
    with pytest.raises(ValueError, match="cannot be null"):
        stored.merge_arrow_table(rows, True)


def test_an_empty_chunk_reads_nothing(stored) -> None:
    empty = Quote.FIELD.into_arrow_schema().empty_table()
    assert stored.merge_arrow_table(empty, True) == (0, 0)


def test_a_chunk_the_table_would_refuse_is_refused_the_same_way(stored) -> None:
    """Whatever `Table.upsert`'s schema check rejects, this rejects too."""
    wrong = pyarrow.Table.from_pydict(
        {"symbol": ["A"], "day": [DAY], "seq": [1], "size": ["not a number"], "venue": ["X"]}
    )
    with pytest.raises(ValueError, match="[Mm]ismatch|not compatible|type"):
        stored.merge_arrow_table(wrong, True)


@pytest.mark.parametrize(
    "case",
    ["uuid", "int32-key", "naive-vs-zoned", "struct", "dictionary"],
)
def test_the_comparison_agrees_or_hands_back(case: str) -> None:
    """Every type Arrow cannot compare is pyiceberg's problem, not a crash."""
    import datetime as dt
    import uuid as uuidlib

    from pyiceberg.table import upsert_util

    from rekep.iceberg.dataset import _changed

    if case == "uuid":
        ids = pyarrow.array(
            [uuidlib.uuid4().bytes, uuidlib.uuid4().bytes], pyarrow.binary(16)
        ).cast(pyarrow.uuid())
        source = pyarrow.table({"k": [1, 2], "id": ids})
        target = pyarrow.table({"k": [1, 2], "id": ids})
    elif case == "int32-key":
        source = pyarrow.table({"k": pyarrow.array([1, 2], pyarrow.int32()), "v": [1, 2]})
        target = pyarrow.table({"k": pyarrow.array([1, 2], pyarrow.int64()), "v": [1, 9]})
    elif case == "naive-vs-zoned":
        source = pyarrow.table(
            {"k": [1], "t": pyarrow.array([dt.datetime(2026, 8, 14)], pyarrow.timestamp("us"))}  # noqa: DTZ001
        )
        target = pyarrow.table(
            {
                "k": [1],
                "t": pyarrow.array(
                    [dt.datetime(2026, 8, 14, tzinfo=dt.UTC)], pyarrow.timestamp("us", tz="UTC")
                ),
            }
        )
    elif case == "struct":
        book = pyarrow.array([{"bid": 1.0}], pyarrow.struct([("bid", pyarrow.float64())]))
        source = pyarrow.table({"k": [1], "book": book})
        target = pyarrow.table({"k": [1], "book": book})
    else:
        words = pyarrow.array(["a", "b"]).dictionary_encode()
        source = pyarrow.table({"k": [1, 2], "w": words})
        target = pyarrow.table({"k": [1, 2], "w": words})

    ours = _changed(source, target, ["k"])
    theirs = upsert_util.get_rows_to_update(source, target, ["k"])
    assert ours.num_rows == theirs.num_rows
    assert ours.sort_by("k").to_pylist() == theirs.sort_by("k").to_pylist()


# -- partition transforms ---------------------------------------------------


@field
class Event(Convertible):
    """One event, partitioned by a *transform* of its timestamp."""

    at: Annotated[datetime.datetime, Field.primary_key(), Field.partition_key("day")]
    """When it happened, and the partition it lands in -- by day, not by value."""

    size: int
    """Quantity."""


def events(indexes: range, version: int) -> pyarrow.Table:
    """Rows five hours apart, so every partition holds several."""
    start = datetime.datetime(2026, 1, 1)
    return pyarrow.Table.from_pydict(
        {
            "at": [start + datetime.timedelta(hours=index * 5) for index in indexes],
            "size": [version * 1000 + index for index in indexes],
        },
        schema=Event.FIELD.into_arrow_schema(),
    )


@pytest.fixture
def event_pair(tmp_path: Path) -> tuple[IcebergDataset, IcebergDataset]:
    built = []
    for name in ("ours", "theirs"):
        catalog = IcebergCatalog(name=name, properties=properties(tmp_path, name))
        built.append(catalog.dataset("trading.events", struct=Event.FIELD).create_with())
    return built[0], built[1]


def test_a_merge_through_a_partition_transform_agrees(event_pair) -> None:
    """`_key_ranges` names the raw column; Iceberg prunes on `day(at)`.

    A projection that went the wrong way would plan no file, match nothing and
    insert a second copy of every row -- so this compares row for row.
    """
    ours, theirs = event_pair
    for dataset in event_pair:
        dataset.write_arrow(events(range(60), 0), commit_row_size=0)
        dataset.write_arrow(events(range(30, 90), 1), merge_by=True, commit_row_size=0)
    order = [("at", "ascending")]
    assert ours.read_arrow_table().num_rows == 90, "merged, not duplicated"
    assert (
        ours.read_arrow_table().sort_by(order).to_pylist()
        == theirs.read_arrow_table().sort_by(order).to_pylist()
    )


def test_a_transformed_partition_prunes_a_read(event_pair) -> None:
    """The point of `day(at)`: a day's filter opens a day's files."""
    ours, _ = event_pair
    for start in range(0, 90, 30):
        ours.write_arrow(events(range(start, start + 30), 0), commit_row_size=0)
    plan = ours.scan_plan("at >= '2026-01-02T00:00:00' and at < '2026-01-03T00:00:00'")
    assert plan["skipped"] > 0, "a day is one partition, not the whole table"
    assert plan["files"] < plan["total_files"]


def test_a_nan_merge_key_is_refused_by_both(tmp_path: Path) -> None:
    """No literal can name a NaN, so neither library will build a filter with one."""

    @field
    class Level(Convertible):
        """A price level."""

        price: float
        """The key, deliberately a float."""

        size: int
        """Quantity."""

    schema = Level.FIELD.into_arrow_schema()
    catalog = IcebergCatalog(name="nan", properties=properties(tmp_path, "nan"))
    dataset = catalog.dataset("trading.levels", struct=Level.FIELD)
    dataset.write_arrow(
        pyarrow.Table.from_pydict({"price": [1.0], "size": [1]}, schema=schema), commit_row_size=0
    )
    chunk = pyarrow.Table.from_pydict({"price": [float("nan")], "size": [2]}, schema=schema)
    with pytest.raises(ValueError, match="NaN"):
        dataset.merge_arrow_table(chunk, ["price"])
    with pytest.raises(ValueError, match="NaN"):
        dataset.get_or_create_table().upsert(chunk, join_cols=["price"])
