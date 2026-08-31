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

from rekep import Convertible, Field, scalar
from rekep.iceberg import IcebergCatalog, IcebergDataset
from rekep.iceberg.dataset import MERGE_IN_LIMIT

from ..conftest import catalog_properties

pytestmark = pytest.mark.integration


@scalar
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


@scalar
class Ordered(Convertible):
    """One event under the chronological key used by streamed inserts."""

    unix: Annotated[int, Field.primary_key(), Field.sort_key()]
    """Event time."""

    hash: Annotated[int, Field.primary_key()]
    """Content identity."""

    payload: str = "x"
    """Payload."""


DAY = datetime.date(2026, 8, 14)


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
        schema=Quote.into_field().into_arrow_schema(),
    )


def sorted_rows(table: pyarrow.Table) -> list[dict]:
    """Rows in a stable order, so two tables compare by content alone."""
    keys = [(name, "ascending") for name in ("seq", "symbol") if name in table.column_names]
    return table.sort_by(keys).to_pylist()


def ordered(start: int, count: int) -> pyarrow.Table:
    return pyarrow.Table.from_pydict(
        {
            "unix": list(range(start, start + count)),
            "hash": [index * 1_000_003 for index in range(start, start + count)],
            "payload": ["x"] * count,
        },
        schema=Ordered.into_field().into_arrow_schema(),
    )


@pytest.fixture
def pair(tmp_path: Path) -> tuple[IcebergDataset, IcebergDataset]:
    """Two identical tables: one this package writes, one pyiceberg does."""
    built = []
    for name in ("ours", "theirs"):
        catalog = IcebergCatalog(catalog_name=name, properties=catalog_properties(tmp_path, name))
        dataset = catalog.dataset("trading.quotes", field=Quote.into_field())
        dataset.create_with()
        built.append(dataset)
    ours, theirs = built
    assert ours.plan_merges, "the point of the comparison"
    theirs.plan_merges = False
    return ours, theirs


def merge(pair: tuple[IcebergDataset, IcebergDataset], chunk: pyarrow.Table, **kwargs) -> None:
    for dataset in pair:
        dataset.overwrite_arrow(chunk, merge_by=True, commit_row_size=1_000_000, **kwargs)


# -- the merge --------------------------------------------------------------


def test_a_merge_into_an_empty_table_agrees(pair) -> None:
    merge(pair, quotes(0, 50))
    ours, theirs = pair
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


def test_monotonic_insert_shortcut_agrees_with_pyiceberg_across_the_literal_limit(
    tmp_path: Path,
) -> None:
    """Fresh 200- and 201-row commits keep the library's rows and storage shape."""
    built = []
    for name in ("ours_ordered", "theirs_ordered"):
        catalog = IcebergCatalog(catalog_name=name, properties=catalog_properties(tmp_path, name))
        built.append(catalog.dataset("trading.ordered", field=Ordered.into_field()).create_with())
    ours, theirs = built
    for chunk in (ordered(0, MERGE_IN_LIMIT), ordered(MERGE_IN_LIMIT, MERGE_IN_LIMIT + 1)):
        ours.append_arrow_table(chunk, merge_by=True, commit_row_size=1_000_000)
        theirs.iceberg_table.upsert(chunk, join_cols=["unix", "hash"])

    assert ours.read_arrow_table().sort_by("unix").to_pylist() == (
        theirs.read_arrow_table().sort_by("unix").to_pylist()
    )
    for dataset in (ours, theirs):
        assert (
            dataset.records,
            dataset.data_files().num_rows,
            dataset.iceberg_table.inspect.manifests().num_rows,
            len(dataset.iceberg_table.snapshots()),
        ) == (401, 2, 2, 2)


def test_a_merge_of_entirely_new_keys_agrees(pair) -> None:
    """The case the planning exists for: nothing can match, so nothing is read."""
    for dataset in pair:
        dataset.append_arrow(quotes(0, 100), commit_row_size=1_000_000)
    merge(pair, quotes(100, 100))
    ours, theirs = pair
    assert ours.read_arrow_table().num_rows == 200
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


@pytest.mark.parametrize("count", [MERGE_IN_LIMIT, MERGE_IN_LIMIT + 1])
def test_new_exact_keys_inside_stored_bounds_agree_at_the_literal_limit(pair, count) -> None:
    """An exact miss is one append even when its safe scan range reads stored rows."""
    rows = quotes(0, count * 2)
    stored = rows.take(pyarrow.array(range(0, count * 2, 2)))
    incoming = rows.take(pyarrow.array(range(1, count * 2, 2)))
    for dataset in pair:
        dataset.append_arrow(stored, commit_row_size=1_000_000)

    merge(pair, incoming)
    ours, theirs = pair
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())
    for dataset in pair:
        assert (
            dataset.data_files().num_rows,
            dataset.iceberg_table.inspect.manifests().num_rows,
            len(dataset.iceberg_table.snapshots()),
        ) == (2, 2, 2)


def test_a_merge_of_unchanged_rows_agrees(pair) -> None:
    """Re-ingesting the same lines must not duplicate them -- or rewrite them."""
    for dataset in pair:
        dataset.append_arrow(quotes(0, 60), commit_row_size=1_000_000)
    merge(pair, quotes(0, 60))
    ours, theirs = pair
    assert ours.read_arrow_table().num_rows == 60
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


def test_a_merge_that_updates_values_agrees(pair) -> None:
    for dataset in pair:
        dataset.append_arrow(quotes(0, 60, "XPAR"), commit_row_size=1_000_000)
    merge(pair, quotes(0, 60, "XETR"))
    ours, theirs = pair
    stored = ours.read_arrow_table()
    assert set(stored.column("venue").to_pylist()) == {"XETR"}
    assert sorted_rows(stored) == sorted_rows(theirs.read_arrow_table())


def test_a_half_matching_merge_agrees(pair) -> None:
    """The interesting one: some rows update, some insert, in one chunk."""
    for dataset in pair:
        dataset.append_arrow(quotes(0, 80, "XPAR"), commit_row_size=1_000_000)
    merge(pair, quotes(40, 80, "XETR"))
    ours, theirs = pair
    stored = ours.read_arrow_table()
    assert stored.num_rows == 120
    assert sorted_rows(stored) == sorted_rows(theirs.read_arrow_table())


def test_a_merge_across_partitions_agrees(pair) -> None:
    for dataset in pair:
        dataset.append_arrow(quotes(0, 90, "XPAR", days=3), commit_row_size=1_000_000)
    merge(pair, quotes(45, 90, "XETR", days=3))
    ours, theirs = pair
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


def test_a_streamed_merge_agrees_with_a_single_one(pair) -> None:
    """Chunking changes how many commits happen, never what is stored."""
    ours, theirs = pair
    ours.append_arrow(quotes(0, 120, "XPAR"), commit_row_size=1_000_000)
    theirs.append_arrow(quotes(0, 120, "XPAR"), commit_row_size=1_000_000)
    ours.overwrite_arrow_reader(
        iter(quotes(60, 120, "XETR").to_batches(max_chunksize=25)),
        merge_by=True,
        commit_row_size=25,
    )
    theirs.overwrite_arrow(quotes(60, 120, "XETR"), merge_by=True, commit_row_size=1_000_000)
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


def test_a_merge_on_named_columns_agrees(pair) -> None:
    for dataset in pair:
        dataset.append_arrow(quotes(0, 40, "XPAR"), commit_row_size=1_000_000)
    for dataset in pair:
        dataset.overwrite_arrow(quotes(20, 40, "XETR"), merge_by=["seq"], commit_row_size=1_000_000)
    ours, theirs = pair
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


def test_a_merge_on_a_branch_agrees(pair) -> None:
    for dataset in pair:
        dataset.append_arrow(quotes(0, 40), commit_row_size=1_000_000)
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
            dataset.overwrite_arrow(doubled, merge_by=True, commit_row_size=1_000_000)


def test_the_report_says_what_moved(pair) -> None:
    ours, _ = pair
    ours.append_arrow(quotes(0, 40, "XPAR"), commit_row_size=1_000_000)
    assert ours.merge_arrow_table(quotes(20, 40, "XETR"), True) == (20, 20)
    assert ours.merge_arrow_table(quotes(20, 40, "XETR"), True) == (0, 0), "nothing changed"


# -- reading ----------------------------------------------------------------


@pytest.fixture
def stored(tmp_path: Path) -> IcebergDataset:
    catalog = IcebergCatalog(catalog_name="read", properties=catalog_properties(tmp_path, "read"))
    dataset = catalog.dataset("trading.quotes", field=Quote.into_field())
    dataset.append_arrow(quotes(0, 300, days=5), commit_row_size=100)
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
        pyarrow.schema(
            [Quote.into_field().into_arrow_schema().field(name) for name in ("seq", "size")]
        ),
        "Narrow",
    )
    pushed = stored.read_arrow_table(narrow)
    assert pushed.column_names == ["seq", "size"]
    assert sorted_rows(pushed) == sorted_rows(stored.read_arrow_table().select(["seq", "size"]))


def test_a_projection_does_not_read_the_columns_it_drops(stored) -> None:
    """The scan is told the shape, rather than the cast dropping columns after."""
    narrow = Field.from_arrow_schema(
        pyarrow.schema([Quote.into_field().into_arrow_schema().field("seq")]), "Narrow"
    )
    scan = stored.iceberg_table.scan()
    assert stored._selected(narrow, scan) == {"seq": "seq"}, "the scan is told, not the cast"
    assert stored.read_arrow_table(narrow).column_names == ["seq"]
    assert stored.read_arrow_table(columns=["size"]).column_names == ["size"], "an explicit list"
    assert stored.read_arrow_table().column_names == Quote.into_field().names, (
        "no shape, every column"
    )


def test_explicit_columns_narrow_the_requested_schema(stored) -> None:
    """`columns` intersects the cast shape instead of only narrowing its scan."""
    narrow = Field.from_arrow_schema(
        pyarrow.schema(
            [Quote.into_field().into_arrow_schema().field(name) for name in ("seq", "size")]
        ),
        "Narrow",
    )
    rows = stored.read_arrow_table(narrow, columns=["venue", "size", "seq"])
    assert rows.column_names == ["size", "seq"]
    assert sorted_rows(rows) == sorted_rows(stored.read_arrow_table().select(["size", "seq"]))


@pytest.mark.parametrize("pin", ["snapshot", "branch"])
def test_a_pinned_read_follows_the_schema_that_snapshot_was_written_under(
    tmp_path: Path, pin: str
) -> None:
    """A rename is metadata-only, so an older snapshot answers to the old names.

    Matching the target's columns by name against the *current* schema leaves
    the renamed one out of the projection and then fills it with nulls -- the
    data is on disk and readable, and nothing raises. Compared against
    pyiceberg's own scan of the same snapshot, which is where the values are.
    """
    catalog = IcebergCatalog(
        catalog_name="evolved", properties=catalog_properties(tmp_path, "evolved")
    )
    dataset = catalog.dataset("trading.quotes", field=Quote.into_field())
    dataset.append_arrow(quotes(0, 3), commit_row_size=1_000_000)
    table = dataset.get_or_create_table()
    snapshot = table.current_snapshot().snapshot_id
    table.manage_snapshots().create_branch(snapshot, "old").commit()
    with table.update_schema() as update:
        update.rename_column("venue", "market")
    dataset.refresh()
    pinned = {"snapshot_id": snapshot} if pin == "snapshot" else {"branch": "old"}

    official = table.scan(snapshot_id=snapshot).to_arrow()
    assert official.column("venue").to_pylist() == ["XPAR"] * 3, "the data is there"

    # The shape as the table declares it *now* -- which is what a caller has.
    rows = dataset.read_arrow_table(dataset.table_field, **pinned)
    assert rows.column("market").to_pylist() == ["XPAR"] * 3, "under the name it has now"
    # And the shape as it was then, which is what a caller who kept one has:
    # the column comes back under the name it was asked for either way.
    then = dataset.read_arrow_table(Quote.into_field(), **pinned)
    assert then.column("venue").to_pylist() == ["XPAR"] * 3, "under the name it had then"

    # And a column that snapshot never had is still filled, not refused.
    from pyiceberg.types import StringType

    with dataset.get_or_create_table().update_schema() as update:
        update.add_column("desk", StringType())
    dataset.refresh()
    wider = dataset.read_arrow_table(dataset.table_field, **pinned)
    assert wider.column("desk").to_pylist() == [None] * 3
    assert wider.column("market").to_pylist() == ["XPAR"] * 3


def test_a_shape_the_table_does_not_have_still_reads(stored) -> None:
    """A column the target declares and the store lacks is filled, not refused."""
    wider = Quote.into_field().merge_with(pyarrow.schema([("desk", pyarrow.string())]))
    table = stored.read_arrow_table(wider)
    assert table.column("desk").null_count == table.num_rows


# -- what the store itself says ---------------------------------------------


def test_the_official_library_reads_what_we_wrote(stored) -> None:
    """No wrapper on the read side: pyiceberg's own scan, on its own terms."""
    ours = stored.read_arrow_table()
    theirs = stored.catalog.load_table(stored.identifier).scan().to_arrow()
    assert theirs.num_rows == ours.num_rows
    assert sorted_rows(theirs.select(ours.column_names)) == sorted_rows(ours)


def test_the_schema_we_declare_is_the_schema_it_stores(stored) -> None:
    declared = Quote.into_field().into_iceberg_schema()
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


def test_a_column_arrow_cannot_compare_is_compared_in_python() -> None:
    """A struct has no equality kernel, so that column is read out and compared
    the way the library would. The answer is the library's."""
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


NESTED_KINDS = {
    "list": pyarrow.list_(pyarrow.int64()),
    "struct": pyarrow.struct([("bid", pyarrow.float64())]),
    "map": pyarrow.map_(pyarrow.string(), pyarrow.string()),
}
NESTED_VALUES = {
    "list": ([1, 2], [1, 3], [None, 2]),
    "struct": ({"bid": 1.0}, {"bid": 2.0}, {"bid": None}),
    "map": ({"a": "1"}, {"a": "2"}, {"a": None}),
}


@pytest.mark.parametrize("kind", sorted(NESTED_KINDS))
def test_a_nested_column_agrees_with_the_library_row_for_row(kind: str) -> None:
    """Every shape Arrow has no equality kernel for, against every shape of
    disagreement: same, different, one side null, both null, a null inside."""
    from pyiceberg.table import upsert_util

    from rekep.iceberg.dataset import _changed

    one, other, holed = NESTED_VALUES[kind]
    schema = pyarrow.schema([("k", pyarrow.int64()), ("nested", NESTED_KINDS[kind])])
    left = [one, one, one, None, None, holed, holed]
    right = [one, other, None, None, one, holed, one]
    source = pyarrow.Table.from_pylist(
        [{"k": index, "nested": value} for index, value in enumerate(left)], schema=schema
    )
    stored = pyarrow.Table.from_pylist(
        [{"k": index, "nested": value} for index, value in enumerate(right)], schema=schema
    )
    assert rows_of(_changed(source, stored, ["k"])) == rows_of(
        upsert_util.get_rows_to_update(source, stored, ["k"])
    )


def test_a_nested_column_does_not_drag_the_scalars_into_python() -> None:
    """The bug this closes: one `list<int64>` beside twenty scalar columns sent
    the whole comparison row by row through the library, and a merge of 50,000
    stored rows spent 6.35 seconds of 7.2 inside it."""
    from rekep.iceberg import dataset as module

    schema = pyarrow.schema(
        [
            ("k", pyarrow.int64()),
            ("parents", pyarrow.list_(pyarrow.int64())),
            ("venue", pyarrow.string()),
        ]
    )
    rows = 500
    source = pyarrow.Table.from_pylist(
        [{"k": index, "parents": [index], "venue": "XPAR"} for index in range(rows)],
        schema=schema,
    )
    stored = pyarrow.Table.from_pylist(
        [{"k": index, "parents": [index], "venue": "XETR"} for index in range(rows)],
        schema=schema,
    )
    fell_back: list[str] = []
    original = module._column_differs_row_by_row

    def counted(one: object, other: object) -> object:
        fell_back.append("once")
        return original(one, other)

    module._column_differs_row_by_row = counted
    try:
        changed = module._changed(source, stored, ["k"])
    finally:
        module._column_differs_row_by_row = original
    assert changed.num_rows == rows, "every venue differs"
    assert len(fell_back) == 1, "the list, and not the string beside it"


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
        dataset.append_arrow(quotes(0, 300, days=3), commit_row_size=1_000_000)
        # A key the chunk below never touches, stored twice.
        stray = quotes(900, 1)
        dataset.append_arrow(stray, commit_row_size=1_000_000)
        dataset.append_arrow(stray, commit_row_size=1_000_000)
    chunk = quotes(0, 300, "XETR", days=3)
    assert len(chunk) > 200, "past MERGE_IN_LIMIT, so the filter is a range"
    merge(pair, chunk)
    assert sorted_rows(ours.read_arrow_table()) == sorted_rows(theirs.read_arrow_table())


@pytest.mark.parametrize("keys", [1, MERGE_IN_LIMIT + 20])
def test_a_stored_duplicate_is_refused_either_side_of_the_limit(pair, keys: int) -> None:
    """The duplicate check runs on what the scan returned, which the limit decides.

    Below the limit the filter names the keys; above it, it is a range that
    brings back rows the chunk never mentions. Both have to reach the same
    verdict about a key the chunk *does* match.

    And refusing at all is deliberately stricter than the library, on exactly
    the shape it misses: the copies here sit in two different files, and
    pyiceberg checks the stored rows for duplicates one record batch at a time,
    so they slip past it and its upsert writes a third. A table whose
    identifier fields do not identify a row cannot be merged into.
    """
    ours, _ = pair
    doubled = quotes(0, keys)
    ours.append_arrow(doubled, commit_row_size=1_000_000)
    ours.append_arrow(doubled, commit_row_size=1_000_000)  # every key now stored twice
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        ours.overwrite_arrow(quotes(0, keys, "XETR"), merge_by=True, commit_row_size=1_000_000)


def test_an_update_past_the_in_limit_still_prunes(tmp_path: Path) -> None:
    """The delete side of a merge names its rows exactly, and past 200 of them
    Iceberg stops using that list to prune -- unless the ranges are ANDed on.

    Single-column keys only: pyiceberg's ceiling is on `In` literals, and a
    composite key becomes an `Or` of equalities, which keeps pruning (slowly).
    """
    from pyiceberg.expressions import And
    from pyiceberg.table.upsert_util import create_match_filter

    from rekep.iceberg.dataset import _key_ranges

    catalog = IcebergCatalog(catalog_name="wide", properties=catalog_properties(tmp_path, "wide"))
    dataset = catalog.dataset("trading.quotes", field=Quote.into_field())
    # One commit per key range, so the files carry disjoint bounds -- which is
    # what makes a range predicate able to skip any of them at all.
    for start in range(0, 1_000, 200):
        dataset.append_arrow(quotes(start, 200), commit_row_size=1_000_000)

    updates = quotes(0, 250, "XETR")  # 250 keys: past the 200-literal ceiling
    exact = create_match_filter(updates, ["seq"])
    assert dataset.scan_plan(exact)["skipped"] == 0, "the exact filter alone cannot prune"
    narrowed = And(exact, _key_ranges(updates, ["seq"]))
    assert dataset.scan_plan(narrowed)["skipped"] > 0, "the ranges are what prune it"


def _terms(expression: object) -> int:
    """How many leaf predicates an expression is made of.

    The number that decides what a merge's commit costs: pyiceberg binds the
    whole tree once per manifest it plans.
    """
    left, right = getattr(expression, "left", None), getattr(expression, "right", None)
    if left is None and right is None:
        return 1
    return _terms(left) + _terms(right)


def _matched_by(expression: object, haystack: pyarrow.Table, schema: object) -> list[dict]:
    """The rows of `haystack` an Iceberg expression selects, through Arrow.

    Which is where the filter ends up: pyiceberg binds it and hands it to
    `pyarrow.compute` to decide what a partially-matched file keeps.
    """
    from pyiceberg.expressions.visitors import bind, rewrite_not
    from pyiceberg.io.pyarrow import expression_to_pyarrow

    bound = bind(schema, rewrite_not(expression), case_sensitive=True)
    return sorted_rows(haystack.filter(expression_to_pyarrow(bound)))


@pytest.mark.parametrize(
    ("count", "days"),
    [(60, 1), (250, 1), (60, 7), (250, 12), (1, 1)],
)
def test_the_factored_delete_filter_matches_what_pyiceberg_matches(
    tmp_path: Path, count: int, days: int
) -> None:
    """The filter that decides what a merge *deletes* stays exact, and exact
    here means the library's own answer, row for row.

    Factoring the repeated half of a composite key out is what makes the commit
    affordable -- pyiceberg binds that tree once per manifest it plans -- but
    a filter that matched one row more or less than `create_match_filter`
    would delete or keep a row nobody asked about.
    """
    from pyiceberg.table.upsert_util import create_match_filter

    from rekep.iceberg.dataset import _match_filter

    catalog = IcebergCatalog(
        catalog_name="factored", properties=catalog_properties(tmp_path, "factored")
    )
    dataset = catalog.dataset("trading.quotes", field=Quote.into_field())
    schema = dataset.into_struct_field().into_iceberg_schema()
    join = ["symbol", "seq"]

    updates = quotes(0, count, "XETR", days=days)
    symbols = updates.column("symbol").to_pylist()
    days_ = updates.column("day").to_pylist()
    seqs = updates.column("seq").to_pylist()
    # The haystack is what separates an exact filter from a superset: for every
    # pair to match, a row sharing its `symbol` and not its `seq`, and one
    # sharing its `seq` and not its `symbol`. A filter that dropped either half
    # of the key still matches every row it should and now matches these too.
    decoys = pyarrow.Table.from_pydict(
        {
            "symbol": [f"Z{name}" for name in symbols] + symbols,
            "day": days_ + days_,
            "seq": seqs + [seq + 10**6 for seq in seqs],
            "size": [0] * (2 * len(seqs)),
            "venue": ["XPAR"] * (2 * len(seqs)),
        },
        schema=Quote.into_field().into_arrow_schema(),
    )
    haystack = pyarrow.concat_tables([updates, decoys])
    ours = _match_filter(updates, join)
    theirs = create_match_filter(updates, join)
    assert _matched_by(theirs, haystack, schema) == sorted_rows(updates), (
        "the reference matches the update set and none of the decoys"
    )
    assert _matched_by(ours, haystack, schema) == _matched_by(theirs, haystack, schema)
    if len(set(symbols)) < count:
        assert _terms(ours) < _terms(theirs), (
            "and it is grouped on the column that repeats, which is the whole point"
        )


def test_a_factored_filter_falls_back_where_a_zero_could_hide(tmp_path: Path) -> None:
    """`pc.is_in` hashes `-0.0` apart from the `0.0` it equals, so a key column
    that is a float holding a zero keeps pyiceberg's per-row equalities --
    which compare numerically. The same trap the single-column form is widened
    against, on the half a grouping would turn into an `In`."""

    @scalar
    class Reading(Convertible):
        """A reading under a float key."""

        sensor: Annotated[str, Field.primary_key()]
        """Which sensor."""

        offset: Annotated[float, Field.primary_key()]
        """Offset, which may be a signed zero."""

        value: int
        """The reading."""

    from pyiceberg.table.upsert_util import create_match_filter

    from rekep.iceberg.dataset import _match_filter

    updates = pyarrow.Table.from_pydict(
        {"sensor": ["A", "A", "B"], "offset": [0.0, 1.5, 0.0], "value": [1, 2, 3]},
        schema=Reading.into_field().into_arrow_schema(),
    )
    join = ["sensor", "offset"]
    assert str(_match_filter(updates, join)) == str(create_match_filter(updates, join))

    without = pyarrow.Table.from_pydict(
        {"sensor": ["A", "A", "B"], "offset": [0.5, 1.5, 0.5], "value": [1, 2, 3]},
        schema=Reading.into_field().into_arrow_schema(),
    )
    assert str(_match_filter(without, join)) != str(create_match_filter(without, join)), (
        "with no zero in it there is nothing to be careful of, and it groups"
    )


def test_a_key_that_repeats_nothing_is_never_grouped(monkeypatch: pytest.MonkeyPatch) -> None:
    """One group per row is the tree `create_match_filter` already builds, so
    there is nothing to factor -- and grouping anyway would call it once per
    row to rebuild it a term at a time."""
    from pyiceberg.table import upsert_util

    from rekep.iceberg.dataset import _match_filter

    calls: list[int] = []
    original = upsert_util.create_match_filter
    monkeypatch.setattr(
        upsert_util,
        "create_match_filter",
        lambda df, cols: (calls.append(df.num_rows), original(df, cols))[1],
    )
    updates = quotes(0, 40, days=1)
    unique = (
        updates.drop_columns(["symbol"])
        .append_column("symbol", pyarrow.array([f"U{i}" for i in range(40)]))
        .select(updates.column_names)
    )
    join = ["symbol", "seq"]
    filter_ = _match_filter(unique, join)
    assert calls == [40], "one call, over the whole of it"
    assert str(filter_) == str(original(unique, join))


@pytest.mark.parametrize(
    ("duplicates", "days", "grouped"),
    [(2, 5, True), (40, 5, True), (400, 5, True), (400, 200, False)],
    ids=["two-books", "forty-books", "a-book-each", "groups-of-four"],
)
def test_a_three_column_key_matches_what_pyiceberg_matches(
    duplicates: int, days: int, grouped: bool
) -> None:
    """A key of three columns takes each group back through the library to
    spell what is left of it, which is the per-group cost the grouping is only
    worth past `MERGE_GROUP_GAIN` rows a group. Both sides of that threshold
    have to match the reference exactly -- and the last case is what crosses
    it: 800 rows over 200 days groups four at a time, so the whole filter goes
    back to `create_match_filter` and matches the reference by *being* it.

    The grouping is on whichever column repeats most, which is `day` here
    except where there are fewer books than days -- so the parameters move both.
    """
    from pyiceberg.schema import Schema
    from pyiceberg.table.upsert_util import create_match_filter
    from pyiceberg.types import LongType, NestedField, StringType

    from rekep.iceberg.dataset import _match_filter

    schema = Schema(
        NestedField(1, "book", StringType(), required=True),
        NestedField(2, "day", LongType(), required=True),
        NestedField(3, "seq", LongType(), required=True),
    )
    rows = 800
    updates = pyarrow.table(
        {
            "book": [f"B{index % duplicates}" for index in range(rows)],
            "day": [index % days for index in range(rows)],
            "seq": list(range(rows)),
        }
    )
    decoys = pyarrow.table(
        {
            "book": [f"B{index % duplicates}" for index in range(rows)],
            "day": [index % days for index in range(rows)],
            "seq": [index + 10**6 for index in range(rows)],
        }
    )
    haystack = pyarrow.concat_tables([updates, decoys])
    join = ["book", "day", "seq"]
    ours = _match_filter(updates, join)
    theirs = create_match_filter(updates, join)
    reference = _matched_by(theirs, haystack, schema)
    assert reference == sorted_rows(updates), "the reference matches the update set alone"
    if grouped:
        assert _matched_by(ours, haystack, schema) == reference
        assert _terms(ours) < _terms(theirs), "grouped, which is the smaller tree"
    else:
        # Past the gain there is nothing to evaluate: the filter *is* the one
        # the reference was just checked against, term for term.
        assert str(ours) == str(theirs)


def test_a_merge_of_many_updates_agrees_with_the_library(tmp_path: Path) -> None:
    """The rows, through both paths, on the shape the factoring is for: a
    composite key one half of which repeats. Measured on 8,000 stored rows,
    5,000 of them updated: 33.4 s through the per-row filter and 0.46 s
    through the factored one."""
    ours = IcebergCatalog(
        catalog_name="mine", properties=catalog_properties(tmp_path, "mine")
    ).dataset("trading.quotes", field=Quote.into_field())
    theirs = IcebergCatalog(
        catalog_name="lib", properties=catalog_properties(tmp_path, "lib")
    ).dataset("trading.quotes", field=Quote.into_field())
    stored = quotes(0, 600, days=6)
    for target in (ours, theirs):
        target.append_arrow(stored, commit_row_size=200)
    updates = quotes(0, 300, "XETR", days=6)

    assert ours.merge_arrow_table(updates, ["symbol", "seq"]) == (300, 0)
    theirs.get_or_create_table().upsert(updates, join_cols=["symbol", "seq"])
    assert sorted_rows(ours.refresh().read_arrow_table()) == sorted_rows(
        theirs.refresh().read_arrow_table()
    )


# -- what Arrow and Iceberg disagree about ----------------------------------


@scalar
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
        schema=Nested.into_field().into_arrow_schema(),
    )


def nested_pair(tmp_path: Path) -> tuple[IcebergDataset, IcebergDataset]:
    built = []
    for name in ("nested-ours", "nested-theirs"):
        catalog = IcebergCatalog(catalog_name=name, properties=catalog_properties(tmp_path, name))
        dataset = catalog.dataset("trading.nested", field=Nested.into_field())
        dataset.create_with()
        built.append(dataset)
    built[1].plan_merges = False
    return built[0], built[1]


def test_a_nested_column_does_not_stop_a_merge(tmp_path: Path) -> None:
    """Arrow refuses a map as join payload, so no join may carry one."""
    ours, theirs = nested_pair(tmp_path)
    for dataset in (ours, theirs):
        dataset.append_arrow(nested_rows(range(4), 1), commit_row_size=1_000_000)
        dataset.overwrite_arrow(
            nested_rows(range(2, 6), 9), merge_by=True, commit_row_size=1_000_000
        )
    assert ours.read_arrow_table().num_rows == 6
    assert sorted(ours.read_arrow_table().column("size").to_pylist()) == sorted(
        theirs.read_arrow_table().column("size").to_pylist()
    )


@pytest.mark.parametrize("keys", [1, 2, MERGE_IN_LIMIT, MERGE_IN_LIMIT + 20])
@pytest.mark.parametrize("stored_sign", [1.0, -1.0])
def test_a_signed_zero_key_matches_the_zero_it_equals(
    tmp_path: Path, keys: int, stored_sign: float
) -> None:
    """`-0.0 == 0.0` in Python and in Iceberg; they hash apart in Arrow.

    Across the `In` limit, because the scan filter is a different thing either
    side of it -- values named one by one below, a range above -- and a guard
    is only as wide as the branch it is on. That is exactly how the NaN key
    got through: refused under the limit, silently duplicating above it.

    And at **two** keys as well as one, because that is where the other
    boundary is: an `In` of one literal collapses to `EqualTo`, which compares
    numerically and matches `-0.0`, while an `In` of two or more reaches Arrow
    as `pc.is_in`, which hashes them apart. Both directions too -- the zero may
    already be stored with the other sign, written by an older version of this
    code or by another engine, and nothing can normalise that afterwards.
    """

    @scalar
    class Level(Convertible):
        """A price level."""

        price: float
        """The key, deliberately a float."""

        size: int
        """Quantity."""

    schema = Level.into_field().into_arrow_schema()
    filler = [float(index + 1) for index in range(keys - 1)]
    stored = pyarrow.Table.from_pydict(
        {"price": [0.0 * stored_sign, *filler], "size": [1] * keys}, schema=schema
    )
    incoming = pyarrow.Table.from_pydict(
        {"price": [0.0 * -stored_sign, *filler], "size": [2] * keys}, schema=schema
    )
    catalog = IcebergCatalog(catalog_name="zero", properties=catalog_properties(tmp_path, "zero"))
    dataset = catalog.dataset("trading.levels", field=Level.into_field())
    dataset.append_arrow(stored, commit_row_size=1_000_000)
    dataset.overwrite_arrow(incoming, merge_by=["price"], commit_row_size=1_000_000)
    rows = dataset.refresh().read_arrow_table()
    assert rows.num_rows == keys, "one row per price, not two for the zero"
    assert set(rows.column("size").to_pylist()) == {2}, "and every one of them updated"


def test_signed_zero_source_keys_are_duplicates(tmp_path: Path) -> None:
    """Normalisation happens before duplicate validation because the two zeros compare equal."""

    @scalar
    class Level(Convertible):
        """A price level."""

        price: float
        """The key, deliberately a float."""

        size: int
        """Quantity."""

    rows = pyarrow.Table.from_pydict(
        {"price": [0.0, -0.0], "size": [1, 2]}, schema=Level.into_field().into_arrow_schema()
    )
    catalog = IcebergCatalog(
        catalog_name="zero-source", properties=catalog_properties(tmp_path, "zero-source")
    )
    dataset = catalog.dataset("trading.levels", field=Level.into_field())

    with pytest.raises(ValueError, match="Duplicate rows found in source dataset"):
        dataset.overwrite_arrow(rows, merge_by=["price"], commit_row_size=1_000_000)
    assert dataset.read_arrow_table().num_rows == 0


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
        schema=Quote.into_field().into_arrow_schema(),
    )
    with pytest.raises(ValueError, match="cannot be null"):
        stored.merge_arrow_table(rows, True)


def test_an_empty_chunk_reads_nothing(stored) -> None:
    empty = Quote.into_field().into_arrow_schema().empty_table()
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


@scalar
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
        schema=Event.into_field().into_arrow_schema(),
    )


@pytest.fixture
def event_pair(tmp_path: Path) -> tuple[IcebergDataset, IcebergDataset]:
    built = []
    for name in ("ours", "theirs"):
        catalog = IcebergCatalog(catalog_name=name, properties=catalog_properties(tmp_path, name))
        built.append(catalog.dataset("trading.events", field=Event.into_field()).create_with())
    return built[0], built[1]


def test_a_merge_through_a_partition_transform_agrees(event_pair) -> None:
    """`_key_ranges` names the raw column; Iceberg prunes on `day(at)`.

    A projection that went the wrong way would plan no file, match nothing and
    insert a second copy of every row -- so this compares row for row.
    """
    ours, theirs = event_pair
    for dataset in event_pair:
        dataset.append_arrow(events(range(60), 0), commit_row_size=1_000_000)
        dataset.overwrite_arrow(events(range(30, 90), 1), merge_by=True, commit_row_size=1_000_000)
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
        ours.append_arrow(events(range(start, start + 30), 0), commit_row_size=1_000_000)
    plan = ours.scan_plan("at >= '2026-01-02T00:00:00' and at < '2026-01-03T00:00:00'")
    assert plan["skipped"] > 0, "a day is one partition, not the whole table"
    assert plan["files"] < plan["total_files"]


@pytest.mark.parametrize("keys", [1, MERGE_IN_LIMIT + 20])
def test_a_nan_merge_key_is_refused_by_both(tmp_path: Path, keys: int) -> None:
    """No literal can name a NaN, so no filter either library builds can find one.

    Parameterised across the `In` limit because the two branches of the scan
    filter fail *differently*: pyiceberg refuses to build a NaN literal, while
    `min_max` silently skips it and returns a range the stored row falls
    outside -- which would insert a second copy, and a third on the next merge,
    without ever raising. A one-row chunk only ever tests the first branch.
    """

    @scalar
    class Level(Convertible):
        """A price level."""

        price: float
        """The key, deliberately a float."""

        size: int
        """Quantity."""

    schema = Level.into_field().into_arrow_schema()
    prices = [float(index) for index in range(keys)] + [float("nan")]
    catalog = IcebergCatalog(catalog_name="nan", properties=catalog_properties(tmp_path, "nan"))
    dataset = catalog.dataset("trading.levels", field=Level.into_field())
    stored = pyarrow.Table.from_pydict({"price": prices, "size": [1] * len(prices)}, schema=schema)
    dataset.append_arrow(stored, commit_row_size=1_000_000)
    chunk = pyarrow.Table.from_pydict({"price": prices, "size": [2] * len(prices)}, schema=schema)
    with pytest.raises(ValueError, match="NaN"):
        dataset.merge_arrow_table(chunk, ["price"])
    with pytest.raises(ValueError, match="NaN"):
        dataset.get_or_create_table().upsert(chunk, join_cols=["price"])
    assert dataset.refresh().read_arrow_table().num_rows == len(prices), "and nothing was written"


def test_the_snapshot_log_the_two_paths_leave_is_the_same(pair) -> None:
    """Same rows is not the whole claim: the same *commits* is.

    Every assertion here compares what a reader of the metadata sees -- the
    operation of each snapshot, the records it says it added and deleted, and
    the properties the job stamped on it -- which rows alone would never catch.
    """
    ours, theirs = pair
    theirs.plan_merges = False
    for dataset in pair:
        dataset.append_arrow(quotes(0, 6), commit_row_size=1_000_000)
        dataset.overwrite_arrow(
            quotes(3, 6, "XETR"),
            merge_by=True,
            commit_row_size=1_000_000,
            properties={"job": "abc"},
        )
    counted = ("added-records", "deleted-records", "job")

    def log(dataset: IcebergDataset) -> list[tuple]:
        return [
            (
                snapshot.summary.operation.value,
                {
                    name: value
                    for name, value in snapshot.summary.additional_properties.items()
                    if name in counted
                },
            )
            for snapshot in dataset.refresh().iceberg_table.snapshots()
        ]

    assert log(ours) == log(theirs)
    assert any("job" in properties for _, properties in log(ours)), "and the job was recorded"


def test_a_chunk_missing_a_column_is_refused(stored) -> None:
    """The schema check allows a missing optional column; a merge cannot.

    Without the refusal the narrow chunk passes the check, becomes the shape
    everything is cast onto, and writes nulls over the column it left out.
    `Table.upsert` refuses this too -- but only where a row actually matches,
    because that is where it casts; on a chunk of new keys it appends happily.
    Refusing either way is the stricter of the two, and the safe one.
    """
    narrow = Quote.into_field().into_arrow_schema().remove(4)  # `venue`, which is optional
    matching = pyarrow.Table.from_pydict(
        {"symbol": ["S1"], "day": [DAY + datetime.timedelta(days=1)], "seq": [1], "size": [99]},
        schema=narrow,
    )
    with pytest.raises(ValueError, match="missing"):
        stored.merge_arrow_table(matching, True)
    with pytest.raises(ValueError, match="not matching"):
        stored.get_or_create_table().upsert(matching, join_cols=["symbol", "day", "seq"])
    assert set(stored.refresh().read_arrow_table().column("venue").to_pylist()) == {"XPAR"}


def test_a_merge_after_a_rename_compares_the_column_that_was_renamed(tmp_path: Path) -> None:
    """A rename is metadata-only, so the branch head still carries the old name.

    A scan pinned to a ref reads under *that snapshot's* schema. Matching the
    columns by name there compares the renamed column against nulls: every row
    read looks changed, and a merge of rows identical to the stored ones
    rewrites the whole table.
    """
    catalog = IcebergCatalog(
        catalog_name="renamed", properties=catalog_properties(tmp_path, "renamed")
    )
    dataset = catalog.dataset("trading.quotes", field=Quote.into_field())
    dataset.append_arrow(quotes(0, 4), commit_row_size=1_000_000)
    with dataset.get_or_create_table().update_schema() as update:
        update.rename_column("venue", "market")
    dataset.refresh()
    dataset.field = dataset.table_field
    same = dataset.read_arrow_table()
    before = len(dataset.iceberg_table.snapshots())
    dataset.overwrite_arrow(same, merge_by=["symbol", "day", "seq"], commit_row_size=1_000_000)
    dataset.refresh()
    assert dataset.read_arrow_table().sort_by("seq").to_pylist() == same.sort_by("seq").to_pylist()
    assert len(dataset.iceberg_table.snapshots()) == before, "nothing changed, so nothing committed"
    with pytest.raises(ValueError, match="not matching"):
        # And the library's own path cannot do this at all: it reads the head
        # under the old schema and then fails to cast the chunk onto it.
        dataset.get_or_create_table().upsert(same, join_cols=["symbol", "day", "seq"])
