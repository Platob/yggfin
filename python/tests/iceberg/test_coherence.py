"""Where we replace rows ourselves, the rows must be the ones pyiceberg's upsert leaves.

Every write test here runs the same scenario twice -- once through this
package's replace verb, once through the official library's `Table.upsert` on
an identical table -- and compares the rows that come out. A faster path that
returns something else is not an optimisation.
"""

import datetime
from pathlib import Path
from typing import Annotated

import pyarrow
import pyarrow.compute
import pytest

from rekep import Convertible, scalar
from rekep.fields import field_of, partition_key, primary_key
from rekep.iceberg import IcebergCatalog, IcebergDataset, iceberg_schema, partition_keys

from ..conftest import catalog_properties

#: The zone every instant here is spelled in.
UTC = datetime.timezone.utc

pytestmark = pytest.mark.integration


@scalar
class Quote(Convertible):
    """One quote."""

    symbol: Annotated[str, primary_key()]
    """Instrument."""

    day: Annotated[datetime.date, partition_key()]
    """Trading day, and the partition."""

    seq: Annotated[int, primary_key()]
    """Sequence within the day -- the second half of a composite key."""

    size: int
    """Quantity."""

    venue: str | None = None
    """Where it traded, when known."""


DAY = datetime.date(2026, 8, 14)

#: Past what a manifest names one by one: enough keys that a file's bounds,
#: not a list of literals, decide whether it is opened.
MANY = 220


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


@pytest.fixture
def pair(tmp_path: Path) -> tuple[IcebergDataset, IcebergDataset]:
    """Two identical tables: one this package writes, one pyiceberg does."""
    built = []
    for name in ("ours", "theirs"):
        catalog = IcebergCatalog(name=name, properties=catalog_properties(tmp_path, name))
        dataset = catalog.dataset("trading.quotes", field=Quote.into_field())
        dataset.create_with()
        built.append(dataset)
    return built[0], built[1]


def merge(
    pair: tuple[IcebergDataset, IcebergDataset],
    chunk: pyarrow.Table,
    join: list[str] | None = None,
    **kwargs: object,
) -> int:
    """The same rows into both: ours replaced, theirs upserted by the library."""
    ours, theirs = pair
    join = join or ["symbol", "seq"]
    written = ours.overwrite_arrow(chunk, merge_by=join, commit_row_size=1_000_000, **kwargs)
    theirs.get_or_create_table().upsert(chunk, join_cols=join, **kwargs)
    return written


def same_rows(pair: tuple[IcebergDataset, IcebergDataset], **kwargs: object) -> None:
    ours, theirs = pair
    assert sorted_rows(ours.refresh().read_arrow_table(**kwargs)) == sorted_rows(
        theirs.refresh().read_arrow_table(**kwargs)
    )


# -- the replace --------------------------------------------------------------


def test_a_replace_into_an_empty_table_agrees(pair) -> None:
    assert merge(pair, quotes(0, 50)) == 50
    same_rows(pair)


def test_a_replace_of_entirely_new_keys_agrees(pair) -> None:
    """The case the key bounds exist for: nothing can match, so nothing is read."""
    for dataset in pair:
        dataset.append_arrow(quotes(0, 100), commit_row_size=1_000_000)
    merge(pair, quotes(100, 100))
    assert pair[0].read_arrow_table().num_rows == 200
    same_rows(pair)


def test_new_keys_inside_stored_bounds_agree_and_leave_the_stored_file_standing(pair) -> None:
    """A miss inside a file's bounds reads the file and keeps it as it was."""
    rows = quotes(0, MANY * 2)
    stored = rows.take(pyarrow.array(range(0, MANY * 2, 2)))
    incoming = rows.take(pyarrow.array(range(1, MANY * 2, 2)))
    for dataset in pair:
        dataset.append_arrow(stored, commit_row_size=1_000_000)

    merge(pair, incoming)
    same_rows(pair)
    for dataset in pair:
        assert (dataset.data_files().num_rows, len(dataset.iceberg_table.snapshots())) == (2, 2)


def test_a_replace_of_unchanged_rows_agrees(pair) -> None:
    """Re-ingesting the same lines must not duplicate them."""
    for dataset in pair:
        dataset.append_arrow(quotes(0, 60), commit_row_size=1_000_000)
    merge(pair, quotes(0, 60))
    assert pair[0].read_arrow_table().num_rows == 60
    same_rows(pair)


def test_a_replace_that_updates_values_agrees(pair) -> None:
    for dataset in pair:
        dataset.append_arrow(quotes(0, 60, "XPAR"), commit_row_size=1_000_000)
    merge(pair, quotes(0, 60, "XETR"))
    stored = pair[0].read_arrow_table()
    assert set(stored.column("venue").to_pylist()) == {"XETR"}
    same_rows(pair)


def test_a_half_matching_replace_agrees(pair) -> None:
    """The interesting one: some rows replace, some add, in one chunk."""
    for dataset in pair:
        dataset.append_arrow(quotes(0, 80, "XPAR"), commit_row_size=1_000_000)
    merge(pair, quotes(40, 80, "XETR"))
    assert pair[0].read_arrow_table().num_rows == 120
    same_rows(pair)


def test_a_replace_across_partitions_agrees(pair) -> None:
    for dataset in pair:
        dataset.append_arrow(quotes(0, 30, "XPAR", days=3), commit_row_size=1_000_000)
    merge(pair, quotes(15, 30, "XETR", days=3))
    same_rows(pair)


def test_a_replace_past_many_keys_still_finds_every_stored_row(pair) -> None:
    """A file's bounds are a superset, so what they plan must still hold every match."""
    for dataset in pair:
        dataset.append_arrow(quotes(0, MANY + 1), commit_row_size=1_000_000)
    merge(pair, quotes(0, MANY + 1, "XETR"))
    stored = pair[0].read_arrow_table()
    assert stored.num_rows == MANY + 1
    assert set(stored.column("venue").to_pylist()) == {"XETR"}
    same_rows(pair)


def test_a_streamed_replace_agrees_with_a_single_one(pair) -> None:
    """Chunking changes how many commits happen, never what is stored."""
    ours, theirs = pair
    for dataset in pair:
        dataset.append_arrow(quotes(0, 24, "XPAR"), commit_row_size=1_000_000)
    ours.overwrite_arrow_reader(
        quotes(12, 24, "XETR").to_reader(max_chunksize=5),
        merge_by=True,
        commit_row_size=5,
    )
    theirs.get_or_create_table().upsert(quotes(12, 24, "XETR"), join_cols=["symbol", "seq"])
    same_rows(pair)


def test_a_replace_on_named_columns_agrees(pair) -> None:
    for dataset in pair:
        dataset.append_arrow(quotes(0, 40, "XPAR"), commit_row_size=1_000_000)
    merge(pair, quotes(20, 40, "XETR"), ["seq"])
    same_rows(pair)


def test_a_replace_on_a_branch_agrees(pair) -> None:
    for dataset in pair:
        dataset.append_arrow(quotes(0, 40), commit_row_size=1_000_000)
        dataset.create_branch("dev")
    merge(pair, quotes(20, 40, "XETR"), branch="dev")
    same_rows(pair, branch="dev")
    assert pair[0].read_arrow_table().num_rows == 40, "main is untouched, as pyiceberg leaves it"


def test_a_key_the_chunk_carries_twice_lands_once(pair) -> None:
    """The library refuses a duplicated source key; this keeps the first row,
    which is what a stream that carries a line twice means."""
    ours, theirs = pair
    doubled = pyarrow.concat_tables([quotes(0, 5), quotes(0, 5, "XETR")])
    assert ours.overwrite_arrow(doubled, merge_by=True, commit_row_size=1_000_000) == 5
    stored = ours.read_arrow_table()
    assert stored.num_rows == 5
    assert set(stored.column("venue").to_pylist()) == {"XPAR"}
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        theirs.get_or_create_table().upsert(doubled, join_cols=["symbol", "seq"])


def test_the_verb_says_what_it_carried(pair) -> None:
    ours, _ = pair
    ours.append_arrow(quotes(0, 40, "XPAR"), commit_row_size=1_000_000)
    assert ours.overwrite_arrow(quotes(20, 40, "XETR"), merge_by=True) == 40
    assert ours.overwrite_arrow(quotes(20, 40, "XETR"), merge_by=True) == 40, "carried again"
    assert ours.read_arrow_table().num_rows == 60


# -- reading ----------------------------------------------------------------


@pytest.fixture(scope="module")
def stored(tmp_path_factory: pytest.TempPathFactory) -> IcebergDataset:
    root = tmp_path_factory.mktemp("read")
    catalog = IcebergCatalog(name="read", properties=catalog_properties(root, "read"))
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
    narrow = field_of(
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
    narrow = field_of(
        pyarrow.schema([Quote.into_field().into_arrow_schema().field("seq")]), "Narrow"
    )
    scan = stored.iceberg_table.scan()
    assert stored._selected(narrow, scan) == {"seq": "seq"}, "the scan is told, not the cast"
    assert stored.read_arrow_table(narrow).column_names == ["seq"]
    assert stored.read_arrow_table(columns=["size"]).column_names == ["size"], "an explicit list"
    assert stored.read_arrow_table().column_names == [
        member.name for member in Quote.into_field()
    ], "no shape, every column"


def test_explicit_columns_narrow_the_requested_schema(stored) -> None:
    """`columns` intersects the cast shape instead of only narrowing its scan."""
    narrow = field_of(
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
    catalog = IcebergCatalog(name="evolved", properties=catalog_properties(tmp_path, "evolved"))
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
    wider = field_of(
        pyarrow.schema([*Quote.into_field().into_arrow_schema(), ("desk", pyarrow.string())]),
        Quote.into_field().name,
    )
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
    declared = iceberg_schema(Quote.into_field())
    stored_schema = stored.iceberg_table.schema()
    assert [(f.name, str(f.field_type), f.required, f.doc) for f in stored_schema.fields] == [
        (f.name, str(f.field_type), f.required, f.doc) for f in declared.fields
    ]
    assert stored_schema.identifier_field_ids == declared.identifier_field_ids


def test_the_partition_spec_we_declare_is_the_one_it_partitions_by(stored) -> None:
    assert [f.name for f in stored.iceberg_table.spec().fields] == ["day"]
    assert partition_keys(stored.table_field) == {"day": "identity"}


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


def test_the_key_bounds_of_new_keys_plan_nothing(stored) -> None:
    """What turns a replace into an append: no stored file can hold the keys."""
    from rekep.iceberg.dataset import _key_bounds

    fresh = quotes(10_000, 20)
    assert stored.scan_plan(_key_bounds(fresh, ["symbol", "seq"]))["files"] == 0


# -- the file bounds are a superset, and must be treated as one -------------


def test_a_stored_row_outside_the_chunks_keys_is_left_alone(pair) -> None:
    """A file's bounds admit rows the chunk never names; they are not its business."""
    for dataset in pair:
        dataset.append_arrow(quotes(0, MANY + 20, days=3), commit_row_size=1_000_000)
        stray = quotes(900, 1)
        dataset.append_arrow(stray, commit_row_size=1_000_000)
    merge(pair, quotes(0, MANY + 20, "XETR", days=3))
    same_rows(pair)
    assert pair[0].read_arrow_table().num_rows == MANY + 21


@pytest.mark.parametrize("keys", [1, MANY])
def test_a_stored_duplicate_is_replaced_by_one_row(pair, keys: int) -> None:
    """A table whose identifier fields do not identify a row is repaired by a
    replace: both stored copies go, and the row the chunk carries lands once.

    The library upserts a third copy instead, because it checks the stored
    rows for duplicates one record batch at a time and the copies here sit in
    two files -- so this is the one place the two are allowed to differ.
    """
    ours, _ = pair
    doubled = quotes(0, keys)
    ours.append_arrow(doubled, commit_row_size=1_000_000)
    ours.append_arrow(doubled, commit_row_size=1_000_000)  # every key now stored twice
    assert ours.overwrite_arrow(quotes(0, keys, "XETR"), merge_by=True) == keys
    stored = ours.refresh().read_arrow_table()
    assert stored.num_rows == keys
    assert set(stored.column("venue").to_pylist()) == {"XETR"}


def test_a_replace_of_many_updates_agrees_with_the_library(tmp_path: Path) -> None:
    """A repeated composite-key half stays coherent across six partitions."""
    ours = IcebergCatalog(name="mine", properties=catalog_properties(tmp_path, "mine")).dataset(
        "trading.quotes", field=Quote.into_field()
    )
    theirs = IcebergCatalog(name="lib", properties=catalog_properties(tmp_path, "lib")).dataset(
        "trading.quotes", field=Quote.into_field()
    )
    stored = quotes(0, 60, days=6)
    for target in (ours, theirs):
        target.append_arrow(stored, commit_row_size=20)
    updates = quotes(0, 30, "XETR", days=6)

    assert ours.overwrite_arrow(updates, merge_by=["symbol", "seq"]) == 30
    theirs.get_or_create_table().upsert(updates, join_cols=["symbol", "seq"])
    assert sorted_rows(ours.refresh().read_arrow_table()) == sorted_rows(
        theirs.refresh().read_arrow_table()
    )


# -- what Arrow and Iceberg disagree about ----------------------------------


@scalar
class Nested(Convertible):
    """A row with a column Arrow cannot compare."""

    key: Annotated[str, primary_key()]
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


def test_a_nested_column_does_not_stop_a_replace(tmp_path: Path) -> None:
    """Arrow refuses a map as join payload, so the join carries the keys alone."""
    built = []
    for name in ("nested-ours", "nested-theirs"):
        catalog = IcebergCatalog(name=name, properties=catalog_properties(tmp_path, name))
        built.append(catalog.dataset("trading.nested", field=Nested.into_field()).create_with())
    ours, theirs = built
    for dataset in built:
        dataset.append_arrow(nested_rows(range(4), 1), commit_row_size=1_000_000)
    ours.overwrite_arrow(nested_rows(range(2, 6), 9), merge_by=True, commit_row_size=1_000_000)
    theirs.get_or_create_table().upsert(nested_rows(range(2, 6), 9), join_cols=["key"])
    assert ours.read_arrow_table().num_rows == 6
    assert sorted(ours.read_arrow_table().column("size").to_pylist()) == sorted(
        theirs.read_arrow_table().column("size").to_pylist()
    )


@pytest.mark.parametrize("keys", [1, 2, MANY])
@pytest.mark.parametrize("stored_sign", [1.0, -1.0])
def test_a_signed_zero_key_matches_the_zero_it_equals(
    tmp_path: Path, keys: int, stored_sign: float
) -> None:
    """`-0.0 == 0.0` in Python and in Iceberg; they hash apart in Arrow.

    Both directions, because the zero may already be stored with the other
    sign -- written by an older version of this code or by another engine --
    and nothing can normalise that afterwards.
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
    catalog = IcebergCatalog(name="zero", properties=catalog_properties(tmp_path, "zero"))
    dataset = catalog.dataset("trading.levels", field=Level.into_field())
    dataset.append_arrow(stored, commit_row_size=1_000_000)
    dataset.overwrite_arrow(incoming, merge_by=["price"], commit_row_size=1_000_000)
    rows = dataset.refresh().read_arrow_table()
    assert rows.num_rows == keys, "one row per price, not two for the zero"
    assert set(rows.column("size").to_pylist()) == {2}, "and every one of them replaced"


def test_signed_zero_source_keys_are_one_key(tmp_path: Path) -> None:
    """Normalisation happens before the first row is kept, because the two zeros compare equal."""

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
        name="zero-source", properties=catalog_properties(tmp_path, "zero-source")
    )
    dataset = catalog.dataset("trading.levels", field=Level.into_field())

    assert dataset.overwrite_arrow(rows, merge_by=["price"], commit_row_size=1_000_000) == 1
    assert dataset.read_arrow_table().to_pylist() == [{"price": 0.0, "size": 1}]


def test_a_null_merge_key_is_refused(stored) -> None:
    """No join finds the row it would replace, so writing it would duplicate it."""
    rows = quotes(0, 1)
    rows = rows.set_column(
        rows.schema.get_field_index("venue"),
        rows.schema.field("venue"),
        pyarrow.array([None], pyarrow.string()),
    )
    with pytest.raises(ValueError, match="cannot be null"):
        stored.overwrite_arrow(rows, merge_by=["venue"])


def test_an_empty_chunk_commits_nothing(stored) -> None:
    empty = Quote.into_field().into_arrow_schema().empty_table()
    before = len(stored.iceberg_table.snapshots())
    assert stored.overwrite_arrow(empty, merge_by=True) == 0
    assert len(stored.refresh().iceberg_table.snapshots()) == before


def test_a_chunk_the_shape_refuses_is_refused_before_anything_is_staged(stored) -> None:
    """Whatever the strict field apply rejects, the replace rejects too."""
    wrong = pyarrow.Table.from_pydict(
        {"symbol": ["A"], "day": [DAY], "seq": [1], "size": ["not a number"], "venue": ["X"]}
    )
    before = len(stored.iceberg_table.snapshots())
    with pytest.raises(Exception, match="[Ff]ailed to parse|[Mm]ismatch|not compatible|type|cast"):
        stored.overwrite_arrow(wrong, merge_by=True)
    assert len(stored.refresh().iceberg_table.snapshots()) == before


# -- partition transforms ---------------------------------------------------


@scalar
class Event(Convertible):
    """One event, partitioned by a *transform* of its timestamp."""

    at: Annotated[datetime.datetime, primary_key(), partition_key("day")]
    """When it happened, and the partition it lands in -- by day, not by value."""

    size: int
    """Quantity."""


def events(indexes: range, version: int) -> pyarrow.Table:
    """Rows five hours apart, so every partition holds several."""
    start = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
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
        catalog = IcebergCatalog(name=name, properties=catalog_properties(tmp_path, name))
        built.append(catalog.dataset("trading.events", field=Event.into_field()).create_with())
    return built[0], built[1]


def test_a_replace_through_a_partition_transform_agrees(event_pair) -> None:
    """The key bounds name the raw column; Iceberg prunes on `day(at)`.

    A projection that went the wrong way would plan no file, match nothing and
    insert a second copy of every row -- so this compares row for row.
    """
    ours, theirs = event_pair
    for dataset in event_pair:
        dataset.append_arrow(events(range(18), 0), commit_row_size=1_000_000)
    ours.overwrite_arrow(events(range(9, 27), 1), merge_by=True, commit_row_size=1_000_000)
    theirs.get_or_create_table().upsert(events(range(9, 27), 1), join_cols=["at"])
    order = [("at", "ascending")]
    assert ours.read_arrow_table().num_rows == 27, "replaced, not duplicated"
    assert (
        ours.read_arrow_table().sort_by(order).to_pylist()
        == theirs.read_arrow_table().sort_by(order).to_pylist()
    )


def test_a_transformed_partition_prunes_a_read(event_pair) -> None:
    """The point of `day(at)`: a day's filter opens a day's files."""
    ours, _ = event_pair
    for start in range(0, 90, 30):
        ours.append_arrow(events(range(start, start + 30), 0), commit_row_size=1_000_000)
    plan = ours.scan_plan("at >= '2026-01-02T00:00:00+00:00' and at < '2026-01-03T00:00:00+00:00'")
    assert plan["skipped"] > 0, "a day is one partition, not the whole table"
    assert plan["files"] < plan["total_files"]


@pytest.mark.parametrize("keys", [1, MANY])
def test_a_nan_merge_key_is_refused_by_both(tmp_path: Path, keys: int) -> None:
    """No join matches a NaN and no literal names one, so neither library can find it.

    Parameterised across many keys because `min_max` silently skips a NaN and
    answers bounds the stored row falls outside -- which would insert a second
    copy, and a third on the next replace, without ever raising.
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
    catalog = IcebergCatalog(name="nan", properties=catalog_properties(tmp_path, "nan"))
    dataset = catalog.dataset("trading.levels", field=Level.into_field())
    stored = pyarrow.Table.from_pydict({"price": prices, "size": [1] * len(prices)}, schema=schema)
    dataset.append_arrow(stored, commit_row_size=1_000_000)
    chunk = pyarrow.Table.from_pydict({"price": prices, "size": [2] * len(prices)}, schema=schema)
    with pytest.raises(ValueError, match="NaN"):
        dataset.overwrite_arrow(chunk, merge_by=["price"])
    with pytest.raises(ValueError, match="NaN"):
        dataset.get_or_create_table().upsert(chunk, join_cols=["price"])
    assert dataset.refresh().read_arrow_table().num_rows == len(prices), "and nothing was written"


def test_a_replace_is_one_snapshot_carrying_the_job_it_was_given(pair) -> None:
    """Same rows is not the whole claim: what a reader of the metadata sees is
    one commit per chunk, stamped with the properties the job handed over."""
    ours, _ = pair
    ours.append_arrow(quotes(0, 6), commit_row_size=1_000_000)
    before = len(ours.iceberg_table.snapshots())
    ours.overwrite_arrow(
        quotes(3, 6, "XETR"),
        merge_by=True,
        commit_row_size=1_000_000,
        properties={"job": "abc"},
    )
    snapshots = ours.refresh().iceberg_table.snapshots()
    assert len(snapshots) == before + 1, "one chunk, one commit"
    last = snapshots[-1]
    assert last.summary.operation.value == "overwrite"
    assert last.summary.additional_properties["job"] == "abc"
    assert int(last.summary.additional_properties["added-records"]) == 9, "kept 3, carried 6"
    assert int(last.summary.additional_properties["deleted-records"]) == 6, "the file it emptied"


def test_a_replace_after_a_rename_lands_the_same_rows_under_the_new_name(tmp_path: Path) -> None:
    """A rename is metadata-only: the stored file still carries the old name,
    and a replace of rows identical to the stored ones leaves the table saying
    what it said, under the name it has now."""
    catalog = IcebergCatalog(name="renamed", properties=catalog_properties(tmp_path, "renamed"))
    dataset = catalog.dataset("trading.quotes", field=Quote.into_field())
    dataset.append_arrow(quotes(0, 4), commit_row_size=1_000_000)
    with dataset.get_or_create_table().update_schema() as update:
        update.rename_column("venue", "market")
    dataset.refresh()
    dataset.field = dataset.table_field
    same = dataset.read_arrow_table()
    dataset.overwrite_arrow(same, merge_by=["symbol", "day", "seq"], commit_row_size=1_000_000)
    dataset.refresh()
    assert dataset.read_arrow_table().sort_by("seq").to_pylist() == same.sort_by("seq").to_pylist()
    assert dataset.read_arrow_table().column("market").to_pylist() == ["XPAR"] * 4
