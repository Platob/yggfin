"""What the shapes declare, and the properties every one of them has to have."""

from __future__ import annotations

import pyarrow
import pytest

from rekep.fields import Field
from rekep.market import Book, BookSide, Event, Execution, Instrument, MarketEvent, Order
from rekep.market.book import Level, LevelExecution, LevelUpdate

EVENTS = (Order, Execution, BookSide, Book)
SHAPES = (*EVENTS, Instrument, Level, LevelUpdate, LevelExecution)

#: The envelope every event carries, in the order it carries it. Pinned, because
#: a column inserted in the middle moves every one after it -- and a reader that
#: selects by position, or a contract already published, does not move with it.
ENVELOPE = [
    "unix",
    "unix_hour",
    "etype",
    "cunix",
    "runix",
    "eunix",
    "sunix",
    "hash",
    "xhash",
    "version",
    "state",
    "symbol",
    "seq",
    "prev_hash",
    "prev_state",
    "prev_unix",
    "parent_hash",
]

#: What `MarketEvent` adds on top, also in order. `instrument_hash` leads because
#: it is the partition column: an engine that prunes on it reads it first, and a
#: declaration order is a physical order everywhere the schema travels.
PRICED = [
    "instrument_hash",
    "side",
    "px",
    "px_unit",
    "qty",
    "qty_unit",
    "notional",
    "venue",
    "instrument",
    "metadata",
]


@pytest.mark.parametrize("shape", EVENTS, ids=lambda cls: cls.__name__)
def test_every_event_leads_with_the_same_envelope(shape: type) -> None:
    """One reader must be able to read the envelope of any of them, at one offset."""
    assert shape.FIELD.names[: len(ENVELOPE)] == ENVELOPE


@pytest.mark.parametrize("shape", EVENTS, ids=lambda cls: cls.__name__)
def test_every_event_carries_the_priced_slots_next(shape: type) -> None:
    assert shape.FIELD.names[len(ENVELOPE) : len(ENVELOPE) + len(PRICED)] == PRICED


@pytest.mark.parametrize("shape", EVENTS, ids=lambda cls: cls.__name__)
def test_every_event_is_keyed_by_time_and_content(shape: type) -> None:
    """`hash` identifies the version; leading with time is what an engine prunes on."""
    assert shape.FIELD.primary_keys() == ["unix", "hash"]
    assert shape.FIELD.partition_keys() == {
        "unix_hour": "identity",
        "instrument_hash": "bucket[16]",
    }


@pytest.mark.parametrize("shape", EVENTS, ids=lambda cls: cls.__name__)
def test_every_event_is_laid_out_in_time_inside_its_partition(shape: type) -> None:
    """Sorted by `unix` is what makes a time range a few files rather than all of them."""
    assert shape.FIELD.sort_keys() == {"unix": "asc"}


@pytest.mark.parametrize("shape", EVENTS, ids=lambda cls: cls.__name__)
def test_a_market_event_lands_in_the_bucket_of_its_instrument(shape: type) -> None:
    """One instrument's stream has to be one partition, or a book cannot be built."""
    partition = shape.FIELD.field("instrument_hash")
    assert partition.partition_transform == "bucket[16]"
    assert not partition.nullable, "a bucket transform on a null reads every bucket"


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_no_primary_key_is_nullable(shape: type) -> None:
    """Iceberg refuses an optional identifier field, and so does the declaration."""
    for member in shape.FIELD.fields:
        if member.is_primary_key:
            assert not member.nullable, member.name


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_every_column_says_what_it_is(shape: type) -> None:
    """A doc is the column comment everywhere it travels; a blank one ships blind."""
    missing = [member.name for member in shape.FIELD.fields if not member.description]
    assert not missing, f"{shape.__name__} has undocumented columns: {missing}"


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_every_nested_column_says_what_it_is_too(shape: type) -> None:
    for member in shape.FIELD.fields:
        for inner in member.fields:
            if inner.name in ("item", "key", "value"):
                continue
            assert inner.description, f"{shape.__name__}.{member.name}.{inner.name}"


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_a_schema_still_names_the_class_it_came_from(shape: type) -> None:
    schema = shape.FIELD.into_arrow_schema()
    assert Field.from_arrow_schema(schema).name == shape.__name__
    assert schema.metadata[b"namespace"].decode() == shape.__module__


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_a_declaration_survives_being_written_down_and_read_back(shape: type) -> None:
    """The one property a contract has to have: what it says is what it loads as."""
    dumped = shape.FIELD.into_dict()
    assert Field.from_dict(dumped).into_arrow_field() == shape.FIELD.into_arrow_field()


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_a_declaration_rebuilds_as_a_class(shape: type) -> None:
    rebuilt = Field.from_arrow_schema(shape.FIELD.into_arrow_schema()).into_dataclass()
    assert rebuilt.__name__ == shape.__name__


def test_a_market_event_is_an_event_and_an_order_is_both() -> None:
    """The hierarchy is real inheritance, so `isinstance` and the schema agree."""
    assert issubclass(Order, MarketEvent) and issubclass(MarketEvent, Event)
    assert issubclass(BookSide, MarketEvent) and issubclass(Book, MarketEvent)
    assert set(ENVELOPE) <= set(Event.FIELD.names)


def test_a_subclass_builds_its_own_projection_rather_than_its_bases() -> None:
    """The descriptor is per class; sharing one would give `Order` a `Book`'s columns."""
    assert Order.FIELD is not MarketEvent.FIELD
    assert "tif" in Order.FIELD.names and "tif" not in Execution.FIELD.names
    assert "bid_px" in Book.FIELD.names and "bid_px" not in BookSide.FIELD.names


def test_an_order_carries_what_it_asked_for_and_how_far_it_got() -> None:
    assert {"kind", "tif", "stop_px", "display_qty"} <= set(Order.FIELD.names)
    assert {"filled_qty", "leaves_qty", "avg_px"} <= set(Order.FIELD.names)
    assert "prev_client_order_id" in Order.FIELD.names


def test_an_execution_links_to_its_order_with_a_flat_typed_column() -> None:
    """A list cannot be a join key without an explode, so the one parent is flat."""
    link = Execution.FIELD.field("order_xhash")
    assert link.arrow_type == pyarrow.int64() and link.nullable
    assert Execution.FIELD.field("parent_hash").arrow_type.equals(
        pyarrow.list_(pyarrow.field("item", pyarrow.int64(), nullable=False))
    )


def test_a_book_side_carries_the_state_the_delta_and_the_trace() -> None:
    assert BookSide.FIELD.field("alive").item.arrow_type == Level.FIELD.arrow_type
    assert BookSide.FIELD.field("updates").item.arrow_type == LevelUpdate.FIELD.arrow_type
    assert BookSide.FIELD.field("executions").item.arrow_type == LevelExecution.FIELD.arrow_type
    for name in ("alive", "updates", "executions"):
        assert BookSide.FIELD.field(name).nullable, name


def test_a_book_keeps_the_sides_flat_and_still_says_which_versions_it_used() -> None:
    """Unnested for the bounds budget; `bid_hash`/`ask_hash` keep the provenance."""
    names = set(Book.FIELD.names)
    assert "bid" not in names and "ask" not in names, "the sides are columns, not structs"
    for side in ("bid", "ask"):
        assert {f"{side}_hash", f"{side}_px", f"{side}_qty", f"{side}_depth"} <= names
        assert {f"{side}_alive", f"{side}_updates", f"{side}_executions"} <= names
        assert Book.FIELD.field(f"{side}_hash").arrow_type == pyarrow.int64()
    assert Book.FIELD.field("bid_alive").item.arrow_type == Level.FIELD.arrow_type


def test_a_price_and_a_quantity_may_be_absent_because_zero_is_a_price() -> None:
    for shape in EVENTS:
        assert shape.FIELD.field("px").nullable, shape.__name__
        assert shape.FIELD.field("qty").nullable, shape.__name__


def test_a_level_is_not_an_event_because_it_has_no_life_of_its_own() -> None:
    assert "hash" not in Level.FIELD.names
    assert Level.FIELD.field("px").arrow_type == pyarrow.float64()
    assert not Level.FIELD.field("px").nullable, "a live level has a price"
    assert LevelUpdate.FIELD.field("px").nullable, "a deletion does not"


def test_the_metadata_map_keeps_what_the_venue_sent_in_order() -> None:
    """A struct would need the keys known in advance, and a venue does not agree."""
    carried = MarketEvent.FIELD.field("metadata")
    assert pyarrow.types.is_map(carried.arrow_type)
    assert carried.key.arrow_type == pyarrow.string()


#: Iceberg collects column bounds for this many **leaf** columns, in pre-order:
#: `write.metadata.metrics.max-inferred-column-defaults`, whose default is 100.
#: Past it a column is written with no lower or upper bound, so a filter on it
#: reads every file and still returns the right answer -- which is why nothing
#: notices. `Book` is 140 leaves, so this is not a theoretical margin.
METRICS_BUDGET = 100

#: The columns a reader actually filters on, per shape. Each has to be inside
#: the budget, which is a statement about **declaration order**: these are
#: exactly the columns that a nested member declared before them would push out.
FILTERED = {
    Order: ("unix", "unix_hour", "etype", "state", "instrument_hash", "side", "px", "symbol"),
    Execution: ("unix", "unix_hour", "etype", "state", "instrument_hash", "kind", "px", "symbol"),
    BookSide: (
        "unix",
        "unix_hour",
        "etype",
        "instrument_hash",
        "side",
        "px",
        "qty",
        "symbol",
        "depth",
        "total_qty",
    ),
    Book: (
        "unix",
        "unix_hour",
        "etype",
        "instrument_hash",
        "px",
        "spread",
        "micro_px",
        "imbalance",
        "symbol",
        "bid_px",
        "ask_px",
        "bid_total_qty",
        "ask_total_qty",
    ),
    Instrument: ("xhash", "symbol", "kind"),
}


def leaves(arrow_type: pyarrow.DataType, prefix: str = "") -> list[str]:
    """Every leaf column of `arrow_type`, in the pre-order Iceberg counts in."""
    kinds = pyarrow.types
    if kinds.is_struct(arrow_type):
        found: list[str] = []
        for index in range(arrow_type.num_fields):
            member = arrow_type.field(index)
            found += leaves(member.type, f"{prefix}{member.name}.")
        return found
    if kinds.is_list(arrow_type) or kinds.is_large_list(arrow_type):
        return leaves(arrow_type.field(0).type, f"{prefix}item.") or [f"{prefix}item"]
    if kinds.is_map(arrow_type):
        return [f"{prefix}key", f"{prefix}value"]
    return [prefix.rstrip(".")]


def test_the_leaf_walk_finds_the_nesting_it_is_supposed_to() -> None:
    """Otherwise the budget test below passes by counting too few columns."""
    counted = leaves(Book.FIELD.arrow_type)
    assert len(counted) > 60, "a book is wide, or this walk is wrong"
    assert "bid_alive.item.px" in counted, "the walk reached inside a list of structs"
    assert "instrument.symbol" in counted and "metadata.key" in counted


@pytest.mark.parametrize(
    "shape,names", FILTERED.items(), ids=lambda value: getattr(value, "__name__", "")
)
def test_every_column_a_reader_filters_on_is_inside_the_metrics_budget(
    shape: type, names: tuple[str, ...]
) -> None:
    """A filter column past the cutoff prunes nothing and looks like it works."""
    counted = leaves(shape.FIELD.arrow_type)
    for name in names:
        assert name in counted, f"{shape.__name__} has no leaf {name}"
        assert counted.index(name) + 1 <= METRICS_BUDGET, (
            f"{shape.__name__}.{name} is leaf {counted.index(name) + 1}: "
            "declare it before the nested members that pushed it out"
        )
