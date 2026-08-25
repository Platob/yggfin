"""What the shapes declare, and the properties every one of them has to have."""

from __future__ import annotations

import dataclasses
import json
import weakref

import pyarrow
import pytest

from rekep.fields import Field
from rekep.market import Book, Event, Execution, Instrument, Level, MarketEvent, Order
from rekep.text import FixMessage

EVENTS = (Order, Execution, Book)
SHAPES = (*EVENTS, Instrument, Level)
HOT_ROWS = (Event, MarketEvent, FixMessage, Instrument, Order, Execution, Book, Level)

#: The envelope every event carries, in the order it carries it. Pinned, because
#: a column inserted in the middle moves every one after it -- and a reader that
#: selects by position, or a contract already published, does not move with it.
ENVELOPE = [
    "unix",
    "unix_partition",
    "etype",
    "cunix",
    "runix",
    "eunix",
    "sunix",
    "hash",
    "xhash",
    "linked_events",
    "version",
    "state",
    "code",
    "codes",
    "prev_unix",
    "parent_hash",
    "mic",
    "reason",
]

#: What `MarketEvent` adds on top, also in order. `instrument_xhash` leads because
#: it is the partition column: an engine that prunes on it reads it first, and a
#: declaration order is a physical order everywhere the schema travels.
PRICED = [
    "instrument_xhash",
    "instrument_code",
    "kind",
    "side",
    "px",
    "prev_px",
    "px_unit",
    "ccy",
    "qty",
    "prev_qty",
    "qty_unit",
    "notional",
    "prev_notional",
    "metadata",
]


@pytest.mark.parametrize("shape", EVENTS, ids=lambda cls: cls.__name__)
def test_every_event_leads_with_the_same_envelope(shape: type) -> None:
    """One reader must be able to read the envelope of any of them, at one offset."""
    assert shape.into_field().names[: len(ENVELOPE)] == ENVELOPE


@pytest.mark.parametrize("shape", EVENTS, ids=lambda cls: cls.__name__)
def test_every_event_carries_the_priced_slots_next(shape: type) -> None:
    assert shape.into_field().names[len(ENVELOPE) : len(ENVELOPE) + len(PRICED)] == PRICED


@pytest.mark.parametrize("shape", EVENTS, ids=lambda cls: cls.__name__)
def test_every_event_is_keyed_by_time_and_content(shape: type) -> None:
    """`hash` identifies the version; leading with time is what an engine prunes on."""
    assert shape.into_field().primary_keys() == ["unix", "hash"]
    assert shape.into_field().partition_keys() == {"unix_partition": "identity"}


@pytest.mark.parametrize("shape", EVENTS, ids=lambda cls: cls.__name__)
def test_every_event_is_laid_out_in_time_inside_its_partition(shape: type) -> None:
    """Sorted by `unix` is what makes a time range a few files rather than all of them."""
    assert shape.into_field().sort_keys() == {"unix": "asc"}


@pytest.mark.parametrize("shape", EVENTS, ids=lambda cls: cls.__name__)
def test_the_hour_is_the_only_partition_a_market_event_declares(shape: type) -> None:
    """A hash bucket splits every hour into as many files and prunes nothing extra."""
    assert list(shape.into_field().partition_keys()) == ["unix_partition"]
    assert not shape.into_field().field("instrument_xhash").nullable


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_no_primary_key_is_nullable(shape: type) -> None:
    """Iceberg refuses an optional identifier field, and so does the declaration."""
    for member in shape.into_field().fields:
        if member.is_primary_key:
            assert not member.nullable, member.name


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_every_column_says_what_it_is(shape: type) -> None:
    """A doc is the column comment everywhere it travels; a blank one ships blind."""
    missing = [member.name for member in shape.into_field().fields if not member.description]
    assert not missing, f"{shape.__name__} has undocumented columns: {missing}"


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_every_nested_column_says_what_it_is_too(shape: type) -> None:
    for member in shape.into_field().fields:
        for inner in member.fields:
            if inner.name in ("item", "key", "value"):
                continue
            assert inner.description, f"{shape.__name__}.{member.name}.{inner.name}"


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_a_schema_still_names_the_class_it_came_from(shape: type) -> None:
    schema = shape.into_field().into_arrow_schema()
    assert Field.from_arrow_schema(schema).name == shape.__name__
    assert schema.metadata[b"namespace"].decode() == shape.__module__


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_a_declaration_survives_being_written_down_and_read_back(shape: type) -> None:
    """The one property a contract has to have: what it says is what it loads as."""
    dumped = shape.into_field().into_dict()
    assert Field.from_dict(dumped).into_arrow_field() == shape.into_field().into_arrow_field()


@pytest.mark.parametrize("shape", SHAPES, ids=lambda cls: cls.__name__)
def test_a_declaration_rebuilds_as_a_class(shape: type) -> None:
    rebuilt = Field.from_arrow_schema(shape.into_field().into_arrow_schema()).into_dataclass()
    assert rebuilt.__name__ == shape.__name__


def test_a_market_event_is_an_event_and_an_order_is_both() -> None:
    """The hierarchy is real inheritance, so `isinstance` and the schema agree."""
    assert issubclass(Order, MarketEvent) and issubclass(MarketEvent, Event)
    assert issubclass(Book, MarketEvent)
    assert set(ENVELOPE) <= set(Event.into_field().names)


def test_a_subclass_builds_its_own_projection_rather_than_its_bases() -> None:
    """The descriptor is per class; sharing one would give `Order` a `Book`'s columns."""
    assert Order.into_field() is not MarketEvent.into_field()
    assert "tif" in Order.into_field().names and "tif" not in Execution.into_field().names
    assert "bid_px" in Book.into_field().names and "bid_px" not in Execution.into_field().names


@pytest.mark.parametrize("shape", HOT_ROWS, ids=lambda cls: cls.__name__)
def test_hot_persisted_rows_are_slotted_and_round_trip_through_arrow(shape: type) -> None:
    row = shape()
    assert not hasattr(row, "__dict__")
    assert weakref.ref(row)() is row
    schema = shape.into_field().into_arrow_schema()
    stored = pyarrow.Table.from_pylist([row.into_dict()], schema=schema).to_pylist()[0]
    assert shape.from_dict(stored) == row


def test_transient_instrument_uses_a_slot_but_not_a_market_column() -> None:
    instrument = Instrument(symbol="AAPL", multiplier=2.0)
    order = Order(px=3.0, qty=4.0).attach_instrument(instrument)
    assert order.into_instrument() is instrument
    assert order.into_notional() == 24.0
    assert "_MarketEvent__instrument" not in order.into_dict()
    assert "_MarketEvent__instrument" not in dataclasses.asdict(order)
    assert "_MarketEvent__instrument" not in {field.name for field in dataclasses.fields(Order)}
    assert "_MarketEvent__instrument" not in Order.into_field().names


def test_an_order_carries_what_it_asked_for_and_how_far_it_got() -> None:
    assert {"kind", "tif", "stop_px", "hidden_qty", "vwap", "indicative"} <= set(
        Order.into_field().names
    )
    assert not {"display_qty", "filled_qty", "leaves_qty"} & set(Order.into_field().names)
    assert "prev_client_order_id" in Order.into_field().names


def test_currency_is_typed_but_price_convention_stays_explicit() -> None:
    ccy = MarketEvent.into_field().field("ccy")
    assert ccy.nullable and ccy.arrow_type == pyarrow.int32()
    assert ccy.fix["name"] == "Currency" and ccy.fix["tag"] == "15"
    assert ccy.fix["type"] == "Currency", "the newest reading, and 4.0's char is collapsed"
    assert json.loads(ccy.fix["versions"])[0] == "5.0.SP2"
    assert MarketEvent.into_field().field("px_unit").arrow_type == pyarrow.string()


def test_every_event_uses_one_typed_list_for_lifecycle_links() -> None:
    link = Execution.into_field().field("linked_events")
    assert link.arrow_type.equals(
        pyarrow.list_(
            pyarrow.field(
                "item",
                pyarrow.struct(
                    [
                        pyarrow.field("unix", pyarrow.int64(), nullable=False),
                        pyarrow.field("xhash", pyarrow.int64(), nullable=False),
                    ]
                ),
                nullable=False,
            )
        )
    )
    assert not link.nullable
    assert "order_xhash" not in Execution.into_field().names
    assert "order_xcode" not in Execution.into_field().names
    assert (
        Execution.into_field()
        .field("parent_hash")
        .arrow_type.equals(pyarrow.list_(pyarrow.field("item", pyarrow.int64(), nullable=False)))
    )


def test_an_execution_carries_each_source_order_identifier() -> None:
    assert {"order_id", "client_order_id", "prev_client_order_id"} <= set(
        Execution.into_field().names
    )


def test_a_level_carries_only_price_and_quantity() -> None:
    assert Level.into_field().names == ["px", "qty"]


def test_a_book_keeps_only_compact_best_first_level_lists() -> None:
    names = set(Book.into_field().names)
    assert "bid" not in names and "ask" not in names, "the sides are columns, not structs"
    for side in ("bid", "ask"):
        assert {f"{side}_px", f"{side}_qty", f"{side}_depth", f"{side}_levels"} <= names
        assert Book.into_field().field(f"{side}_levels").item.arrow_type == (
            Level.into_field().arrow_type
        )
    assert not names & {"bid_hash", "ask_hash", "bid_total_qty", "ask_total_qty", "micro_px"}
    assert {"vwap", "exec_px", "prev_exec_px"} <= names
    assert {"deltas", "executions", "bid_alive", "ask_alive"} <= names
    for name in ("bid_levels", "ask_levels", "deltas", "executions", "bid_alive", "ask_alive"):
        assert not Book.into_field().field(name).nullable
    assert not Book.into_field().field("bid_depth").nullable
    assert not Book.into_field().field("ask_depth").nullable


def test_a_price_and_a_quantity_may_be_absent_because_zero_is_a_price() -> None:
    for shape in EVENTS:
        assert shape.into_field().field("px").nullable, shape.__name__
        assert shape.into_field().field("qty").nullable, shape.__name__


def test_a_level_is_not_an_event_because_it_has_no_life_of_its_own() -> None:
    assert "hash" not in Level.into_field().names
    assert Level.into_field().field("px").arrow_type == pyarrow.float64()
    assert not Level.into_field().field("px").nullable
    assert not Level.into_field().field("qty").nullable, "zero quantity marks a deletion"


def test_the_metadata_map_keeps_what_the_venue_sent_in_order() -> None:
    """A struct would need the keys known in advance, and a venue does not agree."""
    carried = MarketEvent.into_field().field("metadata")
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
    Order: (
        "unix",
        "unix_partition",
        "etype",
        "state",
        "code",
        "instrument_xhash",
        "side",
        "px",
    ),
    Execution: (
        "unix",
        "unix_partition",
        "etype",
        "state",
        "code",
        "instrument_xhash",
        "px",
    ),
    Book: (
        "unix",
        "unix_partition",
        "etype",
        "instrument_xhash",
        "px",
        "spread",
        "vwap",
        "exec_px",
        "imbalance",
        "code",
        "bid_px",
        "ask_px",
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
    counted = leaves(Book.into_field().arrow_type)
    assert len(counted) > 60, "a book is wide, or this walk is wrong"
    assert "bid_levels.item.px" in counted, "the walk reached inside a list of structs"
    assert "instrument" not in Book.into_field().names and "metadata.key" in counted


@pytest.mark.parametrize(
    "shape,names", FILTERED.items(), ids=lambda value: getattr(value, "__name__", "")
)
def test_every_column_a_reader_filters_on_is_inside_the_metrics_budget(
    shape: type, names: tuple[str, ...]
) -> None:
    """A filter column past the cutoff prunes nothing and looks like it works."""
    counted = leaves(shape.into_field().arrow_type)
    for name in names:
        assert name in counted, f"{shape.__name__} has no leaf {name}"
        assert counted.index(name) + 1 <= METRICS_BUDGET, (
            f"{shape.__name__}.{name} is leaf {counted.index(name) + 1}: "
            "declare it before the nested members that pushed it out"
        )
