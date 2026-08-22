"""The envelope's own behaviour: what kind of event a shape is, and what it normalises."""

from __future__ import annotations

import pyarrow
import pytest

from rekep.market import (
    Book,
    BookSide,
    Event,
    EventType,
    Execution,
    Instrument,
    MarketEvent,
    Order,
    State,
)
from rekep.market.event import HOUR
from rekep.market.identity import NIL

SHAPES = {
    Order: EventType.ORDER,
    Execution: EventType.EXECUTION,
    BookSide: EventType.BOOK_SIDE,
    Book: EventType.BOOK,
}


@pytest.mark.parametrize(
    "shape,declared", SHAPES.items(), ids=lambda value: getattr(value, "__name__", "")
)
def test_a_shape_says_what_kind_of_event_it_is(shape: type, declared: EventType) -> None:
    assert shape.EVENT_TYPE is declared
    assert shape().etype is declared


def test_the_type_is_the_class_rather_than_a_caller_s_word_for_it() -> None:
    """A row whose type disagreed with its table would be unreadable."""
    assert Order().etype is EventType.ORDER
    assert Order(etype=EventType.UNKNOWN).etype is EventType.ORDER
    # A shape that deliberately carries more than one kind still says so.
    assert Order(etype=EventType.QUOTE).etype is EventType.QUOTE


@pytest.mark.parametrize(
    "shape,declared", SHAPES.items(), ids=lambda value: getattr(value, "__name__", "")
)
def test_exactly_one_of_the_named_questions_is_true(shape: type, declared: EventType) -> None:
    """`is_order` and friends must partition the shapes, not overlap them."""
    answers = {
        EventType.ORDER: shape.is_order(),
        EventType.EXECUTION: shape.is_execution(),
        EventType.BOOK_SIDE: shape.is_book_side(),
        EventType.BOOK: shape.is_book(),
    }
    assert [kind for kind, yes in answers.items() if yes] == [declared]


def test_a_band_answers_for_everything_inside_it() -> None:
    """So a caller can ask "is this a state?" without knowing which state."""
    assert Book.is_a(EventType.STATE) and BookSide.is_a(EventType.STATE)
    assert Order.is_a(EventType.INTENT) and not Order.is_a(EventType.STATE)
    assert Execution.is_a(EventType.FACT)
    assert Book.is_a(EventType.BOOK), "and a member still answers for itself"


def test_only_the_shapes_that_are_pictures_are_snapshots() -> None:
    assert Book.is_snapshot() and BookSide.is_snapshot()
    assert not Order.is_snapshot() and not Execution.is_snapshot()


def test_the_base_event_claims_to_be_nothing_in_particular() -> None:
    """`Event` is abstract, so it must not answer yes to any of them."""
    assert Event.EVENT_TYPE is EventType.UNKNOWN
    assert not any((Event.is_order(), Event.is_execution(), Event.is_book(), Event.is_book_side()))
    assert MarketEvent.EVENT_TYPE is EventType.UNKNOWN


def test_the_hour_is_derived_from_the_timestamp_and_never_given() -> None:
    """Denormalised for the partition, so one authority rather than two columns."""
    unix = 1710374400_000000000 + 5
    assert Order(unix=unix).unix_hour == 1710374400_000000000
    assert Order(unix=unix, unix_hour=999).unix_hour == 1710374400_000000000, (
        "what is given is ignored"
    )
    assert 0 <= unix - Order(unix=unix).unix_hour < HOUR


def test_the_hour_floors_on_both_sides_of_the_epoch() -> None:
    """Integer division truncates towards zero in most languages and floors in Python."""
    assert Order(unix=0).unix_hour == 0
    assert Order(unix=HOUR - 1).unix_hour == 0
    assert Order(unix=HOUR).unix_hour == HOUR
    assert Order(unix=-1).unix_hour == -HOUR, "a pre-epoch instant"
    assert Order(unix=-HOUR - 1).unix_hour == -2 * HOUR


def test_the_hour_and_the_instant_are_the_same_type() -> None:
    """So a partition filter and a time filter are one comparison, with no cast."""
    assert Order.FIELD.field("unix_hour").arrow_type == Order.FIELD.field("unix").arrow_type
    assert Order.FIELD.field("unix_hour").metadata["unit"] == "nanosecond"


def test_a_snapshot_keeps_both_when_it_was_taken_and_what_it_is_of() -> None:
    """`unix` orders it against the stream; `sunix` says what it is a picture of."""
    taken, subject = 2_000_000_000, 1_000_000_000
    built = Book(unix=taken, sunix=subject)
    assert built.unix == taken and built.sunix == subject
    assert built.unix - built.sunix == 1_000_000_000, "staleness, without a join"
    assert Order().sunix is None, "and nothing that is not a snapshot carries one"


def test_a_shape_hashes_whole_columns_the_way_it_hashes_one_row() -> None:
    """`Event.hash_arrow` is `Event.hash_of` over columns, and it had no test.

    The free function underneath is pinned in `test_identity.py`; the
    classmethod that puts the class name in front of it -- which is what keeps
    an `Order` and a `Book` off one identifier -- was the uncovered line.
    """
    symbols = pyarrow.array(["AAPL", "MSFT"])
    ids = pyarrow.array(["cl-1", "cl-2"])
    assert Order.hash_arrow(symbols, ids).to_pylist() == [
        Order.hash_of("AAPL", "cl-1"),
        Order.hash_of("MSFT", "cl-2"),
    ]
    assert Order.hash_arrow(symbols, ids).to_pylist() != Book.hash_arrow(symbols, ids).to_pylist()


def test_an_unhashed_event_carries_the_nil_identifier_rather_than_a_null() -> None:
    """`hash` is NOT NULL, so an unsaved row is a visible repeat, not a late failure."""
    assert Order().hash == NIL and Order().xhash == NIL
    assert Order().state is State.UNKNOWN and Order().prev_state is State.UNKNOWN
    assert Order().prev_hash is None, "but the previous version really is absent"


@pytest.mark.parametrize("shape", (Order, Execution, BookSide, Book), ids=lambda cls: cls.__name__)
def test_the_instrument_is_flattened_onto_the_column_the_partition_reads(shape: type) -> None:
    """A nested member nothing can partition on and a flat column everything does
    have to be the same instrument, so the flat one is derived and never given."""
    instrument = Instrument(symbol="BTC-USD")
    assert instrument.xhash != NIL
    assert shape(instrument=instrument).instrument_hash == instrument.xhash
    assert shape(instrument=instrument, instrument_hash=7).instrument_hash == instrument.xhash


def test_a_row_that_names_no_instrument_keeps_the_hash_it_was_handed() -> None:
    """A projection reading back the flat column alone must not lose it to a NIL."""
    assert Order(instrument_hash=7).instrument_hash == 7
    assert Order().instrument_hash == NIL, "and unset really is unset"


def test_the_instrument_hash_is_not_nullable_because_a_bucket_of_null_is_every_bucket() -> None:
    partition = Order.FIELD.field("instrument_hash")
    assert not partition.nullable
    assert partition.partition_transform == "bucket[16]"
