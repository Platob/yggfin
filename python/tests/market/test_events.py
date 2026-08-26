"""The envelope's own behaviour: what kind of event a shape is, and what it normalises."""

from __future__ import annotations

import pyarrow
import pytest

from rekep.market import (
    MIC,
    Book,
    Currency,
    Event,
    EventType,
    Execution,
    Instrument,
    MarketEvent,
    Order,
    Side,
    State,
)
from rekep.market.event import CODES_TYPE, DAY, HOUR, SECOND
from rekep.market.identity import NIL

SHAPES = {
    Order: EventType.ORDER,
    Execution: EventType.EXECUTION,
    Book: EventType.BOOK,
}


@pytest.mark.parametrize(
    "shape,declared", SHAPES.items(), ids=lambda value: getattr(value, "__name__", "")
)
def test_a_shape_says_what_kind_of_event_it_is(shape: type, declared: EventType) -> None:
    assert shape.into_event_type() is declared
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
        EventType.BOOK: shape.is_book(),
    }
    assert [kind for kind, yes in answers.items() if yes] == [declared]


def test_a_band_answers_for_everything_inside_it() -> None:
    """So a caller can ask "is this a state?" without knowing which state."""
    assert Book.is_a(EventType.STATE)
    assert Order.is_a(EventType.INTENT) and not Order.is_a(EventType.STATE)
    assert Execution.is_a(EventType.FACT)
    assert Book.is_a(EventType.BOOK), "and a member still answers for itself"


def test_only_the_shapes_that_are_pictures_are_snapshots() -> None:
    assert Book.is_snapshot()
    assert not Order.is_snapshot() and not Execution.is_snapshot()


def test_the_base_event_claims_to_be_nothing_in_particular() -> None:
    """`Event` is abstract, so it must not answer yes to any of them."""
    assert Event.into_event_type() is EventType.UNKNOWN
    assert not any((Event.is_order(), Event.is_execution(), Event.is_book()))
    assert MarketEvent.into_event_type() is EventType.UNKNOWN


def test_the_hour_is_derived_from_the_timestamp_and_never_given() -> None:
    """Denormalised for the partition, so one authority rather than two columns."""
    unix = 1710374400_000000000 + 5
    assert Order(unix=unix).unix_partition == 1710374400
    assert Order(unix=unix, unix_partition=999).unix_partition == 1710374400, (
        "what is given is ignored"
    )
    assert 0 <= unix - Order(unix=unix).unix_partition * SECOND < HOUR


def test_the_hour_floors_on_both_sides_of_the_epoch() -> None:
    """Integer division truncates towards zero in most languages and floors in Python."""
    hour_seconds = HOUR // SECOND
    assert Order(unix=0).unix_partition == 0
    assert Order(unix=HOUR - 1).unix_partition == 0
    assert Order(unix=HOUR).unix_partition == hour_seconds
    assert Order(unix=-1).unix_partition == -hour_seconds, "a pre-epoch instant"
    assert Order(unix=-HOUR - 1).unix_partition == -2 * hour_seconds


def test_the_partition_clock_is_narrower_than_the_instant() -> None:
    assert Order.into_field().field("unix_partition").arrow_type == pyarrow.int32()
    assert Order.into_field().field("unix").arrow_type == pyarrow.int64()
    assert Order.into_field().field("unix_partition").metadata["unit"] == "second"
    assert Order.into_field().field("unix").metadata["unit"] == "nanosecond"


def test_a_snapshot_keeps_both_when_it_was_taken_and_what_it_is_of() -> None:
    """`unix` orders it against the stream; `sunix` says what it is a picture of."""
    taken, subject = 2_000_000_000, 1_000_000_000
    built = Book(unix=taken, sunix=subject)
    assert built.unix == taken and built.sunix == subject
    assert built.unix - built.sunix == 1_000_000_000, "staleness, without a join"
    assert Order().sunix is None, "and nothing that is not a snapshot carries one"


def test_a_snapshot_drops_previous_market_values_because_they_are_a_delta() -> None:
    current = Book(
        unix=1,
        prev_px=10.0,
        prev_qty=20.0,
        prev_notional=200.0,
        prev_bid_px=9.0,
        prev_bid_qty=4.0,
        prev_ask_px=11.0,
        prev_ask_qty=6.0,
        exec_px=10.5,
        prev_exec_px=10.0,
    )
    snapshot = current.make_snapshot(2)
    assert snapshot is not None
    assert (snapshot.prev_px, snapshot.prev_qty, snapshot.prev_notional) == (None, None, None)
    assert (
        snapshot.prev_bid_px,
        snapshot.prev_bid_qty,
        snapshot.prev_ask_px,
        snapshot.prev_ask_qty,
        snapshot.prev_exec_px,
    ) == (None, None, None, None, None)
    assert snapshot.exec_px == 10.5


def test_an_unchanged_state_expires_once_after_one_day() -> None:
    current = Book(
        unix=1,
        instrument_xhash=1,
        code="AAPL",
        state=State.OPEN,
    ).identify()
    expired = current.make_snapshot(current.unix + DAY)

    assert expired is not None
    assert expired.state is State.INTERNAL_EXPIRED
    assert expired.eunix == current.unix + DAY
    assert expired.make_snapshot(expired.unix + HOUR) is None


def test_a_finished_state_does_not_emit_an_unchanged_snapshot() -> None:
    closed = Book(unix=1, instrument_xhash=1, state=State.CLOSED).identify()
    assert closed.make_snapshot(HOUR) is None


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


def test_event_object_streams_cross_the_arrow_boundary_in_bounded_batches() -> None:
    events = [
        Order(
            unix=1,
            code="A",
            codes={"order_id": "A", "client_order_id": "C1"},
            linked_events=[(0, 7)],
            side=Side.BUY,
        ).identify(),
        Order(unix=2, code="B", side=Side.SELL).identify(),
    ]
    reader = Order.into_arrow_reader(events, batch_row_size=1)
    batches = list(reader)
    assert [batch.num_rows for batch in batches] == [1, 1]

    source = pyarrow.RecordBatchReader.from_batches(reader.schema, batches)
    rebuilt = list(Order.from_arrow_reader(source))
    assert [one.into_dict() for one in rebuilt] == [one.into_dict() for one in events]


def test_an_empty_event_stream_still_declares_its_schema() -> None:
    reader = Order.into_arrow_reader(())
    assert reader.read_all().num_rows == 0
    assert reader.schema.equals(Order.into_field().into_arrow_schema(), check_metadata=True)
    with pytest.raises(ValueError, match="batch_row_size must be positive"):
        Order.into_arrow_reader((), batch_row_size=0)


def test_an_empty_book_version_includes_explicit_empty_side_lengths() -> None:
    unix, instrument_xhash = 1_710_374_400_000_000_123, 42
    built = Book(unix=unix, instrument_xhash=instrument_xhash).identify()

    assert built.version_parts() == (unix, instrument_xhash, 0, 0)
    assert built.hash == Book.hash_of(unix, instrument_xhash, 0, 0)
    assert Book.hash_arrow(
        pyarrow.array([unix], pyarrow.int64()),
        pyarrow.array([instrument_xhash], pyarrow.int64()),
        pyarrow.array([0], pyarrow.int64()),
        pyarrow.array([0], pyarrow.int64()),
    ).to_pylist() == [built.hash]


def test_an_unhashed_event_carries_the_nil_identifier_rather_than_a_null() -> None:
    """`hash` is NOT NULL, so an unsaved row is a visible repeat, not a late failure."""
    assert Order().hash == NIL and Order().xhash == NIL
    assert Order().state is State.UNKNOWN and Order().prev_unix is None


def test_mic_and_reason_distinguish_otherwise_identical_event_versions() -> None:
    xpar = MIC.from_str("XPAR")
    base = Event(unix=1, code="A", mic=xpar, reason="bad quantity").identify()
    other_reason = Event(unix=1, code="A", mic=xpar, reason="bad price").identify()
    other_mic = Event(unix=1, code="A", mic=MIC.from_str("XLON"), reason="bad quantity").identify()
    assert len({base.hash, other_reason.hash, other_mic.hash}) == 3


def test_a_silent_update_keeps_the_lifecycle_mic_but_not_an_old_reason() -> None:
    previous = Event(unix=1, code="A", mic=MIC.from_str("XPAR"), reason="rejected").identify()
    current = Event(unix=2, code="A").completed_from(previous)
    assert current.mic is previous.mic
    assert current.reason is None


def test_the_code_is_the_lifecycle_and_every_other_identifier_is_beside_it() -> None:
    """One string names the lifecycle; the rest are a map and not a column each."""
    declared = Event.into_field()
    assert "fix:tag" not in declared.field("code").metadata, "a lifecycle is not a FIX field"
    assert declared.field("code").arrow_type == pyarrow.string()
    assert declared.field("codes").arrow_type == CODES_TYPE
    assert declared.names.index("codes") == declared.names.index("code") + 1
    assert MarketEvent.into_field().names == declared.names + [
        name for name in MarketEvent.into_field().names if name not in declared.names
    ], "a market event adds to the envelope and never respells it"
    assert {"seq", "prev_hash", "prev_state", "error"}.isdisjoint(declared.names)
    assert declared.names.count("reason") == 1
    assert "venue" not in MarketEvent.into_field().names
    assert "symbol" not in MarketEvent.into_field().names, "instrument_code is the flat spelling"


@pytest.mark.parametrize("shape", (Order, Execution, Book), ids=lambda cls: cls.__name__)
def test_reference_data_is_transient_and_only_its_identity_is_stored(shape: type) -> None:
    instrument = Instrument(symbol="BTC-USD", currency=Currency.USD)
    assert instrument.xhash != NIL
    built = shape().attach_instrument(instrument)
    scoped = shape(instrument_xhash=7, code="given").attach_instrument(instrument)
    assert built.instrument_xhash == instrument.xhash and built.symbol == instrument.symbol
    assert scoped.instrument_xhash == 7 and scoped.code == "given"
    assert scoped.symbol == instrument.symbol, "a lifecycle code never displaces the symbol"
    assert built.ccy is instrument.currency
    assert "instrument" not in shape.into_field().names
    assert built.into_instrument() is instrument


def test_a_row_that_names_no_instrument_keeps_the_hash_it_was_handed() -> None:
    """A projection reading back the flat column alone must not lose it to a NIL."""
    assert Order(instrument_xhash=7).instrument_xhash == 7
    assert Order().instrument_xhash == NIL, "and unset really is unset"


def test_market_currency_input_is_normalized_to_its_compact_enum() -> None:
    assert Order(ccy=" usd ").ccy is Currency.USD


def test_market_float_members_match_their_arrow_physical_type_before_hashing() -> None:
    integer_input = Order(unix=1, code="BTC-USD", px=100, qty=2).with_previous(None)
    float_input = Order(unix=1, code="BTC-USD", px=100.0, qty=2.0).with_previous(None)

    assert integer_input is not None and float_input is not None
    assert (integer_input.px, integer_input.qty) == (100.0, 2.0)
    assert integer_input.hash == float_input.hash


def test_the_market_fallback_stores_the_readable_part_its_scoped_hash_uses() -> None:
    instrument = Instrument(symbol="BTC-USD")
    built = Book(side=Side.UNKNOWN).attach_instrument(instrument).with_previous(None)
    assert built is not None
    assert built.code == built.code == instrument.symbol
    assert built.xhash == Book.hash_of(instrument.xhash, built.code, Side.UNKNOWN)


def test_the_instrument_identity_is_flat_required_and_not_partitioned_on() -> None:
    """The hour prunes an instrument read already; bucketing a hash only adds files."""
    identity = Order.into_field().field("instrument_xhash")
    assert not identity.nullable
    assert not identity.is_partition_key
    assert not Order.into_field().field("instrument_code").nullable
