"""The envelope's own behaviour: what kind of event a shape is, and what it normalises."""

from __future__ import annotations

import dataclasses

import pyarrow
import pytest

from rekep import txhash
from rekep.enums import ManualIndicator
from rekep.market import (
    HASH,
    MIC,
    Book,
    Currency,
    Event,
    EventType,
    Execution,
    Instrument,
    InstUpdate,
    MarketEvent,
    Order,
    Side,
    State,
)
from rekep.market.event import ALTIDS_TYPE, DAY, HOUR, SECOND
from rekep.market.identity import NIL, hash_int_of

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
    assert shape().eventtype is declared


def test_the_type_is_the_class_rather_than_a_caller_s_word_for_it() -> None:
    """A row whose type disagreed with its table would be unreadable."""
    assert Order().eventtype is EventType.ORDER
    assert Order(eventtype=EventType.UNKNOWN).eventtype is EventType.ORDER
    # A shape that deliberately carries more than one kind still says so.
    assert Order(eventtype=EventType.QUOTE).eventtype is EventType.QUOTE


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
    assert Order(unix=unix).unixpartition == 1710374400
    assert Order(unix=unix, unixpartition=999).unixpartition == 1710374400, (
        "what is given is ignored"
    )
    assert 0 <= unix - Order(unix=unix).unixpartition * SECOND < HOUR


def test_the_hour_floors_on_both_sides_of_the_epoch() -> None:
    """Integer division truncates towards zero in most languages and floors in Python."""
    hour_seconds = HOUR // SECOND
    assert Order(unix=0).unixpartition == 0
    assert Order(unix=HOUR - 1).unixpartition == 0
    assert Order(unix=HOUR).unixpartition == hour_seconds
    assert Order(unix=-1).unixpartition == -hour_seconds, "a pre-epoch instant"
    assert Order(unix=-HOUR - 1).unixpartition == -2 * hour_seconds


def test_the_partition_clock_is_narrower_than_the_instant() -> None:
    assert Order.into_field().field("unixpartition").dtype == pyarrow.int32()
    assert Order.into_field().field("unix").dtype == pyarrow.int64()
    assert Order.into_field().field("unixpartition").metadata["unit"] == "second"
    assert Order.into_field().field("unix").metadata["unit"] == "ns"


def test_a_snapshot_keeps_both_when_it_was_taken_and_what_it_is_of() -> None:
    """`unix` orders it against the stream; `snapunix` says what it pictures."""
    taken, subject = 2_000_000_000, 1_000_000_000
    built = Book(unix=taken, snapunix=subject)
    assert built.unix == taken and built.snapunix == subject
    assert built.unix - built.snapunix == 1_000_000_000, "staleness, without a join"
    assert Order().snapunix is None, "and nothing that is not a snapshot carries one"


def test_a_snapshot_drops_previous_market_values_because_they_are_a_delta() -> None:
    current = Book(
        unix=1,
        prevpx=10.0,
        prevqty=20.0,
        prevnotional=200.0,
        prevbidpx=9.0,
        prevbidqty=4.0,
        prevaskpx=11.0,
        prevaskqty=6.0,
        execpx=10.5,
        prevexecpx=10.0,
    )
    snapshot = current.make_snapshot(2)
    assert snapshot is not None
    assert (snapshot.prevpx, snapshot.prevqty, snapshot.prevnotional) == (None, None, None)
    assert (
        snapshot.prevbidpx,
        snapshot.prevbidqty,
        snapshot.prevaskpx,
        snapshot.prevaskqty,
        snapshot.prevexecpx,
    ) == (None, None, None, None, None)
    assert snapshot.execpx == 10.5


def test_an_unchanged_state_expires_once_after_one_day() -> None:
    current = Book(
        unix=1,
        symbolticker="AAPL",
        code="AAPL",
        state=State.OPEN,
    ).identify()
    expired = current.make_snapshot(current.unix + DAY)

    assert expired is not None
    assert expired.state is State.INTERNAL_EXPIRED
    assert expired.expunix == current.unix + DAY
    assert expired.make_snapshot(expired.unix + HOUR) is None


def test_a_finished_state_does_not_emit_an_unchanged_snapshot() -> None:
    closed = Book(unix=1, symbolticker="AAPL", state=State.CLOSED).identify()
    assert closed.make_snapshot(HOUR) is None


def test_a_shape_hashes_whole_columns_the_way_it_hashes_one_row() -> None:
    """`Event.hash_arrow` is `Event.hash_of` over columns, and it had no test.

    The free function underneath is pinned in `test_identity.py`; the
    classmethod that puts the class name in front of it -- which is what keeps
    an `Order` and a `Book` off one identifier -- was the uncovered line.
    """
    symbols = pyarrow.array(["AAPL", "MSFT"])
    ids = pyarrow.array(["cl-1", "cl-2"])
    assert [hash_int_of(one) for one in Order.hash_arrow(symbols, ids).to_pylist()] == [
        Order.hash_of("AAPL", "cl-1"),
        Order.hash_of("MSFT", "cl-2"),
    ]
    assert Order.hash_arrow(symbols, ids).to_pylist() != Book.hash_arrow(symbols, ids).to_pylist()


def test_event_object_streams_cross_the_arrow_boundary_in_bounded_batches() -> None:
    events = [
        Order(
            unix=1,
            code="A",
            altids={"orderid": "A", "clordid": "C1"},
            linkhashes=[7],
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
    unix = 1_710_374_400_000_000_123
    built = Book(unix=unix, symbolticker="AAPL").identify()
    expected = (*built._version_prefix_parts(0), 0)

    assert built.version_parts() == expected
    assert built.vhash == Book.hash_of(*expected)
    assert built.hash == txhash.couple128(unix // 1_000, built.vhash)
    later = dataclasses.replace(built, unix=unix + 1_000, hash=NIL).identify()
    assert later.vhash == built.vhash
    assert later.hash != built.hash


def test_an_unhashed_event_carries_the_nil_identifier_rather_than_a_null() -> None:
    """`hash` is NOT NULL, so an unsaved row is a visible repeat, not a late failure."""
    assert Order().hash == NIL and Order().vhash == NIL and Order().xhash == NIL
    assert Order().state is State.UNKNOWN and Order().prevunix is None
    assert Order().prevhash is None


@pytest.mark.parametrize("creaunix", (1_710_374_400_000_000_123, -1))
def test_xhash_is_the_signed_128_bit_code_digest(creaunix: int) -> None:
    built = Event(creaunix=creaunix, code="ORD-1").identify()
    assert built.xhash == Event.xhash_of("ORD-1")
    assert -(2**127) <= built.xhash < 2**127


def test_xhash_ignores_creation_time() -> None:
    first = Event(creaunix=1_999, code="ORD-1").identify()
    earlier = Event(creaunix=1_000, code="ORD-1").identify()
    later = Event(creaunix=2_000, code="ORD-1").identify()

    assert first.xhash == earlier.xhash == later.xhash == Event.xhash_of("ORD-1")


def test_xhash_arrow_writes_the_direct_digest_and_a_wide_zero_sentinel() -> None:
    codes = pyarrow.array(["ORD-1", "", None, "café"])

    assert Event.xhash_arrow(codes).to_pylist() == [
        txhash.wide_bytes(Event.xhash_of("ORD-1")),
        txhash.wide_bytes(NIL),
        txhash.wide_bytes(NIL),
        txhash.wide_bytes(Event.xhash_of("café")),
    ]


def test_xhash_round_trips_between_the_scalar_and_stored_byte_spellings() -> None:
    event = Event(code="ORD-1").identify()
    stored = event.into_row()

    assert stored["xhash"] == txhash.wide_bytes(event.xhash)
    assert Event.from_dict(stored).xhash == event.xhash


def test_xhash_needs_a_readable_code() -> None:
    assert Event(creaunix=1_000).identify().xhash == NIL


def test_xhash_ignores_the_event_shape_and_market_scope() -> None:
    creation = 1_710_374_400_000_000_123
    events = (
        Event(creaunix=creation, code="ORD-1"),
        Order(
            creaunix=creation,
            symbolticker="AAPL",
            lastmkt=MIC.from_str("XNAS"),
            side=Side.BUY,
            code="ORD-1",
        ),
        Execution(
            creaunix=creation,
            symbolticker="MSFT",
            lastmkt=MIC.from_str("XLON"),
            side=Side.SELL,
            code="ORD-1",
        ),
    )

    assert len({event.identify().xhash for event in events}) == 1


def test_an_event_version_cannot_link_to_itself() -> None:
    own = Event(unix=1, code="ORD-1").identify()
    other = Event(unix=2, code="ORD-2").identify()
    built = Event(unix=1, code="ORD-1", linkhashes=[own.hash, other.hash]).identify()

    assert built.linkhashes == [other.hash]
    assert built.link_to(built, other).linkhashes == [other.hash]
    assert built.primary_link == other.hash


def test_lastmkt_and_reason_distinguish_otherwise_identical_event_versions() -> None:
    xpar = MIC.from_str("XPAR")
    base = Event(unix=1, code="A", lastmkt=xpar, reason="bad quantity").identify()
    other_reason = Event(unix=1, code="A", lastmkt=xpar, reason="bad price").identify()
    other_mic = Event(
        unix=1, code="A", lastmkt=MIC.from_str("XLON"), reason="bad quantity"
    ).identify()
    assert len({base.hash, other_reason.hash, other_mic.hash}) == 3


def test_a_silent_update_keeps_lastmkt_but_not_an_old_reason() -> None:
    previous = Event(unix=1, code="A", lastmkt=MIC.from_str("XPAR"), reason="rejected").identify()
    current = Event(unix=2, code="A").completed_from(previous)
    assert current.lastmkt is previous.lastmkt
    assert current.reason is None


def test_the_code_is_the_lifecycle_and_every_other_identifier_is_beside_it() -> None:
    """One string names the lifecycle; the rest are a map and not a column each."""
    declared = Event.into_field()
    assert "fix:tag" not in declared.field("code").metadata, "a lifecycle is not a FIX field"
    assert declared.field("code").dtype == pyarrow.string()
    assert declared.field("xhash").dtype == HASH
    assert "instrumentxhash" not in MarketEvent.into_field().names
    assert MarketEvent.into_field().field("symbolticker").dtype == pyarrow.string()
    assert declared.field("linkhashes").dtype.value_type == HASH
    assert declared.field("altids").dtype == ALTIDS_TYPE
    assert declared.names.index("altids") == declared.names.index("code") + 1
    assert declared.field("prevhash").dtype == HASH
    assert declared.field("prevhash").nullable
    assert declared.names.index("prevhash") == declared.names.index("prevunix") + 1
    assert MarketEvent.into_field().names == declared.names + [
        name for name in MarketEvent.into_field().names if name not in declared.names
    ], "a market event adds to the envelope and never respells it"
    assert {"seq", "prev_hash", "prev_state", "error"}.isdisjoint(declared.names)
    assert declared.names.count("reason") == 1
    assert "venue" not in MarketEvent.into_field().names
    assert "symbol" not in MarketEvent.into_field().names, "symbolticker is the flat spelling"


@pytest.mark.parametrize(
    "event,code,source",
    (
        (Event(code="GENERIC"), "GENERIC", "Code"),
        (Order(altids={"orderid": "O-1"}), "O-1", "OrderID"),
        (
            Order(altids={"origclordid": "C-0", "clordid": "C-1"}),
            "C-0",
            "OrigClOrdID",
        ),
        (Order(altids={"clordid": "C-1"}), "C-1", "ClOrdID"),
        (Execution(altids={"execid": "E-1"}), "E-1", "ExecID"),
        (Execution(altids={"tradeid": "T-1"}), "T-1", "TradeID"),
        (
            Execution(
                state=State.CANCELLED,
                altids={"execid": "E-2", "execrefid": "E-1"},
            ),
            "E-1",
            "ExecRefID",
        ),
    ),
)
def test_a_lifecycle_code_names_the_field_that_supplied_it(
    event: Event, code: str, source: str
) -> None:
    event.identify()
    assert event.code == code
    assert event.altids["code"] == event.altids[source.lower()] == code


def test_an_instrument_only_market_event_uses_the_ticker_without_aliasing_it() -> None:
    event = Book(symbolticker="AAPL").identify()

    assert event.code == "AAPL"
    assert event.altids == {"code": "AAPL"}


def test_an_instrument_update_keeps_the_nested_ticker_in_all_code_slots() -> None:
    update = InstUpdate.from_instrument(Instrument(symbolticker="AAPL")).identify()
    assert update.code == "AAPL"
    assert update.altids == {"code": "AAPL", "symbolticker": "AAPL"}


@pytest.mark.parametrize("shape", (Order, Execution, Book), ids=lambda cls: cls.__name__)
def test_reference_data_is_transient_and_only_its_identity_is_stored(shape: type) -> None:
    instrument = Instrument(symbol="BTC-USD", currency=Currency.USD)
    assert instrument.xhash != NIL
    built = shape().attach_instrument(instrument)
    scoped = shape(symbolticker="ETH/USD", code="given").attach_instrument(instrument)
    assert built.symbolticker == instrument.symbolticker
    assert scoped.code == "given" and scoped.symbolticker == "ETH/USD"
    assert built.currency is instrument.currency
    assert "instrumentxhash" not in shape.into_field().names
    assert "instrument" not in shape.into_field().names
    assert built.into_instrument() is instrument


@pytest.mark.parametrize("shape", (Order, Execution, Book), ids=lambda cls: cls.__name__)
def test_market_altids_keep_lifecycle_codes_but_drop_instrument_codes(shape: type) -> None:
    event = shape(
        symbolticker="AAPL",
        altids={
            "ClOrdID": "C-1",
            "instrumentid": "INSTRUMENT-1",
            "securityid": "SECURITY-1",
            "SecurityAltID": "XX0000000002",
            "isin": "XX0000000001",
            "symbolticker": "AAPL",
        },
    )
    event.name_altid("UnderlyingSecurityID", "UNDERLYING-1")
    event.name_altid("Symbol", "MSFT")
    event.name_altid("SecondaryOrderID", "ORDER-2")

    assert event.altids == {"clordid": "C-1", "secondaryorderid": "ORDER-2"}


def test_a_stored_market_ticker_is_canonicalized_once() -> None:
    assert Order(symbolticker="eurnok").symbolticker == "EUR/NOK"


def test_market_currency_input_is_normalized_to_its_compact_enum() -> None:
    assert Order(currency=" usd ").currency is Currency.USD


def test_market_float_members_match_their_arrow_physical_type_before_hashing() -> None:
    integer_input = Order(unix=1, code="BTC-USD", lastpx=100, lastqty=2).with_previous(None)
    float_input = Order(unix=1, code="BTC-USD", lastpx=100.0, lastqty=2.0).with_previous(None)

    assert integer_input is not None and float_input is not None
    assert (integer_input.lastpx, integer_input.lastqty) == (100.0, 2.0)
    assert integer_input.hash == float_input.hash


def test_the_market_fallback_stores_the_readable_part_its_xhash_uses() -> None:
    instrument = Instrument(symbol="BTC-USD")
    built = Book(side=Side.UNKNOWN).attach_instrument(instrument).with_previous(None)
    assert built is not None
    assert built.code == instrument.symbolticker
    assert built.altids == {"code": instrument.symbolticker}
    assert built.xhash == Event.xhash_of(built.code)


def test_the_symbol_ticker_is_the_only_flat_instrument_key() -> None:
    ticker = Order.into_field().field("symbolticker")
    assert ticker.dtype == pyarrow.string() and not ticker.nullable
    assert not ticker.is_partition_key
    assert "instrumentxhash" not in Order.into_field().names


@pytest.mark.parametrize("shape", (Order, Execution), ids=lambda cls: cls.__name__)
def test_order_and_execution_keep_manual_indicator_as_a_typed_code(shape: type) -> None:
    event = shape(manualindicator=ManualIndicator.MANUAL)

    assert event.manualindicator is ManualIndicator.MANUAL
    assert shape.into_field().field("manualindicator").dtype == pyarrow.int32()
