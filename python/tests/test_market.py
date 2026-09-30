"""Native books cross storage once; columnar projections preserve their events."""

import datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from rekep import MarketDataKind, Side, State
from rekep.arrow_reader import OwnedRecordBatchReader
from rekep.fields import stored_arrow_reader
from rekep.fix import FixCodec, fix_message_field, fix_parse_field
from rekep.iceberg import partition_keys, primary_keys
from rekep.market import (
    EVENT_KINDS,
    FLATTENED_COLUMNS,
    book_arrow_reader,
    book_event_arrow_reader,
    book_field,
    categorized_symbol_reader,
    market_event_field,
    market_window_reader,
)

FRAMES = (
    b"8=FIX.4.4|35=0|52=20260921-10:00:00|10=0|",
    b"8=FIX.4.4|35=W|52=20260921-10:00:01|55=AAPL|268=2|"
    b"269=0|278=B1|270=100|271=10|269=1|278=A1|270=102|271=12|10=0|",
    b"8=FIX.4.4|35=X|52=20260921-10:00:02|55=AAPL|268=2|"
    b"279=1|269=0|278=B1|270=101|271=11|"
    b"279=0|269=2|278=T1|270=101|271=2|10=0|",
    b"8=FIX.4.4|35=D|52=20260921-10:00:03|11=O1|55=AAPL|54=1|38=5|44=99|10=0|",
    b"8=FIX.4.4|35=AE|52=20260921-10:00:04|571=T2|150=F|55=AAPL|"
    b"32=10|31=101.25|60=20260921-10:00:04|552=2|"
    b"54=1|1427=BUY-EXEC|1009=4|37=BUY-ORDER|"
    b"54=2|1427=SELL-EXEC|1009=6|37=SELL-ORDER|10=0|",
)

#: Three orders on two markets. The second states a coarse `CFICode(461)` and
#: the detailed code its bridge wrote beside it; the third a coarse code alone.
CATEGORIZED = (
    b"8=FIX.4.4|35=D|52=20260921-10:00:00|11=O1|55=AAPL|54=1|38=5|44=99|207=XNAS|461=ESVUFR|10=0|",
    b"8=FIX.4.4|35=D|52=20260921-10:00:01|11=O2|55=MSFT|54=1|38=3|44=98|"
    b"207=XNAS|461=ESXXXX|DETAILEDCFICODE=ESVUFR|10=0|",
    b"8=FIX.4.4|35=D|52=20260921-10:00:02|11=O3|55=ACME|54=2|38=1|44=5|461=ESXXXX|10=0|",
)


def stored_rows(frames):
    """The frames' messages as the silver table holds their rows."""
    codec = FixCodec.from_env(batch_row_size=1)
    # The line door splits a trade report into the executions it states.
    messages = [message for frame in frames for message in codec.parse_line(frame)]
    refined = codec.arrow_reader(fix_parse_field(codec), messages)
    return codec, stored_arrow_reader(refined, fix_message_field(codec)).read_all()


def native_books():
    """The frames' books, folded from their rows as the silver table holds them."""
    codec, stored = stored_rows(FRAMES)
    return book_arrow_reader(codec, stored.to_reader())


def test_books_accept_refined_storage_and_ignore_administration():
    books = native_books().read_all()
    assert books.num_rows == 4
    assert books.column("marketdatakind").to_pylist() == [int(MarketDataKind.BOOK)] * 4
    assert books.column("ticker").to_pylist() == ["AAPL"] * 4
    assert books.column("bidlimits")[1].as_py()[0]["price"] == Decimal("101")
    assert books.column("bidpx")[1].as_py() == Decimal("101")
    # The trade report was split into one execution per side it states when
    # it was parsed, each keeping the quantity its own side traded, and the
    # book holds each once.
    traded = books.column("executions")[3].as_py()
    assert [(held["side"], held["lastqty"], held["state"]) for held in traded] == [
        (int(Side.BUYS), Decimal("4"), int(State.FILLED)),
        (int(Side.SELL), Decimal("6"), int(State.FILLED)),
    ]


def test_a_categorized_book_is_the_folds_own_mic_cfi_key():
    codec, stored = stored_rows(CATEGORIZED)
    categorized = categorized_symbol_reader(stored.to_reader()).read_all()
    assert categorized.column("symbol").null_count == categorized.num_rows
    assert categorized.drop_columns("symbol").equals(stored.drop_columns("symbol"))
    books = book_arrow_reader(codec, categorized.to_reader()).read_all()
    # The coarse stated code the bridge refined keys with the detailed one,
    # so the two XNAS orders share one book, and a coarse code alone keys as
    # no classification at all.
    assert books.column("crosscode").to_pylist() == ["XNAS:ESVUFR"] * 2 + ["XXXX:XXXXXX"]
    assert books.column("ticker").to_pylist() == [None] * 3
    assert [len(alive) for alive in books.column("alive").to_pylist()] == [1, 2, 1]
    # The key is the one the fold gives every leaf stating no ticker.
    for book in books.to_pylist():
        for leaf in book["deltas"]:
            assert leaf["ticker"] is None
            key = f"{leaf['miccode'] or 'XXXX'}:{leaf['cficode'] or 'XXXXXX'}"
            assert key == book["crosscode"]


@pytest.mark.parametrize(("kind", "expected"), [("orders", 1), ("quotes", 3), ("executions", 3)])
def test_flatten_preserves_each_native_event_once(kind, expected):
    books = stored_arrow_reader(native_books(), book_field()).read_all()
    column = FLATTENED_COLUMNS[kind]
    events = book_event_arrow_reader(books.select([column]).to_reader(), kind).read_all()
    assert events.num_rows == expected
    assert events.schema.equals(market_event_field().into_arrow_schema(), check_metadata=False)
    reference = [
        value
        for book in books.to_pylist()
        for value in book[column]
        if value["marketdatakind"] == EVENT_KINDS[kind]
    ]
    assert events.to_pylist() == reference
    assert len(set(events.column("curruuid").to_pylist())) == expected
    assert events.column("marketdatakind").to_pylist() == [int(EVENT_KINDS[kind])] * expected


def test_empty_books_keep_the_native_contract():
    for kind in ("orders", "quotes", "executions"):
        empty = pa.RecordBatchReader.from_batches(book_field().into_arrow_schema(), [])
        events = book_event_arrow_reader(empty, kind).read_all()
        assert events.num_rows == 0
        assert events.schema.equals(market_event_field().into_arrow_schema(), check_metadata=False)
    for field in (book_field(), market_event_field()):
        assert primary_keys(field) == ["curruuid"]
        assert partition_keys(field) == {"currunix": "hour"}


def test_execution_flatten_shares_value_buffers():
    books = stored_arrow_reader(native_books(), book_field()).read_all()
    batch = books.to_batches()[-1]
    values = batch.column("executions").values.field("price")
    flat = book_event_arrow_reader(
        pa.RecordBatchReader.from_batches(batch.schema, [batch]), "executions"
    ).read_next_batch()
    assert flat.column("price").buffers()[1].address == values.buffers()[1].address


def test_flatten_refuses_an_unknown_kind():
    empty = pa.RecordBatchReader.from_batches(book_field().into_arrow_schema(), [])
    with pytest.raises(ValueError, match="expected orders, quotes or executions"):
        book_event_arrow_reader(empty, "trades")


@pytest.mark.parametrize("operation", ["books", "events", "window"])
@pytest.mark.parametrize("pull", [False, True])
def test_readers_release_sources_on_early_close(operation, pull):
    released = []
    table = stored_arrow_reader(native_books(), book_field()).read_all()
    if operation == "books":
        codec = FixCodec.from_env()
        table = codec.arrow_reader(
            fix_parse_field(codec), [codec.parse_fix_line(FRAMES[1])]
        ).read_all()
    source = OwnedRecordBatchReader(
        table.schema, iter(table.to_batches()), lambda: released.append(1)
    )
    if operation == "books":
        reader = book_arrow_reader(codec, source)
    elif operation == "events":
        reader = book_event_arrow_reader(source, "quotes")
    else:
        start = datetime.datetime(2026, 9, 21, tzinfo=datetime.timezone.utc)
        reader = market_window_reader(source, (start, start + datetime.timedelta(days=1)))
    if pull:
        reader.read_next_batch()
    reader.close()
    if operation == "books":
        # The codec took the source through its C stream, so the native reader
        # owns it, and a native reader dropped without the interpreter's lock
        # releases the Python objects it held at the next call into the
        # extension rather than at the drop.
        _ = codec.threads
    assert released == [1]


def test_window_is_strict_at_both_bounds_and_excludes_undated_rows():
    start = datetime.datetime(2026, 9, 21, 10, tzinfo=datetime.timezone.utc)
    end = start + datetime.timedelta(seconds=1)
    batch = pa.record_batch(
        [
            pa.array(
                [None, datetime.datetime(1970, 1, 1), start, end], type=pa.timestamp("us", "UTC")
            )
        ],
        names=["currunix"],
    )
    source = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    selected = market_window_reader(source, (start, end)).read_all()
    assert selected.column("currunix").to_pylist() == [start]


def test_an_entry_the_fold_cannot_place_is_left_out_and_the_fold_goes_on():
    """The fold never fails on what a message states. A book entry stating no
    `MDEntryType(269)` has no side to stand on, so it is left out of its book,
    with a warning through `logging`, and every entry after it still books."""
    codec = FixCodec.from_env()
    bad = codec.parse_fix_line(
        b"8=FIX.4.4|35=W|52=20260921-10:00:00|55=AAPL|268=1|278=B1|270=100|271=10|10=0|"
    )
    good = codec.parse_fix_line(
        b"8=FIX.4.4|35=W|52=20260921-10:00:01|55=AAPL|268=1|269=0|278=B2|270=99|271=10|10=0|"
    )

    alone = codec.arrow_reader(fix_parse_field(codec), [bad])
    assert book_arrow_reader(codec, alone).read_all().num_rows == 0
    both = codec.arrow_reader(fix_parse_field(codec), [bad, good])
    (book,) = book_arrow_reader(codec, both).read_all().to_pylist()
    assert [(level["price"], level["quantity"]) for level in book["bidlimits"]] == [
        (Decimal("99"), Decimal("10"))
    ]
    assert len(book["alive"]) == len(book["deltas"]) == 1
