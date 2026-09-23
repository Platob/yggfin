"""Native books cross storage once; columnar projections preserve their events."""

import datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from rekep.arrow_reader import OwnedRecordBatchReader
from rekep.fields import stored_arrow_reader
from rekep.fix import fix_codec, fix_message_field, fix_parse_field
from rekep.iceberg import partition_keys, primary_keys
from rekep.market import (
    book_arrow_reader,
    book_event_arrow_reader,
    book_field,
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


def native_books():
    codec = fix_codec(batch_row_size=1)
    messages = [codec.parse_fix_line(frame) for frame in FRAMES]
    refined = codec.arrow_reader(fix_parse_field(codec), messages)
    stored = stored_arrow_reader(refined, fix_message_field(codec))
    return book_arrow_reader(codec, stored)


def test_books_accept_refined_storage_and_ignore_administration():
    books = native_books().read_all()
    assert books.num_rows == 4
    assert books.column("symbolticker").to_pylist() == ["AAPL"] * 4
    assert books.column("bid")[1].as_py()["live"][0]["price"] == Decimal("101")
    assert books.column("executions")[3].as_py()[0]["quantity"] == Decimal("4")


@pytest.mark.parametrize(("kind", "expected"), [("orders", 1), ("quotes", 3), ("executions", 3)])
def test_flatten_preserves_each_native_event_once(kind, expected):
    books = stored_arrow_reader(native_books(), book_field()).read_all()
    columns = ["executions"] if kind == "executions" else ["bid", "ask"]
    events = book_event_arrow_reader(books.select(columns).to_reader(), kind).read_all()
    assert events.num_rows == expected
    assert events.schema.equals(market_event_field().into_arrow_schema(), check_metadata=False)
    rows = books.to_pylist()
    reference = []
    for book in rows:
        if kind == "executions":
            reference.extend(book["executions"])
        else:
            reference.extend(
                {name: value[name] for name in events.schema.names}
                for side in ("bid", "ask")
                for value in book[side]["deltas"]
                if value["operationkind"] == kind[:-1]
            )
    assert events.to_pylist() == reference
    assert len(set(events.column("curruuid").to_pylist())) == expected
    if kind == "quotes":
        assert events.column("marketoperationid").to_pylist() == [3, 3, 3]


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
        codec = fix_codec()
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


def test_admitted_errors_are_not_dropped():
    codec = fix_codec()
    bad = codec.parse_fix_line(b"8=FIX.4.4|35=AE|52=20260921-10:00:00|571=T1|150=H|55=AAPL|10=0|")
    source = codec.arrow_reader(fix_parse_field(codec), [bad])
    with pytest.raises(Exception, match=r"MsgType\(35\).*AE"):
        book_arrow_reader(codec, source).read_all()
