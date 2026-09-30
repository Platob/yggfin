"""Native market books and their columnar Iceberg projections.

A book is one `MIC:CFI` category per instant: `categorized_symbol_reader`
clears every row's `symbol` first, so the native fold keys each message by
its category. Books hold the native fold over the requested FIX window,
opening on the membership the hourly lifecycle views state. Orders and
quotes are the book's deltas; executions are its execution events, each
split out of its report once, when the message was parsed.
"""

from __future__ import annotations

import datetime
import functools
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from yggdryl import MarketDataKind

from rekep.arrow_reader import OwnedRecordBatchReader
from rekep.fields import Field
from rekep.fix import EVENT_CLOCK, FixCodec, fix_row_messages, iceberg_event_field


@functools.lru_cache(maxsize=1)
def _book_schema() -> pa.Schema:
    with FixCodec.from_env().book_arrow_reader(()) as reader:
        return reader.schema


def book_field() -> Field:
    """The native book schema under Iceberg's storage types and hour layout."""
    return iceberg_event_field(_book_schema(), "Book")


def market_event_field() -> Field:
    """The native event payload shared by orders, quotes and executions."""
    events = _book_schema().field("executions").type.value_type
    return iceberg_event_field(pa.schema(events), "MarketEvent")


def categorized_symbol_reader(source: pa.RecordBatchReader) -> pa.RecordBatchReader:
    """FIX rows that state no `symbol`, so the native fold books each by category.

    A message stating no ticker is booked under its category,
    `{miccode}:{cficode}` -- `XXXX` for a market it names none of and
    `XXXXXX` for a classification it knows none of -- and the fold is the
    one owner of that key: the classification is the detailed code the
    message's whole chain reaches, a coarse stated `cficode` refined by what
    the bridge states beside it, which no cell of the row holds. Every row
    is categorized, whatever ticker it stated; every other cell is kept.
    """
    index = source.schema.get_field_index("symbol")
    field = source.schema.field(index)

    def batches() -> Iterator[pa.RecordBatch]:
        try:
            for batch in source:
                yield batch.set_column(index, field, pa.nulls(batch.num_rows, field.type))
        finally:
            source.close()

    return OwnedRecordBatchReader(source.schema, batches(), source.close)


#: The grid a book fold restates every book on, in milliseconds: one hour,
#: epoch-aligned, so a book standing at a whole hour is answered there whole,
#: `snapunix` set. Zero disables it.
SNAPSHOT_MILLIS = 3_600_000


def book_arrow_reader(
    codec: FixCodec,
    source: pa.RecordBatchReader,
    *,
    snapshot_millis: int = SNAPSHOT_MILLIS,
) -> pa.RecordBatchReader:
    """Continue books from ordered refined rows through the native codec.

    One book per `symbol` a row states, else per `MIC:CFI` category, so rows
    passed through `categorized_symbol_reader` fold one book per category.
    The codec ignores non-market records, and never fails on what a message
    states: an entry it cannot place -- an order or a quote stating no side,
    an entry stating no `MDEntryType` -- is left out of its book with a
    warning through `logging`. Refined lifecycle is already settled and is
    not run a second time.
    A grid book restates what its book holds and answers no delta or
    execution a book before it answered. A source row whose `snapunix` is set
    -- a lifecycle view of a live chain -- is folded at that instant as the
    whole membership of its book there, with no delta, so the views of one
    hour restate what their books hold.
    """
    reader = codec.book_arrow_reader(
        fix_row_messages(codec, source), snapshot_millis=snapshot_millis
    )
    return OwnedRecordBatchReader.from_reader(reader, source.close)


#: The book column each event table flattens, by the table's kind.
FLATTENED_COLUMNS = {"orders": "deltas", "quotes": "deltas", "executions": "executions"}

#: The native leaf each event table holds, by the table's kind.
EVENT_KINDS = {
    "orders": MarketDataKind.ORDR,
    "quotes": MarketDataKind.QUOT,
    "executions": MarketDataKind.EXEC,
}


def book_event_arrow_reader(source: pa.RecordBatchReader, kind: str) -> pa.RecordBatchReader:
    """Flatten one event kind using Arrow kernels, preserving native facts.

    Orders and quotes are the book's deltas, selected by their native
    `marketdatakind`; executions are the book's own execution list. All
    three are the one native operation row, so every kind answers one
    schema. Live depth is intentionally not expanded on every continuation:
    doing so would manufacture repeated events for unchanged resting entries.
    """
    if kind not in EVENT_KINDS:
        raise ValueError(f"expected orders, quotes or executions; got {kind!r}")
    column = FLATTENED_COLUMNS[kind]
    schema = pa.schema(source.schema.field(column).type.value_type)
    code = pa.scalar(int(EVENT_KINDS[kind]), pa.int32())

    def batches() -> Iterator[pa.RecordBatch]:
        try:
            for batch in source:
                events = pc.list_flatten(batch.column(column))
                if kind != "executions":
                    kinds = pc.struct_field(events, "marketdatakind")
                    if isinstance(kinds, pa.ExtensionArray):
                        kinds = kinds.storage
                    events = pc.filter(events, pc.equal(kinds.cast(pa.int32()), code))
                if len(events):
                    yield pa.RecordBatch.from_arrays(events.flatten(), schema=schema)
        finally:
            source.close()

    return OwnedRecordBatchReader(schema, batches(), source.close)


def market_window_filter(window: tuple[datetime.datetime, datetime.datetime]) -> Any:
    """An exact half-open Iceberg predicate; undated rows receive no exception."""
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

    return And(GreaterThanOrEqual(EVENT_CLOCK, window[0]), LessThan(EVENT_CLOCK, window[1]))


def market_window_reader(
    source: pa.RecordBatchReader, window: tuple[datetime.datetime, datetime.datetime]
) -> pa.RecordBatchReader:
    """Keep only the requested instants, including native scheduled expirations."""

    def batches() -> Iterator[pa.RecordBatch]:
        try:
            for batch in source:
                clock = batch.column(EVENT_CLOCK)
                selected = batch.filter(
                    pc.and_(pc.greater_equal(clock, window[0]), pc.less(clock, window[1]))
                )
                if selected.num_rows:
                    yield selected
        finally:
            source.close()

    return OwnedRecordBatchReader(source.schema, batches(), source.close)


__all__ = [
    "EVENT_KINDS",
    "FLATTENED_COLUMNS",
    "SNAPSHOT_MILLIS",
    "book_arrow_reader",
    "book_event_arrow_reader",
    "book_field",
    "categorized_symbol_reader",
    "market_event_field",
    "market_window_filter",
    "market_window_reader",
]
