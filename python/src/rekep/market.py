"""Native market books and their columnar Iceberg projections.

Books hold the native fold over the requested FIX window. A window does not
restore resting entries created before its start. Orders and quotes are the
book's deltas; executions are its already decomposed execution events.
"""

from __future__ import annotations

import datetime
import functools
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

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


def book_arrow_reader(
    codec: FixCodec,
    source: pa.RecordBatchReader,
    *,
    snapshot_millis: int = 0,
    global_: bool = False,
) -> pa.RecordBatchReader:
    """Continue books from ordered refined rows through the native codec.

    The codec ignores non-market records and refuses malformed admitted
    events. Refined lifecycle is already settled and is not run a second time.
    """
    reader = codec.book_arrow_reader(
        fix_row_messages(codec, source), snapshot_millis=snapshot_millis, global_=global_
    )
    return OwnedRecordBatchReader.from_reader(reader, source.close)


#: The book sides whose deltas the order and quote tables flatten.
SIDES = ("bidside", "askside")

#: The native leaf each event table holds, by the table's kind.
EVENT_KINDS = {"orders": "order_event", "quotes": "quote_event", "executions": "execution_event"}


def book_event_arrow_reader(source: pa.RecordBatchReader, kind: str) -> pa.RecordBatchReader:
    """Flatten one event kind using Arrow kernels, preserving native facts.

    Orders and quotes are the deltas of both book sides, selected by their
    native `kind`; executions are the book's own execution list. All three
    are the one native operation event, so every kind answers one schema.
    Live depth is intentionally not expanded on every continuation: doing so
    would manufacture repeated events for unchanged resting entries.
    """
    if kind not in EVENT_KINDS:
        raise ValueError(f"expected orders, quotes or executions; got {kind!r}")
    if kind == "executions":
        listed = source.schema.field("executions").type
    else:
        listed = source.schema.field(SIDES[0]).type.field("deltas").type
    schema = pa.schema(listed.value_type)
    leaf = EVENT_KINDS[kind]

    def batches() -> Iterator[pa.RecordBatch]:
        try:
            for batch in source:
                if kind == "executions":
                    arrays = (pc.list_flatten(batch.column("executions")),)
                else:
                    arrays = (
                        pc.list_flatten(pc.struct_field(batch.column(side), "deltas"))
                        for side in SIDES
                    )
                for events in arrays:
                    if kind != "executions":
                        events = pc.filter(events, pc.equal(pc.struct_field(events, "kind"), leaf))
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
    "SIDES",
    "book_arrow_reader",
    "book_event_arrow_reader",
    "book_field",
    "market_event_field",
    "market_window_filter",
    "market_window_reader",
]
