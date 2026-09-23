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
from rekep.fix import EVENT_CLOCK, FixCodec, fix_codec, fix_row_messages, iceberg_fix_field


@functools.lru_cache(maxsize=1)
def _book_schema() -> pa.Schema:
    with fix_codec().book_arrow_reader(()) as reader:
        return reader.schema


def book_field() -> Field:
    """The native book schema under Iceberg's storage types and hour layout."""
    return iceberg_fix_field(_book_schema(), "Book")


def market_event_field() -> Field:
    """The native event payload shared by orders, quotes and executions."""
    events = _book_schema().field("executions").type.value_type
    return iceberg_fix_field(pa.schema(events), "MarketEvent")


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


def book_event_arrow_reader(source: pa.RecordBatchReader, kind: str) -> pa.RecordBatchReader:
    """Flatten one event kind using Arrow kernels, preserving native facts.

    Live depth is intentionally not expanded on every continuation: doing so
    would manufacture repeated events for unchanged resting entries.
    """
    if kind not in ("orders", "quotes", "executions"):
        raise ValueError(f"expected orders, quotes or executions; got {kind!r}")
    if kind == "executions":
        schema = pa.schema(source.schema.field("executions").type.value_type)
        indices = tuple(range(len(schema)))
    else:
        operations = source.schema.field("bid").type.field("deltas").type.value_type
        schema = pa.schema(
            member for member in operations if member.name not in ("operationkind", "executions")
        )
        indices = tuple(operations.get_field_index(name) for name in schema.names)

    def batches() -> Iterator[pa.RecordBatch]:
        try:
            for batch in source:
                if kind == "executions":
                    arrays = (pc.list_flatten(batch.column("executions")),)
                else:
                    arrays = (
                        pc.list_flatten(pc.struct_field(batch.column(side), "deltas"))
                        for side in ("bid", "ask")
                    )
                for events in arrays:
                    if kind != "executions":
                        events = pc.filter(
                            events, pc.equal(pc.struct_field(events, "operationkind"), kind[:-1])
                        )
                    if len(events):
                        children = events.flatten()
                        yield pa.RecordBatch.from_arrays(
                            [children[index] for index in indices], schema=schema
                        )
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
    "book_arrow_reader",
    "book_event_arrow_reader",
    "book_field",
    "market_event_field",
    "market_window_filter",
    "market_window_reader",
]
