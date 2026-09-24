"""Parse one window of stored lines into `fix.raw`: every frame a line carried, nothing walked."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import ExitStack
from typing import Any

import pyarrow

from rekep.fields import stored_arrow_reader
from rekep.fix import (
    EVENT_CLOCK,
    PARSE_COLUMNS,
    fix_codec,
    fix_message_field,
    fix_parse_arrow_reader,
    fix_registry,
)
from rekep.iceberg import IcebergCatalog, window_filter
from rekep.logs import Stage
from rekep.text import Message
from rekep.times import window_of

#: The table this task writes.
TARGET = "fix.raw"
TARGETS = (TARGET,)


def run(
    *,
    messages: str,
    registry: str | None,
    codec_options: Mapping[str, Any] | None,
    start: Any,
    end: Any,
    catalog: Mapping[str, Any],
) -> dict[str, Any]:
    """Parse the `messages` rows whose `currunix` falls in `[start, end)`."""
    with ExitStack() as opened:
        # The same window `parse_messages` wrote, read back off the stored
        # capture: `[start, end)` over `currunix`, the event the read settled
        # over each line, with the lines at the epoch pin beside them, where
        # a handle with no clock at all leaves a line. It is the partition
        # column itself and it is never null, so Iceberg projects the bounds
        # through the hour transform and opens the partitions the window
        # touches and the pin's own hour, and nothing else.
        window = window_of(start, end)
        stage = Stage(
            "parse_fix_raw",
            sources={"messages": messages},
            targets={"raw": TARGET},
            window=window,
        )
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        carrier = Message.into_field()
        lines = store.dataset(messages, field=carrier)
        opened.callback(lines.close)
        # Projected to what the parse consumes -- the line's clock and
        # identity, the body it reads the frames out of, and the four
        # captures that fill a field by their name -- so the scan opens no
        # other column: the line's own code, cross code and row number say
        # nothing about a message, and the thread and level name no field.
        source = lines.read_arrow_reader(
            carrier, row_filter=window_filter(EVENT_CLOCK, window), columns=PARSE_COLUMNS
        )
        opened.callback(source.close)
        counts = {"read": 0, "messages": 0}

        def batches():
            for batch in source:
                counts["read"] += batch.num_rows
                yield batch

        counted = pyarrow.RecordBatchReader.from_batches(source.schema, batches())
        opened.callback(counted.close)
        # The codec is the whole parse surface: the dictionary and the instant
        # an undated message takes are pinned on it once, and the stage after
        # it is a call. No capture order is among them -- this door reads
        # stored rows, where a column named after a field fills it by that
        # name, and a position is what the line door resolves. Pinning one
        # here would be a reading of a header this task never sees, stale the
        # moment the capture is read under one of its own.
        codec = fix_codec(fix_registry(registry), **(codec_options or {}))
        # The published field is what both FIX tables are declared with, read
        # from the dictionary alone rather than from the first batch -- so an
        # empty window creates the same table a full one does.
        field = fix_message_field(codec)
        # The parse alone, over the stored capture's batches, and no walk: a
        # row is a message, so one line carrying two frames answers two -- and
        # one message logged at three hops answers three rows of one identity,
        # which is what the key below folds. `seqnum` and `prevuuid` are empty
        # on every row, because nothing has placed a message in its chain yet;
        # that is `parse_fix_refined`'s reading of this table.
        parsed = fix_parse_arrow_reader(codec, counted)
        opened.callback(parsed.close)

        def answered_batches():
            for batch in parsed:
                counts["messages"] += batch.num_rows
                yield batch

        answered = pyarrow.RecordBatchReader.from_batches(parsed.schema, answered_batches())
        opened.callback(answered.close)
        # The storage boundary: the content codes read as the signed integers
        # Iceberg stores, then the field applied in its native order.
        applied = stored_arrow_reader(answered, field)
        opened.callback(applied.close)
        raw = store.dataset(TARGET, field=field, merge_schema=True)
        opened.callback(raw.close)
        # What the window answers replaces what the table held under the same
        # `curruuid`, so a replay lands the same events again and a message
        # logged at every hop it passed lands once. A key is scoped to its
        # partition and the partition is the hour of `currunix`, which is the
        # event's own instant: every restatement of one event carries the same
        # one, so they meet. A message the parse could not date sits at the
        # codec's pin -- one hour, one partition -- until the walk dates it.
        written = raw.overwrite_arrow_reader(applied, field, merge_by=True)
        # A row is a message, not a line: one line carrying two frames answers
        # two and one carrying none answers nothing, so what the write left
        # out -- a restatement of an identity already landed -- is counted
        # against the messages the codec answered rather than the lines read.
        return stage.finished(
            read=counts["read"],
            written=written,
            skipped=counts["messages"] - written,
            messages=counts["messages"],
        )
