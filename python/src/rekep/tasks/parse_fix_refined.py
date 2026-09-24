"""Walk one window of `fix.raw`, warmed by the hour before it, into `fix.refined`."""

from __future__ import annotations

import datetime
from collections.abc import Mapping
from contextlib import ExitStack
from typing import Any

import pyarrow
import pyarrow.compute

from rekep.fields import stored_arrow_reader
from rekep.fix import (
    EVENT_CLOCK,
    SORT_COLUMNS,
    UNDATED,
    fix_codec,
    fix_lifecycle_arrow_reader,
    fix_message_field,
    fix_registry,
    fix_window_filter,
)
from rekep.iceberg import IcebergCatalog
from rekep.logs import Stage
from rekep.times import window_of, within

#: The table this task writes.
TARGET = "fix.refined"
TARGETS = (TARGET,)


def run(
    *,
    raw: str,
    registry: str | None,
    codec_options: Mapping[str, Any] | None,
    start: Any,
    end: Any,
    catalog: Mapping[str, Any],
) -> dict[str, Any]:
    """Walk the `raw` rows of `[start, end)` and land only the events in it."""
    with ExitStack() as opened:
        window = window_of(start, end)
        history = (window[0] - datetime.timedelta(hours=1), window[1])
        stage = Stage(
            "parse_fix_refined",
            sources={"raw": raw},
            targets={"refined": TARGET},
            window=window,
        )
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        # The same codec `parse_fix_raw` pinned: the walk reads each row
        # back as the message that wrote it, and the dictionary is what wrote
        # it. The same field too, because a walked row is the parsed row
        # restated: same columns, same key, same layout.
        codec = fix_codec(fix_registry(registry), **(codec_options or {}))
        field = fix_message_field(codec)
        parsed = store.dataset(raw, field=field)
        opened.callback(parsed.close)
        # The hour transform prunes partitions, then the ordered reader
        # concatenates disjoint file ranges and merges only overlapping ones.
        # Undated rows use the epoch partition and TransactTime file bounds.
        source = parsed.read_arrow_reader(
            field, row_filter=fix_window_filter(history), order_by=SORT_COLUMNS
        )
        opened.callback(source.close)
        counts = {"read": 0, "events": 0, "outside_window": 0}

        def batches():
            for batch in source:
                counts["read"] += batch.num_rows
                yield batch

        counted = pyarrow.RecordBatchReader.from_batches(source.schema, batches())
        opened.callback(counted.close)
        # The native lifecycle owns dating, delivery deduplication, state and
        # expiry, and collects and stable-sorts its finite input itself -- the
        # previous hour and this window; there is no second collected Arrow
        # table or union before the walk.
        walked = fix_lifecycle_arrow_reader(codec, counted)
        opened.callback(walked.close)

        def in_window():
            for batch in walked:
                clock = batch.column(EVENT_CLOCK)
                selected = batch.filter(
                    pyarrow.compute.or_(
                        within(clock, window), pyarrow.compute.equal(clock, UNDATED)
                    )
                )
                counts["events"] += selected.num_rows
                counts["outside_window"] += batch.num_rows - selected.num_rows
                if selected.num_rows:
                    yield selected

        events = pyarrow.RecordBatchReader.from_batches(walked.schema, in_window())
        opened.callback(events.close)
        # The storage boundary, the same one the raw stage crossed: the codes
        # read as the signed integers Iceberg stores, then the field applied
        # in its native order.
        applied = stored_arrow_reader(events, field)
        opened.callback(applied.close)
        refined = store.dataset(TARGET, field=field, merge_schema=True)
        opened.callback(refined.close)
        # Only this window is written. Previous-hour rows warm the lifecycle
        # without replacing the historical chain with a truncated replay.
        written = refined.overwrite_arrow_reader(applied, field, merge_by=True)
        return stage.finished(
            read=counts["read"],
            written=written,
            skipped=counts["events"] - written,
            events=counts["events"],
            outside_window=counts["outside_window"],
        )
