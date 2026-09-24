"""The one body `parse_orders`, `parse_quotes` and `parse_executions` share.

Each reads one window from one committed `market.books` snapshot and stores
the native children of one kind. The book already owns the FIX fold and the
lifecycle facts, so nothing here repeats either.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import ExitStack
from typing import Any

import pyarrow

from rekep.fields import stored_arrow_reader
from rekep.iceberg import IcebergCatalog
from rekep.logs import Stage
from rekep.market import (
    book_event_arrow_reader,
    book_field,
    market_event_field,
    market_window_filter,
    market_window_reader,
)
from rekep.times import window_of

#: The book columns each kind flattens, and nothing else is scanned.
COLUMNS = {
    "orders": ("bid.deltas", "ask.deltas"),
    "quotes": ("bid.deltas", "ask.deltas"),
    "executions": ("executions",),
}


def flattened(
    kind: str,
    *,
    books: str,
    snapshot_id: int | None,
    start: Any,
    end: Any,
    catalog: Mapping[str, Any],
) -> dict[str, Any]:
    """Replace `[start, end)` of `market.<kind>` from one `books` snapshot.

    `snapshot_id` null pins the head this run finds; zero is a pinned absence
    and reads nothing, never permission to follow a newer head.
    """
    if snapshot_id is not None and (
        isinstance(snapshot_id, bool) or not isinstance(snapshot_id, int) or snapshot_id < 0
    ):
        raise ValueError(f"expected a nonnegative book snapshot_id or None, got {snapshot_id!r}")
    target = f"market.{kind}"
    with ExitStack() as opened:
        window = window_of(start, end)
        selected = market_window_filter(window)
        stage = Stage(
            f"parse_{kind}",
            sources={"books": books},
            targets={kind: target},
            window=window,
        )
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        declared = book_field()
        bookset = store.dataset(books, field=declared)
        opened.callback(bookset.close)
        source_snapshot_id = snapshot_id
        if source_snapshot_id is None:
            snapshot = bookset.iceberg_table.current_snapshot() if bookset.exists else None
            source_snapshot_id = snapshot.snapshot_id if snapshot is not None else 0
        if source_snapshot_id == 0:
            source = pyarrow.RecordBatchReader.from_batches(declared.into_arrow_schema(), [])
        else:
            if not bookset.exists:
                raise ValueError(f"{books} has no snapshot {source_snapshot_id}: table is missing")
            source = bookset.read_arrow_reader(
                row_filter=selected, columns=COLUMNS[kind], snapshot_id=source_snapshot_id
            )
        opened.callback(source.close)
        counts = {"read": 0, "events": 0}

        def batches():
            for batch in source:
                counts["read"] += batch.num_rows
                yield batch

        counted = pyarrow.RecordBatchReader.from_batches(source.schema, batches())
        opened.callback(counted.close)
        children = book_event_arrow_reader(counted, kind)
        opened.callback(children.close)
        bounded = market_window_reader(children, window)
        opened.callback(bounded.close)

        def answered_batches():
            for batch in bounded:
                counts["events"] += batch.num_rows
                yield batch

        answered = pyarrow.RecordBatchReader.from_batches(bounded.schema, answered_batches())
        opened.callback(answered.close)
        field = market_event_field()
        applied = stored_arrow_reader(answered, field)
        opened.callback(applied.close)
        stored = store.dataset(target, field=field, merge_schema=True)
        opened.callback(stored.close)
        written = stored.overwrite_arrow_reader(applied, field, row_filter=selected)
        return stage.finished(
            read=counts["read"],
            written=written,
            skipped=max(0, counts["events"] - written),
            source_snapshot_id=source_snapshot_id,
        )
