"""Fold one window of `fix.refined` into `market.books` and publish the snapshot it committed."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from contextlib import ExitStack
from typing import Any

import pyarrow

from rekep.fields import stored_arrow_reader
from rekep.fix import SORT_COLUMNS, fix_codec, fix_message_field, fix_registry
from rekep.iceberg import IcebergCatalog
from rekep.logs import Stage
from rekep.market import (
    book_arrow_reader,
    book_field,
    market_window_filter,
    market_window_reader,
)
from rekep.times import window_of

#: The table this task writes.
TARGET = "market.books"
TARGETS = (TARGET,)


def run(
    *,
    refined: str,
    registry: str | None,
    codec_options: Mapping[str, Any] | None,
    snapshot_millis: int,
    start: Any,
    end: Any,
    catalog: Mapping[str, Any],
) -> dict[str, Any]:
    """Replace `[start, end)` of `market.books` and answer its `snapshot_id`."""
    with ExitStack() as opened:
        window = window_of(start, end)
        selected = market_window_filter(window)
        stage = Stage(
            "parse_books",
            sources={"refined": refined},
            targets={"books": TARGET},
            window=window,
        )
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        codec = fix_codec(fix_registry(registry), **(codec_options or {}))
        fixed = fix_message_field(codec)
        messages = store.dataset(refined, field=fixed)
        opened.callback(messages.close)
        source = messages.read_arrow_reader(fixed, row_filter=selected, order_by=SORT_COLUMNS)
        opened.callback(source.close)
        counts = {"read": 0}

        def batches():
            for batch in source:
                counts["read"] += batch.num_rows
                yield batch

        counted = pyarrow.RecordBatchReader.from_batches(source.schema, batches())
        opened.callback(counted.close)
        folded = book_arrow_reader(codec, counted, snapshot_millis=snapshot_millis)
        opened.callback(folded.close)
        bounded = market_window_reader(folded, window)
        opened.callback(bounded.close)
        field = book_field()
        applied = stored_arrow_reader(bounded, field)
        opened.callback(applied.close)
        bookset = store.dataset(TARGET, field=field, merge_schema=True)
        opened.callback(bookset.close)
        run_id = uuid.uuid4().hex
        written = bookset.overwrite_arrow_reader(
            applied, field, row_filter=selected, properties={"rekep.books-run-id": run_id}
        )
        # A recovered commit acknowledgement may refresh to a newer head.
        # Identify this write by its snapshot summary, never by that head.
        snapshots = bookset.iceberg_table.metadata.snapshots
        committed = [
            snapshot.snapshot_id
            for snapshot in snapshots
            if snapshot.summary is not None and snapshot.summary.get("rekep.books-run-id") == run_id
        ]
        if len(committed) == 1:
            snapshot_id = committed[0]
        elif not snapshots and written == 0:
            snapshot_id = 0
        else:
            raise ValueError(f"{TARGET} has no unique committed snapshot for run {run_id}")
        return stage.finished(
            read=counts["read"],
            written=written,
            skipped=0,
            snapshot_id=snapshot_id,
        )
