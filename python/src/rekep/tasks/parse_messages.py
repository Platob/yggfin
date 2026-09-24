"""Read physical text lines into `logs.messages`, one window of `currunix` at a time."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import ExitStack
from typing import Any

import pyarrow

from rekep import IOBase
from rekep.fields import stored_arrow_reader
from rekep.iceberg import IcebergCatalog
from rekep.logs import Stage
from rekep.text import Message
from rekep.times import where_within, window_of

#: The table this task writes.
TARGET = "logs.messages"
TARGETS = (TARGET,)


def run(
    *,
    filesystem: str,
    rowheader: str | None,
    start: Any,
    end: Any,
    catalog: Mapping[str, Any],
) -> dict[str, Any]:
    """Land the lines of `filesystem` whose `currunix` falls in `[start, end)`."""
    with ExitStack() as opened:
        # The window a run covers, `[start, end)`: the last day when the
        # parameters name neither bound, and exactly the scheduler's interval
        # when they name both. Read before anything is opened, so a bound that
        # is not an instant is refused before a capture is.
        window = window_of(start, end)
        source = IOBase.from_uri(filesystem)
        opened.callback(source.close)
        source_location = source.masked_uri or str(source.url)
        if not source.exists():
            raise FileNotFoundError(source_location)
        stage = Stage(
            "parse_messages",
            sources={"capture": source_location},
            targets={"messages": TARGET},
            window=window,
        )
        field = Message.into_field()
        # A bridge writing these same facts in a layout of its own is read by
        # naming its header here; the names it captures are still the ones
        # the read fills from -- a column each, and `currunix` for `mtime` --
        # and a header that renames one is refused rather than stored as a
        # column of nulls under a clock that settled nothing.
        options = Message.text_options(rowheader)
        # The window is the read's `where`, answered by the record surface
        # over the rows the lines become: the lines whose `currunix` -- the
        # event the read settled over each, off its header or off the
        # object's own modification time where the header did not match, and
        # the column the table is laid out by -- falls in `[start, end)`, and
        # no other. What the read answers is what the run read.
        options.filter = where_within("currunix", window)
        counts = {"read": 0}
        store = IcebergCatalog.from_dict(catalog)
        opened.callback(store.close)
        messages = store.dataset(stage.targets["messages"], field=field)
        opened.callback(messages.close)
        reader = source.read_arrow_reader(options=options)
        opened.callback(reader.close)

        def batches():
            for batch in reader:
                counts["read"] += batch.num_rows
                yield batch

        read = pyarrow.RecordBatchReader.from_batches(reader.schema, batches())
        opened.callback(read.close)
        # The storage boundary: the read states the content code and the row
        # number unsigned and Iceberg's only sixty-four-bit integer is signed,
        # so those eight bytes are viewed rather than converted, and the field
        # then casts the rest -- the instant to the microsecond a table holds,
        # the identity to the sixteen bytes it keys on -- in its native order.
        parsed = stored_arrow_reader(read, field)
        opened.callback(parsed.close)
        # What the window carries replaces what the table held under the same
        # `curruuid`, the sole key the field declares, within the hour of
        # `currunix` it falls in: a replay of the window lands the same rows
        # again, so the table holds each line once however often it runs.
        written = messages.overwrite_arrow_reader(parsed, field, merge_by=True)
        return stage.finished(read=counts["read"], written=written)
