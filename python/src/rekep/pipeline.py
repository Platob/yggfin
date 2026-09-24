"""The pipeline's stages: each replaces one window of the table it writes.

One function per table the graph writes, in production order. Each opens its
source and its target through one `IcebergCatalog`, reads its source's
window and writes its target the way that table is replaced:

```text
capture       -> parse_messages    -> logs.messages   keyed on curruuid
logs.messages -> parse_fix_raw     -> fix.raw         keyed on curruuid
fix.raw       -> parse_fix_refined -> fix.refined     keyed on curruuid
fix.refined   -> parse_books       -> market.books    the window replaced
market.books  -> parse_events      -> market.<kind>   the window replaced
```

A window is `[start, end)` as `rekep.times.window_of` answers it. Where a
stage runs, how often and over which window are the caller's, and so is the
catalog, which a stage never closes. A stage creates a missing target.
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import uuid

import pyarrow
from yggdryl import IOBase

from rekep.arrow_reader import OwnedRecordBatchReader
from rekep.fields import stored_arrow_reader
from rekep.fix import (
    EVENT_CLOCK,
    PARSE_COLUMNS,
    SORT_COLUMNS,
    FixCodec,
    fix_codec,
    fix_lifecycle_arrow_reader,
    fix_message_field,
    fix_parse_arrow_reader,
    fix_window_filter,
)
from rekep.iceberg import IcebergCatalog, window_filter
from rekep.market import (
    book_arrow_reader,
    book_event_arrow_reader,
    book_field,
    market_event_field,
    market_window_filter,
    market_window_reader,
)
from rekep.text import Message
from rekep.times import where_within, within

#: The tables the graph writes, each under the name its stage writes by default.
MESSAGES = "logs.messages"
RAW = "fix.raw"
REFINED = "fix.refined"
BOOKS = "market.books"
EVENTS = {"orders": "market.orders", "quotes": "market.quotes", "executions": "market.executions"}

#: The book columns each event kind flattens; the scan opens no other.
FLATTENED = {
    "orders": ("bid.deltas", "ask.deltas"),
    "quotes": ("bid.deltas", "ask.deltas"),
    "executions": ("executions",),
}

#: How far before its window the walk reads: a chain that began in the hour
#: before `start` is placed, and none of that hour is written.
HISTORY = datetime.timedelta(hours=1)

#: The snapshot summary property that names the `parse_books` write that
#: committed it, since a recovered commit may refresh to a newer head.
BOOKS_RUN = "rekep.books-run-id"

Window = tuple[datetime.datetime, datetime.datetime]


@dataclasses.dataclass(frozen=True)
class Landed:
    """What one stage read from its source and wrote into its target."""

    #: Source rows the window selected.
    read: int

    #: Target rows the stage wrote.
    written: int

    #: Rows the stage answered that the target's key folded into a written
    #: one: a message logged again at every hop it passed, one row each.
    skipped: int = 0

    #: The `market.books` snapshot the stage committed (`parse_books`) or read
    #: (`parse_events`, zero for none); None for every other stage.
    snapshot_id: int | None = None


class _Count:
    """The rows of every reader passed through it, counted as they are read.

    The counted reader owns the one it counts, so closing it -- or a stage
    unwinding on an error -- closes both.
    """

    def __init__(self) -> None:
        self.rows = 0

    def __call__(self, source: pyarrow.RecordBatchReader) -> pyarrow.RecordBatchReader:
        def batches():
            for batch in source:
                self.rows += batch.num_rows
                yield batch

        return OwnedRecordBatchReader(source.schema, batches(), source.close)


def parse_messages(
    source: IOBase | str,
    catalog: IcebergCatalog,
    window: Window,
    *,
    rowheader: str | None = None,
    target: str = MESSAGES,
) -> Landed:
    """Land the lines of `source` whose `currunix` falls in `window`.

    `source` is an `IOBase`, or a URI bound here and closed after. One that
    does not exist is refused: the native read of an absent path answers no
    rows, which would read as a window without lines. `rowheader` names the
    header of a bridge writing the same facts in a layout of its own; its
    capture names are still the ones `Message.text_options` requires.
    """
    with contextlib.ExitStack() as opened:
        if isinstance(source, str):
            source = IOBase.from_uri(source)
            opened.callback(source.close)
        if not source.exists():
            raise FileNotFoundError(source.masked_uri or str(source.url))
        field = Message.into_field()
        options = Message.text_options(rowheader)
        # The window is the read's `where`, answered by the record surface
        # over the rows the lines become: the lines whose `currunix` -- off
        # the header, or off the object's own modification time where the
        # header did not match -- falls in `[start, end)`, and no other.
        options.filter = where_within(EVENT_CLOCK, window)
        messages = catalog.dataset(target, field=field)
        opened.callback(messages.close)
        read = _Count()
        lines = read(source.read_arrow_reader(options=options))
        opened.callback(lines.close)
        # The storage boundary: the content code and the row number are read
        # unsigned and Iceberg's only sixty-four-bit integer is signed, so
        # those eight bytes are viewed rather than converted, and the field
        # then casts the rest in its native order.
        stored = stored_arrow_reader(lines, field)
        opened.callback(stored.close)
        # Keyed on `curruuid` within the hour of `currunix`: a replay of the
        # window lands the same rows again, so the table holds each line once.
        written = messages.overwrite_arrow_reader(stored, field, merge_by=True)
        return Landed(read=read.rows, written=written)


def parse_fix_raw(
    catalog: IcebergCatalog,
    window: Window,
    *,
    codec: FixCodec | None = None,
    source: str = MESSAGES,
    target: str = RAW,
) -> Landed:
    """Parse the stored lines of `window` into `fix.raw`: every frame, nothing walked.

    `codec` is the whole parse surface, `fix_codec()` when None. A row is a
    message, so a line carrying two frames answers two and one message
    logged at three hops answers three rows of one identity, which the key
    folds: `skipped` counts them.
    """
    codec = fix_codec() if codec is None else codec
    # Declared from the dictionary alone rather than the first batch, so an
    # empty window creates the same table a full one does.
    field = fix_message_field(codec)
    with contextlib.ExitStack() as opened:
        carrier = Message.into_field()
        lines = catalog.dataset(source, field=carrier)
        opened.callback(lines.close)
        # `[start, end)` over `currunix` with the epoch pin beside it, projected
        # to what the parse consumes, so the scan opens no other column.
        read = _Count()
        scanned = read(
            lines.read_arrow_reader(
                carrier, row_filter=window_filter(EVENT_CLOCK, window), columns=PARSE_COLUMNS
            )
        )
        opened.callback(scanned.close)
        answered = _Count()
        parsed = answered(fix_parse_arrow_reader(codec, scanned))
        opened.callback(parsed.close)
        stored = stored_arrow_reader(parsed, field)
        opened.callback(stored.close)
        raw = catalog.dataset(target, field=field, merge_schema=True)
        opened.callback(raw.close)
        # Keyed on `curruuid` within the hour of the event's own instant, so
        # every restatement of one event meets the others and lands once.
        written = raw.overwrite_arrow_reader(stored, field, merge_by=True)
        return Landed(read=read.rows, written=written, skipped=answered.rows - written)


def parse_fix_refined(
    catalog: IcebergCatalog,
    window: Window,
    *,
    codec: FixCodec | None = None,
    source: str = RAW,
    target: str = REFINED,
) -> Landed:
    """Walk the `fix.raw` rows of `window`, warmed by the `HISTORY` before it.

    The walk reads the window and the hour before it in `SORT_COLUMNS` order,
    undated rows included, and only the events it places in the window --
    undated ones included -- are written: the hour before warms the chains
    without replacing their history with a truncated replay, and an expiry
    the walk generates past `end` waits for its own window. `codec` must be
    the one the raw rows were parsed with, `fix_codec()` when None.
    """
    codec = fix_codec() if codec is None else codec
    history = (window[0] - HISTORY, window[1])
    with contextlib.ExitStack() as opened:
        field = fix_message_field(codec)
        raw = catalog.dataset(source, field=field)
        opened.callback(raw.close)
        read = _Count()
        scanned = read(
            raw.read_arrow_reader(
                field,
                row_filter=fix_window_filter(history),
                order_by=SORT_COLUMNS,
            )
        )
        opened.callback(scanned.close)
        walked = fix_lifecycle_arrow_reader(codec, scanned)
        opened.callback(walked.close)
        placed = _Count()

        def in_window():
            for batch in walked:
                selected = batch.filter(within(batch.column(EVENT_CLOCK), window))
                if selected.num_rows:
                    yield selected

        events = placed(pyarrow.RecordBatchReader.from_batches(walked.schema, in_window()))
        opened.callback(events.close)
        stored = stored_arrow_reader(events, field)
        opened.callback(stored.close)
        refined = catalog.dataset(target, field=field, merge_schema=True)
        opened.callback(refined.close)
        written = refined.overwrite_arrow_reader(stored, field, merge_by=True)
        return Landed(read=read.rows, written=written, skipped=placed.rows - written)


def parse_books(
    catalog: IcebergCatalog,
    window: Window,
    *,
    codec: FixCodec | None = None,
    snapshot_millis: int = 0,
    source: str = REFINED,
    target: str = BOOKS,
) -> Landed:
    """Replace `window` of `market.books` and answer the snapshot it committed.

    The book folds the `fix.refined` rows of the strict window from no depth
    before `start`, and every book it answers in the window replaces the
    window's rows in one commit -- an empty window too, which removes the
    window's earlier rows. `snapshot_id` is that commit, and is what
    `parse_events` pins its kinds to.
    `snapshot_millis` above zero emits owned snapshots on that grid.
    """
    codec = fix_codec() if codec is None else codec
    selected = market_window_filter(window)
    with contextlib.ExitStack() as opened:
        fixed = fix_message_field(codec)
        refined = catalog.dataset(source, field=fixed)
        opened.callback(refined.close)
        read = _Count()
        scanned = read(refined.read_arrow_reader(fixed, row_filter=selected, order_by=SORT_COLUMNS))
        opened.callback(scanned.close)
        folded = book_arrow_reader(codec, scanned, snapshot_millis=snapshot_millis)
        opened.callback(folded.close)
        bounded = market_window_reader(folded, window)
        opened.callback(bounded.close)
        field = book_field()
        stored = stored_arrow_reader(bounded, field)
        opened.callback(stored.close)
        books = catalog.dataset(target, field=field, merge_schema=True)
        opened.callback(books.close)
        run = uuid.uuid4().hex
        written = books.overwrite_arrow_reader(
            stored, field, row_filter=selected, properties={BOOKS_RUN: run}
        )
        # A recovered commit acknowledgement may refresh to a newer head, so
        # this write is found by its own summary, never by that head.
        snapshots = books.iceberg_table.metadata.snapshots
        committed = [
            snapshot.snapshot_id
            for snapshot in snapshots
            if snapshot.summary is not None and snapshot.summary.get(BOOKS_RUN) == run
        ]
        if len(committed) != 1:
            raise ValueError(f"{target} has no unique committed snapshot for run {run}")
        return Landed(read=read.rows, written=written, snapshot_id=committed[0])


def parse_events(
    kind: str,
    catalog: IcebergCatalog,
    window: Window,
    *,
    snapshot_id: int | None = None,
    source: str = BOOKS,
    target: str | None = None,
) -> Landed:
    """Replace `window` of `market.<kind>` from one `market.books` snapshot.

    `kind` is `orders`, `quotes` or `executions`. `snapshot_id` None pins the
    head this call finds; zero is a pinned absence and reads nothing, never
    permission to follow a newer head, so the window is emptied. A positive
    snapshot of a missing table is refused before anything is written.
    """
    if kind not in FLATTENED:
        raise ValueError(f"expected orders, quotes or executions; got {kind!r}")
    if snapshot_id is not None and (
        isinstance(snapshot_id, bool) or not isinstance(snapshot_id, int) or snapshot_id < 0
    ):
        raise ValueError(f"expected a nonnegative book snapshot_id or None, got {snapshot_id!r}")
    target = EVENTS[kind] if target is None else target
    selected = market_window_filter(window)
    with contextlib.ExitStack() as opened:
        declared = book_field()
        books = catalog.dataset(source, field=declared)
        opened.callback(books.close)
        if snapshot_id is None:
            head = books.iceberg_table.current_snapshot() if books.exists else None
            snapshot_id = 0 if head is None else head.snapshot_id
        if snapshot_id == 0:
            scanned = pyarrow.RecordBatchReader.from_batches(declared.into_arrow_schema(), [])
        elif not books.exists:
            raise ValueError(f"{source} has no snapshot {snapshot_id}: table is missing")
        else:
            scanned = books.read_arrow_reader(
                row_filter=selected, columns=FLATTENED[kind], snapshot_id=snapshot_id
            )
        opened.callback(scanned.close)
        read = _Count()
        counted = read(scanned)
        opened.callback(counted.close)
        children = book_event_arrow_reader(counted, kind)
        opened.callback(children.close)
        answered = _Count()
        bounded = answered(market_window_reader(children, window))
        opened.callback(bounded.close)
        field = market_event_field()
        stored = stored_arrow_reader(bounded, field)
        opened.callback(stored.close)
        events = catalog.dataset(target, field=field, merge_schema=True)
        opened.callback(events.close)
        written = events.overwrite_arrow_reader(stored, field, row_filter=selected)
        return Landed(
            read=read.rows,
            written=written,
            skipped=max(0, answered.rows - written),
            snapshot_id=snapshot_id,
        )


__all__ = [
    "BOOKS",
    "BOOKS_RUN",
    "EVENTS",
    "FLATTENED",
    "HISTORY",
    "MESSAGES",
    "RAW",
    "REFINED",
    "Landed",
    "parse_books",
    "parse_events",
    "parse_fix_raw",
    "parse_fix_refined",
    "parse_messages",
]
