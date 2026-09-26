"""The pipeline's tasks: each replaces one window of the table it writes.

One function per table the graph writes, in production order. Each opens its
source and its target through the `Storages` it is handed -- a table is named
`<layer>.<namespace>.<table>`, and the layer is the catalog that holds it --
reads its source's window and writes its target the way that table is
replaced:

```text
task                        reads                               writes
parse_log_messages          a capture URI                       bronze.record_keeping.log_messages
parse_fix_messages_raw      bronze.record_keeping.log_messages  bronze.record_keeping.fix_messages
parse_fix_messages_refined  bronze.record_keeping.fix_messages  silver.record_keeping.fix_messages
parse_books                 silver.record_keeping.fix_messages  silver.record_keeping.books
parse_orders                silver.record_keeping.books         silver.record_keeping.orders
parse_quotes                silver.record_keeping.books         silver.record_keeping.quotes
parse_executions            silver.record_keeping.books         silver.record_keeping.executions
```

The first three are keyed on `curruuid` and a replay of their window lands
the same rows again; the books and the three event tables replace exactly
their window. A window is `[start, end)` over `currunix` as
`rekep.times.window_of` answers it, and every scan hands it to Iceberg as a
predicate on that column, so a task opens only the hour partitions its
window covers. Where a task runs, how often and over which window are the
caller's, and so are the catalogs, which a task never closes. A task creates
a missing target.
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
    UNDATED,
    FixCodec,
    fix_lifecycle_arrow_reader,
    fix_message_field,
    fix_parse_arrow_reader,
    fix_window_filter,
)
from rekep.iceberg import window_filter
from rekep.market import (
    book_arrow_reader,
    book_event_arrow_reader,
    book_field,
    market_event_field,
    market_window_filter,
    market_window_reader,
)
from rekep.storages import Storages
from rekep.text import log_message_field, text_options
from rekep.times import where_within, within

#: The tables the graph writes, each under the name its task writes by default.
LOG_MESSAGES = "bronze.record_keeping.log_messages"
FIX_MESSAGES_RAW = "bronze.record_keeping.fix_messages"
FIX_MESSAGES = "silver.record_keeping.fix_messages"
BOOKS = "silver.record_keeping.books"
ORDERS = "silver.record_keeping.orders"
QUOTES = "silver.record_keeping.quotes"
EXECUTIONS = "silver.record_keeping.executions"

#: The three event tables, by the kind each flattens out of the books.
EVENTS = {"orders": ORDERS, "quotes": QUOTES, "executions": EXECUTIONS}

#: The book columns each event kind flattens; the scan opens no other.
FLATTENED = {
    "orders": ("bidside.deltas", "askside.deltas"),
    "quotes": ("bidside.deltas", "askside.deltas"),
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

    #: The silver `books` snapshot the stage committed (`parse_books`) or read
    #: (`parse_orders`, `parse_quotes`, `parse_executions`, zero for none);
    #: None for every other stage.
    snapshot_id: int | None = None


def _codec_or_env(codec: FixCodec | None) -> FixCodec:
    """`codec`, else the process registry's own, pinned so an undated message
    takes `UNDATED` and a replay of the same bytes answers the same identity."""
    return FixCodec.from_env(default_sending_time=UNDATED) if codec is None else codec


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


def parse_log_messages(
    source: IOBase | str,
    storages: Storages,
    window: Window,
    *,
    rowheader: str | None = None,
    timezone: str = "UTC",
    target: str = LOG_MESSAGES,
) -> Landed:
    """Land the lines of `source` whose `currunix` falls in `window`.

    `source` is an `IOBase`, or a URI bound here and closed after. One that
    does not exist is refused: the read of an absent path answers no rows,
    which would read as a window without lines. `rowheader` names the header
    of a bridge writing the same facts in a layout of its own; its capture
    names must still be `rekep.text.CAPTURES`. `timezone` is the zone the
    bridge prints its clock in: a bridge printing local time is read in its
    zone, so a line lands in the hour of the message it carries.
    """
    with contextlib.ExitStack() as opened:
        if isinstance(source, str):
            source = IOBase.from_uri(source)
            opened.callback(source.close)
        if not source.exists():
            raise FileNotFoundError(source.masked_uri or str(source.url))
        options = text_options(rowheader, timezone)
        field = log_message_field(rowheader)
        # The window is the read's `where`, answered by the record surface
        # over the rows the lines become: the lines whose `currunix` -- off
        # the header, or off the object's own modification time where the
        # header did not match -- falls in `[start, end)`, and no other.
        options.filter = where_within(EVENT_CLOCK, window)
        messages = storages.dataset(target, field=field)
        opened.callback(messages.close)
        read = _Count()
        lines = read(source.read_arrow_reader(options=options))
        opened.callback(lines.close)
        # The storage boundary: the content codes and the row number are read
        # unsigned and Iceberg's only sixty-four-bit integer is signed, so
        # those eight bytes are viewed rather than converted, and the field
        # then casts the rest in its native order.
        stored = stored_arrow_reader(lines, field)
        opened.callback(stored.close)
        # Keyed on `curruuid` within the hour of `currunix`: a replay of the
        # window lands the same rows again, so the table holds each line once.
        written = messages.overwrite_arrow_reader(stored, field, merge_by=True)
        return Landed(read=read.rows, written=written)


def parse_fix_messages_raw(
    storages: Storages,
    window: Window,
    *,
    codec: FixCodec | None = None,
    source: str = LOG_MESSAGES,
    target: str = FIX_MESSAGES_RAW,
) -> Landed:
    """Parse the stored lines of `window` into bronze `fix_messages`: every frame, nothing walked.

    `codec` is the whole parse surface, `FixCodec.from_env()` pinned at
    `UNDATED` when None. A row is a message, so a line carrying two frames
    answers two and one message
    logged at three hops answers three rows of one identity, which the key
    folds: `skipped` counts them.
    """
    codec = _codec_or_env(codec)
    # Declared from the dictionary alone rather than the first batch, so an
    # empty window creates the same table a full one does.
    field = fix_message_field(codec)
    with contextlib.ExitStack() as opened:
        carrier = log_message_field()
        lines = storages.dataset(source, field=carrier)
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
        raw = storages.dataset(target, field=field, merge_schema=True)
        opened.callback(raw.close)
        # Keyed on `curruuid` within the hour of the event's own instant, so
        # every restatement of one event meets the others and lands once.
        written = raw.overwrite_arrow_reader(stored, field, merge_by=True)
        return Landed(read=read.rows, written=written, skipped=answered.rows - written)


def parse_fix_messages_refined(
    storages: Storages,
    window: Window,
    *,
    codec: FixCodec | None = None,
    source: str = FIX_MESSAGES_RAW,
    target: str = FIX_MESSAGES,
) -> Landed:
    """Walk the bronze `fix_messages` rows of `window` into silver, warmed by `HISTORY` before it.

    The walk reads the window and the hour before it in `SORT_COLUMNS` order,
    undated rows included, and only the events it places in the window --
    undated ones included -- are written: the hour before warms the chains
    without replacing their history with a truncated replay, and an expiry
    the walk generates past `end` waits for its own window. `codec` must be
    the one the raw rows were parsed with, `FixCodec.from_env()` when None.
    """
    codec = _codec_or_env(codec)
    history = (window[0] - HISTORY, window[1])
    with contextlib.ExitStack() as opened:
        field = fix_message_field(codec)
        raw = storages.dataset(source, field=field)
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
        refined = storages.dataset(target, field=field, merge_schema=True)
        opened.callback(refined.close)
        written = refined.overwrite_arrow_reader(stored, field, merge_by=True)
        return Landed(read=read.rows, written=written, skipped=placed.rows - written)


def parse_books(
    storages: Storages,
    window: Window,
    *,
    codec: FixCodec | None = None,
    snapshot_millis: int = 0,
    source: str = FIX_MESSAGES,
    target: str = BOOKS,
) -> Landed:
    """Replace `window` of silver `books` and answer the snapshot it committed.

    The book folds the silver `fix_messages` rows of the strict window from no depth
    before `start`, and every book it answers in the window replaces the
    window's rows in one commit -- an empty window too, which removes the
    window's earlier rows. `snapshot_id` is that commit, and is what
    `parse_orders`, `parse_quotes` and `parse_executions` pin their reads to.
    `snapshot_millis` above zero emits owned snapshots on that grid.
    """
    codec = _codec_or_env(codec)
    selected = market_window_filter(window)
    with contextlib.ExitStack() as opened:
        fixed = fix_message_field(codec)
        refined = storages.dataset(source, field=fixed)
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
        books = storages.dataset(target, field=field, merge_schema=True)
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


def parse_orders(
    storages: Storages,
    window: Window,
    *,
    snapshot_id: int | None = None,
    source: str = BOOKS,
    target: str = ORDERS,
) -> Landed:
    """Replace `window` of silver `orders` with the order deltas of one books snapshot."""
    return _flatten("orders", storages, window, snapshot_id, source, target)


def parse_quotes(
    storages: Storages,
    window: Window,
    *,
    snapshot_id: int | None = None,
    source: str = BOOKS,
    target: str = QUOTES,
) -> Landed:
    """Replace `window` of silver `quotes` with the quote deltas of one books snapshot."""
    return _flatten("quotes", storages, window, snapshot_id, source, target)


def parse_executions(
    storages: Storages,
    window: Window,
    *,
    snapshot_id: int | None = None,
    source: str = BOOKS,
    target: str = EXECUTIONS,
) -> Landed:
    """Replace `window` of silver `executions` with the executions of one books snapshot."""
    return _flatten("executions", storages, window, snapshot_id, source, target)


#: The three flattening tasks, by the kind each reads out of the books.
FLATTENERS = {"orders": parse_orders, "quotes": parse_quotes, "executions": parse_executions}


def _flatten(
    kind: str,
    storages: Storages,
    window: Window,
    snapshot_id: int | None,
    source: str,
    target: str,
) -> Landed:
    """Replace `window` of one event table from one books snapshot.

    `snapshot_id` None pins the head this call finds; zero is a pinned
    absence and reads nothing, never permission to follow a newer head, so
    the window is emptied. A positive snapshot of a missing table is refused
    before anything is written. The three kinds read one snapshot and write
    three tables, so they may run in parallel.
    """
    if snapshot_id is not None and (
        isinstance(snapshot_id, bool) or not isinstance(snapshot_id, int) or snapshot_id < 0
    ):
        raise ValueError(f"expected a nonnegative book snapshot_id or None, got {snapshot_id!r}")
    selected = market_window_filter(window)
    with contextlib.ExitStack() as opened:
        declared = book_field()
        books = storages.dataset(source, field=declared)
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
        events = storages.dataset(target, field=field, merge_schema=True)
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
    "EXECUTIONS",
    "FIX_MESSAGES",
    "FIX_MESSAGES_RAW",
    "FLATTENED",
    "FLATTENERS",
    "HISTORY",
    "LOG_MESSAGES",
    "ORDERS",
    "QUOTES",
    "Landed",
    "Storages",
    "parse_books",
    "parse_executions",
    "parse_fix_messages_raw",
    "parse_fix_messages_refined",
    "parse_log_messages",
    "parse_orders",
    "parse_quotes",
]
