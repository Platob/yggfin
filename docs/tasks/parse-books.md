# parse_books

`parse_books(storages, window, *, codec=None, snapshot_millis=SNAPSHOT_MILLIS, commit_row_size=COMMIT_ROW_SIZE, source=FIX_MESSAGES, target=BOOKS)`
folds the silver FIX events of `[start - HISTORY, end)` into order books --
one per `MIC:CFI` category and instant, and every book again on each whole
hour -- replaces the window `[start, end)` of `silver.record_keeping.books`
in one snapshot, and answers the snapshot it committed for the
[flattening tasks](parse-orders-quotes-executions.md) to read.

```python
import tempfile
from pathlib import Path

from rekep import Storages
from rekep.pipeline import (
    BOOKS,
    BOOKS_RUN,
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
)
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
storages = Storages.from_dict(
    {
        layer: {
            "name": layer,
            "properties": {
                "type": "sql",
                "uri": f"sqlite:///{root / layer}.db",
                "warehouse": str(root / layer),
            },
        }
        for layer in ("bronze", "silver", "gold")
    }
)
window = window_of("2026-08-14T00:00:00Z", "2026-08-14T16:30:00Z")
with storages:
    parse_log_messages("file:data/capture/ulbridge.log", storages, window)
    parse_fix_messages_raw(storages, window)
    parse_fix_messages_refined(storages, window)

    first = parse_books(storages, window)
    assert (first.read, first.written) == (47, 29)
    # A rerun replaces the same window with the same books, in a new snapshot.
    again = parse_books(storages, window)
    assert (again.read, again.written) == (47, 29)
    assert again.snapshot_id != first.snapshot_id

    books = storages.dataset(BOOKS)
    try:
        assert books.read_arrow_table().num_rows == 29
        runs = {
            snapshot.snapshot_id: snapshot.summary.get(BOOKS_RUN)
            for snapshot in books.iceberg_table.metadata.snapshots
        }
    finally:
        books.close()
    # Each fold's commit names the run that wrote it.
    assert None not in (runs[first.snapshot_id], runs[again.snapshot_id])
    assert runs[first.snapshot_id] != runs[again.snapshot_id]
```

## Fold

For a window `[start, end)` the scan reads the silver events of
`[start - HISTORY, end)`, `rekep.pipeline.HISTORY` being one hour, with no
epoch or null exception, in `currunix, seqnum, curruuid` order: one hour
partition after the other, each finished before the next is opened, the
files of one hour merged in bounded runs, and nothing collected before the
fold, which reads them as they come.
`rekep.market.categorized_symbol_reader` clears each row's `symbol`, and
`rekep.market.book_arrow_reader` reads each row back as its message and hands
them to the native book fold, which owns admission, operation kinds,
continuation, matching, expiration and book identity; the lifecycle is not
walked again. A message stating no ticker is booked under its category,
`{miccode}:{cficode}` -- `XXXX` where it names no market, `XXXXXX` where it
knows no detailed classification -- so every book is one `MIC:CFI` category:
the classification is the detailed one the message's chain reaches, a coarse
stated `CFICode(461)` refined by the detailed code the bridge states beside
it, and the fold is its one owner.

The fold admits orders, quotes, executions and market-data updates; a trade
report's fills are the executions its parse split off, each booked once, and
administration, requests, acknowledgements and reports that execute nothing
contribute nothing. A message is booked on the side its silver row states,
which the walk takes from the one live side of its order where the message
states none: the shipped capture's cancel reject at 21:59:46 states no
`Side(54)` and joins its order's sell, so the evening's one book holds it on
that side, beside the cancel request it answers.

```python
import tempfile
from pathlib import Path

from rekep import Side, State, Storages
from rekep.pipeline import (
    BOOKS,
    FIX_MESSAGES,
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
)
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
storages = Storages.from_dict(
    {
        layer: {
            "name": layer,
            "properties": {
                "type": "sql",
                "uri": f"sqlite:///{root / layer}.db",
                "warehouse": str(root / layer),
            },
        }
        for layer in ("bronze", "silver", "gold")
    }
)
# The bridge prints the reject's lines at 23:59 on its Central European clock:
# read in that zone, they share the hour of 21:00 UTC with the frames they carry.
evening = window_of("2026-08-14T21:00:00Z", "2026-08-14T22:00:00Z")
order = "SELL:816179183-1983-98963_912"


def rows(table: str) -> list:
    dataset = storages.dataset(table)
    try:
        return dataset.read_arrow_table().to_pylist()
    finally:
        dataset.close()


with storages:
    parse_log_messages("file:data/capture/ulbridge.log", storages, evening)
    parse_fix_messages_raw(storages, evening)
    parse_fix_messages_refined(storages, evening)
    (reject,) = [row for row in rows(FIX_MESSAGES) if row["msgtype"] == "9"]
    assert (reject["crosscode"], reject["side"]) == (order, Side.SELL)

    landed = parse_books(storages, evening)
    assert (landed.read, landed.written) == (3, 1)
    (book,) = rows(BOOKS)
    assert (book["currunix"], book["crosscode"]) == (reject["currunix"], "XXXX:XXXXXX")
    # The cancel request, then its reject, in the chain's order: the reject
    # states the order rejected, so it leaves the book.
    assert [(delta["side"], State(delta["state"]).name) for delta in book["deltas"]] == [
        (Side.SELL, "PENDING_CANCEL"),
        (Side.SELL, "REJECTED"),
    ]
    assert not book["alive"]
```

An admitted message the fold still cannot read is an error, never a skipped
row, and the table keeps its previous snapshot: a new order stating no
`Side(54)`, with no order before it to take one from, is refused by path.

```python
import tempfile
from pathlib import Path

from rekep import Storages
from rekep.pipeline import (
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
)
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
storages = Storages.from_dict(
    {
        layer: {
            "name": layer,
            "properties": {
                "type": "sql",
                "uri": f"sqlite:///{root / layer}.db",
                "warehouse": str(root / layer),
            },
        }
        for layer in ("bronze", "silver", "gold")
    }
)
capture = root / "sideless.log"
capture.write_text(
    "2026-08-14 10:00:00.000 [250-e7256476:9effef3e6a:72504] [ULBridge] (INFO) "
    "Sending : 8=FIX.4.4|35=D|34=1|52=20260814-10:00:00|11=O1|55=AAPL|38=5|44=99|10=0|\n",
    encoding="utf-8",
)
day = window_of("2026-08-14", "2026-08-14")
with storages:
    parse_log_messages(capture.as_uri(), storages, day)
    parse_fix_messages_raw(storages, day)
    parse_fix_messages_refined(storages, day)
    try:
        parse_books(storages, day)
    except Exception as refusal:
        assert "$.operation.side: expected a bid or ask operation" in str(refusal)
    else:
        raise AssertionError("an order stating no side is refused")
```

The hour before `start` warms the books and none of its books is written:
an entry resting there is in the book at `start`. Silver holds, on every whole
hour, a view of each chain the walk left alive there
([`parse_fix_messages_refined`](parse-fix-messages-refined.md#the-hourly-grid)),
and the fold takes the views of one instant as the whole membership of their
book at it, adding no delta: so the book at `start` holds every chain alive
there that the walk which landed those views saw, older than `HISTORY` or
not. Beyond that a book starts with no depth before `start - HISTORY` -- the
fold is not a checkpoint reconstruction -- and a partial update whose missing
facts need an earlier order can be refused. A book the fills emptied names no
live chain a view could carry, so a window opening after it emptied does not
restate it empty, as a fold over the whole history does. Operation times must not
decrease. The native fold keeps live depth as it streams bounded batches, so
its memory grows with the depth outstanding.

Over the shipped capture, a window opening at 02:00 opens on the book the
report of 01:03 left, and lands, book for book, what the fold over the whole
morning lands from 02:00 -- as it would with no hour before it, since the
view of 02:00 carries that book:

```python
import datetime
import tempfile
from pathlib import Path

from rekep import Storages
from rekep.pipeline import (
    BOOKS,
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
)
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
storages = Storages.from_dict(
    {
        layer: {
            "name": layer,
            "properties": {
                "type": "sql",
                "uri": f"sqlite:///{root / layer}.db",
                "warehouse": str(root / layer),
            },
        }
        for layer in ("bronze", "silver", "gold")
    }
)
morning = window_of("2026-08-14T00:00:00Z", "2026-08-14T16:30:00Z")
opened = window_of("2026-08-14T02:00:00Z", "2026-08-14T16:30:00Z")


def books(table: str) -> list:
    dataset = storages.dataset(table)
    try:
        held = dataset.read_arrow_table().sort_by("currunix").to_pylist()
    finally:
        dataset.close()
    return [book for book in held if book["currunix"] >= opened[0]]


with storages:
    parse_log_messages("file:data/capture/ulbridge.log", storages, morning)
    parse_fix_messages_raw(storages, morning)
    parse_fix_messages_refined(storages, morning)
    parse_books(storages, morning)
    parse_books(storages, opened, target="silver.record_keeping.opened_books")
    later = books("silver.record_keeping.opened_books")
    assert later == books(BOOKS)
    first = later[0]
    assert first["currunix"] == first["snapunix"] == opened[0]
    assert (first["crosscode"], len(first["alive"])) == ("JOVM:XXXXXX", 1)
```

## The row

A book row is the fold's answer at an instant, per category: the event
columns every table opens with, `crosscode` the category, the market facts of
the operation that moved it -- `bidpx`, `bidqty`, `askpx` and `askqty` its
best tradable levels -- `alive`, every order and quote standing on either
side, the `deltas` applied since the book before, the `executions` it traded,
and `bidlimits` and `asklimits`, the price levels `alive` aggregates to, best
first, each saying whether it is `tradable`. [The table page](../tables/silver/books.md) lists
every column and [its samples](../samples/silver/books.md) show a book with
depth.

`snapshot_millis` is the fold's epoch-aligned grid in milliseconds,
`rekep.pipeline.SNAPSHOT_MILLIS` -- one hour -- unless stated, and zero for
none. At every grid instant between the first operation the fold reads and
the last instant it reaches, each book is answered again whole, `currunix`
and `snapunix` the grid instant: its `alive` and limits as they stand, and
no delta or execution a book before it answered, so a flattening task reads
every event once. The grid stops where the input does: nothing is restated
past the last operation or expiry the fold read. A silver row whose
`snapunix` is set is a view of a live chain, and the views of one instant
replace their book's membership there with no delta, so every order, quote
and execution a flattening task reads is an event, once.

The fold can emit an expiry past the last input, so the output is filtered
to the same strict window before it is written.

## Write

The write stages chunks of `commit_row_size` rows and replaces exactly
`[start, end)` in one Iceberg snapshot: rows outside the window survive, those
sharing its hour partition included, an empty rerun clears the window, and a
failure leaves the previous snapshot visible.

`snapshot_id` is the snapshot this call committed, found by the
`rekep.books-run-id` summary property (`BOOKS_RUN`) the write records with a
run identifier of its own -- never by the table's head, which a recovered
commit acknowledgement may refresh past. An empty window still commits a
snapshot, and `snapshot_id` names it. Hand it and the same window to
`parse_orders`, `parse_quotes` and `parse_executions`, so all three read one
book state even when another writer moves the table on.
