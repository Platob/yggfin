# Market server

The market server shows books as a web page and a JSON API: for each ticker,
the candles its books fold into, the book standing at an instant, and every
entry, delta and execution of a range on the bid and on the ask. It reads the
book fold's native row, keyed by `ticker`, from a record file -- an Arrow
IPC stream among them -- or an Iceberg table folder.

What it serves on this page is a ticker fold of the silver events, not the
`silver.record_keeping.books` table. That table cannot be served as it
stands: each of its books is one `MIC:CFI` category and states no ticker, so
the server lists none of them and no query can name one, and it stores its
unsigned 64-bit codes as signed bit views, which the server reads back by
value and refuses with a `500`. A consumer builds the display's feed
instead: it runs the stages `parse_books` runs, over the same silver rows and
window, but for `categorized_symbol_reader`, so the fold books each message
under the `symbol` it states, and writes the rows the fold answers to one
file. From the repository root, land the capture as [Start here](../index.md)
does and fold its silver events into `root / "books.arrows"`:

```python
import tempfile
from collections import Counter
from pathlib import Path

import pyarrow as pa

from rekep import Storages
from rekep.fix import SORT_COLUMNS, UNDATED, FixCodec, fix_message_field
from rekep.market import book_arrow_reader, market_window_filter, market_window_reader
from rekep.pipeline import (
    BOOKS,
    FIX_MESSAGES,
    HISTORY,
    SNAPSHOT_MILLIS,
    Landed,
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
    capture = "file:data/capture/ulbridge.log"
    assert parse_log_messages(capture, storages, window) == Landed(read=129, written=129)
    assert parse_fix_messages_raw(storages, window) == Landed(read=129, written=126)
    assert parse_fix_messages_refined(storages, window) == Landed(read=126, written=49)
    assert parse_books(storages, window).written == 30
    stored = storages.dataset(BOOKS)
    try:
        categories = stored.read_arrow_reader(columns=("crosscode", "ticker")).read_all()
    finally:
        stored.close()

    # The stages parse_books runs, but for the one that clears `symbol`.
    codec = FixCodec.from_env(default_sending_time=UNDATED)
    fixed = fix_message_field(codec)
    events = storages.dataset(FIX_MESSAGES, field=fixed)
    try:
        scanned = events.read_arrow_reader(
            fixed,
            row_filter=market_window_filter((window[0] - HISTORY, window[1])),
            order_by=SORT_COLUMNS,
        )
        folded = book_arrow_reader(codec, scanned, snapshot_millis=SNAPSHOT_MILLIS)
        books = market_window_reader(folded, window)
        with (
            pa.OSFile(str(root / "books.arrows"), "wb") as sink,
            pa.ipc.new_stream(sink, books.schema) as writer,
        ):
            for batch in books:
                writer.write_batch(batch)
    finally:
        events.close()

# The books table: one book per MIC:CFI category, none stating a ticker.
assert set(categories.column("crosscode").to_pylist()) == {
    "JOVM:XXXXXX",
    "XSWX:ESVTFR",
    "XXXX:XXXXXX",
}
assert categories.column("ticker").null_count == 30

# The feed: the same silver rows folded into one book per ticker.
with pa.OSFile(str(root / "books.arrows")) as source:
    served = pa.ipc.open_stream(source).read_all()
assert Counter(served.column("ticker").to_pylist()) == {
    "1605": 16,
    "ABBN.S": 6,
    "EXAMPLECO.S": 5,
    "HOLN": 6,
    "XAU/USD": 3,
}
assert served.column("snapunix").null_count == 7
```

The window's 49 silver rows fold into 36 books over five tickers: 7 an
event moved, and 29 restated on the whole hours after, the `SNAPSHOT_MILLIS`
grid, `snapunix` set. A message stating no `symbol` would still be booked
under its category, with no ticker, and the server lists no such book. The
file holds the rows as the fold answers them, not as a table stores them --
codes unsigned, clocks in nanoseconds -- which is the shape the server
reads.

Then serve the file under the name `books`:

```bash
# Every option and route: https://platob.github.io/yggdryl/graph/serve/
yggdryl market serve books=<root>/books.arrows --bind 127.0.0.1:8080
```

The first line the server prints is its endpoint, `http://127.0.0.1:8080/`,
where the page is; `--bind 127.0.0.1:0` takes a free port and prints the one
it took. The server reads the file at every request, so an export rerun after
`parse_fix_messages_refined` lands a window shows on the next request, with
no restart.

## What the page shows

The page is the endpoint's `index.html`, and its view -- table, ticker, range,
zone, interval and selected bucket -- is the URL's hash, so a view is a link.
Both views below are HOLN over the afternoon, hourly on the Zurich clock, with
the 14:00 bucket selected:

```text
http://127.0.0.1:8080/index.html#table=books&ticker=HOLN&from=2026-08-14T12:00:00Z&to=2026-08-14T17:00:00Z&tz=Europe%2FZurich&interval=1h&at=2026-08-14T12:00:00Z
```

![HOLN's hourly bid candles on the Zurich clock, the 14:00 bucket selected](../assets/market-server/chart-light.png#only-light)
![HOLN's hourly bid candles on the Zurich clock, the 14:00 bucket selected](../assets/market-server/chart-dark.png#only-dark)

The header selects the table, the ticker -- each labelled with its number of
books -- the range as a wall clock in the zone, the zone and the candle
interval. The chart draws each bucket's bid candle on its left and its ask
candle on its right, the mid close as a line and the spread as a band
beneath; hovering or the arrow keys read a bucket, and a click or Enter
selects it. HOLN's bid from 14:46 on is one buy order resting at 72.3 for 50,
and nothing was offered, so each hour is a flat bid candle with no mid or
spread.

![HOLN's book before 15:00 and the bid and ask events of the 14:00 bucket](../assets/market-server/point-light.png#only-light)
![HOLN's book before 15:00 and the bid and ask events of the 14:00 bucket](../assets/market-server/point-dark.png#only-dark)

Point is the last book before the selected bucket ends: its best bid and ask,
spread, mid and imbalance, the counts of its alive entries, deltas and
executions, and each side's top limits. Audit lists the bucket's bid and ask
events -- here an order's fill of 300 at 72.28, as its delta and its
execution, then the order resting at 72.3, as its alive entry and its delta
-- and downloads the whole range as CSV, gzip or zstd.

## Routes

Every route is a `GET` under the endpoint's `api/`. `table` is the name the
command gave, `books`; `from`, `to` and `at` are ISO 8601 instants, one
stating an offset or `Z` that instant and a naive one a wall clock in `tz`,
`UTC` unless stated, and `to` is exclusive. A refusal is JSON
`{"error": "..."}`: `400` for a parameter, `404` for a table, a ticker or a
book there is none of, `500` otherwise.

### /api/tables

The tables served, each with the location it reads.

```bash
curl -s http://127.0.0.1:8080/api/tables
```

### /api/tickers

Every ticker a table holds, with its number of books and the whole seconds
they stand in.

```bash
curl -s 'http://127.0.0.1:8080/api/tickers?table=books'
```

### /api/candles

A ticker's books in `[from, to)`, folded into candles of `interval` aligned
to `tz`: the bid, the ask, the mid and the spread, each an open, high, low and
close, beside each bucket's books, executions and volume.

```bash
curl -s 'http://127.0.0.1:8080/api/candles?table=books&ticker=HOLN&from=2026-08-14T14:00:00&to=2026-08-14T19:00:00&tz=Europe/Zurich&interval=1h'
```

### /api/book

The last book at or before `at`: its best bid and ask, spread, mid,
imbalance, the counts of its entries, and each side's limits, best first.

```bash
curl -s 'http://127.0.0.1:8080/api/book?table=books&ticker=HOLN&at=2026-08-14T15:00:00&tz=Europe/Zurich'
```

### /api/events

One row per alive entry, delta and execution of every book in `[from, to)`,
at most `limit` of them, `truncated` saying whether more were held back;
`side=bid` or `side=ask` keeps one side.

```bash
curl -s 'http://127.0.0.1:8080/api/events?table=books&ticker=HOLN&from=2026-08-14T14:00:00&to=2026-08-14T15:00:00&tz=Europe/Zurich&side=bid'
```

### /api/audit.csv.gz

The same rows with no bound, as a CSV attachment: `audit.csv` plain,
`audit.csv.gz` gzip-coded, `audit.csv.zst` zstd-coded.

```bash
curl -s -o audit.csv.gz 'http://127.0.0.1:8080/api/audit.csv.gz?table=books&ticker=HOLN&from=2026-08-14T00:00:00&to=2026-08-15T00:00:00&tz=Europe/Zurich'
```

## Read-only

Serve without `--capture`: it walks a capture itself, beside the tasks, and
appends the books it folds to the first table -- here the feed, which is
exported from silver again and never appended to. The tasks remain the one
writer of the tables a feed is folded from.
