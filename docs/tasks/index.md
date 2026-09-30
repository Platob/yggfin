# Tasks

`rekep.pipeline` holds one function per table the graph writes, in
production order. Each is `(storages, window, *, ...) -> Landed`: it opens its
source and its target through the [`Storages`](../storages/index.md) it is
handed, reads one window of its source, writes its target the way that table
is written, creates the target where it is missing, and answers what it read
and wrote. `parse_log_messages` takes the capture it reads first, and given
no window lands every line and answers the whole hours they span.

| task | reads | writes | how |
| --- | --- | --- | --- |
| [`parse_log_messages`](parse-log-messages.md) | a capture `IOBase` or URI | `bronze.record_keeping.log_messages` | merges on `curruuid` within its hour |
| [`parse_fix_messages_raw`](parse-fix-messages-raw.md) | `bronze.record_keeping.log_messages` | `bronze.record_keeping.fix_messages` | merges on `curruuid` within its hour |
| [`parse_fix_messages_refined`](parse-fix-messages-refined.md) | `bronze.record_keeping.fix_messages`, from `HISTORY` before the window | `silver.record_keeping.fix_messages` | merges on `curruuid` within its hour |
| [`parse_books`](parse-books.md) | `silver.record_keeping.fix_messages`, from `HISTORY` before the window | `silver.record_keeping.books` | replaces the exact window, in one snapshot |
| [`parse_orders`](parse-orders-quotes-executions.md) | one `silver.record_keeping.books` snapshot | `silver.record_keeping.orders` | replaces the exact window, in one snapshot |
| [`parse_quotes`](parse-orders-quotes-executions.md) | one `silver.record_keeping.books` snapshot | `silver.record_keeping.quotes` | replaces the exact window, in one snapshot |
| [`parse_executions`](parse-orders-quotes-executions.md) | one `silver.record_keeping.books` snapshot | `silver.record_keeping.executions` | replaces the exact window, in one snapshot |

The module names every table once -- `LOG_MESSAGES`, `FIX_MESSAGES_RAW`,
`FIX_MESSAGES`, `BOOKS`, `ORDERS`, `QUOTES`, `EXECUTIONS` -- and each task's
`source` and `target` keywords default to them, so a run into other tables
names them there. `EVENTS` maps each event kind to its table and `FLATTENERS`
to its task.

## The window

A window is `[start, end)` over `currunix`, as `rekep.times.window_of`
answers it: two aware UTC instants, `start` included and `end` excluded. A
bound is a `datetime`, an ISO instant (`2026-08-14T10:00:00Z`), a date
(`2026-08-14`), an `int` of epoch nanoseconds, or a named instant -- `now`,
`today`, `yesterday`, `tomorrow`, `epoch`. A date as `end` means the end of
that day, and a window given neither bound is the last day up to now.

```python
from rekep.times import window_of

day = window_of("2026-08-14", "2026-08-14")
assert [bound.isoformat() for bound in day] == [
    "2026-08-14T00:00:00+00:00",
    "2026-08-15T00:00:00+00:00",
]
try:
    window_of("2026-08-15", "2026-08-14")
except ValueError as refusal:
    assert "is empty" in str(refusal)
```

Every table is laid out by the hour of `currunix`, and every scan hands the
window to Iceberg as a predicate on it, so a task plans only the hour
partitions its window covers. What `currunix` is differs by table: on
`log_messages` the instant a line was printed at, read in the zone its bridge
prints in; on a FIX row the instant a message states; on a book the instant
it was folded at. [DAGs](../dags/index.md#late-events) says what that means
when windows are scheduled one after another.

## What a task answers

Every task returns `Landed`:

| field | meaning |
| --- | --- |
| `read` | source rows the window selected |
| `written` | target rows the task wrote: those a keyed task inserted or replaced, those a book or event task replaced its window with |
| `skipped` | rows the task answered that the target's key folded into another: a row the table already held as it is, or one the stream answered twice under one identity |
| `snapshot_id` | the `books` snapshot `parse_books` committed, or the one a flattening task read; None for the other tasks |
| `window` | the whole-hour window `parse_log_messages` inferred from the lines it landed when given none, for the tasks after it; None otherwise |

A run over a window already landed leaves each table holding each row once.
The three keyed tasks merge the differences into their table: a row it lacks
is inserted, one it holds with other values replaced, and one it holds as it
is left alone, so a replay answers `written=0`, counts every row in `skipped`
and commits no snapshot. The book and event tasks answer the same `written`
again, in one new snapshot each. A task that wrote no row succeeded.
[Data samples](../samples/index.md#what-each-task-answered) lists what every
task answers over the shipped capture.

## Commits

Every task takes `commit_row_size`, `rekep.pipeline.COMMIT_ROW_SIZE` (131,072
rows) unless stated, and its commits are cut by rows alone, never by how many
batches its producer handed over. Every task writes its rows in its table's
sort order, spilling them locally first, so its commits start once its
source is read. A keyed task commits once per `commit_row_size` rows it
answers, holding only the rows among them its table lacks or holds with other
values -- none, and no commit, where it holds them all as they are -- so a
task whose write fails after a commit keeps it, and its rerun writes the
rest. A book or event task stages its window `commit_row_size` rows at a time
and replaces it in one commit whatever it holds.
[Stream writes](../storages/iceberg.md#stream-writes) is how either write
reads and commits, and [what a commit holds](../storages/iceberg.md#what-a-commit-holds)
where it spills.

## The FIX codec

`parse_fix_messages_raw`, `parse_fix_messages_refined` and `parse_books` read
FIX, and take the `codec` they read it with: `FixCodec.from_env()` over the
process registry, pinned so a message stating no clock at all takes
`rekep.fix.UNDATED`, when None. Hand all three the same codec, because each
task after the parse reads a stored row back as the message the same
dictionary wrote. [FIX registry](../fix/index.md) says how to build one over
another dictionary.

## Logging

Tasks log to the `rekep.*` loggers and configure nothing:
`logging.getLogger("rekep").setLevel(logging.INFO)` under
`logging.basicConfig()` shows each table created and each commit, and `DEBUG`
adds the scans, projections and files.

## When a task fails

A failed task leaves the table's previous snapshot visible; nothing it staged
for the commit it failed in is published, and a keyed task's earlier
[commits](#commits) stand. Its refusals:

| refusal | meaning |
| --- | --- |
| `FileNotFoundError: <uri>` | `parse_log_messages` was handed a capture that does not exist, which would otherwise read as a window without lines |
| `window [...) is empty` / `end=... is not an instant` | bad window bounds |
| `row header captures nothing for ...` | a `rowheader` that renames or drops a capture |
| `FIX registry contains no specification fields` | a codec over an empty dictionary |
| `FixCodec.__new__() got an unexpected keyword argument` | a codec pin the native codec does not declare |
| `expected a nonnegative book snapshot_id or None` | a flattening task handed a bad snapshot |
| `... has no snapshot N: table is missing` | a flattening task pinned to a snapshot of a books table that does not exist; nothing was written |
| `unable to open database file` | a SQLite catalog whose folder does not exist |

Nothing a message states fails a task. What the parse, the walk or the book
fold cannot read -- a value its field's type refuses, a trade side stating no
`Side(54)`, an order with no side to stand on -- takes its default or is left
out, and each kind is logged as a `WARNING` through `logging`, once and then
at every tenfold count.
