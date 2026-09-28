# Tasks

`rekep.pipeline` holds one function per table the graph writes, in
production order. Each is `(storages, window, *, ...) -> Landed`: it opens its
source and its target through the [`Storages`](../storages/index.md) it is
handed, reads one window of its source, writes its target the way that table
is replaced, creates the target where it is missing, and answers what it read
and wrote. `parse_log_messages` takes the capture it reads first, and given
no window lands every line and answers the whole hours they span.

| task | reads | writes | replaces |
| --- | --- | --- | --- |
| [`parse_log_messages`](parse-log-messages.md) | a capture `IOBase` or URI | `bronze.record_keeping.log_messages` | on `curruuid` within its hour |
| [`parse_fix_messages_raw`](parse-fix-messages-raw.md) | `bronze.record_keeping.log_messages` | `bronze.record_keeping.fix_messages` | on `curruuid` within its hour |
| [`parse_fix_messages_refined`](parse-fix-messages-refined.md) | `bronze.record_keeping.fix_messages`, from `HISTORY` before the window | `silver.record_keeping.fix_messages` | on `curruuid` within its hour |
| [`parse_books`](parse-books.md) | `silver.record_keeping.fix_messages`, from `HISTORY` before the window | `silver.record_keeping.books` | the exact window, in one snapshot |
| [`parse_orders`](parse-orders-quotes-executions.md) | one `silver.record_keeping.books` snapshot | `silver.record_keeping.orders` | the exact window, in one snapshot |
| [`parse_quotes`](parse-orders-quotes-executions.md) | one `silver.record_keeping.books` snapshot | `silver.record_keeping.quotes` | the exact window, in one snapshot |
| [`parse_executions`](parse-orders-quotes-executions.md) | one `silver.record_keeping.books` snapshot | `silver.record_keeping.executions` | the exact window, in one snapshot |

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
| `written` | target rows the task carried into the table |
| `skipped` | rows the task answered that the target's key folded into a written one |
| `snapshot_id` | the `books` snapshot `parse_books` committed, or the one a flattening task read; None for the other tasks |
| `window` | the whole-hour window `parse_log_messages` inferred from the lines it landed when given none, for the tasks after it; None otherwise |

`written` is what a run carried, not what it changed: a run over a window
already landed answers the same numbers and leaves each table holding each
row once. A task that wrote no row succeeded. [Data samples](../samples/index.md#what-each-task-answered)
lists what every task answers over the shipped capture.

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
is published. Its refusals:

| refusal | meaning |
| --- | --- |
| `FileNotFoundError: <uri>` | `parse_log_messages` was handed a capture that does not exist, which would otherwise read as a window without lines |
| `window [...) is empty` / `end=... is not an instant` | bad window bounds |
| `row header captures nothing for ...` | a `rowheader` that renames or drops a capture |
| `FIX registry contains no specification fields` | a codec over an empty dictionary |
| `FixCodec.__new__() got an unexpected keyword argument` | a codec pin the native codec does not declare |
| `ArrowInvalid ... expected a bid or ask operation` | `parse_books` met an admitted message the book fold cannot read, such as one stating no `Side(54)` whose order has no live side to lend it one; it is an error, never a skipped row |
| `expected a nonnegative book snapshot_id or None` | a flattening task handed a bad snapshot |
| `... has no snapshot N: table is missing` | a flattening task pinned to a snapshot of a books table that does not exist; nothing was written |
| `unable to open database file` | a SQLite catalog whose folder does not exist |
