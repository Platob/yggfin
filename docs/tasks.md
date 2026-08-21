# Tasks

A task is a **unit of work declared in a document** rather than written as a
script. The same `from_yaml` that reads a [schema contract](contracts.md) reads
one, and running it is one call:

```python
from rekep import Task

Task.from_yaml("tasks/parse_logs/parse_logs.yml").run()
```

The document says which task it is with a `kind`, and `from_dict` dispatches on
it the way `from_`/`into_` dispatch everywhere else in this package. That is
what lets a scheduler read a directory of documents it has never seen the
classes for.

## Parsing a capture into one table per event

Two tasks are shipped here, and together they are the pipeline a capture
actually needs. `ParseLogs` sorts a capture into a table per kind of line,
keeping the line; [`ParseMarket`](#reading-market-logs) reads those lines as FIX
and lands what they *mean*.

`ParseLogs` first: read a folder of trading logs, decide what each line is
about, and land each line in the Iceberg table for its kind — `order_logs`,
`execution_logs`, … `unknown_logs`.

=== "From a document"

    ```python
    from rekep import Task

    report = Task.from_yaml("tasks/parse_logs/parse_logs.yml").run()
    print(report)
    # parse-trading-logs: 600 read, 600 written in 1.06s -- logs.order_logs=100, ...
    ```

    `tasks/parse_logs/parse_logs.yml` in this repository is a commented example of every
    field, and `tasks/parse_logs/parse_logs.ipynb` is a notebook that builds a small
    capture and runs it end to end.

=== "In Python"

    ```python
    from rekep import ParseLogs

    task = ParseLogs(
        source="s3://captures/2026-08-14",
        pattern="*.log*",
        timezone="Europe/Paris",
        catalog="rekep",
        namespace="logs",
        properties={"type": "rest", "uri": "https://catalog.internal"},
        static_values={"bridge": "bridge-1"},
    )
    report = task.run()
    ```

=== "The report"

    ```python
    report.rows        # read from the capture
    report.written     # {'logs.order_logs': 100, ...}
    report.landed      # written across every target
    report.skipped     # rows a target already held
    report.seconds
    ```

    Returned rather than printed, like every maintenance verb here: a caller
    that wants to log it can, one that wants to assert on it can, and a
    notebook can render it.

## One pass, fanning out

Every batch the parser yields is cut by `etype` and each part appended to its
own table. **Not one pass per kind**, which would reread and reparse the whole
capture once for every kind of line in it, and not a staging table either,
which would write every row twice.

```text
                    ┌──────────────► logs.order_logs
                    │
   capture ──parse──┼──────────────► logs.execution_logs
   (one read)       │
                    ├──────────────► logs.book_side_logs
                    │
                    └──────────────► logs.unknown_logs
```

**Streaming, with the memory that costs stated.** The parser is a reader and
stays one; what a fan-out has to add is a buffer per target, because the rows
for one table arrive interleaved with every other table's. Each buffer flushes
at `commit_row_size`, so the job holds at most one commit's worth per target
rather than the capture — about `targets × commit_row_size` rows.

Splitting turns out to be [cheaper than writing one table](#benchmarks), which
is not obvious: an append's merge anti-joins each chunk against what its own
target already holds, so N targets each hold a fraction of the rows and each
anti-join is against a fraction of the keys.

## Appending, not writing

`merge_by` here is an **append's** merge, which is the cheap half of an upsert:

- a row whose key a target already holds is dropped,
- the rest are inserted,
- nothing stored is ever rewritten, and no delete file is produced.

That is what makes re-running the job over a capture that grew by a day cost
the day. A replay over one that did not grow writes nothing at all.

```python
first  = task.run()   # 600 read, 600 written
again  = task.run()   # 600 read, 0 written, 600 already stored
```

The key is `(unix, hash)` — the instant the line is stamped with, and the
digest of the raw line. Two captures of the same line deduplicate; two
different lines in the same microsecond, from the same thread, both land.

Set `merge_by: false` to append everything, duplicates included. That is a
different job, and it says so.

## What lands where

`table` is a pattern: `{event_type}` is the lower-cased `EventType` name.

| a line like | `etype` | lands in |
| --- | --- | --- |
| `35=8\|` or `ExecutionReport` | `EXECUTION` | `logs.execution_logs` |
| `35=D\|` or `NewOrderSingle` | `ORDER` | `logs.order_logs` |
| `35=X\|` or `MarketDataIncrementalRefresh` | `BOOK_SIDE` | `logs.book_side_logs` |
| `35=W\|` or `MarketDataSnapshot` | `BOOK` | `logs.book_logs` |
| `heartbeat` | `UNKNOWN` | `logs.unknown_logs` |

Each is created on the first write to it, from the parser's own shape —
[`Log`](logs.md) plus whatever `static_values` adds — and partitioned by
`hunix`, the hour the line happened in.

**A line nothing classifies still lands.** Dropping it would make the job lossy
in exactly the case a new log format shows up.

## Reading market logs

`ParseMarket` is the second half. It reads FIX messages and lands the events
they carry: an orders table, an executions table, and a book table folded from
both.

=== "From a document"

    ```python
    from rekep import Task

    report = Task.from_yaml("tasks/parse_market/parse_market.yml").run()
    print(report)
    # parse-market-logs: 5 read, 16 written in 0.95s -- market.books=8, market.orders=7, ...
    ```

    `tasks/parse_market/parse_market.yml` is a commented example of every field,
    and `tasks/parse_market/parse_market.ipynb` builds a small capture and runs
    it end to end.

=== "In Python"

    ```python
    from rekep import ParseMarket

    ParseMarket(
        source="s3://captures/2026-08-21",
        venue="XCME",
        catalog="rekep",
        namespace="market",
        properties={"type": "rest", "uri": "https://catalog/api"},
    ).run()
    ```

Three tables come out, each created from the declaration of the shape it holds
— which is the same declaration published under `schemas/rekep/`:

| table | shape | holds |
| --- | --- | --- |
| `market.orders` | [`Order`](market.md) | what somebody asked for, version by version |
| `market.executions` | `Execution` | what actually moved |
| `market.books` | `Book` | the book after each instant that changed it |

**The source is a `Dataset`, whichever kind.** Point it at a folder and it is
read as text; point it at a *document naming a store* and it is read from
there — which is how this chains onto `ParseLogs` without re-reading the
capture:

```yaml
source:
  kind: iceberg
  name: logs.book_side_logs
  catalog: rekep
  properties: {type: sql, uri: "sqlite:///data/catalog.db", warehouse: "file://data/warehouse"}
```

`kind` names the store and `Dataset.from_dict` finds the class for it — the
same dispatch a task's own `kind` gets, and the mechanism that keeps the
Iceberg dependency optional: the module is imported by a document that asks for
it, and by nothing else.

**Books are folded last, and per instrument.** `Book.from_events` needs one
instrument's events in time order, so the orders and executions are grouped by
`instrument_hash` and each group folded on its own. That is the one pass that
is not streaming the way the other two are: a fold has to see a whole
instrument's stream, and what it costs is the live orders of every instrument
in the capture rather than the capture itself. Set `books: false` for a job
landing raw events for something else to fold.

!!! warning "A schema is handed over, never inferred"

    `into_dict` leaves out a member that is None, and `RecordBatch.from_pylist`
    with no schema builds one from the **first row's keys**. A first book with
    no bid — which is what the first book of a capture usually is — therefore
    defined a schema with no `bid_px`, `spread`, `micro_px` or `imbalance` in
    it, and every row after it was cast onto that and came back null. Silently,
    and for the whole table. With the declaration handed over, a missing key is
    a null in *that row* and nothing else moves.

## Writing your own

Subclass `Task`, declare a `KIND`, and say what `run` does. A kind is reachable
from a document by existing, not by registering:

```python
import dataclasses
from rekep import Task, TaskRun

@dataclasses.dataclass
class Compact(Task):
    """Compact every table in a namespace."""

    KIND = "compact"

    namespace: str = ""

    def run(self) -> TaskRun:
        started = self._timed()
        ...
        return TaskRun(task=self._named(), seconds=self._timed() - started)

Task.from_dict({"kind": "compact", "namespace": "logs"})   # a Compact
```

A document that names no kind, or one nothing declares, is refused by name —
and a subclass refuses a document for a different task rather than quietly
building the wrong one out of the right fields.

## Benchmarks

`benchmarks/bench_tasks.py` is the sweep behind every number here. Each case
asserts what landed before it is timed, because a benchmark that measures the
wrong answer measures nothing. The method the whole site shares is on the
[Benchmarks](benchmarks.md) page.

```bash
cd python
uv run python benchmarks/bench_tasks.py            # 200,000 rows over 6 kinds
uv run python benchmarks/bench_tasks.py --quick    # 20,000
```

A write is not idempotent in cost, so these are single runs rather than
best-of; the figures below were measured twice and the range is quoted.

**Parsing and appending**, 200,000 rows over six kinds, into a local SQLite
catalog:

| case | measured |
| --- | --- |
| first run: parse, fan out, write | 2.16–2.71 s — 74–93k rows/s |
| replay: everything already stored | 1.44–1.58 s — 126–139k rows/s, **1.4–1.9×** cheaper |
| a capture that gained a day (400k rows read) | 3.06–3.09 s — 130k rows/s |
| `merge_by=False`, same rows | 2.51–2.78 s — 144–159k rows/s |

So the merge costs roughly what it saves on the first run and everything on
every run after it: a replay is the read and nothing else.

**Fanning out**, the same 200,000 rows:

| case | measured |
| --- | --- |
| one pass → 6 tables | 1.33–1.54 s — 130–150k rows/s |
| one pass → 1 table | 2.77–3.10 s — 64–72k rows/s |

Splitting is **2.0–2.1× cheaper** than not splitting, for the reason above: the
merge's anti-join is per target.

**The commit size**, which is what bounds the job's memory:

| `commit_row_size` | commits per target | measured |
| --- | --- | --- |
| 10,000 | ~20 | 4.88–5.25 s |
| 100,000 | ~2 | 1.42–1.45 s |
| 1,000,000 | 1 | 1.49–1.51 s |

Committing twenty times instead of twice costs **3.4×**, and past the point
where the whole capture fits in one commit there is nothing further to buy. The
result is identical at every size — the buffering is a memory bound, not a
filter, and the benchmark asserts it.

**The second half**, `ParseMarket` over a market-data capture whose every
message carries three entries:

| case | measured |
| --- | --- |
| events only: parse, translate, write | ~2.1k messages/s |
| and folded into books | ~1.1k messages/s, **~0.5×** |

So the fold roughly doubles the job, which is what a pass that has to see a
whole instrument's stream costs against two that stream. The per-event cost of
the translation itself is on the [market](market.md#benchmarks) page; what this
adds is the write and the fold around it.
