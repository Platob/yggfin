# Logs

`Console` renders for the person who just typed a command. Logging records
what the library did for whoever reads the run afterwards.

| level | what it carries |
| --- | --- |
| `INFO` | a completed operation — one record per public verb, whatever it commits inside |
| `DEBUG` | the detail under it — per stream and per file, never per batch or per row |

Nothing is configured at import. With no handler installed the standard
library carries WARNING and above to `stderr`, so a consumer that never asks
sees what it saw before.

```python
import logging

from rekep.logs import configure

configure("INFO")
print(logging.getLogger("rekep").level, logging.getLogger().level)
```

```text
20 30
```

Scoped to `rekep`, not the root logger — configuring the root would set the
level for every library the caller also imported.

## In a task

Every task document carries the level, and the notebook applies it before the
first record:

```yaml
# tasks/parse_messages/parse_messages.yml
parameters:
  log_level: INFO
```

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_messages/parse_messages.yml \
  --parameter log_level=DEBUG \
  --output parse_messages.executed.ipynb
```

Airflow runs the notebooks with `log_output=True`, so the same records reach
the task log with no DAG change.

## On the command line

```bash
rekep --log-level INFO fields load --target schemas/rekep/message.yaml
```

The CLI defaults to `WARNING` and a task run to `INFO`: a person at a terminal
is reading `Console`, and a second stream over it is noise. Records go to
`stderr` and carry no escape sequences, so a redirect still gets only the
payload.

## What one run says

Eleven rows through `parse_messages`, at `INFO`:

```text
INFO rekep.logs parse_messages reading capture=python/tests/data/app_messages_sample.txt
INFO rekep.iceberg.dataset logs.messages created at file://data/warehouse/logs/messages with 59 columns, partitioned by {'unixpartition': 'identity'}
INFO rekep.iceberg.dataset logs.messages wrote branch=main snapshot=2257423558696136046 in 97ms
INFO rekep.logs parse_messages finished: 11 read, 11 written, 0 skipped → messages=logs.messages in 0.9s
```

Four records, because four things finished. The stage brackets the run, and
the numbers in its closing record are the numbers in the dict it returns —
the same rule maintenance already follows.

A six-stage run reads top to bottom:

```text
INFO rekep.logs parse_fix reading messages=logs.messages
INFO rekep.logs parse_fix routed 2 market, 8 misc
INFO rekep.logs parse_fix resolved unix from SendingTime 1, TransactTime 1, recorded 8; 5 of 10 rows carry a symbolticker
INFO rekep.logs parse_fix finished: 10 read, 10 written, 0 skipped → market=fix.market, misc=fix.misc in 7.2s
INFO rekep.logs parse_instruments reading market=fix.market
INFO rekep.logs parse_instruments observed 1 instruments, of which 1 are new versions
INFO rekep.logs parse_instruments finished: 1 read, 1 written, 0 skipped → instruments=market.instruments in 3.2s
```

`rekep task run` writes these to `stderr` as the notebook produces them, and
the returned dict to `stdout` — so a run is readable and a pipe into `jq`
still gets only the payload.

The same run at `DEBUG` adds the cast and the staged file:

```text
DEBUG rekep.fields.field casting a stream onto Message: 56 columns
DEBUG rekep.iceberg.dataset staged 11 rows to file://data/warehouse/logs/messages/data/unixpartition=1786658400/…
```

A write that commits forty chunks is still one `INFO`. The per-chunk work is
`DEBUG`, which is the whole point of the split.

## Where each record comes from

Loggers are named for the module that emits them, so a record's origin is
also the grep target:

| logger | INFO | DEBUG |
| --- | --- | --- |
| `rekep.logs` | a task opening, what it alone knows, and the result it closes with | — |
| `rekep.iceberg.dataset` | table created, columns added, write finished, compaction and cleanup settled | scan planned, columns projected, file staged |
| `rekep.fields.field` | — | a stream cast onto a shape |
| `rekep.text.text_files` | — | each log opened, and the rows it yielded |

Filter or silence the package by its parent name:

```python
import logging

logging.getLogger("rekep.text").setLevel(logging.WARNING)
```

A durable audit trail already has a home in the table itself — Iceberg
snapshot summaries, which every write verb accepts through `properties=`.
The log is the transient operational record; use the snapshot summary for
what has to survive the run.

## What one task returns

Every task returns the same keys, built by `rekep.logs.Stage`, so a scrap read
out of context says which task it came from and a reader learns one shape:

| key | what it is |
| --- | --- |
| `task` | the task document's own name |
| `read` | rows drawn from the source, or products folded |
| `written` | rows committed |
| `skipped` | read and not written |
| `sources` | what was read, keyed by the role this stage calls it |
| `targets` | what was written, keyed the same way; a role a run did not use is absent |
| `window` | the half-open `[start, end)` interval, in nanoseconds since the epoch |
| `elapsed_ms` | how long the task took |

Whatever else a task knows keeps its own name beside them: `parse_fix` adds
`routed`, `unixsource` and `tickered`; `parse_market` adds `mode`, `products`,
`flatten`, `checkpoint` and `scan`; `optimize_iceberg` adds `tables`,
`expired`, `deleted`, `byte_size` and `reports`.

```python
from rekep.logs import Stage

stage = Stage("flatten_orders", sources={"books": "market.books"})
stage.targets["orders"] = "market.orders"
print(sorted(stage.finished(read=2, written=2)))
```

```text
['elapsed_ms', 'read', 'skipped', 'sources', 'targets', 'task', 'window', 'written']
```
