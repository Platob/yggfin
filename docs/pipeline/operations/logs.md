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
uv run --project python --with papermill --with ipykernel rekep task run \
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
INFO rekep.iceberg.dataset logs.messages created at file://data/warehouse/logs/messages with 56 columns, partitioned by {'unixpartition': 'identity'}
INFO rekep.iceberg.dataset logs.messages wrote branch=main snapshot=2257423558696136046 in 97ms
```

Two records, because two things finished. The same run at `DEBUG` adds the
cast and the staged file:

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
