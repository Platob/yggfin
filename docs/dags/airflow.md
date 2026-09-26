# Airflow

An Airflow DAG is the graph with one task per `rekep.pipeline` task, each
run over a window read off the DAG run's data interval. Every task opens the
three catalogs itself, because Airflow may run each in a process of its own,
and returns what it landed, which Airflow keeps as the task's XCom: the book
snapshot `parse_books` committed reaches the three flatteners that way.

```python
"""rekep's record-keeping graph: bronze per day, silver one day behind it."""

import dataclasses
import datetime

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from rekep import Storages
from rekep.pipeline import (
    FLATTENERS,
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
)
from rekep.times import window_of

#: Where the bridge's captures land, read recursively.
CAPTURE = "s3://market-capture/ulbridge?region=eu-west-1"

#: How far silver trails bronze: long enough for every line that can date an
#: event into a window to be in bronze before the window is walked.
SETTLE = datetime.timedelta(days=1)


def storages() -> Storages:
    """The three catalogs, from the Variable holding one mapping per layer."""
    return Storages.from_dict(Variable.get("rekep_storages", deserialize_json=True))


def bronze_window(start, end):
    return window_of(start, end)


def silver_window(start, end):
    return window_of(start - SETTLE, end - SETTLE)


@dag(
    dag_id="rekep_record_keeping",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": pendulum.duration(minutes=5)},
    tags=["rekep"],
)
def rekep_record_keeping():
    @task
    def log_messages(data_interval_start=None, data_interval_end=None) -> dict:
        with storages() as held:
            window = bronze_window(data_interval_start, data_interval_end)
            return dataclasses.asdict(parse_log_messages(CAPTURE, held, window))

    @task
    def fix_messages_raw(data_interval_start=None, data_interval_end=None) -> dict:
        with storages() as held:
            window = bronze_window(data_interval_start, data_interval_end)
            return dataclasses.asdict(parse_fix_messages_raw(held, window))

    @task
    def fix_messages_refined(data_interval_start=None, data_interval_end=None) -> dict:
        with storages() as held:
            window = silver_window(data_interval_start, data_interval_end)
            return dataclasses.asdict(parse_fix_messages_refined(held, window))

    @task
    def books(data_interval_start=None, data_interval_end=None) -> int:
        with storages() as held:
            window = silver_window(data_interval_start, data_interval_end)
            return parse_books(held, window).snapshot_id

    @task
    def flatten(kind: str, snapshot_id: int, data_interval_start=None, data_interval_end=None):
        with storages() as held:
            window = silver_window(data_interval_start, data_interval_end)
            landed = FLATTENERS[kind](held, window, snapshot_id=snapshot_id)
            return dataclasses.asdict(landed)

    snapshot = books()
    log_messages() >> fix_messages_raw() >> fix_messages_refined() >> snapshot
    for kind in FLATTENERS:
        flatten.override(task_id=f"parse_{kind}")(kind, snapshot)


rekep_record_keeping()
```

The `rekep_storages` Variable holds the JSON mapping
[Three catalogs](../storages/index.md) spells, one entry per layer.

## The edges

```text
log_messages >> fix_messages_raw >> fix_messages_refined >> books
books >> parse_orders
books >> parse_quotes
books >> parse_executions
```

The flatteners depend on `books` through its return value, the snapshot id,
so they start together once it commits and run in parallel slots: they read
one snapshot and write three tables. Handing each the id `parse_books`
answered, never letting it pin the head itself, keeps the three on one book
state however the table moves between them.

## Data intervals and windows

Airflow names each run's interval `[data_interval_start, data_interval_end)`,
aware UTC instants, which `window_of` reads as they are. The bronze tasks run
over that interval. The silver tasks run over the interval `SETTLE` before
it: a line may be printed hours after the event it carries -- the shipped
capture's bridge prints its local time, two hours ahead of UTC -- so a window
is walked once bronze holds every line that can date an event into it.
[Late events](index.md#late-events) says why, and what a window must hold. A
DAG that trails by a whole interval has nothing to walk on its first run;
`catchup` walks each day as the next one lands.

## Retries, reruns and backfills

Every task replaces its window, so a retry, a cleared task and a rerun of an
old interval land the same rows again. Clearing `books` clears the three
flatteners downstream of it, which then read the new snapshot. A backfill is
the same DAG over past intervals, in order:

```bash
airflow dags backfill rekep_record_keeping --start-date 2026-08-01 --end-date 2026-08-15
```

`max_active_runs=1` keeps runs in time order, because a silver window reads
what earlier bronze runs landed. Runs over different windows may otherwise
land side by side: a commit that loses a race inside another's window is
retried by PyIceberg against the refreshed table, or raises for a rerun. A
task that raises leaves its table's previous snapshot visible, so the tasks
after it -- which Airflow does not start -- read nothing half-written.

On Airflow 3, `dag`, `task` and `Variable` are imported from `airflow.sdk`;
the DAG is otherwise the same.
