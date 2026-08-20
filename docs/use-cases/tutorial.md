# Tutorial: zero to a local lakehouse

Everything below runs on a laptop with no services — the default deployment
is a SQLite Iceberg catalog and a file warehouse. Prefer it guided and
animated? The same tour ships in the CLI:

```bash
rekep tutorial          # step by step, press Enter between steps
rekep tutorial --auto   # straight through, script-friendly
```

## 1 · Install

```bash
pip install "rekep[local]"      # Arrow + Iceberg + the local SQLite catalog
# or, for everything (YAML, TOML, Jinja, xxhash, Airflow on POSIX):
pip install "rekep[all]"
```

Check the toolchain in one line:

```bash
rekep install doris --dry-run     # shows what a Doris install would run
rekep install airflow --dry-run   # same for Airflow
```

Each installer first checks honestly — Doris by probing the FE port, Airflow
by asking Python — and a service already present is a logged no-op.

## 2 · Declare your first record

One dataclass is the whole data product. Create `models.py`:

```python
import datetime
from typing import Annotated

from rekep import Arrow, Record, record


@record
class Trade(Record):
    """One executed trade."""

    trade_id: Annotated[str, Arrow(key=True)]
    """Exchange-assigned identifier -- the primary key."""

    day: Annotated[datetime.date, Arrow(partition=True)]
    """Trading day; every table below partitions on it."""

    symbol: str
    """Instrument ticker."""

    qty: int
    """Signed quantity, negative for sells."""

    price: float
    """Execution price."""

    note: str | None = None
    """Free-form remark; the only nullable column."""
```

Three declarations to notice: the field docstring becomes the column comment
in every projection; `str | None` is the only way a column becomes nullable;
`Arrow(key=True)` / `Arrow(partition=...)` place primary keys and partitions
once, for every engine.

## 3 · Look at every projection

```python
Trade.into_arrow_schema()     # Arrow: types, nullability, descriptions, field ids
Trade.into_iceberg_schema()   # Iceberg: same schema, identifier fields set
Trade.into_iceberg_ddl()      # CREATE TABLE ... USING iceberg, comments included
Trade.into_doris_ddl()        # Doris: UNIQUE KEY leading, AUTO PARTITION
Trade.into_yaml()             # the declaration itself, reviewable YAML
```

Or from the shell, for any record on the import path:

```bash
rekep product dump --namespace models.Trade --out -
rekep ddl dump --namespace models.Trade --dialect doris --out -
```

## 4 · Declare the deployment

A deployment is a folder registry — one entry per file, the file stem
defaulting the name. Iceberg and Doris declare only their infrastructure:

```text
stacks/iceberg/
├── catalogs/iceberg.yaml       # omit entirely for the built-in local default
└── namespaces/trading.yaml     # catalog: local
```

Tables need no side file of their own: a `Dataset` under `stacks/datasets/`
carries its record, name and namespace, and deploys autonomously against
whichever catalog/namespace registry it is pointed at:

```yaml
# stacks/datasets/trades.yaml
record: models.Trade
name: trades
namespace: trading
```

Values may be Jinja: `{{ env.BUCKET }}`, and the git context is always there
— `trades{{ git_branch_suffix }}` names branch deployments automatically and
stays clean on `main`.

## 5 · Deploy

```bash
rekep iceberg deploy --dry-run          # catalogs, namespaces: the plan
rekep iceberg deploy                    # catalog -> namespace
rekep dataset deploy --target iceberg   # tables, from stacks/datasets/
rekep dataset deploy --target iceberg   # second run: every line a no-op
```

Priority is built in — catalogs are checked, then namespaces converge, then
`dataset deploy` builds each table ad hoc (`Dataset.into_iceberg_table()`)
and converges it the same way — and every action logs what it did. In
Python it is two calls:

```python
from rekep.dataset import Dataset
from rekep.iceberg import Iceberg

stack = Iceberg.load("stacks/iceberg")
stack.deploy()  # catalogs, namespaces
for dataset in Dataset.load_all("stacks/datasets"):
    dataset.deploy_iceberg(stack)
```

Doris is the same shape; without a cluster connection the deploy *is* the
ordered SQL plan:

```bash
rekep doris deploy --dry-run
rekep dataset deploy --target doris --dry-run
```

## 6 · Parse and land data

```python
from rekep.logs import LogFile

with LogFile.from_url("s3://bucket/app-2026-08-14.txt.gz") as log:
    for batch in log.into_arrow_reader():
        ...  # pyarrow.RecordBatch, bounded memory, ready to append to the table
```

## Synthetic ideas to try next

- **A second record**: declare `Quote`, add `stacks/datasets/quotes.yaml`,
  `dataset deploy` — only the new table is created.
- **Schema evolution**: add an optional field to `Trade`, `dataset deploy`
  again — `create_or_update` unions the new column in, no side file to sync.
- **A task**: subclass `rekep.job.Job`, implement `arrow_transform`
  (batches in, batches out), declare it in `stacks/jobs/`, name it from a
  `stacks/dags/` graph, and run it with `rekep dag run` -- or let
  `rekep.airflow.dags.dags()` turn that graph into a lineage-tagged DAG.
- **A branch environment**: `export GITHUB_REF_NAME=feature/x` and redeploy —
  every table name picks up `_feature_x`.
