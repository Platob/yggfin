# Notebook workflow

Pipeline implementations live in `tasks/<name>/<name>.ipynb`. Each adjacent
YAML file points to its notebook and supplies parameters:

```yaml
name: parse_fix
notebook: parse_fix.ipynb
parameters:
  source: data/capture
```

`Task` only reads, writes, and resolves this configuration. A notebook runner
owns execution; the package contains no prebuilt pipeline jobs or task reports.

```python
from pathlib import Path

import papermill
from rekep import Task

document = Path("tasks/parse_fix/parse_fix.yml")
task = Task.from_yaml(document)
papermill.execute_notebook(
    task.into_notebook_path(document),
    "parse_fix.executed.ipynb",
    parameters=task.parameters,
)
```

## Flow

```text
Text files -> parse_messages -> logs.messages -> parse_fix -> fix.market
                                                                  |
                       +------------------------------------------+
                       |
                       +-> flatten_instruments -> Instrument
                       `-> parse_market -+-> Book -+-> Order
                                         |         `-> Execution
                                         `---------> Order + Execution (books: false)
```

`parse_messages` writes protocol-neutral `Message` rows and tokenizes generic
key/value syntax once. `parse_fix` owns classification, dictionary resolution
and routing from those ordered arguments. The retained message table lets a
dictionary change rerun FIX resolution without reopening the source logs or
splitting the payload again.

`parse_fix` resumes Instrument lifecycles from the prior completed Instrument
table. The current run has no dependency cycle: both downstream notebooks read
the normalized Instrument rows already committed inside `fix.market`.

The default market path folds books and then flattens their deltas. With
`books: false`, `parse_market` skips the fold and writes only the Order and
Execution events carried by each FIX message. This path deliberately
does not create snapshots, synthetic expiries, book validation changes, or a
carrying `Book.hash` parent.

The persisted products are protocol-neutral `Message` rows, categorized
`FixMsg` tables, and `Instrument`, `Book`, `Order`, and `Execution`. Arrow
readers carry each stream; Iceberg stores the boundaries.

- [Parse messages](tasks/parse-messages.md)
- [Parse FIX](tasks/parse-fix.md)
- [End-to-end run](workflow-run.md)
- [Deploy and operate with Airflow](airflow.md)
- [Flatten instruments](tasks/flatten-instruments.md)
- [Parse market](tasks/parse-market.md)
- [Flatten orders](tasks/flatten-orders.md)
- [Flatten executions](tasks/flatten-executions.md)
- [Airflow](#airflow)

## Airflow

`tasks/airflow/market_pipeline.py` uses
`apache-airflow-providers-papermill`. Airflow injects its half-open data
interval and one `branch` DAG parameter into each notebook. `root`, `main`, and
`master` all select Iceberg's root ref.

Each notebook publishes its result through Scrapbook. Branch tasks inspect
attempted/read counts from that result: an empty capture skips `parse_fix`, a
FIX interval without market rows skips both market consumers, and direct
market mode skips the two book flatteners through zero `flatten` counts. In
book mode Order and Execution flatteners route independently from the number
of nested rows the selected Books carry. The routes deliberately do not use
`written`; a replay can write zero while still needing downstream work after
parsing or schema logic changes.

Install the provider in the scheduler environment, not as a `rekep` runtime
dependency. Set `REKEP_ROOT` to the checkout and `REKEP_NOTEBOOK_OUTPUT` to an
existing directory visible to every worker; the routing tasks read the
executed notebooks from there.

The [Airflow deployment and operations guide](airflow.md) covers the complete
runtime installation, local validation, scheduled runs, backfills, Iceberg
branches, production storage, monitoring, retries, and troubleshooting.
