# Notebook workflow

Pipeline implementations live in `tasks/<name>/<name>.ipynb`. Each adjacent
YAML file points to its notebook and supplies parameters:

```yaml
name: parse_logs
notebook: parse_logs.ipynb
parameters:
  source: data/capture
```

`Task` only reads, writes, and resolves this configuration. A notebook runner
owns execution; the package contains no prebuilt pipeline jobs or task reports.

```python
from pathlib import Path

import papermill
from rekep import Task

document = Path("tasks/parse_logs/parse_logs.yml")
task = Task.from_yaml(document)
papermill.execute_notebook(
    task.into_notebook_path(document),
    "parse_logs.executed.ipynb",
    parameters=task.parameters,
)
```

## Flow

```text
Text files -> parse_logs -> logs.market -+-> flatten_instruments -> Instrument
                            (Log stream)  `-> parse_market -> Book -+-> Order
                                                                  `-> Execution
```

`parse_logs` resumes Instrument lifecycles from the prior completed Instrument
table. The current run has no dependency cycle: both downstream notebooks read
the normalized Instrument rows already committed inside `logs.market`.

The persisted products are only categorized `Log` tables plus `Instrument`,
`Book`, `Order`, and `Execution`. Arrow readers carry each stream; Iceberg
stores the boundaries.

- [Parse logs](tasks/parse-logs.md)
- [End-to-end run](workflow-run.md)
- [Flatten instruments](tasks/flatten-instruments.md)
- [Parse market](tasks/parse-market.md)
- [Flatten orders](tasks/flatten-orders.md)
- [Flatten executions](tasks/flatten-executions.md)
- [Airflow](#airflow)

## Airflow

`tasks/airflow/market_pipeline.py` uses
`apache-airflow-providers-papermill`. Airflow injects its half-open data
interval into each notebook. `parse_logs` starts the instrument and market
branches; the two flat views run in parallel after `parse_market`.

Install the provider in the scheduler environment, not as a `rekep` runtime
dependency. Set `REKEP_ROOT` to the checkout and `REKEP_NOTEBOOK_OUTPUT` to an
existing worker-visible directory.
