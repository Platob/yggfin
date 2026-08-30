# Flatten executions

`tasks/flatten_executions/flatten_executions.ipynb`
streams `Book.executions` into the existing `Execution` contract and
writes `market.executions`. It retains order links and event identity, then
appends the carrying `Book.hash` to `parenthash`.

## Run this step

In book mode, after `parse_market` has populated `market.books`, run from the
repository root:

```bash
uv run --project python --group runner rekep task run \
  tasks/flatten_executions/flatten_executions.yml \
  --output flatten_executions.executed.ipynb
```

The package, a FIX registry and a catalog have to exist first:
[deploy from scratch](../operations/deploy.md).

The adjacent `flatten_executions.yml` sets the `[start, end)` interval,
source, target, catalog, and commit size. Replay skips existing event keys
when `merge_by` is enabled.

Airflow runs this task only when `parse_market` reports nested Execution rows
in `flatten.executions`. Direct mode leaves that count zero because it writes
its configured execution target itself.
