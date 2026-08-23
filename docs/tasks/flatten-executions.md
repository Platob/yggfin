# Flatten executions

`tasks/flatten_executions/flatten_executions.ipynb`
streams `Book.executions` into the existing `Execution` contract and
writes `market.executions`. It retains order links and event identity, then
appends the carrying `Book.hash` to `parent_hash`.

The adjacent `flatten_executions.yml`
sets the `[start, end)` interval, source, target, catalog, and commit size.
Replay skips existing event keys when `merge_by` is enabled.
