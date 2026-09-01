# Flatten executions

`flatten_executions` streams `Book.executions` into `market.executions`,
preserving order links and appending the carrying `Book.hash` to `parenthash`.

## Run this step

```bash
uv run --project python --group runner rekep task run \
  tasks/flatten_executions/flatten_executions.yml \
  --output flatten_executions.executed.ipynb
```

```yaml
source: market.books
target: market.executions
merge_by: true
commit_batch_num: 8
commit_row_size: null # Optional earlier row cap.
```

Airflow runs this only when book mode reports `flatten.executions`. Direct
market mode writes Executions itself. Replay skips existing event keys.
