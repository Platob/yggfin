# Flatten orders

`flatten_orders` streams `Book.deltas` into `market.orders`, preserving event
identity and appending the carrying `Book.hash` to `parenthash`.

## Run this step

```bash
uv run --project python --group runner rekep task run \
  tasks/flatten_orders/flatten_orders.yml \
  --output flatten_orders.executed.ipynb
```

```yaml
source: market.books
target: market.orders
merge_by: true
commit_batch_num: 8
commit_row_size: null # Optional earlier row cap.
```

Airflow runs this only when book mode reports `flatten.orders`. Direct market
mode writes Orders itself. Replay skips existing event keys.
