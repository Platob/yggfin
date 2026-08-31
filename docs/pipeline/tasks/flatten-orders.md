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
commit_row_size: 250000
```

Airflow runs this only when book mode reports `flatten.orders`. Direct market
mode writes Orders itself. Replay skips existing event keys.
