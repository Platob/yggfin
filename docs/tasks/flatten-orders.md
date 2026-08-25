# Flatten orders

`tasks/flatten_orders/flatten_orders.ipynb`
streams `Book.deltas` into the existing `Order` contract and writes
`market.orders`. It retains event identity and appends the carrying `Book.hash`
to `parent_hash`.

The adjacent `flatten_orders.yml`
sets the `[start, end)` interval, source, target, catalog, and commit size.
Replay skips existing event keys when `merge_by` is enabled.
Airflow runs this task only when `parse_market` reports nested Order rows in
`flatten.orders`. Direct mode leaves that count zero because it writes its
configured order target itself.
