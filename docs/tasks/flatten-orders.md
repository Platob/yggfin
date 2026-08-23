# Flatten orders

`tasks/flatten_orders/flatten_orders.ipynb`
streams `Book.order_events` into the existing `Order` contract and writes
`market.orders`. It retains event identity and appends the carrying `Book.hash`
to `parent_hash`.

The adjacent `flatten_orders.yml`
sets the `[start, end)` interval, source, target, catalog, and commit size.
Replay skips existing event keys when `merge_by` is enabled.
