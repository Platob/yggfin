# Parse market

`parse_market` reads `fix.market` in event order and produces market events.

## Run this step

Book mode folds resting state and writes `market.books`:

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_market/parse_market.yml \
  --output parse_market.executed.ipynb
```

Direct mode writes only the Order and Execution events carried by FIX:

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_market/parse_market.yml \
  --parameter books=false \
  --output parse_market.direct.executed.ipynb
```

Deploy the catalog first: [deploy from scratch](../operations/deploy.md).

## Modes

| `books` | writes | stateful behavior |
| --- | --- | --- |
| `true` | `market.books` | snapshots, lifecycle completion, expiry and rejection |
| `false` | configured Order/Execution targets | none; captured events only |

```yaml
source: fix.market
books: true
target: market.books
order_target: market.orders
execution_target: market.executions
snapshot_every: 3600000000000
max_lateness_ns: 900000000000
max_order_age_ns: 86400000000000
max_side_alive: 10000
commit_row_size: 250000
```

The FIX registry supplies message dispatch and lifecycle mappings. Flat rows
use Arrow kernels; structured or custom rows fall back to the same scalar
translator.
Book mode translates each stored batch once, emits `Order` before `Execution`
when one report carries both, and releases the batch before reading the next.

A bounded run starts one hour before the hour containing `start` and ends
`max_lateness_ns` after the hour ceiling of `end`, then emits only
`[start, end)`. Both modes exclude rows whose `error` is non-null. They read
the transient `Instrument` facts on each FIX event and never read
`market.instruments`.

Airflow schedules flattening only when book mode reports nested Orders or
Executions. Direct mode writes those targets itself and reports zero flatten
work.
