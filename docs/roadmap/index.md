# Data-product roadmap

The ingestion foundation is complete when `fix.messages` is replayable,
typed, auditable, and deployable. The next work is not another parser layer;
it is a sequence of business products derived from that table.

```mermaid
flowchart LR
    F[("fix.messages")] --> OE[("orders.events")]
    OE --> OC[("orders.current")]
    F --> EX[("executions.fills")]
    F --> BU[("book.updates")]
    BU --> BS[("book.snapshots")]
```

| order | product | grain | main consumer | status |
| ---: | --- | --- | --- | --- |
| 1 | [`orders.events`](orders.md) | one normalized order event | lifecycle audit and state reconstruction | planned |
| 2 | [`orders.current`](orders.md#orderscurrent) | latest settled state per logical order | operations and exposure views | planned |
| 3 | [`executions.fills`](executions.md) | one economic fill/correction/cancel | trading, allocation, and TCA | planned |
| 4 | [`book.updates`](order-book.md) | one normalized depth mutation | market-data replay | planned |
| 5 | [`book.snapshots`](order-book.md#booksnapshots) | one ordered book image at a checkpoint | research and monitoring | planned |

## Rules shared by every product

1. Read `fix.messages` as a `RecordBatchReader`; never re-open captures.
2. Retain `url`, `rownum`, and `msghash` as source lineage.
3. Make the row grain and key explicit before adding columns.
4. Keep stated protocol values separate from derived identities or state.
5. Preserve corrections, cancels, rejects, and unknown states as events; do
   not rewrite history in place.
6. Build current-state or snapshot products from immutable events.
7. Publish a `Field` schema, Iceberg key, partition, replay test, and source
   coverage report together.

## Build gates

Each product lands only when it has:

| gate | requirement |
| --- | --- |
| grain | one sentence that decides whether a source message emits zero, one, or many rows |
| identity | deterministic key across replay and capture duplication |
| lineage | source position and parsed digest retained |
| semantics | precedence for identifiers, clocks, corrections, and nulls |
| schema | metadata-bearing field plus reviewed JSON snapshot |
| quality | unmapped/rejected counters and reconciliation queries |
| storage | partition, sort intent, create/append/overwrite behavior |
| integration | fixture from capture through the product and idempotent replay |

The order and execution products come first because the current fixed schema
already projects their identifiers, quantities, prices, state, clocks, and
parties. Order-book products follow after the registry projection includes the
market-data repeating groups needed to express depth without consulting raw
pairs in normal queries.
