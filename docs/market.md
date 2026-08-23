# Market

Market tables store immutable event versions. `hash` identifies one version;
`xhash` identifies its lifecycle; `linked_xhash` relates order, execution, and
book lifecycles. `seq` orders equal clocks.

`MarketEvent` adds `instrument_xhash`, kind, side, price, quantity, notional,
currency, and their previous values. Protocol spelling stays in metadata while
the stored fields use common semantics.

## State

`State` is one ordered integer vocabulary. Pending, live, partial, terminal,
closed, and failed bands support range predicates. Venue rejection/expiry uses
`REJECTED`/`EXPIRED`; records rejected or expired by this pipeline use
`INTERNAL_REJECTED`/`INTERNAL_EXPIRED` so audit queries can separate them.

Finished events do not produce more snapshots. Generic `Event` snapshot logic
also expires unchanged state after one day, using `sunix` when present and
otherwise `unix`.

## Instrument

`Instrument` is both the persisted instrument record and the source of its
stable identity. Identity preference is a scoped security id, ISIN, then venue
symbol. A new market log symbol creates a synthetic minimal instrument; later
facts enrich it without changing its canonical lifecycle. Changed versions and
hourly snapshots first travel as normalized rows in `logs.market`, then the
flattening notebook projects them unchanged into the Instrument table.

There is no separate reference model or contract.

## Orders and executions

Orders keep current price, quantity, hidden quantity, side, time in force,
kind, indication flag, expiry, client/order identifiers, and lineage.
Executions include their order identifiers and link the matched order through
`linked_xhash`. Missing identifiers may resolve against a live client-order id.

Invalid book inputs are retained with `INTERNAL_REJECTED` and an `error` reason.
Missing or non-finite price/quantity cannot mutate the book.

## Books

`BookIterator` consumes sorted `Log` records, translates their already-parsed
FIX fields, indexes normalized Instrument rows by event type, restores prior
Book snapshots, and emits only `Book` rows. It is deliberately single-threaded
because order state is sequential.

A compact level contains price, quantity, order lifecycle ids, and execution
lifecycle ids. `bid_levels` and `ask_levels` keep best-price order. Delta rows
carry changed order/execution events; snapshots carry all live orders and clear
the delta. A per-side bound expires worse orders beyond `max_side_alive` with
synthetic internal expiry events.

Top-level best prices, quantities, spread, midpoint, microprice, and imbalance
stay flat for Iceberg pruning. A Book version hash uses only `(unix,
instrument_xhash)`, so one instrument has at most one row per instant. When
only one side changes, the iterator recomputes that side and carries the other
side's flat summary before deriving cross-side values. There is no separate
`BookSide` schema.

## Benchmark

`python/benchmarks/bench_market.py --quick` verifies identity, FIX translation,
and book derivations before timing them.
