# Market

Market tables store immutable event versions. `hash` identifies one version;
`xhash` identifies its lifecycle; `linked_events` stores ordered `(unix,
xhash)` relations between order, execution, and book lifecycles. Links are
deduplicated and never point to the event's own lifecycle. Parsed logs retain
FIX wire order as `msg_seq_num`; normalized market events do not repeat it.

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
identity. `xhash` and `code` derive only from the exact symbol; security ids,
ISINs, and venues remain reference facts. The same spelling therefore shares
one lifecycle across venues, while different spellings never alias through an
identifier. An exact `AAA/BBB` symbol is classified as currency when no kind is
declared and supplies `BBB` as the price currency when that is absent.

A new market log symbol creates a synthetic minimal instrument; later facts
enrich that symbol's lifecycle. Changed versions and hourly snapshots first
travel as normalized rows in `logs.market`, then the flattening notebook
projects them unchanged into the Instrument table.

There is no separate reference model or contract.

## Orders and executions

`Order.qty` is the remaining live quantity after that event. New orders carry
their initial quantity, partial fills reduce it, and terminal orders carry
zero and do not remain in the book. `prev_qty` and execution totals preserve
useful source quantities without changing that meaning.

An execution report that changes an order produces both the `Execution`
evidence and the resulting `Order` state transition. The order is authoritative
for the remaining quantity, so the execution is not subtracted a second time.
Both rows retain their source identifiers and relate through `linked_events`.
Missing identifiers may resolve against indexed live order names.
Pending replace/cancel requests leave acknowledged interest resting; a replace
confirmation publishes the amended live quantity and price.

Invalid book inputs are retained with `INTERNAL_REJECTED` and a generic `reason`.
Missing or non-finite price/quantity cannot mutate the book.

## Books

`BookIterator` consumes sorted `Log` records, translates their already-parsed
FIX fields, indexes normalized Instrument rows by event type, restores prior
Book snapshots, and emits only `Book` rows. It is deliberately single-threaded
because order state is sequential.

A compact level contains only price and quantity. `bid_levels` and `ask_levels`
keep best-price order. On normal rows, `deltas` and `executions` say what
changed while the level lists contain only changed levels. `bid_alive` and
`ask_alive` are empty.

On recovery snapshots, `deltas` and `executions` are empty; the level lists and
the two alive lists contain the complete living state. `sunix`, not a nullable
list, distinguishes that picture from a delta: an empty delta list means
nothing changed, while an empty snapshot list means the side is empty. All six
collections and both depth columns are non-null; zero depth means no live
levels. A per-side bound emits synthetic internal expiry deltas for removed
orders.

Top-level best prices, quantities, spread, midpoint, top-of-book `vwap`, latest
filled `exec_px`, and imbalance stay flat for Iceberg pruning. A Book version
hash frames `(unix, instrument_xhash)` followed by the ordered live bid and ask
Order version hashes. The fold retains those identity inputs privately on
deltas; recovery snapshots persist the complete live orders. When only one
side changes, the iterator recomputes that side and carries the other side's
flat summary before deriving cross-side values. `prev_bid_px`, `prev_bid_qty`,
`prev_ask_px`, `prev_ask_qty`, and `prev_exec_px` retain preceding values on
deltas and clear on snapshots. There is no separate `BookSide` schema.

Live orders, names, expiry deadlines, and level quantities stay indexed.
Mutation therefore probes dictionaries and a lazy deadline heap; Book identity
still walks the ordered live Order hashes required by its contract, while full
Order copying belongs only to requested snapshots. Fully repeated orders stop
before completion and emit no Book, same-price amendments update their existing
level, expiry scans start only when the heap's first deadline is due, and
single-instrument streams skip cross-book sweeps.

## Benchmark

`python/benchmarks/bench_market.py --quick` verifies identity, FIX translation,
book derivations, focused state transitions, and a small replay-shape matrix
before timing them. The full run expands the event and live-order matrix and
reports operation counters beside throughput.
