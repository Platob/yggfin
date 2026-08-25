# Market

Market tables store immutable event versions. `hash` identifies one version;
`xhash` identifies its lifecycle; `linked_events` stores ordered `(unix,
xhash)` relations between order, execution, and book lifecycles. Links are
deduplicated and never point to the event's own lifecycle. Parsed logs retain
FIX wire order as `msg_seq_num`; normalized market events do not repeat it.

`MarketEvent` adds `instrument_xhash` and its readable `instrument_code`,
kind, side, price, quantity, notional,
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
travel as normalized rows in `fixmessage.market`, then the flattening notebook
projects them unchanged into the Instrument table.

There is no separate reference model or contract.

## When it happened

`unix` is when the transaction happened, not when somebody wrote it down.
`rekep.market.transacted` resolves it, and it is one resolver read by both
layers -- the parse stage fills the column over a whole batch in kernels, and
the translation layer reads one message at a time -- so the two cannot
disagree about when a row happened.

The chain, best first:

| rung | what it is |
| --- | --- |
| `TrdRegTimestamps` | the regulatory record, per `PREFERRED` below |
| `SideTrdRegTS` | the same, stated per side |
| `TransactTime <60>` | what the message claims about its own business event |
| `MDEntryDate <272>` + `MDEntryTime <273>` | a market-data entry's own instant |
| `OrigTime <42>` | time of message origination, for a relayed message |
| `OrigSendingTime <122>` | on a resend, when it first went out |
| `SendingTime <52>` | transmission, and the last FIX clock there is |

Below all of them is the clock that *recorded* the line, which is not in the
table because it is not something the message said. It is `runix`, and it is
where the log header's own stamp now lives on every row.

`unix_source` says which rung answered -- `TrdRegTimestamps=1`,
`TransactTime`, `recorded`, or empty for a row carrying no clock at all.
Without it nothing downstream can tell a real transaction time from a print
time, and that distinction is the whole point of resolving one.

### Which regulatory stamp is the transaction

A regulatory group carries several instants and they are not interchangeable,
so which one counts depends on what the row asserts. `PREFERRED` declares it
once, keyed by `EventType`:

| event | preferred `TrdRegTimestampType <770>`, best first |
| --- | --- |
| execution | `1` execution, `5` broker execution, `2` time in |
| order | `10` order submission, `9` orderbook entry, `2` time in, `4` broker receipt, `6` desk receipt |
| quote | `10`, `9`, `2`, `4` |
| book | `9` orderbook entry, `2` time in, `1` execution |

An execution happened when it executed; an order happened when it arrived.
One group on two kinds of row therefore gives two answers, which is what the
table exists for -- reading either as "the group's first entry" would stamp
one of them with the other's instant. A group carrying none of the preferred
types still answers, with its first entry: a regulatory stamp nobody ranked
is still nearer the transaction than a transmission clock.

`9` and `10` are later extension-pack codes the packaged dictionary does not
enumerate. They are ranked anyway, because a venue that sends one is not
sending a code this package should refuse.

### What this moves

Transaction time is not monotonic in file order -- a resend or a late
regulatory stamp lands behind rows already read -- so:

- `unix_hour` is recomputed from the resolved `unix`, and rows move between
  partitions accordingly. The partition stays a function of the column it is
  derived from, which is what keeps a partition-ordered read globally sorted.
- The book fold already asks the storage engine for
  `order_by=("unix", "msg_seq_num", "hash")`, so it gets an explicit sort pass
  rather than trusting file order. That was incidental before and is
  load-bearing now; no bounded reorder window is needed, because the sort is
  the storage engine's and not the stream's.
- `hash` changes for every row whose `unix` moved, since `unix` is part of a
  version's identity. Existing tables cannot be appended to and are rebuilt.

## How market rows are laid out

`unix_hour` is the only partition, and `instrument_code` is deliberately not a
second one. The case for bucketing it is real -- the hour prunes time and not
instrument, so a scan for one instrument across a week opens every hour's
files -- so it was measured rather than argued.

144,000 rows across 72 hours and 40 instruments, one instrument's whole week
against one hour, best of five:

| layout | files | mean file | one instrument, one week | one hour |
| --- | ---: | ---: | ---: | ---: |
| `unix_hour` alone | 72 | 76.0 KiB | 632 ms | 24 ms |
| `unix_hour` + `bucket[8]` | 576 | 28.5 KiB | 650 ms | 165 ms |
| `unix_hour` + `bucket[16]` | 1,152 | 25.0 KiB | 671 ms | 320 ms |

Bucketing loses on the query it was meant to win. The reason is that a bucket
prunes files *inside* an hour, and a scan across N hours still opens at least
one file per hour -- so the work does not fall, while the file count
multiplies by the bucket width and the hourly read every consumer writes gets
seven to thirteen times slower.

A narrower bucket, or a `truncate` transform on the code's prefix, moves the
numbers but not the shape of the argument: the cost being paid is per file
per hour, and no transform on the instrument reduces the number of hours. If
one-instrument scans ever do need to be fast, the answer is a sort order or a
secondary table keyed on the instrument, not a partition on it.

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

`BookIterator` consumes sorted `FixMessage` records, translates their already-parsed
FIX fields, indexes normalized Instrument rows by event type, restores prior
Book snapshots, and emits only `Book` rows. It is deliberately single-threaded
because order state is sequential.

Three settings bound what stays alive, and they answer different questions.
`max_order_age_ns` expires an order nothing has touched for that long;
`max_side_alive` evicts past that many per side by price-time priority; and
`purge_alive` decides what happens to whatever is still resting when the
*stream* ends. A window ending is not an order ageing out, and a reader of the
last book cannot otherwise tell a resting order from one nobody cancelled --
so `purge_alive=True` ends each of them as its own `INTERNAL_EXPIRED` version,
linked to the book that closed it. It is off by default, which is what a run
that will be resumed from its snapshots wants.

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
Mutation therefore probes dictionaries and a lazy deadline heap; full Order
copying belongs only to requested snapshots. Fully repeated orders stop before
completion and emit no Book, same-price amendments update their existing level,
expiry scans start only when the heap's first deadline is due, and
single-instrument streams skip cross-book sweeps.

Book identity still reads the ordered live Order hashes its contract requires,
but reads them per level and not per book: each level caches the order its
members settled into and their hashes, and forgets both whenever anything about
them moves -- a member joining or leaving, a quantity revised, or a new version
of an order that stands exactly where the old one did. A book with a hundred
live levels therefore pays for the one an event touched, and the frame those
hashes go into is written a run of integers at a time rather than one at a
time. Both leave the identity bytes exactly as they were; together they are
most of a 3.7x fold on a thousand-order book.

Translating a parsed row back into market events is the other half of the
generator, and on a real feed the larger half. Which columns carry a FIX tag,
which names a dictionary version resolves to which wire tag, and a message's
values under folded keys are each read once and kept -- per class, per version,
and per message -- rather than recomputed for every line.

## Benchmark

`python/benchmarks/bench_market.py --quick` verifies identity, FIX translation,
book derivations, focused state transitions, the whole parsed-row-to-books
generator, and a small replay-shape matrix before timing them. The full run
replays at `--rows` events a shape and reports operation counters beside
throughput.
