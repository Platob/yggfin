# Market

Market tables store immutable event versions. `vhash` identifies its value,
`hash` anchors that value to the event time, `prevhash` names its predecessor,
`xhash` identifies its lifecycle, and `linkedhashes` holds
ordered lifecycle hashes relating order, execution and book rows —
deduplicated, and never pointing at the event's own lifecycle.

Each product has its own page: [Instrument update](../products/instrument.md),
[Order](../products/order.md), [Execution](../products/execution.md),
[Book](../products/book.md). This page is what they share.

```python
from rekep import FixMsg

line = "8=FIX.4.4|35=8|11=C1|37=O1|17=E1|55=BTC-USD|54=1|31=100.25|32=10|38=10|39=2|150=F|10=000"
for event in FixMsg.from_text(line).into_market_events(fix_version="4.4"):
    print(type(event).__name__, event.state.name, event.qty)
```

```text
Order FILLED 0.0
Execution FILLED 10.0
```

`MarketEvent` adds `instrumentxhash` and its readable `symbolticker`,
kind, side, price, quantity, notional, currency, and their previous values.
Protocol spelling stays in metadata; stored fields use common semantics.
Parsed logs retain FIX wire order as `MsgSeqNum`; normalized market events do
not repeat it.

## State

One ranked vocabulary. Pending, live, partial, terminal, closed and failed
bands answer what a detailed state broadly means. Ranks make "still live" and
"finished" one question each, and name the finite code sets a scan pushes down:

```python
from rekep.enums import State

print(State.FILLED.is_live, len(State.live_codes()))
```

```text
False 9
```

Venue rejection and expiry use `REJECTED`/`EXPIRED`; records this pipeline
rejects or expires use `INTERNAL_REJECTED`/`INTERNAL_EXPIRED`, so audit
queries can separate them. Finished events produce no more snapshots, and
generic `Event` snapshot logic expires unchanged state after one day, using
`snapunix` where present and `unix` otherwise.

## When it happened

`unix` is when the transaction happened, not when somebody wrote it down.
`rekep.market.transacted` resolves it, and one resolver is read by both layers
— the parse stage fills the column over a whole batch in kernels, the
translation layer reads one message at a time — so the two cannot disagree.

```python
from rekep.enums import EventType
from rekep.market.transacted import PREFERRED, TRANSACTED

print(" > ".join(rung.name for rung in TRANSACTED))
for kind, ranked in PREFERRED.items():
    print(f"{EventType(kind).name:9} {ranked}")
```

```text
TrdRegTimestamps > SideTrdRegTS > TransactTime > MDEntry > OrigTime > OrigSendingTime > SendingTime
EXECUTION (1, 5, 2)
ORDER     (10, 9, 2, 4, 6)
QUOTE     (10, 9, 2, 4)
BOOK      (9, 2, 1)
```

Below every rung is the clock that *recorded* the line. It is not in that
chain because it is not something the message said: it is `recunix`, on every
row.

`PREFERRED` ranks `TrdRegTimestampType <770>` per event kind, because a
regulatory group carries several instants and they are not interchangeable. An
execution happened when it executed; an order happened when it arrived. One
group on two kinds of row gives two answers — reading either as "the group's
first entry" would stamp one with the other's instant. A group carrying none
of the ranked types still answers with its first entry: a regulatory stamp
nobody ranked is still nearer the transaction than a transmission clock. `9`
and `10` are later extension-pack codes the packaged dictionary does not
enumerate, ranked anyway, because a venue that sends one is not sending a code
this package should refuse.

`unixsource` says which rung answered — `TrdRegTimestamps=1`, `TransactTime`,
`recorded`, or empty for a row with no clock at all. Without it nothing
downstream can tell a real transaction time from a print time, and that
distinction is the whole point of resolving one.

### What this moves

Transaction time is not monotonic in file order — a resend or a late
regulatory stamp lands behind rows already read — so:

- `unixpartition` is recomputed from the resolved `unix`, and rows move
  between partitions. The partition stays a function of the column it derives
  from, which is what keeps a partition-ordered read globally sorted.
- The book fold asks storage for `order_by=("unix", "MsgSeqNum", "hash")`, so
  it gets an explicit sort pass rather than trusting file order. No bounded
  reorder window is needed, because the sort is the storage engine's and not
  the stream's.
- `hash` changes for every row whose `unix` moved, since `unix` is part of a
  version's identity.

## Layout

`unixpartition` is the only partition: the hour boundary in whole epoch
seconds as a signed `int32`, while `unix` keeps the instant in nanoseconds.
The shorter value avoids nine meaningless zeroes in partition paths without
changing hourly cardinality, and covers 1901-12-13 21:00 UTC inclusive to
2038-01-19 04:00 UTC exclusive.

Reference-data nested members stay declared last. Iceberg counts leaf columns
in declaration order for the bounds it collects, so an earlier struct could
push a flat filter column past the cutoff. `FixMsg.instrument`,
`InstrumentUpdate.instrument`, and the component's `legs` therefore follow
their flat siblings.

`symbolticker` is deliberately not a second partition. The case for
bucketing it is real — the hour prunes time, not instrument — so it was
measured. 144,000 rows across 72 hours and 40 instruments, best of five:

| layout | files | mean file | one instrument, one week | one hour |
| --- | ---: | ---: | ---: | ---: |
| `unixpartition` alone | 72 | 76.0 KiB | 632 ms | 24 ms |
| `+ bucket[8]` | 576 | 28.5 KiB | 650 ms | 165 ms |
| `+ bucket[16]` | 1,152 | 25.0 KiB | 671 ms | 320 ms |

Bucketing loses on the query it was meant to win: a bucket prunes files
*inside* an hour, and a scan across N hours still opens at least one file per
hour — so the work does not fall while the file count multiplies and the
hourly read every consumer writes gets seven to thirteen times slower. A
narrower bucket or a `truncate` on the code's prefix moves the numbers, not
the shape of the argument. If one-instrument scans must be fast, the answer is
a sort order or a secondary table keyed on the instrument, not a partition.

## Folding books

```python
from rekep import FixMsg
from rekep.market import BookIterator

resting = "8=FIX.4.4|35=8|34=1|11=B1|37=OB1|17=E1|55=BTC-USD|54=1|44=99.5|38=5|151=5|14=0|39=0|150=0|10=000"
events = list(FixMsg.from_text(resting).into_market_events(fix_version="4.4"))

for purge in (False, True):
    rows = BookIterator.from_events(events, purge_alive=purge)
    print(purge, [(book.bidpx, book.bidqty, book.state.name) for book in rows])
```

```text
False [(99.5, 5.0, 'OPEN')]
True [(None, None, 'CLOSED')]
```

`purge_alive=True` ends the one resting order as the stream closes, so the
last book a reader sees is an empty one rather than a bid nobody cancelled.


`BookIterator` consumes sorted `FixMsg` records, translates their parsed FIX
fields, restores prior Book snapshots, and emits only `Book` rows. Instrument
facts travel with each translated market event. Single-threaded on purpose:
order state is sequential. What stays alive, and the delta/snapshot
distinction, are on the [Book](../products/book.md) page.

Live orders, names, expiry deadlines and level quantities stay indexed, so
mutation probes dictionaries and a lazy deadline heap; full Order copying
belongs only to requested snapshots. Fully repeated orders stop before
completion and emit no Book, same-price amendments update their existing
level, expiry scans start only when the heap's first deadline is due, and
single-instrument streams skip cross-book sweeps.

Book value identity reads the ordered live Order `vhash` values its contract
requires. Each level caches the order its members settle into and their value
hashes, then forgets both when a member joins, leaves, or changes. A book with
a hundred live levels therefore re-hashes the level one event touched, and
frames the cached values as runs of signed `int64`.

Translating a parsed row back into market events is the other half of the
generator, and on a real feed the larger half. Which columns carry a FIX tag,
which names a dictionary version resolves to which wire tag, and a message's
values under folded keys are each read once and kept — per class, per version,
per message.

Invalid book inputs are retained with `INTERNAL_REJECTED` and a generic
`reason`; missing or non-finite price and quantity cannot mutate the book.

## Benchmark

```bash
python/benchmarks/bench_market.py --quick
```

Verifies identity, FIX translation, book derivations, focused state
transitions, the whole parsed-row-to-books generator, and a small replay-shape
matrix before timing them. The full run replays `--rows` events a shape and
reports operation counters beside throughput.
