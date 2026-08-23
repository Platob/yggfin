# End-to-end run

Measured on 2026-08-23, this run executed the code cells from all five task
notebooks against a fresh local Iceberg warehouse, then replayed the same
interval. It is a correctness fixture; the larger focused measurements remain
on [Benchmarks](benchmarks.md).

![End-to-end execution architecture](assets/workflow-run.svg)

Only `logs.market` continues into market readers. `logs.misc_logs` and
`logs.unknown_logs` remain complete terminal Iceberg tables.

## Run context

| Context | Value |
| --- | --- |
| Source | 13 generated text rows |
| Interval | `[2026-08-21T10:30:00Z, 2026-08-21T10:30:04Z)` |
| Code | Local working tree based on `7a471ce` |
| Runtime | Python 3.12.13, PyArrow 25.0.1, PyIceberg 0.11.1 |
| Host | Windows 11, AMD Ryzen 5 150, 24 GB RAM |
| Storage | SQLite catalog and local file warehouse |
| Test bounds | 4-row commits, 1 s snapshots, 1.5 s live-order age |

The small commit size deliberately crossed storage boundaries. Cold times
therefore include registry loading, catalog setup, table creation, and several
Iceberg commits; they are not throughput claims.

## Generated input

The capture combined four incremental depth/trade messages, one Security
Definition, three orders, two Execution Reports, one two-sided quote, one
known heartbeat, and one unmatched transport line. Selected message bodies:

```text
8=FIX.4.4|35=X|55=BTC-USD|268=2|279=0|269=0|270=100.0|271=5|...|
8=FIX.4.4|35=d|55=AAPL|48=US0378331005|22=4|207=XNAS|15=USD|969=0.01|561=1|107=Apple Inc.|
8=FIX.4.4|35=D|55=AAPL|11=BAD-AAPL|54=1|38=10|40=2|...|
heartbeat without structured protocol
opaque transport payload :: 7f3a9c
```

The missing-price order is intentional. It proves that a rejected event
remains auditable and carries its reason.

## Stage results

| Notebook | First result | Cold wall time | Replay writes |
| --- | --- | ---: | ---: |
| `parse_logs` | 13 raw read; 13 raw + 16 Instrument rows written | 4.706 s | 0 |
| `flatten_instruments` | 16 versions, 16 written | 1.182 s | 0 |
| `parse_market` | 17 books, 17 written | 2.396 s | 0 |
| `flatten_orders` | 21 projected, 15 written | 1.539 s | 0 |
| `flatten_executions` | 3 projected, 3 written | 0.800 s | 0 |
| **Total** |  | **10.623 s** | **0** |

For the requested `[10:30:00, 10:30:04)` output, `parse_market` planned the
hour-aligned superset `[09:00:00, 11:15:00)`: floor the start hour and subtract
one hour, then ceil the end hour and add 15 minutes. The Iceberg predicates
remain half-open and the output filter restores the exact requested interval.

The replay took 4.520 s. It preserved every row count and sorted hash set;
every notebook wrote zero. `parse_logs` reported 29 skips: 13 raw rows and 16
deterministically reconstructed Instrument lifecycle rows.

| Iceberg table | Rows | Iceberg snapshots |
| --- | ---: | ---: |
| `logs.market` | 27 | 7 |
| `logs.misc_logs` | 1 | 1 |
| `logs.unknown_logs` | 1 | 1 |
| `market.instruments` | 16 | 4 |
| `market.books` | 17 | 5 |
| `market.orders` | 15 | 4 |
| `market.executions` | 3 | 1 |

Routing conserved all input: 11 FIX market rows, one `MISC` heartbeat, and one
`OTHER` row. `parse_logs` added 16 normalized Instrument versions to the same
market table, including hourly snapshots; only this table continued.

| Contract | State counts | Errors |
| --- | --- | ---: |
| Instrument | `OPEN=16` | 0 |
| Book | `OPEN=12`, `CLOSED=5` | carried in nested events |
| Order | `PENDING_NEW=2`, `NEW=2`, `OPEN=3`, `PARTIALLY_FILLED=1`, `FILLED=1`, `CANCELLED=1`, `INTERNAL_EXPIRED=4`, `INTERNAL_REJECTED=1` | 5 |
| Execution | `FILLED=3` | 0 |

The intentional rejection rate was 1/15 order rows (6.67%), or 1/18 flattened
order and execution rows (5.56%). The four internal expiries are expected from
the short 1.5 s test bound; the normal workflow default is one day.

## Sampled output

### Logs

| Category and time | Classification | Identity | Selected parsed data |
| --- | --- | --- | --- |
| market, `10:30:00.100Z` | `FIX / BOOK / X` | `BTC-USD` | repeated depth tags retained in wire order |
| market, `10:30:00.350Z` | `FIX / INSTRUMENT / d` | `US0378331005` | `969=.01`, `561=1`, `107=Apple Inc.` |
| market, `10:30:00.350Z` | `REKEP / INSTRUMENT / d` | `AAPL` | normalized lifecycle envelope and ordered registry fields |
| misc, `10:30:00.800Z` | `MISC` | `HealthMonitor` | heartbeat retained verbatim |
| unknown, `10:30:00.900Z` | `OTHER` | `ExperimentalAdapter` | opaque payload retained verbatim |

The raw Security Definition remains lossless. `parse_logs` infers 4.4 from its
`BeginString`, prepares that registry once, versions the Instrument, and stores
the normalized result beside the raw line. `flatten_instruments` then performs
only the exact `Log` to `Instrument` projection.

### Instruments

| Symbol | Venue / currency | Enrichment | `xhash` |
| --- | --- | --- | --- |
| `BTC-USD` | `XCME / USD` | synthetic market identity | `7683678321830537938` |
| `AAPL` | `XNAS / USD` | ISIN `US0378331005`, tick `0.01`, lot `1`, label `Apple Inc.` | `-9052458260103799025` |
| `MSFT` | `XNAS / USD` | synthetic market identity | `-1664556628408186290` |

### Books

| Event time | Instrument | State | Best bid / ask |
| --- | --- | --- | --- |
| `10:30:00.010Z` | `BTC-USD` | `OPEN` | `100 x 5` / `100.5 x 7` |
| `10:30:00.400Z` | `AAPL` | `CLOSED` | none / none; rejected delta retained |
| `10:30:00.600Z` | `MSFT` | `OPEN` | none / `200 x 8` |

The BTC book relates both source order lifecycles with their event times in
`linked_events` and both source versions in `parent_hash`. Bid and ask levels
remain best-price ordered.

### Orders

| Order | State | Side, price, quantity | Lineage / reason |
| --- | --- | --- | --- |
| `BAD-AAPL` | `INTERNAL_REJECTED` | buy limit, null, `10` | parent Book version; `rejected for book: required price is missing or non-finite` |
| `CA1 / OA1` | `INTERNAL_EXPIRED` | buy limit, `150`, `0` | preceding observation time retained; live-age expiry |
| `CM1 / OM1` | `FILLED` | sell limit, `200`, `0` | lifecycle closed by execution report |

### Executions

| Execution | State | Side, price, quantity | Order link |
| --- | --- | --- | --- |
| BTC depth trade | `FILLED` | trade, `100.5`, `3` | source depth lifecycle |
| `EA1` | `FILLED` | buy, `150`, `4` | `OA1 / CA1`, timed order link |
| `EM1` | `FILLED` | sell, `200`, `8` | `OM1 / CM1`, timed order link |

Flattening appends the carrying `Book.hash` to each event's `parent_hash`;
`linked_events` retains the event time and lifecycle linkage between executions
and orders.

## Schema lineage

![Schema lineage from logs to instruments, books, orders, and executions](assets/schema-lineage.svg)

| Contract | Fields | Primary key | Partitions | Nested payloads |
| --- | ---: | --- | --- | --- |
| `Log` | 105 | `unix, hash` | `unix_hour` | `fix_tags`, `fix_miss_tags`, `keyval`, `parties` |
| `Instrument` | 36 | `unix, hash` | `unix_hour`, `xhash` | `alt_ids`, `legs` |
| `Book` | 52 | `unix, hash` | `unix_hour`, `instrument_xhash` | levels, deltas, executions, live snapshot orders |
| `Order` | 39 | `unix, hash` | `unix_hour`, `instrument_xhash` | standard event lineage and metadata |
| `Execution` | 41 | `unix, hash` | `unix_hour`, `instrument_xhash` | standard event lineage and metadata |

The YAML contracts under `schemas/rekep/` are the portable source. Arrow owns
types and metadata between stages, Iceberg owns table ids and snapshots, and
the [binary identity contract](identity.md) keeps hashes cross-language stable.
