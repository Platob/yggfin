# End-to-end run

A correctness run measured 2026-08-29 executes the five notebook tasks against
the checked-in message fixture and a fresh local Iceberg warehouse, then
replays the same input. Throughput measurements live on
[Benchmarks](../../storage/benchmarks.md).

![End-to-end execution architecture](../../assets/workflow-run.svg#only-dark)
![End-to-end execution architecture](../../assets/workflow-run-light.svg#only-light)

## Run the workflow locally

From the repository root:

```bash
uv run --project python --with papermill rekep task run \
  tasks/parse_messages/parse_messages.yml \
  --parameter source=python/tests/data/app_messages_sample.txt \
  --output parse_messages.executed.ipynb

uv run --project python --with papermill rekep task run \
  tasks/parse_fix/parse_fix.yml \
  --output parse_fix.executed.ipynb

uv run --project python --with papermill rekep task run \
  tasks/parse_market/parse_market.yml \
  --output parse_market.executed.ipynb

uv run --project python --with papermill rekep task run \
  tasks/flatten_orders/flatten_orders.yml \
  --output flatten_orders.executed.ipynb

uv run --project python --with papermill rekep task run \
  tasks/flatten_executions/flatten_executions.yml \
  --output flatten_executions.executed.ipynb
```

The YAML selects the catalog, branch, tables and commit sizes. Repeatable
`--parameter NAME=VALUE` options override one run.

## Pinned results

The run used 4-row commits to cross storage boundaries. Its 11 source records
span FIX wire, FIXML, operational prose, a folded stack trace and unknown
fields.

| Notebook | First run | Replay writes |
| --- | --- | ---: |
| `parse_messages` | 11 read, 11 written | 0 |
| `parse_fix` | 11 read; 11 FixMsg and 1 Instrument written | 0 |
| `parse_market` | 2 Books written; 2 Orders and 1 Execution nested | 0 |
| `flatten_orders` | 2 projected, 2 written | 0 |
| `flatten_executions` | 1 projected, 1 written | 0 |

`parse_fix` routed 2 rows to `fix.market` and 9 to `fix.misc`; no
`fix.unknown` table was needed. The replay reported 12 skips: the 11 FixMsg
rows and the one canonical Instrument record.

| Iceberg table | Rows | Iceberg snapshots |
| --- | ---: | ---: |
| `logs.messages` | 11 | 1 |
| `fix.market` | 2 | 2 |
| `fix.misc` | 9 | 2 |
| `market.instruments` | 1 | 1 |
| `market.books` | 2 | 2 |
| `market.orders` | 2 | 2 |
| `market.executions` | 1 | 1 |

## Sampled output

The flat Instrument table holds one `TTF` record keyed by `symbolticker`, with
`xhash = -5992726579138353958`. `fix.market` contains only captured rows.

| Product | Selected rows |
| --- | --- |
| Book | `CLOSED` with one fill; then `OPEN`, bid `41.25 x 1200` |
| Order | `ORD-0000038106`: `PARTIALLY_FILLED` with 800 leaves; then `PENDING_NEW` at `41.25 x 1200` |
| Execution | `EXE-0000091233`: `FILLED`, buy `400 @ 41.25` |

The first order line supplies the resting price. The execution report supplies
the fill and remaining quantity without inventing a missing order price.

## Schema lineage

![Schema lineage from logs to instruments, books, orders, and executions](../../assets/schema-lineage.svg#only-dark)
![Schema lineage from logs to instruments, books, orders, and executions](../../assets/schema-lineage-light.svg#only-light)

Event products are keyed `(unix, hash)`; Instrument is keyed by
`symbolticker`. All six contracts are sorted by `unix` and partitioned on
`unixpartition` alone.

`schemas/rekep/` is the portable source. Arrow owns types and metadata between
stages, Iceberg owns table ids and snapshots, and the
[binary identity contract](../../contracts/identity.md) keeps hashes
cross-language stable.
