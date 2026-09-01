# End-to-end run

A correctness run measured 2026-08-30 executes the six notebook tasks against
the checked-in message fixture and a fresh local Iceberg warehouse, then
replays the same input. Throughput measurements live on
[Benchmarks](../../storage/benchmarks.md).

![End-to-end execution architecture](../../assets/workflow-run.svg#only-dark)
![End-to-end execution architecture](../../assets/workflow-run-light.svg#only-light)

## Deploy the tables

Every task creates its own target on the first write, so a run against an
empty catalog needs nothing done first. Where the catalog is not the runner's
to write to -- a Glue catalog over an S3 warehouse, deployed once by whoever
owns the account -- create them ahead of the jobs instead:

```bash
uv run --project python rekep iceberg deploy tasks/parse_fix/parse_fix.yml
```

The task document supplies the catalog, its properties and the branch, so a
deployment lands where the pipeline will write; `--catalog`, `--property
NAME=VALUE`, `--table-property NAME=VALUE` and `--branch` override any of
them, `--table` restricts the run to one table, and `--dry-run` reports which
tables are missing without creating any. It is idempotent: a table already in
the catalog is left as it is, properties included, and reported `present`.
`rekep.deploy.TABLES` is the declared layout it reads.

## Run the workflow locally

From the repository root. The `runner` dependency group is Papermill and the
kernel it executes a notebook under:

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_messages/parse_messages.yml \
  --parameter source=python/tests/data/app_messages_sample.txt \
  --output parse_messages.executed.ipynb

uv run --project python --group runner rekep task run \
  tasks/parse_fix/parse_fix.yml \
  --output parse_fix.executed.ipynb

uv run --project python --group runner rekep task run \
  tasks/parse_instruments/parse_instruments.yml \
  --output parse_instruments.executed.ipynb

uv run --project python --group runner rekep task run \
  tasks/parse_market/parse_market.yml \
  --output parse_market.executed.ipynb

uv run --project python --group runner rekep task run \
  tasks/flatten_orders/flatten_orders.yml \
  --output flatten_orders.executed.ipynb

uv run --project python --group runner rekep task run \
  tasks/flatten_executions/flatten_executions.yml \
  --output flatten_executions.executed.ipynb
```

The YAML selects the catalog, branch, tables and commit sizes. Repeatable
`--parameter NAME=VALUE` options override one run. The same six commands run
unchanged against S3 — only the `source`, `fix_dictionary` and
`catalog.properties` values in the YAML move; see
[AWS S3](deploy.md#aws-s3). Each command writes the
run's records to `stderr` as they happen and the task's result to `stdout`;
[Logs](logs.md) has the record a stage opens and closes with, and the keys
every result carries. The shipped documents write
a SQLite catalog to `data/catalog.db` and a file warehouse to `data/warehouse`,
both ignored by git along with the executed notebooks -- delete them for a
clean run.

## Pinned results

The run used 4-row commits to cross storage boundaries. Its 11 source records
span FIX wire, FIXML, operational prose, a folded stack trace and unknown
fields.

| Notebook | First run | Replay writes |
| --- | --- | ---: |
| `parse_messages` | 11 read, 11 written | 0 |
| `parse_fix` | 11 read, 11 FixMsg written | 0 |
| `parse_instruments` | 1 observed, 1 written | 0 |
| `parse_market` | 2 Books written; 2 Orders and 1 Execution nested | 0 |
| `flatten_orders` | 2 projected, 2 written | 0 |
| `flatten_executions` | 1 projected, 1 written | 0 |

`parse_fix` routed 2 rows to `fix.market` and 9 to `fix.misc`; no
`fix.unknown` table was needed and no row carried a transcription error. It
resolved `unix` from `SendingTime` on 2 rows, from `TransactTime` on 1, and
fell back to the recording clock on the other 8. Five of the 11 rows carried a
`symbolticker`. The replay wrote nothing at any stage: 11
FixMsg rows skipped, and the one canonical `InstrumentUpdate` unchanged.

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

The InstrumentUpdate table holds one `TTF` component keyed by its top-level
sixteen-byte `xhash`, versioned out of `fix.market` by `parse_instruments`.
Its readable identity is nested at `instrument.symbolticker`; `fix.market`
itself contains only captured rows.

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

Event products are keyed `(unix, hash)` except `InstrumentUpdate`, whose
current row is keyed by its sixteen-byte `xhash`. `unixpartition` is the
event-table partition; the pipeline does not add an automatic write sort.

`schemas/rekep/` is the portable source. Arrow owns types and metadata between
stages, Iceberg owns table ids and snapshots, and the
[binary identity contract](../../contracts/identity.md) keeps hashes
cross-language stable.
