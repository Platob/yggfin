# End-to-end run

A correctness run measured 2026-09-01 executes the six task applications
against the checked-in message fixture and a fresh local Iceberg warehouse,
then replays the same input. Throughput measurements live on
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

Prepare the environment once. The `runner` dependency group is Marimo and the
catalog extras a task imports:

```bash
uv sync --project python --locked --group runner
```

Then, from the repository root:

```bash
uv run --project python --group runner rekep task run \
  tasks/parse_messages/parse_messages.yml \
  --parameter source=python/tests/data/app_messages_sample.txt

uv run --project python --group runner rekep task run \
  tasks/parse_fix/parse_fix.yml

uv run --project python --group runner rekep task run \
  tasks/parse_instruments/parse_instruments.yml

uv run --project python --group runner rekep task run \
  tasks/parse_market/parse_market.yml

uv run --project python --group runner rekep task run \
  tasks/flatten_orders/flatten_orders.yml

uv run --project python --group runner rekep task run \
  tasks/flatten_executions/flatten_executions.yml
```

The YAML selects the catalog, branch, tables and commit cadence. Repeatable
`--parameter NAME=VALUE` options override one run. The same six commands run
unchanged against S3 — only the `source`, `fix_dictionary` and
`catalog.properties` values in the YAML move; see
[AWS S3](deploy.md#aws-s3). Each command writes the
run's records to `stderr` as they happen and the task's result to `stdout`;
`--result-file PATH` publishes that same JSON document atomically, and is what
`MarimoOperator` reads. [Logs](logs.md) has the record a stage opens and closes
with, and the keys every result carries. The shipped documents write
a SQLite catalog to `data/catalog.db` and a file warehouse to `data/warehouse`,
both ignored by git -- delete them for a clean run.

## Pinned results

The run used the default eight-batch commit cadence. Its 11 source records
span FIX wire, FIXML, operational prose, a folded stack trace and unknown
fields.

| Task | First run | Replay writes |
| --- | --- | ---: |
| `parse_messages` | 11 read, 11 written | 0 |
| `parse_fix` | 10 read, 10 FixMsg written | 0 |
| `parse_instruments` | 1 observed, 1 written | 0 |
| `parse_market` | 2 Books written; 2 Orders and 1 Execution nested | 0 |
| `flatten_orders` | 2 projected, 2 written | 0 |
| `flatten_executions` | 1 projected, 1 written | 0 |

`parse_fix` left the `35=0` heartbeat in `logs.messages`, routed 2 rows to
`fix.market` and 8 to `fix.misc`; no
`fix.unknown` table was needed and no row carried a transcription error. It
resolved `unix` from `SendingTime` on 1 row, from `TransactTime` on 1, and
fell back to the recording clock on the other 8. Five of the 10 rows carried a
`symbolticker`. The replay wrote nothing at any stage: 10 FixMsg rows were
skipped and the one canonical `InstUpdate` was unchanged.

| Iceberg table | Rows | Iceberg snapshots |
| --- | ---: | ---: |
| `logs.messages` | 11 | 1 |
| `fix.market` | 2 | 1 |
| `fix.misc` | 8 | 1 |
| `market.instruments` | 1 | 1 |
| `market.books` | 2 | 1 |
| `market.orders` | 2 | 1 |
| `market.executions` | 1 | 1 |

Every one of the seven tables holds exactly one snapshot, the commit the first
run made; the replay adds none.

## Sampled output

The InstUpdate table holds one `TTF` component keyed by its top-level
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

Event products are keyed `(unix, hash)` except `InstUpdate`, whose
current row is keyed by its sixteen-byte `xhash`. `unixpartition` is the
event-table partition; the pipeline does not add an automatic write sort.

`schemas/rekep/` is the portable source. Arrow owns types and metadata between
stages, Iceberg owns table ids and snapshots, and the
[binary identity contract](../../contracts/identity.md) keeps hashes
cross-language stable.
