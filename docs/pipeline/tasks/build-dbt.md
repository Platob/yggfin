# build_dbt

`build_dbt` runs the dbt project under [`data/dbt`](#the-project) and commits
each of its products back into Iceberg. dbt owns the SQL; every read, schema
and commit stays on the same `IcebergDataset` the three ingestion tasks write
through.

## Task document

```json
{
  "name": "build_dbt",
  "application": "build_dbt.py",
  "parameters": {
    "project": "data/dbt",
    "profiles": null,
    "target": null,
    "select": null,
    "catalog": null,
    "log_level": "INFO"
  }
}
```

| parameter | default | meaning |
| --- | --- | --- |
| `project` | `data/dbt` | the dbt project directory, relative to the working directory |
| `profiles` | `null` | where `profiles.yml` is; `null` is the project itself |
| `target` | `null` | the profile target to build; `null` is the profile's own |
| `select` | `null` | dbt selection, one string or a list; `null` builds everything |
| `catalog` | `null` | the catalog to read and commit through; `null` is the profile's own |
| `log_level` | `INFO` | the level this package's records are written at |

`catalog` is the same mapping every other task document spells. The task hands
it to dbt as `REKEP_DBT_CATALOG`, which the plugin reads ahead of the profile,
so a deployment configures dbt the way it configures every other task and the
checked-in project keeps its local default.

## How a model reaches Iceberg

```mermaid
flowchart LR
    F[("fix.silver")] --> P["rekep.dbt load()"]
    P --> D["DuckDB :memory:"]
    D --> S["staged Parquet"]
    S --> C["rekep.dbt store()"]
    C --> O[("orders.events")]
    C --> R[("orders.current")]
    C --> X[("executions.fills")]
```

dbt-duckdb reaches a store that is not DuckDB through a plugin, and this
repository's plugin is `rekep.dbt`. A source is one Iceberg read, projected and
filtered in scan planning and handed to DuckDB as one Arrow table. A model is
built as a DuckDB table, staged as one Parquet file, and read back as an Arrow
stream that `IcebergDataset` commits -- so a commit holds a batch of a model at
a time and not the whole of it.

The database is `:memory:`. DuckDB is the compute and holds nothing between
runs: every source is read out of Iceberg at the start of a build and every
product is committed back at the end of one, so a build leaves no second copy
of the warehouse behind.

`iceberg` is the materialization this project declares, in
`data/dbt/macros/iceberg.sql`. It is not dbt-duckdb's own `external`: that one
writes a row of nulls for an empty model so the file always has a schema, and a
row of nulls is not a row this would commit.

## Products

| model | table | grain | key | mode |
| --- | --- | --- | --- | --- |
| `orders_events` | `orders.events` | one normalized order event | `eventkey` | overwrite |
| `orders_current` | `orders.current` | latest settled state per order | `orderkey` | overwrite |
| `executions_fills` | `executions.fills` | one economic execution occurrence | `executionkey` | overwrite |

An identity the parser already named is reused rather than computed again:
`orderkey` is `crossuuid`, the identity over the chain the message states, and
`eventkey` is `curruuid`, the event's own. A digest is written only where SQL
has to name something the parser had no word for -- `executionkey` over the
chain and the venue execution id -- and it is MD5, which is what DuckDB spells.

- `orders.events` takes a message that carries an order identity and an order
  lifecycle fact. A message carrying one and not the other stays in
  `fix.silver` rather than being assigned a guessed order, and an unknown
  chain -- an empty `crosscode` -- is not an order.
- `orders.current` is folded from `orders.events` alone, never from FIX. The
  winning event is the latest `eventtime`, then the latest source position, so
  a late event changes the row only through that ordering and the same events
  in any arrival order fold to the same row.
- `executions.fills` takes a message that states both a quantity and a price
  for what it executed. A status-only report states neither and does not become
  a zero fill. A bridge relays one execution into every chain it belongs to and
  logs the copy it received beside the copy it sent, so the occurrence is
  scoped by its chain and the earliest source position wins.

## What a model declares

A model's own configuration is its Iceberg declaration; everything else is the
Arrow schema DuckDB staged, so a column a model stops selecting stops being
written.

| key | meaning |
| --- | --- |
| `table` | the Iceberg table to commit to; `{schema}.{identifier}` by default |
| `mode` | `append` adds every row the model built; `overwrite` replaces the rows whose keys match -- or the partitions the model touches, when it has no key -- and adds the rest |
| `primary_key` | the identifier columns; each becomes non-null, and the merge is on them |
| `not_null` | further columns a row must carry |
| `partition_by` | column to Iceberg transform, such as `{'timepartition': 'day'}` |
| `sort_by` | columns each committed chunk is sorted by |
| `arrow_types` | storage types SQL cannot spell, such as `fixed_size_binary[16]` |
| `merge_schema` | add columns a model grew to the table before its rows land; `true` by default |
| `branch` | the Iceberg branch to commit on |
| `batch_row_size` | rows one staged batch carries back out of the file |

It is `arrow_types` and not `column_types` because dbt owns that name for a
seed's own casts. A column named in any of these that the model does not select
fails the commit rather than being ignored.

`merge_schema` adds a column a model grew, which is the one shape change an
Iceberg table takes in place. A key, a partition, a sort order or a narrowed
nullability is fixed when the table is created, so changing one of those in a
model changes the next table created from it and not the one already there:
drop the table and let the next build make it again.

## What a source declares

```yaml
sources:
  - name: fix
    schema: fix
    meta:
      plugin: rekep
    tables:
      - name: silver
        meta:
          table: fix.silver
          columns: [sourceurl, rownum, curruuid, crosscode]
```

| key | meaning |
| --- | --- |
| `plugin` | `rekep`, which is what makes the source an Iceberg read |
| `table` | the Iceberg table; `{schema}.{identifier}` by default |
| `columns` | the projection pushed into scan planning |
| `row_filter` | a PyIceberg filter expression, pushed down with it |
| `snapshot_id`, `branch` | the state to read |
| `limit` | a row cap |

DuckDB takes a table, so a source is read into memory: a large one is narrowed
by `columns`, `row_filter` and `limit` rather than read whole. The shipped
project projects the 44 columns its products read out of the 123 a FIX row
carries.

The source is `fix.silver` and never `fix.bronze`, though both are declared:
a product needs the chain -- the step an event follows, the state its chain
reached -- and only the walked rows carry one; bronze holds the same events
with `seqnum` and `prevuuid` empty on every row, so a product built on it
would fold every order from its first event alone. A market fact is FIX's own
field, and the staging model `stg_fix_messages` restates the products' reading
of it off those fields: `px` is `coalesce(price, lastpx, avgpx)`, `qty` is
`coalesce(orderqty, lastqty, cumqty, leavesqty)`, `symbolticker` is
`nullif(symbol, '[N/A]')`, `isincode` is `securityid` where
`lower(securityidsource) in ('4', 'isin')`, `miccode` is
`coalesce(securityexchange, exdestination, lastmkt)`, and `eventtime` is
`coalesce(transacttime, currunix)`.

## Run it

```bash
uv run --project python rekep task run tasks/build_dbt/build_dbt.json
```

Relative locations -- the project, the staging directory, the local catalog and
warehouse -- are spelled from the repository root, which is where a run starts.
The task prints one result and nothing else: dbt's own console is silent and
its events are relayed into this package's records, so `stdout` carries the
result a route reads.

dbt on its own reads the same project and the same profile:

```bash
uv run --project python dbt build --project-dir data/dbt --profiles-dir data/dbt
```

Narrow a run the way dbt does:

```bash
uv run --project python rekep task run tasks/build_dbt/build_dbt.json \
  --parameter 'select=["orders_events+"]'
```

Airflow runs the same document. The [`rekep_products`](../airflow.md#the-products-dag)
DAG is scheduled on the `fix.silver` Asset the ingestion DAG publishes, so a
build starts when `parse_fix_silver` writes.

## What the result says

```json
{
  "task": "build_dbt",
  "read": 29,
  "written": 66,
  "skipped": 0,
  "sources": {"project": "data/dbt"},
  "targets": {
    "orders_events": "orders.events",
    "orders_current": "orders.current",
    "executions_fills": "executions.fills"
  },
  "window": {"start": null, "end": null},
  "elapsed_ms": 2417,
  "models": 4,
  "tests": 25,
  "warned": [],
  "rows": {"orders.events": 49, "orders.current": 9, "executions.fills": 8}
}
```

A build's unit of work is a node, so `read` is the nodes dbt ran -- models,
tests and all -- and `skipped` is the nodes it skipped. `written` is rows: what
the plugin committed, and `rows` says which table each went to. Every shipped
model is an overwrite, and an overwrite states the rows it carried: a replay of
the same capture builds the same rows and lands them over the ones it landed
before, so the count is what the build produced and not what changed in the
table. An append would state the rows it added, which is the same number.

A failing node fails the task, and the record names it. Every model's own
tests run in the same build: `dbt build` runs a model and then the tests
attached to it, so a product that broke its key is reported by the run that
built it. A test the project declares as a warning is a quality signal rather
than a failure, and `warned` names the ones that fired. Two are declared that
way: `every_fill_belongs_to_a_known_order`, because a capture that starts
mid-stream holds executions whose order was accepted before its first line, and
the accepted values of `state`, because the normalized vocabulary is the
codec's and this repository cannot enumerate it -- a state the products have no
reading for is worth reporting and is not a reason to stop. What the folds do
read is pinned against the codec itself in `python/tests/test_dbt.py`.

## Sample rows

The sample is chain `e7254b12:9f03166699` of `python/tests/data/ulbridge.log`,
a partial fill and the fill that closed the order: ten lines, as `build_dbt`
lands them in `orders.events`, `orders.current` and `executions.fills`. An
identity is shown by its last eight hex digits behind a leading `…`, and the
stored value is sixteen bytes; a null is an empty cell.

--8<-- "docs/pipeline/tasks/samples/build-dbt.md"

The first table is the order's ten events in `eventtime` then `rownum` order.
`eventtime` is what [the staging model](#what-a-source-declares) reads off
`transacttime`, so the eight bridge lines sit at `12:46:39.743`, row 6, the
received frame with microseconds on its tag, comes last at `12:46:39.743016`,
and row 35 comes first at `2026-08-14 00:00:00.000`, because its
`TransactTime(60)` was a bare date, as
[`parse_fix_bronze`](parse-fix-bronze.md#sample-rows) shows. `prevuuid` and
`seqnum` are as the walk left them, and they read as two successions: row 7
is a head, rows 8, 9, 10, 11, 15 and 22 are its steps 1 to 6, and row 36 is
step 7, following `…f16b9d55`, row 22's key; row 6 is a head, and row 35 is
its step 1, following `…91130359`. The walk that gave them those steps is on
[`parse_fix_silver`](parse-fix-silver.md#walk-the-chains). `cumqty` and
`leavesqty` read `340` and `260` until the fill's rows, 35 and 36, where they
read `600` and `0` and `state` reads `80FILLED`. `clordid` is empty on those
two rows, because neither line states a `ClOrdID(11)`, and `avgpx` is empty
on row 36.

The second table is the one row the ten events fold to. `orderkey` is
`…b7b57111`, the chain's `crossuuid`, and `eventcount` is `10`.
`last_eventkey` is `…91130359`: row 6's `12:46:39.743016` is the latest
`eventtime` of the ten, later than the fill's midnight, so row 6 wins the
fold and the row reads `state` `40PARTFILL`, `cumqty` `340` and `leavesqty`
`260` although the venue reported the order filled. That is what the capture
states, and reading a bare-date `TransactTime` as no instant is not done
here. `openedat` is `2026-08-14 00:00:00.000`, the earliest `eventtime`,
since no event of this order is in an opening state; `closedat` is
`12:46:39.743`, the latest terminal event, row 36's; `updatedat` is row 6's
instant.

The third table is the two venue executions, one row each. `00011377089XEEA0`,
`21` at `83.08`, is keyed to row 7's event `…caf49857` and not to row 6's:
the earliest `eventtime` wins among the copies of one execution, and the
bridge's `.743` is earlier than the received frame's `.743016`.
`00011377090XEEA0`, `57` at `83.08`, is row 35's, at midnight, in state
`80FILLED`. Both read `isincode` `CH0012221716`, `miccode` `XSWX` and
`currency` `CHF`.

`tools/pipeline_samples.py` regenerates the file from a run over the fixture,
and the integration suite checks it with `--check`.

## The project

```text
data/dbt/
  dbt_project.yml     the project, its paths and its model defaults
  profiles.yml        DuckDB in memory, and the catalog the plugin opens
  macros/
    iceberg.sql       the materialization that commits a model
    rekep.sql         the digest and the normalized states more than one model reads
  models/
    sources.yml       the three published tables, and the projection the one a model reads is read under
    staging/          one narrowing of `fix.silver`, never published
    orders/           orders.events and orders.current
    executions/       executions.fills
  tests/              the checks that span two products
```

Nothing here needs `dbt deps`: there is no package file, and every macro a
model reads is in the checkout. A build writes `data/dbt/target/` -- its
compiled project, its run artifacts and the Parquet each model was staged as --
and `data/dbt/logs/`; neither is tracked. `DBT_TARGET_PATH` and `DBT_LOG_PATH`
move them, and the staging follows the target path, so a worker whose checkout
is read-only writes nothing into it. That is what the Airflow operator's
`environment` argument is for.

## What these products are not yet

The [roadmap](../../roadmap/index.md) specifies these three products as native
`Field` declarations with a replay test and a source-coverage report. These are
the SQL projection of that specification and state where they differ:

| planned | here | why |
| --- | --- | --- |
| `side`, `state`, `exectype` as fixed-width bytes | the normalized strings `fix.silver` carries | a storage width is a declaration, and this layer does not add one to a value it passes through |
| `parties`, `regulatorytimestamps` | not carried | a repeating group is in the arrival record, and a list of structs through DuckDB is a conversion this seam does not own |
| `originalexecutionkey`, `liquidity` | not carried | `ExecRefID` and `LastLiquidityInd` are not in the fixed projection yet |
| rejected-derivation counters | not carried | what a product left behind is still countable in `fix.silver`, but nothing publishes it |

`orders.current` is unpartitioned: one row per order is the whole table, and a
partition on a column that moves would rewrite a file every time an order
ticks.
