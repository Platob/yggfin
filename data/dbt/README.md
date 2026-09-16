# The dbt project

This is the SQL half of the pipeline: `fix.messages` in, three business
products out. DuckDB runs the SQL and holds nothing — the database is
`:memory:` — and every read and every commit goes through `rekep.dbt`, the
plugin that binds dbt to the same `IcebergDataset` the ingestion tasks write
through. There is no second catalog, no second warehouse and no extract.

```text
fix.messages -> stg_fix_messages -> orders.events   -> orders.current
                                 -> executions.fills
```

| model | table | grain | key | mode |
| --- | --- | --- | --- | --- |
| `orders_events` | `orders.events` | one normalized order event | `eventkey` | append |
| `orders_current` | `orders.current` | latest settled state per order | `orderkey` | overwrite |
| `executions_fills` | `executions.fills` | one economic execution occurrence | `executionkey` | append |

`stg_fix_messages` is a view: it narrows the source and settles the two
readings both products share — the event's time and the session it was seen
on — and is never published.

Every model's key is tested as one, `unique` and `not_null`, beside the column
documentation in each `schema.yml`. `tests/` holds the one check that spans two
products — a fill whose chain produced no order event — and it warns rather
than fails: a capture that starts mid-stream holds executions whose order was
accepted before its first line.

## Run it

From the repository root, because every relative location here is spelled from
there:

```bash
uv run --project python rekep task run tasks/build_dbt/build_dbt.json
```

That is the repository's own route: it runs this project, reports what each
model committed, and fails on a failing model or a failing test. dbt on its own
reads the same project and the same profile:

```bash
uv run --project python dbt build --project-dir data/dbt --profiles-dir data/dbt
uv run --project python dbt build --project-dir data/dbt --profiles-dir data/dbt \
  --select orders_events+
```

A build needs `logs.messages` and `fix.messages` to exist, which is what
`parse_messages` and `parse_fix` write. Nothing needs `dbt deps`: there is no
package file, and every macro a model reads is here.

## Configure it

`profiles.yml` names the local SQLite catalog and file warehouse a clone
already has. `REKEP_DBT_CATALOG` replaces it with the JSON mapping every task
document spells, which is how `build_dbt` passes a deployed catalog through:

```bash
REKEP_DBT_CATALOG='{"name": "rekep", "properties": {"type": "glue", "warehouse": "s3://bucket/warehouse"}}' \
  uv run --project python dbt build --project-dir data/dbt --profiles-dir data/dbt
```

A model's `config()` block is its Iceberg declaration — `table`, `mode`,
`primary_key`, `not_null`, `partition_by`, `sort_by` and `arrow_types` — and a
source's `meta` block is its Iceberg read: `table`, and the `columns`,
`row_filter`, `snapshot_id`, `branch` and `limit` pushed into scan planning.
Both are documented on the [Build dbt](../../docs/pipeline/tasks/build-dbt.md)
page, beside what these products do not carry yet.

## What a build leaves

`target/` holds the compiled project, the run artifacts and the Parquet file
each model was staged as on its way into Iceberg; `logs/` holds dbt's own log.
Neither is tracked, and `DBT_TARGET_PATH` and `DBT_LOG_PATH` move them.
