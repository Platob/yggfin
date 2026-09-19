# Coding patterns

Optimize Rust core behavior in Yggdryl first, then Python and JavaScript
bindings, then yggfin documentation. Keep one obvious implementation per
behavior.

## Writing

- Write for an agent searching for one fact.
- Prefer deletion to compatibility layers or deprecation.
- Keep docstrings synthetic: what the object is, then only hidden constraints.
- Put a contract beside its owner and link to it instead of repeating it.
- Keep examples executable and prose connective.

## Ownership

- Yggdryl owns `Field`, scalar compilation, resource binding, filesystems,
  streams, codecs, decompression, text media, FIX registries, FIX batch
  parsing, the fixed `fixmsg` row, and the lifecycle stage after the parse.
- Arrow owns columnar shape conversions and kernels.
- PyIceberg owns table conversion, ids, snapshots, scan planning, and commits.
- Yggfin owns the raw `Message` contract and its narrow PyArrow/PyIceberg seam.
- Never add a second Field class, filesystem/path layer, text reader, codec, or
  registry in yggfin.

The deleted Rekep FIX and market implementation is not a compatibility target.

## Fields and Arrow

- `rekep.Field is yggdryl.Field`.
- Use native `@yggdryl.scalar` and `Annotated` options for declarations.
- Arrow schema metadata is authoritative; portable JSON derives from it.
- `Field` JSON is the runtime declaration form; `iceberg_contract` is the
  published one, and it states only what an Iceberg schema, spec and sort
  order can.
- Use Yggdryl `Field.apply_arrow_*` at producer and consumer boundaries so
  cast, derived partitions, and digests run in their native order.
- Use Yggdryl's strict nullability policy directly at every Arrow boundary.
- Do not use Python row loops for Arrow shape conversion.

## Resources and text

- Bind paths and URIs with `IOBase`; preserve injected Arrow filesystem
  identity and opaque paths.
- `IOBase` and `TextOptions` own traversal, header capture, decompression, and
  physical-line batching.
- A raw text row names its source only through Yggdryl `sourceurl` and
  `rownum`.
- Every capture a row header declares is named for the column it fills, so
  `capture_names` alone tells the codec which bracket part is which. Never map
  a capture spelling onto a tag.
- `ULBRIDGE_ROWHEADER` is the default and the only one spelled here. A bridge
  writing the same facts in a layout of its own is read by naming its header
  in the task document, never by a second constant: the layout is a parameter
  and the capture names are the contract. `Message.text_options` refuses a
  header that renames or omits one, because the read drops a capture no column
  holds in silence and the table lands complete, keyed, and empty down one
  column.
- Streams open one leaf at a time with bounded transport read-ahead and
  row-bounded batches. One record is unbounded until Yggdryl provides an
  error-on-overflow byte limit that preserves exact bodies.
- Never stage a remote file locally as the production path.
- Size parameters state their unit (`batch_row_size`, `read_byte_size`).

## Iceberg

- Primary APIs consume and return `RecordBatchReader`; table helpers explicitly
  require memory-sized data.
- `append_*` is blind; `overwrite_*` replaces: each bounded chunk is staged
  locally, the stored rows it replaces are taken out -- those carrying its keys
  in the same transformed partition, or every row of the partitions it touches
  when `merge_by` names nothing -- and the staged files are appended, in one
  commit. Both create a missing table and return the rows they wrote.
- `merge_by=True` means the native Field's declared primary key. A key is
  scoped to its partition: the same key on two days is two rows.
- Commit after `commit_batch_num` input batches or the earlier optional
  `commit_row_size` bound.
- Every write streams one transformed partition at a time through PyIceberg's
  file-format writer on the table's `FileIO` and commits it by path, so a
  commit holds its chunk and not a multiple of it, and no data file touches
  local disk. Never hand a whole chunk to a writer that splits it.
- An overwrite declares the rows it takes out as a predicate. PyIceberg
  validates a retried commit against it, so a concurrent commit elsewhere in
  the table lands and one under it is handed back for a fresh plan.
- Push filters, projections, ordering, and limits into storage planning.
- Every verb accepts `branch`; every read accepts `snapshot_id`.
- Preserve supplied Iceberg ids and assign missing ids.
- Keep PyIceberg's configured `FileIO` and native PyArrow streams at the table
  boundary. Standard `s3.*` catalog properties own endpoints and credentials.
- Maintenance reports settled changes and never deletes a file whose ownership
  is ambiguous.

## Workflow

The supported graph is:

```text
filesystem URI -> parse_messages -> logs.messages -> parse_fix -> fix.messages
fix.messages -> build_dbt -> orders.events, orders.current, executions.fills
```

Each task directory contains one Marimo application beside its JSON document.
`parse_messages` passes `filesystem` to `IOBase.from_uri`, frames each line
under the `rowheader` its document names, applies `Message.into_field()` to
each batch, keeps the lines whose `timepartition` falls in the run's window,
and writes one schema-bearing reader directly to Iceberg. The window is
`[start, end)`; a task given neither takes the last day up to now, and a run
over a window replaces what an earlier run of it landed. `logs.messages` is
keyed on `bodyhash`, the digest of the exact body bytes, so identical bytes are
one row whatever session carried them. `parse_fix` reads the stored rows of the
same window and passes them through the two stages one codec exposes, in this
order and no other:

```text
parse -> lifecycle
```

It writes `fix_schema_carrying(carrier, fix_schema(registry, "fixmsg"))` without
a yggfin FIX model: the dictionary's row, the capture's own columns in front,
and nothing else defined here. A FIX row is a message and not a line -- a line
carrying two frames answers two and a line carrying none answers none -- and a
message logged again at every hop it passes is one event, so `fix.messages` is
keyed on `curruuid`, laid out by the hour of `unix`, and sorted within a
partition by `unix, seqnum, curruuid`. The walk reads the fixed row alone: a
capture's own column beside it would be read as content and give every arrival
its own identity.

The codec is the whole parse surface: the dictionary and the instant an undated
message takes are pinned on it once, and each stage after it is a call rather
than another pin. A capture order is pinned only where a door resolves one by
position, which is the line door; the batch door fills from a column named
after the field, so `parse_fix` pins none and cannot go stale against a header
it never sees. A version is not among the pins -- what a message was read at
is what its own `beginstring` said -- and `fix_codec` refuses by name any
keyword that is not one of its seven.

`build_dbt` runs the dbt project under `data/dbt`. dbt owns the SQL a product
is written in and nothing else: `rekep.dbt` is the one seam, a source is one
`IcebergDataset` read and a model is one commit through the same dataset, and
the DuckDB database is `:memory:` because Iceberg holds the state. A model's
`config()` block is its Iceberg declaration -- table, key, partition, sort
order and the storage types SQL cannot spell -- so no second Field, catalog or
warehouse is declared anywhere under `data/dbt`.

Airflow launches the adjacent standalone runner through the locked `uv`
`runner` group; the operator never calls the Rekep CLI. `rekep_ingestion` is
the two streaming stages, daily, each run over its own data interval unless
the run's conf names `start` or `end`; `rekep_products` is `build_dbt`,
scheduled on the `fix.messages` Asset the first one publishes.

Every task result and its closing INFO record use `rekep.logs.Stage` and agree
on `task`, `read`, `written`, `skipped`, `sources`, `targets`, `window`, and
`elapsed_ms`.

## Tests and benchmarks

- Test reusable internals; pin the application contract once as integration.
- Mark long Iceberg transactions `integration`; default CI excludes them.
- Cross zero/one rows, batch bounds, nulls, retries, and alternate Arrow types.
- Compare optimized code with a reference before timing it.
- Keep focused component benchmarks. Do not add million-row development or
  duplicate pipeline benchmarks.

## Layout

```text
python/src/rekep/
  fields/       native Field metadata helpers
  iceberg/      catalog, dataset, schema bridge, and PyIceberg FileIO
  tasks/        application configuration only
  text/         raw Message declaration
  fix.py        the bundled registry and the three-stage pipeline surface
  times.py      instant readings, the run window and the ULBridge row header
  resources.py  Yggdryl binding and required byte reads
  dbt.py        the dbt-duckdb plugin: a source is a read, a model is a commit
tasks/
  airflow/
  parse_fix/
  parse_messages/
  optimize_iceberg/
  build_dbt/
data/dbt/       the dbt project: models, schemas, macros and its one profile
schemas/rekep/message.json
schemas/rekep/fix-message.json
```
