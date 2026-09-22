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

- The published native dependency is pinned to `yggdryl==0.1.9`; public
  applications and documentation import only `rekep`.

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
- Every capture a row header declares is named for what the native read fills
  from it -- one lowercase column for each of the bracket's own facts, named
  for the fact it holds, and the settled `currunix` for `mtime`, the record
  clock -- so `capture_names` alone tells the codec which bracket part is
  which. Never map a capture spelling onto a tag.
- `ULBRIDGE_ROWHEADER` is the default and the only one spelled here. A bridge
  writing the same facts in a layout of its own is read by naming its header
  in the task document, never by a second constant: the layout is a parameter
  and the capture names are the contract. `Message.text_options` refuses a
  header that renames or omits one, because the read drops a capture it fills
  nothing from in silence -- a table that lands complete, keyed and empty down
  one column, or one whose clock settled nothing.
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
- `type: s3tables` is the one catalog type resolved here, because a table
  bucket is served by an Iceberg REST catalog AWS hosts at two endpoints and
  the `warehouse` is what picks one: a bucket ARN is the S3 Tables endpoint
  signed for `s3tables`, and `<account>:s3tablescatalog/<name>` is the Glue
  endpoint signed for `glue`, under Lake Formation. The ARN states its region;
  the Glue name takes one from `rest.signing-region` or the AWS environment,
  and nothing guesses. Every other property stays as written. The service owns
  the files under a table bucket, so no sweep deletes one there and a drop
  purges.
- Maintenance reports settled changes and never deletes a file whose ownership
  is ambiguous.

## Workflow

The supported graph is:

```text
filesystem URI -> parse_messages   -> logs.messages
logs.messages  -> parse_fix_bronze -> fix.bronze
fix.bronze     -> parse_fix_silver -> fix.silver
fix.silver     -> build_dbt        -> orders.events, orders.current, executions.fills
```

Each task directory contains one Marimo application beside its JSON document.
`parse_messages` passes `filesystem` to `IOBase.from_uri`, frames each line
under the `rowheader` its document names, applies `Message.into_field()` to
each batch, keeps the lines whose `currunix` falls in the run's window,
and writes one schema-bearing reader directly to Iceberg. The window is
`[start, end)`; a task given neither takes the last day up to now, and a run
over a window replaces what an earlier run of it landed. `currunix` is the
instant the read settles over the line, read off the header's `mtime` capture,
and a line the header could not date settles at `EPOCH`, which every window
covers -- so a header that matched nothing loses no line, and the table is laid
out by the hour of that instant and nothing beside it. `logs.messages` is
keyed on `curruuid` alone, the line identity the native read states;
`currhashcode` remains its exact-content code and is not a second key. Nothing
here computes a digest beside it. A raw text row names its source
through Yggdryl `sourceurl` and `rownum`, and itself through `curruuid`, the
line's own identity the native read states. A message parsed out of a stored
line records that identity in `srcuuids`, which joins to the raw row's
`curruuid`; it is provenance, never lineage, and no walk changes what the
identity means.

The two FIX stages one codec exposes are two tasks over two tables, in this
order and no other:

```text
parse -> fix.bronze, lifecycle -> fix.silver
```

`parse_fix_bronze` reads the stored rows of the same window off `currunix`,
the event the read settled over each line, and parses them, and only that: a
bronze row is what the message implied about itself, and `seqnum`, `prevuuid`
and `parentuuids` are empty on every one because nothing has walked yet.
`parse_fix_silver` reads the previous
hour and the run's window from `fix.bronze`, including undated epoch rows, in
`currunix, seqnum, curruuid` order. The previous hour is context only: it lands
the job window plus still-undated rows and excludes future expiry events. This
bounded history does not claim arbitrary old-chain completeness. It reads each
row back as the message that wrote it, walks the chains, and lands the walked
rows. The walk reads the fixed row alone. A silver row differs from its
bronze twin in what the walk filled -- its place, its lineage, the folded
`creaunix`, `exprtime` and `state` -- and in the identity those re-settle to;
a duplicate is not a successor, and the walk gives every copy of one message
the same place, the same lineage and the same state.

Both tables use the native `fix_message_field(codec)` field directly,
without a yggfin FIX model. Parse, storage, reconstruction,
and lifecycle all use the same 123-column **FixMsg** contract. `sourceurl`,
`rownum`, `msgthreadid`, `loglevel` and `body` remain only in `logs.messages`;
the bridge's `msgsessionid`, `msgctxid`, `msgseqnum` and `msgpluginid` are
native FixMsg fields a raw line fills, so they stand on both shapes under one
spelling and nothing translates between them. `srcuuids` joins a FIX row to
the raw row's `curruuid`.
The native row contains 29 crate fields; code vocabularies live centrally and
fields reference them through `FIX:codeset`. `fixentries` is residual and does
not duplicate successfully lifted scalars or complete groups. A reconstructed
row promises canonical message semantics, not arrival pair order or bytes.
A FIX row is a message and not a line -- a line carrying two frames
answers two and a line carrying none answers none -- and a message logged
again at every hop it passes is one event, so both tables are keyed on
`curruuid` alone, laid out by the hour of `currunix` alone, and sorted within a
partition by `currunix, seqnum, curruuid`. Each is declared once, on the field,
and nothing else carries either mark. A replay of a window lands the same
rows under the same key.

`crosscode` takes the first non-empty `OrderID`, `ClOrdID`, `OrigClOrdID`,
`QuoteID`, `QuoteReqID`, or `MDReqID`. Capture session and context instead form
`identifiers["msgsectxid"]` when both exist. Default absence spellings are
empty text, `null`, `<null>`, `none`, `n/a`, and `[n/a]`, trimmed and compared
case-insensitively.

The codec is the whole parse surface: the dictionary and the instant an undated
message takes are pinned on it once, and each stage after it is a call rather
than another pin. Parsing and local enrichment are independent per event and
may use `threads`; the default is the available CPU count and zero becomes one.
Only lifecycle enrichment owns cross-event state and order. `snapshot_ns`
defaults to zero (off), and a positive value emits owned lifecycle snapshots
on that nanosecond grid. A capture order is pinned only where a door resolves one by
position, which is the line door. A version is not among the pins -- what a message was
read at is what its own `beginstring` said. Native construction validates
every keyword; both FIX tasks expose `codec_options`, where `null` delegates
native defaults and an object is forwarded unchanged. Useful pins include
`batch_row_size`, `include_msgtypes`, `exclude_msgtypes`, `threads`, and
`snapshot_ns`. The doors are named for
their stage: `fix_parse_*` and `fix_lifecycle_*`, a line door and a batch door each.

The silver Iceberg scan prunes with `fix_window_filter`, requests
`SORT_COLUMNS`, and merges at most 16 overlapping file streams at once. It
does not form a Python `read_all` union. Native 0.1.8 lifecycle processing
still collects and stable-sorts its finite scan result. Undated rows read from
the epoch partition may accumulate, so this path is not batch-memory-bounded.

`build_dbt` runs the dbt project under `data/dbt`. dbt owns the SQL a product
is written in and nothing else: `rekep.dbt` is the one seam, a source is one
`IcebergDataset` read and a model is one commit through the same dataset, and
the DuckDB database is `:memory:` because Iceberg holds the state. A model's
`config()` block is its Iceberg declaration -- table, key, partition, sort
order and the storage types SQL cannot spell -- so no second Field, catalog or
warehouse is declared anywhere under `data/dbt`. A product reads `fix.silver`
and never `fix.bronze`, because a product needs the chain and bronze carries
none; a market fact is FIX's own field, and the staging model restates the
products' reading of it off those fields.

Airflow launches the adjacent standalone runner through the locked `uv`
`runner` group; the operator never calls the Rekep CLI. `rekep_ingestion` is
the three streaming stages, daily, each run over its own data interval unless
the run's conf names `start` or `end`; `rekep_products` is `build_dbt`,
scheduled on the `fix.silver` Asset the first one publishes last.

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
  fix.py        the bundled registry and the two FIX stages over two tables
  times.py      instant readings, the run window and the ULBridge row header
  resources.py  Yggdryl binding and required byte reads
  dbt.py        the dbt-duckdb plugin: a source is a read, a model is a commit
tasks/
  airflow/
  parse_messages/
  parse_fix_bronze/
  parse_fix_silver/
  optimize_iceberg/
  build_dbt/
data/dbt/       the dbt project: models, schemas, macros and its one profile
schemas/rekep/message.json
schemas/rekep/fixmsg.json
```
