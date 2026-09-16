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
  parsing, and the enrichment and lifecycle stages after it.
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
- `append_*` inserts and optionally skips existing keys; `overwrite_*` replaces
  matching keys and inserts the rest. Both create a missing table.
- `merge_by=True` means the native Field's declared primary key.
- Commit after `commit_batch_num` input batches or the earlier optional
  `commit_row_size` bound.
- Every write stages one transformed partition at a time as a local Parquet
  file and commits it by path, so a commit holds its chunk and not a multiple
  of it. Never hand a whole chunk to a writer that splits it.
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
```

Each task directory contains one Marimo application beside its JSON document.
`parse_messages` passes `filesystem` to `IOBase.from_uri`, frames each line
under the `rowheader` its document names, applies `Message.into_field()` to
each batch, and writes one schema-bearing reader directly to Iceberg. `parse_fix` passes that stored reader through the three
stages one codec exposes, in this order and no other:

```text
parse -> enrich -> lifecycle
```

It writes the registry-defined schema without a yggfin FIX model. A FIX row is
a message and not a line -- a line carrying two frames answers two rows and a
line carrying none answers none -- so `fix.messages` is keyed on
`(sourceurl, rownum, msghash)`.

The codec is the whole parse surface: the dictionary and the instant an undated
message takes are pinned on it once, and each stage after it is a call rather
than another pin. A capture order is pinned only where a door resolves one by
position, which is the line door; the batch door fills from a column named
after the field, so `parse_fix` pins none and cannot go stale against a header
it never sees. A version is not among the pins -- what a message was read at
is what its own `beginstring` said -- and `fix_codec` refuses by name any
keyword that is not one of its seven.

Airflow launches the adjacent standalone runner through the locked `uv`
`runner` group; the operator never calls the Rekep CLI.

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
  times.py      instant readings and the ULBridge row header
  resources.py  Yggdryl binding and required byte reads
tasks/
  airflow/
  parse_fix/
  parse_messages/
  optimize_iceberg/
schemas/rekep/message.json
schemas/rekep/fix-message.json
```
