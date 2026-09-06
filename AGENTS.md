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
  streams, codecs, decompression, text media, and general FIX primitives.
- Arrow owns columnar shape conversions and kernels.
- PyIceberg owns table conversion, ids, snapshots, scan planning, and commits.
- Yggfin owns the raw `Message` contract and its narrow PyArrow/PyIceberg seam.
- Never add a second Field class, filesystem/path layer, text reader, codec, or
  registry in yggfin.

The deleted Rekep FIX and market implementation is not a compatibility target.
Use `docs/prompts/yggdryl-fix-refactor.md` for the next vertical slice.

## Fields and Arrow

- `rekep.Field is yggdryl.Field`.
- Use native `@yggdryl.scalar` and `Annotated` options for declarations.
- Arrow schema metadata is authoritative; portable JSON derives from it.
- Use Yggdryl cast methods at producer and consumer boundaries.
- Keep Rekep strict preflight limited to missing or null non-null fields until
  Yggdryl exposes the same opt-in policy.
- Do not use Python row loops for Arrow shape conversion.

## Resources and text

- Bind paths and URIs with `IOBase`; preserve injected Arrow filesystem
  identity and opaque paths.
- `IOBase` and `TextOptions` own traversal, header capture, decompression, and
  physical-line batching.
- A raw text row names its source only through Yggdryl `url` and `rownum`.
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
filesystem URI -> parse_messages -> logs.messages
```

`tasks/parse_messages/` contains the Marimo application beside its JSON
document. The application passes `filesystem` to `IOBase.from_uri`, casts each
batch to `Message.field()`, and writes the iterator directly to Iceberg.

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
  fields/       native Field helpers and strict Arrow boundary checks
  iceberg/      catalog, dataset, schema bridge, and PyIceberg FileIO
  tasks/        application configuration only
  text/         raw Message declaration
  resources.py  Yggdryl binding and required byte reads
tasks/
  parse_messages/
  optimize_iceberg/
schemas/rekep/message.json
```
