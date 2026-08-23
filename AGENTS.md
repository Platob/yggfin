# Coding patterns

House style for `rekep`. Keep one obvious implementation for each behavior.

## API

- Put behavior on the class that owns the data. Keep justified helpers private.
- `from_*` builds an instance; `into_*` converts or emits it.
- Generic `from_`/`into_` dispatch through cached `into_redirects()` mappings.
- Prefer cached class methods over class variables for derived declarations.
- Dataclass `__post_init__` normalizes state once.
- Expensive handles are `cached_property`; teardown pops `__dict__` and never
  opens a lazy resource.

## Declarations

`@scalar` makes a dataclass an Arrow `StructField`, returned by
`Class.into_field()`.

- Type hints declare nullability, including list items.
- `__`-prefixed annotations are private state, not columns.
- Use `Annotated[..., Field(...)]` for Arrow types, metadata, keys, partitions,
  and sort order.
- Container behavior belongs to its `Field` subclass. Do not branch on Arrow
  kinds at call sites.
- Refuse recursive classes, ambiguous unions, unknown leaves, and missing
  non-null columns.
- Protocol metadata is prefixed (`fix:`, `iceberg:`, `enum:`).
- Nested fields never publish table keys.
- Portable contracts live in `schemas/<namespace>/<name>.yaml` and round-trip
  losslessly through Arrow.

### Description budget

Descriptions are contract text, not tutorials. Use one short factual sentence
that adds information such as units, source, null meaning, or derivation.
Delete text that repeats the name, type, signature, or implementation. Put a
non-obvious constraint in one nearby `#` comment and longer rationale in the
single guide that owns it. Optimize descriptions whenever touching a field.

## Arrow is the hub

- Arrow schema metadata is the authority; other representations derive from it.
- Cast at producer and consumer boundaries. Recursive casts reorder columns,
  fill only nullable omissions, drop extras unless `merge_schema=True`, and
  handle nested shapes with Arrow kernels.
- Never use a Python row loop for an Arrow shape conversion. Column
  comprehensions are fine.
- Let Arrow own codecs, filesystems, decompression, and kernels. Let pyiceberg
  own table conversion, ids, snapshots, planning, and commits.
- Dictionary-encode repeated code columns; store enum values as integers so
  unknown future codes survive.

## Streaming datasets

`Dataset` is a stream in and a stream out.

- Primary APIs use `RecordBatchReader`; table helpers explicitly require data
  to fit in memory.
- Writes append and create missing tables. `merge_by=True` uses declared keys;
  append skips stored keys while write upserts.
- Accumulate `commit_row_size`; an input batch is not a storage commit.
- Push filters, projections, limits, and ordering to the storage engine.
- File sets open one naturally sorted path at a time and combine short batches.
- Nothing names a source except caller-supplied `static_values`.
- Size parameters state their unit (`batch_row_size`, `read_byte_size`).

## Iceberg

- Every verb accepts `branch`; every read accepts `snapshot_id`.
- Preserve field ids when present and assign them when absent.
- Read planned partitions in deterministic order and stream one partition at a
  time when ordered input is requested.
- Merge scan filters are safe supersets; deletion matching remains exact.
- Compare filesystem paths only after resolving both through the same
  filesystem.
- Maintenance must settle and report what it changed.
- Tests assert planned files, snapshots, and stored results, not only returned
  rows.

## Market data

- Events are immutable versions. `hash` identifies a version; `xhash` a
  lifecycle; `linked_xhash` relates lifecycles.
- Composite identity is the cross-language `rekep-identity-v1` frame: signed
  little-endian `int64` lengths, `-1` for null, typed payload bytes, XXH3-64,
  and two's-complement `int64` storage. Numbers are never formatted as text.
- Store market notions as integer enums. Ordered state bands make live and
  terminal checks range predicates; unknown codes degrade to their band.
- Nest nothing a reader filters on. Keep instrument identity and book summary
  values flat.
- FIX transcription preserves repeated tags and wire order in lists, not maps.
- The registry owns FIX names, types, descriptions, tags, and values across
  versions. Hard-code only normalization rules the registry cannot express.
- Generic `Event` owns snapshot and idle-expiry behavior. Finished states do
  not keep producing snapshots.
- `BookIterator` consumes time-sorted parsed `Log` records and emits only
  `Book` rows. Keep state mutation single-threaded and bounded.

## Workflow ownership

Concrete jobs are notebooks under `tasks/<name>/`, beside a YAML document that
points to the notebook. Package code contains reusable parsing and model logic,
not project-specific job classes. `Task` only defines/serializes notebook
configuration. Airflow executes the notebooks through its Papermill provider.

The supported graph is:

```text
parse_logs -> parse_market -> flatten_orders
     |             `-------> flatten_executions
     `------------> flatten_instruments
```

Inputs are text files. Persisted outputs are log, instrument, book, order, and
execution tables.

## Tests and benchmarks

- Test reusable internal logic; do not mirror notebooks with packaged task
  tests.
- Mark long Iceberg transactions `integration`. Default CI excludes them; the
  integration workflow opts in explicitly.
- Derive expectations from fixtures, then pin counts so broken producers cannot
  move both sides of an assertion.
- Cross every control-flow boundary: zero/one rows, batch sizes, thresholds,
  nulls, and alternate types.
- Compare optimized paths with a reference implementation before timing them.
- Keep focused internal benchmarks. Do not add full-pipeline or million-row
  development benchmarks.

## Layout

```text
python/src/rekep/
  fields/       declarations and recursive Arrow casts
  fix/          messages, registry, components, and protocol rules
  enums/        one persisted market enum per file
  market/       event, instrument, order, execution, and book logic
  iceberg/      catalog, dataset, schema bridge, and Arrow FileIO
  text/         Log plus streamed text files
  tasks/        notebook configuration only
schemas/rekep/  the five persisted output contracts
tasks/          notebooks, adjacent YAML, and Airflow DAG
```

Comments explain why a constraint exists. Docstrings and descriptions never
restate what code already says.
