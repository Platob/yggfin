# Coding patterns

House style for `yggfin`. Match these patterns; never add a parallel way to
do the same thing.

## 1. Object oriented first

Behaviour lives on the class that owns the data: `log.into_arrow_table()`,
never `read_log_as_table(log)`. Justified free functions stay private
(`_utf8`) and sit below the classes they serve. Shared behaviour is a mixin
(`Convertible`), not a copy.

## 2. `from_*` builds, `into_*` converts

- `from_<thing>`: classmethod, builds an instance (`TextFile.from_path`).
- `into_<thing>`: instance method, converts or emits (`Field.into_json`).
- Never a `format=` argument; never a module-level factory beside a class --
  `TextFile.from_path(...)` is the whole API.

## 3. `from_` and `into_` infer and redirect

The generic forms dispatch through the class's `REDIRECTS` mapping (keyed by
file extension or by type): `book.into_("b.json")` -> `into_json`,
`log.into_(pyarrow.Table)` -> `into_arrow_table`. A **type** argument is the
requested result (consumed); a **value** is a source/destination (passed
through). Never re-implement the inference at a call site.

## 4. A declaration is a schema

`@field` turns a class into one `StructField`, reachable as `FIELD`: a name, an
Arrow struct type, and metadata. Rules:

- `__`-prefixed annotations are working state, never members or columns.
- Nullability is declared: `str` is NOT NULL, `str | None` is nullable. Item
  nullability survives: `list[str | None]`.
- **A field doc is the string literal under the member, one line** -- it lands
  as the column comment (recovered from source via `ast`, like Sphinx).
  Rationale goes in a `#` comment above the member. Never an `Attributes:`
  block; never restate a description in metadata.
- Declarations ride on `Annotated[..., Field(...)]`: exact `arrow_type`,
  `metadata`, `nullable`, plus `Field.primary_key()` and
  `Field.partition_key(transform)`. A bare `DataType`/`Mapping`/`str` is a
  shorthand for the type, the metadata or the description.
- **The type picks the class.** `Field(...)` returns `StructField`, `MapField`
  or one of the list flavours (`ListField`, `LargeListField`, `ListViewField`,
  `LargeListViewField`, `FixedSizeListField`) through `__new__` -- so `fields`,
  `item`, `key`/`value` and the recursive casts live on the class that has
  them, never behind a kind check at a call site. Every builder here goes
  through `Field(...)`, so none of them repeats the rule. A new Arrow kind is a
  new subclass plus one row in `_KINDS`.
- A member reached through a container is a **view** of it: setting
  `is_primary_key`, `is_partition_key` or `description` on it rebuilds the
  struct, list or map it came from, to the root. Derived views (the Arrow
  schema, the member list) are cached and dropped whenever the declaration
  changes.
- Protocol properties live in metadata under a prefixed key
  (`iceberg:primary_key`, `iceberg:partition_key`, `iceberg:field_id`);
  unprefixed keys are ours. A nullable primary key is refused, at the
  declaration and at the setter. A **column id** is Iceberg's own identity for
  a column, so it is read back into `iceberg:field_id` and published in a
  contract; the ecosystem's `PARQUET:field_id` is what parquet files carry, and
  the two are translated at the Iceberg boundary and nowhere else.
- Refuse rather than guess: recursive classes, non-optional unions and unknown
  leaves all raise, naming the member and the way out.
- The projection is built once per class, lazily, by a descriptor -- and a
  subclass builds its own.
- Extend `FieldBuilder.SCALARS` in a subclass and wire it with
  `FIELD_BUILDER`.
- Reverse projection: `Field.from_arrow_schema(schema).into_dataclass()`
  builds a lossless class, identity included (schema metadata carries `name`
  and `namespace`). Same for Iceberg, both ways
  (`into_iceberg_schema`/`from_iceberg_schema`).
- **A dump is the declaration, not a resemblance of it.** Every container names
  its own flavour -- `large_list`, `list_view`, `large_list_view`,
  `fixed_size_list` with the `list_size` that is part of its type, a map with
  `keys_sorted` -- because a document that spelled all five `list` narrowed a
  64-bit offset and dropped a width *silently*, both of them being things a
  cast then papered over. So a new Arrow kind is a new subclass, one row in
  `_KINDS`, a `kind()` that names it, and one row in `_LIST_KINDS` when it is a
  list. A hand-written field that omits what a type needs (`list_size`, `type`
  itself) is refused by name.
- **A contract is a file.** `schemas/<namespace>/<name>.{yaml,json}` at the repo
  root is the declaration as whoever does not import this package sees it:
  `Field.from_yaml` reads it back as the same Arrow type, keys, comments and
  nesting included. `tests/test_schemas.py` pins every file (parse -> dump ->
  parse) and pins `schemas/rekep/log.yaml` against `Log.FIELD`, so a column
  that exists in code and not in the contract fails the build. Contracts change
  by adding a nullable column; retyping or dropping is a new version, not an
  edit.
- A format nobody needs is an extra (`yaml`, `toml`, `fast`, `iceberg`);
  import optional deps at the point of use via `require`, never at module top.

## 5. Dataclasses hold state, `__post_init__` normalises it

`__post_init__` makes fields agree (resolve the filesystem, canonicalise the
url) so everything downstream assumes a normalised object. Use `eq=False` on
handle-like dataclasses.

## 6. Lazy by `cached_property`, and teardown never triggers it

Expensive resources are `cached_property`; `close()`/`__del__`/`__repr__`
must pop `self.__dict__`, never read the property -- or disposal opens a
remote file. This rules out `frozen=True`/`slots=True` on such classes.

## 7. Arrow is the hub: metadata and processing

- The Arrow schema is the one authority on what data *is*; every other view
  derives from it.
- A schema carries `name`, `namespace` and `description` metadata, so it still
  says which class it came from wherever it travels -- and
  `Field.from_arrow_schema` reads that identity back.
- Hot paths hand columns to `pyarrow.compute`, never loop in Python; per-row
  work is a regex match, an append, a hash. Benchmark (`python/benchmarks/`)
  before and after touching a hot path.
- A field is also a **target** shape: `cast_arrow_array`, `cast_arrow_batch`,
  `cast_arrow_table` and `cast_arrow_reader` (unsafe by default) cast columns,
  fill missing *nullable* ones, drop extras and reorder, so a nearly-right
  batch writes. A missing NOT NULL column is refused by its path
  (`venue.mic`) -- filling it would only fail later, at the write.
- The cast **recurses**: a struct casts member by member, a list its item, a
  map both halves, so a nested member that is missing, narrowed or in another
  order is handled where it is declared. `merge_schema=True` keeps what the
  data has and the field does not, the field's own types still winning.
- The recursion is also what converts *between* kinds -- map to struct
  (`map_lookup` per member), struct to map or list (a transpose), map to a list
  of entries, any list flavour to any other. Arrow refuses most of those
  outright, which is why they exist here.
- **Never a Python row loop in a cast.** Every shape change is
  `pyarrow.compute` kernels and array builders (`fields/arrays.py`); the index
  arithmetic an interleave needs is built from `repeat` + `cumulative_sum`,
  never from `range`. A comprehension over columns is fine; one over rows is
  the bug.
- Where Arrow can do a step, let it: a list flavour change is one `cast` over
  the layout after the item is cast here, and a same-kind cast re-wraps the
  array's own offsets instead of flattening. Both were measured
  (`benchmarks/bench_cast.py`): the walk runs at 0.8-2.1x of `Array.cast`
  where Arrow can do the same job -- faster on everything but a plain flavour
  change, which is where Arrow's own kernel is one memcpy -- and
  `RecordBatch.cast` cannot reorder columns at all. Quote the slow row too:
  rounding it away is how a claim stops matching the benchmark under it.
- Generic redirects infer from what they are handed: `cast_arrow` picks
  array/batch/table/reader, `Dataset.write_arrow` picks batch/table/reader,
  `read_arrow` picks by the type asked for. `Convertible.redirect_of(value,
  mapping)` is the one lookup; a second family of methods passes its own
  mapping rather than reimplementing it.
- **Between two systems, the hub is a document.** A producer casts onto the
  contract before it sends -- shipping whatever it happens to hold makes a
  parser of every consumer, and the first one to get it wrong does so in
  silence. A consumer loads the same contract and casts on receipt, with
  `merge_schema=True` so a producer that has moved ahead does not break it.
  Evolution is additive at every level (`merge_with`, `add_fields`); anything
  else is a version. The transport carries the schema where it can (Arrow IPC,
  parquet, Iceberg) and the contract file is what stands in for it where it
  cannot (a log file).

## 8. Let the library own what it already knows

Delegate to Arrow (codec detection, URI resolution, decompression, every
shape-changing kernel) and to pyiceberg (type conversion, id assignment, scan
planning, snapshots, upserts, schema evolution) -- but probe real behaviour
before designing around an assumption; several APIs surprise. The Iceberg
projection is pyiceberg's own conversion plus the identity Arrow cannot carry
(ids, docs, identifier fields, transforms), never a second walk of the type
system.

**The Iceberg module's own rules.** Every verb takes `branch`, and every read
takes `snapshot_id`, so a job works on a branch without a second dataset
object. Field ids are checked, not demanded: a schema that carries them keeps
them, one that does not is numbered fresh, because a user handing over a plain
Arrow schema should not have to know the protocol. FileIO defaults to
`PyArrowFileIO` so the store is read, written, listed and swept through the
same `pyarrow.fs` handles as everything else. Maintenance is autonomous and
honest: `compact` plans per partition when the transforms are identities and
rewrites nothing it cannot address, `cleanup` expires *and* sweeps what expiry
stranded (pyiceberg's expiry is metadata-only) while never touching a file a
live snapshot references or one younger than `orphan_age`, `add_fields` adds
what is missing and commits nothing when there is nothing to add, and
`optimize` is the three in the order that makes them cheap. Every one of them
reports what it did.

Ten things that were learned the expensive way, all of them measured:

- **A maintenance verb must settle.** `compact` marks its own snapshots and
  skips a part nothing has landed in since; without that it replans forever,
  because pyiceberg sizes output files from *in-memory* bytes and a part that
  legitimately needs ten files still reports ten afterwards. A version of this
  that counted files quadrupled a table in three runs.
- **Never assume a library default is doing something.**
  `commit.manifest-merge.enabled` is inert until `min-count-to-merge` (default
  100) is lowered; `write.target-file-size-bytes` cannot fill a file across
  commits; `limit=` is not pushed down at all. Read the source, then measure.
- **Rows returned say nothing about files read.** A filter Iceberg cannot use
  returns the right answer and reads the whole table, so pruning is asserted on
  planned files (`scan_plan`), never on results.
- **A merge's scan filter is a *superset*.** Anything implied by "equal on
  every key column" is safe and cheap -- key values under
  `IN_PREDICATE_LIMIT`, ranges past it -- but the rows it brings back must be
  narrowed to the keys the chunk actually references before anything looks at
  them. The filter that decides what is *deleted* stays exact; ranges may only
  narrow it.
- **A feature is advertised only once something wrote through it.** Every
  partition transform but `identity` is computed by Iceberg's Rust core, so a
  `day` or `bucket[16]` partition our docstrings had shown since day one raised
  `NotInstalledError` on the first write. Building the spec was tested;
  *writing* one was not. The extra now pulls the core in, and a merge through a
  transformed partition is compared with pyiceberg's own upsert.
- **Find every branch first, then test the guard on each.** The NaN key was
  refused below the 200-literal ceiling and silently duplicated above it,
  because `min_max` skips NaN where `In` refuses it. Crossing that one ceiling
  then found a *second* boundary nobody had named: an `In` of one literal
  collapses to `EqualTo`, which compares numerically and matches `-0.0`, while
  an `In` of two or more becomes `pc.is_in`, which hashes it apart -- so a
  stored `-0.0` was updated and never deleted, at two keys and not at one.
  A guard is only as wide as the branch it is on, and the branches are not
  always the ones the constant names.
- **A guard is only as wide as the branch it is on.** A merge key that is
  null was refused; one that is NaN was refused *only* under the 200-literal
  ceiling, because the other branch of the same filter builds a range and
  `min_max` skips NaN -- so past the ceiling the stored row fell outside the
  scan and was inserted again, and again on every later merge. Any test of a
  guard on a filter with two branches has to cross the boundary between them.
  The same reading found a chunk missing an *optional* column passing Iceberg's
  own schema check and then writing nulls over what was stored, and a scan
  pinned to a ref reading under that snapshot's schema, so a renamed column
  compared against nulls until field ids were used to recover the name.
- **A path is not a string, and neither is a netloc.** The live set for the
  sweep was built by
  stripping `://` off recorded URIs while the listing resolved its paths
  through `pyarrow.fs`. They agree on `file:///x` and `s3://b/x` and on
  nothing else -- `file:/x`, `abfss://c@acct.dfs.../x`, `hdfs://host:8020/x`,
  a Windows drive letter -- so every live file looked orphaned and `cleanup`
  deleted the table. Two paths are only comparable through the same resolver;
  reduce both to what follows a directory that was resolved once. The same rule
  twice more, both found by CI rather than by reading: a local path is spelled
  POSIX **everywhere**, because `pyarrow.fs` answers a Windows listing with
  forward slashes and a root spelled `C:\logs` is a prefix of none of them; and
  a rule written for the shape a fixture happens to have (`s3://host:port/`,
  `file:/x`) is a rule that was never tested against the shape the world
  mostly has (`s3://bucket.s3.<region>.amazonaws.com/`, `file:C:/x`).
- **Ask Iceberg where things are.** `write.data.path` moves the data,
  `list_namespaces` returns one level, and a scan pinned to a ref projects
  under that snapshot's schema. Each of those was assumed instead, and each
  assumption was a silent skip: a sweep that swept nothing, a maintenance loop
  that never saw a nested namespace, a read that filled a renamed column with
  nulls.
- **Our own commits update the table object in place.** `refresh()` is for
  seeing *other* writers, and calling it per chunk is a catalog round trip per
  commit -- free on SQLite, a network hop on REST or Glue.

## 9. A dataset is a stream in and a stream out

`Dataset` is three methods -- `into_struct_field`, `read_arrow_reader`,
`write_arrow_reader` -- and everything else is built from them. Rules:

- A dataset is the one thing here bigger than memory, so nothing in the
  interface may need all of it: `read_arrow_table`/`write_arrow_table` are for
  when the caller says it fits.
- **Writes append, and appending to nothing creates.** `create_with_field` is
  the one place a dataset is built; `create_with` infers the shape from a
  field, an Arrow schema/field/type or a `@field` class, and `get_or_create` is
  what a write calls first. Creating what exists is never an error.
- `schema=` on a read or a write is what to cast onto; None means this
  dataset's own shape, and on a read it means "hand over the store's own
  reader", widths included -- a conversion nobody asked for is paid per row.
- `merge_by` is one argument: True is the declared primary key, a list is
  those columns, falsy appends. On a **write** it upserts; on an **append**
  (`append_arrow_reader`, same signature) it only *skips* the rows a stored
  key already matches -- nothing stored is ever rewritten, which is the half
  of an upsert an immutable stream needs and the cheap one.
- **A batch is not a unit of work downstream.** A store that commits per call
  accumulates `commit_row_size` rows first (`dataset.arrow_chunks`).
- Push filters, columns and limits down to the engine that holds the
  statistics; never read rows to throw them away here.
- **A set of files is a dataset too, and its order is ours.** `TextFiles` walks
  `pyarrow.fs` one directory at a time (a generator, so the first path arrives
  before the tree is listed) and sorts every listing itself with digit runs
  compared as numbers -- no filesystem promises an order, so a set that did not
  sort would read a capture differently on every machine. One file is open at a
  time; short per-file batches are combined to the size asked for, because 500
  rotated logs otherwise mean 500 batches for every stage downstream, and a
  batch already at size is passed through untouched. A missing root is refused
  rather than skipped, and a write is refused outright: nothing says which file
  a row belongs in. The bytes have their own flow beside the rows -- streamed,
  and compressed *as it goes* through Arrow's codecs, since `Codec.compress`
  would need the whole capture in memory.

**Nothing names a source but the caller.** A column that says which bridge,
desk or environment a capture came from is `static_values` on the reader --
inferred from the value or stated with a `pyarrow.Scalar` -- appended after the
data columns in insertion order, so adding one never moves a column a reader
selects. No source name is ever hardcoded in a shape.

## 10. Stream; never materialise

Anything scaling with input is an iterator; memory is bounded by a
batch/byte parameter. Hot paths work in `bytes`; Arrow does one bulk UTF-8
cast per batch. Size parameters name unit and dimension: `batch_row_size`,
`read_byte_size`.

## 11. Optimise what was measured, and keep the measurement

A benchmark lives in `python/benchmarks/`, sweeps the configurations that
matter *including the ones expected to be bad*, and reports what the next
reader pays for -- files, manifests and snapshots -- beside the seconds. Verify
the result before timing it: a benchmark that measures the wrong answer
measures nothing. Where a faster path replaces a library's own, the
replacement is compared against it row by row in the tests
(`tests/iceberg/test_coherence.py`), and a flag switches back to the library.
Numbers quoted in docstrings or docs are measured twice; a single run is noise.

Measure warm, and in isolation. An Acero join costs its own initialisation on
the first call in a process, so a sequence of timed stages charges the whole of
it to whichever stage ran first: one reordering looked 5x faster that way and
was worth 1.7 ms once both sides were warmed and run best-of-five. A profile
made of single calls, in order, is a story about warm-up.

## 12. Comments say why

Docstrings and comments carry the constraint, trade-off, or failure that
motivated the code -- never a restated signature.

## 13. Tests derive expectations, then pin them

Derive from the fixture, then assert the derived count against a literal so
a broken regex cannot move both sides together. Cover lifecycle (laziness,
double-close, use-after-close) and the sweeps that must not change results.

**A test that cannot fail is worse than no test**, because it is counted. An
adversarial pass over this package found four of them, each sitting on a live
defect: a batching sweep that compared `sum` and `max` of row counts, which
dropping a continuation never changes; a "custom pattern" built by a `replace`
that matched nothing, so it ran the default pattern against the default
fixture; a compaction suite that only ever used the partitioned fixture, so
every verb raised on an unpartitioned table untested; and a NaN guard tested
with a one-row chunk, which only reaches one of the two branches it guards.
When a test passes the first time you write it, break the code and watch it
fail.

Three shapes worth reaching for, all of which found real defects here:

- **Compare against the reference, not against yourself.** pyiceberg for
  anything Iceberg, `Array.cast` for anything cast-shaped, parse -> render ->
  parse for anything that claims a round trip. Where the reference is *wrong*
  -- Arrow's view-to-list cast reads offsets and ignores sizes -- say so in the
  test and compare against the source's own values.
- **Cross every boundary the code branches on.** The 200-literal `In` limit,
  the batch size, the width a slicing path assumes, zero rows, one row.
- **Assert what a later reader pays for**, not only what came back: planned
  files, snapshot summaries, catalog round trips, the files a sweep left.

## 14. Module layout

```text
rekep/
├── annotations.py what a declaration says: type hints, and the docstrings
│                  (class summary, Google/Sphinx sections, and the literal
│                  under a member) that become descriptions
├── convert.py     Convertible: generic from_/into_ dispatch, and the
│                  serialisation it dispatches to -- any dataclass to and
│                  from dict/JSON/YAML/TOML, nested classes included, over
│                  files, paths, URIs or raw bytes
├── cli.py         the `rekep` command: `fields dump` publishes a class as a
│                  document, `fields load` reads one back and builds it -- both
│                  thin over `field_of`, `Field.from_file` and the `into_*`
├── require.py     optional deps at the point of use
├── urls.py        Url: the one parser for a location -- parts, percent
│                  decoding, a secret that may contain a colon, an S3
│                  endpoint told from a bucket by port *and* by hostname
│                  (most endpoints answer on 443 and carry no port),
│                  a Windows drive told from a scheme, every local path
│                  spelled POSIX, `join`/`parent` as a mutable walk, and
│                  `into_filesystem()`/`properties_of()` as what a store and
│                  a catalog are configured from
├── filesystems.py resolve(): a location as (FileSystem, path), cached per URL
├── fields/        a dataclass is its own Arrow schema:
│                  field.py (Field, its container subclasses, the `field`
│                  decorator, every cast), arrays.py (the kernel-only array
│                  builders those casts are made of), builder.py (FieldBuilder:
│                  type hints -> fields), classes.py (ClassBuilder: the reverse
│                  projection), arrow.py (merge_fields/merge_schemas)
├── dataset.py     Dataset: the abstract read/write/append/create ends of a
│                  stored product, arrow_chunks (the commit-sized grouping
│                  every store that commits per call needs), and the key
│                  joins every merge-shaped write is made of (keys_of,
│                  semi_join, anti_join, first_rows, normalised_keys)
├── iceberg/       fields.py (the Field <-> pyiceberg projection: ids, docs,
│                  identifier fields, partition transforms), catalog.py
│                  (IcebergCatalog/IcebergNamespace: CRUD around the tables)
│                  and dataset.py (IcebergDataset: scan pushdown out, cast +
│                  append/upsert in, one commit per chunk, and the
│                  maintenance -- add_fields, compact, cleanup, optimize)
├── fix/           message.py (FixMessage and the vectorised line parsing:
│                  separator detection, tag=value and rendered
│                  Name[i]=Member=value cutting, repeating groups, and
│                  tag_arrow_array: map keys as integer tags), fields.py
│                  (the FIX datatype -> Arrow projection and the forgiving
│                  Boolean reading), registry.py (FixRegistry: the OnixS
│                  dictionary scraped per version, cached in ~/.config/fix/
│                  and dumped into data/fix/ here, lookup and fuzzy search,
│                  all names case-insensitive) and sqlite.py
│                  (SqliteFixRegistry: the same registry over an indexed
│                  file -- the five store methods and the four questions,
│                  as SQL)
└── logs/          log.py (the Log shape), text_file.py (TextFile: a log read
                   into Arrow batches and written back out as lines, itself a
                   Dataset) and text_files.py (TextFiles: a folder of them as one
                   ordered stream -- the lazy pyarrow.fs walk, the batch
                   combining, and the raw/compressed byte flow)
```

Beside `python/`, `schemas/` holds the published contracts (one directory per
namespace, one file per shape), `data/` the dictionaries this repository
publishes -- the FIX one under `data/fix/`, which is a `FixRegistry` cache
directory and nothing else -- and `docs/` the site.

Dependencies point one way: `logs`/`iceberg` -> `dataset` -> `fields` ->
`convert` -> `annotations`, and `fix` sits beside `dataset` on the same
`fields` base. `urls` is a leaf below all of it: `filesystems`, `convert`,
`logs` and `iceberg/fileio` all reach a store through it, so there is one
answer to "what is this location" rather than one per caller. The one loop back is deliberate and lazy: a
`Field`'s `into_iceberg_*` imports `rekep.iceberg.fields` at the point of use,
so the API stays on the class that owns the data without `fields/` depending
on an extra. `tests/` mirrors `src/` folder for folder.

**Documentation is a site, not a README.** `docs/` is mkdocs-material at the
repo root, in two groups: *Architecture* (design.md, contracts.md -- the rules
and the schema contracts, which is where anyone building on this starts) and
*Guides* (types.md, logs.md, fix.md, iceberg.md). Content tabs carry the
examples so a page reads as one narrative instead of a wall of code.

**Every page has the same shape**: a short description of what the thing is,
then the usages -- each example with a line saying what it is for -- then
`## Benchmarks` last, holding the measurements for *that* topic and the command
that produced them. Numbers live with the thing they describe; benchmarks.md is
the method (how a number is made, how to read a range, why a count beats a
second) and an index, not a dump of every table.

It builds `--strict` in CI, and `validation.links.anchors` is raised to `warn`
so a broken `#anchor` fails there too rather than shipping. The README is a
landing page that points at it.

## 15. Typing and file structure

`from __future__ import annotations` everywhere; full annotations on public
methods; `ClassVar` for registries; classes first, private helpers last,
`# -- section ---` banners in long modules.

## Tooling

```text
cd python
uv sync            # env + every extra the tests use
uv run pytest
uv run ruff check
uv run ruff format
```

```text
uv sync --group docs
uv run --project python mkdocs build --strict   # from the repo root
```

CI (`.github/workflows/ci.yml`) runs Linux + Windows, plus a docs build. A bare
`pip install rekep` is Arrow only; `rekep[all]` adds the format extras and
Iceberg. Benchmarks live in `python/benchmarks/` and are measured twice before
being quoted -- say what reproduced and what is noise, never a single run as
if it were a spec.
