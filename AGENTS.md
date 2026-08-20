# Coding patterns

House style for `yggfin`. Match these patterns; never add a parallel way to
do the same thing.

## 1. Object oriented first

Behaviour lives on the class that owns the data: `log.into_arrow_table()`,
never `read_log_as_table(log)`. Justified free functions stay private
(`_utf8`) and sit below the classes they serve. Shared behaviour is a mixin
(`Convertible`), not a copy.

## 2. `from_*` builds, `into_*` converts

- `from_<thing>`: classmethod, builds an instance (`LogFile.from_path`).
- `into_<thing>`: instance method, converts or emits (`Field.into_json`).
- Never a `format=` argument; never a module-level factory beside a class --
  `LogFile.from_path(...)` is the whole API.

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
- **The type picks the class.** `Field(...)` returns `StructField`, `ListField`
  or `MapField` when its type is one of those, through `__new__` -- so
  `fields`, `item`, `key`/`value` and the recursive casts live on the class
  that has them, never behind a kind check at a call site. Every builder here
  goes through `Field(...)`, so none of them repeats the rule.
- A member reached through a container is a **view** of it: setting
  `is_primary_key`, `is_partition_key` or `description` on it rebuilds the
  struct, list or map it came from, to the root. Derived views (the Arrow
  schema, the member list) are cached and dropped whenever the declaration
  changes.
- Protocol properties live in metadata under a prefixed key
  (`iceberg:primary_key`); unprefixed keys are ours. A nullable primary key is
  refused, at the declaration and at the setter.
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
- Do not hand a whole batch to one Arrow call to save the walk: measured, the
  recursion is 1.1-2.4x faster than `Array.cast` on the same data, and
  `RecordBatch.cast` cannot reorder columns at all
  (`benchmarks/bench_cast.py`).

## 8. Let the library own what it already knows

Delegate to Arrow (codec detection, URI resolution, decompression) and to
pyiceberg (type conversion, id assignment, scan planning, snapshots, upserts)
-- but probe real behaviour before designing around an assumption; several
APIs surprise. The Iceberg projection is pyiceberg's own conversion plus the
identity Arrow cannot carry (ids, docs, identifier fields, transforms), never
a second walk of the type system.

## 9. A dataset is a stream in and a stream out

`Dataset` is three methods -- `into_struct_field`, `read_arrow_reader`,
`write_arrow_reader` -- and everything else is built from them. Rules:

- A dataset is the one thing here bigger than memory, so nothing in the
  interface may need all of it: `read_arrow_table`/`write_arrow_table` are for
  when the caller says it fits.
- `schema=` on a read or a write is what to cast onto; None means this
  dataset's own shape, and on a read it means "hand over the store's own
  reader", widths included -- a conversion nobody asked for is paid per row.
- `merge_by` is one argument: True is the declared primary key, a list is
  those columns, falsy appends.
- **A batch is not a unit of work downstream.** A store that commits per call
  accumulates `commit_row_size` rows first (`dataset.arrow_chunks`).
- Push filters, columns and limits down to the engine that holds the
  statistics; never read rows to throw them away here.

## 10. Stream; never materialise

Anything scaling with input is an iterator; memory is bounded by a
batch/byte parameter. Hot paths work in `bytes`; Arrow does one bulk UTF-8
cast per batch. Size parameters name unit and dimension: `batch_row_size`,
`read_byte_size`.

## 11. Comments say why

Docstrings and comments carry the constraint, trade-off, or failure that
motivated the code -- never a restated signature.

## 12. Tests derive expectations, then pin them

Derive from the fixture, then assert the derived count against a literal so
a broken regex cannot move both sides together. Cover lifecycle (laziness,
double-close, use-after-close) and the sweeps that must not change results.

## 13. Module layout

```text
rekep/
├── annotations.py what a declaration says: type hints, and the docstrings
│                  (class summary, Google/Sphinx sections, and the literal
│                  under a member) that become descriptions
├── convert.py     Convertible: generic from_/into_ dispatch, and the
│                  serialisation it dispatches to -- any dataclass to and
│                  from dict/JSON/YAML/TOML, nested classes included, over
│                  files, paths, URIs or raw bytes
├── require.py     optional deps at the point of use
├── filesystems.py FileSystem.from_uri, cached per URL
├── fields/        a dataclass is its own Arrow schema:
│                  field.py (Field and its ListField/MapField/StructField
│                  subclasses, the `field` decorator, the casts),
│                  builder.py (FieldBuilder: type hints -> fields),
│                  classes.py (ClassBuilder: the reverse projection),
│                  arrow.py (merge_fields/merge_schemas: widening a schema)
├── dataset.py     Dataset: the abstract read/write ends of a stored product,
│                  and arrow_chunks, the commit-sized grouping every store
│                  that commits per call needs
├── iceberg/       fields.py (the Field <-> pyiceberg projection: ids, docs,
│                  identifier fields, partition transforms) and dataset.py
│                  (IcebergDataset: scan pushdown out, cast + append/upsert
│                  in, one commit per chunk)
└── logs/          log.py (the Log shape) and log_file.py (LogFile:
                   streaming Arrow-native log access)
```

Dependencies point one way: `logs`/`iceberg` -> `dataset` -> `fields` ->
`convert` -> `annotations`. The one loop back is deliberate and lazy: a
`Field`'s `into_iceberg_*` imports `rekep.iceberg.fields` at the point of use,
so the API stays on the class that owns the data without `fields/` depending
on an extra. `tests/` mirrors `src/` folder for folder.

## 14. Typing and file structure

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

CI (`.github/workflows/ci.yml`) runs Linux + Windows. A bare
`pip install rekep` is Arrow only; `rekep[all]` adds the format extras and
Iceberg. Benchmarks live in `python/benchmarks/` and are measured twice before
being quoted -- say what reproduced and what is noise, never a single run as
if it were a spec.
