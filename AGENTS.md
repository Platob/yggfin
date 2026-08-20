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

`@field` turns a class into one `Field`, reachable as `FIELD`: a name, an
Arrow struct type, and metadata. Rules:

- `__`-prefixed annotations are working state, never members or columns.
- Nullability is declared: `str` is NOT NULL, `str | None` is nullable. Item
  nullability survives: `list[str | None]`.
- **A field doc is the string literal under the member, one line** -- it lands
  as the column comment (recovered from source via `ast`, like Sphinx).
  Rationale goes in a `#` comment above the member. Never an `Attributes:`
  block; never restate a description in metadata.
- Declarations ride on `Annotated[..., Field(...)]`: exact `arrow_type`,
  `metadata`, `nullable`. A bare `DataType`/`Mapping`/`str` is a shorthand for
  the type, the metadata or the description.
- Refuse rather than guess: recursive classes, non-optional unions and unknown
  leaves all raise, naming the member and the way out.
- The projection is built once per class, lazily, by a descriptor -- a class
  cannot change after it is declared, and a subclass builds its own.
- Extend `FieldBuilder.SCALARS` in a subclass and wire it with
  `FIELD_BUILDER`.
- Reverse projection: `Field.from_arrow_schema(schema).into_dataclass()`
  builds a lossless class, identity included (schema metadata carries `name`
  and `namespace`).
- A format nobody needs is an extra (`yaml`, `toml`, `fast`); import optional
  deps at the point of use via `require`, never at module top.

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
- A field is also a **target** shape: `cast_arrow_batch`/`cast_arrow_reader`
  (unsafe by default) cast columns, fill missing *nullable* ones, drop extras
  and reorder, so a nearly-right batch writes. A missing NOT NULL column is
  refused by name -- filling it would only fail later, at the write.

## 8. Let the library own what it already knows

Delegate to Arrow (codec detection, URI resolution, decompression) -- but
probe real behaviour before designing around an assumption; several APIs
surprise.

## 9. Stream; never materialise

Anything scaling with input is an iterator; memory is bounded by a
batch/byte parameter. Hot paths work in `bytes`; Arrow does one bulk UTF-8
cast per batch. Size parameters name unit and dimension: `batch_row_size`,
`read_byte_size`.

## 10. Comments say why

Docstrings and comments carry the constraint, trade-off, or failure that
motivated the code -- never a restated signature.

## 11. Tests derive expectations, then pin them

Derive from the fixture, then assert the derived count against a literal so
a broken regex cannot move both sides together. Cover lifecycle (laziness,
double-close, use-after-close) and the sweeps that must not change results.

## 12. Module layout

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
│                  field.py (Field, the `field` decorator, FieldBuilder),
│                  classes.py (ClassBuilder: the reverse projection),
│                  arrow.py (cast_batch/cast_reader onto a schema,
│                  merge_fields/merge_schemas widening one)
└── logs/          log.py (the Log shape) and log_file.py (LogFile:
                   streaming Arrow-native log access)
```

Dependencies point one way: `logs` -> `fields` -> `convert` -> `annotations`.
`tests/` mirrors `src/` folder for folder.

## 13. Typing and file structure

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
`pip install rekep` is Arrow only; `rekep[all]` adds every format extra.
