# Coding patterns

House style for `yggfin`. Match these patterns; never add a parallel way to
do the same thing.

## 1. Object oriented first

Behaviour lives on the class that owns the data: `log.into_arrow_table()`,
never `read_log_as_table(log)`. Justified free functions stay private
(`_utf8`) and sit below the classes they serve. Shared behaviour is a mixin
(`Convertible`, `Record`), not a copy.

## 2. `from_*` builds, `into_*` converts

- `from_<thing>`: classmethod, builds an instance (`LogFile.from_path`).
- `into_<thing>`: instance method, converts or emits (`Record.into_json`).
- Never a `format=` argument; never a module-level factory beside a class —
  `LogFile.from_path(...)` is the whole API.

## 3. `from_` and `into_` infer and redirect

The generic forms dispatch through the class's `REDIRECTS` mapping (keyed by
file extension or type): `venue.into_("v.json")` → `into_json`,
`log.into_(pyarrow.Table)` → `into_arrow_table`. A **type** argument is the
requested result (consumed); a **value** is a source/destination (passed
through). Never re-implement the inference at a call site.

## 4. Data products are records

One `@record class X(Record)` is the whole product: schema, files, DDL,
tables, lineage. Rules:

- `__`-prefixed annotations are working state, never fields or columns.
- Nullability is declared: `str` is NOT NULL, `str | None` is nullable —
  through Arrow, Iceberg and DDL alike. Item nullability survives:
  `list[str | None]`.
- **A field doc is the string literal under the field, one line** — it lands
  as the column comment everywhere (recovered from source via `ast`, like
  Sphinx). Rationale goes in a `#` comment above the field. Never an
  `Attributes:` block; never restate a description in metadata.
- Overrides ride on `Annotated[..., Arrow(...)]`: exact type, metadata,
  `partition=True|"day"|"bucket[16]"`, `key=True` (primary key; nullable
  keys are refused). Bare `DataType`/`Mapping`/`str` are shorthands.
- Refuse rather than guess: recursive records, non-optional unions, unknown
  leaves all raise naming the field and the way out.
- Projections are `functools.cache`d per class (immutable outputs); derive
  the second projection from the first — Iceberg is built from the Arrow
  schema, never a second walk of the hints.
- Extend `ArrowFieldBuilder.SCALARS` in a subclass and wire it with
  `ARROW_BUILDER`; same for `ICEBERG_BUILDER`, `DDL_BUILDER`.
- Serialisers are dual: instance dumps values, class dumps the declaration
  (`dualmethod`). `into_*` with no destination returns bytes. `from_*`
  accepts files, paths, URIs, or raw bytes.
- Reverse projection: `Record.from_arrow_schema(schema)` builds a lossless
  record class, identity included (schema metadata carries `name` and
  `namespace`).
- A format nobody needs is an extra (`yaml`, `toml`, `jinja`, `fast`,
  `local`, `airflow`); import optional deps at the point of use via
  `require`, never at module top.

## 5. Dataclasses hold state, `__post_init__` normalises it

`__post_init__` makes fields agree (resolve the filesystem, canonicalise the
url) so everything downstream assumes a normalised object. Use `eq=False` on
handle-like dataclasses.

## 6. Lazy by `cached_property`, and teardown never triggers it

Expensive resources are `cached_property`; `close()`/`__del__`/`__repr__`
must pop `self.__dict__`, never read the property — or disposal opens a
remote file. This rules out `frozen=True`/`slots=True` on such classes.

## 7. Arrow is the hub: metadata and processing

- The Arrow schema is the one authority on what data *is*; every other view
  (Iceberg, DDL, lineage, docs) derives from it.
- Builders stamp default schema metadata (`name`, `namespace`,
  `description`) and Iceberg-order field ids under `PARQUET:field_id` —
  column identity is by id, not name.
- Field metadata keys for a downstream protocol are prefixed
  (`iceberg:partition_key`); unprefixed keys are ours. Product dumps group them
  under protocol blocks (`iceberg: {field_id: 1, partition_key: day}`).
- Hot paths hand columns to `pyarrow.compute`, never loop in Python; per-row
  work is a regex match, an append, a hash. Benchmark (`python/benchmarks/`)
  before and after touching a hot path.
- Transforms are batch streams: `Job.arrow_transform` takes and yields
  `RecordBatch` iterators.
- A record is also a **target** shape: `cast_arrow_batch`/`cast_arrow_reader`
  (unsafe by default) cast columns, fill missing *nullable* ones, drop extras
  and reorder, so a nearly-right batch writes. A missing NOT NULL column is
  refused by name -- filling it would only fail later, at the write.
- **A batch is not a unit of work downstream.** Iceberg commits a snapshot
  and lands a file per call, so writes accumulate `chunk_rows` first;
  `iceberg_compact` repairs what accumulates across runs instead.

## 8. Let the library own what it already knows

Delegate to Arrow (codec detection, URI resolution, decompression) and to
pyiceberg (schema conversion, evolution) — but probe real behaviour before
designing around an assumption; several APIs surprise.

## 9. Stream; never materialise

Anything scaling with input is an iterator; memory is bounded by a
batch/byte parameter. Hot paths work in `bytes`; Arrow does one bulk UTF-8
cast per batch. Size parameters name unit and dimension: `batch_row_size`,
`read_byte_size`.

## 10. Comments say why

Docstrings and comments carry the constraint, trade-off, or failure that
motivated the code — never a restated signature.

## 11. Tests derive expectations, then pin them

Derive from the fixture, then assert the derived count against a literal so
a broken regex cannot move both sides together. Cover lifecycle (laziness,
double-close, use-after-close) and the sweeps that must not change results.
Shipped artifacts are drift-tested: product dumps and table side files fail
CI when they lag the record.

## 12. Module layout

```text
rekep/
├── convert.py     Convertible: generic from_/into_ dispatch
├── require.py     optional deps at the point of use
├── imports.py     dotted-path resolution
├── render.py      Jinja + env + git context
├── filesystems.py FileSystem.from_uri, cached per URL
├── namespace.py   Namespace: the OpenLineage resource for a hierarchical
│                  identifier -- recursive parent levels building a path;
│                  unique_uri(scheme, namespace, name) is the one place a
│                  Job's and a Dataset's `scheme://namespace/name` id comes from
├── job.py         Job: the OpenLineage resource for a process -- config
│                  record + arrow_transform (not enforced abstract, bindable
│                  via @arrow_task); repo_url/script_path -> the
│                  sourceCodeLocation facet, airflow{dag,task}/env/properties
│                  dicts; side files under stacks/jobs, run_tracked()
│                  lineage-wraps extract -> transform -> load, Job -> Airflow
│                  via into_airflow
├── dataset.py     Dataset: the OpenLineage resource for a data product --
│                  schema (via Record) + cross-platform location (shared
│                  `properties`/`direct`, per-protocol `protocols`);
│                  read_arrow_reader/write_arrow_reader dispatch to
│                  `_{format}_..._arrow_reader`, which lineage-tracks a call
│                  to the public `{format}_..._arrow_reader` hook -- pyiceberg's
│                  own API, not reimplemented: iceberg_read (row_filter/columns
│                  pushed to the scan planner, use_ref/snapshot_id),
│                  iceberg_write (one `merge_by`: True=primary key, list=those
│                  columns, falsy=append; `overwrite`; branch-aware; chunk_rows
│                  per commit), iceberg_compact/expire_snapshots/publish;
│                  file_read/write via rekep.filesystems, hive-partitioned from
│                  the record's own Arrow(partition=...)
├── run.py         Run/RunEvent: OpenLineage's own event shape, kept as
│                  Job's and Dataset's internal lineage bookkeeping
│                  (`.events()`), never emitted anywhere
├── cli.py         one service class per capability
├── tutorial.py    the guided rich tour (`rekep tutorial`)
├── install/       installers: check, plan, converge
├── records/       machinery: record.py, annotations.py, arrow.py,
│                  iceberg.py (+deployment), doris.py (+deployment),
│                  ddl.py, registry.py (folder registries)
├── models/        the concrete records (one module per model): log.py,
│                  parsed_message.py (pipe key=value + FIX protocol tag)
├── jobs/          the concrete jobs (one module per job), mirroring
│                  models/: files_to_logs.py, logs_to_records.py (regex
│                  key=value parser, rekep.jobs.parse_fields)
├── logs/          LogFile: streaming Arrow-native log access
├── iceberg/       Iceberg stack: Catalogs/Namespaces/Tables CRUD (pyiceberg)
├── doris/         Doris stack: same resources, SQL plan + pluggable executor
└── airflow/       DAG authoring with record lineage (POSIX-only);
                   service.py: Dags resource deploying generated DAG
                   modules (renders strings, never imports Airflow)
```

Dependencies point one way: services → models → records → convert; among the
root OpenLineage resources, `namespace.py` -> `job.py` -> `run.py` ->
`dataset.py`, never back. A new model is a new module in `models/`. `tests/`
mirrors `src/` folder for folder.

**Records are the schema helper.** `Record` (`records/record.py`) carries no
resource identity of its own -- it is the dataclass-is-its-own-schema
machinery every data-carrying model and every OpenLineage resource's `record:`
field project through (`Dataset.schema_facet()`, `IcebergTable`/`DorisTable`'s
own `record:`). The resources are `Namespace`, `Job`, `Dataset`, `Run`/`RunEvent`.

**Deploy artifacts** live under repo `stacks/`, all tracked — only runtime
state is ignored (`stacks/iceberg/catalog.db`, `stacks/iceberg/warehouse/`,
generated `stacks/ddl/`), and the tutorial builds in gitignored `tutorial/`.
The layout: `jobs/` (one job per file), `iceberg/` and `doris/` (folder
registries: `catalogs/`, `namespaces/` only, file stem defaults `name`),
`datasets/` (one `Dataset` per file -- `record:`/`name:`/`namespace:`,
deployed autonomously into whichever `--target` names, no `tables/` folder
anywhere and no protocol-adapted fields committed to disk), `product/`
(whole definitions as YAML, `name` only — no namespace). Generated DDL is
never committed (`stacks/ddl/` gitignored). Defaults are **fully local**:
SQLite Iceberg catalog, file warehouse — a laptop runs without services. No
READMEs in `stacks/`.

**Branch-conditional naming is a per-file choice, not a mode.** Every side
file already renders through `rekep.render.render` -- `git_context()`'s
`git_branch_suffix`/`git_branch_slug` are always in scope, Jinja or not --
so a file picks whether it uses them at all: `stacks/jobs/logs_to_records.yaml`
and `stacks/datasets/parsed_messages.yaml` (an Iceberg `branch`) pick up the
branch because they are working/iterating assets, `files_to_logs.yaml` and
`log.yaml` stay stable because they are the shared, canonical ones. Nothing
new to wire in for a file to make either choice.

**Stacks are resource services with idempotent verbs**: `get_or_create`,
`create_or_update`, `deploy` / `deploy_folder` / `deploy_one` — dependency
order catalog → namespace → table, parallel within a level, every action
logged ("created", "exists, nothing to do", "would add columns [x]"). Every
mutating verb takes `dry_run`. Installers (`rekep install`) follow
the same contract: honest `installed()` check, exact `plan()`, converge.
Never call pyiceberg raw at a call site — extend the resource service.

`rekep service records deploy --pyclass <dotted> --target iceberg|doris`
converges one bare record, stack defaults filling in namespace and
properties. `rekep service dataset deploy --target iceberg|doris` converges
every `Dataset` under `stacks/datasets/` instead -- each carries its own
namespace and per-protocol properties, autonomous of any table side file.
`rekep service dataset maintain` compacts and expires those same tables,
taking no policy arguments: `protocols.iceberg.compact_min_files`/`retain`
in the side file are the policy. The `protocols.<protocol>` keys that *route*
a write rather than describe the table (`location`, `branch`, `merge_by`,
`compact_min_files`, `retain`) are filtered out of `table_properties()`, so
they never land on disk pretending to describe the data.

**The CLI is services** (`rekep service <svc> <cmd>`), plus the top-level
`rekep tutorial`. **Human-facing CLI output is modern and animated**: rich
panels, spinners and progress (rich is a core dependency; construct
`Console(legacy_windows=False)` and reconfigure stdout to UTF-8) — while
machine-facing output (plans, dumps, paths) stays plain text on stdout.
String options may be Jinja, rendered with args + `env` + git context
(`git_branch_suffix`/`git_branch_prefix` carry their own `_`, empty on
trunk). Undefined template variables raise.

**Documentation is generated where it can be**: `docs/models.md` comes from
`rekep service docs models`; new code-describing docs should be a
`DocsService` projection, not prose that drifts. mkdocs-material at the repo
root, built `--strict`; the tutorial lives at `docs/use-cases/tutorial.md`
and as the CLI tour.

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

CI (`.github/workflows/ci.yml`) runs Linux + Windows (Airflow only on
Linux); docs deploy from `docs.yml`; stack deploys from `deploy.yml`
(dispatch inputs for stack and dry-run, logs uploaded as artifacts). A bare
`pip install rekep` is Arrow + Iceberg; `rekep[all]` adds everything.
