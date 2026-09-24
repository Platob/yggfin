---
name: rekep
description: Operate and extend the yggfin `rekep` pipeline, which turns ULBridge text captures into FIX and market Iceberg tables. Use for any work in this repository or with the `rekep` package - running, deploying or inspecting the bundled tasks through `rekep tasks <name> run|deploy|show`, choosing windows and catalogs (local SQLite, S3, AWS Glue, S3 Tables), pinning the market fan-out to one book snapshot, reading landed tables from Python, the FIX codec and registry, table contracts (`rekep fields`), dbt products, Airflow scheduling on the worker or in pods on Amazon EKS (`EksRekepOperator`, the task image), and changing, testing or documenting the code.
---

# rekep

`rekep` (Python package under `python/`, CLI `rekep`) streams text captures
through the native Yggdryl FIX codec into Iceberg. Every pipeline step is a
**task bundled in the package** and run by the CLI:

```text
filesystem URI -> parse_messages    -> logs.messages
logs.messages  -> parse_fix_raw     -> fix.raw
fix.raw        -> parse_fix_refined -> fix.refined
fix.refined    -> parse_books       -> market.books
market.books   -> parse_orders      -> market.orders       (these three read the
               -> parse_quotes      -> market.quotes        same pinned book
               -> parse_executions  -> market.executions    snapshot)
fix.refined    -> build_dbt (optional) -> orders.events, orders.current, executions.fills
any catalog    -> optimize_iceberg (maintenance)
```

`AGENTS.md` is the binding contract for how code here is written and who owns
what; read it before changing code. This skill is the operating manual.

## Where things are

| path | what |
| --- | --- |
| `python/src/rekep/tasks/<name>.py` | one task: `run(*, ...)` and `TARGETS` |
| `python/src/rekep/tasks/<name>.json` | that task's shipped defaults (a parameters object) |
| `python/src/rekep/tasks/task.py` | `Task`, `TASKS`: the registry the CLI and Airflow use |
| `python/src/rekep/cli.py` | the `rekep` command |
| `python/src/rekep/{text,fix,market,times,iceberg,fields}` | the library the tasks compose |
| `airflow/pipeline.py`, `airflow/products.py` | the `rekep_ingestion` and `rekep_products` DAGs |
| `airflow/dispatch.py` | where each node runs: the worker, or EKS under `REKEP_EKS_CONFIG` |
| `airflow/rekep_operator.py`, `airflow/eks_rekep_operator.py` | `RekepOperator` (worker), `EksRekepOperator` (pod) |
| `Dockerfile` | the task image a pod runs |
| `data/` | local defaults: `capture/` sample, `catalog.db` + `warehouse/` (untracked), `dbt/` project |
| `config/` | where an operator's own FIX dictionary goes (`config/README.md`) |
| `schemas/rekep/*.json` | reviewed Iceberg contracts (Message, FixMsg, Book, MarketEvent) |
| `docs/` | the mkdocs site; `docs/pipeline/` is the operations guide |
| `python/tests/data/ulbridge.log` | the 144-line fixture every documented count comes from |

## Setup

Run everything from the **repository root**: task defaults such as
`file:data/capture`, `sqlite:///data/catalog.db` and `data/dbt` are relative
to the working directory.

```bash
uv sync --project python          # default groups: dev, runner (dbt, glue), airflow
uv run --project python rekep --version
```

`rekep` is not published on PyPI. Outside a checkout install it from one,
`pip install "./python[iceberg]"` (add `glue` or `s3tables` for those
catalogs), or from Git with
`pip install "rekep[iceberg] @ git+https://github.com/Platob/yggfin#subdirectory=python"`.
That is enough for every task but `build_dbt`, which also needs `dbt-core` and
`dbt-duckdb` (the `runner` group) and the `data/dbt` project.

## Driving tasks: `rekep tasks`

In a checkout prefix every command with `uv run --project python` (from the
repo root); with the package installed, `rekep` alone.

```bash
rekep tasks list                      # JSON: name, summary, targets of all nine
rekep tasks parse_fix_raw --help      # its commands and every default
rekep tasks parse_fix_raw show        # the parameters a run would take (JSON)
rekep tasks parse_fix_raw deploy      # create the tables it writes, if missing
rekep tasks parse_fix_raw run         # run once, print one result line
```

Each of `show`, `run` and `deploy` takes the same overrides:

- `--parameter NAME=VALUE`, repeatable. VALUE is read as JSON first, then as
  plain text: `start=2026-08-14` is text, `snapshot_millis=500` is a number,
  `codec_options='{"include_msgtypes": ["8"]}'` an object, `registry=null` null.
  Anything that parses as JSON is JSON: `start=20260814` is the number 20260814
  (epoch nanoseconds, i.e. 1970). Quote such text as a JSON string:
  `--parameter 'start="20260814"'`.
- `--parameters-file PATH`, a JSON object of overrides (keep catalogs and other
  nested settings here).
- Precedence: shipped defaults < `--parameters-file` < `--parameter`. An
  override replaces the parameter whole: a `catalog` override is the whole
  mapping, not merged into the default one. `show` prints what a run would take.
- A name the task does not declare is refused before anything runs.

`run` contract, which scripts and Airflow rely on:

- stdout carries exactly one compact JSON result; log records and any task
  output go to stderr.
- exit 0 only with a valid result; exit 2 for a command-line usage error
  (unknown option or task); exit 1 for everything else. A task that fails
  prints its traceback and a `✗ <task>: <Error>: <message>` line; a refused
  parameter prints one `✗` line. Nothing reaches stdout and no result file is
  written.
- `--result-file PATH` also publishes the result atomically (whole or absent).
- records are INFO (one per completed operation) unless the global option
  says otherwise: `rekep --log-level DEBUG tasks <name> run` adds scans,
  projections and files (the option goes before `tasks`). `build_dbt` and
  `optimize_iceberg` configure their own records from a `log_level` parameter,
  which `--log-level` sets unless the parameter is named itself.
- every result has `task, read, written, skipped, sources, targets, window,
  elapsed_ms` (`window` bounds in epoch nanoseconds) plus task-specific keys:
  `messages` (parse_fix_raw), `events`/`outside_window` (parse_fix_refined),
  `snapshot_id` (parse_books), `source_snapshot_id` (event tasks),
  `models`/`tests`/`warned`/`rows` (build_dbt), `tables`/`expired`/`deleted`/
  `byte_size`/`reports` (optimize_iceberg). See `docs/pipeline/operations/logs.md`.

`deploy` creates the task's declared tables that its catalog lacks and prints
`{"catalog": ..., "tables": {"fix.raw": "created"|"present"|"missing"}}`;
`--dry-run` only reports (`missing`), and `--table-property NAME=VALUE` sets
properties on the tables it creates. Existing tables are never altered. The
FIX tables take the shape of the task's own `registry`/`codec_options`,
exactly as its run would create them, and a registry or codec option the run
would refuse is refused first. `build_dbt` and `optimize_iceberg` declare
nothing to deploy (dbt models create their tables on first build). Every task
also creates its own target on first write, so `deploy` matters where the
catalog is owned by someone else (Glue, S3 Tables).

To deploy every pipeline table:

```bash
for task in parse_messages parse_fix_raw parse_fix_refined parse_books \
            parse_orders parse_quotes parse_executions; do
  uv run --project python rekep tasks "$task" deploy --parameters-file catalog.json
done
```

## Run the pipeline locally

The fixture is dated 2026-08-14; a date given as `end` means the end of that
day, so `start=end=2026-08-14` is that whole day.

```bash
uv run --project python rekep tasks parse_messages run --parameter start=2026-08-14 \
  --parameter end=2026-08-14 --parameter filesystem=file:python/tests/data/ulbridge.log
uv run --project python rekep tasks parse_fix_raw run --parameter start=2026-08-14 --parameter end=2026-08-14
uv run --project python rekep tasks parse_fix_refined run --parameter start=2026-08-14 --parameter end=2026-08-14
uv run --project python rekep tasks build_dbt run
```

The four report read/written/skipped 144/144/0, 144/49/30 (`messages` 79),
49/19/0 and 29/31/0. This writes `data/catalog.db` and `data/warehouse/`;
delete both to start over, or point every run at a scratch catalog. SQLite
creates the database file but not its directory:

```bash
mkdir -p /tmp/rekep
cat > /tmp/rekep/catalog.json <<'JSON'
{"catalog": {"name": "rekep", "properties": {
  "type": "sql", "uri": "sqlite:////tmp/rekep/catalog.db", "warehouse": "file:///tmp/rekep/warehouse"}}}
JSON
uv run --project python rekep tasks parse_messages run --parameters-file /tmp/rekep/catalog.json \
  --parameter start=2026-08-14 --parameter end=2026-08-14
```

Order matters: each task reads only the table before it. A replay of the same
window lands the same rows under the same keys (tables hold each row once).

From Python the same run is `Task(name).run(overrides)`; it returns the
validated result dict. Only `build_dbt` and `optimize_iceberg` configure
logging themselves; call `rekep.logs.configure()` first to see the others'
records:

```python
from pathlib import Path

from rekep.tasks import TASKS, Task

Path("/tmp/rekep").mkdir(exist_ok=True)
catalog = {"name": "rekep", "properties": {
    "type": "sql", "uri": "sqlite:////tmp/rekep/catalog.db", "warehouse": "file:///tmp/rekep/warehouse"}}
print([task.name for task in TASKS])
print(Task("parse_fix_raw").deploy({"catalog": catalog}, dry_run=True))  # {"catalog": ..., "tables": ...}
result = Task("parse_fix_raw").run({"catalog": catalog, "start": "2026-08-14", "end": "2026-08-14"})
```

## When a run fails

| message | meaning |
| --- | --- |
| `<task> takes no X; it takes ...` | misspelled or foreign parameter (checked before anything runs) |
| `window [...) is empty` / `end=... is not an instant` | bad bounds; a date `end` is the end of that day |
| `FileNotFoundError: <uri>` (parse_messages) | the capture URI does not exist |
| `FIX registry contains no specification fields` | `registry` points at an empty/wrong dictionary |
| `unable to open database file` | the SQLite catalog's directory does not exist (a scratch dir not created, or the relative default outside the repo root) |
| `URI missing, please provide using --uri ...` | a `catalog` override without its `properties` (an override replaces the mapping whole) |
| `expected a nonnegative book snapshot_id` | bad pinned books snapshot |
| `has no snapshot N: table is missing` / `Snapshot not found: N` | the pinned books table or snapshot does not exist |
| `ArrowInvalid ... expected a bid or ask side` (parse_books) | admitted market data the native book fold refuses |

Re-run with `rekep --log-level DEBUG ...` to see what was scanned and written,
then inspect the tables from Python (below).

## Windows

- `[start, end)`, over `currunix` (the event instant). A bound is an ISO
  instant (`2026-08-14T10:00:00Z`), a date (`2026-08-14`), epoch nanoseconds
  (a JSON number), or a named instant: `now`, `today`, `yesterday`,
  `tomorrow`, `epoch` (`utcnow`, `utctoday` alike). A date as `end` is the
  end of that day. Neither bound = the last day up to now (what a nightly run
  means). An empty or inverted window is refused.
- `parse_messages` passes the window to the native read as its `where`; a line
  its header cannot date takes the object's modification time.
- `parse_fix_refined` also reads the hour before `start` as lifecycle context
  and writes only the window (plus still-undated epoch rows).
- Market tasks (`parse_books`, the three event tasks) replace exactly
  `[start, end)` atomically: an empty rerun clears the window; rows outside it
  survive. Books start with no depth before `start`.

## Market fan-out from one book snapshot

`parse_books` returns `snapshot_id`; hand the same id and window to the three
event tasks so they read one commit even if `market.books` moves on. Wait on
each child, since a bare `wait` succeeds whatever they returned:

```bash
uv run --project python rekep tasks parse_books run --parameter start=2026-09-21T10:00:00Z \
  --parameter end=2026-09-21T10:00:10Z --result-file /tmp/books.json
SNAP=$(python -c 'import json; print(json.load(open("/tmp/books.json"))["snapshot_id"])')
pids=()
for kind in orders quotes executions; do
  uv run --project python rekep tasks "parse_$kind" run --parameter start=2026-09-21T10:00:00Z \
    --parameter end=2026-09-21T10:00:10Z --parameter "snapshot_id=$SNAP" &
  pids+=("$!")
done
for pid in "${pids[@]}"; do wait "$pid" || exit 1; done
```

`snapshot_id: null` pins whatever head the task finds; `0` means "the source
had no commit" and reads nothing. The bundled August fixture is not a book
demo (it has an AE report with no valid sided trade); use real refined data.

## Catalogs

`catalog` is `{"name": ..., "properties": {...}}` (PyIceberg catalog + FileIO
properties). `deploy` refuses any other key; a run ignores one. Put it in a
parameters file. Never put credentials in parameters, Airflow Params or CLI
arguments: use roles, profiles or the standard AWS environment.

| mode | properties |
| --- | --- |
| local (default) | `type: sql`, `uri: sqlite:///data/catalog.db`, `warehouse: data/warehouse` |
| SQL catalog, S3 data | `type: sql`, `uri: <sqlalchemy url>`, `warehouse: s3://bucket/prefix`, `s3.region` |
| AWS Glue | `type: glue`, `warehouse: s3://bucket/prefix`, `glue.region`, `s3.region` |
| S3 Tables | `type: s3tables`, `warehouse: arn:aws:s3tables:<region>:<account>:bucket/<name>` |
| S3 Tables via Glue | `type: s3tables`, `warehouse: <account>:s3tablescatalog/<name>`, `rest.signing-region` |

S3-compatible stores add `s3.endpoint` (and `s3.force-virtual-addressing=false`
where needed). `s3tables` endpoints also honour `AWS_ENDPOINT_URL_S3TABLES` /
`AWS_ENDPOINT_URL_GLUE`. Full reference: `docs/pipeline/operations/deploy.md`
and `docs/storage/iceberg.md`.

## Task parameters worth knowing

- `parse_messages`: `filesystem` is a file, directory or prefix URI
  (`file:data/capture`, `file:///abs/path`, `s3://bucket/prefix?region=eu-west-1`,
  `s3://b/p?endpoint_override=host:9000&scheme=http&force_path_style=true`);
  source credentials/endpoints belong on this URI, not on the catalog.
  `rowheader: null` is the shipped `ULBRIDGE_ROWHEADER`; a custom header must
  keep the same capture names (it is refused otherwise).
- `parse_fix_raw`, `parse_fix_refined`, `parse_books`: `registry` (null = the
  bundled FIX dictionary, or a directory/URI of another; `config/README.md`)
  and `codec_options` (null = native defaults; an object is forwarded to the
  native `FixCodec`, which validates every keyword: `batch_row_size`,
  `include_msgtypes`, `exclude_msgtypes`, `threads`, `official_time_delay_ms`,
  `snapshot_ns`, ...). Give the refined and books stages the same `registry` as
  the raw stage. Source-table names (`messages`, `raw`, `refined`, `books`) are
  parameters too.
- `parse_books`: `snapshot_millis` (0 = off) emits book snapshots on that grid.
- `build_dbt`: `project` (`data/dbt`), `profiles`, `target`, `select` (keep it
  null for the shipped project, which builds whole: DuckDB is `:memory:` and
  its tests span products, so a partial selection fails), `catalog` (null =
  `REKEP_DBT_CATALOG` from the environment, else the profile's), `log_level`.
- `optimize_iceberg`: `namespace` (null = all), `branch`, `min_files`,
  `retain`, `snapshot_age_days`, `orphan_age_days`, `remove_orphans` and
  `metadata` (booleans: write `false`, not `False`), `log_level`. It compacts,
  expires snapshots and sweeps orphans; see `docs/storage/iceberg.md#maintenance`
  before running it on S3 Tables, whose service owns its files:

```bash
uv run --project python rekep tasks optimize_iceberg run --parameters-file catalog.json \
  --parameter namespace=fix --parameter remove_orphans=false
```

The pages under `docs/pipeline/tasks/` explain each streaming task's
parameters; four of them (`parse-messages`, `parse-fix-raw`,
`parse-fix-refined`, `build-dbt`) also show real rows the fixture lands.

## dbt products

`build_dbt` runs the dbt project in `data/dbt` (`data/dbt/README.md`). Models
live in `data/dbt/models/`; a model's `config()` block is its Iceberg
declaration (`table`, `primary_key`, `partition_by`, `sort_by`, `arrow_types`,
`mode`), and a source is one Iceberg read through the same catalog
(`docs/pipeline/tasks/build-dbt.md#what-a-model-declares`). Products read
`fix.refined`, never `fix.raw`. The catalog comes from the task's `catalog`
parameter, else `REKEP_DBT_CATALOG` (the same mapping as JSON), else the
profile. dbt's own CLI works too, from the repo root:
`uv run --project python dbt build --project-dir data/dbt --profiles-dir data/dbt`.

## Reading tables from Python

Public code imports `rekep`, never `yggdryl` directly.

```python
from rekep.fix import SORT_COLUMNS, fix_window_filter
from rekep.iceberg import IcebergCatalog
from rekep.times import window_of

catalog = IcebergCatalog.from_dict({"name": "rekep", "properties": {
    "type": "sql", "uri": "sqlite:///data/catalog.db", "warehouse": "data/warehouse"}})
try:
    refined = catalog.dataset("fix.refined")
    try:
        reader = refined.read_arrow_reader(   # streams; pushes filter/projection/limit down
            columns=("crosscode", "currunix", "seqnum", "curruuid", "msgtype", "state"),
            row_filter=fix_window_filter(window_of("2026-08-14", "2026-08-14")),
            order_by=SORT_COLUMNS,             # ordered columns must be projected
            snapshot_id=None,                   # or a pinned snapshot
        )
        table = reader.read_all()
    finally:
        refined.close()
finally:
    catalog.close()
```

`catalog.tables()` lists identifiers; `dataset.read_arrow_table()` reads a
small table whole. Parse one line directly with the codec:

```python
from rekep.fix import FixCodec, fix_registry

codec = FixCodec(fix_registry())
message = next(iter(codec.parse_line(b"Sending : 8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|")))
assert message.by_name("symbol").as_py() == "AAPL"
```

Keys to join on: `logs.messages.curruuid` is a line; FIX rows are keyed on
`curruuid` (one event), and `srcuuids` lists the line identities an event was
logged on. `crosscode` is the business id (OrderID, ClOrdID, ...) on FIX rows.
Column meanings: `docs/products/`.

## Contracts

The four reviewed Iceberg contracts live in `schemas/rekep/`; regenerate after a
deliberate shape change and review the diff:

```bash
uv run --project python rekep fields dump --pyclass rekep.text:Message --target schemas/rekep/message.json
uv run --project python rekep fields dump --pyclass rekep.fix:fix_message_field --target schemas/rekep/fixmsg.json
uv run --project python rekep fields dump --pyclass rekep.market:book_field --target schemas/rekep/book.json
uv run --project python rekep fields dump --pyclass rekep.market:market_event_field --target schemas/rekep/marketevent.json
uv run --project python rekep fields load --target schemas/rekep/fixmsg.json   # validate one
```

An identity or key change means rebuilding affected tables from source under
one native revision (see AGENTS.md, Workflow); never mix old and new keys.

## Airflow

`airflow/pipeline.py` declares `rekep_ingestion` (daily; the seven streaming
tasks; the three event tasks pinned to the book task's committed snapshot by
`upstream_task_id`), `airflow/products.py` declares `rekep_products`
(`build_dbt`, scheduled on the `fix.refined` Asset). `airflow/dispatch.py`
builds every node, and `REKEP_EKS_CONFIG` decides where it runs.

Both operators resolve parameters the same way: the checkout's task defaults <
operator `parameters` < DAG Params < the run's data interval (`start`/`end`),
except bounds named in the run's conf win; the upstream snapshot pin applies
last. An unknown task name or undeclared operator parameter fails before
anything is launched. Each validates the result and attaches its counts to the
outlet Assets. Trigger one day:

```bash
uv run --project python --group airflow airflow dags trigger rekep_ingestion \
  --conf '{"start": "2026-08-14", "end": "2026-08-14"}'
```

**On the worker** (`REKEP_EKS_CONFIG` unset): `RekepOperator` runs
`uv run --project <checkout>/python --group runner --no-sync --offline ... --
rekep tasks <name> run --parameters-file <attempt>/parameters.json
--result-file <attempt>/result.json` with the checkout as working directory.

**On EKS** (`REKEP_EKS_CONFIG=/path/eks.json` in the DAG processor's and the
workers' environment): `EksRekepOperator`, an `EksPodOperator`, runs the pod command
`rekep tasks <name> run --parameter NAME=<json> ... --result-file
/airflow/xcom/return.json`; the XCom sidecar hands the result back. The
document is `EksPodOperator` keywords, shared, with per-task replacements:

```json
{
  "cluster_name": "market-data",
  "image": "123456789012.dkr.ecr.eu-west-1.amazonaws.com/rekep:abc1234",
  "namespace": "rekep",
  "service_account_name": "rekep",
  "region": "eu-west-1",
  "container_resources": {"requests": {"cpu": "2", "memory": "8Gi"}},
  "tasks": {"build_dbt": {"container_resources": {"requests": {"memory": "16Gi"}}}}
}
```

- Build the image from the same commit the DAGs ship from (`docker build -t
  <registry>/rekep:<commit> .` at the repo root) and push it where the cluster
  pulls from; the pod receives every parameter the checkout declares.
- The pod's AWS access is its service account's IAM role (IRSA or EKS Pod
  Identity). Airflow's `aws_conn_id` needs an EKS access entry and, in the
  namespace, pods create/get/list/watch/patch/delete, `pods/log` get and
  `pods/exec` create/get (the result is read by exec into the sidecar); with
  `deferrable=True` the triggerer needs the same. The worker still needs
  `rekep` installed: it resolves parameters and validates results.
- Every task's `catalog` must be shared (Glue, S3 Tables, a SQL service with
  an `s3://` warehouse); the image's `data/` is read-only, so the local
  default fails in a pod. Parameters reach the pod as arguments: keep a
  credential-bearing property (a SQL `uri` with a password) out of them and
  give the pod `PYICEBERG_CATALOG__<NAME>__<PROPERTY>` from a Secret through
  `pod_template_dict` (container `base`, `envFrom`).
- The sidecar image defaults to `alpine` from Docker Hub; a private cluster
  sets `xcom_sidecar_container_image` in the `kubernetes_default` connection's
  extras.
- `rekep_products` does not inherit ingestion's conf: set its catalog with
  `REKEP_DBT_CATALOG` in the worker environment, or under
  `tasks.build_dbt.env_vars` on EKS.
- Objects the operator takes only as Kubernetes objects (`volumes`,
  `volume_mounts`, `env_from`) go in `pod_template_dict`; `cmds`,
  `arguments` and `do_xcom_push` are the operator's own; name a pod with
  `pod_name`.

Worker setup, the dispatch document and the image: `docs/pipeline/airflow.md`,
`airflow/README.md`.

## Changing the code

Ownership (from AGENTS.md): Yggdryl owns `Field`, text reading, codecs, FIX
parsing and lifecycle; Arrow owns shape kernels; PyIceberg owns tables and
commits; yggfin owns the text `Message` contract and the thin seams. Never add
a second Field class, filesystem layer, text reader, codec or registry here;
no Python row loops over Arrow data; prefer deleting to compatibility layers.

To add or change a task:

1. `python/src/rekep/tasks/<name>.py`: module docstring (its first line is the
   CLI summary), `TARGETS`, and `run(*, ...) -> dict` returning
   `rekep.logs.Stage(...).finished(...)`. Keyword-only parameters, no defaults.
2. `python/src/rekep/tasks/<name>.json`: the defaults, keys exactly `run`'s
   parameters, canonical `json.dumps(indent=2)` + newline.
3. Add the name to `NAMES` in `python/src/rekep/tasks/task.py` (graph order);
   add deployable tables to `rekep.deploy.TABLES`.
4. Tests under `python/tests/` (registry pins in `tests/tasks/`), a page under
   `docs/pipeline/tasks/` plus `mkdocs.yml`, and a `dispatch.node(...)` in a
   DAG if scheduled.

Checks before a commit:

```bash
cd python
uv run ruff check . ../airflow ../tools && uv run ruff format --check . ../airflow ../tools
uv run pytest -q                      # unit (integration excluded by default)
uv run pytest -q -m integration       # real local Iceberg transactions, a few minutes
cd .. && uv run --project python --group runner python tools/pipeline_samples.py --check
UV_PROJECT_ENVIRONMENT=/tmp/docs-venv uv run --project python --group docs mkdocs build --strict
docker build -t rekep:dev . && REKEP_IMAGE=rekep:dev uv run --project python pytest -q \
  -m integration python/tests/test_eks_rekep_operator.py -k image
```

CI lints `python/` only, runs the unit suite and the market integration tests
on Linux and Windows under Python 3.10 and 3.13, the Airflow DAG run on Linux
with 3.13, builds the task image and runs each stage in it, and builds the
docs strictly; lint `../airflow` and `../tools` yourself. The docs build uses
its own environment so the shared `.venv` keeps its default groups.
`tools/pipeline_samples.py --check` proves the tasks still land exactly the
rows the task pages show, and without `--check` regenerates them after a
deliberate change; `uv run --project python python tools/fix_registry_dump.py`
regenerates `docs/assets/fix-*.json` after a registry change.

## Pitfalls

- Relative defaults resolve against the working directory: from another
  directory with a `data/` folder a run silently uses another catalog, and
  without one the default SQLite catalog fails to open. Run from the repo root
  or give absolute catalog locations.
- `--log-level` is a global option: `rekep --log-level DEBUG tasks ...`, not
  after the task name.
- No window = the last day up to now; the fixture is dated 2026-08-14, so an
  unbounded run over it reads nothing.
- A value made only of digits is a JSON number: `end=2026` or `start=20260814`
  is epoch nanoseconds (1970), not a year or a compact date. Write dashed
  dates, or quote: `'start="20260814"'`.
- Booleans are JSON: `remove_orphans=false`; `False` is text and is refused.
- `fix.raw` rows have empty `seqnum`/`prevuuid`/`parentuuids` by design; the
  chain is `fix.refined`'s. Products read `fix.refined`, never `fix.raw`.
- `logs.messages` and the FIX tables replay by key within the hour partition;
  a changed identity leaves the old row — rebuild the window or table.
- Market window replacement is exact; `parse_books` does not reconstruct depth
  from before `start`.
- `build_dbt` needs the `runner` group (dbt) and the `data/dbt` project path.
