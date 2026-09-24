---
name: rekep
description: Use and extend `rekep`, the processing library that turns ULBridge text captures into FIX and market Iceberg tables. Use for any work in this repository or with the `rekep` package - landing a window through the `rekep.pipeline` stages (`parse_messages`, `parse_fix_raw`, `parse_fix_refined`, `parse_books`, `parse_events`) over an `IcebergCatalog`, choosing windows and catalogs (local SQLite, S3, AWS Glue, S3 Tables), pinning the market event kinds to one book snapshot, creating tables ahead of a run with `rekep.deploy`, reading landed tables from Python, the FIX codec and registry, table contracts (`iceberg_contract`, `schemas/rekep`), the dbt products built with `dbt build`, and changing, testing or documenting the code.
---

# rekep

`rekep` (Python package under `python/`) streams text captures through the
native Yggdryl FIX codec into Iceberg. It is a library: every pipeline step is
a **function of `rekep.pipeline`** over one `IcebergCatalog` and one window,
and where, when and over what a stage runs is the caller's.

```text
capture URI    -> parse_messages             -> logs.messages
logs.messages  -> parse_fix_raw              -> fix.raw
fix.raw        -> parse_fix_refined          -> fix.refined
fix.refined    -> parse_books                -> market.books
market.books   -> parse_events("orders")     -> market.orders      (the three kinds
               -> parse_events("quotes")     -> market.quotes       read one pinned
               -> parse_events("executions") -> market.executions   book snapshot)
fix.refined    -> dbt build (optional)       -> orders.events, orders.current, executions.fills
```

`AGENTS.md` is the binding contract for how code here is written and who owns
what; read it before changing code. This skill is the operating manual.

## Where things are

| path | what |
| --- | --- |
| `python/src/rekep/pipeline.py` | the stages, `Landed`, and the table names they write (`MESSAGES`, `RAW`, `REFINED`, `BOOKS`, `EVENTS`) |
| `python/src/rekep/deploy.py` | `deploy(catalog)` and `TABLES`: the graph's tables, created ahead of a run |
| `python/src/rekep/{text,fix,market,times,iceberg,fields}` | the library the stages compose |
| `python/src/rekep/dbt.py` | the dbt-duckdb plugin `data/dbt/profiles.yml` loads |
| `python/tests/` | the suite; `-m integration` runs real Iceberg transactions |
| `data/capture/ulbridge.log` | the 144-line capture every documented count comes from (`data/README.md`) |
| `data/dbt/` | the dbt project (`data/dbt/README.md`) |
| `config/` | where an operator's own FIX dictionary goes (`config/README.md`) |
| `schemas/rekep/*.json` | reviewed Iceberg contracts (Message, FixMsg, Book, MarketEvent) |
| `docs/` | the mkdocs site; `docs/pipeline/` explains each stage |
| `tools/fix_registry_dump.py` | regenerates `docs/assets/fix-*.json` after a registry change |

## Setup

Work from the **repository root**: the examples' relative paths
(`file:data/capture`, `data/dbt`) resolve against the working directory.

```bash
uv sync --project python          # default groups: dev, dbt
```

`rekep` is not published on PyPI. Outside a checkout install it from one,
`pip install "./python[iceberg]"` (add `glue` or `s3tables` for those
catalogs), or from Git with
`pip install "rekep[iceberg] @ git+https://github.com/Platob/yggfin#subdirectory=python"`.
The dbt products also need `dbt-core` and `dbt-duckdb` (the `dbt` group) and
the `data/dbt` project.

## Land a window

Each stage takes an open catalog and a window, reads its source table,
replaces its window of its target, creates the target where it is missing,
and answers `Landed`. It never closes the catalog; the caller does.

```python
import tempfile
from pathlib import Path

from rekep.iceberg import IcebergCatalog
from rekep.pipeline import Landed, parse_fix_raw, parse_fix_refined, parse_messages
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
catalog = IcebergCatalog.from_dict({"name": "rekep", "properties": {
    "type": "sql", "uri": f"sqlite:///{root}/catalog.db", "warehouse": str(root / "warehouse")}})
day = window_of("2026-08-14", "2026-08-14")  # the capture's day: a date `end` is the end of that day
try:
    assert parse_messages("file:data/capture", catalog, day) == Landed(read=144, written=144)
    assert parse_fix_raw(catalog, day) == Landed(read=144, written=49, skipped=30)
    assert parse_fix_refined(catalog, day) == Landed(read=49, written=19)
finally:
    catalog.close()
```

Order matters: each stage reads only the table before it. A replay of the same
window lands the same rows under the same keys and answers the same numbers,
so a table holds each row once. A stage per process is fine: open the catalog,
run it, close it.

`Landed` holds `read` (source rows the window selected), `written` (target rows
carried into the table), `skipped` (answered rows the target's key folded into
a written one: 30 frames of `fix.raw` are one message logged again at another
hop) and `snapshot_id` (the `market.books` snapshot `parse_books` committed or
`parse_events` read; None for the other stages).

The stages log to the `rekep.*` loggers and configure nothing:
`logging.basicConfig()` with `logging.getLogger("rekep").setLevel(logging.INFO)`
shows each table created and each commit, and `DEBUG` adds scans,
projections and files.

## When a stage fails

| message | meaning |
| --- | --- |
| `FileNotFoundError: <uri>` (`parse_messages`) | the capture URI does not exist; an absent path would otherwise read as a window without lines |
| `window [...) is empty` / `end=... is not an instant` | bad bounds; a date `end` is the end of that day |
| `FIX registry contains no specification fields` | `fix_registry(location)` names an empty, missing or wrong dictionary |
| `unable to open database file` | the SQLite catalog's directory does not exist (SQLite creates the file, not its directory) |
| `URI missing, please provide using --uri ...` | a catalog mapping without its `properties` |
| `FixCodec.__new__() got an unexpected keyword argument` | a codec pin the native codec does not declare |
| `expected orders, quotes or executions` | `parse_events` given another kind |
| `expected a nonnegative book snapshot_id or None` | a bad pinned books snapshot |
| `has no snapshot N: table is missing` / `Snapshot not found: N` | the pinned books table or snapshot does not exist; nothing was written |
| `ArrowInvalid ... expected a bid or ask side` / `expected a symbol outside global mode` (`parse_books`) | an admitted message the native book fold refuses; the table's prior snapshot stays visible |

Turn on `DEBUG` records to see what was scanned and written, then inspect the
tables from Python (below).

## Windows

- `rekep.times.window_of(start, end)` is `[start, end)` as aware UTC instants,
  over `currunix` (the event instant). A bound is a `datetime`, an ISO instant
  (`2026-08-14T10:00:00Z`), a date (`2026-08-14`), epoch nanoseconds (an
  `int`), or a named instant: `now`, `today`, `yesterday`, `tomorrow`, `epoch`
  (`utcnow`, `utctoday` alike). A date as `end` is the end of that day.
  Neither bound is the last day up to now. An empty or inverted window is
  refused.
- `parse_messages` hands the window to the native read as its `where`; a line
  its header cannot date takes the object's modification time.
- `parse_fix_refined` also reads `HISTORY`, the hour before `start`, as
  lifecycle context and writes only the window (plus still-undated epoch rows).
- The market stages replace exactly `[start, end)` atomically: an empty rerun
  clears the window; rows outside it survive. Books start with no depth before
  `start`.

## Market: books, then the kinds off one snapshot

`parse_books` answers the snapshot it committed; hand that id and the same
window to `parse_events` for each kind, so all three read one commit even if
`market.books` moves on. The kinds are independent and may run in separate
processes. Over the capture, the midday hour is a book window; the whole day
is not, because the capture holds an AE report whose side states no
`Side(54)` and a cancel reject that names no symbol, both of which the fold
refuses.

```python
import tempfile
from pathlib import Path

from rekep.iceberg import IcebergCatalog
from rekep.pipeline import (
    EVENTS,
    parse_books,
    parse_events,
    parse_fix_raw,
    parse_fix_refined,
    parse_messages,
)
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
catalog = IcebergCatalog.from_dict({"name": "rekep", "properties": {
    "type": "sql", "uri": f"sqlite:///{root}/catalog.db", "warehouse": str(root / "warehouse")}})
day = window_of("2026-08-14", "2026-08-14")
midday = window_of("2026-08-14T12:00:00Z", "2026-08-14T13:00:00Z")
try:
    parse_messages("file:data/capture", catalog, day)
    parse_fix_raw(catalog, day)
    parse_fix_refined(catalog, day)
    books = parse_books(catalog, midday)
    written = {
        kind: parse_events(kind, catalog, midday, snapshot_id=books.snapshot_id).written
        for kind in EVENTS
    }
    assert (books.read, books.written) == (12, 5)
    assert written == {"orders": 1, "quotes": 0, "executions": 7}
finally:
    catalog.close()
```

`snapshot_id=None` pins whatever head the call finds; `0` means the books
table had no commit, reads nothing and empties the window. A positive id of a
missing table is refused before anything is written. After a book
replacement, run the kinds again against the new snapshot.

## Catalogs

`IcebergCatalog.from_dict` takes `{"name": ..., "properties": {...}}`
(PyIceberg catalog and FileIO properties). Never put credentials in the
mapping: use roles, profiles or the standard AWS environment.

| mode | properties |
| --- | --- |
| local | `type: sql`, `uri: sqlite:////abs/catalog.db`, `warehouse: /abs/warehouse` |
| SQL catalog, S3 data | `type: sql`, `uri: <sqlalchemy url>`, `warehouse: s3://bucket/prefix`, `s3.region` |
| AWS Glue | `type: glue`, `warehouse: s3://bucket/prefix`, `glue.region`, `s3.region` |
| S3 Tables | `type: s3tables`, `warehouse: arn:aws:s3tables:<region>:<account>:bucket/<name>` |
| S3 Tables via Glue | `type: s3tables`, `warehouse: <account>:s3tablescatalog/<name>`, `rest.signing-region` |

S3-compatible stores add `s3.endpoint` (and `s3.force-virtual-addressing=false`
where needed). `s3tables` endpoints also honour `AWS_ENDPOINT_URL_S3TABLES` /
`AWS_ENDPOINT_URL_GLUE`. Full reference: `docs/storage/catalogs.md` and
`docs/storage/iceberg.md`.

## Stage parameters worth knowing

- `parse_messages(source, catalog, window, *, rowheader=None, target=MESSAGES)`:
  `source` is a file, directory or prefix URI (`file:data/capture`,
  `file:///abs/path`, `s3://bucket/prefix?region=eu-west-1`,
  `s3://b/p?endpoint_override=host:9000&scheme=http&force_path_style=true`),
  bound and closed by the stage, or an `IOBase` the caller keeps open. Source
  credentials and endpoints belong on this URI, not on the catalog.
  `rowheader=None` is `ULBRIDGE_ROWHEADER`; another header must keep the same
  capture names, and is refused otherwise.
- `parse_fix_raw`, `parse_fix_refined`, `parse_books`: `codec`, `fix_codec()`
  when None. Build one with `fix_codec(registry, **pins)`, where `registry` is
  `fix_registry()` (the bundled dictionary) or `fix_registry("file:config/fix")`
  (`config/README.md`), and the pins go to the native `FixCodec`, which
  validates every keyword: `batch_row_size`, `include_msgtypes`,
  `exclude_msgtypes`, `threads`, `official_time_delay_ms`, `snapshot_ns`, ...
  Hand the refined and books stages the codec the raw stage parsed with.
- `parse_books`: `snapshot_millis` (0 = off) emits book snapshots on that grid.
- `parse_events(kind, catalog, window, *, snapshot_id=None, ...)`: `kind` is
  `orders`, `quotes` or `executions`.
- Every stage takes a `target` table name, and every stage after
  `parse_messages` a `source` table name, defaulted to the constants above.

## Deploying tables ahead of a run

Every stage creates its own target on first write; `deploy` matters where the
catalog is owned by someone else (Glue, S3 Tables) and is created once,
ahead of the jobs that fill it.

```python
import tempfile
from pathlib import Path

from rekep.deploy import deploy
from rekep.fix import fix_codec
from rekep.iceberg import IcebergCatalog

root = Path(tempfile.mkdtemp())
catalog = IcebergCatalog.from_dict({"name": "rekep", "properties": {
    "type": "sql", "uri": f"sqlite:///{root}/catalog.db", "warehouse": str(root / "warehouse")}})
try:
    assert set(deploy(catalog, dry_run=True).values()) == {"missing"}
    assert deploy(catalog, tables=["fix.raw"], codec=fix_codec()) == {"fix.raw": "created"}
    assert deploy(catalog)["fix.raw"] == "present"
finally:
    catalog.close()
```

`deploy` answers `created`, `present` or, under `dry_run`, `missing` per table,
and never alters an existing table. `tables` narrows it to some of `TABLES`,
`table_properties` sets properties on the tables it creates, and `codec` types
the two FIX tables exactly as the run parsing into them would. The dbt models
create their tables on first build.

## dbt products

dbt builds the project in `data/dbt` (`data/dbt/README.md`,
`docs/pipeline/dbt.md`) with its own CLI, from the repository root, once
`fix.refined` exists. `REKEP_DBT_CATALOG` names the catalog as the JSON of the
mapping above; without it the profile's local SQLite catalog under `data/` is
used.

```bash
REKEP_DBT_CATALOG='{"name": "rekep", "properties": {"type": "sql", "uri": "sqlite:////abs/catalog.db", "warehouse": "/abs/warehouse"}}' \
  uv run --project python dbt build --project-dir data/dbt --profiles-dir data/dbt
```

Over the capture's day it commits 16 `orders.events`, 8 `orders.current` and 7
`executions.fills` rows. The project builds whole: DuckDB is `:memory:` and its
tests span products, so a partial `--select` fails. A model's `config()` block
is its Iceberg declaration (`table`, `primary_key`, `partition_by`, `sort_by`,
`arrow_types`, `mode`), and a source is one Iceberg read through the same
catalog. Products read `fix.refined`, never `fix.raw`. A caller running
`dbtRunner` in its own process calls `rekep.dbt.released()` after the build to
close the catalogs the plugin opened.

## Reading tables from Python

Public code imports `rekep`, never `yggdryl` directly. With `catalog` open on
tables landed as above:

```python
from rekep.fix import SORT_COLUMNS, fix_window_filter
from rekep.times import window_of

refined = catalog.dataset("fix.refined")
try:
    reader = refined.read_arrow_reader(   # streams; pushes filter/projection/limit down
        columns=("crosscode", "currunix", "seqnum", "curruuid", "msgtype", "state"),
        row_filter=fix_window_filter(window_of("2026-08-14", "2026-08-14")),
        order_by=SORT_COLUMNS,             # ordered columns must be projected
        snapshot_id=None,                  # or a pinned snapshot
    )
    table = reader.read_all()
finally:
    refined.close()
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

The four reviewed Iceberg contracts live in `schemas/rekep/`; regenerate them
after a deliberate shape change and review the diff:

```python
from pathlib import Path

from rekep.fields import field_of
from rekep.fix import fix_message_field
from rekep.iceberg import iceberg_contract
from rekep.market import book_field, market_event_field
from rekep.text import Message

CONTRACTS = {
    "message": Message,
    "fixmsg": fix_message_field(),
    "book": book_field(),
    "marketevent": market_event_field(),
}
for name, declared in CONTRACTS.items():
    document = f"{iceberg_contract(field_of(declared))}\n"
    Path(f"schemas/rekep/{name}.json").write_text(document, encoding="utf-8")
```

`iceberg_contract_field(document, name)` reads one back and refuses a layout
one Field cannot hold; `python/tests/test_schemas.py` fails on drift. An
identity or key change means rebuilding affected tables from source under one
native revision (AGENTS.md, Pipeline); never mix old and new keys.

## Changing the code

Ownership (from AGENTS.md): Yggdryl owns `Field`, text reading, codecs, FIX
parsing and lifecycle; Arrow owns shape kernels; PyIceberg owns tables and
commits; rekep owns the text `Message` contract, the thin seams and the stages
that compose them. Never add a second Field class, filesystem layer, text
reader, codec or registry here; no Python row loops over Arrow data; prefer
deleting to compatibility layers.

To add or change a stage:

1. A function in `python/src/rekep/pipeline.py`, in graph order, taking
   `(catalog, window, *, source=..., target=...)` after any leading input and
   answering `Landed`. It composes library readers, opens its datasets through
   the catalog it is handed and closes them, never closes the catalog, and
   writes with one `overwrite_arrow_reader`: `merge_by=True` for a keyed
   table, `row_filter` for an exact-window replacement. Its table name is a
   constant beside the others and in `__all__`.
2. Its table in `rekep.deploy.TABLES`.
3. Tests: refusals and internals in `python/tests/test_pipeline.py`; the landed
   contract once as `integration` (`test_workflow.py` over the capture,
   `test_market_pipeline.py` for the market stages).
4. A page under `docs/pipeline/` and its entry in `mkdocs.yml`.

Checks before a commit:

```bash
cd python
uv run ruff check . ../tools && uv run ruff format --check . ../tools
uv run pytest -q                      # unit (integration excluded by default)
uv run pytest -q -m integration       # real local Iceberg transactions, a few minutes
cd .. && UV_PROJECT_ENVIRONMENT=/tmp/docs-venv uv run --project python --group docs mkdocs build --strict
```

CI lints `python/`, runs the unit suite and the market integration tests on
Linux and Windows under Python 3.10 and 3.13, and builds the docs strictly;
the whole integration suite runs on pushes to `main` and on a pull request
comment containing `--integration`. Lint `../tools` yourself. The docs build
uses its own environment so the shared `.venv` keeps its default groups.
`uv run --project python python tools/fix_registry_dump.py` regenerates
`docs/assets/fix-*.json` after a registry change.

## Pitfalls

- Relative locations resolve against the working directory: `file:data/capture`
  and the dbt profile's `sqlite:///data/catalog.db` mean another place from
  another directory. Work from the repo root or spell absolute locations.
- `window_of()` with no bounds is the last day up to now; the capture is dated
  2026-08-14, so an unbounded run over it reads nothing.
- An `int` bound is epoch nanoseconds: `window_of(20260814, ...)` starts in
  1970. Spell the date, `"2026-08-14"`.
- `fix.raw` rows have empty `seqnum`/`prevuuid`/`parentuuids` by design; the
  chain is `fix.refined`'s. Products read `fix.refined`, never `fix.raw`.
- `logs.messages` and the FIX tables replay by key within the hour partition;
  a changed identity leaves the old row, so rebuild the window or the table.
- Market window replacement is exact; `parse_books` does not reconstruct depth
  from before `start`.
- A stage never closes the catalog. Close it yourself: on Windows an open
  SQLite catalog is a file its caller cannot delete.
