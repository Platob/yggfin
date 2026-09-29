---
name: rekep
description: Use and extend `rekep`, the processing library that lands ULBridge FIX bridge captures as bronze and silver Iceberg tables. Use for any work in this repository or with the `rekep` package - running the `rekep.pipeline` tasks (`parse_log_messages`, `parse_fix_messages_raw`, `parse_fix_messages_refined`, `parse_books`, `parse_orders`, `parse_quotes`, `parse_executions`) over a window and a `Storages` of three catalogs (local SQLite, AWS Glue, S3 Tables), scheduling the graph (a Python runner, Airflow), pinning the flatteners to one book snapshot, deploying tables with `rekep.deploy`, reading landed tables, the FIX registry and codec (`FixRegistry.from_env`, `FixCodec.from_env`, `State`), the published contracts under `schemas/`, and changing, testing or documenting the code.
---

# rekep

`rekep` (the Python package under `python/`) reads the text a ULBridge FIX
bridge logs and lands it as Iceberg tables in three layers. It is a library:
every step is a **task**, a function of `rekep.pipeline` over one
`rekep.Storages` and one window, and where, when and over which window a task
runs is its caller's.

```text
capture                              -> parse_log_messages         -> bronze.record_keeping.log_messages
bronze.record_keeping.log_messages   -> parse_fix_messages_raw     -> bronze.record_keeping.fix_messages
bronze.record_keeping.fix_messages   -> parse_fix_messages_refined -> silver.record_keeping.fix_messages
silver.record_keeping.fix_messages   -> parse_books                -> silver.record_keeping.books
silver.record_keeping.books          -> parse_orders               -> silver.record_keeping.orders      (the three
                                     -> parse_quotes               -> silver.record_keeping.quotes       read one book
                                     -> parse_executions           -> silver.record_keeping.executions   snapshot)
```

A table is `<layer>.<namespace>.<table>`: the layer is one of the three
Iceberg catalogs `Storages` holds, `<namespace>.<table>` its name there. Gold
is the consumers' layer; no task writes it. `AGENTS.md` is the binding
contract for how code here is written and who owns what; read it before
changing code. This skill is the operating manual.

## Where things are

| path | what |
| --- | --- |
| `python/src/rekep/pipeline.py` | the tasks, `Landed`, the table names (`LOG_MESSAGES`, `FIX_MESSAGES_RAW`, `FIX_MESSAGES`, `BOOKS`, `ORDERS`, `QUOTES`, `EXECUTIONS`), `EVENTS`, `FLATTENERS`, `FLATTENED`, `HISTORY`, `SNAPSHOT_MILLIS`, `COMMIT_ROW_SIZE` |
| `python/src/rekep/storages.py` | `Storages`: one catalog per layer, `dataset("<layer>.<ns>.<table>")` |
| `python/src/rekep/deploy.py` | `deploy(storages)` and `TABLES`: the graph's tables, created ahead of a run |
| `python/src/rekep/text.py` | the bridge read: `text_options`, `log_message_field`, `CAPTURES`, `RECORD_CLOCK` |
| `python/src/rekep/fix.py` | the bundled registry install, the FIX doors, `fix_message_field`, `iceberg_event_field`, `UNDATED`, `REGISTRY_VARIABLE` |
| `python/src/rekep/market.py` | the book fold and the event flattening, `book_field`, `market_event_field` |
| `python/src/rekep/{times,iceberg,fields}` | windows, the Iceberg dataset, field metadata |
| `python/tests/` | unit tests mirroring the modules; `tests/storages/` lands the capture through every task under `-m integration` |
| `data/capture/ulbridge.log` | the 144-line capture every documented count reads (`data/README.md`) |
| `schemas/` | generated: each table's Iceberg contract and each layer's dbt sources (`schemas/README.md`) |
| `docs/` | the mkdocs site; `docs/tables/` and `docs/samples/` are generated |
| `tools/` | `schemas_dump.py`, `samples_dump.py`, `fix_registry_dump.py`: the generators |

## Setup

Work from the **repository root**: the examples' relative paths
(`file:data/capture/ulbridge.log`) resolve against the working directory.
The environment is `python/.venv`, which `uv sync --project python` creates
with the default `dev` group.

`rekep` is not published on PyPI. Outside a checkout install it from one,
`pip install "./python[iceberg]"` (add `glue` or `s3tables` for those
catalogs), or from Git with
`pip install "rekep[iceberg] @ git+https://github.com/Platob/yggfin#subdirectory=python"`.

## Land a window

```python
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from rekep import Storages
from rekep.pipeline import (
    FLATTENERS,
    Landed,
    parse_books,
    parse_fix_messages_raw,
    parse_fix_messages_refined,
    parse_log_messages,
)
from rekep.times import window_of

root = Path(tempfile.mkdtemp())
storages = Storages.from_dict({layer: {"name": layer, "properties": {
    "type": "sql", "uri": f"sqlite:///{root / layer}.db", "warehouse": str(root / layer)}}
    for layer in ("bronze", "silver", "gold")})
window = window_of("2026-08-14T00:00:00Z", "2026-08-14T16:30:00Z")
with storages:
    assert parse_log_messages("file:data/capture/ulbridge.log", storages, window) == Landed(129, 129)
    assert parse_fix_messages_raw(storages, window) == Landed(read=129, written=72, skipped=53)
    assert parse_fix_messages_refined(storages, window) == Landed(read=72, written=47)
    books = parse_books(storages, window)
    with ThreadPoolExecutor(max_workers=3) as pool:
        running = {kind: pool.submit(task, storages, window, snapshot_id=books.snapshot_id)
                   for kind, task in FLATTENERS.items()}
    written = {kind: future.result().written for kind, future in running.items()}
    assert (books.written, written) == (29, {"orders": 8, "quotes": 0, "executions": 7})
```

Order matters: each task reads only the table before it. Every task creates
its target where it is missing and never closes the catalogs. The three keyed
tasks merge on `curruuid`: a row their table lacks is inserted, one it holds
with other values replaced, one it holds as it is left alone, so a rerun
writes none, commits nothing and answers `written=0`; `parse_books` and the
flatteners replace their window, so a rerun writes the same rows again in a
new snapshot. Either way a retry is running it again. A task per process is
fine: open `Storages`, run, close.

`Landed` holds `read` (source rows the window selected), `written` (target
rows written: inserted or replaced by a keyed task, the window's rows for the
others), `skipped` (answered rows the key folded into a stored one: 53 bronze
FIX messages restate another hop's exactly, and on a rerun every row a keyed
task answers) and `snapshot_id` (the books snapshot `parse_books` committed
or a flattener read; None for the others). Tasks log to the `rekep.*`
loggers and configure nothing: `INFO` shows each table created and each
commit, `DEBUG` adds scans and files.

## When a task fails

| message | meaning |
| --- | --- |
| `FileNotFoundError: <uri>` | `parse_log_messages` got a capture that does not exist |
| `window [...) is empty` / `end=... is not an instant` | bad bounds; a date `end` is the end of that day |
| `expected one catalog per layer (bronze, silver, gold): missing ...` | a `Storages` mapping without every layer |
| `row header captures nothing for ...` | a `rowheader` that renames or drops a capture |
| `FIX registry contains no specification fields` | a codec over an empty dictionary folder |
| `FixCodec.__new__() got an unexpected keyword argument` | a codec pin the native codec does not declare |
| `ArrowInvalid ... expected a bid or ask operation` | `parse_books` met an admitted message the fold cannot read, such as one stating no `Side(54)` whose order has no live side to lend it one; the table's prior snapshot stays |
| `expected a nonnegative book snapshot_id or None` / `has no snapshot N: table is missing` | a bad or missing pinned books snapshot; nothing was written |
| `unable to open database file` | a SQLite catalog whose folder does not exist |

## Windows

- `rekep.times.window_of(start, end)` is `[start, end)` as aware UTC instants
  over `currunix`. A bound is a `datetime`, an ISO instant, a date, epoch
  nanoseconds (`int`) or `now`/`today`/`yesterday`/`tomorrow`/`epoch`; a date
  `end` is the end of that day; neither bound is the last day up to now.
- Each table is windowed on its own clock: the line's printed instant on
  `log_messages`, read in `timezone` (`Europe/Zurich` unless stated), the
  message's own on FIX rows. The capture's bridge prints a Central European
  clock, two hours ahead of UTC in summer, so its lines printed 14:46 are
  dated 12:46 UTC, the hour of the messages they carry. Read as UTC they
  would sit two hours after their messages, and one delivery's copies would
  stop folding.
- `parse_fix_messages_refined` reads `HISTORY` (one hour) before its window,
  hour partition by hour partition in instant order, walks it an hour at a
  time (never collecting the read) and writes only the rows the walk dates
  inside the window -- at `start` the views of every chain the hour before
  left alive. An event lands only in a window holding both the bronze rows it
  is walked from and the instant it is walked to: an expiry more than
  `HISTORY` after its order began, or any message of a capture read in a zone
  other than its bridge's. Run silver behind bronze, over wide windows (a
  day), and rerun a wider window to reconcile.
  `docs/dags/index.md#late-events` has the numbers.
- `parse_books` and the flatteners replace exactly `[start, end)`. The fold
  reads `HISTORY` before `start` too, so a book standing at `start` holds what
  that hour left resting, and every hourly view silver holds there is its
  book's membership at that hour -- a chain older than `HISTORY` stands in
  the book when the walk that landed it saw it alive. A book is one `MIC:CFI`
  category per instant:
  `parse_books` clears each row's `symbol`, so the fold keys a message by
  `{miccode}:{cficode}` (`XXXX`/`XXXXXX` where unknown) and no book or event
  states a ticker. A book nests `alive`, `deltas`, `executions`, `bidlimits`
  and `asklimits`; orders and quotes are its `deltas` by `marketdatakind`,
  executions its `executions`.
- Every scan pushes the window into Iceberg, so only the window's hour
  partitions are planned (`dataset.scan_plan(row_filter)` shows it).

## Storages

`Storages.from_dict({layer: IcebergCatalog.from_dict mapping})` needs all of
`bronze`, `silver`, `gold`. Bronze and silver both hold
`record_keeping.fix_messages`, so the layers never share a catalog: three
SQLite databases, three Glue Data Catalogs (`glue.id`), three S3 table
buckets.

| mode | one layer's properties |
| --- | --- |
| local | `type: sql`, `uri: sqlite:////abs/<layer>.db`, `warehouse: /abs/<layer>` |
| SQL catalog, S3 data | `type: sql`, `uri: <sqlalchemy url>`, `warehouse: s3://bucket/<layer>`, `s3.region` |
| AWS Glue | `type: glue`, `glue.id: <catalog id>`, `warehouse: s3://bucket/<layer>`, `glue.region`, `s3.region` |
| S3 Tables | `type: s3tables`, `warehouse: arn:aws:s3tables:<region>:<account>:bucket/<name>` |
| S3 Tables via Glue | `type: s3tables`, `warehouse: <account>:s3tablescatalog/<name>`, `rest.signing-region` |

Never put credentials in a mapping. `docs/storages/` is the full reference.

## Task parameters worth knowing

- `parse_log_messages(source, storages, window=None, *, rowheader=None, timezone=TIMEZONE, commit_row_size=COMMIT_ROW_SIZE, target=LOG_MESSAGES)`:
  `source` is a file, folder or prefix URI (`file:data/capture`,
  `s3://bucket/prefix?region=eu-west-1`) or an `IOBase` the caller keeps open.
  `rowheader=None` is `rekep.times.ULBRIDGE_ROWHEADER`, whose clock takes
  three fraction digits after a point, optionally grouped micros
  (`.524_315`): a line spelling a comma or no fraction is left unmatched,
  dated by its object's modification time with every capture null, and needs
  a header of its own, which must keep `rekep.text.CAPTURES`.
  `timezone` is the IANA zone the bridge prints its clock in,
  `rekep.times.TIMEZONE` = `Europe/Zurich` unless stated; pass another for a
  bridge printing elsewhere, or its lines are dated hours from their
  messages. `window=None` lands every line and answers `Landed.window`, the
  whole hours its dated lines span, for the next tasks:
  `[2026-08-14 01:00, 22:00)` UTC for the capture.
- `parse_fix_messages_raw`, `parse_fix_messages_refined`, `parse_books`:
  `codec=None` is `FixCodec.from_env(default_sending_time=UNDATED)`. Hand all
  three the same codec.
- `parse_books`: `snapshot_millis=SNAPSHOT_MILLIS` (one hour; 0 = off) restates
  every book whole on that epoch-aligned grid (`snapunix` set, no delta or
  execution repeated), between the fold's first and last operation.
- `parse_fix_messages_refined`: `snapshot_millis=SNAPSHOT_MILLIS` (one hour;
  0 = off, whatever the codec's `snapshot_ns` pin) restates every live chain
  on that grid as a view: `currunix == snapunix ==` the hour, its own
  `curruuid`, the live event's content and place, no chain moved on. Filter
  `snapunix is null` for events alone.
- `parse_orders`, `parse_quotes`, `parse_executions`: `snapshot_id=None` pins
  the head found; pass the one `parse_books` answered.
- Every task takes a `target` table name, and every task after the first a
  `source`, defaulted to the constants above.
- Every task takes `commit_row_size=COMMIT_ROW_SIZE` (131,072 rows), and rows
  alone cut its commits: a keyed task commits per that many rows it answers,
  only the ones it inserts or replaces, so a write that fails after a commit
  keeps it; `parse_books` and the flatteners stage that many at a time and
  commit their window once. Every write spills its rows to the system
  temporary directory first and lays them out in the table's sort order.

## Deploying tables ahead of a run

```python
import tempfile
from pathlib import Path

from rekep import Storages
from rekep.deploy import deploy

root = Path(tempfile.mkdtemp())
storages = Storages.from_dict({layer: {"name": layer, "properties": {
    "type": "sql", "uri": f"sqlite:///{root / layer}.db", "warehouse": str(root / layer)}}
    for layer in ("bronze", "silver", "gold")})
raw = "bronze.record_keeping.fix_messages"
with storages:
    assert set(deploy(storages, dry_run=True).values()) == {"missing"}
    assert deploy(storages, tables=[raw]) == {raw: "created"}
    assert deploy(storages)[raw] == "present"
```

`deploy` answers `created`, `present` or, under `dry_run`, `missing` per
table, and never alters an existing table.

## Reading tables

Public code imports `rekep` and nothing beneath it. With `storages` open on
landed tables:

```python
from rekep import State
from rekep.fix import SORT_COLUMNS, fix_window_filter
from rekep.pipeline import FIX_MESSAGES
from rekep.times import window_of

events = storages.dataset(FIX_MESSAGES)
try:
    table = events.read_arrow_reader(   # streams; pushes filter, projection, order down
        columns=("currunix", "seqnum", "curruuid", "crosscode", "msgtype", "state"),
        row_filter=fix_window_filter(window_of("2026-08-14", "2026-08-14")),
        order_by=SORT_COLUMNS,          # ordered columns must be projected
    ).read_all()
finally:
    events.close()
states = [State(code) for code in table.column("state").to_pylist()]
```

`state` is an `int32` code of `rekep.State` (61 members, code = rank * 100 +
place: `FILLED` is 8003); `side` and `marketdatakind` on market rows are
`int32` codes of `rekep.Side` and `rekep.MarketDataKind` (`ORDR` 10, `QUOT`
14, `EXEC` 8, `BOOK` 3). Keys: `curruuid` on every table; `srcuuids` on FIX
and event rows lists the `log_messages.curruuid` of the lines an event was
logged on, and an execution split out of a report lists that report's
`curruuid` beside them; `crosscode` is the business id (OrderID, ClOrdID,
..., `ExecID=` for a split execution) prefixed with the side (`BUY:...`) on
FIX rows, and the object a line was read from on `log_messages`. Column meanings:
`docs/tables/`; real rows: `docs/samples/`.

## FIX registry

Importing `rekep` installs the bundled dictionary as the process default:
`FixRegistry.from_env()` (7,790 definitions, 737 code sets including the
intrinsic `statecodeset` and `msgcatcodeset`) and `FixCodec.from_env(**pins)`.
`FixRegistry.install_env` refuses a second default. For another dictionary,
set the variable `rekep.fix.REGISTRY_VARIABLE` names to its folder before the
process imports `rekep`, or build `FixCodec(FixRegistry.from_handle(folder),
default_sending_time=UNDATED)` and pass it as `codec=` to the three FIX tasks
and to `deploy`.

```python
from rekep import FixCodec

message = next(iter(FixCodec.from_env().parse_line(b"8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|")))
assert message.by_name("symbol").as_py() == "AAPL"
```

## Generated files

Regenerate from the repository root, review the diff, commit the result:

| run | writes | when | drift fails |
| --- | --- | --- | --- |
| `python tools/schemas_dump.py` | `schemas/**`, `docs/tables/**` | a table's field changes | `tests/test_schemas.py` |
| `python tools/samples_dump.py` | `docs/samples/**` | a task, a field or the capture changes | `tests/test_docs.py` under `-m integration` |
| `python tools/fix_registry_dump.py` | `docs/assets/fix-*.json` | the bundled registry changes | nothing: regenerate by hand |

An identity or key change means rebuilding the affected tables and every
table after them from the capture; never mix old and new keys.

## Changing the code

Ownership (AGENTS.md): the native dependency owns `Field`, text reading,
codecs, the FIX registry, parsing, lifecycle and the book fold; Arrow owns
shape kernels; PyIceberg owns tables and commits; rekep owns the thin seams,
`Storages` and the tasks that compose them. Never add a second Field class,
filesystem layer, text reader, codec or registry here; no Python row loops
over Arrow data; prefer deleting to compatibility layers.

To add or change a task:

1. A function in `python/src/rekep/pipeline.py`, in graph order, taking
   `(storages, window, *, commit_row_size=COMMIT_ROW_SIZE, source=..., target=...)`
   after any leading input and answering `Landed`. It opens its datasets
   through `storages` and closes them, never closes a catalog, pushes the
   window into its scan, opens its target with `_target`, so rows alone cut
   its commits, and writes a keyed table with one `merge_arrow_reader`, an
   exact-window replacement with one `overwrite_arrow_reader(row_filter=...)`.
   Its table name is a constant beside the others and in `__all__`.
2. Its table in `rekep.deploy.TABLES`, with a description and writer in
   `tools/schemas_dump.py`; regenerate `schemas/` and `docs/tables/`.
3. Tests: unit tests beside the module's own (`tests/test_<module>.py`); the
   landed contract once in `tests/storages/`, marked `integration`.
4. A page under `docs/tasks/`, its nav entry in `mkdocs.yml`, the samples
   regenerated, and this skill.

Checks before a commit, from `python/`:

```bash
uv run ruff check . ../tools && uv run ruff format --check . ../tools
uv run pytest -q                      # unit (integration excluded by default)
uv run pytest -q -m integration       # real local Iceberg transactions and every asserting doc example
cd .. && uv run --project python --group docs mkdocs build --strict
```

CI lints `python/`, runs the unit suite and the market integration tests on
Linux and Windows under Python 3.10 and 3.13, and builds the docs strictly.
Documentation never names the native dependency: `tests/test_docs.py` fails
on it.

## Pitfalls

- Relative locations resolve against the working directory. Work from the
  repository root or spell absolute locations.
- `window_of()` with no bounds is the last day up to now; the capture is dated
  2026-08-14, so an unbounded run over it reads nothing.
- An `int` bound is epoch nanoseconds: `window_of(20260814, ...)` starts in
  1970. Spell the date, `"2026-08-14"`.
- Bronze FIX rows have empty `seqnum`/`prevuuid` by design; chains are
  silver's. Products read silver, never bronze FIX.
- A message stating no `Side(54)` is side-less in bronze and takes the one
  live side of its order in silver: the capture's cancel reject at 21:59:46
  is `816179183-1983-98963_912` in bronze and `SELL:816179183-1983-98963_912`,
  side `SELL`, in silver, which is what the books fold. An order with no live
  side to take stays side-less, and `parse_books` refuses it.
- The parse splits every report of a fill into the report and the execution
  it reports (`FILLED`), one per side for a trade report, and a two-sided
  quote into one quote per side, so FIX tables hold more rows than frames.
- A task never closes the catalogs. Close `Storages` yourself: on Windows an
  open SQLite catalog is a file its caller cannot delete.
