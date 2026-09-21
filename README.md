# rekep

`rekep` turns ULBridge text captures into typed, queryable Iceberg products.
Its public Python surface includes resource binding, text framing, fields, the
FIX codec, and a complete FIX registry; applications and examples import only
`rekep`.

```bash
pip install "rekep[iceberg]"
```

The package ships its registry, so no dictionary path or environment variable
is required:

```python
from rekep.fix import FixCodec, fix_registry

codec = FixCodec(fix_registry())
message = next(iter(codec.parse_line(
    b"Sending : 8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|"
)))

assert message.by_name("symbol").as_py() == "AAPL"
assert message.by_tag(38).as_py() == 12.0
```

The supported ingestion graph is deliberately short:

```text
filesystem URI -> parse_messages   -> logs.messages
logs.messages  -> parse_fix_bronze -> fix.bronze
fix.bronze     -> parse_fix_silver -> fix.silver
```

and one dbt build derives the business products from its end:

```text
fix.silver -> build_dbt -> orders.events, orders.current, executions.fills
```

The two FIX tasks are the two native stages one codec exposes, each over a
table of its own, in this order and no other:

```text
parse -> fix.bronze, lifecycle -> fix.silver
```

`parse_fix_bronze` reads every frame a stored line carried and settles it where
it is read: a parsed message already carries what it implied about itself, so
there is no enriching stage between the two, and nothing has walked yet, so
`seqnum`, `prevuuid` and `parentuuids` are empty on every bronze row.
`parse_fix_silver` reads those rows back as the chains they belong to and fills
what a message implied about the message before it -- the `prevuuid` it
follows, the `seqnum` it stands at, the `parentuuids` it descends from, and the
`creaunix`, `exprtime` and `state` its chain folded forward.

Silver scans the previous hour plus the job window with `fix_window_filter`
and `SORT_COLUMNS`. Iceberg streams chronological hour paths and merges no more
than 16 overlapping files at once; there is no Python-wide `read_all` union.
Native 0.1.8 lifecycle processing still collects and stable-sorts that finite
scan result. Undated rows come from the epoch partition and may accumulate, so
this is not a batch-bounded memory path. The previous hour provides context only: output is the job window plus
unresolved epoch rows, with future expiry excluded, so this bounded run does
not claim arbitrary older-chain completeness.

Run it locally from the repository root:

```bash
uv sync --project python --all-extras --dev
uv run --project python rekep iceberg deploy tasks/parse_messages/parse_messages.json
uv run --project python rekep task run tasks/parse_messages/parse_messages.json \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
uv run --project python rekep task run tasks/parse_fix_bronze/parse_fix_bronze.json \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
uv run --project python rekep task run tasks/parse_fix_silver/parse_fix_silver.json \
  --parameter 'start="2026-08-14"' --parameter 'end="2026-08-14"'
uv run --project python rekep task run tasks/build_dbt/build_dbt.json
```

A streaming task parses one window, `[start, end)`, and given neither bound
takes the last day up to now; the sample capture under `data/capture` is dated
2026-08-14, which is why the three runs above name that day. A run over a
window lands its rows over what an earlier run of the same window landed, so a
replay leaves each table holding each row once.

`logs.messages` stores one physical line as the read decoded it, row header
included, and its source position, keyed on `currhashcode`, the content code
the read states over the whole line: identical lines are one row whatever
session carried them. All three tables are laid out by the hour of `currunix`
alone. On a raw row that instant is what the native text read settles over the
line -- the line's own clock where the header dated it, else `EPOCH`, so the
same bytes answer the same instant on every re-read of a capture. A line at
that pin is in every window, so no window loses a line the header could not
date. `fix.bronze`
stores one row per *event* as the parse answered it -- typed columns, residual
FIX entries, and the identities the parse settled -- and
`fix.silver` the same events walked; both are keyed on `curruuid`, because a
bridge logs one message again at every hop it passes and those arrivals are one
event. A source message that stated no sending clock takes the codec's fixed
epoch rather than the instant the parse ran; lifecycle may date that source
event from its `TransactTime`, while synthetic expiry keeps its exact deadline.

The 123-column **FixMsg** row is the native parse, storage, and lifecycle
shape. It reconstructs canonical message semantics
from lifted columns and residual `fixentries`; lifted values are not duplicated
as a second arrival record. `sourceurl`, `rownum`, `msgthreadid`, `loglevel`,
and `body` remain only in `logs.messages`, while the bridge's `msgsessionid`,
`msgctxid`, `msgseqnum`, and `msgpluginid` are native FixMsg fields a raw line
fills; `srcuuids` joins a FIX row back to raw
`curruuid`. `crosscode` uses the first available business
identifier (`OrderID`, `ClOrdID`, `OrigClOrdID`, `QuoteID`, `QuoteReqID`, then
`MDReqID`), while capture `session:context` is
`identifiers["msgsectxid"]`. Default null spellings are empty text, `null`,
`<null>`, `none`, `n/a`, and `[n/a]`, trimmed and case-insensitive.

`build_dbt` runs the [dbt project](data/dbt/README.md) under `data/dbt` and
reads `fix.silver`: DuckDB owns the SQL, and every read and commit goes through
the same Iceberg dataset the tasks write through, so there is no second catalog
and no extract.

The reviewed contracts are [Message](schemas/rekep/message.json) and
[FixMsg](schemas/rekep/fixmsg.json), the Iceberg schema, partition
spec and sort order PyIceberg records for `logs.messages` and the one both FIX
tables share.
The [pipeline guide](docs/pipeline/index.md) covers local files, S3, AWS Glue,
AWS S3 Tables, Airflow, and operations; the [data-product guide](docs/products/index.md)
defines every published column.

Development:

```bash
cd python
uv run pytest
uv run pytest -m integration
uv run ruff check . ../tasks ../tools
uv run ruff format --check . ../tasks ../tools
uv run --group docs mkdocs build --strict --config-file ../mkdocs.yml
```

`mkdocs-material` is in the `docs` group, which is not a default group, so the
documentation build names it; everything above it runs under the default
`dev`, `runner` and `airflow` groups.
