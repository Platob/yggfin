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
(message,) = codec.parse_line(
    b"Sending : 8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|"
)

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
`creaunix`, `expirunix` and `state` its chain folded forward.

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

`logs.messages` stores one physical line with its exact bytes, row header
included, and its source position, keyed on `bodyhash`, the digest of the whole
line: identical lines are one row whatever session carried them. `fix.bronze`
stores one row per *event* as the parse answered it -- typed columns, the
complete arrival record, and the identities the parse settled -- and
`fix.silver` the same events walked; both are keyed on `curruuid`, because a
bridge logs one message again at every hop it passes and those arrivals are one
event. A message that stated no clock of its own takes the instant the codec is
pinned with rather than the instant the parse ran, and the walk dates it by its
`TransactTime`, so every stage replays idempotently.

`build_dbt` runs the [dbt project](data/dbt/README.md) under `data/dbt` and
reads `fix.silver`: DuckDB owns the SQL, and every read and commit goes through
the same Iceberg dataset the tasks write through, so there is no second catalog
and no extract.

The reviewed contracts are [Message](schemas/rekep/message.json) and
[FixMessage](schemas/rekep/fix-message.json), the Iceberg schema, partition
spec and sort order PyIceberg records for `logs.messages` and the one both FIX
tables share.
The [pipeline guide](docs/pipeline/index.md) covers local files, S3, AWS Glue,
Airflow, and operations; the [data-product guide](docs/products/index.md)
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
