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
capture URI -> parse_messages -> logs.messages -> parse_fix -> fix.messages
```

and one dbt build derives the business products from its end:

```text
fix.messages -> build_dbt -> orders.events, orders.current, executions.fills
```

`parse_fix` is three native stages over one codec, in this order and no other:

```text
parse -> enrich -> lifecycle
```

`parse` reads every frame a line carried, `enrich` fills what a message implied
but did not carry -- including the session pair a bridge configuration
announced earlier in the same stream -- and `lifecycle` names the chains: the
`code` a message belongs to, its `updatedat` on the snapshot grid, the
`createdat` its chain opened at, and the `prevmsghash` linking it to the
message before it.

Run it locally from the repository root:

```bash
uv sync --project python --all-extras --dev
uv run --project python rekep iceberg deploy tasks/parse_messages/parse_messages.json
uv run --project python rekep task run tasks/parse_messages/parse_messages.json
uv run --project python rekep task run tasks/parse_fix/parse_fix.json
uv run --project python rekep task run tasks/build_dbt/build_dbt.json
```

`logs.messages` stores one physical line with its exact body bytes and source
identity, keyed on `(sourceurl, rownum)`. `fix.messages` stores one row per
message that line carried -- typed columns, the complete arrival record, and
derived identities -- keyed on `(sourceurl, rownum, msghash)`, because a line
can carry more than one message. A message that stated no clock of its own is
dated by the instant its line was captured, so both replay idempotently.

`build_dbt` runs the [dbt project](data/dbt/README.md) under `data/dbt`: DuckDB
owns the SQL, and every read and commit goes through the same Iceberg dataset
the tasks write through, so there is no second catalog and no extract.

The reviewed contracts are [Message](schemas/rekep/message.json) and
[FixMessage](schemas/rekep/fix-message.json), each the Iceberg schema,
partition spec and sort order PyIceberg records for its table.
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
