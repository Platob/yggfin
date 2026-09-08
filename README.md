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

codec = FixCodec(fix_registry(), branch="ulbridge")
message = codec.transform_line(
    b"Sending : 8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|38=12|10=000|"
)

assert message.by_name("symbol").as_py() == "AAPL"
assert message.by_tag(38).as_py() == 12.0
```

The supported ingestion graph is deliberately short:

```text
capture URI -> parse_messages -> logs.messages -> parse_fix -> fix.messages
```

Run it locally from the repository root:

```bash
uv sync --project python --all-extras --dev
uv run --project python rekep iceberg deploy tasks/parse_messages/parse_messages.json
uv run --project python rekep task run tasks/parse_messages/parse_messages.json
uv run --project python rekep task run tasks/parse_fix/parse_fix.json
```

`logs.messages` stores one physical line with its exact body bytes and source
identity. `fix.messages` stores one codec result for that row, including typed
columns, the complete arrival record, unmapped pairs, and derived identities.
Both use `(url, rownum)` as their primary key, so replay is idempotent.

The reviewed contracts are [Message](schemas/rekep/message.json) and
[FixMessage](schemas/rekep/fix-message.json). The [pipeline guide](docs/pipeline/index.md)
covers local files, S3, AWS Glue, Airflow, and operations; the
[data-product guide](docs/products/index.md) defines every published column.

Development:

```bash
cd python
uv run pytest
uv run pytest -m integration
uv run ruff check . ../tasks ../tools
uv run mkdocs build --strict --config-file ../mkdocs.yml
```
