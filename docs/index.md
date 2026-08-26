# rekep

`rekep` turns ordered text logs into Arrow records and writes six portable
shapes: source messages, FIX messages, instruments, books, orders, and
executions.

![Apache Arrow connects Iceberg tables, DataFrames, compute engines, and SQL databases; zero-copy sharing requires compatible buffers.](assets/arrow-hub.svg)

Arrow is the project's shared columnar boundary: Iceberg tables and encoded
files sit on one side, while Spark, DataFrames, query engines, and SQL database
drivers sit on the other. The distinction matters—Arrow is neither the store
nor the engine, so each can change without replacing the in-memory contract.
See [why rekep chooses Apache Arrow](arrow.md) for the sourced interoperability
details and the limits of zero-copy exchange.

## Workflow

```mermaid
flowchart TD
    L[TextFile / TextFiles] --> PM[parse_messages]
    PM --> M[(logs.messages)]
    M --> PF[parse_fix]
    PF --> FM[(fix.market)]
    PF --> FX[(fix.misc)]
    PF --> FU[(fix.unknown)]
    FM --> FI[flatten_instruments] --> I[(market.instruments)]
    FM --> PK[parse_market]
    PK -->|books: true| B[(market.books)]
    B --> FO[flatten_orders] --> O[(market.orders)]
    B --> FE[flatten_executions] --> E[(market.executions)]
    PK -->|books: false| O
    PK -->|books: false| E
```

Concrete stages are notebooks with adjacent YAML files under `tasks/`. The
package owns reusable parsing, schemas, lifecycle logic, and storage adapters;
it does not own deployment-specific jobs.

## Guides

**Declaring data**

- [Why Arrow](arrow.md): storage, database, and compute interoperability.
- [Design](design.md): boundaries and maintenance rules.
- [Types](types.md): `@scalar`, fields, and recursive casts.
- [Contracts](contracts.md): the six portable schemas.
- [Identity](identity.md): cross-language binary hashing.

**Parsing**

- [FixMsg](fixmsg.md): streamed text parsing and routing.
- [FIX](fix.md): registry-driven transcription.
- [Configuring a parse](configuring.md): headers, rules, field readings.

**Downstream**

- [Market](market.md): events, instruments, books, and audit rows.
- [Iceberg](iceberg.md): streaming reads, writes, and maintenance.
- [Pipeline](tasks.md): notebooks, configs, and Airflow.
- [Airflow](airflow.md): deployment, runs, backfills, and operations.
- [End-to-end run](workflow-run.md): execution evidence and schema lineage.
- [Benchmarks](benchmarks.md): focused internal measurements.

## Install

```bash
pip install rekep
pip install "rekep[iceberg]"   # persisted tables
pip install "rekep[all]"       # all package extras
```

```python
from rekep import FixCodec, FixMsg, FixRegistry, TextFiles

registry = FixRegistry(cache_dir="data/fix", offline=True)

source = TextFiles.from_folder(
    "logs",
    pattern="*.log*",
    msg_type_event_types=registry.msg_type_event_types(),
)
for messages in source.read_arrow_reader():
    parsed = FixMsg.from_message_arrow_batch(messages, FixCodec(registry=registry))
```

Every scalable API returns an Arrow reader. Table helpers are explicit choices
for data known to fit in memory.

## Command line

```bash
rekep fields dump --pyclass rekep.text.fixmsg:FixMsg --target fixmsg.yaml
rekep fields load --target fixmsg.yaml
rekep fix registry show --store data/fix 35
rekep fix registry check --store data/fix
rekep fix shell --store data/fix
```

`fields` publishes declarations. `fix registry` is the JSON command surface;
`fix shell` is the interactive terminal. Styling stays on `stderr`, payloads
stay on `stdout`, and colour disables itself outside a capable terminal.
