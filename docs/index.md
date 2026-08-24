# rekep

`rekep` turns ordered text logs into Arrow records and writes five Iceberg
products: logs, instruments, books, orders, and executions.

![Arrow-centred workflow](assets/arrow-hub.svg)

## Workflow

```text
TextFile(s)
    |
parse_logs -----------------> log tables
    |\
    | `---------------------> flatten_instruments -> instrument
    v
parse_market ---------------> book
    |\
    | `---------------------> flatten_executions -> execution
    `-----------------------> flatten_orders -----> order
```

Concrete stages are notebooks with adjacent YAML files under `tasks/`. The
package owns reusable parsing, schemas, lifecycle logic, and storage adapters;
it does not own deployment-specific jobs.

## Guides

- [Design](design.md): boundaries and maintenance rules.
- [Types](types.md): `@scalar`, fields, and recursive casts.
- [Contracts](contracts.md): the five portable schemas.
- [Logs](logs.md): streamed text parsing and routing.
- [FIX](fix.md): registry-driven transcription.
- [Market](market.md): events, instruments, books, and audit rows.
- [Iceberg](iceberg.md): streaming reads, writes, and maintenance.
- [Tasks](tasks.md): notebooks, configs, and Airflow.
- [End-to-end run](workflow-run.md): execution evidence and schema lineage.
- [Identity](identity.md): cross-language binary hashing.
- [Benchmarks](benchmarks.md): focused internal measurements.

## Install

```bash
pip install rekep
pip install "rekep[iceberg]"   # persisted tables
pip install "rekep[all]"       # all package extras
```

```python
from rekep import Log, TextFiles

source = TextFiles.from_folder("logs", pattern="*.log*")
reader = source.read_arrow_reader(schema=Log.into_field())
```

Every scalable API returns an Arrow reader. Table helpers are explicit choices
for data known to fit in memory.

## Command line

```bash
rekep fields dump --pyclass rekep.text.log:Log --target log.yaml
rekep fields load --target log.yaml
rekep fix shell --store data/fix
```

`fields` publishes a declaration and checks one loads; `fix` reads, edits and
checks the FIX dictionary, either verb by verb or from a prompt. Styling goes
to `stderr` and the payload to `stdout`, so a dump piped into a file is the
document and nothing else -- and colour and box drawing turn themselves off
without a terminal, under `NO_COLOR`, or where the stream cannot encode them.
