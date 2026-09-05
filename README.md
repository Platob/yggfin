# rekep

`rekep` streams trading logs through Arrow into six Iceberg contracts: source
messages, FIX messages, instruments, books, orders, and executions.

![Apache Arrow connects Iceberg tables, DataFrames, compute engines, and SQL databases; zero-copy sharing requires compatible buffers.](docs/assets/arrow-hub.svg)

```bash
pip install "rekep[all]"
```

```python
import pyarrow.fs
from yggdryl import IOBase, TextOptions

options = TextOptions()
options.with_rownum = 1
options.rowheader = (
    r"^(?<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}_\d{3}) "
    r"\[(?<threadname>[^]]*)\] \[(?<plugin>[^]]*)\] "
    r"(?:\((?<level>[A-Za-z]{1,12})\) )?"
)

capture = IOBase.from_fs(
    pyarrow.fs.S3FileSystem(region="eu-west-1"),
    "bucket/logs/2026-08-14/capture.log",
).into_text(options)

for messages in capture.read_arrow_reader():
    consume(messages)
```

The text batch is raw: source URL, physical row number, captured log header,
and exact binary body. Protocol classification and FIX parsing start in the
next stage.

Arrow is the boundary between every stage. `@scalar` declarations define the
in-memory schema, recursive casts, Iceberg projection, and portable YAML
contract. FIX parsing preserves repeated tags, unknown keys, metadata, and
structured components. The [Arrow interoperability guide](docs/overview/arrow.md)
explains how that boundary connects Iceberg, Parquet, Avro, SQL databases,
Spark, and DataFrame/query engines without claiming every exchange is
zero-copy.

The project workflow is intentionally outside the package:

```text
                  +-> parse_fix_market  -> fix.market -+-> parse_instruments -> market.instruments
                  |                                    `-> parse_market -+-> flatten_orders
parse_messages --+                                                     `-> flatten_executions
                  +-> parse_fix_misc    -> fix.misc
                  `-> parse_fix_unknown -> fix.unknown
```

`parse_market` can instead set `books: false` and write FIX-carried orders and
executions directly, without a book table or the two flattening stages.

Each step is a Marimo application under `tasks/<step>/` with an adjacent YAML
config. Airflow reuses the one `parse_fix` definition for three category runs
with pushed filters, and runs every application through `MarimoOperator`;
package `Task` only reads and writes their configuration.

Core properties:

- yggdryl text media over local and caller-supplied `pyarrow.fs` filesystems;
- registry-driven, cross-version FIX metadata;
- stable cross-language XXH3 `int64` identities;
- immutable event histories and `InstUpdate` reference data with nested
  `Instrument` facts;
- deterministic partition-aware Iceberg reads and bounded writes;
- six checked contracts under `schemas/rekep/`.

See the [documentation](https://platob.github.io/yggfin/) or the local
[architecture overview](docs/index.md).

Development:

```bash
cd python
uv sync --all-extras --dev
uv run pytest
uv run ruff check .
```

Long Iceberg checks are explicit:

```bash
uv run pytest -m integration
```
