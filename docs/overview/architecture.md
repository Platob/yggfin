# Architecture

rekep presents one public API over a native, Arrow-first data path. Tasks,
tools, and documentation use `rekep` names; implementation dependencies do not
leak into an application.

```mermaid
flowchart LR
    U["local or S3 capture"] --> R["rekep.IOBase"]
    R --> T["rekep.TextOptions"]
    T --> M[("logs.messages")]
    M --> C["parse"]
    D[["bundled FIX registry"]] -.types.-> C
    C --> B[("fix.bronze")]
    B --> L["lifecycle"]
    D -.types.-> L
    L --> F[("fix.silver")]
    F --> P["orders · executions · book"]
```

## Ownership

| layer | owns |
| --- | --- |
| resource | URI binding, local and object-store traversal, decompression, bounded reads |
| text | physical-line framing, header capture, source URL and row number |
| field | schema metadata, casts, digests, partitions, Arrow conversion |
| FIX | dictionary, dialect membership, code sets, line classification, parsing, lifecycle, fixed Arrow projection |
| Iceberg | table conversion, identifiers, snapshots, scan planning, commits |
| tasks | application parameters, stage boundaries, counts, and orchestration |

There is one `Field`, one resource handle, one text reader, one codec, and one
FIX registry. rekep re-exports those types rather than wrapping them in
compatibility classes.

```python
from rekep import Field, IOBase, Message, TextOptions
from rekep.fix import FixCodec, FixRegistry, fix_registry

assert isinstance(Message.into_field(), Field)
assert isinstance(Message.text_options(), TextOptions)
assert isinstance(fix_registry(), FixRegistry)
assert FixCodec(fix_registry())
assert IOBase.from_uri("file:data/capture").exists()
```

## Streaming boundary

The text reader yields `RecordBatch` objects. `parse_messages` applies the
`Message` field and gives one `RecordBatchReader` directly to Iceberg.
`parse_fix_bronze` reads that table as another reader, passes it through the
codec's parse, and writes the native FixMsg row directly.
`parse_fix_silver` reads `fix.bronze` in turn, walks those native rows, and
writes the same shape. No production stage converts rows through Python dictionaries or
stages an S3 object on local disk.

## Stable identity

A stored line names its source through the object URI and the 1-based
physical row number, and itself through `curruuid`, the identity the native
read states over it. A message parsed out of that line records the identity in
`srcuuids`, which joins to the raw row's `curruuid`; no walk changes what the
identity means. The join across the products is exact provenance rather than
a recomputation:

```text
fix.bronze(srcuuids[*]) -> logs.messages(curruuid)
fix.silver(srcuuids[*]) -> logs.messages(curruuid)
```

Capture location is read only after joining `srcuuids` to raw `curruuid`. A
parse answers one row per message rather than one per line, so the FIX products
add the identities the parse settled and are keyed on a `curruuid` of their own.

A line's `currhashcode` identifies the line itself: the code the read states
over its whole content, beside the `curruuid` that keys `logs.messages`.
`curruuid` on a
FIX row identifies the settled
message: a UUIDv7 over its settled millisecond, its place in the sequence, and
its named content.
They intentionally answer different questions.

## Repository layout

```text
python/src/rekep/       public package and bundled registry
tasks/parse_messages/   raw-line Marimo application + JSON parameters
tasks/parse_fix_bronze/ FIX parse Marimo application + JSON parameters
tasks/parse_fix_silver/ FIX lifecycle Marimo application + JSON parameters
tasks/build_dbt/        dbt Marimo application + JSON parameters
tasks/optimize_iceberg/ maintenance Marimo application + JSON parameters
tasks/airflow/          DAGs, operator, and standalone child runner
schemas/rekep/          reviewed table contracts
docs/                   contracts, operations, products, and roadmap
tools/                  registry browser and documentation projection
data/                   default capture, catalog and warehouse locations
data/dbt/               the dbt project: models, schemas, macros, one profile
config/                 an operator's own FIX dictionary, when one is used
```

`optimize_iceberg` is maintenance rather than ingestion: it is not in the
scheduled graph, and it is documented with the storage it settles, under
[Iceberg maintenance](../storage/iceberg.md#maintenance).

`build_dbt` is derivation rather than ingestion: dbt owns the SQL its products
are written in, and `rekep.dbt` is the one seam that makes a source an Iceberg
read and a model an Iceberg commit through the dataset above. It is documented
with the task that runs it, under [Build dbt](../pipeline/tasks/build-dbt.md).
