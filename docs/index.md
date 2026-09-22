<section class="rkp-hero" aria-labelledby="rkp-home-title">
  <div class="rkp-hero__copy">
    <p class="rkp-hero__eyebrow">RKP / Arrow-native ingestion</p>
    <h1 id="rkp-home-title">rekep</h1>
    <p class="rkp-hero__lead">Stream ULBridge text records through Arrow into Iceberg.</p>
    <p class="rkp-hero__flow" aria-label="Text to Arrow to Iceberg">TEXT → MESSAGE → FIX → ICEBERG</p>
    <nav class="rkp-hero__actions" aria-label="Start with rekep">
      <a href="pipeline/operations/run/">Run ingestion</a>
      <a href="products/message/">Inspect Message</a>
      <a href="fix/">Explore FIX</a>
    </nav>
  </div>
  <figure class="rkp-hero__mark">
    <img src="assets/rkp-logo.svg" alt="RKP, the rekep project trigram" width="420" height="230">
  </figure>
</section>

## Install

```bash
pip install "rekep[iceberg]"
```

## Run

```bash
rekep task run tasks/parse_messages/parse_messages.json
rekep task run tasks/parse_fix_bronze/parse_fix_bronze.json
rekep task run tasks/parse_fix_silver/parse_fix_silver.json
```

The checked ULBridge fixture demonstrates the complete contract:

```text
parse_messages    144 read, 144 written,  0 skipped  → logs.messages
parse_fix_bronze  144 read,  49 written, 30 skipped  → fix.bronze   (79 messages)
parse_fix_silver   49 read,  22 written,  0 skipped  → fix.silver
```

```mermaid
flowchart LR
    S["capture URI<br/>file · directory · s3://"] --> T["native text reader<br/>Message field"]
    T --> M[("logs.messages<br/>12 columns")]
    M --> F["native FIX codec<br/>parse"]
    F --> B[("fix.bronze<br/>128 columns")]
    B --> L["native FIX codec<br/>lifecycle"]
    L --> O[("fix.silver<br/>128 columns")]
```

The text reader emits the exact `Message` schema: header captures are typed,
and the line's own `currunix`, `curruuid` and `currhashcode` arrive with the
read rather than being computed after it. Nothing in that contract is derived
from anything else, and the table is laid out by the hour of `currunix` alone,
exactly as both FIX tables are laid out by the hour of theirs. The FIX codec
reads that table back as a reader, through two stages over one codec, each
landing in a table: parse reads every frame a line carried and settles what it
implied, and lifecycle names the chains it belongs to. A row is a message and
not a line, so the fixture's 144 stored lines settle as 79 messages and 49
bronze events: a line carrying prose answers none, a line carrying many frames
answers one row per frame, and the same message logged at every hop it passed
is one event. Lifecycle adds one expiry row.

```python
from rekep import Message

print(Message.into_field().into_arrow_schema())
```

## Where to go

| you want | read |
| --- | --- |
| what the three tables hold | [Data products](products/index.md) |
| how the parts fit | [Architecture](overview/architecture.md) |
| the exact task contracts | [Pipeline](pipeline/index.md) |
| the two FIX tasks | [Parse FIX bronze](pipeline/tasks/parse-fix-bronze.md) · [Parse FIX silver](pipeline/tasks/parse-fix-silver.md) |
| the order and execution products | [Build dbt](pipeline/tasks/build-dbt.md) |
| the runtime FIX dictionary | [Registry](fix/registry.md) |
| to decode or encode a frame | [Decode](fix/decode.md) · [Encode](fix/encode.md) |
| to schedule it | [Airflow](pipeline/airflow.md) |
