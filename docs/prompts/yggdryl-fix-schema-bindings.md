# Final FIX slice: publish the schema and registry JSON

From latest Yggdryl main, generate the canonical native `Field` JSON for the
exact full-registry output of `FixBatchReader::from_column` carrying yggfin's
eight-column `Message` schema. Use the existing Rust schema construction and
`Field.into_json(indent=2)`; do not add or change a schema accessor, reader,
parser, projection, batch type, registry model, or compatibility API.

The document must contain the 95 source-first columns with all Arrow types,
nullability and metadata: `(url, rownum)` keys, hourly `timepartition`, numeric
FIX columns, every `timestamp[us, UTC]`, nested groups, derived fields, and
closing `entries` and `unmapped`. Prove `Field.from_json(document)` round-trips
exactly and its Arrow schema equals `FixBatchReader::from_column(...).schema`
without consuming a row.

Also migrate the registry through Yggdryl's native serializer, not by copying
its generator inputs:

```python
registry = FixRegistry.from_handle(yggdryl / "config" / "fix")
registry.write_into(yggfin / "schemas" / "rekep" / "fix-registry")
```

Commit the canonical `primitive/`, `nested/`, and optional `branches.json`
that `write_into` emits. Do not copy `layouts.json`, `provenance.json`, caches,
or the retired Rekep registry ZIP. Prove the migrated registry reloads equal,
contains 6,203 fields, and a second native dump has no diff. Schema generation,
tests, the local task run, and the standalone registry page must use this
checked dump without referring to a sibling Yggdryl checkout; an explicit
runtime registry URI remains allowed.

Check the schema into yggfin as `schemas/rekep/fix-message.json`. Use these
generated contracts to build schema-shaped dataclass/scalar fixtures and mock
Arrow batches. Runtime parsing remains the existing direct
`parse_arrow_reader`/`FixBatchReader::from_column` stream into Iceberg; add no
second FIX implementation. Run the focused schema, streamed write/read,
Marimo and MkDocs checks, then push clean main.
