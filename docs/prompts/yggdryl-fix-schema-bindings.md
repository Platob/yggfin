# Final FIX slice: use the native `FixMsg` schema

Pull latest Yggdryl main containing the merged FIX Arrow work. Rust remains
the owner of the fixed table shape and source composition. Require this single
binding accessor before changing yggfin:

```python
class FixMsg:
    @staticmethod
    def schema(
        registry: FixRegistry | None = None,
        name: str = "FixMsg",
        *,
        carried: FieldLike | pyarrow.Schema | None = None,
    ) -> Field: ...
```

Expose the equivalent JavaScript static method with `Field | null` for
`carried`. Keep the instance `message.field` accessor unchanged. Back both
bindings directly with `rust/src/fix/schema.rs::fix_schema`; move the private
source combiner beside it and make `FixBatchReader::from_column` call the same
operation. No carried value returns only the native FIX projection. A carried
Struct preserves all children, order, nullability and metadata, then appends
the FIX columns and rejects case-insensitive collisions before pulling input.
Do not add a binding-owned schema, tag list, JSON model, parser, or second
combiner. Do not add another reader accessor: `parse_arrow_reader` already
returns the final lazy Arrow stream. If `FixMsg.schema` is absent, implement it
in Yggdryl first; never emulate it in yggfin.

Prove in Rust, Python and JavaScript: global and explicit registries;
empty/partial/full registries; carried and native-only shapes; deterministic
ordering; nested groups and metadata; numeric tag names; derived fields;
`timestamp[us, UTC]`; closing `entries` and `unmapped`; collision refusal; no
source pull; Arrow/JSON parity across bindings. With the full checked registry,
the native shape has 87 columns and carrying `Message.field()` adds its eight
source-first columns for 95. The carried result must equal
`parse_arrow_reader(...).schema` exactly.

After Yggdryl lands, pull and pin that exact commit in yggfin. In
`tasks/parse_fix/parse_fix.py`, obtain `FixMessage` with
`FixMsg.schema(dictionary, name="FixMessage", carried=Message.field())`, then
pass the `RecordBatchReader` returned by `parse_arrow_reader` directly to
`IcebergDataset.append_arrow_reader(parsed, field, merge_by=True)`. That
Iceberg boundary already applies and validates `field`; delete the redundant
task-level `Field.apply_arrow_reader`. In `tools/fix_registry.py`, obtain the
native-only `FixMsg` view from the same accessor. Delete both empty
`RecordBatchReader` schema probes, their `Field.from_arrow_schema` conversions,
and now-unused imports. Regenerate `schemas/rekep/fix-message.json` from the
accessor through `Field.into_json`; keep it only as a derived fixture.

Update the FIX task, registry-tool and contract pages to show only the native
accessor. Test accessor/parser schema equality, the 95-column JSON round trip,
empty input, streamed hourly-partition append/read, replay idempotence,
commit-before-next-chunk, definite-failure cleanup, and no remaining
`rekep-iceberg-*` temporary directories. Run Rust first, then Python and
JavaScript bindings, yggfin Ruff, focused unit/integration tests, strict Marimo
and strict MkDocs. Delete obsolete helpers and assertions, commit each
repository once, and push both main branches only when clean.
