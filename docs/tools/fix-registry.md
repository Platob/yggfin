# FIX registry browser

**Standalone tool.** This Marimo application inspects a dictionary; it is not
a pipeline task, scheduler target, registry editor, or table writer.

Launch it from the repository root with the locked runner environment:

```bash
uv run --project python --group runner --frozen \
  marimo run tools/fix_registry.py
```

Paste a local path or filesystem URI into **Registry location**, then select
**Open registry**. A blank location selects Yggdryl's process registry, which
resolves `YGGDRYL_FIX_REGISTRY` and its normal user configuration. Remote
locations use the backends already supported by
`yggdryl.fix.FixRegistry.from_handle`.

## Browse and export

The summary counts native definitions, repeating groups, branches, and typed
fields. Search is case-insensitive and covers tags, alternate tags, canonical
and display names, aliases, and descriptions. Branch and shape controls filter
the same native iterator.

Select one result to open six views:

| View | Native source |
| --- | --- |
| Overview | `Field` and its typed `field.fix` view |
| Members | `Field.explode_fields()` then `Field.unnest_fields()` |
| Lineage | validated `fix:lineage` metadata |
| Codes | validated `fix:codes` metadata |
| Metadata | `Field.metadata.items()` |
| Field JSON | `Field.into_json(indent=2)` |

The Field JSON tab is copyable and downloadable. Nested structure stays owned
by Yggdryl: the browser does not reconstruct components, invent a catalog, or
declare a second FIX model.

## Full FixMsg schema

The schema panel creates an empty Arrow reader, passes it to Yggdryl's native
`parse_arrow_reader`, and reads the output schema before any row exists. It then
uses `Field.from_arrow_schema` and `Field.into_json(indent=2)` to display and
download the complete registry-defined `FixMsg` schema.

The equivalent schema-only call is:

```python
import pyarrow
from yggdryl import Field
from yggdryl.fix import FixRegistry, parse_arrow_reader

registry = FixRegistry.from_handle("file:///srv/config/fix")
empty = pyarrow.RecordBatchReader.from_batches(pyarrow.schema([]), [])
parsed = parse_arrow_reader(empty, registry=registry)
fixmsg = Field.from_arrow_schema(parsed.schema, name="FixMsg")

print(fixmsg.into_json(indent=2))
```

The empty source deliberately contributes no provenance columns. The result is
the native parser's full registry-dependent shape. A pipeline that supplies raw
message columns keeps those columns first, exactly as `parse_arrow_reader`
defines.
