# FIX registry browser

**Standalone tool.** It reads a registry and creates no tables,
edits no definitions, and runs no pipeline task.

```bash
uv run --project python --group runner --frozen \
  marimo run tools/fix_registry.py
```

A blank location opens rekep's bundled default. Enter a local path, `file:`
URI, or supported object-store URI to inspect an explicit dictionary; runtime
and bridge fields are added exactly as they are for parsing.

## Search and filter

Search covers canonical tags, alternate tags, storage/display names, aliases,
and descriptions. Branch and shape filters narrow to standard/bridge fields or
scalar/repeating-group definitions.

The summary reports definition, group, branch, and typed-field counts. The
bundled registry should report 6,262 definitions across two branches.

## Definition views

| view | source |
| --- | --- |
| Overview | identity, branch, type, nullability, description |
| Members | `Field.explode_fields()` and `Field.unnest_fields()` |
| Lineage | validated version history metadata |
| Codes | validated wire-value/name translations |
| Metadata | the complete field metadata mapping |
| Field JSON | deterministic `Field.into_json(indent=2)` |

The full-schema panel creates an empty Arrow reader and asks
`parse_arrow_reader` for its output schema. It therefore displays the actual
registry-dependent `FixMsg` projection without parsing or fabricating a row.

```python
import pyarrow

from rekep import Field
from rekep.fix import fix_registry, parse_arrow_reader

empty = pyarrow.RecordBatchReader.from_batches(pyarrow.schema([]), [])
parsed = parse_arrow_reader(empty, registry=fix_registry())
field = Field.from_arrow_schema(parsed.schema, name="FixMsg")

print(field.into_json(indent=2))
```

Downloads are diagnostic snapshots. The packaged registry and runtime field
remain authoritative.
