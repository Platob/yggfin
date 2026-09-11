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
bundled registry should report 6,883 definitions across two branches: its
6,303 tagged scalars and the 580 repeating groups, which are named definitions
rather than tagged ones and so are listed beside them rather than by iterating
the registry.

## Definition views

| view | source |
| --- | --- |
| Overview | identity, branch, type, nullability, description |
| Members | `Field.explode_fields()` and `Field.unnest_fields()` |
| Lineage | validated version history metadata |
| Codes | validated wire-value/name translations |
| Metadata | the complete field metadata mapping |
| Field JSON | deterministic `Field.into_json(indent=2)` |

The full-schema panel builds a carrier stating the payload column and nothing
else, then asks `FixCodec.parse_text_arrow_reader` for its output schema. It
therefore displays the actual registry-dependent `FixMsg` projection without
parsing or fabricating a row: a codec answers a schema from the capture it is
given, so a carrier holding only `body` answers the dictionary's own columns.

```python
import pyarrow

from rekep import Field
from rekep.fix import PAYLOAD_COLUMN, FixCodec, fix_registry

carrier = pyarrow.schema([pyarrow.field(PAYLOAD_COLUMN, pyarrow.large_binary())])
empty = pyarrow.RecordBatchReader.from_batches(carrier, [])
parsed = FixCodec(fix_registry()).parse_text_arrow_reader(empty)
field = Field.from_arrow_schema(parsed.schema, name="FixMsg")

assert [member.name for member in field][:2] == ["body", "beginstring"]
assert [member.name for member in field][-2:] == [
    "nofixentries",
    "nounmappedfixentries",
]
```

Downloads are diagnostic snapshots. The packaged registry and runtime field
remain authoritative.

## Regenerate the published assets

The FIX pages embed their own widgets — the
[registry search](../fix/registry.md#browser),
[decoder](../fix/decode.md#try-one-line) and
[encoder](../fix/encode.md#build-a-message) — reading a projection of the same
dictionary, because a browser cannot open an `IOBase` folder. Two files, so a
page load does not carry five megabytes of code sets:

| file | holds |
| --- | --- |
| `docs/assets/fix-registry.json` | the index every widget needs to resolve a key |
| `docs/assets/fix-details.json` | members, code sets and lineage, fetched when an entry is opened |

Both are generated and committed. Rebuild them from the repository root
whenever the bundled registry changes, and commit the result:

```bash
uv run --project python python tools/fix_registry_dump.py
```

Nothing rebuilds them automatically, so a registry change that skips this step
leaves the published pages showing the previous dictionary.
