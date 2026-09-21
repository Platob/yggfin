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

Search covers canonical tags, alternate tags, storage and display names, the
other spellings the dictionary keeps, and descriptions. Branch and category filters narrow to one dialect, or to one
of the three categories a dictionary stores: `fields`, `components` and
`groups`. A scalar field, a component and a repeating group are one kind of
thing under three names, so the browser reads all three through one projection
and the `shape` column says which of them is nested.

The summary reports the counts per category, the dialects, code sets, and the
message types. The bundled dictionary should report 7,778 definitions --
6,268 scalar fields, 928 components and 582 repeating groups -- plus 736
centrally owned code sets, under no named dialect.

## Message types

One table above the browser lists every message type the dictionary defines,
by the wire code `MsgType(35)` carries: its display name, its storage name,
the identifiers the type declares, and its description. A message type is a
component carrying `FIX:msgtype`, so those are where they are counted: the
bundled dictionary defines 181 of them.

## Definition views

| view | source |
| --- | --- |
| Overview | identity, dialect, type, shape, description |
| Members | `Field.explode_fields()` and `Field.unnest_fields()` |
| Lineage | validated version history metadata |
| Codes | validated wire-value/name translations |
| Metadata | the complete field metadata mapping |
| Field JSON | deterministic `Field.into_json(indent=2)` |

The full-schema panel asks the codec for its native field. It therefore
displays the actual registry-dependent `FixMsg` projection without parsing or
fabricating a row or input schema.

```python
from rekep.fix import fix_message_field

field = fix_message_field()

print(field.into_json(indent=2))
```

Downloads are diagnostic snapshots. The packaged registry and runtime field
remain authoritative.

## Regenerate the published assets

The FIX pages embed their own widgets -- the
[registry search](../fix/registry.md#browser),
[decoder](../fix/decode.md#try-one-line) and
[encoder](../fix/encode.md#build-a-message) -- reading a projection of the same
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
