# FIX registry assets

The FIX pages embed three widgets -- the
[registry search](../fix/registry.md#browser),
[decoder](../fix/decode.md#try-one-line) and
[encoder](../fix/encode.md#build-a-message) -- which read a projection of the
bundled dictionary, because a browser cannot open an `IOBase` folder.
`tools/fix_registry_dump.py` writes that projection. It reads the registry and
creates no tables, edits no definitions, and runs no pipeline task.

## Regenerate the published assets

Two files, so a page load does not carry five megabytes of code sets:

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

## Browse the registry

The [registry search](../fix/registry.md#browser) covers canonical tags,
alternate tags, storage and display names, the other spellings the dictionary
keeps, and descriptions. The bundled dictionary holds 7,781 definitions --
6,271 scalar fields, 928 components and 582 repeating groups -- plus 736
centrally owned code sets, under no named dialect. A message type is a
component carrying `FIX:msgtype`, and the bundled dictionary defines 181.

An opened entry shows its members, code set and lineage. What the search does
not show -- a dictionary at another location, a definition's complete metadata
and Field JSON, or the `FixMsg` row a codec lands -- the Python API reads from
the registry itself:

```python
from rekep.fix import fix_message_field, fix_registry

registry = fix_registry()
parties = registry.field_by_name("parties")

assert registry.field_by_tag(55).name == "symbol"
assert registry.msgtype("D").name == "newordersingle"
print([member.name for member in parties.explode_fields()])
print(parties.into_json(indent=2))
print(fix_message_field().into_json(indent=2))
```

`fix_registry(path_or_uri)` reads an explicit dictionary instead, with the
runtime and bridge fields added exactly as they are for parsing. The packaged
registry and the runtime field are authoritative; the assets and anything
printed from them are projections of it.
