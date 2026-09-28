# Registry assets

The FIX pages embed three widgets -- the
[registry search](registry.md#browser), the
[decoder](decode.md#try-one-line) and the
[encoder](encode.md#build-a-message) -- which read a projection of the
bundled dictionary, because a browser cannot open a registry folder.
`tools/fix_registry_dump.py` writes that projection from
`FixRegistry.from_env()`. It reads the registry and creates no tables, edits
no definitions and runs no task.

## Regenerate the published assets

Two files, so a page load does not carry a megabyte of code sets:

| file | holds |
| --- | --- |
| `docs/assets/fix-registry.json` | the index every widget needs to resolve a key |
| `docs/assets/fix-details.json` | members, code sets and lineage, fetched when an entry is opened |

Both are generated and committed. Rebuild them from the repository root
whenever the bundled registry changes, and commit the result:

```bash
uv run --project python python tools/fix_registry_dump.py
```

Nothing rebuilds them automatically, so a registry change that skips this
step leaves the published pages showing the previous dictionary.

## What the search covers

The [registry search](registry.md#browser) covers canonical tags, alternate
tags, storage and display names, the other spellings the dictionary keeps,
and descriptions, over the 7,790 definitions -- 6,281 scalar fields, 928
components and 581 repeating groups -- and the 737 code sets, `statecodeset`
among them. What the search does not show -- a definition's complete
metadata and Field JSON, or the fixed row a codec lands -- the Python API
reads from the registry itself:

```python
from rekep import FixRegistry
from rekep.fix import fix_message_field

registry = FixRegistry.from_env()
parties = registry.field_by_name("parties")

assert registry.field_by_tag(55).name == "symbol"
assert registry.msgtype("D").name == "newordersingle"
assert len(fix_message_field()) == 133
print([member.name for member in parties.explode_fields()])
print(parties.into_json(indent=2))
```

The packaged registry and the fields it declares are authoritative; the
assets and anything printed from them are projections of it.
