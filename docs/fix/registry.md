# Registry

A FIX definition is a native Yggdryl `Field` with `fix:` metadata. Scalar,
struct, and list datatypes already express primitive fields, components, and
repeating groups, so there is no second FIX field class.

| property | metadata | contract |
| --- | --- | --- |
| branch | `fix:branch` | dialect; empty means standard |
| tag | `fix:tag` | canonical numeric tag |
| alternate tags | `fix:tags` | identifier aliases, in priority order |
| aliases | `fix:aliases` | name aliases, in priority order |
| description | `description` | generic field description |

The canonical column name is the field's folded `name`; display capitalization
stays in `display`.

## Browse

Search by tag, name, alias, or description. Opening a row shows its Arrow type,
members, code set, and lineage.

<div data-fix="registry">Loading the dictionary…</div>

## Resolution

Within a branch, the registry tries canonical tag then alternate tags, followed
by canonical folded name then folded aliases. A tag lookup never guesses from
a name. Without an explicit branch, standard fields win before named branches.

```python
from yggdryl import IOBase
from yggdryl.fix import FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
side = registry.field_by_tag(54)

assert side.name == "side"
assert side.display == "Side"
assert registry.field_by_name("SIDE").name == "side"
assert registry.get_field_by_name("Px") is None
```

`get_*` returns `None` where `field_*` raises. `registry[key]` and
`key in registry` use the same tag-or-name resolution.

## Seeded and bridge fields

Every registry contains the 16 Yggdryl fields at construction, even when its
storage location is empty. They use standard tags 65000–65015 and are listed
by `fix_crate_fields()`; callers do not install them.

```python
from yggdryl.fix import FixRegistry, fix_crate_fields

registry = FixRegistry()
fields = fix_crate_fields()

assert len(registry) == len(fields) == 16
assert [field.fix.tag for field in fields] == list(range(65000, 65016))
```

`with_ulbridge_fields()` adds 43 ULBridge management fields on branch
`ulbridge`. `parse_fix` calls it because its source is a ULBridge log. This is
idempotent and enriches the selected registry in place.

## Stored dictionary

A registry reads and writes one `IOBase` folder:

```text
<root>/primitive/<shard>.json
<root>/primitive/<branch>/<shard>.json
<root>/nested/<shard>.json
<root>/nested/<branch>/<shard>.json
<root>/branches.json
```

Primitive and nested definitions are sharded by `tag / 100`. Seeded crate
fields are runtime-owned and are not written to the folder. A missing folder
loads as a registry containing only those seeded fields; `parse_fix` rejects
that as an empty external dictionary.

```python
from yggdryl import IOBase
from yggdryl.fix import FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
copy = IOBase.from_uri("file:/tmp/fix-copy")
registry.write_into(copy)
```

## Add a definition

```python
from yggdryl import Field

field = Field("venueorderflag", "bool", nullable=True)
field.fix.branch = "venue"
field.fix.tag = 20001
field.fix.description = "Whether the venue accepted the order."

registry.insert(field)
assert registry.field_by_name("venueorderflag", "venue").fix.tag == 20001
```

`insert` replaces only the same canonical identity and name; `update` folds
metadata into a matching field and refuses a name or datatype conflict.

## Generated browser projection

The browser cannot open an `IOBase` folder, so
`tools/fix_registry_dump.py` writes two derived assets:

| asset | content |
| --- | --- |
| `docs/assets/fix-registry.json` | searchable field index |
| `docs/assets/fix-details.json` | members, code sets, and lineage |

Regenerate them whenever `config/fix` or the Yggdryl registry fields change:

```bash
uv run --project python --frozen python tools/fix_registry_dump.py
```

The generated projection is for documentation only. `parse_fix` always asks
its live registry for the reader schema before consuming a batch.
