# FIX registry

The registry is a collection of ordinary `rekep.Field` definitions. FIX
identity and behavior live in each field's `fix` view; nested datatypes express
components and repeating groups without another model.

## Loaded inventory

| source | count | dialect |
| --- | ---: | --- |
| crate fields | 37 | standard, tags 65001–65038 |
| bundled specification | 6,239 | standard |
| plugin vocabulary | 38 | `plugin` |
| process total | 6,314 | 1 dialect |

```python
from rekep.fix import fix_crate_fields, fix_plugin_fields, fix_registry

registry = fix_registry()

assert len(registry) == 6314
assert registry.dialects() == ["plugin"]
assert len(fix_crate_fields()) == 37
assert len(fix_plugin_fields()) == 38
```

Scalar fields are one shape of definition among four. Components and repeating
groups are declarations the same dictionary holds; a message type is what a
`MsgType(35)` value names:

| shape | count | read by |
| --- | ---: | --- |
| scalar fields | 6,314 | `registry.definitions("fields")` |
| components | 930 | `registry.definitions("components")` |
| repeating groups | 581 | `registry.definitions("groups")` |
| message types | 182 | `registry.msgtypes()` |

```python
from rekep.fix import fix_registry

registry = fix_registry()

assert sum(1 for _ in registry.definitions("fields")) == 6314
assert sum(1 for _ in registry.definitions("components")) == 930
assert sum(1 for _ in registry.definitions("groups")) == 581
assert sum(1 for _ in registry.msgtypes()) == 182
```

## What a field carries

| property | API | meaning |
| --- | --- | --- |
| canonical id | `field.fix.id` | stable integer identity of the definition |
| canonical tag | `field.fix.tag` | integer protocol tag |
| alternate tags | `field.fix.tags` | historical or equivalent identifiers |
| storage name | `field.name` | folded canonical Arrow column name |
| display name | `field.display` | specification capitalization |
| aliases | `field.fix.aliases` | alternate folded name lookups |
| dialects | `field.fix.branches` | the named dialects claiming it; empty is standard |
| datatype | `field.dtype` | scalar, struct, or list storage shape |
| description | `field.fix.description` | specification meaning |
| code set | `field.fix["codes"]` | versioned wire value/name translations |
| lineage | `field.fix["lineage"]` | names and datatypes by FIX version |

```python
import json

from rekep.fix import fix_registry

registry = fix_registry()
side = registry.field_by_tag(54)
codes = json.loads(side.fix["codes"])

assert side.name == "side"
assert side.display == "Side"
assert side.fix.tag == 54
assert side.fix.branches == []
assert str(side.dtype.into_arrow()) == "string"
assert codes[0]["value"] == "1"
assert codes[0]["name"] == "Buy"
```

## Lookup methods

Raising methods are useful when absence is an invalid contract; `get_*`
methods return `None` for exploratory code.

| strict | optional | key |
| --- | --- | --- |
| `field(key)` / `registry[key]` | `get_field(key)` / `get(key)` | integer tag or string name |
| `field_by_id(id)` | `get_field_by_id(id)` | exact definition identity |
| `field_by_tag(tag)` | `get_field_by_tag(tag)` | canonical or alternate tag |
| `field_by_name(name)` | `get_field_by_name(name)` | canonical name or alias |
| `field_by_path(path)` | `get_field_by_path(path)` | nested component/group path |
| `group_by_tag(tag)` | `get_group_by_tag(tag)` | the list a counter tag declares |

```python
from rekep.fix import fix_registry

registry = fix_registry()

assert registry[54] == registry.field_by_tag(54)
assert registry["SIDE"] == registry.field_by_name("side")
assert registry.get_field_by_name("not-a-field") is None
assert 55 in registry
assert "Symbol" in registry
```

Name lookup is ASCII case-insensitive and ignores the separators used by the
FIX name fold. Tag lookup never guesses from a textual name. An exact id names
one definition and falls through to nothing.

## Resolution tiers

The registry is one namespace, so a bare key resolves in this order:

1. a numeric key, against the tag it is;
2. a folded name, against the canonical names and their aliases — a plugin
   name and a standard name are read the same way;
3. nothing, which is an unknown pair rather than a failure.

A capture is no exception: it is named for the field it fills, so the bridge's
`msgseqnum` bracket part *is* `msgseqnum` and fills `MsgSeqNum(34)`, with no
alias table mapping a spelling onto a tag in between. Standard tags remain
standard whatever dialect claims a field beside them; a named dialect may
claim only its own user-tag range.

```python
from rekep.fix import PLUGIN_DIALECT, fix_registry

registry = fix_registry()
standard = registry.field_by_name("MsgType")
plugin = registry.field_by_name("PriorityLevel")

assert standard.fix.tag == 35
assert standard.fix.branches == []
assert plugin.fix.branches == [PLUGIN_DIALECT]
```

## Repeating groups and paths

A counter field counts; the group it counts is a list whose item is a struct,
and `group_by_tag` answers it under the same tag. Inspect the declaration
instead of reconstructing members from names:

```python
from rekep.fix import fix_registry

registry = fix_registry()
counter = registry.field_by_tag(453)
parties = registry.group_by_tag(453)
members = [
    member.name
    for expanded in parties.explode_fields()
    for member in expanded.unnest_fields()
]

assert counter.name == "nopartyids"
assert parties.name == "parties"
assert members[:4] == ["partyid", "partyidsource", "partyrole", "partyrolequalifier"]
```

`field_by_path` walks component and group declarations, for example
`Parties.PartyID`. `FixMsg.by_path` walks values and therefore requires an
occurrence when entering a list, spelled the way the one grammar spells it:
`Parties[0].PartyID`.

## Code sets and version lineage

Code translation happens before datatype conversion. For `Side(54)`, both
`1` and `buy` can resolve to the stored code `1`; for `MsgType(35)`,
`executionreport` resolves to `8`. No version is pinned anywhere: the version
a row itself states selects the code spellings valid at it, without renaming
the Arrow column.

Lineage explains how one field evolved while preserving one current identity:

```python
import json

from rekep.fix import fix_registry

msgtype = fix_registry().field_by_tag(35)
lineage = json.loads(msgtype.fix["lineage"])

assert lineage[0]["since"] == "2.7"
assert lineage[-1]["name"] == "msgtype"
```

## Runtime fields

Every registry starts with the crate's own fields: tags 65001–65038, with
65004 permanently retired. They are generated by the codec and are not stored
in dictionary shards.

| tag range | fields |
| --- | --- |
| 65001–65003 | version, normalized symbol, settled instant |
| 65005–65008 | parent order ids, stated sender session, message context |
| 65009–65012 | plugin ids and session names |
| 65013–65015 | resolved ISIN, MIC, and lifecycle state |
| 65016–65019 | instrument, message, and chain identities, stated target session |
| 65020–65026 | declared identifiers, chain links, creation and snapshot instants, source object |
| 65027–65029 | arrival counter, capture and expiry instants |
| 65030–65038 | lane currencies, bridge session instance, instrument codes, session message ids |

65004 is a retired slot and stays empty: a FIX row materializes no partition
column of its own, and `timepartition` — rekep's own hour transform over
`timestamp` — remains the only layout column either table carries.

```python
from rekep.fix import fix_crate_fields

assert [field.fix.tag for field in fix_crate_fields()] == [
    tag for tag in range(65001, 65039) if tag != 65004
]
```

## Iterate, filter, and export

```python
from pathlib import Path

from rekep.fix import fix_registry

registry = fix_registry()
groups = [field for field in registry.definitions("groups")]
priced = [
    field
    for field in registry
    if "price" in (field.fix.description or "").casefold()
]

registry.write_into(Path("build/fix-registry-copy"))
assert groups
assert priced
```

Iterating the registry itself walks its scalar fields; the other three shapes
are reached through `definitions` and `msgtypes`. An explicit registry loaded
with `fix_registry(path_or_uri)` receives the same crate and plugin fields. A
location containing no specification fields is rejected, preventing a pipeline
from silently creating a narrow table.

## Browser

Search the same 6,314 definitions here, by tag, name, alias or description:

<div data-fix="registry"></div>

The [registry browser](../tools/fix-registry.md) is the Marimo tool over the
same dictionary; it exposes branches, groups, members, code sets, lineage, raw
metadata, Field JSON, and the complete `FixMsg` schema. Both read generated
assets that are read-only projections of this same bundled registry.
