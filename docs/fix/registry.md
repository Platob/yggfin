# FIX registry

The registry is a collection of ordinary `rekep.Field` definitions. FIX
identity and behavior live in each field's `fix` view; nested datatypes express
components and repeating groups without another model.

## Loaded inventory

| source | count | dialect |
| --- | ---: | --- |
| crate definitions | 32 | standard, tags 65003-65064 |
| bundled specification and the crate's own | 6,271 scalar fields | standard |
| definitions in all | 7,781 | no named dialect |
| central code sets | 736 | field references use `FIX:codeset` |

Every registry holds the crate's own columns and the two standard clocks from
construction, so a bundled dictionary is those definitions and the
specification's, in one namespace.

```python
from rekep.fix import fix_crate_fields, fix_registry

registry = fix_registry()

assert len(registry) == 7781
assert registry.dialects() == []
assert len(fix_crate_fields()) == 32
```

Scalar fields are one shape of definition among four. Components and repeating
groups are declarations the same dictionary holds; a message type is what a
`MsgType(35)` value names. `MsgCat` is the central message-category vocabulary
derived beside it:

| shape | count | read by |
| --- | ---: | --- |
| scalar fields | 6,271 | iterating the registry |
| components | 928 | the `components` array of `registry.into_json()` |
| repeating groups | 582 | the `groups` array of the same document |
| message types | 181 | the components carrying `FIX:msgtype` |

Iteration walks the scalars; the dictionary's own document is what enumerates
the other two, and a component or a group is reached directly by
`registry.field_by_name` or `registry.field_by_path` once its name is known.

```python
import json

from rekep.fix import fix_registry

registry = fix_registry()
document = json.loads(registry.into_json())

assert sum(1 for _ in registry) == 6271
assert len(document["components"]) == 928
assert len(document["groups"]) == 582
assert registry.msgtype("D").name == "newordersingle"
```

## What a field carries

| property | API | meaning |
| --- | --- | --- |
| canonical id | `field.fix.id` | stable integer identity of the definition |
| canonical tag | `field.fix.tag` | integer protocol tag |
| alternate tags | `field.fix.tags` | historical or equivalent identifiers |
| storage name | `field.name` | folded canonical Arrow column name |
| display name | `field.display` | specification capitalization |
| alternate names | `field.fix.names` | other folded name lookups the dictionary keeps |
| dialects | `field.fix.branches` | the named dialects claiming it; empty is standard |
| datatype | `field.dtype` | scalar, struct, or list storage shape |
| description | `field.fix.description` | specification meaning |
| code set | `field.fix.codeset` | name of the centrally owned `FIX:codeset` vocabulary |

```python
from rekep.fix import fix_registry

registry = fix_registry()
side = registry.field_by_tag(54)
codes = registry.get_codeset(side.fix.codeset)

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
2. a folded name, against the canonical names and the other spellings the
   dictionary keeps for them -- every name is read the same way;
3. nothing, which is an unknown pair rather than a failure.

A capture is no exception: it is named for the field it fills, so the bridge's
`msgseqnum` bracket part *is* `msgseqnum` and fills `MsgSeqNum(34)`, with no
alias table mapping a spelling onto a tag in between. Standard tags remain
standard whatever dialect claims a field beside them; a named dialect may
claim only its own user-tag range.

```python
from rekep.fix import fix_registry

registry = fix_registry()
standard = registry.field_by_name("MsgType")

assert standard.fix.tag == 35
assert standard.fix.branches == []
assert registry.field_by_name("side") == registry["SIDE"]
```

## Repeating groups and paths

A counter field counts; the group it counts is a list whose item is a struct,
and `group_by_tag` answers it under the same tag. Inspect the declaration
instead of reconstructing members from names:

```python
from rekep.fix import fix_registry

registry = fix_registry()
counter = registry.field_by_tag(453)
parties = registry.field_by_counter(453)
members = [
    member.name
    for expanded in parties.explode_fields()
    for member in expanded.unnest_fields()
]

assert counter.name == "nopartyids"
assert parties.name == "parties"
assert parties.dtype.is_nested
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

Each vocabulary is stored once. Fields carry its name in `FIX:codeset`, and
registry mutation invalidates the typed lookup cache. Lineage separately
explains how one field evolved while preserving one current identity:

```python
from rekep.fix import fix_registry

registry = fix_registry()
msgtype = registry.field_by_tag(35)
codes = registry.get_codeset(msgtype.fix.codeset)

assert msgtype.name == "msgtype"
assert {"value": "D", "name": "NewOrderSingle"}.items() <= codes[
    next(index for index, code in enumerate(codes) if code["value"] == "D")
].items()
```

## Runtime fields

Every registry starts with 32 crate definitions: 28 scalar columns and four
nested ones. They cover event clocks and identities, lifecycle state, capture
context, metadata, residual entries, and the seven normalized instrument-code
columns. The code fields use `ISINCode`, `CFICode`, `CUSIPCode`, `SEDOLCode`,
`BloombergCode`, `FIGICode`, and `MICCode`; CFI remains standard FIX tag 461.
They are derived runtime facts rather than copies of dictionary shards.

```python
from rekep.fix import fix_crate_fields

tags = [field.fix.tag for field in fix_crate_fields()]

assert len(tags) == 32
assert min(tags) == 65003 and max(tags) == 65064
assert 65017 in tags and 65039 in tags
```

## Iterate, filter, and export

```python
from pathlib import Path

from rekep.fix import fix_registry

registry = fix_registry()
priced = [
    field
    for field in registry
    if "price" in (field.fix.description or "").casefold()
]

registry.write_into(Path("build/fix-registry-copy"))
assert priced
assert registry.field_by_name("parties").dtype.is_nested
```

Iterating the registry itself walks its scalar fields; a component, a group or
a message type is reached by name, by path or by counter, and enumerated
through the dictionary's own document. An explicit registry loaded with
`fix_registry(path_or_uri)` holds the same crate columns. A
location containing no specification fields is rejected, preventing a pipeline
from silently creating a narrow table.

## Browser

Search the same 7,781 definitions here, by tag, name, spelling or description:

<div data-fix="registry"></div>

The [registry browser](../tools/fix-registry.md) is the Marimo tool over the
same dictionary; it exposes branches, groups, members, code sets, lineage, the
complete field metadata, Field JSON, and the complete `FixMsg` schema. Both
read generated
assets that are read-only projections of this same bundled registry.
