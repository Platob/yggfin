# Definitions and lookups

The registry is a collection of ordinary `rekep.Field` definitions. FIX
identity and behavior live in each field's `fix` view; nested datatypes express
components and repeating groups without another model. Scalar fields,
components and repeating groups share one namespace, and a message type is
what a `MsgType(35)` value names. [The bundled registry](index.md) counts them.

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
from rekep import FixRegistry

registry = FixRegistry.from_env()
side = registry.field_by_tag(54)
codes = registry.get_codeset(side.fix.codeset)

assert side.name == "side"
assert side.display == "Side"
assert side.fix.tag == 54
assert side.fix.branches == []
assert str(side.dtype.into_arrow()) == "int32"
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
| `field_by_counter(tag)` | `get_field_by_counter(tag)` | the group a counter tag counts |

```python
from rekep import FixRegistry

registry = FixRegistry.from_env()

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
from rekep import FixRegistry

registry = FixRegistry.from_env()
standard = registry.field_by_name("MsgType")

assert standard.fix.tag == 35
assert standard.fix.branches == []
assert registry.field_by_name("side") == registry["SIDE"]
```

## Repeating groups and paths

A counter field counts; the group it counts is a list whose item is a struct,
and `field_by_counter` answers it by the counter's tag. Inspect the declaration
instead of reconstructing members from names:

```python
from rekep import FixRegistry

registry = FixRegistry.from_env()
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
from rekep import FixRegistry

registry = FixRegistry.from_env()
msgtype = registry.field_by_tag(35)
codes = registry.get_codeset(msgtype.fix.codeset)

assert msgtype.name == "msgtype"
assert {"value": "D", "name": "NewOrderSingle"}.items() <= codes[
    next(index for index, code in enumerate(codes) if code["value"] == "D")
].items()
```

## Crate fields

Every registry starts with 41 crate definitions, 39 scalar and two nested --
`srcuuids` and `metadata` -- at tags 65001 to 65041, numbered in the order
the fixed row states them. They cover the event clocks and identities, the
lifecycle `state` and `msgcat`, the capture context a bridge's row header
fills, the normalized instrument codes `isincode`, `forexcode`,
`bloombergcode`, `figicode` and `miccode`, the option's `strikepx`, and the
bridge's own order and instrument identifiers. They are
derived facts rather than copies of dictionary shards.

```python
from rekep.fix import fix_crate_fields

fields = fix_crate_fields()
tags = [field.fix.tag for field in fields]

assert len(tags) == 41
assert min(tags) == 65001 and max(tags) == 65041
assert [field.name for field in fields if field.dtype.is_nested] == ["srcuuids", "metadata"]
```

## Iterate, filter, and export

```python
import tempfile
from pathlib import Path

from rekep import FixRegistry

registry = FixRegistry.from_env()
priced = [
    field
    for field in registry
    if "price" in (field.fix.description or "").casefold()
]

registry.write_into(Path(tempfile.mkdtemp()) / "fix")
assert priced
assert registry.field_by_name("parties").dtype.is_nested
```

Iterating the registry itself walks its scalar fields; a component, a group or
a message type is reached by name, by path or by counter, and enumerated
through the dictionary's own document. A registry read back with
`FixRegistry.from_handle(folder)` holds the same crate columns, and a folder
holding no specification field is refused wherever a table's shape is built,
so a task never creates a narrow table.

## Browser

Search the same 7,790 definitions here, by tag, name, spelling or description:

<div data-fix="registry"></div>

An opened entry shows its members, code set and lineage. The search reads
generated assets, read-only projections of this same bundled registry, and
[Registry assets](assets.md) regenerates them. The complete field metadata,
Field JSON and the fixed FIX row are the Python API's: `FixRegistry.from_env()`
above, and `rekep.fix.fix_message_field()` for the row a codec lands.
