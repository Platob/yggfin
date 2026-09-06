# Registry

A FIX field is an ordinary Yggdryl `Field` whose `fix:` metadata the protocol
view `field.fix` reads and writes as typed properties. There is no second field
class, and nesting needs no second type: a component is a Struct field, a
repeating group is a `List` of that Struct, and the group's counter tag is the
group field's own `fix:tag`.

| property | key | meaning |
| --- | --- | --- |
| branch | `fix:branch` | the dictionary this field belongs to; absent is standard |
| tag | `fix:tag` | canonical FIX tag |
| tags | `fix:tags` | alternate tags, highest priority first |
| aliases | `fix:aliases` | alternate names, highest priority first |
| description | `description` | the specification's own wording, on the generic key rather than under `fix:` |

The canonical name is the field's own `name()`, the datatype its own `dtype()`,
and the display name the generic `display` key. Nobody spells `fix:` at a call
site.

## Browse the dictionary

Search by tag, name, alias, or description. Every entry opens to its Arrow
type, its alternate spellings, its members when it is a repeating group, and
its code set and lineage when the dictionary carries them.

<div data-fix="registry">Loading the dictionary…</div>

## Resolution

The registry answers an identifier or a name through two tiers within one
branch, and a later tier is consulted only when every earlier one missed:

1. canonical identifier, then alternate identifiers;
2. canonical name folded, then aliases folded.

Names fold ASCII case once, on the way in, so a query spelled in any case finds
the field and the answer is always the canonical spelling. A tag query never
consults names and a name query never consults tags, and an alias can never
take a name away from a field that claims it canonically. An explicit branch
pins one dictionary; when omitted, resolution tries the standard dictionary
first and then named dictionaries in canonical name order.

```python
from yggdryl import IOBase
from yggdryl.fix import FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))

side = registry.field_by_tag(54)
# The canonical name is the field's own name, folded; the specification's
# own capitalisation is the display name beside it.
assert side.name == "side"
assert side.display == "Side"
# A query spelled in any case answers that canonical spelling.
assert registry.field_by_name("Side").name == "side"
# A name the dictionary does not claim answers nothing rather than guessing.
assert registry.get_field_by_name("Px") is None
```

`get_*` answers `None` where `*` raises, and `registry[key]` /
`key in registry` accept a tag or a name through the same match.

## Identity and the user range

`FixId` packs one tag and one branch digest into an `i64`. It is derived on
every read from `fix:branch` and `fix:tag` and never stored -- there is no
`fix:id` key on disk. `FixId.from_parts` is the one admissibility gate: a
non-standard branch may claim only `USER_TAG_MIN` through `USER_TAG_MAX`
(exclusive), while the standard branch may hold every non-negative tag.

```python
from yggdryl.fix import STANDARD_BRANCH, USER_TAG_MAX, USER_TAG_MIN

assert STANDARD_BRANCH == ""
assert (USER_TAG_MIN, USER_TAG_MAX) == (5000, 40000)
```

## Storage

A registry reads and writes through one `IOBase` folder handle, into two trees
plus one branch manifest:

```text
<root>/primitive/<shard>.json
<root>/primitive/<branch>/<shard>.json
<root>/nested/<shard>.json
<root>/nested/<branch>/<shard>.json
<root>/branches.json
```

`primitive` holds the fields whose datatype is one scalar value and `nested`
the ones whose datatype carries a subtree. `shard = tag / 100` is unchanged
inside each tree, each shard a JSON array of the core field document ordered by
canonical identifier, so a tag reaches exactly one shard by arithmetic and an
alternate tag never fans a field across shards. `branches.json` records
optional dialect, extension-pack and session facts; its absence means bare
branch records.

Writing is the exact inverse, and it removes the shards, branch folders and
trees no field populates any more:

```python
from yggdryl import IOBase
from yggdryl.fix import FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
registry.write_into(IOBase.from_uri("file:/tmp/fix-copy"))
```

A folder that is not there loads as the empty registry and is not created. A
shard that does not parse is a `ValueError` naming the URL.

## Adding a definition

A definition is a `Field` with `fix:` metadata, so it is declared the way every
other field is:

```python
from yggdryl import DataType, Field
from yggdryl.fix import FixRegistry

msgdirection = Field("MsgDirection", DataType("utf8"), nullable=True)
msgdirection.fix.tag = 385
msgdirection.fix.description = "Specifies the direction of the message."

registry = FixRegistry()
registry.insert(msgdirection)
assert registry.field_by_tag(385).name == "MsgDirection"
```

`insert` replaces only an equal canonical identity and name, and answers what
it replaced. `update` merges the whole of a stored field -- the generic keys,
and the entire `fix:` namespace, lineage and code set included -- and refuses a
differing name or datatype outright. Both refuse a collision by name rather
than silently shadowing one.

## The fields this crate adds

Adding a definition is not the only way a dictionary grows. Yggdryl derives
seven facts no standard tag names, on its own branch, and `with_crate_fields`
puts them in the dictionary:

```python
from yggdryl.fix import fix_crate_fields

for field in fix_crate_fields():
    print(field.fix.tag, field.name, field.dtype.into_arrow())
```

They are the message digest, the version read, the cross-venue ticker, the
market clock, the partition it falls in, and the two parent order identifiers.
The columns exist eitherway -- `fix_schema` falls back to `fix_crate_fields()`
for a tag the dictionary does not declare -- so registering them is what puts
them in *this* dictionary, for a reader browsing it or resolving one by name.
What each one means is in [Quality](quality.md).

## The dump these pages read

A browser cannot open an `IOBase` folder, so the widgets on these pages read a
projection of `config/fix` rather than the dictionary itself. Two files, because
six thousand definitions carry several megabytes of code sets and a page load
does not owe a reader that:

| asset | holds | read |
| --- | --- | --- |
| [`assets/fix-registry.json`](../assets/fix-registry.json) | tag, name, display, branch, shape, Arrow type, nullability, description, and the alternate spellings when there are any | once, on load |
| [`assets/fix-details.json`](../assets/fix-details.json) | members, code sets and lineage, keyed by tag | once, the first time an entry is opened |

[`tools/fix_registry_dump.py`](https://github.com/Platob/yggfin/blob/main/tools/fix_registry_dump.py)
writes both, and is rerun from the repository root whenever `config/fix`
changes:

```text
python tools/fix_registry_dump.py
  ✓ docs/assets/fix-registry.json (1,809,232 bytes)
  ✓ docs/assets/fix-details.json (3,408,019 bytes)
6,210 definitions, 3,520 with deep records
```

Every value in either file comes off the field itself, through the same
protocol view a call site uses -- there is no second description of a
definition anywhere:

```python
from yggdryl import IOBase
from yggdryl.fix import FixRegistry

registry = FixRegistry.from_handle(IOBase.from_uri("file:config/fix"))
field = registry.field_by_tag(54)

assert {
    "tag": field.fix.tag,
    "name": field.name,
    "display": field.display or field.name,
    "branch": field.fix.branch or "standard",
    "shape": "group" if field.dtype.is_nested else "field",
    "type": str(field.dtype.into_arrow()),
    "nullable": field.nullable,
} == {
    "tag": 54,
    "name": "side",
    "display": "Side",
    "branch": "standard",
    "shape": "field",
    "type": "fixed_size_binary[4]",
    "nullable": True,
}
```

The dump is a projection for reading, never an authority: `parse_fix` asks its
selected runtime registry for the schema before it reads a batch.

## The standalone application

[`tools/fix_registry.py`](https://github.com/Platob/yggfin/blob/main/tools/fix_registry.py)
is the same inspection against a live registry rather than a dump, including
the complete `FixMsg` parser schema. See
[FIX registry browser (tool)](../tools/fix-registry.md).
