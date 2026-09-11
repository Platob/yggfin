# FIX registry

A registry is a dictionary: everything a reader has to know before it can turn
a captured line into a typed row. It holds four categories of ordinary
`rekep.Field` definitions, and FIX identity lives in each field's `fix` view,
so a tag, a code set and a repeating group need no model beside `Field`.

| category | what a definition is | identified by |
| --- | --- | --- |
| `fields` | one tagged scalar; a group counter is an `int32` here | canonical tag and branch, plus the folded name |
| `messages` | a non-null struct owned by one immutable `MsgType` | name and branch; `fix:msgtype` carries the wire code |
| `components` | a named struct | name and branch |
| `groups` | a named list of a non-null struct occurrence | name and branch; `fix:counter` names the scalar that counts it |

Iteration and `len()` walk the tagged scalars only. The other three categories
are reached through `definitions(category)`, because a message and a field that
share a spelling are not the same definition.

## Loaded inventory

```python
from rekep.fix import fix_registry

registry = fix_registry()

assert len(registry) == 6303
assert sum(1 for _ in registry.definitions("messages")) == 181
assert sum(1 for _ in registry.definitions("components")) == 747
assert sum(1 for _ in registry.definitions("groups")) == 580
assert [branch.name for branch in registry.branches()] == ["", "ulbridge"]
```

| source | count | branch |
| --- | ---: | --- |
| bundled specification | 6,241 | standard |
| runtime fields | 20 | standard, tags 65000–65019 |
| bridge vocabulary | 42 | `ulbridge` |
| process total | 6,303 | 2 branches |

Beside those scalars the bundle carries 181 messages, 747 components and 580
repeating groups, generated from the eleven published FIX sources named in
`provenance.json` at specification 5.0.2, extension pack 309.

## Where the dictionary lives

The bundled dictionary is a folder of JSON documents inside the installed
package, one folder per category:

```text
python/src/rekep/_data/fix/
  fields/<tag div 100>.json          tagged scalars, sharded a hundred tags at a time
  fields/<branch>/<shard>.json       the same, for a named branch
  components/<name>.json             one document per component
  groups/<name>.json                 one document per repeating group
  messages/<name>.json               one document per message
  branches.json                      optional: the dialects the dictionary declares
  provenance.json                    the sources the generator read, with their digests
```

The four category folders are read in the order `fields`, `components`,
`groups`, `messages`, so a definition's members exist by the time it is read.
A folder directly under a category is a branch, named by the folder;
`branches.json` is optional and states what a branch means where the fields
alone do not. `provenance.json` is the generator's own record and the loader
never reads it.

Every document is the core `Field` projection, so the whole `fix:` namespace
persists with no serializer of rekep's own. A `fields/` shard is an array; a
component, group or message document is one `Field`. A definition states its
members as reference occurrences, and those resolve once, when the folder is
read, into shared typed subtrees. `FixRegistry.write_into(location)` writes the
same layout back — never the runtime fields, which every registry already has —
which is how an edited dictionary becomes one a task can point at.

`fix_registry(location)` reads any such folder through `IOBase`, so a local
path, an `s3://` prefix or an open handle are the same call. A location holding
no specification fields is rejected rather than loaded, which is what stops a
pipeline from silently publishing a narrow table:

```python
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from rekep.fix import fix_registry, registry_path

assert registry_path().is_dir()
assert (registry_path() / "fields").is_dir()
assert (registry_path() / "provenance.json").is_file()

with TemporaryDirectory() as empty, pytest.raises(ValueError, match="no specification fields"):
    fix_registry(Path(empty))
```

Every registry — loaded, built or empty — starts with the twenty runtime fields
below, so `timestamp` and `sendersessionid` resolve before anything is
inserted. A stored dictionary never holds them and a read steps past a stored
copy.

## What a field carries

| property | API | meaning |
| --- | --- | --- |
| canonical id | `field.fix.id` | tag plus branch identity |
| canonical tag | `field.fix.tag` | integer protocol tag |
| alternate tags | `field.fix.tags` | historical or equivalent identifiers |
| storage name | `field.name` | folded canonical Arrow column name |
| display name | `field.display` | specification capitalization |
| aliases | `field.fix.aliases` | alternate folded name lookups |
| datatype | `field.dtype` | scalar, struct, or list storage shape |
| description | `field.fix.description` | specification meaning |
| code set | `field.fix["codes"]` | versioned wire value/name translations |
| lineage | `field.fix["lineage"]` | names and datatypes by FIX version |
| replacements | `field.fix["replacements"]` | what restates this field at the newest version |
| stated absences | `field.fix["nulls"]` | spellings that mean nothing was sent |

```python
import json

from rekep.fix import fix_registry

registry = fix_registry()
side = registry.field_by_tag(54)
codes = json.loads(side.fix["codes"])["codes"]

assert side.name == "side"
assert side.display == "Side"
assert side.fix.id == "54:"
assert str(side.dtype.into_arrow()) == "fixed_size_binary[4]"
assert codes[0]["value"] == "1"
assert codes[0]["name"] == "Buy"
assert codes[0]["since"] == "2.7"
```

## Lookup methods

Raising methods are useful when absence is an invalid contract; `get_*`
methods return `None` for exploratory code.

| strict | optional | key |
| --- | --- | --- |
| `field(key)` / `registry[key]` | `get_field(key)` / `get(key)` | integer tag or string name |
| `field_by_id(id)` | `get_field_by_id(id)` | exact branch-qualified identifier |
| `field_by_tag(tag)` | `get_field_by_tag(tag)` | canonical or alternate tag |
| `field_by_name(name, branch)` | `get_field_by_name(...)` | canonical name or alias |
| `field_by_path(path, branch)` | `get_field_by_path(...)` | nested component/group path |
| `definition(category, name, branch)` | `get_definition(...)` | one explicit category |
| `msgtype(spelling, branch)` | `get_msgtype(...)` | exact wire code, or a folded name |
| `group_by_counter(id)` | `get_group_by_counter(id)` | the group a counter numbers |

```python
from rekep.fix import fix_registry

registry = fix_registry()

assert registry[54] == registry.field_by_tag(54)
assert registry["SIDE"] == registry.field_by_name("side")
assert registry.get_field_by_name("not-a-field") is None
assert 55 in registry
assert "Symbol" in registry
assert registry.group_by_counter("453:").name == "parties"
assert str(registry.msgtype("D")) == "D"
assert registry.msgtype("D").field.name == "newordersingle"
```

Name lookup is ASCII case-insensitive and ignores the separators the FIX name
fold drops, so `Hedge_Currency`, `hedge-currency` and `HedgeCurrency` are one
key. Tag lookup never guesses from a textual name, and an exact id never falls
through to another branch. Names and aliases use separate indexes, and a stored
name is rechecked after hashing, so a digest collision cannot select an
unrelated field.

## Resolution tiers

For a message read on a named branch, a bare key resolves in this order:

1. the message branch;
2. the standard branch;
3. for capture-column fill only, any declared branch;
4. bridge capture aliases such as `seqNum` → `MsgSeqNum`.

Within one lookup kind, an omitted branch tries standard canonical keys, then
named-branch canonical keys, then standard alternates, then named-branch
alternates. A branch-qualified identifier bypasses every tier. Standard tags
remain standard even while a message is read under `ulbridge`; a named branch
may claim only the user-tag range, tags 5000 through 40000.

```python
from rekep.fix import STANDARD_BRANCH, USER_TAG_MAX, USER_TAG_MIN, fix_registry

registry = fix_registry()
standard = registry.field_by_name("MsgType", STANDARD_BRANCH)
bridge = registry.field_by_name("PriorityLevel", "ulbridge")

assert standard.fix.tag == 35
assert bridge.fix.branch == "ulbridge"
assert USER_TAG_MIN <= bridge.fix.tag <= USER_TAG_MAX
```

Which branch a row is read under is not only the codec's pin. A capture row
whose `pluginid` names a branch the registry declares — by name or by one of
its aliases — is read under that branch, outranking the pin; any other
`pluginid` leaves the pin standing. That is the whole rule, and
[Capture columns](capture.md#the-dialect-a-row-is-read-under) works it through
against the bundled bridge branch.

## Repeating groups and paths

A repeating group is two definitions, not one. The counter the wire carries is
a tagged `int32` scalar in `fields`; the occurrences are a named list in
`groups` whose `fix:counter` points back at that tag. Inspect the declaration
rather than reconstructing members from names:

```python
from rekep.fix import fix_registry

registry = fix_registry()
counter = registry.field_by_tag(453)
group = registry.definition("groups", "Parties")
members = [
    member.name
    for expanded in group.explode_fields()
    for member in expanded.unnest_fields()
]

assert counter.name == "nopartyids"
assert str(counter.dtype.into_arrow()) == "int32"
assert group.name == "parties"
assert group.fix["counter"] == "453"
assert members[:4] == ["partyid", "partyidsource", "partyrole", "partyrolequalifier"]
```

`field_by_path` walks component and group declarations and accepts either a
string or a resolved `FieldPath`. A schema has no positions to skip, so a path
through a group may omit the occurrence; `FixMsg.by_path` walks values instead
and therefore needs the occurrence when it enters a list.

```python
from rekep import FieldPath
from rekep.fix import fix_registry

registry = fix_registry()

assert registry.field_by_path("Parties.PartyID").fix.tag == 448
assert registry.field_by_path(FieldPath("Parties.PartyID")).fix.tag == 448
assert registry.field_by_path("Parties[0].PartyID").fix.tag == 448
```

## Code sets and version lineage

Code translation happens before datatype conversion. For `Side(54)`, both `1`
and `buy` resolve to the stored code `1`; for `MsgType(35)`,
`executionreport` resolves to `8`. A pinned version selects the code spellings
valid at that version without renaming the Arrow column.

A code set folds every listing from FIX 4.0 to 5.0 SP2, so a value an older
version declared and the newest dropped is still a code, dated `since` its
first listing and `deprecated` at the version after its last. A legacy name
that folds onto a current one takes the suffix `Legacy`; an older spelling of a
value the newest version keeps becomes one of its aliases.

Lineage explains how one field evolved while preserving one current identity:

```python
import json

from rekep.fix import fix_registry

registry = fix_registry()
lastqty = json.loads(registry.field_by_tag(32).fix["lineage"])["entries"]

assert lastqty[0]["since"] == "2.7"
assert lastqty[0]["name"] == "lastshares"
assert lastqty[-1]["name"] == "lastqty"
assert registry.field_by_name("LastShares").fix.tag == 32
```

| lineage entry key | meaning |
| --- | --- |
| `since` | numeric dotted version; entries run oldest first |
| `ep` | the extension pack that dated the change |
| `name`, `type`, `doc` | what the field was called and how it was typed from that point |
| `deprecated` | the specification deprecated the field from this version |
| `removed` | the specification stopped declaring it; the entry states no name or type |

A field FIX retired is still in the dictionary — `Rule80A(47)`,
`ExecBroker(76)`, `ClientID(109)` and the rest — because a capture holds what
was sent. `field_by_tag` answers at every version; the lineage is what says
which versions declared it.

## What restates a retired field

A field the specification replaced carries `fix:replacements`: the rule that
says what stands in for it at the newest version. The rule is metadata on the
field, so a registry edit is a rule edit, and `FixMsg.into_latest()` is what
applies it.

```python
import json

from rekep.fix import FixCodec, fix_registry

registry = fix_registry()
rules = json.loads(registry.field_by_tag(47).fix["replacements"])["replacements"]

assert rules[0]["since"] == "4.3"
assert rules[0]["when"] == "A"
assert rules[0]["fills"] == [{"tag": 528, "value": "A"}]

message = next(FixCodec(registry).parse_line(b"8=FIX.4.2|35=D|11=A|47=A|10=0|"))
latest = message.into_latest()

assert latest.by_tag(528).as_py() == "A"
assert latest.by_tag(47).as_py() == "A"
assert latest.into_bytes(ord("|")) == message.into_bytes(ord("|"))
```

Entries are read in document order and the first whose conditions hold wins, so
a catch-all entry stating no `when` comes last.

| entry key | required | meaning |
| --- | :---: | --- |
| `since` | yes | the version the specification replaced the feature at |
| `ep` | no | the extension pack that dated it |
| `msgtypes` | no | only when the root's `MsgType(35)` is one of these wire codes |
| `in` | no | only when the source sits inside one of these repeating groups |
| `when` | no | only for this held value; absent is any stated value |
| `fills` | yes | the fields that take a value, and what value |
| `doc` | no | the specification's own wording |

A fill states `tag` alone (the source's own value, re-typed), `tag` with
`value` (a constant), `tag` with `from` (another tag's stated value at the same
level), `tag` with `join` (two or more tags' wire texts concatenated), or
`group` with `members` (one occurrence of a repeating group whose members are
fills of their own). A fill is applied all-or-nothing: one target that cannot
take its value blocks the whole entry, and the arrival record never changes, so
a restated message re-emits the received line byte for byte.

## Runtime fields

Every registry starts with tags 65000–65019. They are generated by the codec
and are not stored in dictionary documents.

| tag | field | holds |
| ---: | --- | --- |
| 65000 | `msghash` | digest of what the message said, envelope excluded |
| 65001 | `version` | the FIX version it was read at |
| 65002 | `symbolticker` | one instrument symbol, the same across venues |
| 65003 | `timestamp` | the clock a capture is ordered by |
| 65004 | `unixpartition` | the partition that clock falls in, in whole seconds |
| 65005 | `parentclordid` | the client order id this order descends from |
| 65006 | `parentorderid` | the venue order id this order descends from |
| 65007 | `sendersessionid` | the session the message came from, as it states it |
| 65008 | `msgctxid` | the message context a bridge handled it in |
| 65009 | `pluginid` | the plugin that logged the line, and the dialect it is read under |
| 65010 | `prevpluginid` | the plugin it came through before that one |
| 65011 | `sendersessionname` | the name of the session it came from |
| 65012 | `targetsessionname` | the name of the session it went to |
| 65013 | `isincode` | the instrument's ISIN |
| 65014 | `miccode` | the market the message names |
| 65015 | `state` | the order's state |
| 65016 | `instid` | the instrument identity |
| 65017 | `id` | the message identity |
| 65018 | `persistentid` | the order-chain identity |
| 65019 | `targetsessionid` | the session the message went to, as it states it |

```python
from rekep.fix import fix_crate_fields

assert [field.fix.tag for field in fix_crate_fields()] == list(range(65000, 65020))
assert [field.name for field in fix_crate_fields()][8:11] == [
    "msgctxid",
    "pluginid",
    "prevpluginid",
]
```

The four that changed spelling in this release were plugin-shaped and are now
session-shaped: `sessionid` became `sendersessionid`, `senderpluginid` became
`pluginid`, `targetpluginid` became `prevpluginid`, and the two plugin-session
columns became `sendersessionname` and `targetsessionname`. `targetsessionid`
at 65019 is new. The tags did not move, so a stored row keeps its meaning; the
column names in `fix.messages` did.

## Adding to a dictionary

Two families of mutation, and the difference between them is what happens when
the definition is already there.

| verb | contract |
| --- | --- |
| `add_field(field)` | inserts where the identity is free; answers `False` rather than raising when it is taken |
| `add_definition(category, field)` | the same, for one of the three named categories |
| `add_fields(fields)` | folds a whole iterable; answers `(added, merged)` |
| `merge_with(other)` | folds another registry and its branch declarations |
| `insert(field)` | inserts or replaces one tagged scalar; answers the replaced field |
| `update(field)` | merges metadata into the stored identity, per key |
| `create_definition(category, field)` | refuses an existing canonical name or identifier |
| `insert_definition(category, field)` | inserts or replaces one complete definition |
| `update_definition(category, field)` | replaces an existing definition in full |
| `remove(key)` / `remove_definition(...)` | removes one; refuses while something still references it |

The lenient verbs are for folding a venue's vocabulary into a dictionary that
may already know some of it; the strict ones are for a build that must fail
when a name collides. A refusal leaves every category and index unchanged.

```python
from rekep import Field
from rekep.fix import FixRegistry

registry = FixRegistry.from_fields([])
venue = Field("VenueRank", "int32")
venue.fix.tag = 20100
venue.fix.branch = "venue"

assert registry.add_field(venue) is True
assert registry.add_field(venue) is False
assert registry.field_by_name("VenueRank", "venue").fix.tag == 20100
```

`update` merges rather than replaces: `fix:tag` and `fix:branch` must agree,
alternate tags and aliases combine, lineage merges by pedigree and code sets by
wire value with the incoming entry winning, and `fix:replacements` wins whole
because two documents have no order between them. A key only the stored field
declares is kept.

## Iterate, filter, and export

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from rekep.fix import fix_registry

registry = fix_registry()
groups = [field.name for field in registry.definitions("groups")]
priced = [
    field
    for field in registry
    if "price" in (field.fix.description or "").casefold()
]

with TemporaryDirectory() as scratch:
    registry.write_into(Path(scratch) / "fix")
    assert (Path(scratch) / "fix" / "fields").is_dir()

assert "parties" in groups
assert priced
```

An explicit registry loaded with `fix_registry(path_or_uri)` receives the same
runtime and bridge fields as the bundled one.

## Browser

Search the same dictionary here — its 6,303 tagged scalars and 580 repeating
groups — by tag, name, alias or description:

<div data-fix="registry"></div>

The [registry browser](../tools/fix-registry.md) is the Marimo tool over the
same dictionary; it exposes branches, groups, members, code sets, lineage, raw
metadata, Field JSON, and the complete `FixMsg` schema. Both read generated
assets that are read-only projections of this same bundled registry.
