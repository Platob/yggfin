# FIX

`rekep.fix` parses wire or rendered FIX while preserving repeated fields and
unknown source data.

```python
from rekep.fix import FixMessage, FixRegistry

message = FixMessage.from_text("8=FIX.4.4|35=D|11=C1|55=IBM|10=001")
message.msg_type  # 'D'
message.get(55)  # 'IBM'
message.pairs  # ordered entries

registry = FixRegistry(offline=True)
registry.field("OrigClOrdID", "4.4")
```

The separator is detected from the message. SOH, pipe, caret forms, and
rendered `Name=Value` logs use the same ordered representation. Vectorized
Arrow helpers split whole columns and resolve distinct rendered names once.

## Version selection

Normal parsing has no default FIX version. Each message first inspects
`BeginString <8>` and, for FIXT, `ApplVerID <1128>` then
`DefaultApplVerID <1137>`. A known application version selects its prepared
registry index. If the evidence is absent, inconsistent, or unavailable, the
message remains valid raw FIX with ordered numeric tags; parsing never silently
chooses 4.2, 4.4, or the newest registry. Direct registry and codec callers may
still supply an explicit version.

## Registry

The reviewable `data/fix/` directory and `data/fix.zip` archive contain the
same registry. It combines OnixS field definitions with QuickFIX symbols,
components and headers. Resource locations use the project's URL resolver and
`pyarrow.fs`; a remote resource is materialized only when a downstream parser
requires an OS-local path, then reused from the local cache. Resolution and
network work happen before the message loop.

The registry supplies:

- canonical name and numeric tag;
- FIX datatype and Arrow projection;
- description, valid values, and component/message usage;
- explicit-version and inferred-version lookup through cached indexes.

### One file per identity

A store holds one document per field or component *identity*, not one per FIX
version:

```text
versions.json           the version list, each version's session layer,
                        and which versions have had their spec read
fields/party_role.json  one field, and every version's reading of it
components/parties.json one component, and every version's member tree
```

JSON, and measured: the dictionary is seven thousand documents and every
process importing this package parses a projection of it, where pure-Python
YAML costs 25 seconds to read against a tenth of one for JSON. A store
somebody wrote in YAML still reads, and converts itself the first time
anything rewrites it.

A field's identity is its **tag**, never its name. Tag 64 is `FutSettDate`
through 4.3 and `SettlDate` after, so `fields/settl_date.json` is one file
saying in passing that four older versions spelled it differently -- rather
than two half-histories nobody diffs. Each version's variant states only what
it does not share with the identity.

A field FIX never numbered -- a bridge's rendered `ISINCODE`, a vendor's
`TECH.CLIENTID` -- is the same document with `kind: namespace`, no tag, and a
`*` variant that holds whichever version the session negotiated. One naming
`fix:column` is lifted into that column of the parsed log.

Stores written one file per version keep working: which layout a store is in
is read off what it holds, and `rekep fix registry migrate` is how one
changes, checked field by field against what it used to answer.

### Resolving a name

`registry.resolve(name)` walks three tiers, in order, and stops at the first
that answers:

1. an identity's canonical name;
2. a name some version spells for it;
3. a declared alias -- a rendered or namespaced spelling, a legacy name, a near
   miss confirmed against a capture.

Matching folds **case and nothing else**. A separator is part of a name, so
`party_role` is a spelling of its own rather than a second way of writing
`PartyRole` -- dropping separators merged identities a store deliberately
holds apart, and a match a registry cannot tell from a real collision is worse
than a miss. A real renderer spelling is recorded as an alias, which is what
`rekep fix classify --report` finds and `rekep fix apply --aliases` writes.

A later tier never takes a name from an earlier one. Two identities claiming
one name inside a tier, an alias an earlier tier already answers for, two
identities claiming one **tag**, and two claiming one canonical name are all
defects `registry.check()` reports and every write refuses -- with the
conflicting names in the message.

### Editing and refreshing

```bash
rekep fix registry add-field --store data/fix --name TECH.CLIENTID \
    --type String --column tech_client_id
rekep fix registry alias-field --store data/fix --name PartyRole \
    --alias PARTYROLLE --source brk --occurrences 41
rekep fix registry check --store data/fix
```

Each verb schema-checks the change, re-runs the collision check against the
whole store, and refuses the write rather than leaving it half applied.

The same verbs run at a prompt, which is what a person editing more than one
field wants:

```bash
rekep fix shell --store data/fix
```

`find`, `show`, `component` and `check` read; `add`, `edit`, `alias` and
`remove` write. A change is built one answered question at a time, offers the
stored value as each default, shows the whole entry back, and is written only
after a yes -- through the same `FixRegistry` verbs, never a second
implementation of them.

`FixRegistry(cache_ttl=seconds)` regenerates a store older than the TTL from
the QuickFIX spec before serving it. The default, `0`, never refetches. A
refetch that fails is reported and the local copy served anyway: a dictionary
a day stale parses every message, and one that raises parses none.

### Merged views

`merged_fields()` and `merged_components()` hand over the whole unified table
in one call, where `scalar()` answers one key at a time. A merged component is
an entry rather than one tree: `paths(version)`, `delimiters(version)` and
`diff()` are the questions worth asking of it.

Protocol-specific code should normalize values, not duplicate registry tables.

### What the wheel carries

`data/fix.zip` is the whole dictionary and stays beside the repository. The
wheel ships `rekep/fix/registry.zip`, a projection of it holding the keys
`rekep.fix.publish.PROJECTED` names -- numbered tags, the fields FIX never
numbered that the log gives a column, and every version's component
declarations, whole. A component says where a repeating group starts and ends,
so a projection that selected its members alongside the fields would end the
group somewhere else, and one that dropped them extracts no group at all.

`components()` answers `[]` twice over: for a version whose spec declares none
-- nothing before 4.3 has a component -- and for a store written before any
were kept. `components_available()` is what tells them apart, and the second
case warns rather than quietly extracting nothing.

Rebuild the projection after refreshing the dictionary:

```bash
cd python
uv run python -c "from rekep.fix.publish import publish_builtin; \
publish_builtin('../data/fix.zip', 'src/rekep/fix/registry.zip')"
```

## Classifying a capture's key names

About half the key occurrences in a bridge capture match nothing the registry
has, and that is not one problem. `rekep fix classify` counts every distinct
key name a capture spells -- streamed, names and counts only, never a value --
and says which of four things each one is:

| kind | what it means | what closes it |
| --- | --- | --- |
| `exact` | the dictionary has this name | nothing |
| `aliased` | a spelling already recorded against a field | nothing |
| `near` | one or two edits from a known name | record an alias, with its count |
| `namespace` | a name FIX never numbered | declare the field |

```bash
rekep fix classify --source /captures/brk --store data/fix \
    --plugins '^UL' --report brk.json
rekep fix apply --store data/fix --report brk.json --aliases --minimum 50
```

Nothing is applied unless asked for, and a near miss never silently: a case or
spelling variant is *evidence* that two names are one field, and the point of
separating it from an exact match is that somebody decides. What `apply` does
is make that decision one command instead of a thousand file edits -- and the
alias it writes carries the capture and the count that earned it.

Two readings the classification depends on:

- A dotted key is a component path when the segment *nearest the name* --
  subscript dropped -- names something the dictionary has, and a namespace
  otherwise. That segment is the one saying what the field is a member of;
  anything further out only says where the container came from. So
  `NoPartyIDs[0].PartyID` is `PartyID` inside a group, and `TECH.CLIENTID` is a
  vendor's own field rather than `ClientID <109>` with a prefix.
- `#Foo` and `Foo` are counted apart. The two namespaces a bridge writes are
  asymmetric -- some names only ever marked, some only ever bare, a few both --
  and a count that summed them would say nothing about which.

## Groups and components

Repeating groups remain ordered entries. Structured components are extracted
into typed lists; each entry retains an ordered string buffer for members
absent from the projected shape.

The extraction is driven by the component declaration and by nothing else:
which tag counts the entries, which tag opens one, which tags may belong to
one and which group each sits inside all come out of the tree. A
`ComponentGroup` subclass adds only the shape:

```python
class TrdRegTimestamps(ComponentGroup):
    component = "TrdRegTimestamps"
    group = "NoTrdRegTimestamps"

    @classmethod
    def into_row(cls):
        return TrdRegTimestamp

    @classmethod
    def into_projection(cls):
        return (
            ("trd_reg_timestamp", "TrdRegTimestamp"),
            ("trd_reg_timestamp_type", "TrdRegTimestampType"),
            ("trd_reg_timestamp_origin", "TrdRegTimestampOrigin"),
        )
```

The parsed log carries two of them, `parties` and `trd_reg_timestamps`.
`FixCodec.COMPONENTS` maps each column to its extractor and applies them in
order against what the last one left, so a member lifted into one component's
entries cannot also be lifted into another's.

The delimiter leads the projection because it is what opens an entry. Every
member is lifted only where its value is one the column's type can hold, and
falls to `buffer` where it is not -- so a malformed `UTCTimestamp` is kept as
the text that arrived rather than becoming a null nobody can explain.

`NoSecurityAltID` and `NoTradingSessions` split exactly as `NoPartyIDs` does;
reaching a column of their own is a change to the log's schema and a
twenty-line subclass, not a change to the extraction.

### What a component requires

The spec states `required` for every member, and
`FixRegistry.component_field(name, version)` reads it: a member a message must
carry is a non-nullable column, one it may omit is nullable, a repeating group
is a list whose entries are never null, and a referenced component is inlined
where it sits. That is the declaration a projected shape is checked against.

## Nested payloads

`XmlData <213>` is an XML data stream in the standard and a `key=value`
message in real bridge traffic. A payload that reads as pairs becomes pairs,
under `XmlData.<key>`, in the place the tag sat -- so `XmlData.ClOrdID`
resolves like the rendered `NoPartyIDs.PartyID` already does, and lands in the
column its name earns. A payload that opens an XML tag, or that carries only
one pair, stays exactly as it was.

The payload is read under its own separator, detected per row: it sits inside
a token of the message around it and so cannot be written with that message's
separator.

## Repeated readings

A rendered line carries two namespaces -- `#Side` as a field arrived and
`Side` after enrichment -- and on a third to a half of a real capture's lines
some fields appear in both. A field is lifted into its column when every
reading of it in that row agrees, and every copy leaves the residual pairs
with it. Readings that *disagree* are left where they were: two values under
one key is a repeating group, or an enrichment that rewrote something, and
picking between them would be a guess.

## Parsed-log projection

Common fields are promoted once into `Log`. Residual `kwargs` keeps everything
not promoted -- resolved or not, in wire order, with the tag, the name, the
container or namespace it sat in, and what its value means. A later market
conversion rebuilds a `FixEvents` view from those typed columns and that one
residual list instead of tokenizing the raw message again.

## Benchmark

`python/benchmarks/bench_fix.py --quick` verifies and measures scalar versus
column parsing, tag projection, groups, and parsed-log reconstruction.
