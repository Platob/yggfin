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
versions.json          the version list, each version's session layer,
                       and which versions have had their spec read
fields/party_role.json one field, and every version's reading of it
components/parties.json one component, and every version's member tree
```

A field's identity is its **tag**, never its name. Tag 64 is `FutSettDate`
through 4.3 and `SettlDate` after, so `fields/settl_date.json` is one file
saying in passing that four older versions spelled it differently -- rather
than two half-histories nobody diffs. Each version's variant states only what
it does not share with the identity.

A field FIX never numbered -- a bridge's rendered `ISINCODE`, a vendor's
`TECH.CLIENTID` -- is the same document with `kind: vendor`, no tag, and a
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
3. a declared alias -- a rendered or vendor spelling, a legacy name, a near
   miss confirmed against a capture.

A later tier never takes a name from an earlier one. Two identities claiming
one name inside a tier, or an alias an earlier tier already answers for, are
defects `registry.check()` reports and the CRUD verbs refuse.

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
| `vendor` | a name FIX never numbered | declare the field |

```bash
rekep fix classify --source /captures/brk --store data/fix \
    --drivers '^UL' --report brk.json
rekep fix apply --store data/fix --report brk.json --aliases --minimum 50
```

Nothing is applied unless asked for, and a near miss never silently: a case or
spelling variant is *evidence* that two names are one field, and the point of
separating it from an exact match is that somebody decides. What `apply` does
is make that decision one command instead of a thousand file edits -- and the
alias it writes carries the capture and the count that earned it.

Two readings the classification depends on:

- A dotted key is a component path when every segment in front of the last one
  names something the dictionary has, and a vendor namespace otherwise. So
  `NoPartyIDs.PartyID` is `PartyID` inside a group, and `TECH.CLIENTID` is a
  vendor's own field rather than `ClientID <109>` with a prefix.
- `#Foo` and `Foo` are counted apart. The two namespaces a bridge writes are
  asymmetric -- some names only ever marked, some only ever bare, a few both --
  and a count that summed them would say nothing about which.

## Groups and components

Repeating groups remain ordered entries. Known components such as Parties are
extracted into typed lists; each Party retains an ordered string buffer for
fields absent from the current model.

The extraction is driven by the component declaration and by nothing else:
which tag counts the entries, which tag opens one, which tags may belong to
one and which group each sits inside all come out of the tree. Naming another
component and its group is the whole of what makes it another group's
extractor --

```python
Parties(
    components=registry.components("4.4"),
    component="TrdRegTimestamps",
    group="NoTrdRegTimestamps",
)
```

-- and `NoTrdRegTimestamps`, `NoSecurityAltID` and `NoTradingSessions` split
exactly as `NoPartyIDs` does. What remains specific to Parties is the *shape*
it projects into: `Party`, and the parsed log's `parties` column. Another
group reaching a column of its own is a change to the log's schema, not to the
extraction.

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

Common fields are promoted once into `Log`. Residual `fix_tags`, `keyval`, and
`fix_miss_tags` keep everything not promoted. A later market conversion rebuilds
a `FixEvents` view from those typed columns and residual pairs instead of
tokenizing the raw message again.

## Benchmark

`python/benchmarks/bench_fix.py --quick` verifies and measures scalar versus
column parsing, tag projection, groups, and parsed-log reconstruction.
