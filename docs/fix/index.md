# FIX

`rekep.fix` parses wire or rendered FIX while preserving repeated fields and
unknown source data.

```python
from rekep import FixMsg
from rekep.fix import FixRegistry

message = FixMsg.from_text("8=FIX.4.4|35=D|11=C1|55=IBM|10=001")
print(message.get(35).raw, message.get(55).raw)

registry = FixRegistry.from_builtin()
print(registry.field("OrigClOrdID", "4.4").fix.tag)
```

```text
D IBM
41
```

The separator is detected from the message: the standard's SOH (`\x01`)
first, then the substitutions tools write in its place -- `|`, EOT/ETX
(`\x04\x03`), `^A`, `^`, `;` -- in that order, so a multi-character candidate
is tried before anything it contains. Rendered `#Name=Value` bridge logs use
the same ordered representation. Vectorized Arrow helpers split whole columns
and resolve distinct rendered names once.

## Version selection

Normal parsing has no default FIX version. Each message first inspects
`BeginString <8>` and, for FIXT, `ApplVerID <1128>` then
`DefaultApplVerID <1137>`; a known application version selects its prepared
registry index.

If the evidence is absent, inconsistent, or unavailable, the message remains
valid raw FIX with ordered numeric tags: parsing never silently chooses 4.2,
4.4, or the newest registry. Direct registry and codec callers may still
supply an explicit version.

## Registry

The reviewable `data/fix/` directory and `data/fix.zip` archive hold the same
registry. Nanoconda leads field and value names, OnixS fills missing prose and
usage, and QuickFIX supplies machine declarations, components and headers.
Resource locations use the project's URL resolver and
`pyarrow.fs`; a remote resource is materialized only when a downstream parser
requires an OS-local path, then reused from cache -- all before the loop.

The registry supplies:

[Browse every component and field](registry.md) with full-text and
categorical filters, linked references, and repository source records.

- canonical name and numeric tag;
- FIX datatype and Arrow projection;
- description, valid values, and component/message usage;
- explicit-version and inferred-version lookup through cached indexes.

### Every point in time is a timestamp

Every width the standard writes lands on one Arrow type:

```python
from rekep.fix import FixRegistry

registry = FixRegistry.from_builtin()
for name in ("OrigTime", "TransactTime", "MDEntryDate", "MDEntryTime", "MaturityMonthYear"):
    field = registry.field(name, "4.4")
    print(f"{field.fix.type:14} {field.dtype}")
```

```text
UTCTimestamp   timestamp[us, tz=UTC]
UTCTimestamp   timestamp[us]
UTCDateOnly    timestamp[us]
UTCTimeOnly    timestamp[us]
MonthYear      string
```

A date is midnight, a time-of-day is that clock on the epoch's day, and a
zoned spelling is the instant its offset names. The reader already normalised
all three to the same epoch nanoseconds -- only the projection was throwing
the difference away, and a `date32` column is the one shape a timezone can no
longer be applied to.

The registry keeps the zone when the field's own description explicitly says
UTC, as `OrigTime <42>` does. The parsed-log projection also applies what the
FIX datatype establishes: a UTC datatype, or one whose value carries its UTC
offset, lands in `timestamp[us, tz=UTC]`. A `LocalMktDate` is a wall clock in a
place the message never names, so its column stays naive rather than claiming
a zone it does not have.

```python
from rekep.market import Execution, Instrument

print(Instrument.into_field().field("maturitydate").dtype)
print(Execution.into_field().field("settldate").dtype)
```

```text
timestamp[us]
timestamp[us]
```

`MonthYear` is the deliberate exception and stays text: `202608` is a month
and `202608w2` a week, and neither is an instant.

### One record per identity, sharded by tag

A field's reading is cross-version by nature -- one tag, one meaning, and a set
of versions that declare it -- so a store holds one *record* per identity and
not one per version:

```json
{"54": {"name": "Side", "type": "string", "nullable": true,
        "description": "Side of order.",
        "fix": {"tag": "54", "type": "char",
                "versions": ["4.4", "4.3", "4.2"],
                "values": [{"value": "1", "meaning": "Buy",
                            "aliases": ["Buy"]}]}}}
```

A record is a `Field` document -- the Arrow reading at the top, the protocol's
own keys under `fix`, with JSON objects and arrays left nested for review.
Loading the record encodes those values back into Arrow's bytes-to-bytes
metadata. A component, a message and every file in `schemas/rekep/` use the
same `Field` document shape.

Having a tag is the whole of being a standard field, so nothing states the
kind beside it. One enumerated value is one record -- what the wire carries,
what it means, and every other spelling naming it -- and the lookups a parse
needs are derived from it, never stored beside it.

Records live in tag-range shards of one thousand, named by the shard index:

```text
versions.json         the version list, each version's session layer,
                      and which versions have had their spec read
fields/000000.json    tags 0-999
fields/000030.json    tags 30000-30999, rekep package vocabulary
fields/000040.json    tags 40000-40999, the 5.0.SP2 extension pack
fields/999999.json    the fields FIX never numbered
components/parties.json          one component
components/new_order_single.json a message
```

The document holding a tag is `tag // 1000` -- arithmetic, so there is no index,
no lookup table and no scan, and `registry.lookup(54)` deserializes one shard
rather than the dictionary. The tag space is sparse, and an empty shard is
simply absent: ten files hold the populated ranges and named fields.

JSON, and measured: every process importing this package parses a projection of
the dictionary, where pure-Python YAML costs 25 seconds to read against a tenth
of one for JSON. One spelling, so a read is one open — a declaration a person
hands to `rekep fix add-field` may still be YAML, which is a courtesy at the
edge and not a second store format.

A field's identity is its **tag**, never its name. Tag 64 is `FutSettDate`
through 4.3 and `SettlDate` after, so one record is tag 64 named `SettlDate`
with `FutSettDate` recorded as an alias carrying the version that spelled it --
rather than two half-histories nobody diffs.

A field FIX never numbered -- a bridge's rendered `ISINCODE`, a vendor's
`TECH.CLIENTID` -- is the same record with no tag and `*` for its versions.
It keys by its name rather than by a tag, so it lands in `999999`, the one
shard index the arithmetic never reaches: the same naming rule as every other
document, not a document of another kind. Having no tag is the whole of being
one, so no record states it twice. One naming `fix:column` is lifted into that
column of the parsed log.

### The collapse, and what it costs

Where two versions disagree, the newest one wins. `FIXT1.1` is excluded from
that walk for an application field: it is the session transport, and letting it
win would give a session-layer reading to fields it merely carries.

Enumerated values are the *union* across versions with the newest winning per
value, so a value that only ever existed in 4.2 still parses. Each half of a
value collapses on its own: a version that lists a value without writing it up
still names it, so its silence does not erase the prose another version had.

Every reading a collapse drops is written to `data/fix-conflicts.json`: the
field, its tag, the part, the readings it saw with their versions and which one
it kept. The report is a list somebody can read; a silent drop is not.

The counts are pinned in `rekep.fix.publish.CONFLICT_BASELINE` and a rebuild
that grows past them fails.

### Value codecs

`encode` maps a value spelled as text to the wire value it names, so
`TrdRegTimestampType=OrderSubmissionTime` resolves to `10`. Each raw value
maps to itself, so a caller has one lookup path and not two.

It is derived from every spelling a value carries -- the prose, the aliases
and the value itself -- normalized by casefold and then by dropping every
character outside `[a-z0-9]`, which is what makes `ORDER_SUBMISSION_TIME` and
`Order Submission Time` one key where plain lowercasing leaves two. Cached and
never stored: it was three hundred kilobytes of the published dictionary
saying nothing the values did not already say. Recording an estate's own
spelling is one more alias on the value it names.

`decode` maps a wire value to the leading symbolic spelling the registry
decided, while `meaning` returns its prose. Both fall through safely when a
field declares no transformation. `arrow_encode` and `arrow_decode` apply the
same maps to columns and return an identity column untouched.

Two values that normalize alike emit neither key: an ambiguous encoding that
silently picks one is worse than none, and the lookup falls through to the raw
value. The dropped keys are counted with the conflict report.

```python
import pyarrow

field = registry.resolve("Side")
assert field.fix.encode("Buy") == "1"
assert field.fix.decode("1") == "Buy"
assert field.fix.meaning("1") == "Buy"
print(field.fix.arrow_encode(pyarrow.array(["Buy", "2"])).to_pylist())
```

```text
['1', '2']
```

### Resolving a name

`registry.resolve(name)` walks two tiers, in order, and stops at the first that
answers:

1. an identity's canonical name;
2. a declared alias -- a rendered or namespaced spelling, the name an older
   version gave the tag, a near miss confirmed against a capture.

Matching folds to lowercase letters and digits, so `PartyRole`, `party_role`
and `PARTY-ROLE` are one name. `registry.check()` refuses two identities whose
canonical names or aliases meet under that fold.

A real renderer spelling is recorded as an alias, which is what
`rekep fix classify --report` finds and `rekep fix apply --aliases` writes.

A later tier never takes a name from an earlier one. Two identities claiming
one name inside a tier, an alias an earlier tier already answers for, two
identities claiming one **tag**, and two claiming one canonical name are all
defects `registry.check()` reports and every write refuses -- with the
conflicting names in the message.

An older version's spelling that another identity already claims as its
canonical name cannot become an alias, and stays in the conflict report as the
dropped reading it is.

### Editing and refreshing

The [registry CLI](shell.md) owns the complete command workflow. Its two
surfaces share the same validated registry operations:

```bash
rekep fix registry show --store data/fix 35
rekep fix registry check --store data/fix
rekep fix shell --store data/fix
```

Registry reads emit JSON to `stdout`; status and failures stay on `stderr`.
The shell writes its interface to `stderr` and bounds long previews. Every
proposed write is shown and requires confirmation.

### Where a registry gets its dictionary

Naming a `cache_dir` is the whole of saying where the answers come from, so a
registry given one **serves that store and never reaches a source** — warm or
cold, and whether the store is a directory, a zip, or a location on an object
store. Nothing has to ask for that; it is what pointing at a store means.

```python
from rekep.fix import FixRegistry

print(FixRegistry(cache_dir="data/fix.zip").offline)
```

```text
True
```

The default store, `~/.config/fix`, is the one nobody chose, and the only one
that fills itself. A **fetch verb** opens the door explicitly everywhere else:
`FixRegistry.scrape()`, `rebuild()`, `fields(version, refresh=True)`, or a
`cache_ttl` that has come due.

`cache_dir` accepts any location `Url` resolves. A directory is read where it
is, local or remote. An archive on a filesystem this process cannot open a
file on — `s3://bucket/fix/fix.zip` — is copied down once into
`~/.config/fix-remote` and read from there, because a zip is read by seeking
and seeking over an object store reads it whole for every lookup. The copy is
reused by identity and size, so the next process on the same machine does not
fetch it again.

```yaml
# tasks/parse_fix/parse_fix.yml — every task takes the same value
fix_dictionary: s3://example-bucket/rekep/fix.zip
```

### The default every unconfigured lookup resolves through

`FixRegistry.from_builtin()` is what `FixMsg().registry`, `FixCodec()`,
`MarketTags.of()` and `FieldAccess.of()` answer through when nothing named a
dictionary. `set_builtin` moves it, once, at startup:

```python
from rekep.fix import FixRegistry

FixRegistry.set_builtin(FixRegistry(cache_dir="data/fix.zip"))
```

It refuses a registry missing rekep's own 26 identities, and it
does **not** move the published Arrow contracts: `rekep.fix.columns` reads the
default while its module body runs, and those declarations are what
`schemas/rekep/*.yaml` publishes. `set_builtin(None)` restores the packaged
projection.

### Bootstrapping the default store

A registry resolves where its dictionary comes from **once, at construction**,
and never on a miss -- a parse that meets its first bridge line must not answer
it by starting a fourteen-thousand-page scrape in the middle of a batch.

| what it finds | what it does |
| --- | --- |
| a store at `cache_dir` | serves it, silently, and never reaches a source |
| configured registry URL | validates and atomically expands the full archive |
| archive unavailable | fetches the configured sources in priority order |
| no URL, no store | fetches the configured sources in priority order |
| no store, the fetch failed | serves the packaged projection and says the registry is reduced |

Only the *default* store (`~/.config/fix`) is bootstrapped. A `cache_dir`
somebody named is that store, cold or not: it is about to be written, or it is
a projection that is complete for what it projects.

Both channels carry the lines. `warnings.warn` is the record -- filterable, and
shown once, which is why it is not the only one -- and `announce` is the
foreground writer a person waiting on a multi-hour fetch reads; it defaults to
`stderr`, and the CLI and the notebooks pass their own.

The start line says what was not found and where it looked, what is being
fetched from `DEFAULT_SOURCES`, roughly how many pages and how
long, where it installs, and how to skip it. The finish line says what was
written and how long it took.

```bash
export REKEP_FIX_REGISTRY_URL="https://artifactory.example/artifactory/rekep/fix-registry.zip"
export REKEP_FIX_REGISTRY_TOKEN="$ARTIFACTORY_READ_TOKEN"
```

```python
from rekep.fix import FixRegistry

registry = FixRegistry()  # installs into ~/.config/fix only when it is absent
```

The token is optional, HTTPS-only, and never serialised. Registry URLs cannot
carry user information or a query. The compressed download and expanded ZIP
are bounded. Every index, field and component is validated before the staged
directory is renamed, so a failed or concurrent install never becomes a cache.

```bash
rekep fix registry scrape --output data/fix
```

```python
from rekep.fix import FixRegistry

registry = FixRegistry.scrape("data/fix")
```

The scrape is staged, validated, and then replaces the local directory in one
rename. Source URLs and request limits are optional CLI flags.

### Publishing package and registry

`.github/workflows/release.yml` runs for a published GitHub release or a manual
dispatch. Configure its `artifactory` environment:

| setting | value |
| --- | --- |
| variable `ARTIFACTORY_PYPI_URL` | `https://host/artifactory/api/pypi/python-local` |
| variable `ARTIFACTORY_PYPI_CHECK_URL` | `https://host/artifactory/api/pypi/python-local/simple` |
| variable `REKEP_FIX_REGISTRY_URL` | full Generic-repository target URL |
| secret `ARTIFACTORY_TOKEN` | token with deploy permission |
| secret `ARTIFACTORY_USERNAME` | optional identity; leave empty for JWT token authentication |

The job builds both distributions and a deterministic registry from
`data/fix/`. `uv publish` checks the simple index so an identical package is
skipped on a rerun. The registry is uploaded second with its SHA-256 checksum.
Consumers use its target URL as their cold-cache fallback.

`FixRegistry(cache_ttl=seconds)` regenerates a store older than the TTL from
the configured registry sources before serving it. The default, `0`, never refetches. A
refetch that fails is reported and the local copy served anyway: a dictionary
a day stale parses every message, and one that raises parses none.

### Merged views

`merged_fields()` and `component_records()` hand over the whole unified table
in one call, where `scalar()` answers one key at a time. A component record is
one record rather than one tree: `paths()` and `delimiters()` are the questions
worth asking of it.

Protocol-specific code should normalize values, not duplicate registry tables.

### What the wheel carries

`data/fix.zip` is the whole dictionary plus rekep's 35 frozen fields and stays
beside the repository. The wheel ships `rekep/fix/registry.zip`: the standard
keys `rekep.fix.publish.PROJECTED` names, those same package fields, and every
version's declarations, messages included, whole.

Package fields have bare canonical names. `MarketEventType <30002>` avoids the
standard `EventType <865>`; the six package message declarations keep their
`REKEP.` namespace. `LastMkt <30>` and `SettlCurrency <120>` keep their standard
FIX identities while storing packed `MIC` and `Currency` codes. Tags 30018,
30022, and 30023 are unassigned because the components reference `LastMkt`,
`Price <44>`, and `LastQty <32>` directly.

A component says where a repeating group starts and ends, so a projection that
selected its members alongside the fields would end the group somewhere else,
and one that dropped them extracts no group at all. The messages travel for
their own reason: a wheel that could not say what a `D` is would send every
reader after the full dictionary.

`components()` answers `[]` both when a version declares no reusable block and
when its declarations are absent. `components_available()` distinguishes the
two states.

It hands back the components a version declares **and** the ones their trees
reference: a record keeps the newest member tree, so 4.3's `Parties` is now
the tree that reaches `PartySubID` through `PtysSubGrp` rather than naming it
directly, and a reader without `PtysSubGrp` would split the group elsewhere.

### A message is a component

Both spec sections are read into one folder of records, because the only
difference between the two declarations is that a message carries the code it
arrives under. So `merged_component()` answers for a name, one of its aliases,
or a MsgType -- `merged_component("D")` and `merged_component("NewOrderSingle")`
are the same record -- and `message_records()` is the whole `{MsgType: record}`
index, newest declaration winning a code two names claim (`J` is `Allocation`
through 4.2 and `AllocationInstruction` after).

```python
single = registry.merged_component("D")

print(single.msg_type, [member.name for member in single.members][:3])
print(len(registry.component_field("D", "4.4").fields), "columns")
```

```text
D ['ClOrdID', 'OrderRequestID', 'SecondaryClOrdID']
474 columns
```

A reusable block omits `fix:msgtype` rather than writing it null, and carries
`fix:msgtypes` instead: the messages whose trees reach it, derived on the
collapse exactly as a field's own `fix:msgtypes` is scraped. `Parties` names the
ninety-odd messages that carry it; six blocks name none, because the standard
reaches them from the session header (`HopGrp`, `MsgTypeGrp`) or no longer
reaches them at all.

### One shape for a field, a group and a message

A component, a message type and a repeating group are the same thing here: a
`Field`. A block is a **struct** of its members, a repeating group is a
**list** of the entry it repeats, and a member that defers to another block is
a struct with no members yet and that block's name in `fix:component`.

```json
{"name": "Parties", "type": "struct", "fix": {"component": "Parties"},
 "fields": [{"name": "NoPartyIDs", "type": "list", "fix": {"tag": "453"},
             "item": {"type": "struct", "fields": [
               {"name": "PartyID", "type": "string", "fix": {"tag": "448"}}]}}]}
```

So a component file reads like a contract file, because it is one -- the same
document `Field.into_dict()` writes for `schemas/rekep/*.yaml` -- and there is
no second tree to keep in step with the first. FIX's own names are what the
declaration says; the Arrow projection folds them when it builds columns, and
records each one under `fix:name` so the spelling survives the fold.
Whether a member is required is its nullability, which is the same fact under
the name the rest of the package already uses for it.

A reference is **not** expanded where it is stored. Expanding the published
dictionary in place turns 3,229 members into 120,241 -- `Instrument` alone is
referenced twenty-two times -- so the reference stays, and whoever reads it
expands it. `into_field` does exactly that when it projects to Arrow, because
that is where a referenced component's fields arrive on the wire.

### A component, materialised

Because the declaration already says every member's name, its Arrow type and
whether a message must carry it, there is nothing left to write by hand:
`component_scalar()` is `into_dataclass()` over the projection.

```python
Parties = registry.component_scalar("Parties", "4.4")
Parties(nopartyids=[Parties.PartyID(partyid="BUY-A", partyrole=3)])
```

Group entries are classes named after the entry a group repeats -- `NoPartyIDs`
yields `Parties.PartyID` -- they hang off the
class that declares them so a caller can build one, and a dictionary refresh
moves all of it. A column Python cannot spell keeps its own name: FIX tag 236
is `Yield`, and `yield` is a statement, so the attribute is `yield_` while the
column stays `yield` -- `into_field_columns()` is where the class says so, and
any hand-written `@scalar` class can declare it the same way.

The handful of components this package projects into **published** columns
keep their hand-written declarations. Those are a contract, and a contract
that changed shape whenever the dictionary was refreshed would not be one.

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

The report is JSON on stdout and JSON in the file, which is the one text form
anything here serialises to -- `jq` reads it, and `apply` reads the same
document back.

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
- Classification normalizes `#Foo` to `Foo` but retains separate marked and
  bare occurrence counters. Parsed entries receive the normalized spelling;
  the counters preserve how each bridge spelling arrived.

## Groups and components

Repeating groups remain ordered entries. Structured components are extracted
into typed lists carrying the members they declare; anything absent from the
projected shape stays in the row's residual `entries`, in wire order.

The extraction is driven by the component declaration and by nothing else:
which tag counts the entries, which tag opens one, which tags may belong to
one and which group each sits inside all come out of the tree. A
`ComponentGroup` subclass adds only the shape:

```python
import dataclasses
from functools import cache

from rekep.fix.components import ComponentGroup, TrdRegTimestamp


@dataclasses.dataclass(eq=False)
class TrdRegTimestamps(ComponentGroup):
    component: str = "TrdRegTimestamps"
    group: str = "NoTrdRegTimestamps"

    @classmethod
    @cache
    def into_row(cls) -> type:
        return TrdRegTimestamp

    @classmethod
    @cache
    def into_projection(cls) -> tuple[tuple[str, str], ...]:
        return (
            ("TrdRegTimestamp", "TrdRegTimestamp"),
            ("TrdRegTimestampType", "TrdRegTimestampType"),
            ("TrdRegTimestampOrigin", "TrdRegTimestampOrigin"),
        )
```

The parsed message carries `Parties`, `TrdRegTimestamps`, `SideTrdRegTS`,
`SecurityAltID`, and `Legs`.

The two instrument groups are *scoped*: the dictionary nests the instrument
inside market-data and quote entries, so an occurrence opening after such a
group's count belongs to that entry and stays in `entries` for the per-entry
readers, where the regulatory components hoist to the message deliberately.

`FixCodec.into_components()` maps each column to its extractor and applies
them in order against what the last one left, so a member lifted into one
component's entries cannot also be lifted into another's.

There are no fallback tags: a regenerated dictionary always carries the
declarations of the versions that have them, so a version without one extracts
nothing rather than a group the standard never gave it.

The delimiter leads the projection because it is what opens an entry. Every
member is lifted only where its value is one the column's type can hold, and
stays in `entries` where it is not — so a malformed `UTCTimestamp` is kept as
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

FIXML is a FIX frame whose `XmlData <213>` carries a rendered `NAME=VALUE`
document. A payload that reads as pairs becomes pairs,
under `XmlData.<key>`, in the place the tag sat -- so `XmlData.ClOrdID`
resolves like the rendered `NoPartyIDs.PartyID` already does, and lands in the
column its name earns.

A payload that opens an XML tag, or that carries only one pair, stays exactly
as it was.

The payload is read under its own separator, detected per row: it sits inside
a token of the message around it and so cannot be written with that message's
separator.

## Repeated readings

A rendered line carries two spellings -- `#Side` as a field arrived and
`Side` after enrichment -- and on a third to a half of a real capture's lines
some fields appear in both.

A field is lifted into its column when every reading of it in that row agrees,
and every copy leaves the residual pairs with it. Readings that *disagree* are
left where they were: two values under one key is a repeating group, or an
enrichment that rewrote something, and picking between them would be a guess.

Indexed group members are never readings of one field: `NoPartyIDs[0].PartyID`
and `NoPartyIDs[1].PartyID` share a tag because they are two entries, and both
remain.

## Reading a field

`FieldAccess` (`rekep.fix.access`) is the one way in. A caller holds a field
one of four ways, and every one of them resolves to the same reading:

| how it is named | example |
| --- | --- |
| numeric tag | `770` |
| canonical name | `TrdRegTimestampType` |
| component path | `NoTrdRegTimestamps[0].TrdRegTimestamp` |
| whole vendor key | `TECH.CLIENTID` |

```python
from rekep import FixMsg
from rekep.fix import FieldAccess, FixRegistry

row = FixMsg.from_text("8=FIX.4.4|35=D|11=C1|38=125|10=000")
access = FieldAccess.of(FixRegistry.from_builtin())
found = access.reading(row.entries, "OrderQty")

print(repr(found.raw), repr(found.value))
```

```text
'125' 125.0
```

`Reading.meaning` is the third thing one call answers: what the value means
where its field enumerates its values (`Side=1` is "Buy"). Derived, never
stored -- it is a fact about the dictionary, not about the row.

One call answers every half, so no call site picks an accessor by which one
it wants. The typed reading applies the dictionary's own `encode` before
the cast, so a value spelled by its meaning (`Side=Buy`) resolves without the
call site knowing there was anything to resolve.

A group entry answers a bare name -- `PartyID` finds `NoPartyIDs[0].PartyID`,
because the group is *where* the field sits and not what it is -- while a
vendor key does not: `TECH.CLIENTID` is its own field, and reading it as
`CLIENTID` would file an enrichment value under a standard field.

The rules are declared once and executed twice. `TagIndex` resolves whole
columns in Arrow kernels and `TagIndex.resolve_key` reads the same index one
key at a time, off the same value sets and the same pattern sources, so the
scalar and the vectorized paths cannot answer differently for one input.

`FixMsg.get` uses the same accessor for directly parsed wire text and for a
persisted parsed row. Direct text starts as ordered spellings; registry-backed
transcription then resolves those same entries without another parser model.

## Parsed-log projection

Common fields are promoted once into `FixMsg`. Ordered `entries` keeps every
unpromoted field plus a raw audit sidecar when a typed column cannot reproduce
the wire spelling, such as `0010.5000`.

Typed components are restored as count-led groups and promoted copies
represented by an audit sidecar are suppressed, so scalar and persisted rows
have the same reading; the raw message is never tokenized again.

## Benchmark

`python/benchmarks/bench_fix.py --quick` verifies and measures scalar versus
column parsing, tag projection, groups, and parsed-log reconstruction.
