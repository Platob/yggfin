# FIX registry data

`data/fix/` is the reviewable FIX registry; `data/fix.zip` is its deterministic
compressed copy. Runtime jobs can therefore stay offline.

```text
data/fix/versions.json          the version list, session layers, and which
                                versions have had their spec read
data/fix/fields/000000.json     tags 0-999, one cross-version record each
data/fix/fields/000030.json     tags 30000-30999, rekep package vocabulary
data/fix/fields/000040.json     tags 40000-40999, the 5.0.SP2 extension pack
data/fix/fields/999999.json     the fields FIX never numbered
data/fix/components/parties.json  one component, declared as a Field
data/fix/components/new_order_single.json  a message, declared the same way
data/fix/repgroup/no_party_i_ds.json  one derived repeating-group list Field
data/fix/namespaces/fixtrading-udf/fields/000009.json
                                registered definitions for tags 9000-9999
data/fix/namespaces/clear-street/fields/000009.json
                                Clear Street definitions for the same tag range
data/fix/sources.json           complete-source URLs, versions, checksums, and terms
data/fix-conflicts.json         every reading the collapse dropped
```

A field's record is cross-version by nature: one tag, one reading, and
`versions` -- the list of versions that declare it. Shards hold one thousand
tags each and are named by the shard index, so the document holding a tag is
`tag // 1000` and nothing has to be looked up; a field FIX never numbered keys
by its name and lands in `999999`, the one index no tag reaches. The tag space
is sparse, so eleven files hold the populated ranges and named fields. Empty
ranges are absent, and a lookup reads one shard.

Every document here is a field document, and a shard is a JSON *list* of them:

```json
[
 {"name": "LastQty", "type": "double", "nullable": true,
  "fix": {"tag": "32", "type": "Qty", "versions": ["4.4", "5.0"]}}
]
```

A record states the tag or the name it is, so a key above it would be that
identity written twice -- and two spellings of one fact are one fact that can
contradict itself. A component and a repeating group are stored the same way:
the `Field` they declare, carrying the versions declaring them and the names
they answer to in their own `fix`, exactly as a field record does. One
serialization, so a reader that can read a field can read the dictionary.

FIX Latest Orchestra is the standard authority. Its datatypes, code sets,
messages, components, nested groups, pedigree, and deprecation metadata are
read from one complete XML file. QuickFIX and the existing standard shards
remain fallbacks, so refreshing extensions does not remove an older standard
definition.

| source ID | namespace | complete input | refresh policy |
| --- | --- | --- | --- |
| `fix-latest` | `standard` | [FIX Latest EP309 Orchestra XML](https://raw.githubusercontent.com/FIXTradingCommunity/orchestrations/master/FIX%20Standard/OrchestraFIXLatest.xml) | default, Apache-2.0 repository |
| `quickfix` | `standard` | [FIX 5.0 SP2 EP280 XML](https://raw.githubusercontent.com/quickfix/quickfix/master/spec/FIX50SP2.xml) | standard fallback |
| `fixtrading-udf` | `fixtrading-udf` | [registered UDF Orchestra XML](https://orchestrahub.org/community/fix-udf) | requested source, terms recorded in manifest |
| `clear-street` | `clear-street` | [official FIX Markdown](https://github.com/clear-street/FIX-docs) | explicit venue source, Apache-2.0 |

The generated store currently contains 10,324 definitions:

| namespace | fields | components | groups | messages |
| --- | ---: | ---: | ---: | ---: |
| `standard` | 6,285 | 927 | 525 | 193 |
| `fixtrading-udf` | 4,028 | 126 | 0 | 0 |
| `clear-street` | 11 | 0 | 0 | 0 |

The refresh records 2,107 reviewed conflicts and five UDF string fallbacks.

Cboe, Kraken, MIAX, Lime, FalconX, Eurex/Xetra, and B2BITS are catalogued as
excluded sources. Their current format or terms do not support a reviewed,
deterministic artifact here; the CLI reports the exact exclusion and a default
refresh does not fetch them.

The [FIX Repository](https://www.fixtrading.org/standards/fix-repository/),
[Orchestra model](https://github.com/FIXTradingCommunity/fix-orchestra), and
[UDF PDF](https://fixtrading.org/packages/user-defined-fields-pdf/) cross-check
the complete machine-readable sources. Nanoconda, OnixS, and B2BITS enrich or
validate descriptions but never outrank FIX Latest.

An authorized Orchestra file needs only a source declaration; the shared
parser supplies fields, code sets, messages, components, and groups:

```python
import hashlib
from pathlib import Path

from rekep.fix.adapters import OrchestraAdapter

document = Path("/srv/fix/venue.xml")
source = OrchestraAdapter(
    source_id="venue",
    namespace="venue",
    version="1.2",
    url=document.as_uri(),
    format="orchestra",
    checksum=f"sha256:{hashlib.sha256(document.read_bytes()).hexdigest()}",
    license_url="https://venue.example/terms",
    default=False,
)
parsed = source.load("data/.fix-sources")
assert parsed.messages and parsed.components
```

Extensions keep their own identities. Unscoped lookup is ordered:

```text
standard -> fixtrading-udf -> configured venue namespaces
```

Registered UDFs therefore fill the reserved range without replacing a later
standard definition. A namespaced lookup can still select every alternative:

```python
from rekep.fix import FixRegistry

registry = FixRegistry(cache_dir="data/fix")
assert registry.field(9001).fix.canonical == "MaxShow"
assert registry.field(9001, namespace="clear-street").fix.canonical == "TradeType"
assert [field.fix.get("namespace") for field in registry.definitions(9001)] == [
    "fixtrading-udf",
    "clear-street",
]
```

Each field stores its primary source, every source that answered, and the
source of each scalar and enumeration value. An authoritative datatype maps to
one Arrow type; an unknown or disputed datatype stays `string` and preserves
its original spelling in FIX metadata. Unknown enumeration values remain raw
strings.

```python
from rekep.fix import FixRegistry

registry = FixRegistry(cache_dir="data/fix")
field = registry.field("Side", "4.4")
field.fix.encode("BUY")  # '1'
```

The registry is also the source of the package fields and per-field timezone
refinements:

```python
for name in ("OrigTime", "MarketEventType", "XHash", "LinkHashes", "AltIDs"):
    field = registry.field(name, "5.0.SP2")
    print(f"{name:18} {field.dtype}")
```

```text
OrigTime           timestamp[us, tz=UTC]
MarketEventType    int64
XHash              fixed_size_binary[16]
LinkHashes         list<item: fixed_size_binary[16] not null>
AltIDs             map<string, string>
```

Package fields use bare names. `MarketEventType <30002>` distinguishes the
package event kind from standard `EventType <865>`; the six package message
declarations retain `REKEP.`. The event venue uses standard `LastMkt <30>`
with the packed `MIC` enum. Tags 30018, 30022, and 30023 stay empty:
`RekepHeader` references `LastMkt`; `RekepMarket` references standard
`Price <44>` and `LastQty <32>`.

```bash
cd python
uv run rekep fix registry coverage --store ../data/fix
```

Where sources disagree the first reading wins. Where versions disagree the
newest application version wins; `FIXT1.1` is the session transport and never
owns an application field's reading. Every dropped reading and its source is
written to `data/fix-conflicts.json`. Its counts are pinned in
`rekep.fix.publish.CONFLICT_BASELINE`, so a refresh that introduces conflicts
nobody looked at fails rather than shipping them.

Promote a rendered bridge name into a typed column, record a spelling, or
check the whole store. `promote` is one call whether the name is brand new or
a classification run already declared it without a column; it refuses a
standard tagged field and refuses to move a column already assigned:

```bash
cd python
uv run rekep fix registry promote --store ../data/fix \
    --name TECH.CLIENTID --column techclientid
uv run rekep fix registry check --store ../data/fix
```

Or browse and edit it at a prompt, where every verb above is one command and a
new field is built one answered question at a time:

```bash
uv run rekep fix shell --store ../data/fix
```

`scrape` is the only operation that reads a source. Complete files are cached
beside the target, verified by SHA-256, then parsed; a second refresh can be
fully offline:

```bash
cd python
uv run rekep fix registry scrape --output ../data/fix --source fix-latest \
    --source quickfix --source fixtrading-udf --source clear-street \
    --conflicts ../data/fix-conflicts.json
uv run rekep fix registry scrape --output ../data/fix --source fix-latest \
    --source quickfix --source fixtrading-udf --source clear-street --offline
uv run python -c "from rekep.fix.publish import publish_full; \
publish_full('../data/fix', '../data/fix.zip')"
```

Refresh only the registered definitions, or inspect a collision without
changing the standard shards:

```bash
uv run rekep fix registry scrape --output ../data/fix --source fixtrading-udf
uv run rekep fix registry find --store ../data/fix 9001
uv run rekep fix registry find --store ../data/fix 9001 \
    --namespace fixtrading-udf
uv run rekep fix registry definitions --store ../data/fix 9001
uv run rekep fix registry coverage --store ../data/fix
```

The source cache is `data/.fix-sources` for `data/fix`. A restricted venue
descriptor is not executable: add and review an adapter for the authorized
complete file before selecting it. `sources.json` records the source ID,
namespace, version, URL, format, checksum, and license or terms URL for every
artifact that contributed data.

`data/fix` is itself the dictionary every unconfigured lookup reads, so there
is nothing further to rebuild: `data/fix.zip` is that directory packed for
offline distribution, carrying the same standard, registered-UDF, venue,
provenance, component, and message records.

`python/tests/test_data.py` checks completeness, byte-stable archive
rebuilding, that the directory and the archive agree, and that the committed
conflict report is what the collapse reports.
