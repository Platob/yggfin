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
data/fix-conflicts.json         every reading the collapse dropped
```

A field's record is cross-version by nature: one tag, one reading, and
`versions` -- the list of versions that declare it. Shards hold one thousand
tags each and are named by the shard index, so the document holding a tag is
`tag // 1000` and nothing has to be looked up; a field FIX never numbered keys
by its name and lands in `999999`, the one index no tag reaches. The tag space
is sparse, so ten files hold the populated ranges and named fields. Empty
ranges are absent, and a lookup reads one shard.

Three ordered sources fill it. Nanoconda supplies the first reading and the
symbolic name of every enumerated value. OnixS fills missing prose, values and
usage. The QuickFIX spec fills machine-readable types, extension-pack fields,
session layers and the component and message trees. Each field stores its
primary source, every source that answered, and the source of each scalar and
value part. JSON-valued metadata stays as nested arrays and objects in the
shards; the reader restores its compact Arrow metadata string on load.
Prose-only valid-value lists are not enumerations: every published enumeration
has a source-supplied symbolic name for each wire value.

```python
from rekep.fix import FixRegistry

registry = FixRegistry(cache_dir="data/fix")
field = registry.field("Side", "4.4")
field.fix.encode("BUY")  # '1'
```

The registry is also the source of the package fields and per-field timezone
refinements:

```python
for name in ("OrigTime", "MarketEventType", "XHash", "LinkHashes", "CodeSource"):
    field = registry.field(name, "5.0.SP2")
    print(f"{name:18} {field.dtype}")
```

```text
OrigTime           timestamp[us, tz=UTC]
MarketEventType    int64
XHash              fixed_size_binary[16]
LinkHashes         list<item: fixed_size_binary[16] not null>
CodeSource         string
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

Replace the dictionary from every source -- about fourteen thousand pages and
several hours:

```bash
cd python
uv run rekep fix registry scrape --output ../data/fix \
    --conflicts ../data/fix-conflicts.json
uv run python -c "from rekep.fix.publish import publish_full; \
publish_full('../data/fix', '../data/fix.zip')"
```

Then rebuild the projection the wheel ships. It selects the standard keys
`rekep.fix.publish.PROJECTED` names, adds rekep's 35 frozen fields, and carries
every declaration, messages included:

```bash
cd python
uv run python -c "from rekep.fix.publish import publish_builtin; \
publish_builtin('../data/fix.zip', 'src/rekep/fix/registry.zip')"
```

`python/tests/test_data.py` checks completeness, byte-stable archive
rebuilding, that the published projection is what publishing produces, and
that the committed conflict report is what the collapse reports.
