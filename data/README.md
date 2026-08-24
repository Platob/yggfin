# FIX registry data

`data/fix/` is the reviewable FIX registry; `data/fix.zip` is its deterministic
compressed copy. Runtime jobs can therefore stay offline.

```text
data/fix/versions.json          the version list, session layers, and which
                                versions have had their spec read
data/fix/fields/000000.json     tags 0-499, one cross-version record each
data/fix/fields/000080.json     tags 40000-40499, the 5.0.SP2 extension pack
data/fix/fields/named.json      the fields FIX never numbered
data/fix/components/parties.json  one component's member tree
data/fix-conflicts.json         every reading the collapse dropped
```

A field's record is cross-version by nature: one tag, one reading, and
`versions` -- the list of versions that declare it. Shards hold five hundred
tags each and are named by the shard index, so the document holding a tag is
`tag // 500` and nothing has to be looked up; the tag space is sparse, so
fourteen shards answer for six thousand fields and the empty ranges are simply
absent. A lookup for one tag reads one shard.

Two sources fill it. The OnixS dictionary supplies prose, enumerated values and
where each field is used -- in messages *and* in component blocks, which is how
a field FIX only carries inside a component (`TrdRegTimestamp <769>`, and three
hundred others in 4.4 alone) records where it lives. The QuickFIX spec supplies
the symbolic name of every enumerated value and every field an extension pack
numbered past what the site wrote up. Both are needed: without the spec there
are no symbols, and `translations` is built from them.

```python
from rekep.fix import FixRegistry

registry = FixRegistry(cache_dir="data/fix", offline=True)
field = registry.field("Side", "4.4")
registry.resolve("Side").translate("BUY")  # '1'
```

Where two versions disagree the newest application version wins -- `FIXT1.1` is
the session transport and never owns an application field's reading -- and
every dropped reading is written to `data/fix-conflicts.json`. Its counts are
pinned in `rekep.fix.publish.CONFLICT_BASELINE`, so a refresh that introduces
conflicts nobody looked at fails rather than shipping them.

Add a field, record a spelling, or check the whole store:

```bash
cd python
uv run rekep fix registry add-field --store ../data/fix \
    --name TECH.CLIENTID --type String --column tech_client_id
uv run rekep fix registry check --store ../data/fix
```

Or browse and edit it at a prompt, where every verb above is one command and a
new field is built one answered question at a time:

```bash
uv run rekep fix shell --store ../data/fix
```

Rebuild the whole dictionary from both sources -- one scrape of seven thousand
pages, several minutes -- and write the collapse report beside it:

```bash
cd python
uv run rekep fix registry bootstrap --store ../data/fix \
    --report ../data/fix-conflicts.json
uv run python -c "from rekep.fix import FixRegistry; \
FixRegistry(cache_dir='../data/fix', offline=True).into_zip('../data/fix.zip')"
```

Then rebuild the projection the wheel ships, which selects the keys
`rekep.fix.publish.PROJECTED` names and carries every component declaration:

```bash
cd python
uv run python -c "from rekep.fix.publish import publish_builtin; \
publish_builtin('../data/fix.zip', 'src/rekep/fix/registry.zip')"
```

`python/tests/test_data.py` checks completeness, byte-stable archive
rebuilding, that the published projection is what publishing produces, and
that the committed conflict report is what the collapse reports.
