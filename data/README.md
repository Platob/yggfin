# FIX registry data

`data/fix/` is the reviewable FIX registry, one file per field or component
identity; `data/fix.zip` is its deterministic compressed copy. Runtime jobs
can therefore stay offline.

```text
data/fix/versions.json            the version list, session layers, and which
                                  versions have had their spec read
data/fix/fields/party_role.json   one field, and every version's reading of it
data/fix/components/parties.json  one component, and every version's members
```

```python
from rekep.fix import FixRegistry

registry = FixRegistry(cache_dir="data/fix", offline=True)
field = registry.field("Side", "4.4")
```

Each field stores its canonical name, tag, the spellings it also answers to,
and a per-version map of the datatype, description, valid values, symbols and
message usage that version gives it. Both published forms must contain
identical documents.

Add a field, record a spelling, or check the whole store:

```bash
cd python
uv run rekep fix registry add-field --store ../data/fix \
    --name TECH.CLIENTID --type String --column tech_client_id
uv run rekep fix registry check --store ../data/fix
```

Refresh deliberately, then rebuild the archive:

```bash
cd python
uv run python -c "from rekep.fix import FixRegistry; r=FixRegistry(cache_dir='../data/fix'); r.load(refresh=True); r.into_zip('../data/fix.zip')"
```

Then rebuild the projection the wheel ships, which selects the keys
`rekep.fix.publish.PROJECTED` names and carries every component declaration:

```bash
cd python
uv run python -c "from rekep.fix.publish import publish_builtin; \
publish_builtin('../data/fix.zip', 'src/rekep/fix/registry.zip')"
```

`python/tests/test_data.py` checks completeness, byte-stable archive
rebuilding, and that the published projection is what publishing produces.
