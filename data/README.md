# FIX registry data

`data/fix/` is the reviewable versioned FIX registry; `data/fix.zip` is its
deterministic compressed copy. Runtime jobs can therefore stay offline.

```python
from rekep.fix import FixRegistry

registry = FixRegistry(cache_dir="data/fix", offline=True)
field = registry.field("Side", "4.4")
```

Each field stores its Arrow projection, short description, tag, FIX datatype,
valid values, version, and component/message usage. Both published forms must
contain identical documents.

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
