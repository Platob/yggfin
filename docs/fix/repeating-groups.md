# FIX repeating groups

A repeating group is the list field already embedded in its component tree:

```python
from rekep.fix import FixRegistry

registry = FixRegistry(cache_dir="data/fix.zip")
group = registry.repeating_group("NoPartyIDs")

print(type(group.declaration.dtype).__name__, type(group.declaration.dtype.value_type).__name__)
print(group.versions)
```

```text
ListType StructType
('4.3', '4.4', '5.0', '5.0.SP1', '5.0.SP2')
```

The reviewable record uses the common `Field` list representation:

```json
{
  "name": "NoPartyIDs",
  "versions": ["4.3", "4.4", "5.0", "5.0.SP1", "5.0.SP2"],
  "declaration": {
    "name": "NoPartyIDs",
    "type": "list",
    "fix": {"tag": "453"},
    "item": {
      "name": "PartyID",
      "type": "struct",
      "fields": [
        {"name": "PartyID", "type": "string", "fix": {"tag": "448"}}
      ]
    }
  }
}
```

`data/fix/repgroup/` is derived from the component trees on every registry
write and publication. It gives each group one address without creating a
second declaration to maintain. Archive validation rejects a group index that
does not exactly match its component owners.

[Browse repeating groups](registry.md?ck=repeating#components).
