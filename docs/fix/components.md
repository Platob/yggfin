# FIX components

A component is an Arrow struct in FIX wire order:

```python
from rekep.fix import FixRegistry

registry = FixRegistry(cache_dir="data/fix.zip")
parties = registry.component_field("Parties", "4.4")

print(type(parties.dtype).__name__)
print([field.name for field in parties.fields])
```

```text
StructType
['nopartyids']
```

The stored declaration keeps FIX names and tags. References stay collapsed so
one component is not copied into every owner:

```json
{
  "name": "Instrument",
  "type": "struct",
  "nullable": true,
  "fix": {"component": "Instrument"},
  "fields": []
}
```

`component_field()` expands references when it builds the Arrow projection.
Required FIX members become non-null fields; optional members remain nullable.

The same declaration can build a Python component class:

```python
Parties = registry.component_scalar("Parties", "4.4")
row = Parties(nopartyids=[Parties.PartyID(partyid="BUY-A", partyrole=3)])
```

Messages use the same record shape and add `fix.msgtype`. Component records
live under `data/fix/components/`, one JSON document per identity.

[Browse components](registry.md#components) or create and update them through
the [registry CLI](shell.md#scriptable-commands).
