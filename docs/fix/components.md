# FIX components

A component is an Arrow struct in FIX wire order:

```python
from rekep.fix import FixRegistry

registry = FixRegistry()
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

## Typed projections

```python
from pprint import pprint

from rekep.fix.components import Party, SecurityAltID, TrdRegTimestamp

for row in (Party, SecurityAltID, TrdRegTimestamp):
    print(row.__name__)
    pprint(row.into_field().names, width=72)
```

```text
Party
['partyid', 'partyidsource', 'partyrole', 'partyrolequalifier']
SecurityAltID
['securityaltid', 'securityaltidsource', 'symbolpositionnumber']
TrdRegTimestamp
['trdregtimestamp',
 'trdregtimestamptype',
 'trdregtimestamporigin',
 'trdregtimestampmanualindicator',
 'desktype',
 'desktypesource',
 'deskorderhandlinginst',
 'informationbarrierid',
 'nbboentrytype',
 'nbboprice',
 'nbboqty',
 'nbbosource']
```

Every `ComponentGroup` derives its projection from that row declaration. The
column name remains generic inside a struct while `fix:name` identifies the
wire member, such as `symbol` reading `LegSymbol <600>`. Component trees from
the registry decide where the group occurs, which members are required, and
which ordered `fix:tags` can fill one typed member.

The same declaration can build a Python component class:

```python
Parties = registry.component_scalar("Parties", "4.4")
row = Parties(nopartyids=[Parties.PartyID(partyid="BUY-A", partyrole=3)])
```

Messages use the same record shape and add `fix.msgtype`. Component records
live under `data/fix/components/`, one JSON document per identity, and that
document is the `Field` the component declares -- the versions declaring it
ride in its own `fix:versions`, the way a field record carries them.

[Browse components](registry.md#components) or create and update them through
the [registry CLI](shell.md#scriptable-commands).
