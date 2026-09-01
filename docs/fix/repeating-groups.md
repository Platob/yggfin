# FIX repeating groups

A repeating group is the list field already embedded in its component tree:

```python
from rekep.fix import FixRegistry

registry = FixRegistry()
group = registry.repeating_group("NoPartyIDs")

print(type(group.declaration.dtype).__name__, type(group.declaration.dtype.value_type).__name__)
print(group.versions)
```

```text
ListType StructType
('4.3', '4.4', '5.0', '5.0.SP1', '5.0.SP2')
```

ULBridge may omit separators after the first pair while retaining an explicit
entry index:

```text
#NoPartyIDs=1|#NoPartyIDs[0]=PartyID=P-1PartyIDSource=DPartyRole=3
```

Each `parse_fix_*` task splits only names declared by that group's registry
component. It uses the longest declared match, extracts partial or
out-of-order indices, and leaves unknown members in `entries`. A disputed
split or a count that differs from the indexed members is kept in the row's
`error`; the row still parses.

## Control-separated members

```python
from rekep import FixMsg, Message
from rekep.enums import Protocol

sub = "\x04\x03"
payload = (
    "#NOPARTYIDS=1|"
    "#NOPARTYIDS[0]=PARTYID=SYNTH-01" + sub
    + "PARTYIDSOURCE=shortcodeid" + sub
    + "PARTYROLE=executingsystem" + sub
    + "PARTYROLEQUALIFIER=exchangeordersubmitter|"
)
row = FixMsg.from_message_batch([Message.from_text(payload)]).to_pylist()[0]

print(Protocol.from_stored(row["protocol"]).version)
print(row["parties"])
```

```text
5.0.SP2
[{'partyid': 'SYNTH-01', 'partyidsource': 'P', 'partyrole': 16, 'partyrolequalifier': 30}]
```

The outer `|` keeps the indexed entry together; EOT/ETX separates members
inside it. The registry supplies member boundaries and turns unambiguous value
meanings into FIX codes. This evidence-free UL row records the packaged
registry's newest application version, currently `5.0.SP2`.

Unknown venue meanings stay lossless:

```python
for qualifier in ("buyside", "sellside"):
    unknown = (
        payload.replace("shortcodeid", "proprietary/customcode")
        .replace("executingsystem", "orderoriginatorsystem")
        .replace("exchangeordersubmitter", qualifier)
    )
    row = FixMsg.from_message_batch([Message.from_text(unknown)]).to_pylist()[0]

    print(qualifier, row["parties"][0])
    print([(entry["tag"], entry["value"]) for entry in row["entries"]])
```

```text
buyside {'partyid': 'SYNTH-01', 'partyidsource': 'D', 'partyrole': None, 'partyrolequalifier': None}
[(452, 'orderoriginatorsystem'), (2376, 'buyside')]
sellside {'partyid': 'SYNTH-01', 'partyidsource': 'D', 'partyrole': None, 'partyrolequalifier': None}
[(452, 'orderoriginatorsystem'), (2376, 'sellside')]
```

No feed-specific table assigns those spellings a numeric meaning. The typed
members are null and the source values remain in `entries`.

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
