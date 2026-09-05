# FIX fields

Read a field by tag, canonical name, or recorded alias:

```python
from rekep.fix import FixRegistry

registry = FixRegistry()
side = registry.field("Side", "4.4")

print(side.name, side.fix.tag, side.dtype)
print(side.fix.spellings())
```

```text
Side 54 string
('Side',)
```

A stored record is an Arrow field plus readable FIX metadata:

```json
{
  "name": "SettlDate",
  "type": "timestamp[us]",
  "nullable": true,
  "fix": {
    "tag": "64",
    "type": "LocalMktDate",
    "versions": ["4.0", "4.1", "4.2", "4.3", "4.4", "5.0"],
    "aliases": [{"name": "FutSettDate", "source": "4.3"}]
  }
}
```

The Arrow type is the runtime contract. `fix.type` records the protocol
spelling that produced it.

## Ordered aliases

Canonical names lead, then aliases in stored order. The first spelling present
in a row fills a promoted column; a lower-priority spelling remains in
`entries` when a higher-priority value was available.

```bash
rekep fix registry alias-field --store data/fix \
  --name BRKR.VenueTier --alias BROKER_VENUE_TIER --source broker-a
rekep fix registry show --store data/fix BRKR.VenueTier
```

Fields are sharded by `tag // 1000` under `data/fix/records/`, the one
keyspace that also holds the components, the messages and the repeating
groups. A record with no tag keys by its canonical name and lands in one of
the sixteen named shards above every reachable tag index.

[Browse fields](registry.md#fields) or use the [registry CLI](shell.md).
