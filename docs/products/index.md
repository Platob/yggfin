# Data products

rekep currently publishes two immutable-grain Iceberg products. Every later
product starts from `fix.messages`, never by reparsing source files.

```mermaid
flowchart LR
    C["capture objects"] --> M[("logs.messages")]
    M --> F[("fix.messages")]
    F -.planned.-> O[("orders")]
    F -.planned.-> E[("executions")]
    F -.planned.-> B[("book")]
```

| product | row grain | key | purpose |
| --- | --- | --- | --- |
| [`logs.messages`](message.md) | one physical source line | `(url, rownum)` | exact replayable capture record |
| [`fix.messages`](fix-message.md) | one codec result for that line | `(url, rownum, msghash)` | typed protocol record plus lossless arrivals |

Both tables are partitioned by the UTC hour derived from the capture
timestamp. Both keep the raw `body` and `bodyhash`; the FIX table additionally
has `msghash`, normalized identifiers, message direction, and every parsed or
unmapped pair.

## Read products

```python
from rekep.iceberg import IcebergCatalog

store = IcebergCatalog.from_dict(
    {
        "name": "rekep",
        "properties": {
            "type": "sql",
            "uri": "sqlite:///data/catalog.db",
            "warehouse": "data/warehouse",
        },
    }
)
messages = store.dataset("logs.messages").read_arrow_table()
fixed = store.dataset("fix.messages").read_arrow_table()
store.close()
```

## Join and audit

```python
joined = fixed.join(messages, keys=["url", "rownum"], right_suffix="_raw")
assert joined.column("bodyhash").equals(joined.column("bodyhash_raw"))
```

`bodyhash` is an exact-byte identity and `msghash` is a parsed-message
identity. The roadmap uses both: source corrections track the former; protocol
deduplication and downstream event identity use the latter.
