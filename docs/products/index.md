# Two tables

rekep publishes one raw text table and one codec-shaped FIX table.

```mermaid
flowchart LR
    C["ULBridge capture"] --> P1[parse_messages]
    P1 --> M[("logs.messages<br/>12 columns")]
    M --> P2[parse_fix]
    P2 --> F[("fix.messages<br/>108 columns")]
```

| | [`logs.messages`](message.md) | [`fix.messages`](fix-message.md) |
| --- | --- | --- |
| one row is | one physical line | one codec result for that line |
| body | exact bytes, uninterpreted | retained and parsed into fixed columns |
| columns | 12 | 108 for the checked registry |
| schema owner | `Message` | Yggdryl FIX schema + runtime registry |
| key | `(url, rownum)` | `(url, rownum)` |
| partition | `timepartition`, hourly | `timepartition`, hourly |
| exact-byte digest | `bodyhash` | carried as `bodyhash` |
| parsed-message digest | none | `msghash` |

The codec deliberately emits a row for prose and malformed input. That keeps
the tables positionally aligned unless `parse_fix.dedup` is explicitly enabled.
Both writers merge on their key, so replay creates no duplicate rows or empty
snapshot.

## Read either table

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
fixes = store.dataset("fix.messages").read_arrow_table()
store.close()
```

## Join and compare identities

Both rows retain `url` and `rownum`, which is the lossless join. `bodyhash`
also arrives unchanged in the fixed row; `msghash` answers a different
question and is not expected to equal it.

```python
joined = fixes.join(messages, keys=["url", "rownum"], right_suffix="_raw")

assert joined.column("bodyhash").equals(joined.column("bodyhash_raw"))
```

The codec folds source columns into fixed fields where their names match. Raw
`timestamp` therefore becomes fixed `timestamp`, and `msgCtxId` becomes
`msgctxid`, without duplicate carrier columns.
