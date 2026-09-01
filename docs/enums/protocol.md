# Protocol

[`Ascii128`](ascii-codes.md){ .enum-base } — sixteen bytes of printable ASCII
stored as `fixed_size_binary[16]`, with valid unseen codes registered on read.

```python
from rekep.enums import Protocol

carried = Protocol.from_str("FIX.5.0.SP2")
assert carried.code == "FIX5SP2"
assert carried.family is Protocol.FIX
assert carried.version == "5.0.SP2"
assert Protocol.from_stored(carried.into_stored()) is carried
```

The grammar comes first and the resolved version follows it. Service packs
drop punctuation to fit exactly: `FIX5SP2` is FIX 5.0 SP2. `FXML5SP2` keeps
the FIXML grammar and the same version.

The vocabulary belongs to the logs rather than to this package. A rule naming
its own bridge stores that name without a release here. Up to sixteen bytes of
`[A-Z0-9._-]` is the stored shape.

`Message.protocol` carries the grammar found without a registry. `FixMsg.protocol`
adds the version resolved from `BeginString` and application-version fields. A
line no rule recognised is `OTHER`, so the column is NOT NULL.

| Key | Code | Stored hex | Meaning |
| --- | --- | --- | --- |
| `UNKNOWN` |  | `00000000000000000000000000000000` | No name resolved. |
| `FIX` | `FIX` | `46495800000000000000000000000000` | Numbered FIX tags alone. |
| `FIXML` | `FIXML` | `4649584d4c0000000000000000000000` | Numbered tags and named keys together. |
| `XML` | `XML` | `584d4c00000000000000000000000000` | Structured XML events without a FIX version. |
| `REFERENTIAL` | `REFER` | `52454645520000000000000000000000` | Depth-delimited ULBridge instrument reference data. |
| `UL` | `UL` | `554c0000000000000000000000000000` | Named keys alone. |
| `MISC` | `MISC` | `4d495343000000000000000000000000` | Known operational traffic carrying no message. |
| `OTHER` | `OTHER` | `4f544845520000000000000000000000` | The fall-through: a line no rule recognised. |
