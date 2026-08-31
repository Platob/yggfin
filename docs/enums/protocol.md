# Protocol

[`Ascii64`](ascii-codes.md){ .enum-base } — eight bytes of printable ASCII packed left-justified into one `int64`, an open vocabulary, so a code it meets and can round-trip registers itself.

```python
from rekep.enums import Protocol

carried = Protocol.from_str("FIX.5.0.SP2")
assert carried.code == "FIX5SP2"
assert carried.family is Protocol.FIX
assert carried.version == "5.0.SP2"
assert Protocol.from_int(int(carried)) is carried
```

The grammar comes first and the resolved version follows it. Service packs
drop punctuation to fit exactly: `FIX5SP2` is FIX 5.0 SP2. `FXML5SP2` keeps
the FIXML grammar and the same version.

The vocabulary belongs to the logs rather than to this package. A rule naming
its own bridge stores that name without a release here. Up to eight bytes of
`[A-Z0-9._-]` is the stored shape.

`Message.protocol` carries the grammar found without a registry. `FixMsg.protocol`
adds the version resolved from `BeginString` and application-version fields. A
line no rule recognised is `OTHER`, so the column is NOT NULL.

| Key | Code | Stored value | Meaning |
| --- | --- | ---: | --- |
| `UNKNOWN` |  | 0 | No name resolved. |
| `FIX` | `FIX` | 5064676012978077696 | Numbered FIX tags alone. |
| `FIXML` | `FIXML` | 5064676344965627904 | Numbered tags and named keys together. |
| `UL` | `UL` | 6146287591453884416 | Named keys alone. |
| `MISC` | `MISC` | 5569073961448243200 | Known operational traffic carrying no message. |
| `OTHER` | `OTHER` | 5716273289605677056 | The fall-through: a line no rule recognised. |
