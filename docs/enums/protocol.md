# Protocol

[`Ascii64`](ascii-codes.md){ .enum-base } — eight bytes of printable ASCII packed left-justified into one `int64`, an open vocabulary, so a code it meets and can round-trip registers itself.

```python
from rekep.enums import Protocol

carried = Protocol.from_str("FIX")
assert carried.code == "FIX"
assert Protocol.from_int(int(carried)) is carried
```

Which grammar a payload is written in, decided by the keys it holds and never
by their values. The vocabulary belongs to the logs rather than to this
package: [`rekep.fix.rules`](../fix/configuring.md#which-lines-carry-a-message)
ships the five below, and a rule naming its own bridge stores that name
without a release here. Up to eight bytes of `[A-Z0-9._-]` is the shape such a
name has to fit -- the shape a code reads back as, so one name never packs as
two -- and a rule declaring anything else is refused rather than stored as
`UNKNOWN`.

`Message.protocol` and `FixMsg.protocol` are the columns that hold it. A line
no rule recognised is `OTHER`, which is most of a capture, so the column is
NOT NULL and `OTHER` is what a row starts as.

| Key | Code | Stored value | Meaning |
| --- | --- | ---: | --- |
| `UNKNOWN` |  | 0 | No name resolved. |
| `FIX` | `FIX` | 5064676012978077696 | Numbered FIX tags alone. |
| `FIXML` | `FIXML` | 5064676344965627904 | Numbered tags and named keys together. |
| `UL` | `UL` | 6146287591453884416 | Named keys alone. |
| `MISC` | `MISC` | 5569073961448243200 | Known operational traffic carrying no message. |
| `OTHER` | `OTHER` | 5716273289605677056 | The fall-through: a line no rule recognised. |
