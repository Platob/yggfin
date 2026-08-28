# MIC

```python
from rekep.enums import MIC

venue = MIC.from_str("XPAR")
assert venue.code == "XPAR"
assert MIC.from_int(int(venue)) is venue
```

MIC accepts any four-character uppercase ISO 10383 spelling matching
`[A-Z0-9]{4}`. The spelling fills all four big-endian ASCII bytes of one
`int32`, so it needs no padding; the table lists the built-in special values.

| Key | Code | Stored value | Meaning |
| --- | --- | ---: | --- |
| `UNKNOWN` |  | 0 | No valid market identifier was present. |
| `XOFF` | `XOFF` | 1,481,590,342 | Off-market transaction. |
| `XXXX` | `XXXX` | 1,482,184,792 | No market, including an unlisted instrument. |
