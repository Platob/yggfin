# OptionKind

[`Ascii32`](ascii-codes.md){ .enum-base } — four bytes of printable ASCII
packed left-justified into one `int32`.

```python
from rekep.enums import OptionKind

assert OptionKind.from_fix("0") is OptionKind.PUT
assert OptionKind.from_fix("1") is OptionKind.CALL
```

Option direction comes from FIX `PutOrCall <201>`; the stored value is the
readable mnemonic.

| Key | Mnemonic | Stored value | Rank | FIX code |
| --- | --- | ---: | ---: | --- |
| `UNKNOWN` |  | 0 | 0 |  |
| `PUT` | `PUT` | 1347769344 | 100 | `0` |
| `CALL` | `CALL` | 1128352844 | 200 | `1` |
