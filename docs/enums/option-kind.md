# OptionKind

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
| `PUT` | `PUT` | 5788625255031373824 | 100 | `0` |
| `CALL` | `CALL` | 4846238563328589824 | 200 | `1` |
