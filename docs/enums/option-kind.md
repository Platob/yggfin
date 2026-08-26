# OptionKind

```python
from rekep.enums import OptionKind

assert OptionKind.from_fix("0") is OptionKind.PUT
assert OptionKind.from_fix("1") is OptionKind.CALL
```

Option direction comes from FIX `PutOrCall <201>`.

| Key | Stored value | FIX code |
| --- | ---: | --- |
| `UNKNOWN` | 0 | |
| `PUT` | 100 | `0` |
| `CALL` | 200 | `1` |
