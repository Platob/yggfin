# AssetKind

```python
from rekep.enums import AssetKind

kind = AssetKind.from_fix("F")
assert kind is AssetKind.FUTURE
assert kind.is_derivative
assert int(kind) == 210
```

Asset kinds are grouped by settlement and instrument structure. Rows without a
FIX code are band markers or normalized kinds without one unique wire spelling.

| Key | Stored value | FIX code |
| --- | ---: | --- |
| `UNKNOWN` | 0 | |
| `CASH` | 100 | |
| `EQUITY` | 110 | `E` |
| `DEBT` | 120 | `D` |
| `FUND` | 130 | `C` |
| `CURRENCY` | 140 | `T` |
| `COMMODITY` | 150 | `J` |
| `INDEX` | 160 | `M` |
| `DERIVATIVE` | 200 | |
| `FUTURE` | 210 | `F` |
| `OPTION` | 220 | `O` |
| `SWAP` | 230 | `S` |
| `WARRANT` | 240 | `R` |
| `FORWARD` | 250 | |
| `STRUCTURED` | 300 | |
| `SPREAD` | 310 | |
| `MULTILEG` | 320 | |
| `BASKET` | 330 | |
| `FINANCING` | 400 | |
| `REPO` | 410 | |
| `LOAN` | 420 | |
