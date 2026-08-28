# AssetKind

```python
from rekep.enums import AssetKind

kind = AssetKind.from_fix("F")
assert kind is AssetKind.FUTURE
assert kind.is_derivative
assert int(kind) == int.from_bytes(b"FUTURE\0\0", "big")
```

Asset kinds are grouped by settlement and instrument structure; the grouping
rides in each member's rank, and the stored value is its readable mnemonic.
Rows without a FIX code are band markers or normalized kinds without one
unique wire spelling.

| Key | Mnemonic | Stored value | Rank | FIX code |
| --- | --- | ---: | ---: | --- |
| `UNKNOWN` |  | 0 | 0 |  |
| `CASH` | `CASH` | 4846246242730115072 | 100 |  |
| `EQUITY` | `EQUITY` | 4994867235166683136 | 110 | `E` |
| `DEBT` | `DEBT` | 4919411096516820992 | 120 | `D` |
| `FUND` | `FUND` | 5068043009640103936 | 130 | `C` |
| `CURRENCY` | `CURRENCY` | 4851874686865130329 | 140 | `T` |
| `COMMODITY` | `COMMDTY` | 4850180317955512576 | 150 | `J` |
| `INDEX` | `INDEX` | 5282234477571997696 | 160 | `M` |
| `DERIVATIVE` | `DERIV` | 4919428642901065728 | 200 |  |
| `FUTURE` | `FUTURE` | 5068049681104568320 | 210 | `F` |
| `OPTION` | `OPTION` | 5715160600973017088 | 220 | `O` |
| `SWAP` | `SWAP` | 6005340439975034880 | 230 | `S` |
| `WARRANT` | `WARRANT` | 6287397068022371328 | 240 | `R` |
| `FORWARD` | `FORWARD` | 5066358640526640128 | 250 |  |
| `STRUCTURED` | `STRUCTD` | 6004514729347007488 | 300 |  |
| `SPREAD` | `SPREAD` | 6003388760686067712 | 310 |  |
| `MULTILEG` | `MULTILEG` | 5572444038831555911 | 320 |  |
| `BASKET` | `BASKET` | 4774188662740221952 | 330 |  |
| `FINANCING` | `FINANCE` | 5064665298347705600 | 400 |  |
| `REPO` | `REPO` | 5928232784735764480 | 410 |  |
| `LOAN` | `LOAN` | 5498685473305919488 | 420 |  |
