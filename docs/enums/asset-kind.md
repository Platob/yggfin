# AssetKind

```python
from rekep.enums import AssetKind

kind = AssetKind.from_fix("F")
assert kind is AssetKind.FUTURE
assert kind.is_derivative
assert int(kind) == int.from_bytes(b"FUTURE", "big")
```

Asset kinds are grouped by settlement and instrument structure; the grouping
rides in each member's rank, and the stored value is its readable mnemonic.
Rows without a FIX code are band markers or normalized kinds without one
unique wire spelling.

| Key | Mnemonic | Stored value | Rank | FIX code |
| --- | --- | ---: | ---: | --- |
| `UNKNOWN` | | 0 | 0 | |
| `CASH` | `CASH` | 1128354632 | 100 |  |
| `EQUITY` | `EQUITY` | 76215625536601 | 110 | `E` |
| `DEBT` | `DEBT` | 1145389652 | 120 | `D` |
| `FUND` | `FUND` | 1179995716 | 130 | `C` |
| `CURRENCY` | `CURRENCY` | 4851874686865130329 | 140 | `T` |
| `COMMODITY` | `COMMDTY` | 18946016867013721 | 150 | `J` |
| `INDEX` | `INDEX` | 314845709656 | 160 | `M` |
| `DERIVATIVE` | `DERIV` | 293220796758 | 200 |  |
| `FUTURE` | `FUTURE` | 77332301042245 | 210 | `F` |
| `OPTION` | `OPTION` | 87206430068558 | 220 | `O` |
| `SWAP` | `SWAP` | 1398227280 | 230 | `S` |
| `WARRANT` | `WARRANT` | 24560144796962388 | 240 | `R` |
| `FORWARD` | `FORWARD` | 19790463439557188 | 250 |  |
| `STRUCTURED` | `STRUCTD` | 23455135661511748 | 300 |  |
| `SPREAD` | `SPREAD` | 91604442759492 | 310 |  |
| `MULTILEG` | `MULTILEG` | 5572444038831555911 | 320 |  |
| `BASKET` | `BASKET` | 72848337749332 | 330 |  |
| `FINANCING` | `FINANCE` | 19783848821670725 | 400 |  |
| `REPO` | `REPO` | 1380274255 | 410 |  |
| `LOAN` | `LOAN` | 1280262478 | 420 |  |
